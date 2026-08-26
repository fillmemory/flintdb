#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "flintdb.h"
#include "internal.h"
#include "runtime.h"
#include "buffer.h"
#include "storage.h"

// row_bytes is implemented in table.c; declare prototype for use here.
extern int row_bytes(const struct flintdb_meta *m);
#define MIN(a,b) (((a)<(b))?(a):(b))

/*
 * File-based external sorter (decorate-sort / index permutation)
 * ==============================================================
 *
 * Layout
 * ------
 * Two layers, never mixed:
 *
 *   storage  : row payloads written once in insertion order. Never moved,
 *              rewritten, or swapped during sort (no write_at of records).
 *   offsets[]: i64 rowids into storage. This is the only array that changes
 *              order. After sort, offsets[i] is the payload of the i-th
 *              sorted row; filesort_read(i) follows that pointer.
 *
 * Why not swap records
 * --------------------
 * Comparator API is still cmpr(row, row), so a naive get/put sort would
 * decode a full row on every comparison (and, worse, rewrite payloads).
 * Bottom-up merge used to call filesort_read inside the merge loop: O(n log n)
 * storage I/O + alloc/decode/free per compare. Payloads on disk did not move,
 * but the comparison unit was still the whole record.
 *
 * Decorate-sort (what we do)
 * --------------------------
 *  1. Sequential pass: decode each offsets[i] once into decoded[i].
 *     decoded[i]->rowid == offsets[i]. Rows are comparison keys only.
 *  2. perm[i] = i. Merge-sort permutes perm[], not rows and not payloads.
 *     Compare is cmpr(decoded[perm[a]], decoded[perm[b]]) — RAM only.
 *  3. Apply: offsets[i] = offsets[perm[i]] (via aux). Then free decoded[].
 *
 * Swap unit is one i64. A flintdb_row is never an exchange target.
 *
 * Why merge, not qsort
 * --------------------
 * qsort_r argument order differs between macOS/BSD and glibc. Merge is
 * iterative (bounded stack) and stable: cmpr <= 0 takes the left run, so
 * equal keys keep insertion order (see TESTCASE_SORTABLE stability case).
 *
 * filesort_read is for result iteration after sort (and for the decorate
 * pass via filesort_decode_offset). The merge hot path does not call it.
 *
 * Row pool is not used here. Pool keys by meta pointer; priv->meta is a
 * copy freed in filesort_close, so pooled rows would dangle / mix schemas
 * if the next sorter reused that address. Use flintdb_row_new and free.
 */


struct flintdb_filesort_priv {
	struct storage storage;      // row payloads (insertion order, immutable during sort)
	struct formatter formatter;  // row encoder/decoder (binary)
	struct flintdb_meta meta;    // copy of table meta; valid until filesort_close

    i32 row_bytes;              // cached row byte size
	i64 *offsets;                // permutation of storage rowids; sort reorders this only
	i64 rows;                    // number of rows
	i64 cap;                     // capacity of offsets array
};

static int ensure_capacity(struct flintdb_filesort *me, i64 need, char **e) {
	struct flintdb_filesort_priv *priv = (struct flintdb_filesort_priv *)me->priv;
    if (need <= priv->cap)
        return 0;
    i64 ncap = priv->cap > 0 ? priv->cap : 1024;
    while (ncap < need)
        ncap <<= 1;
    i64 *n = (i64 *)REALLOC(priv->offsets, sizeof(i64) * (size_t)ncap);
    if (!n) {
        THROW(e, ERR_OUT_OF_MEMORY);
    }
    priv->offsets = n;
    priv->cap = ncap;
    return 0;
EXCEPTION:
    return -1;
}

void filesort_close(struct flintdb_filesort *me) {
	struct flintdb_filesort_priv *priv = (struct flintdb_filesort_priv *)me->priv;
    if (!priv)
        return;
    if (priv->offsets) {
        FREE(priv->offsets);
        priv->offsets = NULL;
    }
    if (priv->storage.fd > 0) {
        priv->storage.close(&priv->storage);
    }
    if (priv->formatter.close) {
        priv->formatter.close(&priv->formatter);
    }

	FREE(priv);
	FREE(me);
	priv = NULL;
}

i64 filesort_rows(const struct flintdb_filesort *me) {
    struct flintdb_filesort_priv *priv = (struct flintdb_filesort_priv *)me->priv;
    return priv ? priv->rows : -1;
}

i64 filesort_add(struct flintdb_filesort *me, struct flintdb_row *r, char **e) {
	struct flintdb_filesort_priv *priv = (struct flintdb_filesort_priv *)me->priv;
    if (!priv || !r) {
        THROW(e, ERR_INVALID_ARGS);
    }
    if (ensure_capacity(me, priv->rows + 1, e) != 0)
        THROW_S(e);

    // Append payload in insertion order; offsets[] records the rowid. Sort
    // later permutes offsets[] only — this write is never relocated.
    int bytes = priv->row_bytes;
    struct buffer *raw = buffer_alloc(bytes); // TODO: reuse
    if (!raw)
        THROW(e, ERR_OUT_OF_MEMORY);
    if (priv->formatter.encode(&priv->formatter, r, raw, e) != 0) {
        if (raw)
            raw->free(raw);
        THROW_S(e);
    }

    i64 off = priv->storage.write(&priv->storage, raw, e);
    raw->free(raw);
    if (e && *e)
        THROW_S(e);
    priv->offsets[priv->rows++] = off;
    return 0;

EXCEPTION:
    return -1;
}

// Decode the payload at storage offset `off` into a new row (rowid = off).
// Used by filesort_read (sorted result iteration) and the one-pass decorate
// step in filesort_sort. merge_perm must not call this — that would bring
// I/O back into the O(n log n) compare loop.
static struct flintdb_row *filesort_decode_offset(struct flintdb_filesort_priv *priv, i64 off, char **e) {
    struct buffer *buf = NULL;
    struct flintdb_row *r = NULL;
    if (!priv)
        THROW(e, ERR_INVALID_ARGS);

    buf = priv->storage.read(&priv->storage, off, e);
    if (e && *e)
        THROW_S(e);

    r = flintdb_row_new(&priv->meta, e);
    if ((e && *e) || !r) {
        if (buf)
            buf->free(buf);
        if (e && *e)
            THROW_S(e);
        THROW(e, ERR_OUT_OF_MEMORY);
    }
    if (priv->formatter.decode(&priv->formatter, buf, r, e) != 0) {
        if (buf)
            buf->free(buf);
        r->free(r);
        THROW_S(e);
    }
    if (buf)
        buf->free(buf);
    r->rowid = off; // same as offsets[i] after decorate; payload pointer, not a swap handle
    return r;

EXCEPTION:
    return NULL;
}

// i-th row in current offsets[] order (insertion order before sort, sorted
// order after). Caller owns the returned row and must free it.
struct flintdb_row * filesort_read(const struct flintdb_filesort *me, i64 i, char **e) {
    struct flintdb_filesort_priv *priv = (struct flintdb_filesort_priv *)me->priv;
    if (!priv || i < 0 || i >= priv->rows) {
        THROW(e, "index out of bounds");
    }
    return filesort_decode_offset(priv, priv->offsets[i], e);

EXCEPTION:
    return NULL;
}

static void filesort_free_decoded(struct flintdb_row **decoded, i64 n) {
    if (!decoded)
        return;
    for (i64 i = 0; i < n; i++) {
        if (decoded[i]) {
            decoded[i]->free(decoded[i]);
            decoded[i] = NULL;
        }
    }
}

// Merge perm[left..mid] and perm[mid+1..right] into aux, then copy back to perm.
//
// perm[k] is an original index into rows[] / offsets[]. We only move those
// integers; rows[p] stays at original slot p for the whole sort.
//
// Stability: `<= 0` takes the left run on ties, so equal keys keep the order
// they already had in perm (insertion order on the first pass).
static void merge_perm(i64 *perm,
                       struct flintdb_row **rows,
                       i64 left, i64 mid, i64 right,
                       i64 *aux,
                       int (*cmpr)(const void *obj, const struct flintdb_row *a, const struct flintdb_row *b),
                       const void *ctx) {
    i64 i = left;
    i64 j = mid + 1;
    i64 k = left;

    while (i <= mid && j <= right) {
        // Left on compare <= 0: stable, and no row/payload swap.
        if (cmpr(ctx, rows[perm[i]], rows[perm[j]]) <= 0)
            aux[k++] = perm[i++];
        else
            aux[k++] = perm[j++];
    }
    while (i <= mid)
        aux[k++] = perm[i++];
    while (j <= right)
        aux[k++] = perm[j++];
    for (i64 p = left; p <= right; p++)
        perm[p] = aux[p];
}

i64 filesort_sort(struct flintdb_filesort *me, int (*cmpr)(const void *obj, const struct flintdb_row *a, const struct flintdb_row *b), const void *ctx, char **e) {
	struct flintdb_filesort_priv *priv = (struct flintdb_filesort_priv *)me->priv;

    // decoded[i] : row for original slot i (decorate cache; not swapped)
    // perm[i]    : original index currently at sorted position i
    // aux        : merge scratch, then the permuted offsets[] copy-back
    struct flintdb_row **decoded = NULL;
    i64 *perm = NULL;
    i64 *aux = NULL;
    i64 loaded = 0;
    if (!priv || !cmpr) {
        THROW(e, ERR_INVALID_ARGS);
    }
    const i64 n = priv->rows;
    if (n <= 1)
        return n;

    decoded = (struct flintdb_row **)CALLOC((size_t)n, sizeof(*decoded));
    if (!decoded)
        THROW(e, ERR_OUT_OF_MEMORY);
    perm = (i64 *)MALLOC(sizeof(i64) * (size_t)n);
    if (!perm)
        THROW(e, ERR_OUT_OF_MEMORY);
    aux = (i64 *)MALLOC(sizeof(i64) * (size_t)n);
    if (!aux)
        THROW(e, ERR_OUT_OF_MEMORY);

    // One sequential I/O pass. After this, compares never touch storage.
    for (i64 i = 0; i < n; i++) {
        decoded[i] = filesort_decode_offset(priv, priv->offsets[i], e);
        if ((e && *e) || !decoded[i]) {
            if (e && *e)
                THROW_S(e);
            THROW(e, ERR_OUT_OF_MEMORY);
        }
        loaded = i + 1;
        perm[i] = i;
    }

    // Bottom-up merge: width 1,2,4,... runs. Only perm[] is reordered.
    for (i64 width = 1; width < n; width <<= 1) {
        for (i64 left = 0; left < n - width; left += (width << 1)) {
            i64 mid = left + width - 1;
            i64 right = left + (width << 1) - 1;
            if (right >= n)
                right = n - 1;

            // Adjacent runs already ordered ⇒ skip merge (still stable).
            if (cmpr(ctx, decoded[perm[mid]], decoded[perm[mid + 1]]) <= 0)
                continue;

            merge_perm(perm, decoded, left, mid, right, aux, cmpr, ctx);
        }
    }

    // Rebuild offsets[] in sorted order. Storage payloads stay put.
    for (i64 i = 0; i < n; i++)
        aux[i] = priv->offsets[perm[i]];
    memcpy(priv->offsets, aux, sizeof(i64) * (size_t)n);

    filesort_free_decoded(decoded, loaded);
    FREE(decoded);
    FREE(perm);
    FREE(aux);
    return n;

EXCEPTION:
    filesort_free_decoded(decoded, loaded);
    if (decoded)
        FREE(decoded);
    if (perm)
        FREE(perm);
    if (aux)
        FREE(aux);
    return -1;
}

i16 compact_safe(int bytes) {
    if (bytes >= 4080) return 4080; // storage block header (16) + data (4080) = 4096
    return -1;
}

struct flintdb_filesort *flintdb_filesort_new(const char *file, const struct flintdb_meta *m, char **e) {
	struct flintdb_filesort *sorter = (struct flintdb_filesort *)CALLOC(1, sizeof(struct flintdb_filesort));
	struct flintdb_filesort_priv *priv = NULL;
	if (!sorter) THROW(e, ERR_OUT_OF_MEMORY);
	sorter->priv = priv = (struct flintdb_filesort_priv *)CALLOC(1, sizeof(struct flintdb_filesort_priv));
	if (!priv) THROW(e, ERR_OUT_OF_MEMORY);

	sorter->close = filesort_close;
	sorter->rows = filesort_rows;
	sorter->add = filesort_add;
	sorter->read = filesort_read;
	sorter->sort = filesort_sort;

	priv->meta = *m; // copy meta

    // Setup formatter (binary format to match table/storage layout)
    if (formatter_init(FORMAT_BIN, &priv->meta, &priv->formatter, e) != 0)
        THROW_S(e);

    // Setup storage with block size based on row bytes
    struct storage_opts opts = {0};
    opts.block_bytes = row_bytes(&priv->meta);
    // opts.block_bytes = MIN(64 * 1024, row_bytes(&priv->meta)); // limit to 64KB block size
    opts.compact = compact_safe(opts.block_bytes);
    opts.mode = FLINTDB_RDWR;
    // leave opts.increment = 0 to let storage use its default
    // LOG("opts.block_bytes=%d, opts.compact=%d", opts.block_bytes, opts.compact);
    strncpy(opts.file, file, sizeof(opts.file) - 1);
    if (storage_open(&priv->storage, opts, e) != 0)
        THROW_S(e);

    priv->row_bytes = opts.block_bytes;
    priv->rows = 0;
    priv->cap = 0;
    priv->offsets = NULL;
    return sorter;

EXCEPTION:
    if (priv) {
        if (priv->storage.fd > 0)
            priv->storage.close(&priv->storage);
        if (priv->formatter.close)
            priv->formatter.close(&priv->formatter);
    }
	if (sorter) {
		sorter->close(sorter);
	}
    return NULL;
}