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
 *
 * On-disk file (FlintDB storage, same engine as table .bin)
 * ---------------------------------------------------------
 * One file: the path passed to flintdb_filesort_new(). Callers:
 *   sql_exec  <tmpdir>/flintdb_sort_<epoch>.tmp
 *   tests     temp/test-sortable.sort
 * No sidecar index. offsets[] is RAM-only; a crash leaves an unused temp
 * payload file. (Java FileSorter also keeps offsets in memory; C does not
 * write a second file.)
 *
 * Integers are little-endian (buffer memcpy of host i16/i32/i64).
 *
 *   [0, 16384)     file header (HEADER_BYTES / FLINTDB_FILE_HEADER_BYTES)
 *   [16384, eof)   data blocks
 *
 * File header field layout is in storage.c (Storage file format). filesort
 * does not interpret it; storage_open does. Blocks begin at byte 16384.
 *
 * One physical block = BLOCK_HEADER_BYTES (16) + payload_cap:
 *   1B  status     '+' occupied / '-' empty
 *   1B  mark       'D' first block of a record / 'N' overflow continuation
 *   2B  data_len   payload bytes in THIS block
 *   4B  total_len  remaining record bytes from this block
 *   8B  next       next block INDEX, or -1
 *   payload_cap bytes
 *
 * offsets[i] / rowid is a block INDEX (0, 1, 2, ...), not a file byte
 * offset. Byte position = 16384 + index * physical_block.
 *
 * payload_cap (see flintdb_filesort_new / compact_safe):
 *   opts.block_bytes = row_bytes(meta)   // max FORMAT_BIN size for the schema
 *   if row_bytes >= 4080:
 *     compact = 4080, physical_block = 4096 (16 + 4080)
 *     a row longer than 4080 spans blocks via next ('N')
 *   else:
 *     compact = -1, physical_block = 16 + row_bytes
 *     encoded row fits in one block (row_bytes is the schema max)
 *
 * FORMAT_BIN row payload (row.c bin_encode; same as table rows):
 *   i16 ncol
 *   for each column:
 *     i16 type     VARIANT_NULL (0) if null — then no payload
 *     varlen (STRING, DECIMAL, BYTES, BLOB): i16 n + n bytes
 *       STRING is not padded to column width
 *     fixed: INT8/UINT8=1  INT16/UINT16=2  INT32/UINT32/FLOAT=4
 *            INT64/DOUBLE/TIME=8  DATE=3  UUID/IPV6=16
 *
 * filesort_add appends one record (write → new block index).
 * filesort_sort never write_at / delete / relocates those blocks.
 */


struct flintdb_filesort_priv {
	struct storage storage;      // row payloads (insertion order, immutable during sort)
	struct formatter formatter;  // row encoder/decoder (binary)
	struct flintdb_meta meta;    // copy of table meta; valid until filesort_close

    i32 row_bytes;              // cached row byte size
	i64 *offsets;                // RAM-only permutation of block indexes; not stored in the file
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

// Map schema max row size to storage compact mode.
// >= 4080: 4KB physical blocks (16-byte header + 4080 payload), overflow via 'N'.
// <  4080: one block of 16 + bytes, compact disabled (-1).
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

    // FORMAT_BIN: same on-disk row encoding as table .bin (see file comment).
    if (formatter_init(FORMAT_BIN, &priv->meta, &priv->formatter, e) != 0)
        THROW_S(e);

    // Single mmap storage file at `file`. block_bytes = schema max BIN size;
    // compact_safe may clamp the physical block to 4096 (overflow chain).
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