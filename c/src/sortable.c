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


struct flintdb_filesort_priv {
	struct storage storage;      // backing storage for row payloads
	struct formatter formatter;  // row encoder/decoder (binary)
	struct flintdb_meta meta;            // a persistent copy of table meta

    i32 row_bytes;              // cached row byte size
	i64 *offsets;                // dynamic array of row offsets (rowids)
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
        THROW(e, "Out of memory");
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
        THROW(e, "filesorter_add: bad args");
    }
    if (ensure_capacity(me, priv->rows + 1, e) != 0)
        THROW_S(e);

    // Encode row into a binary buffer and write to storage
    int bytes = priv->row_bytes;
    struct buffer *raw = buffer_alloc(bytes); // TODO: reuse
    if (!raw)
        THROW(e, "Out of memory");
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

struct flintdb_row * filesort_read(const struct flintdb_filesort *me, i64 i, char **e) {
    struct flintdb_filesort_priv *priv = (struct flintdb_filesort_priv *)me->priv;
    if (!priv || i < 0 || i >= priv->rows) {
        THROW(e, "index out of bounds");
    }
    i64 off = priv->offsets[i];
    struct buffer *buf = priv->storage.read(&priv->storage, off, e);
    if (e && *e)
        return NULL;

    struct flintdb_row *r = flintdb_row_new(&priv->meta, e);
    if (e && *e) {
        if (buf)
            buf->free(buf);
        return NULL;
    }
    if (priv->formatter.decode(&priv->formatter, buf, r, e) != 0) {
        if (buf)
            buf->free(buf);
        return NULL;
    }
    if (buf)
        buf->free(buf);
    r->rowid = off; // for swap/put
    return r;

EXCEPTION:
    return NULL;
}

// Merge function for bottom-up mergesort on offsets array using row comparator
static void merge_runs(struct flintdb_filesort *me,
                       i64 left, i64 mid, i64 right,
                       i64 *aux,
                       int (*cmpr)(const void *obj, const struct flintdb_row *a, const struct flintdb_row *b), 
                       const void *ctx,
                       char **e) {
	struct flintdb_filesort_priv *priv = (struct flintdb_filesort_priv *)me->priv;

    i64 i = left;
    i64 j = mid + 1;
    i64 k = left;

    // Prime caches
    struct flintdb_row *ri = NULL;
    struct flintdb_row *rj = NULL;
    ri = (i <= mid) ? filesort_read(me, i, e) : NULL;
    if (e && *e)
        goto EXCEPTION;
    rj = (j <= right) ? filesort_read(me, j, e) : NULL;
    if (e && *e)
        goto EXCEPTION;

    while (i <= mid && j <= right) {
        int c = cmpr(ctx, ri, rj);
        if (c <= 0) {
            aux[k++] = priv->offsets[i];
            if (ri) {
                ri->free(ri);
            }
            i++;
            ri = (i <= mid) ? filesort_read(me, i, e) : NULL;
            if (e && *e)
                goto EXCEPTION;
        } else {
            aux[k++] = priv->offsets[j];
            if (rj) {
                rj->free(rj);
            }
            j++;
            rj = (j <= right) ? filesort_read(me, j, e) : NULL;
            if (e && *e)
                goto EXCEPTION;
        }
    }

    while (i <= mid) {
        aux[k++] = priv->offsets[i++];
    }
    while (j <= right) {
        aux[k++] = priv->offsets[j++];
    }

    // Copy back to offsets
    for (i64 p = left; p <= right; p++)
        priv->offsets[p] = aux[p];

EXCEPTION:
    if (ri)
        ri->free(ri);
    if (rj)
        rj->free(rj);
}

i64 filesort_sort(struct flintdb_filesort *me, int (*cmpr)(const void *obj, const struct flintdb_row *a, const struct flintdb_row *b), const void *ctx, char **e) {
	struct flintdb_filesort_priv *priv = (struct flintdb_filesort_priv *)me->priv;

    i64 *aux = NULL; // declare early so EXCEPTION path can safely reference it
    if (!priv || !cmpr) {
        THROW(e, "bad arguments");
    }
    const i64 n = priv->rows;
    if (n <= 1)
        return n;

    aux = (i64 *)MALLOC(sizeof(i64) * (size_t)n);
    if (!aux)
        THROW(e, "Out of memory");

    // Bottom-up iterative merge sort for better locality / bounded stack
    for (i64 width = 1; width < n; width <<= 1) {
        for (i64 left = 0; left < n - width; left += (width << 1)) {
            i64 mid = left + width - 1;
            i64 right = left + (width << 1) - 1;
            if (right >= n)
                right = n - 1;

            // Optional skip if already ordered
            struct flintdb_row *lm = filesort_read(me, mid, e);
            if (e && *e)
                goto DONE;
            struct flintdb_row *rn = filesort_read(me, mid + 1, e);
            if (e && *e) {
                if (lm)
                    lm->free(lm);
                goto DONE;
            }
            int ordered = (cmpr(ctx, lm, rn) <= 0);
            if (lm)
                lm->free(lm);
            if (rn)
                rn->free(rn);
            if (ordered)
                continue;

            merge_runs(me, left, mid, right, aux, cmpr, ctx, e);
            if (e && *e)
                goto DONE;
        }
    }

DONE:
    if (aux)
        FREE(aux);
    if (e && *e)
        return -1;
    return n;

EXCEPTION:
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
	if (!sorter) THROW(e, "Out of memory");
	sorter->priv = priv = (struct flintdb_filesort_priv *)CALLOC(1, sizeof(struct flintdb_filesort_priv));
	if (!priv) THROW(e, "Out of memory");

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