#include "hashmap.h"
#include "hyperloglog.h"
#include "internal.h"
#include "flintdb.h"
#include "roaringbitmap.h"
#include "runtime.h"
#include "sql.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>


#define HASHMAP_SORTING_ENABLED 0 // whether to use sorted treemap for group keys, currently unused

// Thread-local raw scratch buffer (no struct buffer indirection)
// Increased size for better performance with long group keys
static __thread char scratch_buf[8192];
static __thread int scratch_pos = 0;

#define SCRATCH_CAP ((int)sizeof(scratch_buf))

static inline void scratch_reset() { scratch_pos = 0; }
static inline int scratch_len() { return scratch_pos; }
static inline const char *scratch_data() { return scratch_buf; }
static inline void scratch_put_char(char c) {
    if (scratch_pos < SCRATCH_CAP)
        scratch_buf[scratch_pos++] = c; /* drop if full */
}
static inline void scratch_put_bytes(const char *p, int n) {
    if (!p || n <= 0)
        return;
    int space = SCRATCH_CAP - scratch_pos;
    if (space <= 0)
        return;
    if (n > space)
        n = space;
    memcpy(scratch_buf + scratch_pos, p, (size_t)n);
    scratch_pos += n;
}
static inline void scratch_put_sep() { scratch_put_char('\x1F'); }

// Append decimal stable textual form directly to scratch buffer
static void scratch_append_decimal(const struct flintdb_decimal  *d) {
    if (!d || d->length == 0)
        return;
    char tmp[64];
    int tp = 0;
    if (d->sign) {
        if (tp < (int)sizeof(tmp))
            tmp[tp++] = '-';
    }
    for (int i = 0; i < (int)d->length; i++) {
        unsigned char byte = (unsigned char)d->data[i];
        int hi = (byte >> 4) & 0xF;
        int lo = byte & 0xF;
        if (tp < (int)sizeof(tmp))
            tmp[tp++] = (char)('0' + hi);
        if (tp < (int)sizeof(tmp))
            tmp[tp++] = (char)('0' + lo);
    }
    if (d->scale > 0 && tp > 0) {
        int digits = tp - (d->sign ? 1 : 0);
        if (d->scale >= digits) {
            char out[96];
            int op = 0;
            if (d->sign && op < (int)sizeof(out))
                out[op++] = '-';
            if (op < (int)sizeof(out))
                out[op++] = '0';
            if (op < (int)sizeof(out))
                out[op++] = '.';
            int z = (int)d->scale - digits;
            for (int i = 0; i < z && op < (int)sizeof(out); i++)
                out[op++] = '0';
            int start = (d->sign ? 1 : 0);
            for (int i = start; i < tp && op < (int)sizeof(out); i++)
                out[op++] = tmp[i];
            scratch_put_bytes(out, op);
            return;
        } else {
            int point = tp - (int)d->scale;
            char out[96];
            int op = 0;
            for (int i = 0; i < tp; i++) {
                if (i == point && op < (int)sizeof(out))
                    out[op++] = '.';
                if (op < (int)sizeof(out))
                    out[op++] = tmp[i];
            }
            scratch_put_bytes(out, op);
            return;
        }
    }
    scratch_put_bytes(tmp, tp);
}

static const char *safe_str(const char *s) { return s ? s : ""; }

static void scratch_append_col_stable_str(const struct flintdb_row *r, int idx) {
    if (!r || idx < 0 || idx >= r->length)
        return;
    enum flintdb_variant_type  t = r->meta && (idx < r->meta->columns.length) ? r->meta->columns.a[idx].type : r->array[idx].type;
    char *e = NULL;
    switch (t) {
    case VARIANT_STRING: {
        const char *s = r->string_get(r, idx, &e);
        scratch_put_bytes(safe_str(s), s ? (int)strlen(s) : 0);
        break;
    }
    case VARIANT_DOUBLE:
    case VARIANT_FLOAT: {
        double fv = r->f64_get(r, idx, &e);
        char tmp[64];
        int n = snprintf(tmp, sizeof(tmp), "%.*g", 17, fv);
        if (n > 0)
            scratch_put_bytes(tmp, n);
        break;
    }
    case VARIANT_INT8:
    case VARIANT_UINT8:
    case VARIANT_INT16:
    case VARIANT_UINT16:
    case VARIANT_INT32:
    case VARIANT_UINT32:
    case VARIANT_INT64: {
        long long iv = (long long)r->i64_get(r, idx, &e);
        char tmp[32];
        int n = snprintf(tmp, sizeof(tmp), "%lld", iv);
        if (n > 0)
            scratch_put_bytes(tmp, n);
        break;
    }
    case VARIANT_DECIMAL: {
        struct flintdb_decimal  d = r->decimal_get(r, idx, &e);
        scratch_append_decimal(&d);
        break;
    }
    case VARIANT_BYTES:
    case VARIANT_UUID:
    case VARIANT_IPV6: {
        u32 bl = 0;
        const char *bp = r->bytes_get ? r->bytes_get(r, idx, &bl, &e) : NULL;
        static const char HEX[] = "0123456789abcdef";
        for (u32 k = 0; k < bl; k++) {
            unsigned char v = (unsigned char)bp[k];
            char hx[2] = {HEX[v >> 4], HEX[v & 0xF]};
            scratch_put_bytes(hx, 2);
        }
        break;
    }
    case VARIANT_DATE:
    case VARIANT_TIME: {
        long long tv = (long long)((t == VARIANT_DATE) ? r->date_get(r, idx, &e) : r->time_get(r, idx, &e));
        char tmp[32];
        int n = snprintf(tmp, sizeof(tmp), "%lld", tv);
        if (n > 0)
            scratch_put_bytes(tmp, n);
        break;
    }
    case VARIANT_NULL:
    case VARIANT_ZERO:
    default:
        break;
    }
}

// Complete C implementation of Java Aggregate with full GROUP BY support

enum aggr_func {
    FUNC_COUNT = 0,
    FUNC_DISTINCT_RB = 1,  // exact distinct using RoaringBitmap
    FUNC_DISTINCT_HLL = 2, // approximate distinct using HyperLogLog
    FUNC_SUM = 3,
    FUNC_AVG = 4,
    FUNC_MIN = 5,
    FUNC_MAX = 6,
    FUNC_FIRST = 7,
    FUNC_LAST = 8,
    FUNC_ROWID = 9,
    FUNC_HASH = 10,
    FUNC_CUSTOM = 99
};

// Forward declarations
struct flintdb_aggregate;
struct flintdb_aggregate_groupkey;
struct flintdb_aggregate_groupby;
struct flintdb_aggregate_func;

// Main aggregate private structure (defined here for use in groupkey_from_row)
struct flintdb_aggregate_priv {
    char id[64];
    struct flintdb_aggregate_groupby **groupby;
    u16 groupby_count;
    struct flintdb_aggregate_func **funcs;
    u16 func_count;

    // Set of unique group keys using hashmap (key: groupkey id string -> dummy value 1)
    struct hashmap *keys;
    
    // Cache for group column names (allocated once)
    const char **group_cols_cache;
    
    // Cache for group column indices (allocated once, computed per row's meta)
    int *group_col_indices;
    const struct flintdb_meta *cached_meta;
    
    // Cache for indices buffer used in groupkey_from_row (allocated once)
    int *indices_cache;
    int indices_cache_size;
    
    // Cache for result meta (built once during first compute)
    struct flintdb_meta *result_meta;
};

// === GROUP KEY IMPLEMENTATION ===

struct flintdb_aggregate_groupkey_priv {
    char *id;         // joined key string (Unit Separator delimited)
    u32 hash;         // precomputed hash for fast comparison
    struct flintdb_meta *m;   // meta of key row (STRING columns)
    struct flintdb_row *krow; // key row values as strings
};

static void gk_free(struct flintdb_aggregate_groupkey *g) {
    if (!g)
        return;
    struct flintdb_aggregate_groupkey_priv *p = (struct flintdb_aggregate_groupkey_priv *)g->priv;
    if (p) {
        if (p->krow) {
            p->krow->free(p->krow);
            p->krow = NULL;
        }
        if (p->m) {
            flintdb_meta_close(p->m);
            FREE(p->m);
            p->m = NULL;
        }
        if (p->id) {
            FREE(p->id);
            p->id = NULL;
        }
        FREE(p);
    }
    FREE(g);
}

static struct flintdb_row *gk_key(const struct flintdb_aggregate_groupkey *g, char **e) {
    (void)e;
    if (!g)
        return NULL;
    struct flintdb_aggregate_groupkey_priv *p = (struct flintdb_aggregate_groupkey_priv *)g->priv;
    return p ? p->krow : NULL;
}

static i8 gk_equals(const struct flintdb_aggregate_groupkey *g, const struct flintdb_aggregate_groupkey *o, char **e) {
    (void)e;
    if (g == o)
        return 1;
    if (!g || !o)
        return 0;
    struct flintdb_aggregate_groupkey_priv *a = (struct flintdb_aggregate_groupkey_priv *)g->priv;
    struct flintdb_aggregate_groupkey_priv *b = (struct flintdb_aggregate_groupkey_priv *)o->priv;
    if (!a || !b)
        return 0;
    // Fast path: compare hash first
    if (a->hash != b->hash)
        return 0;
    // Hash collision check: compare full string
    if (!a->id || !b->id)
        return 0;
    return (strcmp(a->id, b->id) == 0) ? 1 : 0;
}

// Integer hash function for groupkey (already hashed)
static u32 groupkey_hash(keytype k) {
    // k is pointer to hash value - just return it
    return (u32)(uintptr_t)k;
}

static i32 groupkey_compare(keytype k1, keytype k2) {
    u32 h1 = (u32)(uintptr_t)k1;
    u32 h2 = (u32)(uintptr_t)k2;
    if (h1 < h2) return -1;
    if (h1 > h2) return 1;
    return 0;
}

static inline struct hashmap *groupkey_map_new() {
// #if HASHMAP_SORTING_ENABLED
//     return treemap_new(groupkey_compare);
// #else
    // Use integer hash for much faster lookups
    return hashmap_new(128, groupkey_hash, groupkey_compare);
// #endif
}

struct flintdb_aggregate_groupkey *flintdb_groupkey_from_row(struct flintdb_aggregate *agg, const struct flintdb_row *source, const char **columns, u16 n, char **e) {
    // Allow NULL source when n=0 (no groupby columns - global aggregation)
    struct flintdb_aggregate_priv *ap = (struct flintdb_aggregate_priv *)agg->priv;
    assert(ap);
    
    if (!source && n > 0)
        return NULL;

    scratch_reset();

    // Use cached indices array from aggregate_priv (allocated once)
    int *indices = NULL;
    if (n > 0) {
        if (!ap->indices_cache || ap->indices_cache_size < n) {
            if (ap->indices_cache)
                FREE(ap->indices_cache);
            ap->indices_cache = (int *)CALLOC((size_t)n, sizeof(int));
            ap->indices_cache_size = n;
            if (!ap->indices_cache) {
                return NULL;
            }
        }
        indices = ap->indices_cache;
    }
    
    if (n > 0 && source) {
        for (int i = 0; i < n; i++) {
            int idx = -1;
            if (columns && columns[i])
                idx = flintdb_column_at(source->meta, columns[i]);
            indices[i] = idx;
        }
        for (int i = 0; i < n; i++) {
            if (i > 0)
                scratch_put_sep();
            int idx = indices[i];
            if (idx >= 0)
                scratch_append_col_stable_str(source, idx);
        }
    }
    scratch_put_char('\0');
    u32 ln = scratch_len() > 0 ? (u32)(scratch_len() - 1) : 0;
    char *id = (char *)MALLOC(ln + 1);
    if (!id) {
        return NULL;
    }
    if (ln)
        memcpy(id, scratch_data(), ln);
    id[ln] = '\0';
    
    // Compute hash from the key string
    u32 hash = hashmap_string_hash((keytype)(uintptr_t)id);
    /* thread-local scratch buffer reused */

    struct flintdb_meta m0 = flintdb_meta_new("", e);
    for (int i = 0; i < n; i++) {
        const char *cname = (columns && columns[i]) ? columns[i] : "";
        flintdb_meta_columns_add(&m0, cname, VARIANT_STRING, 32, 0, SPEC_NULLABLE, NULL, NULL, e);
    }
    struct flintdb_meta *km = (struct flintdb_meta *)MALLOC(sizeof(struct flintdb_meta));
    if (!km) {
        FREE(id);
        return NULL;
    }
    memcpy(km, &m0, sizeof(struct flintdb_meta));

    struct flintdb_row *kr = flintdb_row_new(km, e);
    if (!kr) {
        FREE(id);
        FREE(km);
        return NULL;
    }

    for (int i = 0; i < n; i++) {
        scratch_reset();
        if (indices && indices[i] >= 0) {
            scratch_append_col_stable_str(source, indices[i]);
        }
        scratch_put_char('\0');
        const char *sv = scratch_data();
        kr->string_set(kr, i, sv, e);
        /* reuse thread-local scratch */
    }

    struct flintdb_aggregate_groupkey *g = (struct flintdb_aggregate_groupkey *)CALLOC(1, sizeof(struct flintdb_aggregate_groupkey));
    if (!g) {
        kr->free(kr);
        flintdb_meta_close(km);
        FREE(km);
        FREE(id);
        return NULL;
    }
    struct flintdb_aggregate_groupkey_priv *p = (struct flintdb_aggregate_groupkey_priv *)CALLOC(1, sizeof(struct flintdb_aggregate_groupkey_priv));
    if (!p) {
        FREE(g);
        kr->free(kr);
        flintdb_meta_close(km);
        FREE(km);
        FREE(id);
        return NULL;
    }

    p->id = id;
    p->hash = hash;
    p->m = km;
    p->krow = kr;
    g->priv = p;
    g->free = gk_free;
    g->key = gk_key;
    g->equals = gk_equals;
    return g;
}


// === GROUPBY IMPLEMENTATION ===

struct flintdb_aggregate_groupby_priv {
    char alias[64];
    char column[64];
    enum flintdb_variant_type  type;
};

static void groupby_free(struct flintdb_aggregate_groupby *gb) {
    if (!gb)
        return;
    if (gb->priv)
        FREE(gb->priv);
    FREE(gb);
}

static const char *groupby_alias(const struct flintdb_aggregate_groupby *gb) {
    if (!gb || !gb->priv)
        return "";
    struct flintdb_aggregate_groupby_priv *p = (struct flintdb_aggregate_groupby_priv *)gb->priv;
    return p->alias;
}

static const char *groupby_column(const struct flintdb_aggregate_groupby *gb) {
    if (!gb || !gb->priv)
        return "";
    struct flintdb_aggregate_groupby_priv *p = (struct flintdb_aggregate_groupby_priv *)gb->priv;
    return p->column;
}

static enum flintdb_variant_type  groupby_type(const struct flintdb_aggregate_groupby *gb) {
    if (!gb || !gb->priv)
        return VARIANT_NULL;
    struct flintdb_aggregate_groupby_priv *p = (struct flintdb_aggregate_groupby_priv *)gb->priv;
    return p->type;
}

static struct flintdb_variant *groupby_get(const struct flintdb_aggregate_groupby *gb, const struct flintdb_row *r, char **e) {
    if (!gb || !gb->priv || !r)
        return NULL;
    struct flintdb_aggregate_groupby_priv *p = (struct flintdb_aggregate_groupby_priv *)gb->priv;
    int idx = flintdb_column_at((struct flintdb_meta *)r->meta, p->column);
    if (idx < 0)
        return NULL;
    return r->get((struct flintdb_row *)r, idx, e);
}

struct flintdb_aggregate_groupby *groupby_new(const char *alias, const char *column, enum flintdb_variant_type  type, char **e) {
    (void)e;
    struct flintdb_aggregate_groupby *gb = (struct flintdb_aggregate_groupby *)CALLOC(1, sizeof(struct flintdb_aggregate_groupby));
    if (!gb)
        return NULL;

    struct flintdb_aggregate_groupby_priv *p = (struct flintdb_aggregate_groupby_priv *)CALLOC(1, sizeof(struct flintdb_aggregate_groupby_priv));
    if (!p) {
        FREE(gb);
        return NULL;
    }

    s_copy(p->alias, sizeof(p->alias), alias ? alias : "");
    s_copy(p->column, sizeof(p->column), column ? column : "");
    p->type = type;

    gb->priv = p;
    gb->free = groupby_free;
    gb->alias = groupby_alias;
    gb->column = groupby_column;
    gb->type = groupby_type;
    gb->get = groupby_get;

    return gb;
}

// === AGGREGATE FUNCTION IMPLEMENTATION ===

// Per-group function data stored in hashmap
struct group_func_data {
    enum aggr_func kind; // Store function kind to know which union field is active
    union {
        i64 count;                // for COUNT
        struct roaringbitmap *rb; // for DISTINCT exact
        struct hyperloglog *hll;  // for DISTINCT approximate
        i64 rowid;                // for ROWID
        i64 hash;                 // for HASH
    } u;

    struct flintdb_decimal  sum; // SUM/AVG as exact decimal
    i64 n;              // AVG count or generic counter
    struct flintdb_variant acc; // MIN/MAX/FIRST/LAST current value
    i8 has_acc;         // whether acc is set
    int sum_scale;      // target scale used for SUM/AVG

    struct flintdb_variant result;
};

static void group_func_data_free(struct group_func_data *gfd) {
    if (!gfd)
        return;

    // Only free rb/hll based on function kind
    if (gfd->kind == FUNC_DISTINCT_RB && gfd->u.rb) {
        rbitmap_free(gfd->u.rb);
        gfd->u.rb = NULL;
    }
    if (gfd->kind == FUNC_DISTINCT_HLL && gfd->u.hll) {
        hll_free(gfd->u.hll);
        gfd->u.hll = NULL;
    }

    flintdb_variant_free(&gfd->acc);
    flintdb_variant_free(&gfd->result);
    FREE(gfd);
}

// Hashmap deallocator for group_func_data
// Note: This can be called from hashmapiter when we don't have access to p->kind!
// Solution: Store kind in group_func_data or only free rb/hll when non-NULL and looks like valid pointer
static void group_data_dealloc(keytype k, valtype v) {
    // Key is integer hash - no need to free
    (void)k;
    // Value is group_func_data
    struct group_func_data *gfd = (struct group_func_data *)(uintptr_t)v;
    if (gfd)
        group_func_data_free(gfd);
}

struct flintdb_aggregate_func_priv {
    char name[64];
    char alias[64];
    enum flintdb_variant_type  out_type;
    struct flintdb_aggregate_condition cond;
    enum aggr_func kind;

    // Per-group storage using hashmap (key: GROUPKEY id string, value: struct group_func_data*)
    struct hashmap *group_data;

    int precision;
    
    // Cache for column index lookup (per meta)
    int cached_col_idx;
    const struct flintdb_meta *cached_col_meta;
};

static struct group_func_data *get_or_create_group_data(struct flintdb_aggregate_func *f, u32 group_key_hash, char **e) {
    if (!f || !f->priv)
        return NULL;
    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)f->priv;

    if (!p->group_data) {
        p->group_data = groupkey_map_new();
        if (!p->group_data) {
            if (e)
                *e = "Out of memory creating group hashmap";
            return NULL;
        }
    }

    // Use integer hash as key directly
    valtype v = p->group_data->get(p->group_data, (keytype)(uintptr_t)group_key_hash);
    if (v != HASHMAP_INVALID_VAL) {
        return (struct group_func_data *)(uintptr_t)v;
    }

    // Create new group data
    struct group_func_data *gfd = (struct group_func_data *)CALLOC(1, sizeof(struct group_func_data));
    if (!gfd) {
        if (e)
            *e = "Out of memory creating group data";
        return NULL;
    }

    gfd->kind = p->kind; // Store function kind
    flintdb_variant_init(&gfd->acc);
    flintdb_variant_init(&gfd->result);

    // Initialize based on function kind
    switch (p->kind) {
    case FUNC_DISTINCT_RB:
        gfd->u.rb = rbitmap_new();
        break;
    case FUNC_DISTINCT_HLL:
        gfd->u.hll = hll_new_default();
        break;
    default:
        break;
    }

    // Store in hashmap using integer hash as key (no string copy needed!)
    p->group_data->put(p->group_data, (keytype)(uintptr_t)group_key_hash, (valtype)(uintptr_t)gfd, group_data_dealloc);

    // Verify it was stored
    // valtype v_check = p->group_data->get(p->group_data, (keytype)(uintptr_t)key_copy);

    return gfd;
}

static void aggr_func_free(struct flintdb_aggregate_func *f) {
    if (!f)
        return;
    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)f->priv;
    if (p) {
        if (p->group_data) {
            p->group_data->clear(p->group_data);
            p->group_data->free(p->group_data);
            p->group_data = NULL;
        }
        FREE(p);
    }
    FREE(f);
}

static const char *aggr_func_name(const struct flintdb_aggregate_func *f) {
    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)f->priv;
    return p ? p->name : "";
}

static const char *aggr_func_alias(const struct flintdb_aggregate_func *f) {
    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)f->priv;
    return p ? p->alias : "";
}

static enum flintdb_variant_type  aggr_func_type(const struct flintdb_aggregate_func *f) {
    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)f->priv;
    return p ? p->out_type : VARIANT_INT64;
}

static int aggr_func_precision(const struct flintdb_aggregate_func *f) {
    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)f->priv;
    return p ? p->precision : 0;
}

static const struct flintdb_aggregate_condition *aggr_func_condition(const struct flintdb_aggregate_func *f) {
    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)f->priv;
    return p ? &p->cond : NULL;
}

// Build stable key from row for distinct hashing - writes to scratch buffer
static inline void row_to_stable_key_scratch(const struct flintdb_row *r) {
    if (!r)
        return;
    scratch_reset();
    char *e = NULL;
    for (int i = 0; i < r->length; i++) {
        if (i > 0)
            scratch_put_sep();
        enum flintdb_variant_type  t = r->meta && (i < r->meta->columns.length) ? r->meta->columns.a[i].type : r->array[i].type;
        switch (t) {
        case VARIANT_STRING: {
            const char *s = r->string_get(r, i, &e);
            scratch_put_bytes(safe_str(s), s ? (int)strlen(s) : 0);
            break;
        }
        case VARIANT_DOUBLE:
        case VARIANT_FLOAT: {
            double fv = r->f64_get(r, i, &e);
            char tmp[64];
            int n = snprintf(tmp, sizeof(tmp), "%.*g", 17, fv);
            if (n > 0)
                scratch_put_bytes(tmp, n);
            break;
        }
        case VARIANT_INT8:
        case VARIANT_UINT8:
        case VARIANT_INT16:
        case VARIANT_UINT16:
        case VARIANT_INT32:
        case VARIANT_UINT32:
        case VARIANT_INT64: {
            long long iv = (long long)r->i64_get(r, i, &e);
            char tmp[32];
            int n = snprintf(tmp, sizeof(tmp), "%lld", iv);
            if (n > 0)
                scratch_put_bytes(tmp, n);
            break;
        }
        case VARIANT_DECIMAL: {
            struct flintdb_decimal  d = r->decimal_get(r, i, &e);
            scratch_append_decimal(&d);
            break;
        }
        case VARIANT_BYTES:
        case VARIANT_UUID:
        case VARIANT_IPV6: {
            u32 bl = 0;
            const char *bp = r->bytes_get ? r->bytes_get(r, i, &bl, &e) : NULL;
            static const char HEX[] = "0123456789abcdef";
            for (u32 k = 0; k < bl; k++) {
                unsigned char v = (unsigned char)bp[k];
                char hx[2] = {HEX[v >> 4], HEX[v & 0xF]};
                scratch_put_bytes(hx, 2);
            }
            break;
        }
        case VARIANT_DATE:
        case VARIANT_TIME: {
            long long tv = (long long)((t == VARIANT_DATE) ? r->date_get(r, i, &e) : r->time_get(r, i, &e));
            char tmp[32];
            int n = snprintf(tmp, sizeof(tmp), "%lld", tv);
            if (n > 0)
                scratch_put_bytes(tmp, n);
            break;
        }
        case VARIANT_NULL:
        case VARIANT_ZERO:
        default:
            break;
        }
    }
    scratch_put_char('\0');
}

static int key_hash31_from_row(const struct flintdb_row *r) {
    row_to_stable_key_scratch(r);
    int32_t h = hll_java_string_hashcode(scratch_data());
    return (int)(h & 0x7FFFFFFF);
}

static void aggr_func_row(struct flintdb_aggregate_func *f, const struct flintdb_aggregate_groupkey *gk, const struct flintdb_row *r, char **e) {
    if (!f || !r)
        return;
    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)f->priv;
    if (!p)
        return;

    // Check condition
    if (p->cond.ok && !p->cond.ok(&p->cond, r, e))
        return;

    // Get group key hash
    u32 group_hash = 0;
    if (gk && gk->priv) {
        struct flintdb_aggregate_groupkey_priv *gkp = (struct flintdb_aggregate_groupkey_priv *)gk->priv;
        group_hash = gkp->hash;
    }

    // Get or create group-specific data using hash
    struct group_func_data *gfd = get_or_create_group_data(f, group_hash, e);
    if (!gfd)
        return;

    // Resolve source column index with caching
    int col_idx = -1;
    struct flintdb_variant *col_v = NULL;
    switch (p->kind) {
    case FUNC_SUM:
    case FUNC_AVG:
    case FUNC_MIN:
    case FUNC_MAX:
    case FUNC_FIRST:
    case FUNC_LAST:
        // Cache column index per meta
        if (r->meta != p->cached_col_meta) {
            p->cached_col_idx = flintdb_column_at((struct flintdb_meta *)r->meta, p->name);
            p->cached_col_meta = r->meta;
        }
        col_idx = p->cached_col_idx;
        if (col_idx >= 0)
            col_v = r->get((struct flintdb_row *)r, col_idx, e);
        break;
    default:
        break;
    }

    switch (p->kind) {
    case FUNC_COUNT:
        gfd->u.count++;
        break;

    case FUNC_DISTINCT_RB: {
        if (!gfd->u.rb)
            gfd->u.rb = rbitmap_new();
        int h = key_hash31_from_row(r);
        if (h >= 0)
            rbitmap_add(gfd->u.rb, h);
        break;
    }

    case FUNC_DISTINCT_HLL: {
        if (!gfd->u.hll)
            gfd->u.hll = hll_new_default();
        row_to_stable_key_scratch(r);
        hll_add_cstr(gfd->u.hll, scratch_data());
        break;
    }

    case FUNC_SUM: {
        if (!col_v || col_v->type == VARIANT_NULL)
            break;
        int target_scale = 0;
        if (r && r->meta && col_idx >= 0 && col_idx < r->meta->columns.length) {
            target_scale = r->meta->columns.a[col_idx].precision;
            if (target_scale < 0)
                target_scale = 0;
            if (target_scale > 32)
                target_scale = 32;
        }
        if (gfd->sum_scale == 0 && target_scale > 0)
            gfd->sum_scale = target_scale;

        struct flintdb_decimal  dv = {0};
        if (flintdb_variant_to_decimal(col_v, &dv, e) != 0)
            break;
        if (gfd->sum.length == 0) {
            gfd->sum = dv;
        } else {
            int S = (target_scale > 0) ? target_scale : ((gfd->sum.scale > dv.scale) ? gfd->sum.scale : dv.scale);
            struct flintdb_decimal  outd = {0};
            if (flintdb_decimal_plus(&gfd->sum, &dv, S, &outd) == 0) {
                gfd->sum = outd;
            }
        }
        break;
    }

    case FUNC_AVG: {
        if (!col_v || col_v->type == VARIANT_NULL)
            break;
        int target_scale = 0;
        if (r && r->meta && col_idx >= 0 && col_idx < r->meta->columns.length) {
            target_scale = r->meta->columns.a[col_idx].precision;
            if (target_scale < 0)
                target_scale = 0;
            if (target_scale > 32)
                target_scale = 32;
        }
        if (gfd->sum_scale == 0 && target_scale > 0)
            gfd->sum_scale = target_scale;

        struct flintdb_decimal  dv = {0};
        if (flintdb_variant_to_decimal(col_v, &dv, e) != 0)
            break;
        if (gfd->sum.length == 0) {
            gfd->sum = dv;
            gfd->n++;
        } else {
            int S = (target_scale > 0) ? target_scale : ((gfd->sum.scale > dv.scale) ? gfd->sum.scale : dv.scale);
            struct flintdb_decimal  outd = {0};
            if (flintdb_decimal_plus(&gfd->sum, &dv, S, &outd) == 0) {
                gfd->sum = outd;
                gfd->n++;
            }
        }
        break;
    }

    case FUNC_MIN: {
        struct flintdb_variant *v = col_v;
        if (!v || v->type == VARIANT_NULL)
            break;
        if (!gfd->has_acc) {
            flintdb_variant_free(&gfd->acc);
            flintdb_variant_copy(&gfd->acc, v);
            gfd->has_acc = 1;
        } else {
            if (flintdb_variant_compare(v, &gfd->acc) < 0) {
                flintdb_variant_free(&gfd->acc);
                flintdb_variant_copy(&gfd->acc, v);
            }
        }
        break;
    }

    case FUNC_MAX: {
        struct flintdb_variant *v = col_v;
        if (!v || v->type == VARIANT_NULL)
            break;
        if (!gfd->has_acc) {
            flintdb_variant_free(&gfd->acc);
            flintdb_variant_copy(&gfd->acc, v);
            gfd->has_acc = 1;
        } else {
            if (flintdb_variant_compare(v, &gfd->acc) > 0) {
                flintdb_variant_free(&gfd->acc);
                flintdb_variant_copy(&gfd->acc, v);
            }
        }
        break;
    }

    case FUNC_FIRST: {
        if (!gfd->has_acc) {
            struct flintdb_variant *v = col_v;
            if (v && v->type != VARIANT_NULL) {
                flintdb_variant_free(&gfd->acc);
                flintdb_variant_copy(&gfd->acc, v);
                gfd->has_acc = 1;
            }
        }
        break;
    }

    case FUNC_LAST: {
        struct flintdb_variant *v = col_v;
        if (v && v->type != VARIANT_NULL) {
            flintdb_variant_free(&gfd->acc);
            flintdb_variant_copy(&gfd->acc, v);
            gfd->has_acc = 1;
        }
        break;
    }

    case FUNC_ROWID:
        // ROWID is computed at compute time, not accumulated
        break;

    case FUNC_HASH:
        // HASH is computed based on columns at compute time
        break;

    default:
        break;
    }
}

static void aggr_func_compute(struct flintdb_aggregate_func *f, const struct flintdb_aggregate_groupkey *gk, char **e) {
    (void)e;
    if (!f)
        return;
    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)f->priv;
    if (!p)
        return;

    u32 group_hash = 0;
    if (gk && gk->priv) {
        struct flintdb_aggregate_groupkey_priv *gkp = (struct flintdb_aggregate_groupkey_priv *)gk->priv;
        group_hash = gkp->hash;
    }
    struct group_func_data *gfd = get_or_create_group_data(f, group_hash, e);
    if (!gfd)
        return;

    flintdb_variant_free(&gfd->result);
    flintdb_variant_init(&gfd->result);

    switch (p->kind) {
    case FUNC_COUNT: {
        flintdb_variant_i64_set(&gfd->result, gfd->u.count);
        break;
    }

    case FUNC_DISTINCT_RB: {
        int card = gfd->u.rb ? rbitmap_cardinality(gfd->u.rb) : 0;
        flintdb_variant_i64_set(&gfd->result, (i64)card);
        break;
    }

    case FUNC_DISTINCT_HLL: {
        u64 est = gfd->u.hll ? hll_cardinality(gfd->u.hll) : 0;
        flintdb_variant_i64_set(&gfd->result, (i64)est);
        break;
    }

    case FUNC_SUM: {
        char sbuf[128];
        sbuf[0] = '\0';
        flintdb_decimal_to_string(&gfd->sum, sbuf, sizeof(sbuf));
        struct flintdb_decimal  d = {0};
        if (flintdb_decimal_from_string(sbuf, 5, &d) == 0) {
            flintdb_variant_decimal_set(&gfd->result, d.sign, d.scale, d);
        } else {
            flintdb_variant_decimal_set(&gfd->result, gfd->sum.sign, gfd->sum.scale, gfd->sum);
        }
        break;
    }

    case FUNC_AVG: {
        if (gfd->n <= 0) {
            flintdb_variant_null_set(&gfd->result);
        } else {
            int scale = (gfd->sum_scale > 0) ? gfd->sum_scale : 5;
            char nbuf[32];
            snprintf(nbuf, sizeof(nbuf), "%lld", (long long)gfd->n);
            struct flintdb_decimal  den = {0}, out = {0};
            if (flintdb_decimal_from_string(nbuf, 0, &den) == 0 &&
                flintdb_decimal_divide(&gfd->sum, &den, scale, &out) == 0) {
                flintdb_variant_decimal_set(&gfd->result, out.sign, out.scale, out);
            } else {
                char sbuf[96];
                sbuf[0] = '\0';
                flintdb_decimal_to_string(&gfd->sum, sbuf, sizeof(sbuf));
                double sd = strtod(sbuf, NULL);
                double av = sd / (double)gfd->n;
                char *ee = NULL;
                struct flintdb_decimal  d = flintdb_decimal_from_f64((f64)av, scale, &ee);
                flintdb_variant_decimal_set(&gfd->result, d.sign, d.scale, d);
            }
        }
        break;
    }

    case FUNC_MIN:
    case FUNC_MAX:
    case FUNC_FIRST:
    case FUNC_LAST: {
        if (gfd->has_acc) {
            flintdb_variant_copy(&gfd->result, &gfd->acc);
        } else {
            flintdb_variant_null_set(&gfd->result);
        }
        break;
    }

    case FUNC_ROWID: {
        gfd->u.rowid++;
        flintdb_variant_i64_set(&gfd->result, gfd->u.rowid);
        break;
    }

    case FUNC_HASH: {
        // Hash not implemented yet - return 0
        flintdb_variant_i64_set(&gfd->result, 0);
        break;
    }

    default:
        break;
    }
}

static const struct flintdb_variant *aggr_func_result(const struct flintdb_aggregate_func *f, const struct flintdb_aggregate_groupkey *gk, char **e) {
    (void)e;
    if (!f)
        return NULL;
    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)f->priv;
    if (!p)
        return NULL;

    u32 group_hash = 0;
    if (gk && gk->priv) {
        struct flintdb_aggregate_groupkey_priv *gkp = (struct flintdb_aggregate_groupkey_priv *)gk->priv;
        group_hash = gkp->hash;
    }

    if (!p->group_data)
        return NULL;
    valtype v = p->group_data->get(p->group_data, (keytype)(uintptr_t)group_hash);
    if (v == HASHMAP_INVALID_VAL)
        return NULL;

    struct group_func_data *gfd = (struct group_func_data *)(uintptr_t)v;
    return gfd ? &gfd->result : NULL;
}

static struct flintdb_aggregate_func *aggr_func_new_common(const char *name, const char *alias, enum flintdb_variant_type  type,
                                                   struct flintdb_aggregate_condition cond, enum aggr_func k, int precision, char **e) {
    (void)e;
    struct flintdb_aggregate_func *f = (struct flintdb_aggregate_func *)CALLOC(1, sizeof(struct flintdb_aggregate_func));
    if (!f)
        return NULL;

    struct flintdb_aggregate_func_priv *p = (struct flintdb_aggregate_func_priv *)CALLOC(1, sizeof(struct flintdb_aggregate_func_priv));
    if (!p) {
        FREE(f);
        return NULL;
    }

    s_copy(p->name, sizeof(p->name), name ? name : "");
    s_copy(p->alias, sizeof(p->alias), alias ? alias : (name ? name : ""));
    p->out_type = type;
    p->cond = cond;
    p->kind = k;
    p->precision = precision;
    p->group_data = NULL; // Created on first use
    p->cached_col_idx = -1;
    p->cached_col_meta = NULL;

    f->priv = p;
    f->free = aggr_func_free;
    f->name = aggr_func_name;
    f->alias = aggr_func_alias;
    f->type = aggr_func_type;
    f->precision = aggr_func_precision;
    f->condition = aggr_func_condition;
    f->row = aggr_func_row;
    f->compute = aggr_func_compute;
    f->result = aggr_func_result;

    return f;
}

// Factory functions for each aggregate type

struct flintdb_aggregate_func *flintdb_func_count(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    enum flintdb_variant_type  t = (type != VARIANT_NULL) ? type : VARIANT_INT64;
    return aggr_func_new_common(name ? name : "COUNT", alias ? alias : "count", t, cond, FUNC_COUNT, 0, e);
}

struct flintdb_aggregate_func *flintdb_func_distinct_count(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    enum flintdb_variant_type  t = (type != VARIANT_NULL) ? type : VARIANT_INT64;
    return aggr_func_new_common(name ? name : "DISTINCT_COUNT", alias ? alias : "distinct_count", t, cond, FUNC_DISTINCT_RB, 0, e);
}

struct flintdb_aggregate_func *flintdb_func_distinct_hll_count(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    enum flintdb_variant_type  t = (type != VARIANT_NULL) ? type : VARIANT_INT64;
    return aggr_func_new_common(name ? name : "DISTINCT_HLL_COUNT", alias ? alias : "distinct_hll_count", t, cond, FUNC_DISTINCT_HLL, 0, e);
}

struct flintdb_aggregate_func *flintdb_func_sum(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    enum flintdb_variant_type  t = (type != VARIANT_NULL) ? type : VARIANT_DECIMAL;
    return aggr_func_new_common(name ? name : "SUM", alias ? alias : "sum", t, cond, FUNC_SUM, 5, e);
}

struct flintdb_aggregate_func *flintdb_func_avg(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    enum flintdb_variant_type  t = (type != VARIANT_NULL) ? type : VARIANT_DECIMAL;
    return aggr_func_new_common(name ? name : "AVG", alias ? alias : "avg", t, cond, FUNC_AVG, 5, e);
}

struct flintdb_aggregate_func *flintdb_func_min(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    return aggr_func_new_common(name ? name : "MIN", alias ? alias : "min", type, cond, FUNC_MIN, 0, e);
}

struct flintdb_aggregate_func *flintdb_func_max(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    return aggr_func_new_common(name ? name : "MAX", alias ? alias : "max", type, cond, FUNC_MAX, 0, e);
}

struct flintdb_aggregate_func *flintdb_func_first(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    return aggr_func_new_common(name ? name : "FIRST", alias ? alias : "first", type, cond, FUNC_FIRST, 0, e);
}

struct flintdb_aggregate_func *flintdb_func_last(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    return aggr_func_new_common(name ? name : "LAST", alias ? alias : "last", type, cond, FUNC_LAST, 0, e);
}

struct flintdb_aggregate_func *flintdb_func_rowid(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    enum flintdb_variant_type  t = (type != VARIANT_NULL) ? type : VARIANT_INT64;
    return aggr_func_new_common(name ? name : "ROWID", alias ? alias : "rowid", t, cond, FUNC_ROWID, 0, e);
}

struct flintdb_aggregate_func *flintdb_func_hash(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e) {
    enum flintdb_variant_type  t = (type != VARIANT_NULL) ? type : VARIANT_INT64;
    return aggr_func_new_common(name ? name : "HASH", alias ? alias : "hash", t, cond, FUNC_HASH, 0, e);
}

// === MAIN AGGREGATE STRUCTURE ===

static void aggregate_free(struct flintdb_aggregate *agg) {
    if (!agg)
        return;
    struct flintdb_aggregate_priv *p = (struct flintdb_aggregate_priv *)agg->priv;
    if (p) {
        if (p->groupby) {
            for (int i = 0; i < p->groupby_count; i++) {
                if (p->groupby[i])
                    p->groupby[i]->free(p->groupby[i]);
            }
            FREE(p->groupby);
        }
        if (p->funcs) {
            for (int i = 0; i < p->func_count; i++) {
                if (p->funcs[i])
                    p->funcs[i]->free(p->funcs[i]);
            }
            FREE(p->funcs);
        }
        if (p->keys) {
            p->keys->clear(p->keys);
            p->keys->free(p->keys);
        }
        if (p->group_cols_cache)
            FREE((void *)p->group_cols_cache);
        if (p->group_col_indices)
            FREE(p->group_col_indices);
        if (p->indices_cache)
            FREE(p->indices_cache);
        if (p->result_meta) {
            flintdb_meta_close(p->result_meta);
            FREE(p->result_meta);
        }
        FREE(p);
    }
    FREE(agg);
}

static void key_dealloc(keytype k, valtype v) {
    // k is integer hash - no need to free
    (void)k;
    // v is groupkey pointer
    struct flintdb_aggregate_groupkey *gk = (struct flintdb_aggregate_groupkey *)(uintptr_t)v;
    if (gk)
        gk->free(gk);
}

static void aggregate_row(struct flintdb_aggregate *agg, const struct flintdb_row *r, char **e) {
    if (!agg || !r)
        return;
    struct flintdb_aggregate_priv *p = (struct flintdb_aggregate_priv *)agg->priv;
    if (!p)
        return;

    if (!p->keys) {
        p->keys = groupkey_map_new();
    }

    // Initialize group_cols_cache once
    if (!p->group_cols_cache && p->groupby_count > 0) {
        p->group_cols_cache = (const char **)CALLOC(p->groupby_count, sizeof(char *));
        for (int i = 0; i < p->groupby_count; i++) {
            p->group_cols_cache[i] = p->groupby[i]->column(p->groupby[i]);
        }
    }
    
    // Initialize column indices cache if meta changed
    if (r->meta != p->cached_meta) {
        if (!p->group_col_indices && p->groupby_count > 0) {
            p->group_col_indices = (int *)CALLOC(p->groupby_count, sizeof(int));
        }
        if (p->group_col_indices) {
            for (int i = 0; i < p->groupby_count; i++) {
                p->group_col_indices[i] = flintdb_column_at((struct flintdb_meta *)r->meta, p->group_cols_cache[i]);
            }
        }
        p->cached_meta = r->meta;
    }

    // Fast path: compute hash directly without creating full groupkey
    scratch_reset();
    for (int i = 0; i < p->groupby_count; i++) {
        if (i > 0)
            scratch_put_sep();
        int idx = p->group_col_indices ? p->group_col_indices[i] : -1;
        if (idx >= 0)
            scratch_append_col_stable_str(r, idx);
    }
    scratch_put_char('\0');
    u32 hash = hashmap_string_hash((keytype)(uintptr_t)scratch_data());
    
    // Check if this group already exists
    valtype existing_val = p->keys->get(p->keys, (keytype)(uintptr_t)hash);
    struct flintdb_aggregate_groupkey *gk = NULL;
    
    if (existing_val == HASHMAP_INVALID_VAL) {
        // New group - create full groupkey only once per unique group
        gk = flintdb_groupkey_from_row(agg, r, p->group_cols_cache, p->groupby_count, e);

        if (!gk)
            return;
        // Store in keys map
        p->keys->put(p->keys, (keytype)(uintptr_t)hash, (valtype)(uintptr_t)gk, key_dealloc);
    } else {
        // Use existing groupkey
        gk = (struct flintdb_aggregate_groupkey *)(uintptr_t)existing_val;
    }
    
    if (!gk)
        return;

    // Process all functions with this row and group key
    for (int i = 0; i < p->func_count; i++) {
        const struct flintdb_aggregate_condition *cond = p->funcs[i]->condition(p->funcs[i]);
        if (cond && cond->ok && !cond->ok(cond, r, e))
            continue;

        p->funcs[i]->row(p->funcs[i], gk, r, e);
    }

    // Don't free gk - it's either stored in hashmap or is from hashmap
}

static int aggregate_compute(struct flintdb_aggregate *agg, struct flintdb_row ***out_rows, char **e) {
    if (!agg)
        return 0;
    struct flintdb_aggregate_priv *p = (struct flintdb_aggregate_priv *)agg->priv;
    if (!p)
        return 0;

    // Count keys
    int key_count = 0;
    if (p->keys) {
        key_count = p->keys->count_get(p->keys);
    }

    // If no keys and no groupby, create one default group
    if (key_count == 0 && p->groupby_count == 0) {
        key_count = 1;
    }

    if (key_count == 0) {
        *out_rows = NULL;
        return 0;
    }

    // Build meta for result (cached after first compute)
    if (!p->result_meta) {
        int col_count = p->groupby_count + p->func_count;
        p->result_meta = (struct flintdb_meta *)CALLOC(1, sizeof(struct flintdb_meta));
        if (!p->result_meta)
            return 0;

        p->result_meta->columns.length = col_count;
        for (int i = 0; i < p->groupby_count; i++) {
            const char *alias = p->groupby[i]->alias(p->groupby[i]);
            s_copy(p->result_meta->columns.a[i].name, sizeof(p->result_meta->columns.a[i].name), alias);
            p->result_meta->columns.a[i].type = p->groupby[i]->type(p->groupby[i]);
            p->result_meta->columns.a[i].bytes = 32;
        }

        for (int i = 0; i < p->func_count; i++) {
            int col = p->groupby_count + i;
            s_copy(p->result_meta->columns.a[col].name, sizeof(p->result_meta->columns.a[col].name), p->funcs[i]->alias(p->funcs[i]));
            p->result_meta->columns.a[col].type = p->funcs[i]->type(p->funcs[i]);
            p->result_meta->columns.a[col].bytes = 8;
            p->result_meta->columns.a[col].precision = p->funcs[i]->precision(p->funcs[i]);

            if (p->result_meta->columns.a[col].type == VARIANT_DECIMAL) {
                if (p->result_meta->columns.a[col].bytes < 16)
                    p->result_meta->columns.a[col].bytes = 16;
            }
        }
    }

    // Allocate result rows
    struct flintdb_row **rows = (struct flintdb_row **)CALLOC(key_count, sizeof(struct flintdb_row *));
    if (!rows) {
        return 0;
    }

    // If no groupby, just create one global result
    if (p->groupby_count == 0) {
        // Compute all functions for empty group key
        struct flintdb_aggregate_groupkey *gk = flintdb_groupkey_from_row(agg, NULL, NULL, 0, e);
        for (int i = 0; i < p->func_count; i++) {
            p->funcs[i]->compute(p->funcs[i], gk, e);
        }

        struct flintdb_row *row = flintdb_row_new(p->result_meta, e);
        for (int i = 0; i < p->func_count; i++) {
            const struct flintdb_variant *v = p->funcs[i]->result(p->funcs[i], gk, e);
            if (v) {
                row->set(row, i, (struct flintdb_variant *)v, e);
            }
        }
        rows[0] = row;
        if (gk)
            gk->free(gk);

        *out_rows = rows;
        return 1;
    }

    // Iterate through all keys and compute results
    struct map_iterator it = {0};
    int row_idx = 0;

    while (p->keys->iterate(p->keys, &it)) {
        // const char *group_id = (const char*)(uintptr_t)it.key;
        struct flintdb_aggregate_groupkey *gk = (struct flintdb_aggregate_groupkey *)(uintptr_t)it.val;

        // Compute all functions for this group
        for (int i = 0; i < p->func_count; i++) {
            p->funcs[i]->compute(p->funcs[i], gk, e);
        }

        // Build result row
        struct flintdb_row *row = flintdb_row_new(p->result_meta, e);

        // Set group columns from stored groupkey
        if (gk && gk->priv) {
            struct flintdb_aggregate_groupkey_priv *gkp = (struct flintdb_aggregate_groupkey_priv *)gk->priv;
            if (gkp->krow) {
                for (int i = 0; i < p->groupby_count && i < gkp->krow->meta->columns.length; i++) {
                    const struct flintdb_variant *v = gkp->krow->get(gkp->krow, i, e);
                    if (v) {
                        row->set(row, i, (struct flintdb_variant *)v, e);
                    }
                }
            }
        }

        // Set function results
        for (int i = 0; i < p->func_count; i++) {
            const struct flintdb_variant *v = p->funcs[i]->result(p->funcs[i], gk, e);
            if (v) {
                row->set(row, p->groupby_count + i, (struct flintdb_variant *)v, e);
            }
        }

        rows[row_idx++] = row;
        // Don't free gk here - it's owned by hashmap and will be freed in key_dealloc
    }

    *out_rows = rows;
    return row_idx;
}

struct flintdb_aggregate *aggregate_new(const char *id, struct flintdb_aggregate_groupby **groupby, u16 groupby_count,
                                struct flintdb_aggregate_func **funcs, u16 func_count, char **e) {
    (void)e;
    struct flintdb_aggregate *agg = (struct flintdb_aggregate *)CALLOC(1, sizeof(struct flintdb_aggregate));
    if (!agg)
        return NULL;

    struct flintdb_aggregate_priv *p = (struct flintdb_aggregate_priv *)CALLOC(1, sizeof(struct flintdb_aggregate_priv));
    if (!p) {
        FREE(agg);
        return NULL;
    }

    s_copy(p->id, sizeof(p->id), id ? id : "");
    p->groupby = groupby;
    p->groupby_count = groupby_count;
    p->funcs = funcs;
    p->func_count = func_count;
    p->keys = NULL; // Created on first row

    agg->priv = p;
    agg->free = aggregate_free;
    agg->row = aggregate_row;
    agg->compute = aggregate_compute;

    return agg;
}
