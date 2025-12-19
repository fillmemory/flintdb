#include "flintdb.h"
#include "runtime.h"
#include "buffer.h"
#include "internal.h"
#include "simd.h"

#include <assert.h>
#include <stdio.h>
#include <strings.h>
#include <math.h>
#include <stdatomic.h>

// Optional row pooling to reduce per-row malloc/free churn in hot paths (e.g. table scans).
// Pool stores up to ROW_POOL_MAX reusable row objects per meta schema. Rows are cleaned
// (variants freed + reinitialized) before reuse. Only structure/array allocation is reused.
// Enable by using flintdb_row_pool_acquire()/flintdb_row_pool_release() instead of flintdb_row_new()/row->free().
// Safe because variant memory is released on return to pool.
#define ROW_POOL_MAX 256

struct flintdb_row_pool_bucket {
    struct flintdb_meta *meta;            // schema this bucket serves
    struct flintdb_row *rows[ROW_POOL_MAX];
    int count;                    // number of cached rows
};

// Use C11 stdatomic spinlock for cross-platform compatibility (macOS, Linux, Windows MinGW)
#define ROW_POOL_LOCK(lock) do { int expected = 0; while (!atomic_compare_exchange_weak_explicit(lock, &expected, 1, memory_order_acquire, memory_order_relaxed)) { expected = 0; } } while(0)
#define ROW_POOL_UNLOCK(lock) atomic_store_explicit(lock, 0, memory_order_release)

// Simple linear bucket array; for small distinct metas this is fine.
static struct {
    atomic_int lock;
    int bucket_count;
    struct flintdb_row_pool_bucket buckets[32]; // supports up to 32 distinct metas in pool
} g_row_pool = {0, 0, {{0}}};

static void row_pool_cleanup_row(struct flintdb_row *r) {
    if (!r) return;
    // Free owned variant data then re-init variants for clean reuse.
    for (int i = 0; i < r->length; i++) {
        flintdb_variant_free(&r->array[i]);
        flintdb_variant_init(&r->array[i]); // reset to NIL
    }
    r->rowid = -1;
}

// Acquire a pooled row for given meta; allocate new if none available.
struct flintdb_row *flintdb_row_pool_acquire(struct flintdb_meta *meta, char **e) {
    if (!meta) {
        if (e) *e = "row_pool_acquire: meta is NULL";
        return NULL;
    }
    if (e) *e = NULL;
    ROW_POOL_LOCK(&g_row_pool.lock);
    // Find bucket
    int bi = -1;
    for (int i = 0; i < g_row_pool.bucket_count; i++) {
        if (g_row_pool.buckets[i].meta == meta) { bi = i; break; }
    }
    if (bi >= 0) {
        struct flintdb_row_pool_bucket *b = &g_row_pool.buckets[bi];
        if (b->count > 0) {
            struct flintdb_row *r = b->rows[--b->count];
            b->rows[b->count] = NULL;
            ROW_POOL_UNLOCK(&g_row_pool.lock);
            row_pool_cleanup_row(r); // ensure clean state
            return r;
        }
    }
    ROW_POOL_UNLOCK(&g_row_pool.lock);
    // Allocate new row (not found or empty bucket)
    struct flintdb_row *r = flintdb_row_new(meta, e);
    if (!r) return NULL;
    // Override free to pooled release
    r->free = (void (*)(struct flintdb_row *))flintdb_row_pool_release;
    return r;
}

// Release a row back to pool (called via r->free). Falls back to real free if pool full.
void flintdb_row_pool_release(struct flintdb_row *r) {
    if (!r) return;
    struct flintdb_meta *meta = r->meta;
    if (!meta) { // if meta missing just hard free
        // restore original behavior
        for (int i = 0; i < r->length; i++) flintdb_variant_free(&r->array[i]);
        FREE(r->array);
        FREE(r);
        return;
    }
    ROW_POOL_LOCK(&g_row_pool.lock);
    int bi = -1;
    for (int i = 0; i < g_row_pool.bucket_count; i++) {
        if (g_row_pool.buckets[i].meta == meta) { bi = i; break; }
    }
    if (bi < 0) { // create new bucket if space
        if (g_row_pool.bucket_count < (int)(sizeof(g_row_pool.buckets)/sizeof(g_row_pool.buckets[0]))) {
            bi = g_row_pool.bucket_count++;
            g_row_pool.buckets[bi].meta = meta;
            g_row_pool.buckets[bi].count = 0;
        } else {
            bi = -1; // no bucket space; will hard free below
        }
    }
    if (bi >= 0) {
        struct flintdb_row_pool_bucket *b = &g_row_pool.buckets[bi];
        if (b->count < ROW_POOL_MAX) {
            row_pool_cleanup_row(r);
            b->rows[b->count++] = r;
            ROW_POOL_UNLOCK(&g_row_pool.lock);
            return;
        }
    }
    ROW_POOL_UNLOCK(&g_row_pool.lock);
    // Pool full or no bucket: hard free
    for (int i = 0; i < r->length; i++) flintdb_variant_free(&r->array[i]);
    FREE(r->array);
    FREE(r);
}

// Optional stats helper (not exported unless prototype added)
int row_pool_size(struct flintdb_meta *meta) {
    ROW_POOL_LOCK(&g_row_pool.lock);
    for (int i = 0; i < g_row_pool.bucket_count; i++) {
        if (g_row_pool.buckets[i].meta == meta) {
            int c = g_row_pool.buckets[i].count;
            ROW_POOL_UNLOCK(&g_row_pool.lock);
            return c;
        }
    }
    ROW_POOL_UNLOCK(&g_row_pool.lock);
    return 0;
}


// Optimized BCD to i64 conversion for DECIMAL encoding
static inline i64 row_bcd_to_i64_opt(const u8 *data, u32 length, int skipLeadingHi) {
    i64 result = 0;
    
#if defined(SIMD_HAS_NEON) && 0  // Disabled for now - complex logic, marginal benefit for small decimals
    // ARM NEON path for large BCD arrays (>8 bytes)
    // For typical decimals (<= 8 bytes), scalar is faster
    if (length > 8) {
        // Process in chunks, accumulating partial results
        // This is complex and may not provide significant speedup for typical use
    }
#endif
    
    // Optimized scalar path with reduced branching
    for (u32 bi = 0; bi < length; ++bi) {
        u8 b = data[bi];
        int hi = (b >> 4) & 0x0F;
        int lo = b & 0x0F;
        
        // Skip leading high nibble only on first byte if requested
        if (LIKELY(!(bi == 0 && skipLeadingHi))) {
            result = result * 10 + (i64)hi;
        }
        result = result * 10 + (i64)lo;
    }
    
    return result;
}

// Optimized i64 to bytes conversion with minimal branching - Little Endian native
static inline int row_i64_to_bytes_opt(i64 value, u8 *tmp, int *start_out) {
    // Convert i64 to little-endian bytes (LSB first)
    for (int k = 0; k < 8; k++) {
        tmp[k] = (u8)(value & 0xFF);
        value >>= 8;
    }
    
    // Find end position (remove redundant sign extension bytes from high end)
    int end = 8;
    int is_neg = (tmp[7] & 0x80) != 0;
    
    // Unrolled loop for common cases
    if (!is_neg) {
        // Positive number - skip trailing 0x00 bytes with 0 sign bit
        while (end > 1 && tmp[end - 1] == 0x00 && (tmp[end - 2] & 0x80) == 0) {
            end--;
        }
    } else {
        // Negative number - skip trailing 0xFF bytes with 1 sign bit
        while (end > 1 && tmp[end - 1] == 0xFF && (tmp[end - 2] & 0x80) != 0) {
            end--;
        }
    }
    
    *start_out = 0;  // Always start at 0 for little endian
    return end;
}

// Optimized bytes to i64 conversion for DECIMAL decoding - Little Endian native
static inline i64 row_bytes_to_i64_opt(const char *p, u32 n) {
    i64 x = 0;
    
#if defined(SIMD_HAS_AVX2) || defined(SIMD_HAS_SSE2) || defined(SIMD_HAS_NEON)
    // For small byte arrays (≤8 bytes), vectorization overhead exceeds benefit
    // Use optimized scalar path
#endif
    
    // Optimized scalar with unrolled cases for common sizes (little-endian)
    switch (n) {
        case 1:
            x = (i64)(i8)p[0];
            break;
        case 2:
            x = (i64)(u8)p[0] | ((i64)(i8)p[1] << 8);
            break;
        case 3:
            x = (i64)(u8)p[0] | ((i64)(u8)p[1] << 8) | ((i64)(i8)p[2] << 16);
            break;
        case 4:
            x = (i64)(u8)p[0] | ((i64)(u8)p[1] << 8) | 
                ((i64)(u8)p[2] << 16) | ((i64)(i8)p[3] << 24);
            break;
        case 8:
            // Full 8-byte load with proper sign extension (little-endian)
            x = (i64)(u8)p[0] | ((i64)(u8)p[1] << 8) |
                ((i64)(u8)p[2] << 16) | ((i64)(u8)p[3] << 24) |
                ((i64)(u8)p[4] << 32) | ((i64)(u8)p[5] << 40) |
                ((i64)(u8)p[6] << 48) | ((i64)(i8)p[7] << 56);
            break;
        default:
            // General case for other sizes (5, 6, 7 bytes) - little endian
            for (u32 j = 0; j < n; j++) {
                x |= ((i64)(u8)p[j]) << (j * 8);
            }
            // Sign extend if needed (check last byte)
            if (n < 8 && (p[n - 1] & 0x80)) {
                u64 mask = (~(u64)0) << (n * 8);
                x |= (i64)mask;
            }
            break;
    }
    
    return x;
}

// Fast time_t to date components conversion (avoiding localtime_r)
static inline void row_fast_time_to_date(time_t tt, int *year_out, int *month_out, int *day_out) {
    // Convert time_t to days since epoch
    i64 days = (i64)tt / 86400;
    
    // Adjust for timezone (approximate - assumes UTC or local time is close)
    // For exact local time, would need timezone info, but this is faster
    
    // Modified Julian Day calculation (inverse of days_since_epoch)
    i64 a = days + 719468; // Adjust to March 1, 0000
    i64 era = (a >= 0 ? a : a - 146096) / 146097;
    i64 doe = a - era * 146097;
    i64 yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    i64 y = yoe + era * 400;
    i64 doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    i64 mp = (5 * doy + 2) / 153;
    i64 d = doy - (153 * mp + 2) / 5 + 1;
    i64 m = mp + (mp < 10 ? 3 : -9);
    
    if (m <= 2) y++;
    
    *year_out = (int)y;
    *month_out = (int)m;
    *day_out = (int)d;
}

static void row_init(struct flintdb_meta *meta, struct flintdb_row *r, char **e);

// Fast datetime parsing with cache to avoid expensive mktime() calls
// Cache last 8 parsed dates (thread-local)
#define DATETIME_CACHE_SIZE 8
static _Thread_local struct
{
    u32 packed_date;  // yyyyMMdd packed as integer
    time_t base_time; // midnight timestamp for this date
} g_datetime_cache[DATETIME_CACHE_SIZE];
static _Thread_local int g_datetime_cache_init = 0;

// Fast calculation of days since epoch (1970-01-01)
static inline int days_since_epoch(int year, int month, int day) {
    // Adjust for months (March = 0)
    int a = (14 - month) / 12;
    int y = year - a;
    int m = month + 12 * a - 3;
    // Days calculation using Zeller-like formula
    int days = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 719469;
    return days;
}

static inline int parse_datetime(const char *s, u32 len, time_t *out) {
    if (UNLIKELY(!s || !out))
        return -1;

    // Initialize cache on first use
    if (UNLIKELY(!g_datetime_cache_init)) {
        memset(g_datetime_cache, 0, sizeof(g_datetime_cache));
        g_datetime_cache_init = 1;
    }

    // Parse components - optimized with minimal branches
    int year = 0, mon = 0, day = 0, hh = 0, mm = 0, ss = 0;

    if (len == 10) {
        // yyyy-MM-dd
        // Validate format characters first
        if (UNLIKELY(!(s[4] == '-' && s[7] == '-')))
            return -2;

#if defined(SIMD_HAS_SSE2) || defined(SIMD_HAS_NEON)
        // SIMD-optimized digit validation and conversion for yyyy-MM-dd
        // Load 10 bytes and check if all expected positions are digits
        const u8 *p = (const u8 *)s;
        
        // Create digit validation mask: check if all digit positions are in '0'-'9' range
        // Positions: 0,1,2,3 (year), 5,6 (month), 8,9 (day) - skip 4,7 (dashes)
        u8 digits[10];
        for (int i = 0; i < 10; i++) {
            digits[i] = p[i] - '0';
        }
        
        // Validate all digit positions are in [0, 9]
        if (UNLIKELY(digits[0] > 9 || digits[1] > 9 || digits[2] > 9 || digits[3] > 9 ||
                     digits[5] > 9 || digits[6] > 9 || digits[8] > 9 || digits[9] > 9))
            return -2;
        
        // Convert to integers (compiler will optimize these)
        year = digits[0] * 1000 + digits[1] * 100 + digits[2] * 10 + digits[3];
        mon = digits[5] * 10 + digits[6];
        day = digits[8] * 10 + digits[9];
#else
        // Fallback scalar path
        if (s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9' || 
            s[2] < '0' || s[2] > '9' || s[3] < '0' || s[3] > '9')
            return -2;
        year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');

        if (s[5] < '0' || s[5] > '9' || s[6] < '0' || s[6] > '9')
            return -2;
        mon = (s[5] - '0') * 10 + (s[6] - '0');

        if (s[8] < '0' || s[8] > '9' || s[9] < '0' || s[9] > '9')
            return -2;
        day = (s[8] - '0') * 10 + (s[9] - '0');
#endif
    } else if (len >= 19) {
        // yyyy-MM-dd HH:mm:ss
        // Validate format characters
        if (UNLIKELY(!(s[4] == '-' && s[7] == '-' && s[10] == ' ' && s[13] == ':' && s[16] == ':')))
            return -3;

#if defined(SIMD_HAS_SSE2) || defined(SIMD_HAS_NEON)
        // SIMD-optimized digit validation and conversion for full timestamp
        const u8 *p = (const u8 *)s;
        
        // Extract and validate digits
        u8 digits[19];
        for (int i = 0; i < 19; i++) {
            digits[i] = p[i] - '0';
        }
        
        // Validate all digit positions (skip separators at 4,7,10,13,16)
        if (UNLIKELY(digits[0] > 9 || digits[1] > 9 || digits[2] > 9 || digits[3] > 9 ||
                     digits[5] > 9 || digits[6] > 9 || digits[8] > 9 || digits[9] > 9 ||
                     digits[11] > 9 || digits[12] > 9 || digits[14] > 9 || digits[15] > 9 ||
                     digits[17] > 9 || digits[18] > 9))
            return -3;
        
        // Convert to integers
        year = digits[0] * 1000 + digits[1] * 100 + digits[2] * 10 + digits[3];
        mon = digits[5] * 10 + digits[6];
        day = digits[8] * 10 + digits[9];
        hh = digits[11] * 10 + digits[12];
        mm = digits[14] * 10 + digits[15];
        ss = digits[17] * 10 + digits[18];
#else
        // Fallback scalar path
        if (s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9' || 
            s[2] < '0' || s[2] > '9' || s[3] < '0' || s[3] > '9')
            return -3;
        year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');

        if (s[5] < '0' || s[5] > '9' || s[6] < '0' || s[6] > '9')
            return -3;
        mon = (s[5] - '0') * 10 + (s[6] - '0');

        if (s[8] < '0' || s[8] > '9' || s[9] < '0' || s[9] > '9')
            return -3;
        day = (s[8] - '0') * 10 + (s[9] - '0');

        if (s[11] < '0' || s[11] > '9' || s[12] < '0' || s[12] > '9')
            return -3;
        hh = (s[11] - '0') * 10 + (s[12] - '0');

        if (s[14] < '0' || s[14] > '9' || s[15] < '0' || s[15] > '9')
            return -3;
        mm = (s[14] - '0') * 10 + (s[15] - '0');

        if (s[17] < '0' || s[17] > '9' || s[18] < '0' || s[18] > '9')
            return -3;
        ss = (s[17] - '0') * 10 + (s[18] - '0');
#endif
    } else {
        return -4;
    }

    // Range validation
    if (UNLIKELY(year < 1900))
        return -5;
    if (UNLIKELY(mon < 1 || mon > 12))
        return -5;
    if (UNLIKELY(day < 1 || day > 31))
        return -5;

    // Pack date for cache lookup
    u32 packed = (u32)year * 10000 + (u32)mon * 100 + (u32)day;

    // Check cache (linear search is fast for small cache)
    time_t base_time = 0;
    int cache_hit = 0;
    for (int i = 0; i < DATETIME_CACHE_SIZE; i++) {
        if (g_datetime_cache[i].packed_date == packed) {
            base_time = g_datetime_cache[i].base_time;
            cache_hit = 1;
            break;
        }
    }

    if (UNLIKELY(!cache_hit)) {
        // Fast calculation without mktime()
        // Calculate days since Unix epoch (1970-01-01)
        int days = days_since_epoch(year, mon, day);
        base_time = (time_t)days * 86400; // 86400 seconds per day

        // Store in cache (simple round-robin)
        static _Thread_local int cache_index = 0;
        g_datetime_cache[cache_index].packed_date = packed;
        g_datetime_cache[cache_index].base_time = base_time;
        cache_index = (cache_index + 1) % DATETIME_CACHE_SIZE;
    }

    // Add time components
    time_t result = base_time + (time_t)hh * 3600 + (time_t)mm * 60 + (time_t)ss;
    *out = result;
    return 0;
}

// helper to treat certain tokens as NULL
static inline int is_nil_token(const char *s, const struct flintdb_meta *m) {
    if (!s || s[0] == '\0')
        return 1;
    if (m && m->nil_str[0] && strcmp(s, m->nil_str) == 0)
        return 1;
    if (strcmp(s, "\\N") == 0)
        return 1;
    if (strcmp(s, "NULL") == 0)
        return 1;
    if (strcmp(s, "null") == 0)
        return 1;
    if (strcmp(s, "Null") == 0)
        return 1;
    return 0;
}

static inline char *row_error_set(const struct flintdb_row *r, const char *msg) {
    if (!r)
        return NULL;

    snprintf(TL_ERROR, ERROR_BUFSZ - 1, "%s", msg);
    return TL_ERROR;
}

// Forward declarations for internal setter helpers so row_set can use them
static void row_string_set(struct flintdb_row *rr, u16 i, const char *str, char **ee);
static void row_i64_set(struct flintdb_row *rr, u16 i, i64 val, char **ee);
static void row_f64_set(struct flintdb_row *rr, u16 i, f64 val, char **ee);
static void row_u8_set(struct flintdb_row *rr, u16 i, u8 val, char **ee);
static void row_i8_set(struct flintdb_row *rr, u16 i, i8 val, char **ee);
static void row_u16_set(struct flintdb_row *rr, u16 i, u16 val, char **ee);
static void row_i16_set(struct flintdb_row *rr, u16 i, i16 val, char **ee);
static void row_u32_set(struct flintdb_row *rr, u16 i, u32 val, char **ee);
static void row_i32_set(struct flintdb_row *rr, u16 i, i32 val, char **ee);
static void row_bytes_set(struct flintdb_row *rr, u16 i, const char *data, u32 length, char **ee);
static void row_date_set(struct flintdb_row *rr, u16 i, time_t val, char **ee);
static void row_time_set(struct flintdb_row *rr, u16 i, time_t val, char **ee);
static void row_uuid_set(struct flintdb_row *rr, u16 i, const char *data, u32 length, char **ee);
static void row_ipv6_set(struct flintdb_row *rr, u16 i, const char *data, u32 length, char **ee);
static void row_decimal_set(struct flintdb_row *rr, u16 i, struct flintdb_decimal  data, char **ee);

static i64 row_id(const struct flintdb_row *r) {
    if (!r)
        return -1;
    return r->rowid;
}

static struct flintdb_variant *row_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || !r->array) {
        if (e)
            *e = row_error_set(r, "row_get: row or array is NULL");
        return NULL;
    }
    if (i >= r->length) {
        if (e)
            *e = row_error_set(r, "row_get: index out of bounds");
        return NULL;
    }
    if (e)
        *e = NULL;
    return &r->array[i];
}

static void row_set(struct flintdb_row *r, u16 i, struct flintdb_variant *v, char **e) {
    if (!r || !r->array) {
        if (e)
            *e = row_error_set(r, "row_set: row or array is NULL");
        return;
    }
    if (!r || !r->array) {
        if (e)
            *e = row_error_set(r, "row_set: row or array is NULL");
        return;
    }
    if (i >= r->length) {
        if (e)
            *e = row_error_set(r, "row_set: index out of bounds");
        return;
    }
    if (!v) {
        if (e)
            *e = row_error_set(r, "row_set: source variant is NULL");
        return;
    }
    if (e)
        *e = NULL;

    // Determine target type from meta.columns
    enum flintdb_variant_type  target = VARIANT_NULL;
    // int precision = 0;
    if (r->meta && r->meta->columns.length > i) {
        target = r->meta->columns.a[i].type;
        // precision = r->meta->columns.a[i].precision;
    }

    // Fast path: identical type (or STRING where we accept direct copy)
    if (target == v->type || target == VARIANT_NULL) {
        flintdb_variant_copy(&r->array[i], v);
        return;
    }

    // Perform casting similar to Java Row.cast
    switch (target) {
    case VARIANT_STRING: {
        // Convert any value to string representation
        char buf[64];
        buf[0] = '\0';
        switch (v->type) {
        case VARIANT_STRING:
            flintdb_variant_copy(&r->array[i], v);
            return;
        case VARIANT_INT8:
        case VARIANT_UINT8:
        case VARIANT_INT16:
        case VARIANT_UINT16:
        case VARIANT_INT32:
        case VARIANT_UINT32:
        case VARIANT_INT64: {
            snprintf(buf, sizeof(buf), "%lld", (long long)v->value.i);
            row_string_set(r, i, buf, e);
            return;
        }
        case VARIANT_DOUBLE: {
            snprintf(buf, sizeof(buf), "%.*g", 17, v->value.f);
            row_string_set(r, i, buf, e);
            return;
        }
        default:
            // Fallback: store empty string for unsupported
            row_string_set(r, i, "", e);
            return;
        }
    }
    case VARIANT_INT32: {
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            i64 x;
            if (parse_i64(v->value.b.data, v->value.b.length, &x) == 0) {
                row_i32_set(r, i, (i32)x, e);
                return;
            }
        } else if (v->type == VARIANT_DOUBLE) {
            row_i32_set(r, i, (i32)(v->value.f), e);
            return;
        } else if (v->type == VARIANT_INT64 || v->type == VARIANT_INT32 || v->type == VARIANT_INT16 || v->type == VARIANT_INT8 || v->type == VARIANT_UINT32 || v->type == VARIANT_UINT16 || v->type == VARIANT_UINT8) {
            row_i32_set(r, i, (i32)(v->value.i), e);
            return;
        }
        break;
    }
    case VARIANT_UINT32: {
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            u64 x;
            if (parse_u64(v->value.b.data, v->value.b.length, &x) == 0) {
                row_u32_set(r, i, (u32)x, e);
                return;
            }
        } else if (v->type == VARIANT_DOUBLE) {
            row_u32_set(r, i, (u32)(v->value.f), e);
            return;
        } else if (v->type == VARIANT_INT64 || v->type == VARIANT_INT32 || v->type == VARIANT_INT16 || v->type == VARIANT_INT8 || v->type == VARIANT_UINT32 || v->type == VARIANT_UINT16 || v->type == VARIANT_UINT8) {
            row_u32_set(r, i, (u32)(v->value.i), e);
            return;
        }
        break;
    }
    case VARIANT_INT64: {
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            i64 x;
            if (parse_i64(v->value.b.data, v->value.b.length, &x) == 0) {
                row_i64_set(r, i, x, e);
                return;
            }
        } else if (v->type == VARIANT_DOUBLE) {
            row_i64_set(r, i, (i64)(v->value.f), e);
            return;
        } else if (v->type == VARIANT_INT64 || v->type == VARIANT_INT32 || v->type == VARIANT_INT16 || v->type == VARIANT_INT8 || v->type == VARIANT_UINT32 || v->type == VARIANT_UINT16 || v->type == VARIANT_UINT8) {
            row_i64_set(r, i, (i64)(v->value.i), e);
            return;
        }
        break;
    }
    case VARIANT_DOUBLE:
    case VARIANT_FLOAT: {
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            f64 x;
            if (parse_f64(v->value.b.data, v->value.b.length, &x) == 0) {
                row_f64_set(r, i, x, e);
                return;
            }
        } else if (v->type == VARIANT_DOUBLE) {
            row_f64_set(r, i, v->value.f, e);
            return;
        } else if (v->type == VARIANT_INT64 || v->type == VARIANT_INT32 || v->type == VARIANT_INT16 || v->type == VARIANT_INT8 || v->type == VARIANT_UINT32 || v->type == VARIANT_UINT16 || v->type == VARIANT_UINT8) {
            row_f64_set(r, i, (f64)(v->value.i), e);
            return;
        }
        break;
    }
    case VARIANT_INT16: {
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            i64 x;
            if (parse_i64(v->value.b.data, v->value.b.length, &x) == 0) {
                row_i16_set(r, i, (i16)x, e);
                return;
            }
        } else if (v->type == VARIANT_DOUBLE) {
            row_i16_set(r, i, (i16)(v->value.f), e);
            return;
        } else {
            row_i16_set(r, i, (i16)(v->value.i), e);
            return;
        }
        break;
    }
    case VARIANT_UINT16: {
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            u64 x;
            if (parse_u64(v->value.b.data, v->value.b.length, &x) == 0) {
                row_u16_set(r, i, (u16)x, e);
                return;
            }
        } else if (v->type == VARIANT_DOUBLE) {
            row_u16_set(r, i, (u16)(v->value.f), e);
            return;
        } else {
            row_u16_set(r, i, (u16)(v->value.i), e);
            return;
        }
        break;
    }
    case VARIANT_INT8: {
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            i64 x;
            if (parse_i64(v->value.b.data, v->value.b.length, &x) == 0) {
                row_i8_set(r, i, (i8)x, e);
                return;
            }
        } else if (v->type == VARIANT_DOUBLE) {
            row_i8_set(r, i, (i8)(v->value.f), e);
            return;
        } else {
            row_i8_set(r, i, (i8)(v->value.i), e);
            return;
        }
        break;
    }
    case VARIANT_UINT8: {
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            u64 x;
            if (parse_u64(v->value.b.data, v->value.b.length, &x) == 0) {
                row_u8_set(r, i, (u8)x, e);
                return;
            }
        } else if (v->type == VARIANT_DOUBLE) {
            row_u8_set(r, i, (u8)(v->value.f), e);
            return;
        } else {
            row_u8_set(r, i, (u8)(v->value.i), e);
            return;
        }
        break;
    }
    case VARIANT_DATE: {
        if (v->type == VARIANT_TIME || v->type == VARIANT_DATE) {
            row_date_set(r, i, v->value.t, e);
            return;
        }
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            time_t t;
            if (parse_datetime(v->value.b.data, v->value.b.length, &t) == 0) {
                row_date_set(r, i, t, e);
                return;
            }
        }
        break;
    }
    case VARIANT_TIME: {
        if (v->type == VARIANT_TIME || v->type == VARIANT_DATE) {
            row_time_set(r, i, v->value.t, e);
            return;
        }
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            time_t t;
            if (parse_datetime(v->value.b.data, v->value.b.length, &t) == 0) {
                row_time_set(r, i, t, e);
                return;
            }
        }
        break;
    }
    case VARIANT_BYTES: {
        if (v->type == VARIANT_BYTES) {
            flintdb_variant_copy(&r->array[i], v);
            return;
        }
        if (v->type == VARIANT_STRING) {
            if (strempty(v->value.b.data)) {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
            unsigned char *b = NULL;
            u32 blen = 0;
            if (hex_decode(v->value.b.data, &b, &blen) == 0) {
                row_bytes_set(r, i, (const char *)b, blen, e);
                FREE(b);
                return;
            }
        }
        break;
    }
    case VARIANT_UUID: {
        if (v->type == VARIANT_UUID) {
            flintdb_variant_copy(&r->array[i], v);
            return;
        }
        if (v->type == VARIANT_BYTES) {
            u32 len = v->value.b.length;
            row_uuid_set(r, i, v->value.b.data, len, e);
            return;
        }
        if (v->type == VARIANT_STRING) {
            unsigned char *b = NULL;
            u32 blen = 0;
            if (hex_decode(v->value.b.data, &b, &blen) == 0 && blen == 16) {
                row_uuid_set(r, i, (const char *)b, blen, e);
                FREE(b);
                return;
            }
            if (b)
                FREE(b);
        }
        break;
    }
    case VARIANT_IPV6: {
        if (v->type == VARIANT_IPV6) {
            flintdb_variant_copy(&r->array[i], v);
            return;
        }
        if (v->type == VARIANT_BYTES) {
            u32 len = v->value.b.length;
            row_ipv6_set(r, i, v->value.b.data, len, e);
            return;
        }
        if (v->type == VARIANT_STRING) {
            unsigned char *b = NULL;
            u32 blen = 0;
            if (hex_decode(v->value.b.data, &b, &blen) == 0 && blen == 16) {
                row_ipv6_set(r, i, (const char *)b, blen, e);
                FREE(b);
                return;
            }
            if (b)
                FREE(b);
        }
        break;
    }
    case VARIANT_DECIMAL: {
        // Accept DECIMAL directly, or parse from STRING/DOUBLE/INT* honoring precision
        if (v->type == VARIANT_DECIMAL) {
            flintdb_variant_copy(&r->array[i], v);
            return;
        }
        int scale = 0;
        if (r->meta && r->meta->columns.length > i)
            scale = r->meta->columns.a[i].precision;
        struct flintdb_decimal  d = (struct flintdb_decimal ){0};
        if (v->type == VARIANT_STRING) {
            const char *s = v->value.b.data;
            if (s && s[0] != '\0') {
                // parse string to BCD decimal with target scale
                // helper declared later
                
                if (flintdb_decimal_from_string(s, scale, &d) == 0) {
                    row_decimal_set(r, i, d, e);
                    return;
                }
            } else {
                flintdb_variant_null_set(&r->array[i]);
                return;
            }
        } else if (v->type == VARIANT_DOUBLE) {
            char buf[64];
            snprintf(buf, sizeof(buf), "%.17g", v->value.f);
            
            if (flintdb_decimal_from_string(buf, scale, &d) == 0) {
                row_decimal_set(r, i, d, e);
                return;
            }
        } else if (v->type == VARIANT_INT64 || v->type == VARIANT_INT32 || v->type == VARIANT_INT16 || v->type == VARIANT_INT8 || v->type == VARIANT_UINT32 || v->type == VARIANT_UINT16 || v->type == VARIANT_UINT8) {
            char buf[64];
            int n = snprintf(buf, sizeof(buf), "%lld", (long long)v->value.i);
            (void)n;
            
            if (flintdb_decimal_from_string(buf, scale, &d) == 0) {
                row_decimal_set(r, i, d, e);
                return;
            }
        }
        break;
    }
    case VARIANT_BLOB:
    case VARIANT_OBJECT:
    default: {
        // not supported, fall through to copy
        break;
    }
    }

    // Fallback: direct copy to avoid data loss if conversion unsupported
    flintdb_variant_copy(&r->array[i], v);
}

static i8 row_is_nil(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || !r->array) {
        if (e)
            *e = "row_is_nil: row or array is NULL";
        return 1;
    }
    if (i >= r->length) {
        if (e)
            *e = "row_is_nil: index out of bounds";
        return 1;
    }
    if (e)
        *e = NULL;
    return flintdb_variant_is_null(&r->array[i]);
}

// setters (static helpers)
static void row_string_set(struct flintdb_row *rr, u16 i, const char *str, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "string_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "string_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    u32 len = (str ? (u32)strlen(str) : 0);
    flintdb_variant_string_set(&rr->array[i], str, len);
}

static void row_i64_set(struct flintdb_row *rr, u16 i, i64 val, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "i64_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "i64_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_i64_set(&rr->array[i], val);
}

static void row_f64_set(struct flintdb_row *rr, u16 i, f64 val, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "f64_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "f64_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_f64_set(&rr->array[i], val);
}

static void row_u8_set(struct flintdb_row *rr, u16 i, u8 val, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "u8_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "u8_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_u8_set(&rr->array[i], val);
}

static void row_i8_set(struct flintdb_row *rr, u16 i, i8 val, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "i8_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "i8_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_i8_set(&rr->array[i], val);
}

static void row_u16_set(struct flintdb_row *rr, u16 i, u16 val, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "u16_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "u16_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_u16_set(&rr->array[i], val);
}

static void row_i16_set(struct flintdb_row *rr, u16 i, i16 val, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "i16_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "i16_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_i16_set(&rr->array[i], val);
}

static void row_u32_set(struct flintdb_row *rr, u16 i, u32 val, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "u32_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "u32_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_u32_set(&rr->array[i], val);
}

static void row_i32_set(struct flintdb_row *rr, u16 i, i32 val, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "i32_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "i32_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_i32_set(&rr->array[i], val);
}

static void row_bytes_set(struct flintdb_row *rr, u16 i, const char *data, u32 length, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "bytes_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "bytes_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_bytes_set(&rr->array[i], data, length);
}

static void row_date_set(struct flintdb_row *rr, u16 i, time_t val, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "date_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "date_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_date_set(&rr->array[i], val);
}

static void row_time_set(struct flintdb_row *rr, u16 i, time_t val, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "time_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "time_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_time_set(&rr->array[i], val);
}

static void row_uuid_set(struct flintdb_row *rr, u16 i, const char *data, u32 length, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "uuid_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "uuid_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_uuid_set(&rr->array[i], data, length);
}

static void row_ipv6_set(struct flintdb_row *rr, u16 i, const char *data, u32 length, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "ipv6_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "ipv6_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_ipv6_set(&rr->array[i], data, length);
}

static void row_decimal_set(struct flintdb_row *rr, u16 i, struct flintdb_decimal  data, char **ee) {
    if (!rr || !rr->array) {
        if (ee)
            *ee = "decimal_set: row/array NULL";
        return;
    }
    if (i >= rr->length) {
        if (ee)
            *ee = "decimal_set: index out of bounds";
        return;
    }
    if (ee)
        *ee = NULL;
    flintdb_variant_decimal_set(&rr->array[i], data.sign, data.scale, data);
}

// getters
static const char *row_string_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_string_get: index out of bounds";
        return NULL;
    }
    if (e)
        *e = NULL;
    return flintdb_variant_string_get(&r->array[i]);
}

static i8 row_i8_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_i8_get: index out of bounds";
        return 0;
    }
    return flintdb_variant_i8_get(&r->array[i], e);
}

static u8 row_u8_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_u8_get: index out of bounds";
        return 0;
    }
    return flintdb_variant_u8_get(&r->array[i], e);
}

static i16 row_i16_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_i16_get: index out of bounds";
        return 0;
    }
    return flintdb_variant_i16_get(&r->array[i], e);
}

static u16 row_u16_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_u16_get: index out of bounds";
        return 0;
    }
    return flintdb_variant_u16_get(&r->array[i], e);
}

static i32 row_i32_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_i32_get: index out of bounds";
        return 0;
    }
    return variant_i32_get(&r->array[i], e);
}

static u32 row_u32_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_u32_get: index out of bounds";
        return 0;
    }
    return flintdb_variant_u32_get(&r->array[i], e);
}

static i64 row_i64_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_i64_get: index out of bounds";
        return 0;
    }
    return flintdb_variant_i64_get(&r->array[i], e);
}

static f64 row_f64_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_f64_get: index out of bounds";
        return 0.0;
    }
    return flintdb_variant_f64_get(&r->array[i], e);
}

static struct flintdb_decimal  row_decimal_get(const struct flintdb_row *r, u16 i, char **e) {
    struct flintdb_decimal  d = {0};
    if (!r || i >= r->length) {
        if (e)
            *e = "row_decimal_get: index out of bounds";
        return d;
    }
    return flintdb_variant_decimal_get(&r->array[i], e);
}

static const char *row_bytes_get(const struct flintdb_row *r, u16 i, u32 *length, char **e) {
    if (length)
        *length = 0;
    if (!r || i >= r->length) {
        if (e)
            *e = "row_bytes_get: index out of bounds";
        return NULL;
    }
    return flintdb_variant_bytes_get(&r->array[i], length, e);
}

static time_t row_date_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_date_get: index out of bounds";
        return (time_t)0;
    }
    return flintdb_variant_date_get(&r->array[i], e);
}

static time_t row_time_get(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || i >= r->length) {
        if (e)
            *e = "row_time_get: index out of bounds";
        return (time_t)0;
    }
    return flintdb_variant_time_get(&r->array[i], e);
}

static const char *row_uuid_get(const struct flintdb_row *r, u16 i, u32 *length, char **e) {
    if (length)
        *length = 0;
    if (!r || i >= r->length) {
        if (e)
            *e = "row_uuid_get: index out of bounds";
        return NULL;
    }
    return flintdb_variant_uuid_get(&r->array[i], length, e);
}

static const char *row_ipv6_get(const struct flintdb_row *r, u16 i, u32 *length, char **e) {
    if (length)
        *length = 0;
    if (!r || i >= r->length) {
        if (e)
            *e = "row_ipv6_get: index out of bounds";
        return NULL;
    }
    return flintdb_variant_ipv6_get(&r->array[i], length, e);
}

//
static i8 row_is_zero(const struct flintdb_row *r, u16 i, char **e) {
    if (!r || !r->array) {
        if (e)
            *e = "row_is_zero: row or array is NULL";
        return 1;
    }
    if (i >= r->length) {
        if (e)
            *e = "row_is_zero: index out of bounds";
        return 1;
    }
    if (e)
        *e = NULL;
    return (r->array[i].type == VARIANT_ZERO) ? 1 : 0;
}

static i8 row_equals(const struct flintdb_row *r, const struct flintdb_row *o) {
    if (r == o)
        return 1;
    if (!r || !o)
        return 0;
    if (r->length != o->length)
        return 0;
    for (int i = 0; i < r->length; i++) {
        if (flintdb_variant_compare(&r->array[i], &o->array[i]) != 0)
            return 0;
    }
    return 1;
}

static i8 row_compare(const struct flintdb_row *r, const struct flintdb_row *o, int (*cmp)(const struct flintdb_row *, const struct flintdb_row *)) {
    if (!r && !o)
        return 0;
    if (!r)
        return -1;
    if (!o)
        return 1;
    if (cmp)
        return (i8)cmp(r, o);
    // default lexicographic compare by variants
    int n = (r->length < o->length) ? r->length : o->length;
    for (int i = 0; i < n; i++) {
        int c = flintdb_variant_compare(&r->array[i], &o->array[i]);
        if (c != 0)
            return (i8)((c < 0) ? -1 : 1);
    }
    if (r->length == o->length)
        return 0;
    return (r->length < o->length) ? -1 : 1;
}

struct flintdb_row *row_copy(const struct flintdb_row *r, char **e) {
    if (!r)
        return NULL;
    // allocate new row
    struct flintdb_row *nr = (struct flintdb_row *)MALLOC(sizeof(struct flintdb_row));
    if (!nr) {
        if (e)
            *e = "row_copy: out of memory";
        return NULL;
    }
    // initialize using row_init semantics
    memset(nr, 0, sizeof(struct flintdb_row));
    row_init(r->meta, nr, e);
    nr->rowid = r->rowid;
    // deep-copy variants
    for (int i = 0; i < r->length && i < nr->length; i++) {
        flintdb_variant_copy(&nr->array[i], &r->array[i]);
    }
    return nr;
}

i8 row_validate(const struct flintdb_row *r, char **e) {
    if (!r || !r->meta) {
        if (e)
            *e = "row_validate: row or meta is NULL";
        return 0;
    }
    // validate each column based on meta
    for (int i = 0; i < r->length; i++) {
        const struct flintdb_column *col = &r->meta->columns.a[i];
        const struct flintdb_variant *v = &r->array[i];
        if (col->nullspec == SPEC_NOT_NULL && flintdb_variant_is_null(v)) {
            return 0;
        }
    }
    return 1;
}

static void row_free(struct flintdb_row *r) {
    if (!r)
        return;

    // Use reference counting - decrement and free only if last reference
    int old_count = __atomic_sub_fetch(&r->refcount, 1, __ATOMIC_SEQ_CST);
    if (old_count > 0) {
        // Still has references, don't free yet
        return;
    }
    
    // Last reference, free the row
    for (int i = 0; i < r->length; i++) {
        flintdb_variant_free(&r->array[i]);
    }
    FREE(r->array);
    FREE(r);
}

/**
 * Increment reference count and return the row
 * Thread-safe using atomic operations
 * If custom retain function is bound, delegates to it
 */
static struct flintdb_row *row_retain(struct flintdb_row *r) {
    if (!r)
        return NULL;
    // Avoid infinite recursion: if retain points to us, just increment
    if (r->retain && r->retain != &row_retain) {
        return r->retain(r);
    }
    __atomic_add_fetch(&r->refcount, 1, __ATOMIC_SEQ_CST);
    return r;
}



static inline void row_init(struct flintdb_meta *meta, struct flintdb_row *r, char **e) {
    if (!r || !meta) {
        if (e)
            *e = row_error_set(r, "row_init: row or meta is NULL");
        return;
    }
    memset(r, 0, sizeof(struct flintdb_row));

    r->meta = meta;
    r->rowid = -1;
    r->refcount = 1; // Initialize reference count
    r->length = (meta && meta->columns.length > 0) ? meta->columns.length : 0;
    if (r->length > 0) {
        r->array = (struct flintdb_variant *)CALLOC((size_t)r->length, sizeof(struct flintdb_variant));
        if (!r->array) {
            if (e)
                *e = row_error_set(r, "row_init: out of memory");
            r->length = 0;
            return;
        }
        for (int i = 0; i < r->length; i++)
            flintdb_variant_init(&r->array[i]);
        // initialize defaults based on meta.columns value string (if any)
        for (int i = 0; i < r->length; i++) {
            const char *defv = meta->columns.a[i].value;
            if (defv && defv[0] != '\0') {
                // construct a temporary STRING variant and cast into target
                struct flintdb_variant tmp;
                flintdb_variant_init(&tmp);
                // Use non-owning slice to avoid allocation; row_set will copy/convert as needed
                flintdb_variant_string_ref_set(&tmp, defv, (u32)strlen(defv), VARIANT_SFLAG_NULL_TERMINATED);
                row_set(r, i, &tmp, e);
                flintdb_variant_free(&tmp);
            }
        }
    } else {
        r->array = NULL;
    }

    // wire function pointers
    r->free = &row_free;
    r->retain = &row_retain;
    r->id = &row_id;
    r->get = &row_get;
    r->set = &row_set;
    r->is_nil = &row_is_nil;

    // setters
    r->string_set = &row_string_set;
    r->i64_set = &row_i64_set;
    r->f64_set = &row_f64_set;
    r->u8_set = &row_u8_set;
    r->i8_set = &row_i8_set;
    r->u16_set = &row_u16_set;
    r->i16_set = &row_i16_set;
    r->u32_set = &row_u32_set;
    r->i32_set = &row_i32_set;
    r->bytes_set = &row_bytes_set;
    r->date_set = &row_date_set;
    r->time_set = &row_time_set;
    r->uuid_set = &row_uuid_set;
    r->ipv6_set = &row_ipv6_set;
    r->decimal_set = &row_decimal_set;

    // getters
    r->string_get = &row_string_get;
    r->i8_get = &row_i8_get;
    r->u8_get = &row_u8_get;
    r->i16_get = &row_i16_get;
    r->u16_get = &row_u16_get;
    r->i32_get = &row_i32_get;
    r->u32_get = &row_u32_get;
    r->i64_get = &row_i64_get;
    r->f64_get = &row_f64_get;
    r->decimal_get = &row_decimal_get;
    r->bytes_get = &row_bytes_get;
    r->date_get = &row_date_get;
    r->time_get = &row_time_get;
    r->uuid_get = &row_uuid_get;
    r->ipv6_get = &row_ipv6_get;

    // others
    r->is_zero = &row_is_zero;
    r->equals = &row_equals;
    r->compare = &row_compare;
    r->copy = &row_copy;

    r->validate = &row_validate;
}

struct flintdb_row *flintdb_row_new(struct flintdb_meta *meta, char **e) {
    struct flintdb_row *r = (struct flintdb_row *)CALLOC(1, sizeof(struct flintdb_row));
    row_init(meta, r, e);
    return r;
}

struct flintdb_row *flintdb_row_from_argv(struct flintdb_meta *meta, u16 argc, const char **argv, char **e) {
    struct flintdb_row *r = NULL;
    if (!meta)
        THROW(e, "row_from_argv: meta is NULL");

    if ((argc & 1) == 1)
        THROW(e, "argc must be an even number: %d", argc);

    r = (struct flintdb_row *)CALLOC(1, sizeof(struct flintdb_row));
    if (!r)
        THROW(e, "row_from_argv: OOM");
    row_init(meta, r, e);

    for (int i = 0; i < argc; i += 2) {
        const char *k = argv[i];
        const char *v = (i + 1 < argc) ? argv[i + 1] : NULL;
        if (!k)
            continue;

        // Special: rowid
        if (strcasecmp(k, "rowid") == 0) {
            if (!is_nil_token(v, meta)) {
                i64 rid = -1;
                if (parse_i64(v, strlen(v), &rid) != 0)
                    THROW(e, "invalid rowid: %s", v ? v : "(null)");
                r->rowid = rid;
            }
            continue;
        }

        int col = flintdb_column_at(meta, k);
        if (col < 0)
            THROW(e, "unknown column: %s", k);
        if (col >= r->length)
            THROW(e, "column index out of range: %d", col);

        if (is_nil_token(v, meta)) {
            flintdb_variant_null_set(&r->array[col]);
            continue;
        }
        // Set as STRING then cast via row_set to target type
        struct flintdb_variant tmp;
        flintdb_variant_init(&tmp);
        u32 L = (u32)strlen(v);
        // Use non-owning reference to avoid allocation; row_set will convert/copy
        flintdb_variant_string_ref_set(&tmp, v, L, VARIANT_SFLAG_NULL_TERMINATED);
        r->set(r, col, &tmp, e);
        flintdb_variant_free(&tmp);
        if (e && *e)
            THROW_S(e);
    }

    return r;

EXCEPTION:
    if (r)
        r->free(r);
    return NULL;
}

// Safely cast a row to a new meta structure
struct flintdb_row *flintdb_row_cast(struct flintdb_row *src, struct flintdb_meta *meta, char **e) {
    struct flintdb_row *r = NULL;

    if (!src)
        THROW(e, "src is NULL");
    if (!meta)
        THROW(e, "meta is NULL");
    if (!src->meta)
        THROW(e, "src->meta is NULL");

    // Create a new row with the target meta
    r = (struct flintdb_row *)CALLOC(1, sizeof(struct flintdb_row));
    if (!r)
        THROW(e, "Out of memory");

    row_init(meta, r, e);
    if (e && *e)
        goto EXCEPTION;

    // do not copy rowid, leave as -1 or default
    r->rowid = -1L; // bplustree.c NOT_FOUND

    // For each column in the target meta, try to find a matching column in the source
    for (int dst_col = 0; dst_col < meta->columns.length; dst_col++) {
        const char *dst_name = meta->columns.a[dst_col].name;

        // Find matching column in source by name
        int src_col = -1;
        for (int i = 0; i < src->meta->columns.length; i++) {
            if (strcasecmp(src->meta->columns.a[i].name, dst_name) == 0) {
                src_col = i;
                break;
            }
        }

        if (src_col < 0) {
            // Column doesn't exist in source, leave as default (NIL or default value from meta)
            continue;
        }

        if (src_col >= src->length) {
            // Out of bounds in source array
            continue;
        }

        // Get the source variant
        struct flintdb_variant *src_var = &src->array[src_col];

        // If source is NIL, keep target as NIL
        if (flintdb_variant_is_null(src_var)) {
            continue;
        }

        // Set the value using row_set which handles type casting
        r->set(r, dst_col, src_var, e);
        if (e && *e) {
            // If there's an error during casting, log but continue
            // (Optional: you can choose to fail completely or skip this column)
            // For safety, we'll continue and let the column remain NIL
            if (*e) {
                FREE(*e);
                *e = NULL;
            }
        }
    }

    return r;

EXCEPTION:
    if (r)
        r->free(r);
    return NULL;
}

// Optimized version: Reuse existing destination row to avoid CALLOC/FREE overhead
// This is critical for bulk operations (e.g., importing millions of rows)
// where repeated allocation/deallocation becomes a major bottleneck.
//
// Usage pattern:
//   struct flintdb_row *dst = flintdb_row_new(&target_meta, &e);  // Allocate once
//   for (many iterations) {
//       flintdb_row_cast_reuse(src_row, dst, &e);         // Reuse dst, only update values
//       table->apply(table, dst, &e);
//   }
//   dst->free(dst);  // Free once
//
// Performance: In bulk import tests (6M rows), this approach showed:
// - 25% faster than row_cast (32s → 24s)
// - 31% higher throughput (186K → 244K ops/s)
// - Eliminates malloc/free overhead entirely in the hot path
int flintdb_row_cast_reuse(const struct flintdb_row *src, struct flintdb_row *dst, char **e) {
    if (!src)
        THROW(e, "src is NULL");
    if (!dst)
        THROW(e, "dst is NULL");
    if (!src->meta)
        THROW(e, "src->meta is NULL");
    if (!dst->meta)
        THROW(e, "dst->meta is NULL");

    // Reset rowid for insert operations (caller can override if needed)
    dst->rowid = -1L;

    // Fast path: If schemas have same column count and order, use direct copy
    // This is the common case in bulk imports where source and dest have identical schemas
    if (src->meta->columns.length == dst->meta->columns.length) {
        int schemas_match = 1;
        for (int i = 0; i < src->meta->columns.length; i++) {
            if (strcasecmp(src->meta->columns.a[i].name, dst->meta->columns.a[i].name) != 0) {
                schemas_match = 0;
                break;
            }
        }

        if (schemas_match) {
            // Schemas match: check if types also match for even faster path
            int types_match = 1;
            for (int i = 0; i < src->meta->columns.length; i++) {
                if (src->meta->columns.a[i].type != dst->meta->columns.a[i].type) {
                    types_match = 0;
                    break;
                }
            }

            if (types_match) {
                // Ultra-fast path: schemas AND types match - direct variant copy!
                for (int col = 0; col < dst->meta->columns.length && col < src->length; col++) {
                    flintdb_variant_copy(&dst->array[col], &src->array[col]);
                }
                return 0;
            }

            // Schemas match but types differ: use row->set for type casting
            for (int col = 0; col < dst->meta->columns.length && col < src->length; col++) {
                dst->set(dst, col, &src->array[col], e);
                if (e && *e) {
                    flintdb_variant_null_set(&dst->array[col]);
                    if (*e) {
                        FREE(*e);
                        *e = NULL;
                    }
                }
            }
            return 0;
        }
    }

    // Slow path: schemas differ, need to match by column name
    // Use flintdb_column_at() for O(1) lookup instead of nested loops
    for (int dst_col = 0; dst_col < dst->meta->columns.length; dst_col++) {
        const char *dst_name = dst->meta->columns.a[dst_col].name;

        // Fast O(1) name lookup using hashmap (column_at)
        int src_col = flintdb_column_at(src->meta, dst_name);

        if (src_col < 0 || src_col >= src->length) {
            // Column doesn't exist in source or out of bounds, set to NIL
            flintdb_variant_null_set(&dst->array[dst_col]);
            continue;
        }

        // Get the source variant
        struct flintdb_variant *src_var = &src->array[src_col];

        // If source is NIL, set destination to NIL
        if (flintdb_variant_is_null(src_var)) {
            flintdb_variant_null_set(&dst->array[dst_col]);
            continue;
        }

        // Set the value using row_set which handles type casting
        dst->set(dst, dst_col, src_var, e);
        if (e && *e) {
            // On error, set to NIL and continue (or you can choose to fail)
            flintdb_variant_null_set(&dst->array[dst_col]);
            if (*e) {
                FREE(*e);
                *e = NULL;
            }
        }
    }

    return 0;

EXCEPTION:
    return -1;
}

// Hashing function for a variant
static inline u32 variant_hash32(const struct flintdb_variant *v, u32 seed) {
    // Incorporate type first to avoid cross-type collisions
    u32 h = hash32_from_bytes(&v->type, sizeof(v->type), seed);
    if (!v) return hash_fmix32(h);

    switch (v->type) {
    case VARIANT_NULL:
    case VARIANT_ZERO:
        return hash_fmix32(h ^ 0xA5A5A5A5u);
    case VARIANT_INT8: case VARIANT_UINT8: {
        u8 x = (u8)(v->value.i & 0xFF);
        return hash32_from_bytes(&x, 1, h);
    }
    case VARIANT_INT16: case VARIANT_UINT16: {
        u16 x = (u16)(v->value.i & 0xFFFF);
        // little-endian stable layout
        u8 b[2] = { (u8)(x & 0xFF), (u8)((x >> 8) & 0xFF) };
        return hash32_from_bytes(b, 2, h);
    }
    case VARIANT_INT32: case VARIANT_UINT32: {
        u32 x = (u32)(v->value.i & 0xFFFFFFFFu);
        u8 b[4] = { (u8)(x & 0xFF), (u8)((x >> 8) & 0xFF), (u8)((x >> 16) & 0xFF), (u8)((x >> 24) & 0xFF) };
        return hash32_from_bytes(b, 4, h);
    }
    case VARIANT_INT64: {
        u64 x = (u64)v->value.i;
        u8 b[8];
        for (int i = 0; i < 8; i++) { b[i] = (u8)(x & 0xFFu); x >>= 8; }
        return hash32_from_bytes(b, 8, h);
    }
    case VARIANT_DOUBLE: {
        // Normalize -0.0 to 0.0 and canonicalize NaN
        double dv = v->value.f;
        if (dv == 0.0) dv = 0.0; // squash -0.0
        if (isnan(dv)) dv = NAN;
        u64 bits = 0;
        memcpy(&bits, &dv, sizeof(bits));
        u8 b[8];
        for (int i = 0; i < 8; i++) { b[i] = (u8)(bits & 0xFFu); bits >>= 8; }
        return hash32_from_bytes(b, 8, h);
    }
    case VARIANT_FLOAT: {
        float fv = (float)v->value.f;
        if (fv == 0.0f) fv = 0.0f;
        if (isnan(fv)) fv = NAN;
        u32 bits = 0; memcpy(&bits, &fv, sizeof(bits));
        u8 b[4] = { (u8)(bits & 0xFF), (u8)((bits >> 8) & 0xFF), (u8)((bits >> 16) & 0xFF), (u8)((bits >> 24) & 0xFF) };
        return hash32_from_bytes(b, 4, h);
    }
    case VARIANT_STRING: {
        const char *s = v->value.b.data;
        u32 L = v->value.b.length;
        return hash32_from_bytes(s, L, h);
    }
    case VARIANT_BYTES:
    case VARIANT_UUID:
    case VARIANT_IPV6: {
        const char *p = v->value.b.data;
        u32 L = v->value.b.length;
        return hash32_from_bytes(p, L, h);
    }
    case VARIANT_DECIMAL: {
        // Mix sign, scale, length and the BCD data bytes
        u32 t = (((u32)v->value.d.sign) | ((u32)v->value.d.scale << 8)) ^ (u32)v->value.d.length;
        h = hash32_from_bytes(&t, sizeof(t), h);
        return hash32_from_bytes(v->value.d.data, v->value.d.length, h);
    }
    case VARIANT_DATE:
    case VARIANT_TIME: {
        // Hash time_t as 64-bit value for stability
        i64 tt = (i64)v->value.t;
        u64 x = (u64)tt;
        u8 b[8];
        for (int i = 0; i < 8; i++) { b[i] = (u8)(x & 0xFFu); x >>= 8; }
        return hash32_from_bytes(b, 8, h);
    }
    default:
        return hash_fmix32(h);
    }
}

static inline u64 variant_hash64(const struct flintdb_variant *v, u64 seed) {
    // Include type
    u64 h = hash64_from_bytes(&v->type, sizeof(v->type), seed);
    if (!v) return hash_fmix64(h);
    switch (v->type) {
    case VARIANT_NULL:
    case VARIANT_ZERO:
        return hash_fmix64(h ^ 0xA5A5A5A5A5A5A5A5ULL);
    case VARIANT_INT8: case VARIANT_UINT8: {
        u8 x = (u8)(v->value.i & 0xFF);
        return hash64_from_bytes(&x, 1, h);
    }
    case VARIANT_INT16: case VARIANT_UINT16: {
        u16 x = (u16)(v->value.i & 0xFFFF);
        u8 b[2] = { (u8)(x & 0xFF), (u8)((x >> 8) & 0xFF) };
        return hash64_from_bytes(b, 2, h);
    }
    case VARIANT_INT32: case VARIANT_UINT32: {
        u32 x = (u32)(v->value.i & 0xFFFFFFFFu);
        u8 b[4] = { (u8)(x & 0xFF), (u8)((x >> 8) & 0xFF), (u8)((x >> 16) & 0xFF), (u8)((x >> 24) & 0xFF) };
        return hash64_from_bytes(b, 4, h);
    }
    case VARIANT_INT64: {
        u64 x = (u64)v->value.i;
        u8 b[8];
        for (int i = 0; i < 8; i++) { b[i] = (u8)(x & 0xFFu); x >>= 8; }
        return hash64_from_bytes(b, 8, h);
    }
    case VARIANT_DOUBLE: {
        double dv = v->value.f;
        if (dv == 0.0) dv = 0.0;
        if (isnan(dv)) dv = NAN;
        u64 bits = 0; memcpy(&bits, &dv, sizeof(bits));
        u8 b[8]; for (int i = 0; i < 8; i++) { b[i] = (u8)(bits & 0xFFu); bits >>= 8; }
        return hash64_from_bytes(b, 8, h);
    }
    case VARIANT_FLOAT: {
        float fv = (float)v->value.f;
        if (fv == 0.0f) fv = 0.0f;
        if (isnan(fv)) fv = NAN;
        u32 bits = 0; memcpy(&bits, &fv, sizeof(bits));
        u8 b[4] = { (u8)(bits & 0xFF), (u8)((bits >> 8) & 0xFF), (u8)((bits >> 16) & 0xFF), (u8)((bits >> 24) & 0xFF) };
        return hash64_from_bytes(b, 4, h);
    }
    case VARIANT_STRING: {
        const char *s = v->value.b.data;
        u32 L = v->value.b.length;
        return hash64_from_bytes(s, L, h);
    }
    case VARIANT_BYTES:
    case VARIANT_UUID:
    case VARIANT_IPV6: {
        const char *p = v->value.b.data;
        u32 L = v->value.b.length;
        return hash64_from_bytes(p, L, h);
    }
    case VARIANT_DECIMAL: {
        u32 t = (((u32)v->value.d.sign) | ((u32)v->value.d.scale << 8)) ^ (u32)v->value.d.length;
        h = hash64_from_bytes(&t, sizeof(t), h);
        return hash64_from_bytes(v->value.d.data, v->value.d.length, h);
    }
    case VARIANT_DATE:
    case VARIANT_TIME: {
        i64 tt = (i64)v->value.t;
        u64 x = (u64)tt;
        u8 b[8]; for (int i = 0; i < 8; i++) { b[i] = (u8)(x & 0xFFu); x >>= 8; }
        return hash64_from_bytes(b, 8, h);
    }
    default:
        return hash_fmix64(h);
    }
}

u32 row_hash32(const struct flintdb_row *r, u32 seed) {
    assert(r != NULL);
    const struct flintdb_meta *m = r->meta;
    assert(m != NULL);
    u32 h = hash_fmix32(seed ^ (u32)m->columns.length ^ 0x9E3779B9u);
    for (int i = 0; i < m->columns.length; i++) {
        struct flintdb_variant *v = &r->array[i];
        // Slightly perturb seed per column index
        u32 col_seed = h + (u32)i * 0x9E3779B9u;
        h ^= variant_hash32(v, col_seed);
        // Mix between columns
        h = (h << 13) | (h >> 19);
        h *= 0x85ebca6bU;
    }
    return hash_fmix32(h);
}

u64 row_hash64(const struct flintdb_row *r, u64 seed) {
    assert(r != NULL);
    const struct flintdb_meta *m = r->meta;
    assert(m != NULL);
    u64 h = hash_fmix64(seed ^ (u64)m->columns.length ^ 0x9E3779B97F4A7C15ULL);
    for (int i = 0; i < m->columns.length; i++) {
        struct flintdb_variant *v = &r->array[i];
        u64 col_seed = h + ((u64)i * 0x9E3779B97F4A7C15ULL);
        h ^= variant_hash64(v, col_seed);
        h = (h << 27) | (h >> 37);
        h *= 0x9ddfea08eb382d69ULL;
    }
    return hash_fmix64(h);
}

// Formatter (stub implementations for now)
// ---- Formatter implementation (text TSV/CSV and binary) ----

// small helpers for formatter
static inline int is_varlen(enum flintdb_variant_type  t) {
    return (t == VARIANT_STRING || t == VARIANT_DECIMAL || t == VARIANT_BYTES || t == VARIANT_BLOB || t == VARIANT_OBJECT);
}

// --- DATE helpers ---
// Convert civil date (Y-M-D) to days since Unix epoch (1970-01-01) using a branchless algorithm.
static inline i64 days_from_civil_fast(int y, int m, int d) {
    y -= (m <= 2);
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);                                              // [0, 399]
    const unsigned doy = (153u * (unsigned)(m + (m > 2 ? -3 : 9)) + 2u) / 5u + (unsigned)d - 1u; // [0, 365]
    const unsigned doe = yoe * 365u + yoe / 4u - yoe / 100u + doy;                               // [0, 146096]
    return (i64)(era * 146097 + (int)doe - 719468);
}

// --- DECIMAL helpers ---
// Build BCD from signed 64-bit unscaled integer
static inline void decimal_from_unscaled_i64(i64 x, int scale, struct flintdb_decimal  *out) {
    struct flintdb_decimal  d = {0};
    d.scale = (u8)((scale > 0) ? scale : 0);
    if (x < 0) {
        d.sign = 1;
        x = -x;
    }
    // Collect digits in reverse
    u8 rev[32];
    int nd = 0;
    if (x == 0)
        rev[nd++] = 0;
    while (x > 0 && nd < (int)sizeof(rev)) {
        rev[nd++] = (u8)(x % 10);
        x /= 10;
    }
    int bi = 0;
    int msd = nd - 1;
    if ((nd & 1) != 0) {
        u8 dgt = (msd >= 0) ? rev[msd--] : 0;
        d.data[bi++] = (u8)((0u << 4) | (dgt & 0x0F));
    }
    while (msd >= 0 && bi < (int)sizeof(d.data)) {
        u8 hi = rev[msd--] & 0x0F;
        u8 lo = (msd >= 0) ? (rev[msd--] & 0x0F) : 0;
        d.data[bi++] = (u8)((hi << 4) | lo);
    }
    d.length = (u32)bi;
    *out = d;
}

// Divide little-endian magnitude by 10 in-place; return remainder. 'a' length is n bytes.
static inline u8 le_div10(u8 *a, u32 n) {
    unsigned int carry = 0; // < 10 always
    // Process from MSB to LSB (little-endian: MSB is at highest index)
    for (int i = (int)n - 1; i >= 0; i--) {
        unsigned int cur = (carry << 8) | a[i];
        a[i] = (u8)(cur / 10);
        carry = cur % 10;
    }
    return (u8)carry;
}

// Convert two's-complement little-endian bytes to decimal BCD with given scale (supports up to 32 bytes input)
static inline void decimal_from_twos_bytes(const u8 *p, u32 n, int scale, struct flintdb_decimal  *out) {
    if (!p || n == 0) {
        memset(out, 0, sizeof(*out));
        out->scale = (u8)((scale > 0) ? scale : 0);
        return;
    }
    // Clamp input size to a sane limit
    if (n > 32)
        n = 32;
    // Determine sign and build magnitude (little-endian: MSB is last byte)
    int neg = (p[n - 1] & 0x80) ? 1 : 0;
    u8 mag[32];
    simd_memcpy(mag, p, n);
    if (neg) {
        // two's complement -> magnitude: invert and add 1
        for (u32 i = 0; i < n; i++)
            mag[i] = (u8)(~mag[i]);
        // add 1 starting from LSB (little-endian => first index)
        for (u32 i = 0; i < n; i++) {
            unsigned int v = (unsigned int)mag[i] + 1u;
            mag[i] = (u8)(v & 0xFFu);
            if ((v & 0x100u) == 0)
                break; // no carry
        }
    }
    // Strip trailing zeros (little-endian: high bytes are at the end)
    u32 end = n;
    while (end > 1 && mag[end - 1] == 0)
        end--;
    // Collect decimal digits by repeated div10
    u8 rev[64]; // enough to hold many digits; we'll clamp to 32 when packing
    int nd = 0;
    if (end == 1 && mag[0] == 0) {
        rev[nd++] = 0;
    } else {
        // Work on the effective slice [0, end)
        u8 *start = mag;
        u32 len = end;
        // Loop until magnitude becomes zero
        int nonzero = 1;
        while (nonzero && nd < (int)sizeof(rev)) {
            u8 r = le_div10(start, len);
            rev[nd++] = r;
            // update end if highest byte became zero
            while (len > 1 && start[len - 1] == 0) {
                len--;
            }
            nonzero = !(len == 1 && start[0] == 0);
        }
        // If still non-zero but ran out of rev size, continue draining without storing (to avoid infinite loop)
        while (!(len == 1 && start[0] == 0)) {
            (void)le_div10(start, len);
            while (len > 1 && start[len - 1] == 0) {
                len--;
            }
        }
        if (nd == 0)
            rev[nd++] = 0;
    }
    struct flintdb_decimal  d = {0};
    d.sign = neg ? 1 : 0;
    d.scale = (u8)((scale > 0) ? scale : 0);
    // Pack BCD MSB-first with possible high-nibble pad when odd number of digits
    int bi = 0;
    int msd = nd - 1; // most significant digit index in rev
    // Clamp to at most 32 digits (16 bytes)
    int maxDigits = 32;
    if (nd > maxDigits) {
        // Drop most significant excess digits
        msd = maxDigits - 1;
    }
    int used = (msd + 1);
    if ((used & 1) != 0) {
        u8 dgt = (msd >= 0) ? rev[msd--] : 0;
        d.data[bi++] = (u8)((0u << 4) | (dgt & 0x0F));
    }
    while (msd >= 0 && bi < (int)sizeof(d.data)) {
        u8 hi = rev[msd--] & 0x0F;
        u8 lo = (msd >= 0) ? (rev[msd--] & 0x0F) : 0;
        d.data[bi++] = (u8)((hi << 4) | lo);
    }
    d.length = (u32)bi;
    *out = d;
}

static inline int col_fixed_bytes(enum flintdb_variant_type  t) {
    switch (t) {
    case VARIANT_INT8:
    case VARIANT_UINT8:
        return 1;
    case VARIANT_INT16:
    case VARIANT_UINT16:
        return 2;
    case VARIANT_INT32:
    case VARIANT_UINT32:
        return 4;
    case VARIANT_INT64:
        return 8;
    case VARIANT_DOUBLE:
        return 8;
    case VARIANT_FLOAT:
        return 4;
    case VARIANT_DATE:
        return 3; // uint24 days from epoch
    case VARIANT_TIME:
        return 8; // ms since epoch
    case VARIANT_UUID:
    case VARIANT_IPV6:
        return 16;
    default:
        return 0; // var-len or unsupported
    }
}

static inline void buffer_ensure(struct buffer *b, u32 extra) {
    if (!b || !b->realloc)
        return;
    if (b->position + extra <= b->capacity)
        return;
    u32 need = b->position + extra;
    // grow 1.5x to reduce reallocs
    u32 cap = b->capacity ? b->capacity : 64;
    while (cap < need) {
        cap = cap + (cap >> 1);
        if (cap < 256)
            cap = cap * 2; // speed up early growth
        if (cap < need)
            continue;
    }
    b->realloc(b, (i32)cap);
}

static inline void buffer_put_bytes(struct buffer *b, const char *p, u32 len, char **e) {
    if (len == 0)
        return;
    buffer_ensure(b, len);
    b->array_put(b, (char *)p, len, e);
}

static inline void buffer_put_zero(struct buffer *b, u32 len, char **e) {
    if (len == 0)
        return;
    buffer_ensure(b, len);
    // write in chunks
    #define ZERO_CHUNK_SIZE 64
    char zero[ZERO_CHUNK_SIZE];
    memset(zero, 0, ZERO_CHUNK_SIZE);
    while (len > 0) {
        u32 w = (len > ZERO_CHUNK_SIZE) ? ZERO_CHUNK_SIZE : len;
        b->array_put(b, zero, w, e);
        len -= w;
    }
    #undef ZERO_CHUNK_SIZE
}

// --- Binary encode/decode ---

static inline void put_u24(struct buffer *b, u32 v, char **e) {
    buffer_ensure(b, 3);
    b->i8_put(b, (char)((v >> 16) & 0xFF), e);
    b->i8_put(b, (char)((v >> 8) & 0xFF), e);
    b->i8_put(b, (char)(v & 0xFF), e);
}

static inline u32 get_u24(struct buffer *b, char **e) {
    u32 b1 = (u8)b->i8_get(b, e);
    u32 b2 = (u8)b->i8_get(b, e);
    u32 b3 = (u8)b->i8_get(b, e);
    return (b1 << 16) | (b2 << 8) | b3;
}

HOT_PATH
static int bin_encode(struct formatter *me, struct flintdb_row *r, struct buffer *out, char **e) {
    if (!r || !r->meta || !out)
        THROW(e, "bin_encode: invalid args");

    const struct flintdb_meta *m = me->meta;
    // Rough capacity estimate
    u32 estimate = 2; // column count
    for (int i = 0; i < m->columns.length; i++) {
        const struct flintdb_column *c = &m->columns.a[i];
        estimate += 2; // type tag
        if (is_varlen(c->type))
            // Only length + payload for var-len types; no padding in BIN format
            estimate += 2 + (u32)c->bytes;
        else
            estimate += (u32)col_fixed_bytes(c->type);
    }
    out->clear(out);
    buffer_ensure(out, estimate);
    out->i16_put(out, (i16)m->columns.length, e);
    for (int i = 0; i < m->columns.length && i < r->length; i++) {
        const struct flintdb_column *c = &m->columns.a[i];
        struct flintdb_variant *v = &r->array[i];
        int isNil = flintdb_variant_is_null(v);
        // Java BIN formatter writes type=0 (NIL) for nulls and no payload. Align to that.
        if (isNil) {
            out->i16_put(out, (i16)VARIANT_NULL, e);
            continue;
        }
        // Non-null: write the actual type tag
        out->i16_put(out, (i16)c->type, e);
        if (is_varlen(c->type)) {
            switch (c->type) {
            case VARIANT_STRING: {
                // Fast path: read string from variant buffer directly
                const char *s = NULL;
                u32 sl = 0;
                if (v->type == VARIANT_STRING && v->value.b.data) {
                    s = v->value.b.data;
                    sl = v->value.b.length;
                } else {
                    s = r->string_get(r, i, e);
                    sl = s ? (u32)strlen(s) : 0;
                }
                u32 n = sl > (u32)c->bytes ? (u32)c->bytes : sl;
                out->i16_put(out, (i16)n, e);
                buffer_put_bytes(out, s, n, e);
                break;
            }
            case VARIANT_DECIMAL: {
                // Java BIN formatter encodes DECIMAL as unscaled two's-complement integer bytes
                // with scale provided by column.precision. Compute unscaled from internal BCD directly
                // while ignoring the possible padding nibble for odd digit counts.
                struct flintdb_decimal  d = r->decimal_get(r, i, e);
                
                // Optimized BCD to i64 conversion
                int skipLeadingHi = (d.length > 0 && ((unsigned char)d.data[0] >> 4) == 0);
                i64 unscaled = row_bcd_to_i64_opt((const u8 *)d.data, d.length, skipLeadingHi);
                
                if (d.sign)
                    unscaled = -unscaled;

                // Optimized i64 to bytes conversion
                unsigned char tmp[8];
                int start = 0;
                int blen = row_i64_to_bytes_opt(unscaled, tmp, &start);
                
                out->i16_put(out, (i16)blen, e);
                buffer_put_bytes(out, (const char *)(tmp + start), (u32)blen, e);
                break;
            }
            case VARIANT_BYTES:
            case VARIANT_BLOB:
            case VARIANT_OBJECT: {
                // Fast path: read bytes from variant buffer directly
                u32 bl = 0;
                const char *bp = NULL;
                if (v->type == VARIANT_BYTES && v->value.b.data) {
                    bp = v->value.b.data;
                    bl = v->value.b.length;
                } else {
                    bp = r->bytes_get(r, i, &bl, e);
                }
                u32 n = bl > (u32)c->bytes ? (u32)c->bytes : bl;
                out->i16_put(out, (i16)n, e);
                if (n)
                    buffer_put_bytes(out, bp, n, e);
                break;
            }
            default: {
                // shouldn't happen, but pad
                out->i16_put(out, 0, e);
                break;
            }
            }
            continue;
        }
        // fixed-size types
        switch (c->type) {
        case VARIANT_INT8:
            if (v->type == VARIANT_INT8)
                out->i8_put(out, (char)v->value.i, e);
            else
                out->i8_put(out, (char)r->i8_get(r, i, e), e);
            break;
        case VARIANT_UINT8:
            if (v->type == VARIANT_UINT8)
                out->i8_put(out, (char)(u8)v->value.i, e);
            else
                out->i8_put(out, (char)r->u8_get(r, i, e), e);
            break;
        case VARIANT_INT16:
            if (v->type == VARIANT_INT16)
                out->i16_put(out, (i16)v->value.i, e);
            else
                out->i16_put(out, (i16)r->i16_get(r, i, e), e);
            break;
        case VARIANT_UINT16:
            if (v->type == VARIANT_UINT16)
                out->i16_put(out, (i16)(u16)v->value.i, e);
            else
                out->i16_put(out, (i16)r->u16_get(r, i, e), e);
            break;
        case VARIANT_INT32:
            if (v->type == VARIANT_INT32)
                out->i32_put(out, (i32)v->value.i, e);
            else
                out->i32_put(out, (i32)r->i32_get(r, i, e), e);
            break;
        case VARIANT_UINT32:
            if (v->type == VARIANT_UINT32)
                out->i32_put(out, (i32)(u32)v->value.i, e);
            else
                out->i32_put(out, (i32)r->u32_get(r, i, e), e);
            break;
        case VARIANT_INT64:
            if (v->type == VARIANT_INT64)
                out->i64_put(out, (i64)v->value.i, e);
            else
                out->i64_put(out, (i64)r->i64_get(r, i, e), e);
            break;
        case VARIANT_DOUBLE:
            if (v->type == VARIANT_DOUBLE)
                out->f64_put(out, (f64)v->value.f, e);
            else
                out->f64_put(out, (f64)r->f64_get(r, i, e), e);
            break;
        case VARIANT_FLOAT: {
            float fv = (float)((v->type == VARIANT_DOUBLE) ? v->value.f : r->f64_get(r, i, e));
            u32 bits = 0;
            memcpy(&bits, &fv, sizeof(float));
            out->i32_put(out, (i32)bits, e);
            break;
        }
        case VARIANT_DATE: {
            // Encode as Java-compatible 24-bit Y/M/D packed layout
            time_t tt = r->date_get(r, i, e);
            
            // Fast conversion without localtime_r system call
            int year, month, day;
            row_fast_time_to_date(tt, &year, &month, &day);
            
            u32 v = (u32)(((year) << 9) | (month << 5) | (day & 0x1F));
            put_u24(out, v, e);
            break;
        }
        case VARIANT_TIME: {
            time_t tt = r->time_get(r, i, e);
            i64 ms = (i64)tt * 1000; // seconds to ms
            out->i64_put(out, ms, e);
            break;
        }
        case VARIANT_UUID:
        case VARIANT_IPV6: {
            u32 bl = 0;
            const char *bp = (c->type == VARIANT_UUID) ? r->uuid_get(r, i, &bl, e) : r->ipv6_get(r, i, &bl, e);
            if (bl >= 16)
                buffer_put_bytes(out, bp + (bl - 16), 16, e);
            else { // left pad
                if (bl > 0) {
                    char pad[16] = {0};
                    memcpy(pad + (16 - bl), bp, bl);
                    buffer_put_bytes(out, pad, 16, e);
                } else
                    buffer_put_zero(out, 16, e);
            }
            break;
        }
        default: {
            // pad zeros if unknown fixed size
            buffer_put_zero(out, (u32)col_fixed_bytes(c->type), e);
            break;
        }
        }
    }
    out->flip(out);
    return 0;

EXCEPTION:
    return -1;
}

HOT_PATH
static int bin_decode(struct formatter *me, struct buffer *in, struct flintdb_row *r, char **e) {
    if (!in || !r || !r->meta)
        THROW(e, "bin_decode: invalid args, buffer:%p, row:%p, meta:%p", in, r, r ? r->meta : NULL);

    const struct flintdb_meta *m = me->meta;
    // Peek optional column-count header (fast-path marker for exact BIN with no var-len padding)
    u32 saved = in->position;
    i16 first = in->i16_get(in, e);
    int rowHeaderSeen = 0;
    if ((int)first == m->columns.length) {
        rowHeaderSeen = 1; // exact encoder wrote row header; safe to skip legacy padding logic
    } else {
        // treat as first type tag; rewind
        in->position = saved;
    }
    for (int i = 0; i < m->columns.length && i < r->length; i++) {
        i16 tag = in->i16_get(in, e);
        enum flintdb_variant_type  ctype = (enum flintdb_variant_type )tag;
        const struct flintdb_column *c = &m->columns.a[i];
        // var-len
        if (is_varlen(ctype)) {
            i16 ln = in->i16_get(in, e);
            // For var-len fields (STRING/DECIMAL/BYTES/BLOB/OBJECT), Java formatter does not write padding,
            // while legacy C formatter may write zero padding up to c->bytes. We detect and skip padding
            // only when it's actually present (all-zero bytes followed by a plausible next tag or row header).
            u32 n = (ln > 0) ? (u32)ln : 0u;
            char *p = NULL;
            if (n > 0) {
                p = in->array_get(in, n, e);
            } else {
                flintdb_variant_null_set(&r->array[i]);
            }
            // Conditional pad skip detection: skip entirely when we saw an exact row header (no padding in new BIN)
            if (!rowHeaderSeen && c->bytes > (int)n) {
                u32 padLen = (u32)(c->bytes - (int)n);
                // Bounds
                u32 currPos = (u32)in->position;
                u32 lim = (u32)in->limit;
                int shouldSkipPad = 0;
                if (currPos + padLen <= lim) {
                    // Check that the would-be pad region is all zeros
                    int allZero = 1;
                    const char *arr = in->array;
                    for (u32 zz = 0; zz < padLen; zz++) {
                        if ((unsigned char)arr[currPos + zz] != 0) {
                            allZero = 0;
                            break;
                        }
                    }
                    if (allZero) {
                        // Peek the short immediately after the zero region (candidate next tag or next row header)
                        if (currPos + padLen + 2 <= lim) {
                            i16 after = (i16)((((unsigned char)arr[currPos + padLen]) << 8) | ((unsigned char)arr[currPos + padLen + 1]));
                            // Determine plausibility of boundary:
                            // - If not last column: expect next tag equals declared next type or NIL (0)
                            // - If last column: expect next is row header (column count)
                            if (i + 1 < m->columns.length) {
                                i16 expected = (i16)m->columns.a[i + 1].type;
                                if (after == expected || after == 0 /* NIL */) {
                                    shouldSkipPad = 1;
                                }
                            } else {
                                // last column: next should be row header (column count)
                                if (after == (i16)m->columns.length) {
                                    shouldSkipPad = 1;
                                }
                            }
                        } else {
                            // No room to peek next tag safely; if zeros till end, conservatively skip
                            shouldSkipPad = 1;
                        }
                    }
                }
                if (shouldSkipPad) {
                    in->skip(in, (i32)padLen);
                }
            }
            switch (ctype) {
            case VARIANT_STRING: {
                // flintdb_variant_string_set(&r->array[i], (p ? p : ""), n); // safe but slower

                // Use string_ref_set for better performance - sflag will track null-termination status
                // string_get will handle null-termination conversion when needed
                flintdb_variant_string_ref_set(&r->array[i], (p ? p : ""), n, VARIANT_SFLAG_NOT_NULL_TERMINATED);
                break;
            }
            case VARIANT_DECIMAL: {
                int scale = (c->precision > 0) ? c->precision : 0;
                struct flintdb_decimal  d = {0};
                if (n <= 8) {
                    // Optimized fast path: up to 8 bytes -> sign-extended i64
                    i64 x = 0;
                    if (n > 0 && p) {
                        x = row_bytes_to_i64_opt(p, n);
                    }
                    decimal_from_unscaled_i64(x, scale, &d);
                } else if (n <= 16) {
                    // 16-byte (128-bit) path: two's-complement big-int division by 10
                    decimal_from_twos_bytes((const u8 *)p, n, scale, &d);
                } else {
                    // Fallback: attempt big-int conversion up to 32 bytes, else NIL
                    decimal_from_twos_bytes((const u8 *)p, (n > 32 ? 32 : n), scale, &d);
                }
                flintdb_variant_decimal_set(&r->array[i], d.sign, d.scale, d);
                break;
            }
            case VARIANT_BYTES:
            case VARIANT_BLOB:
            case VARIANT_OBJECT: {
                flintdb_variant_bytes_set(&r->array[i], (p ? p : NULL), n);
                break;
            }
            default: {
                flintdb_variant_null_set(&r->array[i]);
                break;
            }
            }
            continue;
        }
        // fixed-size
        switch (ctype) {
        case VARIANT_INT8: {
            char v = in->i8_get(in, e);
            flintdb_variant_i8_set(&r->array[i], (i8)v);
            break;
        }
        case VARIANT_UINT8: {
            char v = in->i8_get(in, e);
            flintdb_variant_u8_set(&r->array[i], (u8)(unsigned char)v);
            break;
        }
        case VARIANT_INT16: {
            i16 v = in->i16_get(in, e);
            flintdb_variant_i16_set(&r->array[i], v);
            break;
        }
        case VARIANT_UINT16: {
            i16 v = in->i16_get(in, e);
            flintdb_variant_u16_set(&r->array[i], (u16)v);
            break;
        }
        case VARIANT_INT32: {
            i32 v = in->i32_get(in, e);
            flintdb_variant_i32_set(&r->array[i], v);
            break;
        }
        case VARIANT_UINT32: {
            i32 v = in->i32_get(in, e);
            flintdb_variant_u32_set(&r->array[i], (u32)v);
            break;
        }
        case VARIANT_INT64: {
            i64 v = in->i64_get(in, e);
            flintdb_variant_i64_set(&r->array[i], v);
            break;
        }
        case VARIANT_DOUBLE: {
            f64 v = in->f64_get(in, e);
            flintdb_variant_f64_set(&r->array[i], v);
            break;
        }
        case VARIANT_FLOAT: {
            i32 bits = in->i32_get(in, e);
            float fv = 0.0f;
            memcpy(&fv, &bits, sizeof(float));
            flintdb_variant_f64_set(&r->array[i], (f64)fv);
            break;
        }
        case VARIANT_DATE: {
            // Dual decode: support both Java Date24Bits (Y/M/D packing)
            u32 v = get_u24(in, e);
            // Try packed Y/M/D first
            int year = (int)(v >> 9);
            int month = (int)((v >> 5) & 0x0F);
            int day = (int)(v & 0x1F);
            int ymdValid = (year >= 1900 && year <= 9999 && month >= 1 && month <= 12 && day >= 1 && day <= 31);
            time_t t = 0;
            if (ymdValid) {
                // Fast compute days since epoch without mktime()
                i64 days = days_from_civil_fast(year, month, day);
                t = (time_t)(days * 86400);
            } else {
                // treat as days since epoch
                t = (time_t)v * 86400;
            }
            flintdb_variant_date_set(&r->array[i], t);
            break;
        }
        case VARIANT_TIME: {
            i64 ms = in->i64_get(in, e);
            time_t t = (time_t)(ms / 1000);
            flintdb_variant_time_set(&r->array[i], t);
            break;
        }
        case VARIANT_UUID: {
            char *p = in->array_get(in, 16, e);
            flintdb_variant_uuid_set(&r->array[i], p, 16);
            break;
        }
        case VARIANT_IPV6: {
            char *p = in->array_get(in, 16, e);
            flintdb_variant_ipv6_set(&r->array[i], p, 16);
            break;
        }
        default: {
            // skip unknown
            int fb = col_fixed_bytes(ctype);
            if (fb > 0)
                in->skip(in, fb);
            flintdb_variant_null_set(&r->array[i]);
            break;
        }
        }
    }
    return 0;

EXCEPTION:
    return -1;
}

// --- Text encode/decode ---
struct text_formatter_priv {
    char nil_str[MAX_COLUMN_NAME_LIMIT]; // custom NULL representation, e.g. "NULL", "\\N", etc.
    char delimiter;                      // field delimiter, e.g. ',' or '\t'
    char quote;                          // quote character, e.g. '"', default none
    char name[32];                       // formatter name

    // Performance optimization: pre-allocated buffers to avoid repeated allocations
    u32 nil_len;         // cached length of nil_str
    char **temp_fields;  // reusable array for split results
    u32 temp_fields_cap; // capacity of temp_fields array

    struct string_pool *pool; // arena for field strings
    unsigned char *temp_is_pool; // flags per temp_fields entry: 1 if allocated from pool, 0 if heap or NULL
};

static int text_escape(struct text_formatter_priv *priv, const char *field, u32 fieldlen, struct buffer *out, char **e) { // equivalent to TSVFile.java TEXTROWFORMATTER.appendEscaped()
    if (!priv || !out)
        THROW(e, "text_escape: invalid args");

    // NULL sentinel already handled by caller; just write field with proper escaping/quoting
    const char DELIM = priv->delimiter ? priv->delimiter : '\t';
    const char QUOTE = priv->quote; // 0 means no-quote TSV mode

    if (QUOTE == 0) {
        // Backslash-escaped mode: escape \\ \t \n \r and delimiter
        for (u32 i = 0; i < fieldlen; i++) {
            char ch = field[i];
            if (ch == '\\') {
                buffer_put_bytes(out, "\\\\", 2, e);
            } else if (ch == '\t') {
                buffer_put_bytes(out, "\\t", 2, e);
            } else if (ch == '\n') {
                buffer_put_bytes(out, "\\n", 2, e);
            } else if (ch == '\r') {
                buffer_put_bytes(out, "\\r", 2, e);
            } else if (ch == DELIM) {
                buffer_put_bytes(out, "\\", 1, e);
                buffer_put_bytes(out, &DELIM, 1, e);
            } else {
                buffer_put_bytes(out, &ch, 1, e);
            }
        }
        return 0;
    }

    // CSV-style quoting: wrap if contains QUOTE/DELIM/newline
    int needsQuote = 0;
    for (u32 i = 0; i < fieldlen && !needsQuote; i++) {
        char ch = field[i];
        if (ch == QUOTE || ch == '\n' || ch == '\r' || ch == DELIM)
            needsQuote = 1;
    }
    if (!needsQuote) {
        buffer_put_bytes(out, field, fieldlen, e);
        return 0;
    }
    buffer_put_bytes(out, &priv->quote, 1, e);
    for (u32 i = 0; i < fieldlen; i++) {
        char ch = field[i];
        if (ch == QUOTE) {
            buffer_put_bytes(out, &priv->quote, 1, e);
            buffer_put_bytes(out, &priv->quote, 1, e);
        } else {
            buffer_put_bytes(out, &ch, 1, e);
        }
    }
    buffer_put_bytes(out, &priv->quote, 1, e);
    return 0;

EXCEPTION:
    return -1;
}

// Fast path for typical TSV (no quotes) using memchr and minimal branching
static int text_split_fast_unquoted(struct text_formatter_priv *priv, const char *line, u32 linelen, char ***fields, u32 *fieldcount, char **e) {
    const char DELIM = priv->delimiter ? priv->delimiter : '\t';
    const char *nil = priv->nil_str;
    const u32 nil_len = priv->nil_len;

    // Reuse priv's temp_fields array
    u32 cap = priv->temp_fields_cap;
    char **arr = priv->temp_fields;
    unsigned char *flags = priv->temp_is_pool;
    u32 cnt = 0;

    const char *p = line;
    const char *end = line + linelen;
    while (p < end) {
        const char *nl = memchr(p, '\n', (size_t)(end - p));
        const char *dl = memchr(p, DELIM, (size_t)(end - p));
        const char *stop = NULL;
        int is_newline = 0;
        if (nl && (!dl || nl < dl)) {
            stop = nl;
            is_newline = 1;
        } else if (dl) {
            stop = dl;
        } else {
            stop = end;
        }

        u32 flen = (u32)(stop - p);
        char *fieldstr = NULL;
        unsigned char use_pool = 0;
        if (flen == nil_len && simd_memcmp(p, nil, flen) == 0) {
            fieldstr = NULL; // NULL token
            flen = 0;
        } else {
            // Prefer pool when field fits
            if (priv->pool && flen + 1 <= priv->pool->str_size) {
                fieldstr = priv->pool->borrow(priv->pool);
                use_pool = 1;
            } else {
                fieldstr = (char *)MALLOC((size_t)flen + 1);
                use_pool = 0;
            }
            if (!fieldstr) { THROW(e, "Out of memory"); }
            if (flen) simd_memcpy(fieldstr, p, flen);
            fieldstr[flen] = '\0';
        }
        if (cnt >= cap) {
            u32 old_cap = cap;
            cap <<= 1;
            char **na = (char **)REALLOC(arr, cap * sizeof(char *));
            if (!na) {
                THROW(e, "Out of memory");
            }
            arr = na;
            priv->temp_fields = arr;
            priv->temp_fields_cap = cap;
            // grow flags in tandem
            unsigned char *nf = (unsigned char *)REALLOC(flags, cap * sizeof(unsigned char));
            if (!nf) { THROW(e, "Out of memory"); }
            // zero initialize new region
            memset(nf + old_cap, 0, (size_t)(cap - old_cap));
            flags = nf;
            priv->temp_is_pool = flags;
        }
        arr[cnt] = fieldstr;
        if (flags) flags[cnt] = use_pool;
        cnt++;

        if (is_newline) {
            // handle CRLF by consuming optional '\r' before '\n'
            // If stop points at '\n' and previous is '\r', we already excluded it from field
            stop++; // consume '\n'
            break;  // end of record
        }
        // consume delimiter or reach end
        if (stop < end)
            stop++;
        p = stop;
    }

    *fields = arr;
    *fieldcount = cnt;
    return (int)(p - line);

EXCEPTION:
    return -1;
}

static int text_split(struct text_formatter_priv *priv, const char *line, u32 linelen, char ***fields, u32 *fieldcount, char **e) { // equivalent to TSVFile.java TEXTROWFORMATTER.split()
    if (!priv || !line || !fields || !fieldcount)
        THROW(e, "text_split: invalid args");

    const char DELIM = priv->delimiter ? priv->delimiter : '\t';
    const char QUOTE = priv->quote;
    const char BSLASH = '\\';

    // Fast path: unquoted mode and no backslashes in the slice -> split using memchr
    if (QUOTE == 0) {
        const void *has_bslash = memchr(line, BSLASH, linelen);
        if (!has_bslash) {
            return text_split_fast_unquoted(priv, line, linelen, fields, fieldcount, e);
        }
    }

    // Reuse priv's temp_fields array
    u32 cap = priv->temp_fields_cap;
    char **arr = priv->temp_fields;
    unsigned char *flags = priv->temp_is_pool;
    u32 cnt = 0;

    // Working buffer for unescaped field - borrow from pool or use heap scratch
    char *sb = NULL;
    u32 sbcap = 0;
    int sb_borrowed = 0; // 1 if from pool
    if (priv->pool) {
        sb = priv->pool->borrow(priv->pool);
        sbcap = priv->pool->str_size;
        sb_borrowed = 1;
    } else {
        sbcap = 1024;
        sb = (char *)MALLOC(sbcap);
        if (!sb) THROW(e, "Out of memory");
        sb_borrowed = 0;
    }
    u32 sbn = 0;

    // Pre-calculate nil_str length to avoid repeated strlen calls
    const u32 nil_len = priv->nil_len;
    const char *nil_str = priv->nil_str;

    int qoute = 0;        // inside quote
    int quoted_field = 0; // remember if current field was quoted (affects NULL token handling)
    u32 i = 0;

// Inline function to finalize and add a field
#define FINALIZE_FIELD()                                                                 \
    do {                                                                                 \
        sb[sbn] = '\0';                                                                  \
        char *fieldstr = NULL;                                                           \
        unsigned char use_pool = 0;                                                      \
        if (!quoted_field && sbn == nil_len && simd_memcmp(sb, nil_str, sbn) == 0) {          \
            fieldstr = NULL;                                                             \
        } else {                                                                         \
            if (priv->pool && sbn + 1 <= priv->pool->str_size) {                         \
                fieldstr = priv->pool->borrow(priv->pool);                               \
                use_pool = 1;                                                            \
            } else {                                                                     \
                fieldstr = (char *)MALLOC((size_t)sbn + 1);                              \
                use_pool = 0;                                                            \
            }                                                                            \
            if (!fieldstr) THROW(e, "Out of memory");                                             \
            simd_memcpy(fieldstr, sb, sbn + 1);                                               \
        }                                                                                \
        if (cnt >= cap) {                                                                \
            u32 old_cap = cap;                                                           \
            cap <<= 1;                                                                   \
            arr = (char **)REALLOC(arr, cap * sizeof(char *));                           \
            if (!arr) THROW(e, "Out of memory");                                                  \
            priv->temp_fields = arr;                                                     \
            priv->temp_fields_cap = cap;                                                 \
            unsigned char *nf = (unsigned char *)REALLOC(flags, cap * sizeof(unsigned char)); \
            if (!nf) THROW(e, "Out of memory");                                                   \
            memset(nf + old_cap, 0, (size_t)(cap - old_cap));                            \
            flags = nf;                                                                  \
            priv->temp_is_pool = flags;                                                  \
        }                                                                                \
        arr[cnt] = fieldstr;                                                             \
        if (flags) flags[cnt] = use_pool;                                                \
        cnt++;                                                                           \
        sbn = 0;                                                                         \
        quoted_field = 0;                                                                \
    } while (0)

    while (i < linelen) {
        char ch = line[i];
        char next = (i + 1 < linelen) ? line[i + 1] : '\0';

        // Record terminator outside quotes => end of record
        if (qoute == 0 && (ch == '\n' || ch == '\r')) {
            FINALIZE_FIELD();
            // consume newline (and optional paired \r\n)
            if (ch == '\r' && next == '\n')
                i += 2;
            else
                i += 1;
            break; // stop at end of record
        }

        if (qoute > 0 && QUOTE == ch && QUOTE == next) {
            // Escaped quote inside quotes => append one quote
            sb[sbn++] = ch;
            i += 2;
            continue;
        } else if (qoute > 0 && QUOTE == ch) {
            qoute = 0;
            i++;
            continue;
        } else if (QUOTE != 0 && QUOTE == ch) {
            qoute = 1;
            quoted_field = 1;
            i++;
            continue;
        } else if (qoute > 0) {
            sb[sbn++] = ch;
            i++;
            continue;
        } else if (BSLASH == ch) {
            // backslash escapes - optimized with fewer branches
            char esc = 0;
            if (next == DELIM)
                esc = DELIM;
            else if (next == BSLASH)
                esc = BSLASH;
            else if (next == 'n')
                esc = '\n';
            else if (next == 'r')
                esc = '\r';
            else if (next == 't')
                esc = '\t';

            if (esc) {
                if (sbn + 1 >= sbcap) {                                                     
                    if (sb_borrowed) {                                                       
                        // switch to heap buffer and grow
                        u32 newcap = (sbcap < 4096) ? 8192 : (sbcap << 1);                  
                        char *nsb = (char *)MALLOC(newcap);                                  
                        if (!nsb) THROW(e, "Out of memory");                                          
                        if (sbn) simd_memcpy(nsb, sb, sbn);                                       
                        sb = nsb;                                                            
                        sbcap = newcap;                                                      
                        sb_borrowed = 0;                                                     
                    } else {                                                                 
                        u32 newcap = (sbcap < 4096) ? 8192 : (sbcap << 1);                  
                        char *nsb = (char *)REALLOC(sb, newcap);                             
                        if (!nsb) THROW(e, "Out of memory");                                          
                        sb = nsb;                                                            
                        sbcap = newcap;                                                      
                    }                                                                        
                }                                                                            
                sb[sbn++] = ch;
                i += 2;
                continue;
            } else {
                sb[sbn++] = ch;
                i++;
                continue;
            }
        } else if (DELIM == ch) {
            // finalize field at delimiter
            FINALIZE_FIELD();
            i++;
        } else {
            sb[sbn++] = ch;
            i++;
        }
    }

    // If we haven't hit a newline, flush the last field (end-of-buffer record)
    if (i >= linelen) {
        FINALIZE_FIELD();
    }

#undef FINALIZE_FIELD

    // Return or free scratch buffer
    if (sb_borrowed && priv->pool) {
        priv->pool->return_string(priv->pool, sb);
    } else if (!sb_borrowed) {
        FREE(sb);
    }
    *fields = arr;
    *fieldcount = cnt;
    return (int)i; // return consumed chars (up to and including newline if present)

EXCEPTION:
    return -1;
}

HOT_PATH
static int text_encode(struct formatter *me, struct flintdb_row *r, struct buffer *out, char **e) { // equivalent to TSVFile.java TEXTROWFORMATTER.format()
    struct text_formatter_priv *priv = (struct text_formatter_priv *)me->priv;
    if (!priv)
        THROW(e, "formatter not initialized");
    if (!r || !r->meta || !out)
        THROW(e, "text_encode: invalid args");
    const struct flintdb_meta *m = me->meta;
    out->clear(out);

    for (int i = 0; i < m->columns.length && i < r->length; i++) {
        if (i > 0)
            buffer_put_bytes(out, &priv->delimiter, 1, e);
        struct flintdb_variant *v = &r->array[i];
        if (flintdb_variant_is_null(v)) {
            buffer_put_bytes(out, priv->nil_str, (u32)strlen(priv->nil_str), e);
            continue;
        }
        const struct flintdb_column *c = &m->columns.a[i];
        char buf[256];
        buf[0] = '\0';
        switch (c->type) {
        case VARIANT_DATE: {
            time_t tt = r->date_get(r, i, e);
            
            // Fast conversion without localtime_r
            int year, month, day;
            row_fast_time_to_date(tt, &year, &month, &day);
            
            int n = snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
            (void)n;
            text_escape(priv, buf, (u32)strlen(buf), out, e);
            break;
        }
        case VARIANT_TIME: {
            time_t tt = r->time_get(r, i, e);
            
            // Fast conversion without localtime_r
            int year, month, day;
            row_fast_time_to_date(tt, &year, &month, &day);
            
            // Extract time components
            i64 seconds_in_day = (i64)tt % 86400;
            if (seconds_in_day < 0) seconds_in_day += 86400; // Handle negative
            int hour = (int)(seconds_in_day / 3600);
            int min = (int)((seconds_in_day % 3600) / 60);
            int sec = (int)(seconds_in_day % 60);
            
            int n = snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                             year, month, day, hour, min, sec);
            (void)n;
            text_escape(priv, buf, (u32)strlen(buf), out, e);
            break;
        }
        case VARIANT_STRING: {
            const char *s = r->string_get(r, i, e);
            u32 L = s ? (u32)strlen(s) : 0;
            if (s && L > 0)
                text_escape(priv, s, L, out, e);
            else
                buffer_put_bytes(out, "", 0, e);
            break;
        }
        case VARIANT_DOUBLE:
        case VARIANT_FLOAT: {
            double dv = r->f64_get(r, i, e);
            int n = snprintf(buf, sizeof(buf), "%.*g", 17, dv);
            (void)n;
            text_escape(priv, buf, (u32)strlen(buf), out, e);
            break;
        }
        case VARIANT_INT8:
        case VARIANT_UINT8:
        case VARIANT_INT16:
        case VARIANT_UINT16:
        case VARIANT_INT32:
        case VARIANT_UINT32:
        case VARIANT_INT64: {
            long long iv = (long long)r->i64_get(r, i, e);
            int n = snprintf(buf, sizeof(buf), "%lld", iv);
            (void)n;
            text_escape(priv, buf, (u32)strlen(buf), out, e);
            break;
        }
        case VARIANT_DECIMAL: {
            struct flintdb_decimal  d = r->decimal_get(r, i, e);
            flintdb_decimal_to_string(&d, buf, sizeof(buf));
            text_escape(priv, buf, (u32)strlen(buf), out, e);
            break;
        }
        case VARIANT_BYTES:
        case VARIANT_BLOB:
        case VARIANT_OBJECT: {
            // encode as hex without separators
            u32 bl = 0;
            const char *bp = r->bytes_get(r, i, &bl, e);
            if (bp && bl > 0) {
                // each byte -> 2 hex chars
                u32 need = bl * 2;
                char *hex = (char *)MALLOC(need);
                if (!hex)
                    THROW(e, "Out of memory");
                static const char *HX = "0123456789abcdef";
                for (u32 k = 0; k < bl; k++) {
                    unsigned char b = (unsigned char)bp[k];
                    hex[k * 2] = HX[(b >> 4) & 0xF];
                    hex[k * 2 + 1] = HX[b & 0xF];
                }
                text_escape(priv, hex, need, out, e);
                FREE(hex);
            }
            break;
        }
        default: {
            // Fallback: stringify via string_get if possible
            const char *s = r->string_get(r, i, e);
            u32 L = s ? (u32)strlen(s) : 0;
            if (s && L > 0)
                text_escape(priv, s, L, out, e);
            break;
        }
        }
    }
    // newline
    buffer_put_bytes(out, "\n", 1, e);
    out->flip(out);
    return 0;

EXCEPTION:
    return -1;
}

HOT_PATH
static int text_decode(struct formatter *me, struct buffer *in, struct flintdb_row *r, char **e) { // equivalent to TSVFile.java TEXTROWFORMATTER.parse()
    struct text_formatter_priv *priv = (struct text_formatter_priv *)me->priv;
    if (!priv)
        THROW(e, "formatter not initialized");
    if (!in || !r || !r->meta)
        THROW(e, "text_decode: invalid args");

    // Parse from current position up to end-of-record (newline outside quotes) or buffer end
    const char *data = in->array + in->position;
    u32 len = in->limit - in->position;
    if (len == 0)
        return -1; // nothing to parse

    char **fields = NULL;
    u32 nfields = 0;
    int consumed = text_split(priv, data, len, &fields, &nfields, e);
    if (consumed < 0)
        THROW(e, "text_decode: split failed");

    // (debug removed)

    // Advance buffer position by consumed bytes
    in->position += (u32)consumed;

    // Fill row using meta types via row.set casting
    const struct flintdb_meta *m = me->meta;
    int cols = m->columns.length;

    for (int i = 0; i < cols && i < r->length; i++) {
        const char *fv = (i < (int)nfields) ? fields[i] : NULL;
        if (fv == NULL) {
            flintdb_variant_null_set(&r->array[i]);
        } else {
            // Get column type and field length for optimized parsing
            const struct flintdb_column *col = &m->columns.a[i];
            enum flintdb_variant_type  ctype = col->type;
            u32 fl = (u32)strlen(fv); // Calculate length for parsing

            // STRING type: copy into variant-owned buffer so we can safely return pool memory
            if (ctype == VARIANT_STRING) {
                flintdb_variant_string_set(&r->array[i], fv, fl);
            }
            // Direct parsing for numeric types - use fast length-based parsers
            else if (ctype == VARIANT_INT64) {
                i64 val = 0;
                if (parse_i64(fv, fl, &val) == 0) {
                    flintdb_variant_i64_set(&r->array[i], val);
                } else {
                    flintdb_variant_null_set(&r->array[i]);
                }
            } else if (ctype == VARIANT_INT32) {
                i64 val = 0;
                if (parse_i64(fv, fl, &val) == 0) {
                    flintdb_variant_i32_set(&r->array[i], (i32)val);
                } else {
                    flintdb_variant_null_set(&r->array[i]);
                }
            } else if (ctype == VARIANT_INT16) {
                i64 val = 0;
                if (parse_i64(fv, fl, &val) == 0) {
                    flintdb_variant_i16_set(&r->array[i], (i16)val);
                } else {
                    flintdb_variant_null_set(&r->array[i]);
                }
            } else if (ctype == VARIANT_INT8) {
                i64 val = 0;
                if (parse_i64(fv, fl, &val) == 0) {
                    flintdb_variant_i8_set(&r->array[i], (i8)val);
                } else {
                    flintdb_variant_null_set(&r->array[i]);
                }
            } else if (ctype == VARIANT_DOUBLE || ctype == VARIANT_FLOAT) {
                f64 val = 0.0;
                if (parse_f64(fv, fl, &val) == 0) {
                    flintdb_variant_f64_set(&r->array[i], val);
                } else {
                    flintdb_variant_null_set(&r->array[i]);
                }
            }
            // DATE and TIME parsing
            else if (ctype == VARIANT_DATE || ctype == VARIANT_TIME) {
                time_t t = 0;
                if (parse_datetime(fv, fl, &t) == 0) {
                    if (ctype == VARIANT_DATE) {
                        flintdb_variant_date_set(&r->array[i], t);
                    } else {
                        flintdb_variant_time_set(&r->array[i], t);
                    }
                } else {
                    flintdb_variant_null_set(&r->array[i]);
                }
            }
            // DECIMAL parsing - parse directly to decimal struct
            else if (ctype == VARIANT_DECIMAL) {
                int scale = col->precision;
                struct flintdb_decimal  d = {0};
                
                if (flintdb_decimal_from_string(fv, scale, &d) == 0) {
                    flintdb_variant_decimal_set(&r->array[i], d.sign, d.scale, d);
                } else {
                    flintdb_variant_null_set(&r->array[i]);
                }
            }
            // For other types, use the row.set method
            else {
                // Fallback: pass through as STRING using a non-owning slice to avoid allocation
                struct flintdb_variant tmp;
                flintdb_variant_init(&tmp);
                flintdb_variant_string_ref_set(&tmp, fv, fl, VARIANT_SFLAG_NULL_TERMINATED);
                r->set(r, i, &tmp, e);
                flintdb_variant_free(&tmp);
            }
        }
    }
    // Return/free field strings back to pool or heap now that row variants own/copy data
    for (u32 i = 0; i < nfields; i++) {
        char *fv = fields[i];
        if (!fv) continue;
        if (priv->temp_is_pool && priv->temp_is_pool[i]) {
            if (priv->pool) priv->pool->return_string(priv->pool, fv);
            priv->temp_is_pool[i] = 0;
        } else {
            FREE(fv);
        }
        fields[i] = NULL;
    }
    return 0;

EXCEPTION:
    return -1;
}

// -- Formatter init/close

void formatter_close(struct formatter *me) {
    // Be defensive: allow NULL and already-closed formatters
    if (!me || !me->priv)
        return;

    // Free temp_fields if it's a text formatter
    struct text_formatter_priv *priv = (struct text_formatter_priv *)me->priv;
    if (priv->temp_fields) {
        // Return/free any outstanding field strings
        for (u32 i = 0; i < priv->temp_fields_cap; i++) {
            char *p = priv->temp_fields[i];
            if (!p) continue;
            if (priv->temp_is_pool && priv->temp_is_pool[i]) {
                if (priv->pool) priv->pool->return_string(priv->pool, p);
            } else {
                FREE(p);
            }
            priv->temp_fields[i] = NULL;
            if (priv->temp_is_pool) priv->temp_is_pool[i] = 0;
        }
        FREE(priv->temp_fields);
        priv->temp_fields = NULL;
    }
    if (priv->temp_is_pool) { FREE(priv->temp_is_pool); priv->temp_is_pool = NULL; }
    if (priv->pool) { priv->pool->free(priv->pool); priv->pool = NULL; }

    FREE(me->priv);
    me->priv = NULL;
}

int formatter_init(enum fileformat format, struct flintdb_meta *meta, struct formatter *formatter, char **e) {
    if (!formatter)
        return -1;

    memset(formatter, 0, sizeof(*formatter));
    formatter->meta = meta;

    switch (format) {
    case FORMAT_BIN:
        formatter->encode = &bin_encode;
        formatter->decode = &bin_decode;
        formatter->close = &formatter_close;
        break;

    case FORMAT_CSV: {
        formatter->encode = &text_encode;
        formatter->decode = &text_decode;
        formatter->close = &formatter_close;

        struct text_formatter_priv *priv = formatter->priv = CALLOC(1, sizeof(struct text_formatter_priv));
        if (!priv)
            THROW(e, "formatter_init: memory allocation failed");
        // Set default values
        strncpy(priv->nil_str, "NULL", sizeof(priv->nil_str));
        priv->delimiter = ',';
        priv->quote = '"';
        strncpy(priv->name, "CSV", sizeof(priv->name));

        if (meta->nil_str[0])
            strncpy(priv->nil_str, meta->nil_str, sizeof(priv->nil_str));
        if (meta->delimiter)
            priv->delimiter = meta->delimiter;
        if (meta->quote)
            priv->quote = meta->quote;

    // Initialize performance optimization fields
        priv->nil_len = (u32)strlen(priv->nil_str);
        // Pre-size temp_fields to next power-of-two >= column count (min 32)
        u32 want = (u32)(meta->columns.length > 0 ? meta->columns.length : 32);
        // round up to next power-of-two
        want--;
        want |= want >> 1;
        want |= want >> 2;
        want |= want >> 4;
        want |= want >> 8;
        want |= want >> 16;
        want++;
        if (want < 32)
            want = 32;
        priv->temp_fields_cap = want;
        priv->temp_fields = (char **)CALLOC(priv->temp_fields_cap, sizeof(char *));
        if (!priv->temp_fields) {
            FREE(priv);
            THROW(e, "formatter_init: memory allocation failed");
        }
        // flags parallel array
        priv->temp_is_pool = (unsigned char *)CALLOC(priv->temp_fields_cap, sizeof(unsigned char));
        if (!priv->temp_is_pool) {
            FREE(priv->temp_fields);
            FREE(priv);
            THROW(e, "formatter_init: memory allocation failed");
        }

        // Create a string pool for scratch/field buffers
        // Choose capacity roughly columns*2 (+ extra) and per-string size 64KB by default
        u32 pool_cap = want + 16; // a bit of headroom
        u32 str_size = 64 * 1024; // 64KB default per string
        u32 preload = (pool_cap > 4) ? (pool_cap / 2) : pool_cap; // preload half for performance
        priv->pool = string_pool_create(pool_cap, str_size, preload);
        if (!priv->pool) {
            FREE(priv->temp_is_pool);
            FREE(priv->temp_fields);
            FREE(priv);
            THROW(e, "formatter_init: cannot create string pool");
        }

        break;
    }
    case FORMAT_TSV: {
        formatter->encode = &text_encode;
        formatter->decode = &text_decode;
        formatter->close = &formatter_close;

        struct text_formatter_priv *priv2 = formatter->priv = CALLOC(1, sizeof(struct text_formatter_priv));
        if (!priv2)
            THROW(e, "formatter_init: memory allocation failed");
        // Set default values
        strncpy(priv2->nil_str, "\\N", sizeof(priv2->nil_str));
        priv2->delimiter = '\t';
        priv2->quote = 0; // no quote by default
        strncpy(priv2->name, "TSV", sizeof(priv2->name));

        if (meta->nil_str[0])
            strncpy(priv2->nil_str, meta->nil_str, sizeof(priv2->nil_str));
        if (meta->delimiter)
            priv2->delimiter = meta->delimiter;
        if (meta->quote)
            priv2->quote = meta->quote;

    // Initialize performance optimization fields
        priv2->nil_len = (u32)strlen(priv2->nil_str);
        // Pre-size temp_fields to next power-of-two >= column count (min 32)
        u32 want2 = (u32)(meta->columns.length > 0 ? meta->columns.length : 32);
        want2--;
        want2 |= want2 >> 1;
        want2 |= want2 >> 2;
        want2 |= want2 >> 4;
        want2 |= want2 >> 8;
        want2 |= want2 >> 16;
        want2++;
        if (want2 < 32)
            want2 = 32;
        priv2->temp_fields_cap = want2;
        priv2->temp_fields = (char **)CALLOC(priv2->temp_fields_cap, sizeof(char *));
        if (!priv2->temp_fields) {
            FREE(priv2);
            THROW(e, "formatter_init: memory allocation failed");
        }

        priv2->temp_is_pool = (unsigned char *)CALLOC(priv2->temp_fields_cap, sizeof(unsigned char));
        if (!priv2->temp_is_pool) {
            FREE(priv2->temp_fields);
            FREE(priv2);
            THROW(e, "formatter_init: memory allocation failed");
        }

        u32 pool_cap2 = want2 + 16;
        u32 str_size2 = 64 * 1024; // 64KB default per string
        u32 preload2 = (pool_cap2 > 4) ? (pool_cap2 / 2) : pool_cap2; // preload half for performance
        priv2->pool = string_pool_create(pool_cap2, str_size2, preload2);
        if (!priv2->pool) {
            FREE(priv2->temp_is_pool);
            FREE(priv2->temp_fields);
            FREE(priv2);
            THROW(e, "formatter_init: cannot create string pool");
        }

        break;
    }
    default:
        THROW(e, "formatter_init: unsupported format %d", format);
    }
    return 0;

EXCEPTION:
    return -1;
}

// Print a row in TSV format using the existing formatter pipeline.
// This respects meta settings (delimiter, quote, nil_str) and applies
// proper escaping consistent with file I/O behavior.
FLINTDB_API void flintdb_print_row(const struct flintdb_row *r) {
    if (!r || !r->meta) {
        printf("print_row: invalid row\n");
        return;
    }

    char *e = NULL;
    struct formatter fmt;
    if (formatter_init(FORMAT_TSV, r->meta, &fmt, &e) == 0) {
        struct buffer *bout = buffer_alloc(1 << 20); // 1MB scratch buffer
        if (bout) {
            if (fmt.encode(&fmt, (struct flintdb_row *)r, bout, &e) == 0) {
                // Write exactly the produced bytes (includes trailing newline)
                if (bout->limit > 0 && bout->array) {
                    printf("%lld\t", r->rowid);
                    fwrite(bout->array, 1, bout->limit, stdout);
                }
                bout->free(bout);
                if (fmt.close) fmt.close(&fmt);
                return;
            }
            bout->free(bout);
        }
        if (fmt.close) fmt.close(&fmt);
    }
}