#ifndef FLINTDB_INTERNAL_H
#define FLINTDB_INTERNAL_H

#include "flintdb.h"
#include "types.h"
#include "buffer.h"
#include "runtime.h"
#include "allocator.h"

enum fileformat {
    FORMAT_BIN = 0,
    FORMAT_TSV = 1,
    FORMAT_CSV = 2,
    // FORMAT_JSON = 3, // reserved for future
    FORMAT_JSONL = 4,
    FORMAT_PARQUET = 11, 
    FORMAT_UNKNOWN = 99
};

enum fileformat detect_file_format(const char *name);

struct formatter {
    void *priv;
    const struct flintdb_meta *meta;

    int (*encode)(struct formatter * me, struct flintdb_row *r, struct buffer *out, char **e);
    int (*decode)(struct formatter * me, struct buffer *in, struct flintdb_row *r, char **e);
    void (*close)(struct formatter * me);
};

// Formatter operations
int formatter_init(enum fileformat format, struct flintdb_meta *meta, struct formatter *formatter, char **e);




// Removed str_empty; use strempty from runtime.h instead for consistency.

static inline i64 parse_i64(const char *s, u32 len, i64 *out) {
    if (!s || len == 0)
        return -1;

    const char *end = s + len;

    // Skip leading whitespace
    while (s < end && (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r')) {
        s++;
    }

    if (s >= end)
        return -2;

    // Handle sign
    int negative = 0;
    if (s < end && *s == '-') {
        negative = 1;
        s++;
    } else if (s < end && *s == '+') {
        s++;
    }

    if (s >= end)
        return -2; // sign only, no digits

    // Parse digits manually
    i64 result = 0;
    const char *start = s;
    while (s < end && *s >= '0' && *s <= '9') {
        result = result * 10 + (*s - '0');
        s++;
    }

    if (s == start)
        return -3; // no digits

    *out = negative ? -result : result;
    return 0;
}

static inline int parse_u64(const char *s, u32 len, u64 *out) {
    if (!s || len == 0)
        return -1;

    const char *end = s + len;

    // Skip leading whitespace
    while (s < end && (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r')) {
        s++;
    }

    if (s >= end)
        return -2;

    // Skip optional '+'
    if (s < end && *s == '+')
        s++;

    if (s >= end)
        return -2; // '+' only, no digits

    // Parse digits manually
    u64 result = 0;
    const char *start = s;
    while (s < end && *s >= '0' && *s <= '9') {
        result = result * 10 + (*s - '0');
        s++;
    }

    if (s == start)
        return -3; // no digits

    *out = result;
    return 0;
}

static inline f64 parse_f64(const char *s, u32 len, f64 *out) {
    if (!s || len == 0)
        return -1;

    const char *end = s + len;

    // Skip leading whitespace
    while (s < end && (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r')) {
        s++;
    }

    if (s >= end)
        return -2;

    // strtod requires null-terminated string, so we need to copy
    // Use a reasonable buffer size for numbers
    char buf[128];
    u32 copy_len = (u32)(end - s);
    if (copy_len >= sizeof(buf))
        copy_len = sizeof(buf) - 1;

    memcpy(buf, s, copy_len);
    buf[copy_len] = '\0';

    char *endptr = NULL;
    double v = strtod(buf, &endptr);
    if (endptr == buf)
        return -3;

    *out = v;
    return 0;
}

// --- Internal string helpers (unified) ---
// Bounded safe copy: copies at most cap-1 chars, always null-terminates.
static inline void s_copy(char *dst, size_t cap, const char *src) {
    if (!dst || cap == 0) return;
    if (!src) { dst[0] = '\0'; return; }
    size_t n = strlen(src);
    if (n >= cap) n = cap - 1;
    memcpy(dst, src, n);
    dst[n] = '\0';
}

// Bounded safe concatenation: appends src within remaining space (cap-1 total), null-terminates.
static inline void s_cat(char *dst, size_t cap, const char *src) {
    if (!dst || cap == 0 || !src) return;
    size_t n = strlen(dst);
    if (n >= cap - 1) return;
    size_t remain = cap - 1 - n;
    size_t m = strlen(src);
    if (m > remain) m = remain;
    memcpy(dst + n, src, m);
    dst[n + m] = '\0';
}

// Trim leading/trailing ASCII whitespace in-place, returns same pointer.
static inline char * trim(char *s) {
    if (!s) return s;
    size_t n = strlen(s);
    size_t i = 0;
    while (i < n && isspace((unsigned char)s[i])) i++;
    size_t j = n;
    while (j > i && isspace((unsigned char)s[j - 1])) j--;
    size_t m = j - i;
    if (i > 0 && m > 0) memmove(s, s + i, m);
    s[m] = '\0';
    return s;
}

// hex helpers (decode ASCII hex to bytes, ignoring '-' ':' delimiters)
static inline int hex_nibble(char c) {
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F')
        return 10 + (c - 'A');
    return -1;
}

static inline int hex_compact(const char *in, char *out, size_t cap, size_t *olen) {
    // remove '-', ':' and whitespaces
    size_t n = 0;
    for (const char *p = in; *p; ++p) {
        char ch = *p;
        if (ch == '-' || ch == ':' || ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r')
            continue;
        if (n + 1 >= cap)
            return -1;
        out[n++] = ch;
    }
    out[n] = '\0';
    if (olen)
        *olen = n;
    return 0;
}

static inline int hex_decode(const char *hex, unsigned char **out, u32 *outlen) {
    if (!hex || !out || !outlen)
        return -1;
    char buf[512];
    size_t n = 0;
    if (hex_compact(hex, buf, sizeof(buf), &n) != 0)
        return -2;
    if (n % 2 != 0)
        return -3;
    size_t blen = n / 2;
    unsigned char *b = (unsigned char *)MALLOC(blen);
    if (!b)
        return -4;
    for (size_t i = 0; i < blen; i++) {
        int hi = hex_nibble(buf[i * 2]);
        int lo = hex_nibble(buf[i * 2 + 1]);
        if (hi < 0 || lo < 0) {
            FREE(b);
            return -5;
        }
        b[i] = (unsigned char)((hi << 4) | lo);
    }
    *out = b;
    *outlen = (u32)blen;
    return 0;
}



// --- Hash helpers (FNV-1a with Murmur-style finalization) ---
static inline u32 hash_fmix32(u32 h) {
    h ^= h >> 16;
    h *= 0x85ebca6bU;
    h ^= h >> 13;
    h *= 0xc2b2ae35U;
    h ^= h >> 16;
    return h;
}

static inline u64 hash_fmix64(u64 k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

static inline u32 hash32_from_bytes(const void *data, u32 len, u32 seed) {
    const u8 *p = (const u8 *)data;
    u32 h = seed ^ 2166136261u; // FNV offset basis folded with seed
    for (u32 i = 0; i < len; i++) {
        h ^= (u32)p[i];
        h *= 16777619u; // FNV prime
    }
    return hash_fmix32(h ^ len);
}

static inline u64 hash64_from_bytes(const void *data, u32 len, u64 seed) {
    const u8 *p = (const u8 *)data;
    u64 h = seed ^ 1469598103934665603ULL; // 64-bit FNV offset basis folded with seed
    for (u32 i = 0; i < len; i++) {
        h ^= (u64)p[i];
        h *= 1099511628211ULL; // 64-bit FNV prime
    }
    return hash_fmix64(h ^ len);
}


#endif // FLINTDB_INTERNAL_H