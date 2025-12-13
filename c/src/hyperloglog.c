#include "hyperloglog.h"

#include "allocator.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>

// --- internal helpers ---
static inline double hll_get_alpha(int m) {
    switch (m) {
    case 16:
        return 0.673;
    case 32:
        return 0.697;
    case 64:
        return 0.709;
    default:
        return 0.7213 / (1.0 + 1.079 / (double)m);
    }
}

static inline int hll_clz64(u64 x) {
    // Count leading zeros in 64-bit; for x==0, return 64 like Java
    if (x == 0)
        return 64;
#if defined(__clang__) || defined(__GNUC__)
    return __builtin_clzll((unsigned long long)x);
#else
    // Portable fallback
    int n = 0;
    u64 mask = (u64)1 << 63;
    while (mask && (x & mask) == 0) {
        n++;
        mask >>= 1;
    }
    return n;
#endif
}

// --- public API ---

struct hyperloglog *hll_new(int b) {
    if (b < 4 || b > 16)
        return NULL;
    struct hyperloglog *h = (struct hyperloglog *)CALLOC(1, sizeof(struct hyperloglog));
    if (!h)
        return NULL;
    h->b = b;
    h->m = 1 << b;
    h->buckets = (u8 *)CALLOC(h->m, sizeof(u8));
    if (!h->buckets) {
        FREE(h);
        return NULL;
    }
    double alpha = hll_get_alpha(h->m);
    h->alphaMM = alpha * (double)h->m * (double)h->m;
    return h;
}

struct hyperloglog *hll_new_default(void) { return hll_new(14); }

struct hyperloglog *hll_from_bytes(const u8 *buf, u32 len) {
    int b = 14;
    int m = 1 << b;
    if (!buf || len < (u32)m)
        return NULL;
    struct hyperloglog *h = (struct hyperloglog *)CALLOC(1, sizeof(struct hyperloglog));
    if (!h)
        return NULL;
    h->b = b;
    h->m = m;
    h->buckets = (u8 *)CALLOC(m, sizeof(u8));
    if (!h->buckets) {
        FREE(h);
        return NULL;
    }
    memcpy(h->buckets, buf, (size_t)m);
    double alpha = hll_get_alpha(m);
    h->alphaMM = alpha * (double)m * (double)m;
    return h;
}

void hll_free(struct hyperloglog *h) {
    if (!h)
        return;
    if (h->buckets)
        FREE(h->buckets);
    FREE(h);
}

void hll_clear(struct hyperloglog *h) {
    if (!h || !h->buckets)
        return;
    memset(h->buckets, 0, (size_t)h->m);
}

void hll_merge(struct hyperloglog *h, const struct hyperloglog *other) {
    if (!h || !other)
        return;
    if (h->b != other->b || h->m != other->m)
        return; // incompatible; ignore
    for (int i = 0; i < h->m; i++) {
        if (other->buckets[i] > h->buckets[i])
            h->buckets[i] = other->buckets[i];
    }
}

void hll_add_hash(struct hyperloglog *h, u64 hash) {
    if (!h || !h->buckets)
        return;
    // Bucket index from low b bits (matching Java)
    int bucket_idx = (int)(hash & ((1ULL << h->b) - 1ULL));
    // Remaining bits for leading zero count
    u64 w = hash >> (unsigned)h->b;
    // lz = LeadingZeros64(w) - b + 1
    int lz = hll_clz64(w) - h->b + 1;
    if (lz < 0)
        lz = 0;
    if ((u8)lz > h->buckets[bucket_idx])
        h->buckets[bucket_idx] = (u8)lz;
}

void hll_add_cstr(struct hyperloglog *h, const char *s) {
    if (!h || !s)
        return;
    int32_t h32 = hll_java_string_hashcode(s);
    u64 h64 = hll_java_hash_to_64(h32);
    hll_add_hash(h, h64);
}

u64 hll_cardinality(const struct hyperloglog *h) {
    if (!h || !h->buckets)
        return 0;
    // sum = sum(2^{-bucket})
    double sum = 0.0;
    for (int i = 0; i < h->m; i++) {
        sum += pow(2.0, -(double)h->buckets[i]);
    }
    if (sum == 0.0)
        return 0;
    double raw = h->alphaMM / sum;

    // small range correction
    if (raw <= 2.5 * (double)h->m) {
        int zeros = 0;
        for (int i = 0; i < h->m; i++)
            if (h->buckets[i] == 0)
                zeros++;
        if (zeros != 0) {
            double est = (double)h->m * log((double)h->m / (double)zeros);
            if (est < 0)
                return 0;
            return (u64)llround(est);
        }
    }

    const double two32 = (double)(1ULL << 32);
    if (raw <= (1.0 / 30.0) * two32) {
        if (raw < 0)
            return 0;
        return (u64)llround(raw);
    }

    double v = -two32 * log(1.0 - raw / two32);
    if (v < 0)
        return 0;
    return (u64)llround(v);
}

u32 hll_size_in_bytes(const struct hyperloglog *h) { return h ? (u32)h->m : 0; }
u32 hll_bucket_count(const struct hyperloglog *h) { return h ? (u32)h->m : 0; }
u32 hll_precision(const struct hyperloglog *h) { return h ? (u32)h->b : 0; }

int hll_write_bytes(const struct hyperloglog *h, u8 *out, u32 out_len) {
    if (!h || !h->buckets)
        return 0;
    if (!out || out_len < (u32)h->m)
        return 0;
    memcpy(out, h->buckets, (size_t)h->m);
    return h->m;
}

u8 *hll_bytes_alloc(const struct hyperloglog *h) {
    if (!h || !h->buckets)
        return NULL;
    u8 *buf = (u8 *)MALLOC((size_t)h->m);
    if (!buf)
        return NULL;
    memcpy(buf, h->buckets, (size_t)h->m);
    return buf;
}

// --- hashing helpers to mirror Java implementation ---
int32_t hll_java_string_hashcode(const char *s) {
    // Java String.hashCode(): h = 31*h + char (char treated as unsigned byte here)
    int32_t h = 0;
    if (!s)
        return 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; ++p) {
        h = h * 31 + (int32_t)(*p);
    }
    return h;
}

u64 hll_java_hash_to_64(int32_t h32) {
    // Sign-extend 32-bit to 64-bit and mix like Java code
    long long signed64 = (long long)h32; // sign-extended
    u64 h = (u64)signed64;
    h ^= h >> 32;
    h *= 0x9e3779b97f4a7c15ULL;
    h ^= h >> 32;
    h *= 0x9e3779b97f4a7c15ULL;
    h ^= h >> 32;
    return h;
}
