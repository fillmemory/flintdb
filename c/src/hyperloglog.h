#ifndef FLINTDB_HYPERLOGLOG_H
#define FLINTDB_HYPERLOGLOG_H

#include "flintdb.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

// HyperLogLog structure compatible with Java implementation (default b=14 serialization)
struct hyperloglog {
	int b;        // precision bits
	int m;        // number of buckets = 1<<b
	u8 *buckets;  // bucket array of length m
	double alphaMM; // alpha(m) * m^2
};

// Constructors / destructors
struct hyperloglog *hll_new(int b);
struct hyperloglog *hll_new_default(void); // b = 14
struct hyperloglog *hll_from_bytes(const u8 *buf, u32 len); // assumes default b=14 (len >= 1<<14)
void hll_free(struct hyperloglog *h);

// Operations
void hll_clear(struct hyperloglog *h);
void hll_merge(struct hyperloglog *h, const struct hyperloglog *other); // same b required
void hll_add_hash(struct hyperloglog *h, u64 hash);
void hll_add_cstr(struct hyperloglog *h, const char *s); // Java-compatible hashing of string
u64  hll_cardinality(const struct hyperloglog *h);

// Introspection
u32 hll_size_in_bytes(const struct hyperloglog *h); // equals m
u32 hll_bucket_count(const struct hyperloglog *h);  // equals m
u32 hll_precision(const struct hyperloglog *h);     // equals b

// Serialization (Java-compatible: raw buckets only, no header)
// Returns number of bytes written (m). If out==NULL, allocates and returns a new buffer via *outp.
int hll_write_bytes(const struct hyperloglog *h, u8 *out, u32 out_len);
u8 *hll_bytes_alloc(const struct hyperloglog *h); // caller must FREE()

// Hash helpers mirroring Java's String.hashCode() and the 64-bit mixing used in Java version
int32_t hll_java_string_hashcode(const char *s);
u64     hll_java_hash_to_64(int32_t h32);

#ifdef __cplusplus
}
#endif

#endif // FLINTDB_HYPERLOGLOG_H