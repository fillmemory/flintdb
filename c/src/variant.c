#include "flintdb.h"
#include "runtime.h"
#include "buffer.h"
#include "allocator.h"
#include "internal.h"
#include "simd.h"
#include <stdlib.h>
#include <errno.h>
#include <ctype.h>
#include <stdio.h>
#include <pthread.h>


// Optional small-string pooling for variants
// Enable with: add -DVARIANT_USE_STRPOOL to CFLAGS
#ifndef VARIANT_EMPTY_STR_DEFINED
// Shared immutable empty string buffer to avoid per-call 1-byte allocations.
// Treated as non-owned (owned=0) wherever referenced.
static const char VARIANT_EMPTY_STR[] = "";
#define VARIANT_EMPTY_STR_DEFINED 1
#endif

// Thread-local temporary string buffer (always enabled for flintdb_variant_string_get)
static pthread_key_t VARIANT_TEMPSTR_KEY;
static pthread_once_t VARIANT_TEMPSTR_KEY_ONCE = PTHREAD_ONCE_INIT;
static _Thread_local char *temp_str_buf = NULL;
static _Thread_local size_t temp_str_capacity = 0;
static void variant__tempstr_destroy(void *p) {
	if (p) {
		FREE(p);
		DEBUG("Variant temp string buffer destroyed");
	}
}
static void variant__tempstr_make_key(void) {
	(void)pthread_key_create(&VARIANT_TEMPSTR_KEY, variant__tempstr_destroy);
	DEBUG("Variant temp string buffer key created");
}

#ifdef VARIANT_USE_STRPOOL
// Tunables: sized for common short strings like column values, UUID text, etc.
#ifndef VARIANT_STRPOOL_STR_SIZE
#define VARIANT_STRPOOL_STR_SIZE 256u
#endif
#ifndef VARIANT_STRPOOL_CAPACITY
#define VARIANT_STRPOOL_CAPACITY 4096u
#endif
#ifndef VARIANT_STRPOOL_PRELOAD
#define VARIANT_STRPOOL_PRELOAD 16u
#endif

// Thread-local string pool with cleanup on thread exit (POSIX pthread TLS).
// This path works with MinGW (pthread-win32) as well.
static pthread_key_t VARIANT_STRPOOL_KEY;
static pthread_once_t VARIANT_STRPOOL_KEY_ONCE = PTHREAD_ONCE_INIT;
static void variant__strpool_destroy(void *p) {
	if (p) ((struct string_pool*)p)->free((struct string_pool*)p);
	DEBUG("Variant string pool destroyed");
}
static void variant__strpool_make_key(void) {
	(void)pthread_key_create(&VARIANT_STRPOOL_KEY, variant__strpool_destroy);
	DEBUG("Variant string pool created");
}
// Thread-local cached pool pointer to avoid repeated pthread_getspecific calls
static _Thread_local struct string_pool *VARIANT_STRPOOL_CACHED = NULL;

// Cleanup function to explicitly free the main thread's variant string pool
void variant_strpool_cleanup(void) {
	// Only cleanup if the pool was actually created
	if (VARIANT_STRPOOL_CACHED != NULL) {
		struct string_pool *pool = VARIANT_STRPOOL_CACHED;
		pool->free(pool);
		VARIANT_STRPOOL_CACHED = NULL;
	}
}

static inline struct string_pool * variant__pool(void) {
	if (LIKELY(VARIANT_STRPOOL_CACHED != NULL)) {
		return VARIANT_STRPOOL_CACHED;
	}
	(void)pthread_once(&VARIANT_STRPOOL_KEY_ONCE, variant__strpool_make_key);
	struct string_pool *pool = (struct string_pool*)pthread_getspecific(VARIANT_STRPOOL_KEY);
	if (!pool) {
		pool = string_pool_create(VARIANT_STRPOOL_CAPACITY, VARIANT_STRPOOL_STR_SIZE, VARIANT_STRPOOL_PRELOAD);
		(void)pthread_setspecific(VARIANT_STRPOOL_KEY, pool);
	}
	VARIANT_STRPOOL_CACHED = pool;
	return pool;
}

// ownership marker: 0 = ref, 1 = malloc, 2 = pool
#define OWNED_HEAP 1
#define OWNED_POOL 2

// Inline pool borrow for hot path performance
static inline char *variant__pool_borrow_inline(struct string_pool *pool) {
	if (LIKELY(pool->top > 0)) {
		return pool->items[--pool->top];
	}
	return (char *)MALLOC(pool->str_size);
}

// Inline pool return for hot path performance
static inline void variant__pool_return_inline(struct string_pool *pool, char *s) {
	if (LIKELY(pool->top < pool->capacity)) {
		pool->items[pool->top++] = s;
	} else {
		FREE(s);
	}
}

static inline char *variant__alloc_for(u32 needed, i8 *owned_out) {
	if (needed == 0) {
		// Use shared immutable empty buffer; mark as non-owned.
		if (owned_out) *owned_out = 0;
		return (char *)VARIANT_EMPTY_STR;
	}
	u32 bytes = needed + 1; // room for trailing NUL for convenience
	if (bytes <= VARIANT_STRPOOL_STR_SIZE) {
		struct string_pool *pool = variant__pool();
		if (pool) {
			char *p = variant__pool_borrow_inline(pool);
			if (p) {
				if (owned_out) *owned_out = OWNED_POOL;
				return p;
			}
		}
	}
	// fallback to heap
	char *p = (char*)MALLOC(bytes);
	if (owned_out) *owned_out = OWNED_HEAP;
	return p;
}

static inline void variant__free_owned(char *p, i8 owned) {
	if (!p) return;
	if (owned == OWNED_POOL) {
		struct string_pool *pool = variant__pool();
		if (pool) variant__pool_return_inline(pool, p);
		else FREE(p); // very unlikely; fallback
	} else if (owned) {
		FREE(p);
	}
}
#else
// Stub function when VARIANT_USE_STRPOOL is not defined
void variant_strpool_cleanup(void) {
	// No-op when string pool is not enabled
}
#endif // VARIANT_USE_STRPOOL

static inline char * variant_error_set(const struct flintdb_variant *v, const char *msg) {
    if (!v) return NULL;
    snprintf(TL_ERROR, ERROR_BUFSZ - 1, "%s", msg);
    return TL_ERROR;
}

// Cleanup function for temp string buffer (always available)
void variant_tempstr_cleanup(void) {
	if (temp_str_buf != NULL) {
		FREE(temp_str_buf);
		temp_str_buf = NULL;
		temp_str_capacity = 0;
	}
}

// Free only when current variant owns a heap buffer
static inline void variant_release_if_owned(struct flintdb_variant *v) {
	if (!v) return;
	if ((v->type == VARIANT_STRING || v->type == VARIANT_BYTES || v->type == VARIANT_UUID || v->type == VARIANT_IPV6)
		&& v->value.b.owned && v->value.b.data) {
#ifdef VARIANT_USE_STRPOOL
		variant__free_owned(v->value.b.data, v->value.b.owned);
#else
		FREE(v->value.b.data);
#endif
		v->value.b.data = NULL;
		v->value.b.length = 0;
		v->value.b.owned = 0;
	}
}

void flintdb_variant_init(struct flintdb_variant *v) {
	if (!v) return;
	v->type = VARIANT_NULL;
	v->value.i = 0;
}

HOT_PATH
void flintdb_variant_free(struct flintdb_variant *v) {
	if (!v) return;
	variant_release_if_owned(v);
	v->type = VARIANT_NULL;
	v->value.i = 0;
}

int flintdb_variant_i32_set(struct flintdb_variant *v, i32 val) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_INT32;
	v->value.i = (i64)val;
	return 0;
}

int flintdb_variant_u32_set(struct flintdb_variant *v, u32 val) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_UINT32;
	v->value.i = (i64)val;
	return 0;
}

int flintdb_variant_i8_set(struct flintdb_variant *v, i8 val) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_INT8;
	v->value.i = (i64)val;
	return 0;
}

int flintdb_variant_u8_set(struct flintdb_variant *v, u8 val) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_UINT8;
	v->value.i = (i64)val;
	return 0;
}

int flintdb_variant_i16_set(struct flintdb_variant *v, i16 val) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_INT16;
	v->value.i = (i64)val;
	return 0;
}

int flintdb_variant_u16_set(struct flintdb_variant *v, u16 val) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_UINT16;
	v->value.i = (i64)val;
	return 0;
}

int flintdb_variant_i64_set(struct flintdb_variant *v, i64 val) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_INT64;
	v->value.i = val;
	return 0;
}

int flintdb_variant_f64_set(struct flintdb_variant *v, f64 val) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_DOUBLE;
	v->value.f = val;
	return 0;
}

HOT_PATH
int flintdb_variant_string_set(struct flintdb_variant *v, const char *str, u32 length) {
	if (UNLIKELY(!v)) return -1;
	
#ifdef VARIANT_USE_STRPOOL
	// Ultra-fast path: reuse existing pool buffer (most common case)
	if (LIKELY(v->type == VARIANT_STRING && v->value.b.owned == OWNED_POOL)) {
		v->value.b.length = length;
		if (LIKELY(length)) simd_memcpy(v->value.b.data, str, length);
		v->value.b.data[length] = '\0';
		return 0;
	}
#endif
	
	// Empty string
	if (UNLIKELY(length == 0)) {
		variant_release_if_owned(v);
		v->type = VARIANT_STRING;
		v->value.b.length = 0;
		v->value.b.data = (char *)VARIANT_EMPTY_STR;
		v->value.b.owned = 0;
		return 0;
	}
	
#ifdef VARIANT_USE_STRPOOL
	// Heap reuse for large strings
	if (v->type == VARIANT_STRING && v->value.b.owned == OWNED_HEAP && v->value.b.data) {
		char *buf = (char *)REALLOC(v->value.b.data, (size_t)length + 1);
		if (LIKELY(buf)) {
			v->value.b.length = length;
			v->value.b.data = buf;
			simd_memcpy(buf, str, length);
			buf[length] = '\0';
			return 0;
		}
	}
	
	// Allocate new buffer
	variant_release_if_owned(v);
	
	char *buf;
	i8 owned;
	if (LIKELY(length <= VARIANT_STRPOOL_STR_SIZE)) {
		struct string_pool *pool = variant__pool();
		buf = variant__pool_borrow_inline(pool);
		owned = OWNED_POOL;
	} else {
		buf = (char *)MALLOC((size_t)length + 1);
		owned = OWNED_HEAP;
	}
	
	if (UNLIKELY(!buf)) return -1;
	
	v->type = VARIANT_STRING;
	v->value.b.length = length;
	v->value.b.data = buf;
	v->value.b.owned = owned;
	simd_memcpy(buf, str, length);
	buf[length] = '\0';
#else
	// Non-pool path
	if (v->type == VARIANT_STRING && v->value.b.owned && v->value.b.data) {
		char *buf = (char *)REALLOC(v->value.b.data, (size_t)length + 1);
		if (LIKELY(buf)) {
			v->value.b.length = length;
			v->value.b.data = buf;
			simd_memcpy(buf, str, length);
			buf[length] = '\0';
			return 0;
		}
	}
	
	variant_release_if_owned(v);
	char *buf = (char *)MALLOC((size_t)length + 1);
	if (UNLIKELY(!buf)) return -1;
	
	v->type = VARIANT_STRING;
	v->value.b.length = length;
	v->value.b.data = buf;
	v->value.b.owned = 1;
	v->value.b.sflag = VARIANT_SFLAG_NULL_TERMINATED; // null-terminated
	simd_memcpy(buf, str, length);
	buf[length] = '\0';
#endif
	return 0;
}

int flintdb_variant_string_ref_set(struct flintdb_variant *v, const char *str, u32 length, enum flintdb_variant_sflag sflag) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_STRING;
	v->value.b.length = length;
	v->value.b.data = (char *)str; // non-owning reference into external buffer
	v->value.b.owned = 0;
	v->value.b.sflag = sflag; // 1; // not null-terminated
	return 0;
}

int flintdb_variant_length(const struct flintdb_variant *v) {
	if (!v) return -1;
	switch (v->type) {
		case VARIANT_STRING:
		case VARIANT_BYTES:
		case VARIANT_UUID:
		case VARIANT_IPV6:
			return (int)v->value.b.length;
		case VARIANT_DECIMAL:
			return (int)v->value.d.length;
		default:
			return -1;
	}
}

int flintdb_variant_decimal_set(struct flintdb_variant *v, u8 sign, u8 scale, struct flintdb_decimal  data) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_DECIMAL;
	v->value.d = data;
	v->value.d.sign = sign;
	v->value.d.scale = scale;
	return 0;
}

int flintdb_variant_bytes_set(struct flintdb_variant *v, const char *data, u32 length) {
	if (!v) return -1;
	// Reuse existing owned buffer when possible
	int can_reuse = (v->type == VARIANT_BYTES && v->value.b.owned && v->value.b.data != NULL);
	if (can_reuse) {
#ifdef VARIANT_USE_STRPOOL
		if (v->value.b.owned == OWNED_POOL) {
			if (length + 1u <= VARIANT_STRPOOL_STR_SIZE) {
				v->type = VARIANT_BYTES;
				v->value.b.length = length;
				if (data && length) simd_memcpy(v->value.b.data, data, length);
				v->value.b.data[length] = '\0';
				return 0;
			}
			variant_release_if_owned(v);
			can_reuse = 0;
		}
#endif
	} else {
		variant_release_if_owned(v);
	}
	v->type = VARIANT_BYTES;
	v->value.b.length = length;
	char *buf = NULL;
    if (length == 0) {
        v->value.b.data = (char *)VARIANT_EMPTY_STR;
        v->value.b.owned = 0;
        return 0;
    }
	if (can_reuse) {
#ifdef VARIANT_USE_STRPOOL
		if (v->value.b.owned == OWNED_HEAP) {
			buf = (char *)REALLOC(v->value.b.data, (size_t)length + 1);
		}
#else
		buf = (char *)REALLOC(v->value.b.data, (size_t)length + 1);
#endif
	}
	if (!buf) {
#ifdef VARIANT_USE_STRPOOL
		i8 owned = 0;
		buf = variant__alloc_for(length, &owned);
		if (!buf) return -1;
		v->value.b.owned = owned;
#else
		buf = (char *)MALLOC((size_t)length + 1);
		if (!buf) return -1;
		v->value.b.owned = 1;
#endif
	} else {
#ifdef VARIANT_USE_STRPOOL
		v->value.b.owned = OWNED_HEAP;
#endif
	}
	if (data && length) simd_memcpy(buf, data, length);
	buf[length] = '\0';
	v->value.b.data = buf;
	return 0;
}

int flintdb_variant_date_set(struct flintdb_variant *v, time_t val) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_DATE;
	v->value.t = val;
	return 0;
}

int flintdb_variant_time_set(struct flintdb_variant *v, time_t val) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_TIME;
	v->value.t = val;
	return 0;
}

int flintdb_variant_uuid_set(struct flintdb_variant *v, const char *data, u32 length) {
	if (!v) return -1;
	int can_reuse = (v->type == VARIANT_UUID && v->value.b.owned && v->value.b.data != NULL);
	if (can_reuse) {
#ifdef VARIANT_USE_STRPOOL
		if (v->value.b.owned == OWNED_POOL) {
			if (length + 1u <= VARIANT_STRPOOL_STR_SIZE) {
				v->type = VARIANT_UUID;
				v->value.b.length = length;
				if (data && length) simd_memcpy(v->value.b.data, data, length);
				v->value.b.data[length] = '\0';
				return 0;
			}
			variant_release_if_owned(v);
			can_reuse = 0;
		}
#endif
	} else {
		variant_release_if_owned(v);
	}
	v->type = VARIANT_UUID;
	v->value.b.length = length;
	char *buf = NULL;
    if (length == 0) {
        v->value.b.data = (char *)VARIANT_EMPTY_STR;
        v->value.b.owned = 0;
        return 0;
    }
	if (can_reuse) {
#ifdef VARIANT_USE_STRPOOL
		if (v->value.b.owned == OWNED_HEAP) {
			buf = (char *)REALLOC(v->value.b.data, (size_t)length + 1);
		}
#else
		buf = (char *)REALLOC(v->value.b.data, (size_t)length + 1);
#endif
	}
	if (!buf) {
#ifdef VARIANT_USE_STRPOOL
		i8 owned = 0;
		buf = variant__alloc_for(length, &owned);
		if (!buf) return -1;
		v->value.b.owned = owned;
#else
		buf = (char *)MALLOC((size_t)length + 1);
		if (!buf) return -1;
		v->value.b.owned = 1;
#endif
	} else {
#ifdef VARIANT_USE_STRPOOL
		v->value.b.owned = OWNED_HEAP;
#endif
	}
	if (data && length) simd_memcpy(buf, data, length);
	buf[length] = '\0';
	v->value.b.data = buf;
	return 0;
}

int flintdb_variant_ipv6_set(struct flintdb_variant *v, const char *data, u32 length) {
	if (!v) return -1;
	int can_reuse = (v->type == VARIANT_IPV6 && v->value.b.owned && v->value.b.data != NULL);
	if (can_reuse) {
#ifdef VARIANT_USE_STRPOOL
		if (v->value.b.owned == OWNED_POOL) {
			if (length + 1u <= VARIANT_STRPOOL_STR_SIZE) {
				v->type = VARIANT_IPV6;
				v->value.b.length = length;
				if (data && length) simd_memcpy(v->value.b.data, data, length);
				v->value.b.data[length] = '\0';
				return 0;
			}
			variant_release_if_owned(v);
			can_reuse = 0;
		}
#endif
	} else {
		variant_release_if_owned(v);
	}
	v->type = VARIANT_IPV6;
	v->value.b.length = length;
	char *buf = NULL;
    if (length == 0) {
        v->value.b.data = (char *)VARIANT_EMPTY_STR;
        v->value.b.owned = 0;
        return 0;
    }
	if (can_reuse) {
#ifdef VARIANT_USE_STRPOOL
		if (v->value.b.owned == OWNED_HEAP) {
			buf = (char *)REALLOC(v->value.b.data, (size_t)length + 1);
		}
#else
		buf = (char *)REALLOC(v->value.b.data, (size_t)length + 1);
#endif
	}
	if (!buf) {
#ifdef VARIANT_USE_STRPOOL
		i8 owned = 0;
		buf = variant__alloc_for(length, &owned);
		if (!buf) return -1;
		v->value.b.owned = owned;
#else
		buf = (char *)MALLOC((size_t)length + 1);
		if (!buf) return -1;
		v->value.b.owned = 1;
#endif
	} else {
#ifdef VARIANT_USE_STRPOOL
		v->value.b.owned = OWNED_HEAP;
#endif
	}
	if (data && length) simd_memcpy(buf, data, length);
	buf[length] = '\0';
	v->value.b.data = buf;
	return 0;
}

int flintdb_variant_null_set(struct flintdb_variant *v) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_NULL;
	return 0;
}

int flintdb_variant_zero_set(struct flintdb_variant *v) {
	if (!v) return -1;
	variant_release_if_owned(v);
	v->type = VARIANT_ZERO;
	v->value.i = 0;
	return 0;
}

int flintdb_variant_copy(struct flintdb_variant *dest, const struct flintdb_variant *src) {
	if (!dest || !src) return -1;
	flintdb_variant_free(dest);
	dest->type = src->type;
	switch (src->type) {
		case VARIANT_INT8:
		case VARIANT_UINT8:
		case VARIANT_INT16:
		case VARIANT_UINT16:
		case VARIANT_INT32:
		case VARIANT_UINT32:
		case VARIANT_INT64:
			dest->value.i = src->value.i;
			break;
		case VARIANT_DOUBLE:
			dest->value.f = src->value.f;
			break;
		case VARIANT_DECIMAL:
			dest->value.d = src->value.d;
			break;
		case VARIANT_STRING:
		case VARIANT_BYTES:
		case VARIANT_UUID:
		case VARIANT_IPV6: {
			u32 len = src->value.b.length;
			dest->value.b.length = len;
            if (len == 0 || !src->value.b.data) {
                dest->value.b.data = (char *)VARIANT_EMPTY_STR;
                dest->value.b.owned = 0;
            } else {
#ifdef VARIANT_USE_STRPOOL
				i8 owned = 0;
				char *buf = variant__alloc_for(len, &owned);
				if (!buf) return -1;
				simd_memcpy(buf, src->value.b.data, len);
				buf[len] = '\0';
				dest->value.b.data = buf;
				dest->value.b.owned = owned;
#else
				char *buf = (char *)MALLOC((size_t)len + 1);
				if (!buf) return -1;
				simd_memcpy(buf, src->value.b.data, len);
				buf[len] = '\0';
				dest->value.b.data = buf;
				dest->value.b.owned = 1;
#endif
			}
			break;
		}
		case VARIANT_DATE:
		case VARIANT_TIME:
			dest->value.t = src->value.t;
			break;
		case VARIANT_NULL:
		case VARIANT_ZERO:
		default:
			dest->value.i = 0;
			break;
	}
	return 0;
}

const char * flintdb_variant_string_get(const struct flintdb_variant *v) {
	if (!v) return NULL;
	if (v->type == VARIANT_STRING) {
		// If string is not null-terminated (sflag=1), create a temporary null-terminated copy
		if (VARIANT_SFLAG_NOT_NULL_TERMINATED == v->value.b.sflag) {
			// Use thread-local buffer for non-null-terminated strings
			// Initialize pthread key for cleanup on thread exit
			(void)pthread_once(&VARIANT_TEMPSTR_KEY_ONCE, variant__tempstr_make_key);
			
			size_t needed = (size_t)v->value.b.length + 1;
			if (needed > temp_str_capacity) {
				char *new_buf = (char*)REALLOC(temp_str_buf, needed);
				if (!new_buf) return NULL;
				temp_str_buf = new_buf;
				temp_str_capacity = needed;
				// Register buffer for cleanup when thread exits
				(void)pthread_setspecific(VARIANT_TEMPSTR_KEY, temp_str_buf);
			}
			
			simd_memcpy(temp_str_buf, v->value.b.data, v->value.b.length);
			temp_str_buf[v->value.b.length] = '\0';
			return temp_str_buf;
		}
		return v->value.b.data;
	}
	// Provide string views for numeric and date/time types only
	static __thread char s_buf[64];
	switch (v->type) {
		case VARIANT_DECIMAL: {
			// Render DECIMAL to string using scale-aware conversion
			// 64 bytes is enough for up to 32 digits + sign + dot
			s_buf[0] = '\0';
			flintdb_decimal_to_string(&v->value.d, s_buf, sizeof(s_buf));
			return s_buf[0] ? s_buf : "0"; // fallback to "0" if empty
		}
		case VARIANT_INT8: case VARIANT_UINT8:
		case VARIANT_INT16: case VARIANT_UINT16:
		case VARIANT_INT32: case VARIANT_UINT32:
		case VARIANT_INT64:
			// value.i stores the integer payload
			snprintf(s_buf, sizeof(s_buf), "%lld", (long long)v->value.i);
			return s_buf;
		case VARIANT_DOUBLE:
			// Use %.17g for reasonable round-trip fidelity
			snprintf(s_buf, sizeof(s_buf), "%.17g", (double)v->value.f);
			return s_buf;
		case VARIANT_ZERO:
			snprintf(s_buf, sizeof(s_buf), "0");
			return s_buf;
		case VARIANT_DATE:
		case VARIANT_TIME:
			// Format as epoch seconds
			snprintf(s_buf, sizeof(s_buf), "%lld", (long long)v->value.t);
			return s_buf;
		default:
			break;
	}
	return NULL;
}

i8 flintdb_variant_i8_get(const struct flintdb_variant *v, char **e) {
	if (!v) { if (e) *e = "variant is NULL"; return 0; }
	// Accept any numeric type and cast
	switch (v->type) {
		case VARIANT_INT8: case VARIANT_UINT8:
		case VARIANT_INT16: case VARIANT_UINT16:
		case VARIANT_INT32: case VARIANT_UINT32:
		case VARIANT_INT64: return (i8)(v->value.i);
		case VARIANT_DOUBLE: return (i8)(v->value.f);
		case VARIANT_STRING: {
			i64 tmp;
			if (parse_i64(v->value.b.data, v->value.b.length, &tmp) == 0) return (i8)tmp;
			if (e) *e = variant_error_set(v, "invalid numeric string");
			return 0;
		}
		case VARIANT_ZERO: return (i8)0;
		default: break;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected numeric");
	return 0;
}

u8 flintdb_variant_u8_get(const struct flintdb_variant *v, char **e) {
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return 0; }
	switch (v->type) {
		case VARIANT_INT8: case VARIANT_UINT8:
		case VARIANT_INT16: case VARIANT_UINT16:
		case VARIANT_INT32: case VARIANT_UINT32:
		case VARIANT_INT64: return (u8)(v->value.i);
		case VARIANT_DOUBLE: return (u8)(v->value.f);
		case VARIANT_STRING: {
			i64 tmp;
			if (parse_i64(v->value.b.data, v->value.b.length, &tmp) == 0) return (u8)tmp;
			if (e) *e = variant_error_set(v, "invalid numeric string");
			return 0;
		}
		case VARIANT_ZERO: return (u8)0;
		default: break;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected numeric");
	return 0;
}

i16 flintdb_variant_i16_get(const struct flintdb_variant *v, char **e) {
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return 0; }
	switch (v->type) {
		case VARIANT_INT8: case VARIANT_UINT8:
		case VARIANT_INT16: case VARIANT_UINT16:
		case VARIANT_INT32: case VARIANT_UINT32:
		case VARIANT_INT64: return (i16)(v->value.i);
		case VARIANT_DOUBLE: return (i16)(v->value.f);
		case VARIANT_STRING: {
			i64 tmp;
			if (parse_i64(v->value.b.data, v->value.b.length, &tmp) == 0) return (i16)tmp;
			if (e) *e = variant_error_set(v, "invalid numeric string");
			return 0;
		}
		case VARIANT_ZERO: return (i16)0;
		default: break;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected numeric");
	return 0;
}

u16 flintdb_variant_u16_get(const struct flintdb_variant *v, char **e) {
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return 0; }
	switch (v->type) {
		case VARIANT_INT8: case VARIANT_UINT8:
		case VARIANT_INT16: case VARIANT_UINT16:
		case VARIANT_INT32: case VARIANT_UINT32:
		case VARIANT_INT64: return (u16)(v->value.i);
		case VARIANT_DOUBLE: return (u16)(v->value.f);
		case VARIANT_STRING: {
			i64 tmp;
			if (parse_i64(v->value.b.data, v->value.b.length, &tmp) == 0) return (u16)tmp;
			if (e) *e = variant_error_set(v, "invalid numeric string");
			return 0;
		}
		case VARIANT_ZERO: return (u16)0;
		default: break;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected numeric");
	return 0;
}

i32 variant_i32_get(const struct flintdb_variant *v, char **e) {
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return 0; }
	switch (v->type) {
		case VARIANT_INT8: case VARIANT_UINT8:
		case VARIANT_INT16: case VARIANT_UINT16:
		case VARIANT_INT32: case VARIANT_UINT32:
		case VARIANT_INT64: return (i32)(v->value.i);
		case VARIANT_DOUBLE: return (i32)(v->value.f);
		case VARIANT_STRING: {
			i64 tmp;
			if (parse_i64(v->value.b.data, v->value.b.length, &tmp) == 0) return (i32)tmp;
			if (e) *e = variant_error_set(v, "invalid numeric string");
			return 0;
		}
		case VARIANT_ZERO: return (i32)0;
		default: break;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected numeric");
	return 0;
}

u32 flintdb_variant_u32_get(const struct flintdb_variant *v, char **e) {
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return 0; }
	switch (v->type) {
		case VARIANT_INT8: case VARIANT_UINT8:
		case VARIANT_INT16: case VARIANT_UINT16:
		case VARIANT_INT32: case VARIANT_UINT32:
		case VARIANT_INT64: return (u32)(v->value.i);
		case VARIANT_DOUBLE: return (u32)(v->value.f);
		case VARIANT_STRING: {
			i64 tmp;
			if (parse_i64(v->value.b.data, v->value.b.length, &tmp) == 0) return (u32)tmp;
			if (e) *e = variant_error_set(v, "invalid numeric string");
			return 0;
		}
		case VARIANT_ZERO: return (u32)0;
		default: break;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected numeric");
	return 0;
}

i64 flintdb_variant_i64_get(const struct flintdb_variant *v, char **e) {
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return 0; }
	switch (v->type) {
		case VARIANT_INT8: case VARIANT_UINT8:
		case VARIANT_INT16: case VARIANT_UINT16:
		case VARIANT_INT32: case VARIANT_UINT32:
		case VARIANT_INT64: return (i64)(v->value.i);
		case VARIANT_DOUBLE: return (i64)(v->value.f);
		case VARIANT_STRING: {
			i64 tmp;
			if (parse_i64(v->value.b.data, v->value.b.length, &tmp) == 0) return tmp;
			if (e) *e = variant_error_set(v, "invalid numeric string");
			return 0;
		}
		case VARIANT_ZERO: return (i64)0;
		default: break;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected numeric");
	return 0;
}

f64 flintdb_variant_f64_get(const struct flintdb_variant *v, char **e) {
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return 0.0; }
	switch (v->type) {
		case VARIANT_DOUBLE: return v->value.f;
		case VARIANT_INT8: case VARIANT_UINT8:
		case VARIANT_INT16: case VARIANT_UINT16:
		case VARIANT_INT32: case VARIANT_UINT32:
		case VARIANT_INT64: return (f64)(v->value.i);
		case VARIANT_STRING: {
			f64 tmp;
			if (parse_f64(v->value.b.data, v->value.b.length, &tmp) == 0) return tmp;
			if (e) *e = variant_error_set(v, "invalid numeric string");
			return 0.0;
		}
		case VARIANT_ZERO: return 0.0;
		default: break;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected numeric");
	return 0.0;
}

struct flintdb_decimal  flintdb_variant_decimal_get(const struct flintdb_variant *v, char **e) {
	struct flintdb_decimal  d = {0};
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return d; }
	if (v->type == VARIANT_DECIMAL) return v->value.d;
	if (e) *e = variant_error_set(v, "type mismatch: expected DECIMAL");
	return d;
}

const char * flintdb_variant_bytes_get(const struct flintdb_variant *v, u32 *length, char **e) {
	if (length) *length = 0;
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return NULL; }
	if (v->type == VARIANT_BYTES) {
		if (length) *length = v->value.b.length;
		return v->value.b.data;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected BYTES");
	return NULL;
}

time_t flintdb_variant_date_get(const struct flintdb_variant *v, char **e) {
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return (time_t)0; }
	if (v->type == VARIANT_DATE) return v->value.t;
	if (e) *e = variant_error_set(v, "type mismatch: expected DATE");
	return (time_t)0;
}

time_t flintdb_variant_time_get(const struct flintdb_variant *v, char **e) {
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return (time_t)0; }
	if (v->type == VARIANT_TIME) return v->value.t;
	if (e) *e = variant_error_set(v, "type mismatch: expected TIME");
	return (time_t)0;
}

const char * flintdb_variant_uuid_get(const struct flintdb_variant *v, u32 *length, char **e) {
	if (length) *length = 0;
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return NULL; }
	if (v->type == VARIANT_UUID) {
		if (length) *length = v->value.b.length;
		return v->value.b.data;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected UUID");
	return NULL;
}

const char * flintdb_variant_ipv6_get(const struct flintdb_variant *v, u32 *length, char **e) {
	if (length) *length = 0;
	if (!v) { if (e) *e = variant_error_set(v, "variant is NULL"); return NULL; }
	if (v->type == VARIANT_IPV6) {
		if (length) *length = v->value.b.length;
		return v->value.b.data;
	}
	if (e) *e = variant_error_set(v, "type mismatch: expected IPV6");
	return NULL;
}

i8 flintdb_variant_is_null(const struct flintdb_variant *v) {
	if (!v) return 1;
	return v->type == VARIANT_NULL ? 1 : 0;
}

HOT_PATH
int flintdb_variant_compare(const struct flintdb_variant *a, const struct flintdb_variant *b) {
	if (a == b) return 0;
	if (!a) return -1;
	if (!b) return 1;

	// Same type fast path
	if (a->type == b->type) {
		switch (a->type) {
			case VARIANT_NULL:
			case VARIANT_ZERO:
				return 0;
			case VARIANT_INT8: case VARIANT_UINT8:
			case VARIANT_INT16: case VARIANT_UINT16:
			case VARIANT_INT32: case VARIANT_UINT32:
			case VARIANT_INT64: {
				if (a->value.i < b->value.i) return -1;
				if (a->value.i > b->value.i) return 1;
				return 0;
			}
			case VARIANT_DOUBLE: {
				if (a->value.f < b->value.f) return -1;
				if (a->value.f > b->value.f) return 1;
				return 0;
			}
			case VARIANT_STRING: {
				u32 la = a->value.b.length;
				u32 lb = b->value.b.length;
				u32 lm = la < lb ? la : lb;
				int c = simd_memcmp(a->value.b.data, b->value.b.data, lm);
				if (c != 0) return (c < 0) ? -1 : 1;
				if (la < lb) return -1;
				if (la > lb) return 1;
				return 0;
			}
			case VARIANT_BYTES: {
				u32 la = a->value.b.length;
				u32 lb = b->value.b.length;
				u32 lm = la < lb ? la : lb;
				int c = simd_memcmp(a->value.b.data, b->value.b.data, lm);
				if (c != 0) return (c < 0) ? -1 : 1;
				if (la < lb) return -1;
				if (la > lb) return 1;
				return 0;
			}
			case VARIANT_DECIMAL: {
				// Deterministic compare: sign, then scale, then length, then data bytes
				if (a->value.d.sign != b->value.d.sign)
					return (a->value.d.sign < b->value.d.sign) ? -1 : 1;
				if (a->value.d.scale != b->value.d.scale)
					return (a->value.d.scale < b->value.d.scale) ? -1 : 1;
				if (a->value.d.length != b->value.d.length)
					return (a->value.d.length < b->value.d.length) ? -1 : 1;
				int c = simd_memcmp(a->value.d.data, b->value.d.data, a->value.d.length);
				if (c != 0) return (c < 0) ? -1 : 1;
				return 0;
			}
			case VARIANT_DATE:
			case VARIANT_TIME: {
				if (a->value.t < b->value.t) return -1;
				if (a->value.t > b->value.t) return 1;
				return 0;
			}
			case VARIANT_UUID:
			case VARIANT_IPV6: {
				u32 la = a->value.b.length;
				u32 lb = b->value.b.length;
				u32 lm = la < lb ? la : lb;
				int c = simd_memcmp(a->value.b.data, b->value.b.data, lm);
				if (c != 0) return (c < 0) ? -1 : 1;
				if (la < lb) return -1;
				if (la > lb) return 1;
				return 0;
			}
			default:
				break;
		}
	}

	// Handle NIL ordering: NIL < anything else
	if (a->type == VARIANT_NULL && b->type != VARIANT_NULL) return -1;
	if (b->type == VARIANT_NULL && a->type != VARIANT_NULL) return 1;

	// Numeric coercion for numeric vs numeric
	int a_num = (a->type == VARIANT_INT8 || a->type == VARIANT_UINT8 || a->type == VARIANT_INT16 || a->type == VARIANT_UINT16 ||
				 a->type == VARIANT_INT32 || a->type == VARIANT_UINT32 || a->type == VARIANT_INT64 || a->type == VARIANT_DOUBLE);
	int b_num = (b->type == VARIANT_INT8 || b->type == VARIANT_UINT8 || b->type == VARIANT_INT16 || b->type == VARIANT_UINT16 ||
				 b->type == VARIANT_INT32 || b->type == VARIANT_UINT32 || b->type == VARIANT_INT64 || b->type == VARIANT_DOUBLE);
	if (a_num && b_num) {
		// Fast path: both are integer-like (no DOUBLE)
		if (a->type != VARIANT_DOUBLE && b->type != VARIANT_DOUBLE) {
			if (a->value.i < b->value.i) return -1;
			if (a->value.i > b->value.i) return 1;
			return 0;
		}
		// Otherwise, compare as double
		double av = (a->type == VARIANT_DOUBLE) ? (double)a->value.f : (double)a->value.i;
		double bv = (b->type == VARIANT_DOUBLE) ? (double)b->value.f : (double)b->value.i;
		if (av < bv) return -1;
		if (av > bv) return 1;
		return 0;
	}

	// Fallback: deterministic order by type id
	if (a->type < b->type) return -1;
	if (a->type > b->type) return 1;
	return 0;
}

int flintdb_variant_to_string(const struct flintdb_variant *v, char *out, u32 len) {
	if (!v || !out || len == 0) return -1;

	const char NIL_STR[] = "\\N";
	
	switch (v->type) {
	case VARIANT_NULL:
		snprintf(out, len, "%s", NIL_STR);
		return strlen(out);
	case VARIANT_STRING: {
		u32 slen = v->value.b.length < len - 1 ? v->value.b.length : len - 1;
		simd_memcpy(out, v->value.b.data, slen);
		out[slen] = '\0';
		return (int)slen;
	}
	case VARIANT_BYTES: {
		// Render a short hex preview: <HEX 0102... (len=123)>
		static const char HEX[] = "0123456789ABCDEF";
		const u32 max_preview = 16;
		const u8 *b = (const u8 *)v->value.b.data;
		u32 n = v->value.b.length;
		if (!b) {
			snprintf(out, len, "%s", NIL_STR);
			return (int)strlen(out);
		}
		u32 show = n < max_preview ? n : max_preview;
		int w = snprintf(out, len, "<HEX ");
		if (w < 0 || (u32)w >= len) {
			out[len - 1] = '\0';
			return (int)strlen(out);
		}
		for (u32 i = 0; i < show; i++) {
			if ((u32)(w + 2) >= len) break;
			u8 v8 = b[i];
			out[w++] = HEX[(v8 >> 4) & 0x0F];
			out[w++] = HEX[v8 & 0x0F];
		}
		if (n > show) {
			if ((u32)(w + 3) < len) {
				out[w++] = '.';
				out[w++] = '.';
				out[w++] = '.';
			}
		}
		// Always include length if there's space
		if ((u32)(w + 10) < len) {
			w += snprintf(out + w, len - (u32)w, " (len=%u)>", n);
		} else {
			if ((u32)(w + 1) < len) out[w++] = '>';
			out[w] = '\0';
		}
		return (int)strlen(out);
	}
	case VARIANT_INT8:
	case VARIANT_INT16:
	case VARIANT_INT32:
	case VARIANT_UINT8:
	case VARIANT_UINT16:
	case VARIANT_UINT32: {
		snprintf(out, len, "%lld", (long long)v->value.i);
		return strlen(out);
	}
	case VARIANT_INT64: {
		snprintf(out, len, "%lld", (long long)v->value.i);
		return strlen(out);
	}
	case VARIANT_FLOAT:
	case VARIANT_DOUBLE: {
		snprintf(out, len, "%.17g", (double)v->value.f);
		return strlen(out);
	}
	case VARIANT_DECIMAL: {
		flintdb_decimal_to_string(&v->value.d, out, len);
		return strlen(out);
	}
	case VARIANT_DATE: {
		char *verr = NULL;
		time_t t = (v->type == VARIANT_DATE) ? flintdb_variant_date_get(v, &verr) : flintdb_variant_time_get(v, &verr);
		struct tm *tm = gmtime(&t);
		if (tm) {
			strftime(out, len, "%Y-%m-%d", tm);
		} else {
			snprintf(out, len, "%s", NIL_STR);
		}
		return strlen(out);
	}
	case VARIANT_TIME: {
		char *verr = NULL;
		time_t t = (v->type == VARIANT_DATE) ? flintdb_variant_date_get(v, &verr) : flintdb_variant_time_get(v, &verr);
		struct tm *tm = gmtime(&t);
		if (tm) {
			strftime(out, len, "%Y-%m-%d %H:%M:%S.0", tm);
		} else {
			snprintf(out, len, "%s", NIL_STR);
		}
		return strlen(out);
	}
	default:
		snprintf(out, len, "%s", NIL_STR);
		return strlen(out);
	}
}

// Optimized textual conversion for hot CLI scan paths.
// Provides faster integer formatting than snprintf and delegates
// to variant_to_string for all other types. Returns number of bytes written.
int variant_to_string_fast(const struct flintdb_variant *v, char *out, u32 len) {
	if (!v || !out || len == 0) return -1;
	switch (v->type) {
	case VARIANT_INT8: case VARIANT_INT16: case VARIANT_INT32: case VARIANT_UINT8: case VARIANT_UINT16: case VARIANT_UINT32: case VARIANT_INT64: {
		// Manual int to string (base 10) without snprintf overhead.
		// Use unsigned conversion for magnitude then prepend sign if needed.
		long long val = (long long)v->value.i;
		unsigned long long u = (val < 0) ? (unsigned long long)(-val) : (unsigned long long)val;
		char buf[32];
		int i = 0;
		do {
			buf[i++] = (char)('0' + (u % 10ULL));
			u /= 10ULL;
		} while (u && i < (int)sizeof(buf)-1);
		if (val < 0 && i < (int)sizeof(buf)-1) buf[i++] = '-';
		// Reverse into out (ensure space)
		if ((u32)i >= len) { // Fallback if not enough room
			return flintdb_variant_to_string(v, out, len);
		}
		int w = 0;
		while (i > 0) {
			out[w++] = buf[--i];
		}
		out[w] = '\0';
		return w;
	}
	case VARIANT_STRING: // Direct copy already efficient; reuse original path.
		return flintdb_variant_to_string(v, out, len);
	default:
		return flintdb_variant_to_string(v, out, len);
	}
}

int flintdb_variant_to_decimal(const struct flintdb_variant *v, struct flintdb_decimal  *out, char **e) {
	if (e) *e = NULL;
	if (!v || !out) {
		if (e) *e = variant_error_set(v, "variant/out is NULL");
		return -1;
	}

	// Zero-initialize output by default
	memset(out, 0, sizeof(*out));

	switch (v->type) {
	case VARIANT_DECIMAL:
		*out = v->value.d;
		return 0;

	case VARIANT_ZERO:
	case VARIANT_INT8: case VARIANT_UINT8:
	case VARIANT_INT16: case VARIANT_UINT16:
	case VARIANT_INT32: case VARIANT_UINT32:
	case VARIANT_INT64: {
		// Convert integer-like values with scale 0
		char buf[64];
		long long val = (v->type == VARIANT_ZERO) ? 0LL : (long long)v->value.i;
		snprintf(buf, sizeof(buf), "%lld", val);
		if (flintdb_decimal_from_string(buf, 0, out) != 0) {
			if (e) *e = variant_error_set(v, "decimal_from_string failed");
			return -1;
		}
		return 0;
	}

	case VARIANT_FLOAT:
	case VARIANT_DOUBLE: {
		// Try to preserve digits if not in exponential form; otherwise use a reasonable default scale.
		char tmp[96];
		snprintf(tmp, sizeof(tmp), "%.17g", (double)v->value.f);
		int has_exp = 0;
		for (const char *p = tmp; *p; ++p) { if (*p == 'e' || *p == 'E') { has_exp = 1; break; } }
		if (!has_exp) {
			const char *dot = strchr(tmp, '.');
			int scale = dot ? (int)strlen(dot + 1) : 0;
			if (flintdb_decimal_from_string(tmp, scale, out) != 0) {
				if (e) *e = variant_error_set(v, "decimal_from_string failed");
				return -1;
			}
			return 0;
		} else {
			int dflt_scale = 6; // fallback for exponential representations
			char *ee = NULL;
			struct flintdb_decimal  d = flintdb_decimal_from_f64(v->value.f, dflt_scale, &ee);
			if (ee && *ee) { if (e) *e = variant_error_set(v, ee); return -1; }
			*out = d;
			return 0;
		}
	}

	case VARIANT_STRING: {
		const char *s = v->value.b.data ? v->value.b.data : "0";
		// Detect exponent notation; if present, parse via double then convert with fixed scale.
		int has_exp = 0;
		for (const char *p = s; *p; ++p) { if (*p == 'e' || *p == 'E') { has_exp = 1; break; } }
		if (!has_exp) {
			const char *dot = strchr(s, '.');
			int scale = dot ? (int)strlen(dot + 1) : 0;
			if (flintdb_decimal_from_string(s, scale, out) != 0) {
				if (e) *e = variant_error_set(v, "invalid numeric string");
				return -1;
			}
			return 0;
		} else {
			errno = 0;
			char *endp = NULL;
			double dv = strtod(s, &endp);
			if (endp == s || errno == ERANGE) {
				if (e) *e = variant_error_set(v, "invalid numeric string");
				return -1;
			}
			int dflt_scale = 6;
			char *ee = NULL;
			struct flintdb_decimal  d = flintdb_decimal_from_f64((f64)dv, dflt_scale, &ee);
			if (ee && *ee) { if (e) *e = variant_error_set(v, ee); return -1; }
			*out = d;
			return 0;
		}
	}

	default:
		break;
	}

	if (e) *e = variant_error_set(v, "type mismatch: expected numeric/decimal/string");
	return -1;
}

UNUSED_FN
int variant_flintdb_decimal_plus(struct flintdb_variant *result, const struct flintdb_variant *a, const struct flintdb_variant *b) {
	if (!result || !a || !b) return -1;
	if (a->type != VARIANT_DECIMAL || b->type != VARIANT_DECIMAL) return -1;

	// Core helper implemented below
	// Choose desired scale as the max of input scales to preserve precision
	int desired_scale = (a->value.d.scale > b->value.d.scale) ? a->value.d.scale : b->value.d.scale;

	struct flintdb_decimal  out = {0};
	if (flintdb_decimal_plus(&a->value.d, &b->value.d, desired_scale, &out) != 0)
		return -1;
	result->type = VARIANT_DECIMAL;
	result->value.d = out;
	return 0;
}

UNUSED_FN
int variant_decimal_add(struct flintdb_variant *target, const struct flintdb_variant *other) {
    if (!target || !other) return -1;
    if (target->type != VARIANT_DECIMAL) return -1;

    struct flintdb_variant temp;
    flintdb_variant_init(&temp);
    switch(other->type) {
        case VARIANT_DECIMAL: {
			// Rescale other to target's scale using string round/truncate rules
			char buf[96];
			buf[0] = '\0';
			flintdb_decimal_to_string(&other->value.d, buf, sizeof(buf));
			struct flintdb_decimal  d = {0};
			if (flintdb_decimal_from_string(buf, target->value.d.scale, &d) != 0) return -1;
			temp.type = VARIANT_DECIMAL;
			temp.value.d = d;
            break;
        }
        case VARIANT_INT8: case VARIANT_UINT8:
        case VARIANT_INT16: case VARIANT_UINT16:
        case VARIANT_INT32: case VARIANT_UINT32:
        case VARIANT_INT64: {
			char buf[64];
			snprintf(buf, sizeof(buf), "%lld", (long long)other->value.i);
			struct flintdb_decimal  d = {0};
			if (flintdb_decimal_from_string(buf, target->value.d.scale, &d) != 0) return -1;
			temp.type = VARIANT_DECIMAL;
			temp.value.d = d;
			break;
        }
        case VARIANT_FLOAT: case VARIANT_DOUBLE: {
			char *e = NULL;
			struct flintdb_decimal  d = flintdb_decimal_from_f64(other->value.f, target->value.d.scale, &e);
			// decimal_from_f64 returns zero on failure, treat as error only if e is set
			if (e && *e) return -1;
			temp.type = VARIANT_DECIMAL;
			temp.value.d = d;
			break;
        }
        case VARIANT_STRING: {
			// other->value.b.data is NUL-terminated by setters
			struct flintdb_decimal  d = {0};
			if (flintdb_decimal_from_string(other->value.b.data ? other->value.b.data : "0", target->value.d.scale, &d) != 0) return -1;
			temp.type = VARIANT_DECIMAL;
			temp.value.d = d;
			break;
        }
        default:
            return -1; // unsupported type
    }

	if (variant_flintdb_decimal_plus(target, target, &temp) != 0) return -1;
    return 0;
}
