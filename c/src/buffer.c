#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// #ifndef _WIN32
// #include <arpa/inet.h>
// #include <sys/types.h>
// #else
// #include <windows.h>
// #include <winsock2.h>
// #endif

#include "allocator.h"
#include "buffer.h"
#include "runtime.h"
#include "simd.h"

const char *dump_as_hex(const char *in, int offset, int len, int width, char *out) {
    // address + hex + ascii
    int i = 0;
    char ascii[width + 1];
    memset(ascii, 0, width + 1);
    for (i = 0; i < len; i++) {
        if (i % width == 0) {
            sprintf(out, "\n%08d : ", i);
            out += strlen(out);
        }
        sprintf(out, "%02x ", in[offset + i] & 0xff);
        out += 3;

        if ((i + 1) % width == 0) {
            sprintf(out, " : %s", ascii);
            out += strlen(out);
        } else {
            ascii[i % width] = (in[offset + i] >= 32 && in[offset + i] <= 126) ? in[offset + i] : '.';
        }
    }
    return out;
}

static void buffer_flip(struct buffer *p) {
    p->limit = p->position;
    p->position = 0;
}

static void buffer_clear(struct buffer *p) {
    p->limit = p->capacity;
    p->position = 0;
}

static i32 buffer_remaining(struct buffer *p) {
    return p->limit - p->position;
}

static i32 buffer_skip(struct buffer *p, i32 n) {
    p->position += n;
    return p->position;
}

static void buffer_array_put(struct buffer *p, const char *bytes, u32 len, char **e) {
    e = NULL;
    if (UNLIKELY((p->position + len) > p->capacity)) {
        THROW(e, "buffer_array_put pos : %d, len : %d, capacity : %d", p->position, len, p->capacity);
    }
    simd_memcpy(&p->array[p->position], bytes, len);
    p->position += len;

EXCEPTION:
    return;
}

static char *buffer_array_get(struct buffer *p, u32 len, char **e) {
    char *r = &p->array[p->position];
    p->position += len;
    return r;
}

static void buffer_i8_put(struct buffer *p, char v, char **e) {
    if (UNLIKELY((p->position + 1) > p->capacity)) {
        THROW(e, "buffer_i8_put pos : %d, len : %d, capacity : %d", p->position, 1, p->capacity);
    }

    p->array[p->position] = v;
    p->position++;

EXCEPTION:
    return;
}

static void buffer_i16_put(struct buffer *p, i16 v, char **e) {
    if (UNLIKELY((p->position + 2) > p->capacity)) {
        THROW(e, "buffer_i16_put pos : %d, len : %d, capacity : %d", p->position, 2, p->capacity);
    }

    // Little endian native - direct memory copy for performance
    simd_memcpy(&p->array[p->position], &v, 2);
    p->position += 2;

EXCEPTION:
    return;
}

static void buffer_i32_put(struct buffer *p, i32 v, char **e) {
    if (UNLIKELY((p->position + 4) > p->capacity)) {
        THROW(e, "buffer_i32_put pos : %d, len : %d, capacity : %d", p->position, 4, p->capacity);
    }

    // Little endian native - direct memory copy for performance
    simd_memcpy(&p->array[p->position], &v, 4);
    p->position += 4;

EXCEPTION:
    return;
}

static void buffer_i64_put(struct buffer *p, i64 v, char **e) {
    if (UNLIKELY((p->position + 8) > p->capacity)) {
        THROW(e, "buffer_i64_put pos : %d, len : %d, capacity : %d", p->position, 8, p->capacity);
    }

    // Little endian native - direct memory copy for performance
    simd_memcpy(&p->array[p->position], &v, 8);
    p->position += 8;

EXCEPTION:
    return;
}

static void buffer_f64_put(struct buffer *p, f64 v, char **e) {
    if (UNLIKELY((p->position + 8) > p->capacity)) {
        THROW(e, "buffer_f64_put pos : %d, len : %d, capacity : %d", p->position, 8, p->capacity);
    }

    // Little endian native - direct memory copy for performance
    simd_memcpy(&p->array[p->position], &v, 8);
    p->position += 8;

EXCEPTION:
    return;
}

static char buffer_i8_get(struct buffer *p, char **e) {
    if (UNLIKELY((p->position + 1) > p->capacity)) {
        THROW(e, "buffer_i8_get pos : %d, len : %d, capacity : %d", p->position, 1, p->capacity);
    }

    char v = p->array[p->position];
    p->position++;
    return v;

EXCEPTION:
    return 0;
}

static i16 buffer_i16_get(struct buffer *p, char **e) {
    if (UNLIKELY((p->position + 2) > p->capacity)) {
        THROW(e, "buffer_i16_get pos : %d, len : %d, capacity : %d", p->position, 2, p->capacity);
    }

    // Little endian native - direct memory copy for performance
    i16 v;
    simd_memcpy(&v, &p->array[p->position], 2);
    p->position += 2;
    return v;

EXCEPTION:
    return 0;
}

static i32 buffer_i32_get(struct buffer *p, char **e) {
    if (UNLIKELY((p->position + 4) > p->capacity)) {
        THROW(e, "buffer_i32_get pos : %d, len : %d, capacity : %d", p->position, 4, p->capacity);
    }

    // Little endian native - direct memory copy for performance
    i32 v;
    simd_memcpy(&v, &p->array[p->position], 4);
    p->position += 4;
    return v;

EXCEPTION:
    return 0;
}

static i64 buffer_i64_get(struct buffer *p, char **e) {
    if (UNLIKELY((p->position + 8) > p->capacity)) {
        THROW(e, "buffer_i64_get pos : %d, len : %d, capacity : %d", p->position, 8, p->capacity);
    }

    // Little endian native - direct memory copy for performance
    i64 v;
    simd_memcpy(&v, &p->array[p->position], 8);
    p->position += 8;
    return v;

EXCEPTION:
    return 0;
}

static f64 buffer_f64_get(struct buffer *p, char **e) {
    if (UNLIKELY((p->position + 8) > p->capacity)) {
        THROW(e, "buffer_f64_get pos : %d, len : %d, capacity : %d", p->position, 8, p->capacity);
    }

    // Little endian native - direct memory copy for performance
    f64 v;
    simd_memcpy(&v, &p->array[p->position], 8);
    p->position += 8;
    return v;

EXCEPTION:
    return 0.0;
}

static void buffer_free(struct buffer *me) {
    FREE(me->array);
    FREE(me);
}

static void buffer_borrow_free(struct buffer *me) {
    // do nothing
}

static void buffer_slice_free(struct buffer *me) {
    // Slices do not own the underlying array, but the struct itself may be heap-allocated
    // (e.g., storage_read/storage_head). Free only the struct.
    if (me && me->freeable) {
        FREE(me);
    }
}

extern int munmap(void *addr, size_t length);

static void mmap_free(struct buffer *me) {
    munmap(me->mapped.addr, me->mapped.length);
    FREE(me);
}

static void buffer_slice_to(struct buffer *me, i32 offset, i32 length, struct buffer *out, char **e) {
    if (UNLIKELY(me == NULL || out == NULL)) {
        THROW(e, "buffer_slice: input buffer is NULL");
    }
    if (UNLIKELY(offset < 0 || length < 0 || (me->position + offset + length) > me->limit))
        THROW(e, "buffer_slice offset : %d, length : %d, limit : %d", offset, length, me->limit);

    out->array = me->array + me->position + offset;
    out->position = 0;
    out->limit = length;
    out->capacity = length;

    out->flip = &buffer_flip;
    out->clear = &buffer_clear;
    out->skip = &buffer_skip;
    out->remaining = &buffer_remaining;
    out->array_put = &buffer_array_put;
    out->array_get = &buffer_array_get;
    out->i8_put = &buffer_i8_put;
    out->i16_put = &buffer_i16_put;
    out->i32_put = &buffer_i32_put;
    out->i64_put = &buffer_i64_put;
    out->f64_put = &buffer_f64_put;
    out->i8_get = &buffer_i8_get;
    out->i16_get = &buffer_i16_get;
    out->i32_get = &buffer_i32_get;
    out->i64_get = &buffer_i64_get;
    out->f64_get = &buffer_f64_get;

    out->slice = &buffer_slice_to;
    out->free = &buffer_slice_free;

EXCEPTION:
    return;
}

struct buffer *buffer_slice(struct buffer *in, i32 offset, i32 length, char **e) {
    struct buffer *out = NULL;
    if (UNLIKELY(in == NULL)) {
        THROW(e, "buffer_slice: input buffer is NULL");
    }
    out = CALLOC(1, sizeof(struct buffer));
    if (!out) {
        THROW(e, "Out of memory");
    }
    out->freeable = 1;
    buffer_slice_to(in, offset, length, out, e);
    if (e && *e) {
        out->free(out);
        return NULL;
    }
    return out;

EXCEPTION:
    if (out) out->free(out);
    return NULL;
}

struct buffer *buffer_wrap(char *array, u32 capacity, struct buffer *out) {
    out->freeable = 0;
    out->array = array;
    out->position = 0;
    out->limit = capacity; // MODIFIED 12-24 : 0 -> capacity
    out->capacity = capacity;

    out->flip = &buffer_flip;
    out->clear = &buffer_clear;
    out->skip = &buffer_skip;
    out->remaining = &buffer_remaining;
    out->array_put = &buffer_array_put;
    out->array_get = &buffer_array_get;
    out->i8_put = &buffer_i8_put;
    out->i16_put = &buffer_i16_put;
    out->i32_put = &buffer_i32_put;
    out->i64_put = &buffer_i64_put;
    out->f64_put = &buffer_f64_put;
    out->i8_get = &buffer_i8_get;
    out->i16_get = &buffer_i16_get;
    out->i32_get = &buffer_i32_get;
    out->i64_get = &buffer_i64_get;
    out->f64_get = &buffer_f64_get;

    out->slice = &buffer_slice_to;
    out->free = &buffer_borrow_free;

    return out;
}

void buffer_realloc(struct buffer *me, i32 size) {
    // LOG("buffer_realloc(%p, %d, %d, %d)", me, me->capacity, size, me->capacity + size);
    me->array = REALLOC(me->array, size);
    me->capacity = size;
    me->limit = size; // ADD 12-24
}

struct buffer *buffer_alloc(u32 capacity) {
    struct buffer *out = CALLOC(1, sizeof(struct buffer));
    out->freeable = 1;
    out->array = MALLOC(capacity);
    out->position = 0;
    out->limit = capacity; // MODIFIED 12-24 : 0 -> capacity
    out->capacity = capacity;

    out->flip = &buffer_flip;
    out->clear = &buffer_clear;
    out->skip = &buffer_skip;
    out->remaining = &buffer_remaining;
    out->array_put = &buffer_array_put;
    out->array_get = &buffer_array_get;
    out->i8_put = &buffer_i8_put;
    out->i16_put = &buffer_i16_put;
    out->i32_put = &buffer_i32_put;
    out->i64_put = &buffer_i64_put;
    out->f64_put = &buffer_f64_put;
    out->i8_get = &buffer_i8_get;
    out->i16_get = &buffer_i16_get;
    out->i32_get = &buffer_i32_get;
    out->i64_get = &buffer_i64_get;
    out->f64_get = &buffer_f64_get;

    out->slice = &buffer_slice_to;

    out->realloc = &buffer_realloc;
    out->free = &buffer_free;

    return out;
}

struct buffer *buffer_mmap(void *addr, u32 offset, u32 length) {
    struct buffer *out = CALLOC(1, sizeof(struct buffer));
    out->freeable = 1;
    out->mapped.addr = addr;
    // mapped.length must equal the exact size passed to mmap().
    // The 'offset' here is the in-buffer view offset, not additional mapping size.
    // Using offset+length would over-unmap and can crash on munmap.
    out->mapped.length = length;
    out->array = (char *)addr + offset;
    out->position = 0;
    out->limit = length; // MODIFIED 12-24 : 0 -> length
    out->capacity = length;

    out->flip = &buffer_flip;
    out->clear = &buffer_clear;
    out->skip = &buffer_skip;
    out->remaining = &buffer_remaining;
    out->array_put = &buffer_array_put;
    out->array_get = &buffer_array_get;
    out->i8_put = &buffer_i8_put;
    out->i16_put = &buffer_i16_put;
    out->i32_put = &buffer_i32_put;
    out->i64_put = &buffer_i64_put;
    out->f64_put = &buffer_f64_put;
    out->i8_get = &buffer_i8_get;
    out->i16_get = &buffer_i16_get;
    out->i32_get = &buffer_i32_get;
    out->i64_get = &buffer_i64_get;
    out->f64_get = &buffer_f64_get;

    out->slice = &buffer_slice_to;
    out->free = &mmap_free;

    return out;
}

//
static struct buffer *buffer_pool_borrow(struct buffer_pool *pool, u32 buf_size) {
    if (pool->top > 0) {
        struct buffer *b = pool->items[--pool->top];
        if (b->capacity < buf_size) {
            b->realloc(b, buf_size);
        }
        b->clear(b);
        return b;
    } else {
        struct buffer *b = buffer_alloc(buf_size > (u32)pool->align ? buf_size : (u32)pool->align);
        return b;
    }
}

static void buffer_pool_return(struct buffer_pool *pool, struct buffer *b) {
    // Only pool buffers that are owned by the pool and safely reallocatable.
    // Criteria:
    //  - realloc is set (buffer_alloc provides this)
    //  - free function is the owning heap free (buffer_free)
    //  - freeable flag is set
    int pool_owned = (b && b->realloc != NULL && b->free == &buffer_free && b->freeable == 1);

    if (!pool_owned) {
        // Do not cache foreign buffers (slice/mmap/wrap). Free only if heap-owned.
        if (b && b->freeable == 1) {
            b->free(b);
        }
        return;
    }

    if (pool->top < pool->capacity) {
        b->clear(b);
        pool->items[pool->top++] = b;
    } else {
        b->free(b);
    }
}

static void buffer_pool_free(struct buffer_pool *pool) {
    if (!pool)
        return;
    for (int i = 0; i < pool->top; i++) {
        if (pool->items[i]) {
            struct buffer *b = pool->items[i];
            b->free(b);
        }
    }
    FREE(pool->items);
    FREE(pool);
}

struct buffer_pool *buffer_pool_create(u32 capacity, u32 align, u32 preload) {
    struct buffer_pool *pool = CALLOC(1, sizeof(struct buffer_pool));
    pool->capacity = capacity;
    // Treat 'align' as minimum buffer capacity; guard against zero.
    pool->align = (align == 0) ? 1 : align;
    pool->top = 0;
    pool->items = CALLOC((size_t)capacity, sizeof(struct buffer *));

    if (preload > 0) {
        for (u32 i = 0; i < capacity && i < preload; i++) {
            struct buffer *b = buffer_alloc((u32)pool->align);
            pool->items[pool->top++] = b;
        }
    }

    pool->borrow = &buffer_pool_borrow;
    pool->return_buffer = &buffer_pool_return;
    pool->free = &buffer_pool_free;

    return pool;
}

static struct buffer *buffer_pool_safe_borrow(struct buffer_pool_safe *me, u32 buf_size) {
    if (!me || !me->pool)
        return NULL;
    // Use C11 stdatomic spinlock (cross-platform: Linux, macOS, Windows MinGW)
    atomic_int *lock = (atomic_int *)me->mtx;
    int expected = 0;
    while (!atomic_compare_exchange_weak_explicit(lock, &expected, 1, memory_order_acquire, memory_order_relaxed)) {
        expected = 0;
    }
    struct buffer *b = me->pool->borrow(me->pool, buf_size);
    atomic_store_explicit(lock, 0, memory_order_release);
    return b;
}

static void buffer_pool_safe_return(struct buffer_pool_safe *me, struct buffer *b) {
    if (!me || !me->pool) {
        if (b)
            b->free(b);
        return;
    }
    atomic_int *lock = (atomic_int *)me->mtx;
    int expected = 0;
    while (!atomic_compare_exchange_weak_explicit(lock, &expected, 1, memory_order_acquire, memory_order_relaxed)) {
        expected = 0;
    }
    me->pool->return_buffer(me->pool, b);
    atomic_store_explicit(lock, 0, memory_order_release);
}

static void buffer_pool_safe_free(struct buffer_pool_safe *me) {
    if (!me)
        return;
    if (me->pool) {
        me->pool->free(me->pool);
        me->pool = NULL;
    }
    if (me->mtx) {
        FREE((atomic_int *)me->mtx);
        me->mtx = NULL;
    }
    FREE(me);
}

struct buffer_pool_safe *buffer_pool_safe_create(u32 capacity, u32 align, u32 preload) {
    struct buffer_pool_safe *safe = CALLOC(1, sizeof(struct buffer_pool_safe));
    if (!safe)
        return NULL;
    safe->pool = buffer_pool_create(capacity, align, preload);
    if (!safe->pool) {
        FREE(safe);
        return NULL;
    }
    safe->mtx = CALLOC(1, sizeof(atomic_int));
    if (!safe->mtx) {
        safe->pool->free(safe->pool);
        FREE(safe);
        return NULL;
    }
    atomic_store_explicit((atomic_int *)safe->mtx, 0, memory_order_relaxed);
    safe->borrow = &buffer_pool_safe_borrow;
    safe->return_buffer = &buffer_pool_safe_return;
    safe->free = &buffer_pool_safe_free;
    return safe;
}

HOT_PATH
char *string_pool_borrow(struct string_pool *pool) {
    if (UNLIKELY(!pool))
        return NULL;
    if (LIKELY(pool->top > 0)) {
        return pool->items[--pool->top];
    }
    // Lazy allocate when pool is empty (rare with preload)
    u32 sz = (pool->str_size == 0) ? 1 : pool->str_size;
    char *s = (char *)MALLOC(sz);
    return s;
}

HOT_PATH
void string_pool_return(struct string_pool *pool, char *s) {
    if (UNLIKELY(!pool || !s))
        return;
    if (LIKELY(pool->top < pool->capacity)) {
        pool->items[pool->top++] = s;
    } else {
        FREE(s);
    }
}

void string_pool_free(struct string_pool *pool) {
    if (!pool)
        return;
    for (int i = 0; i < pool->top; i++) {
        if (pool->items[i]) {
            FREE(pool->items[i]);
        }
    }
    FREE(pool->items);
    FREE(pool);
}

struct string_pool *string_pool_create(u32 capacity, u32 str_size, u32 preload) {
    struct string_pool *pool = CALLOC(1, sizeof(struct string_pool));
    if (!pool)
        return NULL;
    pool->capacity = (int)capacity;
    pool->top = 0;
    pool->str_size = (str_size == 0) ? 1 : str_size;
    pool->items = CALLOC((size_t)capacity, sizeof(char *));
    if (!pool->items) {
        FREE(pool);
        return NULL;
    }

    // Preload strings to avoid lazy allocation overhead
    u32 count = (preload > capacity) ? capacity : preload;
    for (u32 i = 0; i < count; i++) {
        char *s = (char *)MALLOC(pool->str_size);
        if (LIKELY(s)) {
            pool->items[pool->top++] = s;
        }
    }

    pool->borrow = &string_pool_borrow;
    pool->return_string = &string_pool_return;
    pool->free = &string_pool_free;

    return pool;
}