//
// Generic fixed-size object freelist.
// Hot path is inline (borrow/return); create/destroy live in pool.c.
// Adapters: TLS (per-thread instance), keyed (pointer-keyed buckets), spinlock.
//
#ifndef FLINTDB_POOL_H
#define FLINTDB_POOL_H

#include "allocator.h"
#include "types.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#ifndef LIKELY
#ifdef __GNUC__
#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif
#endif

typedef void *(*object_pool_alloc_fn)(void *ctx);
typedef void (*object_pool_reset_fn)(void *obj, void *ctx);
typedef void (*object_pool_dtor_fn)(void *obj, void *ctx);

struct object_pool {
    int capacity;
    int top;
    void **items;
    void *ctx;
    object_pool_alloc_fn alloc;
    object_pool_reset_fn reset;
    object_pool_dtor_fn dtor;
};

int object_pool_init(struct object_pool *p, u32 capacity, u32 preload,
                     object_pool_alloc_fn alloc, object_pool_reset_fn reset,
                     object_pool_dtor_fn dtor, void *ctx);
void object_pool_deinit(struct object_pool *p);
struct object_pool *object_pool_create(u32 capacity, u32 preload,
                                       object_pool_alloc_fn alloc, object_pool_reset_fn reset,
                                       object_pool_dtor_fn dtor, void *ctx);
void object_pool_destroy(struct object_pool *p);

static inline void *object_pool_borrow(struct object_pool *p) {
    if (UNLIKELY(!p))
        return NULL;
    if (LIKELY(p->top > 0)) {
        void *obj = p->items[--p->top];
        p->items[p->top] = NULL;
        return obj;
    }
    return p->alloc ? p->alloc(p->ctx) : NULL;
}

static inline void object_pool_return(struct object_pool *p, void *obj) {
    if (UNLIKELY(!p || !obj))
        return;
    if (p->reset)
        p->reset(obj, p->ctx);
    if (LIKELY(p->top < p->capacity) && p->items) {
        p->items[p->top++] = obj;
        return;
    }
    if (p->dtor)
        p->dtor(obj, p->ctx);
}

static inline int object_pool_count(const struct object_pool *p) {
    return p ? p->top : 0;
}

// --- spinlock (C11 atomic, same pattern previously copied per module) ---

static inline void pool_spin_lock(atomic_int *lock) {
    int expected = 0;
    while (!atomic_compare_exchange_weak_explicit(lock, &expected, 1, memory_order_acquire, memory_order_relaxed)) {
        expected = 0;
    }
}

static inline void pool_spin_unlock(atomic_int *lock) {
    atomic_store_explicit(lock, 0, memory_order_release);
}

// --- TLS wrapper: one object_pool per thread ---
// Usage:
//   static struct object_pool *my_pool_new(void) { return object_pool_create(...); }
//   TLS_OBJECT_POOL_DEFINE(my, my_pool_new, "my pool")
//   ... my_get() ... my_cleanup_main() ...

#define TLS_OBJECT_POOL_DEFINE(name, create_fn, label)                                                         \
    static pthread_key_t name##_key;                                                                           \
    static pthread_once_t name##_once = PTHREAD_ONCE_INIT;                                                     \
    static _Thread_local struct object_pool *name##_cached = NULL;                                             \
    static void name##_destroy(void *p) {                                                                      \
        if (p)                                                                                                 \
            object_pool_destroy((struct object_pool *)p);                                                      \
        DEBUG("%s destroyed", label);                                                                          \
    }                                                                                                          \
    static void name##_make_key(void) {                                                                        \
        (void)pthread_key_create(&name##_key, name##_destroy);                                                 \
        DEBUG("%s created", label);                                                                            \
    }                                                                                                          \
    static inline struct object_pool *name##_get(void) {                                                       \
        if (LIKELY(name##_cached != NULL))                                                                     \
            return name##_cached;                                                                              \
        (void)pthread_once(&name##_once, name##_make_key);                                                     \
        struct object_pool *pool = (struct object_pool *)pthread_getspecific(name##_key);                      \
        if (!pool) {                                                                                           \
            pool = (create_fn)();                                                                              \
            (void)pthread_setspecific(name##_key, pool);                                                       \
        }                                                                                                      \
        name##_cached = pool;                                                                                  \
        return pool;                                                                                           \
    }                                                                                                          \
    static void name##_cleanup_main(void) {                                                                    \
        if (name##_cached != NULL) {                                                                           \
            object_pool_destroy(name##_cached);                                                                \
            name##_cached = NULL;                                                                              \
            (void)pthread_setspecific(name##_key, NULL);                                                       \
        }                                                                                                      \
    }

// --- keyed buckets: one object_pool per pointer key (e.g. row meta) ---

#ifndef KEYED_OBJECT_POOL_MAX_BUCKETS
#define KEYED_OBJECT_POOL_MAX_BUCKETS 32
#endif

struct keyed_object_pool_bucket {
    void *key;
    struct object_pool pool;
};

struct keyed_object_pool {
    atomic_int lock;
    int bucket_count;
    int bucket_cap;
    u32 slot_capacity;
    object_pool_alloc_fn alloc;
    object_pool_reset_fn reset;
    object_pool_dtor_fn dtor;
    struct keyed_object_pool_bucket buckets[KEYED_OBJECT_POOL_MAX_BUCKETS];
};

void keyed_object_pool_init(struct keyed_object_pool *kp, u32 slot_capacity,
                            object_pool_alloc_fn alloc, object_pool_reset_fn reset,
                            object_pool_dtor_fn dtor);
void *keyed_object_pool_borrow(struct keyed_object_pool *kp, void *key);
void keyed_object_pool_return(struct keyed_object_pool *kp, void *key, void *obj);
void keyed_object_pool_drain(struct keyed_object_pool *kp);
int keyed_object_pool_size(struct keyed_object_pool *kp, void *key);

// --- typed string slab on object_pool (formatter / scratch) ---

struct string_pool {
    struct object_pool pool;
    u32 str_size;
};

struct string_pool *string_pool_create(u32 capacity, u32 str_size, u32 preload);

static inline char *string_pool_borrow(struct string_pool *pool) {
    return pool ? (char *)object_pool_borrow(&pool->pool) : NULL;
}

static inline void string_pool_return(struct string_pool *pool, char *s) {
    if (pool)
        object_pool_return(&pool->pool, s);
}

static inline void string_pool_free(struct string_pool *pool) {
    if (!pool)
        return;
    object_pool_deinit(&pool->pool);
    FREE(pool);
}

#endif // FLINTDB_POOL_H
