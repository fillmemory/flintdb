#include "pool.h"

#include <stdlib.h>

int object_pool_init(struct object_pool *p, u32 capacity, u32 preload,
                     object_pool_alloc_fn alloc, object_pool_reset_fn reset,
                     object_pool_dtor_fn dtor, void *ctx) {
    if (!p)
        return -1;
    memset(p, 0, sizeof(*p));
    p->capacity = (int)capacity;
    p->alloc = alloc;
    p->reset = reset;
    p->dtor = dtor;
    p->ctx = ctx;
    if (capacity > 0) {
        p->items = (void **)CALLOC((size_t)capacity, sizeof(void *));
        if (!p->items)
            return -1;
    }
    u32 n = (preload > capacity) ? capacity : preload;
    for (u32 i = 0; i < n; i++) {
        void *obj = alloc ? alloc(ctx) : NULL;
        if (!obj)
            break;
        p->items[p->top++] = obj;
    }
    return 0;
}

void object_pool_deinit(struct object_pool *p) {
    if (!p)
        return;
    if (p->items) {
        for (int i = 0; i < p->top; i++) {
            if (p->items[i] && p->dtor)
                p->dtor(p->items[i], p->ctx);
            p->items[i] = NULL;
        }
        FREE(p->items);
        p->items = NULL;
    }
    p->top = 0;
    p->capacity = 0;
}

struct object_pool *object_pool_create(u32 capacity, u32 preload,
                                       object_pool_alloc_fn alloc, object_pool_reset_fn reset,
                                       object_pool_dtor_fn dtor, void *ctx) {
    struct object_pool *p = (struct object_pool *)CALLOC(1, sizeof(struct object_pool));
    if (!p)
        return NULL;
    if (object_pool_init(p, capacity, preload, alloc, reset, dtor, ctx) != 0) {
        FREE(p);
        return NULL;
    }
    return p;
}

void object_pool_destroy(struct object_pool *p) {
    if (!p)
        return;
    object_pool_deinit(p);
    FREE(p);
}

void keyed_object_pool_init(struct keyed_object_pool *kp, u32 slot_capacity,
                            object_pool_alloc_fn alloc, object_pool_reset_fn reset,
                            object_pool_dtor_fn dtor) {
    if (!kp)
        return;
    memset(kp, 0, sizeof(*kp));
    kp->bucket_cap = KEYED_OBJECT_POOL_MAX_BUCKETS;
    kp->slot_capacity = slot_capacity;
    kp->alloc = alloc;
    kp->reset = reset;
    kp->dtor = dtor;
}

static int keyed_find_bucket(struct keyed_object_pool *kp, void *key) {
    for (int i = 0; i < kp->bucket_count; i++) {
        if (kp->buckets[i].key == key)
            return i;
    }
    return -1;
}

void *keyed_object_pool_borrow(struct keyed_object_pool *kp, void *key) {
    if (!kp || !key)
        return NULL;
    pool_spin_lock(&kp->lock);
    int bi = keyed_find_bucket(kp, key);
    if (bi >= 0) {
        struct object_pool *p = &kp->buckets[bi].pool;
        if (p->top > 0) {
            void *obj = p->items[--p->top];
            p->items[p->top] = NULL;
            pool_spin_unlock(&kp->lock);
            return obj;
        }
    }
    pool_spin_unlock(&kp->lock);
    return kp->alloc ? kp->alloc(key) : NULL;
}

void keyed_object_pool_return(struct keyed_object_pool *kp, void *key, void *obj) {
    if (!kp || !obj)
        return;
    if (!key) {
        if (kp->dtor)
            kp->dtor(obj, key);
        return;
    }
    if (kp->reset)
        kp->reset(obj, key);

    pool_spin_lock(&kp->lock);
    int bi = keyed_find_bucket(kp, key);
    if (bi < 0 && kp->bucket_count < kp->bucket_cap) {
        bi = kp->bucket_count++;
        kp->buckets[bi].key = key;
        if (object_pool_init(&kp->buckets[bi].pool, kp->slot_capacity, 0,
                             kp->alloc, kp->reset, kp->dtor, key) != 0) {
            kp->bucket_count--;
            bi = -1;
        }
    }
    if (bi >= 0) {
        struct object_pool *p = &kp->buckets[bi].pool;
        if (p->top < p->capacity && p->items) {
            p->items[p->top++] = obj;
            pool_spin_unlock(&kp->lock);
            return;
        }
    }
    pool_spin_unlock(&kp->lock);
    if (kp->dtor)
        kp->dtor(obj, key);
}

void keyed_object_pool_drain(struct keyed_object_pool *kp) {
    if (!kp)
        return;
    pool_spin_lock(&kp->lock);
    for (int i = 0; i < kp->bucket_count; i++) {
        object_pool_deinit(&kp->buckets[i].pool);
        kp->buckets[i].key = NULL;
    }
    kp->bucket_count = 0;
    pool_spin_unlock(&kp->lock);
}

int keyed_object_pool_size(struct keyed_object_pool *kp, void *key) {
    if (!kp)
        return 0;
    pool_spin_lock(&kp->lock);
    int bi = keyed_find_bucket(kp, key);
    int c = (bi >= 0) ? kp->buckets[bi].pool.top : 0;
    pool_spin_unlock(&kp->lock);
    return c;
}

static void *string_pool_alloc(void *ctx) {
    u32 sz = (u32)(uintptr_t)ctx;
    if (sz == 0)
        sz = 1;
    return MALLOC(sz);
}

static void string_pool_dtor(void *obj, void *ctx) {
    (void)ctx;
    FREE(obj);
}

struct string_pool *string_pool_create(u32 capacity, u32 str_size, u32 preload) {
    struct string_pool *pool = (struct string_pool *)CALLOC(1, sizeof(struct string_pool));
    if (!pool)
        return NULL;
    pool->str_size = (str_size == 0) ? 1 : str_size;
    if (object_pool_init(&pool->pool, capacity, preload, string_pool_alloc, NULL, string_pool_dtor,
                         (void *)(uintptr_t)pool->str_size) != 0) {
        FREE(pool);
        return NULL;
    }
    return pool;
}

void *arena_alloc_chunk(struct arena *a, u32 need) {
    if (!a || need == 0)
        return NULL;
    if (a->chunk_size == 0)
        a->chunk_size = ARENA_DEFAULT_CHUNK;
    u32 cap = a->chunk_size;
    if (need > cap)
        cap = need;
    struct arena_chunk *chunk = (struct arena_chunk *)CALLOC(1, sizeof(struct arena_chunk) + (size_t)cap);
    if (!chunk)
        return NULL;
    chunk->cap = cap;
    chunk->used = 0;
    chunk->next = NULL;
    if (!a->first)
        a->first = chunk;
    else if (a->head)
        a->head->next = chunk;
    a->head = chunk;
    void *p = (char *)(chunk + 1);
    chunk->used = need;
    return p;
}

void arena_reset(struct arena *a) {
    if (!a || !a->first)
        return;
    struct arena_chunk *c = a->first->next;
    while (c) {
        struct arena_chunk *n = c->next;
        FREE(c);
        c = n;
    }
    a->first->next = NULL;
    a->first->used = 0;
    a->head = a->first;
}

void arena_destroy(struct arena *a) {
    if (!a)
        return;
    struct arena_chunk *c = a->first;
    while (c) {
        struct arena_chunk *n = c->next;
        FREE(c);
        c = n;
    }
    memset(a, 0, sizeof(*a));
}
