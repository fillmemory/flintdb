#ifdef MTRACE
#include "allocator.h"
#include <stdio.h>

#ifdef __APPLE__
    #include <malloc/malloc.h>
    #define malloc_usable_size malloc_size
#elif _WIN32

#else
    #include <malloc.h>
#endif

#include <stdatomic.h>  // added for atomic operations


static atomic_uint_least64_t d_allocated_count = 0;
static atomic_uint_least64_t d_allocated_bytes = 0;
static atomic_uint_least64_t d_freed_count = 0;
static atomic_uint_least64_t d_freed_bytes = 0;


void * d_malloc(size_t size, const char *f, int l, const char *fn) {
    void *p = malloc(size);
    size_t sz = p ? malloc_usable_size(p) : 0;
    fprintf(stderr, "+ MALLOC %p, %zu, %zu, %s:%d %s\n", p, sz, size, f, l, fn); fflush(stderr);
    if (p) {
        atomic_fetch_add_explicit(&d_allocated_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&d_allocated_bytes, (uint64_t)sz, memory_order_relaxed);
    }
    return p;
}

void * d_calloc(size_t num, size_t size, const char *f, int l, const char *fn) {
    void *p = calloc(num, size);
    size_t sz = p ? malloc_usable_size(p) : 0;
    fprintf(stderr, "+ CALLOC %p, %zu, %zu, %s:%d %s\n", p, sz, num*size, f, l, fn); fflush(stderr);
    if (p) {
        atomic_fetch_add_explicit(&d_allocated_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&d_allocated_bytes, (uint64_t)sz, memory_order_relaxed);
    }
    return p;
}

void * d_realloc(void *p, size_t size, const char *f, int l, const char *fn) {
    void *o = p;
    size_t sz1 = o ? malloc_usable_size(o) : 0;
    void *n = realloc(p, size);
    size_t sz2 = n ? malloc_usable_size(n) : 0;
    fprintf(stderr, "+ REALLOC %p, %zu <= %p, %zu, %s:%d %s\n", n, sz2, o, sz1, f, l, fn); fflush(stderr);
    if (n) {
        if (sz2 >= sz1) {
            atomic_fetch_add_explicit(&d_allocated_bytes, (uint64_t)(sz2 - sz1), memory_order_relaxed);
        } else {
            atomic_fetch_sub_explicit(&d_allocated_bytes, (uint64_t)(sz1 - sz2), memory_order_relaxed);
        }
    }
    return n;
}

char * d_strdup(const char *s, const char *f, int l, const char *fn) {
    if (!s) {
        fprintf(stderr, "+ STRDUP NULL src, %s:%d %s\n", f, l, fn); fflush(stderr);
        return NULL;
    }
    char *p = strdup(s);
    size_t sz = p ? malloc_usable_size(p) : 0;
    fprintf(stderr, "+ STRDUP %p %zu, %s:%d %s\n", p, sz, f, l, fn); fflush(stderr);
    if (p) {
        atomic_fetch_add_explicit(&d_allocated_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&d_allocated_bytes, (uint64_t)sz, memory_order_relaxed);
    }
    return p;
}

void d_free(void *p, const char *f, int l, const char *fn) {
    if (!p) {
        fprintf(stderr, "- FREE %p, %d, %s:%d %s\n", p, 0, f, l, fn); fflush(stderr);
        // free(NULL) is a no-op; nothing to account.
        return;
    }
    size_t sz = malloc_usable_size(p);
    free(p);
    fprintf(stderr, "- FREE %p, %zu, %s:%d %s\n", p, sz, f, l, fn); fflush(stderr);
    // fprintf(stderr, "- FREE OK %p, %zu, %s:%d %s\n", p, sz, f, l, fn); fflush(stderr);
    atomic_fetch_add_explicit(&d_freed_count, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&d_freed_bytes, (uint64_t)sz, memory_order_relaxed);
}

void print_memory_leak_info() {
    uint64_t allocated_bytes = atomic_load_explicit(&d_allocated_bytes, memory_order_relaxed);
    uint64_t allocated_count = atomic_load_explicit(&d_allocated_count, memory_order_relaxed);
    uint64_t freed_bytes = atomic_load_explicit(&d_freed_bytes, memory_order_relaxed);
    uint64_t freed_count = atomic_load_explicit(&d_freed_count, memory_order_relaxed);

    fprintf(stderr, "MEMORY LEAK INFO: allocated %llu bytes in %llu blocks, freed %llu bytes in %llu blocks, leak %lld bytes in %lld blocks\n",
        (unsigned long long)allocated_bytes,
        (unsigned long long)allocated_count,
        (unsigned long long)freed_bytes,
        (unsigned long long)freed_count,
        (long long)(allocated_bytes - freed_bytes),
        (long long)(allocated_count - freed_count)
    );
    fflush(stderr);
}

#else

void print_memory_leak_info() {
    // No-op when MTRACE is not defined
}
#endif