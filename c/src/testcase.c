#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "flintdb.h"
#include "runtime.h"
#include "internal.h"
#include "allocator.h"
#include "bplustree.h"
#include "buffer.h"
#include "hashmap.h"
#include "hyperloglog.h"
#include "iostream.h"
#include "list.h"
#include "roaringbitmap.h"
#include "sql.h"
#include "storage.h"
#include "plugin.h"


extern void print_memory_leak_info(); // in debug.c

#define PRINT_MEMORY_LEAK_INFO() \
    flintdb_cleanup(NULL);        \
    print_memory_leak_info()

#ifdef CPU_FEATURE_DETECT
// ./testcase.sh CPU_FEATURE_DETECT

int main(int argc, char **argv) {
    printf("=== CPU Feature Detection ===\n\n");

    // Architecture detection
    printf("Architecture:\n");
#if defined(__x86_64__) || defined(_M_X64)
    printf("  - x86_64 (64-bit)\n");
#elif defined(__i386__) || defined(_M_IX86)
    printf("  - x86 (32-bit)\n");
#elif defined(__aarch64__) || defined(_M_ARM64)
    printf("  - ARM64 (AArch64)\n");
#elif defined(__arm__) || defined(_M_ARM)
    printf("  - ARM (32-bit)\n");
#else
    printf("  - Unknown\n");
#endif

    printf("\nSIMD Support:\n");

    // ARM NEON detection
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    printf("  ✓ ARM NEON enabled\n");
#ifdef __aarch64__
    printf("    - 64-bit NEON (AArch64)\n");
#else
    printf("    - 32-bit NEON\n");
#endif
#else
#if defined(__aarch64__) || defined(__arm__)
    printf("  ✗ ARM NEON not enabled\n");
#endif
#endif

    // x86/x64 SIMD detection
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#ifdef __AVX512F__
    printf("  ✓ AVX-512 enabled\n");
#else
    printf("  ✗ AVX-512 not enabled\n");
#endif

#ifdef __AVX2__
    printf("  ✓ AVX2 enabled (32-byte SIMD)\n");
#else
    printf("  ✗ AVX2 not enabled\n");
#endif

#ifdef __AVX__
    printf("  ✓ AVX enabled\n");
#else
    printf("  ✗ AVX not enabled\n");
#endif

#ifdef __SSE4_2__
    printf("  ✓ SSE4.2 enabled\n");
#else
    printf("  ✗ SSE4.2 not enabled\n");
#endif

#ifdef __SSE4_1__
    printf("  ✓ SSE4.1 enabled\n");
#else
    printf("  ✗ SSE4.1 not enabled\n");
#endif

#ifdef __SSSE3__
    printf("  ✓ SSSE3 enabled\n");
#else
    printf("  ✗ SSSE3 not enabled\n");
#endif

#ifdef __SSE3__
    printf("  ✓ SSE3 enabled\n");
#else
    printf("  ✗ SSE3 not enabled\n");
#endif

#ifdef __SSE2__
    printf("  ✓ SSE2 enabled (16-byte SIMD)\n");
#else
    printf("  ✗ SSE2 not enabled\n");
#endif

#ifdef __SSE__
    printf("  ✓ SSE enabled\n");
#else
    printf("  ✗ SSE not enabled\n");
#endif
#endif

    printf("\nOptimized Modules:\n");
    printf("  - variant.c: SIMD memory operations\n");
    printf("  - row.c: SIMD + DECIMAL/DATE/TIME optimizations\n");
    printf("  - buffer.c: SIMD + endian conversion optimizations\n");

    printf("\nActive Optimizations:\n");
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    printf("  ✓ Using ARM NEON (16-byte chunks)\n");
#elif defined(__AVX2__)
    printf("  ✓ Using AVX2 (32-byte chunks)\n");
#elif defined(__SSE2__)
    printf("  ✓ Using SSE2 (16-byte chunks)\n");
#else
    printf("  - Using standard C library (fallback)\n");
#endif

    printf("\nCompiler:\n");
#if defined(__clang__)
    printf("  - Clang %d.%d.%d\n", __clang_major__, __clang_minor__, __clang_patchlevel__);
#elif defined(__GNUC__)
    printf("  - GCC %d.%d.%d\n", __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
#elif defined(_MSC_VER)
    printf("  - MSVC %d\n", _MSC_VER);
#else
    printf("  - Unknown\n");
#endif

    printf("\nBuild Flags:\n");
#ifdef __OPTIMIZE__
    printf("  ✓ Optimizations enabled\n");
#else
    printf("  - Optimizations disabled\n");
#endif

#ifdef NDEBUG
    printf("  ✓ Release build (NDEBUG)\n");
#else
    printf("  - Debug build\n");
#endif

#ifdef __FAST_MATH__
    printf("  ✓ Fast math enabled\n");
#endif

    printf("\n");

    return 0;
}

#endif

#ifdef TESTCASE_EXCEPTION
// ./testcase.sh TESTCASE_EXCEPTION

void test_func(char **e) {
    THROW(e, "This is a test exception");

EXCEPTION:
    return;
}

int main(int argc, char **argv) {
    char *e = NULL; // e will be pointed at TL_ERROR message buffer, no need to free

    test_func(NULL); // ignore error
    test_func(&e);   // handle error
    if (e) {
        printf("Exception: %s\n", e);
    }
    return 0;
}
#endif

#ifdef TESTCASE_ARRAYLIST
// ./testcase.sh TESTCASE_ARRAYLIST --mtrace

void list_entry_dealloc(valtype item) {
    if (item) {
        printf("Deallocating item: %s\n", (char *)item);
        FREE((char *)item);
    }
}

int main(int argc, char **argv) {
    char *e = NULL;
    struct list *list = arraylist_new(8);
    if (!list) {
        fprintf(stderr, "Failed to create list\n");
        return 1;
    }

    // Add some test entries
    for (int i = 0; i < 5; i++) {
        char *item = (char *)MALLOC(32);
        sprintf(item, "Item %d", i);
        list->add(list, (valtype)item, list_entry_dealloc, &e);
    }

    // Print and free the list
    for (int i = 0; i < list->count(list); i++) {
        const char *item = (const char *)list->get(list, i, &e);
        if (item) {
            printf("List item: %s\n", item);
        }
    }

    list->free(list);
    PRINT_MEMORY_LEAK_INFO();
    return 0;
}

#endif

#ifdef TESTCASE_ARRAYLIST_STRINGS_WRAP
// ./testcase.sh TESTCASE_ARRAYLIST_STRINGS_WRAP --mtrace

int main(int argc, char **argv) {
    char *e = NULL;
    const char *strings[] = {"apple", "banana", "cherry", "date", "elderberry"};
    int num_strings = sizeof(strings) / sizeof(strings[0]);
    struct list *list = arraylist_strings_wrap(num_strings, strings, &e);
    if (e)
        THROW_S(e);

    // Print the list contents
    for (int i = 0; i < list->count(list); i++) {
        const char *item = (const char *)list->get(list, i, &e);
        if (item) {
            printf("item[%d]: %s\n", i, item);
        }
    }
    printf("Total items: %d\n", list->count(list));

    list->free(list);
    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("Exception: %s", e);
    if (list)
        list->free(list);
    return 1;
}
#endif // TESTCASE_ARRAYLIST_STRINGS_WRAP

#ifdef TESTCASE_ARRAYLIST_STRING_SPLIT
// ./testcase.sh TESTCASE_ARRAYLIST_STRING_SPLIT --mtrace

int main(int argc, char **argv) {
    char *e = NULL;
    struct list *list = arraylist_string_split("apple&nbsp;banana&nbsp;cherry&nbsp;date&nbsp;elderberry", "&nbsp;", &e);
    if (e)
        THROW_S(e);

    // Print the list contents
    for (int i = 0; i < list->count(list); i++) {
        const char *item = (const char *)list->get(list, i, &e);
        if (item) {
            printf("item[%d]: %s\n", i, item);
        }
    }
    printf("Total items: %d\n", list->count(list));

    list->free(list);
    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("Exception: %s", e);
    if (list)
        list->free(list);
    return 1;
}

#endif //

#ifdef TESTCASE_EXCEPTION2
// ./testcase.sh TESTCASE_EXCEPTION2
int main(int argc, char **argv) {
    char e[256] = {0};

    sprintf(e, "This is a test exception2");
    THROW_S(e); // use existing string e

EXCEPTION:
    printf("Caught exception : %s\n", e);
    return 0;
}
#endif

#ifdef TESTCASE_BUFFER
// ./testcase.sh TESTCASE_BUFFER
int main(int argc, char **argv) {
    struct buffer *b = buffer_alloc(100);

    b->i32_put(b, 123456, NULL);
    b->f64_put(b, 3.14159, NULL);
    b->array_put(b, "Hello, World!", 13, NULL);
    b->flip(b);

    printf("i32: %d\n", b->i32_get(b, NULL));

    struct buffer slice = {0};
    b->slice(b, 0, b->remaining(b), &slice, NULL);
    printf("f64: %f\n", slice.f64_get(&slice, NULL));
    char *s = slice.array_get(&slice, 13, NULL);
    printf("str: %.*s\n", 13, s);

    b->free(b);

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif

#ifdef TESTCASE_STORAGE
// ./testcase.sh TESTCASE_STORAGE
int main(int argc, char **argv) {
    struct storage_opts opts = {
        .file = "./temp/strorage.bin",
        .mode = FLINTDB_RDWR,
        .block_bytes = 512 - 16,
        // .extra_header_bytes = 0,
        // .compact = -1,
        // .increment = -1, // 1024*1024*10,
        // .type = TYPE_DEFAULT,
        // .compress = ""
    };
    unlink(opts.file);

    char *e = NULL;
    struct storage s = {0};

    int ok = storage_open(&s, opts, &e);
    assert(ok == 0);

    char str[1000] = {
        0,
    };
    struct buffer bb = {0};

    STOPWATCH_START(watch);
    int max = 2 * 1024 * 1024;
    // int max = 1_000_000; --- IGNORE ---
    for (int i = 0; i < max; i++) {
        sprintf(str, "Hello, %s! %03d", "PRODUCT_NAME", i + 1);
        buffer_wrap(str, strlen(str), &bb);
        s.write(&s, &bb, &e);
    }

    i64 count = s.count_get(&s);
    printf("time  : %lld \n", time_elapsed(&watch));
    printf("ops   : %f \n", time_ops(count, &watch));
    printf("count : %lld \n", count);
    printf("bytes : %lld \n", s.bytes_get(&s));

    for (i64 i = count - 10; i < (count); i++) {
        struct buffer *r = s.read(&s, i, &e);
        int remaining = r->remaining(r);
        // printf("read remaining : %d \n", remaining);
        memcpy(str, r->array_get(r, remaining, NULL), remaining);
        str[remaining] = '\0';
        printf("read : %d - %s \n", remaining, str);
        r->free(r);
    }

    s.close(&s);

    unlink(opts.file);
    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif // TESTCASE_STORAGE

#ifdef TESTCASE_STORAGE_DIO
// ./testcase.sh TESTCASE_STORAGE_DIO

int main(int argc, char **argv) {
    struct storage_opts opts = {
        .file = "./temp/storage_dio.bin",
        .mode = FLINTDB_RDWR,
        .block_bytes = 512 - 16,
        .type = TYPE_DIO,
    };
    unlink(opts.file);

    char *e = NULL;
    struct storage s = {0};

    int ok = storage_open(&s, opts, &e);
    assert(ok == 0);

    char str[1000] = {
        0,
    };
    struct buffer bb = {0};

    STOPWATCH_START(watch);
    int max = 2 * 1024 * 1024;
    for (int i = 0; i < max; i++) {
        sprintf(str, "Hello, %s! %03d", "PRODUCT_NAME", i + 1);
        buffer_wrap(str, strlen(str), &bb);
        s.write(&s, &bb, &e);
    }

    i64 count = s.count_get(&s);
    printf("time  : %lld \n", time_elapsed(&watch));
    printf("ops   : %f \n", time_ops(count, &watch));
    printf("count : %lld \n", count);
    printf("bytes : %lld \n", s.bytes_get(&s));

    for (i64 i = count - 10; i < (count); i++) {
        struct buffer *r = s.read(&s, i, &e);
        int remaining = r->remaining(r);
        // printf("read remaining : %d \n", remaining);
        memcpy(str, r->array_get(r, remaining, NULL), remaining);
        str[remaining] = '\0';
        printf("read : %d - %s \n", remaining, str);
        r->free(r);
    }

    s.close(&s);

    unlink(opts.file);
    PRINT_MEMORY_LEAK_INFO();
    return 0;
}

#endif // TESTCASE_STORAGE_DIO

#ifdef TESTCASE_STORAGE_DIO_RANDOM
// ./testcase.sh TESTCASE_STORAGE_DIO_RANDOM
// Usage: ./bin/testcase [N_init=100000] [M_ops=200000] [seed=42]
// Random mix of: reads, overwrites (incl. overflow), deletes+reinserts.

static u64 fnv1a64(const void *data, size_t n) {
    const unsigned char *p = (const unsigned char *)data;
    u64 h = 1469598103934665603ull;
    for (size_t i = 0; i < n; i++) {
        h ^= (u64)p[i];
        h *= 1099511628211ull;
    }
    return h;
}

static int build_payload(char *out, size_t out_cap, i64 slot, u32 ver, u32 len) {
    // Deterministic payload: header + repeated pattern.
    // Ensure there's always at least some prefix text.
    int n = snprintf(out, out_cap, "slot=%lld ver=%u ", (long long)slot, (unsigned)ver);
    if (n < 0) return -1;
    size_t pos = (size_t)n;
    if (pos >= out_cap) return -1;
    while (pos < (size_t)len && pos < out_cap) {
        char ch = (char)('a' + (char)((slot + (i64)ver + (i64)pos) % 26));
        out[pos] = ch;
        pos++;
    }
    if (pos > out_cap) return -1;
    return (int)pos;
}

int main(int argc, char **argv) {
    i64 N_init = 100000;
    i64 M_ops = 200000;
    unsigned seed = 42;
    if (argc >= 2) {
        long long t = atoll(argv[1]);
        if (t > 0) N_init = (i64)t;
    }
    if (argc >= 3) {
        long long t = atoll(argv[2]);
        if (t > 0) M_ops = (i64)t;
    }
    if (argc >= 4) {
        long long t = atoll(argv[3]);
        if (t >= 0) seed = (unsigned)t;
    }

    struct storage_opts opts = {
        .file = "./temp/storage_dio_random.bin",
        .mode = FLINTDB_RDWR,
        .block_bytes = 512 - 16,
        .type = TYPE_DIO,
    };
    unlink(opts.file);

    char *e = NULL;
    struct storage s = {0};
    int ok = storage_open(&s, opts, &e);
    assert(ok == 0);

    i64 *offs = (i64 *)CALLOC((size_t)N_init, sizeof(i64));
    u64 *hashes = (u64 *)CALLOC((size_t)N_init, sizeof(u64));
    u32 *lens = (u32 *)CALLOC((size_t)N_init, sizeof(u32));
    u32 *vers = (u32 *)CALLOC((size_t)N_init, sizeof(u32));
    assert(offs && hashes && lens && vers);

    // Payload sizes: include overflow sometimes (up to 3 blocks worth of data).
    const u32 block_data = (u32)opts.block_bytes;
    const u32 max_payload = (block_data * 3u);
    char *payload = (char *)MALLOC((size_t)max_payload + 64);
    assert(payload);
    struct buffer bb = {0};

    srand(seed);

    // Initial population
    STOPWATCH_START(w_init);
    for (i64 i = 0; i < N_init; i++) {
        u32 len = (u32)(16 + (rand() % (int)(max_payload - 16)));
        int actual = build_payload(payload, (size_t)max_payload + 64, i, 1, len);
        if (actual < 0) {
            fprintf(stderr, "payload build failed\n");
            abort();
        }
        buffer_wrap(payload, (u32)actual, &bb);
        offs[i] = s.write(&s, &bb, &e);
        if (e && *e) {
            fprintf(stderr, "write error: %s\n", e);
            abort();
        }
        lens[i] = (u32)actual;
        vers[i] = 1;
        hashes[i] = fnv1a64(payload, (size_t)actual);
    }
    printf("init: %lld writes, %lldms\n", (long long)N_init, (long long)time_elapsed(&w_init));

    // Random operations
    STOPWATCH_START(w_ops);
    i64 reads = 0, overwrites = 0, deletes = 0;
    for (i64 op = 0; op < M_ops; op++) {
        i64 idx = (i64)(rand() % (int)(N_init > 0 ? N_init : 1));
        int r = rand() % 100;

        if (r < 70) {
            // Read + verify
            struct buffer *rb = s.read(&s, offs[idx], &e);
            if (e && *e) {
                fprintf(stderr, "read error at idx=%lld off=%lld: %s\n", (long long)idx, (long long)offs[idx], e);
                abort();
            }
            int n = rb->remaining(rb);
            if (n < 0 || (u32)n != lens[idx]) {
                fprintf(stderr, "len mismatch idx=%lld off=%lld got=%d expected=%u\n", (long long)idx, (long long)offs[idx], n, (unsigned)lens[idx]);
                abort();
            }
            char *p = rb->array_get(rb, (u32)n, NULL);
            u64 h = fnv1a64(p, (size_t)n);
            if (h != hashes[idx]) {
                fprintf(stderr, "hash mismatch idx=%lld off=%lld\n", (long long)idx, (long long)offs[idx]);
                abort();
            }
            rb->free(rb);
            reads++;
        } else if (r < 90) {
            // Overwrite at same offset (forces random access overwrite path)
            u32 len = (u32)(16 + (rand() % (int)(max_payload - 16)));
            vers[idx]++;
            int actual = build_payload(payload, (size_t)max_payload + 64, idx, vers[idx], len);
            if (actual < 0) {
                fprintf(stderr, "payload build failed\n");
                abort();
            }
            buffer_wrap(payload, (u32)actual, &bb);
            (void)s.write_at(&s, offs[idx], &bb, &e);
            if (e && *e) {
                fprintf(stderr, "write_at error idx=%lld off=%lld: %s\n", (long long)idx, (long long)offs[idx], e);
                abort();
            }
            lens[idx] = (u32)actual;
            hashes[idx] = fnv1a64(payload, (size_t)actual);
            overwrites++;
        } else {
            // Delete then insert a new record (exercises free-list reuse)
            (void)s.delete(&s, offs[idx], &e);
            if (e && *e) {
                fprintf(stderr, "delete error idx=%lld off=%lld: %s\n", (long long)idx, (long long)offs[idx], e);
                abort();
            }
            u32 len = (u32)(16 + (rand() % (int)(max_payload - 16)));
            vers[idx]++;
            int actual = build_payload(payload, (size_t)max_payload + 64, idx, vers[idx], len);
            if (actual < 0) {
                fprintf(stderr, "payload build failed\n");
                abort();
            }
            buffer_wrap(payload, (u32)actual, &bb);
            offs[idx] = s.write(&s, &bb, &e);
            if (e && *e) {
                fprintf(stderr, "write error after delete idx=%lld: %s\n", (long long)idx, e);
                abort();
            }
            lens[idx] = (u32)actual;
            hashes[idx] = fnv1a64(payload, (size_t)actual);
            deletes++;
        }
    }
    u64 ms_ops = time_elapsed(&w_ops);
    printf("ops: %lld total, %lldms, %.0f ops/sec (reads=%lld overwrites=%lld delete+insert=%lld)\n",
           (long long)M_ops, (long long)ms_ops, (double)M_ops / ((double)ms_ops / 1000.0),
           (long long)reads, (long long)overwrites, (long long)deletes);

    // Final spot-checks
    for (int j = 0; j < 20; j++) {
        i64 idx = (i64)(rand() % (int)(N_init > 0 ? N_init : 1));
        struct buffer *rb = s.read(&s, offs[idx], &e);
        if (e && *e) {
            fprintf(stderr, "final read error idx=%lld off=%lld: %s\n", (long long)idx, (long long)offs[idx], e);
            abort();
        }
        int n = rb->remaining(rb);
        char *p = rb->array_get(rb, (u32)n, NULL);
        u64 h = fnv1a64(p, (size_t)n);
        if ((u32)n != lens[idx] || h != hashes[idx]) {
            fprintf(stderr, "final verify failed idx=%lld off=%lld\n", (long long)idx, (long long)offs[idx]);
            abort();
        }
        rb->free(rb);
    }

    s.close(&s);
    FREE(payload);
    FREE(offs);
    FREE(hashes);
    FREE(lens);
    FREE(vers);
    unlink(opts.file);
    PRINT_MEMORY_LEAK_INFO();
    return 0;
}

#endif // TESTCASE_STORAGE_DIO_RANDOM

#ifdef TESTCASE_STORAGE_DIO_RANDOM_MT
// ./testcase.sh TESTCASE_STORAGE_DIO_RANDOM_MT
// Usage: ./bin/testcase [threads=4] [N_init=50000] [M_ops=200000] [seed=42] [lock_storage=1]
// - lock_storage=1: serialize storage ops (should be stable)
// - lock_storage=0: allow concurrent storage ops (expected to be unsafe; can expose races)

static u64 fnv1a64(const void *data, size_t n) {
    const unsigned char *p = (const unsigned char *)data;
    u64 h = 1469598103934665603ull;
    for (size_t i = 0; i < n; i++) {
        h ^= (u64)p[i];
        h *= 1099511628211ull;
    }
    return h;
}

static int build_payload(char *out, size_t out_cap, i64 slot, u32 ver, u32 len) {
    int n = snprintf(out, out_cap, "slot=%lld ver=%u ", (long long)slot, (unsigned)ver);
    if (n < 0) return -1;
    size_t pos = (size_t)n;
    if (pos >= out_cap) return -1;
    while (pos < (size_t)len && pos < out_cap) {
        char ch = (char)('a' + (char)((slot + (i64)ver + (i64)pos) % 26));
        out[pos] = ch;
        pos++;
    }
    if (pos > out_cap) return -1;
    return (int)pos;
}

typedef struct dio_mt_ctx {
    struct storage *s;
    i64 N;
    i64 ops;
    u32 max_payload;

    volatile int failed;

    i64 *offs;
    u64 *hashes;
    u32 *lens;
    u32 *vers;

    pthread_mutex_t *stripes;
    int stripe_n;
    pthread_mutex_t storage_mtx;
    int lock_storage;
} dio_mt_ctx;

static inline int dio_mt_is_failed(dio_mt_ctx *ctx) {
    return __atomic_load_n(&ctx->failed, __ATOMIC_SEQ_CST) != 0;
}

static inline void dio_mt_set_failed(dio_mt_ctx *ctx) {
    __atomic_store_n(&ctx->failed, 1, __ATOMIC_SEQ_CST);
}

static inline u64 xorshift64(u64 *state) {
    u64 x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

static inline int stripe_index(dio_mt_ctx *ctx, i64 idx) {
    // idx is non-negative
    return (int)((u64)idx % (u64)ctx->stripe_n);
}

typedef struct dio_mt_worker {
    dio_mt_ctx *ctx;
    i64 tid;
    u64 rng;
    i64 reads;
    i64 overwrites;
    i64 deletes;
} dio_mt_worker;

static void *dio_mt_run(void *arg) {
    dio_mt_worker *w = (dio_mt_worker *)arg;
    dio_mt_ctx *ctx = w->ctx;

    char *payload = (char *)MALLOC((size_t)ctx->max_payload + 64);
    assert(payload);
    struct buffer bb = {0};

    for (i64 op = 0; op < ctx->ops; op++) {
        if (dio_mt_is_failed(ctx)) break;
        i64 idx = (i64)(xorshift64(&w->rng) % (u64)(ctx->N > 0 ? ctx->N : 1));
        int r = (int)(xorshift64(&w->rng) % 100ull);
        int si = stripe_index(ctx, idx);

        if (r < 70) {
            // Read + verify (lock slot stripe to avoid concurrent modification)
            pthread_mutex_lock(&ctx->stripes[si]);
            i64 off = ctx->offs[idx];
            u32 len = ctx->lens[idx];
            u64 expect = ctx->hashes[idx];
            char *e = NULL;
            if (ctx->lock_storage) pthread_mutex_lock(&ctx->storage_mtx);
            struct buffer *rb = ctx->s->read(ctx->s, off, &e);
            if (ctx->lock_storage) pthread_mutex_unlock(&ctx->storage_mtx);
            if (e && *e) {
                fprintf(stderr, "[T%lld] read error idx=%lld off=%lld: %s\n", (long long)w->tid, (long long)idx, (long long)off, e);
                dio_mt_set_failed(ctx);
                pthread_mutex_unlock(&ctx->stripes[si]);
                FREE(payload);
                return (void *)1;
            }
            int n = rb->remaining(rb);
            if ((u32)n != len) {
                fprintf(stderr, "[T%lld] len mismatch idx=%lld off=%lld got=%d expected=%u\n", (long long)w->tid, (long long)idx, (long long)off, n, (unsigned)len);
                rb->free(rb);
                dio_mt_set_failed(ctx);
                pthread_mutex_unlock(&ctx->stripes[si]);
                FREE(payload);
                return (void *)1;
            }
            char *p = rb->array_get(rb, (u32)n, NULL);
            u64 h = fnv1a64(p, (size_t)n);
            if (h != expect) {
                fprintf(stderr, "[T%lld] hash mismatch idx=%lld off=%lld\n", (long long)w->tid, (long long)idx, (long long)off);
                rb->free(rb);
                dio_mt_set_failed(ctx);
                pthread_mutex_unlock(&ctx->stripes[si]);
                FREE(payload);
                return (void *)1;
            }
            rb->free(rb);
            pthread_mutex_unlock(&ctx->stripes[si]);
            w->reads++;
        } else if (r < 90) {
            // Overwrite at same offset
            pthread_mutex_lock(&ctx->stripes[si]);
            i64 off = ctx->offs[idx];
            ctx->vers[idx]++;
            u32 ver = ctx->vers[idx];
            u32 len = (u32)(16 + (xorshift64(&w->rng) % (u64)(ctx->max_payload - 16)));
            int actual = build_payload(payload, (size_t)ctx->max_payload + 64, idx, ver, len);
            if (actual < 0) {
                fprintf(stderr, "[T%lld] payload build failed\n", (long long)w->tid);
                dio_mt_set_failed(ctx);
                pthread_mutex_unlock(&ctx->stripes[si]);
                FREE(payload);
                return (void *)1;
            }
            buffer_wrap(payload, (u32)actual, &bb);
            char *e = NULL;
            if (ctx->lock_storage) pthread_mutex_lock(&ctx->storage_mtx);
            (void)ctx->s->write_at(ctx->s, off, &bb, &e);
            if (ctx->lock_storage) pthread_mutex_unlock(&ctx->storage_mtx);
            if (e && *e) {
                fprintf(stderr, "[T%lld] write_at error idx=%lld off=%lld: %s\n", (long long)w->tid, (long long)idx, (long long)off, e);
                dio_mt_set_failed(ctx);
                pthread_mutex_unlock(&ctx->stripes[si]);
                FREE(payload);
                return (void *)1;
            }
            ctx->lens[idx] = (u32)actual;
            ctx->hashes[idx] = fnv1a64(payload, (size_t)actual);
            pthread_mutex_unlock(&ctx->stripes[si]);
            w->overwrites++;
        } else {
            // Delete then insert new record (free-list reuse)
            pthread_mutex_lock(&ctx->stripes[si]);
            i64 off = ctx->offs[idx];
            char *e = NULL;
            if (ctx->lock_storage) pthread_mutex_lock(&ctx->storage_mtx);
            (void)ctx->s->delete(ctx->s, off, &e);
            if (ctx->lock_storage) pthread_mutex_unlock(&ctx->storage_mtx);
            if (e && *e) {
                fprintf(stderr, "[T%lld] delete error idx=%lld off=%lld: %s\n", (long long)w->tid, (long long)idx, (long long)off, e);
                dio_mt_set_failed(ctx);
                pthread_mutex_unlock(&ctx->stripes[si]);
                FREE(payload);
                return (void *)1;
            }

            ctx->vers[idx]++;
            u32 ver = ctx->vers[idx];
            u32 len = (u32)(16 + (xorshift64(&w->rng) % (u64)(ctx->max_payload - 16)));
            int actual = build_payload(payload, (size_t)ctx->max_payload + 64, idx, ver, len);
            if (actual < 0) {
                fprintf(stderr, "[T%lld] payload build failed\n", (long long)w->tid);
                dio_mt_set_failed(ctx);
                pthread_mutex_unlock(&ctx->stripes[si]);
                FREE(payload);
                return (void *)1;
            }
            buffer_wrap(payload, (u32)actual, &bb);

            e = NULL;
            if (ctx->lock_storage) pthread_mutex_lock(&ctx->storage_mtx);
            i64 new_off = ctx->s->write(ctx->s, &bb, &e);
            if (ctx->lock_storage) pthread_mutex_unlock(&ctx->storage_mtx);
            if (e && *e) {
                fprintf(stderr, "[T%lld] write error after delete idx=%lld: %s\n", (long long)w->tid, (long long)idx, e);
                dio_mt_set_failed(ctx);
                pthread_mutex_unlock(&ctx->stripes[si]);
                FREE(payload);
                return (void *)1;
            }
            ctx->offs[idx] = new_off;
            ctx->lens[idx] = (u32)actual;
            ctx->hashes[idx] = fnv1a64(payload, (size_t)actual);
            pthread_mutex_unlock(&ctx->stripes[si]);
            w->deletes++;
        }
    }

    FREE(payload);
    if (dio_mt_is_failed(ctx)) return (void *)1;
    return NULL;
}

int main(int argc, char **argv) {
    int threads = 4;
    i64 N_init = 50000;
    i64 M_ops = 200000;
    unsigned seed = 42;
    int lock_storage = 1;
    if (argc >= 2) {
        int t = atoi(argv[1]);
        if (t > 0) threads = t;
    }
    if (argc >= 3) {
        long long t = atoll(argv[2]);
        if (t > 0) N_init = (i64)t;
    }
    if (argc >= 4) {
        long long t = atoll(argv[3]);
        if (t > 0) M_ops = (i64)t;
    }
    if (argc >= 5) {
        long long t = atoll(argv[4]);
        if (t >= 0) seed = (unsigned)t;
    }
    if (argc >= 6) {
        int t = atoi(argv[5]);
        if (t == 0 || t == 1) lock_storage = t;
    }

    struct storage_opts opts = {
        .file = "./temp/storage_dio_random_mt.bin",
        .mode = FLINTDB_RDWR,
        .block_bytes = 512 - 16,
        .type = TYPE_DIO,
    };
    unlink(opts.file);

    char *e = NULL;
    struct storage s = {0};
    int ok = storage_open(&s, opts, &e);
    assert(ok == 0);

    dio_mt_ctx ctx = {0};
    ctx.s = &s;
    ctx.N = N_init;
    ctx.ops = (threads > 0) ? (M_ops / (i64)threads) : M_ops;
    ctx.max_payload = (u32)opts.block_bytes * 3u;
    ctx.lock_storage = lock_storage;
    ctx.failed = 0;

    if (lock_storage == 0) {
        fprintf(stderr, "NOTE: lock_storage=0 is intentionally unsafe; failures are expected (storage is not thread-safe).\n");
    }

    ctx.offs = (i64 *)CALLOC((size_t)N_init, sizeof(i64));
    ctx.hashes = (u64 *)CALLOC((size_t)N_init, sizeof(u64));
    ctx.lens = (u32 *)CALLOC((size_t)N_init, sizeof(u32));
    ctx.vers = (u32 *)CALLOC((size_t)N_init, sizeof(u32));
    assert(ctx.offs && ctx.hashes && ctx.lens && ctx.vers);

    ctx.stripe_n = 1024;
    ctx.stripes = (pthread_mutex_t *)CALLOC((size_t)ctx.stripe_n, sizeof(pthread_mutex_t));
    assert(ctx.stripes);
    for (int i = 0; i < ctx.stripe_n; i++) {
        pthread_mutex_init(&ctx.stripes[i], NULL);
    }
    pthread_mutex_init(&ctx.storage_mtx, NULL);

    // Initial population (single-threaded)
    char *payload = (char *)MALLOC((size_t)ctx.max_payload + 64);
    assert(payload);
    struct buffer bb = {0};
    STOPWATCH_START(w_init);
    u64 rng = ((u64)seed << 1) ^ 0x9e3779b97f4a7c15ull;
    for (i64 i = 0; i < N_init; i++) {
        u32 len = (u32)(16 + (xorshift64(&rng) % (u64)(ctx.max_payload - 16)));
        ctx.vers[i] = 1;
        int actual = build_payload(payload, (size_t)ctx.max_payload + 64, i, ctx.vers[i], len);
        if (actual < 0) abort();
        buffer_wrap(payload, (u32)actual, &bb);
        ctx.offs[i] = s.write(&s, &bb, &e);
        if (e && *e) {
            fprintf(stderr, "init write error: %s\n", e);
            abort();
        }
        ctx.lens[i] = (u32)actual;
        ctx.hashes[i] = fnv1a64(payload, (size_t)actual);
    }
    printf("init: %lld writes, %lldms\n", (long long)N_init, (long long)time_elapsed(&w_init));
    FREE(payload);

    // Run workers
    pthread_t *tids = (pthread_t *)CALLOC((size_t)threads, sizeof(pthread_t));
    dio_mt_worker *workers = (dio_mt_worker *)CALLOC((size_t)threads, sizeof(dio_mt_worker));
    assert(tids && workers);

    STOPWATCH_START(w_ops);
    for (int i = 0; i < threads; i++) {
        workers[i].ctx = &ctx;
        workers[i].tid = i;
        workers[i].rng = (((u64)seed + (u64)i * 1315423911ull) ^ 0xD1B54A32D192ED03ull);
        pthread_create(&tids[i], NULL, dio_mt_run, &workers[i]);
    }
    for (int i = 0; i < threads; i++) {
        void *ret = NULL;
        pthread_join(tids[i], &ret);
        if (ret != NULL) dio_mt_set_failed(&ctx);
    }
    u64 ms_ops = time_elapsed(&w_ops);

    i64 reads = 0, overwrites = 0, deletes = 0;
    for (int i = 0; i < threads; i++) {
        reads += workers[i].reads;
        overwrites += workers[i].overwrites;
        deletes += workers[i].deletes;
    }
    i64 total_ops = (i64)threads * ctx.ops;
    printf("mt ops: %lld total, %lldms, %.0f ops/sec (threads=%d lock_storage=%d reads=%lld overwrites=%lld delete+insert=%lld)\n",
           (long long)total_ops, (long long)ms_ops, (double)total_ops / ((double)ms_ops / 1000.0),
           threads, lock_storage, (long long)reads, (long long)overwrites, (long long)deletes);

    // Final verify some slots (skip if already failed)
    if (!dio_mt_is_failed(&ctx))
    rng = ((u64)seed << 1) ^ 0xA0761D6478BD642Full;
    for (int j = 0; j < 50; j++) {
        i64 idx = (i64)(xorshift64(&rng) % (u64)(ctx.N > 0 ? ctx.N : 1));
        int si = stripe_index(&ctx, idx);
        pthread_mutex_lock(&ctx.stripes[si]);
        i64 off = ctx.offs[idx];
        u32 len = ctx.lens[idx];
        u64 expect = ctx.hashes[idx];
        char *e2 = NULL;
        if (ctx.lock_storage) pthread_mutex_lock(&ctx.storage_mtx);
        struct buffer *rb = s.read(&s, off, &e2);
        if (ctx.lock_storage) pthread_mutex_unlock(&ctx.storage_mtx);
        if (e2 && *e2) {
            fprintf(stderr, "final read error idx=%lld off=%lld: %s\n", (long long)idx, (long long)off, e2);
            dio_mt_set_failed(&ctx);
            pthread_mutex_unlock(&ctx.stripes[si]);
            break;
        }
        int n = rb->remaining(rb);
        char *p = rb->array_get(rb, (u32)n, NULL);
        u64 h = fnv1a64(p, (size_t)n);
        if ((u32)n != len || h != expect) {
            fprintf(stderr, "final verify failed idx=%lld off=%lld\n", (long long)idx, (long long)off);
            rb->free(rb);
            dio_mt_set_failed(&ctx);
            pthread_mutex_unlock(&ctx.stripes[si]);
            break;
        }
        rb->free(rb);
        pthread_mutex_unlock(&ctx.stripes[si]);
    }

    s.close(&s);
    for (int i = 0; i < ctx.stripe_n; i++) pthread_mutex_destroy(&ctx.stripes[i]);
    pthread_mutex_destroy(&ctx.storage_mtx);

    FREE(ctx.stripes);
    FREE(ctx.offs);
    FREE(ctx.hashes);
    FREE(ctx.lens);
    FREE(ctx.vers);
    FREE(tids);
    FREE(workers);
    unlink(opts.file);
    PRINT_MEMORY_LEAK_INFO();
    if (dio_mt_is_failed(&ctx)) {
        fprintf(stderr, "TESTCASE_STORAGE_DIO_RANDOM_MT: FAILED (lock_storage=%d)\n", lock_storage);
        return 2;
    }
    return 0;
}

#endif // TESTCASE_STORAGE_DIO_RANDOM_MT

#ifdef TESTCASE_BPLUSTREE
// ./testcase.sh TESTCASE_BPLUSTREE

int i64_cmpr(void *obj, i64 a, i64 b) {
    if (a > b)
        return 1;
    if (a < b)
        return -1;
    return 0;
}

void btree_make(i64 max) {
    char *e = NULL;
    const char *path = "./temp/test.btree";

    unlink(path);

    STOPWATCH_START(watch);
    struct bplustree tree;
    memset(&tree, 0, sizeof(tree));
    int ok = bplustree_init(&tree, path, 0, FLINTDB_RDWR, TYPE_DEFAULT, NULL, i64_cmpr, NULL, &e);
    if (ok != 0) {
        fprintf(stderr, "init error: %s\n", e);
        return;
    }

    // insert

    for (i64 i = 1; i <= max; i++) {
        tree.put(&tree, i, &e);
    }

    tree.close(&tree);

    printf("time  : %lld \n", time_elapsed(&watch));
    printf("ops   : %f \n", time_ops(max, &watch));
    printf("count : %lld \n", max);
}

extern void bplustree_traverse_leaf(struct bplustree *me);     // for debug
extern void bplustree_traverse_internal(struct bplustree *me); // for debug

void btree_trace() {
    char *e = NULL;

    const char *path = "./temp/test.btree";

    struct bplustree tree;
    memset(&tree, 0, sizeof(tree));
    int ok = bplustree_init(&tree, path, 0, FLINTDB_RDONLY, TYPE_DEFAULT, NULL, i64_cmpr, NULL, &e);
    if (ok != 0) {
        fprintf(stderr, "init error: %s\n", e);
        return;
    }

    bplustree_traverse_leaf(&tree);
    bplustree_traverse_internal(&tree);

    tree.close(&tree);
}

void btree_read() {
    char *e = NULL;

    const char *path = "./temp/test.btree";

    struct bplustree tree;
    memset(&tree, 0, sizeof(tree));
    int ok = bplustree_init(&tree, path, 0, FLINTDB_RDONLY, TYPE_DEFAULT, NULL, i64_cmpr, NULL, &e);
    if (ok != 0) {
        fprintf(stderr, "init error: %s\n", e);
        return;
    }

    i64 max = tree.count_get(&tree);
    // printf("count: %lld\n", max);

    STOPWATCH_START(watch);
    for (i64 i = 1; i <= max; i++) {
        i64 v = tree.get(&tree, i, &e);
        if (v != i) {
            fprintf(stderr, "get error: %lld != %lld\n", v, i);
            // Let's test a few more around this key
            // for (i64 j = i-2; j <= i+2; j++) {
            //     if (j >= 1 && j <= max) {
            //         i64 test_v = tree.get(&tree, j, &e);
            //         fprintf(stderr, "debug get(%lld) = %lld\n", j, test_v);
            //     }
            // }
            break;
        }
    }

    printf("%lld rows, %lld ms, %f ops\n", max, time_elapsed(&watch), time_ops(max, &watch));
    tree.close(&tree);

    PRINT_MEMORY_LEAK_INFO();
}

int main(int argc, char **argv) {
    printf("btree_make --------------------\n");
    btree_make(1024 * 1024 * 1);
    // printf("btree_debug --------------------\n");
    // btree_trace();
    printf("btree_read --------------------\n");
    btree_read();

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}

#endif

#ifdef TESTCASE_TRANSACTION
// ./testcase.sh TESTCASE_TRANSACTION

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    char *e = NULL;
    struct flintdb_table *tbl = NULL;
    struct flintdb_transaction *tx = NULL;

    const char *tablename = "temp/tx_test"TABLE_NAME_SUFFIX;
    const char *walname = "temp/tx_test"TABLE_NAME_SUFFIX".wal";
    
    struct flintdb_meta mt = flintdb_meta_new("tx_test"TABLE_NAME_SUFFIX, &e);
    // NOTE: meta.wal is empty by default, which disables WAL (WAL_NONE).
    // For this testcase, we need WAL enabled so rollback is meaningful.
    strncpy(mt.wal, WAL_OPT_LOG, sizeof(mt.wal) - 1);
    flintdb_meta_columns_add(&mt, "customer_id", VARIANT_INT64, 0, 0, SPEC_NULLABLE, "0", "int64 primary key", &e);
    flintdb_meta_columns_add(&mt, "customer_name", VARIANT_STRING, 255, 0, SPEC_NULLABLE, "0", "", &e);

    char keys_arr[1][MAX_COLUMN_NAME_LIMIT] = {"customer_id"};
    flintdb_meta_indexes_add(&mt, PRIMARY_NAME, NULL, (const char (*)[MAX_COLUMN_NAME_LIMIT])keys_arr, 1, &e);
    if (e) THROW_S(e);

    flintdb_table_drop(tablename, NULL); // ignore error
    (void)unlink(walname); // ignore error

    tbl = flintdb_table_open(tablename, FLINTDB_RDWR, &mt, &e);
    if (e) THROW_S(e);
    if (!tbl) THROW(&e, "table_open failed");

    // 1) Commit path: begin -> apply(2 rows) -> commit
    tx = flintdb_transaction_begin(tbl, &e);
    if (e) THROW_S(e);
    if (!tx) THROW(&e, "transaction_begin failed");

    for (int i = 1; i <= 2; i++) {
        struct flintdb_row *r = flintdb_row_new(&mt, &e);
        if (e) THROW_S(e);
        r->i64_set(r, 0, i, &e);
        if (e) THROW_S(e);
        char name[64];
        snprintf(name, sizeof(name), "Name-%d", i);
        r->string_set(r, 1, name, &e);
        if (e) THROW_S(e);

        i64 rowid = tx->apply(tx, r, 1, &e);
        if (e) THROW_S(e);
        if (rowid < 0) THROW(&e, "tx apply failed");
        TRACE("tx apply: customer_id=%d => rowid=%lld", i, rowid);
        r->free(r);
    }

    tx->commit(tx, &e);
    if (e) THROW_S(e);
    tx->close(tx);
    tx = NULL;

    i64 rows = tbl->rows(tbl, &e);
    if (e) THROW_S(e);
    TRACE("rows after commit=%lld", rows);
    assert(rows == 2);

    TRACE("before one(customer_id=1)");

    const char *argv1[] = {"customer_id", "1"};
    const struct flintdb_row *r1 = tbl->one(tbl, 0, 2, argv1, &e);
    if (e) THROW_S(e);
    assert(r1);
    assert(strcmp(r1->string_get(r1, 1, &e), "Name-1") == 0);
    if (e) THROW_S(e);

    TRACE("after one(customer_id=1)");

    // 2) Rollback path: begin -> apply(1 row) -> rollback
    TRACE("before begin #2");
    tx = flintdb_transaction_begin(tbl, &e);
    if (e) THROW_S(e);
    if (!tx) THROW(&e, "transaction_begin failed");

    {
        struct flintdb_row *r = flintdb_row_new(&mt, &e);
        if (e) THROW_S(e);
        r->i64_set(r, 0, 3, &e);
        if (e) THROW_S(e);
        r->string_set(r, 1, "Name-3", &e);
        if (e) THROW_S(e);
        (void)tx->apply(tx, r, 1, &e);
        if (e) THROW_S(e);
        r->free(r);
    }

    tx->rollback(tx, &e);
    if (e) THROW_S(e);
    tx->close(tx);
    tx = NULL;

    TRACE("after rollback #2");

    rows = tbl->rows(tbl, &e);
    if (e) THROW_S(e);
    TRACE("rows after rollback=%lld", rows);
    assert(rows == 2);

    const char *argv3[] = {"customer_id", "3"};
    const struct flintdb_row *r3 = tbl->one(tbl, 0, 2, argv3, &e);
    if (e) THROW_S(e);
    assert(r3 == NULL);

EXCEPTION:
    if (e) WARN("EXC: %s", e);
    if (tx) tx->close(tx);
    if (tbl) tbl->close(tbl);
    flintdb_meta_close(&mt);

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}

#endif

#ifdef TESTCASE_BPLUSTREE_DELETE2
/*
삭제 기능에 대한 직접 테스트 추가:
TESTCASE_BPLUSTREE에 일부 키 삭제 시나리오를 추가하거나 별도 TESTCASE 플래그로
N개 삽입
중간/양끝 키 다수 삭제(언더플로/병합/차용 경계 포함)
남은 키 전체 조회해서 정합성 검증
내부 노드에서의 키 최소값 업데이트 경로가 다양한 케이스에서 잘 작동하는지 스트레스 테스트 권장.
*/

#endif

#ifdef TESTCASE_DECIMAL_OPS
// ./testcase.sh TESTCASE_DECIMAL_OPS
int main(int argc, char **argv) {
    printf("Running TESTCASE_DECIMAL_OPS...\n");

    int rc = 0;
    char buf[128];
    struct flintdb_decimal  a = {0}, b = {0}, r = {0};

    // 1) 10.00 / 4, scale=2 => 2.50
    rc = flintdb_decimal_from_string("10.00", 2, &a); assert(rc == 0);
    rc = flintdb_decimal_from_string("4", 0, &b); assert(rc == 0);
    rc = flintdb_decimal_divide(&a, &b, 2, &r); assert(rc == 0);
    flintdb_decimal_to_string(&r, buf, sizeof buf);
    printf("10.00 / 4 @S=2 => %s (expect 2.50)\n", buf);
    assert(strcmp(buf, "2.50") == 0);

    // 2) 1.00 / 3, scale=4 => 0.3333 (truncate)
    rc = flintdb_decimal_from_string("1.00", 2, &a); assert(rc == 0);
    rc = flintdb_decimal_from_string("3", 0, &b); assert(rc == 0);
    rc = flintdb_decimal_divide(&a, &b, 4, &r); assert(rc == 0);
    flintdb_decimal_to_string(&r, buf, sizeof buf);
    printf("1.00 / 3 @S=4 => %s (expect 0.3333)\n", buf);
    assert(strcmp(buf, "0.3333") == 0);

    // 3) -12.34 / 2, scale=2 => -6.17
    rc = flintdb_decimal_from_string("-12.34", 2, &a); assert(rc == 0);
    rc = flintdb_decimal_from_string("2", 0, &b); assert(rc == 0);
    rc = flintdb_decimal_divide(&a, &b, 2, &r); assert(rc == 0);
    flintdb_decimal_to_string(&r, buf, sizeof buf);
    printf("-12.34 / 2 @S=2 => %s (expect -6.17)\n", buf);
    assert(strcmp(buf, "-6.17") == 0);

    // 4) 123.45 / 0.6 (scale1), S=3 => 205.750
    rc = flintdb_decimal_from_string("123.45", 2, &a); assert(rc == 0);
    rc = flintdb_decimal_from_string("0.6", 1, &b); assert(rc == 0);
    rc = flintdb_decimal_divide(&a, &b, 3, &r); assert(rc == 0);
    flintdb_decimal_to_string(&r, buf, sizeof buf);
    printf("123.45 / 0.6 @S=3 => %s (expect 205.750)\n", buf);
    assert(strcmp(buf, "205.750") == 0);

    // 5) 123.45 / 0.006 (scale3), S=2 => 20575.00
    rc = flintdb_decimal_from_string("123.45", 2, &a); assert(rc == 0);
    rc = flintdb_decimal_from_string("0.006", 3, &b); assert(rc == 0);
    rc = flintdb_decimal_divide(&a, &b, 2, &r); assert(rc == 0);
    flintdb_decimal_to_string(&r, buf, sizeof buf);
    printf("123.45 / 0.006 @S=2 => %s (expect 20575.00)\n", buf);
    assert(strcmp(buf, "20575.00") == 0);

    // 6) 0 / 7, S=3 => 0.000
    rc = flintdb_decimal_from_string("0", 0, &a); assert(rc == 0);
    rc = flintdb_decimal_from_string("7", 0, &b); assert(rc == 0);
    rc = flintdb_decimal_divide(&a, &b, 3, &r); assert(rc == 0);
    flintdb_decimal_to_string(&r, buf, sizeof buf);
    printf("0 / 7 @S=3 => %s (expect 0.000)\n", buf);
    assert(strcmp(buf, "0.000") == 0);

    // 7) divide-by-zero => error
    rc = flintdb_decimal_from_string("1", 0, &a); assert(rc == 0);
    rc = flintdb_decimal_from_string("0", 0, &b); assert(rc == 0);
    rc = flintdb_decimal_divide(&a, &b, 2, &r);
    printf("1 / 0 @S=2 => rc=%d (expect <0)\n", rc);
    assert(rc < 0);

    // 8) divide_by_int preserves numerator scale: 100.00 / 4 => 25.00
    rc = flintdb_decimal_from_string("100.00", 2, &a); assert(rc == 0);
    rc = flintdb_decimal_divide_by_int(&a, 4, &r); assert(rc == 0);
    flintdb_decimal_to_string(&r, buf, sizeof buf);
    printf("100.00 / 4 (by_int) => %s (expect 25.00)\n", buf);
    assert(strcmp(buf, "25.00") == 0);

    // 9) divide_by_int with negative: -5.50 / 2 => -2.75
    rc = flintdb_decimal_from_string("-5.50", 2, &a); assert(rc == 0);
    rc = flintdb_decimal_divide_by_int(&a, 2, &r); assert(rc == 0);
    flintdb_decimal_to_string(&r, buf, sizeof buf);
    printf("-5.50 / 2 (by_int) => %s (expect -2.75)\n", buf);
    assert(strcmp(buf, "-2.75") == 0);

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif // TESTCASE_DECIMAL_OPS

#ifdef TESTCASE_VARIANT
// ./testcase.sh TESTCASE_VARIANT
int main(int argc, char **argv) {
    printf("Running TESTCASE_VARIANT...\n");

    char *e = NULL;
    struct flintdb_variant v = {0};
    flintdb_variant_init(&v);

    // Numeric types
    flintdb_variant_i8_set(&v, -7);
    e = NULL;
    assert(flintdb_variant_i8_get(&v, &e) == -7 && e == NULL);
    e = NULL;
    assert(flintdb_variant_u8_get(&v, &e) == 0 && e != NULL); // mismatch

    flintdb_variant_u8_set(&v, 250);
    e = NULL;
    assert(flintdb_variant_u8_get(&v, &e) == 250 && e == NULL);

    flintdb_variant_i16_set(&v, -1234);
    e = NULL;
    assert(flintdb_variant_i16_get(&v, &e) == -1234 && e == NULL);
    e = NULL;
    assert(flintdb_variant_u16_get(&v, &e) == 0 && e != NULL);

    flintdb_variant_u16_set(&v, 65000);
    e = NULL;
    assert(flintdb_variant_u16_get(&v, &e) == 65000 && e == NULL);

    flintdb_variant_i32_set(&v, 12345);
    e = NULL;
    assert(variant_i32_get(&v, &e) == 12345 && e == NULL);
    e = NULL;
    assert(flintdb_variant_f64_get(&v, &e) == 0.0 && e != NULL);

    flintdb_variant_u32_set(&v, 4000000000U);
    e = NULL;
    assert(flintdb_variant_u32_get(&v, &e) == 4000000000U && e == NULL);

    flintdb_variant_i64_set(&v, -900000000000LL);
    e = NULL;
    assert(flintdb_variant_i64_get(&v, &e) == -900000000000LL && e == NULL);

    flintdb_variant_f64_set(&v, 3.14159);
    e = NULL;
    assert(flintdb_variant_f64_get(&v, &e) == 3.14159 && e == NULL);

    // String non-owned (implementation now always owns & copies)
    char buf_hello[] = "Hello";
    flintdb_variant_string_set(&v, buf_hello, 5);
    const char *s = flintdb_variant_string_get(&v);
    assert(s != NULL);
    assert(memcmp(s, buf_hello, 5) == 0);
    // ensure terminator present
    assert(s[5] == '\0');

    // String owned
    char buf_owned[] = "Hello, Variant!";
    flintdb_variant_string_set(&v, buf_owned, (u32)strlen(buf_owned));
    s = flintdb_variant_string_get(&v);
    assert(s != NULL);
    assert(memcmp(s, buf_owned, strlen(buf_owned)) == 0);
    assert(s[strlen(buf_owned)] == '\0');

    // Bytes (owned)
    unsigned char bdata[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
    flintdb_variant_bytes_set(&v, (const char *)bdata, sizeof(bdata));
    u32 blen = 0;
    e = NULL;
    const char *bp = flintdb_variant_bytes_get(&v, &blen, &e);
    assert(e == NULL && bp != NULL && blen == sizeof(bdata));
    assert(memcmp(bp, bdata, blen) == 0);
    assert(bp[blen] == '\0');

    // UUID (16 bytes)
    unsigned char uuid[16];
    for (int i = 0; i < 16; i++)
        uuid[i] = (unsigned char)i;
    flintdb_variant_uuid_set(&v, (const char *)uuid, 16);
    u32 ulen = 0;
    e = NULL;
    const char *up = flintdb_variant_uuid_get(&v, &ulen, &e);
    assert(e == NULL && up != NULL && ulen == 16);
    assert(memcmp(up, uuid, 16) == 0);
    assert(up[ulen] == '\0');

    // IPv6 (16 bytes)
    unsigned char ip[16];
    for (int i = 0; i < 16; i++)
        ip[i] = (unsigned char)(255 - i);
    flintdb_variant_ipv6_set(&v, (const char *)ip, 16);
    u32 iplen = 0;
    e = NULL;
    const char *ipp = flintdb_variant_ipv6_get(&v, &iplen, &e);
    assert(e == NULL && ipp != NULL && iplen == 16);
    assert(memcmp(ipp, ip, 16) == 0);
    assert(ipp[iplen] == '\0');

    // Decimal
    struct flintdb_decimal  d = {0};
    d.sign = 1;
    d.scale = 2;
    d.length = 4;
    d.data[0] = 0x12;
    d.data[1] = 0x34;
    d.data[2] = 0x56;
    d.data[3] = 0x78;
    flintdb_variant_decimal_set(&v, d.sign, d.scale, d);
    e = NULL;
    struct flintdb_decimal  got = flintdb_variant_decimal_get(&v, &e);
    assert(e == NULL);
    assert(got.sign == 1 && got.scale == 2 && got.length == 4);
    assert(got.data[0] == 0x12 && got.data[1] == 0x34 && got.data[2] == 0x56 && got.data[3] == 0x78);

    // Date / Time
    time_t now = time(NULL);
    flintdb_variant_date_set(&v, now);
    e = NULL;
    assert(flintdb_variant_date_get(&v, &e) == now && e == NULL);

    flintdb_variant_time_set(&v, now + 1);
    e = NULL;
    assert(flintdb_variant_time_get(&v, &e) == now + 1 && e == NULL);

    // Nil / Zero
    flintdb_variant_null_set(&v);
    assert(flintdb_variant_is_null(&v) == 1);

    flintdb_variant_zero_set(&v);
    assert(flintdb_variant_is_null(&v) == 0);

    // Copy semantics: deep copy for buffer-like
    struct flintdb_variant v1 = {0};
    struct flintdb_variant v2 = {0};
    flintdb_variant_init(&v1);
    flintdb_variant_init(&v2);

    char extbuf[] = "COPY-TEST";
    flintdb_variant_string_set(&v1, extbuf, (u32)strlen(extbuf));
    int ok = flintdb_variant_copy(&v2, &v1);
    assert(ok == 0);
    // modify source buffer; destination should remain unchanged (deep copy)
    extbuf[0] = 'X';
    const char *v2s = flintdb_variant_string_get(&v2);
    assert(v2s != NULL);
    assert(memcmp(v2s, "COPY-TEST", strlen("COPY-TEST")) == 0);
    flintdb_variant_free(&v1);
    flintdb_variant_free(&v2);

    // Numeric vs String comparison
    struct flintdb_variant vn = {0};
    struct flintdb_variant vs = {0};
    flintdb_variant_init(&vn);
    flintdb_variant_init(&vs);
    flintdb_variant_i32_set(&vn, 123);
    flintdb_variant_string_set(&vs, "123", 3);
    int cmp_ns = flintdb_variant_compare(&vn, &vs);
    // By current compare rule: non-equal types fallback to type id order -> INT32(2) < STRING(11)
    assert(cmp_ns < 0);
    // Reverse should be > 0
    int cmp_sn = flintdb_variant_compare(&vs, &vn);
    assert(cmp_sn > 0);
    flintdb_variant_free(&vn);
    flintdb_variant_free(&vs);

    // Final quick sanity print
    printf("TESTCASE_VARIANT: OK\n");
    flintdb_variant_free(&v);

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif

#ifdef TESTCASE_VARIANT_DECIMAL_OPS
// ./testcase.sh TESTCASE_VARIANT_DECIMAL_OPS

extern int variant_decimal_add(struct flintdb_variant *target, const struct flintdb_variant *other);
extern int variant_flintdb_decimal_plus(struct flintdb_variant *result, const struct flintdb_variant *a, const struct flintdb_variant *b);

static void dec_to_cstr(const struct flintdb_variant *v, char *buf, size_t n) {
    if (!v || v->type != DECIMAL) { snprintf(buf, n, "<non-decimal>"); return; }
    flintdb_decimal_to_string(&v->value.d, buf, n);
}

int main(int argc, char **argv) {
    printf("Running TESTCASE_VARIANT_DECIMAL_OPS...\n");
    char *e = NULL;
    int rc = 0;

    // 1) In-place add: DECIMAL + INT64
    struct flintdb_variant v; flintdb_variant_init(&v);
    struct flintdb_decimal  d = {0};
    rc = flintdb_decimal_from_string("123.45", 2, &d); 
    assert(rc == 0);
    flintdb_variant_decimal_set(&v, d.sign, d.scale, d);
    struct flintdb_variant rhs; flintdb_variant_init(&rhs);
    flintdb_variant_i64_set(&rhs, 10);
    rc = variant_decimal_add(&v, &rhs); 
    assert(rc == 0);
    char buf[64]; dec_to_cstr(&v, buf, sizeof buf);
    printf("DECIMAL+INT64 => %s (expected 133.45)\n", buf);
    assert(strcmp(buf, "133.45") == 0);

    // 2) In-place add: DECIMAL + DOUBLE (scale preserved)
    flintdb_variant_f64_set(&rhs, 0.55);
    rc = variant_decimal_add(&v, &rhs); 
    assert(rc == 0);
    dec_to_cstr(&v, buf, sizeof buf);
    printf("+DOUBLE(0.55) => %s (expected 134.00)\n", buf);
    assert(strcmp(buf, "134.00") == 0);

    // 3) In-place add: DECIMAL + STRING negative
    flintdb_variant_string_set(&rhs, "-34.01", (u32)strlen("-34.01"));
    rc = variant_decimal_add(&v, &rhs); 
    assert(rc == 0);
    dec_to_cstr(&v, buf, sizeof buf);
    printf("+STRING(-34.01) => %s (expected 99.99)\n", buf);
    assert(strcmp(buf, "99.99") == 0);
    flintdb_variant_free(&rhs);

    // 4) Standalone plus: different scales (1.2 + 0.34 = 1.54)
    struct flintdb_variant a; flintdb_variant_init(&a);
    struct flintdb_variant b; flintdb_variant_init(&b);
    struct flintdb_variant r; flintdb_variant_init(&r);
    struct flintdb_decimal  da = {0}, db = {0};
    flintdb_decimal_from_string("1.2", 1, &da);
    flintdb_decimal_from_string("0.34", 2, &db);
    flintdb_variant_decimal_set(&a, da.sign, da.scale, da);
    flintdb_variant_decimal_set(&b, db.sign, db.scale, db);
    rc = variant_flintdb_decimal_plus(&r, &a, &b); assert(rc == 0);
    dec_to_cstr(&r, buf, sizeof buf);
    printf("plus(1.2,0.34) => %s (expected 1.54)\n", buf);
    assert(strcmp(buf, "1.54") == 0);

    // 5) Zero result with scale kept
    flintdb_decimal_from_string("100.00", 2, &da);
    flintdb_decimal_from_string("-100.00", 2, &db);
    flintdb_variant_decimal_set(&a, da.sign, da.scale, da);
    flintdb_variant_decimal_set(&b, db.sign, db.scale, db);
    rc = variant_flintdb_decimal_plus(&r, &a, &b); assert(rc == 0);
    dec_to_cstr(&r, buf, sizeof buf);
    printf("plus(100.00,-100.00) => %s (expected 0.00)\n", buf);
    assert(strcmp(buf, "0.00") == 0);

    flintdb_variant_free(&a);
    flintdb_variant_free(&b);
    flintdb_variant_free(&r);
    flintdb_variant_free(&v);

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif

#ifdef TESTCASE_SQL_PARSE
// ./testcase.sh TESTCASE_SQL_PARSE
// Test parsing SQL-like column definitions into meta structure

char * sql_unwrap(char *s) {
    // // Remove surrounding quotes if present
    // size_t len = strlen(s);
    // if (len >= 2) {
    //     if ((s[0] == '\'' && s[len - 1] == '\'') ||
    //         (s[0] == '\"' && s[len - 1] == '\"') ||
    //         (s[0] == '`' && s[len - 1] == '`')
    //     ) {
    //         // Shift left and null-terminate
    //         memmove(s, s + 1, len - 2);
    //         s[len - 2] = '\0';
    //     }
    // }
    return s;
}

int main(int argc, char **argv) {
    printf("Running TESTCASE_SQL_PARSE...\n");

    char *e = NULL;
    int rc = 0;
    char buf[4096];

    // 1) SELECT parsing
    struct flintdb_sql *q = NULL;
    const char *sql1 = // "SELECT DISTINCT a, b, c FROM t WHERE a=1 ORDER BY b DESC LIMIT 10";
        "-- This is a single line comment\n"
        "SELECT DISTINCT \n"
        "C1, -- Column 1\n"
        "C2, /* Column 2 */ \n"
        "C3, \n"
        "`C X 1` \n"
        "/* Multi-line \n"
        "comment */\n"
        "FROM 'table name' USE INDEX(IX_SECOND DESC) WHERE a=1 ORDER BY b DESC LIMIT 10";
    q = flintdb_sql_parse(sql1, &e);

    printf("table=[%s]\n", sql_unwrap(q->table));
    printf("CX1=[%s]\n", sql_unwrap(q->columns.name[3]));

    assert(e == NULL);
    assert(q);
    assert(q->distinct == 1);
    assert(strcmp(q->statement, "SELECT") == 0);
    assert(strcmp(q->table, "table name") == 0);
    assert(q->columns.length == 4);
    assert(strcmp(q->columns.name[0], "C1") == 0);
    assert(strcmp(q->columns.name[1], "C2") == 0);
    assert(strcmp(q->columns.name[2], "C3") == 0);
    assert(strcmp(q->where, "a=1") == 0);
    assert(strcmp(q->orderby, "b DESC") == 0);
    assert(strcmp(q->limit, "10") == 0);
    assert(strcmp(q->index, "IX_SECOND DESC") == 0);
    int n = flintdb_sql_to_string(q, buf, sizeof(buf), &e);
    printf("SQL Context to String:\n%s\n", buf);
    assert(n > 0);
    assert(e == NULL);
    flintdb_sql_free(q);
    // printf("select: %s\n", buf);

    // 2) INSERT parsing

    const char *sql2 = "INSERT INTO foo (a,b) VALUES ('x', NULL) WHERE a>0 LIMIT 5";
    q = flintdb_sql_parse(sql2, &e);
    assert(q);
    assert(e == NULL);
    assert(strcmp(q->statement, "INSERT") == 0);
    assert(strcmp(q->table, "foo") == 0);
    assert(q->columns.length == 2);
    assert(strcmp(q->columns.name[0], "a") == 0);
    assert(strcmp(q->columns.name[1], "b") == 0);
    assert(q->values.length == 2);
    assert(strcmp(q->values.value[0], "x") == 0);
    // NULL is represented as empty string in our parse_values_into
    assert(q->values.value[1][0] == '\0');
    assert(strcmp(q->where, "a>0") == 0);
    assert(strcmp(q->limit, "5") == 0);
    flintdb_sql_free(q);

    // 3) UPDATE parsing
    const char *sql3 = "UPDATE bar SET a = 1, b = 'y' WHERE id=10";
    q = flintdb_sql_parse(sql3, &e);
    assert(q);
    assert(e == NULL);
    assert(strcmp(q->statement, "UPDATE") == 0);
    assert(strcmp(q->table, "bar") == 0);
    // columns parsed from SET clause
    assert(q->columns.length >= 2);
    assert(strcmp(q->columns.name[0], "a") == 0);
    assert(strcmp(q->columns.name[1], "b") == 0);
    assert(strcmp(q->where, "id=10") == 0);
    flintdb_sql_free(q);
    // 4) DELETE parsing
    const char *sql4 = "DELETE FROM foo WHERE a=1 LIMIT 3";
    q = flintdb_sql_parse(sql4, &e);
    assert(q);
    assert(e == NULL);
    assert(strcmp(q->statement, "DELETE") == 0);
    assert(strcmp(q->table, "foo") == 0);
    assert(strcmp(q->where, "a=1") == 0);
    assert(strcmp(q->limit, "3") == 0);
    flintdb_sql_free(q);

    // 5) CREATE TABLE + meta
    const char *sql5 =
        "CREATE TABLE customers ("
        " id INT64 NOT NULL,"
        " name STRING(128) NOT NULL DEFAULT 'n/a' COMMENT 'cmt',"
        " PRIMARY KEY (id),"
        " KEY ix_name (name)"
        ") STORAGE=file INCREMENT=1M CACHE=2M COMPRESSOR=zstd HEADER=ABSENT DELIMITER=, QUOTE=\" NULL=\\N FORMAT=csv DATE=YYYY-MM-DD WAL=TRUNCATE";
    q = flintdb_sql_parse(sql5, &e);
    assert(q);
    assert(e == NULL);
    assert(strcmp(q->statement, "CREATE") == 0);
    assert(strcmp(q->table, "customers") == 0);
    assert(q->definition.length >= 3); // at least 2 columns + primary key

    struct flintdb_meta m = {0};
    rc = flintdb_sql_to_meta(q, &m, &e);
    assert(rc == 0);
    assert(e == NULL);
    // debug
    // printf("q->increment='%s' -> m.increment=%d\n", q->increment, m.increment);
    // table name
    assert(strcmp(m.name, "customers") == 0);
    // columns
    assert(m.columns.length >= 2);
    assert(strcmp(m.columns.a[0].name, "id") == 0);
    assert(m.columns.a[0].nullspec == SPEC_NOT_NULL);
    assert(m.columns.a[0].type == VARIANT_INT64);
    assert(strcmp(m.columns.a[1].name, "name") == 0);
    assert(m.columns.a[1].type == VARIANT_STRING);
    assert(m.columns.a[1].bytes == 128);
    assert(m.columns.a[1].nullspec == SPEC_NOT_NULL);
    assert(strcmp(m.columns.a[1].value, "n/a") == 0);
    assert(strcmp(m.columns.a[1].comment, "cmt") == 0);
    // indexes (C meta stores only names in current implementation)
    assert(m.indexes.length >= 1);
    // options
    assert(strcmp(m.storage, "file") == 0);
    // increment parsed to bytes
    // printf("q->increment='%s' -> m.increment=%d\n", q->increment, m.increment);
    // assert(m.increment == 1024 * 1024);
    // cache parsed to bytes
    assert(m.cache == 2 * 1024 * 1024);
    assert(strcmp(m.compressor, "zstd") == 0);
    assert(m.absent_header == 1);
    printf("q->delimiter='%s', m.delimiter='%c' (int=%d)\n", q->delimiter, m.delimiter, (int)m.delimiter);
    assert(m.delimiter == ',');
    printf("q->quote='%s', m.quote='%c' (int=%d)\n", q->quote, m.quote, (int)m.quote);
    assert(m.quote == '"');
    assert(strcmp(m.nil_str, "\\N") == 0);
    assert(strcmp(m.format, "csv") == 0);
    // date remains as provided; meta_new sets default to current date, but sql_to_meta overwrote when provided
    assert(strcmp(m.date, "YYYY-MM-DD") == 0);
    assert(strcmp(m.wal, WAL_OPT_TRUNCATE) == 0);

    flintdb_meta_to_sql_string(&m, buf, sizeof(buf), &e);
    printf("Meta to SQL String:\n%s\n", buf);
    assert(e == NULL);

    // finalize
    flintdb_meta_close(&m);
    flintdb_sql_free(q);

    printf("TESTCASE_SQL_PARSE: OK\n");

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif

#ifdef TESTCASE_VARIANT_STRING_REF
// ./testcase.sh TESTCASE_VARIANT_STRING_REF --mtrace
int main(int argc, char **argv) {
    char *e = NULL;

    struct flintdb_variant v1;
    flintdb_variant_init(&v1);
    struct flintdb_variant v2;
    flintdb_variant_init(&v2);

    const char *s = "Hello, String Ref!";
    flintdb_variant_string_set(&v1, s, (u32)strlen(s));
    flintdb_variant_string_ref_set(&v1, s, (u32)strlen(s), 0);
    flintdb_variant_string_ref_set(&v2, v1.value.b.data, v1.value.b.length, v1.value.b.sflag);

    printf("v1 string ref: '%s' (len=%u, sflag=%d)\n", flintdb_variant_string_get(&v1), v1.value.b.length, v1.value.b.sflag);
    printf("v2 string ref: '%s' (len=%u, sflag=%d)\n", flintdb_variant_string_get(&v2), v2.value.b.length, v2.value.b.sflag);
    flintdb_variant_free(&v1);
    flintdb_variant_free(&v2);

    printf("TESTCASE_VARIANT_STRING_REF: OK\n");
    PRINT_MEMORY_LEAK_INFO();

    return 0;
}
#endif // TESTCASE_VARIANT_STRING_REF

#ifdef TESTCASE_SIZEOF_STRUCT
// ./testcase.sh TESTCASE_SIZEOF_STRUCT
int main(int argc, char **argv) {
    printf("sizeof(struct flintdb_meta) = %zu\n", sizeof(struct flintdb_meta));
    printf("sizeof(struct flintdb_column) = %zu\n", sizeof(struct flintdb_column));
    printf("sizeof(struct flintdb_index) = %zu\n", sizeof(struct flintdb_index));
    printf("sizeof(struct flintdb_row) = %zu\n", sizeof(struct flintdb_row));
    printf("sizeof(struct flintdb_variant) = %zu\n", sizeof(struct flintdb_variant));
    printf("sizeof(struct flintdb_decimal ) = %zu\n", sizeof(struct flintdb_decimal ));
    printf("sizeof(struct flintdb_sql) = %zu\n", sizeof(struct flintdb_sql));
    printf("sizeof(enum flintdb_variant_sflag) = %zu\n", sizeof(enum flintdb_variant_sflag));

    return 0;
}
#endif // TESTCASE_SIZEOF_STRUCT

#ifdef TESTCASE_COLUMN_AT
// ./testcase.sh TESTCASE_COLUMN_AT

int main(int argc, char **argv) {
    char *e = NULL;
    struct flintdb_meta mt = flintdb_meta_new("customer"TABLE_NAME_SUFFIX, &e);
    flintdb_meta_columns_add(&mt, "customer_id", VARIANT_INT64, 0, 0, SPEC_NULLABLE, "0", "int64 primary key", &e);
    flintdb_meta_columns_add(&mt, "customer_name", VARIANT_STRING, 255, 0, SPEC_NULLABLE, "0", "", &e);
    if (e || *e)
        THROW_S(e);

    for (int i = 0; i < mt.columns.length; i++) {
        DEBUG("Column at index %d: %s", i, mt.columns.a[i].name);
    }

    DEBUG("Column 'customer_id' at index: %d", flintdb_column_at(&mt, "customer_id"));
    DEBUG("Column 'CUSTOMER_ID' at index: %d", flintdb_column_at(&mt, "CUSTOMER_ID"));
    DEBUG("Column 'customer_name' at index: %d", flintdb_column_at(&mt, "customer_name"));
    DEBUG("Column 'CUSTOMER_NAME' at index: %d", flintdb_column_at(&mt, "CUSTOMER_NAME"));
    DEBUG("mt.priv: %p", mt.priv);

    flintdb_meta_close(&mt);
    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    return 1;
}

#endif // TESTCASE_COLUMN_AT

#ifdef TESTCASE_SQL_META
// ./testcase.sh TESTCASE_SQL_META

int main(int argc, char **argv) {
    char *e = NULL;
    const char *tablename = "temp/customer"TABLE_NAME_SUFFIX;

    struct flintdb_meta mt = flintdb_meta_new("customer"TABLE_NAME_SUFFIX, &e);
    flintdb_meta_columns_add(&mt, "customer_id", VARIANT_INT64, 0, 0, SPEC_NULLABLE, "0", "int64 primary key", &e);
    flintdb_meta_columns_add(&mt, "customer_name", VARIANT_STRING, 255, 0, SPEC_NULLABLE, "0", "", &e);

    char keys_arr[1][MAX_COLUMN_NAME_LIMIT] = {"customer_id"};
    int kcnt = sizeof(keys_arr) / sizeof(keys_arr[0]);
    // TRACE("kcnt: %d", kcnt);

    flintdb_meta_indexes_add(&mt, PRIMARY_NAME, NULL, (const char (*)[MAX_COLUMN_NAME_LIMIT])keys_arr, kcnt, &e);
    if (e)
        THROW_S(e);

    char sql[SQL_STRING_LIMIT] = {0};
    if (flintdb_meta_to_sql_string(&mt, sql, sizeof(sql), &e) < 0)
        THROW(&e, "meta_to_sql_string failed");
    TRACE("SQL: %s \n", sql);

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}

#endif

#ifdef TESTCASE_FORMATTER
// ./testcase.sh TESTCASE_FORMATTER
static void build_meta(struct flintdb_meta *m, const char *name, enum fileformat ff, char delim, char quote, const char *nilstr) {
    char *e = NULL;
    memset(m, 0, sizeof(*m));
    struct flintdb_meta tmp = flintdb_meta_new(name, &e);
    memcpy(m, &tmp, sizeof(tmp));
    // 4 columns: id(INT64), name(STRING(16)), amount(DOUBLE), ts(DATE)
    flintdb_meta_columns_add(m, "id", VARIANT_INT64, 8, 0, SPEC_NULLABLE, "", "", NULL);
    flintdb_meta_columns_add(m, "name", VARIANT_STRING, 16, 0, SPEC_NULLABLE, "", "", NULL);
    flintdb_meta_columns_add(m, "amount", VARIANT_DOUBLE, 8, 0, SPEC_NULLABLE, "", "", NULL);
    flintdb_meta_columns_add(m, "ts", VARIANT_DATE, 0, 0, SPEC_NULLABLE, "", "", NULL);
    m->absent_header = 1;
    m->delimiter = delim;
    m->quote = quote;
    m->nil_str[0] = '\0';
    if (nilstr)
        strncpy(m->nil_str, nilstr, sizeof(m->nil_str) - 1);
    // format string purely informational here
    if (ff == FORMAT_TSV)
        strcpy(m->format, "tsv");
    else if (ff == FORMAT_CSV)
        strcpy(m->format, "csv");
    else
        strcpy(m->format, "bin");
}

static struct flintdb_row *make_row(struct flintdb_meta *m) {
    char *e = NULL;
    struct flintdb_row *r = flintdb_row_new(m, &e);
    r->i64_set(r, 0, 42, &e);
    r->string_set(r, 1, "Alice", &e);
    r->f64_set(r, 2, 12.5, &e);
    time_t now = 1700000000; // fixed
    // normalize to midnight (local time) to be stable across encode/decode
    struct tm tmv;
    localtime_r(&now, &tmv);
    tmv.tm_hour = 0;
    tmv.tm_min = 0;
    tmv.tm_sec = 0;
    tmv.tm_isdst = -1;
    time_t midnight = mktime(&tmv);
    r->date_set(r, 3, midnight, &e);
    return r;
}

static void assert_row_eq(struct flintdb_row *a, struct flintdb_row *b) {
    if (!a || !b) {
        fprintf(stderr, "nil row\n");
        exit(1);
    }
    assert(a->length == b->length);
    for (int i = 0; i < a->length; i++) {
        enum flintdb_variant_type  t = (a->meta && a->meta->columns.length > i) ? a->meta->columns.a[i].type : a->array[i].type;
        if (t == VARIANT_DATE) {
            time_t ta = a->date_get(a, i, NULL);
            time_t tb = b->date_get(b, i, NULL);
            long da = (long)(ta / 86400);
            long db = (long)(tb / 86400);
            if (da != db) {
                fprintf(stderr, "row diff at %d (DATE days %ld vs %ld)\n", i, da, db);
                exit(1);
            }
            continue;
        }
        int c = flintdb_variant_compare(&a->array[i], &b->array[i]);
        if (c != 0) {
            fprintf(stderr, "row diff at %d\n", i);
            exit(1);
        }
    }
}

int main(int argc, char **argv) {
    printf("Running TESTCASE_FORMATTER...\n");
    char *e = NULL;
    // TSV roundtrip
    struct flintdb_meta mt;
    build_meta(&mt, "t.tsv", FORMAT_TSV, '\t', '\0', "\\N");
    struct formatter ftsv;
    formatter_init(FORMAT_TSV, &mt, &ftsv, &e);
    struct flintdb_row *r1 = make_row(&mt);
    struct buffer *bout = buffer_alloc(128);
    // assert(formatter_encode(&ftsv, r1, bout, &e) == 0);
    assert(ftsv.encode(&ftsv, r1, bout, &e) == 0);
    // prepare input buffer from output
    struct buffer in1;
    buffer_wrap(bout->array, bout->limit, &in1);
    struct flintdb_row *r2 = flintdb_row_new(&mt, &e);
    // assert(formatter_decode(&ftsv, &in1, r2, &e) == 0);
    assert(ftsv.decode(&ftsv, &in1, r2, &e) == 0);
    // 노말라이즈 비교: 문자열/숫자 캐스팅 동일성 위해 meta 타입 기준으로 비교 수행
    assert_row_eq(r1, r2);
    r1->free(r1);
    r2->free(r2);
    bout->free(bout);
    ftsv.close(&ftsv);
    flintdb_meta_close(&mt);

    // BIN roundtrip
    struct flintdb_meta mb;
    build_meta(&mb, "t.bin", FORMAT_BIN, '\t', '\0', "\\N");
    struct formatter fbin;
    formatter_init(FORMAT_BIN, &mb, &fbin, &e);
    struct flintdb_row *rb1 = make_row(&mb);
    struct buffer *bout2 = buffer_alloc(256);
    // assert(formatter_encode(&fbin, rb1, bout2, &e) == 0);
    assert(fbin.encode(&fbin, rb1, bout2, &e) == 0);
    // Validate on-wire layout matches Java BIN formatter (no padding for var-len, proper tags)
    {
        struct buffer chk;
        buffer_wrap(bout2->array, bout2->limit, &chk);
        i16 ncols = chk.i16_get(&chk, &e);
        assert(ncols == 4);
        // col0: INT64
        assert(chk.i16_get(&chk, &e) == VARIANT_INT64);
        chk.skip(&chk, 8);
        // col1: STRING "Alice" without padding; next should be DOUBLE tag immediately
        assert(chk.i16_get(&chk, &e) == VARIANT_STRING);
        i16 l1 = chk.i16_get(&chk, &e);
        assert(l1 == 5);
        char *s1 = chk.array_get(&chk, (u32)l1, &e);
        assert(memcmp(s1, "Alice", 5) == 0);
        // Next tag must be DOUBLE (no zero padding)
        assert(chk.i16_get(&chk, &e) == VARIANT_DOUBLE);
        chk.skip(&chk, 8);
        // col3: DATE packed Y/M/D (24 bits)
        assert(chk.i16_get(&chk, &e) == VARIANT_DATE);
        u32 b1 = (u8)chk.i8_get(&chk, &e);
        u32 b2 = (u8)chk.i8_get(&chk, &e);
        u32 b3 = (u8)chk.i8_get(&chk, &e);
        u32 d24 = (b1 << 16) | (b2 << 8) | b3;
        int year = (int)(d24 >> 9);
        int month = (int)((d24 >> 5) & 0x0F);
        int day = (int)(d24 & 0x1F);
        time_t tt = rb1->date_get(rb1, 3, &e);
        // row_fast_time_to_date uses UTC-based calculation, so compare with that
        i64 days = (i64)tt / 86400;
        i64 a = days + 719468;
        i64 era = (a >= 0 ? a : a - 146096) / 146097;
        i64 doe = a - era * 146097;
        i64 yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        i64 y = yoe + era * 400;
        i64 doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        i64 mp = (5 * doy + 2) / 153;
        i64 d = doy - (153 * mp + 2) / 5 + 1;
        i64 m = mp + (mp < 10 ? 3 : -9);
        if (m <= 2) y++;
        assert(year == (int)y);
        assert(month == (int)m);
        assert(day == (int)d);
    }
    // Also validate NIL handling for var-len: set name=NULL -> tag=0 and skip to next tag
    {
        struct flintdb_row *rn = make_row(&mb);
        // Force NIL directly into the underlying variant for STRING column (bypass row.set casting)
        flintdb_variant_null_set(&rn->array[1]);
        struct buffer *bb = buffer_alloc(128);
        assert(fbin.encode(&fbin, rn, bb, &e) == 0);
        struct buffer chk;
        buffer_wrap(bb->array, bb->limit, &chk);
        assert(chk.i16_get(&chk, &e) == 4);
        assert(chk.i16_get(&chk, &e) == VARIANT_INT64);
        chk.skip(&chk, 8);
        // name column should be NIL (0), and immediately followed by the DOUBLE tag
        assert(chk.i16_get(&chk, &e) == VARIANT_NULL);
        assert(chk.i16_get(&chk, &e) == VARIANT_DOUBLE);
        bb->free(bb);
        rn->free(rn);
    }
    struct buffer in2;
    buffer_wrap(bout2->array, bout2->limit, &in2);
    struct flintdb_row *rb2 = flintdb_row_new(&mb, &e);
    // assert(formatter_decode(&fbin, &in2, rb2, &e) == 0);
    assert(fbin.decode(&fbin, &in2, rb2, &e) == 0);
    assert_row_eq(rb1, rb2);
    rb1->free(rb1);
    rb2->free(rb2);
    bout2->free(bout2);
    fbin.close(&fbin);
    flintdb_meta_close(&mb);

    printf("TESTCASE_FORMATTER: OK\n");

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif

#ifdef TESTCASE_DECIMAL
// ./testcase.sh TESTCASE_DECIMAL
int main(int argc, char **argv) {
    printf("Running TESTCASE_DECIMAL...\n");
    char *e = NULL;
    struct flintdb_meta m = flintdb_meta_new("t", &e);
    flintdb_meta_columns_add(&m, "price", VARIANT_DECIMAL, 16, 2, SPEC_NULLABLE, "", "", NULL);
    struct flintdb_row *r = flintdb_row_new(&m, &e);
    // set string with scale > target; should truncate via cast (use row.set to honor meta type)
    struct flintdb_variant tmp;
    flintdb_variant_init(&tmp);
    flintdb_variant_string_set(&tmp, "123.4567", (u32)strlen("123.4567"));
    r->set(r, 0, &tmp, &e);
    flintdb_variant_free(&tmp);
    struct flintdb_decimal  d = r->decimal_get(r, 0, &e);
    assert(d.scale == 2);
    // Encode/Decode via BIN
    struct formatter f;
    formatter_init(FORMAT_BIN, &m, &f, &e);
    struct buffer *b = buffer_alloc(128);
    // assert(formatter_encode(&f, r, b, &e) == 0);
    assert(f.encode(&f, r, b, &e) == 0);
    struct buffer in;
    buffer_wrap(b->array, b->limit, &in);
    struct flintdb_row *r2 = flintdb_row_new(&m, &e);
    // assert(formatter_decode(&f, &in, r2, &e) == 0);
    assert(f.decode(&f, &in, r2, &e) == 0);
    // roundtrip compare
    assert(flintdb_variant_compare(&r->array[0], &r2->array[0]) == 0);
    r->free(r);
    r2->free(r2);
    b->free(b);
    flintdb_meta_close(&m);
    printf("UNIT_TEST_DECIMAL: OK\n");

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif

#ifdef TESTCASE_CSV_MULTILINE
// ./testcase.sh TESTCASE_CSV_MULTILINE
int main(int argc, char **argv) {
    printf("Running TESTCASE_CSV_MULTILINE...\n");
    char *e = NULL;
    struct flintdb_meta m = flintdb_meta_new("t", &e);
    m.delimiter = ',';
    m.quote = '"';
    strcpy(m.nil_str, "NULL");
    m.absent_header = 1;
    flintdb_meta_columns_add(&m, "id", VARIANT_INT64, 8, 0, SPEC_NULLABLE, "", "", NULL);
    flintdb_meta_columns_add(&m, "msg", VARIANT_STRING, 64, 0, SPEC_NULLABLE, "", "", NULL);
    struct formatter f;
    formatter_init(FORMAT_CSV, &m, &f, &e);
    // record with newline in quoted field
    const char *csv = "1,\"hello\nworld\"\n2,plain\n";
    struct buffer *buf = buffer_alloc((u32)strlen(csv));
    buf->array_put(buf, (char *)csv, (u32)strlen(csv), NULL);
    buf->flip(buf);
    struct flintdb_row *r1 = flintdb_row_new(&m, &e);
    // assert(formatter_decode(&f, buf, r1, &e) == 0);
    assert(f.decode(&f, buf, r1, &e) == 0);
    assert(r1->i64_get(r1, 0, &e) == 1);
    assert(strcmp(r1->string_get(r1, 1, &e), "hello\nworld") == 0);
    // next row
    struct flintdb_row *r2 = flintdb_row_new(&m, &e);
    assert(f.decode(&f, buf, r2, &e) == 0);
    assert(r2->i64_get(r2, 0, &e) == 2);
    assert(strcmp(r2->string_get(r2, 1, &e), "plain") == 0);
    r1->free(r1);
    r2->free(r2);
    buf->free(buf);
    f.close(&f);
    flintdb_meta_close(&m);
    printf("TESTCASE_CSV_MULTILINE: OK\n");

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif

#ifdef TESTCASE_TABLE_BULK_INSERT
// ./testcase.sh TESTCASE_TABLE_BULK_INSERT

int main(int argc, char **argv) {
    char *e = NULL;
    struct flintdb_table *tbl = NULL;

    const char *tablename = "temp/customer"TABLE_NAME_SUFFIX;

    struct flintdb_meta mt = flintdb_meta_new("customer"TABLE_NAME_SUFFIX, &e);
    flintdb_meta_columns_add(&mt, "customer_id", VARIANT_INT64, 0, 0, SPEC_NULLABLE, "0", "int64 primary key", &e);
    flintdb_meta_columns_add(&mt, "customer_name", VARIANT_STRING, 255, 0, SPEC_NULLABLE, "0", "", &e);

    char keys_arr[1][MAX_COLUMN_NAME_LIMIT] = {"customer_id"};
    int kcnt = sizeof(keys_arr) / sizeof(keys_arr[0]);
    // TRACE("kcnt: %d", kcnt);

    flintdb_meta_indexes_add(&mt, PRIMARY_NAME, NULL, (const char (*)[MAX_COLUMN_NAME_LIMIT])keys_arr, kcnt, &e);
    if (e)
        THROW_S(e);

    char sql[SQL_STRING_LIMIT] = {0};
    if (flintdb_meta_to_sql_string(&mt, sql, sizeof(sql), &e) < 0)
        THROW(&e, "meta_to_sql_string failed");
    TRACE("SQL: %s \n", sql);

    flintdb_table_drop(tablename, NULL); // ignore error
    if (e)
        THROW_S(e);
    // TRACE("table_drop done");

    tbl = flintdb_table_open(tablename, FLINTDB_RDWR, &mt, &e);
    TRACE("table_open done");
    if (e)
        THROW_S(e);
    if (!tbl)
        THROW(&e, "table_open failed");

    if (e)
        THROW_S(e);
    // insert
    for (int i = 0; i < 10000; i++) {
        TRACE("flintdb_row_new(%d)", i);
        struct flintdb_row *r = flintdb_row_new(&mt, &e);
        TRACE("after flintdb_row_new(%d), r=%p, e=%s", i, (void *)r, e ? e : "NULL");
        if (e)
            THROW_S(e);
        r->i64_set(r, 0, i + 1, &e);
        TRACE("after i64_set(%d), e=%s", i, e ? e : "NULL");
        if (e)
            THROW_S(e);
        char name[64];
        snprintf(name, sizeof(name), "Name-%d", i + 1);
        r->string_set(r, 1, name, &e);
        TRACE("after string_set(%d), e=%s", i, e ? e : "NULL");
        if (e)
            THROW_S(e);

        TRACE("apply: %d, %s", i + 1, name);

        i64 rowid = tbl->apply(tbl, r, 1, &e);
        TRACE("after tbl->apply(%d), rowid=%lld, e=%s", i, rowid, e ? e : "NULL");
        if (e)
            THROW_S(e);
        if (rowid < 0)
            THROW(&e, "table apply failed");

        TRACE("rowid: %lld", rowid);
        // Free row after apply; ownership is not transferred on insert path
        r->free(r);
    }

    TRACE("rows=%lld", tbl->rows(tbl, &e));
    if (e)
        THROW_S(e);

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    if (tbl)
        tbl->close(tbl);
    flintdb_meta_close(&mt);

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}

#endif

#ifdef TESTCASE_TABLE_FIND
// ./testcase.sh TESTCASE_TABLE_FIND
int main(int argc, char **argv) {
    char *e = NULL;
    const char *tablename = "temp/customer"TABLE_NAME_SUFFIX;
    struct flintdb_table *tbl = flintdb_table_open(tablename, FLINTDB_RDONLY, NULL, &e);
    if (e)
        THROW_S(e);
    if (!tbl)
        THROW(&e, "table_open failed");

    const struct flintdb_meta *mt = tbl->meta(tbl, &e);
    if (e)
        THROW_S(e);
    if (!mt)
        THROW(&e, "table meta is NULL");
    char sql[SQL_STRING_LIMIT] = {0};
    if (flintdb_meta_to_sql_string(mt, sql, sizeof(sql), &e) < 0)
        THROW(&e, "meta_to_sql_string failed");
    TRACE("TABLE META SQL: %s", sql);

    i64 rows = tbl->rows(tbl, &e);
    if (e)
        THROW_S(e);
    TRACE("TABLE ROWS: %lld", rows);

    struct flintdb_cursor_i64 *c = tbl->find(tbl, "USE INDEX(PRIMARY DESC) WHERE customer_id > 5000 AND customer_id < 5007 LIMIT 10", &e);
    if (e)
        THROW_S(e);
    if (c) {
        for (i64 i; c && (i = c->next(c, &e)) > -1;) {
            if (e) {
                WARN("cursor next error: %s", e);
                break;
            }
            const struct flintdb_row *r = tbl->read(tbl, i, &e);
            if (e) {
                WARN("cursor row error: %s", e);
                break;
            }
            if (r) {
                i64 cid = r->i64_get(r, 0, &e);
                const char *cname = r->string_get(r, 1, &e);
                if (e) {
                    WARN("row get error: %s", e);
                    break;
                }
                printf("ROW: customer_id=%lld, customer_name=%s\n", cid, cname);
            }
        }
        // Close the cursor to release any underlying resources (including bplustree cursor)
        c->close(c);
        c = NULL;
    }

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    if (tbl)
        tbl->close(tbl);

    PRINT_MEMORY_LEAK_INFO();

    return 0;
}
#endif

#ifdef TESTCASE_PARQUET_WRITE
// ./testcase.sh TESTCASE_PARQUET_WRITE

int main(int argc, char **argv) {
    char *e = NULL;
    struct flintdb_genericfile *f = NULL;
    const char *filepath = "temp/test_parquet_output.parquet";
    
    // Clean up any existing file
    unlink(filepath);
    char desc_path[PATH_MAX];
    snprintf(desc_path, sizeof(desc_path), "%s%s", filepath, META_NAME_SUFFIX);
    unlink(desc_path);

    printf("=== TESTCASE_PARQUET_WRITE ===\n");

    // Create meta for test data: id(INT64), name(STRING), price(DOUBLE), active(INT8)
    struct flintdb_meta m = flintdb_meta_new("test_parquet", &e);
    if (e) THROW_S(e);
    
    flintdb_meta_columns_add(&m, "id", VARIANT_INT64, 0, 0, SPEC_NULLABLE, "", "Row ID", &e);
    if (e) THROW_S(e);
    flintdb_meta_columns_add(&m, "name", VARIANT_STRING, 128, 0, SPEC_NULLABLE, "", "Name field", &e);
    if (e) THROW_S(e);
    flintdb_meta_columns_add(&m, "price", VARIANT_DOUBLE, 0, 0, SPEC_NULLABLE, "", "Price value", &e);
    if (e) THROW_S(e);
    flintdb_meta_columns_add(&m, "active", VARIANT_INT8, 0, 0, SPEC_NULLABLE, "1", "Active flag", &e);
    if (e) THROW_S(e);

    printf("Opening Parquet file for writing: %s\n", filepath);
    
    // Open Parquet file for writing (requires plugin)
    f = flintdb_genericfile_open(filepath, FLINTDB_RDWR, &m, &e);
    if (e) {
        printf("\n=== Parquet Plugin Status ===\n");
        printf("Error: %s\n\n", e);
        printf("Current Implementation:\n");
        printf("  ✓ Plugin loading and symbol resolution\n");
        printf("  ✓ Schema builder and file opening\n");
        printf("  ✓ Parquet file reading (via Arrow C Data Interface)\n");
        printf("  ✗ Parquet file writing (requires row batching)\n\n");
        printf("Parquet Write Requirements:\n");
        printf("  - Columnar format requires batching rows\n");
        printf("  - Need to accumulate 1000+ rows before writing\n");
        printf("  - Convert row data to Arrow columnar arrays\n\n");
        printf("Workaround: Use TSV format for row-by-row writes\n");
        printf("  Example: flintdb_genericfile_open(\"file.tsv\", FLINTDB_RDWR, &meta, &e)\n\n");
        flintdb_meta_close(&m);
        free(e);
        PRINT_MEMORY_LEAK_INFO();
        return 0;
    }
    
    if (!f) THROW(&e, "Failed to open Parquet file");
    
    printf("Writing test data rows...\n");
    
    // Write test data
    STOPWATCH_START(watch);
    int num_rows = 1000;
    for (int i = 0; i < num_rows; i++) {
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e) THROW_S(e);
        
        // Set values
        r->i64_set(r, 0, (i64)(i + 1), &e);
        if (e) THROW_S(e);
        
        char name[64];
        snprintf(name, sizeof(name), "Item-%04d", i + 1);
        r->string_set(r, 1, name, &e);
        if (e) THROW_S(e);
        
        r->f64_set(r, 2, 10.5 + (i % 100) * 0.25, &e);
        if (e) THROW_S(e);
        
        r->i8_set(r, 3, (i8)(i % 2), &e);
        if (e) THROW_S(e);
        
        // Write row
        if (f->write(f, r, &e) != 0) {
            r->free(r);
            THROW_S(e);
        }
        
        r->free(r);
        
        if ((i + 1) % 100 == 0) {
            printf("  Written %d rows...\n", i + 1);
        }
    }
    
    printf("Wrote %d rows in %lld ms (%.0f ops/sec)\n", 
           num_rows, time_elapsed(&watch), time_ops(num_rows, &watch));
    
    // Get file stats
    i64 bytes = f->bytes(f, &e);
    if (e) THROW_S(e);
    printf("File size: %lld bytes\n", bytes);
    
    // Close file
    f->close(f);
    f = NULL;
    
    flintdb_meta_close(&m);
    
    printf("TESTCASE_PARQUET_WRITE: OK\n");
    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e) WARN("Error: %s", e);
    if (f) f->close(f);
    flintdb_meta_close(&m);
    return 1;
}

#endif // TESTCASE_PARQUET_WRITE

#ifdef TESTCASE_PARQUET_READ
// ./testcase.sh TESTCASE_PARQUET_READ

int main(int argc, char **argv) {
    char *e = NULL;
    struct flintdb_genericfile *f = NULL;
    const char *filepath = "temp/test_read.parquet";

    printf("=== TESTCASE_PARQUET_READ ===\n");
    
    // Check if file exists
    if (access(filepath, F_OK) != 0) {
        printf("Test file does not exist: %s\n", filepath);
        printf("Please run TESTCASE_PARQUET_WRITE first to create test data.\n");
        return 0;
    }

    printf("Opening Parquet file for reading: %s\n", filepath);
    
    // Open Parquet file for reading (requires plugin, meta from .desc or schema)
    f = flintdb_genericfile_open(filepath, FLINTDB_RDONLY, NULL, &e);
    if (e) {
        printf("\n=== Parquet Support Status ===\n");
        printf("Error: %s\n\n", e);
        printf("Parquet reading requires Arrow C++ wrapper.\n");
        printf("See TESTCASE_PARQUET_WRITE for implementation details.\n\n");
        return 0;
    }
    
    if (!f) THROW(&e, "Failed to open Parquet file");
    
    // Get file metadata
    const struct flintdb_meta *m = f->meta(f, &e);
    if (e) THROW_S(e);
    if (!m) THROW(&e, "Failed to get file metadata");
    
    printf("Schema: %s\n", m->name);
    printf("Columns: %d\n", m->columns.length);
    for (int i = 0; i < m->columns.length; i++) {
        printf("  [%d] %s (%d)\n", i, m->columns.a[i].name, m->columns.a[i].type);
    }
    
    // Get row count
    i64 rows = f->rows(f, &e);
    if (e) THROW_S(e);
    printf("Total rows: %lld\n", rows);
    
    // Get file size
    i64 bytes = f->bytes(f, &e);
    if (e) THROW_S(e);
    printf("File size: %lld bytes\n", bytes);
    
    // Test 1: Read first 10 rows
    printf("\n--- Test 1: Read first 10 rows ---\n");
    struct flintdb_cursor_row *c1 = f->find(f, "LIMIT 10", &e);
    if (e) THROW_S(e);
    if (!c1) THROW(&e, "Failed to create cursor");
    
    int count1 = 0;
    STOPWATCH_START(watch1);
    for (struct flintdb_row *r; (r = c1->next(c1, &e)) != NULL; ) {
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        
        if (count1 < 3) {
            // Print first 3 rows
            printf("Row %d: id=%lld, name=%s, price=%.2f, active=%d\n",
                   count1 + 1,
                   r->i64_get(r, 0, &e),
                   r->string_get(r, 1, &e),
                   r->f64_get(r, 2, &e),
                   r->i8_get(r, 3, &e));
            if (e) {
                r->free(r);
                THROW_S(e);
            }
        }
        
        r->free(r);
        count1++;
    }
    printf("Read %d rows in %lld ms\n", count1, time_elapsed(&watch1));
    c1->close(c1);
    c1 = NULL;
    
    // Test 2: Read with WHERE filter
    printf("\n--- Test 2: Read with WHERE filter (id >= 10 AND id < 15) ---\n");
    struct flintdb_cursor_row *c2 = f->find(f, "WHERE id >= 10 AND id < 15", &e);
    if (e) THROW_S(e);
    if (!c2) THROW(&e, "Failed to create cursor");
    
    int count2 = 0;
    STOPWATCH_START(watch2);
    for (struct flintdb_row *r; (r = c2->next(c2, &e)) != NULL; ) {
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        
        i64 id = r->i64_get(r, 0, &e);
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        
        // Verify filter constraint
        assert(id >= 10 && id < 15);
        
        if (count2 < 3) {
            printf("Row: id=%lld, name=%s\n", id, r->string_get(r, 1, &e));
            if (e) {
                r->free(r);
                THROW_S(e);
            }
        }
        
        r->free(r);
        count2++;
    }
    printf("Read %d filtered rows in %lld ms\n", count2, time_elapsed(&watch2));
    c2->close(c2);
    c2 = NULL;
    
    // Test 3: Read with LIMIT and OFFSET
    printf("\n--- Test 3: Read with OFFSET 50 LIMIT 5 ---\n");
    struct flintdb_cursor_row *c3 = f->find(f, "LIMIT 5 OFFSET 50", &e);
    if (e) THROW_S(e);
    if (!c3) THROW(&e, "Failed to create cursor");
    
    int count3 = 0;
    for (struct flintdb_row *r; (r = c3->next(c3, &e)) != NULL; ) {
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        
        i64 id = r->i64_get(r, 0, &e);
        const char *name = r->string_get(r, 1, &e);
        f64 price = r->f64_get(r, 2, &e);
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        
        printf("Row: id=%lld, name=%s, price=%.2f\n", id, name, price);
        
        r->free(r);
        count3++;
    }
    assert(count3 <= 5);
    printf("Read %d rows with offset\n", count3);
    c3->close(c3);
    c3 = NULL;
    
    // Test 4: Full scan statistics
    printf("\n--- Test 4: Full scan statistics ---\n");
    struct flintdb_cursor_row *c4 = f->find(f, "", &e);
    if (e) THROW_S(e);
    if (!c4) THROW(&e, "Failed to create cursor");
    
    int count4 = 0;
    i64 sum_ids = 0;
    f64 sum_prices = 0.0;
    STOPWATCH_START(watch4);
    for (struct flintdb_row *r; (r = c4->next(c4, &e)) != NULL; ) {
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        
        sum_ids += r->i64_get(r, 0, &e);
        sum_prices += r->f64_get(r, 2, &e);
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        
        r->free(r);
        count4++;
    }
    printf("Scanned %d rows in %lld ms (%.0f ops/sec)\n", 
           count4, time_elapsed(&watch4), time_ops(count4, &watch4));
    printf("Sum of IDs: %lld, Average price: %.2f\n", 
           sum_ids, count4 > 0 ? sum_prices / count4 : 0.0);
    c4->close(c4);
    c4 = NULL;
    
    // Close file
    f->close(f);
    f = NULL;
    
    printf("\nTESTCASE_PARQUET_READ: OK\n");
    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e) WARN("Error: %s", e);
    if (c1) c1->close(c1);
    if (c2) c2->close(c2);
    if (c3) c3->close(c3);
    if (c4) c4->close(c4);
    if (f) f->close(f);
    return 1;
}

#endif // TESTCASE_PARQUET_READ

#ifdef TESTCASE_STREAM_GZIP_READ
// ./testcase.sh TESTCASE_STREAM_GZIP_READ
int main(int argc, char **argv) {
    char *e = NULL;
    const char *gzpath = "temp/tpch/lineitem.tbl.gz";

    // write gz
    struct bufio *bio = NULL;
    bio = file_bufio_open(gzpath, FLINTDB_RDONLY, 65536, &e);

    assert(e == NULL && bio != NULL);

    TRACE("Reading gzipped file: %s", gzpath);
    char buf[8192] = {0};
    i64 lines = 0;
    STOPWATCH_START(watch);
    while (1) {
        ssize_t n = bio->readline(bio, buf, sizeof(buf), &e);
        if (e) {
            WARN("readline error: %s", e);
            break;
        }
        if (n <= 0)
            break; // EOF
        lines++;
    }

    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%lldrows, %s, %.0fops\n", lines, tbuf, time_ops(lines, &watch));

    bio->close(bio);

    TRACE("TESTCASE_STREAM_GZIP_READ: OK");
    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif


#ifdef TESTCASE_HYPERLOGLOG
// ./testcase.sh TESTCASE_HYPERLOGLOG
int main(int argc, char **argv) {
    printf("Running TESTCASE_HYPERLOGLOG...\n");
    // Basic creation
    struct hyperloglog *h = hll_new_default();
    assert(h != NULL);

    // Add N distinct string values
    const int N = 50000; // keep it fast but meaningful
    char buf[64];
    for (int i = 0; i < N; i++) {
        int n = snprintf(buf, sizeof(buf), "user-%d", i);
        (void)n;
        hll_add_cstr(h, buf);
    }
    u64 est = hll_cardinality(h);
    // Expect within ~2.5% to be robust
    double rel_err = fabs((double)est - (double)N) / (double)N;
    printf("HLL est=%llu, N=%d, rel_err=%.4f\n", (unsigned long long)est, N, rel_err);
    assert(rel_err < 0.03); // 3% tolerance

    // Serialization roundtrip (Java-compatible buckets only)
    u32 m = hll_size_in_bytes(h);
    u8 *b1 = hll_bytes_alloc(h);
    assert(b1 != NULL && m == (1u << hll_precision(h)));
    struct hyperloglog *h2 = hll_from_bytes(b1, m);
    assert(h2 != NULL);
    u8 *b2 = hll_bytes_alloc(h2);
    assert(b2 != NULL);
    assert(memcmp(b1, b2, m) == 0);
    u64 est2 = hll_cardinality(h2);
    double rel_err2 = fabs((double)est2 - (double)N) / (double)N;
    assert(rel_err2 < 0.03);

    // Merge test: split into halves and merge
    struct hyperloglog *a = hll_new_default();
    struct hyperloglog *b = hll_new_default();
    assert(a && b);
    for (int i = 0; i < N; i++) {
        int n = snprintf(buf, sizeof(buf), "user-%d", i);
        (void)n;
        if ((i & 1) == 0)
            hll_add_cstr(a, buf);
        else
            hll_add_cstr(b, buf);
    }
    hll_merge(a, b);
    u64 est_merged = hll_cardinality(a);
    double rel_err_m = fabs((double)est_merged - (double)N) / (double)N;
    assert(rel_err_m < 0.03);

    // Clear test
    hll_clear(h);
    assert(hll_cardinality(h) == 0);

    // Cleanup
    FREE(b1);
    FREE(b2);
    hll_free(h);
    hll_free(h2);
    hll_free(a);
    hll_free(b);

    printf("TESTCASE_HYPERLOGLOG: OK\n");

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif

#ifdef TESTCASE_ROARINGBITMAP
// ./testcase.sh TESTCASE_ROARING_BITMAP
int main(int argc, char **argv) {
    printf("Running TESTCASE_ROARING_BITMAP...\n");
    char *e = NULL;

    // Build a bitmap with singles and a dense range
    roaringbitmap *rb = rbitmap_new();
    assert(rb != NULL);
    rbitmap_add(rb, 1);
    rbitmap_add(rb, 2);
    rbitmap_add(rb, 3);
    rbitmap_add_range(rb, 1000, 2000); // [1000,2000) => 1000 elements
    // duplicate adds shouldn't change count
    rbitmap_add(rb, 2);
    int card = rbitmap_cardinality(rb);
    assert(card == 3 + 1000);
    assert(rbitmap_contains(rb, 1));
    assert(rbitmap_contains(rb, 2));
    assert(rbitmap_contains(rb, 3));
    assert(rbitmap_contains(rb, 1000));
    assert(rbitmap_contains(rb, 1999));
    assert(!rbitmap_contains(rb, 2000));

    // Rank/Select checks
    assert(rbitmap_rank(rb, 999) == 3);  // only 1,2,3 <= 999
    assert(rbitmap_rank(rb, 1000) == 4); // 1,2,3,1000
    int v = -1;
    assert(rbitmap_select(rb, 0, &v) == 0 && v == 1);
    assert(rbitmap_select(rb, 2, &v) == 0 && v == 3);
    assert(rbitmap_select(rb, 3, &v) == 0 && v == 1000);
    assert(rbitmap_select(rb, card - 1, &v) == 0 && v == 1999);

    // Remove and verify
    rbitmap_remove(rb, 2);
    assert(!rbitmap_contains(rb, 2));
    assert(rbitmap_cardinality(rb) == card - 1);

    // Serialization roundtrip (RBM1)
    struct buffer *bout = buffer_alloc(1 << 20); // 1MB scratch
    rbitmap_write(rb, bout, &e);
    assert(e == NULL);
    bout->flip(bout);
    struct buffer in;
    buffer_wrap(bout->array, bout->limit, &in);
    roaringbitmap *rb2 = rbitmap_read(&in, &e);
    assert(e == NULL && rb2 != NULL);
    assert(rbitmap_cardinality(rb2) == rbitmap_cardinality(rb));
    // sample membership checks on roundtrip
    assert(rbitmap_contains(rb2, 1));
    assert(!rbitmap_contains(rb2, 2));
    assert(rbitmap_contains(rb2, 1000));
    assert(rbitmap_contains(rb2, 1999));

    // Set algebra
    roaringbitmap *x = rbitmap_new();
    roaringbitmap *y = rbitmap_new();
    assert(x && y);
    rbitmap_add_range(x, 0, 10);              // 0..9 (10)
    rbitmap_add_range(y, 5, 15);              // 5..14 (10)
    roaringbitmap *u = rbitmap_or(x, y);      // 0..14 (15)
    roaringbitmap *inter = rbitmap_and(x, y); // 5..9 (5)
    roaringbitmap *df = rbitmap_andnot(x, y); // 0..4 (5)
    assert(rbitmap_cardinality(u) == 15);
    assert(rbitmap_cardinality(inter) == 5);
    assert(rbitmap_cardinality(df) == 5);
    int t = -1;
    assert(rbitmap_select(inter, 0, &t) == 0 && t == 5);
    assert(rbitmap_select(df, 4, &t) == 0 && t == 4);

    // Cleanup
    bout->free(bout);
    rbitmap_free(rb);
    rbitmap_free(rb2);
    rbitmap_free(x);
    rbitmap_free(y);
    rbitmap_free(u);
    rbitmap_free(inter);
    rbitmap_free(df);

    printf("TESTCASE_ROARING_BITMAP: OK\n");

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif

#ifdef TESTCASE_SORTABLE
// ./testcase.sh TESTCASE_SORTABLE

static int tc_sortable_add_row(struct flintdb_filesort *sorter, struct flintdb_meta *m, i64 id, const char *name, int age, char **e) {
    struct flintdb_row *r = flintdb_row_new(m, e);
    if (e && *e)
        return -1;
    if (!r) {
        if (e)
            *e = "row_new failed";
        return -1;
    }
    r->i64_set(r, 0, id, e);
    if (e && *e) {
        r->free(r);
        return -1;
    }
    r->string_set(r, 1, name, e);
    if (e && *e) {
        r->free(r);
        return -1;
    }
    r->i32_set(r, 2, age, e);
    if (e && *e) {
        r->free(r);
        return -1;
    }
    if (sorter->add(sorter, r, e) != 0 || (e && *e)) {
        r->free(r);
        return -1;
    }
    r->free(r);
    return 0;
}

static int tc_sortable_cmp_id_asc(const void *ctx, const struct flintdb_row *a, const struct flintdb_row *b) {
    (void)ctx;
    char *ce = NULL;
    i64 ai = ((struct flintdb_row *)a)->i64_get((struct flintdb_row *)a, 0, &ce);
    if (ce)
        return 0;
    i64 bi = ((struct flintdb_row *)b)->i64_get((struct flintdb_row *)b, 0, &ce);
    if (ce)
        return 0;
    if (ai < bi)
        return -1;
    if (ai > bi)
        return 1;
    return 0;
}

int main(int argc, char **argv) {
    char *e = NULL;

    // Build a simple meta: id INT64, name STRING(64), age INT32
    struct flintdb_meta m = flintdb_meta_new("filesort", &e);
    if (e)
        THROW_S(e);
    flintdb_meta_columns_add(&m, "id", VARIANT_INT64, 0, 0, SPEC_NULLABLE, "0", "", &e);
    if (e)
        THROW_S(e);
    flintdb_meta_columns_add(&m, "name", VARIANT_STRING, 64, 0, SPEC_NULLABLE, "", "", &e);
    if (e)
        THROW_S(e);
    flintdb_meta_columns_add(&m, "age", VARIANT_INT32, 0, 0, SPEC_NULLABLE, "0", "", &e);
    if (e)
        THROW_S(e);

    // Initialize filesorter on a temp path
    const char *file = "temp/test-sortable.sort";
    struct flintdb_filesort *sorter = flintdb_filesort_new(file, &m, &e);
    if (e)
        THROW_S(e);

    // Add rows in shuffled order
    if (tc_sortable_add_row(sorter, &m, 5, "Eve", 45, &e) != 0)
        THROW_S(e);
    if (tc_sortable_add_row(sorter, &m, 1, "Alice", 30, &e) != 0)
        THROW_S(e);
    if (tc_sortable_add_row(sorter, &m, 3, "Carol", 28, &e) != 0)
        THROW_S(e);
    if (tc_sortable_add_row(sorter, &m, 2, "Bob", 22, &e) != 0)
        THROW_S(e);
    if (tc_sortable_add_row(sorter, &m, 4, "Dave", 33, &e) != 0)
        THROW_S(e);
    if (tc_sortable_add_row(sorter, &m, 0, "Zoe", 19, &e) != 0)
        THROW_S(e);

    // Sort: id ascending
    if (sorter->sort(sorter, tc_sortable_cmp_id_asc, NULL, &e) < 0 || e)
        THROW_S(e);

    // Validate ascending by reading back
    i64 n = sorter->rows(sorter);
    if (e)
        THROW_S(e);
    assert(n == 6);
    i64 prev = -1;
    for (i64 i = 0; i < n; i++) {
        struct flintdb_row *r = sorter->read(sorter, i, &e);
        if (e || !r)
            THROW_S(e);
        i64 id = r->i64_get(r, 0, &e);
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        // monotonic non-decreasing (strictly increasing with our data)
        assert(id > prev);
        prev = id;
        r->free(r);
    }

    sorter->close(sorter);
    flintdb_meta_close(&m);

    printf("TESTCASE_SORTABLE: OK\n");

    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    return -1;
}
#endif

#ifdef TESTCASE_AGGREGATE_FUNCTIONS
// ./testcase.sh TESTCASE_AGGREGATE_FUNCTIONS

static struct flintdb_meta tc_build_meta_1(enum flintdb_variant_type  t, char **e) {
    struct flintdb_meta m = flintdb_meta_new("", e);
    if (e && *e)
        return m;
    flintdb_meta_columns_add(&m, "v", t, (t == VARIANT_STRING ? 32 : 0), 0, NULL, NULL, e);
    return m;
}

int main(int argc, char **argv) {
    char *e = NULL;

    printf("Running TESTCASE_AGGREGATE_FUNCTIONS...\n");

    // COUNT
    {
        struct flintdb_meta m = tc_build_meta_1(VARIANT_INT64, &e);
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e || !r)
            THROW_S(e);
        struct flintdb_aggregate_condition cond = {0};
        struct flintdb_aggregate_func *f = flintdb_func_count("v", NULL, VARIANT_NULL, cond, &e);
        if (e || !f)
            THROW_S(e);
        for (int i = 0; i < 5; i++) {
            r->i64_set(r, 0, (i64)i, &e);
            if (e)
                THROW_S(e);
            f->row(f, NULL, r, &e);
            if (e)
                THROW_S(e);
        }
        f->compute(f, NULL, &e);
        if (e)
            THROW_S(e);
        const struct flintdb_variant *res = f->result(f, NULL, &e);
        if (e || !res)
            THROW_S(e);
        assert(flintdb_variant_i64_get(res, &e) == 5);
        if (e)
            THROW_S(e);
        f->free(f);
        r->free(r);
        flintdb_meta_close(&m);
    }

    // DISTINCT_COUNT exact
    {
        struct flintdb_meta m = tc_build_meta_1(VARIANT_INT64, &e);
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e || !r)
            THROW_S(e);
        struct flintdb_aggregate_condition cond = {0};
        struct flintdb_aggregate_func *f = flintdb_func_distinct_count("v", NULL, VARIANT_NULL, cond, &e);
        if (e || !f)
            THROW_S(e);
        i64 vals[] = {1, 1, 2, 3, 3, 3, 4};
        for (size_t i = 0; i < sizeof(vals) / sizeof(vals[0]); i++) {
            r->i64_set(r, 0, vals[i], &e);
            if (e)
                THROW_S(e);
            f->row(f, NULL, r, &e);
            if (e)
                THROW_S(e);
        }
        f->compute(f, NULL, &e);
        if (e)
            THROW_S(e);
        const struct flintdb_variant *res = f->result(f, NULL, &e);
        if (e || !res)
            THROW_S(e);
        assert(flintdb_variant_i64_get(res, &e) == 4);
        if (e)
            THROW_S(e);
        f->free(f);
        r->free(r);
        flintdb_meta_close(&m);
    }

    // DISTINCT_HLL_COUNT approx (check rough bounds)
    {
        struct flintdb_meta m = tc_build_meta_1(VARIANT_INT64, &e);
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e || !r)
            THROW_S(e);
        struct flintdb_aggregate_condition cond = {0};
        struct flintdb_aggregate_func *f = flintdb_func_distinct_hll_count("v", NULL, VARIANT_NULL, cond, &e);
        if (e || !f)
            THROW_S(e);
        i64 vals[] = {1, 1, 2, 3, 3, 3, 4};
        for (size_t i = 0; i < sizeof(vals) / sizeof(vals[0]); i++) {
            r->i64_set(r, 0, vals[i], &e);
            if (e)
                THROW_S(e);
            f->row(f, NULL, r, &e);
            if (e)
                THROW_S(e);
        }
        f->compute(f, NULL, &e);
        if (e)
            THROW_S(e);
        const struct flintdb_variant *res = f->result(f, NULL, &e);
        if (e || !res)
            THROW_S(e);
        i64 est = flintdb_variant_i64_get(res, &e);
        if (e)
            THROW_S(e);
        assert(est >= 2 && est <= 6); // expect around 4
        f->free(f);
        r->free(r);
        flintdb_meta_close(&m);
    }

    // SUM, AVG
    {
        struct flintdb_meta m = tc_build_meta_1(VARIANT_INT64, &e);
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e || !r)
            THROW_S(e);
        struct flintdb_aggregate_condition cond = {0};
        struct flintdb_aggregate_func *fs = flintdb_func_sum("v", NULL, VARIANT_NULL, cond, &e);
        struct flintdb_aggregate_func *fa = flintdb_func_avg("v", NULL, VARIANT_NULL, cond, &e);
        if (e || !fs || !fa)
            THROW_S(e);
        i64 vals[] = {1, 2, 3};
        for (size_t i = 0; i < sizeof(vals) / sizeof(vals[0]); i++) {
            r->i64_set(r, 0, vals[i], &e);
            if (e)
                THROW_S(e);
            fs->row(fs, NULL, r, &e);
            fa->row(fa, NULL, r, &e);
            if (e)
                THROW_S(e);
        }
        fs->compute(fs, NULL, &e);
        fa->compute(fa, NULL, &e);
        if (e)
            THROW_S(e);
        const struct flintdb_variant *sum_res = fs->result(fs, NULL, &e);
        const struct flintdb_variant *avg_res = fa->result(fa, NULL, &e);
        if (e)
            THROW_S(e);
        // Results are DECIMAL, convert to string
        char buf[64];
        flintdb_variant_to_string(sum_res, buf, sizeof(buf));
        assert(strcmp(buf, "6.00000") == 0);
        flintdb_variant_to_string(avg_res, buf, sizeof(buf));
        assert(strcmp(buf, "2.00000") == 0);
        if (e)
            THROW_S(e);
        fs->free(fs);
        fa->free(fa);
        r->free(r);
        flintdb_meta_close(&m);
    }

    // MIN, MAX on integers
    {
        struct flintdb_meta m = tc_build_meta_1(VARIANT_INT64, &e);
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e || !r)
            THROW_S(e);
        struct flintdb_aggregate_condition cond = {0};
        struct flintdb_aggregate_func *fmin = flintdb_func_min("v", NULL, VARIANT_NULL, cond, &e);
        struct flintdb_aggregate_func *fmax = flintdb_func_max("v", NULL, VARIANT_NULL, cond, &e);
        if (e || !fmin || !fmax)
            THROW_S(e);
        i64 vals[] = {5, 1, 3};
        for (size_t i = 0; i < sizeof(vals) / sizeof(vals[0]); i++) {
            r->i64_set(r, 0, vals[i], &e);
            if (e)
                THROW_S(e);
            fmin->row(fmin, NULL, r, &e);
            fmax->row(fmax, NULL, r, &e);
            if (e)
                THROW_S(e);
        }
        fmin->compute(fmin, NULL, &e);
        fmax->compute(fmax, NULL, &e);
        if (e)
            THROW_S(e);
        assert(flintdb_variant_i64_get(fmin->result(fmin, NULL, &e), &e) == 1);
        if (e)
            THROW_S(e);
        assert(flintdb_variant_i64_get(fmax->result(fmax, NULL, &e), &e) == 5);
        if (e)
            THROW_S(e);
        fmin->free(fmin);
        fmax->free(fmax);
        r->free(r);
        flintdb_meta_close(&m);
    }

    // FIRST, LAST with NIL in between
    {
        struct flintdb_meta m = tc_build_meta_1(VARIANT_INT64, &e);
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e || !r)
            THROW_S(e);
        struct flintdb_aggregate_condition cond = {0};
        struct flintdb_aggregate_func *ffirst = flintdb_func_first("v", NULL, VARIANT_NULL, cond, &e);
        struct flintdb_aggregate_func *flast = flintdb_func_last("v", NULL, VARIANT_NULL, cond, &e);
        if (e || !ffirst || !flast)
            THROW_S(e);
        // 10, NIL, 30
        r->i64_set(r, 0, 10, &e);
        if (e)
            THROW_S(e);
        ffirst->row(ffirst, NULL, r, &e);
        flast->row(flast, NULL, r, &e);
        struct flintdb_variant tmp;
        flintdb_variant_init(&tmp);
        flintdb_variant_null_set(&tmp);
        r->set(r, 0, &tmp, &e);
        flintdb_variant_free(&tmp);
        if (e)
            THROW_S(e);
        ffirst->row(ffirst, NULL, r, &e);
        flast->row(flast, NULL, r, &e);
        r->i64_set(r, 0, 30, &e);
        if (e)
            THROW_S(e);
        ffirst->row(ffirst, NULL, r, &e);
        flast->row(flast, NULL, r, &e);
        ffirst->compute(ffirst, NULL, &e);
        flast->compute(flast, NULL, &e);
        if (e)
            THROW_S(e);
        assert(flintdb_variant_i64_get(ffirst->result(ffirst, NULL, &e), &e) == 10);
        if (e)
            THROW_S(e);
        assert(flintdb_variant_i64_get(flast->result(flast, NULL, &e), &e) == 30);
        if (e)
            THROW_S(e);
        ffirst->free(ffirst);
        flast->free(flast);
        r->free(r);
        flintdb_meta_close(&m);
    }

    // MIN/MAX with strings
    {
        struct flintdb_meta m = tc_build_meta_1(VARIANT_STRING, &e);
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e || !r)
            THROW_S(e);
        struct flintdb_aggregate_condition cond = {0};
        struct flintdb_aggregate_func *fmin = flintdb_func_min("v", NULL, VARIANT_NULL, cond, &e);
        struct flintdb_aggregate_func *fmax = flintdb_func_max("v", NULL, VARIANT_NULL, cond, &e);
        if (e || !fmin || !fmax)
            THROW_S(e);
        const char *vals[] = {"b", "a", "c"};
        for (size_t i = 0; i < sizeof(vals) / sizeof(vals[0]); i++) {
            r->string_set(r, 0, vals[i], &e);
            if (e)
                THROW_S(e);
            fmin->row(fmin, NULL, r, &e);
            fmax->row(fmax, NULL, r, &e);
            if (e)
                THROW_S(e);
        }
        fmin->compute(fmin, NULL, &e);
        fmax->compute(fmax, NULL, &e);
        if (e)
            THROW_S(e);
        assert(strcmp(flintdb_variant_string_get(fmin->result(fmin, NULL, &e)), "a") == 0);
        assert(strcmp(flintdb_variant_string_get(fmax->result(fmax, NULL, &e)), "c") == 0);
        fmin->free(fmin);
        fmax->free(fmax);
        r->free(r);
        flintdb_meta_close(&m);
    }

    // Test 9: GROUP BY with aggregate functions
    {
        struct flintdb_meta m = flintdb_meta_new("test_groupby", &e);
        if (e)
            THROW_S(e);
        flintdb_meta_columns_add(&m, "category", VARIANT_STRING, 32, 0,SPEC_NULLABLE,  NULL, NULL, &e);
        flintdb_meta_columns_add(&m, "amount", VARIANT_INT64, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
        if (e)
            THROW_S(e);

        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e || !r)
            THROW_S(e);

        // Insert test data: A:10, A:20, B:30, B:40, C:50
        const char *categories[] = {"A", "A", "B", "B", "C"};
        int64_t amounts[] = {10, 20, 30, 40, 50};
        int data_count = 5;

        struct flintdb_aggregate_condition cond = {0};
        // Create groupby and aggregate functions
        struct flintdb_aggregate_groupby *gb = groupby_new("category", "category", VARIANT_STRING, &e);
        struct flintdb_aggregate_func *fc = flintdb_func_count("amount", "cnt", VARIANT_NULL, cond, &e);
        struct flintdb_aggregate_func *fs = flintdb_func_sum("amount", "total", VARIANT_NULL, cond, &e);
        if (e || !gb || !fc || !fs)
            THROW_S(e);

        // Allocate arrays on heap (aggregate_new stores these pointers)
        struct flintdb_aggregate_groupby **gbs = (struct flintdb_aggregate_groupby **)CALLOC(1, sizeof(struct flintdb_aggregate_groupby *));
        struct flintdb_aggregate_func **funcs = (struct flintdb_aggregate_func **)CALLOC(2, sizeof(struct flintdb_aggregate_func *));
        if (!gbs || !funcs)
            THROW_S(e);
        gbs[0] = gb;
        funcs[0] = fc;
        funcs[1] = fs;
        
        struct flintdb_aggregate *agg = aggregate_new("test_groupby", gbs, 1, funcs, 2, &e);
        if (e || !agg)
            THROW_S(e);

        // Feed rows to aggregate
        for (int i = 0; i < data_count; i++) {
            r->string_set(r, 0, categories[i], &e);
            r->i64_set(r, 1, amounts[i], &e);
            if (e)
                THROW_S(e);
            agg->row(agg, r, &e);
            if (e)
                THROW_S(e);
        }

        // Compute results
        struct flintdb_row **out_rows = NULL;
        int row_count = agg->compute(agg, &out_rows, &e);
        if (e)
            THROW_S(e);
        
        // Expected: 3 groups (A:2,30), (B:2,70), (C:1,50)
        assert(row_count == 3);
        
        // Verify results
        for (int i = 0; i < row_count; i++) {
            struct flintdb_row *rr = out_rows[i];
            const char *cat = rr->string_get(rr, 0, &e);
            int64_t cnt = rr->i64_get(rr, 1, &e);
            const char *total_str = rr->string_get(rr, 2, &e);
            if (e)
                THROW_S(e);
            
            if (strcmp(cat, "A") == 0) {
                assert(cnt == 2);
                assert(strcmp(total_str, "30.00000") == 0);
            } else if (strcmp(cat, "B") == 0) {
                assert(cnt == 2);
                assert(strcmp(total_str, "70.00000") == 0);
            } else if (strcmp(cat, "C") == 0) {
                assert(cnt == 1);
                assert(strcmp(total_str, "50.00000") == 0);
            }
        }

        // Cleanup
        for (int i = 0; i < row_count; i++) {
            out_rows[i]->free(out_rows[i]);
        }
        FREE(out_rows);
        agg->free(agg);
        r->free(r);
        flintdb_meta_close(&m);
    }

    printf("TESTCASE_AGGREGATE_FUNCTIONS: OK\n");
    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    return -1;
}
#endif

#ifdef TESTCASE_AGGREGATE_TUTORIAL
// ./testcase.sh TESTCASE_AGGREGATE_TUTORIAL
// = java/src/tutorial/java/GroupByExample.java
// Comparator: category (col 0) ascending
static int tc_cmp_category(const struct flintdb_row *a, const struct flintdb_row *b, void *ctx) {
    (void)ctx;
    char *ce = NULL;
    const char *sa = ((struct flintdb_row *)a)->string_get((struct flintdb_row *)a, 0, &ce);
    if (ce)
        return 0;
    const char *sb = ((struct flintdb_row *)b)->string_get((struct flintdb_row *)b, 0, &ce);
    if (ce)
        return 0;
    if (!sa && !sb)
        return 0;
    if (!sa)
        return -1;
    if (!sb)
        return 1;
    return strcmp(sa, sb);
}

struct tc_group_state {
    char cur_cat[128];
    int have_group;
    struct flintdb_meta *rm;
    struct flintdb_aggregate_func *f_sum, *f_cnt, *f_avg, *f_dcnt, *f_hll, *f_rb;
    char **e;
};

static void tc_group_free(keytype k, valtype v) {
    // free key and group state (if any aggregate funcs remain, free them)
    if (k)
        FREE((void *)(var)k);
    struct tc_group_state *gst = (struct tc_group_state *)(var)v;
    if (!gst)
        return;
    if (gst->f_sum) {
        gst->f_sum->free(gst->f_sum);
        gst->f_sum = NULL;
    }
    if (gst->f_cnt) {
        gst->f_cnt->free(gst->f_cnt);
        gst->f_cnt = NULL;
    }
    if (gst->f_avg) {
        gst->f_avg->free(gst->f_avg);
        gst->f_avg = NULL;
    }
    if (gst->f_dcnt) {
        gst->f_dcnt->free(gst->f_dcnt);
        gst->f_dcnt = NULL;
    }
    if (gst->f_hll) {
        gst->f_hll->free(gst->f_hll);
        gst->f_hll = NULL;
    }
    if (gst->f_rb) {
        gst->f_rb->free(gst->f_rb);
        gst->f_rb = NULL;
    }
    FREE(gst);
}

static void tc_finalize_group(struct tc_group_state *st) {
    if (!st || !st->have_group)
        return;
    char **e = st->e;
    if (!st->f_sum || !st->f_cnt || !st->f_avg || !st->f_dcnt || !st->f_hll || !st->f_rb) {
        TRACE("finalize_group missing funcs: sum=%p cnt=%p avg=%p dcnt=%p hll=%p rb=%p",
              (void *)st->f_sum, (void *)st->f_cnt, (void *)st->f_avg, (void *)st->f_dcnt, (void *)st->f_hll, (void *)st->f_rb);
        return;
    }
    // compute (groupkey is NULL for non-grouped aggregation)
    TRACE("finalize %s: compute sum", st->cur_cat);
    st->f_sum->compute(st->f_sum, NULL, e);
    TRACE("finalize %s: compute cnt", st->cur_cat);
    st->f_cnt->compute(st->f_cnt, NULL, e);
    TRACE("finalize %s: compute avg", st->cur_cat);
    st->f_avg->compute(st->f_avg, NULL, e);
    TRACE("finalize %s: compute dcnt", st->cur_cat);
    st->f_dcnt->compute(st->f_dcnt, NULL, e);
    TRACE("finalize %s: compute hll", st->cur_cat);
    st->f_hll->compute(st->f_hll, NULL, e);
    TRACE("finalize %s: compute rb", st->cur_cat);
    st->f_rb->compute(st->f_rb, NULL, e);
    if (e && *e)
        return;

    // Build result row
    struct flintdb_row *rr = flintdb_row_new(st->rm, e);
    if (!rr || (e && *e))
        return;
    rr->string_set(rr, 0, st->cur_cat, e);
    
    // Get results - SUM and AVG return DECIMAL by default, need to convert
    const struct flintdb_variant *sum_v = st->f_sum->result(st->f_sum, NULL, e);
    const struct flintdb_variant *cnt_v = st->f_cnt->result(st->f_cnt, NULL, e);
    const struct flintdb_variant *avg_v = st->f_avg->result(st->f_avg, NULL, e);
    
    double sum_val = 0.0, avg_val = 0.0;
    if (sum_v && sum_v->type == VARIANT_DECIMAL) {
        const char *sum_str = flintdb_variant_string_get(sum_v);
        if (sum_str) sum_val = atof(sum_str);
    } else if (sum_v) {
        sum_val = flintdb_variant_f64_get(sum_v, e);
    }
    
    if (avg_v && avg_v->type == VARIANT_DECIMAL) {
        const char *avg_str = flintdb_variant_string_get(avg_v);
        if (avg_str) avg_val = atof(avg_str);
    } else if (avg_v) {
        avg_val = flintdb_variant_f64_get(avg_v, e);
    }
    
    rr->f64_set(rr, 1, sum_val, e);
    rr->i64_set(rr, 2, cnt_v ? flintdb_variant_i64_get(cnt_v, e) : 0, e);
    rr->f64_set(rr, 3, avg_val, e);
    rr->i64_set(rr, 4, flintdb_variant_i64_get(st->f_dcnt->result(st->f_dcnt, NULL, e), e), e);
    rr->i64_set(rr, 5, flintdb_variant_i64_get(st->f_hll->result(st->f_hll, NULL, e), e), e);
    rr->i64_set(rr, 6, flintdb_variant_i64_get(st->f_rb->result(st->f_rb, NULL, e), e), e);

    // Print
    printf("category=%s, total_price=%.0f, item_count=%lld, average_price=%.0f, distinct=%lld, distinct_hll=%lld, distinct_rb=%lld\n",
           st->cur_cat,
           rr->f64_get(rr, 1, e),
           (long long)rr->i64_get(rr, 2, e),
           rr->f64_get(rr, 3, e),
           (long long)rr->i64_get(rr, 4, e),
           (long long)rr->i64_get(rr, 5, e),
           (long long)rr->i64_get(rr, 6, e));

    // Assertions for expected values
    if (strcmp(st->cur_cat, "Fruit") == 0) {
        assert((long long)rr->i64_get(rr, 2, e) == 3);
        assert((long long)rr->i64_get(rr, 4, e) == 3);
        assert((long long)rr->i64_get(rr, 6, e) == 3);
        assert((long long)(rr->f64_get(rr, 1, e) + 0.5) == 270);
        assert((long long)(rr->f64_get(rr, 3, e) + 0.5) == 90);
    } else if (strcmp(st->cur_cat, "Vegetable") == 0) {
        assert((long long)rr->i64_get(rr, 2, e) == 2);
        assert((long long)rr->i64_get(rr, 4, e) == 2);
        assert((long long)rr->i64_get(rr, 6, e) == 2);
        assert((long long)(rr->f64_get(rr, 1, e) + 0.5) == 120);
        assert((long long)(rr->f64_get(rr, 3, e) + 0.5) == 60);
    }

    rr->free(rr);

    // cleanup funcs
    st->f_sum->free(st->f_sum);
    st->f_sum = NULL;
    st->f_cnt->free(st->f_cnt);
    st->f_cnt = NULL;
    st->f_avg->free(st->f_avg);
    st->f_avg = NULL;
    st->f_dcnt->free(st->f_dcnt);
    st->f_dcnt = NULL;
    st->f_hll->free(st->f_hll);
    st->f_hll = NULL;
    st->f_rb->free(st->f_rb);
    st->f_rb = NULL;
    st->have_group = 0;
    st->cur_cat[0] = '\0';
}

int main(int argc, char **argv) {
    printf("Running TESTCASE_AGGREGATE_TUTORIAL...\n");
    char *e = NULL;

    // 1) Prepare sample TSV file in temp
    const char *path = "temp/example_groupby.tsv";
    flintdb_genericfile_drop(path, NULL); // ignore error

    // Build meta: category STRING(50), item STRING(100), price UINT32
    struct flintdb_meta m = flintdb_meta_new("example_groupby.tsv", &e);
    if (e)
        THROW_S(e);
    flintdb_meta_columns_add(&m, "category", VARIANT_STRING, 50, 0, SPEC_NULLABLE, NULL, NULL, &e);
    if (e)
        THROW_S(e);
    flintdb_meta_columns_add(&m, "item", VARIANT_STRING, 100, 0, SPEC_NULLABLE, NULL, NULL, &e);
    if (e)
        THROW_S(e);
    flintdb_meta_columns_add(&m, "price", VARIANT_UINT32, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    if (e)
        THROW_S(e);

    struct flintdb_genericfile *f = flintdb_genericfile_open(path, FLINTDB_RDWR, &m, &e);
    if (e || !f)
        THROW(&e, "genericfile_open failed");

    // Insert sample rows
    struct {
        const char *cat;
        const char *item;
        u32 price;
    } rows[] = {
        {"Fruit", "Apple", 100},
        {"Fruit", "Banana", 80},
        {"Fruit", "Orange", 90},
        {"Vegetable", "Carrot", 50},
        {"Vegetable", "Broccoli", 70},
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e || !r)
            THROW(&e, "row_new failed");
        r->string_set(r, 0, rows[i].cat, &e);
        r->string_set(r, 1, rows[i].item, &e);
        r->u32_set(r, 2, rows[i].price, &e);
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        i64 ok = f->write(f, r, &e);
        if (e || ok != 0) {
            r->free(r);
            THROW(&e, "write failed");
        }
        r->free(r);
    }
    const i64 written = f->rows(f, &e);
    if (e)
        THROW_S(e);
    // Some formats may not track rows until flush; tolerate non-5 but continue
    if (written != 5) {
        TRACE("written rows reported as %lld (expected 5)", (long long)written);
    }
    // 2) Read back using same handle and group-by category using a linked hashmap (preserves insertion order)

    // 3) Group scan with hashmap and compute aggregates
    // Result meta: category STRING(50), total_price DOUBLE, item_count INT64, average_price DOUBLE,
    //              item_count_distinct INT64, item_count_distinct_hll INT64, item_count_distinct_rb INT64
    struct flintdb_meta rm = flintdb_meta_new("groupby_result", &e);
    if (e)
        THROW_S(e);
    flintdb_meta_columns_add(&rm, "category", VARIANT_STRING, 50, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&rm, "total_price", VARIANT_DOUBLE, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&rm, "item_count", VARIANT_INT64, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&rm, "average_price", VARIANT_DOUBLE, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&rm, "item_count_distinct", VARIANT_INT64, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&rm, "item_count_distinct_hll", VARIANT_INT64, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&rm, "item_count_distinct_rb", VARIANT_INT64, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    if (e)
        THROW_S(e);

    // Helper single-column metas/rows for feeding aggregate functions
    struct flintdb_meta m_price = flintdb_meta_new("price_only", &e);
    flintdb_meta_columns_add(&m_price, "v", VARIANT_DOUBLE, 0, 0, NULL, NULL, &e);
    struct flintdb_row *r_price = flintdb_row_new(&m_price, &e);
    if (e)
        THROW_S(e);
    struct flintdb_meta m_item = flintdb_meta_new("item_only", &e);
    flintdb_meta_columns_add(&m_item, "v", VARIANT_STRING, 100, 0, NULL, NULL, &e);
    struct flintdb_row *r_item = flintdb_row_new(&m_item, &e);
    if (e)
        THROW_S(e);

    // Iterate input rows and aggregate by category
    struct tc_group_state st = {0};
    st.rm = &rm;
    st.e = &e;
    st.have_group = 0;
    st.cur_cat[0] = '\0';
    struct flintdb_aggregate_condition cond = {0};

    struct hashmap *groups = linkedhashmap_new(16, hashmap_string_hash, hashmap_string_cmpr);
    if (!groups)
        THROW(&e, "groups hashmap alloc failed");

    // For this tutorial test, feed from in-memory rows[] instead of reading back
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        TRACE("feed row %zu: cat=%s item=%s price=%u", i, rows[i].cat, rows[i].item, rows[i].price);
        // lookup or create group state by category
        const char *cat = rows[i].cat;
        const char *item = rows[i].item;
        u32 price = rows[i].price;
        char *k = NULL;
        if (cat) {
            size_t ln = strlen(cat);
            k = (char *)MALLOC(ln + 1);
            memcpy(k, cat, ln);
            k[ln] = '\0';
        } else {
            k = (char *)MALLOC(1);
            k[0] = '\0';
        }

        TRACE("before groups->get for key=%s", k);
        valtype gv = groups->get(groups, HASHMAP_CAST_KEY(k));
        struct tc_group_state *gst = (gv == HASHMAP_INVALID_VAL) ? NULL : (struct tc_group_state *)(var)gv;
        TRACE("after groups->get for key=%s -> %p (raw=0x%llx)", k, (void *)gst, (unsigned long long)gv);
        if (gst == NULL) {
            TRACE("new group for cat=%s", k);
            gst = (struct tc_group_state *)CALLOC(1, sizeof(struct tc_group_state));
            strncpy(gst->cur_cat, k, sizeof(gst->cur_cat) - 1);
            gst->cur_cat[sizeof(gst->cur_cat) - 1] = '\0';
            gst->rm = &rm;
            gst->e = &e;
            gst->have_group = 1;
            gst->f_sum = flintdb_func_sum("v", "total_price", VARIANT_DOUBLE, cond, &e);
            gst->f_cnt = flintdb_func_count("v", "item_count", VARIANT_NULL, cond, &e);
            gst->f_avg = flintdb_func_avg("v", "average_price", VARIANT_DOUBLE, cond, &e);
            gst->f_dcnt = flintdb_func_distinct_count("v", "item_count_distinct", VARIANT_NULL, cond, &e);
            gst->f_hll = flintdb_func_distinct_hll_count("v", "item_count_distinct_hll", VARIANT_NULL, cond, &e);
            gst->f_rb = flintdb_func_distinct_count("v", "item_count_distinct_rb", VARIANT_NULL, cond, &e);
            if (e) {
                FREE(k);
                THROW_S(e);
            }
            groups->put(groups, HASHMAP_CAST_KEY(k), HASHMAP_CAST_VAL((var)gst), tc_group_free);
        } else {
            FREE(k);
        }

        // feed aggregates
        // Use temp rows for price and item
        // COUNT: must pass a non-NULL row; use r_item as a dummy carrier
        gst->f_cnt->row(gst->f_cnt, NULL, r_item, &e);
        r_price->f64_set(r_price, 0, (f64)price, &e);
        gst->f_sum->row(gst->f_sum, NULL, r_price, &e);
        gst->f_avg->row(gst->f_avg, NULL, r_price, &e);
        r_item->string_set(r_item, 0, item ? item : "", &e);
        gst->f_dcnt->row(gst->f_dcnt, NULL, r_item, &e);
        gst->f_hll->row(gst->f_hll, NULL, r_item, &e);
        gst->f_rb->row(gst->f_rb, NULL, r_item, &e);
    }

    // finalize all groups in insertion order
    int total_groups = groups->count_get(groups);
    TRACE("groups count = %d", total_groups);
    struct map_iterator it = {0};
    int gcount = 0;
    while (groups->iterate(groups, &it)) {
        struct tc_group_state *gst = (struct tc_group_state *)(var)it.val;
        tc_finalize_group(gst);
        gcount++;
    }
    assert(gcount == 2);
    groups->clear(groups); // invokes tc_group_free per entry
    groups->free(groups);

    // Cleanup
    if (f)
        f->close(f);
    r_price->free(r_price);
    r_item->free(r_item);
    flintdb_meta_close(&m_price);
    flintdb_meta_close(&m_item);
    flintdb_meta_close(&rm);
    flintdb_meta_close(&m);

    printf("TESTCASE_AGGREGATE_TUTORIAL: OK\n");
    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    return -1;
}
#endif

#ifdef TESTCASE_PERF_BUFIO_READ
// ./testcase.sh TESTCASE_PERF_BUFIO_READ
int main(int argc, char **argv) {
    char *e = NULL;
    const char *gzpath = "../java/temp/tpch/lineitem.tbl.gz";
    struct bufio *b = file_bufio_open(gzpath, FLINTDB_RDONLY, 64 * 1024, &e);
    if (e || !b)
        THROW(&e, "bufio_open failed: %s", e ? e : "unknown error");

    STOPWATCH_START(watch);
    i64 lines = 0;
    char buf[64 * 1024];
    for (;;) {
        ssize_t nr = b->readline(b, buf, sizeof(buf), &e);
        if (e)
            THROW_S(e);
        if (nr == 0)
            break;
        lines++;
    }
    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%lldrows, %s, %.0fops\n", lines, tbuf, time_ops(lines, &watch));

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    return -1;
}

#endif // TESTCASE_PERF_BUFIO_READ

#ifdef TESTCASE_PERF_TSV_READ
// ./testcase.sh TESTCASE_PERF_TSV_READ
int main(int argc, char **argv) {
    char *e = NULL;

    struct flintdb_genericfile *f = NULL;

    f = flintdb_genericfile_open("../c/temp/tpch_lineitem.tsv.gz", FLINTDB_RDONLY, NULL, &e);
    if (e || !f)
        THROW(&e, "%s", e ? e : "unknown error");

    i64 nrows = f->rows(f, &e);
    if (e)
        THROW_S(e);

    // Iterate all rows from source file and insert into the table
    struct flintdb_cursor_row *cur = f->find(f, NULL, &e);
    if (e || !cur)
        THROW(&e, "find cursor failed: %s", e ? e : "unknown error");

    STOPWATCH_START(watch);
    i64 rows = 0;
    for (struct flintdb_row *r; (r = cur->next(cur, &e)) != NULL;) {
        if (e) {
            r->free(r);
            THROW_S(e);
        }

        // print row
        if (rows < 3) {
            flintdb_print_row(r);
        }

        r->free(r);
        rows++;
    }
    cur->close(cur);
    if (e)
        THROW_S(e);
    TRACE("tpch_lineitem read rows: %lld", (long long)rows);

    f->close(f);
    TRACE("file closed");

    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%lldrows, %s, %.0fops\n", rows, tbuf, time_ops(rows, &watch));

    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    if (f)
        f->close(f);
    return -1;
}

#endif // TESTCASE_PERF_TSV_READ

#ifdef TESTCASE_PERF_TSV_WRITE
// ./testcase.sh TESTCASE_PERF_TSV_WRITE
int main(int argc, char **argv) {
    char *e = NULL;
    i64 max = 1024 * 1024 * 1;

    const char *path = "temp/perf_tsv_write.tsv";
    flintdb_genericfile_drop(path, NULL); // best-effort cleanup

    // Build simple TSV meta with header
    struct flintdb_meta m = flintdb_meta_new("perf_tsv_write", &e);
    if (e)
        THROW_S(e);
    m.delimiter = '\t';
    m.quote = '\0';
    m.escape = '\\';
    m.absent_header = 1;
    strcpy(m.nil_str, "\\N");
    flintdb_meta_columns_add(&m, "id", VARIANT_INT64, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "name", VARIANT_STRING, 32, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "v", VARIANT_INT64, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    if (e)
        THROW_S(e);

    struct flintdb_genericfile *f = flintdb_genericfile_open(path, FLINTDB_RDWR, &m, &e);
    if (e || !f)
        THROW(&e, "genericfile_open failed: %s", e ? e : "?");

    STOPWATCH_START(watch);
    for (i64 i = 0; i < max; i++) {
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e || !r)
            THROW(&e, "row_new failed");
        r->i64_set(r, 0, (i64)(i + 1), &e);
        char nm[40];
        snprintf(nm, sizeof(nm), "name-%09lld", (long long)(i + 1));
        r->string_set(r, 1, nm, &e);
        r->i64_set(r, 2, (i64)(i & 0x7fffffff), &e);
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        i64 ok = f->write(f, r, &e);
        r->free(r);
        if (e || ok != 0) {
            printf("DEBUG: write returned ok=%lld, e=%s\n", (long long)ok, (e ? e : "NULL"));
            THROW(&e, "write failed at i=%lld", (long long)i);
        }
    }
    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));

    // Close and report
    f->close(f);
    f = NULL;
    printf("%lldrows, %s, %.0fops\n", max, tbuf, time_ops(max, &watch));

    PRINT_MEMORY_LEAK_INFO();
    flintdb_meta_close(&m);
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    return -1;
}
#endif // TESTCASE_PERF_TSV_WRITE

#ifdef TESTCASE_PERF_STORAGE_WRITE
// ./testcase.sh TESTCASE_PERF_STORAGE_WRITE
int main(int argc, char **argv) {
    char *e = NULL;
    i64 max = 1024 * 1024 * 1;

    struct storage s = {0};
    struct storage_opts opts = {
        .file = "./temp/strorage.bin",
        .mode = FLINTDB_RDWR,
        .block_bytes = 512 - 16,
        // .extra_header_bytes = 0,
        // .compact = -1,
        // .increment = -1, // 1024*1024*10,
        // .type = TYPE_DEFAULT,
        // .compress = ""
    };
    unlink(opts.file);

    int ok = storage_open(&s, opts, &e);
    assert(ok == 0);

    STOPWATCH_START(watch);
    char str[128] = {0};
    struct buffer bb = {0};

    for (int i = 0; i < max; i++) {
        sprintf(str, "This is a test line number %09d\n", i + 1);
        buffer_wrap(str, strlen(str), &bb);
        s.write(&s, &bb, &e);
    }

    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%lldrows, %s, %.0fops\n", max, tbuf, time_ops(max, &watch));

    s.close(&s);

    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    return -1;
}

#endif // TESTCASE_PERF_STORAGE_WRITE

#ifdef TESTCASE_PERF_STORAGE_READ
// ./testcase.sh TESTCASE_PERF_STORAGE_READ

int main(int argc, char **argv) {
    char *e = NULL;

    struct storage s = {0};
    struct storage_opts opts = {
        .file = "./temp/strorage.bin",
        .mode = FLINTDB_RDONLY,
        .block_bytes = 512 - 16,
        // .extra_header_bytes = 0,
        // .compact = -1,
        // .increment = -1, // 1024*1024*10,
        // .type = TYPE_DEFAULT,
        // .compress = ""
    };

    int ok = storage_open(&s, opts, &e);
    assert(ok == 0);

    STOPWATCH_START(watch);
    i64 max = s.count_get(&s);
    for (i64 i = 0; i < max; i++) {
        struct buffer *r = s.read(&s, i, &e);
        assert(r);
        r->free(r);
    }

    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%lldrows, %s, %.0fops\n", max, tbuf, time_ops(max, &watch));

    s.close(&s);

    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    return -1;
}

#endif // TESTCASE_PERF_STORAGE_READ

#ifdef TESTCASE_PERF_BIN_ENCODE
// ./testcase.sh TESTCASE_PERF_BIN_ENCODE

int main(int argc, char **argv) {
    char *e = NULL;
    // i64 max = 1024 * 1024 * 10;
    i64 max = 1024 * 10;

    struct formatter f;
    struct flintdb_meta m = flintdb_meta_new("engine_test", &e);
    if (e)
        THROW_S(e);
    flintdb_meta_columns_add(&m, "i64_col", VARIANT_INT64, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "f64_col", VARIANT_DOUBLE, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "decimal_col", VARIANT_DECIMAL, 8, 2, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "str_col", VARIANT_STRING, 64, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "date_col", VARIANT_DATE, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "time_col", VARIANT_TIME, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    if (e)
        THROW_S(e);

    formatter_init(FORMAT_BIN, &m, &f, &e);
    if (e)
        THROW_S(e);

    struct buffer *b = buffer_alloc(1024);
    if (!b) {
        e = "buffer_alloc failed";
        THROW_S(e);
    }

    char s[64];
    size_t total_bytes = 0;
    STOPWATCH_START(watch);
    for (i64 i = 0; i < max; i++) {
        struct flintdb_row *r = flintdb_row_new(&m, &e);
        if (e)
            THROW_S(e);
        r->i64_set(r, 0, i, &e);
        r->f64_set(r, 1, (f64)i * 1.1, &e);
        struct flintdb_decimal  d = flintdb_decimal_from_f64(((f64)i) * 1.11, 2, &e);
        if (e) {
            r->free(r);
            THROW_S(e);
        }
        r->decimal_set(r, 2, d, &e);
        snprintf(s, sizeof(s), "string value %09lld", (long long)i);
        r->string_set(r, 3, s, &e);
        time_t dt = 1609459200 + (i * 86400); // 2021-01-01 + i days
        r->date_set(r, 4, dt, &e);
        r->time_set(r, 5, (i * 60) % 86400, &e); // time in seconds
        if (e) {
            r->free(r);
            THROW_S(e);
        }

        int encoded = f.encode(&f, r, b, &e);
        r->free(r);
        if (e)
            THROW_S(e);
        if (encoded < 0)
            THROW(&e, "encode failed at row %lld", (long long)i);
        total_bytes += (size_t)b->limit;
    }
    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%lldrows, %s, %.0fops, total_bytes=%zu\n", max, tbuf, time_ops(max, &watch), total_bytes);

    b->free(b);
    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    return -1;
}

#endif // TESTCASE_PERF_BIN_ENCODE

#ifdef TESTCASE_PERF_BIN_DECODE
// ./testcase.sh TESTCASE_PERF_BIN_DECODE
// ./testcase.sh TESTCASE_PERF_BIN_ENCODE
int main(int argc, char **argv) {
    char *e = NULL;
    i64 max = 1024 * 1024 * 10;

    struct formatter f;
    struct flintdb_meta m = flintdb_meta_new("engine_test", &e);
    if (e)
        THROW_S(e);
    flintdb_meta_columns_add(&m, "i64_col", VARIANT_INT64, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "f64_col", VARIANT_DOUBLE, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "decimal_col", VARIANT_DECIMAL, 8, 2, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "str_col", VARIANT_STRING, 64, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "date_col", VARIANT_DATE, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    flintdb_meta_columns_add(&m, "time_col", VARIANT_TIME, 0, 0, SPEC_NULLABLE, NULL, NULL, &e);
    if (e)
        THROW_S(e);

    formatter_init(FORMAT_BIN, &m, &f, &e);
    if (e)
        THROW_S(e);

    // Prepare one encoded row as the decode source
    struct buffer *b = buffer_alloc(1024);
    if (!b) {
        e = "buffer_alloc failed";
        THROW_S(e);
    }

    struct flintdb_row *src = flintdb_row_new(&m, &e);
    if (e)
        THROW_S(e);
    // sample values
    i64 base_i = 123456789LL;
    f64 base_f = 12345.67;
    struct flintdb_decimal  base_d = {0};
    if (flintdb_decimal_from_string("12345.67", 2, &base_d) < 0)
        THROW(&e, "decimal_from_string failed");
    src->i64_set(src, 0, base_i, &e);
    src->f64_set(src, 1, base_f, &e);
    src->decimal_set(src, 2, base_d, &e);
    src->string_set(src, 3, "hello binary", &e);
    src->date_set(src, 4, 1609459200, &e); // 2021-01-01
    src->time_set(src, 5, 3600, &e);       // 01:00:00
    if (e) {
        src->free(src);
        THROW_S(e);
    }

    int encoded = f.encode(&f, src, b, &e);
    src->free(src);
    if (encoded < 0 || e)
        THROW_S(e);

    // Decode once for correctness
    struct flintdb_row *out = flintdb_row_new(&m, &e);
    if (e)
        THROW_S(e);
    b->position = 0;
    int decoded = f.decode(&f, b, out, &e);
    if (decoded < 0 || e) {
        out->free(out);
        THROW_S(e);
    }
    // Light checks
    if (out->i64_get(out, 0, &e) != base_i) {
        out->free(out);
        THROW(&e, "decode check failed: i64");
    }
    if (fabs(out->f64_get(out, 1, &e) - base_f) > 1e-9) {
        out->free(out);
        THROW(&e, "decode check failed: f64");
    }
    struct flintdb_decimal  gotd = out->decimal_get(out, 2, &e);
    char d1[64], d2[64];
    flintdb_decimal_to_string(&base_d, d1, sizeof d1);
    flintdb_decimal_to_string(&gotd, d2, sizeof d2);
    if (strncmp(d1, d2, sizeof d1) != 0) {
        out->free(out);
        THROW(&e, "decode check failed: decimal");
    }

    // Timed loop: repeatedly decode from the same buffer
    size_t total_bytes = 0;
    STOPWATCH_START(watch);
    for (i64 i = 0; i < max; i++) {
        b->position = 0; // reset for read
        int ok = f.decode(&f, b, out, &e);
        if (ok < 0 || e)
            THROW_S(e);
        total_bytes += (size_t)b->limit; // consumed per decode
    }

    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%lldrows, %s, %.0fops, total_bytes=%zu\n", max, tbuf, time_ops(max, &watch), total_bytes);

    out->free(out);
    b->free(b);
    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    return -1;
}

#endif // TESTCASE_PERF_BIN_DECODE

#ifdef TESTCASE_PERF_VARIANT_COMPARE
// ./testcase.sh TESTCASE_PERF_VARIANT_COMPARE
int main(int argc, char **argv) {
    printf("target macos TESTCASE_PERF_VARIANT_COMPARE\n");
    char *e = NULL;
    i64 max = 1024LL * 1024LL * 10LL; // 10M compares

    // Prepare a set of variant pairs to exercise compare fast paths and mixed paths
    enum { K = 10 };
    struct flintdb_variant A[K];
    struct flintdb_variant B[K];
    for (int i = 0; i < K; i++) {
        flintdb_variant_init(&A[i]);
        flintdb_variant_init(&B[i]);
    }

    // 0) INT64 vs INT64
    flintdb_variant_i64_set(&A[0], 123456789LL);
    flintdb_variant_i64_set(&B[0], 123456790LL);

    // 1) DOUBLE vs DOUBLE
    flintdb_variant_f64_set(&A[1], 12345.67);
    flintdb_variant_f64_set(&B[1], 12345.68);

    // 2) STRING vs STRING (diff at tail)
    const char *sa = "abcdef";
    const char *sb = "abcdeg";
    flintdb_variant_string_set(&A[2], sa, (u32)strlen(sa));
    flintdb_variant_string_set(&B[2], sb, (u32)strlen(sb));

    // 3) BYTES vs BYTES (binary cmp)
    const unsigned char ba[] = {0x00, 0x10, 0x20, 0x30};
    const unsigned char bb[] = {0x00, 0x10, 0x20, 0x31};
    flintdb_variant_bytes_set(&A[3], (const char *)ba, (u32)sizeof(ba));
    flintdb_variant_bytes_set(&B[3], (const char *)bb, (u32)sizeof(bb));

    // 4) DECIMAL vs DECIMAL (same scale)
    struct flintdb_decimal  d4a = {0}, d4b = {0};
    flintdb_decimal_from_string("12345.67", 2, &d4a);
    flintdb_decimal_from_string("12345.68", 2, &d4b);
    flintdb_variant_decimal_set(&A[4], d4a.sign, d4a.scale, d4a);
    flintdb_variant_decimal_set(&B[4], d4b.sign, d4b.scale, d4b);

    // 5) DECIMAL vs DECIMAL (different scale -> compare by sign,scale,length,data)
    struct flintdb_decimal  d5a = {0}, d5b = {0};
    flintdb_decimal_from_string("12345.6", 1, &d5a);
    flintdb_decimal_from_string("12345.60", 2, &d5b);
    flintdb_variant_decimal_set(&A[5], d5a.sign, d5a.scale, d5a);
    flintdb_variant_decimal_set(&B[5], d5b.sign, d5b.scale, d5b);

    // 6) DATE vs DATE
    time_t base = 1609459200; // 2021-01-01
    flintdb_variant_date_set(&A[6], base);
    flintdb_variant_date_set(&B[6], base + 86400);

    // 7) TIME vs TIME
    flintdb_variant_time_set(&A[7], 3600);
    flintdb_variant_time_set(&B[7], 7200);

    // 8) Mixed numeric: INT64 vs DOUBLE (numeric coercion path)
    flintdb_variant_i64_set(&A[8], 100);
    flintdb_variant_f64_set(&B[8], 100.5);

    // 9) Mixed NIL and STRING (NIL ordering path)
    flintdb_variant_null_set(&A[9]);
    const char *sx = "x";
    flintdb_variant_string_set(&B[9], sx, (u32)strlen(sx));

    // Timed compare loop
    volatile long long sink = 0; // prevent optimization
    STOPWATCH_START(watch);
    for (i64 i = 0; i < max; i++) {
        int idx = (int)(i % K);
        sink += (long long)flintdb_variant_compare(&A[idx], &B[idx]);
    }
    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%lld compares, %s, %.0fops, checksum=%lld\n", max, tbuf, time_ops(max, &watch), sink);

    // cleanup
    for (int i = 0; i < K; i++) {
        flintdb_variant_free(&A[i]);
        flintdb_variant_free(&B[i]);
    }
    PRINT_MEMORY_LEAK_INFO();
    return 0;
}

#endif // TESTCASE_PERF_VARIANT_COMPARE

#ifdef TESTCASE_PERF_LRUCACHE
// ./testcase.sh TESTCASE_PERF_LRUCACHE

typedef struct perf_item {
    i64 id;
    i64 pad;
} perf_item;

static void perf_item_dealloc(keytype k, valtype v) {
    (void)k;
    perf_item *p = (perf_item *)(var)v;
    if (p)
        FREE(p);
}

int main(int argc, char **argv) {
    // Parameters:
    //  N: inserts (default 1,000,000)
    //  M: random gets (default 1,000,000)
    //  move_on_get: 1 to move MRU on get (default 1), 0 to keep insertion order for faster gets
    //  buckets: hashsize (default 131072*8)
    //  capacity: LRU max size (default 1024*1024)
    i64 N = 1000000;
    i64 M = 1000000;
    int move_on_get = 1;
    u32 buckets = 131072 * 8;
    u32 capacity = 1024 * 1024;
    if (argc >= 2) {
        i64 t = atoll(argv[1]);
        if (t > 0)
            N = t;
    }
    if (argc >= 3) {
        i64 t = atoll(argv[2]);
        if (t > 0)
            M = t;
    }
    if (argc >= 4) {
        int t = atoi(argv[3]);
        if (t == 0 || t == 1)
            move_on_get = t;
    }
    if (argc >= 5) {
        long long t = atoll(argv[4]);
        if (t > 0)
            buckets = (u32)t;
    }
    if (argc >= 6) {
        long long t = atoll(argv[5]);
        if (t > 0)
            capacity = (u32)t;
    }

    // Using flat open-addressing backend
    struct hashmap *cache = NULL;
    cache = lruhashmap_new(buckets, capacity, &hashmap_int_hash, &hashmap_int_cmpr);
    fprintf(stderr, "LRUCACHE backend: flat (open-addressing)\n");
    assert(cache != NULL);

    // Inserts
    STOPWATCH_START(w_insert);
    for (i64 i = 0; i < N; i++) {
        perf_item *it = (perf_item *)CALLOC(1, sizeof(perf_item));
        it->id = i;
        cache->put(cache, (keytype)i, (valtype)(var)it, perf_item_dealloc);
    }
    u64 ms_ins = time_elapsed(&w_insert);
    char d1[64];
    time_dur(ms_ins, d1, sizeof(d1));
    printf("LRUCACHE insert: %lld items, %s, %.0f ops/sec\n", (long long)N, d1, time_ops(N, &w_insert));

    // Random gets (hits)
    srand(42);
    STOPWATCH_START(w_get);
    i64 hits = 0;
    for (i64 i = 0; i < M; i++) {
        i64 k = (i64)(rand() % (int)(N > 0 ? N : 1));
        valtype v = cache->get(cache, (keytype)k);
        if (v != HASHMAP_INVALID_VAL)
            hits++;
    }
    u64 ms_get = time_elapsed(&w_get);
    char d2[64];
    time_dur(ms_get, d2, sizeof(d2));
    printf("LRUCACHE get(hit): %lld ops, %s, %.0f ops/sec, hit=%lld\n",
           (long long)M, d2, time_ops(M, &w_get), (long long)hits);

    cache->free(cache);
    PRINT_MEMORY_LEAK_INFO();
    return 0;
}
#endif // TESTCASE_PERF_LRUCACHE


#ifdef TESTCASE_FLINTDB_TPCH_LINEITEM_WRITE
// ./testcase.sh TESTCASE_FLINTDB_TPCH_LINEITEM_WRITE

int main(int argc, char **argv) {
    char *e = NULL;

    // i64 max = 1024 * 1024 * 10;
    i64 max = 16384;

    struct flintdb_table *t = NULL;
    struct flintdb_meta meta = {
        0,
    };
    struct flintdb_genericfile *f = NULL;

    const char *ddl = "CREATE TABLE tpch_lineitem ( "
                      "l_orderkey    UINT, "
                      "l_partkey     UINT, "
                      "l_suppkey     UINT16, "
                      "l_linenumber  UINT8, "
                      "l_quantity    DECIMAL(4,2), "
                      "l_extendedprice  DECIMAL(4,2), "
                      "l_discount    DECIMAL(4,2), "
                      "l_tax         DECIMAL(4,2), "
                      "l_returnflag  STRING(1), "
                      "l_linestatus  STRING(1), "
                      "l_shipDATE    DATE, "
                      "l_commitDATE  DATE, "
                      "l_receiptDATE DATE, "
                      "l_shipinstruct STRING(25), "
                      "l_shipmode     STRING(10), "
                      "l_comment      STRING(44), "
                      " "
                      "PRIMARY KEY (l_orderkey, l_linenumber) "
                      ") WAL=COMPRESS";

    struct flintdb_sql *q = NULL;
    q = flintdb_sql_parse(ddl, &e);
    if (e || !q)
        THROW_S(e);
    if (flintdb_sql_to_meta(q, &meta, &e) < 0)
        THROW_S(e);
    flintdb_sql_free(q);

    flintdb_table_drop("../c/temp/c/tpch_lineitem"TABLE_NAME_SUFFIX, &e);

    t = flintdb_table_open("../c/temp/c/tpch_lineitem"TABLE_NAME_SUFFIX, FLINTDB_RDWR, &meta, &e);
    if (e || !t)
        THROW(&e, "table_open failed: %s", e ? e : "unknown error");

    f = flintdb_genericfile_open("../c/temp/tpch/lineitem.tbl.gz", FLINTDB_RDONLY, NULL, &e);
    if (e || !f)
        THROW(&e, "genericfile_open failed: %s", e ? e : "unknown error");

    // Iterate all rows from source file and insert into the table
    struct flintdb_cursor_row *cur = f->find(f, NULL, &e);
    if (e || !cur)
        THROW(&e, "find cursor failed: %s", e ? e : "unknown error");

    STOPWATCH_START(watch);
    i64 rows = 0;

    // Pre-allocate reusable destination row to eliminate 6M malloc/free calls
    // This is the key optimization: avoid row_cast's CALLOC on every iteration
    struct flintdb_row *dst = flintdb_row_new(&meta, &e);
    if (e || !dst)
        THROW(&e, "failed to allocate reusable row");

    for (struct flintdb_row *r; (r = cur->next(cur, &e)) != NULL;) {
        if (e) {
            r->free(r);
            break;
        }

        // Copy/cast values from source to destination row
        // Using row->set() for proper type conversion
        // for (int col = 0; col < meta.columns.length && col < r->length; col++) {
        //     dst->set(dst, col, &r->array[col], &e);
        //     if (e) { r->free(r); goto EXCEPTION; }
        // }
        // // Reset rowid for insert (not update)
        // dst->rowid = -1;

        flintdb_row_cast_reuse(r, dst, &e);
        if (e) {
            r->free(r);
            goto EXCEPTION;
        }

        i64 rid = t->apply(t, dst, 1, &e);
        r->free(r);
        if (e || rid < 0)
            break;

        rows++;
        // Optional progress log for large imports
        // if ((rows % 500000) == 0) fprintf(stderr, "imported %lld rows...\n", rows);
        if (--max < 1)
            break;
    }

    if (dst)
        dst->free(dst);
    cur->close(cur);
    if (e)
        THROW_S(e);
    TRACE("tpch_lineitem imported rows: %lld", (long long)rows);

    t->close(t);
    TRACE("table closed");
    f->close(f);
    TRACE("file closed");
    // Don't close local meta - it's just a template, actual metas are owned by table/file
    plugin_manager_cleanup();
    TRACE("plugins cleaned up");

    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%'lldrows, %s, %'.0fops\n", rows, tbuf, time_ops(rows, &watch));

    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    if (t)
        t->close(t);
    if (f)
        f->close(f);
    // Don't close local meta - it's just a template
    return -1;
}
#endif

#ifdef TESTCASE_FLINTDB_TPCH_LINEITEM_READ
// ./testcase.sh TESTCASE_FLINTDB_TPCH_LINEITEM_READ

int main(int argc, char **argv) {
    char *e = NULL;

    struct flintdb_table *t = NULL;

    t = flintdb_table_open("../c/temp/c/tpch_lineitem"TABLE_NAME_SUFFIX, FLINTDB_RDONLY, NULL, &e);
    if (e || !t)
        THROW(&e, "table_open failed: %s", e ? e : "unknown error");

    i64 nrows = t->rows(t, &e);
    if (e)
        THROW_S(e);
    LOG("Total rows in table: %lld", (long long)nrows);

    // Test: read first few rows to see orderkey values
    LOG("Reading first 5 rows to check orderkey values...");
    for (i64 i = 0; i < 5 && i < nrows; i++) {
        const struct flintdb_row *r = t->read(t, i, &e);
        if (r) {
            struct flintdb_variant *orderkey = r->get(r, 0, &e);
            LOG("row[%lld]: l_orderkey = %u", i, flintdb_variant_u32_get(orderkey, &e));
        }
    }

    STOPWATCH_START(watch);

    // Read some sample rows and validate known values
    const char *q = ""; //"WHERE l_orderkey >= 3 AND l_orderkey <= 5 LIMIT 10";
    LOG("Query: %s", q);
    struct flintdb_cursor_i64 *c = t->find(t, q, &e);
    if (e)
        THROW(&e, "find failed: %s", e);
    if (!c)
        THROW(&e, "find returned NULL cursor");

    LOG("Reading rows...");
    i64 crows = 0;
    for (i64 i; (i = c->next(c, &e)) >= 0;) {
        const struct flintdb_row *r = t->read(t, i, &e);
        // if (e || !r)
        //     THROW_S(e);
        // flintdb_print_row(r);
        // LOG("------------------------");

        // if (e)
        //     THROW_S(e);
        crows++;
    }
    LOG("Finished reading rows.");
    c->close(c);

    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%'lldrows, %s, %'.0fops\n", crows, tbuf, time_ops(crows, &watch));
    LOG("query rows: %lld", (long long)crows);
    LOG("table rows: %lld", (long long)nrows);

    t->close(t);
    PRINT_MEMORY_LEAK_INFO();
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    if (t)
        t->close(t);
    return -1;
}
#endif


#ifdef TESTCASE_MULTI_THREADS
// ./testcase.sh TESTCASE_MULTI_THREADS

struct thread_info {
	pthread_t thread_id;
    int thread_num;
    struct flintdb_table *tbl;
};

static void * thread_writer_run(void *arg) {
    struct thread_info *tinfo = arg;
    struct flintdb_table *tbl = tinfo->tbl;
    struct flintdb_meta *mt = NULL;
    struct flintdb_transaction *tx = NULL;
    char *e = NULL;

    mt = (struct flintdb_meta *) tbl->meta(tbl, NULL);
    // 1) Commit path: begin -> apply(2 rows) -> commit
    tx = flintdb_transaction_begin(tbl, &e);
    if (e) THROW_S(e);
    if (!tx) THROW(&e, "transaction_begin failed");

    int customer_id = tinfo->thread_num + 1;
    TRACE("thread %d: inserting customer_id=%d", tinfo->thread_num, customer_id);
    
    struct flintdb_row *r = flintdb_row_new(mt, &e);
    if (e) THROW_S(e);
    r->i64_set(r, 0, customer_id, &e);
    if (e) THROW_S(e);
    char name[64];
    snprintf(name, sizeof(name), "Name-%d", customer_id);
    r->string_set(r, 1, name, &e);
    if (e) THROW_S(e);

    i64 rowid = tx->apply(tx, r, 1, &e);
    if (e) THROW_S(e);
    if (rowid < 0) THROW(&e, "tx apply failed");
    TRACE("tx apply: customer_id=%d => rowid=%lld", customer_id, rowid);
    r->free(r);

    tx->commit(tx, &e);
    if (e) THROW_S(e);
    tx->close(tx);
    tx = NULL;

    return NULL;

EXCEPTION:
    if (e) WARN("EXC: %s", e);
    if (tx) tx->close(tx);
    return NULL;
}

static void * thread_reader_run(void *arg) {
    struct thread_info *tinfo = arg;
    struct flintdb_table *tbl = tinfo->tbl;
    struct flintdb_cursor_i64 *cursor = NULL;
    i64 rowid = -1;
    char *e = NULL;

    TRACE("thread %d: reading rows", tinfo->thread_num);
    for (int i = 1; i <= 100; i++) {
        cursor = tbl->find(tbl, "USE INDEX(PRIMARY DESC) LIMIT 1", &e);
        if (e) THROW_S(e);
        while((rowid = cursor->next(cursor, &e)) >= 0) {
            const struct flintdb_row *r = tbl->read(tbl, rowid, &e);
            if (e) {
                WARN("thread %d: read failed for rowid=%lld: %s", tinfo->thread_num, rowid, e);
                break;
            }
            i64 customer_id = r->i64_get(r, 0, &e);
            const char *customer_name = r->string_get(r, 1, &e);
            if (i == 100) { // Only trace the last iteration
                TRACE("thread %d: read rowid=%lld => customer_id=%lld, customer_name=%s",
                      tinfo->thread_num, rowid, customer_id, customer_name);
            }
        }
        if (cursor) cursor->close(cursor);
        cursor = NULL;
    }

    if (cursor) cursor->close(cursor);

    return NULL;

EXCEPTION:
    if (e) WARN("EXC: %s", e);
    if (cursor) cursor->close(cursor);
    return NULL;
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    char *e = NULL;
    struct flintdb_table *tbl = NULL;
    struct flintdb_transaction *tx = NULL;
	struct thread_info tinfo[] = {
        {0, 0},
        {0, 1},
        {0, 2},
        {0, 3},
    };
    size_t num_threads = sizeof(tinfo) / sizeof(tinfo[0]);


    const char *tablename = "temp/tx_test"TABLE_NAME_SUFFIX;
    
    struct flintdb_meta mt = flintdb_meta_new("tx_test"TABLE_NAME_SUFFIX, &e);
    // NOTE: meta.wal is empty by default, which disables WAL (WAL_NONE).
    // For this testcase, we need WAL enabled so rollback is meaningful.
    strncpy(mt.wal, WAL_OPT_LOG, sizeof(mt.wal) - 1);
    flintdb_meta_columns_add(&mt, "customer_id", VARIANT_INT64, 0, 0, SPEC_NULLABLE, "0", "int64 primary key", &e);
    flintdb_meta_columns_add(&mt, "customer_name", VARIANT_STRING, 255, 0, SPEC_NULLABLE, "0", "", &e);

    char keys_arr[1][MAX_COLUMN_NAME_LIMIT] = {"customer_id"};
    flintdb_meta_indexes_add(&mt, PRIMARY_NAME, NULL, (const char (*)[MAX_COLUMN_NAME_LIMIT])keys_arr, 1, &e);
    if (e) THROW_S(e);
    
    flintdb_meta_wal_set(&mt, WAL_OPT_TRUNCATE, 0, 0, 0, 0, 0, 0, &e);
    if (e) THROW_S(e);

    flintdb_table_drop(tablename, NULL); // ignore error

    tbl = flintdb_table_open(tablename, FLINTDB_RDWR, &mt, &e);
    if (e) THROW_S(e);
    if (!tbl) THROW(&e, "table_open failed");

    // THREAD
    for(int i=0; i<num_threads; i++)
        tinfo[i].tbl = tbl;
    pthread_create(&tinfo[0].thread_id, NULL, &thread_writer_run, &tinfo[0]);
    pthread_create(&tinfo[1].thread_id, NULL, &thread_writer_run, &tinfo[1]);
    pthread_create(&tinfo[2].thread_id, NULL, &thread_reader_run, &tinfo[2]);
    pthread_create(&tinfo[3].thread_id, NULL, &thread_reader_run, &tinfo[3]);

    for(int i=0; i<num_threads; i++)
        pthread_join(tinfo[i].thread_id, NULL);
    // END THREAD


    i64 rows = tbl->rows(tbl, &e);
    if (e) THROW_S(e);
    LOG("rows after commit=%lld", rows);
    assert(rows == 2);

    LOG("before one(customer_id=1)");

    const char *argv1[] = {"customer_id", "1"};
    const struct flintdb_row *r1 = tbl->one(tbl, 0, 2, argv1, &e);
    if (e) THROW_S(e);
    assert(r1);
    assert(strcmp(r1->string_get(r1, 1, &e), "Name-1") == 0);
    if (e) THROW_S(e);

    LOG("after one(customer_id=1)");

    // 2) Rollback path: begin -> apply(1 row) -> rollback
    LOG("before begin #2");
    tx = flintdb_transaction_begin(tbl, &e);
    if (e) THROW_S(e);
    if (!tx) THROW(&e, "transaction_begin failed");

    {
        struct flintdb_row *r = flintdb_row_new(&mt, &e);
        if (e) THROW_S(e);
        r->i64_set(r, 0, 3, &e);
        if (e) THROW_S(e);
        r->string_set(r, 1, "Name-3", &e);
        if (e) THROW_S(e);
        (void)tx->apply(tx, r, 1, &e);
        if (e) THROW_S(e);
        r->free(r);
    }

    tx->rollback(tx, &e);
    if (e) THROW_S(e);
    tx->close(tx);
    tx = NULL;

    LOG("after rollback #2");

    rows = tbl->rows(tbl, &e);
    if (e) THROW_S(e);
    LOG("rows after rollback=%lld", rows);
    assert(rows == 2);

    const char *argv3[] = {"customer_id", "3"};
    const struct flintdb_row *r3 = tbl->one(tbl, 0, 2, argv3, &e);
    if (e) THROW_S(e);
    assert(r3 == NULL);

EXCEPTION:
    if (e) WARN("EXC: %s", e);
    if (tx) tx->close(tx);
    if (tbl) tbl->close(tbl);
    flintdb_meta_close(&mt);

    PRINT_MEMORY_LEAK_INFO();
    return 0;
}

#endif // TESTCASE_MULTI_THREADS


#ifdef TESTCASE_SQLITE_TPCH_LINEITEM_WRITE
// ./testcase.sh TESTCASE_SQLITE_TPCH_LINEITEM_WRITE
#include <sqlite3.h>

// Convert DECIMAL to C string (scale-aware) for SQLite TEXT bind
static int tc_decimal_to_cstr(const struct flintdb_decimal  *d, char *out, size_t cap) {
    if (!out || cap == 0)
        return -1;
    out[0] = '\0';
    if (!d || d->length <= 0)
        return 0;
    // Build digits from BCD bytes
    char tmp[128];
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
    // Insert decimal point according to scale
    if (d->scale > 0 && tp > 0) {
        int digits = tp - (d->sign ? 1 : 0);
        if (d->scale >= digits) {
            // 0.(zeros)digits
            int op = 0;
            if (d->sign && op < (int)cap)
                out[op++] = '-';
            if (op < (int)cap)
                out[op++] = '0';
            if (op < (int)cap)
                out[op++] = '.';
            int z = (int)d->scale - digits;
            for (int i = 0; i < z && op < (int)cap; i++)
                out[op++] = '0';
            int start = (d->sign ? 1 : 0);
            for (int i = start; i < tp && op < (int)cap; i++)
                out[op++] = tmp[i];
            if (op >= (int)cap)
                op = (int)cap - 1;
            out[op] = '\0';
            return 0;
        } else {
            int point = tp - (int)d->scale;
            int op = 0;
            for (int i = 0; i < tp && op < (int)cap - 1; i++) {
                if (i == point && op < (int)cap - 1)
                    out[op++] = '.';
                out[op++] = tmp[i];
            }
            out[op] = '\0';
            return 0;
        }
    }
    // Integer-like
    int n = (tp < (int)cap - 1) ? tp : (int)cap - 1;
    memcpy(out, tmp, (size_t)n);
    out[n] = '\0';
    return 0;
}

static void tc_date_to_yyyy_mm_dd(time_t t, char *out, size_t cap) {
    if (!out || cap == 0)
        return;
    struct tm tmv;
    localtime_r(&t, &tmv);
    // normalize to date only
    tmv.tm_hour = 0;
    tmv.tm_min = 0;
    tmv.tm_sec = 0;
    tmv.tm_isdst = -1;
    // strftime is locale-safe for this format
    strftime(out, cap, "%Y-%m-%d", &tmv);
}

int main(int argc, char **argv) {
    char *e = NULL;

    i64 max = 1024 * 1024 * 10;
    struct flintdb_genericfile *f = NULL;
    sqlite3 *db = NULL;

    mkdirs("../c/temp/c", 0755);

    f = flintdb_genericfile_open("../c/temp/tpch/lineitem.tbl.gz", FLINTDB_RDONLY, NULL, &e);
    if (e || !f)
        THROW(&e, "genericfile_open failed: %s", e ? e : "unknown error");

    // Iterate all rows from source file and insert into the table
    struct flintdb_cursor_row *cur = f->find(f, NULL, &e);
    if (e || !cur)
        THROW(&e, "find cursor failed: %s", e ? e : "unknown error");

    unlink("../c/temp/c/tpch_lineitem.sqlite"); // remove existing

    if (sqlite3_open("../c/temp/c/tpch_lineitem.sqlite", &db) != SQLITE_OK) {
        THROW(&e, "sqlite3_open failed: %s", sqlite3_errmsg(db));
    }

    const char *create_table_sql = "CREATE TABLE IF NOT EXISTS tpch_lineitem (\n"
                                   "l_orderkey INTEGER,\n"
                                   "l_partkey INTEGER,\n"
                                   "l_suppkey INTEGER,\n"
                                   "l_linenumber INTEGER,\n"
                                   "l_quantity DECIMAL(4,2),\n"
                                   "l_extendedprice DECIMAL(4,2),\n"
                                   "l_discount DECIMAL(4,2),\n"
                                   "l_tax DECIMAL(4,2),\n"
                                   "l_returnflag TEXT,\n"
                                   "l_linestatus TEXT,\n"
                                   "l_shipDATE TEXT,\n"
                                   "l_commitDATE TEXT,\n"
                                   "l_receiptDATE TEXT,\n"
                                   "l_shipinstruct TEXT,\n"
                                   "l_shipmode TEXT,\n"
                                   "l_comment TEXT,\n"
                                   "PRIMARY KEY (l_orderkey, l_linenumber)\n"
                                   ");";
    char *sql = "INSERT INTO tpch_lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipDATE, l_commitDATE, l_receiptDATE, l_shipinstruct, l_shipmode, l_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    if (sqlite3_exec(db, create_table_sql, 0, 0, NULL) != SQLITE_OK) {
        THROW(&e, "Failed to create table: %s", sqlite3_errmsg(db));
    }
    // Speed up SQLite bulk insert
    	// 컴파일 시 아래와 같은 매크로를 정의하여 journal_mode를 선택할 수 있습니다:
    	// -DJOURNAL_MODE_DELETE
    	// -DJOURNAL_MODE_WAL
    	// 기본값은 MEMORY 입니다.
    #if defined(JOURNAL_MODE_DELETE)
    	sqlite3_exec(db, "PRAGMA journal_mode=DELETE;", 0, 0, NULL);
    #elif defined(JOURNAL_MODE_WAL)
    	sqlite3_exec(db, "PRAGMA journal_mode=WAL;", 0, 0, NULL);
    #else // 테스트 케이스에서는 성능을 위해 기본적으로 MEMORY 사용
    	sqlite3_exec(db, "PRAGMA journal_mode=MEMORY;", 0, 0, NULL);
    #endif    
    sqlite3_exec(db, "PRAGMA synchronous=ON;", 0, 0, NULL);
    sqlite3_exec(db, "PRAGMA temp_store=MEMORY;", 0, 0, NULL);

    // Prepare INSERT statement once
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        THROW(&e, "sqlite3_prepare_v2 failed: %s", sqlite3_errmsg(db));
    }

    // Single large transaction for performance
    sqlite3_exec(db, "BEGIN IMMEDIATE;", 0, 0, NULL);

    STOPWATCH_START(watch);
    i64 rows = 0;

    for (struct flintdb_row *r; (r = cur->next(cur, &e)) != NULL;) {
        if (e) {
            r->free(r);
            break;
        }

        int rc = SQLITE_OK;

        // 1) Integers
        if (r->is_nil(r, 0, NULL))
            rc = sqlite3_bind_null(stmt, 1);
        else
            rc = sqlite3_bind_int64(stmt, 1, (sqlite3_int64)r->i64_get(r, 0, NULL));
        if (rc == SQLITE_OK) {
            if (r->is_nil(r, 1, NULL))
                rc = sqlite3_bind_null(stmt, 2);
            else
                rc = sqlite3_bind_int64(stmt, 2, (sqlite3_int64)r->i64_get(r, 1, NULL));
        }
        if (rc == SQLITE_OK) {
            if (r->is_nil(r, 2, NULL))
                rc = sqlite3_bind_null(stmt, 3);
            else
                rc = sqlite3_bind_int64(stmt, 3, (sqlite3_int64)r->i64_get(r, 2, NULL));
        }
        if (rc == SQLITE_OK) {
            if (r->is_nil(r, 3, NULL))
                rc = sqlite3_bind_null(stmt, 4);
            else
                rc = sqlite3_bind_int64(stmt, 4, (sqlite3_int64)r->i64_get(r, 3, NULL));
        }

        // 2) DECIMALs as TEXT to preserve exact scale
        char dbuf[64];
        if (rc == SQLITE_OK) {
            if (r->is_nil(r, 4, NULL))
                rc = sqlite3_bind_null(stmt, 5);
            else {
                struct flintdb_decimal  d = r->decimal_get(r, 4, NULL);
                tc_decimal_to_cstr(&d, dbuf, sizeof(dbuf));
                rc = sqlite3_bind_text(stmt, 5, dbuf, -1, SQLITE_TRANSIENT);
            }
        }
        if (rc == SQLITE_OK) {
            if (r->is_nil(r, 5, NULL))
                rc = sqlite3_bind_null(stmt, 6);
            else {
                struct flintdb_decimal  d = r->decimal_get(r, 5, NULL);
                tc_decimal_to_cstr(&d, dbuf, sizeof(dbuf));
                rc = sqlite3_bind_text(stmt, 6, dbuf, -1, SQLITE_TRANSIENT);
            }
        }
        if (rc == SQLITE_OK) {
            if (r->is_nil(r, 6, NULL))
                rc = sqlite3_bind_null(stmt, 7);
            else {
                struct flintdb_decimal  d = r->decimal_get(r, 6, NULL);
                tc_decimal_to_cstr(&d, dbuf, sizeof(dbuf));
                rc = sqlite3_bind_text(stmt, 7, dbuf, -1, SQLITE_TRANSIENT);
            }
        }
        if (rc == SQLITE_OK) {
            if (r->is_nil(r, 7, NULL))
                rc = sqlite3_bind_null(stmt, 8);
            else {
                struct flintdb_decimal  d = r->decimal_get(r, 7, NULL);
                tc_decimal_to_cstr(&d, dbuf, sizeof(dbuf));
                rc = sqlite3_bind_text(stmt, 8, dbuf, -1, SQLITE_TRANSIENT);
            }
        }

        // 3) Short TEXTs
        const char *sval = NULL;
        if (rc == SQLITE_OK) {
            sval = r->string_get(r, 8, NULL);
            rc = sval ? sqlite3_bind_text(stmt, 9, sval, -1, SQLITE_TRANSIENT) : sqlite3_bind_null(stmt, 9);
        }
        if (rc == SQLITE_OK) {
            sval = r->string_get(r, 9, NULL);
            rc = sval ? sqlite3_bind_text(stmt, 10, sval, -1, SQLITE_TRANSIENT) : sqlite3_bind_null(stmt, 10);
        }

        // 4) DATEs formatted as YYYY-MM-DD
        char datestr[16];
        if (rc == SQLITE_OK) {
            if (r->is_nil(r, 10, NULL))
                rc = sqlite3_bind_null(stmt, 11);
            else {
                tc_date_to_yyyy_mm_dd(r->date_get(r, 10, NULL), datestr, sizeof(datestr));
                rc = sqlite3_bind_text(stmt, 11, datestr, -1, SQLITE_TRANSIENT);
            }
        }
        if (rc == SQLITE_OK) {
            if (r->is_nil(r, 11, NULL))
                rc = sqlite3_bind_null(stmt, 12);
            else {
                tc_date_to_yyyy_mm_dd(r->date_get(r, 11, NULL), datestr, sizeof(datestr));
                rc = sqlite3_bind_text(stmt, 12, datestr, -1, SQLITE_TRANSIENT);
            }
        }
        if (rc == SQLITE_OK) {
            if (r->is_nil(r, 12, NULL))
                rc = sqlite3_bind_null(stmt, 13);
            else {
                tc_date_to_yyyy_mm_dd(r->date_get(r, 12, NULL), datestr, sizeof(datestr));
                rc = sqlite3_bind_text(stmt, 13, datestr, -1, SQLITE_TRANSIENT);
            }
        }

        // 5) Remaining TEXTs
        if (rc == SQLITE_OK) {
            sval = r->string_get(r, 13, NULL);
            rc = sval ? sqlite3_bind_text(stmt, 14, sval, -1, SQLITE_TRANSIENT) : sqlite3_bind_null(stmt, 14);
        }
        if (rc == SQLITE_OK) {
            sval = r->string_get(r, 14, NULL);
            rc = sval ? sqlite3_bind_text(stmt, 15, sval, -1, SQLITE_TRANSIENT) : sqlite3_bind_null(stmt, 15);
        }
        if (rc == SQLITE_OK) {
            sval = r->string_get(r, 15, NULL);
            rc = sval ? sqlite3_bind_text(stmt, 16, sval, -1, SQLITE_TRANSIENT) : sqlite3_bind_null(stmt, 16);
        }

        if (rc != SQLITE_OK) {
            r->free(r);
            THROW(&e, "sqlite3_bind failed (%d): %s", rc, sqlite3_errmsg(db));
        }

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            r->free(r);
            THROW(&e, "sqlite3_step failed (%d): %s", rc, sqlite3_errmsg(db));
        }
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        r->free(r);
        rows++;
        if (--max < 1)
            break;
    }

    // Commit and cleanup
    sqlite3_exec(db, "COMMIT;", 0, 0, NULL);
    if (stmt) {
        sqlite3_finalize(stmt);
        stmt = NULL;
    }

    char tbuf[64];
    time_dur(time_elapsed(&watch), tbuf, sizeof(tbuf));
    printf("%'lldrows, %s, %'.0fops\n", rows, tbuf, time_ops(rows, &watch));

    PRINT_MEMORY_LEAK_INFO();
    if (cur)
        cur->close(cur);
    f->close(f);
    sqlite3_close(db);
    return 0;

EXCEPTION:
    if (e)
        WARN("EXC: %s", e);
    sqlite3_exec(db, "ROLLBACK;", 0, 0, NULL);
    // finalize if prepared
    // Note: db or stmt may be NULL depending on failure point
    // Close cursor/file best-effort

    return -1;
}

#endif // TESTCASE_SQLITE_TPCH_LINEITEM_WRITE
