#ifndef FLINTDB_ROARINGBITMAP_H
#define FLINTDB_ROARINGBITMAP_H

#include "flintdb.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

struct buffer; // from bytebuffer.h

// RoaringBitmap C API compatible with Java serialization (RBM1)

#define RB_KEY_BITS 16
#define RB_LOW_MASK ((1 << RB_KEY_BITS) - 1)
#define RB_ARRAY_TO_BITMAP_THRESHOLD 4096
#define RB_BITMAP_WORDS (1 << (RB_KEY_BITS - 6)) // 65536/64 = 1024

typedef struct rb_container {
    // type: 'A' array, 'B' bitmap
    u8 type;
    int card; // cardinality for bitmap; for array equals size
    union {
        struct {
            u16 *values;
            int size;
            int cap;
        } a;
        struct {
            u64 *words; // length RB_BITMAP_WORDS
        } b;
    } u;
} rb_container;

typedef struct rb_entry {
    int key; // high 16 bits
    rb_container c;
} rb_entry;

typedef struct roaringbitmap {
    rb_entry *entries; // sorted by key
    int size;          // number of containers
    int cap;           // capacity of entries array
    int cardinality;   // total number of integers
} roaringbitmap;

// Lifecycle
roaringbitmap *rbitmap_new(void);
void rbitmap_free(roaringbitmap *rb);
void rbitmap_clear(roaringbitmap *rb);

// Basic ops
int  rbitmap_cardinality(const roaringbitmap *rb);
i8   rbitmap_contains(const roaringbitmap *rb, int x);
void rbitmap_add(roaringbitmap *rb, int x);
void rbitmap_add_range(roaringbitmap *rb, int start_inclusive, int end_exclusive);
void rbitmap_remove(roaringbitmap *rb, int x);
int  rbitmap_rank(const roaringbitmap *rb, int x); // count of elements <= x
int  rbitmap_select(const roaringbitmap *rb, int k, int *out); // 0 on ok, -1 on OOB

// Set algebra
roaringbitmap *rbitmap_or(const roaringbitmap *a, const roaringbitmap *b);
roaringbitmap *rbitmap_and(const roaringbitmap *a, const roaringbitmap *b);
roaringbitmap *rbitmap_andnot(const roaringbitmap *a, const roaringbitmap *b);

// Serialization (Java-compatible)
// Format: [int magic='RBM1'][int n][repeat n times: int key, (byte type, payload)]
void rbitmap_write(const roaringbitmap *rb, struct buffer *out, char **e);
roaringbitmap *rbitmap_read(struct buffer *in, char **e);

#ifdef __cplusplus
}
#endif

#endif // FLINTDB_ROARINGBITMAP_H