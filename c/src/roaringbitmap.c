// Java-compatible RoaringBitmap implementation in C
#include "roaringbitmap.h"
#include "buffer.h"
#include "allocator.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#define RB_MAGIC 0x52424D31 // 'RBM1'

// ---- helpers ----
static inline int rb_high16(int x) { return (x >> RB_KEY_BITS) & RB_LOW_MASK; }
static inline u16 rb_low16(int x) { return (u16)(x & RB_LOW_MASK); }
// static inline int min_int(int a, int b) { return a < b ? a : b; }
static inline int max_int(int a, int b) { return a > b ? a : b; }

static void container_free(rb_container *c) {
    if (!c)
        return;
    if (c->type == 'A') {
        if (c->u.a.values)
            FREE(c->u.a.values);
        c->u.a.values = NULL;
        c->u.a.size = c->u.a.cap = 0;
    } else if (c->type == 'B') {
        if (c->u.b.words)
            FREE(c->u.b.words);
        c->u.b.words = NULL;
        c->card = 0;
    }
    c->type = 0;
}

static void container_init_array(rb_container *c) {
    memset(c, 0, sizeof(*c));
    c->type = 'A';
    c->u.a.cap = 4;
    c->u.a.values = (u16 *)MALLOC(sizeof(u16) * c->u.a.cap);
    c->u.a.size = 0;
    c->card = 0;
}

static void container_init_bitmap(rb_container *c) {
    memset(c, 0, sizeof(*c));
    c->type = 'B';
    c->u.b.words = (u64 *)CALLOC(RB_BITMAP_WORDS, sizeof(u64));
    c->card = 0;
}

static rb_container container_clone(const rb_container *src) {
    rb_container out;
    memset(&out, 0, sizeof(out));
    out.type = src->type;
    out.card = src->card;
    if (src->type == 'A') {
        int n = src->u.a.size;
        out.u.a.cap = max_int(4, n);
        out.u.a.values = (u16 *)MALLOC(sizeof(u16) * out.u.a.cap);
        memcpy(out.u.a.values, src->u.a.values, sizeof(u16) * n);
        out.u.a.size = n;
    } else if (src->type == 'B') {
        out.u.b.words = (u64 *)MALLOC(sizeof(u64) * RB_BITMAP_WORDS);
        memcpy(out.u.b.words, src->u.b.words, sizeof(u64) * RB_BITMAP_WORDS);
    }
    return out;
}

static void array_ensure(rb_container *c, int cap) {
    if (c->u.a.cap >= cap)
        return;
    int n = c->u.a.cap * 2;
    if (n < cap)
        n = cap;
    c->u.a.values = (u16 *)REALLOC(c->u.a.values, sizeof(u16) * n);
    c->u.a.cap = n;
}

static int array_contains(const rb_container *c, u16 low) {
    int lo = 0, hi = c->u.a.size - 1;
    while (lo <= hi) {
        int mid = (lo + hi) >> 1;
        u16 v = c->u.a.values[mid];
        if (v == low)
            return 1;
        if (v < low)
            lo = mid + 1;
        else
            hi = mid - 1;
    }
    return 0;
}

static int array_add(rb_container *c, u16 low) {
    int lo = 0, hi = c->u.a.size - 1, ins = 0;
    while (lo <= hi) {
        int mid = (lo + hi) >> 1;
        u16 v = c->u.a.values[mid];
        if (v == low)
            return 0;
        if (v < low) {
            lo = mid + 1;
            ins = lo;
        } else {
            hi = mid - 1;
            ins = mid;
        }
    }
    array_ensure(c, c->u.a.size + 1);
    if (ins < c->u.a.size)
        memmove(&c->u.a.values[ins + 1], &c->u.a.values[ins], sizeof(u16) * (c->u.a.size - ins));
    c->u.a.values[ins] = low;
    c->u.a.size++;
    c->card = c->u.a.size;
    return 1;
}

static int array_add_range(rb_container *c, u16 s, u16 e) {
    int added = 0; // naive loop
    for (int v = (int)s & 0xFFFF; v < ((int)e & 0xFFFF); v++)
        added += array_add(c, (u16)v);
    return added;
}

static int array_remove(rb_container *c, u16 low) {
    int lo = 0, hi = c->u.a.size - 1;
    while (lo <= hi) {
        int mid = (lo + hi) >> 1;
        u16 v = c->u.a.values[mid];
        if (v == low) {
            if (mid < c->u.a.size - 1)
                memmove(&c->u.a.values[mid], &c->u.a.values[mid + 1], sizeof(u16) * (c->u.a.size - mid - 1));
            c->u.a.size--;
            c->card = c->u.a.size;
            return 1;
        }
        if (v < low)
            lo = mid + 1;
        else
            hi = mid - 1;
    }
    return 0;
}

static int array_rank(const rb_container *c, u16 low) {
    // number of elements <= low
    int lo = 0, hi = c->u.a.size;
    while (lo < hi) {
        int mid = (lo + hi) >> 1;
        if (c->u.a.values[mid] <= low)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

static u16 array_select(const rb_container *c, int idx) {
    if (idx < 0 || idx >= c->u.a.size)
        return 0; // caller must validate
    return c->u.a.values[idx];
}

static int bitmap_contains(const rb_container *c, u16 low) {
    int v = low & 0xFFFF;
    int w = v >> 6;
    int b = v & 63;
    return (c->u.b.words[w] & (1ULL << b)) != 0ULL;
}

static int bitmap_add(rb_container *c, u16 low) {
    int v = low & 0xFFFF;
    int w = v >> 6;
    u64 mask = 1ULL << (v & 63);
    u64 before = c->u.b.words[w];
    if ((before & mask) != 0ULL)
        return 0;
    c->u.b.words[w] = before | mask;
    c->card++;
    return 1;
}

static int bitmap_add_range(rb_container *c, u16 s, u16 e) {
    int added = 0;
    for (int v = (int)s & 0xFFFF; v < ((int)e & 0xFFFF); v++)
        added += bitmap_add(c, (u16)v);
    return added;
}

static int bitmap_remove(rb_container *c, u16 low) {
    int v = low & 0xFFFF;
    int w = v >> 6;
    u64 mask = 1ULL << (v & 63);
    u64 before = c->u.b.words[w];
    if ((before & mask) == 0ULL)
        return 0;
    c->u.b.words[w] = before & ~mask;
    c->card--;
    return 1;
}

static int bitmap_rank(const rb_container *c, u16 low) {
    int v = low & 0xFFFF;
    int wi = v >> 6;
    int sum = 0;
    for (int i = 0; i < wi; i++)
        sum += __builtin_popcountll(c->u.b.words[i]);
    long long w = (long long)c->u.b.words[wi];
    int bits = v & 63;
    u64 mask = (bits == 63) ? ~0ULL : ((1ULL << (bits + 1)) - 1ULL);
    sum += __builtin_popcountll((u64)w & mask);
    return sum;
}

static u16 bitmap_select(const rb_container *c, int idx) {
    int acc = 0;
    for (int word = 0; word < RB_BITMAP_WORDS; word++) {
        int pc = __builtin_popcountll(c->u.b.words[word]);
        if (idx < acc + pc) {
            int within = idx - acc;
            u64 w = c->u.b.words[word];
            int count = 0;
            while (w) {
                u64 t = w & -w;
                int bit = __builtin_ctzll(t);
                if (count == within)
                    return (u16)((word << 6) + bit);
                w ^= t;
                count++;
            }
        }
        acc += pc;
    }
    return 0;
}

static void array_to_bitmap(rb_container *c) {
    rb_container out;
    container_init_bitmap(&out);
    for (int i = 0; i < c->u.a.size; i++)
        bitmap_add(&out, c->u.a.values[i]);
    container_free(c);
    *c = out;
}

static void bitmap_to_array(rb_container *c) {
    rb_container out;
    container_init_array(&out);
    // allocate capacity upfront
    array_ensure(&out, max_int(4, c->card));
    for (int word = 0; word < RB_BITMAP_WORDS; word++) {
        u64 w = c->u.b.words[word];
        int base = word << 6;
        while (w) {
            u64 t = w & -w;
            int bit = __builtin_ctzll(t);
            out.u.a.values[out.u.a.size++] = (u16)(base + bit);
            w ^= t;
        }
    }
    out.card = out.u.a.size;
    container_free(c);
    *c = out;
}

static void container_optimize(rb_container *c) {
    if (c->type == 'A' && c->u.a.size >= RB_ARRAY_TO_BITMAP_THRESHOLD) {
        array_to_bitmap(c);
    } else if (c->type == 'B' && c->card < RB_ARRAY_TO_BITMAP_THRESHOLD) {
        bitmap_to_array(c);
    }
}

// ---- roaringbitmap ----

static int entries_find(rb_entry *a, int n, int key) {
    int lo = 0, hi = n - 1;
    while (lo <= hi) {
        int mid = (lo + hi) >> 1;
        if (a[mid].key == key)
            return mid;
        if (a[mid].key < key)
            lo = mid + 1;
        else
            hi = mid - 1;
    }
    return -(lo + 1);
}

static void entries_insert(roaringbitmap *rb, int idx, int key, const rb_container *cinit) {
    if (rb->size + 1 > rb->cap) {
        int n = rb->cap ? rb->cap * 2 : 4;
        rb->entries = (rb_entry *)REALLOC(rb->entries, sizeof(rb_entry) * n);
        rb->cap = n;
    }
    if (idx < rb->size)
        memmove(&rb->entries[idx + 1], &rb->entries[idx], sizeof(rb_entry) * (rb->size - idx));
    rb->entries[idx].key = key;
    if (cinit)
        rb->entries[idx].c = *cinit;
    else
        memset(&rb->entries[idx].c, 0, sizeof(rb_container));
    rb->size++;
}

roaringbitmap *rbitmap_new(void) {
    roaringbitmap *rb = (roaringbitmap *)CALLOC(1, sizeof(roaringbitmap));
    return rb;
}

void rbitmap_clear(roaringbitmap *rb) {
    if (!rb)
        return;
    for (int i = 0; i < rb->size; i++)
        container_free(&rb->entries[i].c);
    FREE(rb->entries);
    rb->entries = NULL;
    rb->size = rb->cap = 0;
    rb->cardinality = 0;
}

void rbitmap_free(roaringbitmap *rb) {
    if (!rb)
        return;
    rbitmap_clear(rb);
    FREE(rb);
}

int rbitmap_cardinality(const roaringbitmap *rb) { return rb ? rb->cardinality : 0; }

i8 rbitmap_contains(const roaringbitmap *rb, int x) {
    if (!rb || x < 0)
        return 0;
    int key = rb_high16(x);
    u16 low = rb_low16(x);
    int idx = entries_find(rb->entries, rb->size, key);
    if (idx < 0)
        return 0;
    const rb_container *c = &rb->entries[idx].c;
    return (c->type == 'A') ? array_contains(c, low) : bitmap_contains(c, low);
}

void rbitmap_add(roaringbitmap *rb, int x) {
    if (!rb || x < 0)
        return;
    int key = rb_high16(x);
    u16 low = rb_low16(x);
    int idx = entries_find(rb->entries, rb->size, key);
    if (idx < 0) {
        idx = -idx - 1;
        rb_container c;
        container_init_array(&c);
        entries_insert(rb, idx, key, &c);
    }
    rb_container *c = &rb->entries[idx].c;
    int added = (c->type == 'A') ? array_add(c, low) : bitmap_add(c, low);
    if (added) {
        rb->cardinality++;
        container_optimize(c);
    }
}

void rbitmap_add_range(roaringbitmap *rb, int start_inclusive, int end_exclusive) {
    if (!rb)
        return;
    if (start_inclusive < 0 || end_exclusive < 0)
        return; // follow Java: negative unsupported
    if (end_exclusive <= start_inclusive)
        return;
    int sKey = rb_high16(start_inclusive);
    int eKey = rb_high16(end_exclusive - 1);
    for (int key = sKey; key <= eKey; key++) {
        int lowStart = (key == sKey) ? (start_inclusive & RB_LOW_MASK) : 0;
        int lowEndExclusive = (key == eKey) ? (((end_exclusive - 1) & RB_LOW_MASK) + 1) : (1 << RB_KEY_BITS);
        int idx = entries_find(rb->entries, rb->size, key);
        if (idx < 0) {
            idx = -idx - 1;
            // full container range
            if (lowStart == 0 && lowEndExclusive == (1 << RB_KEY_BITS)) {
                rb_container bc;
                container_init_bitmap(&bc);
                // fill all
                for (int i = 0; i < RB_BITMAP_WORDS; i++)
                    bc.u.b.words[i] = ~0ULL;
                bc.card = 1 << RB_KEY_BITS;
                entries_insert(rb, idx, key, &bc);
                rb->cardinality += bc.card;
                continue;
            }
            rb_container c;
            container_init_array(&c);
            entries_insert(rb, idx, key, &c);
        }
        rb_container *c = &rb->entries[idx].c;
        int added = (c->type == 'A') ? array_add_range(c, (u16)lowStart, (u16)lowEndExclusive)
                                     : bitmap_add_range(c, (u16)lowStart, (u16)lowEndExclusive);
        rb->cardinality += added;
        container_optimize(c);
    }
}

void rbitmap_remove(roaringbitmap *rb, int x) {
    if (!rb || x < 0)
        return;
    int key = rb_high16(x);
    u16 low = rb_low16(x);
    int idx = entries_find(rb->entries, rb->size, key);
    if (idx < 0)
        return;
    rb_container *c = &rb->entries[idx].c;
    int removed = (c->type == 'A') ? array_remove(c, low) : bitmap_remove(c, low);
    if (removed) {
        rb->cardinality--;
        if ((c->type == 'A' && c->u.a.size == 0) || (c->type == 'B' && c->card == 0)) {
            container_free(c);
            if (idx < rb->size - 1)
                memmove(&rb->entries[idx], &rb->entries[idx + 1], sizeof(rb_entry) * (rb->size - idx - 1));
            rb->size--;
        } else {
            container_optimize(c);
        }
    }
}

int rbitmap_rank(const roaringbitmap *rb, int x) {
    if (!rb || x < 0 || rb->cardinality == 0)
        return 0;
    int key = rb_high16(x);
    u16 low = rb_low16(x);
    int sum = 0;
    for (int i = 0; i < rb->size; i++) {
        if (rb->entries[i].key < key)
            sum += (rb->entries[i].c.type == 'A') ? rb->entries[i].c.u.a.size : rb->entries[i].c.card;
        else if (rb->entries[i].key == key) {
            const rb_container *c = &rb->entries[i].c;
            sum += (c->type == 'A') ? array_rank(c, low) : bitmap_rank(c, low);
            break;
        } else
            break;
    }
    return sum;
}

int rbitmap_select(const roaringbitmap *rb, int k, int *out) {
    if (!rb || k < 0 || k >= rb->cardinality)
        return -1;
    int remaining = k;
    for (int i = 0; i < rb->size; i++) {
        const rb_container *c = &rb->entries[i].c;
        int sz = (c->type == 'A') ? c->u.a.size : c->card;
        if (remaining < sz) {
            u16 low = (c->type == 'A') ? array_select(c, remaining) : bitmap_select(c, remaining);
            *out = (rb->entries[i].key << RB_KEY_BITS) | (low & RB_LOW_MASK);
            return 0;
        }
        remaining -= sz;
    }
    return -1;
}

// Simple set algebra
roaringbitmap *rbitmap_or(const roaringbitmap *a, const roaringbitmap *b) {
    if (!a || !b)
        return NULL;
    roaringbitmap *out = rbitmap_new();
    int i = 0, j = 0;
    while (i < a->size && j < b->size) {
        int ka = a->entries[i].key, kb = b->entries[j].key;
        if (ka < kb) {
            rb_container tmp = container_clone(&a->entries[i].c);
            entries_insert(out, out->size, ka, &tmp);
            out->cardinality += (a->entries[i].c.type == 'A') ? a->entries[i].c.u.a.size : a->entries[i].c.card;
            i++;
        } else if (kb < ka) {
            rb_container tmp = container_clone(&b->entries[j].c);
            entries_insert(out, out->size, kb, &tmp);
            out->cardinality += (b->entries[j].c.type == 'A') ? b->entries[j].c.u.a.size : b->entries[j].c.card;
            j++;
        } else {
            // same key: merge
            rb_container ca = container_clone(&a->entries[i].c);
            const rb_container *cb = &b->entries[j].c;
            // merge into ca
            if (ca.type == 'B' || cb->type == 'B') {
                if (ca.type != 'B')
                    array_to_bitmap(&ca);
                rb_container tmp = container_clone(cb);
                if (tmp.type != 'B')
                    array_to_bitmap(&tmp);
                int card = 0;
                for (int w = 0; w < RB_BITMAP_WORDS; w++) {
                    ca.u.b.words[w] |= tmp.u.b.words[w];
                    card += __builtin_popcountll(ca.u.b.words[w]);
                }
                ca.card = card;
                container_free(&tmp);
            } else {
                // both arrays: merge sorted
                rb_container outc;
                container_init_array(&outc);
                outc.u.a.cap = max_int(4, a->entries[i].c.u.a.size + b->entries[j].c.u.a.size);
                outc.u.a.values = (u16 *)REALLOC(outc.u.a.values, sizeof(u16) * outc.u.a.cap);
                int k = 0, p = 0, q = 0;
                while (p < a->entries[i].c.u.a.size && q < b->entries[j].c.u.a.size) {
                    u16 va = a->entries[i].c.u.a.values[p];
                    u16 vb = b->entries[j].c.u.a.values[q];
                    if (va == vb) {
                        outc.u.a.values[k++] = va;
                        p++;
                        q++;
                    } else if (va < vb) {
                        outc.u.a.values[k++] = va;
                        p++;
                    } else {
                        outc.u.a.values[k++] = vb;
                        q++;
                    }
                }
                while (p < a->entries[i].c.u.a.size)
                    outc.u.a.values[k++] = a->entries[i].c.u.a.values[p++];
                while (q < b->entries[j].c.u.a.size)
                    outc.u.a.values[k++] = b->entries[j].c.u.a.values[q++];
                outc.u.a.size = k;
                outc.card = k;
                container_free(&ca);
                ca = outc;
                container_optimize(&ca);
            }
            entries_insert(out, out->size, ka, &ca);
            out->cardinality += (ca.type == 'A') ? ca.u.a.size : ca.card;
            i++;
            j++;
        }
    }
    while (i < a->size) {
        rb_container tmp = container_clone(&a->entries[i].c);
        entries_insert(out, out->size, a->entries[i].key, &tmp);
        out->cardinality += (a->entries[i].c.type == 'A') ? a->entries[i].c.u.a.size : a->entries[i].c.card;
        i++;
    }
    while (j < b->size) {
        rb_container tmp = container_clone(&b->entries[j].c);
        entries_insert(out, out->size, b->entries[j].key, &tmp);
        out->cardinality += (b->entries[j].c.type == 'A') ? b->entries[j].c.u.a.size : b->entries[j].c.card;
        j++;
    }
    return out;
}

roaringbitmap *rbitmap_and(const roaringbitmap *a, const roaringbitmap *b) {
    if (!a || !b)
        return NULL;
    roaringbitmap *out = rbitmap_new();
    int i = 0, j = 0;
    while (i < a->size && j < b->size) {
        int ka = a->entries[i].key, kb = b->entries[j].key;
        if (ka < kb) {
            i++;
            continue;
        }
        if (kb < ka) {
            j++;
            continue;
        }
        // same key
        const rb_container *ac = &a->entries[i].c;
        const rb_container *bc = &b->entries[j].c;
        rb_container mc;
        if (ac->type == 'B' || bc->type == 'B') {
            rb_container A = container_clone(ac);
            if (A.type != 'B')
                array_to_bitmap(&A);
            rb_container B = container_clone(bc);
            if (B.type != 'B')
                array_to_bitmap(&B);
            container_init_bitmap(&mc);
            int card = 0;
            for (int w = 0; w < RB_BITMAP_WORDS; w++) {
                mc.u.b.words[w] = A.u.b.words[w] & B.u.b.words[w];
                card += __builtin_popcountll(mc.u.b.words[w]);
            }
            mc.card = card;
            container_free(&A);
            container_free(&B);
            container_optimize(&mc);
        } else {
            // both arrays
            container_init_array(&mc);
            int p = 0, q = 0;
            while (p < ac->u.a.size && q < bc->u.a.size) {
                u16 va = ac->u.a.values[p];
                u16 vb = bc->u.a.values[q];
                if (va == vb) {
                    array_add(&mc, va);
                    p++;
                    q++;
                } else if (va < vb)
                    p++;
                else
                    q++;
            }
        }
        int sz = (mc.type == 'A') ? mc.u.a.size : mc.card;
        if (sz > 0) {
            entries_insert(out, out->size, ka, &mc);
            out->cardinality += sz;
        } else
            container_free(&mc);
        i++;
        j++;
    }
    return out;
}

roaringbitmap *rbitmap_andnot(const roaringbitmap *a, const roaringbitmap *b) {
    if (!a || !b)
        return NULL;
    roaringbitmap *out = rbitmap_new();
    for (int i = 0; i < a->size; i++) {
        int key = a->entries[i].key;
        const rb_container *ac = &a->entries[i].c;
        int j = entries_find((rb_entry *)b->entries, b->size, key);
        rb_container res;
        if (j < 0) {
            res = container_clone(ac);
        } else {
            const rb_container *bc = &b->entries[j].c;
            if (ac->type == 'B' || bc->type == 'B') {
                rb_container A = container_clone(ac);
                if (A.type != 'B')
                    array_to_bitmap(&A);
                rb_container B = container_clone(bc);
                if (B.type != 'B')
                    array_to_bitmap(&B);
                container_init_bitmap(&res);
                int card = 0;
                for (int w = 0; w < RB_BITMAP_WORDS; w++) {
                    res.u.b.words[w] = A.u.b.words[w] & ~B.u.b.words[w];
                    card += __builtin_popcountll(res.u.b.words[w]);
                }
                res.card = card;
                container_free(&A);
                container_free(&B);
                container_optimize(&res);
            } else {
                container_init_array(&res);
                int p = 0, q = 0;
                while (p < ac->u.a.size && q < bc->u.a.size) {
                    u16 va = ac->u.a.values[p];
                    u16 vb = bc->u.a.values[q];
                    if (va == vb) {
                        p++;
                        q++;
                    } else if (va < vb) {
                        array_add(&res, va);
                        p++;
                    } else {
                        q++;
                    }
                }
                while (p < ac->u.a.size) {
                    array_add(&res, ac->u.a.values[p++]);
                }
            }
        }
        int sz = (res.type == 'A') ? res.u.a.size : res.card;
        if (sz > 0) {
            entries_insert(out, out->size, key, &res);
            out->cardinality += sz;
        } else
            container_free(&res);
    }
    return out;
}

// ---- Serialization ----

void rbitmap_write(const roaringbitmap *rb, struct buffer *out, char **e) {
    if (!rb || !out)
        return;
    out->i32_put(out, (i32)RB_MAGIC, e);
    out->i32_put(out, (i32)rb->size, e);
    for (int i = 0; i < rb->size; i++) {
        const rb_entry *en = &rb->entries[i];
        out->i32_put(out, (i32)en->key, e);
        // write container
        out->i8_put(out, (char)en->c.type, e);
        if (en->c.type == 'A') {
            out->i32_put(out, (i32)en->c.u.a.size, e);
            for (int k = 0; k < en->c.u.a.size; k++)
                out->i16_put(out, (i16)(en->c.u.a.values[k] & 0xFFFF), e);
        } else if (en->c.type == 'B') {
            out->i32_put(out, (i32)en->c.card, e);
            for (int w = 0; w < RB_BITMAP_WORDS; w++)
                out->i64_put(out, (i64)en->c.u.b.words[w], e);
        } else {
            // unknown
        }
    }
}

roaringbitmap *rbitmap_read(struct buffer *in, char **e) {
    if (!in)
        return NULL;
    i32 magic = in->i32_get(in, e);
    if (magic != (i32)RB_MAGIC)
        return NULL;
    i32 n = in->i32_get(in, e);
    roaringbitmap *rb = rbitmap_new();
    for (int i = 0; i < n; i++) {
        int key = (int)in->i32_get(in, e);
        char type = in->i8_get(in, e);
        rb_container c;
        memset(&c, 0, sizeof(c));
        c.type = (u8)type;
        if (type == 'A') {
            int size = (int)in->i32_get(in, e);
            container_init_array(&c);
            array_ensure(&c, max_int(4, size));
            for (int k = 0; k < size; k++) {
                u16 v = (u16)in->i16_get(in, e);
                c.u.a.values[c.u.a.size++] = v;
            }
            c.card = c.u.a.size;
            container_optimize(&c);
        } else if (type == 'B') {
            int card = (int)in->i32_get(in, e);
            container_init_bitmap(&c);
            for (int w = 0; w < RB_BITMAP_WORDS; w++)
                c.u.b.words[w] = (u64)in->i64_get(in, e);
            c.card = card;
            container_optimize(&c);
        } else {
            // unknown type; fail gracefully
            rbitmap_free(rb);
            return NULL;
        }
        entries_insert(rb, rb->size, key, &c);
        rb->cardinality += (c.type == 'A') ? c.u.a.size : c.card;
    }
    return rb;
}
