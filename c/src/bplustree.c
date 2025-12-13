#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "bplustree.h"
#include "runtime.h"
#include "allocator.h"



#define OFFSET_NULL -1L  // for node offset
#define KEY_NULL -1L     // for record offset

#define INTERNAL_MARK -2L
#define ROOT_SEEK_OFFSET 0L

#define NODE_BYTE_ALIGN 1024
#define STORAGE_HEAD_BYTES 16 // storage.h

#define LONG_BYTES 8
#define HEAD_BYTES (4 + LONG_BYTES) // bplustree head bytes
#define NODE_BYTES (NODE_BYTE_ALIGN - STORAGE_HEAD_BYTES)
#define KEY_BYTES LONG_BYTES
#define LINK_BYTES LONG_BYTES
#define LEAF_KEYS_MAX ((NODE_BYTES - (LINK_BYTES + LINK_BYTES)) / KEY_BYTES) // 
#define INTERNAL_KEYS_MAX (LEAF_KEYS_MAX / 2)

#define DEFAULT_INCREMENT_BYTES (1024 * 1024 * 16) // MB
#ifndef DEFAULT_BPLUSTREE_CACHE_LIMIT
#define DEFAULT_BPLUSTREE_CACHE_LIMIT (1024 * 1024 * 1)
#endif
#define DEFAULT_BPLUSTREE_CACHE_MIN (1024 * 256) // Do not allow too small capacity (구조적 제약)


enum node_type {
    NODE_INTERNAL = 0,
    NODE_LEAF = 1,
};


struct keyref {
    i64 offset;
    i64 left;  // left child offset
    i64 right; // right child offset
};

struct array_wrap {
    struct keyref *data;
    int length;
    int capacity;
};

static const struct keyref KEYREF_NULL = { .offset = OFFSET_NULL, .left = OFFSET_NULL, .right = OFFSET_NULL };

static inline void array_wrap_init(struct array_wrap *aw, struct keyref *data, int capacity) {
    aw->data = data;
    aw->length = 0;
    aw->capacity = capacity;
}

static inline void array_wrap_join(struct array_wrap *aw, const struct keyref *a, int alen, int offset, int d, struct keyref key) {
    int i = 0, j = 0;
    for (; i < (offset + (d < 0 ? 0 : 1)) && i < aw->capacity; i++, j++)
        aw->data[i] = a[j];
    if (i < aw->capacity) {
        aw->data[i++] = key;
    }
    for (; i < aw->capacity && j < alen; i++, j++)
        aw->data[i] = a[j];
    aw->length = i;
}

struct node {
    enum node_type  type; // 0: internal, 1: leaf
    i64 offset;
    int length;

    union {
        struct internal {
            struct keyref keys[INTERNAL_KEYS_MAX];
        } i;

        struct leaf {
            i64 left; // left sibling offset
            i64 right; // right sibling offset
            i64 keys[LEAF_KEYS_MAX];
        } l;
    } data;
};

struct position {
    int offset;
    int d; // direction: -1 left, 0 match, 1 right
};

struct context {
    struct context *p; // parent context
    struct node *n; // internal node
    struct position i;
};

#ifdef UNIT_TEST
static void print_keyrefs(const char *tag, struct keyref *k, int len) {
    printf("%s: ", tag);
    for(int i=0; i<len; i++) {
        struct keyref *x = &k[i];
        printf(" O:%lld L:%lld R:%lld |", x->offset, x->left, x->right);
        if (x->offset == KEY_NULL) {
            printf(" (SKIPPING KEY_NULL)");
            continue;
        }
        assert(x->offset != KEY_NULL);
        assert(x->left != OFFSET_NULL);
        assert(x->right != OFFSET_NULL);
        assert(x->left != x->right);
    }
    printf("\n");
}

static void print_keys(const char *tag, i64 *k, int len) {
    printf("%s: ", tag);
    for(int i=0; i<len; i++) {
        i64 x = k[i];
        printf(" %lld |", x);
        if (x == KEY_NULL) {
            printf(" (SKIPPING KEY_NULL)");
            continue;
        }
        assert(x != KEY_NULL);
    }
    printf("\n");
}   
#endif

static struct node * bplustree_node_read(struct bplustree *me, i64 offset, char **e);

static void bplustree_close(struct bplustree *me) {
    assert(me);
    if (!me->cache) return;

    // if (me->root) FREE(me->root); // do not free root, it's cached
    if (me->cache) me->cache->free(me->cache); // root will be freed
    if (me->header) me->header->free(me->header);

    if (me->storage) {
        me->storage->close(me->storage);
        FREE(me->storage); // must free here, storage->close does not free itself
    }

    me->storage = NULL;
    me->cache = NULL;
}


static inline u8 is_leaf(struct node *n) {
    assert(n);
    return n->type == NODE_LEAF;
}

static inline struct node* node_leaf_min(struct bplustree *me, struct node *start, char **e) {
    struct node *n = start;
    while(n && !is_leaf(n)) {
        struct keyref *k = &n->data.i.keys[0];
        n = bplustree_node_read(me, k->left, e);
    }
    return n;
}

static inline void node_init(struct node *n, enum node_type type, i64 offset) {
    assert(n);
    n->type = type;
    n->offset = offset;
    n->length = 0;
    if(type == NODE_LEAF) { // leaf
        n->data.l.left = OFFSET_NULL;
        n->data.l.right = OFFSET_NULL;
        memset(n->data.l.keys, 0xFF, sizeof n->data.l.keys); // -1L
    } else { // internal
        // for(int i=0; i<INTERNAL_KEYS_MAX; i++) {
        //     n->data.i.keys[i].offset = KEY_NULL;
        //     n->data.i.keys[i].left = OFFSET_NULL;
        //     n->data.i.keys[i].right = OFFSET_NULL;
        // }
        memset(n->data.i.keys, 0xFF, sizeof n->data.i.keys); // -1L
    }
}

static i64 bplustree_count_get(struct bplustree *me) {
    assert(me);
    return me->count;
}

static i64 bplustree_bytes_get(struct bplustree *me) {
    assert(me);
    return me->storage->bytes_get(me->storage);
}

static void bplustree_count_set(struct bplustree *me, i64 count) {
    assert(me);
    char *e = NULL;
    struct buffer h = {0};
    me->header->slice(me->header, 4, LONG_BYTES, &h, &e);
    h.i64_put(&h, count, &e);
    // do nothing because it's mmaped

    if (e && *e) {
        WARN("bplustree_count_set error: %s\n", e);
    }
}

static inline void bplustree_node_free(keytype k, valtype v) {
    struct node *n = (struct node*)v;
    if (n) FREE(n);
}

HOT_PATH
struct node * bplustree_node_decode(struct bplustree *me, i64 offset, char **e) {
    assert(me);
    assert(OFFSET_NULL != offset);

    struct buffer *mbb = me->storage->read(me->storage, offset, e);
    if (e && *e) return NULL;

    struct node *n = NULL;
    i64 mark = mbb->i64_get(mbb, e);
    if (INTERNAL_MARK == mark) { // INTERNAL
        // layout: MARK(-2) | LEFT(long) | (KEY.offset | KEY.right)* ... until buffer end
        i64 left = mbb->i64_get(mbb, e);
        if (e && *e) goto DONE;

        n = (struct node*)CALLOC(1, sizeof(struct node));
        node_init(n, NODE_INTERNAL, offset);

        int sz = 0;
        while(mbb->remaining(mbb) >= (KEY_BYTES * 2) && sz < INTERNAL_KEYS_MAX) {
            i64 ko = mbb->i64_get(mbb, e); // key (leaf offset reference)
            if (e && *e) goto DONE;
            i64 right = mbb->i64_get(mbb, e);
            if (e && *e) goto DONE;
            assert(ko > 0);
            n->data.i.keys[sz].offset = ko;
            n->data.i.keys[sz].left = left;
            n->data.i.keys[sz].right = right;
            left = right;
            sz++;
        }
        n->length = sz;
    } else { // LEAF
        // layout: LEFT(long) | RIGHT(long) | key* (until KEY_NULL or buffer end)
        i64 left = mark; // first long already read treated as left pointer
        i64 right = mbb->i64_get(mbb, e);
        if (e && *e) goto DONE;

        n = (struct node*)CALLOC(1, sizeof(struct node));
        node_init(n, NODE_LEAF, offset);
        n->data.l.left = left;
        n->data.l.right = right;

        int sz = 0;
        while(mbb->remaining(mbb) >= KEY_BYTES && sz < LEAF_KEYS_MAX) {
            i64 v = mbb->i64_get(mbb, e);
            if (e && *e) goto DONE;
            if (v == KEY_NULL) break;
            n->data.l.keys[sz++] = v;
        }
        n->length = sz;
    }

DONE:
    if (e && *e) {
        WARN("error at offset %lld: %s\n", offset, *e);
    }
    mbb->free(mbb);
    return n;
}

HOT_PATH
static struct node * bplustree_node_read(struct bplustree *me, i64 offset, char **e) {
    assert(me);
    assert(offset > 0); // offset 0 is for root pointer

    if (OFFSET_NULL == offset) return NULL;

    struct node *cached = (struct node*)me->cache->get(me->cache, offset);
    if (cached && cached != (struct node*)HASHMAP_INVALID_VAL) return cached;

    struct node *n = bplustree_node_decode(me, offset, e);
    if (e && *e) return NULL;
    if (n) me->cache->put(me->cache, offset, (valtype)n, bplustree_node_free);
    return n;
}

HOT_PATH
static struct node * bplustree_root_get(struct bplustree *me, char **e) {
    assert(me);
    if (me->root) return me->root;

    struct buffer *bb = me->storage->read(me->storage, ROOT_SEEK_OFFSET, NULL);
    if (!bb) THROW(e, "bplustree_root_get: failed to read root at offset %ld", ROOT_SEEK_OFFSET);

    bb->i32_get(bb, NULL);
    i64 offset = bb->i64_get(bb, NULL);
    if (OFFSET_NULL == offset) {
        bb->free(bb);
        return NULL;
    }
    assert(offset > 0); // offset 0 is for root pointer

    // LOG("root offset = %lld\n", offset);
    me->root = bplustree_node_read(me, offset, NULL);
    bb->free(bb);
    return me->root;

EXCEPTION:
    return NULL;
}

static inline void bplustree_root_set(struct bplustree *me, struct node *n, char **e) {
    assert(me);
    me->root = n;

    char a[NODE_BYTES] = {0, };
    struct buffer bb = {0};
    buffer_wrap(a, NODE_BYTES, &bb);
    bb.array_put(&bb, "ROOT", 4, e);
    bb.i64_put(&bb, (NULL == n) ? OFFSET_NULL : n->offset, e);
    bb.flip(&bb);
    me->storage->write_at(me->storage, ROOT_SEEK_OFFSET, &bb, e);
    bb.free(&bb);
}

static void bplustree_node_write(struct bplustree *me, struct node *n, char **e) {
    assert(me);
    assert(n);
    assert(n->offset > 0); // offset 0 is for root pointer
    assert(n->length > 0);

    char a[NODE_BYTES];
    memset(a, 0, sizeof(a));
    struct buffer bb = {0};
    buffer_wrap(a, NODE_BYTES, &bb);

    if (is_leaf(n)) {
        // print_keys("Writing LEAF keys", n->data.l.keys, n->length);
        // LEAF: left | right | keys...
        bb.i64_put(&bb, n->data.l.left, e);
        bb.i64_put(&bb, n->data.l.right, e);
        for(int i=0; i<n->length; i++) {
            bb.i64_put(&bb, n->data.l.keys[i], e);
        }
    } else {
        assert(n->data.i.keys[0].offset != OFFSET_NULL);

        // INTERNAL: mark | left | (key.offset | key.right)*
        bb.i64_put(&bb, INTERNAL_MARK, e);
        bb.i64_put(&bb, n->data.i.keys[0].left, e);
        for(int i=0; i<n->length; i++) {
            struct keyref *k = &n->data.i.keys[i];
            assert(k->offset != OFFSET_NULL);  
            assert(k->left != OFFSET_NULL);
            assert(k->right != OFFSET_NULL);
            assert(k->left != k->right);

            bb.i64_put(&bb, k->offset, e);
            bb.i64_put(&bb, k->right, e);
        }
    }
    
    bb.flip(&bb);
    me->storage->write_at(me->storage, n->offset, &bb, e);
    bb.free(&bb);
    if (!(e && *e)) me->cache->put(me->cache, n->offset, (valtype)n, bplustree_node_free);
}

static void bplustree_node_delete(struct bplustree *me, struct node *n, char **e) {
    assert(me);
    assert(n);
    me->storage->delete(me->storage, n->offset, e);
    me->cache->remove(me->cache, n->offset);
    // FREE(n);
}

static inline i64 keyref_min(struct bplustree *me, struct keyref *k, char **e) {
    assert(me); 
    assert(k);
    assert(k->offset != OFFSET_NULL);
    assert(k->offset > 0); // offset 0 is for root pointer

    // LOG("keyref_min: keyref offset=%lld left=%lld right=%lld\n", k->offset, k->left, k->right);
    struct node *leaf = bplustree_node_read(me, k->offset, e);
    if (!leaf || !is_leaf(leaf) || leaf->length == 0) return KEY_NULL;
    return leaf->data.l.keys[0];
}

static inline struct position position_leaf(struct bplustree *me, struct node *leaf, i64 key) {
    struct position pos = { .offset = 0, .d = 0 };
    int low = 0;
    int high = leaf->length - 1;
    int cmp = 0;
    while(low <= high) {
        int mid = (low + high) / 2;
        i64 midVal = leaf->data.l.keys[mid];
        // replicate Java cmp = -compare(key, midVal)
        cmp = -me->compare(me->obj, key, midVal);
        if (cmp < 0) {
            low = mid + 1;
        } else if (cmp > 0) {
            high = mid - 1;
        } else {
            pos.offset = mid; 
            pos.d = 0; 
            return pos;
        }
    }
    
    if (cmp < 0) {
        pos.offset = high;
        pos.d = 1;
        return pos;
    }

    pos.offset = low;
    pos.d = -1;
    return pos;
}

static inline struct position position_internal(struct bplustree *me, struct node *in, i64 key, char **e) {
    struct position pos = { .offset = 0, .d = 0 };
    int low = 0;
    int high = in->length - 1;
    int cmp = 0;
    while(low <= high) {
        int mid = (low + high) / 2;
        struct keyref *midVal = &in->data.i.keys[mid];
        struct node *leaf = bplustree_node_read(me, midVal->offset, e);

        assert(e && !*e);
        assert(leaf && leaf->length > 0);
        assert(is_leaf(leaf));

        i64 min = leaf->data.l.keys[0];
        cmp = -me->compare(me->obj, key, min);
        if (cmp < 0) {
            low = mid + 1;
        } else if (cmp > 0) {
            high = mid - 1;
        } else {
            pos.offset = mid;
            pos.d = 0;
            return pos;
        }
    }

    // LOG("position_internal: key=%lld, length=%d, cmp=%d\n", key, in->length, cmp);
    assert(cmp != 0);

    if (cmp < 0) {
        pos.offset = high;
        pos.d = 1;
        return pos;
    }
    pos.offset = low;
    pos.d = -1;
    return pos;
}

// BPlusTree.java offset()
static inline i64 bplustree_offset_new(struct bplustree *me) {
    assert(me);

    static char a[0];
    static struct buffer EMPTY_BUF;
    buffer_wrap(a, sizeof(a), &EMPTY_BUF);

    return me->storage->write(me->storage, &EMPTY_BUF, NULL);
}


static i64 bplustree_key_div(struct bplustree *me, int capacity,
    i64 source[], int slen, 
    i64 target[], int tlen, 
    struct position pos, i64 key
) {
    if (0 == pos.d) return KEY_NULL;

    int insert_pos = pos.offset + (pos.d < 0 ? 0 : 1);
    int total_keys = slen + 1; // source keys + new key
    
    // If all keys fit in target, no split needed
    if (total_keys <= capacity) {
        int i = 0, j = 0;
        // Copy keys before insertion point
        for (; i < insert_pos; i++, j++)
            target[i] = source[j];
        // Insert new key
        target[i++] = key;
        // Copy remaining keys
        for (; j < slen; i++, j++)
            target[i] = source[j];
        return KEY_NULL;
    }
    
    // Need to split - fill target to capacity and return overflow key
    int i = 0, j = 0;
    i64 overflow = KEY_NULL;
    
    for (int logical_pos = 0; logical_pos < total_keys; logical_pos++) {
        i64 curr_key;
        if (logical_pos == insert_pos) {
            curr_key = key;
        } else if (logical_pos < insert_pos) {
            curr_key = source[j++];
        } else {
            curr_key = source[j++];
        }
        
        if (i < capacity) {
            target[i++] = curr_key;
        } else {
            overflow = curr_key;
        }
    }
    
    return overflow;
}

static struct node * bplustree_leaf_sibling_get(struct bplustree *me, struct node *leaf) {
    assert(me);
    assert(leaf);
    i64 r = leaf->data.l.right;
    i64 l = leaf->data.l.left;

    if (OFFSET_NULL == r) {
        if (OFFSET_NULL == l) return NULL;

        assert(l > 0);
        struct node *sib = bplustree_node_read(me, l, NULL);
        if (sib && sib->length < LEAF_KEYS_MAX) {
            assert(sib->type == NODE_LEAF);
            return sib;
        }
    } else {
        assert(r > 0);
        struct node *sib = bplustree_node_read(me, r, NULL);
        if (sib && sib->length < LEAF_KEYS_MAX) {
            assert(sib->type == NODE_LEAF);
            return sib;
        }

        if (OFFSET_NULL != l) {
            assert(l > 0);
            sib = bplustree_node_read(me, l, NULL);
            if (sib && sib->length < LEAF_KEYS_MAX) {
                assert(sib->type == NODE_LEAF);
                return sib;
            }
        }
    }
    return NULL;
}

struct node * bplustree_internal_sibling_get(struct bplustree *me, struct node *l, struct node *r) {
    assert(me);
    if (NULL == r) {
        if ((NULL != l) && !is_leaf(l) && l->length < INTERNAL_KEYS_MAX) 
            return l;
    } else {
        assert(r);
        if (!is_leaf(r) && r->length < INTERNAL_KEYS_MAX) 
            return r;
        if ((NULL != l) && !is_leaf(l) && l->length < INTERNAL_KEYS_MAX)
            return l;
    }
    return NULL;
}

// static inline void bplustree_key_concat_front(i64 key, i64 a[], int len) {
//     if (len <= 0) return;
//     for(int i=len-1; i>0; i--) {
//         a[i] = a[i-1];
//     }
//     a[0] = key;
// }

static inline void bplustree_key_push_back(i64 a[], int len, i64 key) {
    if (len >= LEAF_KEYS_MAX) return;
    a[len] = key;
}

static struct node * bplustree_leaf_put(struct bplustree *me, struct node *leaf, i64 key, char **e) {
    assert(NODE_LEAF == leaf->type);
    struct position pos = position_leaf(me, leaf, key);

    if (0 == pos.d) // existing key
        return NULL;

    struct node *popped = NULL;
    i64 temp[LEAF_KEYS_MAX];
    memset(temp, 0xFF, sizeof temp); // -1L
    i64 split = bplustree_key_div(me, LEAF_KEYS_MAX, 
                        leaf->data.l.keys, leaf->length,  
                        temp, LEAF_KEYS_MAX,
                        pos, key);
                    
    if (KEY_NULL != split) {
        struct node *sib = bplustree_leaf_sibling_get(me, leaf);
        if (NULL != sib) {
            if (sib->offset == leaf->data.l.right) {
                // Right sibling exists
                // 기존 구현은 split(overflow된 가장 큰 키)를 오른쪽 형제의 맨 앞에 넣어
                // 형제 노드의 최소 키를 감소시켰고, 이는 부모 internal 노드의 keyref 정렬 불변식을 깨뜨려
                // 같은 leaf offset이 재삽입되는 중복 상황을 유발할 수 있었다.
                // 수정: split 키를 형제 노드의 맨 뒤에 붙여 최소 키를 변경하지 않음.
                bplustree_key_push_back(sib->data.l.keys, sib->length, split);
                sib->length++;

                #ifdef UNIT_TEST
                // Ensure ordering (split was the largest key of the overflow set, so ordering must hold)
                assert(sib->length <= LEAF_KEYS_MAX);
                for (int _i = 1; _i < sib->length; _i++) {
                    assert(me->compare(me->obj, sib->data.l.keys[_i-1], sib->data.l.keys[_i]) <= 0);
                }
                #endif

                bplustree_node_write(me, sib, e);
                leaf->length = LEAF_KEYS_MAX;
            } else {
                // Left sibling exists
                // We should move the smallest key from current leaf to left sibling
                // and add the split key at the end of current leaf
                bplustree_key_push_back(sib->data.l.keys, sib->length, temp[0]);
                sib->length++;
                bplustree_node_write(me, sib, e);
                // Shift keys left and add split at the end
                memmove(&temp[0], &temp[1], (size_t)(LEAF_KEYS_MAX - 1) * sizeof(i64));
                temp[LEAF_KEYS_MAX - 1] = split;
                leaf->length = LEAF_KEYS_MAX;
            }
        } else {
            // No sibling available - create a new right sibling
            sib = (struct node*)CALLOC(1, sizeof(struct node));
            node_init(sib, NODE_LEAF, bplustree_offset_new(me));
            sib->data.l.keys[0] = split;
            sib->length = 1;
            sib->data.l.left = leaf->offset;
            sib->data.l.right = leaf->data.l.right;
            leaf->data.l.right = sib->offset;
            if (OFFSET_NULL != sib->data.l.right) {
                assert(sib->data.l.right > 0);
                struct node *r = bplustree_node_read(me, sib->data.l.right, e);
                if (r) {
                    r->data.l.left = sib->offset;
                    bplustree_node_write(me, r, e);
                }
            }
            bplustree_node_write(me, sib, e);

            popped = sib;
            leaf->length = LEAF_KEYS_MAX;
        }
    } else {
        // No split needed - all keys fit in current leaf
        leaf->length = leaf->length + 1;
    }

    memcpy(leaf->data.l.keys, temp, sizeof(i64) * leaf->length);
    bplustree_node_write(me, leaf, e);
    me->count++;
    bplustree_count_set(me, me->count);
    return popped;
}

static struct keyref bplustree_node_put(struct bplustree *me, struct context *ctx, struct node *n, i64 key, char **e) {
    if (is_leaf(n)) {
        struct node *popped = bplustree_leaf_put(me, n, key, e);
        if (e && *e) goto DONE;
        if (NULL == popped) goto DONE; // no split
        
        struct keyref k = { 
            .offset = popped->offset, 
            .left = popped->data.l.left, 
            .right = popped->offset, 
        };
        assert(k.left != k.right);
        // printf("leaf K:%lld split: O:%lld L:%lld R:%lld, CTX:<%p>\n", key, k.offset, k.left, k.right, (void*)ctx);

        if (NULL == ctx) {
            struct node *newroot = (struct node*)CALLOC(1, sizeof(struct node));
            node_init(newroot, NODE_INTERNAL, bplustree_offset_new(me));
            newroot->data.i.keys[0] = k;
            newroot->length = 1;
            bplustree_node_write(me, newroot, e);
            if (e && *e) {
                bplustree_node_delete(me, newroot, NULL);
                return KEYREF_NULL;
            }
            bplustree_root_set(me, newroot, e);
            if (e && *e) {
                bplustree_node_delete(me, newroot, NULL);
                return KEYREF_NULL;
            }
            return KEYREF_NULL;
        } else {
            return k; // propagate up
        }
        return k;
    }

    // internal node
    struct position pos = position_internal(me, n, key, e);
    if (e && *e) goto DONE;
    if (0 == pos.d) goto DONE; // existing key, should not happen

    // insert new key
    struct keyref k  = n->data.i.keys[pos.offset];
    struct context nctx = { .p = ctx, .n = n, .i = pos };

    // print_keyrefs("INTERNAL keys", n->data.i.keys, n->length);
    // LOG("pos for key %lld: offset=%d d=%d\n", key, pos.offset, pos.d);
    assert(k.offset != OFFSET_NULL);
    assert(k.offset > 0); // offset 0 is for root pointer
    assert((pos.d < 0 ? k.left : k.right) > 0); // offset 0 is for root pointer
    
    struct keyref nk = bplustree_node_put(me, &nctx,
                                            bplustree_node_read(me, (pos.d < 0 ? k.left : k.right), e), 
                                            key, 
                                            e);
    
    // printf("nk from child: O:%lld L:%lld R:%lld\n", nk.offset, nk.left, nk.right);
    if (e && *e) goto DONE;
    if (nk.offset == OFFSET_NULL) goto DONE; // no split below // JAVA version : if (null == nk) return null;

    
    struct keyref nkeys[INTERNAL_KEYS_MAX + 1];
    memset(nkeys, 0xFF, sizeof nkeys); // -1L

    // Use array_wrap_join instead of manual array manipulation
    struct array_wrap aw;
    array_wrap_init(&aw, nkeys, INTERNAL_KEYS_MAX + 1);
    array_wrap_join(&aw, n->data.i.keys, n->length, pos.offset, pos.d, nk);
    int nlen = aw.length;    

    struct keyref temp[INTERNAL_KEYS_MAX];
    memcpy(temp, nkeys, sizeof(struct keyref) * INTERNAL_KEYS_MAX);

    struct keyref split = (nlen <= INTERNAL_KEYS_MAX) ? KEYREF_NULL : nkeys[INTERNAL_KEYS_MAX];
    if (OFFSET_NULL == split.offset) {
        n->length = nlen;
        memcpy(n->data.i.keys, temp, sizeof(struct keyref) * n->length);
        bplustree_node_write(me, n, e);
        if (e && *e) return KEYREF_NULL;
        return KEYREF_NULL;
    }

    // The node is full and needs to be split
    int mid_idx = (INTERNAL_KEYS_MAX) / 2;
    struct keyref mid_key = temp[mid_idx];

    struct node *sib = (struct node*)CALLOC(1, sizeof(struct node));
    node_init(sib, NODE_INTERNAL, bplustree_offset_new(me));

    // Keys after mid_key go to the new sibling
    int sib_len = 0;
    for (int i = mid_idx + 1; i < INTERNAL_KEYS_MAX; i++) {
        sib->data.i.keys[sib_len++] = temp[i];
    }
    sib->data.i.keys[sib_len++] = split;
    sib->length = sib_len;

    // Update the left pointer of the first key in the sibling
    sib->data.i.keys[0].left = mid_key.right;

    bplustree_node_write(me, sib, e);
    if (e && *e) {
        bplustree_node_delete(me, sib, NULL);
        return KEYREF_NULL;
    }

    // Keys before mid_key remain in the current node
    n->length = mid_idx;
    memset(&n->data.i.keys[n->length], 0xFF, sizeof(struct keyref) * (INTERNAL_KEYS_MAX - n->length));
    bplustree_node_write(me, n, e);
    if (e && *e) {
        // On error, we might have an orphaned sibling node, but it's complex to recover
        return KEYREF_NULL;
    }

    // The mid_key is promoted up
    mid_key.left = n->offset;
    mid_key.right = sib->offset;

    if (ctx == NULL) { // This was the root node
        struct node *new_root = (struct node*)CALLOC(1, sizeof(struct node));
        node_init(new_root, NODE_INTERNAL, bplustree_offset_new(me));
        new_root->data.i.keys[0] = mid_key;
        new_root->length = 1;
        bplustree_node_write(me, new_root, e);
        if (e && *e) {
            bplustree_node_delete(me, new_root, NULL);
            return KEYREF_NULL;
        }
        bplustree_root_set(me, new_root, e);
        return KEYREF_NULL;
    } else {
        // Propagate mid_key up to the parent
        return mid_key;
    }
    
DONE:
    return KEYREF_NULL;
}

static void bplustree_put(struct bplustree *me, i64 key, char **e) { // public
    assert(me);
    assert(key >= 0); // keys are always positive

    struct node *root = bplustree_root_get(me, e);
    if (NULL == root) {
        struct node *leaf = (struct node*)CALLOC(1, sizeof(struct node));
        node_init(leaf, NODE_LEAF, bplustree_offset_new(me));
        leaf->data.l.keys[0] = key;
        leaf->length = 1;
        bplustree_node_write(me, leaf, e);
        if (e && *e) return;
        bplustree_root_set(me, leaf, e);
        me->count++;
        bplustree_count_set(me, me->count);
        return;
    }

    bplustree_node_put(me, NULL, root,  key, e);
}

static i64 bplustree_get(struct bplustree *me, i64 key, char **e) {
    assert(me);
    assert(key > 0); // keys are always positive

    struct node *root = bplustree_root_get(me, e);
    if (!root) return NOT_FOUND;
    struct node *n = root;
    while(n) {
        if (is_leaf(n)) {
            struct position p = position_leaf(me, n, key);
            if (p.d == 0) return n->data.l.keys[p.offset];
            return NOT_FOUND;
        } else {
            // Use position_internal to find the correct child (same logic as Java)
            struct position pos = position_internal(me, n, key, e);
            if (e && *e) return NOT_FOUND;

            if (pos.d == 0) {
                // Exact match on an internal key's referenced leaf's min key.
                // Go directly to that leaf node.
                struct keyref *kref = &n->data.i.keys[pos.offset];
                n = bplustree_node_read(me, kref->offset, e);
                continue;
            }

            assert(pos.offset >= 0 && pos.offset < n->length);

            struct keyref *kref = &n->data.i.keys[pos.offset];

            assert(kref->offset != OFFSET_NULL);
            assert(kref->offset > 0); // offset 0 is for root pointer
            assert((pos.d < 0 ? kref->left : kref->right) != OFFSET_NULL);
            assert((pos.d < 0 ? kref->left : kref->right) > 0);  // offset 0 is for root pointer
            // LOG("internal keyref: O:%lld L:%lld R:%lld for key %lld\n", kref->offset, kref->left, kref->right, key);
            
            i64 child_off = (pos.d < 0) ? kref->left : kref->right;
            assert(child_off > 0); // offset 0 is for root pointer
            n = bplustree_node_read(me, child_off, e);
        }
    }
    return NOT_FOUND;
}


static i8 bplustree_internal_rebalance(struct bplustree *me, struct context *ctx, struct node *n, int child_key_idx, char **e) {
    // Remove key from internal node
    memmove(&n->data.i.keys[child_key_idx], &n->data.i.keys[child_key_idx + 1], (n->length - child_key_idx - 1) * sizeof(struct keyref));
    n->length--;

    if (n->length >= INTERNAL_KEYS_MAX / 2) {
        bplustree_node_write(me, n, e);
        return 1;
    }

    // Underflow
    if (ctx == NULL) { // This is the root
        if (n->length == 0) {
            struct node *new_root = bplustree_node_read(me, n->data.i.keys[0].left, e);
            bplustree_root_set(me, new_root, e);
            bplustree_node_delete(me, n, e);
        } else {
            bplustree_node_write(me, n, e);
        }
        return 1;
    }

    // Rebalance internal node (borrow or merge)
    struct node *parent = ctx->n;
    int node_idx_in_parent = ctx->i.offset;

    // Try to borrow from right sibling
    if (node_idx_in_parent < parent->length) {
        struct node *right_sib = bplustree_node_read(me, parent->data.i.keys[node_idx_in_parent].right, e);
        if (right_sib && right_sib->length > INTERNAL_KEYS_MAX / 2) {
            // Take key from parent and give to n
            struct keyref key_from_parent = parent->data.i.keys[node_idx_in_parent];
            key_from_parent.left = n->data.i.keys[n->length - 1].right;
            key_from_parent.right = right_sib->data.i.keys[0].left;
            n->data.i.keys[n->length++] = key_from_parent;

            // Take key from sibling and give to parent
            parent->data.i.keys[node_idx_in_parent] = right_sib->data.i.keys[0];
            parent->data.i.keys[node_idx_in_parent].left = n->offset;

            // Move keys in sibling
            memmove(right_sib->data.i.keys, &right_sib->data.i.keys[1], (right_sib->length - 1) * sizeof(struct keyref));
            right_sib->length--;

            bplustree_node_write(me, n, e);
            bplustree_node_write(me, right_sib, e);
            bplustree_node_write(me, parent, e);
            return 1;
        }
    }

    // Try to borrow from left sibling
    if (node_idx_in_parent > 0) {
        struct node *left_sib = bplustree_node_read(me, parent->data.i.keys[node_idx_in_parent - 1].left, e);
        if (left_sib && left_sib->length > INTERNAL_KEYS_MAX / 2) {
            // Take key from parent and give to n
            memmove(&n->data.i.keys[1], &n->data.i.keys[0], n->length * sizeof(struct keyref));
            struct keyref key_from_parent = parent->data.i.keys[node_idx_in_parent - 1];
            key_from_parent.right = n->data.i.keys[0].left;
            key_from_parent.left = left_sib->data.i.keys[left_sib->length - 1].right;
            n->data.i.keys[0] = key_from_parent;
            n->length++;

            // Take key from sibling and give to parent
            parent->data.i.keys[node_idx_in_parent - 1] = left_sib->data.i.keys[left_sib->length - 1];
            parent->data.i.keys[node_idx_in_parent - 1].right = n->offset;
            left_sib->length--;

            bplustree_node_write(me, n, e);
            bplustree_node_write(me, left_sib, e);
            bplustree_node_write(me, parent, e);
            return 1;
        }
    }

    // Merge with a sibling
    if (node_idx_in_parent < parent->length) {
        // Merge with right sibling
        struct node *right_sib = bplustree_node_read(me, parent->data.i.keys[node_idx_in_parent].right, e);
        if (right_sib) {
            struct keyref key_from_parent = parent->data.i.keys[node_idx_in_parent];
            key_from_parent.left = n->data.i.keys[n->length - 1].right;
            key_from_parent.right = right_sib->data.i.keys[0].left;
            n->data.i.keys[n->length++] = key_from_parent;

            memcpy(&n->data.i.keys[n->length], right_sib->data.i.keys, right_sib->length * sizeof(struct keyref));
            n->length += right_sib->length;

            bplustree_node_write(me, n, e);
            bplustree_node_delete(me, right_sib, e);
            return -1; // Signal to parent for rebalancing
        }
    } else {
        // Merge with left sibling
        struct node *left_sib = bplustree_node_read(me, parent->data.i.keys[node_idx_in_parent - 1].left, e);
        if (left_sib) {
            struct keyref key_from_parent = parent->data.i.keys[node_idx_in_parent - 1];
            key_from_parent.left = left_sib->data.i.keys[left_sib->length - 1].right;
            key_from_parent.right = n->data.i.keys[0].left;
            left_sib->data.i.keys[left_sib->length++] = key_from_parent;

            memcpy(&left_sib->data.i.keys[left_sib->length], n->data.i.keys, n->length * sizeof(struct keyref));
            left_sib->length += n->length;

            bplustree_node_write(me, left_sib, e);
            bplustree_node_delete(me, n, e);
            return -1; // Signal to parent for rebalancing
        }
    }

    return 1;
}

static i8 bplustree_leaf_rebalance(struct bplustree *me, struct context *ctx, struct node *n, char **e) {
    struct node *parent = ctx->n;
    int key_idx_in_parent = ctx->i.offset;

    // Try to borrow from right sibling only when leaf has some keys
    if (n->length > 0 && n->data.l.right != OFFSET_NULL) {
        struct node *right_sib = bplustree_node_read(me, n->data.l.right, e);
        if (right_sib && right_sib->length > LEAF_KEYS_MAX / 2) {
            n->data.l.keys[n->length++] = right_sib->data.l.keys[0];
            memmove(right_sib->data.l.keys, &right_sib->data.l.keys[1], (right_sib->length - 1) * sizeof(i64));
            right_sib->length--;
            bplustree_node_write(me, n, e);
            bplustree_node_write(me, right_sib, e);

            // Update parent key
            parent->data.i.keys[key_idx_in_parent].offset = right_sib->offset;
            bplustree_node_write(me, parent, e);

            // Propagate key offset updates up the ancestor chain when needed
            // Similar to Java's updateKeys: for ancestors where we descended to the right (d >= 0),
            // ensure their separator key's offset points to the new min leaf of the right child.
            for (struct context *c = ctx->p; c != NULL; c = c->p) {
                struct node *pp = c->n;
                if (!pp) break;
                if (c->i.d >= 0) {
                    struct keyref *ppk = &pp->data.i.keys[c->i.offset];
                    struct node *rch = bplustree_node_read(me, ppk->right, e);
                    if (!rch) continue;
                    struct node *leaf = node_leaf_min(me, rch, e);
                    if (leaf && ppk->offset != leaf->offset) {
                        ppk->offset = leaf->offset;
                        bplustree_node_write(me, pp, e);
                    }
                }
            }
            return 1;
        }
    }

    // Merge with a sibling (preferred path when leaf became empty)
    if (n->data.l.right != OFFSET_NULL) {
        // Merge with right sibling
        struct node *right_sib = bplustree_node_read(me, n->data.l.right, e);
        if (right_sib) {
            memcpy(&n->data.l.keys[n->length], right_sib->data.l.keys, right_sib->length * sizeof(i64));
            n->length += right_sib->length;
            n->data.l.right = right_sib->data.l.right;
            if (n->data.l.right != OFFSET_NULL) {
                struct node *r = bplustree_node_read(me, n->data.l.right, e);
                if(r) {
                    r->data.l.left = n->offset;
                    bplustree_node_write(me, r, e);
                }
            }
            bplustree_node_write(me, n, e);
            bplustree_node_delete(me, right_sib, e);
            return -1; // Signal to parent for rebalancing
        }
    } else if (n->data.l.left != OFFSET_NULL) {
        // Merge with left sibling
        struct node *left_sib = bplustree_node_read(me, n->data.l.left, e);
        if (left_sib) {
            memcpy(&left_sib->data.l.keys[left_sib->length], n->data.l.keys, n->length * sizeof(i64));
            left_sib->length += n->length;
            left_sib->data.l.right = n->data.l.right;
             if (left_sib->data.l.right != OFFSET_NULL) {
                struct node *lr_sib = bplustree_node_read(me, left_sib->data.l.right, e);
                if(lr_sib) {
                    lr_sib->data.l.left = left_sib->offset;
                    bplustree_node_write(me, lr_sib, e);
                }
            }
            bplustree_node_write(me, left_sib, e);
            bplustree_node_delete(me, n, e);
            return -1; // Signal to parent for rebalancing
        }
    }

    return 1; // Should not be reached if logic is correct
}

static i8 bplustree_leaf_delete(struct bplustree *me, struct context *ctx, struct node *n, i64 key, char **e) {
    assert(me);
    assert(n);
    assert(is_leaf(n));

    // Find key position in leaf
    struct position found = position_leaf(me, n, key);
    if (0 != found.d) {
        // Key not found
        return 0;
    }

    // Remove key by shifting left from found.offset
    if (found.offset < n->length - 1) {
        memmove(&n->data.l.keys[found.offset],
                &n->data.l.keys[found.offset + 1],
                (size_t)(n->length - found.offset - 1) * sizeof(i64));
    }
    n->length--;

    // Root is a leaf
    if (NULL == ctx) {
        if (n->length > 0) {
            bplustree_node_write(me, n, e);
            return 1;
        } else {
            // Tree becomes empty
            bplustree_node_delete(me, n, e);
            bplustree_root_set(me, NULL, e);
            return 1;
        }
    }

    // Non-root leaf
    if (n->length > 0) {
        // If we deleted the minimal key in this leaf and we are in the right branch of parent,
        // update parent's separator keyref.offset to the min leaf of the right subtree.
        if (ctx && found.offset == 0 && ctx->i.d >= 0) {
            struct node *parent = ctx->n;
            int idx = ctx->i.offset;
            struct keyref *kr = &parent->data.i.keys[idx];
            if (kr->right != OFFSET_NULL) {
                struct node *rch = bplustree_node_read(me, kr->right, e);
                if (rch) {
                    struct node *minleaf = node_leaf_min(me, rch, e);
                    if (minleaf && kr->offset != minleaf->offset) {
                        kr->offset = minleaf->offset;
                        bplustree_node_write(me, parent, e);
                    }
                }
            }
        }

        bplustree_node_write(me, n, e);
        return 1;
    }

    // Leaf became empty: try to borrow or merge, possibly signaling parent to drop a key
    return bplustree_leaf_rebalance(me, ctx, n, e);
}

static i8 bplustree_node_delete_key(struct bplustree *me, struct context *ctx, struct node *n, i64 key, char **e) {
    if (is_leaf(n)) {
        return bplustree_leaf_delete(me, ctx, n, key, e);
    }

    // Internal node
    struct position pos = position_internal(me, n, key, e);
    if (e && *e) return 0;

    struct keyref *k = &n->data.i.keys[pos.offset];
    struct node *child = bplustree_node_read(me, (pos.d < 0 ? k->left : k->right), e);
    if (child == NULL) {
        return 0; // Key not found
    }

    struct context nctx = { .p = ctx, .n = n, .i = pos };
    i8 result = bplustree_node_delete_key(me, &nctx, child, key, e);

    if (result < 0) { // Rebalance needed from child
        return bplustree_internal_rebalance(me, ctx, n, pos.offset, e);
    }

    return result;
}

static i8 bplustree_delete(struct bplustree *me, i64 key, char **e) {
    struct node *root = bplustree_root_get(me, e);
    if (root == NULL) {
        return 0; // Tree is empty
    }

    i8 result = bplustree_node_delete_key(me, NULL, root, key, e);
    if (result > 0) {
        me->count--;
        bplustree_count_set(me, me->count);
    }
    return result;
}

struct bptree_cursor_impl {
    struct bplustree *tree;
    struct node *leaf;
    int offset;
    enum order order;
    void *obj;
    int (*cmpr)(void *obj, i64 o);
};

static i64 cursor_next_asc(struct cursor_i64 *c, char **e) {
    struct bptree_cursor_impl *impl = (struct bptree_cursor_impl*)c->p;
    if (impl->leaf == NULL) return NOT_FOUND;
    for (;;) {
        // Move to next leaf if we've exhausted current leaf
        if (impl->offset >= impl->leaf->length) {
            if (impl->leaf->data.l.right == OFFSET_NULL) {
                impl->leaf = NULL;
                return NOT_FOUND;
            }
            impl->leaf = bplustree_node_read(impl->tree, impl->leaf->data.l.right, e);
            if (impl->leaf == NULL) return NOT_FOUND;
            impl->offset = 0;
        }

        i64 key = impl->leaf->data.l.keys[impl->offset++];
        int d = impl->cmpr(impl->obj, key);
        if (d > 0) {
            // key is before desired start -> keep scanning forward
            continue;
        } else if (d == 0) {
            // within desired range
            return key;
        } else { // d < 0
            // key is after desired end -> stop iteration
            impl->leaf = NULL;
            return NOT_FOUND;
        }
    }
}

static i64 cursor_next_desc(struct cursor_i64 *c, char **e) {
    struct bptree_cursor_impl *impl = (struct bptree_cursor_impl*)c->p;
    if (impl->leaf == NULL) return NOT_FOUND;
    for (;;) {
        if (impl->offset < 0) {
            if (impl->leaf->data.l.left == OFFSET_NULL) {
                impl->leaf = NULL;
                return NOT_FOUND;
            }
            impl->leaf = bplustree_node_read(impl->tree, impl->leaf->data.l.left, e);
            if (impl->leaf == NULL) return NOT_FOUND;
            impl->offset = impl->leaf->length - 1;
        }

        i64 key = impl->leaf->data.l.keys[impl->offset--];
        int d = impl->cmpr(impl->obj, key);
        if (d > 0) {
            // For DESC, d>0 means key is before start; since we're moving left, keep scanning
            continue;
        } else if (d == 0) {
            return key;
        } else { // d < 0 means after end -> stop
            impl->leaf = NULL;
            return NOT_FOUND;
        }
    }
}

static void cursor_close(struct cursor_i64 *c) {
    if (c) {
        if (c->p) FREE(c->p);
        FREE(c);
    }
}

static struct cursor_i64 * cursor_eof() {
    struct cursor_i64 *c = (struct cursor_i64*)CALLOC(1, sizeof(struct cursor_i64));
    struct bptree_cursor_impl *impl = (struct bptree_cursor_impl*)CALLOC(1, sizeof(struct bptree_cursor_impl));
    c->p = impl;
    c->close = cursor_close;
    c->next = cursor_next_asc; // or desc, doesn't matter
    return c;
}

// for range scans using comparator w/ ascending order
static struct node* node_leaf_min_comparable(struct bplustree *me, struct node *start, void *obj, int (*cmpr)(void *obj, i64 o), char **e) {
    struct node *n = start;
    while(n && !is_leaf(n)) {
        // In internal node, find the correct child to descend
        int i = 0;
        for(i = 0; i < n->length; i++) {
            i64 min_key = keyref_min(me, &n->data.i.keys[i], e);
            // cmpr returns compare(target, min_key):
            //   < 0 => target < min_key (found a node that's too large)
            //   = 0 => target == min_key (perfect match, but may have earlier matches)
            //   > 0 => target > min_key (need to check next node)
            if (cmpr(obj, min_key) <= 0) {
                break;
            }
        }
        if (i == n->length) i--;
        // Choose child based on comparator:
        // If target <= min_key, go left to find potential matches
        // If target > min_key, go right to continue search
        i64 child_offset = n->data.i.keys[i].left;
        i64 min_key = keyref_min(me, &n->data.i.keys[i], e);
        int d = cmpr(obj, min_key);
        if (d > 0) {
            // target > min_key: Need to move right
            child_offset = n->data.i.keys[i].right;
        } else {
            // target <= min_key: go left to find earliest match
            // keep child_offset as left
        }
        n = bplustree_node_read(me, child_offset, e);
    }
    return n;
}

// for range scans using comparator w/ descending order
static struct node* node_leaf_max_comparable(struct bplustree *me, struct node *start, void *obj, int (*cmpr)(void *obj, i64 o), char **e) {
    struct node *n = start;
    while(n && !is_leaf(n)) {
        int i = n->length - 1;
        for(; i >= 0; i--) {
            i64 min_key = keyref_min(me, &n->data.i.keys[i], e);
            if (cmpr(obj, min_key) <= 0) {
                break;
            }
        }
        if (i < 0) i = 0;

        i64 child_offset = n->data.i.keys[i].right;
        n = bplustree_node_read(me, child_offset, e);
    }
    return n;
}

// HOT_PATH
static int first_key_pos(i64 *keys, int len, void *obj, int (*cmpr)(void *obj, i64 o)) {
    // Find the first key where cmpr returns 0 (match)
    // cmpr returns compare(target, key):
    //   < 0 => target < key (search left)
    //   = 0 => target == key (found, but continue searching left for first match)
    //   > 0 => target > key (search right)
    int low = 0, high = len - 1;
    int result = -1;
    while (low <= high) {
        int mid = low + (high - low) / 2;
        int d = cmpr(obj, keys[mid]);
        if (d <= 0) { 
            // target <= key: could be a match or before it
            if (d == 0) result = mid; 
            high = mid - 1; // continue searching left for earlier matches
        }
        else { 
            // target > key: search right
            low = mid + 1; 
        }
    }
    return result;
}

// HOT_PATH
static int last_key_pos(i64 *keys, int len, void *obj, int (*cmpr)(void *obj, i64 o)) {
    // Find the last key where cmpr returns 0 (match)
    // cmpr returns compare(target, key):
    //   < 0 => target < key (search left)
    //   = 0 => target == key (found, but continue searching right for last match)
    //   > 0 => target > key (search right)
    int low = 0, high = len - 1;
    int result = -1;
    while (low <= high) {
        int mid = low + (high - low) / 2;
        int d = cmpr(obj, keys[mid]);
        if (d >= 0) { 
            // target >= key: could be a match or after it
            if (d == 0) result = mid;
            low = mid + 1; // continue searching right for later matches
        }
        else { 
            // target < key: search left
            high = mid - 1; 
        }
    }
    return result;
}

static struct cursor_i64 * bplustree_find(struct bplustree *me, enum order order, void *obj, int (*cmpr)(void *obj, i64 o), char **e) {
    assert(me);
    struct node *root = bplustree_root_get(me, e);
    if (NULL == root) 
        return cursor_eof();

    struct bptree_cursor_impl *impl = (struct bptree_cursor_impl*)CALLOC(1, sizeof(struct bptree_cursor_impl));
    impl->tree = me;
    impl->order = order;
    impl->obj = obj;
    impl->cmpr = cmpr;

    if (order == ASC) {
        impl->leaf = node_leaf_min_comparable(me, root, obj, cmpr, e);
        if (impl->leaf) {
            impl->offset = first_key_pos(impl->leaf->data.l.keys, impl->leaf->length, obj, cmpr);
            if (impl->offset == -1) impl->leaf = NULL;
        }
    } else { // DESC
        impl->leaf = node_leaf_max_comparable(me, root, obj, cmpr, e);
        if (impl->leaf) {
            impl->offset = last_key_pos(impl->leaf->data.l.keys, impl->leaf->length, obj, cmpr);
            if (impl->offset == -1) impl->leaf = NULL;
        }
    }
    
    if (impl->leaf == NULL) {
        FREE(impl);
        return NULL;
    }

    struct cursor_i64 *c = (struct cursor_i64*)CALLOC(1, sizeof(struct cursor_i64));
    c->p = impl;
    c->close = cursor_close;
    c->next = (order == ASC) ? cursor_next_asc : cursor_next_desc;

    return c;
}

HOT_PATH
static struct position position_leaf_comparable(struct node *leaf, void *obj, const void *r, int (*cmpr)(void *obj, const void *r, i64 o)) {
    struct position pos = { .offset = 0, .d = 0 };
    int low = 0;
    int high = leaf->length - 1;
    int cmp = 0;
    while(low <= high) {
        int mid = (low + high) / 2;
        i64 midVal = leaf->data.l.keys[mid];
        cmp = -cmpr(obj, r, midVal);
        if (cmp < 0) {
            low = mid + 1;
        } else if (cmp > 0) {
            high = mid - 1;
        } else {
            pos.offset = mid; 
            pos.d = 0; 
            return pos;
        }
    }

    // Not found, return insertion point
    if (cmp < 0) {
        pos.offset = high; 
        pos.d = 1; 
        return pos;
    }
    pos.offset = low; 
    pos.d = -1; 
    return pos;
}

HOT_PATH
static struct position position_internal_comparable(struct bplustree *me, struct node *in, void *obj, const void *r, int (*cmpr)(void *obj, const void *r, i64 o), char **e) {
    struct position pos = { .offset = 0, .d = 0 };
    int low = 0;
    int high = in->length - 1;
    int cmp = 0;
    while(low <= high) {
        int mid = (low + high) / 2;
        struct keyref *midVal = &in->data.i.keys[mid];
        i64 min = keyref_min(me, midVal, e);
        if(e && *e) return pos;

        cmp = -cmpr(obj, r, min);
        if (cmp < 0) {
            low = mid + 1;
        } else if (cmp > 0) {
            high = mid - 1;
        } else {
            pos.offset = mid;
            pos.d = 0;
            return pos;
        }
    }

    // Not found, return insertion point
    if (cmp < 0) {
        pos.offset = high;
        pos.d = 1;
        return pos;
    }
    pos.offset = low;
    pos.d = -1;
    return pos;
}

HOT_PATH
static i64 bplustree_compare_get(struct bplustree *me, void *obj, const void *r, int (*cmpr)(void *obj, const void *r, i64 o), char **e) {
    assert(me);
    struct node *root = bplustree_root_get(me, e);
    if (!root) return NOT_FOUND;
    struct node *n = root;
    while(n) {
        if (is_leaf(n)) {
            struct position p = position_leaf_comparable(n, obj, r, cmpr);
            // DEBUG("leaf pos: off=%d d=%d", p.offset, p.d);
            if (p.d == 0) return n->data.l.keys[p.offset];
            return NOT_FOUND;
        } else {
            struct position pos = position_internal_comparable(me, n, obj, r, cmpr, e);
            if (e && *e) return NOT_FOUND;

            if (pos.d == 0) {
                struct keyref *kref = &n->data.i.keys[pos.offset];
                return keyref_min(me, kref, e);
            }

            struct keyref *kref = &n->data.i.keys[pos.offset];
            i64 child_off = (pos.d < 0) ? kref->left : kref->right;
            n = bplustree_node_read(me, child_off, e);
        }
    }
    return NOT_FOUND;
}

#ifndef NDEBUG // debug functions
void bplustree_traverse_leaf(struct bplustree *me) { //for debug
    assert(me);
    char *e = NULL;
    struct node *root = bplustree_root_get(me, &e);
    struct node *min = node_leaf_min(me, root, &e);
    if (e && *e) {
        LOG("bplustree_traverse_leaf error: %s", e);
        return;
    }
    int i = 1;
    char buf[64] = {0,};
    for(struct node *n = min; n != NULL; ) {
        snprintf(buf, sizeof(buf), "LEAF[%03d] keys", i++);
        // print_keys(buf, n->data.l.keys, n->length);
        printf("LEAF[%03d] @%lld L:%lld R:%lld (%lld-%lld)\n", i-1, n->offset, n->data.l.left, n->data.l.right, n->data.l.keys[0], n->data.l.keys[n->length-1]  );
        n = (n && n->data.l.right != OFFSET_NULL) ? bplustree_node_read(me, n->data.l.right, NULL) : NULL;
    }
}

void bplustree_traverse_internal(struct bplustree *me) { //for debug
    assert(me);
    char *e = NULL;
    struct node *root = bplustree_root_get(me, &e);
    if (e && *e) {
        LOG("bplustree_traverse_internal error: %s", e);
        return;
    }
    if (!root) {
        printf("EMPTY TREE\n");
        return;
    }

    struct node *queue[1024] = {0,};
    int qlen = 0;
    queue[qlen++] = root;

    int level = 0;
    while(qlen > 0) {
        int next_qlen = 0;
        printf("LEVEL %d:\n", level++);
        for(int i=0; i<qlen; i++) {
            struct node *n = queue[i];
            if (!n) continue;

            if (is_leaf(n)) {
                printf("  LEAF @%lld L:%lld R:%lld (%lld-%lld) LEN=%d\n", n->offset, n->data.l.left, n->data.l.right, n->data.l.keys[0], n->data.l.keys[n->length-1], n->length);
            } else {
                printf("  INTERNAL @%lld LEN=%d KEYS:", n->offset, n->length);
                for(int k=0; k<n->length; k++) {
                    struct keyref *kr = &n->data.i.keys[k];
                    i64 min = keyref_min(me, kr, NULL);
                    printf(" [%d:O:%lld L:%lld R:%lld Min:%lld]", k, kr->offset, kr->left, kr->right, min);
                    if (kr->left != OFFSET_NULL && kr->left > 0)
                        queue[next_qlen++] = bplustree_node_read(me, kr->left, NULL);
                    if (kr->right != OFFSET_NULL && kr->right > 0)
                        queue[next_qlen++] = bplustree_node_read(me, kr->right, NULL);
                }
                printf("\n");
            }
        }
        memcpy(queue, &queue[qlen], sizeof(struct node*) * next_qlen);
        qlen = next_qlen;
    }
}   
#endif // NDEBUG - debug functions


static int bplustree_wal_refresh(const void *obj, i64 offset) {
    struct bplustree *me = (struct bplustree*)obj;
    assert(me);
    struct hashmap *cache = me->cache;
    assert(cache);

    cache->remove(cache, offset);
    return 0; // success
}

static int hashmap_i64_cmpr(keytype k1, keytype k2) {
	if (k1 > k2) return 1;
	if (k1 < k2) return -1;
	return 0;
}

int bplustree_init(
    struct bplustree *me, 
    const char *file,
    int cache_limit,
    enum flintdb_open_mode mode,
    const char *type, // STORAGE TYPE
    void *obj, // compare object
    int (*compare)(void *obj, i64 a, i64 b),
    struct wal *wal,
    char **e) {

    assert(me);
    assert(file);
    assert(compare);

    me->root = NULL;
    me->compare = compare;
    me->obj = obj;
    me->count = 0;
    me->close = bplustree_close;
    me->count_get = bplustree_count_get;
    me->bytes_get = bplustree_bytes_get;
    me->put = bplustree_put;
    me->get = bplustree_get;
    me->delete = bplustree_delete;
    me->find = bplustree_find;
    me->compare_get = bplustree_compare_get;

    struct storage_opts opts;
    memset(&opts, 0, sizeof(opts));
    strncpy(opts.file, file, PATH_MAX-1);
    opts.mode = mode;
    opts.block_bytes = NODE_BYTES;
    opts.increment = DEFAULT_INCREMENT_BYTES;

    me->storage = wal_wrap(wal, &opts, bplustree_wal_refresh, me, e);
    if (e && *e) THROW(e, "wal_wrap failed: %s", *e ? *e : "unknown");
    if (me->storage == NULL) THROW(e, "CALLOC failed");

    cache_limit = (cache_limit <= 0) ? DEFAULT_BPLUSTREE_CACHE_LIMIT : cache_limit;
    if (cache_limit < DEFAULT_BPLUSTREE_CACHE_MIN)
        cache_limit = DEFAULT_BPLUSTREE_CACHE_MIN;
    me->cache = lruhashmap_new(cache_limit * 2, cache_limit, &hashmap_int_hash, &hashmap_i64_cmpr);

    me->header = me->storage->head(me->storage, 0, HEAD_BYTES, e);
    if (e && *e) THROW(e, "storage mmap failed: %s", *e ? *e : "unknown");

    struct buffer h = {0};
    me->header->slice(me->header, 0, HEAD_BYTES, &h, e);

    i8 x = h.i8_get(&h, e);
    h.clear(&h);

    char magic[4] = {'B', '+', 'T', '1'};
    if ('B' == x) {
        char *h_magic = h.array_get(&h, 4, e);
        if (0 != memcmp(magic, h_magic, 4)) THROW(e, "Bad Signature : %s", file);

        me->count = h.i64_get(&h, e);
        bplustree_root_get(me, e);
        if (e && *e) THROW(e, "bplustree_root_get failed: %s", *e ? *e : "unknown");
    } else {
        // New B+Tree
        h.array_put(&h, magic, 4, e);
        h.i64_put(&h, 0L, e); // count
        bplustree_root_set(me, NULL, e);
        if (e && *e) THROW(e, "bplustree_root_set failed: %s", *e ? *e : "unknown");
    }

    return 0;

    EXCEPTION:
    bplustree_close(me);
    return -1;
}