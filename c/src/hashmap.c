#include <assert.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>

#include "allocator.h"
#include "hashmap.h"
#include "runtime.h"


// Unified flat open-addressing implementation.
static inline int is_power_of_two(u32 x) { 
    return x && ((x & (x - 1)) == 0); 
}
static inline u32 hashmap_index(struct hashmap *map, u32 h) { 
    return map->mask ? (h & map->mask) : (h % map->capacity); 
}
static inline u32 hashmap_next(struct hashmap *map, u32 idx) {
    // Next index with wrap-around, supports both pow2 and non-pow2 capacity
    return map->mask ? ((idx + 1) & map->mask) : ((idx + 1) % map->capacity);
}

u32 hashmap_string_hash(keytype k) {
    const char *s = (const char *)k;
    // Use unsigned arithmetic to avoid UB on signed overflow.
    u32 hash = 0;
    unsigned char c;
    while ((c = (unsigned char)*s++) != 0) {
        // djb2 variant: hash = hash * 33 + c
        hash = (hash * 33u) + (u32)c;
    }
    return hash;
}

int hashmap_string_cmpr(keytype k1, keytype k2) {
    return strcmp((const char *)k1, (const char *)k2);
}

u32 hashmap_string_case_hash(keytype k) {
    const char *s = (const char *)k;
    u32 hash = 0;
    int c;
    for (; (c = *s++) != 0;)
        hash = ((hash << 5) + hash) + tolower(c);
    return hash;
}

int hashmap_string_case_cmpr(keytype k1, keytype k2) {
    return strcasecmp((const char *)k1, (const char *)k2);
}

u32 hashmap_int_hash(keytype k) {
    // Fibonacci hashing to better spread sequential integers over power-of-two buckets
    // 2654435761 is 2^32 / golden_ratio
    return (u32)((u32)k * 2654435761u);
}

int hashmap_int_cmpr(keytype k1, keytype k2) {
    if (k1 > k2)
        return 1;
    if (k1 < k2)
        return -1;
    return 0;
}

u32 hashmap_pointer_hash(keytype k) {
    return (u32)k;
}

int hashmap_pointer_cmpr(keytype k1, keytype k2) {
    if (k1 > k2)
        return 1;
    if (k1 < k2)
        return -1;
    return 0;
}

// --

struct flat_entry {
    keytype k;
    valtype v;
    void (*dealloc)(keytype, valtype);
    struct flat_entry *left;  // LRU prev
    struct flat_entry *right; // LRU next
    u8 occupied;              // 0 empty, 1 occupied, 2 tombstone
};


static inline void flat_entry_dealloc(struct flat_entry *e) {
    if (e && e->dealloc) {
        e->dealloc(e->k, e->v);
    }
}

// Linear probing helper - optimized version with tombstone support
static struct flat_entry* flat_find_slot(struct hashmap *map, keytype key, u32 *out_idx) {
    u32 hash = map->hash(key);
    u32 idx = hashmap_index(map, hash);
    u32 start = idx;
    u32 first_tombstone = (u32)-1; // Track first tombstone for insertion
    
    // Unroll first few probes for common case
    struct flat_entry *e = &map->entries[idx];
    if (e->occupied == 0) { // empty
        if (out_idx) *out_idx = (first_tombstone != (u32)-1) ? first_tombstone : idx;
        return NULL;
    }
    if (e->occupied == 2 && first_tombstone == (u32)-1) { // tombstone
        first_tombstone = idx;
    }
    if (e->occupied == 1 && map->compare(e->k, key) == 0) { // found
        if (out_idx) *out_idx = idx;
        return e;
    }
    
    // Continue probing
    idx = hashmap_next(map, idx);
    while (idx != start) {
        e = &map->entries[idx];
        if (e->occupied == 0) { // empty - end of probe chain
            if (out_idx) *out_idx = (first_tombstone != (u32)-1) ? first_tombstone : idx;
            return NULL;
        }
        if (e->occupied == 2 && first_tombstone == (u32)-1) { // tombstone
            first_tombstone = idx;
        }
        if (e->occupied == 1 && map->compare(e->k, key) == 0) { // found
            if (out_idx) *out_idx = idx;
            return e;
        }
        idx = hashmap_next(map, idx);
    }
    
    if (out_idx) *out_idx = (first_tombstone != (u32)-1) ? first_tombstone : start;
    return NULL; // table full or not found
}

// LRU helpers
static inline void linkedlist_add(struct hashmap *map, struct flat_entry *e) {
    // Optimized: minimize memory writes
    struct flat_entry *prev = map->tail->left;
    prev->right = e;
    map->tail->left = e;
    e->left = prev;
    e->right = map->tail;
}

static inline void linkedlist_remove(struct hashmap *map, struct flat_entry *e) {
    if (e->left) e->left->right = e->right;
    if (e->right) e->right->left = e->left;
    e->left = e->right = NULL;
}

HOT_PATH
static valtype hashmap_get(struct hashmap *map, keytype key) {
    
    u32 hash = map->hash(key);
    u32 idx = hashmap_index(map, hash);
    u32 start = idx;

    // Hot path: inline first lookups
    struct flat_entry *e = &map->entries[idx];
    if (e->occupied == 1 && map->compare(e->k, key) == 0) {
        // Move to end of LRU list (most recently used) - only if LRU enabled
        if (map->move_on_get && map->max_size > 0) {
            linkedlist_remove(map, e);
            linkedlist_add(map, e);
        }
        valtype result = e->v;
        return result;
    }
    if (e->occupied == 0) {
        return HASHMAP_INVALID_VAL;  // Empty - not found
    }

    // Probe next slots
    idx = hashmap_next(map, idx);
    while (idx != start) {
        e = &map->entries[idx];
        if (e->occupied == 0) {
            return HASHMAP_INVALID_VAL; // Empty - end of chain
        }
        if (e->occupied == 1 && map->compare(e->k, key) == 0) {
            // Move to end of LRU list (most recently used) - only if LRU enabled
            if (map->move_on_get && map->max_size > 0) {
                linkedlist_remove(map, e);
                linkedlist_add(map, e);
            }
            valtype result = e->v;
            return result;
        }
        idx = hashmap_next(map, idx);
    }

    return HASHMAP_INVALID_VAL;
}

HOT_PATH
static void * hashmap_put(struct hashmap *map, keytype key, valtype val, void (*dealloc)(keytype k, valtype v)) {
    
    u32 hash = map->hash(key);
    u32 idx = hashmap_index(map, hash);
    
    // Fast path: direct insert for sequential integer keys with good hash
    struct flat_entry *e = &map->entries[idx];
    if (e->occupied == 0 || e->occupied == 2) { // empty or tombstone - can insert here
        // Empty slot or reuse tombstone - fast insert without probing
        e->k = key;
        e->v = val;
        e->dealloc = dealloc;
        e->occupied = 1;
        
        // LRU list: only add, never move (insertion order like Java)
        linkedlist_add(map, e);
        
        // Increment count AFTER adding to list
        map->count++;
        if (map->max_size && map->count > map->max_size) {
            // Evict oldest entry using backward-shift deletion (no tombstones)
            struct flat_entry *lru = map->head->right;
            if (lru != map->tail) {
                // Remove from list and free value
                linkedlist_remove(map, lru);
                flat_entry_dealloc(lru);
                // Compute index and perform backward-shift compaction
                u32 i = (u32)(lru - map->entries);
                u32 j = hashmap_next(map, i);
                while (map->entries[j].occupied == 1) {
                    u32 h = hashmap_index(map, map->hash(map->entries[j].k));
                    if ((i <= j && (h <= i || h > j)) || (i > j && (h <= i && h > j))) {
                        map->entries[i] = map->entries[j];
                        i = j;
                    }
                    j = hashmap_next(map, j);
                }
                map->entries[i].occupied = 0;
                map->entries[i].k = 0;
                map->entries[i].v = 0;
                map->entries[i].dealloc = NULL;
                map->count--;
            }
        }
        return e;
    }

    // Slow path: collision or update
    u32 probe_idx;
    struct flat_entry *found = flat_find_slot(map, key, &probe_idx);
    
    if (found) { // update existing - move to end of LRU list (access order)
        if (found->k != key || found->v != val) {
            flat_entry_dealloc(found);
            found->k = key;
            found->v = val;
            found->dealloc = dealloc;
        }
        // Move to end for LRU behavior (most recently used) - only if LRU enabled
        if (map->move_on_get && map->max_size > 0) {
            linkedlist_remove(map, found);
            linkedlist_add(map, found);
        }
        return found;
    }
    
    // insert at probed position
    if (map->count >= (u32)(map->capacity * 0.75)) {
        return NULL; // table full
    }
    
    e = &map->entries[probe_idx];
    e->k = key;
    e->v = val;
    e->dealloc = dealloc;
    e->occupied = 1;
    
    linkedlist_add(map, e);
    
    // Increment count AFTER adding to list
    map->count++;
    if (map->max_size && map->count > map->max_size) {
        // Evict oldest (backward-shift)
        struct flat_entry *lru = map->head->right;
        if (lru != map->tail) {
            linkedlist_remove(map, lru);
            flat_entry_dealloc(lru);
            u32 i = (u32)(lru - map->entries);
            u32 j = hashmap_next(map, i);
            while (map->entries[j].occupied == 1) {
                u32 h = hashmap_index(map, map->hash(map->entries[j].k));
                if ((i <= j && (h <= i || h > j)) || (i > j && (h <= i && h > j))) {
                    map->entries[i] = map->entries[j];
                    i = j;
                }
                j = hashmap_next(map, j);
            }
            map->entries[i].occupied = 0;
            map->entries[i].k = 0;
            map->entries[i].v = 0;
            map->entries[i].dealloc = NULL;
            map->count--;
        }
    }
    
    return e;
}

HOT_PATH
static int hashmap_remove(struct hashmap *map, keytype key) {
    
    u32 idx;
    struct flat_entry *e = flat_find_slot(map, key, &idx);
    
    if (!e) {
        return 0; // not found
    }

    // Remove from LRU list first (list is independent of probing layout)
    linkedlist_remove(map, e);

    // Deallocate value of the removed entry
    flat_entry_dealloc(e);

    // Backward-shift deletion to avoid tombstones for frequent deletes
    // Compute index of the entry pointer
    u32 i = (u32)(e - map->entries);
    u32 j = hashmap_next(map, i);
    while (map->entries[j].occupied == 1) {
        // Home bucket of entry at j
        u32 h = hashmap_index(map, map->hash(map->entries[j].k));
        // If the probe chain of j wraps over i, move j back to i
        // Condition: (i <= j && (h <= i || h > j)) || (i > j && (h <= i && h > j))
        if ((i <= j && (h <= i || h > j)) || (i > j && (h <= i && h > j))) {
            map->entries[i] = map->entries[j];
            i = j;
        }
        j = hashmap_next(map, j);
    }
    // Clear the final slot
    map->entries[i].occupied = 0;
    map->entries[i].k = 0;
    map->entries[i].v = 0;
    map->entries[i].dealloc = NULL;

    map->count--;

    return 1; // success
}

static void hashmap_clear(struct hashmap *map) {
    if (!map) return;
    
    
    for (u32 i = 0; i < map->capacity; i++) {
        struct flat_entry *e = &map->entries[i];
        if (e->occupied == 1) { // Only clear occupied slots
            flat_entry_dealloc(e);
        }
        e->occupied = 0; // Reset to empty (not tombstone)
        e->k = 0;
        e->v = 0;
        e->dealloc = NULL;
    }
    
    // Reset LRU list
    map->tail->left = map->head;
    map->head->right = map->tail;
    map->count = 0;
    
}

static int hashmap_count_get(struct hashmap *map) {
    int count = map->count;
    return count;
}

static void hashmap_free(struct hashmap *map) {
    if (!map) return;
    
    hashmap_clear(map);
    
    if (map->entries) FREE(map->entries);
    if (map->head) FREE(map->head);
    if (map->tail) FREE(map->tail);
    
    FREE(map);
}

// Iterator helpers
static int hashmap_iterate_impl(struct hashmap *map, struct map_iterator *itr) {
    if (!map || !itr) return -1;
    
    
    if (itr->i == 0) {
        // Initialize iterator
        itr->nth = 0;
        itr->cur = (void *)map->head->right; // first real entry
        itr->i = 1;
    } else {
        // Advance to next
        struct flat_entry *cur_entry = (struct flat_entry *)itr->cur;
        if (cur_entry == map->tail) {
            // End of iteration
            return 0;
        }
        cur_entry = cur_entry->right;
        itr->cur = (void *)cur_entry;
    }

    struct flat_entry *cur_entry = (struct flat_entry *)itr->cur;
    if (cur_entry == map->tail) {
        // End of iteration
        return 0;
    }

    itr->key = cur_entry->k;
    itr->val = cur_entry->v;
    itr->nth++;

    return 1; // success
}

static struct hashmap * hashmap_alloc_internal(u32 capacity, u32 max_size, u32 (*hash)(keytype), i32 (*compare)(keytype, keytype), i8 move_on_get) {
	struct hashmap *map = (struct hashmap *) CALLOC(1, sizeof(struct hashmap));

    // Basic parameters
	map->capacity = capacity ? capacity : 16u;
	map->max_size = max_size;
    map->mask = is_power_of_two(map->capacity) ? (map->capacity - 1) : 0;
	map->move_on_get = move_on_get;

    // Initialize mutex

    // Function pointers
    map->hash = hash;
    map->compare = compare;

    // Allocate slots and LRU sentinels
    map->entries = (struct flat_entry *)CALLOC(map->capacity, sizeof(struct flat_entry));
    map->head = (struct flat_entry *)CALLOC(1, sizeof(struct flat_entry));
    map->tail = (struct flat_entry *)CALLOC(1, sizeof(struct flat_entry));

    // Initialize LRU/list sentinels
    map->head->left = NULL;
    map->head->right = map->tail;
    map->tail->left = map->head;
    map->tail->right = NULL;

    map->count = 0;

	map->get = hashmap_get;
	map->put = hashmap_put;
	map->remove = hashmap_remove;
	map->clear = hashmap_clear;
	map->count_get = hashmap_count_get;
	map->free = hashmap_free;
	map->iterate = hashmap_iterate_impl;
	return map;
}

struct hashmap *hashmap_new(u32 hashsize, u32 (*hash)(keytype k), i32 (*compare)(keytype k1, keytype k2)) {
    return hashmap_alloc_internal(hashsize, 0, hash, compare, 0);
}

struct hashmap *linkedhashmap_new(u32 hashsize, u32 (*hash)(keytype k), i32 (*compare)(keytype k1, keytype k2)) {
    // insertion order retained by disabling move_on_get
    return hashmap_alloc_internal(hashsize, 0, hash, compare, 0);
}

struct hashmap *lruhashmap_new(u32 hashsize, u32 max_size, u32 (*hash)(keytype k), i32 (*compare)(keytype k1, keytype k2)) {
    return hashmap_alloc_internal(hashsize, max_size, hash, compare, 1);
}



// -- tree map (not implemented yet) --
#include "rbtree.h"


// --- Tree map support implementation ---
struct tree_iter_state {
    struct rbnode **nodes;
    u32 total;
    u32 idx;
};

static void tree_fill_nodes(struct rbnode *root, struct rbnode **arr, u32 *i) {
    if (!root) return;
    tree_fill_nodes(root->left, arr, i);
    arr[(*i)++] = root;
    tree_fill_nodes(root->right, arr, i);
}

static valtype treemap_get(struct hashmap *m, keytype key) {
    struct rbtree *t = (struct rbtree *)m->priv;
    if (!t) {
        return HASHMAP_INVALID_VAL;
    }
    struct rbnode *n = t->get(t, key);
    valtype result = n ? n->val : HASHMAP_INVALID_VAL;
    return result;
}

static void * treemap_put(struct hashmap *m, keytype key, valtype val, void (*dealloc)(keytype, valtype)) {
    struct rbtree *t = (struct rbtree *)m->priv;
    if (!t) {
        return NULL;
    }
    t->put(t, key, val, dealloc);
    m->count = (u32)t->count_get(t);
    void *result = (void *)t->get(t, key);
    return result;
}

static int treemap_remove(struct hashmap *m, keytype key) {
    struct rbtree *t = (struct rbtree *)m->priv;
    if (!t) {
        return 0;
    }
    struct rbnode *n = t->get(t, key);
    if (!n) {
        return 0;
    }
    t->remove(t, key);
    m->count = (u32)t->count_get(t);
    return 1;
}

static void treemap_clear(struct hashmap *m) {
    struct rbtree *t = (struct rbtree *)m->priv;
    if (t) t->clear(t);
    m->count = 0;
}

static int treemap_count_get(struct hashmap *m) {
    struct rbtree *t = (struct rbtree *)m->priv;
    int count = t ? (int)t->count_get(t) : 0;
    return count;
}

static int treemap_iterate(struct hashmap *m, struct map_iterator *itr) {
    if (!m || !itr) return -1;
    
    
    struct rbtree *t = (struct rbtree *)m->priv;
    if (!t) {
        return -1;
    }

    if (itr->i == 0) {
        itr->nth = 0;
        itr->i = 1;
        u32 total = (u32)t->count_get(t);
        struct tree_iter_state *st = (struct tree_iter_state *)CALLOC(1, sizeof(struct tree_iter_state));
        st->total = total;
        st->idx = 0;
        if (total) {
            st->nodes = (struct rbnode **)CALLOC(total, sizeof(struct rbnode *));
            u32 pos = 0;
            tree_fill_nodes(t->root, st->nodes, &pos);
        }
        itr->cur = (void *)st;
    } else {
        struct tree_iter_state *st = (struct tree_iter_state *)itr->cur;
        if (!st) return 0;
        st->idx++;
    }

    struct tree_iter_state *st = (struct tree_iter_state *)itr->cur;
    if (!st) {
        return 0;
    }
    if (st->idx >= st->total) {
        if (st->nodes) FREE(st->nodes);
        FREE(st);
        itr->cur = NULL;
        return 0;
    }
    struct rbnode *n = st->nodes[st->idx];
    itr->key = n->key;
    itr->val = n->val;
    itr->nth++;
    return 1;
}

static void treemap_free(struct hashmap *m) {
    if (!m) return;
    struct rbtree *t = (struct rbtree *)m->priv;
    if (t) t->free(t);
    FREE(m);
}

struct hashmap * treemap_new(i32 (*compare)(keytype k1, keytype k2)) {
    struct rbtree *tree = rbtree_new(compare);
    if (!tree) return NULL;
    struct hashmap *map = (struct hashmap *)CALLOC(1, sizeof(struct hashmap));
    if (!map) { tree->free(tree); return NULL; }
    
    
    map->capacity = 0;
    map->mask = 0;
    map->count = 0;
    map->max_size = 0;
    map->hash = NULL;
    map->compare = compare;
    map->entries = NULL;
    map->head = NULL;
    map->tail = NULL;
    map->batch_mode = 0;
    map->move_on_get = 0;
    map->priv = (void *)tree;
    map->get = treemap_get;
    map->put = treemap_put;
    map->remove = treemap_remove;
    map->clear = treemap_clear;
    map->count_get = treemap_count_get;
    map->iterate = treemap_iterate;
    map->free = treemap_free;
    return map;
}