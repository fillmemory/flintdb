/**
 * @file hashmap.h
 * @brief Unified hashmap interface and implementations.
 * @note This data structure is intentionally designed to handle only positive numbers, strings, and pointer types.
 */
#ifndef FLINTDB_HASHMAP_H
#define FLINTDB_HASHMAP_H

#include <stdlib.h>
#include "types.h"


#ifndef HASHMAP_ENTRY_POOL_LIMIT
#define HASHMAP_ENTRY_POOL_LIMIT 65536 // max recycled entries kept per map
#endif

#define HASHMAP_INVALID_VAL ((u64)(-1))
#define HASHMAP_CAST_KEY(k) (keytype)k
#define HASHMAP_CAST_VAL(v) (valtype)v

#define HASHMAP_KEY_TO_STR(k) ((const char *)(k))

struct map_iterator {
    keytype key;
    valtype val;
    u32 nth;
    u32 i; // iterator init flag
    void *cur; // internal cursor (flat entry*)
};


struct flat_entry;

// Unified flat open-addressing hashmap (formerly flat_hashmap) renamed to struct hashmap.
struct hashmap {
    u32 capacity;   // total slots (power of 2)
    u32 mask;       // capacity - 1
    u32 count;      // occupied slots
    u32 max_size;   // optional LRU limit (0 = unlimited)

    u32 (*hash)(keytype k);
    i32 (*compare)(keytype k1, keytype k2);

    struct flat_entry *entries; // slot array
    struct flat_entry *head;    // LRU/list sentinel
    struct flat_entry *tail;    // LRU/list sentinel

    u8  batch_mode;  // skip list maintenance when bulk inserting
    i8  move_on_get; // 1: move accessed entry to MRU (true LRU)

    void *priv; // treemap or extensions can use this

    // Operations
    valtype (*get)(struct hashmap *, keytype);
    void *  (*put)(struct hashmap *, keytype, valtype, void (*dealloc)(keytype, valtype));
    int     (*remove)(struct hashmap *, keytype);
    void    (*clear)(struct hashmap *);
    int     (*count_get)(struct hashmap *);
    void    (*free)(struct hashmap *);
    int     (*iterate)(struct hashmap *, struct map_iterator *);
};


struct hashmap * hashmap_new(u32 hashsize, u32 (*hash)(keytype k), i32 (*compare)(keytype k1, keytype k2));
struct hashmap * linkedhashmap_new(u32 hashsize, u32 (*hash)(keytype k), i32 (*compare)(keytype k1, keytype k2));
struct hashmap * lruhashmap_new(u32 hashsize, u32 max_size, u32 (*hash)(keytype k), i32 (*compare)(keytype k1, keytype k2));
struct hashmap * treemap_new(i32 (*compare)(keytype k1, keytype k2));

//
u32 hashmap_string_hash(keytype k);
int hashmap_string_cmpr(keytype k1, keytype k2);

u32 hashmap_string_case_hash(keytype k); // case-insensitive
int hashmap_string_case_cmpr(keytype k1, keytype k2); // case-insensitive

u32 hashmap_int_hash(keytype k);
int hashmap_int_cmpr(keytype k1, keytype k2);

u32 hashmap_pointer_hash(keytype k);
int hashmap_pointer_cmpr(keytype k1, keytype k2);


#endif // FLINTDB_HASHMAP_H