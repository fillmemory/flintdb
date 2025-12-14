/**
 * @file bplustree.h
 * @brief B+Tree data structure interface
 * @note This data structure is intentionally designed to handle only offset.
 */
#ifndef FLINTDB_BPLUSTREE_H
#define FLINTDB_BPLUSTREE_H

#include "types.h"
#include "flintdb.h"
// #include "filter.h"
#include "storage.h"
#include "hashmap.h"
#include "buffer.h"
#include "wal.h"


#define NOT_FOUND -1L

/**
 * @brief Order enum for specifying ascending/descending order
 * 
 */
enum order {
    ASC,
    DESC
};

struct node;

struct bplustree {
    struct storage *storage;
    struct hashmap *cache;
    struct buffer *header;
    void *obj; // user object for compare
    int (*compare)(void *obj, i64 a, i64 b);
    i64 count;
    enum flintdb_open_mode mode;
    struct node *root;

    void (*close)(struct bplustree *me);
    i64  (*count_get)(struct bplustree *me);
    i64  (*bytes_get)(struct bplustree *me);

    void (*put)(struct bplustree *me, i64 key, char **e);
    i64  (*get)(struct bplustree *me, i64 key, char **e); // return NOT_FOUND if not found
    i8   (*delete)(struct bplustree *me, i64 key, char **e);

    // Range scan find using a single-argument comparator: returns 0 while values are in range
    struct flintdb_cursor_i64 * (*find)(struct bplustree *me, enum order order, void *obj, int (*cmpr)(void *obj, i64 o), char **e);
    i64 (*compare_get)(struct bplustree *me, void *obj, const void *r, int (*cmpr)(void *obj, const void *a, i64 b), char **e);
};


int bplustree_init(
    struct bplustree *me, 
    const char *file,
    int cache_limit,
    enum flintdb_open_mode mode,
    const char *type, // STORAGE TYPE
    void *obj, // compare object
    int (*compare)(void *obj, i64 a, i64 b),
    struct wal *wal, // write-ahead log
    char **e);


#endif // FLINTDB_BPLUSTREE_H
