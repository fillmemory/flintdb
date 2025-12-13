#ifndef FLINTDB_RBTREE_H
#define FLINTDB_RBTREE_H

/**
 * @reference https://algs4.cs.princeton.edu/33balanced/RedBlackBST.java
 */

#include "types.h"

// Performance knobs
#ifndef RBTREE_NODE_POOL_LIMIT
#define RBTREE_NODE_POOL_LIMIT 8192 // max recycled nodes kept per tree
#endif

typedef enum { RED, BLACK } rb_color;

struct rbnode {
    keytype key;
    valtype val;
    struct rbnode *left, *right;
    rb_color color;
    i64 size;
	void (*dealloc)(keytype key, valtype val);
};

struct rbtree {
    struct rbnode *root;

	i32 (*compare)(keytype a, keytype b);

	void (*free)(struct rbtree *tree);
    void (*clear)(struct rbtree *);
	i64 (*count_get)(struct rbtree *);
	struct rbnode * (*get)(struct rbtree *, keytype);
	void (*put)(struct rbtree *, keytype, valtype, void (*dealloc)(keytype, valtype));
	void (*remove)(struct rbtree *, keytype);

	// Node pool for fast allocation/deallocation
	void *pool;
	u32 pool_size;
	u32 pool_limit;
};

struct rbtree *rbtree_new(i32 (*compare)(keytype, keytype));

#endif