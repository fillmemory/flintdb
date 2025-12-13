#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "rbtree.h"
#include "allocator.h"

// Inline helpers for performance
#define rb_is_red(node) ((node) != NULL && (node)->color == RED)
#define rb_size(node) ((node) == NULL ? 0 : (node)->size)

// Node pool for fast allocation/deallocation
#if RBTREE_NODE_POOL_LIMIT > 0
static struct rbnode *rb_node_alloc(struct rbtree *tree) {
    if (tree->pool) {
        struct rbnode *node = (struct rbnode *)tree->pool;
        tree->pool = ((struct rbnode *)tree->pool)->left; // reuse left as next pointer
        tree->pool_size--;
        memset(node, 0, sizeof(*node));
        return node;
    }
    return (struct rbnode *)CALLOC(1, sizeof(struct rbnode));
}

static void rb_node_pool_free(struct rbtree *tree, struct rbnode *node) {
    if (!node) return;
    if (tree->pool_size < tree->pool_limit) {
        // push to pool
        node->left = (struct rbnode *)tree->pool;
        tree->pool = node;
        tree->pool_size++;
    } else {
        FREE(node);
    }
}
#else
#define rb_node_alloc(tree) ((struct rbnode*)CALLOC(1, sizeof(struct rbnode)))
#define rb_node_pool_free(tree, node) do { if (node) FREE(node); } while(0)
#endif

struct rbnode *rb_node_new(struct rbtree *tree, keytype key, valtype val, rb_color color, i64 size, void (*dealloc)(keytype, valtype)) {
    struct rbnode *node = rb_node_alloc(tree);
    node->key = key;
    node->val = val;
    node->left = node->right = NULL;
    node->color = color;
    node->size = size;
    node->dealloc = dealloc;
    return node;
}

static inline void rb_flip_colors(struct rbnode *h) {
    h->color = !h->color;
    h->left->color = !h->left->color;
    h->right->color = !h->right->color;
}

static inline i64 rb_count_get(struct rbtree *tree) {
    return rb_size(tree->root);
}

static void rb_node_free(struct rbtree *tree, struct rbnode *x) {
    if (x == NULL) return;
    rb_node_free(tree, x->left);
    rb_node_free(tree, x->right);
    if (x->dealloc) x->dealloc(x->key, x->val);
    rb_node_pool_free(tree, x);
}

static void rbtree_free(struct rbtree *tree) {
    rb_node_free(tree, tree->root);
    
    // Free all pooled nodes
    #if RBTREE_NODE_POOL_LIMIT > 0
    while (tree->pool) {
        struct rbnode *node = (struct rbnode *)tree->pool;
        tree->pool = node->left;
        FREE(node);
    }
    #endif
    
    FREE(tree);
}


static inline struct rbnode *rb_node_get(struct rbtree *tree, struct rbnode *x, keytype key) {
    // Iterative search - no recursion overhead
    while (x != NULL) {
        keytype node_key = x->key;
        // Use compare function if provided, otherwise direct comparison for integer keys
        int cmp;
        if (tree->compare) {
            cmp = tree->compare(key, node_key);
        } else {
            // Direct comparison for integer keys (avoid function call overhead)
            if (key < node_key) cmp = -1;
            else if (key > node_key) cmp = 1;
            else cmp = 0;
        }
        
        if (cmp < 0) {
            x = x->left;
        } else if (cmp > 0) {
            x = x->right;
        } else {
            return x;
        }
    }
    return NULL;
}

static inline struct rbnode *rb_get(struct rbtree *tree, keytype key) {
    return rb_node_get(tree, tree->root, key);
}

static inline struct rbnode *rb_rotate_right(struct rbnode *h) {
    struct rbnode *x = h->left;
    h->left = x->right;
    x->right = h;
    x->color = h->color;
    h->color = RED;
    x->size = h->size;
    h->size = rb_size(h->left) + rb_size(h->right) + 1;
    return x;
}

static inline struct rbnode *rb_rotate_left(struct rbnode *h) {
    struct rbnode *x = h->right;
    h->right = x->left;
    x->left = h;
    x->color = h->color;
    h->color = RED;
    x->size = h->size;
    h->size = rb_size(h->left) + rb_size(h->right) + 1;
    return x;
}

static struct rbnode *rb_node_put(struct rbtree *tree, struct rbnode *h, keytype key, valtype val, void (*dealloc)(keytype, valtype)) {
    if (h == NULL) return rb_node_new(tree, key, val, RED, 1, dealloc);
    
    // Use compare function if provided, otherwise direct integer comparison
    keytype node_key = h->key;
    int cmp;
    if (tree->compare) {
        cmp = tree->compare(key, node_key);
    } else {
        // Direct integer comparison (avoid compare function overhead)
        if (key < node_key) cmp = -1;
        else if (key > node_key) cmp = 1;
        else cmp = 0;
    }
    
    if (cmp < 0) {
        h->left = rb_node_put(tree, h->left, key, val, dealloc);
    } else if (cmp > 0) {
        h->right = rb_node_put(tree, h->right, key, val, dealloc);
    } else {
        // Update existing key
        if (h->dealloc) h->dealloc(h->key, h->val);
        h->val = val;
        h->dealloc = dealloc;
    }

    // Fix-up any right-leaning links
    if (rb_is_red(h->right) && !rb_is_red(h->left))       h = rb_rotate_left(h);
    if (rb_is_red(h->left)  &&  rb_is_red(h->left->left)) h = rb_rotate_right(h);
    if (rb_is_red(h->left)  &&  rb_is_red(h->right))      rb_flip_colors(h);

    h->size = rb_size(h->left) + rb_size(h->right) + 1;
    return h;
}

static struct rbnode *rb_move_red_left(struct rbnode *h) {
    rb_flip_colors(h);
    if (rb_is_red(h->right->left)) {
        h->right = rb_rotate_right(h->right);
        h = rb_rotate_left(h);
        rb_flip_colors(h);
    }
    return h;
}

static struct rbnode *rb_move_red_right(struct rbnode *h) {
    rb_flip_colors(h);
    if (rb_is_red(h->left->left)) {
        h = rb_rotate_right(h);
        rb_flip_colors(h);
    }
    return h;
}

static struct rbnode *rb_balance(struct rbnode *h) {
    if (rb_is_red(h->right)) h = rb_rotate_left(h);
    if (rb_is_red(h->left) && rb_is_red(h->left->left)) h = rb_rotate_right(h);
    if (rb_is_red(h->left) && rb_is_red(h->right)) rb_flip_colors(h);
    h->size = rb_size(h->left) + rb_size(h->right) + 1;
    return h;
}

static struct rbnode *rb_min(struct rbnode *h) {
    if (h->left == NULL) return h;
    else return rb_min(h->left);
}

static struct rbnode *rb_node_remove_min(struct rbtree *tree, struct rbnode *h) {
    if (h->left == NULL) return NULL;
    if (!rb_is_red(h->left) && !rb_is_red(h->left->left))
        h = rb_move_red_left(h);
    h->left = rb_node_remove_min(tree, h->left);
    return rb_balance(h);
}

struct rbnode *rb_node_remove(struct rbtree *tree, struct rbnode *h, keytype key) {
    // Guard against missing keys leading to null traversal
    if (h == NULL) return NULL;

    // Direct integer comparison for performance
    keytype node_key = h->key;
    if (key < node_key) {
        if (!rb_is_red(h->left) && !rb_is_red(h->left->left))
            h = rb_move_red_left(h);
        h->left = rb_node_remove(tree, h->left, key);
    } else {
        if (rb_is_red(h->left))
            h = rb_rotate_right(h);
        if (key == h->key && (h->right == NULL))
            return NULL;
        if (!rb_is_red(h->right) && !rb_is_red(h->right->left))
            h = rb_move_red_right(h);
        if (key == h->key) {
            struct rbnode *x = rb_min(h->right);
            h->key = x->key;
            h->val = x->val;
            h->right = rb_node_remove_min(tree, h->right);
        } else {
            h->right = rb_node_remove(tree, h->right, key);
        }
    }
    return rb_balance(h);
}

static void rb_put(struct rbtree *tree, keytype key, valtype val, void (*dealloc)(keytype, valtype)) {
    tree->root = rb_node_put(tree, tree->root, key, val, dealloc);
    tree->root->color = BLACK;
}

static void rb_remove(struct rbtree *tree, keytype key) {
    if (!rb_is_red(tree->root->left) && !rb_is_red(tree->root->right))
        tree->root->color = RED;

    tree->root = rb_node_remove(tree, tree->root, key);
    if (tree->root != NULL) tree->root->color = BLACK;
}

static void rb_clear(struct rbtree *tree) {
    rb_node_free(tree, tree->root);
    tree->root = NULL;
}

static void rb_node_dump(struct rbnode *node) {
    if (node == NULL) return;
    rb_node_dump(node->left);
    printf("key: %llu, val: %llu, color: %s\n", node->key, node->val, node->color == RED ? "RED" : "BLACK");
    rb_node_dump(node->right);
}

void rbtree_dump(struct rbtree *tree) {
    rb_node_dump(tree->root);
}

struct rbtree *rbtree_new(i32 (*compare)(keytype a, keytype b)) {
    struct rbtree *tree = (struct rbtree *)CALLOC(1, sizeof(struct rbtree));
    tree->compare = compare;

    tree->free = rbtree_free;
    tree->clear = rb_clear;
    tree->count_get = rb_count_get;
    tree->get = rb_get;
    tree->put = rb_put;
    tree->remove = rb_remove;

    // Initialize node pool
    #if RBTREE_NODE_POOL_LIMIT > 0
    tree->pool = NULL;
    tree->pool_size = 0;
    tree->pool_limit = RBTREE_NODE_POOL_LIMIT;
    #endif

    return tree;
}

