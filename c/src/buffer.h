// buffer.h
// Byte buffer abstraction with read/write methods
// Byte order: little-endian
//
#ifndef FLINTDB_BUFFER_H
#define FLINTDB_BUFFER_H

#include "types.h"

// Buffer ownership sentinel values (must never collide with real pointers)
// Used in buffer->owner to mark internal ownership states.
#define BUFFER_OWNER_SLICE_HEAP ((void *)1)

struct buffer {
	char *array;
	u32 position;
	u32 limit;
	u32 capacity;
	struct {
		void *addr;
		u32 length;
	} mapped; // for mmap

	// Optional owner pointer for custom free behavior.
	// - NULL: normal buffer (free behavior defined by ->free)
	// - buffer_pool*: pooled buffer (->free returns to pool)
	// - small sentinel values: internal markers (e.g., heap-allocated slice struct)
	void *owner;

	void (*flip)(struct buffer *me);
	void (*clear)(struct buffer *me);
	i32 (*remaining)(struct buffer *me);
	i32 (*skip)(struct buffer *me, i32 n);
	void (*array_put)(struct buffer *me, const char *bytes, u32 len, char **e);
	// array_put does not modify source bytes; accept const for better const-correctness
	// (Note: keep function pointer type in sync with implementation in buffer.c)
	char* (*array_get)(struct buffer *me, u32 len, char **e);
	void (*i8_put)(struct buffer *me, char v, char **e);
	void (*i16_put)(struct buffer *me, i16 v, char **e);
	void (*i32_put)(struct buffer *me, i32 v, char **e);
	void (*i64_put)(struct buffer *me, i64 v, char **e);
	void (*f64_put)(struct buffer *me, f64 v, char **e);
	char (*i8_get)(struct buffer *me, char **e);
	i16 (*i16_get)(struct buffer *me, char **e);
	i32 (*i32_get)(struct buffer *me, char **e);
	i64 (*i64_get)(struct buffer *me, char **e);
	f64 (*f64_get)(struct buffer *me, char **e);

	void (*realloc)(struct buffer *me, i32 size);
	void (*free)(struct buffer *me);

	void (*slice)(struct buffer *me, i32 offset, i32 length, struct buffer *out, char **e);
};

struct buffer * buffer_wrap(char *array, u32 capacity, struct buffer *out);
// struct buffer * buffer_slice(struct buffer *in, i32 offset, i32 length, struct buffer *out);

struct buffer * buffer_mmap(void *addr, u32 offset, u32 length);

struct buffer * buffer_alloc(u32 capacity);

// Allocate a buffer whose backing array is aligned to `alignment` bytes.
// Useful for Linux O_DIRECT which requires strict alignment.
// Note: capacity may be rounded up to a multiple of alignment.
struct buffer * buffer_alloc_aligned(u32 capacity, u32 alignment);

struct buffer * buffer_slice(struct buffer *in, i32 offset, i32 length, char **e);

const char * dump_as_hex(const char *in, int offset, int len, int width, char *out);



struct buffer_pool {
    int capacity;           // total slots
    int top;                // current count (stack top)
    struct buffer **items;  // buffers
    int align;               // buffer alignment size

    struct buffer * (*borrow)(struct buffer_pool *pool, u32 buf_size);
    void (*return_buffer)(struct buffer_pool *pool, struct buffer *b);
    void (*free)(struct buffer_pool *pool);
};

struct buffer_pool * buffer_pool_create(u32 capacity, u32 align, u32 preload);

// Thread-safe wrapper around buffer_pool. Uses a mutex for borrow/return operations.
struct buffer_pool_safe {
	struct buffer_pool *pool; // underlying non-thread-safe pool
	void *mtx; // opaque pointer to platform mutex (pthread_mutex_t or CRITICAL_SECTION)

	struct buffer * (*borrow)(struct buffer_pool_safe *pool, u32 buf_size);
	void (*return_buffer)(struct buffer_pool_safe *pool, struct buffer *b);
	void (*free)(struct buffer_pool_safe *pool);
};

// Create a thread-safe buffer pool with given capacity, minimum buffer size (align) and preload count.
struct buffer_pool_safe * buffer_pool_safe_create(u32 capacity, u32 align, u32 preload);


struct string_pool {
    int capacity;           // total slots
    int top;                // current count (stack top)
    char **items;          // strings
    u32 str_size;          // size of each string

    char * (*borrow)(struct string_pool *pool);
    void (*return_string)(struct string_pool *pool, char *s);
    void (*free)(struct string_pool *pool);
};

struct string_pool * string_pool_create(u32 capacity, u32 str_size, u32 preload);

#endif // FLINTDB_BUFFER_H