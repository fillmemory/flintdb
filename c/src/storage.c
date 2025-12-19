#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <assert.h>
#ifndef _WIN32
#include <sys/mman.h>
#endif

#include <limits.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>


#include "storage.h"



#define COMMON_HEADER_BYTES (8 + 8 + 8 + 2 + 4 + 24 + 2 + 8)
#define CUSTOM_HEADER_BYTES (HEADER_BYTES - COMMON_HEADER_BYTES)
#define DEFAULT_INCREMENT_BYTES (16 * 1024 * 1024)

#define MAPPED_BYTEBUFFER_POOL_SIZE 2048 // 20

// Status markers
#define STATUS_SET '+'
#define STATUS_EMPTY '-'

#define MARK_AS_DATA 'D'
#define MARK_AS_NEXT 'N'   // When data is overflowed
#define MARK_AS_UNUSED 'X' // When data is deleted or unused

#define R24 "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
#define R24LEN 24

#ifdef O_DIRECT
#define EX_O_DIRECT O_DIRECT
#else
#define EX_O_DIRECT 0
#endif

const i64 NEXT_END = -1;

static i64 storage_count_get(struct storage *me) {
    return me->count;
}

static i64 storage_bytes_get(struct storage *me) {
    return file_length(me->opts.file);
}

static void storage_commit(struct storage *me, u8 force, char **e) {
    assert(me);

    if (me->opts.mode != FLINTDB_RDWR) return;
    if (!force && me->dirty <= 0) {
        me->dirty++;
        return;
    }
    me->dirty = 0;

    assert(me->head);
    
    struct buffer *h = me->h;
    struct buffer bb = {0};
    h->slice(h, CUSTOM_HEADER_BYTES, COMMON_HEADER_BYTES, &bb, e);
    bb.i64_put(&bb, 0, e); // reserved
    bb.i64_put(&bb, me->free, e); // The front of deleted blocks
    bb.i64_put(&bb, 0, e); // The tail of deleted blocks => not used in mmap
    bb.i16_put(&bb, 1, e); // version
    bb.i32_put(&bb, me->increment, e); // increment chunk size
    bb.array_put(&bb, R24, R24LEN, e); // reserved
    bb.i16_put(&bb, me->opts.block_bytes, e); // BLOCK Data Max Size (exclude BLOCK Header)
    bb.i64_put(&bb, me->count, e); // number of blocks
}

static void storage_cache_free(keytype k, valtype v) {
    struct buffer *buffer = (struct buffer *)v;
    if (buffer) {
        buffer->free(buffer);
    }
}

static struct buffer * storage_head(struct storage *me, i64 offset, i32 length, char **e) {
    assert(me != NULL);
    struct buffer *h = me->h;
    if (!h) {
        THROW(e, "storage_head: header mapping is NULL (file=%s)", me->opts.file);
    }
    struct buffer *out = buffer_slice(h, offset, length, e);
    return out;

EXCEPTION:
    return NULL;
}

static struct buffer * storage_mmap(struct storage *me, i64 offset, i32 length, char **e) {
    assert(me != NULL);
    assert(me->cache != NULL);
    assert(length > 0);
    assert(me->fd != -1);
    assert(offset > -1);

    struct buffer *mbb = NULL;
    i64 limit = length + offset;
    if (limit > file_length(me->opts.file)) {
        ftruncate(me->fd, limit);
    }

    i32 page_size = OS_PAGE_SIZE; // 16Kb fixed // getpagesize(); // sysconf(_SC_PAGESIZE)
    i64 page_offset = offset % page_size;
    i64 map_offset = offset - page_offset;
    i64 map_size = length + page_offset;
    // printf("offset : %lld, page_offset : %lld, map_offset : %lld, map_size : %lld \n", offset, page_offset, map_offset, map_size);
    int mmap_flags = MAP_SHARED;
#ifdef MAP_POPULATE
    mmap_flags |= MAP_POPULATE;
#endif
    void *p = mmap(NULL, map_size, PROT_READ | (me->opts.mode == FLINTDB_RDWR ? PROT_WRITE : 0), mmap_flags, me->fd, map_offset);
    if (p == MAP_FAILED) THROW(e, "mmap() : %d - %s", errno, strerror(errno));

    mbb = buffer_mmap(p, page_offset, map_size);
    if (!mbb) THROW(e, "Out of memory");


//     /* advise kernel about expected access pattern to improve read-ahead */
// #ifdef MADV_WILLNEED
//     // Hint kernel to read-ahead this data for better performance
//     madvise(p, map_size, MADV_WILLNEED);
// #endif

// #ifdef MADV_RANDOM
//     madvise(p, map_size, MADV_RANDOM);
// #endif

// #ifdef POSIX_FADV_WILLNEED
//     // Async readahead at file descriptor level
//     posix_fadvise(me->fd, map_offset, map_size, POSIX_FADV_WILLNEED);
// #endif

// #ifdef POSIX_FADV_RANDOM
//     posix_fadvise(me->fd, map_offset, map_size, POSIX_FADV_RANDOM);
// #endif

    return mbb;

EXCEPTION:
    if (mbb) mbb->free(mbb);
    if (p != MAP_FAILED)
        munmap(p, map_size);
    return NULL;
}

static void storage_mmap_buffer_get(struct storage *me, i64 index, struct buffer *out) {
    i64 absolute = me->block_bytes * index;
    i64 i = absolute / me->mmap_bytes;
    i64 r = absolute % me->mmap_bytes;
    char *e = NULL;

    valtype found = me->cache->get(me->cache, i);
    if (HASHMAP_INVALID_VAL != found) {
        struct buffer *mbb = (struct buffer *)found;
        mbb->slice(mbb, r, me->block_bytes, out, &e);
        if (e && *e) THROW_S(e);
        return;
    }

    i64 offset = HEADER_BYTES + me->opts.extra_header_bytes + (i * me->mmap_bytes);
    i64 before = file_length(me->opts.file);
    struct buffer *mbb = storage_mmap(me, offset, me->mmap_bytes, NULL);
    me->cache->put(me->cache, i, (valtype)mbb, storage_cache_free);

    if (me->opts.mode == FLINTDB_RDWR && before < file_length(me->opts.file)) {
        i32 blocks = me->mmap_bytes / me->block_bytes;
        i64 next = 1 + (i * blocks);
        struct buffer bb = {0};
        for(i32 x=0; x<blocks; x++) {
            // struct buffer bb = {0};
            mbb->slice(mbb, x * me->block_bytes, me->block_bytes, &bb, &e);
            if (e && *e) THROW_S(e);
            bb.i8_put(&bb, STATUS_EMPTY, NULL);
            bb.i8_put(&bb, MARK_AS_UNUSED, NULL);
            bb.i16_put(&bb, 0, NULL);
            bb.i32_put(&bb, 0, NULL);
            bb.i64_put(&bb, next, NULL);
            next++;
        }
        storage_commit(me, 1, &e);
        if (e && *e) THROW_S(e);
    }

    mbb->slice(mbb, r, me->block_bytes, out, &e);
    if (e && *e) THROW_S(e);
    return;

EXCEPTION:
    WARN("storage_buffer_get error: %s", e);
}

static i32 storage_mmap_delete(struct storage *me, i64 offset, char **e) {
    struct buffer p = {0};
    struct buffer c = {0};

    storage_mmap_buffer_get(me, offset, &p);
    p.slice(&p, 0, p.remaining(&p), &c, e);
    if (e && *e) THROW_S(e);

    u8 status = c.i8_get(&c, e);
    c.i8_get (&c, NULL);
    c.i16_get(&c, NULL);
    c.i32_get(&c, NULL);
    i64 next = c.i64_get(&c, NULL);

    if (STATUS_SET != status)
        return 0;

    p.i8_put (&p, STATUS_EMPTY, NULL);
    p.i8_put (&p, MARK_AS_UNUSED, NULL);
    p.i16_put(&p, 0, NULL);
    p.i32_put(&p, 0, NULL);
    p.i64_put(&p, me->free, NULL);
    
    #ifdef STORAGE_FILL_ZEROED_BLOCK_ON_DELETE
    // p.array_put(&p, me->clean, me->opts.block_bytes, NULL); // fill zeroed block data
    p.array_put(&p, me->clean, p.remaining(&p), NULL); // fill zeroed block data
    #endif

    me->free = offset;
    me->count--;
    if (next > NEXT_END)
        storage_mmap_delete(me, next, e);

    storage_commit(me, 0, e);
    if (e && *e) THROW_S(e);
    return 1;

EXCEPTION:
    return 0;
}

static struct buffer * storage_mmap_read(struct storage *me, i64 offset, char **e) {
    struct buffer mbb = {0};
    storage_mmap_buffer_get(me, offset, &mbb);
    u8 status = mbb.i8_get(&mbb, e);
    if (status != STATUS_SET) THROW(e, "Block at offset %lld is not set", offset);

    u8 mark = mbb.i8_get(&mbb, e);
    if (mark != MARK_AS_DATA) THROW(e, "Block at offset %lld is not data", offset);
    i16 limit = mbb.i16_get(&mbb, e);
    i32 length = mbb.i32_get(&mbb, e);
    i64 next = mbb.i64_get(&mbb, e);

    if (next > NEXT_END && length > (me->opts.block_bytes)) {
        struct buffer *p = buffer_alloc(length);
        // copy only the first chunk (limit) from the first block
        struct buffer first = {0};
        mbb.slice(&mbb, 0, limit, &first, e);
        if (e && *e) THROW_S(e);
        p->array_put(p, first.array, first.remaining(&first), NULL);
        for(; next > NEXT_END; ) {
            struct buffer n = {0};
            storage_mmap_buffer_get(me, next, &n);
            if (STATUS_SET != n.i8_get(&n, NULL)) break;
            n.i8_get(&n, NULL); // MARK
            i16 remains = n.i16_get(&n, NULL);
            n.i32_get(&n, NULL);
            next = n.i64_get(&n, NULL);

            struct buffer s = {0};
            n.slice(&n, 0, remains, &s, e);
            if (e && *e) THROW_S(e);
            p->array_put(p, s.array, s.remaining(&s), NULL);
        }
        p->flip(p);
        return p;
    }

    // struct buffer *slice = CALLOC(1, sizeof(struct buffer));
    // mbb.slice(&mbb, 0, limit, slice);
    struct buffer *slice = buffer_slice(&mbb, 0, limit, e);
    return slice;

EXCEPTION:
    return NULL;
}

static inline void storage_mmap_write_priv(struct storage *me, i64 offset, u8 mark, struct buffer *in, char **e) {
    const int BLOCK_DATA_BYTES = me->opts.block_bytes;
    i64 curr = offset;
    u8 curr_mark = mark;
    i32 remaining = in->remaining(in);
    i64 next_last = NEXT_END;

    while (1) {
        struct buffer p = {0};
        struct buffer c = {0};
        storage_mmap_buffer_get(me, curr, &p);
        p.slice(&p, 0, p.remaining(&p), &c, e);
        // if (*e && **e) return;

        u8 status = c.i8_get(&c, NULL);
        c.i8_get(&c, NULL);      // mark
        c.i16_get(&c, NULL);     // data length
        c.i32_get(&c, NULL);     // total length
        i64 next = c.i64_get(&c, NULL);

        p.i8_put(&p, STATUS_SET, NULL);
        p.i8_put(&p, curr_mark, NULL);
        int chunk = (remaining < BLOCK_DATA_BYTES ? remaining : BLOCK_DATA_BYTES);
        p.i16_put(&p, (i16)chunk, NULL);
        p.i32_put(&p, remaining, NULL);

        if (STATUS_SET != status) {
            me->count++;
            me->free = next; // relink free list
        }

        i64 next_index = NEXT_END;
        if (remaining > BLOCK_DATA_BYTES) {
            next_index = (next > NEXT_END ? next : me->free);
            p.i64_put(&p, next_index, NULL);
        } else {
            p.i64_put(&p, NEXT_END, NULL);
        }

        // copy data chunk from input to mapped block
        p.array_put(&p, in->array_get(in, chunk, NULL), chunk, NULL);
        // pad remaining of block
        int pad = BLOCK_DATA_BYTES - chunk;
        if (pad > 0) {
            p.array_put(&p, me->clean, pad, NULL);
        }

        remaining -= chunk;
        next_last = next;
        if (remaining <= 0) {
            if (next_last > NEXT_END && next_last != curr) {
                storage_mmap_delete(me, next_last, e);
            }
            storage_commit(me, 1, e);
            if (e && *e) THROW_S(e);
            break;
        }

        curr = next_index;
        curr_mark = MARK_AS_NEXT;
    }

EXCEPTION:
    return;
}

static i64 storage_mmap_write(struct storage *me, struct buffer *in, char **e) {
    // DEBUG("enter, me=%p, in=%p, free=%lld", (void*)me, (void*)in, me ? me->free : -1);
    i64 offset = me->free;
    storage_mmap_write_priv(me, offset, MARK_AS_DATA, in, e);
    // DEBUG("exit, offset=%lld, e=%s", offset, e?*e?*e:"NULL":"NULL");
    return offset;
}

static i64 storage_mmap_write_at(struct storage *me, i64 offset, struct buffer *in, char **e) {
    storage_mmap_write_priv(me, offset, MARK_AS_DATA, in, e);
    return offset;
}

static void storage_transaction(struct storage *me, i64 id, char **e) {
    (void)id;
    assert(me != NULL);
    // No-op 
}

static void storage_mmap_close(struct storage *me) {
    assert(me != NULL);
    if (me->fd <= 0) return;

    // LOG("%p: begin file=%s, count=%lld, free=%lld", me, me->opts.file, me->count, me->free);

    // Force commit any pending changes before closing
    storage_commit(me, 1, NULL);

    if (me->cache) {
        DEBUG("freeing cache %p", me->cache);
        me->cache->free(me->cache);
        me->cache = NULL;
    }
    if (me->h) {
        DEBUG("freeing header mapping %p", me->h);
        me->h->free(me->h);
        me->h = NULL;
    }
    if (me->fd > 0) {
        DEBUG("closing fd");
        close(me->fd);
    }
    if (me->clean) {
        DEBUG("freeing clean buffer");
        FREE(me->clean);
        me->clean = NULL;
    }
    me->fd = -1;
}

static int storage_mmap_open(struct storage * me, struct storage_opts opts, char **e) {
    if (!me)
        return -1;

    me->block_bytes = (opts.compact <= 0) ? (BLOCK_HEADER_BYTES + opts.block_bytes) : (BLOCK_HEADER_BYTES + (opts.compact));
    me->clean = CALLOC(1, me->block_bytes);
    me->increment = (opts.increment <= 0) ? DEFAULT_INCREMENT_BYTES : opts.increment;
    me->mmap_bytes = me->block_bytes * (me->increment / me->block_bytes);

    memcpy(&me->opts, &opts, sizeof(struct storage_opts));

    char dir[PATH_MAX] = {0};
    getdir(me->opts.file, dir);
    mkdirs(dir, S_IRWXU);

    // O_DIRECT removed: incompatible with mmap and can hurt performance
    me->fd = open(me->opts.file, (opts.mode == FLINTDB_RDWR ? O_RDWR | O_CREAT : O_RDONLY), S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (me->fd < 0) {
        THROW(e, "Cannot open file %s: %s", me->opts.file, strerror(errno));
    }

    int bytes = file_length(me->opts.file);
    if (bytes < HEADER_BYTES)
        ftruncate(me->fd, HEADER_BYTES);

    // Map the file header with MAP_SHARED so updates (e.g., magic, counts) are persisted to disk
    // Using MAP_PRIVATE here would create a private COW mapping and header writes wouldn't be visible
    // to other processes (e.g., Java reader) nor persisted after process exit.
    void *p = mmap(NULL, HEADER_BYTES, PROT_READ | (opts.mode == FLINTDB_RDWR ? PROT_WRITE : 0), MAP_SHARED, me->fd, 0);
    if (p == MAP_FAILED) 
        THROW(e, "Cannot mmap file %s: %s", me->opts.file, strerror(errno)); 

    me->h = buffer_mmap(p, 0, HEADER_BYTES);

    me->cache = hashmap_new(MAPPED_BYTEBUFFER_POOL_SIZE, hashmap_int_hash, hashmap_int_cmpr);

    if (!me->cache) THROW(e, "Cannot create cache");

    me->close = storage_mmap_close;
    me->count_get = storage_count_get;
    me->bytes_get = storage_bytes_get;
    me->read = storage_mmap_read;
    me->write = storage_mmap_write;
    me->write_at = storage_mmap_write_at;
    me->delete = storage_mmap_delete;
    me->transaction = storage_transaction;
    me->mmap = storage_mmap;
    me->head = storage_head;

    if (bytes < HEADER_BYTES) {
        // Fresh file: initialize free-list head to first block index (1)
        me->free = 0;
        me->count = 0;
        storage_commit(me, 1, e);
        if (e && *e) THROW_S(e);
    }
    else {
        struct buffer *h = me->h;
        struct buffer bb = {0};
        h->slice(h, CUSTOM_HEADER_BYTES, COMMON_HEADER_BYTES, &bb, e);
        if (e && *e) THROW_S(e);

        bb.i64_get(&bb, e); // reserved
        me->free = bb.i64_get(&bb, e); // The front of deleted blocks
        bb.i64_get(&bb, e); // The tail of deleted blocks => not used in mmap
        bb.i16_get(&bb, e); // version:i16
        i32 inc = bb.i32_get(&bb, e); // increment:i32
        if (inc <= 0)
            THROW(e, "Invalid increment size: %d, file:%s", inc, me->opts.file); // old version was (10MB)
        if (inc != me->increment) {
            me->increment = inc;
            me->mmap_bytes = me->block_bytes * (me->increment / me->block_bytes);
        }
        bb.skip(&bb, R24LEN); // reserved
        i16 blksize = bb.i16_get(&bb, e); // BLOCK Data Max Size (exclude BLOCK Header)
        if (blksize != opts.block_bytes) {
            THROW(e, "Block size mismatch: header=%d, opts=%d", blksize, opts.block_bytes);
        }
        me->count = bb.i64_get(&bb, e);
        assert(me->count > -1);
    }

    return 0;

EXCEPTION:
    // WARN("%s", e);
    if (me) storage_mmap_close(me);
    return -1;
}

/**
 * Memory-backed storage
 */

static void storage_mem_buffer_get(struct storage *me, i64 index, struct buffer *out) {
    i64 absolute = me->block_bytes * index;
    i64 i = absolute / me->mem_bytes;
    i64 r = absolute % me->mem_bytes;
    char *e = NULL;

    valtype found = me->cache->get(me->cache, i);
    if (HASHMAP_INVALID_VAL != found) {
        struct buffer *mbb = (struct buffer *)found;
        mbb->slice(mbb, r, me->block_bytes, out, &e);
        if (e && *e) THROW_S(e);
        return;
    }

    // Allocate new memory buffer
    struct buffer *mbb = buffer_alloc(me->mem_bytes);
    if (!mbb) {
        e = "Out of memory";
        THROW_S(e);
    }
    
    me->cache->put(me->cache, i, (valtype)mbb, storage_cache_free);

    if (me->opts.mode == FLINTDB_RDWR) {
        i32 blocks = me->mem_bytes / me->block_bytes;
        i64 next = 1 + (i * blocks);
        struct buffer bb = {0};
        for(i32 x=0; x<blocks; x++) {
            mbb->slice(mbb, x * me->block_bytes, me->block_bytes, &bb, &e);
            if (e && *e) THROW_S(e);
            bb.i8_put(&bb, STATUS_EMPTY, NULL);
            bb.i8_put(&bb, MARK_AS_UNUSED, NULL);
            bb.i16_put(&bb, 0, NULL);
            bb.i32_put(&bb, 0, NULL);
            bb.i64_put(&bb, next, NULL);
            next++;
        }
        storage_commit(me, 1, &e);
        if (e && *e) THROW_S(e);
    }

    mbb->slice(mbb, r, me->block_bytes, out, &e);
    if (e && *e) THROW_S(e);
    return;

EXCEPTION:
    WARN("storage_mem_buffer_get error: %s", e);
}

static i32 storage_mem_delete(struct storage *me, i64 offset, char **e) {
    struct buffer p = {0};
    struct buffer c = {0};

    storage_mem_buffer_get(me, offset, &p);
    p.slice(&p, 0, p.remaining(&p), &c, e);
    if (e && *e) THROW_S(e);

    u8 status = c.i8_get(&c, e);
    c.i8_get (&c, NULL);
    c.i16_get(&c, NULL);
    c.i32_get(&c, NULL);
    i64 next = c.i64_get(&c, NULL);

    if (STATUS_SET != status)
        return 0;

    p.i8_put (&p, STATUS_EMPTY, NULL);
    p.i8_put (&p, MARK_AS_UNUSED, NULL);
    p.i16_put(&p, 0, NULL);
    p.i32_put(&p, 0, NULL);
    p.i64_put(&p, me->free, NULL);
    
    #ifdef STORAGE_FILL_ZEROED_BLOCK_ON_DELETE
    p.array_put(&p, me->clean, p.remaining(&p), NULL);
    #endif

    me->free = offset;
    me->count--;
    if (next > NEXT_END)
        storage_mem_delete(me, next, e);

    storage_commit(me, 0, e);
    if (e && *e) THROW_S(e);
    return 1;

EXCEPTION:
    return 0;
}

static struct buffer * storage_mem_read(struct storage *me, i64 offset, char **e) {
    struct buffer mbb = {0};
    storage_mem_buffer_get(me, offset, &mbb);
    u8 status = mbb.i8_get(&mbb, e);
    if (status != STATUS_SET) THROW(e, "Block at offset %lld is not set", offset);

    u8 mark = mbb.i8_get(&mbb, e);
    if (mark != MARK_AS_DATA) THROW(e, "Block at offset %lld is not data", offset);
    i16 limit = mbb.i16_get(&mbb, e);
    i32 length = mbb.i32_get(&mbb, e);
    i64 next = mbb.i64_get(&mbb, e);

    if (next > NEXT_END && length > (me->opts.block_bytes)) {
        struct buffer *p = buffer_alloc(length);
        struct buffer first = {0};
        mbb.slice(&mbb, 0, limit, &first, e);
        if (e && *e) THROW_S(e);
        p->array_put(p, first.array, first.remaining(&first), NULL);
        for(; next > NEXT_END; ) {
            struct buffer n = {0};
            storage_mem_buffer_get(me, next, &n);
            if (STATUS_SET != n.i8_get(&n, NULL)) break;
            n.i8_get(&n, NULL);
            i16 remains = n.i16_get(&n, NULL);
            n.i32_get(&n, NULL);
            next = n.i64_get(&n, NULL);

            struct buffer s = {0};
            n.slice(&n, 0, remains, &s, e);
            if (e && *e) THROW_S(e);
            p->array_put(p, s.array, s.remaining(&s), NULL);
        }
        p->flip(p);
        return p;
    }

    struct buffer *slice = buffer_slice(&mbb, 0, limit, e);
    return slice;

EXCEPTION:
    return NULL;
}

static inline void storage_mem_write_priv(struct storage *me, i64 offset, u8 mark, struct buffer *in, char **e) {
    const int BLOCK_DATA_BYTES = me->opts.block_bytes;
    i64 curr = offset;
    u8 curr_mark = mark;
    i32 remaining = in->remaining(in);
    i64 next_last = NEXT_END;

    while (1) {
        struct buffer p = {0};
        struct buffer c = {0};
        storage_mem_buffer_get(me, curr, &p);
        p.slice(&p, 0, p.remaining(&p), &c, e);

        u8 status = c.i8_get(&c, NULL);
        c.i8_get(&c, NULL);
        c.i16_get(&c, NULL);
        c.i32_get(&c, NULL);
        i64 next = c.i64_get(&c, NULL);

        p.i8_put(&p, STATUS_SET, NULL);
        p.i8_put(&p, curr_mark, NULL);
        int chunk = (remaining < BLOCK_DATA_BYTES ? remaining : BLOCK_DATA_BYTES);
        p.i16_put(&p, (i16)chunk, NULL);
        p.i32_put(&p, remaining, NULL);

        if (STATUS_SET != status) {
            me->count++;
            me->free = next;
        }

        i64 next_index = NEXT_END;
        if (remaining > BLOCK_DATA_BYTES) {
            next_index = (next > NEXT_END ? next : me->free);
            p.i64_put(&p, next_index, NULL);
        } else {
            p.i64_put(&p, NEXT_END, NULL);
        }

        p.array_put(&p, in->array_get(in, chunk, NULL), chunk, NULL);
        int pad = BLOCK_DATA_BYTES - chunk;
        if (pad > 0) {
            p.array_put(&p, me->clean, pad, NULL);
        }

        remaining -= chunk;
        next_last = next;
        if (remaining <= 0) {
            if (next_last > NEXT_END && next_last != curr) {
                storage_mem_delete(me, next_last, e);
            }
            storage_commit(me, 1, e);
            if (e && *e) THROW_S(e);
            break;
        }

        curr = next_index;
        curr_mark = MARK_AS_NEXT;
    }

EXCEPTION:
    return;
}

static i64 storage_mem_write(struct storage *me, struct buffer *in, char **e) {
    i64 offset = me->free;
    storage_mem_write_priv(me, offset, MARK_AS_DATA, in, e);
    return offset;
}

static i64 storage_mem_write_at(struct storage *me, i64 offset, struct buffer *in, char **e) {
    storage_mem_write_priv(me, offset, MARK_AS_DATA, in, e);
    return offset;
}

static void storage_mem_close(struct storage *me) {
    assert(me != NULL);
    if (me->cache == NULL) return;

    storage_commit(me, 1, NULL);

    if (me->cache) {
        DEBUG("freeing cache %p", me->cache);
        me->cache->free(me->cache);
        me->cache = NULL;
    }
    if (me->h) {
        DEBUG("freeing header %p", me->h);
        me->h->free(me->h);
        me->h = NULL;
    }
    if (me->clean) {
        DEBUG("freeing clean buffer");
        FREE(me->clean);
        me->clean = NULL;
    }
}

static int storage_mem_open(struct storage * me, struct storage_opts opts, char **e) {
    if (!me)
        return -1;

    me->block_bytes = (opts.compact <= 0) ? (BLOCK_HEADER_BYTES + opts.block_bytes) : (BLOCK_HEADER_BYTES + (opts.compact));
    me->clean = CALLOC(1, me->block_bytes);
    me->increment = (opts.increment <= 0) ? DEFAULT_INCREMENT_BYTES : opts.increment;
    me->mem_bytes = me->block_bytes * (me->increment / me->block_bytes);

    memcpy(&me->opts, &opts, sizeof(struct storage_opts));

    // Allocate header buffer
    me->h = buffer_alloc(HEADER_BYTES);
    if (!me->h) THROW(e, "Cannot allocate header buffer");

    me->cache = hashmap_new(MAPPED_BYTEBUFFER_POOL_SIZE, hashmap_int_hash, hashmap_int_cmpr);
    if (!me->cache) THROW(e, "Cannot create cache");

    me->close = storage_mem_close;
    me->count_get = storage_count_get;
    me->bytes_get = storage_bytes_get;
    me->read = storage_mem_read;
    me->write = storage_mem_write;
    me->write_at = storage_mem_write_at;
    me->delete = storage_mem_delete;
    me->transaction = storage_transaction;
    me->mmap = NULL;  // Not supported for memory storage
    me->head = storage_head;

    // Initialize as fresh storage
    me->free = 0;
    me->count = 0;
    storage_commit(me, 1, e);
    if (e && *e) THROW_S(e);

    return 0;

EXCEPTION:
    if (me) storage_mem_close(me);
    return -1;
}
/// End of memory-backed storage

/// Direct I/O storage

static struct buffer_pool *g_dio_read_pool = NULL;

// Batch size in pages for sequential DIO writes (must be >= 1).
#ifndef DIO_WRITE_BATCH_PAGES
#define DIO_WRITE_BATCH_PAGES 256  // 256 * 16KB = 4MB for better batching
#endif

static inline struct buffer_pool *storage_dio_read_pool(void) {
    if (!g_dio_read_pool) {
        // Keep this pool global for process lifetime to avoid UAF if a caller frees
        // a buffer after storage close. Capacity is modest; buffers grow on demand.
        g_dio_read_pool = buffer_pool_create(1024, 512, 0);
    }
    return g_dio_read_pool;
}

static inline i64 storage_dio_file_offset(struct storage *me, i64 index);
static void storage_dio_file_inflate(struct storage *me, i64 index, struct buffer *out);

static inline void storage_dio_flush_wbatch(struct storage *me, char **e) {
    if (!me || me->opts.mode != FLINTDB_RDWR) return;
    if (!me->dio_wbatch || me->dio_wbatch_count == 0) return;

    // Ensure chunks spanning the batch are inflated.
    // Optimize: only inflate if not already done (sequential writes usually stay in same chunk)
    i64 start_chunk = (me->dio_wbatch_base * me->block_bytes) / me->mmap_bytes;
    i64 end_chunk = ((me->dio_wbatch_base + (i64)me->dio_wbatch_count - 1) * me->block_bytes) / me->mmap_bytes;
    if (me->dio_last_inflated_chunk != start_chunk) {
        storage_dio_file_inflate(me, me->dio_wbatch_base, NULL);
    }
    if (end_chunk != start_chunk && me->dio_last_inflated_chunk != end_chunk) {
        storage_dio_file_inflate(me, me->dio_wbatch_base + (i64)me->dio_wbatch_count - 1, NULL);
    }

    i64 file_offset = storage_dio_file_offset(me, me->dio_wbatch_base);
    size_t nbytes = (size_t)me->dio_wbatch_count * (size_t)me->block_bytes;
    ssize_t written = pwrite(me->fd, me->dio_wbatch, nbytes, file_offset);
    if (written < 0) {
        THROW(e, "pwrite(batch) failed at offset %lld: %s", file_offset, strerror(errno));
    }
    if ((size_t)written != nbytes) {
        THROW(e, "pwrite(batch) incomplete: wrote %zd of %zu bytes at offset %lld", written, nbytes, file_offset);
    }

    me->dio_wbatch_count = 0;
    return;

EXCEPTION:
    return;
}

static inline void storage_dio_wbatch_read_through(struct storage *me, i64 offset, void *dst) {
    // If the requested block is currently buffered but not flushed, serve it from memory.
    if (!me || !me->dio_wbatch || me->dio_wbatch_count == 0) return;
    if (offset < me->dio_wbatch_base) return;
    i64 rel = offset - me->dio_wbatch_base;
    if (rel < 0 || (u64)rel >= (u64)me->dio_wbatch_count) return;
    memcpy(dst, (char *)me->dio_wbatch + ((size_t)rel * (size_t)me->block_bytes), (size_t)me->block_bytes);
}

static inline void *storage_dio_get_aligned(struct storage *me, u32 size, void **slot, u32 *slot_size, char **e) {
    if (*slot && slot_size && *slot_size >= size) {
        return *slot;
    }

    if (*slot) {
        FREE(*slot);
        *slot = NULL;
        if (slot_size) *slot_size = 0;
    }

    void *p = NULL;
#ifndef _WIN32
    // O_DIRECT (Linux) requires alignment; use OS_PAGE_SIZE for safety.
    if (posix_memalign(&p, (size_t)OS_PAGE_SIZE, (size_t)size) != 0) {
        THROW(e, "posix_memalign(%u) failed", size);
    }
#else
    p = MALLOC(size);
    if (!p) {
        THROW(e, "Out of memory");
    }
#endif
    memset(p, 0, size);
    *slot = p;
    if (slot_size) *slot_size = size;
    return p;

EXCEPTION:
    return NULL;
}

static inline void storage_dio_pread_into(struct storage *me, i64 offset, void *dst, char **e) {
    assert(me != NULL);
    assert(me->fd != -1);
    assert(dst != NULL);

    // Read-through from pending write batch (same process visibility)
    storage_dio_wbatch_read_through(me, offset, dst);
    // If it matched, the first byte (status) will already be set/non-zero for valid blocks.
    // We can't reliably detect misses by content, so use range check again.
    if (me && me->dio_wbatch && me->dio_wbatch_count > 0 && offset >= me->dio_wbatch_base && offset < (me->dio_wbatch_base + (i64)me->dio_wbatch_count)) {
        return;
    }

    i64 file_offset = storage_dio_file_offset(me, offset);
    ssize_t nread = pread(me->fd, dst, me->block_bytes, file_offset);
    if (nread < 0) {
        THROW(e, "pread() failed at offset %lld: %s", file_offset, strerror(errno));
    }
    if (nread != me->block_bytes) {
        THROW(e, "pread() incomplete: read %zd of %d bytes at offset %lld", nread, me->block_bytes, file_offset);
    }
    return;

EXCEPTION:
    return;
}

static inline void storage_dio_pwrite_from(struct storage *me, i64 offset, const void *src, char **e) {
    assert(me != NULL);
    assert(me->fd != -1);
    assert(src != NULL);

    i64 file_offset = storage_dio_file_offset(me, offset);
    ssize_t written = pwrite(me->fd, src, me->block_bytes, file_offset);
    if (written < 0) {
        THROW(e, "pwrite() failed at offset %lld: %s", file_offset, strerror(errno));
    }
    if (written != me->block_bytes) {
        THROW(e, "pwrite() incomplete: wrote %zd of %d bytes at offset %lld", written, me->block_bytes, file_offset);
    }
    return;

EXCEPTION:
    return;
}


static void storage_dio_file_inflate(struct storage *me, i64 index, struct buffer *out) {
    char *e = NULL;
    if (me->opts.mode != FLINTDB_RDWR) 
        return;

    i64 absolute = me->block_bytes * index;
    i64 i = absolute / me->mmap_bytes;
    // i64 r = absolute % me->mmap_bytes;

    // PERF: same chunk is often accessed repeatedly (sequential allocation/read).
    // Avoid repeated probe pread/ftruncate checks when we've already inflated this chunk.
    if (me->dio_last_inflated_chunk == i) {
        return;
    }

    i64 chunk_off = HEADER_BYTES + me->opts.extra_header_bytes + (i * me->mmap_bytes);
    i64 chunk_end = chunk_off + me->mmap_bytes;

    // Ensure file is large enough to include this chunk.
    i64 before = file_length(me->opts.file);
    if (before < chunk_end) {
        if (ftruncate(me->fd, chunk_end) < 0) {
            WARN("ftruncate() failed during inflate at offset %lld: %s", chunk_off, strerror(errno));
            return;
        }
    }

    // If the chunk is already initialized, don't rewrite it.
    // Freshly-extended regions are usually zero-filled; status=0 indicates uninitialized.
    u8 first_status = 0;
    if (pread(me->fd, &first_status, 1, chunk_off) != 1) {
        WARN("pread() failed during inflate probe at offset %lld: %s", chunk_off, strerror(errno));
        return;
    }
    if (first_status == STATUS_EMPTY || first_status == STATUS_SET) {
        me->dio_last_inflated_chunk = i;
        return;
    }

    // Initialize free-list blocks in this chunk.
    // PERF: batch init into one large aligned buffer and write once.
    i32 blocks = me->mmap_bytes / me->block_bytes;
    i64 base_index = i * blocks;
    void *chunk_buf = storage_dio_get_aligned(me, (u32)me->mmap_bytes, &me->dio_chunk, &me->dio_chunk_bytes, &e);
    if (e && *e) {
        WARN("inflate: cannot allocate chunk buffer: %s", e);
        return;
    }
    memset(chunk_buf, 0, (size_t)me->mmap_bytes);

    for (i32 x = 0; x < blocks; x++) {
        struct buffer bb = {0};
        char *blk = (char *)chunk_buf + ((size_t)x * (size_t)me->block_bytes);
        buffer_wrap(blk, (u32)me->block_bytes, &bb);
        i64 next = base_index + x + 1;
        bb.i8_put(&bb, STATUS_EMPTY, NULL);
        bb.i8_put(&bb, MARK_AS_UNUSED, NULL);
        bb.i16_put(&bb, 0, NULL);
        bb.i32_put(&bb, 0, NULL);
        bb.i64_put(&bb, next, NULL);
    }

    ssize_t written = pwrite(me->fd, chunk_buf, me->mmap_bytes, chunk_off);
    if (written != me->mmap_bytes) {
        WARN("pwrite() failed during inflate init at offset %lld: %s", chunk_off, strerror(errno));
        return;
    }

    me->dio_last_inflated_chunk = i;

    storage_commit(me, 1, &e);
    if (e && *e) THROW_S(e);

    return;

EXCEPTION:
    WARN("storage_dio_file_inflate error: %s", e);
}

static inline i64 storage_dio_file_offset(struct storage *me, i64 index) {
    i64 absolute = me->block_bytes * index;
    i64 i = absolute / me->mmap_bytes;
    i64 r = absolute % me->mmap_bytes;

    i64 offset = HEADER_BYTES + me->opts.extra_header_bytes + (i * me->mmap_bytes);
    return offset + r;
}

static struct buffer * storage_dio_read(struct storage *me, i64 offset, char **e) {
    assert(me != NULL);
    assert(me->fd != -1);

    const int BLOCK_DATA_BYTES = me->opts.block_bytes;
    struct buffer *result = NULL;
    void *scratch = storage_dio_get_aligned(me, (u32)me->block_bytes, &me->dio_scratch, NULL, e);
    if (e && *e) THROW_S(e);

    i64 curr = offset;
    i32 total_len = -1;
    i32 appended = 0;
    i64 steps = 0;
    i64 max_steps = 1024; // updated once total_len is known
    struct buffer_pool *pool = storage_dio_read_pool();

    while (1) {
        if (++steps > max_steps) {
            THROW(e, "DIO read chain too long (possible cycle/corruption) at offset %lld", offset);
        }
        storage_dio_file_inflate(me, curr, NULL);
        storage_dio_pread_into(me, curr, scratch, e);
        if (e && *e) THROW_S(e);

        struct buffer bb = {0};
        buffer_wrap((char *)scratch, (u32)me->block_bytes, &bb);

        u8 status = bb.i8_get(&bb, e);
        if (e && *e) THROW_S(e);
        if (status != STATUS_SET) THROW(e, "Block at offset %lld is not set", curr);

        u8 mark = bb.i8_get(&bb, e);
        if (e && *e) THROW_S(e);
        if ((curr == offset && mark != MARK_AS_DATA) || (curr != offset && mark != MARK_AS_NEXT)) {
            THROW(e, "Block at offset %lld has invalid mark", curr);
        }

        i16 limit = bb.i16_get(&bb, e);
        if (e && *e) THROW_S(e);
        i32 length = bb.i32_get(&bb, e);
        if (e && *e) THROW_S(e);
        i64 next = bb.i64_get(&bb, e);
        if (e && *e) THROW_S(e);

        if (limit < 0 || limit > BLOCK_DATA_BYTES) {
            THROW(e, "DIO read invalid limit=%d at offset %lld", (int)limit, curr);
        }
        if (length < 0) {
            THROW(e, "DIO read invalid length=%d at offset %lld", (int)length, curr);
        }

        if (total_len < 0) {
            total_len = length;
            // Derive an upper bound for the expected chain length.
            // (total_len is total payload bytes, each block contributes <= BLOCK_DATA_BYTES)
            max_steps = (i64)((total_len + BLOCK_DATA_BYTES - 1) / BLOCK_DATA_BYTES) + 8;
            if (max_steps < 8) max_steps = 8;

            if (next <= NEXT_END || length <= BLOCK_DATA_BYTES) {
                // Non-overflow: copy out exactly `limit` bytes.
                result = pool ? pool->borrow(pool, (u32)limit) : buffer_alloc((u32)limit);
                if (!result) THROW(e, "Out of memory");
                struct buffer s = {0};
                bb.slice(&bb, 0, limit, &s, e);
                if (e && *e) THROW_S(e);
                result->array_put(result, s.array, (u32)s.remaining(&s), NULL);
                result->flip(result);
                return result;
            }

            // Overflow: allocate full length and append chunks.
            result = pool ? pool->borrow(pool, (u32)length) : buffer_alloc((u32)length);
            if (!result) THROW(e, "Out of memory");
        }

        // Append this chunk.
        struct buffer s = {0};
        bb.slice(&bb, 0, limit, &s, e);
        if (e && *e) THROW_S(e);
        if (appended + (i32)s.remaining(&s) > total_len) {
            THROW(e, "DIO read overflow/corruption: appended=%d + chunk=%d > total=%d at offset %lld", appended, (int)s.remaining(&s), total_len, offset);
        }
        result->array_put(result, s.array, (u32)s.remaining(&s), NULL);
        appended += (i32)s.remaining(&s);

        if (next <= NEXT_END) {
            if (appended != total_len) {
                // Don't silently return partial data.
                THROW(e, "DIO read length mismatch: got=%d expected=%d at offset %lld", appended, total_len, offset);
            }
            result->flip(result);
            return result;
        }
        curr = next;
    }

EXCEPTION:
    if (result) result->free(result);
    return NULL;
}

static i32 storage_dio_delete(struct storage *me, i64 offset, char **e) {
    const int BLOCK_DATA_BYTES = me->opts.block_bytes;
    void *scratch = storage_dio_get_aligned(me, (u32)me->block_bytes, &me->dio_scratch, NULL, e);
    if (e && *e) THROW_S(e);

    // Ensure any pending sequential writes are persisted before delete.
    storage_dio_flush_wbatch(me, e);
    if (e && *e) THROW_S(e);

    i64 curr = offset;
    int deleted_any = 0;
    i64 steps = 0;
    // Guard to avoid infinite loops under corruption/concurrent races.
    // We derive a tight bound from the record's stored `length` on the first block.
    i64 max_steps = 0;
    while (curr > NEXT_END) {
        if (max_steps > 0 && ++steps > max_steps) {
            THROW(e, "DIO delete chain too long (possible cycle/corruption) at offset %lld", offset);
        }
        storage_dio_file_inflate(me, curr, NULL);
        storage_dio_pread_into(me, curr, scratch, e);
        if (e && *e) THROW_S(e);

        struct buffer bb = {0};
        buffer_wrap((char *)scratch, (u32)me->block_bytes, &bb);

        u8 status = bb.i8_get(&bb, e);
        if (e && *e) THROW_S(e);
        u8 mark = bb.i8_get(&bb, NULL);
        bb.i16_get(&bb, NULL);
        i32 length = bb.i32_get(&bb, NULL);
        i64 next = bb.i64_get(&bb, NULL);

        if (next == curr) {
            THROW(e, "DIO delete self-cycle at offset %lld", curr);
        }

        if (STATUS_SET != status) {
            break;
        }

        if (max_steps == 0) {
            if (length < 0) {
                THROW(e, "DIO delete invalid length=%d at offset %lld", (int)length, curr);
            }
            max_steps = (i64)((length + BLOCK_DATA_BYTES - 1) / BLOCK_DATA_BYTES) + 8;
            if (max_steps < 8) max_steps = 8;
            // Now that we have a bound, count this first step.
            steps = 1;
        }

        // Validate chain marks to avoid following junk pointers.
        // Note: delete() is used both for deleting a record (starts at MARK_AS_DATA)
        // and for cleaning up an overflow chain (starts at MARK_AS_NEXT).
        if (curr == offset) {
            if (mark != MARK_AS_DATA && mark != MARK_AS_NEXT) {
                THROW(e, "DIO delete invalid mark at offset %lld", curr);
            }
        } else {
            if (mark != MARK_AS_NEXT) {
                THROW(e, "DIO delete invalid mark at offset %lld", curr);
            }
        }

        // Rewrite header as free-list node.
        bb.clear(&bb);
        bb.i8_put(&bb, STATUS_EMPTY, NULL);
        bb.i8_put(&bb, MARK_AS_UNUSED, NULL);
        bb.i16_put(&bb, 0, NULL);
        bb.i32_put(&bb, 0, NULL);
        bb.i64_put(&bb, me->free, NULL);

#ifdef STORAGE_FILL_ZEROED_BLOCK_ON_DELETE
        // fill remaining data
        int remaining = bb.remaining(&bb);
        if (remaining > 0) {
            bb.array_put(&bb, me->clean, (u32)remaining, NULL);
        }
#endif

        storage_dio_pwrite_from(me, curr, scratch, e);
        if (e && *e) THROW_S(e);

        me->free = curr;
        me->count--;
        deleted_any = 1;
        curr = next;
    }

    if (deleted_any) {
        storage_commit(me, 0, e);
        if (e && *e) THROW_S(e);
    }
    return 1;

EXCEPTION:
    return 0;
}

static inline void storage_dio_write_priv(struct storage *me, i64 offset, u8 mark, struct buffer *in, char **e) {
    const int BLOCK_DATA_BYTES = me->opts.block_bytes;
    i64 curr = offset;
    u8 curr_mark = mark;
    i32 remaining = in->remaining(in);
    i64 next_last = NEXT_END;

    void *scratch = storage_dio_get_aligned(me, (u32)me->block_bytes, &me->dio_scratch, NULL, e);
    if (e && *e) THROW_S(e);

    // Pre-inflate the starting chunk once
    storage_dio_file_inflate(me, offset, NULL);

    while (1) {
        // PERF: when allocating from the virgin tail, free == count and the free-list is sequential.
        // In that case we can avoid pread entirely and infer `next = curr + 1`.
        u8 status = STATUS_EMPTY;
        u8 was_set = 0;
        i64 next = curr + 1;

        int tail_fast = (curr == me->free && curr == me->count);

        // If we're about to do a non-tail write (overwrite/random), flush pending sequential batch first.
        if (!tail_fast && me->dio_wbatch_count > 0) {
            storage_dio_flush_wbatch(me, e);
            if (e && *e) THROW_S(e);
        }
        if (!tail_fast) {
            storage_dio_file_inflate(me, curr, NULL);
            storage_dio_pread_into(me, curr, scratch, e);
            if (e && *e) THROW_S(e);

            struct buffer c = {0};
            struct buffer tmp = {0};
            buffer_wrap((char *)scratch, (u32)me->block_bytes, &tmp);
            tmp.slice(&tmp, 0, tmp.remaining(&tmp), &c, e);
            if (e && *e) THROW_S(e);

            status = c.i8_get(&c, NULL);
            was_set = (status == STATUS_SET);
            c.i8_get(&c, NULL);      // mark
            c.i16_get(&c, NULL);     // data length
            c.i32_get(&c, NULL);     // total length
            next = c.i64_get(&c, NULL);
        } else {
            // For tail writes, only inflate when crossing chunk boundaries
            // Check if we're moving to a different chunk
            i64 curr_chunk = (curr * me->block_bytes) / me->mmap_bytes;
            if (me->dio_last_inflated_chunk != curr_chunk) {
                storage_dio_file_inflate(me, curr, NULL);
            }
            // Reuse scratch; no need to preserve existing bytes.
            memset(scratch, 0, (size_t)me->block_bytes);
        }

        struct buffer p = {0};
        buffer_wrap((char *)scratch, (u32)me->block_bytes, &p);

        p.i8_put(&p, STATUS_SET, NULL);
        p.i8_put(&p, curr_mark, NULL);
        int chunk = (remaining < BLOCK_DATA_BYTES ? remaining : BLOCK_DATA_BYTES);
        p.i16_put(&p, (i16)chunk, NULL);
        p.i32_put(&p, remaining, NULL);

        if (STATUS_SET != status) {
            me->count++;
            me->free = next; // relink free list
        }

        i64 next_index = NEXT_END;
        if (remaining > BLOCK_DATA_BYTES) {
            next_index = (next > NEXT_END ? next : me->free);
            p.i64_put(&p, next_index, NULL);
        } else {
            p.i64_put(&p, NEXT_END, NULL);
        }

        // copy data chunk from input to mapped block
        p.array_put(&p, in->array_get(in, chunk, NULL), (u32)chunk, NULL);
        // pad remaining of block
        int pad = BLOCK_DATA_BYTES - chunk;
        if (pad > 0) {
            p.array_put(&p, me->clean, (u32)pad, NULL);
        }

        // PERF: for virgin tail sequential allocation, buffer contiguous blocks and flush in batches.
        if (tail_fast && me->dio_wbatch && me->dio_wbatch_blocks > 0) {
            if (me->dio_wbatch_count == 0) {
                me->dio_wbatch_base = curr;
            }
            // If not contiguous with current batch, flush and start a new batch.
            if (curr != (me->dio_wbatch_base + (i64)me->dio_wbatch_count)) {
                storage_dio_flush_wbatch(me, e);
                if (e && *e) THROW_S(e);
                me->dio_wbatch_base = curr;
                me->dio_wbatch_count = 0;
            }
            // Flush if batch full.
            if (me->dio_wbatch_count >= me->dio_wbatch_blocks) {
                storage_dio_flush_wbatch(me, e);
                if (e && *e) THROW_S(e);
                me->dio_wbatch_base = curr;
                me->dio_wbatch_count = 0;
            }
            memcpy((char *)me->dio_wbatch + ((size_t)me->dio_wbatch_count * (size_t)me->block_bytes), scratch, (size_t)me->block_bytes);
            me->dio_wbatch_count++;

            // Opportunistically flush when full.
            if (me->dio_wbatch_count >= me->dio_wbatch_blocks) {
                storage_dio_flush_wbatch(me, e);
                if (e && *e) THROW_S(e);
            }
        } else {
            storage_dio_pwrite_from(me, curr, scratch, e);
            if (e && *e) THROW_S(e);
        }

        remaining -= chunk;
        next_last = next;
        if (remaining <= 0) {
            // Ensure any buffered writes are flushed before updating header.
            storage_dio_flush_wbatch(me, e);
            if (e && *e) THROW_S(e);
            // Only clean up an existing overflow chain when overwriting an already-set block.
            // For freshly allocated blocks, `next_last` is the free-list link.
            if (was_set && next_last > NEXT_END && next_last != curr) {
                storage_dio_delete(me, next_last, e);
            }
            storage_commit(me, 1, e);
            if (e && *e) THROW_S(e);
            break;
        }

        curr = next_index;
        curr_mark = MARK_AS_NEXT;
    }
    return;

EXCEPTION:
    return;
}

static i64 storage_dio_write(struct storage *me, struct buffer *in, char **e) {
    // DEBUG("enter, me=%p, in=%p, free=%lld", (void*)me, (void*)in, me ? me->free : -1);
    i64 offset = me->free;
    storage_dio_write_priv(me, offset, MARK_AS_DATA, in, e);
    // DEBUG("exit, offset=%lld, e=%s", offset, e?*e?*e:"NULL":"NULL");
    return offset;
}

static i64 storage_dio_write_at(struct storage *me, i64 offset, struct buffer *in, char **e) {
    storage_dio_write_priv(me, offset, MARK_AS_DATA, in, e);
    return offset;
}

static void storage_dio_close(struct storage *me) {
        // Flush any pending sequential writes before closing.
        storage_dio_flush_wbatch(me, NULL);
    assert(me != NULL);
    if (me->fd <= 0) return;

    // LOG("%p: begin file=%s, count=%lld, free=%lld", me, me->opts.file, me->count, me->free);

    // Force commit any pending changes before closing
    storage_commit(me, 1, NULL);

    if (me->cache) {
        DEBUG("freeing cache %p", me->cache);
        me->cache->free(me->cache);
        me->cache = NULL;
    }
    if (me->h) {
        DEBUG("freeing header mapping %p", me->h);
        me->h->free(me->h);
        me->h = NULL;
    }
    if (me->fd > 0) {
        DEBUG("closing fd");
        close(me->fd);
    }
    if (me->clean) {
        DEBUG("freeing clean buffer");
        FREE(me->clean);
        me->clean = NULL;
    }

    if (me->dio_scratch) {
        FREE(me->dio_scratch);
        me->dio_scratch = NULL;
    }
    if (me->dio_chunk) {
        FREE(me->dio_chunk);
        me->dio_chunk = NULL;
        me->dio_chunk_bytes = 0;
    }
    if (me->dio_wbatch) {
        FREE(me->dio_wbatch);
        me->dio_wbatch = NULL;
        me->dio_wbatch_bytes = 0;
        me->dio_wbatch_blocks = 0;
        me->dio_wbatch_count = 0;
        me->dio_wbatch_base = 0;
    }
    me->dio_last_inflated_chunk = -1;
    me->fd = -1;
}

/**
 * @brief Direct I/O storage
 * 
 * @param me 
 * @param opts 
 * @param e 
 * @return int 
 */
static int storage_dio_open(struct storage * me, struct storage_opts opts, char **e) {
    if (!me)
        return -1;

    // DIO requires alignment to sector size (typically 512 bytes)
    #define DIO_SECTOR_SIZE 512
    i32 raw_block_bytes = (opts.compact <= 0) ? (BLOCK_HEADER_BYTES + opts.block_bytes) : (BLOCK_HEADER_BYTES + (opts.compact));
    // Round up to DIO_SECTOR_SIZE alignment
    me->block_bytes = ((raw_block_bytes + DIO_SECTOR_SIZE - 1) / DIO_SECTOR_SIZE) * DIO_SECTOR_SIZE;
    me->clean = CALLOC(1, me->block_bytes);
    me->increment = (opts.increment <= 0) ? DEFAULT_INCREMENT_BYTES : opts.increment;
    me->mmap_bytes = me->block_bytes * (me->increment / me->block_bytes);
    assert(me->increment % OS_PAGE_SIZE == 0); // O_DIRECT requires aligned sizes
    assert(me->mmap_bytes % OS_PAGE_SIZE == 0); // O_DIRECT requires aligned sizes
    assert(me->block_bytes % DIO_SECTOR_SIZE == 0); // O_DIRECT requires sector-aligned block sizes

    memcpy(&me->opts, &opts, sizeof(struct storage_opts));

    me->dio_last_inflated_chunk = -1;

    // Initialize sequential write batch buffer.
    me->dio_wbatch = NULL;
    me->dio_wbatch_bytes = 0;
    me->dio_wbatch_blocks = 0;
    me->dio_wbatch_count = 0;
    me->dio_wbatch_base = 0;

    // Allocate an aligned write batch sized to whole pages.
    // This targets write-heavy workloads by reducing pwrite syscalls.
    u32 pages = (DIO_WRITE_BATCH_PAGES <= 0) ? 1u : (u32)DIO_WRITE_BATCH_PAGES;
    u32 batch_bytes = (u32)OS_PAGE_SIZE * pages;
    if (batch_bytes < (u32)me->block_bytes) batch_bytes = (u32)me->block_bytes;
    // Ensure batch_bytes is a multiple of block_bytes AND sector-aligned
    batch_bytes = (batch_bytes / (u32)me->block_bytes) * (u32)me->block_bytes;
    if (batch_bytes == 0) batch_bytes = (u32)me->block_bytes;
    // Final alignment check for DIO
    assert(batch_bytes % DIO_SECTOR_SIZE == 0);
    void *wb = storage_dio_get_aligned(me, batch_bytes, &me->dio_wbatch, &me->dio_wbatch_bytes, e);
    if (e && *e) THROW_S(*e);
    (void)wb;
    me->dio_wbatch_blocks = (u32)(me->dio_wbatch_bytes / (u32)me->block_bytes);
    me->dio_wbatch_count = 0;

    char dir[PATH_MAX] = {0};
    getdir(me->opts.file, dir);
    mkdirs(dir, S_IRWXU);

    // O_DIRECT removed: incompatible with mmap and can hurt performance
    me->fd = open(me->opts.file, (opts.mode == FLINTDB_RDWR ? O_RDWR | O_CREAT : O_RDONLY), S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (me->fd < 0) {
        THROW(e, "Cannot open file %s: %s", me->opts.file, strerror(errno));
    }

    #ifdef __linux__
        // Advise kernel about sequential write pattern for better I/O scheduling
        posix_fadvise(me->fd, 0, 0, POSIX_FADV_SEQUENTIAL);
        fcntl(me->fd, F_SETFL, O_DIRECT);
    #endif
    #ifdef __APPLE__
        // macOS: optionally disable OS cache.
        // Default keeps previous behavior (nocache enabled). Set FLINTDB_DIO_NOCACHE=0 to allow caching for speed.
        int nocache = 1;
        const char *env_nocache = getenv("FLINTDB_DIO_NOCACHE");
        if (env_nocache && (strcmp(env_nocache, "0") == 0 || strcasecmp(env_nocache, "false") == 0 || strcasecmp(env_nocache, "off") == 0)) {
            nocache = 0;
        }
        if (nocache) {
            fcntl(me->fd, F_NOCACHE, 1);
        }
    #endif

    int bytes = file_length(me->opts.file);
    if (bytes < HEADER_BYTES)
        ftruncate(me->fd, HEADER_BYTES);

    // Map the file header with MAP_SHARED so updates (e.g., magic, counts) are persisted to disk
    // Using MAP_PRIVATE here would create a private COW mapping and header writes wouldn't be visible
    // to other processes (e.g., Java reader) nor persisted after process exit.
    void *p = mmap(NULL, HEADER_BYTES, PROT_READ | (opts.mode == FLINTDB_RDWR ? PROT_WRITE : 0), MAP_SHARED, me->fd, 0);
    if (p == MAP_FAILED) 
        THROW(e, "Cannot mmap file %s: %s", me->opts.file, strerror(errno)); 

    me->h = buffer_mmap(p, 0, HEADER_BYTES);

    me->cache = hashmap_new(MAPPED_BYTEBUFFER_POOL_SIZE, hashmap_int_hash, hashmap_int_cmpr);

    if (!me->cache) THROW(e, "Cannot create cache");

    me->close = storage_dio_close;
    me->count_get = storage_count_get;
    me->bytes_get = storage_bytes_get;
    me->read = storage_dio_read;
    me->write = storage_dio_write;
    me->write_at = storage_dio_write_at;
    me->delete   = storage_dio_delete;
    me->transaction = storage_transaction;
    me->mmap = storage_mmap;
    me->head = storage_head;

    if (bytes < HEADER_BYTES) {
        // Fresh file: initialize free-list head to first block index (1)
        me->free = 0;
        me->count = 0;
        storage_commit(me, 1, e);
        if (e && *e) THROW_S(e);
    }
    else {
        struct buffer *h = me->h;
        struct buffer bb = {0};
        h->slice(h, CUSTOM_HEADER_BYTES, COMMON_HEADER_BYTES, &bb, e);
        if (e && *e) THROW_S(e);

        bb.i64_get(&bb, e); // reserved
        me->free = bb.i64_get(&bb, e); // The front of deleted blocks
        bb.i64_get(&bb, e); // The tail of deleted blocks => not used in mmap
        bb.i16_get(&bb, e); // version:i16
        i32 inc = bb.i32_get(&bb, e); // increment:i32
        if (inc <= 0)
            THROW(e, "Invalid increment size: %d, file:%s", inc, me->opts.file); // old version was (10MB)
        if (inc != me->increment) {
            me->increment = inc;
            me->mmap_bytes = me->block_bytes * (me->increment / me->block_bytes);
        }
        bb.skip(&bb, R24LEN); // reserved
        i16 blksize = bb.i16_get(&bb, e); // BLOCK Data Max Size (exclude BLOCK Header)
        if (blksize != opts.block_bytes) {
            THROW(e, "Block size mismatch: header=%d, opts=%d", blksize, opts.block_bytes);
        }
        me->count = bb.i64_get(&bb, e);
        assert(me->count > -1);
    }

    return 0;

EXCEPTION:
    // WARN("%s", e);
    if (me) storage_dio_close(me);
    return -1;
}

/**
* Compressed storage
*/
static int storage_compression_open(struct storage * me, struct storage_opts opts, char **e) {
    THROW(e, "Unsupported storage type: %s", opts.type);
EXCEPTION:
    return -1;
}


int storage_open(struct storage * me, struct storage_opts opts, char **e) {
    if (strncasecmp(opts.type, TYPE_MEMORY, sizeof(TYPE_MEMORY)-1) == 0)
        return storage_mem_open(me, opts, e);

    if (strncasecmp(opts.type, TYPE_Z, sizeof(TYPE_Z)-1) == 0 
        || strncasecmp(opts.type, TYPE_LZ4, sizeof(TYPE_LZ4)-1) == 0 
        || strncasecmp(opts.type, TYPE_ZSTD, sizeof(TYPE_ZSTD)-1) == 0 
        || strncasecmp(opts.type, TYPE_SNAPPY, sizeof(TYPE_SNAPPY)-1) == 0
    ) 
        return storage_compression_open(me, opts, e);

    // printf("storage type: %s\n", opts.type);
    if (strncasecmp(opts.type, TYPE_DIO, sizeof(TYPE_DIO)-1) == 0)
        return storage_dio_open(me, opts, e);

    return storage_mmap_open(me, opts, e);
}
