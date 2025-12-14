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
    if (!me->h) {
        THROW(e, "storage_head: header mapping is NULL (file=%s)", me->opts.file);
    }
    struct buffer *out = buffer_slice(me->h, offset, length, e);
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

    i32 page_size = getpagesize(); // sysconf(_SC_PAGESIZE)
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

    return storage_mmap_open(me, opts, e);
}
