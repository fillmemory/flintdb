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

static inline int _ftruncate(i32 fd, off_t length) {
    #ifdef __linux__
        int rc = posix_fallocate(fd, 0, (off_t)length);
        if (rc != 0) {
            errno = rc;
            return -1;
        }
        return ftruncate(fd, (off_t)length);
    #elif defined(__APPLE__)
        fstore_t fst;
        memset(&fst, 0, sizeof(fst));
        fst.fst_flags = F_ALLOCATECONTIG;
        fst.fst_posmode = F_PEOFPOSMODE;
        fst.fst_offset = 0;
        fst.fst_length = length;

        if (fcntl(fd, F_PREALLOCATE, &fst) == -1) {
            fst.fst_flags = F_ALLOCATEALL;
            if (fcntl(fd, F_PREALLOCATE, &fst) == -1) {
                return -1;
            }
        }
        return ftruncate(fd, length);
    #else
        return ftruncate(fd, length);
    #endif
}

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
    struct stat st;
    fstat(me->fd, &st);
    if (limit > st.st_size) {
        _ftruncate(me->fd, limit);
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

    struct stat st;
    fstat(me->fd, &st);

    i64 offset = HEADER_BYTES + (i * me->mmap_bytes);
    i64 before = st.st_size;
    struct buffer *mbb = storage_mmap(me, offset, me->mmap_bytes, NULL);
    me->cache->put(me->cache, i, (valtype)mbb, storage_cache_free);

    fstat(me->fd, &st);
    if (me->opts.mode == FLINTDB_RDWR && before < st.st_size) {
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
            storage_commit(me, 0, e); // was 1
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

    struct stat st;
    fstat(me->fd, &st);
    if (st.st_size >= 0 && st.st_size < (i64)HEADER_BYTES)
        _ftruncate(me->fd, (off_t)HEADER_BYTES);

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

    if (st.st_size >= 0 && st.st_size < (i64)HEADER_BYTES) {
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

    // Allocate new memory buffer
    struct buffer *mbb = buffer_alloc(me->mmap_bytes);
    if (!mbb) {
        e = "Out of memory";
        THROW_S(e);
    }
    
    me->cache->put(me->cache, i, (valtype)mbb, storage_cache_free);

    if (me->opts.mode == FLINTDB_RDWR) {
        i32 blocks = me->mmap_bytes / me->block_bytes;
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
    me->mmap_bytes = me->block_bytes * (me->increment / me->block_bytes);

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

#ifdef STORAGE_DIO_USE_BUFFER_POOL
    #define BUFFER_POOL_BORROW(length) me->pool->borrow(me->pool, length);
#else
    #define BUFFER_POOL_BORROW(length) buffer_alloc(length);
#endif

// #define DIO_CACHE_SIZE 32768  // ~32K entries

struct storage_dio_priv {
    // Add any private fields if necessary
};

static inline void storage_dio_commit(struct storage *me, u8 force, char **e) {
    storage_commit(me, force, e);
}

/**
 * @brief relative -> absolute
 * 
 * @param me 
 * @param offset 
 * @return * i64 
 */
static inline i64 storage_dio_file_offset(struct storage *me, i64 offset) {
    return (offset * me->block_bytes) + HEADER_BYTES;
}

static inline ssize_t storage_dio_buffer_get(struct storage *me, i64 offset, struct buffer **out) {
    i64 o = storage_dio_file_offset(me, offset);
    //struct buffer *bb = buffer_alloc(me->block_bytes);
    struct buffer *bb = BUFFER_POOL_BORROW(me->block_bytes);
    ssize_t n = pread(me->fd, bb->array, bb->capacity, o);
    // LOG("storage_dio_buffer_get: offset=%lld, file_offset=%lld, bytes_read=%zd", offset, o, n);
    if (n <= 0 && bb) {
        bb->free(bb);
        if (out) *out = NULL;
    } else {
        bb->clear(bb);
        *out = bb;
    }
    return n;
}

static inline ssize_t pwrite_all(int fd, char *buf, size_t bytes, i64 absolute) {
    size_t total_written = 0;
    while (total_written < bytes) {
        ssize_t written = pwrite(fd, buf + total_written, bytes - total_written, absolute + total_written);
        if (written < 0) {
            return -1; // Error
        }
        total_written += written;
    }
    return total_written;
}

static inline ssize_t storage_dio_pflush(struct storage *me) {
#ifdef DIO_CACHE_SIZE
    int fd = me->fd;
    struct hashmap *cache = me->cache;

    struct map_iterator itr = {0};
    while (cache->iterate(cache, &itr)) {
        i64 abs = itr.key;
        struct buffer *heap = (struct buffer *)itr.val;
        ssize_t n = pwrite_all(fd, heap->array, heap->capacity, abs);
        if (n < 0) 
            return -1;
    }
    // `cache` owns the buffers (dealloc set at put); clearing releases them.
    cache->clear(cache);
    return fsync(fd);
#else
    return 0;
#endif
}

static inline ssize_t storage_dio_pwrite(struct storage *me, struct buffer *heap, i64 absolute) {
#ifdef DIO_CACHE_SIZE
    struct hashmap *cache = me->cache;
    if (cache->count_get(cache) >= DIO_CACHE_SIZE) {
        ssize_t n = storage_dio_pflush(me);
        if (n < 0) 
            return -1;
    }
    // Cache owns `heap` and will free it on overwrite/clear/free.
    cache->put(cache, absolute, (valtype)heap, storage_cache_free);
    return heap->capacity;
#else
    ssize_t written = pwrite_all(me->fd, heap->array, heap->capacity, absolute);
    heap->free(heap);
    return written;
#endif
}

static inline i8 storage_dio_file_inflate(struct storage *me, i64 offset, char **e) {
    assert(me);
    assert(me->opts.mode == FLINTDB_RDWR);

    i64 o = storage_dio_file_offset(me, offset);
    struct stat st;
    if (fstat(me->fd, &st) < 0) 
        return -1;

    if (o >= st.st_size) {
        i32 length = me->mmap_bytes;
        off_t l = st.st_size + (me->mmap_bytes);
        _ftruncate(me->fd, l);

        i64 absolute = me->block_bytes * offset;
        i64 i = absolute / me->mmap_bytes;
        i32 blocks = length / me->block_bytes;
        i64 next = (i * blocks);
        struct buffer *bb = NULL;
        for(i32 x=0; x<blocks; x++) {
            storage_dio_buffer_get(me, next, &bb);
            if (!bb) THROW(e, "storage_dio_file_inflate: pread failed at offset=%lld", next);
            bb->i8_put(bb, STATUS_EMPTY, NULL);
            bb->i8_put(bb, MARK_AS_UNUSED, NULL);
            bb->i16_put(bb, 0, NULL);
            bb->i32_put(bb, 0, NULL);
            bb->i64_put(bb, next + 1, NULL);
            bb->flip(bb);
            //storage_dio_pwrite(me, bb->array, bb->capacity, storage_dio_file_offset(me, next));
            storage_dio_pwrite(me, bb, storage_dio_file_offset(me, next));
            next++;
            // ownership transferred to the DIO write cache
            bb = NULL;
        }
        storage_commit(me, 1, e);
        if (e && *e) THROW_S(e);
        return 1;
    }

    return 0;
EXCEPTION:
    return -1;
}


static struct buffer * storage_dio_read(struct storage *me, i64 offset, char **e) {
    struct buffer *blk = NULL;
    storage_dio_buffer_get(me, offset, &blk);
    if (!blk) THROW(e, "storage_dio_read: pread failed at offset=%lld", offset);
    u8 status = blk->i8_get(blk, e);
    if (status != STATUS_SET) THROW(e, "Block at offset %lld is not set", offset);

    u8 mark = blk->i8_get(blk, e);
    if (mark != MARK_AS_DATA) THROW(e, "Block at offset %lld is not data", offset);
    i16 limit = blk->i16_get(blk, e);
    i32 length = blk->i32_get(blk, e);
    i64 next = blk->i64_get(blk, e);

    if (next > NEXT_END && length > (me->opts.block_bytes)) {
        struct buffer *out = BUFFER_POOL_BORROW((u32)length);
        // copy only the first chunk (limit) from the first block
        out->array_put(out, blk->array_get(blk, limit, NULL), (u32)limit, NULL);
        blk->free(blk);
        blk = NULL;
        for(; next > NEXT_END; ) {
            struct buffer *n = NULL;
            storage_dio_buffer_get(me, next, &n);
            if (!n) THROW(e, "storage_dio_read: pread failed at offset=%lld", next);
            if (STATUS_SET != n->i8_get(n, NULL)) {
                n->free(n);
                break;
            }
            n->i8_get(n, NULL); // MARK
            i16 remains = n->i16_get(n, NULL);
            n->i32_get(n, NULL);
            next = n->i64_get(n, NULL);

            out->array_put(out, n->array_get(n, remains, NULL), (u32)remains, NULL);
            n->free(n);
        }
        out->flip(out);
        return out;
    }

    // Non-overflow: return an owning buffer (do not leak the read block buffer).
    struct buffer *out = BUFFER_POOL_BORROW((u32)limit);
    out->array_put(out, blk->array_get(blk, limit, NULL), (u32)limit, NULL);
    out->flip(out);
    blk->free(blk);
    return out;

EXCEPTION:
    if (blk) blk->free(blk);
    return NULL;
}

static i32 storage_dio_delete(struct storage *me, i64 offset, char **e) {

    return 0;
}

static inline void storage_dio_write_priv(struct storage *me, i64 offset, u8 mark, struct buffer *in, char **e) {
    assert(me != NULL);

    const int BLOCK_DATA_BYTES = me->opts.block_bytes;
    i64 curr = offset;
    u8 curr_mark = mark;
    i32 remaining = in->remaining(in);
    i64 next_last = NEXT_END;
    struct buffer *p = NULL;

    while (1) {
        struct buffer c = {0};
        storage_dio_file_inflate(me, curr, e);
        if (e && *e) THROW_S(*e);
        storage_dio_buffer_get(me, curr, &p);
        if (!p) THROW(e, "storage_dio_write_priv: pread failed at offset=%lld", curr);
        p->slice(p, 0, p->remaining(p), &c, e);

        u8 status = c.i8_get(&c, NULL);
        c.i8_get(&c, NULL);      // mark
        c.i16_get(&c, NULL);     // data length
        c.i32_get(&c, NULL);     // total length
        i64 next = c.i64_get(&c, NULL);

        int chunk = (remaining < BLOCK_DATA_BYTES ? remaining : BLOCK_DATA_BYTES);
        p->i8_put(p, STATUS_SET, NULL);
        p->i8_put(p, curr_mark, NULL);
        p->i16_put(p, (i16)chunk, NULL);
        p->i32_put(p, remaining, NULL);

        if (STATUS_SET != status) {
            me->count++;
            me->free = next; // relink free list
        }

        i64 next_index = NEXT_END;
        if (remaining > BLOCK_DATA_BYTES) {
            next_index = (next > NEXT_END ? next : me->free);
            p->i64_put(p, next_index, NULL);
        } else {
            p->i64_put(p, NEXT_END, NULL);
        }

        // copy data chunk from input to mapped block
        p->array_put(p, in->array_get(in, chunk, NULL), chunk, NULL);
        // pad remaining of block
        int pad = BLOCK_DATA_BYTES - chunk;
        if (pad > 0) {
            p->array_put(p, me->clean, pad, NULL);
        }

        p->flip(p);
        i64 o = storage_dio_file_offset(me, curr);
        // storage_dio_pwrite(me, p->array, p->remaining(p), o);
        storage_dio_pwrite(me, p, o);
        // ownership transferred to the DIO write cache
        p = NULL;

        remaining -= chunk;
        next_last = next;
        if (remaining <= 0) {
            if (next_last > NEXT_END && next_last != curr) {
                storage_dio_delete(me, next_last, e);
            }
            storage_dio_commit(me, 0, e);
            if (e && *e) THROW_S(e);
            break;
        }

        curr = next_index;
        curr_mark = MARK_AS_NEXT;
    }

EXCEPTION:
    if (p) p->free(p);
    return;
}

static i64 storage_dio_write(struct storage *me, struct buffer *in, char **e) {
    i64 offset = me->free;
    storage_dio_write_priv(me, offset, MARK_AS_DATA, in, e);
    return offset;
}

static i64 storage_dio_write_at(struct storage *me, i64 offset, struct buffer *in, char **e) {
    storage_dio_write_priv(me, offset, MARK_AS_DATA, in, e);
    return offset;
}

static void storage_dio_close(struct storage *me) {
    assert(me != NULL);
    if (me->fd <= 0) return;

    storage_dio_pflush(me);
    storage_dio_commit(me, 1, NULL);

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
    if (me->clean) {
        DEBUG("freeing clean buffer");
        FREE(me->clean);
        me->clean = NULL;
    }
    #ifdef STORAGE_DIO_USE_BUFFER_POOL
    if (me->pool) {
        DEBUG("freeing pool buffer");
        me->pool->free(me->pool);
        me->pool = NULL;
    }
    #endif
    if (me->priv) {
        FREE(me->priv);
        me->priv = NULL;
    }
    if (me->fd > 0) {
        DEBUG("closing fd");
        close(me->fd);
    }
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

    DEBUG("DIO OPEN: file=%s, mode=%d, block_bytes=%d", opts.file, opts.mode, opts.block_bytes);

    // Round up to DIO_SECTOR_SIZE alignment
    me->block_bytes = (opts.compact <= 0) ? (BLOCK_HEADER_BYTES + opts.block_bytes) : (BLOCK_HEADER_BYTES + (opts.compact));
    me->clean = CALLOC(1, me->block_bytes);
    me->increment = (opts.increment <= 0) ? DEFAULT_INCREMENT_BYTES : opts.increment;
    me->mmap_bytes = me->block_bytes * (me->increment / me->block_bytes);
    assert(me->increment % OS_PAGE_SIZE == 0); // O_DIRECT requires aligned sizes
    assert(me->mmap_bytes % OS_PAGE_SIZE == 0); // O_DIRECT requires aligned sizes

    memcpy(&me->opts, &opts, sizeof(struct storage_opts));


    char dir[PATH_MAX] = {0};
    getdir(me->opts.file, dir);
    mkdirs(dir, S_IRWXU);

    // O_DIRECT removed: incompatible with mmap and can hurt performance
    me->fd = open(me->opts.file, (opts.mode == FLINTDB_RDWR ? O_RDWR | O_CREAT : O_RDONLY), S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (me->fd < 0) {
        THROW(e, "Cannot open file %s: %s", me->opts.file, strerror(errno));
    }

    // Default keeps previous behavior (nocache enabled). Set FLINTDB_DIO_NOCACHE=0 to allow caching for speed.
    int oscache = 1;
    const char *env_oscache = getenv("FLINTDB_DIO_OS_CACHE");
    if (env_oscache && (strcmp(env_oscache, "0") == 0 || strcasecmp(env_oscache, "false") == 0 || strcasecmp(env_oscache, "off") == 0)) {
        oscache = 0;
    }

    #ifdef __APPLE__
        if (oscache == 0)
            fcntl(me->fd, F_NOCACHE, 1); // F_GLOBAL_NOCACHE
        // macOS doesn't provide posix_fadvise/POSIX_FADV_* in all SDKs; use fcntl hints instead.
        #ifdef F_RDAHEAD
            (void)fcntl(me->fd, F_RDAHEAD, 1);
        #endif
        #ifdef F_RDADVISE
            {
                struct radvisory ra;
                memset(&ra, 0, sizeof(ra));
                ra.ra_offset = 0;
                ra.ra_count = 0; // let kernel decide / whole file
                (void)fcntl(me->fd, F_RDADVISE, &ra);
            }
        #endif
    #endif

    #ifdef __linux__
        if (oscache == 0) 
             fcntl(me->fd, F_SETFL, O_DIRECT);
        #if defined(POSIX_FADV_SEQUENTIAL)
            (void)posix_fadvise(me->fd, 0, 0, POSIX_FADV_SEQUENTIAL);
        #endif
    #endif

    struct stat st;
    fstat(me->fd, &st);
    if (st.st_size >= 0 && st.st_size < (i64)HEADER_BYTES)
        _ftruncate(me->fd, (off_t)HEADER_BYTES);

    // Map the file header with MAP_SHARED so updates (e.g., magic, counts) are persisted to disk
    // Using MAP_PRIVATE here would create a private COW mapping and header writes wouldn't be visible
    // to other processes (e.g., Java reader) nor persisted after process exit.
    void *p = mmap(NULL, HEADER_BYTES, PROT_READ | (opts.mode == FLINTDB_RDWR ? PROT_WRITE : 0), MAP_SHARED, me->fd, 0);
    if (p == MAP_FAILED) 
        THROW(e, "Cannot mmap file %s: %s", me->opts.file, strerror(errno)); 

    me->h = buffer_mmap(p, 0, HEADER_BYTES);

    // DIO doesn't use cache - it bypasses OS page cache and uses direct I/O
    // Cache is only needed if storage_mmap is called, which is rare in DIO mode
    // me->cache = NULL;
#ifdef DIO_CACHE_SIZE
    me->cache = hashmap_new(DIO_CACHE_SIZE, hashmap_int_hash, hashmap_int_cmpr); // batch write cache
#endif

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

    if (st.st_size >= 0 && st.st_size < (i64)HEADER_BYTES) {
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

    #ifdef STORAGE_DIO_USE_BUFFER_POOL
    LOG("Initializing DIO buffer pool: block_bytes=%d", me->block_bytes);
    me->pool = buffer_pool_safe_create(STORAGE_DIO_USE_BUFFER_POOL, me->block_bytes, 0); // 256K blocks
    #endif

    me->priv = CALLOC(1, sizeof(struct storage_dio_priv));
    if (!me->priv) THROW(e, "Cannot allocate DIO private data");

    // LOG("count=%lld, free=%lld", me->count, me->free);
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


/**
 * @brief Open storage based on type
 * 
 * @param me 
 * @param opts 
 * @param e 
 * @return int 
 */
int storage_open(struct storage * me, struct storage_opts opts, char **e) {
    if (strncasecmp(opts.type, TYPE_MEMORY, sizeof(TYPE_MEMORY)-1) == 0)
        return storage_mem_open(me, opts, e);

    if (strncasecmp(opts.type, TYPE_Z, sizeof(TYPE_Z)-1) == 0 
        || strncasecmp(opts.type, TYPE_LZ4, sizeof(TYPE_LZ4)-1) == 0 
        || strncasecmp(opts.type, TYPE_ZSTD, sizeof(TYPE_ZSTD)-1) == 0 
        || strncasecmp(opts.type, TYPE_SNAPPY, sizeof(TYPE_SNAPPY)-1) == 0
    ) 
        return storage_compression_open(me, opts, e);

    if (strncasecmp(opts.type, TYPE_DIO, sizeof(TYPE_DIO)-1) == 0)
        return storage_dio_open(me, opts, e); // Experimental Direct I/O storage

    return storage_mmap_open(me, opts, e);
}
