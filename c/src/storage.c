#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <assert.h>
#include <stdarg.h>
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
#define STORAGE_COMMIT_FORCE 1
#define STORAGE_COMMIT_LAZY 0
#define STORAGE_COMMIT_DEFAULT 0

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
        storage_commit(me, STORAGE_COMMIT_LAZY, &e);
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

    storage_commit(me, STORAGE_COMMIT_LAZY, e);
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
            storage_commit(me, STORAGE_COMMIT_DEFAULT, e); // was 1
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
    storage_commit(me, STORAGE_COMMIT_FORCE, NULL);

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
        storage_commit(me, STORAGE_COMMIT_FORCE, e);
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
        storage_commit(me, STORAGE_COMMIT_DEFAULT, &e);
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

    storage_commit(me, STORAGE_COMMIT_LAZY, e);
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
            storage_commit(me, STORAGE_COMMIT_DEFAULT, e);
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

    storage_commit(me, STORAGE_COMMIT_FORCE, NULL);

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
    storage_commit(me, STORAGE_COMMIT_FORCE, e);
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

#define DIO_CACHE_SIZE (1 * 1024 * 1024) // 1 million blocks


struct storage_dio_priv {
    int drop_os_cache; // best-effort OS page cache drop (Linux: posix_fadvise DONTNEED, macOS: F_NOCACHE)

    // True if the file was opened as a brand-new DIO file (no existing blocks).
    // Used to safely avoid pread() RMW when creating page-cache entries.
    int fresh_file;

    // Cached file size used by storage_dio_file_inflate to avoid per-write fstat().
    i64 inflated_size;

    // Linux O_DIRECT mode: actual IO is performed in page-sized aligned units.
    int o_direct_enabled;
    u32 direct_align;     // e.g., 4096
    u32 direct_io_bytes;  // e.g., 4096 (must be multiple of direct_align)
    u32 page_cache_limit; // max cached pages before flush
};

static inline int env_truthy(const char *v) {
    return v && (strcmp(v, "1") == 0 || strcasecmp(v, "true") == 0 || strcasecmp(v, "on") == 0 || strcasecmp(v, "yes") == 0);
}

static inline i64 align_down_i64(i64 x, i64 a) {
    return (a <= 0) ? x : (x / a) * a;
}

static inline i64 align_up_i64(i64 x, i64 a) {
    if (a <= 0) return x;
    return ((x + a - 1) / a) * a;
}

// Pick a DIO inflate chunk size (mmap_bytes) that satisfies:
// - divisible by block_bytes (so per-block initialization is exact)
// - divisible by OS_PAGE_SIZE (so cache / IO alignment assumptions hold)
static inline i64 storage_dio_chunk_bytes(i64 block_bytes, i64 target_bytes) {
    if (block_bytes <= 0) return align_up_i64(target_bytes, (i64)OS_PAGE_SIZE);
    if (target_bytes < block_bytes) target_bytes = block_bytes;

    i64 blocks = (target_bytes + block_bytes - 1) / block_bytes; // ceil
    if (blocks < 1) blocks = 1;
    i64 length = blocks * block_bytes;

    // Increase by whole blocks until we land on an OS page boundary.
    // This loop is bounded by OS_PAGE_SIZE / gcd(OS_PAGE_SIZE, block_bytes).
    while ((length % (i64)OS_PAGE_SIZE) != 0) {
        blocks++;
        length += block_bytes;
    }
    return length;
}

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

// Uninitialized/sparse blocks may contain zeros, and on some platforms preallocation can
// return non-deterministic bytes. Defensively interpret any invalid header as an empty block
// with linear free-list linkage.
static inline void storage_dio_fixup_uninitialized_meta(i64 offset, u8 *status, u8 mark, i64 *next) {
    if (!status) return;
    if (*status == STATUS_SET) {
        if (mark != MARK_AS_DATA && mark != MARK_AS_NEXT) {
            *status = STATUS_EMPTY;
            if (next) *next = offset + 1;
        }
        return;
    }
    if (*status != STATUS_EMPTY) {
        *status = STATUS_EMPTY;
        if (next) *next = offset + 1;
    }
}

// Read just the on-disk block header (16 bytes) to discover allocation state and free-list linkage.
// This avoids a full block pread on the write path where we overwrite the entire block anyway.
static inline int storage_dio_block_meta_get(struct storage *me, i64 offset, u8 *status_out, i64 *next_out) {
    const i64 absolute = storage_dio_file_offset(me, offset);

#ifdef DIO_CACHE_SIZE
    // If write-back cache is enabled, honor read-your-writes for metadata too.
    if (me->cache) {
        // Page cache: keyed by OS_PAGE_SIZE-aligned page base.
        const i64 page_base = align_down_i64(absolute, (i64)OS_PAGE_SIZE);
        const u32 page_off = (u32)(absolute - page_base);
        valtype found = me->cache->get(me->cache, (keytype)page_base);
        if (HASHMAP_INVALID_VAL != found) {
            struct buffer *page = (struct buffer *)found;
            if (page && ((u32)page->capacity) >= (u32)OS_PAGE_SIZE) {
                u8 hdr[BLOCK_HEADER_BYTES];
                if (page_off + (u32)BLOCK_HEADER_BYTES <= (u32)page->capacity) {
                    memcpy(hdr, page->array + page_off, (size_t)BLOCK_HEADER_BYTES);
                } else {
                    const u32 first = (u32)page->capacity - page_off;
                    memcpy(hdr, page->array + page_off, (size_t)first);
                    const i64 page2_base = page_base + (i64)OS_PAGE_SIZE;
                    valtype found2 = me->cache->get(me->cache, (keytype)page2_base);
                    if (HASHMAP_INVALID_VAL != found2) {
                        struct buffer *page2 = (struct buffer *)found2;
                        if (page2 && ((u32)page2->capacity) >= (u32)OS_PAGE_SIZE) {
                            memcpy(hdr + first, page2->array, (size_t)((u32)BLOCK_HEADER_BYTES - first));
                        } else {
                            ssize_t rn = pread(me->fd, hdr + first, (size_t)((u32)BLOCK_HEADER_BYTES - first), absolute + (i64)first);
                            if (rn != (ssize_t)((u32)BLOCK_HEADER_BYTES - first)) return -1;
                        }
                    } else {
                        ssize_t rn = pread(me->fd, hdr + first, (size_t)((u32)BLOCK_HEADER_BYTES - first), absolute + (i64)first);
                        if (rn != (ssize_t)((u32)BLOCK_HEADER_BYTES - first)) return -1;
                    }
                }

                u8 status = hdr[0];
                u8 mark = hdr[1];
                i64 next = 0;
                memcpy(&next, hdr + 8, 8);
                storage_dio_fixup_uninitialized_meta(offset, &status, mark, &next);
                if (status_out) *status_out = status;
                if (next_out) *next_out = next;
                return 0;
            }
        }

        // Back-compat: if something stored block buffers keyed by absolute offset.
        found = me->cache->get(me->cache, (keytype)absolute);
        if (HASHMAP_INVALID_VAL != found) {
            struct buffer *cached = (struct buffer *)found;
            if (cached && cached->capacity >= (int)BLOCK_HEADER_BYTES) {
                u8 status = (u8)cached->array[0];
                u8 mark = (u8)cached->array[1];
                i64 next = 0;
                memcpy(&next, cached->array + 8, 8);
                storage_dio_fixup_uninitialized_meta(offset, &status, mark, &next);
                if (status_out) *status_out = status;
                if (next_out) *next_out = next;
                return 0;
            }
        }
    }
#endif

    u8 hdr[BLOCK_HEADER_BYTES];
    ssize_t n = pread(me->fd, hdr, (size_t)BLOCK_HEADER_BYTES, absolute);
    if (n != (ssize_t)BLOCK_HEADER_BYTES) {
        return -1;
    }

    u8 status = hdr[0];
    u8 mark = hdr[1];
    i64 next = 0;
    memcpy(&next, hdr + 8, 8);
    storage_dio_fixup_uninitialized_meta(offset, &status, mark, &next);
    if (status_out) *status_out = status;
    if (next_out) *next_out = next;
    return 0;
}

// Read just the on-disk block header (16 bytes) including MARK.
// Used for operations (like delete) that need to validate chain structure.
static inline int storage_dio_block_header_get(struct storage *me, i64 offset, u8 *status_out, u8 *mark_out, i64 *next_out) {
    const i64 absolute = storage_dio_file_offset(me, offset);

#ifdef DIO_CACHE_SIZE
    if (me->cache) {
        const i64 page_base = align_down_i64(absolute, (i64)OS_PAGE_SIZE);
        const u32 page_off = (u32)(absolute - page_base);
        valtype found = me->cache->get(me->cache, (keytype)page_base);
        if (HASHMAP_INVALID_VAL != found) {
            struct buffer *page = (struct buffer *)found;
            if (page && ((u32)page->capacity) >= (u32)OS_PAGE_SIZE) {
                u8 hdr[BLOCK_HEADER_BYTES];
                if (page_off + (u32)BLOCK_HEADER_BYTES <= (u32)page->capacity) {
                    memcpy(hdr, page->array + page_off, (size_t)BLOCK_HEADER_BYTES);
                } else {
                    const u32 first = (u32)page->capacity - page_off;
                    memcpy(hdr, page->array + page_off, (size_t)first);
                    const i64 page2_base = page_base + (i64)OS_PAGE_SIZE;
                    valtype found2 = me->cache->get(me->cache, (keytype)page2_base);
                    if (HASHMAP_INVALID_VAL != found2) {
                        struct buffer *page2 = (struct buffer *)found2;
                        if (page2 && ((u32)page2->capacity) >= (u32)OS_PAGE_SIZE) {
                            memcpy(hdr + first, page2->array, (size_t)((u32)BLOCK_HEADER_BYTES - first));
                        } else {
                            ssize_t rn = pread(me->fd, hdr + first, (size_t)((u32)BLOCK_HEADER_BYTES - first), absolute + (i64)first);
                            if (rn != (ssize_t)((u32)BLOCK_HEADER_BYTES - first)) return -1;
                        }
                    } else {
                        ssize_t rn = pread(me->fd, hdr + first, (size_t)((u32)BLOCK_HEADER_BYTES - first), absolute + (i64)first);
                        if (rn != (ssize_t)((u32)BLOCK_HEADER_BYTES - first)) return -1;
                    }
                }

                u8 status = hdr[0];
                u8 mark = hdr[1];
                i64 next = 0;
                memcpy(&next, hdr + 8, 8);
                storage_dio_fixup_uninitialized_meta(offset, &status, mark, &next);
                if (status_out) *status_out = status;
                if (mark_out) *mark_out = mark;
                if (next_out) *next_out = next;
                return 0;
            }
        }
    }
#endif

    u8 hdr[BLOCK_HEADER_BYTES];
    ssize_t n = pread(me->fd, hdr, (size_t)BLOCK_HEADER_BYTES, absolute);
    if (n != (ssize_t)BLOCK_HEADER_BYTES) {
        return -1;
    }

    u8 status = hdr[0];
    u8 mark = hdr[1];
    i64 next = 0;
    memcpy(&next, hdr + 8, 8);
    storage_dio_fixup_uninitialized_meta(offset, &status, mark, &next);
    if (status_out) *status_out = status;
    if (mark_out) *mark_out = mark;
    if (next_out) *next_out = next;
    return 0;
}

#if defined(__linux__) && defined(O_DIRECT)
static inline struct buffer *storage_dio_odirect_page_get(struct storage *me, struct storage_dio_priv *priv, i64 page_base, int *cached_out) {
    if (cached_out) *cached_out = 0;
    if (!priv) return NULL;

    const u32 io = priv->direct_io_bytes;
    struct hashmap *cache = me->cache;

    if (cache) {
        valtype found = cache->get(cache, (keytype)page_base);
        if (HASHMAP_INVALID_VAL != found) {
            if (cached_out) *cached_out = 1;
            return (struct buffer *)found;
        }
    }

    struct buffer *page = buffer_alloc_aligned(io, priv->direct_align);
    if (!page) return NULL;

    ssize_t r = pread(me->fd, page->array, page->capacity, page_base);
    if (r < 0) {
        page->free(page);
        return NULL;
    }
    if (r == 0) {
        memset(page->array, 0, (size_t)page->capacity);
    } else if ((u32)r < page->capacity) {
        memset(page->array + r, 0, (size_t)page->capacity - (size_t)r);
    }

    if (cache) {
        void *slot = cache->put(cache, (keytype)page_base, (valtype)page, storage_cache_free);
        if (slot) {
            if (cached_out) *cached_out = 1;
            return page;
        }
    }

    // Not cached; caller must write-through and free.
    if (cached_out) *cached_out = 0;
    return page;
}
#endif

static inline ssize_t storage_dio_buffer_get(struct storage *me, i64 offset, struct buffer **out) {
    i64 o = storage_dio_file_offset(me, offset);

#ifdef __linux__
    struct storage_dio_priv *priv = (struct storage_dio_priv *)me->priv;
#endif

#if defined(__linux__) && defined(O_DIRECT)
    // Linux O_DIRECT path: read a whole aligned page then slice/copy out block_bytes.
    if (priv && priv->o_direct_enabled) {
        const u32 io = priv->direct_io_bytes;
        const i64 page_base = align_down_i64(o, (i64)io);
        const u32 page_off = (u32)(o - page_base);

        // First: read-your-writes from the page cache.
        if (me->cache) {
            valtype found = me->cache->get(me->cache, (keytype)page_base);
            if (HASHMAP_INVALID_VAL != found) {
                struct buffer *page = (struct buffer *)found;
                struct buffer *bb = BUFFER_POOL_BORROW((u32)me->block_bytes);
                if (!bb) {
                    if (out) *out = NULL;
                    return -1;
                }
                bb->clear(bb);
                memcpy(bb->array, page->array + page_off, (size_t)me->block_bytes);
                if (out) *out = bb;
                return (ssize_t)me->block_bytes;
            }
        }

        // Miss: do an aligned page pread.
        struct buffer *page = buffer_alloc_aligned(io, priv->direct_align);
        if (!page) {
            if (out) *out = NULL;
            return -1;
        }
        ssize_t n = pread(me->fd, page->array, page->capacity, page_base);
        if (n <= 0) {
            page->free(page);
            if (out) *out = NULL;
            return n;
        }
        if ((u32)n < page->capacity) {
            memset(page->array + n, 0, (size_t)page->capacity - (size_t)n);
        }

        struct buffer *bb = BUFFER_POOL_BORROW((u32)me->block_bytes);
        if (!bb) {
            page->free(page);
            if (out) *out = NULL;
            return -1;
        }
        bb->clear(bb);
        memcpy(bb->array, page->array + page_off, (size_t)me->block_bytes);
        page->free(page);

        if (out) *out = bb;
        return (ssize_t)me->block_bytes;
    }
#endif

#ifdef DIO_CACHE_SIZE
    // If write-back cache is enabled, honor read-your-writes.
    if (me->cache) {
        // Page cache: keyed by OS_PAGE_SIZE-aligned page base.
        const i64 page_base = align_down_i64(o, (i64)OS_PAGE_SIZE);
        const u32 page_off = (u32)(o - page_base);

        const u32 need = (u32)me->block_bytes;
        const u32 first = (page_off + need <= (u32)OS_PAGE_SIZE) ? need : ((u32)OS_PAGE_SIZE - page_off);
        const u32 second = need - first;

        valtype found = me->cache->get(me->cache, (keytype)page_base);
        if (HASHMAP_INVALID_VAL != found) {
            struct buffer *cached = (struct buffer *)found;
            struct buffer *bb = BUFFER_POOL_BORROW(me->block_bytes);
            if (!bb) {
                if (out) *out = NULL;
                return -1;
            }
            bb->clear(bb);
            if (!cached || ((u32)cached->capacity) < (u32)OS_PAGE_SIZE) {
                memset(bb->array, 0, (size_t)bb->capacity);
            } else {
                memcpy(bb->array, cached->array + page_off, (size_t)first);
                if (second > 0) {
                    const i64 page2_base = page_base + (i64)OS_PAGE_SIZE;
                    valtype found2 = me->cache->get(me->cache, (keytype)page2_base);
                    if (HASHMAP_INVALID_VAL != found2) {
                        struct buffer *cached2 = (struct buffer *)found2;
                        if (cached2 && ((u32)cached2->capacity) >= (u32)OS_PAGE_SIZE) {
                            memcpy(bb->array + first, cached2->array, (size_t)second);
                        } else {
                            ssize_t rn = pread(me->fd, bb->array + first, (size_t)second, o + (i64)first);
                            if (rn != (ssize_t)second) {
                                bb->free(bb);
                                if (out) *out = NULL;
                                return -1;
                            }
                        }
                    } else {
                        ssize_t rn = pread(me->fd, bb->array + first, (size_t)second, o + (i64)first);
                        if (rn != (ssize_t)second) {
                            bb->free(bb);
                            if (out) *out = NULL;
                            return -1;
                        }
                    }
                }
            }
            if (out) *out = bb;
            return (ssize_t)bb->capacity;
        }

        // Back-compat: cached blocks keyed by absolute offset.
        found = me->cache->get(me->cache, (keytype)o);
        if (HASHMAP_INVALID_VAL != found) {
            struct buffer *cached2 = (struct buffer *)found;
            struct buffer *bb = BUFFER_POOL_BORROW(me->block_bytes);
            if (!bb) {
                if (out) *out = NULL;
                return -1;
            }
            bb->clear(bb);
            memcpy(bb->array, cached2->array, (size_t)bb->capacity);
            if (out) *out = bb;
            return (ssize_t)bb->capacity;
        }
    }
#endif

    //struct buffer *bb = buffer_alloc(me->block_bytes);
    struct buffer *bb = BUFFER_POOL_BORROW(me->block_bytes);
    ssize_t n = pread(me->fd, bb->array, bb->capacity, o);
#if defined(__linux__) && defined(POSIX_FADV_DONTNEED)
    // Best-effort: drop cache for the range we just touched.
    // This is safer than O_DIRECT (which requires strict alignment of buffers/offsets).
    
    if (priv && priv->drop_os_cache) {
        (void)posix_fadvise(me->fd, (off_t)o, (off_t)bb->capacity, POSIX_FADV_DONTNEED);
    }
#endif
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
        if (written == 0) {
            // Avoid infinite loop if the OS reports no progress.
            errno = EIO;
            return -1;
        }
        total_written += written;
    }
    return total_written;
}

static inline ssize_t storage_dio_pflush(struct storage *me) {
#ifdef DIO_CACHE_SIZE
    int fd = me->fd;
    struct hashmap *cache = me->cache;

#ifdef __linux__
    struct storage_dio_priv *priv = (struct storage_dio_priv *)me->priv;
#endif

#if defined(__linux__) && defined(O_DIRECT)
    if (priv && priv->o_direct_enabled) {
        const u32 io = priv->direct_io_bytes;
        unsigned long long total_bytes = 0ull;
        struct map_iterator itr = {0};
        while (cache->iterate(cache, &itr)) {
            i64 abs = (i64)itr.key; // page base
            struct buffer *page = (struct buffer *)itr.val;
            // Page buffers are aligned and sized for O_DIRECT.
            ssize_t n = pwrite_all(fd, page->array, page->capacity, abs);
            if (n < 0) return -1;
            total_bytes += (unsigned long long)n;
        }
        cache->clear(cache);
        (void)io; // silence unused if compiled without O_DIRECT semantics
        return (ssize_t)total_bytes;
    }
#endif

    // Non-O_DIRECT: coalesce sequential blocks into large writes.
    // In DIO mode the cache is a treemap (sorted by absolute offset), so
    // sequential appends become a small number of large pwrite() calls.
    // This is critical for TESTCASE_STORAGE_DIO performance.

    // Defensive guard: if the hashmap's iteration list is corrupted (cycle),
    // iterate() may never reach tail. Bound by (count + 1).
    int expected = cache ? cache->count_get(cache) : 0;
    int steps = 0;

    size_t batch_cap = (size_t)me->mmap_bytes;
    const size_t unit = (size_t)OS_PAGE_SIZE;
    if (batch_cap < unit) batch_cap = unit;
    batch_cap = (batch_cap / unit) * unit;
    if (batch_cap == 0) batch_cap = unit;

    char *batch = (char *)MALLOC(batch_cap);
    if (!batch) {
        return -1;
    }

    unsigned long long total_bytes = 0ull;
    i64 run_base = 0;
    i64 expected_abs = 0;
    size_t run_len = 0;

    struct map_iterator itr = {0};
    while (cache->iterate(cache, &itr)) {
        if (expected > 0 && ++steps > expected + 1) {
            WARN("storage_dio_pflush: iterator exceeded expected steps (count=%d); possible list corruption", expected);
            FREE(batch);
            return -1;
        }

        i64 abs = itr.key;
        struct buffer *heap = (struct buffer *)itr.val;
        if (!heap) continue;

        // Start a new run if needed.
        if (run_len == 0) {
            run_base = abs;
            expected_abs = abs;
        }

        // If this block isn't the next contiguous offset, flush current run.
        if (abs != expected_abs || (run_len + (size_t)heap->capacity) > batch_cap) {
            if (run_len > 0) {
                ssize_t wn = pwrite_all(fd, batch, run_len, run_base);
                if (wn < 0) {
                    FREE(batch);
                    return -1;
                }

#if defined(__linux__) && defined(POSIX_FADV_DONTNEED)
                if (priv && priv->drop_os_cache) {
                    (void)posix_fadvise(fd, (off_t)run_base, (off_t)run_len, POSIX_FADV_DONTNEED);
                }
#endif

                total_bytes += (unsigned long long)wn;
            }
            run_base = abs;
            expected_abs = abs;
            run_len = 0;
        }

        // Append buffer into batch.
        memcpy(batch + run_len, heap->array, (size_t)heap->capacity);
        run_len += (size_t)heap->capacity;
        expected_abs += (i64)heap->capacity;

        // Flush full batch.
        if (run_len == batch_cap) {
            ssize_t wn = pwrite_all(fd, batch, run_len, run_base);
            if (wn < 0) {
                FREE(batch);
                return -1;
            }

#if defined(__linux__) && defined(POSIX_FADV_DONTNEED)
            if (priv && priv->drop_os_cache) {
                (void)posix_fadvise(fd, (off_t)run_base, (off_t)run_len, POSIX_FADV_DONTNEED);
            }
#endif

            total_bytes += (unsigned long long)wn;
            run_len = 0;
        }
    }

    // Flush tail.
    if (run_len > 0) {
        ssize_t wn = pwrite_all(fd, batch, run_len, run_base);
        if (wn < 0) {
            FREE(batch);
            return -1;
        }

#if defined(__linux__) && defined(POSIX_FADV_DONTNEED)
        if (priv && priv->drop_os_cache) {
            (void)posix_fadvise(fd, (off_t)run_base, (off_t)run_len, POSIX_FADV_DONTNEED);
        }
#endif

        total_bytes += (unsigned long long)wn;
    }

    FREE(batch);

    // `cache` owns the buffers (dealloc set at put); clearing releases them.
    cache->clear(cache);

    // fsync can be very expensive and may look like a hang.
    // Keep previous behavior (no fsync) unless explicitly enabled.
    const char *env_fsync = getenv("FLINTDB_DIO_FSYNC");
    if (env_fsync && env_truthy(env_fsync)) {
        fsync(fd);
    }
    return total_bytes;
#else
    return 0;
#endif
}

static inline ssize_t storage_dio_pwrite(struct storage *me, struct buffer *heap, i64 absolute) {
#ifdef DIO_CACHE_SIZE
    struct hashmap *cache = me->cache;

    if (!heap) {
        return -1;
    }
    u32 nbytes = (u32)heap->remaining(heap);
    if (nbytes == 0) {
        heap->free(heap);
        return 0;
    }

    struct storage_dio_priv *priv = (struct storage_dio_priv *)me->priv;

#if defined(__linux__) && defined(O_DIRECT)
    // Linux O_DIRECT path: merge block writes into an aligned page cache.
    if (priv && priv->o_direct_enabled) {
        const u32 io = priv->direct_io_bytes;
        const i64 page_base = align_down_i64(absolute, (i64)io);
        const u32 page_off = (u32)(absolute - page_base);

        // Fetch or create cached page.
        struct buffer *page = NULL;
        if (cache) {
            valtype found = cache->get(cache, (keytype)page_base);
            if (HASHMAP_INVALID_VAL != found) {
                page = (struct buffer *)found;
            }
        }

        if (!page) {
            page = buffer_alloc_aligned(io, priv->direct_align);
            if (!page) {
                heap->free(heap);
                return -1;
            }

            // Read existing page contents (RMW).
            ssize_t r = pread(me->fd, page->array, page->capacity, page_base);
            if (r < 0) {
                page->free(page);
                heap->free(heap);
                return -1;
            }
            if (r == 0) {
                memset(page->array, 0, (size_t)page->capacity);
            } else if ((u32)r < page->capacity) {
                memset(page->array + r, 0, (size_t)page->capacity - (size_t)r);
            }

            // Insert into cache (cache owns page).
            if (!cache->put(cache, (keytype)page_base, (valtype)page, storage_cache_free)) {
                // If insertion fails, fall back to write-through page.
                ssize_t written = pwrite_all(me->fd, page->array, page->capacity, page_base);
                page->free(page);
                if (written < 0) {
                    heap->free(heap);
                    return -1;
                }
                // Proceed without caching.
                page = NULL;
            }
        }

        if (page) {
            memcpy(page->array + page_off, heap->array, (size_t)nbytes);
        }
        heap->free(heap);

        // Flush cache if too big.
        u32 limit = priv->page_cache_limit ? priv->page_cache_limit : 8192u;
        if (cache && (u32)cache->count_get(cache) >= limit) {
            if (storage_dio_pflush(me) < 0) {
                return -1;
            }
        }

        return (ssize_t)nbytes;
    }
#endif

    // Non-O_DIRECT path: merge block writes into an OS_PAGE_SIZE page cache.
    // IMPORTANT: block_bytes can straddle two pages if it does not divide OS_PAGE_SIZE.
    // Keep cache coherent by updating both pages when needed.
    if (cache) {
        const i64 page_base = align_down_i64(absolute, (i64)OS_PAGE_SIZE);
        const u32 page_off = (u32)(absolute - page_base);
        const u32 need = (u32)nbytes;
        const u32 first = (page_off + need <= (u32)OS_PAGE_SIZE) ? need : ((u32)OS_PAGE_SIZE - page_off);
        const u32 second = need - first;

        // Page 1 (base)
        struct buffer *page1 = NULL;
        valtype found1 = cache->get(cache, (keytype)page_base);
        if (HASHMAP_INVALID_VAL != found1) {
            page1 = (struct buffer *)found1;
        }
        if (!page1) {
            page1 = buffer_alloc_aligned((u32)OS_PAGE_SIZE, (u32)OS_PAGE_SIZE);
            if (!page1) {
                heap->free(heap);
                return -1;
            }
            ssize_t r = pread(me->fd, page1->array, page1->capacity, page_base);
            if (r < 0) {
                page1->free(page1);
                heap->free(heap);
                return -1;
            }
            if (r == 0) {
                memset(page1->array, 0, (size_t)page1->capacity);
            } else if ((u32)r < (u32)page1->capacity) {
                memset(page1->array + r, 0, (size_t)page1->capacity - (size_t)r);
            }
            if (!cache->put(cache, (keytype)page_base, (valtype)page1, storage_cache_free)) {
                // Fallback: write-through the block.
                ssize_t written = pwrite_all(me->fd, heap->array, (size_t)nbytes, absolute);
                page1->free(page1);
                heap->free(heap);
                return written;
            }
        }

        memcpy(page1->array + page_off, heap->array, (size_t)first);

        // Page 2 (next) if straddling
        if (second > 0) {
            const i64 page2_base = page_base + (i64)OS_PAGE_SIZE;
            struct buffer *page2 = NULL;
            valtype found2 = cache->get(cache, (keytype)page2_base);
            if (HASHMAP_INVALID_VAL != found2) {
                page2 = (struct buffer *)found2;
            }
            if (!page2) {
                page2 = buffer_alloc_aligned((u32)OS_PAGE_SIZE, (u32)OS_PAGE_SIZE);
                if (!page2) {
                    heap->free(heap);
                    return -1;
                }
                ssize_t r2 = pread(me->fd, page2->array, page2->capacity, page2_base);
                if (r2 < 0) {
                    page2->free(page2);
                    heap->free(heap);
                    return -1;
                }
                if (r2 == 0) {
                    memset(page2->array, 0, (size_t)page2->capacity);
                } else if ((u32)r2 < (u32)page2->capacity) {
                    memset(page2->array + r2, 0, (size_t)page2->capacity - (size_t)r2);
                }
                if (!cache->put(cache, (keytype)page2_base, (valtype)page2, storage_cache_free)) {
                    // Fallback: write-through the block.
                    ssize_t written = pwrite_all(me->fd, heap->array, (size_t)nbytes, absolute);
                    page2->free(page2);
                    heap->free(heap);
                    return written;
                }
            }
            memcpy(page2->array, heap->array + first, (size_t)second);
        }

        heap->free(heap);

        u32 limit = (priv && priv->page_cache_limit) ? priv->page_cache_limit : 8192u;
        if ((u32)cache->count_get(cache) >= limit) {
            if (storage_dio_pflush(me) < 0) {
                return -1;
            }
        }
        return (ssize_t)nbytes;
    }

    // No cache: write-through.
    ssize_t written = pwrite_all(me->fd, heap->array, (size_t)nbytes, absolute);
    heap->free(heap);
    return written;
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
    struct storage_dio_priv *priv = (struct storage_dio_priv *)me->priv;
    if (!priv) return -1;

    // Ensure internal size starts sane.
    if (priv->inflated_size < (i64)HEADER_BYTES) {
        priv->inflated_size = (i64)HEADER_BYTES;
    }

    const i64 need = o + (i64)me->block_bytes;
    if (need > priv->inflated_size) {
        const i32 length = me->mmap_bytes;

        // Extend by one or more whole chunks.
        i64 old_size = priv->inflated_size;
        i64 new_size = priv->inflated_size;
        while (need > new_size) {
            new_size += (i64)me->mmap_bytes;
        }
        if (_ftruncate(me->fd, (off_t)new_size) < 0) {
            THROW(e, "storage_dio_file_inflate: ftruncate failed to %lld bytes: %s", (long long)new_size, strerror(errno));
        }
        priv->inflated_size = new_size;

        // The old implementation did per-block pread+pwrite (and often via the write-back cache),
        // which is extremely slow for sequential growth. Initialize newly-extended regions in
        // large contiguous writes (one per chunk). If we extend by multiple chunks at once,
        // we must initialize each newly-added chunk.
        const i32 blocks = length / me->block_bytes;

        // Pick an alignment that is compatible with Linux O_DIRECT when enabled.
        u32 alignment = (u32)OS_PAGE_SIZE;
#if defined(__linux__) && defined(O_DIRECT)
        {
            struct storage_dio_priv *priv = (struct storage_dio_priv *)me->priv;
            if (priv && priv->o_direct_enabled && priv->direct_align) {
                alignment = priv->direct_align;
            }
        }
#endif

        struct buffer *chunk = buffer_alloc_aligned((u32)length, alignment);
        if (!chunk) THROW(e, "storage_dio_file_inflate: OOM allocating chunk (%d bytes)", length);

        const i16 z16 = 0;
        const i32 z32 = 0;
        for (i64 abs_first = old_size; abs_first < new_size; abs_first += (i64)length) {
            const i64 first = (abs_first - (i64)HEADER_BYTES) / (i64)me->block_bytes;

            // Zero all data; then stamp the free-list headers.
            memset(chunk->array, 0, (size_t)length);
            for (i32 x = 0; x < blocks; x++) {
                char *blk = chunk->array + ((i64)x * (i64)me->block_bytes);
                blk[0] = STATUS_EMPTY;
                blk[1] = MARK_AS_UNUSED;
                memcpy(blk + 2, &z16, 2);
                memcpy(blk + 4, &z32, 4);
                i64 next_ptr = first + (i64)x + 1;
                memcpy(blk + 8, &next_ptr, 8);
            }

            ssize_t wn = pwrite_all(me->fd, chunk->array, (size_t)length, abs_first);
            if (wn < 0) {
                chunk->free(chunk);
                THROW(e, "storage_dio_file_inflate: pwrite failed at abs=%lld (%d bytes)", abs_first, length);
            }


#if defined(__linux__) && defined(POSIX_FADV_DONTNEED)
            {
                struct storage_dio_priv *priv = (struct storage_dio_priv *)me->priv;
                if (priv && priv->drop_os_cache) {
                    (void)posix_fadvise(me->fd, (off_t)abs_first, (off_t)length, POSIX_FADV_DONTNEED);
                }
            }
#endif
        }

        chunk->free(chunk);

        storage_commit(me, STORAGE_COMMIT_FORCE, e);
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
    if (!me) return 0;
    if (offset <= NEXT_END) return 0;

    i32 deleted = 0;
    i64 curr = offset;
    u8 expect_mark = MARK_AS_DATA;
    while (curr > NEXT_END) {
        u8 status = STATUS_EMPTY;
        u8 mark = MARK_AS_UNUSED;
        i64 next = NEXT_END;
        if (storage_dio_block_header_get(me, curr, &status, &mark, &next) < 0) {
            THROW(e, "storage_dio_delete: header pread failed at offset=%lld", curr);
        }
        if (STATUS_SET != status) {
            // Already free or never allocated.
            break;
        }
        // Safety: refuse to follow a corrupted chain into another record.
        if (mark != expect_mark) {
            break;
        }
        if (next == curr) {
            THROW(e, "storage_dio_delete: corrupt next pointer (self-loop) at offset=%lld", curr);
        }

        struct buffer *p = BUFFER_POOL_BORROW(me->block_bytes);
        if (!p) THROW(e, "storage_dio_delete: OOM allocating block buffer");
        p->clear(p);
        p->i8_put(p, STATUS_EMPTY, NULL);
        p->i8_put(p, MARK_AS_UNUSED, NULL);
        p->i16_put(p, 0, NULL);
        p->i32_put(p, 0, NULL);
        p->i64_put(p, me->free, NULL);

#ifdef STORAGE_FILL_ZEROED_BLOCK_ON_DELETE
        // Fill remaining area with zero bytes.
        p->array_put(p, me->clean, p->remaining(p), NULL);
#endif

        p->flip(p);
        i64 abs = storage_dio_file_offset(me, curr);
        storage_dio_pwrite(me, p, abs);
        // ownership transferred

        me->free = curr;
        if (me->count > 0) me->count--;
        deleted++;

        // After the head, overflow blocks must be MARK_AS_NEXT.
        expect_mark = MARK_AS_NEXT;

        // Follow overflow chain.
        if (next > NEXT_END) {
            curr = next;
            continue;
        }
        break;
    }

    if (deleted > 0) {
        storage_dio_commit(me, STORAGE_COMMIT_LAZY, e);
        if (e && *e) THROW_S(e);
        return 1;
    }
    return 0;

EXCEPTION:
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

    int inserting = -1; // unknown until we read first block's status

#if defined(__linux__) && defined(O_DIRECT)
    struct storage_dio_priv *priv = (struct storage_dio_priv *)me->priv;
#endif

    while (1) {
        storage_dio_file_inflate(me, curr, e);
        if (e && *e) THROW_S(*e);

        // Read minimal metadata (status + next pointer) instead of preading the whole block.
        u8 status = STATUS_EMPTY;
        i64 next = NEXT_END;

#if defined(__linux__) && defined(O_DIRECT)
        // Fast path for Linux O_DIRECT: modify the cached aligned page in-place.
        // This avoids the expensive double pread (once for metadata and again for page RMW).
        if (priv && priv->o_direct_enabled) {
            const i64 abs = storage_dio_file_offset(me, curr);
            const u32 io = priv->direct_io_bytes;
            const i64 page_base = align_down_i64(abs, (i64)io);
            const u32 page_off = (u32)(abs - page_base);

            if ((unsigned long long)page_off + (unsigned long long)me->block_bytes <= (unsigned long long)io) {
                int cached = 0;
                struct buffer *page = storage_dio_odirect_page_get(me, priv, page_base, &cached);
                if (!page) THROW(e, "storage_dio_write_priv: O_DIRECT page pread failed at abs=%lld", abs);

                char *blk = page->array + page_off;
                status = (u8)blk[0];
                u8 mark0 = (u8)blk[1];
                memcpy(&next, blk + 8, 8);
                storage_dio_fixup_uninitialized_meta(curr, &status, mark0, &next);
                const int old_set = (STATUS_SET == status);
                if (inserting < 0) inserting = (old_set ? 0 : 1);

                int chunk = (remaining < BLOCK_DATA_BYTES ? remaining : BLOCK_DATA_BYTES);
                if (STATUS_SET != status) {
                    me->count++;
                    me->free = next; // relink free list
                }

                i64 next_index = NEXT_END;
                if (remaining > BLOCK_DATA_BYTES) {
                    next_index = (next > NEXT_END ? next : me->free);
                }

                // Write header
                blk[0] = STATUS_SET;
                blk[1] = curr_mark;
                i16 chunk16 = (i16)chunk;
                i32 rem32 = remaining;
                i64 next64 = (remaining > BLOCK_DATA_BYTES) ? next_index : NEXT_END;
                memcpy(blk + 2, &chunk16, 2);
                memcpy(blk + 4, &rem32, 4);
                memcpy(blk + 8, &next64, 8);

                // Write data + zero pad
                memcpy(blk + BLOCK_HEADER_BYTES, in->array_get(in, chunk, NULL), (size_t)chunk);
                int pad = BLOCK_DATA_BYTES - chunk;
                if (pad > 0) {
                    memset(blk + BLOCK_HEADER_BYTES + chunk, 0, (size_t)pad);
                }

                // If we couldn't cache the page, write-through immediately.
                if (!cached) {
                    ssize_t wn = pwrite_all(me->fd, page->array, page->capacity, page_base);
                    page->free(page);
                    if (wn < 0) THROW(e, "storage_dio_write_priv: O_DIRECT page pwrite failed at abs=%lld", page_base);
                } else {
                    u32 limit = priv->page_cache_limit ? priv->page_cache_limit : 8192u;
                    if (me->cache && (u32)me->cache->count_get(me->cache) >= limit) {
                        if (storage_dio_pflush(me) < 0) {
                            THROW(e, "storage_dio_write_priv: O_DIRECT pflush failed");
                        }
                    }
                }

                remaining -= chunk;
                next_last = next;
                if (remaining <= 0) {
                    if (old_set && next_last > NEXT_END && next_last != curr) {
                        storage_dio_delete(me, next_last, e);
                    }
                    storage_dio_commit(me, STORAGE_COMMIT_LAZY, e);
                    if (e && *e) THROW_S(e);
                    break;
                }

                curr = (remaining > BLOCK_DATA_BYTES) ? ((next > NEXT_END) ? next : me->free) : NEXT_END;
                if (curr == NEXT_END) {
                    // Should not happen because remaining > 0 implies we need a next block.
                    THROW(e, "storage_dio_write_priv: invalid next in O_DIRECT fast path");
                }
                curr_mark = MARK_AS_NEXT;
                continue;
            }
        }
#endif

        if (storage_dio_block_meta_get(me, curr, &status, &next) < 0) {
            THROW(e, "storage_dio_write_priv: header pread failed at offset=%lld", curr);
        }

        const int old_set = (STATUS_SET == status);
        if (inserting < 0) inserting = (old_set ? 0 : 1);

        int chunk = (remaining < BLOCK_DATA_BYTES ? remaining : BLOCK_DATA_BYTES);

        if (STATUS_SET != status) {
            me->count++;
            me->free = next; // relink free list
        }

        i64 next_index = NEXT_END;
        if (remaining > BLOCK_DATA_BYTES) {
            next_index = (next > NEXT_END ? next : me->free);
        }

        p = BUFFER_POOL_BORROW(me->block_bytes);
        if (!p) THROW(e, "storage_dio_write_priv: OOM allocating block buffer");
        p->clear(p);
        p->i8_put(p, STATUS_SET, NULL);
        p->i8_put(p, curr_mark, NULL);
        p->i16_put(p, (i16)chunk, NULL);
        p->i32_put(p, remaining, NULL);
        p->i64_put(p, (remaining > BLOCK_DATA_BYTES) ? next_index : NEXT_END, NULL);

        // Copy data chunk
        p->array_put(p, in->array_get(in, chunk, NULL), chunk, NULL);

        // Zero pad remaining of block (faster than copying a pre-zeroed buffer for small pads)
        int pad = BLOCK_DATA_BYTES - chunk;
        if (pad > 0) {
            if ((p->position + pad) > p->capacity) {
                THROW(e, "storage_dio_write_priv: pad overflow (pos=%d, pad=%d, cap=%d)", p->position, pad, p->capacity);
            }
            memset(p->array + p->position, 0, (size_t)pad);
            p->position += pad;
        }

        p->flip(p);
        i64 o = storage_dio_file_offset(me, curr);
        storage_dio_pwrite(me, p, o);
        // ownership transferred to the DIO write cache
        p = NULL;

        remaining -= chunk;
        next_last = next;
        if (remaining <= 0) {
            if (old_set && next_last > NEXT_END && next_last != curr) {
                storage_dio_delete(me, next_last, e);
            }
            storage_dio_commit(me, STORAGE_COMMIT_LAZY, e);
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
    
    if (storage_dio_pflush(me) < 0) {
        WARN("storage_dio_close: pflush failed: %d - %s", errno, strerror(errno));
    }
    storage_dio_commit(me, STORAGE_COMMIT_FORCE, NULL);

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

    // DIO block layout: [BLOCK_HEADER_BYTES + opts.block_bytes].
    // opts.block_bytes may be small/non-aligned (e.g. row_bytes), so we must compute
    // inflate chunk sizes (mmap_bytes) that are compatible with OS page alignment.
    me->block_bytes = (opts.compact <= 0) ? (BLOCK_HEADER_BYTES + opts.block_bytes) : (BLOCK_HEADER_BYTES + (opts.compact));
    me->clean = CALLOC(1, me->block_bytes);

    // Align increment to OS_PAGE_SIZE for predictable ftruncate/write patterns.
    i64 inc = (opts.increment <= 0) ? (i64)DEFAULT_INCREMENT_BYTES : (i64)opts.increment;
    if (inc < (i64)me->block_bytes) inc = (i64)me->block_bytes;
    inc = align_up_i64(inc, (i64)OS_PAGE_SIZE);
    me->increment = (i32)inc;

    // mmap_bytes must be divisible by both block_bytes and OS_PAGE_SIZE.
    me->mmap_bytes = (i32)storage_dio_chunk_bytes((i64)me->block_bytes, (i64)me->increment);

    memcpy(&me->opts, &opts, sizeof(struct storage_opts));


    char dir[PATH_MAX] = {0};
    getdir(me->opts.file, dir);
    mkdirs(dir, S_IRWXU);

    // Controls OS page cache behavior.
    // - macOS: uses F_NOCACHE when set to 0
    // - Linux: if opened with O_DIRECT, all data IO uses aligned page RMW; otherwise uses posix_fadvise(DONTNEED).
    int oscache = 1;
    const char *env_oscache2 = getenv("FLINTDB_DIO_OS_CACHE");
    if (env_oscache2 && (strcmp(env_oscache2, "0") == 0 || strcasecmp(env_oscache2, "false") == 0 || strcasecmp(env_oscache2, "off") == 0)) {
        oscache = 0;
    }

    // Decide Linux O_DIRECT at open-time (cannot be reliably enabled via fcntl).
    int open_flags = (opts.mode == FLINTDB_RDWR ? (O_RDWR | O_CREAT) : O_RDONLY);
#if defined(__linux__) && defined(O_DIRECT)
    const char *env_odirect = getenv("FLINTDB_DIO_O_DIRECT");
    int want_odirect = 0;
    if (env_odirect) {
        // Override behavior:
        // - FLINTDB_DIO_O_DIRECT=1 => force on
        // - FLINTDB_DIO_O_DIRECT=0 => force off
        // - FLINTDB_DIO_O_DIRECT=auto => follow oscache
        if (strcasecmp(env_odirect, "auto") == 0) {
            want_odirect = (oscache == 0);
        } else {
            want_odirect = env_truthy(env_odirect);
        }
    } else {
        // Backward compatible default: when OS cache is disabled, prefer O_DIRECT on Linux.
        want_odirect = (oscache == 0);
    }
    if (want_odirect) {
        open_flags |= O_DIRECT;
    }
#endif

    me->fd = open(me->opts.file, open_flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (me->fd < 0) {
        THROW(e, "Cannot open file %s: %s", me->opts.file, strerror(errno));
    }

    me->priv = CALLOC(1, sizeof(struct storage_dio_priv));
    if (!me->priv) THROW(e, "Cannot allocate DIO private data");
    struct storage_dio_priv *priv = (struct storage_dio_priv *)me->priv;

    // Default cache sizing (used by both O_DIRECT and non-O_DIRECT page caching).
    // Can be overridden on Linux O_DIRECT via FLINTDB_DIO_DIRECT_PAGE_CACHE.
    const char *env_pages = getenv("FLINTDB_DIO_PAGE_CACHE");
    if (env_pages && atoi(env_pages) > 0) {
        priv->page_cache_limit = (u32)atoi(env_pages);
    } else if (priv->page_cache_limit == 0) {
        priv->page_cache_limit = 8192u; // 8K pages
    }

    #ifdef __APPLE__
        if (oscache == 0) {
            fcntl(me->fd, F_NOCACHE, 1); // F_GLOBAL_NOCACHE
            priv->drop_os_cache = 1;
        }
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
        if (oscache == 0) {
            // If file was opened with O_DIRECT, enable aligned page IO.
#if defined(O_DIRECT)
            if (open_flags & O_DIRECT) {
                priv->o_direct_enabled = 1;
                priv->direct_align = 4096u;
                priv->direct_io_bytes = 4096u;
                const char *env_pages = getenv("FLINTDB_DIO_DIRECT_PAGE_CACHE");
                priv->page_cache_limit = (env_pages && atoi(env_pages) > 0) ? (u32)atoi(env_pages) : 8192u;
                DEBUG("DIO: Linux O_DIRECT enabled (align=%u, io=%u, page_cache_limit=%u)", priv->direct_align, priv->direct_io_bytes, priv->page_cache_limit);
            } else {
                priv->drop_os_cache = 1;
            }
#else
            priv->drop_os_cache = 1;
#endif
        }
        #if defined(POSIX_FADV_SEQUENTIAL)
            (void)posix_fadvise(me->fd, 0, 0, POSIX_FADV_SEQUENTIAL);
        #endif
    #endif

    struct stat st;
    fstat(me->fd, &st);
    priv->fresh_file = (st.st_size >= 0 && st.st_size < (i64)HEADER_BYTES);
    if (st.st_size >= 0 && st.st_size < (i64)HEADER_BYTES)
        _ftruncate(me->fd, (off_t)HEADER_BYTES);
    priv->inflated_size = (st.st_size >= (i64)HEADER_BYTES) ? (i64)st.st_size : (i64)HEADER_BYTES;

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
    // me->cache = hashmap_new(DIO_CACHE_SIZE, hashmap_int_hash, hashmap_int_cmpr); // batch write cache
    me->cache = treemap_new(hashmap_int_cmpr); // batch write cache
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
        storage_commit(me, STORAGE_COMMIT_FORCE, e);
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
    DEBUG("Initializing DIO buffer pool: block_bytes=%d", me->block_bytes);
    me->pool = buffer_pool_safe_create(STORAGE_DIO_USE_BUFFER_POOL, me->block_bytes, 0); // 256K blocks
    #endif

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
