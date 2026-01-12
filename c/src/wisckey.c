#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "buffer.h"
#include "error_codes.h"
#include "flintdb.h"
#include "internal.h"
#include "runtime.h"
#include "wisckey.h"

#define WISCKEY_MAGIC 0x57495343

int wisckey_open(struct wisckey *me, const char *path, enum flintdb_open_mode mode, char **e) {
    memset(me, 0, sizeof(struct wisckey));
    strncpy_safe(me->path, path, sizeof(me->path));
    me->mode = mode;

    char index_path[PATH_MAX];
    char vlog_path[PATH_MAX];
    snprintf(index_path, sizeof(index_path), "%s.index", path);
    snprintf(vlog_path, sizeof(vlog_path), "%s.vlog", path);

    // 1. Initialize vLog storage (Raw File)
    int flags = O_RDWR | O_CREAT;
    if (mode == FLINTDB_RDONLY) {
        flags = O_RDONLY;
    }

    me->vlog_fd = open(vlog_path, flags, 0644);
    if (me->vlog_fd < 0) {
        THROW(e, "Failed to open vLog: %s", vlog_path);
        return -1;
    }
    me->vlog_tail_offset = lseek(me->vlog_fd, 0, SEEK_END);
    me->vlog_valid_bytes = me->vlog_tail_offset; // Initially all data is valid
    me->gc_threshold = 3;                        // Trigger GC when vlog > 3x valid data

    // 2. Initialize LSM Index
    me->lsm_index = lsm_open(index_path, mode, 8 * 1024 * 1024, e);
    if (!me->lsm_index)
        goto EXCEPTION;

    return 0;

EXCEPTION:
    if (me->vlog_fd >= 0) {
        close(me->vlog_fd);
    }
    return -1;
}

void wisckey_close(struct wisckey *me) {
    if (!me)
        return;
    if (me->lsm_index) {
        lsm_close(me->lsm_index);
        me->lsm_index = NULL;
    }
    if (me->vlog_fd >= 0) {
        close(me->vlog_fd);
        me->vlog_fd = -1;
    }
}

int wisckey_put(struct wisckey *me, i64 key, struct buffer *val, char **e) {
    // 1. Find existing offset
    i64 existing_offset = lsm_get(me->lsm_index, key, e);
    if (existing_offset == -1)
        existing_offset = NOT_FOUND;

    // 2. Write new record to vLog
    u32 vlen = (val == NULL) ? 0 : val->remaining(val);
    u32 klen = 8;
    u32 record_len = 4 + 4 + klen + 4 + vlen + 4;

    struct buffer *bb = buffer_alloc(record_len);
    if (!bb)
        return -1;

    bb->i32_put(bb, (i32)WISCKEY_MAGIC, e);
    bb->i32_put(bb, (i32)klen, e);
    bb->i64_put(bb, key, e);
    bb->i32_put(bb, (i32)vlen, e);
    if (vlen > 0) {
        bb->array_put(bb, val->array_get(val, vlen, e), vlen, e);
    }
    bb->i32_put(bb, 0, e); // TODO: checksum
    bb->flip(bb);

    i64 offset = me->vlog_tail_offset;
    ssize_t nw = write(me->vlog_fd, bb->array, bb->limit);
    if (nw != (ssize_t)bb->limit) {
        bb->free(bb);
        return -1;
    }
    me->vlog_tail_offset += nw;
    bb->free(bb);

    // 3. Update index (Upsert)
    lsm_put(me->lsm_index, key, offset, e);

    // 4. Update valid bytes tracking
    if (existing_offset != NOT_FOUND) {
        // This is an update - old entry becomes garbage
        // We don't subtract here; GC will reclaim space
    } else {
        // New insert - add to valid bytes
        me->vlog_valid_bytes += nw;
    }

    // 5. Check if GC is needed
    if (me->vlog_tail_offset > me->vlog_valid_bytes * me->gc_threshold) {
        wisckey_gc(me, e);
    }

    return 0;
}

struct buffer *wisckey_get(struct wisckey *me, i64 key, char **e) {
    i64 offset = lsm_get(me->lsm_index, key, e);
    if (offset == -1)
        return NULL;
    else if (offset == -2) // tombstone
        return NULL;

    // 1. Read Header (magic, klen, key, vlen)
    // format: magic(4), klen(4), key(8), vlen(4) -> 20 bytes
    char header[20];
    if (pread(me->vlog_fd, header, 20, offset) != 20)
        return NULL;

    struct buffer hb;
    buffer_wrap(header, 20, &hb);
    u32 magic = (u32)hb.i32_get(&hb, e);
    if (magic != WISCKEY_MAGIC)
        return NULL;

    u32 klen = (u32)hb.i32_get(&hb, e);
    hb.i64_get(&hb, e); // skip key
    u32 vlen = (u32)hb.i32_get(&hb, e);

    // 2. Read Value
    struct buffer *val_buf = buffer_alloc(vlen);
    if (vlen > 0) {
        if (pread(me->vlog_fd, val_buf->array, vlen, offset + 20) != (ssize_t)vlen) {
            val_buf->free(val_buf);
            return NULL;
        }
        val_buf->limit = vlen;
        val_buf->position = 0;
    }

    return val_buf;
}

int wisckey_delete(struct wisckey *me, i64 key, char **e) {
    i64 offset = lsm_get(me->lsm_index, key, e);
    if (offset == -1 || offset == -2)
        return NOT_FOUND;

    // Read the record size to update valid_bytes
    char header[20];
    if (pread(me->vlog_fd, header, 20, offset) == 20) {
        struct buffer hb;
        buffer_wrap(header, 20, &hb);
        hb.i32_get(&hb, e); // magic
        hb.i32_get(&hb, e); // klen
        hb.i64_get(&hb, e); // key
        u32 vlen = (u32)hb.i32_get(&hb, e);
        // Subtract deleted entry from valid bytes
        me->vlog_valid_bytes -= (20 + vlen + 4); // header + value + checksum
    }

    lsm_delete(me->lsm_index, key, e);
    return 0;
}

void wisckey_gc(struct wisckey *me, char **e) {
    char new_vlog_path[PATH_MAX];
    snprintf(new_vlog_path, sizeof(new_vlog_path), "%s.vlog.new", me->path);

    // 1. Create new vLog file
    int new_fd = open(new_vlog_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (new_fd < 0)
        return;

    i64 new_tail = 0;
    i64 old_offset = 0;

    // 2. Scan old vLog and copy valid entries
    while (old_offset < me->vlog_tail_offset) {
        char header[20];
        if (pread(me->vlog_fd, header, 20, old_offset) != 20)
            break;

        struct buffer hb;
        buffer_wrap(header, 20, &hb);
        u32 magic = (u32)hb.i32_get(&hb, e);
        if (magic != WISCKEY_MAGIC)
            break;

        hb.i32_get(&hb, e); // klen
        i64 key = hb.i64_get(&hb, e);
        u32 vlen = (u32)hb.i32_get(&hb, e);
        u32 record_len = 20 + vlen + 4;

        // 3. Check if this record is still valid
        i64 current_offset = lsm_get(me->lsm_index, key, e);
        if (current_offset == old_offset) {
            // Valid entry - copy to new vLog
            char *record = malloc(record_len);
            if (record && pread(me->vlog_fd, record, record_len, old_offset) == (ssize_t)record_len) {
                if (write(new_fd, record, record_len) == (ssize_t)record_len) {
                    // Update LSM index with new offset
                    lsm_put(me->lsm_index, key, new_tail, e);
                    new_tail += record_len;
                }
            }
            free(record);
        }

        old_offset += record_len;
    }

    // 4. Swap files
    close(me->vlog_fd);
    close(new_fd);

    char old_vlog_path[PATH_MAX];
    snprintf(old_vlog_path, sizeof(old_vlog_path), "%s.vlog", me->path);
    unlink(old_vlog_path);
    rename(new_vlog_path, old_vlog_path);

    // 5. Reopen and update state
    me->vlog_fd = open(old_vlog_path, O_RDWR, 0644);
    me->vlog_tail_offset = new_tail;
    me->vlog_valid_bytes = new_tail; // All data in new log is valid
}
