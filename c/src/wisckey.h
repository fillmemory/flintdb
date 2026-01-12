#ifndef FLINTDB_WISCKEY_H
#define FLINTDB_WISCKEY_H

#include "lsm.h"
#include "types.h"
#include <limits.h>

struct wisckey {
    struct lsm_tree *lsm_index;
    int vlog_fd;
    i64 vlog_tail_offset;
    i64 vlog_valid_bytes; // Track valid (non-garbage) data size
    i64 gc_threshold;     // Trigger GC when tail > valid * threshold
    char path[PATH_MAX];
    enum flintdb_open_mode mode;
};

int wisckey_open(struct wisckey *me, const char *path, enum flintdb_open_mode mode, char **e);
void wisckey_close(struct wisckey *me);

int wisckey_put(struct wisckey *me, i64 key, struct buffer *val, char **e);
struct buffer *wisckey_get(struct wisckey *me, i64 key, char **e);
int wisckey_delete(struct wisckey *me, i64 key, char **e);

void wisckey_gc(struct wisckey *me, char **e);

#endif // FLINTDB_WISCKEY_H