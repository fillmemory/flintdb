#ifndef FLINTDB_WISCKEY_H
#define FLINTDB_WISCKEY_H

#include "bplustree.h"
#include "storage.h"
#include "types.h"
#include <limits.h>

struct wisckey {
  struct flintdb_table *index;
  struct flintdb_meta *index_meta;
  struct storage vlog; // Storage for values
  char path[PATH_MAX];
  enum flintdb_open_mode mode;
};

int wisckey_open(struct wisckey *me, const char *path,
                 enum flintdb_open_mode mode, char **e);
void wisckey_close(struct wisckey *me);

int wisckey_put(struct wisckey *me, i64 key, struct buffer *val, char **e);
struct buffer *wisckey_get(struct wisckey *me, i64 key, char **e);
int wisckey_delete(struct wisckey *me, i64 key, char **e);

void wisckey_gc(struct wisckey *me, char **e);

#endif // FLINTDB_WISCKEY_H