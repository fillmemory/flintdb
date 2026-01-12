#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer.h"
#include "error_codes.h"
#include "flintdb.h"
#include "internal.h"
#include "runtime.h"
#include "wisckey.h"

#define WISCKEY_MAGIC 0x57495343

int wisckey_open(struct wisckey *me, const char *path,
                 enum flintdb_open_mode mode, char **e) {
  memset(me, 0, sizeof(struct wisckey));
  s_copy(me->path, sizeof(me->path), path);
  me->mode = mode;

  char index_path[PATH_MAX];
  char vlog_path[PATH_MAX];
  snprintf(index_path, sizeof(index_path), "%s.index", path);
  snprintf(vlog_path, sizeof(vlog_path), "%s.vlog", path);

  // 1. Initialize vLog storage
  struct storage_opts vopts = {0};
  s_copy(vopts.file, sizeof(vopts.file), vlog_path);
  vopts.mode = mode;
  vopts.block_bytes = 4096 - 16;
  vopts.increment = 1024 * 1024 * 16;
  s_copy(vopts.type, sizeof(vopts.type), TYPE_MMAP);

  int rc = storage_open(&me->vlog, vopts, e);
  if (rc != 0)
    return rc;

  // 2. Initialize Index Table (UserKey -> vLogOffset)
  me->index_meta = flintdb_meta_new_ptr("wk_index", e);
  if (!me->index_meta)
    goto EXCEPTION;

  // Use compact mode for index blocks
  me->index_meta->compact = 4096;
  flintdb_meta_columns_add(me->index_meta, "id", VARIANT_INT64, 8, 0,
                           SPEC_NOT_NULL, NULL, NULL, e);
  flintdb_meta_columns_add(me->index_meta, "offset", VARIANT_INT64, 8, 0,
                           SPEC_NOT_NULL, NULL, NULL, e);
  char keys[1][MAX_COLUMN_NAME_LIMIT] = {"id"};
  flintdb_meta_indexes_add(me->index_meta, PRIMARY_NAME, "bptree", keys, 1, e);

  me->index = flintdb_table_open(index_path, mode, me->index_meta, e);
  if (!me->index)
    goto EXCEPTION;

  return 0;

EXCEPTION:
  if (me->index_meta) {
    flintdb_meta_free_ptr(me->index_meta);
    me->index_meta = NULL;
  }
  if (me->vlog.close) {
    me->vlog.close(&me->vlog);
  }
  return -1;
}

void wisckey_close(struct wisckey *me) {
  if (!me)
    return;
  if (me->index) {
    me->index->close(me->index);
    me->index = NULL;
  }
  if (me->index_meta) {
    flintdb_meta_free_ptr(me->index_meta);
    me->index_meta = NULL;
  }
  if (me->vlog.close)
    me->vlog.close(&me->vlog);
}

int wisckey_put(struct wisckey *me, i64 key, struct buffer *val, char **e) {
  // 1. Find existing offset
  i64 existing_offset = NOT_FOUND;
  char key_str[32];
  sprintf(key_str, "%lld", key);
  const char *pk_args[] = {"id", key_str};
  const struct flintdb_row *existing_row =
      me->index->one(me->index, PRIMARY_INDEX, 2, pk_args, e);
  if (existing_row) {
    existing_offset = existing_row->i64_get(existing_row, 1, e);
  }

  // 2. Write new record to vLog
  u32 vlen = (val == NULL) ? 0 : val->remaining(val);
  u32 klen = 8;
  u32 record_len = 4 + 4 + klen + 4 + vlen + 4;

  struct buffer *bb = buffer_alloc(record_len);
  if (!bb)
    THROW(e, "Out of memory");

  bb->i32_put(bb, (i32)WISCKEY_MAGIC, e);
  bb->i32_put(bb, (i32)klen, e);
  bb->i64_put(bb, key, e);
  bb->i32_put(bb, (i32)vlen, e);
  if (vlen > 0) {
    bb->array_put(bb, val->array_get(val, vlen, e), vlen, e);
  }
  bb->i32_put(bb, 0, e); // TODO: checksum
  bb->flip(bb);

  i64 offset = me->vlog.write(&me->vlog, bb, e);
  bb->free(bb);
  if (offset == NOT_FOUND)
    return -1;

  // 3. Update index (Upsert)
  struct flintdb_row *new_row = flintdb_row_pool_acquire(me->index_meta, e);
  if (!new_row)
    return -1;
  new_row->i64_set(new_row, 0, key, e);
  new_row->i64_set(new_row, 1, offset, e);
  me->index->apply(me->index, new_row, 1, e); // upsert=1
  flintdb_row_pool_release(new_row);

  // 4. Cleanup old vLog entry
  if (existing_offset != NOT_FOUND) {
    me->vlog.delete(&me->vlog, existing_offset, e);
  }

  return 0;

EXCEPTION:
  if (bb)
    bb->free(bb);
  return -1;
}

struct buffer *wisckey_get(struct wisckey *me, i64 key, char **e) {
  char key_str[32];
  sprintf(key_str, "%lld", key);
  const char *pk_args[] = {"id", key_str};
  const struct flintdb_row *r =
      me->index->one(me->index, PRIMARY_INDEX, 2, pk_args, e);
  if (!r)
    return NULL;

  i64 offset = r->i64_get(r, 1, e);
  struct buffer *mbb = me->vlog.read(&me->vlog, offset, e);
  if (!mbb)
    return NULL;

  mbb->position = 0;
  u32 magic = (u32)mbb->i32_get(mbb, e);
  if (magic != WISCKEY_MAGIC) {
    mbb->free(mbb);
    THROW(e, "Invalid WiscKey magic at offset %lld", offset);
    return NULL;
  }

  u32 klen = (u32)mbb->i32_get(mbb, e);
  mbb->i64_get(mbb, e); // skip key
  u32 vlen = (u32)mbb->i32_get(mbb, e);

  struct buffer *val_buf = buffer_alloc(vlen);
  if (vlen > 0) {
    val_buf->array_put(val_buf, mbb->array_get(mbb, vlen, e), vlen, e);
  }
  val_buf->flip(val_buf);

  mbb->free(mbb);
  return val_buf;

EXCEPTION:
  if (mbb)
    mbb->free(mbb);
  return NULL;
}

int wisckey_delete(struct wisckey *me, i64 key, char **e) {
  char key_str[32];
  sprintf(key_str, "%lld", key);
  const char *pk_args[] = {"id", key_str};
  const struct flintdb_row *r =
      me->index->one(me->index, PRIMARY_INDEX, 2, pk_args, e);
  if (!r)
    return NOT_FOUND;

  i64 offset = r->i64_get(r, 1, e);
  i64 rowid = r->rowid;

  // Delete from index
  int rc = (int)me->index->delete_at(me->index, rowid, e);

  // Delete from vLog
  me->vlog.delete(&me->vlog, offset, e);

  return rc;
}

void wisckey_gc(struct wisckey *me, char **e) {
  // Free-list based GC handled by storage.c
}
