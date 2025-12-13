#ifndef FLINTDB_WAL_H
#define FLINTDB_WAL_H

#include "flintdb.h"
#include "types.h"
#include "storage.h"

struct wal_impl;

struct wal {
    i64 (*begin)(struct wal *me, char **e);
    i64 (*commit)(struct wal *me, i64 id, char **e);
    i64 (*rollback)(struct wal *me, i64 id, char **e);
    i64 (*recover)(struct wal *me, char **e);
    i64 (*checkpoint)(struct wal *me, char **e);

    void (*close)(struct wal *me);

    struct wal_impl *impl;
};

extern struct wal WAL_NONE;

struct wal* wal_open(const char *path, const struct flintdb_meta *meta, char** e);
struct storage* wal_wrap(struct wal* wal, struct storage_opts* opts, int (*refresh)(const void *obj, i64 offset), const void *callback_obj, char** e);

enum wal_ops {
    OP_BEGIN = 0x00,      // Transaction start
    OP_WRITE = 0x01,      // Write page
    OP_DELETE = 0x02,     // Delete page
    OP_UPDATE = 0x03,     // Update page
    OP_COMMIT = 0x10,     // Transaction commit
    OP_ROLLBACK = 0x11,   // Transaction rollback
    OP_CHECKPOINT = 0x20, // Checkpoint marker
};

#endif // FLINTDB_WAL_H