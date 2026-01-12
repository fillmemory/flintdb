#ifndef FLINTDB_LSM_H
#define FLINTDB_LSM_H

#include "flintdb.h"
#include "types.h"

struct lsm_tree;
enum flintdb_open_mode;

/**
 * Open or create an LSM tree index.
 * @param path Base path for the LSM files.
 * @param mode Open mode (RDONLY/RDWR).
 * @param memtable_max_size Maximum size of MemTable in bytes before flushing.
 * @param e Error message pointer.
 * @return Pointer to lsm_tree struct.
 */
struct lsm_tree *lsm_open(const char *path, enum flintdb_open_mode mode, size_t memtable_max_size, char **e);

/**
 * Insert or update a key in the LSM tree.
 * @param me LSM tree handle.
 * @param key Primary key (e.g., hash of user key).
 * @param offset Offset in the vLog.
 * @param e Error message pointer.
 */
int lsm_put(struct lsm_tree *me, i64 key, i64 offset, char **e);

/**
 * Retrieve an offset for a given key.
 * @param me LSM tree handle.
 * @param key Primary key.
 * @param e Error message pointer.
 * @return The offset or -1 if not found.
 */
i64 lsm_get(struct lsm_tree *me, i64 key, char **e);

/**
 * Mark a key as deleted.
 * @param me LSM tree handle.
 * @param key Primary key.
 * @param e Error message pointer.
 */
int lsm_delete(struct lsm_tree *me, i64 key, char **e);

/**
 * Close and free the LSM tree.
 */
void lsm_close(struct lsm_tree *me);

#endif // FLINTDB_LSM_H
