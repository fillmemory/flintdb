#include "flintdb.h"
#include "internal.h"
#include "runtime.h"
#include "bplustree.h"
#include "storage.h"
#include "buffer.h"
#include "hashmap.h"
#include "sql.h"
#include "filter.h"
#include "list.h"
#include "wal.h"
#include "error_codes.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/fcntl.h>
#include <assert.h>
#include <ctype.h>
#include <pthread.h>
#include <stdatomic.h>

// Use C11 stdatomic for cross-platform spinlock
// Works on macOS, Linux, and Windows MinGW
#define TABLE_LOCK_T atomic_int
#define TABLE_LOCK_INIT(lock) atomic_store_explicit(lock, 0, memory_order_relaxed)
#define TABLE_LOCK(lock) do { int expected = 0; while (!atomic_compare_exchange_weak_explicit(lock, &expected, 1, memory_order_acquire, memory_order_relaxed)) { expected = 0; } } while(0)
#define TABLE_UNLOCK(lock) atomic_store_explicit(lock, 0, memory_order_release)
#define TABLE_LOCK_DESTROY(lock) ((void)0)


#define SIGNATURE "ITBL" // TODO: => "LOTS" (LOcal Table Storage)
#define HEAD_SZ 8 // signature(4B) + version(4B)
#define I16_BYTES 2

#define DEFAULT_STORAGE_INCREMENT (1024 * 1024 * 16)
#ifndef DEFAULT_TABLE_CACHE_LIMIT
#define DEFAULT_TABLE_CACHE_LIMIT (1024 * 1024 * 1)
#endif
#define DEFAULT_TABLE_CACHE_MIN (1024 * 256) // Do not allow too small capacity (구조적 제약)


static int row_compare_get(void *o, const void *a, i64 b);
// forward decl
static int meta_index_ordinal(const struct flintdb_meta *m, const char *name);

static const struct flintdb_row * table_read(struct flintdb_table *me, i64 rowid, char **e);
static const struct flintdb_row * table_read_unlocked(struct flintdb_table *me, i64 rowid, char **e);

// sorter compare functions (used by index initialization and rollback refresh)
static int sorter_primary_cmpr(void *o, i64 a, i64 b);
static int sorter_index_cmpr(void *o, i64 a, i64 b);

struct sorter {
    char name[MAX_COLUMN_NAME_LIMIT];
    char algorithm[32]; // "bptree"
    struct bplustree tree;
    struct flintdb_table *table; // parent table

    struct {
        int length;
        int a[MAX_INDEX_KEYS_LIMIT];
    } keys;
};

struct flintdb_table_priv {
    char file[PATH_MAX];
    enum flintdb_open_mode mode;
    struct flintdb_meta meta;
    int row_bytes; // fixed row bytes from meta

    struct {
        int length;
        struct sorter s[MAX_INDEX_KEYS_LIMIT];
    } sorters;
    
    struct wal *wal; // write-ahead log
    struct storage *storage; 
    struct buffer *header;
    struct formatter formatter;
    struct hashmap *cache; // rowid -> row*
    // Reusable raw row buffer pool (new generic buffer_pool)
    struct buffer_pool *raw_pool;
    TABLE_LOCK_T lock; // table-level lock (os_unfair_lock on macOS, spinlock on Linux)
};

struct flintdb_transaction_priv {
    struct flintdb_table *table;
    struct flintdb_table_priv *tpriv;
    i64 id;
    i8 done;

    // Snapshot of index counts at begin; used to restore after rollback.
    // (B+Tree count is stored in its header mapping, not WAL-managed blocks.)
    int count_len;
    i64 counts[MAX_INDEX_KEYS_LIMIT];
};

static i64 tx_id(const struct flintdb_transaction *me) {
    if (!me || !me->priv) return -1;
    const struct flintdb_transaction_priv *p = (const struct flintdb_transaction_priv*)me->priv;
    return p->id;
}

// Forward decls for in-transaction table ops (assumes table lock is held and WAL tx started)
static i64 table_apply_in_tx(struct flintdb_table *me, struct flintdb_row *r, i8 upsert, char **e);
static i64 table_apply_at_in_tx(struct flintdb_table *me, i64 rowid, struct flintdb_row *r, char **e);
static i64 table_delete_in_tx(struct flintdb_table *me, i64 rowid, char **e);

static i64 tx_apply(struct flintdb_transaction *me, struct flintdb_row *r, i8 upsert, char **e) {
    if (!me || !me->priv) {
        if (e) *e = "transaction is null";
        return NOT_FOUND;
    }
    struct flintdb_transaction_priv *p = (struct flintdb_transaction_priv*)me->priv;
    if (p->done) {
        if (e) *e = "transaction already finished";
        return NOT_FOUND;
    }
    return table_apply_in_tx(p->table, r, upsert, e);
}

static i64 tx_apply_at(struct flintdb_transaction *me, i64 rowid, struct flintdb_row *r, char **e) {
    if (!me || !me->priv) {
        if (e) *e = "transaction is null";
        return NOT_FOUND;
    }
    struct flintdb_transaction_priv *p = (struct flintdb_transaction_priv*)me->priv;
    if (p->done) {
        if (e) *e = "transaction already finished";
        return NOT_FOUND;
    }
    return table_apply_at_in_tx(p->table, rowid, r, e);
}

static i64 tx_delete_at(struct flintdb_transaction *me, i64 rowid, char **e) {
    if (!me || !me->priv) {
        if (e) *e = "transaction is null";
        return NOT_FOUND;
    }
    struct flintdb_transaction_priv *p = (struct flintdb_transaction_priv*)me->priv;
    if (p->done) {
        if (e) *e = "transaction already finished";
        return NOT_FOUND;
    }
    return table_delete_in_tx(p->table, rowid, e);
}

static void tx_commit(struct flintdb_transaction *me, char **e) {
    if (!me || !me->priv) {
        if (e) *e = "transaction is null";
        return;
    }
    struct flintdb_transaction_priv *p = (struct flintdb_transaction_priv*)me->priv;
    if (p->done) return;

    // Flush index metadata (root+count) into WAL-managed storages before commit.
    for (int i = 0; i < p->tpriv->sorters.length; i++) {
        struct sorter *s = &p->tpriv->sorters.s[i];
        if (s->tree.flush_meta) {
            s->tree.flush_meta(&s->tree, e);
            if (e && *e) break;
        }
    }
    if (e && *e) {
        // Best-effort rollback; do not deadlock the table.
        if (p->id > 0) p->tpriv->wal->rollback(p->tpriv->wal, p->id, NULL);
        p->done = 1;
        TABLE_UNLOCK(&p->tpriv->lock);
        return;
    }

    // commit WAL then unlock (match Java TransactionImpl)
    p->tpriv->wal->commit(p->tpriv->wal, p->id, e);
    if (e && *e) {
        // Best-effort rollback; do not deadlock the table.
        if (p->id > 0) p->tpriv->wal->rollback(p->tpriv->wal, p->id, NULL);
        p->done = 1;
        TABLE_UNLOCK(&p->tpriv->lock);
        return;
    }
    p->done = 1;
    TABLE_UNLOCK(&p->tpriv->lock);
}

static void table_refresh_after_rollback(struct flintdb_table *table) {
    if (!table || !table->priv) return;
    struct flintdb_table_priv *priv = (struct flintdb_table_priv*)table->priv;

    // Drop row cache (may contain uncommitted rows)
    if (priv->cache) priv->cache->clear(priv->cache);

    // Re-open all indexes to reset in-memory B+Tree state (count/root/cache).
    // This is important because WAL rollback discards staged pages, but the in-memory
    // B+Tree structs may still reflect uncommitted inserts/deletes.
    i32 cache_limit = priv->meta.cache;
    if (cache_limit <= DEFAULT_TABLE_CACHE_LIMIT) cache_limit = DEFAULT_TABLE_CACHE_LIMIT;
    if (priv->mode == FLINTDB_RDONLY) cache_limit = cache_limit / 2;
    if (cache_limit < DEFAULT_TABLE_CACHE_MIN) cache_limit = DEFAULT_TABLE_CACHE_MIN;

    for (int i = 0; i < priv->sorters.length; i++) {
        struct sorter *s = &priv->sorters.s[i];

        // Close without flushing root pointer.
        int saved_mode = s->tree.mode;
        s->tree.mode = FLINTDB_RDONLY;
        s->tree.close(&s->tree);
        s->tree.mode = saved_mode;

        char ixf[PATH_MAX] = {0};
        snprintf(ixf, sizeof(ixf), "%s.i.%s", priv->file, s->name);

        char *e = NULL;
        if (i == 0) {
            bplustree_init(&s->tree, ixf, cache_limit * 1, priv->mode, TYPE_DEFAULT, s, &sorter_primary_cmpr, priv->wal, &e);
        } else {
            bplustree_init(&s->tree, ixf, cache_limit * 1, priv->mode, TYPE_DEFAULT, s, &sorter_index_cmpr, priv->wal, &e);
        }
        if (e) {
            WARN("table_refresh_after_rollback: index reopen failed: %s", e);
            // best-effort: keep going
        }
    }
}

static void tx_rollback(struct flintdb_transaction *me, char **e) {
    (void)e; // rollback is best-effort; errors are ignored (same as existing table_* exception cleanup)
    if (!me || !me->priv) return;
    struct flintdb_transaction_priv *p = (struct flintdb_transaction_priv*)me->priv;
    if (p->done) return;
    if (p->id > 0) {
        p->tpriv->wal->rollback(p->tpriv->wal, p->id, NULL);
    }

    // Restore in-memory counts (best-effort). Persisted counts come from WAL-managed meta.
    for (int i = 0; i < p->count_len; i++) {
        struct sorter *s = &p->tpriv->sorters.s[i];
        s->tree.count = p->counts[i];
        s->tree.meta_dirty = 0;
    }

    // Reset in-memory state to match rolled-back storage.
    table_refresh_after_rollback(p->table);
    p->done = 1;
    TABLE_UNLOCK(&p->tpriv->lock);
}

static i8 tx_validate(struct flintdb_transaction *me, struct flintdb_table *t, char **e) {
    if (!me || !me->priv) {
        if (e) *e = "transaction is null";
        return 0;
    }
    struct flintdb_transaction_priv *p = (struct flintdb_transaction_priv*)me->priv;
    if (p->done) {
        if (e) *e = "transaction already finished";
        return 0;
    }
    if (p->table != t) {
        if (e) *e = "transaction does not belong to the specified table";
        return 0;
    }
    return 1;
}

static void tx_close(struct flintdb_transaction *me) {
    if (!me) return;
    if (me->priv) {
        struct flintdb_transaction_priv *p = (struct flintdb_transaction_priv*)me->priv;
        if (!p->done) {
            // rollback() unlocks; do not unlock twice
            tx_rollback(me, NULL);
        }
        FREE(me->priv);
        me->priv = NULL;
    }
    FREE(me);
}

struct find_context {
    struct flintdb_table *table;
    struct limit limit;
    struct filter_layers *filters;
    enum order order;
    i8 index;
    struct flintdb_cursor_i64 *base_cursor; // B+Tree cursor
};


static i64 table_rows(const struct flintdb_table *me, char **e) {
    if (!me || !me->priv)
        return -1;
        
    struct flintdb_table_priv *priv = (struct flintdb_table_priv*)me->priv;
    assert(priv);
    assert(priv->sorters.length > 0);
    struct sorter *s = &priv->sorters.s[0];
    assert(s->tree.count_get);
    return s->tree.count_get(&s->tree);
}

static i64 table_bytes(const struct flintdb_table *me, char **e) {
    if (!me || !me->priv)
        return -1;
        
    struct flintdb_table_priv *priv = (struct flintdb_table_priv*)me->priv;
    i64 total = priv->storage->bytes_get(priv->storage);
    if (total < 0) return -1;
    for(int i=0; i<priv->sorters.length; i++) {
        struct sorter *s = &priv->sorters.s[i];
        if (s->tree.bytes_get)
            total += s->tree.bytes_get(&s->tree);
    }
    return total;
}

static const struct flintdb_meta * table_meta(const struct flintdb_table *me, char **e) {
    if (!me || !me->priv)
        return NULL;

    struct flintdb_table_priv *priv = (struct flintdb_table_priv*)me->priv;
    return &priv->meta;
}

static void row_cache_dealloc(keytype k, valtype v) {
    // i64 rowid = k; // for debug
    struct flintdb_row *r = (struct flintdb_row *)v;
    r->free(r);
}

// Borrow/return helpers for raw row buffers via buffer_pool. Not thread-safe by design.
static inline struct buffer * table_borrow_raw_buffer(struct flintdb_table_priv *priv) {
    if (priv && priv->raw_pool) {
        struct buffer *b = priv->raw_pool->borrow(priv->raw_pool, (u32)priv->row_bytes);
        if (b) return b;
    }
    return buffer_alloc((u32)priv->row_bytes);
}

static inline void table_return_raw_buffer(struct flintdb_table_priv *priv, struct buffer *b) {
    if (!b) return;
    if (priv && priv->raw_pool) {
        priv->raw_pool->return_buffer(priv->raw_pool, b);
    } else {
        b->free(b);
    }
}

static i64 table_apply_in_tx(struct flintdb_table *me, struct flintdb_row *r, i8 upsert, char **e) {
    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)me->priv;
    assert(r);
    assert(priv);
    struct flintdb_meta *m = &priv->meta;
    struct formatter *fmt = &priv->formatter;
    struct storage *storage = priv->storage;
    assert(fmt);
    assert(m);
    assert(storage);

    struct buffer *raw = NULL;
    raw = table_borrow_raw_buffer(priv);
    if (!raw) THROW(e, "Out of memory");
    if (m->columns.length != r->meta->columns.length) 
        THROW(e, "DB_ERR[%d] column count mismatch: %d != %d", DB_ERR_COLUMN_MISMATCH, m->columns.length, r->meta->columns.length);
    int enc = fmt->encode(fmt, r, raw, e);
    // DEBUG("row bytes: %d\n", raw->remaining(raw));
    if (enc != 0) THROW(e, "failed to encode row");

    assert(raw->remaining(raw) > 0);
    assert(raw->remaining(raw) <= priv->row_bytes);
    if (raw->remaining(raw) > priv->row_bytes) 
        THROW(e, "DB_ERR[%d] row bytes exceeded requested: %d, max: %d", DB_ERR_ROW_BYTES_EXCEEDED, raw->remaining(raw), priv->row_bytes);
    struct sorter *primary = &priv->sorters.s[0];
    // DEBUG("before compare_get, row.id=%lld, primary=%p, tree=%p, r=%p", r->rowid, (void*)primary, (void*)&primary->tree, (void*)r);
    i64 rowid = r->rowid > NOT_FOUND 
                ? r->rowid 
                : primary->tree.compare_get(&primary->tree, primary, r, row_compare_get, e);
    // DEBUG("after compare_get, rowid=%lld, e=%s", rowid, e?*e?*e:"NULL":"NULL");
    if (e && *e) THROW(e, "failed to lookup row"); 

    if (NOT_FOUND == rowid) {
        rowid = storage->write(storage, raw, e);
        // DEBUG("table_apply storage->write => %lld", rowid);
        assert(rowid != NOT_FOUND);
        if (e && *e) THROW_S(e);

        r->rowid = rowid;
        // DEBUG("primary->tree.put begin key=%lld", rowid);
        primary->tree.put(&primary->tree, rowid, e);
        // DEBUG("primary->tree.put end key=%lld", rowid);
        if (e && *e) THROW_S(e);

        for(int i=1; i<priv->sorters.length; i++) {
            struct sorter *sorter = &priv->sorters.s[i];
            // DEBUG("secondary sorter[%d] put begin", i);
            sorter->tree.put(&sorter->tree, rowid, e);
            // DEBUG("secondary sorter[%d] put end", i);
            if (e && *e) THROW_S(e); 
        }

        // put cache
        // me->read(me, rowid, e);
    } else {
        if (!upsert) THROW(e, "DB_ERR[%d] duplicate key on rowid: %lld", DB_ERR_DUPLICATE_KEY, rowid);
        
        r->rowid = rowid;
        for(int i=1; i<priv->sorters.length; i++) {
            struct sorter *sorter = &priv->sorters.s[i];
            // DEBUG("update secondary sorter[%d] delete begin", i);
            if (sorter->tree.delete(&sorter->tree, rowid, e) == NOT_FOUND)
                THROW(e, "index[%s] key : %lld not found", sorter->name, rowid);
            // DEBUG("update secondary sorter[%d] delete end", i);
        }
        // In update path, do NOT cache the caller-provided row pointer 'r'.
        // Caching 'r' here transfers ownership to the cache and causes use-after-free
        // when the caller frees 'r' after apply(). Instead, clear stale cache entry
        // and repopulate from storage after write_at so cache owns its own copy.
        priv->cache->remove(priv->cache, rowid);
        // DEBUG("update storage->write_at begin");
        storage->write_at(storage, rowid, raw, e);
        // DEBUG("update storage->write_at end");
        if (e && *e) THROW_S(e);

        // Repopulate cache with freshly written row (owned by cache)
        // This ensures subsequent reads and index comparators see updated values
        // without retaining the caller's 'r' pointer.
        // (void)me->read(me, rowid, NULL);
        // // priv->cache->put(priv->cache, (keytype)rowid, (valtype)r, row_cache_dealloc);

        for(int i=1; i<priv->sorters.length; i++) {
            struct sorter *sorter = &priv->sorters.s[i];
            // DEBUG("update secondary sorter[%d] put begin", i);
            sorter->tree.put(&sorter->tree, rowid, e);
            // DEBUG("update secondary sorter[%d] put end", i);
            if (e && *e) THROW_S(e);
        }
    }

    // raw buffer no longer needed after write
    if (raw) table_return_raw_buffer(priv, raw);
    return rowid;

    EXCEPTION:
    if (raw) table_return_raw_buffer(priv, raw);
    return NOT_FOUND;
}

static i64 table_apply(struct flintdb_table *me, struct flintdb_row *r, i8 upsert, char **e) {
    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)me->priv;
    assert(priv);
    i64 transaction = 0;
    TABLE_LOCK(&priv->lock);
    // Begin WAL transaction (will set transaction on all storages)
    transaction = priv->wal->begin(priv->wal, e);
    if (e && *e) goto EXCEPTION;

    i64 rowid = table_apply_in_tx(me, r, upsert, e);
    if (e && *e) goto EXCEPTION;
    if (rowid == NOT_FOUND) goto EXCEPTION;

    for (int i = 0; i < priv->sorters.length; i++) {
        struct sorter *s = &priv->sorters.s[i];
        if (s->tree.flush_meta) {
            s->tree.flush_meta(&s->tree, e);
            if (e && *e) goto EXCEPTION;
        }
    }

    priv->wal->commit(priv->wal, transaction, e);
    if (e && *e) goto EXCEPTION;

    TABLE_UNLOCK(&priv->lock);
    return rowid;

    EXCEPTION:
    if (transaction > 0) priv->wal->rollback(priv->wal, transaction, NULL);
    TABLE_UNLOCK(&priv->lock);
    return NOT_FOUND;
}

static i64 table_apply_at_in_tx(struct flintdb_table *me, i64 rowid, struct flintdb_row *r, char **e) {
    // Ensure 'raw' is always defined before any THROW can jump to EXCEPTION
    struct buffer *raw = NULL;
    
    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)me->priv;
    if (rowid <= NOT_FOUND) THROW(e, "bad rowid: %lld", rowid);
    assert(r);
    assert(priv);
    struct flintdb_meta *m = &priv->meta;
    struct formatter *fmt = &priv->formatter;
    struct storage *storage = priv->storage;
    assert(fmt);
    assert(m);
    assert(storage);

    raw = table_borrow_raw_buffer(priv);
    if (!raw) THROW(e, "Out of memory");
    if (m->columns.length != r->meta->columns.length)
        THROW(e, "DB_ERR[%d] column count mismatch: %d != %d", DB_ERR_COLUMN_MISMATCH, m->columns.length, r->meta->columns.length);


    if (fmt->encode(fmt, r, raw, e) != 0) THROW(e, "failed to encode row");

    assert(raw->remaining(raw) > 0);
    assert(raw->remaining(raw) <= priv->row_bytes);
    if (raw->remaining(raw) > priv->row_bytes) 
        THROW(e, "DB_ERR[%d] row bytes exceeded requested: %d, max: %d", DB_ERR_ROW_BYTES_EXCEEDED, raw->remaining(raw), priv->row_bytes);

    r->rowid = rowid;

    for(int i=1; i<priv->sorters.length; i++) {
        struct sorter *sorter = &priv->sorters.s[i];
        if (sorter->tree.delete(&sorter->tree, rowid, e) == NOT_FOUND)
            THROW(e, "index[%s] key : %lld not found", sorter->name, rowid);
        if (e && *e) THROW_S(e); 
    }

    priv->cache->remove(priv->cache, rowid);
    // priv->cache->put(priv->cache, (keytype)rowid, (valtype)r, row_cache_dealloc);
    storage->write_at(storage, rowid, raw, e);
    if (e && *e) THROW_S(e);  

    for(int i=1; i<priv->sorters.length; i++) {
        struct sorter *sorter = &priv->sorters.s[i];
        sorter->tree.put(&sorter->tree, rowid, e);
        if (e && *e) THROW_S(e); 
    }

    // raw buffer no longer needed after write
    if (raw) table_return_raw_buffer(priv, raw);
    return rowid;

    EXCEPTION:
    if (raw) table_return_raw_buffer(priv, raw);
    return NOT_FOUND;
}

static i64 table_apply_at(struct flintdb_table *me, i64 rowid, struct flintdb_row *r, char **e) {
    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)me->priv;
    assert(priv);
    i64 transaction = 0;
    TABLE_LOCK(&priv->lock);
    transaction = priv->wal->begin(priv->wal, e);
    if (e && *e) goto EXCEPTION;

    i64 ok = table_apply_at_in_tx(me, rowid, r, e);
    if (e && *e) goto EXCEPTION;
    if (ok == NOT_FOUND) goto EXCEPTION;

    for (int i = 0; i < priv->sorters.length; i++) {
        struct sorter *s = &priv->sorters.s[i];
        if (s->tree.flush_meta) {
            s->tree.flush_meta(&s->tree, e);
            if (e && *e) goto EXCEPTION;
        }
    }

    priv->wal->commit(priv->wal, transaction, e);
    if (e && *e) goto EXCEPTION;

    TABLE_UNLOCK(&priv->lock);
    return ok;

    EXCEPTION:
    if (transaction > 0) priv->wal->rollback(priv->wal, transaction, NULL);
    TABLE_UNLOCK(&priv->lock);
    return NOT_FOUND;
}

static i64 table_delete_in_tx(struct flintdb_table *me, i64 rowid, char **e) {
    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)me->priv;
    if (rowid <= NOT_FOUND) THROW(e, "bad rowid: %lld", rowid);
    assert(priv);
    struct storage *storage = priv->storage;
    struct hashmap *cache = priv->cache;
    assert(storage);
    assert(cache);

    struct sorter *primary = &priv->sorters.s[0];
    const struct flintdb_row *r = table_read_unlocked(me, rowid, e);
    if (e && *e) THROW_S(e);
    if (r == NULL) THROW(e, "table_read(%lld) not found", rowid);

    for(int i=1; i<priv->sorters.length; i++) {
        struct sorter *sorter = &priv->sorters.s[i];
        if (sorter->tree.delete(&sorter->tree, rowid, e) == NOT_FOUND)
            THROW(e, "index[%s] key : %lld not found", sorter->name, rowid);
        if (e && *e) THROW_S(e); 
    }
    if (primary->tree.delete(&primary->tree, rowid, e) == NOT_FOUND)
        THROW(e, "primary[%s] key : %lld not found", primary->name, rowid);
    if (e && *e) THROW_S(e); 

    cache->remove(cache, rowid);
    storage->delete(storage, rowid, e);
    if (e && *e) THROW_S(e);

    return 1; // ok

    EXCEPTION:
    return NOT_FOUND;
}

static i64 table_delete(struct flintdb_table *me, i64 rowid, char **e) {
    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)me->priv;
    assert(priv);
    i64 transaction = 0;
    TABLE_LOCK(&priv->lock);
    transaction = priv->wal->begin(priv->wal, e);
    if (e && *e) goto EXCEPTION;

    i64 ok = table_delete_in_tx(me, rowid, e);
    if (e && *e) goto EXCEPTION;
    if (ok == NOT_FOUND) goto EXCEPTION;

    for (int i = 0; i < priv->sorters.length; i++) {
        struct sorter *s = &priv->sorters.s[i];
        if (s->tree.flush_meta) {
            s->tree.flush_meta(&s->tree, e);
            if (e && *e) goto EXCEPTION;
        }
    }

    priv->wal->commit(priv->wal, transaction, e);
    if (e && *e) goto EXCEPTION;

    TABLE_UNLOCK(&priv->lock);
    return ok;

    EXCEPTION:
    if (transaction > 0) priv->wal->rollback(priv->wal, transaction, NULL);
    TABLE_UNLOCK(&priv->lock);
    return NOT_FOUND;
}

static void find_close(struct flintdb_cursor_i64 *c) {
    if (!c) return;
    struct find_context *ctx = (struct find_context *)c->p;
    if (ctx) {
        if (ctx->base_cursor) {
            ctx->base_cursor->close(ctx->base_cursor);
            ctx->base_cursor = NULL;
        }
        if (ctx->filters) {
            filter_layers_free(ctx->filters);
            ctx->filters = NULL;
        }
        FREE(ctx);
    }
    FREE(c);
}

static i64 find_next(struct flintdb_cursor_i64 *c, char **e) {
    if (!c) return NOT_FOUND;
    struct find_context *ctx = (struct find_context *)c->p;
    if (!ctx) return NOT_FOUND;
    
    struct flintdb_table *table = ctx->table;
    if (!table) return NOT_FOUND;
    
    // Check offset: skip rows until offset is reached
    while (ctx->limit.priv.o > 0) {
        if (!ctx->base_cursor) return NOT_FOUND;
        i64 rowid = ctx->base_cursor->next(ctx->base_cursor, e);
        if (rowid == NOT_FOUND) return NOT_FOUND;
        if (e && *e) return NOT_FOUND;
        
        // Apply both indexable and non-indexable filters
        const struct flintdb_row *r = table->read(table, rowid, e);
        if (e && *e) return NOT_FOUND;
        if (!r) continue;
        
        int match = 1; // assume no match
        if (ctx->filters) {
            // Apply indexable filter (first layer)
            if (ctx->filters->first) {
                match = filter_compare(ctx->filters->first, (struct flintdb_row *)r, e);
                if (e && *e) return NOT_FOUND;
                if (match != 0) continue; // not matched, skip this row
            }
            
            // Apply non-indexable filter (second layer)
            if (ctx->filters->second) {
                match = filter_compare(ctx->filters->second, (struct flintdb_row *)r, e);
                if (e && *e) return NOT_FOUND;
                if (match != 0) continue; // not matched, skip this row
            }
        }
        
        ctx->limit.priv.o--;
    }
    
    // Check limit counter
    while (ctx->limit.priv.n > 0) {
        if (!ctx->base_cursor) return NOT_FOUND;
        i64 rowid = ctx->base_cursor->next(ctx->base_cursor, e);
        if (rowid == NOT_FOUND) return NOT_FOUND;
        if (e && *e) return NOT_FOUND;
        
        // Apply both indexable and non-indexable filters
        const struct flintdb_row *r = table->read(table, rowid, e);
        if (e && *e) return NOT_FOUND;
        if (!r) continue;
        
        int match = 1; // assume no match
        if (ctx->filters) {
            // Apply indexable filter (first layer) 
            if (ctx->filters->first) {
                match = filter_compare(ctx->filters->first, (struct flintdb_row *)r, e);
                if (e && *e) return NOT_FOUND;
                if (match != 0) continue; // not matched, skip this row
            }
            
            // Apply non-indexable filter (second layer)
            if (ctx->filters->second) {
                match = filter_compare(ctx->filters->second, (struct flintdb_row *)r, e);
                if (e && *e) return NOT_FOUND;
                if (match != 0) continue; // not matched, skip this row
            }
        }
        
        // Found a matching row
        ctx->limit.priv.n--;
        return rowid;
    }
    
    return NOT_FOUND;
}

// HOT_PATH
static int find_row_compare(void *obj, i64 key) {
    // Tri-state comparator for B+Tree range scans.
    // B+Tree expects: 0 = in range (continue scan), non-zero = out of range (stop)
    // filter_compare returns: 0 = match, non-zero = no match
    // So we need to invert the logic: return 0 when filter matches, non-zero otherwise
    struct find_context *ctx = (struct find_context *)obj;
    assert(ctx);
    // If there is no indexable filter, treat all rows as in-range
    if (!ctx->filters || !ctx->filters->first) return 0;
    const struct flintdb_row *r = ctx->table->read(ctx->table, key, NULL);
    // flintdb_print_row((struct flintdb_row *)r);
    assert(r);
    
    // filter_compare returns 0 on match, so we return it as-is
    // B+Tree will continue scanning while this returns 0 (match)
    return filter_compare(ctx->filters->first, (struct flintdb_row *)r, NULL);
}

static struct flintdb_cursor_i64 * table_find(const struct flintdb_table *me, 
    i8 index, 
    enum order order, 
    struct limit limit, 
    struct filter *filter, 
    char **e) {

    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)me->priv;
    assert(priv);
    assert(index > -1 && index < priv->sorters.length);
    struct flintdb_meta *meta = &priv->meta;

    struct sorter *sorter = &priv->sorters.s[index];
    struct flintdb_cursor_i64 *c = (struct flintdb_cursor_i64*)CALLOC(1, sizeof(struct flintdb_cursor_i64));
    if (!c) { if (c) FREE(c); return NULL; }
    struct find_context *impl = (struct find_context*)CALLOC(1, sizeof(struct find_context));

    // TRACE("table_find: filter=%p, index=%d", (void*)filter, index);

    struct filter_layers *filters = filter_split(filter, &priv->meta, &meta->indexes.a[index], e);
    if (e && *e) {
        WARN("table_find: filter_split failed: %s", *e);
        FREE(impl); FREE(c); return NULL;
    }
    
    // TRACE("table_find: filters=%p, first=%p, second=%p", 
    //       (void*)filters, 
    //       filters ? (void*)filters->first : NULL, 
    //       filters ? (void*)filters->second : NULL);

    impl->table = (struct flintdb_table *)me;  // cast away const
    impl->filters = filters;
    impl->limit = limit;
    impl->order = order;
    impl->index = index;
    // initialize limit counters
    impl->limit.priv.n = impl->limit.priv.limit < 0 ? INT_MAX : impl->limit.priv.limit;
    impl->limit.priv.o = impl->limit.priv.offset;

    struct flintdb_cursor_i64 *base = sorter->tree.find(&sorter->tree, order, impl, find_row_compare, e);
    if (e && *e) { 
        WARN("table_find: B+Tree find failed: %s", *e);
        if (filters) filter_layers_free(filters);
        FREE(impl); 
        FREE(c); 
        return NULL; 
    }
    if (!base) { 
        // WARN("table_find: B+Tree find returned NULL (no error, but empty result)");
        if (filters) filter_layers_free(filters);
        FREE(impl); 
        FREE(c); 
        return NULL; 
    }
    
    impl->base_cursor = base;  // store base cursor in context
    c->p = impl;
    c->close = find_close;
    c->next = find_next;
    return c;
}

static int table_find_index_from_hint(const struct flintdb_table *me, 
    const struct flintdb_sql *q, 
    int *index, // output best index
    enum order *order // output preferred order
    ) {
    // User-specified index hint takes precedence
    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)me->priv;
    assert(priv);

    char hint[SQL_OBJECT_STRING_LIMIT];
    const char *src = q->index;
    strncpy(hint, src, sizeof(hint)-1); hint[sizeof(hint)-1] = '\0';
    // tokenize by space: name [order]
    char *save = NULL;
    char *name = strtok_r(hint, " ", &save);
    char *orderkw = strtok_r(NULL, " ", &save);
    *index = meta_index_ordinal(&priv->meta, name);
    if (orderkw && strncasecmp(orderkw, "DESC", 4) == 0) *order = DESC;
    return 1; // found hint
}

static struct flintdb_cursor_i64 * table_find_where(const struct flintdb_table *me, const char *where, char **e) {
    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)me->priv;
    assert(priv);

    // Build a SELECT statement like Java: "SELECT * FROM <file> USE INDEX(primary ASC|DESC) WHERE <where>"
    char sql[SQL_STRING_LIMIT];
    const char *w = where ? where : "";
    struct flintdb_sql *q = NULL;
    struct filter *f  = NULL;

    if (!strempty(w))
        snprintf(sql, sizeof(sql), "SELECT * FROM %s %s", priv->file, w); // snprintf(sql, sizeof(sql), "SELECT * FROM %s WHERE %s", priv->file, w);
    else 
        snprintf(sql, sizeof(sql), "SELECT * FROM %s", priv->file);

    // printf("DEBUG: table_find_where: sql='%s'\n", sql);

    // Parse SQL to extract index hint, WHERE, and LIMIT
    q = flintdb_sql_parse(sql, e);
    if (q == NULL) THROW_S(e);

    // Determine index and order from index hint like Java
    int index = PRIMARY_INDEX; // default to primary
    enum order ord = ASC;
    if (!strempty(q->index)) 
        table_find_index_from_hint(me, q, &index, &ord);

    // Compile WHERE with selected index so filter.c can derive indexable conditions and range
    f = filter_compile(q->where, &priv->meta, e);
    if (e && *e) THROW_S(e);
    

    // DEBUG("table_find_where: filter=%p, where='%s', index=%d", (void*)f, q.where, index);

    // Parse LIMIT
    struct limit l = !strempty(q->limit) ? limit_parse(q->limit) : NOLIMIT;
    struct flintdb_cursor_i64 *result = table_find(me, (i8)index, ord, l, f, e);
    
    // Clean up filter after use (table_find copies it via filter_split)
    if (f) filter_free(f);
    if (q) flintdb_sql_free(q);
    
    return result;

    EXCEPTION:
    if (f) filter_free(f);
    if (q) flintdb_sql_free(q);
    return NULL;
}

static const struct flintdb_row * table_one(const struct flintdb_table *me, i8 index, u16 argc, const char **argv, char **e) {
    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)me->priv;
    assert(priv);
    assert(index > -1 && index < priv->sorters.length);

    struct sorter *sorter = &priv->sorters.s[index];
    struct flintdb_row *r = flintdb_row_from_argv(&priv->meta, argc, argv, e); // Row.create(meta, row)
    assert(r);
    i64 i = sorter->tree.compare_get(&sorter->tree, sorter, r, row_compare_get, e);
    return i < 0 ? NULL : ((struct flintdb_table*)me)->read((struct flintdb_table*)me, i, e);
}

static inline int table_row_from_buffer(struct flintdb_table *me, struct buffer *buf, struct flintdb_row **out, char **e) {
    assert(me);
    assert(buf);

    struct flintdb_table_priv *priv = (struct flintdb_table_priv*)me->priv;
    struct flintdb_meta *m = &priv->meta;
    assert(m);
    struct formatter *f = &priv->formatter;
    struct flintdb_row *r = flintdb_row_new(m, e);
    if (!r) return -1;
    if (e && *e) {
        FREE(r);
        return -1;
    }

    if (f->decode(f, buf, r, e) != 0) {
        FREE(r);
        return -1;
    }
    r->meta = m;
    assert(r->meta);

    *out = r;
    return 0;
}

// Streaming read: decode into caller-owned row buffer, skip cache entirely.
// Used by SELECT scans to eliminate per-row allocations and cache retention.
HOT_PATH
static int table_read_stream(struct flintdb_table *me, i64 rowid, struct flintdb_row *dest, char **e) {
    if (!me || !dest) {
        if (e) *e = STRDUP("table_read_stream: NULL parameter");
        return -1;
    }
    if (!me->priv) {
        if (e) *e = STRDUP("table_read_stream: NULL priv");
        return -1;
    }

    struct flintdb_table_priv *priv = (struct flintdb_table_priv*)me->priv;
    struct buffer *buf = priv->storage->read(priv->storage, rowid, e);
    if (e && *e) return -1;
    if (!buf) {
        if (e) *e = STRDUP("table_read_stream: NULL buffer");
        return -1;
    }

    struct formatter *f = &priv->formatter;
    if (f->decode(f, buf, dest, e) != 0) {
        buf->free(buf);
        return -1;
    }
    buf->free(buf);

    dest->rowid = rowid;
    dest->meta = &priv->meta;
    return 0;
}

// Internal read without lock (caller must hold lock)
static const struct flintdb_row * table_read_unlocked(struct flintdb_table *me, i64 rowid, char **e) {
    if (!me) return NULL;
    if (!me->priv) return NULL;

    struct flintdb_table_priv *priv = (struct flintdb_table_priv*)me->priv;
    struct hashmap *cache = priv->cache;
    assert(cache);
    struct flintdb_row *cached = (struct flintdb_row *)cache->get(cache, rowid);
    if (cached && cached != (struct flintdb_row*)HASHMAP_INVALID_VAL) {
        return cached;
    }

    struct buffer *buf = priv->storage->read(priv->storage, rowid, e);
    if (e && *e) {
        return NULL;
    }

    struct flintdb_row *out = NULL;
    if (table_row_from_buffer(me, buf, &out, e) != 0) {
        if (buf) buf->free(buf);
        return NULL;
    }   
    buf->free(buf);

    out->rowid = rowid;
    cache->put(cache, rowid, (valtype)out, row_cache_dealloc);
    return out;
}

HOT_PATH
static const struct flintdb_row * table_read(struct flintdb_table *me, i64 rowid, char **e) {
    if (!me) return NULL;
    if (!me->priv) return NULL;

    struct flintdb_table_priv *priv = (struct flintdb_table_priv*)me->priv;
    TABLE_LOCK(&priv->lock);
    const struct flintdb_row *result = table_read_unlocked(me, rowid, e);
    TABLE_UNLOCK(&priv->lock);
    return result;
}

static void table_close(struct flintdb_table *me) {
    if (!me) return;

    // LOG("closing table %p", (void*)me);

    if (me->priv) {
        struct flintdb_table_priv *priv = (struct flintdb_table_priv*)me->priv;
        DEBUG("closing %d sorter(s)", priv->sorters.length);
        for(int i=0; i<(priv->sorters.length); i++) {
            struct sorter *s = &priv->sorters.s[i];
            assert(s);
            assert(s->tree.close);
            DEBUG("closing sorter[%d] %s", i, s->name);
            s->tree.close(&s->tree);
        }

        DEBUG("clearing cache");
        if (priv->cache) {
            priv->cache->clear(priv->cache);
            priv->cache->free(priv->cache);
            priv->cache = NULL;
        }

        // Destroy table-level lock
        TABLE_LOCK_DESTROY(&priv->lock);

        // Free table-local header mapping created via storage.mmap in table_open
        DEBUG("freeing header slice");
        if (priv->header) {
            priv->header->free(priv->header);
            priv->header = NULL;
        }

        DEBUG("closing storage");
        if (priv->storage) {
            if (!priv->storage->managed_by_wal) {
                priv->storage->close(priv->storage);
                DEBUG("freeing wrapped storage");
                FREE(priv->storage);
                DEBUG("wrapped storage freed");
            }
            priv->storage = NULL;
        }
        DEBUG("storage closed");

        DEBUG("closing wal");
        if (priv->wal) {
            priv->wal->close(priv->wal);
            priv->wal = NULL;
        }

        DEBUG("closing formatter");
        priv->formatter.close(&priv->formatter);
        DEBUG("closing meta");
        flintdb_meta_close(&priv->meta);

        // Free raw buffer pool
        DEBUG("freeing raw buffer pool");
        if (priv->raw_pool) {
            priv->raw_pool->free(priv->raw_pool);
            priv->raw_pool = NULL;
        }

        DEBUG("freeing table priv");
        FREE(me->priv);
        me->priv = NULL;
    }

    DEBUG("freeing table object");
    FREE(me);
}


struct flintdb_transaction * flintdb_transaction_begin(struct flintdb_table *table, char **e) {
    if (!table || !table->priv) THROW(e, "table is null"); 

    struct flintdb_table_priv *tpriv = (struct flintdb_table_priv*)table->priv;
    if (!tpriv->wal) THROW(e, "WAL is not initialized");

    struct flintdb_transaction *tx = (struct flintdb_transaction*)CALLOC(1, sizeof(struct flintdb_transaction));
    if (!tx) THROW(e, "Out of memory");
    struct flintdb_transaction_priv *p = (struct flintdb_transaction_priv*)CALLOC(1, sizeof(struct flintdb_transaction_priv));
    if (!p) {
        FREE(tx);
        THROW(e, "Out of memory");
    }

    // Acquire the table lock first, then start WAL tx (match Java TransactionImpl semantics)
    TABLE_LOCK(&tpriv->lock);

    // Snapshot current index counts so rollback can restore tbl->rows deterministically.
    p->count_len = tpriv->sorters.length;
    if (p->count_len > MAX_INDEX_KEYS_LIMIT) p->count_len = MAX_INDEX_KEYS_LIMIT;
    for (int i = 0; i < p->count_len; i++) {
        struct sorter *s = &tpriv->sorters.s[i];
        p->counts[i] = (s->tree.count_get ? s->tree.count_get(&s->tree) : s->tree.count);
    }

    i64 id = tpriv->wal->begin(tpriv->wal, e);
    if (e && *e) {
        TABLE_UNLOCK(&tpriv->lock);
        FREE(p);
        FREE(tx);
        return NULL;
    }

    p->table = table;
    p->tpriv = tpriv;
    p->id = id;
    p->done = 0;

    tx->id = tx_id;
    tx->apply = tx_apply;
    tx->apply_at = tx_apply_at;
    tx->delete_at = tx_delete_at;
    tx->commit = tx_commit;
    tx->rollback = tx_rollback;
    tx->close = tx_close;
    tx->validate = tx_validate;
    tx->priv = p;
    return tx;

    EXCEPTION:
    return NULL;
}

int flintdb_table_drop(const char *file, char **e) { // delete <table>, <table>.desc, <table>.i.*
    char dir[PATH_MAX] = {0};
    getdir(file, dir);
    // DEBUG("dir: %s", dir);

    if (!dir_exists(dir)) return 0; // nothing to do

    DIR *d = opendir(dir);
    DEBUG("opendir: %s, %p", dir, d);
    if (!d) THROW(e, "Failed to open directory: %s", dir); 

    // Compute basename once; getname requires a valid buffer, not NULL
    char base[PATH_MAX] = {0};
    getname(file, base);
    size_t base_len = strlen(base);

    struct dirent* de = readdir(d);
    while (de) {
        if (0 == strncmp(de->d_name, base, base_len)) {
            char f[PATH_MAX] = {0, };
            snprintf(f, PATH_MAX, "%s%c%s", dir, PATH_CHAR, de->d_name);
            // printf("rm : %s\n", f);
            if (unlink(f) != 0) {
                THROW(e, "Failed to remove file: %s", f);
            }
        }
        de = readdir(d);
    }

    closedir(d);
    return 0;

    EXCEPTION:
    if (d) closedir(d);
    return -1;
}

int row_bytes(const struct flintdb_meta *m) {
    if (!m) return -1;

    int n = I16_BYTES;  // column count
    for(int i=0; i<m->columns.length; i++) {
        const struct flintdb_column *c = &m->columns.a[i];
        enum flintdb_variant_type  t = c->type;
        n += I16_BYTES; // type
        if (VARIANT_STRING == t || VARIANT_DECIMAL == t || VARIANT_BYTES == t || VARIANT_BLOB == t) {
            assert(c->bytes > 0);
            n += I16_BYTES; // bytes length
            n += c->bytes;  // bytes
        } else if (VARIANT_INT8 == t || VARIANT_UINT8 == t) {
            n += 1;
        } else if (VARIANT_INT16 == t || VARIANT_UINT16 == t) {
            n += 2;
        } else if (VARIANT_INT32 == t || VARIANT_UINT32 == t || VARIANT_FLOAT == t) {
            n += 4;
        } else if (VARIANT_INT64 == t || VARIANT_DOUBLE == t || VARIANT_TIME == t) {
            n += 8;
        } else if (VARIANT_DATE == t) {
            n += 3; // 24bit
        } else if (VARIANT_NULL == t || VARIANT_ZERO == t) {
            // nothing
        } else if (VARIANT_UUID == t || VARIANT_IPV6 == t) {
            n += 16; // fixed 16 bytes
        } else {
            PANIC("Unsupported column type: %d", t);
        }
    }
    return n;
}

HOT_PATH
HOT_PATH
static int sorter_primary_cmpr(void *o, i64 a, i64 b) {
    if (a == b) return 0;

    struct sorter *s = (struct sorter*)o;
    assert(s);
    assert(s->keys.length > 0);
    struct flintdb_table *t = s->table;
    assert(t);

    const struct flintdb_row *r1 = table_read_unlocked(t, a, NULL);
    const struct flintdb_row *r2 = table_read_unlocked(t, b, NULL);
    assert(r1);
    assert(r2);
    int cmp = 0;
    char *e = NULL;
    for(int i=0; i<s->keys.length; i++) {
        int key = s->keys.a[i];
        struct flintdb_variant *v1 = r1->get(r1, key, &e);
        struct flintdb_variant *v2 = r2->get(r2, key, &e);
        cmp = flintdb_variant_compare(v1, v2);
        if (cmp != 0) break;
    }

    if (e) 
        WARN("%s", e);
    return cmp;
}

static int sorter_index_cmpr(void *o, i64 a, i64 b) {
    if (a == b) return 0;

    struct sorter *s = (struct sorter*)o;
    assert(s);
    assert(s->keys.length > 0);
    struct flintdb_table *t = s->table;
    assert(t);

    const struct flintdb_row *r1 = table_read_unlocked(t, a, NULL);
    const struct flintdb_row *r2 = table_read_unlocked(t, b, NULL);
    assert(r1);
    assert(r2);

    if (r1->rowid == r2->rowid && r1->rowid != NOT_FOUND) return 0; // avoid unnecessary compare if rowid is same

    int cmp = 0;
    char *e = NULL;
    for(int i=0; i<s->keys.length; i++) {
        int key = s->keys.a[i];
        struct flintdb_variant *v1 = r1->get(r1, key, &e);
        struct flintdb_variant *v2 = r2->get(r2, key, &e);
        cmp = flintdb_variant_compare(v1, v2);
        if (cmp != 0) break;
    }

    if (e) 
        WARN("%s", e);
    return cmp;
}

HOT_PATH
HOT_PATH
static int row_compare_get(void *o, const void *a, i64 b) {
    // DEBUG("o=%p a=%p b=%lld", o, a, b);
    struct sorter *s = (struct sorter*)o;
    assert(s);
    assert(s->keys.length > 0);
    struct flintdb_table *t = s->table;
    assert(t);

    const struct flintdb_row *r1 = (const struct flintdb_row *)a;
    const struct flintdb_row *r2 = table_read_unlocked(t, b, NULL);
    // DEBUG("r1=%p r2=%p", (void*)r1, (void*)r2);
    assert(r1);
    assert(r2);
    
    // DEBUG("r1.id=%lld r2.id=%lld", r1->rowid, r2->rowid);
    if (r1->rowid == r2->rowid && r1->rowid != NOT_FOUND) return 0; // avoid unnecessary compare if rowid is same

    int cmp = 0;
    char *e = NULL;
    for(int i=0; i<s->keys.length; i++) {
        int key = s->keys.a[i];
        // DEBUG("key[%d]=%d", i, key);
        struct flintdb_variant *v1 = r1->get(r1, key, &e);
        struct flintdb_variant *v2 = r2->get(r2, key, &e);
        // DEBUG("v1=%p v2=%p", (void*)v1, (void*)v2);
        cmp = flintdb_variant_compare(v1, v2);
        // DEBUG("cmp=%d", cmp);
        if (cmp != 0) break;
    }

    if (e) 
        WARN("%s", e);
    return cmp;
}

// Resolve index ordinal by name (case-insensitive). Defaults to primary when name is null/empty.
static int meta_index_ordinal(const struct flintdb_meta *m, const char *name) {
    if (!m || m->indexes.length <= 0) return 0;
    if (!name || !*name) return 0; // default PRIMARY
    // PRIMARY is always the first
    if (strncasecmp(name, PRIMARY_NAME, sizeof(PRIMARY_NAME)-1) == 0) return 0;
    for (int i = 0; i < m->indexes.length; i++) {
        if (strncasecmp(m->indexes.a[i].name, name, MAX_COLUMN_NAME_LIMIT-1) == 0) return i;
    }
    return 0; // fallback to PRIMARY
}

static int table_wal_refresh(const void *obj, i64 rowid) {
    struct flintdb_table *table = (struct flintdb_table *)obj;
    assert(table);
    struct flintdb_table_priv* priv = (struct flintdb_table_priv*)table->priv;
    assert(priv);
    struct hashmap *cache = priv->cache;
    assert(cache);
    // Invalidate cache entry for the updated rowid
    cache->remove(cache, rowid);
    return 0; // success
}

struct flintdb_table * flintdb_table_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e) {
    struct flintdb_table *table = NULL;
    struct flintdb_table_priv *priv = NULL;
    struct wal *wal = NULL;
    struct flintdb_meta m = {0};

    if (!file) THROW(e, "file is NULL");
    if (access(file, F_OK) != 0 && mode == FLINTDB_RDONLY) THROW(e, "file does not exist: %s", file);

    if (NULL == meta) {
        // read meta from <file>.desc
        char desc[PATH_MAX] = {0};
        snprintf(desc, sizeof(desc), "%s%s", file, META_NAME_SUFFIX);
        if (access(desc, F_OK) != 0) THROW(e, "desc file does not exist: %s", desc);
        
        m = flintdb_meta_open(desc, e);
        if (m.columns.length <= 0) THROW(e, "meta has no columns");
        if (m.indexes.length == 0) THROW(e, "meta has no indexes");
    } else if (mode == FLINTDB_RDWR) {
        if (meta->indexes.length == 0) THROW(e, "meta has no indexes");
        if ((strncasecmp(meta->storage, TYPE_MEMORY, sizeof(TYPE_MEMORY)-1) == 0)) {
            // In-memory table: do not write meta to disk
            memcpy(&m, meta, sizeof(struct flintdb_meta));
        } else {
            // write meta to <file>.desc if not exists
            char dir[PATH_MAX] = {0};
            getdir(file, dir);
            mkdirs(dir, S_IRWXU); // ensure directory exists

            char desc[PATH_MAX] = {0}; // <file>.desc
            snprintf(desc, sizeof(desc), "%s%s", file, META_NAME_SUFFIX); 
            if (access(desc, F_OK) != 0) {
                // write meta to <file>.desc
                if (meta->columns.length <= 0) THROW(e, "meta has no columns");
                if (flintdb_meta_write(meta, desc, e) != 0) THROW_S(e);
                memcpy(&m, meta, sizeof(struct flintdb_meta));
            } else {
                // read existing meta and compare
                m = flintdb_meta_open(desc, e);
                if (m.columns.length <= 0) THROW(e, "existing meta has no columns");
                if (flintdb_meta_compare(&m, meta) != 0)
                    THROW(e, "meta does not match existing: %s", desc);
            }
        }
    }

    if (!strempty(m.compressor) && strncmp(TYPE_V1, m.compressor, sizeof(TYPE_V1)-1) != 0) THROW(e, "Compressor not supported yet: %s", m.compressor);
    
    table = CALLOC(1, sizeof(struct flintdb_table));
    if (!table) THROW(e, "Failed to allocate memory for table_priv");
   
    priv = CALLOC(1, sizeof(struct flintdb_table_priv));
    if (!priv) THROW(e, "Failed to allocate memory for table_priv");
    table->priv = priv;
    snprintf(priv->file, sizeof(priv->file), "%s", file);
    priv->mode = mode;
    priv->meta = m;
    priv->meta.priv = NULL; // ensure no dangling pointer to local meta
    priv->row_bytes = row_bytes(&m);
    if (priv->row_bytes <= 0) THROW(e, "Failed to calculate row bytes");
    // IMPORTANT: bind formatter to the persistent meta stored in priv, not the local 'meta' copy
    if (formatter_init(FORMAT_BIN, &priv->meta, &priv->formatter, e) != 0) THROW_S(e);

    // Initialize reusable raw buffer pool using generic buffer_pool
    // capacity: 32 slots, align(min capacity): row_bytes, preload: 8
    priv->raw_pool = buffer_pool_create(32, (u32)priv->row_bytes, 8);

    struct storage_opts opts = {
        .block_bytes = row_bytes(&m),
        // Honor meta.increment when provided; fallback to default if not set
        .increment = (m.increment > 0 ? (i32)m.increment : DEFAULT_STORAGE_INCREMENT),
        .mode = mode,
        .compact = m.compact,
    };
    strncpy(opts.file, file, sizeof(opts.file)-1);
    
    char wal_file[PATH_MAX] = {0};
    snprintf(wal_file, sizeof(wal_file), "%s%s", file, ".wal");

    // wal = &WAL_NONE; // disable WAL for testing
    wal = (mode == FLINTDB_RDONLY || strempty(priv->meta.wal) || 0 == strncasecmp(priv->meta.wal, WAL_OPT_OFF, sizeof(WAL_OPT_OFF)-1) || 0 == strncasecmp(priv->meta.storage, TYPE_MEMORY, sizeof(TYPE_MEMORY)-1))
        ? &WAL_NONE 
        : wal_open(wal_file, (const struct flintdb_meta *)&priv->meta, e);
    if (e && *e) THROW_S(e);
    assert(wal);
    priv->wal = wal;
    priv->storage = wal_wrap(wal, &opts, table_wal_refresh, table, e);
    if (e && *e) THROW_S(e);
    if (!priv->storage) THROW(e, "Failed to allocate memory for storage");

    // LRU cache
    i32 cache_limit = priv->meta.cache; 
    if (cache_limit <= DEFAULT_TABLE_CACHE_LIMIT) cache_limit = DEFAULT_TABLE_CACHE_LIMIT;
    if (mode == FLINTDB_RDONLY) cache_limit = cache_limit / 2; // smaller cache for read-only tables
    if (cache_limit < DEFAULT_TABLE_CACHE_MIN) cache_limit = DEFAULT_TABLE_CACHE_MIN;
    priv->cache = lruhashmap_new(cache_limit * 2, cache_limit, &hashmap_int_hash, &hashmap_int_cmpr);

    if (!priv->cache) THROW(e, "Failed to create row cache");

    // Initialize table-level lock (os_unfair_lock on macOS, spinlock on Linux)
    TABLE_LOCK_INIT(&priv->lock);

    priv->header = priv->storage->mmap(priv->storage, 0, HEAD_SZ, NULL);

    if (mode & FLINTDB_RDWR) {
        struct buffer h = {0, };
        struct buffer p = {0, };
        priv->header->slice(priv->header, 0, HEAD_SZ, &h, e);
        h.slice(&h, 0, HEAD_SZ, &p, e);
        p.i32_get(&p, NULL);
        if (0 == p.i32_get(&p, NULL)) {
            h.array_put(&h, SIGNATURE, 4, NULL);
            h.i32_put(&h, 1, NULL); // version
        }
        //p.free(&p); // uneccessary
        //h.free(&h); // uneccessary
    }

    priv->sorters.length = m.indexes.length;
    for(int i=0; i<m.indexes.length; i++) {
        struct sorter *s = &priv->sorters.s[i];
        s->table = table;
        strncpy(s->name, m.indexes.a[i].name, sizeof(s->name)-1);
        strncpy(s->algorithm, "bptree", sizeof(s->algorithm)-1); // currently only bptree is supported

        if (i == 0 && strncasecmp(PRIMARY_NAME, s->name, sizeof(PRIMARY_NAME)) != 0) 
            THROW(e, "The first index must set to primary key");

        char ixf[PATH_MAX] = {0};
        snprintf(ixf, sizeof(ixf), "%s.i.%s", file, s->name);

        // map this index's key names to column indices
        const struct flintdb_index *idx = &m.indexes.a[i];
        s->keys.length = idx->keys.length;
        for (int j = 0; j < idx->keys.length; j++) {
            int c = flintdb_column_at(&priv->meta, idx->keys.a[j]);
            assert(c != -1);
            s->keys.a[j] = c;
            DEBUG("%s[%d] column:%s => %d", s->name, j, idx->keys.a[j], s->keys.a[j]);
        }

        if (i == 0) {
            bplustree_init(&s->tree, ixf, cache_limit * 1, mode, TYPE_DEFAULT, s, &sorter_primary_cmpr, wal, e);
            if (e && *e) THROW_S(e);
        } else {
            bplustree_init(&s->tree, ixf, cache_limit * 1, mode, TYPE_DEFAULT, s, &sorter_index_cmpr, wal, e);
            if (e && *e)  THROW_S(e);
        }
    }

    //  
    table->rows = table_rows;
    table->bytes = table_bytes;
    table->meta = table_meta;
    table->apply = table_apply;
    table->apply_at = table_apply_at;
    table->delete_at = table_delete;
    // table->find = table_find;
    table->find = table_find_where;
    table->one = table_one;
    table->read = table_read;
    table->read_stream = table_read_stream;
    table->close = table_close;
    return table;

    EXCEPTION:
    if (table) 
        table_close(table);
    return NULL;
}
