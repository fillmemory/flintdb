#include "buffer.h"
#include "allocator.h"
#include "filter.h"
#include "hashmap.h"
#include "hyperloglog.h"
#include "iostream.h"
#include "list.h"
#include "flintdb.h"
#include "roaringbitmap.h"
#include "runtime.h"
#include "sql.h"
#include "internal.h"

#include <dirent.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FLINTDB_TEMP_DIR "./temp"
#define SQL_EXEC_SHARED_TABLES 1

// TODO: union file formats for table_open and genericfile_open?

// Ensure and return path to temp directory. Uses environment variable FLINTDB_TEMP_DIR if set,
// otherwise falls back to the default macro. Automatically creates the directory if missing.
static const char * ensure_temp_dir(void) {
    static int initialized = 0;
    static char path[PATH_MAX];
    const char *envp = getenv("FLINTDB_TEMP_DIR");
    if (envp && *envp) {
        strncpy(path, envp, sizeof(path) - 1);
        path[sizeof(path) - 1] = '\0';
    } else {
        pid_t pid = getpid();
        snprintf(path, sizeof(path), "%s/flintdb_tmp_%d", FLINTDB_TEMP_DIR, (int)pid);
    }
    if (!initialized) {
        struct stat st;
        if (stat(path, &st) != 0) {
            // Attempt to create; ignore errors (will surface on file operations later)
            mkdir(path, 0777);
        } else if (!S_ISDIR(st.st_mode)) {
            // If path exists but not a directory, attempt to recreate as directory with suffix
            strncat(path, "_dir", sizeof(path) - strlen(path) - 1);
            mkdir(path, 0777);
        }
        initialized = 1;
    }
    return path;
}

/**
 * @brief Clean up temporary files created during SQL execution.
 */
void sql_exec_cleanup() {
    const char *temp_dir = ensure_temp_dir();
    DIR *dir = opendir(temp_dir);
    if (!dir) {
        return; // Nothing to clean
    }
    struct dirent *entry;
    char filepath[PATH_MAX];
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        snprintf(filepath, sizeof(filepath), "%s/%s", temp_dir, entry->d_name);
        remove(filepath); // Ignore errors
    }
    closedir(dir);
    rmdir(temp_dir); // Remove temp directory itself
}

/**
 * 
 * TEST: ./bin/flintdb "SELECT  l_shipmode FROM temp/tpch_lineitem.flintdb  LIMIT 10" -pretty
 * TEST: ./bin/flintdb "SELECT  l_shipmode FROM temp/tpch_lineitem.tsv.gz  LIMIT 10" -pretty
 * TEST: ./bin/flintdb "SELECT l_shipmode, COUNT(*), SUM(l_quantity), SUM(l_extendedprice) FROM temp/tpch_lineitem.flintdb GROUP BY l_shipmode LIMIT 10" -pretty
 * TEST: ./bin/flintdb "SELECT l_shipmode, COUNT(*), SUM(l_quantity), SUM(l_extendedprice) FROM temp/tpch/lineitem.tbl.gz GROUP BY l_shipmode LIMIT 10" -pretty
 * TEST: ./bin/flintdb "SELECT DISTINCT l_shipmode FROM temp/tpch_lineitem.flintdb LIMIT 10" -pretty
 * TEST: ./bin/flintdb "SELECT * FROM temp/tpch_lineitem.flintdb USE INDEX(primary DESC) LIMIT 10" -pretty
 * TEST: ./bin/flintdb "SELECT COUNT(*) v FROM temp/tpch_lineitem.flintdb LIMIT 10" -pretty  => table->rows()
 * 
 *  => performance test vs Java
 * TEST: time ./bin/flintdb "SELECT * FROM temp/tpch/lineitem.tbl.gz " > /dev/null
 * TEST: time ../java/bin/flintdb "SELECT * FROM temp/tpch/lineitem.tbl.gz " > /dev/null
 * TEST: time ./bin/flintdb "SELECT l_shipmode, COUNT(*), SUM(l_quantity), SUM(l_extendedprice) FROM temp/tpch/lineitem.tbl.gz GROUP BY l_shipmode LIMIT 10 "
 * TEST: time ../java/bin/flintdb "SELECT l_shipmode, COUNT(*), SUM(l_quantity), SUM(l_extendedprice) FROM temp/tpch/lineitem.tbl.gz GROUP BY l_shipmode LIMIT 10 "
 */

// Forward declarations for functions referenced by sql_exec
static struct flintdb_sql_result * sql_exec_begin_transaction(struct flintdb_sql *q, struct flintdb_transaction *t, char **e);
static struct flintdb_sql_result * sql_exec_commit_transaction(struct flintdb_sql *q, struct flintdb_transaction *t, char **e);
static struct flintdb_sql_result * sql_exec_rollback_transaction(struct flintdb_sql *q, struct flintdb_transaction *t, char **e);
static struct flintdb_sql_result * sql_exec_select(struct flintdb_sql *q, struct flintdb_transaction *t, char **e);
static struct flintdb_sql_result * sql_exec_gf_select(struct flintdb_sql *q, char **e);
static int sql_exec_insert(struct flintdb_sql *q, struct flintdb_transaction *t, char **e);
static int sql_exec_update(struct flintdb_sql *q, struct flintdb_transaction *t, char **e);
static int sql_exec_delete(struct flintdb_sql *q, struct flintdb_transaction *t, char **e);
static int sql_exec_insert_from(struct flintdb_sql *q, struct flintdb_transaction *t, char **e);
static int sql_exec_select_into(struct flintdb_sql *q, struct flintdb_transaction *t, char **e);
static int sql_exec_create(struct flintdb_sql *q, char **e);
static int sql_exec_drop(struct flintdb_sql *q, char **e);
static int sql_exec_alter(struct flintdb_sql *q, char **e);
static struct flintdb_sql_result * sql_exec_describe(struct flintdb_sql *q, char **e);
static struct flintdb_sql_result * sql_exec_meta(struct flintdb_sql *q, char **e);
static struct flintdb_sql_result * sql_exec_show_tables(struct flintdb_sql *q, char **e);
// helper forward decls used before definition
static struct flintdb_sql_result * sql_exec_fast_count(struct flintdb_sql *q, struct flintdb_table *table, char **e);

// Other helpers used before their definitions
static struct flintdb_table * sql_exec_table_borrow(const char *table, char **e);
static int has_aggregate_function(struct flintdb_sql *q);
static struct flintdb_sql_result * sql_exec_select_groupby_row(struct flintdb_sql *q, struct flintdb_table *table, struct flintdb_cursor_row *cr, struct flintdb_genericfile *gf, char **e);
static struct flintdb_sql_result * sql_exec_select_groupby_i64(struct flintdb_sql *q, struct flintdb_table *table, struct flintdb_cursor_i64 *cr, char **e);
static struct flintdb_sql_result * sql_exec_sort(struct flintdb_cursor_row *cr, const char *orderby, const char *limit, char **e);

extern const char * flintdb_variant_type_name(enum flintdb_variant_type  t);

// Helper: format bytes into human readable string (similar to Java IO.readableBytesSize)
static char *bytes_human(i64 bytes, char *buf, size_t cap) {
    if (!buf || cap == 0)
        return NULL;
    if (bytes < 0) {
        snprintf(buf, cap, "");
        return buf;
    }
    const char *units[] = {"B", "KB", "MB", "GB", "TB"};
    int u = 0;
    double v = (double)bytes;
    while (v >= 1024.0 && u < 4) {
        v /= 1024.0;
        u++;
    }
    if (v < 10.0)
        snprintf(buf, cap, "%.2f%s", v, units[u]);
    else if (v < 100.0)
        snprintf(buf, cap, "%.1f%s", v, units[u]);
    else
        snprintf(buf, cap, "%.0f%s", v, units[u]);
    return buf;
}

// Helper: relativize path to current working directory
static void relativize_path(const char *abs, char *out, size_t cap) {
    if (!abs || !out || cap == 0)
        return;
    char cwd[PATH_MAX] = {0};
    if (!getcwd(cwd, sizeof(cwd))) {
        strncpy(out, abs, cap - 1);
        out[cap - 1] = '\0';
        return;
    }
    size_t cwd_len = strlen(cwd);
    if (strncmp(abs, cwd, cwd_len) == 0) {
        const char *p = abs + cwd_len;
        if (*p == '/' || *p == '\\')
            p++;
        strncpy(out, p, cap - 1);
        out[cap - 1] = '\0';
    } else {
        strncpy(out, abs, cap - 1);
        out[cap - 1] = '\0';
    }
}


#ifdef SQL_EXEC_SHARED_TABLES
// Pooled table handles with atomic-based synchronization
struct pooled_table {
    char *key; // strdup(file path)
    struct flintdb_table *table;
    void (*orig_close)(struct flintdb_table *);
    i32 refcount;
    time_t last_used;
};

// Use C11 stdatomic spinlock for cross-platform compatibility (macOS, Linux, Windows MinGW)
#define TABLE_POOL_LOCK(lock) do { int expected = 0; while (!atomic_compare_exchange_weak_explicit(lock, &expected, 1, memory_order_acquire, memory_order_relaxed)) { expected = 0; } } while(0)
#define TABLE_POOL_UNLOCK(lock) atomic_store_explicit(lock, 0, memory_order_release)

static struct {
    atomic_int lock;
    int inited;
    struct hashmap *by_path; // key: char* (string)
    struct hashmap *by_ptr;  // key: table* pointer
} g_table_pool = {0, 0, NULL, NULL};

static void pooled_entry_dealloc(keytype k, valtype v) {
    (void)k;
    struct pooled_table *pt = (struct pooled_table *)(uintptr_t)v;
    if (!pt)
        return;
    if (pt->table && pt->orig_close) {
        pt->orig_close(pt->table);
        pt->table = NULL;
    }
    if (pt->key) {
        FREE(pt->key);
        pt->key = NULL;
    }
    FREE(pt);
}
static void pooled_ptr_dealloc(keytype k, valtype v) {
    (void)k;
    (void)v;
}

static void table_pool_shutdown(void) {
    DEBUG("Shutting down table pool");

    TABLE_POOL_LOCK(&g_table_pool.lock);
    if (g_table_pool.by_ptr) {
        g_table_pool.by_ptr->clear(g_table_pool.by_ptr);
        g_table_pool.by_ptr->free(g_table_pool.by_ptr);
        g_table_pool.by_ptr = NULL;
    }
    if (g_table_pool.by_path) {
        g_table_pool.by_path->clear(g_table_pool.by_path);
        g_table_pool.by_path->free(g_table_pool.by_path);
        g_table_pool.by_path = NULL;
    }
    g_table_pool.inited = 0;
    TABLE_POOL_UNLOCK(&g_table_pool.lock);
}
static void table_pool_init(void) {
    if (g_table_pool.inited)
        return;
    TABLE_POOL_LOCK(&g_table_pool.lock);
    if (!g_table_pool.inited) {
        g_table_pool.by_path = hashmap_new(1024, &hashmap_string_hash, &hashmap_string_cmpr);
        g_table_pool.by_ptr = hashmap_new(1024, &hashmap_pointer_hash, &hashmap_pointer_cmpr);
        atexit(table_pool_shutdown);
        g_table_pool.inited = 1;
    }
    TABLE_POOL_UNLOCK(&g_table_pool.lock);
}

static void pool_close_shim(struct flintdb_table *me) {
    if (!me)
        return;
    table_pool_init();
    TABLE_POOL_LOCK(&g_table_pool.lock);
    if (!g_table_pool.by_ptr) {
        TABLE_POOL_UNLOCK(&g_table_pool.lock);
        return;
    }
    struct pooled_table *pt = (struct pooled_table *)(uintptr_t)g_table_pool.by_ptr->get(g_table_pool.by_ptr, (keytype)(uintptr_t)me);
    if (!pt || pt == (struct pooled_table *)(uintptr_t)HASHMAP_INVALID_VAL) {
        TABLE_POOL_UNLOCK(&g_table_pool.lock);
        return; // unknown
    }
    if (pt->refcount > 0)
        pt->refcount--;
    pt->last_used = time(NULL);
    if (pt->refcount == 0) {
        // remove from maps; by_path removal will trigger pooled_entry_dealloc to close/free
        if (g_table_pool.by_ptr)
            g_table_pool.by_ptr->remove(g_table_pool.by_ptr, (keytype)(uintptr_t)me);
        if (g_table_pool.by_path)
            g_table_pool.by_path->remove(g_table_pool.by_path, (keytype)(uintptr_t)pt->key);
        TABLE_POOL_UNLOCK(&g_table_pool.lock);
        return;
    }
    TABLE_POOL_UNLOCK(&g_table_pool.lock);
}

// Open a table using pool (RDWR). Returned table->close returns to pool.
static struct flintdb_table * sql_exec_table_borrow(const char *file, char **e) {
    table_pool_init();
    if (!file || !*file) {
        THROW(e, "file is NULL");
    }
    TABLE_POOL_LOCK(&g_table_pool.lock);
    struct pooled_table *pt = NULL;
    if (g_table_pool.by_path) {
        pt = (struct pooled_table *)(uintptr_t)g_table_pool.by_path->get(g_table_pool.by_path, (keytype)(uintptr_t)file);
        if (pt == (struct pooled_table *)(uintptr_t)HASHMAP_INVALID_VAL)
            pt = NULL;
    }
    if (!pt) {
        TABLE_POOL_UNLOCK(&g_table_pool.lock);
        struct flintdb_table *t = flintdb_table_open(file, FLINTDB_RDWR, NULL, e);
        if (e && *e)
            return NULL;
        struct pooled_table *entry = (struct pooled_table *)CALLOC(1, sizeof(struct pooled_table));
        if (!entry) {
            t->close(t);
            THROW(e, "Out of memory");
        }
        entry->key = STRDUP(file);
        entry->table = t;
        entry->orig_close = t->close;
        entry->refcount = 1;
        entry->last_used = time(NULL);
        t->close = pool_close_shim;
        TABLE_POOL_LOCK(&g_table_pool.lock);
        if (g_table_pool.by_path)
            g_table_pool.by_path->put(g_table_pool.by_path, (keytype)(uintptr_t)entry->key, (valtype)(uintptr_t)entry, pooled_entry_dealloc);
        if (g_table_pool.by_ptr)
            g_table_pool.by_ptr->put(g_table_pool.by_ptr, (keytype)(uintptr_t)t, (valtype)(uintptr_t)entry, pooled_ptr_dealloc);
        TABLE_POOL_UNLOCK(&g_table_pool.lock);
        return t;
    } else {
        pt->refcount++;
        pt->last_used = time(NULL);
        struct flintdb_table *t = pt->table;
        TABLE_POOL_UNLOCK(&g_table_pool.lock);
        return t;
    }
EXCEPTION:
    TABLE_POOL_UNLOCK(&g_table_pool.lock);
    return NULL;
}
#else
// Open a table without pooling (FLINTDB_RDWR).
static struct flintdb_table * sql_exec_table_borrow(const char *file, char **e) {
    struct flintdb_table *t = flintdb_table_open(file, FLINTDB_RDWR, NULL, e);
    if (e && *e)
        return NULL;
    return t;
}

#endif// Free sql_result and associated resources


static void sql_result_close(struct flintdb_sql_result*me) {
    if (!me)
        return;

    if (me->row_cursor && me->row_cursor->close) {
        me->row_cursor->close(me->row_cursor);
    }
    if (me->column_names) {
        for (int i = 0; i < me->column_count; i++) {
            if (me->column_names[i]) {
                FREE(me->column_names[i]);
            }
        }
        FREE(me->column_names);
    }
    FREE(me);
}

static inline void sql_exec_indexable_where(const struct flintdb_meta *meta, struct flintdb_sql *q, char *out, size_t cap) {
    int has_where = !strempty(q->where);

    if (!strempty(q->index)) {
        snprintf(out, cap, "USE INDEX(%s)%s", q->index, has_where ? " " : "");
    } else if (meta && meta->indexes.length > 0) {
        int index = filter_best_index_get(q->where, q->orderby, (struct flintdb_meta *)meta, NULL);
        if (index != -1) 
            snprintf(out, cap, "USE INDEX(%s)%s", meta->indexes.a[index].name, has_where ? " " : "");
    }
    if (has_where)
        snprintf(out + strlen(out), cap - strlen(out), "WHERE %s", q->where);
    // printf("DEBUG: Combined WHERE clause: [%s]\n", out);
}

// SQL execution entry point
struct flintdb_sql_result * flintdb_sql_exec(const char *sql, const struct flintdb_transaction *transaction, char **e) {
    struct flintdb_sql *q = NULL;

    if (sql == NULL || strlen(sql) == 0)
        THROW(e, "SQL statement is empty");

    q = flintdb_sql_parse(sql, e);
    if (e && *e)
        THROW_S(e);

    // SHOW TABLES does not operate on a single table file; skip format detection
    if (strncasecmp(q->statement, "SHOW", 4) == 0 && strncasecmp(q->object, "TABLES", 6) == 0) {
        struct flintdb_sql_result*sr = sql_exec_show_tables(q, e);
        if (e && *e)
            THROW_S(e);
        // sql_context freed inside sql_exec_show_tables result close path? We free here after obtaining result
        flintdb_sql_free(q);
        return sr;
    }

    enum fileformat fmt = detect_file_format(q->table);
    if (FORMAT_UNKNOWN == fmt)
        THROW(e, "Unable to detect file format for table: %s", q->table);

    i64 affected = 0;
    if (transaction && !strempty(q->table) && fmt == FORMAT_BIN) { // only validate for table-based operations
        struct flintdb_table *table = sql_exec_table_borrow(q->table, e);
        if (!table) THROW_S(e);
        if (transaction->validate((struct flintdb_transaction *)transaction, table, e) != 1) 
            THROW(e, "Transaction is not valid for table: %s", q->table);
    }

    if (strncasecmp(q->statement, "SELECT", 6) == 0 && strempty(q->into)) {
        if (FORMAT_BIN == fmt)
            return sql_exec_select(q, (struct flintdb_transaction *)transaction, e);
        return sql_exec_gf_select(q, e);
    } else if (strncasecmp(q->statement, "SELECT", 6) == 0 && !strempty(q->into)) {
        affected = sql_exec_select_into(q, (struct flintdb_transaction *)transaction, e);
    } else if ((strncasecmp(q->statement, "INSERT", 6) == 0 || strncasecmp(q->statement, "REPLACE", 7) == 0) && strempty(q->from)) {
        if (FORMAT_BIN != fmt)
            THROW(e, "INSERT operation not supported for read-only file formats, %s", q->table);
        affected = sql_exec_insert(q, (struct flintdb_transaction *)transaction, e);
    } else if ((strncasecmp(q->statement, "INSERT", 6) == 0 || strncasecmp(q->statement, "REPLACE", 7) == 0) && !strempty(q->from)) {
        affected = sql_exec_insert_from(q, (struct flintdb_transaction *)transaction, e);
    } else if (strncasecmp(q->statement, "UPDATE", 6) == 0) {
        if (FORMAT_BIN != fmt)
            THROW(e, "UPDATE operation not supported for read-only file formats, %s", q->table);
        affected = sql_exec_update(q, (struct flintdb_transaction *)transaction, e);
    } else if (strncasecmp(q->statement, "DELETE", 6) == 0) {
        if (FORMAT_BIN != fmt)
            THROW(e, "DELETE operation not supported for read-only file formats, %s", q->table);
        affected = sql_exec_delete(q, (struct flintdb_transaction *)transaction, e);
    } else if (strncasecmp(q->statement, "CREATE", 6) == 0) {
        affected = sql_exec_create(q, e);
    } else if (strncasecmp(q->statement, "ALTER", 5) == 0) {
        if (FORMAT_BIN != fmt)
            THROW(e, "ALTER operation not supported for read-only file formats, %s", q->table);
        affected = sql_exec_alter(q, e);
    } else if (strncasecmp(q->statement, "DROP", 4) == 0) {
        affected = sql_exec_drop(q, e);
    } else if (strncasecmp(q->statement, "DESCRIBE", 8) == 0 || strncasecmp(q->statement, "DESC", 4) == 0) {
        return sql_exec_describe(q, e);
    } else if (strncasecmp(q->statement, "META", 4) == 0) {
        return sql_exec_meta(q, e);
    } else if (strncasecmp(q->statement, "BEGIN", 17) == 0) {
        return sql_exec_begin_transaction(q, (struct flintdb_transaction *)transaction, e);
    } else if (strncasecmp(q->statement, "COMMIT", 6) == 0) {
        return sql_exec_commit_transaction(q, (struct flintdb_transaction *)transaction, e);
    } else if (strncasecmp(q->statement, "ROLLBACK", 8) == 0) {
        return sql_exec_rollback_transaction(q, (struct flintdb_transaction *)transaction, e);
    } else {
        THROW(e, "Unsupported SQL statement: %s", q->statement);
    }

    flintdb_sql_free(q);

    struct flintdb_sql_result*result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    result->affected = affected;
    result->close = sql_result_close;
    return result;

EXCEPTION:
    if (q)
        flintdb_sql_free(q);
    return NULL;
}

static int sql_exec_insert(struct flintdb_sql *q, struct flintdb_transaction *t, char **e) {
    struct flintdb_table *table = NULL;
    struct flintdb_row *r = NULL;
    i8 upsert = strncasecmp(q->statement, "REPLACE", 7) == 0 ? 1 : 0; // 0=insert only, 1=insert or update

    table = sql_exec_table_borrow(q->table, e);
    if (!table) THROW_S(e);

    const struct flintdb_meta *m = table->meta(table, e);
    if (e && *e) THROW_S(e);

    // Support two modes:
    // 1. INSERT INTO table (col1, col2, ...) VALUES (val1, val2, ...)
    // 2. INSERT INTO table VALUES (val1, val2, ...) - values for all columns in order
    
    if (q->columns.length == 0) {
        // Mode 2: No columns specified, values must match all table columns in order
        if (q->values.length != m->columns.length) {
            THROW(e, "Number of values (%d) does not match number of table columns (%d)", 
                  q->values.length, m->columns.length);
        }
        
        r = flintdb_row_new((struct flintdb_meta *)m, e);
        if (e && *e) THROW_S(e);
        
        // Set values in order for all columns
        for (int i = 0; i < m->columns.length; i++) {
            const char *col_value = q->values.value[i];
            struct flintdb_variant v;
            flintdb_variant_init(&v);
            flintdb_variant_string_set(&v, col_value, strlen(col_value));
            r->set(r, i, &v, e);
            flintdb_variant_free(&v);
            if (e && *e) THROW_S(e);
        }
    } else {
        // Mode 1: Columns specified, map values to specified columns
        if (q->values.length != q->columns.length) {
            THROW(e, "Number of values (%d) does not match number of columns (%d)", 
                  q->values.length, q->columns.length);
        }
        
        r = flintdb_row_new((struct flintdb_meta *)m, e);
        if (e && *e) THROW_S(e);
        
        for (int i = 0; i < q->columns.length; i++) {
            const char *col_name = q->columns.name[i];
            const char *col_value = q->values.value[i];
            int col_index = flintdb_column_at((struct flintdb_meta *)m, col_name);
            if (col_index == -1) THROW(e, "Column not found: %s", col_name);

            struct flintdb_variant v;
            flintdb_variant_init(&v);
            flintdb_variant_string_set(&v, col_value, strlen(col_value));
            r->set(r, col_index, &v, e);
            flintdb_variant_free(&v);
            if (e && *e) THROW_S(e);
        }
    }

    i64 rowid = table->apply(table, r, upsert, e);
    if (e && *e) THROW_S(e);
    if (rowid < 0) THROW(e, "Failed to insert row");

    if (r) r->free(r);
    if (table) table->close(table);
    return 1;

EXCEPTION:
    if (r) r->free(r);
    if (table) table->close(table);
    return -1;
}

static int sql_exec_update(struct flintdb_sql *q, struct flintdb_transaction *t, char **e) {
    struct flintdb_table *table = NULL;
    struct flintdb_cursor_i64 *cursor = NULL;
    struct flintdb_row *r = NULL;
    int affected = 0;

    if (strempty(q->where))
        THROW(e, "UPDATE operation requires a WHERE clause to prevent full table updates");
    if (q->values.length == 0 || q->columns.length == 0)
        THROW(e, "No columns/values specified for UPDATE operation");
    if (q->columns.length != q->values.length)
        THROW(e, "Number of columns (%d) does not match number of values (%d)", q->columns.length, q->values.length);

    table = sql_exec_table_borrow(q->table, e);
    if (e && *e) THROW_S(e);

    const struct flintdb_meta *meta = table->meta(table, e);
    if (e && *e) THROW_S(e);

    char where[SQL_STRING_LIMIT] = {0};
    sql_exec_indexable_where(meta, q, where, sizeof(where));
    cursor = table->find(table, where, e);
    if (e && *e) THROW_S(e);
    if (!cursor) THROW(e, "No rows found matching WHERE clause");

    const struct flintdb_meta *m = table->meta(table, e);
    if (e && *e) THROW_S(e);

    for (i64 i; (i = cursor->next(cursor, e)) > -1;) {
        if (e && *e) THROW_S(e);
        const struct flintdb_row *found = table->read(table, i, e);
        if (e && *e) THROW_S(e);

        r = found->copy(found, e);
        if (e && *e)
            THROW_S(e);

        // Update specified columns with new values
        for (int j = 0; j < q->columns.length; j++) {
            const char *col_name = q->columns.name[j];
            const char *col_value = q->values.value[j];
            int col_index = flintdb_column_at((struct flintdb_meta *)m, col_name);
            if (col_index == -1)
                THROW(e, "Column not found: %s", col_name);

            struct flintdb_variant v;
            flintdb_variant_init(&v);
            flintdb_variant_string_set(&v, col_value, strlen(col_value));
            r->set(r, col_index, &v, e);
            flintdb_variant_free(&v);
            if (e && *e)
                THROW_S(e);
        }

        table->apply_at(table, i, r, e);
        if (e && *e) THROW_S(e);
        r->free(r);
        r = NULL;
        affected++;
    }

    if (r) r->free(r);
    if (cursor) cursor->close(cursor);
    if (table) table->close(table);
    return affected;

EXCEPTION:
    if (r) r->free(r);
    if (cursor) cursor->close(cursor);
    if (table) table->close(table);
    return -1;
}

static int sql_exec_delete(struct flintdb_sql *q, struct flintdb_transaction *t, char **e) {
    struct flintdb_table *table = NULL;
    struct flintdb_cursor_i64 *cursor = NULL;
    int affected = 0;

    if (strempty(q->where))
        THROW(e, "DELETE operation requires a WHERE clause to prevent full table deletions");

    table = sql_exec_table_borrow(q->table, e);
    if (e && *e) THROW_S(e);

    const struct flintdb_meta *meta = table->meta(table, e);
    if (e && *e) THROW_S(e);

    char where[SQL_STRING_LIMIT] = {0};
    sql_exec_indexable_where(meta, q, where, sizeof(where));
    cursor = table->find(table, where, e);
    if (e && *e)
        THROW_S(e);
    if (!cursor)
        THROW(e, "No rows found matching WHERE clause");

    for (i64 i; (i = cursor->next(cursor, e)) > -1;) {
        if (e && *e) THROW_S(e);
        table->delete_at(table, i, e);
        if (e && *e) THROW_S(e);
        affected++;
    }

    if (cursor) cursor->close(cursor);
    if (table) table->close(table);
    return affected;

EXCEPTION:
    if (cursor) cursor->close(cursor);
    if (table) table->close(table);
    return -1;
}

static int sql_exec_insert_from(struct flintdb_sql *q, struct flintdb_transaction *t, char **e) {
    int affected = 0;
    struct flintdb_meta meta = { .priv = NULL, };
    struct flintdb_table *table = NULL;
    struct flintdb_genericfile *gf = NULL;
    struct flintdb_sql_result*src_result = NULL;
    int *col_mapping = NULL; // array to map source column indices to target column indices
    i8 upsert = strncasecmp(q->statement, "REPLACE", 7) == 0 ? 1 : 0; // 0=insert only, 1=insert or update

    char target[PATH_MAX] = {0};
    char from[PATH_MAX] = {0};
    strncpy(target, q->table, sizeof(target) - 1);
    strncpy(from, q->from, sizeof(from) - 1);

    enum fileformat fmt = detect_file_format(target);

    if (file_exists(from) == 0) 
        THROW(e, "Source file for INSERT ... FROM does not exist: %s", from);
    if (file_exists(target) && FORMAT_BIN != fmt) 
        THROW(e, "INSERT ... FROM operation not supported for read-only file formats, %s", target);

    char desc[PATH_MAX] = {0};
    snprintf(desc, sizeof(desc), "%s%s", target, META_NAME_SUFFIX);
    meta = flintdb_meta_open(desc, e);
    if (e && *e) THROW_S(e);

    if (meta.columns.length == 0)
        THROW(e, "No columns found in metadata for target table: %s", desc);
    if (meta.indexes.length == 0 && FORMAT_BIN == fmt)
        THROW(e, "INSERT ... FROM does not support target tables with indexes yet: %s", desc);

    // Build column mapping and SELECT statement
    char expr[SQL_STRING_LIMIT] = {0};
    if (q->columns.length > 0) {
        col_mapping = (int *)CALLOC(q->columns.length, sizeof(int));
        if (!col_mapping) 
            THROW(e, "Out of memory");
        
        // Map each specified target column to its index in the target meta
        for (int i = 0; i < q->columns.length; i++) {
            int target_idx = flintdb_column_at((struct flintdb_meta *)&meta, q->columns.name[i]);
            if (target_idx < 0) 
                THROW(e, "Column not found in target table: %s", q->columns.name[i]);
            col_mapping[i] = target_idx;
        }
        
        // Build SELECT with only the specified columns from source
        int offset = snprintf(expr, sizeof(expr), "SELECT ");
        for (int i = 0; i < q->columns.length; i++) {
            if (i > 0)
                offset += snprintf(expr + offset, sizeof(expr) - offset, ", ");
            offset += snprintf(expr + offset, sizeof(expr) - offset, "%s", q->columns.name[i]);
        }
        snprintf(expr + offset, sizeof(expr) - offset, " FROM %s", from);
    } else {
        // No columns specified - select all
        snprintf(expr, sizeof(expr), "SELECT * FROM %s", from);
    }

    if (!strempty(q->where)) {
        // Append WHERE clause if specified
        strncat(expr, " WHERE ", sizeof(expr) - strlen(expr) - 1);
        strncat(expr, q->where, sizeof(expr) - strlen(expr) - 1);
    }
    if (!strempty(q->orderby)) {
        // Append ORDER BY clause if specified
        strncat(expr, " ORDER BY ", sizeof(expr) - strlen(expr) - 1);
        strncat(expr, q->orderby, sizeof(expr) - strlen(expr) - 1);
    }
    if (!strempty(q->limit)) {
        // Append LIMIT clause if specified
        strncat(expr, " LIMIT ", sizeof(expr) - strlen(expr) - 1);
        strncat(expr, q->limit, sizeof(expr) - strlen(expr) - 1);
    }
    
    DEBUG("SELECT for INSERT ... FROM: %s\n", expr);
    src_result = flintdb_sql_exec(expr, NULL, e);
    if (e && *e) THROW_S(e);
    if (src_result == NULL || src_result->row_cursor == NULL)
        THROW(e, "Failed to read source data from file: %s", from);

    // When columns are specified, the source SELECT already projected the right columns
    // No need to check column count mismatch - it's already matched by the SELECT projection

    if (fmt == FORMAT_BIN) {
        table = sql_exec_table_borrow(target, e);
        if (e && *e) THROW_S(e);

        // Pre-allocate a reusable row for better performance
        struct flintdb_row *reuse_row = col_mapping ? NULL : flintdb_row_new((struct flintdb_meta *)&meta, e);
        if (!col_mapping && (e && *e)) THROW_S(e);

        for(struct flintdb_row *r; (r = src_result->row_cursor->next(src_result->row_cursor, e)) != NULL;) {
            if (e && *e) THROW_S(e);
            
            struct flintdb_row *copy = NULL;
            if (col_mapping) {
                // Map selected columns from source to target positions
                copy = flintdb_row_new((struct flintdb_meta *)&meta, e);
                if (e && *e) {
                    r->free(r);
                    THROW_S(e);
                }
                
                for (int i = 0; i < q->columns.length; i++) {
                    struct flintdb_variant *v = r->get(r, i, e);
                    if (e && *e) {
                        r->free(r);
                        copy->free(copy);
                        THROW_S(e);
                    }
                    copy->set(copy, col_mapping[i], v, e);
                    if (e && *e) {
                        r->free(r);
                        copy->free(copy);
                        THROW_S(e);
                    }
                }
            } else {
                // Reuse row: cast into pre-allocated row
                flintdb_row_cast_reuse(r, reuse_row, e);
                if (e && *e) {
                    r->free(r);
                    if (reuse_row) reuse_row->free(reuse_row);
                    THROW_S(e);
                }
                copy = reuse_row;
            }
            
            i64 rowid = table->apply(table, copy, upsert, e);
            r->free(r); // Release source row after use
            if (col_mapping && copy) copy->free(copy);
            if (e && *e) {
                if (reuse_row) reuse_row->free(reuse_row);
                THROW_S(e);
            }
            if (rowid < 0) {
                if (reuse_row) reuse_row->free(reuse_row);
                THROW(e, "Failed to insert row into target table: %s", target);
            }
            affected++;
        }
        
        if (reuse_row) reuse_row->free(reuse_row);
    } else {
        gf = flintdb_genericfile_open(target, FLINTDB_RDWR, &meta, e);
        if (e && *e) THROW_S(e);

        // Pre-allocate a reusable row for better performance
        struct flintdb_row *reuse_row = col_mapping ? NULL : flintdb_row_new((struct flintdb_meta *)&meta, e);
        if (!col_mapping && (e && *e)) THROW_S(e);

        for(struct flintdb_row *r; (r = src_result->row_cursor->next(src_result->row_cursor, e)) != NULL;) {
            if (e && *e) THROW_S(e);
     
            struct flintdb_row *copy = NULL;
            if (col_mapping) {
                // Map selected columns from source to target positions
                copy = flintdb_row_new((struct flintdb_meta *)&meta, e);
                if (e && *e) {
                    r->free(r);
                    THROW_S(e);
                }
                
                for (int i = 0; i < q->columns.length; i++) {
                    struct flintdb_variant *v = r->get(r, i, e);
                    if (e && *e) {
                        r->free(r);
                        copy->free(copy);
                        THROW_S(e);
                    }
                    copy->set(copy, col_mapping[i], v, e);
                    if (e && *e) {
                        r->free(r);
                        copy->free(copy);
                        THROW_S(e);
                    }
                }
            } else {
                // Reuse row: cast into pre-allocated row
                flintdb_row_cast_reuse(r, reuse_row, e);
                if (e && *e) {
                    r->free(r);
                    if (reuse_row) reuse_row->free(reuse_row);
                    THROW_S(e);
                }
                copy = reuse_row;
            }
            
            i64 ok = gf->write(gf, copy, e);
            r->free(r); // Release source row after use
            if (col_mapping && copy) copy->free(copy);
            if (e && *e) {
                if (reuse_row) reuse_row->free(reuse_row);
                THROW_S(e);
            }
            if (ok < 0) {
                if (reuse_row) reuse_row->free(reuse_row);
                THROW(e, "Failed to insert row into target generic file: %s", target);
            }
            affected++;
        }
        
        if (reuse_row) reuse_row->free(reuse_row);
    }

// Cleanup and return
    
    if (col_mapping) FREE(col_mapping);
    if (src_result) src_result->close(src_result);
    if (gf) gf->close(gf);
    if (table) table->close(table);    
    flintdb_meta_close(&meta);
    return affected;
EXCEPTION:
    if (col_mapping) FREE(col_mapping);
    if (src_result) src_result->close(src_result);
    if (gf) gf->close(gf);
    if (table) table->close(table);    
    flintdb_meta_close(&meta);
    return -1;
}

static int sql_exec_select_into(struct flintdb_sql *q, struct flintdb_transaction *t, char **e) {
    // TODO: Implement SELECT ... INTO functionality
    THROW(e, "SELECT ... INTO not yet implemented, use INSERT ... FROM instead");
EXCEPTION:
    return -1;
}

static inline int is_valid_tablepath(const char *path) {
    if (!path || !*path)
        return 0;
    const char *p = path;
    while (*p) {
        char c = *p;
        if (c == ' ' || c == '\n' || c == '\t' || c == '\r')
            return 0;
        if (!(isalnum((unsigned char)c) || c == '_' || c == '-' || c == '.' || c == '/' || c == '\\'))
            return 0;
        p++;
    }
    return 1;
}

static int sql_exec_create(struct flintdb_sql *q, char **e) {
    const char *path = q->table;
    struct flintdb_meta meta = { .priv = NULL, };
    struct flintdb_table *table = NULL;

    if (strempty(path)) THROW(e, "Table name is required for CREATE operation");
    if (!is_valid_tablepath(path)) THROW(e, "Invalid characters in table name: %s", path);
    if (file_exists(path)) THROW(e, "Table file already exists: %s", path);
    
    // check tsv/csv/parquet extension to create genericfile, otherwise create binary table
    enum fileformat fmt = detect_file_format(path);
    if (FORMAT_BIN != fmt) THROW(e, "CREATE operation not yet supported for non-binary file formats, %s", path);

    flintdb_sql_to_meta(q, &meta, e);
    if (e && *e) THROW_S(e);

    table = flintdb_table_open(path, FLINTDB_RDWR, &meta, e);
    if (e && *e) THROW_S(e);

    i64 bytes = table->bytes(table, e); // force write header
    if (e && *e) THROW_S(e);
    if (bytes <= 0) THROW(e, "Failed to create table file: %s", path);

//
    table->close(table);
    flintdb_meta_close(&meta);
    return 1;

EXCEPTION:
    flintdb_meta_close(&meta);
    if (table)
        table->close(table);
    return -1;
}

static int sql_exec_drop(struct flintdb_sql *q, char **e) {
    enum fileformat fmt = detect_file_format(q->table);

    if (FORMAT_BIN == fmt) 
        flintdb_table_drop(q->table, e);
     else
        flintdb_genericfile_drop(q->table, e);

    if (e && *e) THROW_S(e);

    return 1;
EXCEPTION :
    return -1;
}

struct flintdb_cursor_array_priv {
    struct list *rows; // arraylist of struct flintdb_row*
    int index;         // current iteration index
    struct flintdb_meta *meta; // meta for the synthetic rows (Column,Type,Key,Default)
};

static void list_row_dealloc(valtype v) {
    struct flintdb_row *r = (struct flintdb_row *)(uintptr_t)v;
    if (r)
        r->free(r);
}

static struct flintdb_row *array_cursor_next(struct flintdb_cursor_row *c, char **e) {
    if (!c || !c->p)
        return NULL;
    struct flintdb_cursor_array_priv *p = (struct flintdb_cursor_array_priv *)c->p;
    if (!p->rows)
        return NULL;
    if (p->index >= p->rows->count(p->rows))
        return NULL;
    struct flintdb_row *r = (struct flintdb_row *)(uintptr_t)p->rows->get(p->rows, p->index, e);
    if (e && *e)
        return NULL;
    p->index++;
    return r;
}

static void array_cursor_close(struct flintdb_cursor_row *c) {
    if (!c)
        return;
    struct flintdb_cursor_array_priv *p = (struct flintdb_cursor_array_priv *)c->p;
    if (p) {
        if (p->rows) {
            p->rows->free(p->rows); // will free rows via dealloc
            p->rows = NULL;
        }
        if (p->meta) {
            // meta has no owned internals here, but call meta_close just in case
            flintdb_meta_close(p->meta);
            FREE(p->meta);
            p->meta = NULL;
        }
        FREE(p);
    }
    FREE(c);
}

static struct flintdb_sql_result * sql_exec_describe(struct flintdb_sql *q, char **e) {
    struct flintdb_sql_result*result = NULL;
    struct flintdb_cursor_array_priv *priv = NULL;
    struct flintdb_cursor_row *c = NULL;
    struct flintdb_table *table = NULL;
    struct flintdb_genericfile *gf = NULL;
    const struct flintdb_meta *m = NULL;

    // Try to open as table first, then genericfile
    enum fileformat fmt = detect_file_format(q->table);
    if (fmt == FORMAT_BIN) {
        table = sql_exec_table_borrow(q->table, e);
        if (e && *e)
            THROW_S(e);
        m = table->meta(table, e);
    } else {
        gf = flintdb_genericfile_open(q->table, FLINTDB_RDONLY, NULL, e);
        if (e && *e) THROW_S(e);
        m = gf->meta(gf, e);
    }
    if (e && *e) THROW_S(e);
    if (!m) THROW(e, "Failed to read metadata");

    // Build primary key column map
    int pk_columns[MAX_COLUMNS_LIMIT] = {0};
    if (m->indexes.length > 0) {
        for (int i = 0; i < m->indexes.a[0].keys.length; i++) {
            int col_idx = flintdb_column_at((struct flintdb_meta *)m, m->indexes.a[0].keys.a[i]);
            if (col_idx >= 0)
                pk_columns[col_idx] = 1;
        }
    }

    // Prepare synthetic meta for describe output: Column, Type, Key, Default
    struct flintdb_meta *dm = CALLOC(1, sizeof(struct flintdb_meta));
    if (!dm) THROW(e, "Out of memory");
    *dm = flintdb_meta_new("describe", e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "Column", VARIANT_STRING, 256, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "Type", VARIANT_STRING, 64, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "Key", VARIANT_STRING, 8, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "Default", VARIANT_STRING, 256, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);

    // Build rows into an array-backed cursor
    priv = (struct flintdb_cursor_array_priv *)CALLOC(1, sizeof(struct flintdb_cursor_array_priv));
    if (!priv) THROW(e, "Out of memory");
    priv->rows = arraylist_new(m->columns.length);
    if (!priv->rows) THROW(e, "Out of memory");
    priv->index = 0;
    priv->meta = dm;

    for (int i = 0; i < m->columns.length; i++) {
        // Create one output row
        struct flintdb_row *r = flintdb_row_new(dm, e);
        if (e && *e) THROW_S(e);

        // Column name
        r->string_set(r, 0, m->columns.a[i].name, e);
        if (e && *e) THROW_S(e);

        // Type formatted with size/precision
        char type_str[256] = {0};
        const char *type_name = flintdb_variant_type_name(m->columns.a[i].type);
        if (m->columns.a[i].type == VARIANT_DECIMAL) {
            snprintf(type_str, sizeof(type_str), "%s(%d,%d)", type_name, m->columns.a[i].bytes, m->columns.a[i].precision);
        } else if (m->columns.a[i].type == VARIANT_STRING || m->columns.a[i].type == VARIANT_BYTES) {
            snprintf(type_str, sizeof(type_str), "%s(%d)", type_name, m->columns.a[i].bytes);
        } else {
            snprintf(type_str, sizeof(type_str), "%s", type_name);
        }
        r->string_set(r, 1, type_str, e);
        if (e && *e) THROW_S(e);

        // Key: PRI or empty
        r->string_set(r, 2, pk_columns[i] ? "PRI" : "", e);
        if (e && *e) THROW_S(e);

        // Default value (may be empty string)
        r->string_set(r, 3, m->columns.a[i].value[0] ? m->columns.a[i].value : "", e);
        if (e && *e) THROW_S(e);

        // Add to list with proper deallocator
        priv->rows->add(priv->rows, (valtype)(uintptr_t)r, list_row_dealloc, e);
        if (e && *e) THROW_S(e);
    }

    // Build cursor
    c = (struct flintdb_cursor_row *)CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!c) THROW(e, "Out of memory");
    c->p = priv;
    c->next = array_cursor_next;
    c->close = array_cursor_close;

    // Build result
    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");
    result->row_cursor = c;
    result->column_count = 4;
    result->column_names = (char **)CALLOC(4, sizeof(char *));
    if (!result->column_names) THROW(e, "Out of memory");
    result->column_names[0] = STRDUP("Column");
    result->column_names[1] = STRDUP("Type");
    result->column_names[2] = STRDUP("Key");
    result->column_names[3] = STRDUP("Default");
    result->affected = -1;
    result->close = sql_result_close;

    if (table) table->close(table);
    if (gf) gf->close(gf);
    return result;

EXCEPTION:
    if (c) FREE(c);
    if (priv) {
        if (priv->rows)
            priv->rows->free(priv->rows);
        if (priv->meta) {
            flintdb_meta_close(priv->meta);
            FREE(priv->meta);
        }
        FREE(priv);
    }
    if (table) table->close(table);
    if (gf) gf->close(gf);
    return NULL;
}

static struct flintdb_sql_result * sql_exec_meta(struct flintdb_sql *q, char **e) {
    struct flintdb_sql_result*result = NULL;
    struct flintdb_cursor_array_priv *priv = NULL;
    struct flintdb_cursor_row *c = NULL;
    struct flintdb_table *table = NULL;
    struct flintdb_genericfile *gf = NULL;
    const struct flintdb_meta *m = NULL;

    // Accept both binary tables and generic file formats
    enum fileformat fmt = detect_file_format(q->table);
    if (fmt == FORMAT_BIN) {
        table = sql_exec_table_borrow(q->table, e);
        if (e && *e) THROW_S(e);
        m = table->meta(table, e);
    } else {
        gf = flintdb_genericfile_open(q->table, FLINTDB_RDONLY, NULL, e);
        if (e && *e) THROW_S(e);
        m = gf->meta(gf, e);
    }
    if (e && *e) THROW_S(e);
    if (!m) THROW(e, "Failed to read metadata");

    // Prepare synthetic meta for META output: single column "SQL"
    struct flintdb_meta *dm = (struct flintdb_meta *)CALLOC(1, sizeof(struct flintdb_meta));
    if (!dm) THROW(e, "Out of memory");
    *dm = flintdb_meta_new("meta", e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "SQL", VARIANT_STRING, SQL_STRING_LIMIT, 0, 0, NULL, NULL, e);
    if (e && *e) THROW_S(e);

    // Build one-row result with the SQL stringified meta
    priv = (struct flintdb_cursor_array_priv *)CALLOC(1, sizeof(struct flintdb_cursor_array_priv));
    if (!priv) THROW(e, "Out of memory");
    priv->rows = arraylist_new(1);
    if (!priv->rows) THROW(e, "Out of memory");
    priv->index = 0;
    priv->meta = dm;

    struct flintdb_row *r = flintdb_row_new(dm, e);
    if (e && *e) THROW_S(e);

    char sqlbuf[SQL_STRING_LIMIT] = {0};
    if (flintdb_meta_to_sql_string(m, sqlbuf, sizeof(sqlbuf), e) < 0) THROW_S(e);
    r->string_set(r, 0, sqlbuf, e);
    if (e && *e) THROW_S(e);

    priv->rows->add(priv->rows, (valtype)(uintptr_t)r, list_row_dealloc, e);
    if (e && *e) THROW_S(e);

    // Build cursor
    c = (struct flintdb_cursor_row *)CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!c) THROW(e, "Out of memory");
    c->p = priv;
    c->next = array_cursor_next;
    c->close = array_cursor_close;

    // Build result
    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");
    result->row_cursor = c;
    result->column_count = 1;
    result->column_names = (char **)CALLOC(1, sizeof(char *));
    if (!result->column_names) THROW(e, "Out of memory");
    result->column_names[0] = STRDUP("SQL");
    result->affected = -1;
    result->close = sql_result_close;

    if (table) table->close(table);
    if (gf) gf->close(gf);
    return result;

EXCEPTION:
    if (c) FREE(c);
    if (priv) {
        if (priv->rows)
            priv->rows->free(priv->rows);
        if (priv->meta) {
            flintdb_meta_close(priv->meta);
            FREE(priv->meta);
        }
        FREE(priv);
    }
    if (table) table->close(table);
    if (gf) gf->close(gf);
    return NULL;
}

static struct flintdb_sql_result * sql_exec_show_tables(struct flintdb_sql *q, char **e) {
    struct flintdb_sql_result*result = NULL;
    struct flintdb_cursor_array_priv *priv = NULL;
    struct flintdb_cursor_row *c = NULL;
    struct dir_stack_entry *stack = NULL; // initialize early to avoid uninitialized warnings in cleanup

    // Determine directory to scan
    char base_dir[PATH_MAX] = {0};
    if (strempty(q->where)) {
        strncpy(base_dir, ".", sizeof(base_dir) - 1);
    } else {
        strncpy(base_dir, q->where, sizeof(base_dir) - 1);
    }

    // Validate directory
    struct stat st;
    if (stat(base_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        THROW(e, "SHOW TABLES directory not found: %s", base_dir);
    }

    int recursive = (!strempty(q->option) && strcasecmp(q->option, "-R") == 0) ? 1 : 0;

    // Prepare synthetic meta: Table, Format, Rows, Bytes, Modified, Path
    struct flintdb_meta *dm = (struct flintdb_meta *)CALLOC(1, sizeof(struct flintdb_meta));
    if (!dm) THROW(e, "Out of memory");
    *dm = flintdb_meta_new("show_tables", e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "Table", VARIANT_STRING, 256, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "Format", VARIANT_STRING, 32, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "Rows", VARIANT_STRING, 32, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "Bytes", VARIANT_STRING, 32, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "Modified", VARIANT_STRING, 64, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, "Path", VARIANT_STRING, 512, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);

    priv = (struct flintdb_cursor_array_priv *)CALLOC(1, sizeof(struct flintdb_cursor_array_priv));
    if (!priv) THROW(e, "Out of memory");
    priv->rows = arraylist_new(128);
    if (!priv->rows) THROW(e, "Out of memory");
    priv->index = 0;
    priv->meta = dm;

    // Recursive directory traversal using manual stack (array of paths)
    struct dir_stack_entry {
        char path[PATH_MAX];
    };
    int stack_cap = 32;
    int stack_len = 0;
    stack = (struct dir_stack_entry *)CALLOC(stack_cap, sizeof(struct dir_stack_entry));
    if (!stack) THROW(e, "Out of memory");
    strncpy(stack[stack_len++].path, base_dir, PATH_MAX - 1);

    int founds = 0;

    while (stack_len > 0) {
        // Pop
        char current[PATH_MAX];
        strncpy(current, stack[--stack_len].path, sizeof(current) - 1);
        current[sizeof(current) - 1] = '\0';

        DIR *d = opendir(current);
        if (!d) {
            // Skip unreadable directory
            continue;
        }
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (de->d_name[0] == '.')
                continue; // skip hidden and . / ..
            char full[PATH_MAX];
            snprintf(full, sizeof(full), "%s/%s", current, de->d_name);
            struct stat fst;
            if (stat(full, &fst) != 0)
                continue;
            int is_dir = S_ISDIR(fst.st_mode);
            if (is_dir) {
                if (recursive) {
                    if (stack_len >= stack_cap) {
                        int new_cap = stack_cap * 2;
                        struct dir_stack_entry *tmp = (struct dir_stack_entry *)REALLOC(stack, new_cap * sizeof(struct dir_stack_entry));
                        if (!tmp) {
                            closedir(d);
                            THROW(e, "Out of memory");
                        }
                        stack = tmp;
                        stack_cap = new_cap;
                    }
                    strncpy(stack[stack_len++].path, full, PATH_MAX - 1);
                }
                continue; // directories are traversed, not listed
            }

            // Skip meta descriptor files
            size_t name_len = strlen(de->d_name);
            if (name_len >= 5 && strcmp(de->d_name + (name_len - 5), META_NAME_SUFFIX) == 0)
                continue;

            // Determine format
            enum fileformat fmt = detect_file_format(de->d_name);
            char fmt_str[32] = {0};
            if (fmt == FORMAT_BIN) {
                // Binary table; verify it has an index in meta
                char desc_path[PATH_MAX] = {0};
                snprintf(desc_path, sizeof(desc_path), "%s%s", full, META_NAME_SUFFIX);
                if (access(desc_path, F_OK) != 0)
                    continue; // no .desc -> not a table
                char *err_local = NULL;
                struct flintdb_meta m = flintdb_meta_open(desc_path, &err_local);
                if (err_local) { /* ignore errors */
                    err_local = NULL;
                    continue;
                }
                if (m.indexes.length <= 0) {
                    flintdb_meta_close(&m);
                    continue;
                }
                strncpy(fmt_str, "table", sizeof(fmt_str) - 1);
                // Acquire rows/bytes via table API
                char *terr = NULL;
                struct flintdb_table *t = flintdb_table_open(full, FLINTDB_RDONLY, NULL, &terr);
                i64 rows = -1, bytes = -1;
                if (!terr && t) {
                    rows = t->rows(t, &terr);
                    bytes = t->bytes(t, &terr);
                }
                if (t)
                    t->close(t);
                flintdb_meta_close(&m);
                // Build row
                struct flintdb_row *r = flintdb_row_new(dm, e);
                if (e && *e) {
                    closedir(d);
                    THROW_S(e);
                }
                r->string_set(r, 0, de->d_name, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Table
                r->string_set(r, 1, fmt_str, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Format
                char rows_buf[64];
                snprintf(rows_buf, sizeof(rows_buf), "%lld", (long long)(rows >= 0 ? rows : 0));
                r->string_set(r, 2, rows_buf, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Rows
                char bytes_buf[64];
                bytes_human(bytes, bytes_buf, sizeof(bytes_buf));
                r->string_set(r, 3, bytes_buf, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Bytes
                char mod_buf[64];
                time_t secs = fst.st_mtime;
                struct tm tmv;
                localtime_r(&secs, &tmv);
                strftime(mod_buf, sizeof(mod_buf), "%Y-%m-%d %H:%M:%S", &tmv);
                r->string_set(r, 4, mod_buf, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Modified
                char rel_buf[PATH_MAX];
                relativize_path(full, rel_buf, sizeof(rel_buf));
                r->string_set(r, 5, rel_buf, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Path
                priv->rows->add(priv->rows, (valtype)(uintptr_t)r, list_row_dealloc, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                }
                founds++;
            } else if (fmt != FORMAT_UNKNOWN) {
                // Generic text/parquet/jsonl file
                if (fmt == FORMAT_PARQUET) {
                    strncpy(fmt_str, "parquet", sizeof(fmt_str) - 1);
                } else if (fmt == FORMAT_TSV) {
                    strncpy(fmt_str, "tsv", sizeof(fmt_str) - 1);
                } else if (fmt == FORMAT_CSV) {
                    strncpy(fmt_str, "csv", sizeof(fmt_str) - 1);
                } else {
                    strncpy(fmt_str, "unknown", sizeof(fmt_str) - 1);
                }

                i64 rows = -1;
                i64 bytes = file_length(full);
                if (fmt == FORMAT_PARQUET) {
                    char *gerr = NULL;
                    struct flintdb_genericfile *gf = flintdb_genericfile_open(full, FLINTDB_RDONLY, NULL, &gerr);
                    if (!gerr && gf) {
                        rows = gf->rows(gf, &gerr);
                        gf->close(gf);
                    }
                }

                struct flintdb_row *r = flintdb_row_new(dm, e);
                if (e && *e) {
                    closedir(d);
                    THROW_S(e);
                }
                r->string_set(r, 0, de->d_name, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Table
                r->string_set(r, 1, fmt_str, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Format
                char rows_buf[64];
                rows >= 0 ? snprintf(rows_buf, sizeof(rows_buf), "%lld", (long long)rows) : snprintf(rows_buf, sizeof(rows_buf), "");
                r->string_set(r, 2, rows_buf, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Rows (blank or number)
                char bytes_buf[64];
                bytes_human(bytes, bytes_buf, sizeof(bytes_buf));
                r->string_set(r, 3, bytes_buf, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Bytes
                char mod_buf[64];
                time_t secs = fst.st_mtime;
                struct tm tmv;
                localtime_r(&secs, &tmv);
                strftime(mod_buf, sizeof(mod_buf), "%Y-%m-%d %H:%M:%S", &tmv);
                r->string_set(r, 4, mod_buf, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Modified
                char rel_buf[PATH_MAX];
                relativize_path(full, rel_buf, sizeof(rel_buf));
                r->string_set(r, 5, rel_buf, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                } // Path
                priv->rows->add(priv->rows, (valtype)(uintptr_t)r, list_row_dealloc, e);
                if (e && *e) {
                    r->free(r);
                    closedir(d);
                    THROW_S(e);
                }
                founds++;
            }
        }
        closedir(d);
    }

    // Build cursor
    c = (struct flintdb_cursor_row *)CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!c) THROW(e, "Out of memory");
    c->p = priv;
    c->next = array_cursor_next;
    c->close = array_cursor_close;

    // Build result
    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");
    result->row_cursor = c;
    result->column_count = 6;
    result->column_names = (char **)CALLOC(result->column_count, sizeof(char *));
    if (!result->column_names) THROW(e, "Out of memory");
    result->column_names[0] = STRDUP("Table");
    result->column_names[1] = STRDUP("Format");
    result->column_names[2] = STRDUP("Rows");
    result->column_names[3] = STRDUP("Bytes");
    result->column_names[4] = STRDUP("Modified");
    result->column_names[5] = STRDUP("Path");
    result->affected = founds;
    result->close = sql_result_close;

    if (stack) FREE(stack);
    return result;

EXCEPTION:
    if (stack) FREE(stack);
    if (c) FREE(c);
    if (priv) {
        if (priv->rows)
            priv->rows->free(priv->rows);
        if (priv->meta) {
            flintdb_meta_close(priv->meta);
            FREE(priv->meta);
        }
        FREE(priv);
    }
    if (result) sql_result_close(result);
    return NULL;
}

static int sql_exec_alter(struct flintdb_sql *q, char **e) {
    // ALTER TABLE syntax would need to be parsed from q->statement
    // Altering table structure requires careful index rebuilding
    THROW(e, "ALTER TABLE not yet supported. Please modify .desc file manually and rebuild indexes");
EXCEPTION:
    return -1;
}

// DISTINCT Cursor wrapper
struct distinct_cursor_priv {
    struct flintdb_cursor_row *inner_cursor;
    struct roaringbitmap *seen_hashes;
    struct limit limit;
    int col_count;                                         // number of columns to hash for DISTINCT; 0 means use all columns
    char cols[SQL_COLUMNS_LIMIT][SQL_OBJECT_STRING_LIMIT]; // selected column names
};

// Build a stable 31-bit hash for selected columns of a row (Java String.hashCode compatible)
// - If col_count > 0: use only those columns in order
// - Else: use all columns of the row
static int distinct_row_hash31(const struct flintdb_row *r, const char cols[][SQL_OBJECT_STRING_LIMIT], int col_count) {
    if (!r)
        return 0;
    char *err = NULL;
    struct buffer *b = buffer_alloc(128);
    if (!b)
        return 0;

    if (col_count > 0) {
        for (int i = 0; i < col_count; i++) {
            if (i > 0)
                b->i8_put(b, '\x1F', &err); // unit separator
            int idx = flintdb_column_at((struct flintdb_meta *)r->meta, cols[i]);
            if (idx >= 0) {
                struct flintdb_variant *v = r->get((struct flintdb_row *)r, idx, &err);
                if (v) {
                    char val[256];
                    val[0] = '\0';
                    flintdb_variant_to_string(v, val, (u32)sizeof(val));
                    b->array_put(b, val, (u32)strlen(val), &err);
                }
            }
        }
    } else {
        // Use entire row
        for (int i = 0; i < r->length; i++) {
            if (i > 0)
                b->i8_put(b, '\x1F', &err);
            struct flintdb_variant *v = r->get((struct flintdb_row *)r, i, &err);
            if (v) {
                char val[256];
                val[0] = '\0';
                flintdb_variant_to_string(v, val, (u32)sizeof(val));
                b->array_put(b, val, (u32)strlen(val), &err);
            }
        }
    }
    // Null-terminate for Java hash helper
    b->i8_put(b, '\0', &err);
    int32_t h = hll_java_string_hashcode(b->array ? b->array : "");
    b->free(b);
    return (int)(h & 0x7FFFFFFF);
}

UNUSED_FN
static struct flintdb_row *distinct_cursor_next(struct flintdb_cursor_row *c, char **e) {
    if (!c || !c->p)
        return NULL;
    struct distinct_cursor_priv *priv = (struct distinct_cursor_priv *)c->p;
    if (!priv || !priv->inner_cursor)
        return NULL;

    // Align behavior with other cursors: check remains at call start
    if (!priv->limit.remains(&priv->limit))
        return NULL;

    for (;;) {
        struct flintdb_row *r = priv->inner_cursor->next(priv->inner_cursor, e);
        if (e && *e)
            return NULL;
        if (!r)
            return NULL; // EOF

        int h = distinct_row_hash31(r, priv->cols, priv->col_count);
        if (h < 0)
            h = -h; // ensure non-negative
        if (priv->seen_hashes && rbitmap_contains(priv->seen_hashes, h)) {
            // duplicate, discard and continue
            r->free(r);
            continue;
        }
        if (priv->seen_hashes)
            rbitmap_add(priv->seen_hashes, h);

        // Handle OFFSET at the DISTINCT-output level
        if (priv->limit.skip(&priv->limit)) {
            r->free(r);
            continue;
        }

        return r;
    }
}

UNUSED_FN
static void distinct_cursor_close(struct flintdb_cursor_row *c) {
    if (!c)
        return;
    struct distinct_cursor_priv *priv = (struct distinct_cursor_priv *)c->p;
    if (priv) {
        if (priv->inner_cursor && priv->inner_cursor->close)
            priv->inner_cursor->close(priv->inner_cursor);
        if (priv->seen_hashes)
            rbitmap_free(priv->seen_hashes);
        FREE(priv);
    }
    FREE(c);
}

static struct flintdb_cursor_row *distinct_cursor_wrap(struct flintdb_sql *q, struct flintdb_cursor_row *inner, struct limit limit, char **e) {
    if (!inner) THROW(e, "DISTINCT requires an input cursor");
    if (!q) THROW(e, "Invalid SQL context for DISTINCT");

    struct distinct_cursor_priv *priv = (struct distinct_cursor_priv *)CALLOC(1, sizeof(struct distinct_cursor_priv));
    if (!priv) THROW(e, "Out of memory");
    priv->inner_cursor = inner;
    priv->seen_hashes = rbitmap_new();
    priv->limit = limit;

    // Derive column list for DISTINCT: if '*' -> use all columns; else copy explicit names
    if (q->columns.length == 1 && strcmp(q->columns.name[0], "*") == 0) {
        priv->col_count = 0; // use entire row
    } else {
        priv->col_count = q->columns.length;
        if (priv->col_count > SQL_COLUMNS_LIMIT)
            priv->col_count = SQL_COLUMNS_LIMIT;
        for (int i = 0; i < priv->col_count; i++) {
            strncpy(priv->cols[i], q->columns.name[i], SQL_OBJECT_STRING_LIMIT - 1);
            priv->cols[i][SQL_OBJECT_STRING_LIMIT - 1] = '\0';
        }
    }

    struct flintdb_cursor_row *wrap = (struct flintdb_cursor_row *)CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!wrap)
        THROW(e, "Out of memory");
    wrap->p = priv;
    wrap->next = distinct_cursor_next;
    wrap->close = distinct_cursor_close;
    return wrap;

EXCEPTION:
    if (inner && inner->close)
        inner->close(inner);
    return NULL;
}

// Simple SELECT for generic files

struct gf_cursor_priv {
    struct flintdb_genericfile *gf;
    struct flintdb_cursor_row *inner_cursor;
    struct limit limit;
    // projection: length = number of SELECT expressions, values are source column indexes
    int proj_count;
    int proj_indexes[MAX_COLUMNS_LIMIT];
    // Reusable row for projection to avoid repeated allocations
    struct flintdb_row *proj_row;
    struct flintdb_meta *proj_meta;
};

static struct flintdb_row *gf_cursor_next(struct flintdb_cursor_row *c, char **e) {
    struct gf_cursor_priv *priv = (struct gf_cursor_priv *)c->p;
    if (!priv)
        return NULL;
    // Apply LIMIT if present
    if (!priv->limit.remains(&priv->limit))
        return NULL;
    while (priv->limit.skip(&priv->limit)) {
        struct flintdb_row *skipr = priv->inner_cursor->next(priv->inner_cursor, e);
        if (e && *e)
            return NULL;
        if (!skipr)
            return NULL; // reached EOF before offset consumed
        if (skipr) {
            skipr->free(skipr);
        }
    }
    struct flintdb_row *r = priv->inner_cursor->next(priv->inner_cursor, e);
    if (e && *e)
        return NULL;
    if (!r)
        return NULL;

    // If no projection is defined, return the row as-is (SELECT *)
    if (priv->proj_count <= 0) {
        return r;
    }

    // Build a projected row with only selected columns, in SELECT order
    struct flintdb_meta *m = (struct flintdb_meta *)r->meta;
    
    // Build projection metadata from source row metadata
    // We create a temporary metadata structure for the projected columns
    struct flintdb_meta proj_meta_temp;
    memset(&proj_meta_temp, 0, sizeof(proj_meta_temp));
    proj_meta_temp.columns.length = priv->proj_count;
    
    for (int i = 0; i < priv->proj_count; i++) {
        int src = priv->proj_indexes[i];
        if (src < 0 || src >= m->columns.length) {
            // invalid mapping; fail hard
            r->free(r);
            if (e) *e = "Invalid column index in projection";
            return NULL;
        }
        // Deep copy column definition to avoid dangling references
        proj_meta_temp.columns.a[i] = m->columns.a[src];
    }
    
    // Create a new projected row for each call
    // Cannot reuse row because caller may retain it for later use (e.g., pretty print)
    struct flintdb_row *proj_row = flintdb_row_new(&proj_meta_temp, e);
    if (e && *e) {
        r->free(r);
        return NULL;
    }
    if (!proj_row) {
        r->free(r);
        return NULL;
    }
    
    // Copy projected columns from source row
    for (int i = 0; i < priv->proj_count; i++) {
        int src = priv->proj_indexes[i];
        struct flintdb_variant *v = r->get(r, src, e);
        if (e && *e) {
            r->free(r);
            proj_row->free(proj_row);
            return NULL;
        }
        proj_row->set(proj_row, i, v, e);
        if (e && *e) {
            r->free(r);
            proj_row->free(proj_row);
            return NULL;
        }
    }
    proj_row->rowid = r->rowid;
    r->free(r);
    
    return proj_row;
}

static void gf_cursor_close(struct flintdb_cursor_row *c) {
    if (!c)
        return;
    struct gf_cursor_priv *priv = (struct gf_cursor_priv *)c->p;
    if (priv) {
        // proj_row and proj_meta are no longer stored - each row uses temporary metadata
        if (priv->inner_cursor && priv->inner_cursor->close) {
            priv->inner_cursor->close(priv->inner_cursor);
        }
        if (priv->gf && priv->gf->close) {
            priv->gf->close(priv->gf);
        }
        FREE(priv);
    }
    FREE(c);
}

#define GF_FAST_PATH 1

static struct flintdb_sql_result * sql_exec_gf_fast_count(struct flintdb_sql *q, char **e);

static struct flintdb_sql_result * sql_exec_gf_select(struct flintdb_sql *q, char **e) {
    struct flintdb_sql_result*result = NULL;
    struct flintdb_genericfile *gf = NULL;
    struct flintdb_cursor_row *c = NULL;
    struct flintdb_cursor_row *wrapped_cursor = NULL;
    struct gf_cursor_priv *priv = NULL;

// Fast COUNT(*) for generic files
#ifdef GF_FAST_PATH
    if (q->columns.length == 1 && strempty(q->where) && strempty(q->groupby) && strempty(q->orderby) && !q->distinct) {
        struct flintdb_sql_result*fast = sql_exec_gf_fast_count(q, e);
        if (fast || (e && *e))
            return fast; // return if handled or error occurred
    }
#endif

    gf = flintdb_genericfile_open(q->table, FLINTDB_RDONLY, NULL, e);
    if (e && *e)
        THROW_S(e);

    // // Fast path for generic files if plugin can provide row count: SELECT COUNT(*) [alias]
    // if (q->columns.length == 1 && strempty(q->where) && strempty(q->groupby) && strempty(q->orderby) && !q->distinct) {
    //
    // }

    char where[SQL_STRING_LIMIT] = {0};
    sql_exec_indexable_where(NULL, q, where, sizeof(where));
    c = gf->find(gf, where, e);
    if (e && *e)
        THROW_S(e);
    if (!c)
        goto SQL_RESULT_EMPTY; // empty result

    // Check if GROUP BY is specified or if aggregate functions exist
    int has_aggregate = has_aggregate_function(q);
    if (!strempty(q->groupby) || has_aggregate) {
        result = sql_exec_select_groupby_row(q, NULL, c, gf, e);
        if (e && *e)
            THROW_S(e);
        // gf will be closed by groupby function
        return result;
    }

    // Apply DISTINCT early for generic files; limit will be applied by outer wrappers
    if (q->distinct) {
        c = distinct_cursor_wrap(q, c, NOLIMIT, e);
        if (e && *e) THROW_S(e);
    }

    // Check if ORDER BY is specified
    if (!strempty(q->orderby)) {
        result = sql_exec_sort(c, q->orderby, q->limit, e);
        if (e && *e) THROW_S(e);
        // gf will be closed by sort function
        return result;
    }

    // No GROUP BY or ORDER BY - wrap cursor to keep gf alive
    priv = (struct gf_cursor_priv *)CALLOC(1, sizeof(struct gf_cursor_priv));
    if (!priv) THROW(e, "Out of memory");
    priv->gf = gf;
    // Keep gf alive and apply LIMIT at outer layer (after DISTINCT)
    priv->inner_cursor = c;
    // If DISTINCT is requested, defer LIMIT to distinct wrapper; inner storage scan should be unlimited
    if (q->distinct) {
        priv->limit = NOLIMIT;
    } else {
        priv->limit = !strempty(q->limit) ? limit_parse(q->limit) : NOLIMIT;
    }
    priv->proj_count = 0;
    priv->proj_row = NULL;
    priv->proj_meta = NULL;

    const struct flintdb_meta *m = gf->meta(gf, e);
    if (e && *e) THROW_S(e);
    if (!m) THROW(e, "Missing file meta");

    wrapped_cursor = (struct flintdb_cursor_row *)CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!wrapped_cursor) THROW(e, "Out of memory");
    wrapped_cursor->p = priv;
    wrapped_cursor->next = gf_cursor_next;
    wrapped_cursor->close = gf_cursor_close;

    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");

    // Set up result structure with cursor; expand '*' wildcard if present
    result->row_cursor = wrapped_cursor;
    if (q->columns.length == 1 && strcmp(q->columns.name[0], "*") == 0) {
        // SELECT * -> no projection remap, use storage order
        result->column_count = m->columns.length;
        result->column_names = (char **)CALLOC(result->column_count, sizeof(char *));
        if (!result->column_names) THROW(e, "Out of memory");
        for (int i = 0; i < result->column_count; i++)
            result->column_names[i] = STRDUP(m->columns.a[i].name);
        priv->proj_count = 0; // means "no projection" in cursor
    } else {
        // Explicit column list -> build projection indexes
        result->column_count = q->columns.length;
        result->column_names = (char **)CALLOC(result->column_count, sizeof(char *));
        if (!result->column_names) THROW(e, "Out of memory");
        for (int i = 0; i < result->column_count; i++) {
            const char *name = q->columns.name[i];
            result->column_names[i] = STRDUP(name);
            int idx = flintdb_column_at((struct flintdb_meta *)m, name);
            if (idx < 0) THROW(e, "Column not found: %s", name);
            priv->proj_indexes[i] = idx;
        }
        priv->proj_count = result->column_count;
    }
    result->affected = -1;
    result->close = sql_result_close;

SQL_RESULT_EMPTY:
    // Don't close gf here - it will be closed by wrapped_cursor
    return result;

EXCEPTION:
    if (wrapped_cursor) FREE(wrapped_cursor);
    if (priv) FREE(priv);
    if (c && c->close) c->close(c);
    if (gf && gf->close) gf->close(gf);
    return NULL;
}

// Helper: fast COUNT(*) for text/gz files without decoding rows
static struct flintdb_sql_result * sql_exec_gf_fast_count(struct flintdb_sql *q, char **e) {
    if (!q) return NULL;
    if (!(q->columns.length == 1 && strempty(q->where) && strempty(q->groupby) && strempty(q->orderby) && !q->distinct))
        return NULL;

    const char *expr = q->columns.name[0];
    char up[MAX_COLUMN_NAME_LIMIT];
    int uj = 0;
    for (const char *p = expr; *p && uj < (int)sizeof(up) - 1; p++) {
        if (*p == ' ' || *p == '\t')
            continue;
        up[uj++] = (char)toupper((unsigned char)*p);
    }
    up[uj] = '\0';
    if (strncmp(up, "COUNT(", 6) != 0)
        return NULL;
    const char *rp = strchr(up + 6, ')');
    if (!rp)
        return NULL;
    char inner[16];
    int ilen = (int)(rp - (up + 6));
    if (ilen <= 0 || ilen >= (int)sizeof(inner))
        return NULL;
    memcpy(inner, up + 6, (size_t)ilen);
    inner[ilen] = '\0';
    if (!(strcmp(inner, "*") == 0 || strcmp(inner, "1") == 0 || strcmp(inner, "0") == 0))
        return NULL;

    // Open once to obtain meta (header info)
    struct flintdb_genericfile *gf = flintdb_genericfile_open(q->table, FLINTDB_RDONLY, NULL, e);
    if (e && *e) THROW_S(e);
    const struct flintdb_meta *m = gf->meta(gf, e);
    if (e && *e) {
        gf->close(gf);
        THROW_S(e);
    }

    i64 rows = gf->rows(gf, e); // maybe parquet or other format with known row count
    if (e && *e) {
        gf->close(gf);
        THROW_S(e);
    }

    if (rows < 0) {
        rows = 0;
        // Buffered read over the raw file (supports .gz via iostream)
        size_t iobsz = (size_t)(1 << 20);
        const char *env = getenv("IO_BUFSZ");
        if (env && *env) {
            long long v = atoll(env);
            if (v > 0)
                iobsz = (size_t)v;
        }
        char *gerr = NULL;
        struct bufio *b = file_bufio_open(q->table, FLINTDB_RDONLY, iobsz, &gerr);
        if (gerr || !b) {
            if (b)
                b->close(b);
            if (!gerr)
                gerr = "bufio open failed";
            THROW(e, "%s", gerr);
        }

        int last_was_nl = 0;
        int any_read = 0;
        const size_t CHUNK = 1 << 20;
        char *buf = (char *)MALLOC(CHUNK);
        if (!buf) {
            b->close(b);
            THROW(e, "Out of memory");
        }
        for (;;) {
            ssize_t n = b->read(b, buf, CHUNK, &gerr);
            if (gerr) {
                FREE(buf);
                b->close(b);
                THROW(e, "%s", gerr);
            }
            if (n <= 0)
                break;
            any_read = 1;
            for (ssize_t i = 0; i < n; i++) {
                if (buf[i] == '\n')
                    rows++;
            }
            last_was_nl = (n > 0 && buf[n - 1] == '\n');
        }
        if (buf)
            FREE(buf);
        b->close(b);

        // If file had data and doesn’t end with \n, count the last line
        if (any_read && !last_was_nl)
            rows++;
        // Adjust for header presence: meta.absent_header==0 means header present
        if (m && !m->absent_header && rows > 0)
            rows -= 1;
    }

    // Build one-row result
    char alias[MAX_COLUMN_NAME_LIMIT];
    if (!sql_extract_alias(expr, alias, sizeof(alias))) {
        strncpy(alias, "COUNT(*)", sizeof(alias) - 1);
        alias[sizeof(alias) - 1] = '\0';
    }
    struct flintdb_meta *dm = (struct flintdb_meta *)CALLOC(1, sizeof(struct flintdb_meta));
    if (!dm) {
        gf->close(gf);
        THROW(e, "Out of memory");
    }
    *dm = flintdb_meta_new("count", e);
    if (e && *e) {
        gf->close(gf);
        THROW_S(e);
    }
    flintdb_meta_columns_add(dm, alias, VARIANT_INT64, 8, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) {
        gf->close(gf);
        THROW_S(e);
    }

    struct flintdb_cursor_array_priv *apriv = (struct flintdb_cursor_array_priv *)CALLOC(1, sizeof(struct flintdb_cursor_array_priv));
    if (!apriv) {
        gf->close(gf);
        THROW(e, "Out of memory");
    }
    apriv->rows = arraylist_new(1);
    apriv->index = 0;
    apriv->meta = dm;
    struct flintdb_row *r = flintdb_row_new(dm, e);
    if (e && *e) {
        gf->close(gf);
        THROW_S(e);
    }
    struct flintdb_variant v;
    flintdb_variant_init(&v);
    flintdb_variant_i64_set(&v, rows);
    r->set(r, 0, &v, e);
    flintdb_variant_free(&v);
    if (e && *e) {
        r->free(r);
        gf->close(gf);
        THROW_S(e);
    }
    apriv->rows->add(apriv->rows, (valtype)(uintptr_t)r, list_row_dealloc, e);
    if (e && *e) {
        r->free(r);
        gf->close(gf);
        THROW_S(e);
    }

    struct flintdb_cursor_row *ac = (struct flintdb_cursor_row *)CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!ac) {
        gf->close(gf);
        THROW(e, "Out of memory");
    }
    ac->p = apriv;
    ac->next = array_cursor_next;
    ac->close = array_cursor_close;

    struct flintdb_sql_result*result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) {
        gf->close(gf);
        THROW(e, "Out of memory");
    }
    result->row_cursor = ac;
    result->column_count = 1;
    result->column_names = (char **)CALLOC(1, sizeof(char *));
    if (!result->column_names) {
        gf->close(gf);
        THROW(e, "Out of memory");
    }
    result->column_names[0] = STRDUP(alias);
    result->affected = 1;
    result->close = sql_result_close;

    gf->close(gf);
    return result;

EXCEPTION:
    if (gf) gf->close(gf);
    return NULL;
}

// Simple SELECT for binary tables

struct flintdb_table_cursor_priv {
    struct flintdb_table *table;
    struct flintdb_cursor_i64 *cr;
    struct limit limit;
    // projection: length = number of SELECT expressions, values are source column indexes
    int proj_count;
    int proj_indexes[MAX_COLUMNS_LIMIT];
    struct flintdb_meta *proj_meta; // cached projection meta (owned, created once per cursor)
    struct flintdb_row *proj_row;   // reused projection row buffer (for performance)
    struct flintdb_row *stream_row; // reused decode buffer for SELECT * streaming (bypass cache)
};

static struct flintdb_row *sql_table_cursor_next(struct flintdb_cursor_row *c, char **e) {
    struct flintdb_table_cursor_priv *priv = (struct flintdb_table_cursor_priv *)c->p;

    // Apply LIMIT
    if (!priv->limit.remains(&priv->limit)) {
        return NULL;
    }

    // Skip OFFSET rows
    while (priv->limit.skip(&priv->limit)) {
        i64 rowid = priv->cr->next(priv->cr, e);
        if (e && *e)
            return NULL;
        if (rowid < 0)
            return NULL;
    }

    i64 rowid = priv->cr->next(priv->cr, e);
    if (e && *e) return NULL;
    if (rowid < 0) return NULL;

    // If no projection is defined (SELECT *), use streaming read to avoid cache overhead
    if (priv->proj_count <= 0) {
        // Lazily allocate stream_row buffer once, reuse across all rows
        if (!priv->stream_row) {
            const struct flintdb_meta *m = priv->table->meta(priv->table, e);
            if ((e && *e) || !m) return NULL;
            priv->stream_row = flintdb_row_pool_acquire((struct flintdb_meta *)m, e);
            if ((e && *e) || !priv->stream_row) return NULL;
        }
        // Decode directly into reusable buffer (no cache, no extra allocation)
        if (priv->table->read_stream(priv->table, rowid, priv->stream_row, e) < 0 || (e && *e)) {
            return NULL;
        }
        return priv->stream_row; // caller must NOT free (owned by cursor, reused per row)
    }

    // Projection path: read from cache (needed for correctness if projection involves same row multiple times)
    const struct flintdb_row *r = priv->table->read(priv->table, rowid, e);
    if ((e && *e) || !r) return NULL;

    // Build (or reuse) projected meta once per cursor
    struct flintdb_meta *m = (struct flintdb_meta *)r->meta;
    if (!priv->proj_meta) {
        // Allocate projection meta once and cache in cursor
        struct flintdb_meta *pm = (struct flintdb_meta *)CALLOC(1, sizeof(struct flintdb_meta));
        if (!pm) return NULL;
        pm->columns.length = priv->proj_count;
        for (int i = 0; i < priv->proj_count; i++) {
            int src = priv->proj_indexes[i];
            if (src < 0 || src >= m->columns.length) {
                FREE(pm);
                return NULL;
            }
            pm->columns.a[i] = m->columns.a[src];
        }
        priv->proj_meta = pm; // cache for cursor lifetime
    }
    
    // Lazily allocate proj_row once per cursor and reuse (like stream_row)
    if (!priv->proj_row) {
        priv->proj_row = flintdb_row_pool_acquire(priv->proj_meta, e);
        if ((e && *e) || !priv->proj_row) return NULL;
    }
    struct flintdb_row *out = priv->proj_row; // reuse buffer
    if ((e && *e) || !out) {
        return NULL;
    }
    for (int i = 0; i < priv->proj_count; i++) {
        int src = priv->proj_indexes[i];
        struct flintdb_variant *v = r->get((struct flintdb_row *)r, src, e);
        if (e && *e)
            return NULL;
        out->set(out, i, v, e);
        if (e && *e)
            return NULL;
    }
    return out; // caller must NOT free (owned by cursor, reused per row)
}

static void sql_table_cursor_close(struct flintdb_cursor_row *c) {
    if (!c)
        return;

    struct flintdb_table_cursor_priv *priv = (struct flintdb_table_cursor_priv *)c->p;
    if (priv) {
        if (priv->cr && priv->cr->close) {
            priv->cr->close(priv->cr);
        }
        if (priv->table && priv->table->close) {
            priv->table->close(priv->table);
        }
        if (priv->proj_meta) {
            // projection meta was shallow (column structs copied); safe to free struct only
            FREE(priv->proj_meta);
            priv->proj_meta = NULL;
        }
        if (priv->proj_row) {
            // Return projection row buffer to pool
            priv->proj_row->free(priv->proj_row);
            priv->proj_row = NULL;
        }
        if (priv->stream_row) {
            // Return streaming row buffer to pool
            priv->stream_row->free(priv->stream_row);
            priv->stream_row = NULL;
        }
        FREE(priv);
    }
    FREE(c);
}

// Fast path: handle SELECT COUNT(*) [alias] FROM <table> with no WHERE/GROUP/ORDER/DISTINCT
static struct flintdb_sql_result * sql_exec_fast_count(struct flintdb_sql *q, struct flintdb_table *table, char **e) {
    if (!q || !table) return NULL;
    if (q->columns.length != 1) return NULL;
    if (!strempty(q->where) || !strempty(q->groupby) || !strempty(q->orderby) || q->distinct)
        return NULL;

    const char *expr = q->columns.name[0];
    // Build uppercase copy without spaces/tabs for pattern match
    char up[MAX_COLUMN_NAME_LIMIT];
    int uj = 0;
    for (const char *p = expr; *p && uj < (int)sizeof(up) - 1; p++) {
        if (*p == ' ' || *p == '\t')
            continue;
        up[uj++] = (char)toupper((unsigned char)*p);
    }
    up[uj] = '\0';

    // Accept COUNT(*), COUNT(1), COUNT(0)
    if (strncmp(up, "COUNT(", 6) != 0)
        return NULL;
    const char *rp = strchr(up + 6, ')');
    if (!rp)
        return NULL;
    // Extract inner token between parentheses
    char inner[16];
    int ilen = (int)(rp - (up + 6));
    if (ilen <= 0 || ilen >= (int)sizeof(inner))
        return NULL;
    memcpy(inner, up + 6, (size_t)ilen);
    inner[ilen] = '\0';
    if (!(strcmp(inner, "*") == 0 || strcmp(inner, "1") == 0 || strcmp(inner, "0") == 0))
        return NULL;

    i64 rows = table->rows(table, e);
    if (e && *e)
        return NULL;

    // Determine output column name (alias if provided, else default)
    char alias[MAX_COLUMN_NAME_LIMIT];
    if (!sql_extract_alias(expr, alias, sizeof(alias))) {
        strncpy(alias, "COUNT(*)", sizeof(alias) - 1);
        alias[sizeof(alias) - 1] = '\0';
    }

    // Build one-row, one-column result using array-backed cursor machinery
    // Honor LIMIT/OFFSET: COUNT(*) produces at most one row
    int visible = 1;
    if (!strempty(q->limit)) {
        struct limit lim = limit_parse(q->limit);
        if (lim.priv.offset >= 1)
            visible = 0;
        if (visible == 1 && lim.priv.limit == 0)
            visible = 0;
        if (visible == 1 && lim.priv.limit > 0)
            visible = 1; // min(1, limit)
    }
    struct flintdb_meta *dm = (struct flintdb_meta *)CALLOC(1, sizeof(struct flintdb_meta));
    if (!dm) THROW(e, "Out of memory");
    *dm = flintdb_meta_new("count", e);
    if (e && *e) THROW_S(e);
    flintdb_meta_columns_add(dm, alias, VARIANT_INT64, 8, 0, SPEC_NULLABLE, NULL, NULL, e);
    if (e && *e) THROW_S(e);

    struct flintdb_cursor_array_priv *apriv = (struct flintdb_cursor_array_priv *)CALLOC(1, sizeof(struct flintdb_cursor_array_priv));
    if (!apriv) THROW(e, "Out of memory");
    apriv->rows = arraylist_new(visible > 0 ? 1 : 0);
    if (!apriv->rows) THROW(e, "Out of memory");
    apriv->index = 0;
    apriv->meta = dm;
    if (visible > 0) {
        struct flintdb_row *r = flintdb_row_new(dm, e);
        if (e && *e) THROW_S(e);
        struct flintdb_variant v;
        flintdb_variant_init(&v);
        flintdb_variant_i64_set(&v, rows);
        r->set(r, 0, &v, e);
        flintdb_variant_free(&v);
        if (e && *e) THROW_S(e);
        apriv->rows->add(apriv->rows, (valtype)(uintptr_t)r, list_row_dealloc, e);
        if (e && *e) THROW_S(e);
    }

    struct flintdb_cursor_row *ac = (struct flintdb_cursor_row *)CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!ac) THROW(e, "Out of memory");
    ac->p = apriv;
    ac->next = array_cursor_next;
    ac->close = array_cursor_close;

    struct flintdb_sql_result*result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");
    result->row_cursor = ac;
    result->column_count = 1;
    result->column_names = (char **)CALLOC(1, sizeof(char *));
    if (!result->column_names) THROW(e, "Out of memory");
    result->column_names[0] = STRDUP(alias);
    result->affected = visible;
    result->close = sql_result_close;
    return result;

EXCEPTION:
    return NULL;
}

static struct flintdb_sql_result * sql_exec_select(struct flintdb_sql *q, struct flintdb_transaction *t, char **e) {
    struct flintdb_sql_result*result = NULL;
    struct flintdb_cursor_i64 *cr = NULL;
    struct flintdb_cursor_row *c = NULL;
    struct flintdb_table_cursor_priv *priv = NULL;
    struct flintdb_table *table = NULL;

    table = sql_exec_table_borrow(q->table, e);
    if (e && *e) THROW_S(e);
    // Try COUNT(*) fast path
    if (q->columns.length == 1 && strempty(q->where) && strempty(q->groupby) && strempty(q->orderby) && !q->distinct) {
        struct flintdb_sql_result*fast = sql_exec_fast_count(q, table, e);
        if (fast) {
            return fast;
        }
    }

    const struct flintdb_meta *meta = table->meta(table, e);
    if (e && *e) THROW_S(e);

    char where[SQL_STRING_LIMIT] = {0};
    sql_exec_indexable_where(meta, q, where, sizeof(where));
    cr = table->find(table, where, e);
    if (e && *e) THROW_S(e);
    if (!cr)
        goto SQL_RESULT_EMPTY; // empty result

    // Aggregates without GROUP BY should also use group-by path (single global group)
    int has_aggregate = has_aggregate_function(q);
    if (!strempty(q->groupby) || has_aggregate) { // check group by or aggregates
        result = sql_exec_select_groupby_i64(q, table, cr, e);
        if (e && *e) THROW_S(e);
        return result;
    } else if (!strempty(q->orderby)) {
        // ORDER BY for binary tables: wrap i64 cursor as row cursor and delegate to common sorter
        priv = (struct flintdb_table_cursor_priv *)CALLOC(1, sizeof(struct flintdb_table_cursor_priv));
        if (!priv) THROW(e, "Out of memory");
        priv->table = table;
        priv->cr = cr;
        priv->limit = NOLIMIT; // limit will be applied by sorter wrapper

        c = (struct flintdb_cursor_row *)CALLOC(1, sizeof(struct flintdb_cursor_row));
        if (!c) THROW(e, "Out of memory");
        c->p = priv;
        c->next = sql_table_cursor_next;
        c->close = sql_table_cursor_close;

        if (q->distinct) {
            // Apply DISTINCT before sort; sorter will handle LIMIT
            c = distinct_cursor_wrap(q, c, NOLIMIT, e);
            if (e && *e)
                THROW_S(e);
        }

        result = sql_exec_sort(c, q->orderby, q->limit, e);
        if (e && *e) THROW_S(e);
        return result;
    }

    // DISTINCT applied after row cursor is created below

    // No group by, no order by
    // wrap cursor_i64 to cursor_row with projection
    priv = (struct flintdb_table_cursor_priv *)CALLOC(1, sizeof(struct flintdb_table_cursor_priv));
    if (!priv) THROW(e, "Out of memory");
    priv->table = table;
    priv->cr = cr;
    priv->limit = !strempty(q->limit) ? limit_parse(q->limit) : NOLIMIT;
    priv->proj_count = 0;

    const struct flintdb_meta *m = table->meta(table, e);
    if (e && *e) THROW_S(e);
    if (!m) THROW(e, "Missing table meta");

    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");

    // extract projection and column names from sql_context; expand '*' wildcard here
    if (q->columns.length == 1 && strcmp(q->columns.name[0], "*") == 0) {
        // SELECT * -> no projection remap, use storage order
        result->column_count = m->columns.length;
        result->column_names = (char **)CALLOC(result->column_count, sizeof(char *));
        if (!result->column_names)
            THROW(e, "Out of memory");
        for (int i = 0; i < result->column_count; i++) {
            result->column_names[i] = STRDUP(m->columns.a[i].name);
        }
        priv->proj_count = 0; // means "no projection" in cursor
    } else {
        // Explicit column list -> build projection indexes
        result->column_count = q->columns.length;
        result->column_names = (char **)CALLOC(result->column_count, sizeof(char *));
        if (!result->column_names) THROW(e, "Out of memory");
        for (int i = 0; i < result->column_count; i++) {
            const char *name = q->columns.name[i];
            result->column_names[i] = STRDUP(name);
            int idx = flintdb_column_at((struct flintdb_meta *)m, name);
            if (idx < 0)
                THROW(e, "Column not found: %s", name);
            priv->proj_indexes[i] = idx;
        }
        priv->proj_count = result->column_count;
    }

    c = (struct flintdb_cursor_row *)CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!c) THROW(e, "Out of memory");
    c->p = priv;
    c->next = sql_table_cursor_next;
    c->close = sql_table_cursor_close;
    // Apply DISTINCT over the projected row stream if requested
    if (q->distinct) {
        struct limit dlim = (!strempty(q->limit)) ? limit_parse(q->limit) : NOLIMIT;
        c = distinct_cursor_wrap(q, c, dlim, e);
        if (e && *e) THROW_S(e);
    }
    // Column aliasing would require parsing AS clauses in sql.c
    // DISTINCT would require deduplication in cursor wrapper

    result->row_cursor = c;
    result->affected = -1; // unknown
    result->close = sql_result_close;
    return result;

SQL_RESULT_EMPTY:
    // empty result
    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");
    result->column_count = q->columns.length;
    result->column_names = (char **)CALLOC(result->column_count, sizeof(char *));
    if (!result->column_names)
        THROW(e, "Out of memory");
    for (int i = 0; i < result->column_count; i++)
        result->column_names[i] = STRDUP(q->columns.name[i]);
    result->affected = 0;
    result->close = sql_result_close;
    return result;
EXCEPTION:
    if (c) c->close(c);
    return NULL;
}

typedef struct flintdb_aggregate_func *(*aggregate_func_factory)(const char *, const char *, enum flintdb_variant_type , struct flintdb_aggregate_condition, char **);

static int has_aggregate_function(struct flintdb_sql *q) {
    for (int i = 0; i < q->columns.length; i++) {
        const char *col = q->columns.name[i];
        // Remove spaces for comparison
        char upper_col[MAX_COLUMN_NAME_LIMIT];
        int j = 0;
        for (const char *p = col; *p && j < MAX_COLUMN_NAME_LIMIT - 1; p++) {
            if (*p != ' ' && *p != '\t') {
                upper_col[j++] = toupper(*p);
            }
        }
        upper_col[j] = '\0';

        if (strstr(upper_col, "COUNT(") ||
            strstr(upper_col, "SUM(") ||
            strstr(upper_col, "AVG(") ||
            strstr(upper_col, "MIN(") ||
            strstr(upper_col, "MAX(") ||
            strstr(upper_col, "HLL_COUNT(") ||
            strstr(upper_col, "HLL_SUM(") ||
            strstr(upper_col, "FIRST(") ||
            strstr(upper_col, "LAST(")) {
            return 1;
        }
    }
    return 0;
}

// Extract alias after AS (case-insensitive). Returns 1 if alias found.
// sql_extract_alias moved to sql.c

// Forward declarations and definitions required before GROUP BY on i64 path

// Filesort cursor wrapper (moved earlier to satisfy dependencies)
struct flintdb_filesort_cursor_priv {
    struct flintdb_filesort *sorter;
    i64 current_idx;
    i64 row_count;
    struct limit limit;
};

static struct flintdb_row *filesort_cursor_next(struct flintdb_cursor_row *c, char **e) {
    struct flintdb_filesort_cursor_priv *p = (struct flintdb_filesort_cursor_priv *)c->p;
    if (!p->limit.remains(&p->limit))
        return NULL;
    while (p->limit.skip(&p->limit)) {
        p->current_idx++;
        if (p->current_idx >= p->row_count)
            return NULL;
    }
    if (p->current_idx >= p->row_count)
        return NULL;
    struct flintdb_row *r = p->sorter->read(p->sorter, p->current_idx++, e);
    return r ? r->copy(r, e) : NULL;
}

static void filesort_cursor_close(struct flintdb_cursor_row *c) {
    if (!c)
        return;
    struct flintdb_filesort_cursor_priv *p = (struct flintdb_filesort_cursor_priv *)c->p;
    if (p) {
        if (p->sorter)
            p->sorter->close(p->sorter);
        FREE(p);
    }
    FREE(c);
}

// Multi-column ORDER BY support
struct order_spec {
    int col_idx;
    i8 descending;
};
struct sort_multi_context {
    int count;
    struct order_spec specs[MAX_COLUMNS_LIMIT];
};
/* Forward declarations needed by common helpers */
// parse_orderby_clause is now sql_parse_orderby_clause in sql.h
static int sort_row_multi_comparator(const void *ctx, const struct flintdb_row *a, const struct flintdb_row *b) {
    const struct sort_multi_context *sc = (const struct sort_multi_context *)ctx;
    char *e = NULL;
    for (int i = 0; i < sc->count; i++) {
        int col = sc->specs[i].col_idx;
        i8 desc = sc->specs[i].descending;
        struct flintdb_variant *va = a->get((struct flintdb_row *)a, col, &e);
        struct flintdb_variant *vb = b->get((struct flintdb_row *)b, col, &e);
        if (!va && !vb)
            continue;
        if (!va)
            return desc ? 1 : -1;
        if (!vb)
            return desc ? -1 : 1;
        int cmp = flintdb_variant_compare(va, vb);
        if (cmp != 0)
            return desc ? -cmp : cmp;
    }
    return 0; // all equal across specs
}

// Parse ORDER BY clause: "col1 [ASC|DESC], col2 [ASC|DESC], ..."
// parse_orderby_clause moved to sql.c (sql_parse_orderby_clause)

// HAVING clause filter helpers
static double convert_to_number(struct flintdb_variant *v, char **e) {
    if (!v) return 0.0;
    
    switch (v->type) {
        case VARIANT_INT32:
        case VARIANT_INT64:
            return (double)flintdb_variant_i64_get(v, e);
        case VARIANT_DOUBLE:
        case VARIANT_FLOAT:
            return flintdb_variant_f64_get(v, e);
        case VARIANT_DECIMAL: {
            struct flintdb_decimal  dec = flintdb_variant_decimal_get(v, e);
            return flintdb_decimal_to_f64(&dec, e);
        }
        case VARIANT_STRING: {
            const char *str = flintdb_variant_string_get(v);
            if (str) {
                char *endptr;
                double val = strtod(str, &endptr);
                if (endptr != str) return val;
                // Non-numeric string: use hash for comparison
                unsigned long hash = 5381;
                for (const char *p = str; *p; p++)
                    hash = ((hash << 5) + hash) + *p;
                return (double)hash;
            }
            return 0.0;
        }
        default:
            return 0.0;
    }
}

static struct flintdb_variant *get_having_value(const struct flintdb_row *row, const char *expr, char **e) {
    if (!row || !expr) return NULL;
    
    const struct flintdb_meta *meta = row->meta;
    if (!meta) return NULL;
    
    // Try direct column name match
    for (int i = 0; i < meta->columns.length; i++) {
        if (strcasecmp(meta->columns.a[i].name, expr) == 0) {
            return row->get(row, i, e);
        }
    }
    
    // Try normalized match (remove spaces)
    char normalized[MAX_COLUMN_NAME_LIMIT];
    int j = 0;
    for (const char *p = expr; *p && j < MAX_COLUMN_NAME_LIMIT - 1; p++) {
        if (*p != ' ' && *p != '\t')
            normalized[j++] = *p;
    }
    normalized[j] = '\0';
    
    for (int i = 0; i < meta->columns.length; i++) {
        char col_normalized[MAX_COLUMN_NAME_LIMIT];
        j = 0;
        for (const char *p = meta->columns.a[i].name; *p && j < MAX_COLUMN_NAME_LIMIT - 1; p++) {
            if (*p != ' ' && *p != '\t')
                col_normalized[j++] = *p;
        }
        col_normalized[j] = '\0';
        
        if (strcasecmp(col_normalized, normalized) == 0) {
            return row->get(row, i, e);
        }
    }
    
    return NULL;
}

static int evaluate_having_condition(const struct flintdb_row *row, const char *condition, char **e) {
    if (!condition || !*condition) return 1;
    
    char cond_copy[1024];
    strncpy(cond_copy, condition, sizeof(cond_copy) - 1);
    cond_copy[sizeof(cond_copy) - 1] = '\0';
    
    // Handle AND/OR operations
    char *and_pos = strcasestr(cond_copy, " AND ");
    if (and_pos) {
        *and_pos = '\0';
        if (!evaluate_having_condition(row, cond_copy, e)) return 0;
        return evaluate_having_condition(row, and_pos + 5, e);
    }
    
    char *or_pos = strcasestr(cond_copy, " OR ");
    if (or_pos) {
        *or_pos = '\0';
        if (evaluate_having_condition(row, cond_copy, e)) return 1;
        return evaluate_having_condition(row, or_pos + 4, e);
    }
    
    // Parse comparison operators
    const char *operators[] = {">=", "<=", "!=", "<>", ">", "<", "="};
    int op_lens[] = {2, 2, 2, 2, 1, 1, 1};
    
    for (int i = 0; i < 7; i++) {
        char *op_pos = strstr(cond_copy, operators[i]);
        if (op_pos) {
            *op_pos = '\0';
            char *left_side = cond_copy;
            char *right_side = op_pos + op_lens[i];
            
            // Trim spaces
            while (*left_side == ' ' || *left_side == '\t') left_side++;
            char *end = left_side + strlen(left_side) - 1;
            while (end > left_side && (*end == ' ' || *end == '\t')) *end-- = '\0';
            
            while (*right_side == ' ' || *right_side == '\t') right_side++;
            end = right_side + strlen(right_side) - 1;
            while (end > right_side && (*end == ' ' || *end == '\t')) *end-- = '\0';
            
            // Get left value from row
            struct flintdb_variant *left_var = get_having_value(row, left_side, e);
            if (!left_var) return 0;
            
            // Parse right value
            double right_num;
            if (*right_side == '\'' || *right_side == '"') {
                // String literal - skip for now, use 0
                right_num = 0.0;
            } else {
                right_num = strtod(right_side, NULL);
            }
            
            double left_num = convert_to_number(left_var, e);
            
            // Compare based on operator
            if (strcmp(operators[i], ">=") == 0) return left_num >= right_num;
            if (strcmp(operators[i], "<=") == 0) return left_num <= right_num;
            if (strcmp(operators[i], ">") == 0) return left_num > right_num;
            if (strcmp(operators[i], "<") == 0) return left_num < right_num;
            if (strcmp(operators[i], "=") == 0) return left_num == right_num;
            if (strcmp(operators[i], "!=") == 0 || strcmp(operators[i], "<>") == 0) 
                return left_num != right_num;
        }
    }
    
    return 1; // Default to true if can't parse
}

static int apply_having_filter(struct flintdb_row **rows, int row_count, const char *having, char **e) {
    if (!having || !*having || row_count <= 0) return row_count;
    
    int write_idx = 0;
    for (int read_idx = 0; read_idx < row_count; read_idx++) {
        if (evaluate_having_condition(rows[read_idx], having, e)) {
            if (write_idx != read_idx) {
                rows[write_idx] = rows[read_idx];
            }
            write_idx++;
        } else {
            // Free filtered out row
            if (rows[read_idx]) {
                rows[read_idx]->free(rows[read_idx]);
                rows[read_idx] = NULL;
            }
        }
    }
    
    return write_idx;
}

// New implementation using aggregate API from aggregate.c
static struct flintdb_sql_result * sql_exec_select_groupby_i64(struct flintdb_sql *q, struct flintdb_table *table, struct flintdb_cursor_i64 *cr, char **e) {
    struct flintdb_sql_result*result = NULL;
    struct flintdb_aggregate *agg = NULL;
    struct flintdb_aggregate_groupby **groupbys = NULL;
    struct flintdb_aggregate_func **funcs = NULL;
    int groupby_count = 0;
    int aggr_count = 0;

    if (q->columns.length == 1 && strcmp(q->columns.name[0], "*") == 0)
        THROW(e, "SELECT * not supported with GROUP BY or aggregate functions");

    // Parse GROUP BY columns
    char group_cols[MAX_COLUMNS_LIMIT][MAX_COLUMN_NAME_LIMIT];
    groupby_count = sql_parse_groupby_columns(q->groupby, group_cols);

    // Create groupby objects
    groupbys = (struct flintdb_aggregate_groupby **)CALLOC(groupby_count, sizeof(struct flintdb_aggregate_groupby *));
    if (!groupbys && groupby_count > 0)
        THROW(e, "Out of memory allocating groupbys");

    for (int i = 0; i < groupby_count; i++) {
        // Find column type from table metadata
        enum flintdb_variant_type  col_type = VARIANT_NULL;
        const struct flintdb_meta *meta = table->meta(table, e);
        if (meta) {
            for (int j = 0; j < meta->columns.length; j++) {
                if (strcmp(meta->columns.a[j].name, group_cols[i]) == 0) {
                    col_type = meta->columns.a[j].type;
                    break;
                }
            }
        }
        groupbys[i] = groupby_new(group_cols[i], group_cols[i], col_type, e);
        if (e && *e) THROW_S(e);
    }

    // Parse aggregate functions from SELECT
    funcs = (struct flintdb_aggregate_func **)CALLOC(MAX_COLUMNS_LIMIT, sizeof(struct flintdb_aggregate_func *));
    if (!funcs)
        THROW(e, "Out of memory allocating funcs");

    for (int i = 0; i < q->columns.length; i++) {
        const char *expr = q->columns.name[i];
        int is_group = 0;
        for (int g = 0; g < groupby_count; g++)
            if (strcmp(expr, group_cols[g]) == 0) {
                is_group = 1;
                break;
            }
        if (is_group)
            continue;

        // Parse aggregate function expression
        char func_name[64], col_name[MAX_COLUMN_NAME_LIMIT], alias[MAX_COLUMN_NAME_LIMIT];
        const char *open_paren = strchr(expr, '(');
        const char *close_paren = strrchr(expr, ')');
        if (!open_paren || !close_paren || close_paren <= open_paren + 1)
            THROW(e, "Malformed aggregate expression: %s", expr);

        int fname_len = (int)(open_paren - expr);
        if (fname_len >= (int)sizeof(func_name))
            fname_len = (int)sizeof(func_name) - 1;
        strncpy(func_name, expr, fname_len);
        func_name[fname_len] = '\0';
        // trim spaces
        while (fname_len > 0 && (func_name[fname_len - 1] == ' ' || func_name[fname_len - 1] == '\t'))
            func_name[--fname_len] = '\0';

        int col_len = (int)(close_paren - open_paren - 1);
        if (col_len >= MAX_COLUMN_NAME_LIMIT)
            col_len = MAX_COLUMN_NAME_LIMIT - 1;
        strncpy(col_name, open_paren + 1, col_len);
        col_name[col_len] = '\0';
        // trim leading spaces
        char *sc = col_name;
        while (*sc == ' ' || *sc == '\t')
            sc++;
        if (sc != col_name)
            memmove(col_name, sc, strlen(sc) + 1);

        if (!sql_extract_alias(expr, alias, MAX_COLUMN_NAME_LIMIT)) {
            // Default alias: use the original expression text (trimmed)
            const char *orig = expr;
            while (*orig == ' ' || *orig == '\t')
                orig++;
            size_t a_len = strnlen(orig, MAX_COLUMN_NAME_LIMIT - 1);
            while (a_len > 0 && (orig[a_len - 1] == ' ' || orig[a_len - 1] == '\t'))
                a_len--;
            if (a_len >= MAX_COLUMN_NAME_LIMIT)
                a_len = MAX_COLUMN_NAME_LIMIT - 1;
            memcpy(alias, orig, a_len);
            alias[a_len] = '\0';
        }

        struct flintdb_aggregate_condition cond = {0};
        // Create aggregate_func based on func_name
        if (strcasecmp(func_name, "COUNT") == 0) {
            funcs[aggr_count++] = flintdb_func_count(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "SUM") == 0) {
            funcs[aggr_count++] = flintdb_func_sum(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "AVG") == 0) {
            funcs[aggr_count++] = flintdb_func_avg(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "MIN") == 0) {
            funcs[aggr_count++] = flintdb_func_min(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "MAX") == 0) {
            funcs[aggr_count++] = flintdb_func_max(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "FIRST") == 0) {
            funcs[aggr_count++] = flintdb_func_first(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "LAST") == 0) {
            funcs[aggr_count++] = flintdb_func_last(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "DISTINCT_COUNT") == 0) {
            funcs[aggr_count++] = flintdb_func_distinct_count(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "DISTINCT_HLL_COUNT") == 0) {
            funcs[aggr_count++] = flintdb_func_distinct_hll_count(col_name, alias, VARIANT_NULL, cond, e);
        } else {
            THROW(e, "Unknown aggregate function: %s", func_name);
        }

        if (e && *e) THROW_S(e);
    }

    if (aggr_count == 0) THROW(e, "No aggregate functions found");

    // Create aggregate
    agg = aggregate_new("sql_groupby_i64", groupbys, groupby_count, funcs, aggr_count, e);
    if (e && *e)
        THROW_S(e);

    // Process rows
    for (i64 rid; (rid = cr->next(cr, e)) > -1;) {
        if (e && *e) THROW_S(e);
        const struct flintdb_row *src_row = table->read(table, rid, e);
        if (e && *e) THROW_S(e);
        if (!src_row)
            continue;

        agg->row(agg, src_row, e);
        if (e && *e) THROW_S(e);
    }

    // Compute results
    struct flintdb_row **out_rows = NULL;
    int row_count = agg->compute(agg, &out_rows, e);
    if (e && *e) THROW_S(e);
    
    // Apply HAVING clause filter if present
    if (!strempty(q->having)) {
        row_count = apply_having_filter(out_rows, row_count, q->having, e);
        if (e && *e) THROW_S(e);
    }

    // Build sql_result from computed rows (they already contain groupby cols + aggr values)
    // Create a sorter for ORDER BY and LIMIT support
    struct flintdb_meta *result_meta = out_rows && row_count > 0 ? (struct flintdb_meta *)out_rows[0]->meta : NULL;
    if (result_meta) {
        for (int i = 0; i < result_meta->columns.length && i < 5; i++) {
            // Column info logged for debugging
        }
    }
    if (!result_meta && row_count > 0)
        THROW(e, "Missing result metadata");

    // Fast path: no ORDER BY and no LIMIT -> avoid filesort allocation
    if (row_count >= 0 && strempty(q->orderby) && strempty(q->limit)) {
        struct flintdb_sql_result*fast = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
        if (!fast) THROW(e, "Out of memory");
        // Build array cursor over out_rows
        struct list *rows_list = arraylist_new(row_count > 0 ? row_count : 8);
        for (int i = 0; i < row_count; i++) {
            // Transfer ownership of each row to list (do NOT free now)
            rows_list->add(rows_list, (valtype)(uintptr_t)out_rows[i], list_row_dealloc, e);
            if (e && *e) THROW_S(e);
        }
        // free only the pointer array container
        if (out_rows)
            FREE(out_rows);
        out_rows = NULL; // ownership moved

        struct flintdb_cursor_array_priv *apriv = CALLOC(1, sizeof(struct flintdb_cursor_array_priv));
        if (!apriv) THROW(e, "Out of memory");
        apriv->rows = rows_list;
        apriv->index = 0;
        apriv->meta = NULL; // not required; row meta used directly
        struct flintdb_cursor_row *ac = CALLOC(1, sizeof(struct flintdb_cursor_row));
        if (!ac) THROW(e, "Out of memory");
        ac->p = apriv;
        ac->next = array_cursor_next;
        ac->close = array_cursor_close;

        fast->row_cursor = ac;
        fast->column_count = result_meta ? result_meta->columns.length : 0;
        fast->column_names = (char **)CALLOC(fast->column_count, sizeof(char *));
        if (!fast->column_names && fast->column_count > 0)
            THROW(e, "Out of memory");
        for (int i = 0; i < fast->column_count; i++)
            fast->column_names[i] = STRDUP(result_meta->columns.a[i].name);
        fast->affected = row_count;
        fast->close = sql_result_close;

        if (agg) agg->free(agg);
        if (cr) cr->close(cr);
        return fast;
    }

    char temp_file[512];
    snprintf(temp_file, sizeof(temp_file), "%s/flintdb_sort_%ld.tmp", ensure_temp_dir(), (long)time(NULL));
    struct flintdb_filesort *sorter = flintdb_filesort_new(temp_file, result_meta, e);
    if (e && *e) THROW_S(e);

    for (int i = 0; i < row_count; i++) {
        // if (out_rows[i] && out_rows[i]->get) {
        //     char *err = NULL;
        //     struct flintdb_variant *v = out_rows[i]->get(out_rows[i], 0, &err);
        //     if (v && v->type == VARIANT_INT64) {
        //         i64 val = flintdb_variant_i64_get(v, &err);
        //     }
        // } else {
        // }
        sorter->add(sorter, out_rows[i], e);
        if (e && *e) THROW_S(e);
    }

    // Apply ORDER BY if specified
    // int orderby_empty = (!q->orderby || !*(q->orderby));
    if (!strempty(q->orderby)) {
        char order_cols[MAX_COLUMNS_LIMIT][MAX_COLUMN_NAME_LIMIT];
        i8 desc_flags[MAX_COLUMNS_LIMIT];
        int ocnt = 0;
        sql_parse_orderby_clause(q->orderby, order_cols, desc_flags, &ocnt);
        if (ocnt <= 0) THROW(e, "Failed to parse ORDER BY clause");
        struct sort_multi_context sc;
        memset(&sc, 0, sizeof(sc));
        sc.count = ocnt;
        for (int i = 0; i < ocnt; i++) {
            int order_idx = flintdb_column_at(result_meta, order_cols[i]);
            if (order_idx < 0)
                THROW(e, "ORDER BY column not found: %s", order_cols[i]);
            sc.specs[i].col_idx = order_idx;
            sc.specs[i].descending = desc_flags[i];
        }
        sorter->sort(sorter, sort_row_multi_comparator, &sc, e);
        if (e && *e) THROW_S(e);
    }

    // Build result cursor
    struct flintdb_filesort_cursor_priv *priv = CALLOC(1, sizeof(struct flintdb_filesort_cursor_priv));
    if (!priv) THROW(e, "Out of memory");
    priv->sorter = sorter;
    priv->current_idx = 0;
    priv->row_count = sorter->rows(sorter);
    priv->limit = !strempty(q->limit) ? limit_parse(q->limit) : NOLIMIT;

    struct flintdb_cursor_row *wrapped_cursor = CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!wrapped_cursor) THROW(e, "Out of memory");
    wrapped_cursor->p = priv;
    wrapped_cursor->next = filesort_cursor_next;
    wrapped_cursor->close = filesort_cursor_close;

    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");
    result->row_cursor = wrapped_cursor;
    result->column_count = result_meta ? result_meta->columns.length : 0;
    result->column_names = (char **)CALLOC(result->column_count, sizeof(char *));
    if (!result->column_names && result->column_count > 0)
        THROW(e, "Out of memory");
    for (int i = 0; i < result->column_count; i++)
        result->column_names[i] = STRDUP(result_meta->columns.a[i].name);

    i64 visible = priv->row_count;
    int off = priv->limit.priv.offset;
    int limv = priv->limit.priv.limit;
    if (off >= visible)
        visible = 0;
    else
        visible -= off;
    if (limv >= 0 && limv < visible)
        visible = limv;
    result->affected = visible;
    result->close = sql_result_close;

    // Cleanup
    for (int i = 0; i < row_count; i++) {
        if (out_rows[i]) {
            out_rows[i]->free(out_rows[i]);
        }
    }
    if (out_rows) FREE(out_rows);
    if (agg) agg->free(agg);
    if (cr) cr->close(cr);
    return result;

EXCEPTION:
    if (agg) agg->free(agg);
    if (cr) cr->close(cr);
    return NULL;
}

static struct flintdb_sql_result * sql_exec_select_groupby_row(struct flintdb_sql *q, struct flintdb_table *table, struct flintdb_cursor_row *cr, struct flintdb_genericfile *gf, char **e);

// (GROUP BY implementation moved earlier; duplicate wrapper & comparator removed)
/* forward declaration updated above */

// Helper: split groupby columns (comma separated) into array
// parse_groupby_columns moved to sql.c (sql_parse_groupby_columns)

// GROUP BY implementation for generic cursor_row (generic files) using new aggregate API
static struct flintdb_sql_result * sql_exec_select_groupby_row(struct flintdb_sql *q, struct flintdb_table *table, struct flintdb_cursor_row *cr, struct flintdb_genericfile *gf, char **e) {
    (void)table;
    struct flintdb_sql_result*result = NULL;
    struct flintdb_aggregate *agg = NULL;
    struct flintdb_aggregate_groupby **groupbys = NULL;
    struct flintdb_aggregate_func **funcs = NULL;
    int groupby_count = 0;
    int aggr_count = 0;
    const struct flintdb_meta *cached_meta = NULL;

    // Parse GROUP BY columns
    char group_cols[MAX_COLUMNS_LIMIT][MAX_COLUMN_NAME_LIMIT];
    groupby_count = sql_parse_groupby_columns(q->groupby, group_cols);

    // Create groupby objects
    groupbys = (struct flintdb_aggregate_groupby **)CALLOC(groupby_count, sizeof(struct flintdb_aggregate_groupby *));
    if (!groupbys && groupby_count > 0)
        THROW(e, "Out of memory allocating groupbys");

    // Get metadata: from table if available, otherwise from genericfile
    if (table) {
        cached_meta = table->meta(table, e);
        if (e && *e) THROW_S(e);
    } else if (gf) {
        cached_meta = gf->meta(gf, e);
        if (e && *e) THROW_S(e);
    }

    // If no metadata yet and we have a cursor, peek at first row to get meta
    if (!cached_meta && cr) {
        struct flintdb_row *first_row = cr->next(cr, e);
        if (first_row) {
            cached_meta = first_row->meta;
            // We need to process this first row, so don't free it yet - handle it in the main loop
            // Put it back somehow or process it immediately
            // For now, let's just get the meta and keep processing
        }
    }

    for (int i = 0; i < groupby_count; i++) {
        // Find column type from metadata
        enum flintdb_variant_type  col_type = VARIANT_STRING; // Default to STRING for genericfiles
        if (cached_meta) {
            for (int j = 0; j < cached_meta->columns.length; j++) {
                if (strcmp(cached_meta->columns.a[j].name, group_cols[i]) == 0) {
                    col_type = cached_meta->columns.a[j].type;
                    break;
                }
            }
        }
        groupbys[i] = groupby_new(group_cols[i], group_cols[i], col_type, e);
        if (e && *e) THROW_S(e);
    }

    // Parse aggregate functions from SELECT
    funcs = (struct flintdb_aggregate_func **)CALLOC(MAX_COLUMNS_LIMIT, sizeof(struct flintdb_aggregate_func *));
    if (!funcs) THROW(e, "Out of memory allocating funcs");

    for (int i = 0; i < q->columns.length; i++) {
        const char *expr = q->columns.name[i];
        int is_group_key = 0;
        for (int g = 0; g < groupby_count; g++)
            if (strcmp(expr, group_cols[g]) == 0) {
                is_group_key = 1;
                break;
            }
        if (is_group_key)
            continue;

        // Parse aggregate function expression
        char func_name[64], col_name[MAX_COLUMN_NAME_LIMIT], alias[MAX_COLUMN_NAME_LIMIT];
        const char *open_paren = strchr(expr, '(');
        const char *close_paren = strrchr(expr, ')');
        if (!open_paren || !close_paren || close_paren <= open_paren + 1)
            THROW(e, "Malformed aggregate expression: %s", expr);

        int fname_len = (int)(open_paren - expr);
        if (fname_len >= (int)sizeof(func_name))
            fname_len = (int)sizeof(func_name) - 1;
        strncpy(func_name, expr, fname_len);
        func_name[fname_len] = '\0';
        // trim spaces
        while (fname_len > 0 && (func_name[fname_len - 1] == ' ' || func_name[fname_len - 1] == '\t'))
            func_name[--fname_len] = '\0';

        int col_len = (int)(close_paren - open_paren - 1);
        if (col_len >= MAX_COLUMN_NAME_LIMIT)
            col_len = MAX_COLUMN_NAME_LIMIT - 1;
        strncpy(col_name, open_paren + 1, col_len);
        col_name[col_len] = '\0';
        // trim leading spaces
        char *sc = col_name;
        while (*sc == ' ' || *sc == '\t')
            sc++;
        if (sc != col_name)
            memmove(col_name, sc, strlen(sc) + 1);

        if (!sql_extract_alias(expr, alias, MAX_COLUMN_NAME_LIMIT)) {
            // Default alias: use the original expression text (trimmed)
            const char *orig = expr;
            while (*orig == ' ' || *orig == '\t')
                orig++;
            size_t a_len = strnlen(orig, MAX_COLUMN_NAME_LIMIT - 1);
            while (a_len > 0 && (orig[a_len - 1] == ' ' || orig[a_len - 1] == '\t'))
                a_len--;
            if (a_len >= MAX_COLUMN_NAME_LIMIT)
                a_len = MAX_COLUMN_NAME_LIMIT - 1;
            memcpy(alias, orig, a_len);
            alias[a_len] = '\0';
        }

        struct flintdb_aggregate_condition cond = {0};
        // Create aggregate_func based on func_name
        if (strcasecmp(func_name, "COUNT") == 0) {
            funcs[aggr_count++] = flintdb_func_count(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "SUM") == 0) {
            funcs[aggr_count++] = flintdb_func_sum(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "AVG") == 0) {
            funcs[aggr_count++] = flintdb_func_avg(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "MIN") == 0) {
            funcs[aggr_count++] = flintdb_func_min(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "MAX") == 0) {
            funcs[aggr_count++] = flintdb_func_max(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "FIRST") == 0) {
            funcs[aggr_count++] = flintdb_func_first(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "LAST") == 0) {
            funcs[aggr_count++] = flintdb_func_last(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "DISTINCT_COUNT") == 0) {
            funcs[aggr_count++] = flintdb_func_distinct_count(col_name, alias, VARIANT_NULL, cond, e);
        } else if (strcasecmp(func_name, "DISTINCT_HLL_COUNT") == 0) {
            funcs[aggr_count++] = flintdb_func_distinct_hll_count(col_name, alias, VARIANT_NULL, cond, e);
        } else {
            THROW(e, "Unknown aggregate function: %s", func_name);
        }

        if (e && *e) THROW_S(e);
    }

    if (aggr_count == 0)
        THROW(e, "No aggregate functions found in SELECT list");

    // Create aggregate
    agg = aggregate_new("sql_groupby_row", groupbys, groupby_count, funcs, aggr_count, e);
    if (e && *e)
        THROW_S(e);

    // Process rows
    struct flintdb_row *r;
    while ((r = cr->next(cr, e)) != NULL) {
        if (e && *e)
            THROW_S(e);

        agg->row(agg, r, e);
        if (e && *e)
            THROW_S(e);
        r->free(r);
    }

    // Compute results
    struct flintdb_row **out_rows = NULL;
    int row_count = agg->compute(agg, &out_rows, e);
    if (e && *e) THROW_S(e);
    
    // Apply HAVING clause filter if present
    if (!strempty(q->having)) {
        row_count = apply_having_filter(out_rows, row_count, q->having, e);
        if (e && *e) THROW_S(e);
    }

    // Build sql_result from computed rows
    struct flintdb_meta *result_meta = out_rows && row_count > 0 ? (struct flintdb_meta *)out_rows[0]->meta : NULL;
    if (!result_meta && row_count > 0)
        THROW(e, "Missing result metadata");

    // Fast path: no ORDER BY and no LIMIT -> avoid filesort allocation
    if (row_count >= 0 && strempty(q->orderby) && strempty(q->limit)) {
        struct flintdb_sql_result*fast = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
        if (!fast) THROW(e, "Out of memory");
        struct list *rows_list = arraylist_new(row_count > 0 ? row_count : 8);
        for (int i = 0; i < row_count; i++) {
            rows_list->add(rows_list, (valtype)(uintptr_t)out_rows[i], list_row_dealloc, e);
            if (e && *e) THROW_S(e);
        }
        if (out_rows)
            FREE(out_rows);
        out_rows = NULL;

        struct flintdb_cursor_array_priv *apriv = CALLOC(1, sizeof(struct flintdb_cursor_array_priv));
        if (!apriv) THROW(e, "Out of memory");
        apriv->rows = rows_list;
        apriv->index = 0;
        apriv->meta = NULL;
        struct flintdb_cursor_row *ac = CALLOC(1, sizeof(struct flintdb_cursor_row));
        if (!ac) THROW(e, "Out of memory");
        ac->p = apriv;
        ac->next = array_cursor_next;
        ac->close = array_cursor_close;

        fast->row_cursor = ac;
        fast->column_count = result_meta ? result_meta->columns.length : 0;
        fast->column_names = (char **)CALLOC(fast->column_count, sizeof(char *));
        if (!fast->column_names && fast->column_count > 0)
            THROW(e, "Out of memory");
        for (int i = 0; i < fast->column_count; i++)
            fast->column_names[i] = STRDUP(result_meta->columns.a[i].name);
        fast->affected = row_count;
        fast->close = sql_result_close;

        if (agg)
            agg->free(agg);
        return fast;
    }

    char temp_file[512];
    snprintf(temp_file, sizeof(temp_file), "%s/flintdb_sort_%ld.tmp", ensure_temp_dir(), (long)time(NULL));
    struct flintdb_filesort *sorter = flintdb_filesort_new(temp_file, result_meta, e);
    if (e && *e)
        THROW_S(e);

    for (int i = 0; i < row_count; i++) {
        sorter->add(sorter, out_rows[i], e);
        if (e && *e)
            THROW_S(e);
    }

    // Apply ORDER BY if specified
    if (!strempty(q->orderby)) {
        char order_cols[MAX_COLUMNS_LIMIT][MAX_COLUMN_NAME_LIMIT];
        i8 desc_flags[MAX_COLUMNS_LIMIT];
        int ocnt = 0;
        sql_parse_orderby_clause(q->orderby, order_cols, desc_flags, &ocnt);
        if (ocnt <= 0)
            THROW(e, "Failed to parse ORDER BY clause");
        struct sort_multi_context sc;
        memset(&sc, 0, sizeof(sc));
        sc.count = ocnt;
        for (int i = 0; i < ocnt; i++) {
            int order_idx = flintdb_column_at(result_meta, order_cols[i]);
            if (order_idx < 0)
                THROW(e, "ORDER BY column not found: %s", order_cols[i]);
            sc.specs[i].col_idx = order_idx;
            sc.specs[i].descending = desc_flags[i];
        }
        sorter->sort(sorter, sort_row_multi_comparator, &sc, e);
        if (e && *e) THROW_S(e);
    }

    // Build result cursor
    struct flintdb_filesort_cursor_priv *priv = CALLOC(1, sizeof(struct flintdb_filesort_cursor_priv));
    if (!priv) THROW(e, "Out of memory");
    priv->sorter = sorter;
    priv->current_idx = 0;
    priv->row_count = sorter->rows(sorter);
    priv->limit = !strempty(q->limit) ? limit_parse(q->limit) : NOLIMIT;

    struct flintdb_cursor_row *wrapped_cursor = CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!wrapped_cursor) THROW(e, "Out of memory");
    wrapped_cursor->p = priv;
    wrapped_cursor->next = filesort_cursor_next;
    wrapped_cursor->close = filesort_cursor_close;

    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");
    result->row_cursor = wrapped_cursor;
    result->column_count = result_meta ? result_meta->columns.length : 0;
    result->column_names = (char **)CALLOC(result->column_count, sizeof(char *));
    if (!result->column_names && result->column_count > 0)
        THROW(e, "Out of memory");
    for (int i = 0; i < result->column_count; i++)
        result->column_names[i] = STRDUP(result_meta->columns.a[i].name);

    i64 visible = priv->row_count;
    int off = priv->limit.priv.offset;
    int limv = priv->limit.priv.limit;
    if (off >= visible)
        visible = 0;
    else
        visible -= off;
    if (limv >= 0 && limv < visible)
        visible = limv;
    result->affected = visible;
    result->close = sql_result_close;

    // Cleanup
    for (int i = 0; i < row_count; i++)
        if (out_rows[i])
            out_rows[i]->free(out_rows[i]);
    if (out_rows) FREE(out_rows);

    if (agg) agg->free(agg);
    return result;

EXCEPTION:
    if (agg) agg->free(agg);
    return NULL;
}

static struct flintdb_sql_result * sql_exec_sort(struct flintdb_cursor_row *cr, const char *orderby, const char *limit, char **e) {
    struct flintdb_sql_result*result = NULL;
    struct flintdb_filesort *sorter = NULL;
    if (!cr || strempty(orderby))
        THROW(e, "Invalid cursor or ORDER BY clause");
    struct flintdb_row *first = cr->next(cr, e);
    if (e && *e)
        THROW_S(e);
    if (!first) {
        result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
        if (!result) THROW(e, "Out of memory");
        result->affected = 0;
        result->close = sql_result_close;
        return result;
    }
    const struct flintdb_meta *src_meta = first->meta;
    if (!src_meta) THROW(e, "Row has no metadata");

    char temp_file[512];
    snprintf(temp_file, sizeof(temp_file), "%s/flintdb_sort_%ld.tmp", ensure_temp_dir(), (long)time(NULL));
    sorter = flintdb_filesort_new(temp_file, (struct flintdb_meta *)src_meta, e);
    if (e && *e) THROW_S(e);
    sorter->add(sorter, first, e);
    if (e && *e) THROW_S(e);
    for (struct flintdb_row *row; (row = cr->next(cr, e)) != NULL;) {
        if (e && *e) THROW_S(e);
        sorter->add(sorter, row, e);
        if (e && *e) THROW_S(e);
    }
    char order_cols[MAX_COLUMNS_LIMIT][MAX_COLUMN_NAME_LIMIT];
    i8 desc_flags[MAX_COLUMNS_LIMIT];
    int ocnt = 0;
    sql_parse_orderby_clause(orderby, order_cols, desc_flags, &ocnt);
    if (ocnt <= 0)
        THROW(e, "Failed to parse ORDER BY clause");
    struct sort_multi_context sc;
    memset(&sc, 0, sizeof(sc));
    sc.count = ocnt;
    for (int i = 0; i < ocnt; i++) {
        int idx = flintdb_column_at((struct flintdb_meta *)src_meta, order_cols[i]);
        if (idx < 0)
            THROW(e, "Sort column not found: %s", order_cols[i]);
        sc.specs[i].col_idx = idx;
        sc.specs[i].descending = desc_flags[i];
    }
    sorter->sort(sorter, sort_row_multi_comparator, &sc, e);
    if (e && *e) THROW_S(e);
    struct flintdb_filesort_cursor_priv *priv = CALLOC(1, sizeof(struct flintdb_filesort_cursor_priv));
    if (!priv) THROW(e, "Out of memory");
    priv->sorter = sorter;
    priv->current_idx = 0;
    priv->row_count = sorter->rows(sorter);
    priv->limit = !strempty(limit) ? limit_parse(limit) : NOLIMIT;
    struct flintdb_cursor_row *wrapped = CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!wrapped) THROW(e, "Out of memory");
    wrapped->p = priv;
    wrapped->next = filesort_cursor_next;
    wrapped->close = filesort_cursor_close;
    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");
    result->row_cursor = wrapped;
    result->column_count = src_meta->columns.length;
    result->column_names = (char **)CALLOC(result->column_count, sizeof(char *));
    if (!result->column_names)
        THROW(e, "Out of memory");
    for (int i = 0; i < result->column_count; i++)
        result->column_names[i] = STRDUP(src_meta->columns.a[i].name);
    i64 visible = priv->row_count;
    int off = priv->limit.priv.offset;
    int limv = priv->limit.priv.limit;
    if (off >= visible)
        visible = 0;
    else
        visible -= off;
    if (limv >= 0 && limv < visible)
        visible = limv;
    result->affected = visible;
    result->close = sql_result_close;
    if (cr)
        cr->close(cr);
    return result;
EXCEPTION:
    if (sorter)
        sorter->close(sorter);
    if (cr)
        cr->close(cr);
    return NULL;
}


static struct flintdb_sql_result * sql_exec_begin_transaction(struct flintdb_sql *q, struct flintdb_transaction *t, char **e) {
    struct flintdb_table *table = NULL;
    struct flintdb_sql_result *result = NULL;

    if (t) t->close(t); // close existing transaction if any
    if (strempty(q->table)) THROW(e, "Table name required for BEGIN TRANSACTION");

    table = sql_exec_table_borrow(q->table, e);
    if (!table) THROW_S(e);

    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");

    result->affected = 1; // indicate success
    result->close = sql_result_close;
    result->transaction = flintdb_transaction_begin(table, e);
    if (e && *e) THROW_S(e);
    
    return result;
EXCEPTION:
    if (result) FREE(result);
    return NULL;
}

static struct flintdb_sql_result * sql_exec_commit_transaction(struct flintdb_sql *q, struct flintdb_transaction *t, char **e) {
    struct flintdb_sql_result *result = NULL;

    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result)  THROW(e, "Out of memory");

    t->commit(t, e);
    t->close(t);
    result->affected = (e && *e) ? 0 : 1; // indicate success or failure
    result->close = sql_result_close;
    return result;

EXCEPTION:
    if (result) FREE(result);
    return NULL;
}

static struct flintdb_sql_result * sql_exec_rollback_transaction(struct flintdb_sql *q, struct flintdb_transaction *t, char **e) {
    struct flintdb_sql_result *result = NULL;

    result = (struct flintdb_sql_result*)CALLOC(1, sizeof(struct flintdb_sql_result));
    if (!result) THROW(e, "Out of memory");

    t->rollback(t, e);
    t->close(t);
    result->affected = (e && *e) ? 0 : 1; // indicate success or failure
    result->close = sql_result_close;
    return result;

EXCEPTION:
    if (result) FREE(result);
    return NULL;
}