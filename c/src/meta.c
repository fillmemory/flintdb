#include "flintdb.h"
#include "runtime.h"
#include "sql.h"
#include "hashmap.h"
#include "allocator.h"
#include "internal.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>

// TODO: check overflow for columns/indexes

enum fileformat detect_file_format(const char *name) {
    // Binary table format
    if (suffix(name, TABLE_NAME_SUFFIX))
        return FORMAT_BIN;
    
    // Simple detection based on file extension
    // Support TPC-H style pipe-delimited files with .tbl/.tbl.gz
    if (suffix(name, ".tbl.gz"))
        return FORMAT_TSV;
    if (suffix(name, ".tbl"))
        return FORMAT_TSV;
    if (suffix(name, ".tsv.gz"))
        return FORMAT_TSV;
    if (suffix(name, ".csv.gz"))
        return FORMAT_CSV;
    if (suffix(name, ".tsv"))
        return FORMAT_TSV;
    if (suffix(name, ".csv"))
        return FORMAT_CSV;
    if (suffix(name, ".jsonl.gz")) // managed by jsonl plugin
        return FORMAT_JSONL;
    if (suffix(name, ".ndjson.gz")) // managed by jsonl plugin
        return FORMAT_JSONL;
    if (suffix(name, ".jsonl")) // managed by jsonl plugin
        return FORMAT_JSONL;
    if (suffix(name, ".ndjson")) // managed by jsonl plugin
        return FORMAT_JSONL;
    if (suffix(name, ".parquet")) // managed by parquet plugin
        return FORMAT_PARQUET;
    return FORMAT_UNKNOWN;
}

struct flintdb_meta flintdb_meta_open(const char *filename, char **e) {
    struct flintdb_sql *q = NULL; 
    struct flintdb_meta m;
    memset(&m, 0, sizeof(struct flintdb_meta));
    // Parse SQL from file into sql_context, then convert to meta

    char pathbuf[1024] = {0};
    if (filename) 
        strncpy(pathbuf, filename, sizeof(pathbuf) - 1);

    q = flintdb_sql_from_file(pathbuf, e);
    if (e && *e) THROW_S(e);

    int ok = flintdb_sql_to_meta(q, &m, e);
    if (ok != 0) THROW_S(e);

    flintdb_sql_free(q);
    return m;

EXCEPTION:
    if (q) flintdb_sql_free(q);
    return m;
}

struct flintdb_meta flintdb_meta_new(const char *name, char **e) {
    struct flintdb_meta m;
    memset(&m, 0, sizeof(struct flintdb_meta));
    m.version = 1.0;

    if (name) {
        size_t name_len = strlen(name);
        if (name_len >= (sizeof(m.name) - 1)) 
            THROW(e, "table name too long (%zu bytes, max: %zu)", name_len, sizeof(m.name) - 1);
    }
    
    if (name)
        strncpy(m.name, name, sizeof(m.name) - 1);

    time_t now = time(NULL);
    strftime(m.date, sizeof(m.date), "%Y-%m-%d", localtime(&now));
    m.compact = -1;
    // strncpy(m.compressor, "none", sizeof(m.compressor) - 1);
    // strncpy(m.storage, "mmap", sizeof(m.storage) - 1);
    // leave unset by default; storage/table will choose a sensible default increment
    m.increment = 0;
    m.cache = 1024 * 1024; // 1Million rows
    m.delimiter = '\t';
    m.quote = '"';
    // WAL defaults: keep page images enabled unless explicitly disabled
    m.wal_page_data = 1;
    return m;

EXCEPTION:
    memset(&m, 0, sizeof(struct flintdb_meta));
    return m;
}

void flintdb_meta_close(struct flintdb_meta *m) {
    if (!m) return;
    if (!m->priv) return;
    
    // Free hashmap - this will automatically call meta_column_cache_free for each entry
    struct hashmap *map = (struct hashmap *)m->priv;
    map->free(map);
    m->priv = NULL;
}

struct flintdb_meta* flintdb_meta_new_ptr(const char *name, char **e) {
    // Allocate and zero-initialize on heap first
    struct flintdb_meta *ptr = CALLOC(1, sizeof(struct flintdb_meta));
    if (!ptr) {
        if (e) *e = "Out of memory";
        return NULL;
    }
    
    // Initialize by calling flintdb_meta_new and assigning result
    *ptr = flintdb_meta_new(name, e);
    if (e && *e) {
        free(ptr);
        return NULL;
    }
    
    return ptr;
}

struct flintdb_meta* flintdb_meta_open_ptr(const char *filename, char **e) {
    // Allocate and zero-initialize on heap first
    struct flintdb_meta *ptr = CALLOC(1, sizeof(struct flintdb_meta));
    if (!ptr) {
        if (e) *e = "Out of memory";
        return NULL;
    }
    
    // Initialize by calling flintdb_meta_open and assigning result
    *ptr = flintdb_meta_open(filename, e);
    if (e && *e) {
        free(ptr);
        return NULL;
    }
    
    return ptr;
}

void flintdb_meta_free_ptr(struct flintdb_meta *m) {
    if (m) {
        flintdb_meta_close(m);
        FREE(m);
    }
}


int flintdb_meta_write(const struct flintdb_meta *m, const char *filename, char **e) {
    int fd = -1;
    if (!m || !filename)  THROW(e, "meta or filename is NULL");
    
    char sql[SQL_STRING_LIMIT] = {0};
    int ok = flintdb_meta_to_sql_string(m, sql, sizeof(sql), e);
    if (ok != 0) THROW_S(e);

    fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (fd < 0) THROW(e, "failed to open file for writing (errno : %d, %s,)", errno, strerror(errno));

    ssize_t written = write(fd, sql, strlen(sql));
    if (written != (ssize_t)strlen(sql)) THROW(e, "failed to write complete SQL string to file (errno : %d, %s,)", errno, strerror(errno));

    close(fd);
    return 0;

EXCEPTION:
    if (fd >= 0) close(fd);
    return -1;
}

void flintdb_meta_columns_add(struct flintdb_meta *m, const char *name, enum flintdb_variant_type  type, i32 bytes, i16 precision, enum flintdb_null_spec nullspec, const char *value, const char *comment, char **e) {
    if (!m || m->columns.length >= MAX_COLUMNS_LIMIT) 
        THROW(e, "meta is NULL or maximum columns limit reached");
    
    if (!name)
        THROW(e, "column name is NULL");
    
    size_t name_len = strlen(name);
    if (name_len >= MAX_COLUMN_NAME_LIMIT)
        THROW(e, "column name too long (%zu bytes, max: %d)", name_len, MAX_COLUMN_NAME_LIMIT - 1);
        
    struct flintdb_column *col = &m->columns.a[m->columns.length];
    memset(col, 0, sizeof(struct flintdb_column));
    strncpy(col->name, name, sizeof(col->name) - 1);
    col->type = type;
    col->bytes = bytes;
    col->precision = precision;
    col->nullspec = nullspec;
    
    if (value) {
        size_t value_len = strlen(value);
        if (value_len >= sizeof(col->value))
            THROW(e, "column default value too long (%zu bytes, max: %zu)", value_len, sizeof(col->value) - 1);
        strncpy(col->value, value, sizeof(col->value) - 1);
    }
    
    if (comment) {
        size_t comment_len = strlen(comment);
        if (comment_len >= sizeof(col->comment))
            THROW(e, "column comment too long (%zu bytes, max: %zu)", comment_len, sizeof(col->comment) - 1);
        strncpy(col->comment, comment, sizeof(col->comment) - 1);
    }
    m->columns.length++;

EXCEPTION:
    return;
}

void flintdb_meta_indexes_add(struct flintdb_meta *m, const char *name, const char *algorithm, const char keys[][MAX_COLUMN_NAME_LIMIT], u16 key_count, char **e) {
    if (!m || m->indexes.length >= MAX_INDEX_KEYS_LIMIT) 
        THROW(e, "meta is NULL or maximum indexes limit reached");
    
    if (!name)
        THROW(e, "index name is NULL");
    
    size_t name_len = strlen(name);
    if (name_len >= MAX_COLUMN_NAME_LIMIT)
        THROW(e, "index name too long (%zu bytes, max: %d)", name_len, MAX_COLUMN_NAME_LIMIT - 1);
 
    if (key_count <= 0 || key_count > MAX_INDEX_KEYS_LIMIT) 
        THROW(e, "invalid key count for index");

    int i = m->indexes.length;
    struct flintdb_index *idx = &m->indexes.a[i];
    memset(idx, 0, sizeof(struct flintdb_index));
    strncpy(idx->name, name, sizeof(idx->name) - 1);
    strncpy(idx->type, strncasecmp(PRIMARY_NAME, name, strlen(PRIMARY_NAME)) == 0 ? "primary" : "sort", sizeof(idx->type) - 1);
    
    if (algorithm && algorithm[0]) {
        size_t algo_len = strlen(algorithm);
        if (algo_len >= sizeof(idx->algorithm))
            THROW(e, "index algorithm name too long (%zu bytes, max: %zu)", algo_len, sizeof(idx->algorithm) - 1);
        strncpy(idx->algorithm, algorithm, sizeof(idx->algorithm) - 1);
    } else {
        strncpy(idx->algorithm, "bptree", sizeof(idx->algorithm) - 1); // algorithm reserved for future
    }
    
    for (int j = 0; j < key_count; j++) {
        size_t key_len = strlen(keys[j]);
        if (key_len >= MAX_COLUMN_NAME_LIMIT)
            THROW(e, "index key name too long (%zu bytes, max: %d)", key_len, MAX_COLUMN_NAME_LIMIT - 1);
        strncpy(idx->keys.a[j], keys[j], sizeof(idx->keys.a[j]) - 1);
    }
    idx->keys.length = key_count;
    m->indexes.length++;

EXCEPTION:
    return;
}

void meta_column_cache_free(keytype k, valtype v) {
    if (k) FREE((void*)(uintptr_t)k);
    // v is an integer index, no need to free
}

// HOT_PATH
int flintdb_column_at(struct flintdb_meta *m, const char *name) {
    if (!m || !name) return -1;

    if (!m->priv) {
        // Small name->index cache using flat open-addressing backend
        struct hashmap *map = hashmap_new(256, &hashmap_string_case_hash, &hashmap_string_case_cmpr);
        assert(map != NULL);
        assert(m->columns.length);
        assert(m->columns.length <= MAX_COLUMNS_LIMIT);
        m->priv = map;
        for(int i = 0; i < m->columns.length; i++) {
            DEBUG("caching column name='%s' at index %d", m->columns.a[i].name, i);
            map->put(map, (keytype)(uintptr_t)STRDUP(m->columns.a[i].name), (valtype)(uintptr_t)i, meta_column_cache_free);
        }
    }

    assert(m->priv != NULL);
    struct hashmap *map = (struct hashmap *)m->priv;
    valtype v = map->get(map, (keytype)(uintptr_t)name);
    if (v != HASHMAP_INVALID_VAL) {
        return (int)(uintptr_t)v;
    }
    return -1; // not found
}

int flintdb_meta_compare(const struct flintdb_meta *a, const struct flintdb_meta *b) {
    if (!a || !b) return -1;
    if (a->columns.length != b->columns.length) return -1;
    for(int i = 0; i < a->columns.length; i++) {
        const struct flintdb_column *ca = &a->columns.a[i];
        const struct flintdb_column *cb = &b->columns.a[i];
        if (strncasecmp(ca->name, cb->name, MAX_COLUMN_NAME_LIMIT) != 0) return -1;
        if (ca->type != cb->type) return -1;
        if (ca->bytes != cb->bytes) return -1;
        if (ca->precision != cb->precision) return -1;
    }
    if (a->indexes.length != b->indexes.length) return -1;
    for(int i = 0; i < a->indexes.length; i++) {
        const struct flintdb_index *ia = &a->indexes.a[i];
        const struct flintdb_index *ib = &b->indexes.a[i];
        if (strncasecmp(ia->name, ib->name, MAX_COLUMN_NAME_LIMIT) != 0) return -1;
        if (ia->keys.length != ib->keys.length) return -1;
        for(int j = 0; j < ia->keys.length; j++) {
            if (strncasecmp(ia->keys.a[j], ib->keys.a[j], MAX_COLUMN_NAME_LIMIT) != 0) return -1;
        }
    }
    return 0; // equal
}