#include "flintdb_swift_shim.h"

// Implementation includes the full API.
#include "../../src/flintdb.h"

#include <string.h>

// Meta
struct flintdb_meta * flintdb_swift_meta_new(const char *name, char **e) {
    return flintdb_meta_new_ptr(name, e);
}

struct flintdb_meta * flintdb_swift_meta_open(const char *filename, char **e) {
    return flintdb_meta_open_ptr(filename, e);
}

void flintdb_swift_meta_free(struct flintdb_meta *m) {
    flintdb_meta_free_ptr(m);
}

int flintdb_swift_meta_to_sql_string(const struct flintdb_meta *m, char *s, int32_t len, char **e) {
    return flintdb_meta_to_sql_string(m, s, (i32)len, e);
}

void flintdb_swift_meta_columns_add(struct flintdb_meta *m,
                                    const char *name,
                                    int32_t type,
                                    int32_t bytes,
                                    int16_t precision,
                                    int32_t nullspec,
                                    const char *value,
                                    const char *comment,
                                    char **e) {
    flintdb_meta_columns_add(m,
                            name,
                            (enum flintdb_variant_type)type,
                            (i32)bytes,
                            (i16)precision,
                            (enum flintdb_null_spec)nullspec,
                            value,
                            comment,
                            e);
}

void flintdb_swift_meta_indexes_add(struct flintdb_meta *m,
                                    const char *name,
                                    const char *algorithm,
                                    const char **keys,
                                    uint16_t key_count,
                                    char **e) {
    if (!m || !name || !keys || key_count == 0) {
        return;
    }
    if (key_count > MAX_INDEX_KEYS_LIMIT) {
        key_count = MAX_INDEX_KEYS_LIMIT;
    }

    char tmp[MAX_INDEX_KEYS_LIMIT][MAX_COLUMN_NAME_LIMIT];
    memset(tmp, 0, sizeof(tmp));

    for (uint16_t i = 0; i < key_count; i++) {
        const char *k = keys[i];
        if (!k) k = "";
        strncpy(tmp[i], k, MAX_COLUMN_NAME_LIMIT - 1);
    }

    flintdb_meta_indexes_add(m,
                            name,
                            algorithm,
                            (const char (*)[MAX_COLUMN_NAME_LIMIT])tmp,
                            (u16)key_count,
                            e);
}

void flintdb_swift_meta_set_cache(struct flintdb_meta *m, int32_t cache_bytes) {
    if (!m) return;
    m->cache = (i32)cache_bytes;
}

int32_t flintdb_swift_column_at(struct flintdb_meta *m, const char *name) {
    return (int32_t)flintdb_column_at(m, name);
}

void flintdb_swift_meta_set_text_format(struct flintdb_meta *m,
                                        int8_t absent_header,
                                        char delimiter,
                                        const char *format) {
    if (!m) return;

    m->absent_header = absent_header;
    m->delimiter = delimiter;

    if (format) {
        memset(m->format, 0, sizeof(m->format));
        strncpy(m->format, format, sizeof(m->format) - 1);
    }
}

// SQL
struct flintdb_sql * flintdb_swift_sql_parse(const char *sql, char **e) {
    return flintdb_sql_parse(sql, e);
}

void flintdb_swift_sql_free(struct flintdb_sql *s) {
    flintdb_sql_free(s);
}

int flintdb_swift_sql_to_meta(struct flintdb_sql *in, struct flintdb_meta *out, char **e) {
    return flintdb_sql_to_meta(in, out, e);
}

// Row
struct flintdb_row * flintdb_swift_row_new(struct flintdb_meta *meta, char **e) {
    return flintdb_row_new(meta, e);
}

void flintdb_swift_row_free(struct flintdb_row *r) {
    if (!r) return;
    if (r->free) r->free(r);
}

int8_t flintdb_swift_row_validate(const struct flintdb_row *r, char **e) {
    if (!r || !r->validate) return 0;
    return r->validate(r, e);
}

void flintdb_swift_row_set_i64(struct flintdb_row *r, uint16_t i, int64_t v, char **e) {
    if (!r || !r->i64_set) return;
    r->i64_set(r, (u16)i, (i64)v, e);
}

void flintdb_swift_row_set_i32(struct flintdb_row *r, uint16_t i, int32_t v, char **e) {
    if (!r || !r->i32_set) return;
    r->i32_set(r, (u16)i, (i32)v, e);
}

void flintdb_swift_row_set_f64(struct flintdb_row *r, uint16_t i, double v, char **e) {
    if (!r || !r->f64_set) return;
    r->f64_set(r, (u16)i, (f64)v, e);
}

void flintdb_swift_row_set_string(struct flintdb_row *r, uint16_t i, const char *s, char **e) {
    if (!r || !r->string_set) return;
    r->string_set(r, (u16)i, s, e);
}

void flintdb_swift_print_row(const struct flintdb_row *r) {
    flintdb_print_row(r);
}

// Table
struct flintdb_table * flintdb_swift_table_open_rdonly(const char *file, char **e) {
    return flintdb_table_open(file, FLINTDB_RDONLY, NULL, e);
}

struct flintdb_table * flintdb_swift_table_open_rdwr(const char *file, const struct flintdb_meta *meta, char **e) {
    return flintdb_table_open(file, FLINTDB_RDWR, meta, e);
}

int flintdb_swift_table_drop(const char *file, char **e) {
    return flintdb_table_drop(file, e);
}

void flintdb_swift_table_close(struct flintdb_table *t) {
    if (!t) return;
    if (t->close) t->close(t);
}

int64_t flintdb_swift_table_apply(struct flintdb_table *t, struct flintdb_row *r, int8_t upsert, char **e) {
    if (!t || !t->apply) return -1;
    return (int64_t)t->apply(t, r, upsert, e);
}

int64_t flintdb_swift_table_apply_at(struct flintdb_table *t, int64_t rowid, struct flintdb_row *r, char **e) {
    if (!t || !t->apply_at) return -1;
    return (int64_t)t->apply_at(t, (i64)rowid, r, e);
}

int64_t flintdb_swift_table_delete_at(struct flintdb_table *t, int64_t rowid, char **e) {
    if (!t || !t->delete_at) return -1;
    return (int64_t)t->delete_at(t, (i64)rowid, e);
}

struct flintdb_cursor_i64 * flintdb_swift_table_find(const struct flintdb_table *t, const char *where, char **e) {
    if (!t || !t->find) return NULL;
    return t->find(t, where, e);
}

const struct flintdb_row * flintdb_swift_table_read(struct flintdb_table *t, int64_t rowid, char **e) {
    if (!t || !t->read) return NULL;
    return t->read(t, (i64)rowid, e);
}

// Cursor i64
int64_t flintdb_swift_cursor_i64_next(struct flintdb_cursor_i64 *c, char **e) {
    if (!c || !c->next) return -1;
    return (int64_t)c->next(c, e);
}

void flintdb_swift_cursor_i64_close(struct flintdb_cursor_i64 *c) {
    if (!c) return;
    if (c->close) c->close(c);
}

// Generic file
struct flintdb_genericfile * flintdb_swift_genericfile_open_rdonly(const char *file, char **e) {
    return flintdb_genericfile_open(file, FLINTDB_RDONLY, NULL, e);
}

struct flintdb_genericfile * flintdb_swift_genericfile_open_rdwr(const char *file, const struct flintdb_meta *meta, char **e) {
    return flintdb_genericfile_open(file, FLINTDB_RDWR, meta, e);
}

void flintdb_swift_genericfile_drop(const char *file, char **e) {
    flintdb_genericfile_drop(file, e);
}

void flintdb_swift_genericfile_close(struct flintdb_genericfile *f) {
    if (!f) return;
    if (f->close) f->close(f);
}

int64_t flintdb_swift_genericfile_write(struct flintdb_genericfile *f, struct flintdb_row *r, char **e) {
    if (!f || !f->write) return -1;
    return (int64_t)f->write(f, r, e);
}

struct flintdb_cursor_row * flintdb_swift_genericfile_find(const struct flintdb_genericfile *f, const char *where, char **e) {
    if (!f || !f->find) return NULL;
    return f->find(f, where, e);
}

// Cursor row
struct flintdb_row * flintdb_swift_cursor_row_next(struct flintdb_cursor_row *c, char **e) {
    if (!c || !c->next) return NULL;
    return c->next(c, e);
}

void flintdb_swift_cursor_row_close(struct flintdb_cursor_row *c) {
    if (!c) return;
    if (c->close) c->close(c);
}
