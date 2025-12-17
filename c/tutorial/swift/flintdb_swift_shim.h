#ifndef FLINTDB_SWIFT_SHIM_H
#define FLINTDB_SWIFT_SHIM_H

// A tiny C surface area for Swift.
//
// Motivation:
// - Importing the full `flintdb.h` into Swift can be slow because it contains
//   large structs with many function pointers.
// - This shim keeps Swift-facing headers small by forward-declaring FlintDB
//   structs and wrapping function-pointer calls into plain C functions.
//
// The implementation (`flintdb_swift_shim.c`) includes `flintdb.h`.

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque types for Swift
struct flintdb_meta;
struct flintdb_row;
struct flintdb_table;
struct flintdb_genericfile;
struct flintdb_cursor_i64;
struct flintdb_cursor_row;
struct flintdb_sql;

// Minimal constants Swift needs
#define FLINTDB_SWIFT_VARIANT_INT32 2
#define FLINTDB_SWIFT_VARIANT_INT64 8
#define FLINTDB_SWIFT_VARIANT_DOUBLE 9
#define FLINTDB_SWIFT_VARIANT_STRING 11

#define FLINTDB_SWIFT_SPEC_NULLABLE 0
#define FLINTDB_SWIFT_SPEC_NOT_NULL 1

// Meta (pointer-based for bindings)
struct flintdb_meta * flintdb_swift_meta_new(const char *name, char **e);
struct flintdb_meta * flintdb_swift_meta_open(const char *filename, char **e);
void flintdb_swift_meta_free(struct flintdb_meta *m);

int flintdb_swift_meta_to_sql_string(const struct flintdb_meta *m, char *s, int32_t len, char **e);
void flintdb_swift_meta_columns_add(struct flintdb_meta *m,
                                    const char *name,
                                    int32_t type,
                                    int32_t bytes,
                                    int16_t precision,
                                    int32_t nullspec,
                                    const char *value,
                                    const char *comment,
                                    char **e);

// Index helpers
// - algorithm can be NULL
// - keys is an array of C strings with length key_count (<= 5)
void flintdb_swift_meta_indexes_add(struct flintdb_meta *m,
                                    const char *name,
                                    const char *algorithm,
                                    const char **keys,
                                    uint16_t key_count,
                                    char **e);

// Common meta setters
void flintdb_swift_meta_set_cache(struct flintdb_meta *m, int32_t cache_bytes);

int32_t flintdb_swift_column_at(struct flintdb_meta *m, const char *name);

// Convenience setters for delimited text meta (TSV/CSV)
void flintdb_swift_meta_set_text_format(struct flintdb_meta *m,
                                        int8_t absent_header,
                                        char delimiter,
                                        const char *format);

// SQL -> meta
struct flintdb_sql * flintdb_swift_sql_parse(const char *sql, char **e);
void flintdb_swift_sql_free(struct flintdb_sql *s);
int flintdb_swift_sql_to_meta(struct flintdb_sql *in, struct flintdb_meta *out, char **e);

// Row
struct flintdb_row * flintdb_swift_row_new(struct flintdb_meta *meta, char **e);
void flintdb_swift_row_free(struct flintdb_row *r);
int8_t flintdb_swift_row_validate(const struct flintdb_row *r, char **e);

void flintdb_swift_row_set_i64(struct flintdb_row *r, uint16_t i, int64_t v, char **e);
void flintdb_swift_row_set_i32(struct flintdb_row *r, uint16_t i, int32_t v, char **e);
void flintdb_swift_row_set_f64(struct flintdb_row *r, uint16_t i, double v, char **e);
void flintdb_swift_row_set_string(struct flintdb_row *r, uint16_t i, const char *s, char **e);

void flintdb_swift_print_row(const struct flintdb_row *r);

// Table
struct flintdb_table * flintdb_swift_table_open_rdonly(const char *file, char **e);
struct flintdb_table * flintdb_swift_table_open_rdwr(const char *file, const struct flintdb_meta *meta, char **e);
int flintdb_swift_table_drop(const char *file, char **e);
void flintdb_swift_table_close(struct flintdb_table *t);

int64_t flintdb_swift_table_apply(struct flintdb_table *t, struct flintdb_row *r, int8_t upsert, char **e);
int64_t flintdb_swift_table_apply_at(struct flintdb_table *t, int64_t rowid, struct flintdb_row *r, char **e);
int64_t flintdb_swift_table_delete_at(struct flintdb_table *t, int64_t rowid, char **e);
struct flintdb_cursor_i64 * flintdb_swift_table_find(const struct flintdb_table *t, const char *where, char **e);
const struct flintdb_row * flintdb_swift_table_read(struct flintdb_table *t, int64_t rowid, char **e);

// Cursor i64
int64_t flintdb_swift_cursor_i64_next(struct flintdb_cursor_i64 *c, char **e);
void flintdb_swift_cursor_i64_close(struct flintdb_cursor_i64 *c);

// Generic file
struct flintdb_genericfile * flintdb_swift_genericfile_open_rdonly(const char *file, char **e);
struct flintdb_genericfile * flintdb_swift_genericfile_open_rdwr(const char *file, const struct flintdb_meta *meta, char **e);
void flintdb_swift_genericfile_drop(const char *file, char **e);
void flintdb_swift_genericfile_close(struct flintdb_genericfile *f);

int64_t flintdb_swift_genericfile_write(struct flintdb_genericfile *f, struct flintdb_row *r, char **e);
struct flintdb_cursor_row * flintdb_swift_genericfile_find(const struct flintdb_genericfile *f, const char *where, char **e);

// Cursor row (BORROWED rows)
struct flintdb_row * flintdb_swift_cursor_row_next(struct flintdb_cursor_row *c, char **e);
void flintdb_swift_cursor_row_close(struct flintdb_cursor_row *c);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // FLINTDB_SWIFT_SHIM_H
