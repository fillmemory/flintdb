/**
 * @file flintdb.h
 * @brief B+Tree-based table and TSV/CSV/JSONL/Parquet/... various data file format manipulation library
 * @author Yongho Kim
 * @version 0.0.1
 * @date 2025-09-09
 */
#ifndef FLINTDB_H
#define FLINTDB_H

#include "types.h"
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>


#ifdef __cplusplus
#  define FLINTDB_EXTERN_C extern "C"
#else
#  define FLINTDB_EXTERN_C
#endif

// FLINTDB_API marks public symbols for .so/.dylib/.dll
// Define FLINTDB_SHARED when building/using the shared library
// Define FLINTDB_BUILD when building the library itself
#if defined(_WIN32) || defined(__CYGWIN__)
#  ifdef FLINTDB_SHARED
#    ifdef FLINTDB_BUILD
#      define FLINTDB_API __declspec(dllexport)
#    else
#      define FLINTDB_API __declspec(dllimport)
#    endif
#  else
#    define FLINTDB_API
#  endif
#  define FLINTDB_LOCAL
#else
#  if __GNUC__ >= 4
#    ifdef FLINTDB_SHARED
#      define FLINTDB_API __attribute__((visibility("default")))
#      define FLINTDB_LOCAL __attribute__((visibility("hidden")))
#    else
#      define FLINTDB_API
#      define FLINTDB_LOCAL
#    endif
#  else
#    define FLINTDB_API
#    define FLINTDB_LOCAL
#  endif
#endif

#ifdef __cplusplus
#  define FLINTDB_BEGIN_DECLS extern "C" {
#  define FLINTDB_END_DECLS }
#else
#  define FLINTDB_BEGIN_DECLS
#  define FLINTDB_END_DECLS
#endif

#define FLINTDB_BORROWED


FLINTDB_BEGIN_DECLS

#define TABLE_NAME_SUFFIX ".flintdb"
#define META_NAME_SUFFIX ".desc"

#define MAX_INDEX_KEYS_LIMIT 5
#define MAX_INDEX_LIMIT 5
#define MAX_COLUMNS_LIMIT 200
#define MAX_COLUMN_NAME_LIMIT 40
#define PRIMARY_NAME "primary"
#define PRIMARY_INDEX 0

#define WAL_OPT_OFF "OFF"
#define WAL_OPT_LOG "LOG"
#define WAL_OPT_TRUNCATE "TRUNCATE"

// WAL sync modes (similar to SQLite semantics)
// 0: platform default (backward compatible)
// 1: NORMAL (fsync/fdatasync)
// 2: FULL (macOS F_FULLFSYNC / others fsync)
// -1: OFF (no sync)
#define WAL_SYNC_DEFAULT 0
#define WAL_SYNC_OFF -1
#define WAL_SYNC_NORMAL 1
#define WAL_SYNC_FULL 2


enum flintdb_open_mode {
    FLINTDB_RDONLY = O_RDONLY,
    FLINTDB_RDWR = O_RDWR | O_CREAT,
};

struct flintdb_cursor_i64 {
    void *p; // Implementation-specific state
    
    i64 (*next)(struct flintdb_cursor_i64 *c, char **e);
    void (*close)(struct flintdb_cursor_i64 *c);
};

struct flintdb_cursor_row {
    void *p; // Implementation-specific state

    // Ownership rule:
    // - Returns a BORROWED row owned by the cursor.
    // - The returned pointer is valid until the next call to next() on the same cursor,
    //   or until cursor->close().
    // - Caller MUST NOT call row->free(row) on the returned pointer.
    // - If the caller needs to keep the row beyond that, call row->retain(row) to
    //   take an additional reference, and later call row->free(row) to release it.
    FLINTDB_BORROWED struct flintdb_row * (*next)(struct flintdb_cursor_row *c, char **e);
    void (*close)(struct flintdb_cursor_row *c);
};


// Variant type and operations
enum flintdb_variant_type  {
	VARIANT_NULL = 0,
	VARIANT_ZERO = 1, // RESERVED
	VARIANT_INT32  = 2,
	VARIANT_UINT32 = 3,
	VARIANT_INT8   = 4,
	VARIANT_UINT8  = 5,
	VARIANT_INT16  = 6,
	VARIANT_UINT16 = 7,
	VARIANT_INT64  = 8,
	VARIANT_DOUBLE = 9,
	VARIANT_FLOAT  = 10,

	VARIANT_STRING  = 11,
	VARIANT_DECIMAL = 12,
	VARIANT_BYTES   = 13,
	VARIANT_DATE    = 14,
	VARIANT_TIME    = 15,
	VARIANT_UUID    = 16,
	VARIANT_IPV6    = 17,
	VARIANT_BLOB    = 18, // reserved for future

    // VARIANT_HYPERLOGLOG = 21, // reserved for future
    // VARIANT_ROARINGBITMAP = 22, // reserved for future
    // VARIANT_GEOPOINT = 30, // reserved for future

	VARIANT_OBJECT = 31 // reserved for future
};

struct flintdb_decimal  {
    u8 sign; // 0: positive, 1: negative
    u8 scale; // number of digits to right of decimal point
    u8 reserved[2];
    u32 length; // length of data
    char data[16]; // BCD encoded digits, not null-terminated
};

int flintdb_decimal_from_string(const char *s, i16 scale, struct flintdb_decimal *out);
int flintdb_decimal_to_string(const struct flintdb_decimal  *d, char *buf, size_t buflen);
struct flintdb_decimal  flintdb_decimal_from_f64(f64 v, i16 scale, char **e);
f64 flintdb_decimal_to_f64(const struct flintdb_decimal *d, char **e);
int flintdb_decimal_plus(const struct flintdb_decimal *a, const struct flintdb_decimal  *b, i16 scale, struct flintdb_decimal *out);
int flintdb_decimal_divide(const struct flintdb_decimal *numerator, const struct flintdb_decimal *denominator, i16 scale, struct flintdb_decimal *out);
int flintdb_decimal_divide_by_int(const struct flintdb_decimal *numerator, int denominator, struct flintdb_decimal *out);

struct flintdb_variant {
    enum flintdb_variant_type  type;
    union {
        i64 i;
        f64 f;
        struct flintdb_decimal  d;
        struct {
            u8 owned; // 0: not owned, 1: owned (free on destroy), 2: string pool
            u8 sflag; // type == VARIANT_STRING => 0: null-terminated, 1: not null-terminated
            u8 reserved[2];
            u32 length;
            char *data; // not null-terminated
        } b; // bytes/string/uuid/ipv6
        time_t t;
    } value;
};

enum flintdb_null_spec {
    SPEC_NULLABLE = 0,
    SPEC_NOT_NULL = 1
};

struct flintdb_column {
    enum flintdb_variant_type  type;
    char name[MAX_COLUMN_NAME_LIMIT];
    int bytes;
    int precision;
    enum flintdb_null_spec nullspec;
    char value[MAX_COLUMN_NAME_LIMIT];
    char comment[MAX_COLUMN_NAME_LIMIT];
};

struct flintdb_index {
    char name[MAX_COLUMN_NAME_LIMIT];
    char type[MAX_COLUMN_NAME_LIMIT];
    char algorithm[MAX_COLUMN_NAME_LIMIT];
    struct {
        char a[MAX_INDEX_KEYS_LIMIT][MAX_COLUMN_NAME_LIMIT];
        u8 length;
    } keys;
};

struct flintdb_meta {
    f64 version;
    char name[64];
    char date[32];
    i16 compact;
    char compressor[32];
    char storage[32];
    char wal[20];
    i32 wal_checkpoint_interval;
    i32 wal_batch_size;
    i32 wal_compression_threshold;
    i32 wal_sync; // WAL_SYNC_DEFAULT|WAL_SYNC_OFF|WAL_SYNC_NORMAL|WAL_SYNC_FULL
    i32 wal_buffer_size; // WAL in-memory batch buffer capacity (bytes)
    i32 wal_page_data; // 1=log page images for UPDATE/DELETE (default), 0=metadata only
    i32 increment;
    i32 cache;

    struct {
        // char a[MAX_INDEX_KEYS_LIMIT][MAX_COLUMN_NAME_LIMIT];
        struct flintdb_index a[MAX_INDEX_LIMIT];
        u8 length;
    } indexes;
    struct {
        struct flintdb_column a[MAX_COLUMNS_LIMIT];
        u8 length;
    } columns;

    i8 absent_header; // CSV, TSV, JSONL, 1=header absent (1st line is data), 0=header present
    char delimiter; // CSV, TSV
    char quote;     // CSV, TSV
    char escape;    // CSV, TSV
    char nil_str[MAX_COLUMN_NAME_LIMIT]; // CSV, TSV

    char format[MAX_COLUMN_NAME_LIMIT]; // reserved for future use

    void *priv; // private data (not serialized)
};

struct flintdb_row {
    struct flintdb_variant *array;
    int length;
    void *priv; // Implementation-specific state

    struct flintdb_meta *meta; // associated meta
    i64 rowid; // optional rowid if applicable
    int refcount; // reference count for zero-copy sharing

    void (*free)(struct flintdb_row *r); // free function
    struct flintdb_row * (*retain)(struct flintdb_row *r); // increment refcount and return self

    i64 (*id)(const struct flintdb_row *r); // get rowid
    struct flintdb_variant * (*get)(const struct flintdb_row *r, u16 i, char **e);
    void (*set)(struct flintdb_row *r, u16 i, struct flintdb_variant *v, char **e);
    i8 (*is_nil)(const struct flintdb_row *r, u16 i, char **e);

    // setters
    void (*string_set)(struct flintdb_row *r, u16 i, const char *str, char **e);
    void (*i64_set)(struct flintdb_row *r, u16 i, i64 val, char **e);
    void (*f64_set)(struct flintdb_row *r, u16 i, f64 val, char **e);
    void (*u8_set)(struct flintdb_row *r, u16 i, u8 val, char **e);
    void (*i8_set)(struct flintdb_row *r, u16 i, i8 val, char **e);
    void (*u16_set)(struct flintdb_row *r, u16 i, u16 val, char **e);
    void (*i16_set)(struct flintdb_row *r, u16 i, i16 val, char **e);
    void (*u32_set)(struct flintdb_row *r, u16 i, u32 val, char **e);
    void (*i32_set)(struct flintdb_row *r, u16 i, i32 val, char **e);
    void (*bytes_set)(struct flintdb_row *r, u16 i, const char *data, u32 length, char **e);
    void (*date_set)(struct flintdb_row *r, u16 i, time_t val, char **e);
    void (*time_set)(struct flintdb_row *r, u16 i, time_t val, char **e);
    void (*uuid_set)(struct flintdb_row *r, u16 i, const char *data, u32 length, char **e);
    void (*ipv6_set)(struct flintdb_row *r, u16 i, const char *data, u32 length, char **e); 
    void (*decimal_set)(struct flintdb_row *r, u16 i, struct flintdb_decimal  data, char **e);

    // getters
    const char * (*string_get)(const struct flintdb_row *r, u16 i, char **e);
    i8 (*i8_get)(const struct flintdb_row *r, u16 i, char **e);
    u8 (*u8_get)(const struct flintdb_row *r, u16 i, char **e);
    i16 (*i16_get)(const struct flintdb_row *r, u16 i, char **e);
    u16 (*u16_get)(const struct flintdb_row *r, u16 i, char **e);
    i32 (*i32_get)(const struct flintdb_row *r, u16 i, char **e);
    u32 (*u32_get)(const struct flintdb_row *r, u16 i, char **e);
    i64 (*i64_get)(const struct flintdb_row *r, u16 i, char **e);
    f64 (*f64_get)(const struct flintdb_row *r, u16 i, char **e);
    struct flintdb_decimal  (*decimal_get)(const struct flintdb_row *r, u16 i, char **e);
    const char * (*bytes_get)(const struct flintdb_row *r, u16 i, u32 *length, char **e);
    time_t (*date_get)(const struct flintdb_row *r, u16 i, char **e);
    time_t (*time_get)(const struct flintdb_row *r, u16 i, char **e);
    const char * (*uuid_get)(const struct flintdb_row *r, u16 i, u32 *length, char **e);
    const char * (*ipv6_get)(const struct flintdb_row *r, u16 i, u32 *length, char **e);

    // 
    i8 (*is_zero)(const struct flintdb_row *r, u16 i, char **e);
    i8 (*equals)(const struct flintdb_row *r, const struct flintdb_row *o);
    i8 (*compare)(const struct flintdb_row *r, const struct flintdb_row *o, int (*cmp)(const struct flintdb_row*, const struct flintdb_row*));
    struct flintdb_row * (*copy)(const struct flintdb_row *r, char **e);

    i8 (*validate)(const struct flintdb_row *r, char **e);
};

// Variant operations
FLINTDB_API void flintdb_variant_init(struct flintdb_variant *v);
FLINTDB_API void flintdb_variant_free(struct flintdb_variant *v);
FLINTDB_API int flintdb_variant_compare(const struct flintdb_variant *a, const struct flintdb_variant *b);
FLINTDB_API int flintdb_variant_to_string(const struct flintdb_variant *v, char *out, u32 len);
FLINTDB_API int flintdb_variant_to_decimal(const struct flintdb_variant *v, struct flintdb_decimal  *out, char **e);

FLINTDB_API int flintdb_variant_i32_set(struct flintdb_variant *v, i32 val);
FLINTDB_API int flintdb_variant_u32_set(struct flintdb_variant *v, u32 val);
FLINTDB_API int flintdb_variant_i8_set(struct flintdb_variant *v, i8 val);
FLINTDB_API int flintdb_variant_u8_set(struct flintdb_variant *v, u8 val);
FLINTDB_API int flintdb_variant_i16_set(struct flintdb_variant *v, i16 val);
FLINTDB_API int flintdb_variant_u16_set(struct flintdb_variant *v, u16 val);
FLINTDB_API int flintdb_variant_i64_set(struct flintdb_variant *v, i64 val);
FLINTDB_API int flintdb_variant_f64_set(struct flintdb_variant *v, f64 val);
FLINTDB_API int flintdb_variant_string_set(struct flintdb_variant *v, const char *str, u32 length);
FLINTDB_API int flintdb_variant_string_ref_set(struct flintdb_variant *v, const char *str, u32 length, u8 sflag); // sflag: 0=null-terminated, 1=not null-terminated
FLINTDB_API int flintdb_variant_decimal_set(struct flintdb_variant *v, u8 sign, u8 scale, struct flintdb_decimal  data);
FLINTDB_API int flintdb_variant_bytes_set(struct flintdb_variant *v, const char *data, u32 length);
FLINTDB_API int flintdb_variant_date_set(struct flintdb_variant *v, time_t val);
FLINTDB_API int flintdb_variant_time_set(struct flintdb_variant *v, time_t val);
FLINTDB_API int flintdb_variant_uuid_set(struct flintdb_variant *v, const char *data, u32 length);
FLINTDB_API int flintdb_variant_ipv6_set(struct flintdb_variant *v, const char *data, u32 length);
FLINTDB_API int flintdb_variant_null_set(struct flintdb_variant *v);
FLINTDB_API int flintdb_variant_zero_set(struct flintdb_variant *v);
FLINTDB_API int flintdb_variant_copy(struct flintdb_variant *dest, const struct flintdb_variant *src);

FLINTDB_API int flintdb_variant_length(const struct flintdb_variant *v);
FLINTDB_API const char * flintdb_variant_string_get(const struct flintdb_variant *v);
FLINTDB_API i8 flintdb_variant_i8_get(const struct flintdb_variant *v, char **e);
FLINTDB_API u8 flintdb_variant_u8_get(const struct flintdb_variant *v, char **e);
FLINTDB_API i16 flintdb_variant_i16_get(const struct flintdb_variant *v, char **e);
FLINTDB_API u16 flintdb_variant_u16_get(const struct flintdb_variant *v, char **e);
FLINTDB_API i32 variant_i32_get(const struct flintdb_variant *v, char **e);
FLINTDB_API u32 flintdb_variant_u32_get(const struct flintdb_variant *v, char **e);
FLINTDB_API i64 flintdb_variant_i64_get(const struct flintdb_variant *v, char **e);
FLINTDB_API f64 flintdb_variant_f64_get(const struct flintdb_variant *v, char **e);
FLINTDB_API struct flintdb_decimal  flintdb_variant_decimal_get(const struct flintdb_variant *v, char **e);
FLINTDB_API const char * flintdb_variant_bytes_get(const struct flintdb_variant *v, u32 *length, char **e);
FLINTDB_API time_t flintdb_variant_date_get(const struct flintdb_variant *v, char **e);
FLINTDB_API time_t flintdb_variant_time_get(const struct flintdb_variant *v, char **e);
FLINTDB_API const char * flintdb_variant_uuid_get(const struct flintdb_variant *v, u32 *length, char **e);
FLINTDB_API const char * flintdb_variant_ipv6_get(const struct flintdb_variant *v, u32 *length, char **e);
FLINTDB_API i8 flintdb_variant_is_null(const struct flintdb_variant *v);


// Meta operations
FLINTDB_API struct flintdb_meta flintdb_meta_new(const char *name, char **e);
FLINTDB_API struct flintdb_meta flintdb_meta_open(const char *filename, char **e);
FLINTDB_API void flintdb_meta_close(struct flintdb_meta *m);

FLINTDB_API int  flintdb_meta_write(const struct flintdb_meta *m, const char *filename, char **e);
FLINTDB_API int flintdb_meta_to_sql_string(const struct flintdb_meta *m, char *s, i32 len, char **e);
FLINTDB_API int  flintdb_meta_compare(const struct flintdb_meta *a, const struct flintdb_meta *b);

FLINTDB_API void flintdb_meta_columns_add(struct flintdb_meta *m, const char *name, enum flintdb_variant_type  type, i32 bytes, i16 precision, enum flintdb_null_spec nullspec, const char *value, const char *comment, char **e);
FLINTDB_API void flintdb_meta_indexes_add(struct flintdb_meta *m, const char *name, const char *algorithm, const char keys[][MAX_COLUMN_NAME_LIMIT], u16 key_count, char **e);

FLINTDB_API int flintdb_column_at(struct flintdb_meta *m, const char *name); // Get column index by name


// Pointer versions for language bindings (e.g., Python, Java, etc.)
FLINTDB_API struct flintdb_meta* flintdb_meta_new_ptr(const char *name, char **e);
FLINTDB_API struct flintdb_meta* flintdb_meta_open_ptr(const char *filename, char **e);
FLINTDB_API void flintdb_meta_free_ptr(struct flintdb_meta *m);


// Row operations
FLINTDB_API struct flintdb_row * flintdb_row_new(struct flintdb_meta *meta, char **e);
FLINTDB_API struct flintdb_row * flintdb_row_from_argv(struct flintdb_meta *meta, u16 argc, const char **argv, char **e);
FLINTDB_API struct flintdb_row * flintdb_row_cast(struct flintdb_row *src, struct flintdb_meta *meta,  char **e);
FLINTDB_API int flintdb_row_cast_reuse(const struct flintdb_row *src, struct flintdb_row *dst, char **e);
FLINTDB_API void flintdb_print_row(const struct flintdb_row *r);
// Row pooling API (optional performance optimization)
FLINTDB_API struct flintdb_row * flintdb_row_pool_acquire(struct flintdb_meta *meta, char **e);
FLINTDB_API void flintdb_row_pool_release(struct flintdb_row *r);


// Table operations
struct flintdb_table {
    i64 (*rows)(const struct flintdb_table *me, char **e);
    i64 (*bytes)(const struct flintdb_table *me, char **e);
    const struct flintdb_meta * (*meta)(const struct flintdb_table *me, char **e);

    i64 (*apply)(struct flintdb_table *me, struct flintdb_row *r, i8 upsert, char **e); // upsert: 0=insert only, 1=insert or update
    i64 (*apply_at)(struct flintdb_table *me, i64 rowid, struct flintdb_row *r, char **e); // insert or update at rowid
    i64 (*delete_at)(struct flintdb_table *me, i64 rowid, char **e); // delete at rowid
    // struct flintdb_cursor_i64 * (*find)(const struct flintdb_table *me, i8 index, enum order order, struct limit limit, const struct filters *filters, char **e);
    struct flintdb_cursor_i64 * (*find)(const struct flintdb_table *me, const char *where, char **e);
    FLINTDB_BORROWED const struct flintdb_row * (*one)(const struct flintdb_table *me, i8 index, u16 argc, const char **argv, char **e); // find one row by primary key or unique index, don't free the returned row
    FLINTDB_BORROWED const struct flintdb_row * (*read)(struct flintdb_table *me, i64 rowid, char **e); // don't free the returned row, it's managed by the lru cache
    int (*read_stream)(struct flintdb_table *me, i64 rowid, struct flintdb_row *dest, char **e); // streaming read: decode into caller-owned buffer, skip cache

    void (*close)(struct flintdb_table *me);

    void *priv; // private data (struct flintdb_table_priv *)
};


// Transaction operations (WAL + table lock)
//
// Mirrors Java's TransactionImpl pattern:
// - begin() acquires the table lock and starts a WAL transaction
// - apply/apply_at/delete_at perform multiple operations inside the same tx
// - commit()/rollback() end the WAL tx and release the table lock
// - close() rolls back if not committed, releases lock, and frees the tx object
// - validate() checks if the transaction can be applied to the given table (schema compatibility)
//
struct flintdb_transaction {
    i64 (*id)(const struct flintdb_transaction *me);
    i64 (*apply)(struct flintdb_transaction *me, struct flintdb_row *r, i8 upsert, char **e);
    i64 (*apply_at)(struct flintdb_transaction *me, i64 rowid, struct flintdb_row *r, char **e);
    i64 (*delete_at)(struct flintdb_transaction *me, i64 rowid, char **e);
    void (*commit)(struct flintdb_transaction *me, char **e);
    void (*rollback)(struct flintdb_transaction *me, char **e);
    i8 (*validate)(struct flintdb_transaction *me, struct flintdb_table *t, char **e);
    void (*close)(struct flintdb_transaction *me);
    void *priv; // private data
};

// Begin a transaction for a table (requires the table to be opened in write mode with WAL enabled)
FLINTDB_API struct flintdb_transaction * flintdb_transaction_begin(struct flintdb_table *table, char **e);


// Table operations
FLINTDB_API struct flintdb_table * flintdb_table_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e); // if meta is NULL, read from <file>.desc
FLINTDB_API int flintdb_table_drop(const char *file, char **e);


// Generic file structure and operations (for TSV/CSV/JSONL/Parquet files)
struct flintdb_genericfile { 
    i64 (*rows)(const struct flintdb_genericfile *me, char **e);
    i64 (*bytes)(const struct flintdb_genericfile *me, char **e);
    const struct flintdb_meta * (*meta)(const struct flintdb_genericfile *me, char **e);

    i64 (*write)(struct flintdb_genericfile *me, struct flintdb_row *r, char **e);
    struct flintdb_cursor_row * (*find)(const struct flintdb_genericfile *me, const char *where, char **e);
    void (*close)(struct flintdb_genericfile *me);
    
    void *priv;
};

// generic file operations (TSV/CSV text files)
FLINTDB_API struct flintdb_genericfile * flintdb_genericfile_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e);
FLINTDB_API void flintdb_genericfile_drop(const char *file, char **e);


// File-based external sorter for rows
struct flintdb_filesort {
	void (*close)(struct flintdb_filesort *me);
    i64 (*rows)(const struct flintdb_filesort *me);
	i64 (*add)(struct flintdb_filesort *me, struct flintdb_row *r, char **e);
    struct flintdb_row * (*read)(const struct flintdb_filesort *me, i64 i, char **e);
    i64 (*sort)(struct flintdb_filesort *me, int (*cmpr)(const void *obj, const struct flintdb_row *a, const struct flintdb_row *b), const void *ctx, char **e);
	
	void *priv; // struct flintdb_filesort_priv *
};

FLINTDB_API struct flintdb_filesort *flintdb_filesort_new(const char *file, const struct flintdb_meta *m, char **e);


// Aggregate functions and group key structures (Java Aggregate.java port)

// Forward declarations
struct flintdb_aggregate_groupkey;

struct flintdb_aggregate {
    void *priv;
    
    void (*free)(struct flintdb_aggregate *agg);
    void (*row)(struct flintdb_aggregate *agg, const struct flintdb_row *r, char **e);
    int (*compute)(struct flintdb_aggregate *agg, struct flintdb_row ***out_rows, char **e);
};

struct flintdb_aggregate_groupby {
    void *priv;
    
    void (*free)(struct flintdb_aggregate_groupby *gb);
    const char * (*alias)(const struct flintdb_aggregate_groupby *gb);
    const char * (*column)(const struct flintdb_aggregate_groupby *gb);
    enum flintdb_variant_type  (*type)(const struct flintdb_aggregate_groupby *gb);
    struct flintdb_variant * (*get)(const struct flintdb_aggregate_groupby *gb, const struct flintdb_row *r, char **e);
};

struct flintdb_aggregate_condition {
    i8 (*ok)(const struct flintdb_aggregate_condition *cond, const struct flintdb_row *r, char **e);
};

struct flintdb_aggregate_func {
    void *priv;

    void (*free)(struct flintdb_aggregate_func *f);
    const char * (*name)(const struct flintdb_aggregate_func *f);
    const char * (*alias)(const struct flintdb_aggregate_func *f);
    enum flintdb_variant_type  (*type)(const struct flintdb_aggregate_func *f);
    int (*precision)(const struct flintdb_aggregate_func *f);
    const struct flintdb_aggregate_condition * (*condition)(const struct flintdb_aggregate_func *f);
    void (*row)(struct flintdb_aggregate_func *f, const struct flintdb_aggregate_groupkey *gk, const struct flintdb_row *r, char **e);
    void (*compute)(struct flintdb_aggregate_func *f, const struct flintdb_aggregate_groupkey *gk, char **e);
    const struct flintdb_variant * (*result)(const struct flintdb_aggregate_func *f, const struct flintdb_aggregate_groupkey *gk, char **e);
};

struct flintdb_aggregate_groupkey {
    void *priv;

    void (*free)(struct flintdb_aggregate_groupkey *g);
    struct flintdb_row * (*key)(const struct flintdb_aggregate_groupkey *g, char **e);
    i8 (*equals)(const struct flintdb_aggregate_groupkey *g, const struct flintdb_aggregate_groupkey *o, char **e);
};

// Main aggregate API
FLINTDB_API struct flintdb_aggregate* aggregate_new(const char *id, struct flintdb_aggregate_groupby **groupby, u16 groupby_count, 
                                            struct flintdb_aggregate_func **funcs, u16 func_count, char **e);

// Groupby API
FLINTDB_API struct flintdb_aggregate_groupby* groupby_new(const char *alias, const char *column, enum flintdb_variant_type  type, char **e);

// Aggregate functions
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_count(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_distinct_count(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_distinct_hll_count(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_sum(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_avg(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_min(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_max(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_first(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_last(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_rowid(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);
FLINTDB_API struct flintdb_aggregate_func * flintdb_func_hash(const char *name, const char *alias, enum flintdb_variant_type  type, struct flintdb_aggregate_condition cond, char **e);

// Groupkey API
FLINTDB_API struct flintdb_aggregate_groupkey * flintdb_groupkey_from_row(struct flintdb_aggregate *agg, const struct flintdb_row *source, const char **columns, u16 n, char **e);

// SQL execution result structure

struct flintdb_sql_result {
    i64 affected;

    // struct flintdb_meta *meta;    
    char **column_names;
    int column_count;     
    struct flintdb_cursor_row *row_cursor;

    struct flintdb_transaction *transaction; // if non-NULL, the transaction must be closed by the caller after use

    void (*close)(struct flintdb_sql_result *me);
};

FLINTDB_API struct flintdb_sql_result* flintdb_sql_exec(const char *sql, const struct flintdb_transaction *transaction, char **e);

// SQL parsing structure and operations
struct flintdb_sql; // forward declaration
FLINTDB_API struct flintdb_sql * flintdb_sql_parse(const char *sql, char **e);
FLINTDB_API struct flintdb_sql * flintdb_sql_from_file(const char *file, char **e);
FLINTDB_API void flintdb_sql_free(struct flintdb_sql *s);

FLINTDB_API int flintdb_sql_to_string(struct flintdb_sql *in, char *s, int len, char **e);
FLINTDB_API int flintdb_sql_to_meta(struct flintdb_sql *in, struct flintdb_meta *out, char **e);


/**
 * @brief Clean up resources before shutting down the library
 */
FLINTDB_API void flintdb_cleanup(char **e);

FLINTDB_END_DECLS

#endif // FLINTDB_H
