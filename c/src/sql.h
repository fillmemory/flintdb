#ifndef FLINTDB_SQL_H
#define FLINTDB_SQL_H

#include "flintdb.h"
#include "types.h"

#include <limits.h>

#define SQL_STRING_LIMIT 8192
#define SQL_OBJECT_STRING_LIMIT 64
#define SQL_COLUMNS_LIMIT 1024

/**
 * @brief SQL parser but not a full-featured SQL engine.
 * 
 */

struct flintdb_sql {
    #ifndef NDEBUG
    char origin[SQL_STRING_LIMIT];
    #endif
    char statement[SQL_STRING_LIMIT];

    // table can be an absolute file path; use PATH_MAX to avoid truncation
    char table[PATH_MAX];
    
    // Dynamically allocated small fields (Phase 1)
    char *object;
    char *index;
    char *ignore;
    
    // Dynamically allocated SQL clause fields
    char *limit;
    char *orderby;
    char *groupby;
    char *having;
    i8 distinct;
    char *from;
    char *into;
    char *where;
    char *connect;

    struct {
        int length;
        char name[SQL_COLUMNS_LIMIT][SQL_OBJECT_STRING_LIMIT];
    } columns;

    struct {
        int length;
        char value[SQL_COLUMNS_LIMIT][SQL_OBJECT_STRING_LIMIT];
    } values;

    struct {
        int length;
        char object[SQL_COLUMNS_LIMIT][SQL_OBJECT_STRING_LIMIT];
    } definition;

    // Dynamically allocated path fields
    char *dictionary;
    char *directory;
    
    // Dynamically allocated metadata fields (Phase 1)
    char *compressor;
    char *compact;
    char *cache;
    char *date;
    char *storage;
    char *header;
    char *delimiter;
    char *quote;
    char *nullString;
    char *format;
    char *wal;
    i32 wal_checkpoint_interval;
    i32 wal_batch_size;
    i32 wal_compression_threshold;
    i32 wal_sync;
    i32 wal_buffer_size;
    i32 wal_page_data;
    
    // Dynamically allocated large field (Phase 2)
    char *option;
};


// struct flintdb_sql * flintdb_sql_parse(const char *sql, char **e);
// struct flintdb_sql * flintdb_sql_from_file(const char *file, char **e);
// void flintdb_sql_free(struct flintdb_sql *sql);

// int flintdb_sql_to_string(struct flintdb_sql *in, char *s, int len, char **e);
// int flintdb_sql_to_meta(struct flintdb_sql *in, struct flintdb_meta *out, char **e);

// Parsing utility functions
int sql_extract_alias(const char *expr, char *alias_out, size_t alias_cap);
int sql_parse_groupby_columns(const char *groupby, char cols[][MAX_COLUMN_NAME_LIMIT]);
int sql_parse_orderby_clause(const char *orderby, char cols[][MAX_COLUMN_NAME_LIMIT], i8 desc_flags[], int *ecount);


#endif // FLINTDB_SQL_H