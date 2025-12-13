#include "flintdb.h"
#include "runtime.h"
#include "filter.h"
#include "sql.h"

#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <dlfcn.h>

/**
 * FlintDB Parquet File Handler (C implementation)
 *
 * Provides high-performance streaming read/write of Parquet files using
 * Apache Arrow C Data Interface via dynamically loaded shared library.
 * 
 * This implementation:
 * 1. Dynamically loads libarrow/libparquet shared objects at runtime
 * 2. Uses Apache Arrow C Data Interface for zero-copy data exchange
 * 3. Maps Parquet schema to FlintDB Meta (column types)
 * 4. Converts between FlintDB Row and Arrow columnar format
 * 
 * Shared library search paths (in order):
 * - Environment variable: ARROW_HOME/lib
 * - Common system paths: /usr/local/lib, /usr/lib, /opt/homebrew/lib
 * - Current directory: ./lib
 */

// ============================================================================
// Apache Arrow C Data Interface
// ============================================================================

#ifndef ARROW_FLAG_DICTIONARY_ORDERED
#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4
#endif

// Arrow C Data Interface structures (ABI-stable)
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};

// ============================================================================
// Dynamic library loading - FlintDB Parquet Plugin function pointers
// ============================================================================

typedef void* (*reader_open_file_t)(const char* path, char** error);
typedef void (*reader_close_t)(void* reader);
typedef int (*reader_get_stream_t)(void* reader, struct ArrowArrayStream* out);
typedef int64_t (*reader_num_rows_t)(void* reader);

typedef void* (*writer_open_file_t)(const char* path, struct ArrowSchema* schema, char** error);
typedef void (*writer_close_t)(void* writer);
typedef int (*writer_write_batch_t)(void* writer, struct ArrowArray* batch);

typedef void* (*schema_builder_new_t)(void);
typedef void (*schema_builder_free_t)(void* builder);
typedef int (*schema_builder_add_column_t)(void* builder, const char* name, const char* arrow_type);
typedef struct ArrowSchema* (*schema_builder_build_t)(void* builder);

typedef void* (*batch_builder_new_t)(struct ArrowSchema* schema);
typedef void (*batch_builder_free_t)(void* builder);
typedef int (*batch_builder_append_int32_t)(void* builder, int col, int32_t value);
typedef int (*batch_builder_append_int64_t)(void* builder, int col, int64_t value);
typedef int (*batch_builder_append_double_t)(void* builder, int col, double value);
typedef int (*batch_builder_append_string_t)(void* builder, int col, const char* value, int32_t length);
typedef int (*batch_builder_append_null_t)(void* builder, int col);
typedef int (*batch_builder_finish_row_t)(void* builder);
typedef struct ArrowArray* (*batch_builder_build_t)(void* builder, int* num_rows);

struct arrow_functions {
    void* handle; // dlopen handle
    reader_open_file_t reader_open_file;
    reader_close_t reader_close;
    reader_get_stream_t reader_get_stream;
    reader_num_rows_t reader_num_rows;
    writer_open_file_t writer_open_file;
    writer_close_t writer_close;
    writer_write_batch_t writer_write_batch;
    schema_builder_new_t schema_builder_new;
    schema_builder_free_t schema_builder_free;
    schema_builder_add_column_t schema_builder_add_column;
    schema_builder_build_t schema_builder_build;
    batch_builder_new_t batch_builder_new;
    batch_builder_free_t batch_builder_free;
    batch_builder_append_int32_t batch_builder_append_int32;
    batch_builder_append_int64_t batch_builder_append_int64;
    batch_builder_append_double_t batch_builder_append_double;
    batch_builder_append_string_t batch_builder_append_string;
    batch_builder_append_null_t batch_builder_append_null;
    batch_builder_finish_row_t batch_builder_finish_row;
    batch_builder_build_t batch_builder_build;
};

static struct arrow_functions g_arrow = {0};
static pthread_mutex_t g_arrow_mutex = PTHREAD_MUTEX_INITIALIZER;
static i8 g_arrow_initialized = 0;

// ============================================================================
// Arrow library initialization
// ============================================================================

static int arrow_load_library(char **e) {
    pthread_mutex_lock(&g_arrow_mutex);
    
    if (g_arrow_initialized) {
        pthread_mutex_unlock(&g_arrow_mutex);
        return 0;
    }

    // Try to find and load FlintDB Parquet plugin (C wrapper around Arrow/Parquet)
    const char* lib_names[] = {
        "libflintdb_parquet.dylib",  // macOS
        "libflintdb_parquet.so",     // Linux
        "flintdb_parquet.dll",       // Windows
        NULL
    };

    const char* search_paths[] = {
        "./lib",                     // Relative to binary
        "../lib",                    // Relative to binary (one level up)
        "./c/lib",                   // From workspace root
        "/usr/local/lib/flintdb",     // System install
        "/opt/flintdb/lib",           // Alternative system install
        "/mingw64/lib",              // MSYS2
        "C:/msys64/mingw64/lib",     // MSYS2 (absolute)
        NULL
    };

    void* handle = NULL;
    char lib_path[PATH_MAX];
    
    // Try each combination of path and library name
    for (int i = 0; search_paths[i] != NULL; i++) {
        for (int j = 0; lib_names[j] != NULL; j++) {
            snprintf(lib_path, sizeof(lib_path), "%s/%s", search_paths[i], lib_names[j]);
            handle = dlopen(lib_path, RTLD_LAZY | RTLD_LOCAL);
            if (handle) {
                DEBUG("Loaded Parquet plugin: %s", lib_path);
                goto found;
            }
        }
    }
    
    // Try without path (system library search)
    for (int j = 0; lib_names[j] != NULL; j++) {
        handle = dlopen(lib_names[j], RTLD_LAZY | RTLD_LOCAL);
        if (handle) {
            DEBUG("Loaded Parquet plugin: %s", lib_names[j]);
            goto found;
        }
    }
    
    pthread_mutex_unlock(&g_arrow_mutex);
    THROW(e, "Failed to load FlintDB Parquet plugin.\n"
             "Build the plugin with: cd c/plugins/parquet && ./build.sh\n"
             "This requires Apache Arrow C++ library:\n"
             "  macOS: brew install apache-arrow\n"
             "  Linux: apt install libarrow-dev libparquet-dev\n"
             "  Windows (MSYS2): pacman -S mingw-w64-x86_64-arrow mingw-w64-x86_64-parquet-cpp");

found:
    g_arrow.handle = handle;
    
    // Load plugin function symbols
    #define LOAD_SYMBOL(name, sym) \
        g_arrow.name = (name##_t)dlsym(handle, sym); \
        if (!g_arrow.name) { \
            DEBUG("Warning: Symbol not found: %s", sym); \
            goto fail_symbols; \
        }
    
    LOAD_SYMBOL(reader_open_file, "flintdb_parquet_reader_open");
    LOAD_SYMBOL(reader_close, "flintdb_parquet_reader_close");
    LOAD_SYMBOL(reader_get_stream, "flintdb_parquet_reader_get_stream");
    LOAD_SYMBOL(reader_num_rows, "flintdb_parquet_reader_num_rows");
    LOAD_SYMBOL(writer_open_file, "flintdb_parquet_writer_open");
    LOAD_SYMBOL(writer_close, "flintdb_parquet_writer_close");
    LOAD_SYMBOL(writer_write_batch, "flintdb_parquet_writer_write_batch");
    LOAD_SYMBOL(schema_builder_new, "flintdb_parquet_schema_builder_new");
    LOAD_SYMBOL(schema_builder_free, "flintdb_parquet_schema_builder_free");
    LOAD_SYMBOL(schema_builder_add_column, "flintdb_parquet_schema_builder_add_column");
    LOAD_SYMBOL(schema_builder_build, "flintdb_parquet_schema_builder_build");
    LOAD_SYMBOL(batch_builder_new, "flintdb_parquet_batch_builder_new");
    LOAD_SYMBOL(batch_builder_free, "flintdb_parquet_batch_builder_free");
    LOAD_SYMBOL(batch_builder_append_int32, "flintdb_parquet_batch_builder_append_int32");
    LOAD_SYMBOL(batch_builder_append_int64, "flintdb_parquet_batch_builder_append_int64");
    LOAD_SYMBOL(batch_builder_append_double, "flintdb_parquet_batch_builder_append_double");
    LOAD_SYMBOL(batch_builder_append_string, "flintdb_parquet_batch_builder_append_string");
    LOAD_SYMBOL(batch_builder_append_null, "flintdb_parquet_batch_builder_append_null");
    LOAD_SYMBOL(batch_builder_finish_row, "flintdb_parquet_batch_builder_finish_row");
    LOAD_SYMBOL(batch_builder_build, "flintdb_parquet_batch_builder_build");
    
    #undef LOAD_SYMBOL
    
    g_arrow_initialized = 1;
    pthread_mutex_unlock(&g_arrow_mutex);
    DEBUG("Parquet plugin loaded successfully");
    return 0;

fail_symbols:
    THROW(e, "Failed to load required symbols from Parquet plugin.\n"
             "The plugin may be incompatible or corrupted.\n"
             "Rebuild with: cd c/plugins/parquet && ./build.sh");

EXCEPTION:
    if (handle)
        dlclose(handle);
    pthread_mutex_unlock(&g_arrow_mutex);
    return -1;
}

static void arrow_unload_library() __attribute__((unused));
static void arrow_unload_library() {
    pthread_mutex_lock(&g_arrow_mutex);
    if (g_arrow.handle) {
        dlclose(g_arrow.handle);
        g_arrow.handle = NULL;
    }
    g_arrow_initialized = 0;
    pthread_mutex_unlock(&g_arrow_mutex);
}

// ============================================================================
// Private structures
// ============================================================================

struct parquetfile_priv {
    char file[PATH_MAX];
    enum flintdb_open_mode mode;
    struct flintdb_meta meta;

    i64 rows; // cached row count for parquet
    i8 writer_opened; // flag if writer has been initialized
    
    void *arrow_reader;  // Apache Arrow ParquetFileReader handle
    void *arrow_writer;  // Apache Arrow ParquetFileWriter handle
    struct ArrowArrayStream stream; // Arrow stream for reading
    struct ArrowSchema *arrow_schema; // Schema for writer
    
    // Write buffer for batching rows
    struct flintdb_row **row_buffer;  // Buffer of rows
    i32 buffer_size;          // Current number of rows in buffer
    i32 buffer_capacity;      // Maximum buffer capacity (default 1024)
};

struct parquetfile_cursor_priv {
    struct filter *filter;
    struct limit limit;
    i64 rowidx;     // current row index
    i8 initialized; // init guard
    i8 finished;    // EOF flag
    
    void* arrow_reader; // Own reader instance (not shared)
    struct ArrowArrayStream* stream_storage; // Owned stream storage
    struct ArrowArrayStream* stream; // Arrow stream pointer
    struct ArrowArray current_batch; // Current record batch
    struct ArrowSchema schema; // Schema from stream
    i64 batch_row_idx; // Current row in batch
    const struct flintdb_meta* meta; // Meta reference for type mapping
};

// Drop parquet file and associated metadata files
void parquetfile_drop(const char *file, char **e) {
    if (!file)
        return;
    char dir[PATH_MAX] = {0};
    getdir(file, dir);
    if (!dir_exists(dir))
        return;

    DIR *d = opendir(dir);
    if (!d)
        THROW(e, "Failed to open directory: %s", dir);

    char base[PATH_MAX] = {0};
    getname(file, base);
    size_t base_len = strlen(base);
    struct dirent *de = NULL;
    while ((de = readdir(d)) != NULL) {
        if (0 == strncmp(de->d_name, base, base_len)) {
            char fpath[PATH_MAX] = {0};
            snprintf(fpath, sizeof(fpath), "%s%c%s", dir, PATH_CHAR, de->d_name);
            unlink(fpath);
        }
    }
    closedir(d);
    return;

EXCEPTION:
    if (d)
        closedir(d);
    return;
}

// ============================================================================
// Helper functions - Arrow/FlintDB type mapping
// ============================================================================

static const char* flintdb_type_to_arrow_format(enum flintdb_variant_type  type) {
    switch (type) {
        case VARIANT_INT8:   return "c";  // int8
        case VARIANT_UINT8:  return "C";  // uint8
        case VARIANT_INT16:  return "s";  // int16
        case VARIANT_UINT16: return "S";  // uint16
        case VARIANT_INT32:  return "i";  // int32
        case VARIANT_UINT32: return "I";  // uint32
        case VARIANT_INT64:  return "l";  // int64
        case VARIANT_FLOAT:  return "f";  // float32
        case VARIANT_DOUBLE: return "g";  // float64
        case VARIANT_STRING: return "u";  // utf8 string
        case VARIANT_BYTES:  return "z";  // binary
        case VARIANT_DATE:   return "tdD"; // date32[days]
        case VARIANT_TIME:   return "tts"; // time64[microseconds]
        default:     return "u";  // default to string
    }
}

static enum flintdb_variant_type  arrow_format_to_flintdb_type(const char* format) {
    if (!format || !format[0]) return VARIANT_STRING;
    
    switch (format[0]) {
        case 'c': return VARIANT_INT8;
        case 'C': return VARIANT_UINT8;
        case 's': return VARIANT_INT16;
        case 'S': return VARIANT_UINT16;
        case 'i': return VARIANT_INT32;
        case 'I': return VARIANT_UINT32;
        case 'l': return VARIANT_INT64;
        case 'f': return VARIANT_FLOAT;
        case 'g': return VARIANT_DOUBLE;
        case 'u': return VARIANT_STRING;
        case 'z': return VARIANT_BYTES;
        case 't': 
            if (format[1] == 'd') return VARIANT_DATE;
            if (format[1] == 't') return VARIANT_TIME;
            return VARIANT_STRING;
        default: return VARIANT_STRING;
    }
}

// ============================================================================
// GenericFile interface implementation
// ============================================================================

// Flush buffered rows to Parquet file (convert to Arrow RecordBatch and write)
static int parquetfile_flush_buffer(struct parquetfile_priv *priv, char **e) {
    if (!priv || priv->buffer_size == 0)
        return 0;
    
    DEBUG("Flushing %d buffered rows to Parquet", priv->buffer_size);
    
    if (!g_arrow.batch_builder_new) {
        THROW(e, "Batch builder API not available in plugin");
    }
    
    // Build a fresh schema for batch builder (schema can only be exported once)
    DEBUG("Building fresh schema for batch builder");
    
    void* schema_builder = g_arrow.schema_builder_new();
    if (!schema_builder)
        THROW(e, "Failed to create schema builder for flush");
    
    for (int i = 0; i < priv->meta.columns.length; i++) {
        struct flintdb_column* col = &priv->meta.columns.a[i];
        const char* arrow_type = flintdb_type_to_arrow_format(col->type);
        if (g_arrow.schema_builder_add_column(schema_builder, col->name, arrow_type) != 0) {
            g_arrow.schema_builder_free(schema_builder);
            THROW(e, "Failed to add column to schema: %s", col->name);
        }
    }
    
    struct ArrowSchema* schema = g_arrow.schema_builder_build(schema_builder);
    g_arrow.schema_builder_free(schema_builder);
    
    if (!schema)
        THROW(e, "Failed to build schema for batch builder");
    
    DEBUG("Creating batch builder with fresh schema");
    
    // Create batch builder with schema
    void* builder = g_arrow.batch_builder_new(schema);
    
    // Free the schema now (builder should have imported it)
    if (schema->release) {
        schema->release(schema);
    }
    FREE(schema);
    
    if (!builder) {
        THROW(e, "Failed to create batch builder");
    }
    
    DEBUG("Batch builder created successfully, processing %d rows", priv->buffer_size);
    
    // Add each buffered row to the builder
    for (int row_idx = 0; row_idx < priv->buffer_size; row_idx++) {
        struct flintdb_row* r = priv->row_buffer[row_idx];
        
        for (int col = 0; col < priv->meta.columns.length; col++) {
            char *get_error = NULL;
            struct flintdb_variant *vptr = r->get(r, col, &get_error);
            if (get_error) {
                g_arrow.batch_builder_free(builder);
                THROW(e, "Failed to get column %d: %s", col, get_error);
            }
            struct flintdb_variant v = *vptr; // Copy the variant
            
            // Append value based on type
            int ret = 0;
            switch (v.type) {
                case VARIANT_NULL:
                    ret = g_arrow.batch_builder_append_null(builder, col);
                    break;
                case VARIANT_INT8:
                case VARIANT_UINT8:
                case VARIANT_INT16:
                case VARIANT_UINT16:
                case VARIANT_INT32:
                case VARIANT_UINT32:
                case VARIANT_INT64:
                    ret = g_arrow.batch_builder_append_int64(builder, col, v.value.i);
                    break;
                case VARIANT_FLOAT:
                case VARIANT_DOUBLE:
                    ret = g_arrow.batch_builder_append_double(builder, col, v.value.f);
                    break;
                case VARIANT_STRING: {
                    u32 len = flintdb_variant_length(&v);
                    const char* str = flintdb_variant_string_get(&v);
                    ret = g_arrow.batch_builder_append_string(builder, col, str, (int32_t)len);
                    break;
                }
                case VARIANT_BYTES: {
                    u32 len = flintdb_variant_length(&v);
                    const u8* data = (const u8*)flintdb_variant_bytes_get(&v, &len, NULL);
                    ret = g_arrow.batch_builder_append_string(builder, col, (const char*)data, (int32_t)len);
                    break;
                }
                default:
                    // Unsupported type - append as null
                    ret = g_arrow.batch_builder_append_null(builder, col);
                    break;
            }
            
            if (ret != 0) {
                g_arrow.batch_builder_free(builder);
                THROW(e, "Failed to append value to batch builder (row %d, col %d)", row_idx, col);
            }
        }
        
        // Finish this row
        if (g_arrow.batch_builder_finish_row(builder) != 0) {
            g_arrow.batch_builder_free(builder);
            THROW(e, "Failed to finish row %d in batch builder", row_idx);
        }
    }
    
    // Build the RecordBatch
    int num_rows = 0;
    struct ArrowArray* batch = g_arrow.batch_builder_build(builder, &num_rows);
    g_arrow.batch_builder_free(builder);
    builder = NULL;
    
    if (!batch) {
        THROW(e, "Failed to build RecordBatch from buffered rows");
    }
    
    // Write batch to Parquet file
    int write_ret = g_arrow.writer_write_batch(priv->arrow_writer, batch);
    
    // Release batch
    if (batch->release) {
        batch->release(batch);
    }
    FREE(batch);
    
    if (write_ret != 0) {
        THROW(e, "Failed to write RecordBatch to Parquet file");
    }
    
    DEBUG("Successfully flushed %d rows to Parquet", num_rows);
    priv->buffer_size = 0;
    return 0;

EXCEPTION:
    return -1;
}

static i64 parquetfile_rows(const struct flintdb_genericfile *me, char **e) {
    if (!me || !me->priv)
        return -1;
    struct parquetfile_priv *priv = (struct parquetfile_priv *)me->priv;
    
    // If rows cached, return immediately
    if (priv->rows >= 0)
        return priv->rows;
    
    // Try to read row count from Parquet file metadata
    if (priv->arrow_reader && g_arrow.reader_num_rows) {
        priv->rows = g_arrow.reader_num_rows(priv->arrow_reader);
        if (priv->rows >= 0)
            return priv->rows;
    }
    
    // Row count not available (writer mode or library not loaded)
    return -1;
}

static i64 parquetfile_bytes(const struct flintdb_genericfile *me, char **e) {
    if (!me || !me->priv)
        return -1;
    return file_length(((struct parquetfile_priv *)me->priv)->file);
}

static const struct flintdb_meta *parquetfile_meta(const struct flintdb_genericfile *me, char **e) {
    if (!me || !me->priv)
        return NULL;
    return &((struct parquetfile_priv *)me->priv)->meta;
}

static i64 parquetfile_write(struct flintdb_genericfile *me, struct flintdb_row *r, char **e) {
    if (!me || !me->priv || !r)
        return -1;
    struct parquetfile_priv *priv = (struct parquetfile_priv *)me->priv;
    
    if (priv->mode != FLINTDB_RDWR) {
        THROW(e, "file not opened for write: %s", priv->file);
    }

    // Initialize writer lazily on first write
    if (!priv->writer_opened) {
        // Ensure parent directory exists
        char dir[PATH_MAX] = {0};
        getdir(priv->file, dir);
        if (dir[0])
            mkdirs(dir, S_IRWXU);
        
        DEBUG("parquetfile_write: initialize writer for %s", priv->file);
        
        // Load Arrow library if not already loaded
        if (!g_arrow_initialized) {
            if (arrow_load_library(e) != 0)
                THROW_S(e);
        }
        
        // Build Arrow schema from Meta
        if (!g_arrow.schema_builder_new || !g_arrow.writer_open_file) {
            THROW(e, "Arrow writer functions not available");
        }
        
        void* schema_builder = g_arrow.schema_builder_new();
        if (!schema_builder)
            THROW(e, "Failed to create Arrow schema builder");
        
        // Add columns to schema
        for (int i = 0; i < priv->meta.columns.length; i++) {
            struct flintdb_column* col = &priv->meta.columns.a[i];
            const char* arrow_type = flintdb_type_to_arrow_format(col->type);
            if (g_arrow.schema_builder_add_column(schema_builder, col->name, arrow_type) != 0) {
                g_arrow.schema_builder_free(schema_builder);
                THROW(e, "Failed to add column to schema: %s", col->name);
            }
        }
        
        struct ArrowSchema* schema = g_arrow.schema_builder_build(schema_builder);
        g_arrow.schema_builder_free(schema_builder);
        
        if (!schema)
            THROW(e, "Failed to build Arrow schema");
        
        // Store schema for later use in batch building
        priv->arrow_schema = schema;
        
        // Open Parquet writer
        char* error_msg = NULL;
        priv->arrow_writer = g_arrow.writer_open_file(priv->file, schema, &error_msg);
        if (!priv->arrow_writer) {
            // Free schema if writer open failed
            if (schema && schema->release) {
                schema->release(schema);
                free(schema);
            }
            priv->arrow_schema = NULL;
            THROW(e, "Failed to open Parquet writer: %s - %s", 
                  priv->file, error_msg ? error_msg : "unknown error");
        }
        if (error_msg) {
            free(error_msg);
        }
        
        // Initialize write buffer
        priv->buffer_capacity = 1024; // Buffer up to 1024 rows
        priv->row_buffer = CALLOC(priv->buffer_capacity, sizeof(struct flintdb_row*));
        if (!priv->row_buffer) {
            THROW(e, "Failed to allocate row buffer");
        }
        priv->buffer_size = 0;
        
        priv->writer_opened = 1;
        if (priv->rows < 0)
            priv->rows = 0;
    }

    // Add row to buffer
    if (priv->buffer_size >= priv->buffer_capacity) {
        // Buffer full, flush it
        if (parquetfile_flush_buffer(priv, e) != 0) {
            THROW_S(e);
        }
    }
    
    // Clone the row for buffering (caller may free original)
    struct flintdb_row *cloned = flintdb_row_new(&priv->meta, e);
    if (e && *e) THROW_S(e);
    
    for (int i = 0; i < priv->meta.columns.length; i++) {
        const struct flintdb_variant *v = r->get(r, i, e);
        if (e && *e) {
            cloned->free(cloned);
            THROW_S(e);
        }
        // Copy variant value (set expects non-const)
        struct flintdb_variant v_copy = *v;
        cloned->set(cloned, i, &v_copy, e);
        if (e && *e) {
            cloned->free(cloned);
            THROW_S(e);
        }
    }
    
    priv->row_buffer[priv->buffer_size++] = cloned;
    
    if (priv->rows >= 0)
        priv->rows++;
    
    return 0;

EXCEPTION:
    return -1;
}

static void parquetfile_cursor_close(struct flintdb_cursor_row *cursor) {
    if (!cursor)
        return;
    if (cursor->p) {
        struct parquetfile_cursor_priv *cp = (struct parquetfile_cursor_priv *)cursor->p;
        if (cp) {
            // Release Arrow resources
            if (cp->current_batch.release) {
                cp->current_batch.release(&cp->current_batch);
            }
            if (cp->schema.release) {
                cp->schema.release(&cp->schema);
            }
            if (cp->stream && cp->stream->release) {
                cp->stream->release(cp->stream);
            }
            
            // Free owned stream storage
            if (cp->stream_storage) {
                FREE(cp->stream_storage);
                cp->stream_storage = NULL;
            }
            
            // Close owned reader
            if (cp->arrow_reader && g_arrow.reader_close) {
                g_arrow.reader_close(cp->arrow_reader);
                cp->arrow_reader = NULL;
            }
            
            if (cp->filter) {
                filter_free(cp->filter);
                cp->filter = NULL;
            }
        }
        FREE(cursor->p);
        cursor->p = NULL;
    }
    FREE(cursor);
}

static struct flintdb_row *parquetfile_cursor_next(struct flintdb_cursor_row *cursor, char **e) {
    if (!cursor || !cursor->p)
        return NULL;
    
    struct parquetfile_cursor_priv *cp = (struct parquetfile_cursor_priv *)cursor->p;
    struct filter *filter = cp->filter;
    struct limit *limit = &cp->limit;
    struct flintdb_row *r = NULL;

    // One-time initialization
    if (!cp->initialized) {
        cp->initialized = 1;
        cp->rowidx = 0;
        cp->finished = 0;
        cp->batch_row_idx = 0;
        
        // Get schema from stream
        if (!cp->stream) {
            cp->finished = 1;
            return NULL;
        }
        
        if (cp->stream->get_schema(cp->stream, &cp->schema) != 0) {
            const char* err = cp->stream->get_last_error ? 
                             cp->stream->get_last_error(cp->stream) : "unknown";
            THROW(e, "Failed to get schema from Arrow stream: %s", err);
        }
    }

    if (cp->finished)
        return NULL;

    // Main read loop
    for (;;) {
        // If current batch is exhausted, get next batch
        if (cp->batch_row_idx >= cp->current_batch.length) {
            // Release previous batch
            if (cp->current_batch.release) {
                cp->current_batch.release(&cp->current_batch);
                memset(&cp->current_batch, 0, sizeof(struct ArrowArray));
            }
            
            // Get next batch from stream
            int status = cp->stream->get_next(cp->stream, &cp->current_batch);
            if (status != 0) {
                const char* err = cp->stream->get_last_error ? 
                                 cp->stream->get_last_error(cp->stream) : "unknown";
                THROW(e, "Failed to get next batch from Arrow stream: %s", err);
            }
            
            // End of stream
            if (cp->current_batch.release == NULL) {
                cp->finished = 1;
                return NULL;
            }
            
            cp->batch_row_idx = 0;
        }
        
        // Extract row from current batch
        i64 row_in_batch = cp->batch_row_idx++;
        
        // Apply offset skipping without materializing row
        if (limit->skip(limit)) {
            cp->rowidx++;
            continue;
        }
        
        // Materialize row from Arrow columnar format
        r = flintdb_row_new((struct flintdb_meta *)cp->meta, e);
        if (e && *e)
            THROW_S(e);
        
        // Extract each column value from Arrow array
        for (i64 col = 0; col < cp->current_batch.n_children && col < cp->meta->columns.length; col++) {
            struct ArrowArray* col_array = cp->current_batch.children[col];
            if (!col_array || !col_array->buffers) {
                // Null column - set NIL
                struct flintdb_variant v = {.type = VARIANT_NULL};
                r->set(r, (int)col, &v, e);
                if (e && *e)
                    THROW_S(e);
                continue;
            }
            
            // Check if value is null (validity bitmap at buffer[0])
            const uint8_t* validity = (const uint8_t*)col_array->buffers[0];
            if (validity && !(validity[row_in_batch / 8] & (1 << (row_in_batch % 8)))) {
                // Null value
                struct flintdb_variant v = {.type = VARIANT_NULL};
                r->set(r, (int)col, &v, e);
                if (e && *e)
                    THROW_S(e);
                continue;
            }
            
            // Extract value based on type (data buffer at buffer[1])
            const struct flintdb_column* meta_col = &cp->meta->columns.a[col];
            const void* data_buf = col_array->buffers[1];
            
            struct flintdb_variant v = {0};
            v.type = meta_col->type;
            
            // Type-specific extraction
            switch (meta_col->type) {
                case VARIANT_INT8:
                    v.value.i = ((const int8_t*)data_buf)[row_in_batch];
                    break;
                case VARIANT_UINT8:
                    v.value.i = ((const uint8_t*)data_buf)[row_in_batch];
                    break;
                case VARIANT_INT16:
                    v.value.i = ((const int16_t*)data_buf)[row_in_batch];
                    break;
                case VARIANT_UINT16:
                    v.value.i = ((const uint16_t*)data_buf)[row_in_batch];
                    break;
                case VARIANT_INT32:
                    v.value.i = ((const int32_t*)data_buf)[row_in_batch];
                    break;
                case VARIANT_UINT32:
                    v.value.i = ((const uint32_t*)data_buf)[row_in_batch];
                    break;
                case VARIANT_INT64:
                    v.value.i = ((const int64_t*)data_buf)[row_in_batch];
                    break;
                case VARIANT_FLOAT:
                    v.value.f = ((const float*)data_buf)[row_in_batch];
                    break;
                case VARIANT_DOUBLE:
                    v.value.f = ((const double*)data_buf)[row_in_batch];
                    break;
                case VARIANT_STRING: {
                    // String: offset buffer + data buffer
                    const int32_t* offsets = (const int32_t*)col_array->buffers[1];
                    const char* str_data = (const char*)col_array->buffers[2];
                    int32_t start = offsets[row_in_batch];
                    int32_t end = offsets[row_in_batch + 1];
                    u32 len = (u32)(end - start);
                    flintdb_variant_string_set(&v, str_data + start, len);
                    break;
                }
                case VARIANT_BYTES: {
                    // Binary: offset buffer + data buffer
                    const int32_t* offsets = (const int32_t*)col_array->buffers[1];
                    const char* bin_data = (const char*)col_array->buffers[2];
                    int32_t start = offsets[row_in_batch];
                    int32_t end = offsets[row_in_batch + 1];
                    u32 len = (u32)(end - start);
                    flintdb_variant_bytes_set(&v, bin_data + start, len);
                    break;
                }
                default:
                    // Unsupported type - set as NIL
                    v.type = VARIANT_NULL;
                    break;
            }
            
            r->set(r, (int)col, &v, e);
            if (e && *e)
                THROW_S(e);
        }
        
        // Apply filter
        int matched = 1;
        if (filter != NULL) {
            int cmp = filter_compare(filter, r, e);
            if (e && *e)
                THROW_S(e);
            matched = (cmp == 0);
        }
        
        if (matched) {
            if (limit->remains(limit) <= 0) {
                r->free(r);
                cp->finished = 1;
                return NULL;
            }
            cp->rowidx++;
            return r;
        }
        
        // Not matched - free and continue
        cp->rowidx++;
        r->free(r);
        r = NULL;
    }

EXCEPTION:
    if (r)
        r->free(r);
    return NULL;
}

static struct flintdb_cursor_row *parquetfile_find(const struct flintdb_genericfile *me, struct limit limit, struct filter *filter, char **e) {
    struct parquetfile_priv *priv = NULL;
    struct flintdb_cursor_row *cursor = NULL;

    if (!me || !me->priv)
        THROW(e, "invalid parquetfile");
    priv = (struct parquetfile_priv *)me->priv;

    // Load Arrow library if not already loaded
    if (!g_arrow_initialized) {
        if (arrow_load_library(e) != 0)
            THROW_S(e);
    }

    // Create a new reader and stream for each cursor (Arrow streams are not reusable)
    if (!g_arrow.reader_open_file) {
        THROW(e, "Arrow reader_open_file function not available");
    }
    
    char* error_msg = NULL;
    void* arrow_reader = g_arrow.reader_open_file(priv->file, &error_msg);
    if (!arrow_reader) {
        THROW(e, "Failed to open Parquet reader: %s - %s", 
              priv->file, error_msg ? error_msg : "unknown error");
    }
    if (error_msg) {
        free(error_msg);
        error_msg = NULL;
    }

    cursor = CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!cursor)
        THROW(e, "Failed to allocate memory for cursor");
    cursor->p = CALLOC(1, sizeof(struct parquetfile_cursor_priv));
    if (!cursor->p)
        THROW(e, "Failed to allocate memory for cursor private data");

    struct parquetfile_cursor_priv *cp = (struct parquetfile_cursor_priv *)cursor->p;
    
    // Each cursor owns its own reader and stream
    cp->arrow_reader = arrow_reader;
    cp->stream_storage = CALLOC(1, sizeof(struct ArrowArrayStream));
    if (!cp->stream_storage)
        THROW(e, "Failed to allocate Arrow stream");
    
    // Get Arrow stream from reader
    if (g_arrow.reader_get_stream && 
        g_arrow.reader_get_stream(cp->arrow_reader, cp->stream_storage) != 0) {
        THROW(e, "Failed to get Arrow stream from reader");
    }
    
    cp->filter = (struct filter *)filter;
    cp->limit = limit;
    // Initialize limit counters
    cp->limit.priv.n = (cp->limit.priv.limit < 0) ? INT_MAX : cp->limit.priv.limit;
    cp->limit.priv.o = cp->limit.priv.offset;
    cp->rowidx = 0;
    cp->initialized = 0;
    cp->finished = 0;
    cp->stream = cp->stream_storage;
    cp->meta = &priv->meta;
    memset(&cp->current_batch, 0, sizeof(struct ArrowArray));
    memset(&cp->schema, 0, sizeof(struct ArrowSchema));

    cursor->next = parquetfile_cursor_next;
    cursor->close = parquetfile_cursor_close;

    return cursor;

EXCEPTION:
    if (cursor) {
        if (cursor->p)
            FREE(cursor->p);
        FREE(cursor);
    }
    if (filter)
        filter_free(filter);
    return NULL;
}

static struct flintdb_cursor_row *parquetfile_find_where(const struct flintdb_genericfile *me, const char *where, char **e) {
    if (!me || !me->priv)
        return NULL;

    struct parquetfile_priv *priv = (struct parquetfile_priv *)me->priv;
    struct filter *filter = NULL;
    struct flintdb_sql *q = NULL;

    // Build SQL: SELECT * FROM <file> WHERE <where>
    char sql[SQL_STRING_LIMIT] = {0};
    if (where && where[0]) {
        snprintf(sql, sizeof(sql), "SELECT * FROM %s %s", priv->file, where);
    } else {
        snprintf(sql, sizeof(sql), "SELECT * FROM %s", priv->file);
    }

    // Parse SQL
    q = flintdb_sql_parse(sql, e);
    if (e && *e)
        THROW_S(e);

    // Compile WHERE filter (Parquet files have no index, so all is non-indexable)
    filter = filter_compile(q->where, &priv->meta, e);
    if (e && *e)
        THROW_S(e);

    // Parse LIMIT
    struct limit lim = !strempty(q->limit) ? limit_parse(q->limit) : NOLIMIT;
    flintdb_sql_free(q);

    return parquetfile_find(me, lim, filter, e);

EXCEPTION:
    if (filter) filter_free(filter);
    if (q) flintdb_sql_free(q);
    return NULL;
}

static struct flintdb_meta parquetfile_meta_from_schema(const char *file, char **e) {
    // Initialize meta with sensible defaults
    char base[PATH_MAX] = {0};
    getname(file, base);
    struct flintdb_meta m = flintdb_meta_new(base, e);
    if (e && *e) {
        m = (struct flintdb_meta){0,};
    }

    // Load Arrow library if not already loaded
    if (!g_arrow_initialized) {
        if (arrow_load_library(e) != 0) {
            // Return empty meta on library load failure
            return m;
        }
    }

    // Open Parquet reader temporarily to get schema
    void* reader = NULL;
    struct ArrowArrayStream stream = {0};
    struct ArrowSchema schema = {0};
    
    if (!g_arrow.reader_open_file || !g_arrow.reader_get_stream) {
        THROW(e, "Arrow reader functions not available");
    }
    
    char* error_msg = NULL;
    reader = g_arrow.reader_open_file(file, &error_msg);
    if (!reader) {
        THROW(e, "Failed to open Parquet reader for schema: %s - %s", 
              file, error_msg ? error_msg : "unknown error");
    }
    if (error_msg) {
        free(error_msg);
    }
    
    if (g_arrow.reader_get_stream(reader, &stream) != 0) {
        THROW(e, "Failed to get Arrow stream from reader");
    }
    
    if (stream.get_schema(&stream, &schema) != 0) {
        const char* err = stream.get_last_error ? stream.get_last_error(&stream) : "unknown";
        THROW(e, "Failed to get schema from Arrow stream: %s", err);
    }
    
    // Convert Arrow schema to FlintDB Meta
    if (schema.format && schema.format[0] == '+' && schema.format[1] == 's') {
        // Struct schema - extract child columns
        for (int64_t i = 0; i < schema.n_children && i < MAX_COLUMNS_LIMIT; i++) {
            struct ArrowSchema* child = schema.children[i];
            if (!child) continue;
            
            struct flintdb_column* col = &m.columns.a[m.columns.length++];
            memset(col, 0, sizeof(struct flintdb_column));
            
            // Copy column name
            if (child->name) {
                strncpy(col->name, child->name, MAX_COLUMN_NAME_LIMIT - 1);
            } else {
                snprintf(col->name, MAX_COLUMN_NAME_LIMIT, "col%d", (int)i);
            }
            
            // Map Arrow type to FlintDB type
            col->type = arrow_format_to_flintdb_type(child->format);
            
            // Set default bytes based on type
            switch (col->type) {
                case VARIANT_STRING:
                case VARIANT_BYTES:
                    col->bytes = 65535;
                    break;
                default:
                    col->bytes = 0; // Use default for numeric types
                    break;
            }
        }
    }
    
    // Clean up
    if (schema.release)
        schema.release(&schema);
    if (stream.release)
        stream.release(&stream);
    if (reader && g_arrow.reader_close)
        g_arrow.reader_close(reader);
    
    if (m.columns.length == 0) {
        THROW(e, "No columns found in Parquet schema: %s", file);
    }
    
    return m;

EXCEPTION:
    if (schema.release)
        schema.release(&schema);
    if (stream.release)
        stream.release(&stream);
    if (reader && g_arrow.reader_close)
        g_arrow.reader_close(reader);
    return m;
}

static void parquetfile_close(struct flintdb_genericfile *me) {
    if (!me)
        return;

    if (me->priv) {
        DEBUG("close parquet file: %s", ((struct parquetfile_priv *)me->priv)->file);
        struct parquetfile_priv *priv = (struct parquetfile_priv *)me->priv;
        
        if (priv) {
            // Flush any remaining buffered rows before closing
            if (priv->row_buffer && priv->buffer_size > 0) {
                DEBUG("Flushing %d remaining rows on close", priv->buffer_size);
                char *flush_error = NULL;
                if (parquetfile_flush_buffer(priv, &flush_error) != 0) {
                    WARN("Failed to flush buffer on close: %s", flush_error ? flush_error : "unknown error");
                    if (flush_error) FREE(flush_error);
                }
            }
            
            // Free buffered rows
            if (priv->row_buffer) {
                for (int i = 0; i < priv->buffer_size; i++) {
                    if (priv->row_buffer[i]) {
                        priv->row_buffer[i]->free(priv->row_buffer[i]);
                        priv->row_buffer[i] = NULL;
                    }
                }
                FREE(priv->row_buffer);
                priv->row_buffer = NULL;
                priv->buffer_size = 0;
            }
            
            // Close Arrow writer/reader handles
            if (priv->arrow_writer && g_arrow.writer_close) {
                g_arrow.writer_close(priv->arrow_writer);
                priv->arrow_writer = NULL;
            }
            
            // Free schema
            if (priv->arrow_schema) {
                if (priv->arrow_schema->release) {
                    priv->arrow_schema->release(priv->arrow_schema);
                }
                FREE(priv->arrow_schema);
                priv->arrow_schema = NULL;
            }
            
            if (priv->stream.release) {
                priv->stream.release(&priv->stream);
                memset(&priv->stream, 0, sizeof(struct ArrowArrayStream));
            }
            
            if (priv->arrow_reader && g_arrow.reader_close) {
                g_arrow.reader_close(priv->arrow_reader);
                priv->arrow_reader = NULL;
            }
            
            // Clean up .crc file if exists (similar to Java implementation)
            char crc_file[PATH_MAX + 10];
            char dir[PATH_MAX] = {0};
            char base[PATH_MAX] = {0};
            getdir(priv->file, dir);
            getname(priv->file, base);
            snprintf(crc_file, sizeof(crc_file), "%s%c.%s.crc", dir, PATH_CHAR, base);
            if (access(crc_file, F_OK) == 0) {
                unlink(crc_file);
            }
            
            flintdb_meta_close(&priv->meta);
        }
        FREE(priv);
        me->priv = NULL;
    }
    FREE(me);
    DEBUG("closed");
}

struct flintdb_genericfile *parquetfile_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e) {
    // Fast-fail on obvious issues before allocating resources
    if (!file || !*file) {
        if (e)
            *e = "file path is empty";
        return NULL;
    }

    struct flintdb_genericfile *f = CALLOC(1, sizeof(struct flintdb_genericfile));
    struct parquetfile_priv *priv = NULL;
    if (!f)
        THROW(e, "Failed to allocate memory for file");

    // If opening for FLINTDB_RDONLY, ensure the data file actually exists
    if (mode == FLINTDB_RDONLY) {
        if (access(file, F_OK) != 0) {
            THROW(e, "parquet file does not exist: %s", file);
        }
    }

    f->close = parquetfile_close;
    f->rows = parquetfile_rows;
    f->bytes = parquetfile_bytes;
    f->meta = parquetfile_meta;
    f->write = parquetfile_write;
    f->find = parquetfile_find_where;

    priv = f->priv = CALLOC(1, sizeof(struct parquetfile_priv));
    if (!priv)
        THROW(e, "Failed to allocate memory for file private data");

    strncpy(priv->file, file, PATH_MAX - 1);
    priv->mode = mode;
    priv->rows = -1; // cached row count for optimization
    priv->writer_opened = 0;
    priv->row_buffer = NULL;
    priv->buffer_size = 0;
    priv->buffer_capacity = 0;

    // Meta handling similar to genericfile_open
    if (NULL == meta) {
        // No meta provided: try to read from .desc or infer from Parquet schema
        char desc[PATH_MAX] = {0};
        snprintf(desc, sizeof(desc), "%s%s", file, META_NAME_SUFFIX);
        if (access(desc, F_OK) == 0) {
            priv->meta = flintdb_meta_open(desc, e);
            if (e && *e)
                THROW_S(e);
            if (priv->meta.columns.length <= 0)
                THROW(e, "meta has no columns");
        } else {
            // Infer from Parquet schema
            priv->meta = parquetfile_meta_from_schema(file, e);
            if (e && *e)
                THROW_S(e);
            if (priv->meta.columns.length <= 0)
                THROW(e, "meta has no columns");
        }
    } else if (mode == FLINTDB_RDWR) {
        // FLINTDB_RDWR mode: create or validate .desc file
        char dir[PATH_MAX] = {0};
        getdir(file, dir);
        mkdirs(dir, S_IRWXU);
        
        char desc[PATH_MAX] = {0};
        snprintf(desc, sizeof(desc), "%s%s", file, META_NAME_SUFFIX);
        if (access(desc, F_OK) != 0) {
            if (meta->columns.length <= 0)
                THROW(e, "meta has no columns");
            if (flintdb_meta_write(meta, desc, e) != 0)
                THROW_S(e);
            priv->meta = *meta;
            // Do not take ownership of caller's meta internals
            priv->meta.priv = NULL;
        } else {
            struct flintdb_meta existing = flintdb_meta_open(desc, e);
            if (e && *e)
                THROW_S(e);
            if (existing.columns.length <= 0)
                THROW(e, "existing meta has no columns");
            if (flintdb_meta_compare(&existing, meta) != 0)
                THROW(e, "meta does not match existing: %s", desc);
            flintdb_meta_close(&existing);
            priv->meta = *meta;
            // Do not take ownership of caller's meta internals
            priv->meta.priv = NULL;
        }
    } else {
        // FLINTDB_RDONLY with provided meta: borrow schema but do not own internals
        priv->meta = *meta;
        priv->meta.priv = NULL;
    }

    // Load Arrow library on first use
    if (!g_arrow_initialized) {
        char *load_err = NULL;
        if (arrow_load_library(&load_err) != 0) {
            // Library or wrapper not available
            DEBUG("Arrow library/wrapper not available: %s", load_err ? load_err : "unknown");
            
            // Provide helpful error message
            THROW(e, "Parquet format not fully supported yet. Apache Arrow C wrapper needed.\n"
                     "\nTo implement Parquet support:\n"
                     "1. Install Apache Arrow: brew install apache-arrow (macOS) or apt install libarrow-dev (Linux)\n"
                     "2. Create C wrapper library that exports these functions:\n"
                     "   - arrow_parquet_reader_open_file()\n"
                     "   - arrow_parquet_writer_open_file()\n"
                     "   - arrow_parquet_reader_get_stream()\n"
                     "   - arrow_parquet_writer_write_batch()\n"
                     "3. Link wrapper library with flintdb\n"
                     "\nAlternatively, use CSV/TSV format which is fully supported.");
        }
    }

    DEBUG("parquetfile_open: opened %s (mode=%s)", file, mode == FLINTDB_RDONLY ? "r" : "rw");
    return f;

EXCEPTION:
    if (f)
        parquetfile_close(f);
    return NULL;
}