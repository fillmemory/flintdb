#ifndef FLINTDB_PARQUET_PLUGIN_H
#define FLINTDB_PARQUET_PLUGIN_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Include Arrow C Data Interface definitions
// This header defines ArrowSchema, ArrowArray, and ArrowArrayStream
// If Arrow is not available, these can be defined manually.
#include "arrow/c/abi.h"

// The following block is now redundant if arrow/c/abi.h is included,
// but kept for standalone use cases where arrow/c/abi.h might not be available.
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

// Forward declarations only - full definitions in arrow/c/abi.h
struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream;

#endif // ARROW_C_DATA_INTERFACE

// ============================================================================
// FlintDB Parquet Plugin C API
// ============================================================================

// Reader API
void *flintdb_parquet_reader_open(const char *path, char **error);
void flintdb_parquet_reader_close(void *reader);
int flintdb_parquet_reader_get_stream(void *reader, struct ArrowArrayStream *out);
int64_t flintdb_parquet_reader_num_rows(void *reader);
char *flintdb_parquet_reader_get_metadata(void *reader, const char *key, char **error); // Caller must free() returned string

// Writer API
void *flintdb_parquet_writer_open(const char *path, struct ArrowSchema *schema, char **error);
void flintdb_parquet_writer_close(void *writer);
int flintdb_parquet_writer_write_batch(void *writer, struct ArrowArray *batch);

// Batch Builder API (for buffering rows before writing)
void *flintdb_parquet_batch_builder_new(struct ArrowSchema *schema);
void flintdb_parquet_batch_builder_free(void *builder);
int flintdb_parquet_batch_builder_append_int32(void *builder, int col, int32_t value);
int flintdb_parquet_batch_builder_append_int64(void *builder, int col, int64_t value);
int flintdb_parquet_batch_builder_append_double(void *builder, int col, double value);
int flintdb_parquet_batch_builder_append_string(void *builder, int col, const char *value, int32_t length);
int flintdb_parquet_batch_builder_append_null(void *builder, int col);
int flintdb_parquet_batch_builder_finish_row(void *builder);
struct ArrowArray *flintdb_parquet_batch_builder_build(void *builder, int *num_rows);

// Schema Builder API
void *flintdb_parquet_schema_builder_new(void);
void flintdb_parquet_schema_builder_free(void *builder);
int flintdb_parquet_schema_builder_add_column(void *builder, const char *name, const char *arrow_type);
int flintdb_parquet_schema_builder_add_metadata(void *builder, const char *key, const char *value);
struct ArrowSchema *flintdb_parquet_schema_builder_build(void *builder);

#ifdef __cplusplus
}
#endif

#endif // FLINTDB_PARQUET_PLUGIN_H
