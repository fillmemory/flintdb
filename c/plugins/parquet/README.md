# FlintDB Parquet Plugin

Apache Arrow/Parquet C wrapper plugin for FlintDB.

## Overview

This plugin provides a C API wrapper around Apache Arrow C++ library for Parquet file support in FlintDB. It uses the Arrow C Data Interface for ABI-stable data exchange.

## Building

```bash
./build.sh
```

Requirements:
- Apache Arrow C++ library (libarrow)
- C++11 compiler

### macOS
```bash
brew install apache-arrow
```

### Linux (Ubuntu/Debian)
```bash
sudo apt-get install libarrow-dev libparquet-dev
```

## Exported C API

The plugin exports the following functions for dynamic loading:

```c
// Reader functions
void* FlintDB_parquet_reader_open(const char* path, char** error);
void FlintDB_parquet_reader_close(void* reader);
int FlintDB_parquet_reader_get_stream(void* reader, struct ArrowArrayStream* out);
int64_t FlintDB_parquet_reader_num_rows(void* reader);

// Writer functions
void* FlintDB_parquet_writer_open(const char* path, struct ArrowSchema* schema, char** error);
void FlintDB_parquet_writer_close(void* writer);
int FlintDB_parquet_writer_write_batch(void* writer, struct ArrowArray* batch);

// Schema builder functions
void* FlintDB_parquet_schema_builder_new();
void FlintDB_parquet_schema_builder_free(void* builder);
int FlintDB_parquet_schema_builder_add_column(void* builder, const char* name, const char* arrow_type);
struct ArrowSchema* FlintDB_parquet_schema_builder_build(void* builder);
```

## Installation

The compiled shared library will be placed in:
- macOS: `../../lib/libflintdb_parquet.dylib`
- Linux: `../../lib/libflintdb_parquet.so`

FlintDB will automatically load this plugin when accessing `.parquet` files.
