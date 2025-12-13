#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/c/bridge.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "parquet_plugin.h"
#include <cstring>
#include <memory>
#include <vector>

// Check Arrow version for API compatibility
// Arrow version macros are defined in arrow/util/config.h or arrow/version.h
#ifdef ARROW_VERSION_MAJOR
  #define ARROW_VERSION_INT (ARROW_VERSION_MAJOR * 1000 + ARROW_VERSION_MINOR)
#else
  // If version not defined, assume older API (pre-10.0)
  #define ARROW_VERSION_INT 9000
#endif
// Arrow API changed in version 10.0 and again in 21.0
// - Arrow <10.0: Returns Result<>
// - Arrow 10.0-20.x: Uses output parameters (Status + pointer)
// - Arrow 21.0+: Returns Result<> again
#define ARROW_API_V10_STYLE (ARROW_VERSION_INT >= 10000 && ARROW_VERSION_INT < 21000)

// ============================================================================
// Internal C++ wrapper structures
// ============================================================================

struct ParquetReader {
    std::shared_ptr<arrow::RecordBatchReader> reader;
    std::shared_ptr<parquet::arrow::FileReader> file_reader;
    int64_t num_rows;
};

struct ParquetWriter {
    std::shared_ptr<parquet::arrow::FileWriter> writer;
    std::shared_ptr<arrow::io::FileOutputStream> output;
    std::shared_ptr<arrow::Schema> schema;
};

struct SchemaBuilder {
    std::vector<std::shared_ptr<arrow::Field>> fields;
};

struct BatchBuilder {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    int num_rows;
    int current_col;
};

// ============================================================================
// Helper functions
// ============================================================================

static std::shared_ptr<arrow::DataType> arrow_type_from_string(const char* type_str) {
    if (!type_str) return arrow::utf8();
    
    switch (type_str[0]) {
        case 'c': return arrow::int8();
        case 'C': return arrow::uint8();
        case 's': return arrow::int16();
        case 'S': return arrow::uint16();
        case 'i': return arrow::int32();
        case 'I': return arrow::uint32();
        case 'l': return arrow::int64();
        case 'L': return arrow::uint64();
        case 'f': return arrow::float32();
        case 'g': return arrow::float64();
        case 'u': return arrow::utf8();
        case 'z': return arrow::binary();
        case 't':
            if (type_str[1] == 'd') return arrow::date32();
            if (type_str[1] == 't') return arrow::time64(arrow::TimeUnit::MICRO);
            return arrow::utf8();
        default: return arrow::utf8();
    }
}

static void set_error(char** error, const std::string& message) {
    if (error) {
        *error = strdup(message.c_str());
    }
}

// ============================================================================
// Reader Implementation
// ============================================================================

void* flintdb_parquet_reader_open(const char* path, char** error) {
    if (!path) {
        set_error(error, "Path is null");
        return nullptr;
    }

    try {
        auto reader = new ParquetReader();
        
        // Open file
        auto maybe_input = arrow::io::ReadableFile::Open(path);
        if (!maybe_input.ok()) {
            set_error(error, "Failed to open file: " + maybe_input.status().ToString());
            delete reader;
            return nullptr;
        }
        std::shared_ptr<arrow::io::RandomAccessFile> input = *maybe_input;

        // Open Parquet file reader
#if ARROW_API_V10_STYLE
        // Arrow 10.0-20.x API with output parameter
        std::unique_ptr<parquet::arrow::FileReader> file_reader;
        auto status = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &file_reader);
        if (!status.ok()) {
            set_error(error, "Failed to open Parquet reader: " + status.ToString());
            delete reader;
            return nullptr;
        }
        reader->file_reader = std::move(file_reader);
#else
        // Arrow 9.x and 21.0+ API with Result<>
        auto maybe_reader = parquet::arrow::OpenFile(input, arrow::default_memory_pool());
        if (!maybe_reader.ok()) {
            set_error(error, "Failed to open Parquet reader: " + maybe_reader.status().ToString());
            delete reader;
            return nullptr;
        }
        reader->file_reader = std::move(*maybe_reader);
#endif

        // Get metadata
        auto metadata = reader->file_reader->parquet_reader()->metadata();
        reader->num_rows = metadata->num_rows();

        // Create record batch reader
#if ARROW_API_V10_STYLE
        // Arrow 10.0-20.x API with output parameter
        std::shared_ptr<arrow::RecordBatchReader> batch_reader;
        auto status_reader = reader->file_reader->GetRecordBatchReader(&batch_reader);
        if (!status_reader.ok()) {
            set_error(error, "Failed to create batch reader: " + status_reader.ToString());
            delete reader;
            return nullptr;
        }
        reader->reader = std::move(batch_reader);
#else
        // Arrow 9.x and 21.0+ API with Result<>
        auto maybe_batch_reader = reader->file_reader->GetRecordBatchReader();
        if (!maybe_batch_reader.ok()) {
            set_error(error, "Failed to create batch reader: " + maybe_batch_reader.status().ToString());
            delete reader;
            return nullptr;
        }
        reader->reader = std::move(*maybe_batch_reader);
#endif

        return reader;
    } catch (const std::exception& e) {
        set_error(error, std::string("Exception: ") + e.what());
        return nullptr;
    }
}

void flintdb_parquet_reader_close(void* reader) {
    if (reader) {
        delete static_cast<ParquetReader*>(reader);
    }
}

int flintdb_parquet_reader_get_stream(void* reader, struct ArrowArrayStream* out) {
    if (!reader || !out) return -1;

    try {
        auto r = static_cast<ParquetReader*>(reader);
        // Use arrow::ExportRecordBatchReader from arrow/c/bridge.h
        auto status = arrow::ExportRecordBatchReader(r->reader, out);
        return status.ok() ? 0 : -1;
    } catch (...) {
        return -1;
    }
}

int64_t flintdb_parquet_reader_num_rows(void* reader) {
    if (!reader) return -1;
    return static_cast<ParquetReader*>(reader)->num_rows;
}

// ============================================================================
// Writer Implementation
// ============================================================================

void* flintdb_parquet_writer_open(const char* path, struct ArrowSchema* schema_c, char** error) {
    if (!path || !schema_c) {
        set_error(error, "Path or schema is null");
        return nullptr;
    }

    try {
        auto writer = new ParquetWriter();

        // Import Arrow schema from C interface
        auto maybe_schema = arrow::ImportSchema(schema_c);
        if (!maybe_schema.ok()) {
            set_error(error, "Failed to import schema: " + maybe_schema.status().ToString());
            delete writer;
            return nullptr;
        }
        writer->schema = std::move(maybe_schema).ValueOrDie();

        // Open output file
        auto maybe_output = arrow::io::FileOutputStream::Open(path);
        if (!maybe_output.ok()) {
            set_error(error, "Failed to open output file: " + maybe_output.status().ToString());
            delete writer;
            return nullptr;
        }
        writer->output = *maybe_output;

        // Create Parquet writer
        auto maybe_writer = parquet::arrow::FileWriter::Open(
            *writer->schema,
            arrow::default_memory_pool(),
            writer->output
        );
        if (!maybe_writer.ok()) {
            set_error(error, "Failed to create Parquet writer: " + maybe_writer.status().ToString());
            delete writer;
            return nullptr;
        }
        writer->writer = std::move(*maybe_writer);

        return writer;
    } catch (const std::exception& e) {
        set_error(error, std::string("Exception: ") + e.what());
        return nullptr;
    }
}

void flintdb_parquet_writer_close(void* writer) {
    if (writer) {
        auto w = static_cast<ParquetWriter*>(writer);
        if (w->writer) {
            (void)w->writer->Close();  // Ignore status
        }
        if (w->output) {
            (void)w->output->Close();  // Ignore status
        }
        delete w;
    }
}

int flintdb_parquet_writer_write_batch(void* writer, struct ArrowArray* batch) {
    if (!writer || !batch) return -1;

    try {
        auto w = static_cast<ParquetWriter*>(writer);
        
        // Import Arrow array from C interface
        auto maybe_batch = arrow::ImportRecordBatch(batch, w->schema);
        if (!maybe_batch.ok()) {
            return -1;
        }
        
        auto status = w->writer->WriteRecordBatch(**maybe_batch);
        return status.ok() ? 0 : -1;
    } catch (...) {
        return -1;
    }
}

// ============================================================================
// Schema Builder Implementation
// ============================================================================

void* flintdb_parquet_schema_builder_new(void) {
    return new SchemaBuilder();
}

void flintdb_parquet_schema_builder_free(void* builder) {
    if (builder) {
        delete static_cast<SchemaBuilder*>(builder);
    }
}

int flintdb_parquet_schema_builder_add_column(void* builder, const char* name, const char* arrow_type) {
    if (!builder || !name || !arrow_type) return -1;

    try {
        auto b = static_cast<SchemaBuilder*>(builder);
        auto type = arrow_type_from_string(arrow_type);
        auto field = arrow::field(name, type);
        b->fields.push_back(field);
        return 0;
    } catch (...) {
        return -1;
    }
}

struct ArrowSchema* flintdb_parquet_schema_builder_build(void* builder) {
    if (!builder) return nullptr;

    try {
        auto b = static_cast<SchemaBuilder*>(builder);
        auto schema = arrow::schema(b->fields);
        
        // Allocate ArrowSchema on heap for export
        auto schema_c = new ArrowSchema();
        auto status = arrow::ExportSchema(*schema, schema_c);
        if (!status.ok()) {
            delete schema_c;
            return nullptr;
        }
        
        return schema_c;
    } catch (...) {
        return nullptr;
    }
}

// ============================================================================
// Batch Builder Implementation
// ============================================================================

void* flintdb_parquet_batch_builder_new(struct ArrowSchema* schema_c) {
    if (!schema_c) return nullptr;

    try {
        auto builder = new BatchBuilder();
        
        // Import schema
        auto maybe_schema = arrow::ImportSchema(schema_c);
        if (!maybe_schema.ok()) {
            delete builder;
            return nullptr;
        }
        builder->schema = std::move(maybe_schema).ValueOrDie();
        
        // Create builders for each column
        for (const auto& field : builder->schema->fields()) {
            auto maybe_builder = arrow::MakeBuilder(field->type(), arrow::default_memory_pool());
            if (!maybe_builder.ok()) {
                delete builder;
                return nullptr;
            }
            builder->builders.push_back(std::move(*maybe_builder));
        }
        
        builder->num_rows = 0;
        builder->current_col = 0;
        return builder;
    } catch (...) {
        return nullptr;
    }
}

void flintdb_parquet_batch_builder_free(void* builder) {
    if (builder) {
        delete static_cast<BatchBuilder*>(builder);
    }
}

int flintdb_parquet_batch_builder_append_int32(void* builder, int col, int32_t value) {
    if (!builder) return -1;
    try {
        auto b = static_cast<BatchBuilder*>(builder);
        if (col < 0 || col >= (int)b->builders.size()) return -1;
        auto int_builder = std::static_pointer_cast<arrow::Int32Builder>(b->builders[col]);
        return int_builder->Append(value).ok() ? 0 : -1;
    } catch (...) {
        return -1;
    }
}

int flintdb_parquet_batch_builder_append_int64(void* builder, int col, int64_t value) {
    if (!builder) return -1;
    try {
        auto b = static_cast<BatchBuilder*>(builder);
        if (col < 0 || col >= (int)b->builders.size()) return -1;
        
        // Handle both INT and INT8 types
        auto field_type = b->schema->field(col)->type();
        if (field_type->id() == arrow::Type::INT8) {
            auto builder = std::static_pointer_cast<arrow::Int8Builder>(b->builders[col]);
            return builder->Append((int8_t)value).ok() ? 0 : -1;
        } else {
            auto builder = std::static_pointer_cast<arrow::Int64Builder>(b->builders[col]);
            return builder->Append(value).ok() ? 0 : -1;
        }
    } catch (...) {
        return -1;
    }
}

int flintdb_parquet_batch_builder_append_double(void* builder, int col, double value) {
    if (!builder) return -1;
    try {
        auto b = static_cast<BatchBuilder*>(builder);
        if (col < 0 || col >= (int)b->builders.size()) return -1;
        auto dbl_builder = std::static_pointer_cast<arrow::DoubleBuilder>(b->builders[col]);
        return dbl_builder->Append(value).ok() ? 0 : -1;
    } catch (...) {
        return -1;
    }
}

int flintdb_parquet_batch_builder_append_string(void* builder, int col, const char* value, int32_t length) {
    if (!builder || !value) return -1;
    try {
        auto b = static_cast<BatchBuilder*>(builder);
        if (col < 0 || col >= (int)b->builders.size()) return -1;
        auto str_builder = std::static_pointer_cast<arrow::StringBuilder>(b->builders[col]);
        return str_builder->Append(value, length).ok() ? 0 : -1;
    } catch (...) {
        return -1;
    }
}

int flintdb_parquet_batch_builder_append_null(void* builder, int col) {
    if (!builder) return -1;
    try {
        auto b = static_cast<BatchBuilder*>(builder);
        if (col < 0 || col >= (int)b->builders.size()) return -1;
        return b->builders[col]->AppendNull().ok() ? 0 : -1;
    } catch (...) {
        return -1;
    }
}

int flintdb_parquet_batch_builder_finish_row(void* builder) {
    if (!builder) return -1;
    try {
        auto b = static_cast<BatchBuilder*>(builder);
        b->num_rows++;
        return 0;
    } catch (...) {
        return -1;
    }
}

struct ArrowArray* flintdb_parquet_batch_builder_build(void* builder, int* num_rows) {
    if (!builder) return nullptr;

    try {
        auto b = static_cast<BatchBuilder*>(builder);
        
        // Finish all builders and create arrays
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (auto& col_builder : b->builders) {
            std::shared_ptr<arrow::Array> array;
            auto status = col_builder->Finish(&array);
            if (!status.ok()) {
                return nullptr;
            }
            arrays.push_back(array);
        }
        
        // Create RecordBatch
        auto batch = arrow::RecordBatch::Make(b->schema, b->num_rows, arrays);
        
        // Export to C interface
        auto array_c = new ArrowArray();
        auto status = arrow::ExportRecordBatch(*batch, array_c);
        if (!status.ok()) {
            delete array_c;
            return nullptr;
        }
        
        if (num_rows) *num_rows = b->num_rows;
        return array_c;
    } catch (...) {
        return nullptr;
    }
}
