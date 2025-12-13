#include "parquet_plugin.h"
#include <stdio.h>
#include <stdlib.h>

/**
 * Simple test program to verify the Parquet plugin is working correctly.
 * 
 * Compile:
 *   gcc -o test_plugin test_plugin.c -L../../lib -lflintdb_parquet -I.
 * 
 * Run:
 *   LD_LIBRARY_PATH=../../lib:$LD_LIBRARY_PATH ./test_plugin
 *   (or on macOS: DYLD_LIBRARY_PATH=../../lib:$DYLD_LIBRARY_PATH ./test_plugin)
 */

int main(int argc, char** argv) {
    printf("Testing FlintDB Parquet Plugin\n");
    printf("==============================\n\n");

    // Test 1: Schema Builder
    printf("Test 1: Schema Builder\n");
    void* builder = flintdb_parquet_schema_builder_new();
    if (!builder) {
        fprintf(stderr, "ERROR: Failed to create schema builder\n");
        return 1;
    }
    printf("  ✓ Created schema builder\n");

    int ret;
    ret = flintdb_parquet_schema_builder_add_column(builder, "id", "l");  // int64
    if (ret != 0) {
        fprintf(stderr, "ERROR: Failed to add column 'id'\n");
        return 1;
    }
    printf("  ✓ Added column 'id' (int64)\n");

    ret = flintdb_parquet_schema_builder_add_column(builder, "name", "u");  // utf8
    if (ret != 0) {
        fprintf(stderr, "ERROR: Failed to add column 'name'\n");
        return 1;
    }
    printf("  ✓ Added column 'name' (utf8)\n");

    ret = flintdb_parquet_schema_builder_add_column(builder, "value", "g");  // double
    if (ret != 0) {
        fprintf(stderr, "ERROR: Failed to add column 'value'\n");
        return 1;
    }
    printf("  ✓ Added column 'value' (double)\n");

    struct ArrowSchema* schema = flintdb_parquet_schema_builder_build(builder);
    if (!schema) {
        fprintf(stderr, "ERROR: Failed to build schema\n");
        return 1;
    }
    printf("  ✓ Built schema successfully\n");

    flintdb_parquet_schema_builder_free(builder);
    printf("  ✓ Freed schema builder\n");

    // Test 2: Writer (optional - requires file path argument)
    if (argc > 1) {
        printf("\nTest 2: Parquet Writer\n");
        char* error = NULL;
        void* writer = flintdb_parquet_writer_open(argv[1], schema, &error);
        if (!writer) {
            fprintf(stderr, "ERROR: Failed to open writer: %s\n", error ? error : "unknown");
            if (error) free(error);
            return 1;
        }
        printf("  ✓ Opened Parquet writer: %s\n", argv[1]);

        // TODO: Write some test data

        flintdb_parquet_writer_close(writer);
        printf("  ✓ Closed writer\n");
    } else {
        printf("\nTest 2: Parquet Writer (skipped - no file path provided)\n");
        printf("  Usage: %s <output.parquet>\n", argv[0]);
    }

    // Clean up schema
    if (schema && schema->release) {
        schema->release(schema);
        free(schema);
    }

    printf("\n✓ All tests passed!\n");
    return 0;
}
