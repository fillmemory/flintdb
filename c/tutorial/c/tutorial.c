/**
 * @file tutorial.c
 * @brief FlintDB api tutorial
 */
#include "flintdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>


/**
 * @brief Demonstrates how to create a table, define a schema, and insert data.
 */
int tutorial_table_create() {
    printf("--- Running %s ---\n", __FUNCTION__);
    char *e = NULL;
    struct flintdb_table *tbl = NULL;
    struct flintdb_meta mt = {0};

    const char *tablename = "./temp/tutorial_customer.flintdb";
    flintdb_table_drop(tablename, NULL); // Delete old table and meta file if it exists

    // 1. Define the table schema (meta-information)
    mt = flintdb_meta_new(tablename, &e);
    if (e) goto exception;
    
    flintdb_meta_columns_add(&mt, "id", VARIANT_INT64, 0, 0, SPEC_NOT_NULL, "0", "PRIMARY KEY", &e);
    if (e) goto exception;
    flintdb_meta_columns_add(&mt, "name", VARIANT_STRING, 50, 0, SPEC_NOT_NULL, "", "Customer name", &e);
    if (e) goto exception;
    flintdb_meta_columns_add(&mt, "age", VARIANT_INT32, 0, 0, SPEC_NOT_NULL, "0", "Customer age", &e);
    if (e) goto exception;

    char pkey[1][MAX_COLUMN_NAME_LIMIT] = {"id"};
    flintdb_meta_indexes_add(&mt, PRIMARY_NAME, NULL, (const char (*)[MAX_COLUMN_NAME_LIMIT])pkey, 1, &e);
    if (e) goto exception;

    char age_key[1][MAX_COLUMN_NAME_LIMIT] = {"age"};
    flintdb_meta_indexes_add(&mt, "ix_age", NULL, (const char (*)[MAX_COLUMN_NAME_LIMIT])age_key, 1, &e);
    if (e) goto exception;

    const char *mt_sql = "CREATE TABLE tutorial_customer.flintdb " \
    "( " \
    "id INT64 NOT NULL DEFAULT 0 COMMENT 'PRIMARY KEY', " \
    "name STRING NOT NULL DEFAULT '' COMMENT 'Customer name', " \
    "age INT32 NOT NULL DEFAULT 0 COMMENT 'Customer age', " \
    "PRIMARY KEY (id) " \
    ") CACHE=256K" \
    "";
    struct flintdb_sql *q = flintdb_sql_parse(mt_sql, &e);
    assert(q != NULL && e == NULL);
    // flintdb_sql_to_meta(q, &mt, &e); // Optional: parse from SQL string
    flintdb_sql_free(q);

    char sql[2048] = {0};
    if (flintdb_meta_to_sql_string(&mt, sql, sizeof(sql), &e) != 0) goto exception;
    printf("Table schema SQL:\n%s\n\n", sql); // Print the schema using

    // 2. Open the table with the defined schema
    tbl = flintdb_table_open(tablename, FLINTDB_RDWR, &mt, &e);
    if (e || !tbl) goto exception;

    // 3. Insert data rows
    printf("Inserting 3 rows...\n");
    for (int i = 0; i < 3; i++) {
        struct flintdb_row *r = flintdb_row_new(&mt, &e);
        if (e) {
            r->free(r);
            goto exception;
        }

        char name_buffer[50];
        snprintf(name_buffer, sizeof(name_buffer), "Customer %d", i + 1);

        r->i64_set(r, 0, i + 1, &e);       // id
        r->string_set(r, 1, name_buffer, &e); // name
        r->i32_set(r, 2, 30 + i, &e);       // age
        
        if (e) {
            r->free(r);
            goto exception;
        }

        i8 passed = r->validate(r, &e); // validate before applying for not nulls
        if (e || !passed) {
            r->free(r);
            goto exception;
        }

        i64 rowid = tbl->apply(tbl, r, 0, &e);
        r->free(r); // Free the row after applying
        if (e || rowid < 0) goto exception;
    }
    
    printf("Successfully created table and inserted data.\n\n");
    
    tbl->close(tbl);
    flintdb_meta_close(&mt);
    return 0;

exception:
    fprintf(stderr, "Error in %s: %s\n", __FUNCTION__, e);
    if (tbl) tbl->close(tbl);
    flintdb_meta_close(&mt);
    return -1;
}

/**
 * @brief Demonstrates how to find and read data from an existing table.
 */
int tutorial_table_find() {
    printf("--- Running %s ---\n", __FUNCTION__);
    char *e = NULL;
    struct flintdb_table *tbl = NULL;
    struct flintdb_cursor_i64 *c = NULL;

    const char *tablename = "./temp/tutorial_customer.flintdb";

    // 1. Open the table in read-only mode
    tbl = flintdb_table_open(tablename, FLINTDB_RDONLY, NULL, &e);
    if (e || !tbl) goto exception;

    // 2. Find data using a WHERE clause. This returns a cursor of rowids.
    printf("Finding rows where age >= 31:\n");
    c = tbl->find(tbl, "WHERE age >= 31", &e);
    if (e || !c) goto exception;

    // 3. Iterate through the cursor to get rowids
    i64 rowid;
    while ((rowid = c->next(c, &e)) > -1) {
        const struct flintdb_row *r = tbl->read(tbl, rowid, &e);
        if (e || !r) break;
        flintdb_print_row(r);
    }
    if (e) goto exception;

    printf("\nSuccessfully found and read data.\n\n");

    c->close(c);
    tbl->close(tbl);
    return 0;

exception:
    fprintf(stderr, "Error in %s: %s\n", __FUNCTION__, e);
    if (c) c->close(c);
    if (tbl) tbl->close(tbl);
    return -1;
}

/**
 * @brief Demonstrates how to create a TSV file and write data to it.
 */
int tutorial_tsv_create() {
    printf("--- Running %s ---\n", __FUNCTION__);
    char *e = NULL;
    struct flintdb_genericfile *f = NULL;
    struct flintdb_meta mt = {0};

    const char *filepath = "./temp/tutorial_products.tsv";
    flintdb_genericfile_drop(filepath, NULL);

    // 1. Define the schema for the TSV file
    mt = flintdb_meta_new(filepath, &e);
    if (e) goto exception;
    strcpy(mt.format, "tsv");
    mt.delimiter = '\t';
    
    flintdb_meta_columns_add(&mt, "product_id", VARIANT_INT32, SPEC_NOT_NULL, 0, 0, "", "", &e);
    flintdb_meta_columns_add(&mt, "product_name", VARIANT_STRING, 100, 0, SPEC_NOT_NULL, "", "", &e);
    flintdb_meta_columns_add(&mt, "price", VARIANT_DOUBLE, 0, 0, SPEC_NOT_NULL,"", "", &e);
    if (e) goto exception;

    // 2. Open the generic file with the TSV format
    f = flintdb_genericfile_open(filepath, FLINTDB_RDWR, &mt, &e);
    if (e || !f) goto exception;

    // 3. Write data rows
    printf("Writing 3 rows to TSV...\n");
    for (int i = 0; i < 3; i++) {
        struct flintdb_row *r = flintdb_row_new(&mt, &e);
        if (e) {
            r->free(r);
            goto exception;
        }

        char name_buffer[100];
        snprintf(name_buffer, sizeof(name_buffer), "Product-%c", 'A' + i);
        
        // r->i32_set(r, 0, 101 + i, &e);
        // r->string_set(r, 1, name_buffer, &e);
        // r->f64_set(r, 2, 9.99 * (i + 1), &e);
        r->i32_set(r, flintdb_column_at(&mt, "product_id"), 101 + i, &e);
        r->string_set(r, flintdb_column_at(&mt, "product_name"), name_buffer, &e);
        r->f64_set(r, flintdb_column_at(&mt, "price"), 9.99 * (i + 1), &e);

        if (e) {
            r->free(r);
            goto exception;
        }

        if (f->write(f, r, &e) != 0) {
            r->free(r);
            goto exception;
        }
        r->free(r);
    }

    printf("Successfully created TSV file.\n\n");
    
    f->close(f);
    flintdb_meta_close(&mt);
    return 0;

exception:
    fprintf(stderr, "Error in %s: %s\n", __FUNCTION__, e);
    if (f) f->close(f);
    flintdb_meta_close(&mt);
    return -1;
}

/**
 * @brief Demonstrates how to read data from a TSV file.
 */
int tutorial_tsv_find() {
    printf("--- Running %s ---\n", __FUNCTION__);
    char *e = NULL;
    struct flintdb_genericfile *f = NULL;
    struct flintdb_cursor_row *c = NULL;

    const char *filepath = "./temp/tutorial_products.tsv";

    // 1. Open the TSV file in read-only mode. 
    // The schema is loaded from the accompanying .desc file.
    f = flintdb_genericfile_open(filepath, FLINTDB_RDONLY, NULL, &e);
    if (e || !f) goto exception;

    // 2. Find all rows (no WHERE clause)
    printf("Reading all rows from TSV:\n");
    c = f->find(f, "WHERE product_id >= 102", &e);
    if (e || !c) goto exception;
    
    // 3. Iterate through the cursor
    struct flintdb_row *r;
    while ((r = c->next(c, &e)) != NULL) {
        flintdb_print_row(r);
        // NOTE: cursor->next() returns a borrowed row owned by the cursor.
        // Do NOT free it unless you first call r->retain(r).
    }
    if (e) goto exception;
    
    printf("\nSuccessfully read from TSV file.\n\n");

    c->close(c);
    f->close(f);
    return 0;

exception:
    fprintf(stderr, "Error in %s: %s\n", __FUNCTION__, e);
    if (c) c->close(c);
    if (f) f->close(f);
    return -1;
}

/**
 * @brief Comparison function for sorting rows by integer value.
 */
int compare_by_value(const void *ctx, const struct flintdb_row *a, const struct flintdb_row *b) {
    (void)ctx;
    char *err = NULL;
    i32 va = a->i32_get(a, 0, &err);
    i32 vb = b->i32_get(b, 0, &err);
    return va - vb;
}

/**
 * @brief Demonstrates how to use filesort for external sorting of rows.
 */
int tutorial_filesort() {
    printf("--- Running %s ---\n", __FUNCTION__);
    char *e = NULL;
    struct flintdb_meta mt = {0};
    struct flintdb_filesort *fs = NULL;

    const char *filepath = "./temp/tutorial_sort.dat";
    
    // 1. Define the schema for sorting
    mt = flintdb_meta_new(filepath, &e);
    if (e) goto exception;
    
    flintdb_meta_columns_add(&mt, "value", VARIANT_INT32, 0, 0, SPEC_NOT_NULL, "0", "Sort value", &e);
    flintdb_meta_columns_add(&mt, "label", VARIANT_STRING, 20, 0, SPEC_NOT_NULL, "", "Label", &e);
    if (e) goto exception;

    // 2. Create filesort
    fs = flintdb_filesort_new(filepath, &mt, &e);
    if (e || !fs) goto exception;

    // 3. Add rows in random order
    printf("Adding unsorted rows...\n");
    int values[] = {5, 2, 8, 1, 9, 3};
    for (int i = 0; i < 6; i++) {
        struct flintdb_row *r = flintdb_row_new(&mt, &e);
        if (e) {
            r->free(r);
            goto exception;
        }

        char label[20];
        snprintf(label, sizeof(label), "Item-%d", values[i]);
        
        r->i32_set(r, 0, values[i], &e);
        r->string_set(r, 1, label, &e);
        if (e) {
            r->free(r);
            goto exception;
        }

        fs->add(fs, r, &e);
        r->free(r);
        if (e) goto exception;
    }

    // 4. Sort using a comparator
    fs->sort(fs, compare_by_value, NULL, &e);
    if (e) goto exception;

    // 5. Read sorted results
    printf("Reading sorted rows:\n");
    i64 count = fs->rows(fs);
    for (i64 i = 0; i < count; i++) {
        struct flintdb_row *r = fs->read(fs, i, &e);
        if (e || !r) goto exception;
        flintdb_print_row(r);
        r->free(r);
    }
    
    printf("\nSuccessfully sorted %lld rows.\n\n", count);
    
    fs->close(fs);
    flintdb_meta_close(&mt);
    return 0;

exception:
    fprintf(stderr, "Error in %s: %s\n", __FUNCTION__, e);
    if (fs) fs->close(fs);
    flintdb_meta_close(&mt);
    return -1;
}

/**
 * @brief Demonstrates how to update and delete rows in a table.
 */
int tutorial_table_update_delete() {
    printf("--- Running %s ---\n", __FUNCTION__);
    char *e = NULL;
    struct flintdb_table *tbl = NULL;
    struct flintdb_cursor_i64 *c = NULL;

    const char *tablename = "./temp/tutorial_customer.flintdb";

    // 1. Open the table in read-write mode
    tbl = flintdb_table_open(tablename, FLINTDB_RDWR, NULL, &e);
    if (e || !tbl) goto exception;

    // 2. Find a row to update
    printf("Finding and updating Customer with age = 30:\n");
    c = tbl->find(tbl, "WHERE age = 30", &e);
    if (e || !c) goto exception;

    i64 rowid = c->next(c, &e);
    if (e) goto exception;
    
    if (rowid > -1) {
        const struct flintdb_row *old_row = tbl->read(tbl, rowid, &e);
        if (e || !old_row) goto exception;
        
        printf("Before update:\n");
        flintdb_print_row(old_row);
        
        // Create updated row
        const struct flintdb_meta *mt = tbl->meta(tbl, &e);
        if (e || !mt) goto exception;
        
        struct flintdb_row *new_row = flintdb_row_new((struct flintdb_meta *)mt, &e);
        if (e) {
            new_row->free(new_row);
            goto exception;
        }
        
        // Copy and modify
        new_row->i64_set(new_row, 0, old_row->i64_get(old_row, 0, &e), &e);
        new_row->string_set(new_row, 1, "Updated Customer", &e);
        new_row->i32_set(new_row, 2, 35, &e); // Update age to 35
        if (e) {
            new_row->free(new_row);
            goto exception;
        }
        
        // Apply update at specific rowid
        i64 updated_rowid = tbl->apply_at(tbl, rowid, new_row, &e);
        new_row->free(new_row);
        if (e || updated_rowid < 0) goto exception;
        
        printf("After update:\n");
        const struct flintdb_row *updated = tbl->read(tbl, rowid, &e);
        if (e || !updated) goto exception;
        flintdb_print_row(updated);
    }
    c->close(c);
    c = NULL;

    // 3. Delete a row
    printf("\nDeleting Customer with id = 3:\n");
    c = tbl->find(tbl, "WHERE id = 3", &e);
    if (e || !c) goto exception;

    rowid = c->next(c, &e);
    if (e) goto exception;
    
    if (rowid > -1) {
        i64 deleted = tbl->delete_at(tbl, rowid, &e);
        if (e || deleted < 0) goto exception;
        printf("Successfully deleted row at rowid %lld\n", rowid);
    }
    c->close(c);
    c = NULL;

    // 4. Verify deletion
    printf("\nRemaining customers:\n");
    c = tbl->find(tbl, "", &e);
    if (e || !c) goto exception;

    while ((rowid = c->next(c, &e)) > -1) {
        const struct flintdb_row *r = tbl->read(tbl, rowid, &e);
        if (e || !r) break;
        flintdb_print_row(r);
    }
    if (e) goto exception;

    printf("\nSuccessfully updated and deleted rows.\n\n");

    c->close(c);
    tbl->close(tbl);
    return 0;

exception:
    fprintf(stderr, "Error in %s: %s\n", __FUNCTION__, e);
    if (c) c->close(c);
    if (tbl) tbl->close(tbl);
    return -1;
}

/**
 * @brief Demonstrates how to use aggregate functions for grouping and summarization.
 */
int tutorial_aggregate() {
    printf("--- Running %s ---\n", __FUNCTION__);
    char *e = NULL;
    struct flintdb_table *tbl = NULL;
    struct flintdb_aggregate *agg = NULL;
    struct flintdb_cursor_i64 *c = NULL;

    const char *tablename = "./temp/tutorial_sales.flintdb";
    flintdb_table_drop(tablename, NULL);

    // 1. Create a sales table
    struct flintdb_meta mt = flintdb_meta_new(tablename, &e);
    if (e) goto exception;
    
    flintdb_meta_columns_add(&mt, "product", VARIANT_STRING, 20, 0, SPEC_NOT_NULL, "", "Product name", &e);
    flintdb_meta_columns_add(&mt, "category", VARIANT_STRING, 20, 0, SPEC_NOT_NULL, "", "Category", &e);
    flintdb_meta_columns_add(&mt, "quantity", VARIANT_INT32, 0, 0, SPEC_NOT_NULL, "0", "Quantity sold", &e);
    flintdb_meta_columns_add(&mt, "price", VARIANT_DOUBLE, 0, 0, SPEC_NOT_NULL, "0.0", "Unit price", &e);
    if (e) goto exception;

    char pkey[1][MAX_COLUMN_NAME_LIMIT] = {"product"};
    flintdb_meta_indexes_add(&mt, PRIMARY_NAME, NULL, (const char (*)[MAX_COLUMN_NAME_LIMIT])pkey, 1, &e);
    if (e) goto exception;

    tbl = flintdb_table_open(tablename, FLINTDB_RDWR, &mt, &e);
    if (e || !tbl) goto exception;

    // 2. Insert sample sales data
    printf("Inserting sales data...\n");
    struct {
        const char *product;
        const char *category;
        i32 quantity;
        f64 price;
    } sales[] = {
        {"Apple", "Fruit", 10, 1.50},
        {"Banana", "Fruit", 15, 0.80},
        {"Carrot", "Vegetable", 8, 1.20},
        {"Tomato", "Vegetable", 12, 2.00},
        {"Orange", "Fruit", 7, 1.80},
    };

    for (size_t i = 0; i < sizeof(sales)/sizeof(sales[0]); i++) {
        struct flintdb_row *r = flintdb_row_new(&mt, &e);
        if (e) {
            r->free(r);
            goto exception;
        }

        r->string_set(r, 0, sales[i].product, &e);
        r->string_set(r, 1, sales[i].category, &e);
        r->i32_set(r, 2, sales[i].quantity, &e);
        r->f64_set(r, 3, sales[i].price, &e);
        if (e) {
            r->free(r);
            goto exception;
        }

        i64 rowid = tbl->apply(tbl, r, 0, &e);
        r->free(r);
        if (e || rowid < 0) goto exception;
    }

    // 3. Create aggregate with group by category
    struct flintdb_aggregate_groupby **groupby = (struct flintdb_aggregate_groupby **)calloc(1, sizeof(struct flintdb_aggregate_groupby *));
    struct flintdb_aggregate_func **funcs = (struct flintdb_aggregate_func **)calloc(3, sizeof(struct flintdb_aggregate_func *));
    if (!groupby || !funcs) {
        if (groupby) free(groupby);
        if (funcs) free(funcs);
        e = "Memory allocation failed";
        goto exception;
    }
    
    groupby[0] = groupby_new("category", "category", VARIANT_STRING, &e);
    if (e) goto exception;

    struct flintdb_aggregate_condition no_cond = {0};
    funcs[0] = flintdb_func_count("*", "count", VARIANT_INT64, no_cond, &e);
    funcs[1] = flintdb_func_sum("quantity", "total_quantity", VARIANT_INT32, no_cond, &e);
    funcs[2] = flintdb_func_avg("price", "avg_price", VARIANT_DOUBLE, no_cond, &e);
    if (e) goto exception;

    agg = aggregate_new("sales_by_category", groupby, 1, funcs, 3, &e);
    if (e) goto exception;

    // 4. Feed rows to aggregate
    c = tbl->find(tbl, "", &e);
    if (e || !c) goto exception;

    i64 rowid;
    while ((rowid = c->next(c, &e)) > -1) {
        const struct flintdb_row *r = tbl->read(tbl, rowid, &e);
        if (e || !r) break;
        agg->row(agg, r, &e);
        if (e) break;
    }
    if (e) goto exception;
    c->close(c);
    c = NULL;

    // 5. Compute results
    printf("\nAggregation results (by category):\n");
    struct flintdb_row **result_rows = NULL;
    int result_count = agg->compute(agg, &result_rows, &e);
    if (e) goto exception;

    for (int i = 0; i < result_count; i++) {
        flintdb_print_row(result_rows[i]);
        result_rows[i]->free(result_rows[i]);
    }
    free(result_rows);

    printf("\nSuccessfully performed aggregation.\n\n");

    agg->free(agg);
    tbl->close(tbl);
    flintdb_meta_close(&mt);
    return 0;

exception:
    fprintf(stderr, "Error in %s: %s\n", __FUNCTION__, e);
    if (c) c->close(c);
    if (agg) agg->free(agg);
    if (tbl) tbl->close(tbl);
    flintdb_meta_close(&mt);
    return -1;
}

/**
 * @brief Demonstrates how to execute SQL queries.
 */
int tutorial_flintdb_sql_exec() {
    printf("--- Running %s ---\n", __FUNCTION__);
    char *e = NULL;
    struct flintdb_sql_result*result = NULL;
    struct flintdb_table *tbl = NULL;
    struct flintdb_meta mt = {0};

    const char *tablename = "./temp/tutorial_employees.flintdb";
    flintdb_table_drop(tablename, NULL); // Delete old table and meta file if it exists

    // 1. Create table using API (not SQL) to ensure PRIMARY KEY is set
    printf("Creating table with API...\n");
    mt = flintdb_meta_new(tablename, &e);
    if (e) goto exception;
    
    flintdb_meta_columns_add(&mt, "id", VARIANT_INT64, 0, 0, SPEC_NOT_NULL, "0", "Employee ID", &e);
    if (e) goto exception;
    flintdb_meta_columns_add(&mt, "name", VARIANT_STRING, 50, 0, SPEC_NOT_NULL, "", "Employee name", &e);
    if (e) goto exception;
    flintdb_meta_columns_add(&mt, "department", VARIANT_STRING, 30, 0, SPEC_NOT_NULL, "", "Department", &e);
    if (e) goto exception;
    flintdb_meta_columns_add(&mt, "salary", VARIANT_DOUBLE, 0, 0, SPEC_NOT_NULL, "0.0", "Salary", &e);
    if (e) goto exception;

    char pkey[1][MAX_COLUMN_NAME_LIMIT] = {"id"};
    flintdb_meta_indexes_add(&mt, PRIMARY_NAME, NULL, (const char (*)[MAX_COLUMN_NAME_LIMIT])pkey, 1, &e);
    if (e) goto exception;

    tbl = flintdb_table_open(tablename, FLINTDB_RDWR, &mt, &e);
    if (e || !tbl) goto exception;
    tbl->close(tbl);
    tbl = NULL;

    // Insert data via SQL
    printf("Executing SQL INSERT...\n");
    const char *insert_sqls[] = {
        "INSERT INTO ./temp/tutorial_employees.flintdb VALUES (1, 'Alice', 'Engineering', 75000.0)",
        "INSERT INTO ./temp/tutorial_employees.flintdb VALUES (2, 'Bob', 'Sales', 65000.0)",
        "INSERT INTO ./temp/tutorial_employees.flintdb VALUES (3, 'Charlie', 'Engineering', 80000.0)"
    };
    
    i64 total_affected = 0;
    for (int i = 0; i < 3; i++) {
        result = flintdb_sql_exec(insert_sqls[i], NULL, &e);
        if (e) goto exception;
        if (!result) {
            e = "Failed to execute INSERT";
            goto exception;
        }
        total_affected += result->affected;
        result->close(result);
        result = NULL;
    }
    printf("Affected rows: %lld\n", total_affected);

    // Query data via SQL
    printf("\nExecuting SQL SELECT...\n");
    const char *select_sql = 
        "SELECT * FROM ./temp/tutorial_employees.flintdb WHERE department = 'Engineering'";
    
    result = flintdb_sql_exec(select_sql, NULL, &e);
    if (e) goto exception;
    if (!result) {
        e = "Failed to execute SELECT";
        goto exception;
    }

    printf("Columns: ");
    for (int i = 0; i < result->column_count; i++) {
        printf("%s", result->column_names[i]);
        if (i < result->column_count - 1) printf(", ");
    }
    printf("\n");

    struct flintdb_row *r;
    while ((r = result->row_cursor->next(result->row_cursor, &e)) != NULL) {
        flintdb_print_row(r);
    }
    if (e) goto exception;

    printf("\nSuccessfully executed SQL statements.\n\n");

    result->close(result);
    flintdb_meta_close(&mt);
    return 0;

exception:
    fprintf(stderr, "Error in %s: %s\n", __FUNCTION__, e);
    if (result) result->close(result);
    if (tbl) tbl->close(tbl);
    flintdb_meta_close(&mt);
    return -1;
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    // Create a temp directory for database files if it doesn't exist
    if (access("./temp", F_OK) == -1) {
        // mkdir is in sys/stat.h
        mkdir("./temp", 0755);
    }

    if (tutorial_table_create() != 0) return 1;
    if (tutorial_table_find() != 0) return 1;
    if (tutorial_table_update_delete() != 0) return 1;
    
    if (tutorial_tsv_create() != 0) return 1;
    if (tutorial_tsv_find() != 0) return 1;
    
    if (tutorial_filesort() != 0) return 1;
    if (tutorial_aggregate() != 0) return 1;
    if (tutorial_flintdb_sql_exec() != 0) return 1;
    
    printf("All tutorial steps completed successfully.\n");
    return 0;
}
 
