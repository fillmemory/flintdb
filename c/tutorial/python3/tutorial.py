#!/usr/bin/env python3
"""
@file tutorial.py
@brief FlintDB API tutorial for Python
@description This tutorial demonstrates how to use FlintDB from Python
"""

import atexit
import os
import sys
from flintdb_cffi import (
    Meta, Table, Row, GenericFile, CursorI64, CursorRow,
    FileSort, Aggregate, groupby_new, func_count, func_sum, func_avg,
    FlintDBError, print_row, sql_exec, cleanup,
    FLINTDB_RDONLY, FLINTDB_RDWR,
    VARIANT_INT32, VARIANT_INT64, VARIANT_DOUBLE, VARIANT_STRING, VARIANT_DECIMAL,
    SPEC_NOT_NULL, PRIMARY_NAME
)

# Register cleanup to run at exit
atexit.register(cleanup)


def ensure_temp_dir():
    """Create temp directory if it doesn't exist"""
    temp_dir = "./temp"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir, mode=0o755)


def tutorial_table_create():
    """Demonstrates how to create a table, define a schema, and insert data."""
    print("--- Running tutorial_table_create ---")
    
    tablename = "./temp/tutorial_customer.flintdb"
    Table.drop(tablename)
    
    try:
        # 1. Define the table schema (meta-information)
        with Meta(tablename) as mt:
            mt.add_column("id", VARIANT_INT64, spec=SPEC_NOT_NULL, default="0", comment="PRIMARY KEY")
            mt.add_column("name", VARIANT_STRING, size=50, spec=SPEC_NOT_NULL, default="", comment="Customer name")
            mt.add_column("age", VARIANT_INT32, spec=SPEC_NOT_NULL, default="0", comment="Customer age")
            
            # Add indexes
            mt.add_index(PRIMARY_NAME.decode(), None, ["id"])
            mt.add_index("ix_age", None, ["age"])
            
            # Print schema SQL
            print(f"Table schema SQL:\n{mt.to_sql_string()}\n")
            
            # 2. Open the table with the defined schema
            with Table(tablename, FLINTDB_RDWR, mt) as tbl:
                # 3. Insert data rows
                print("Inserting 3 rows...")
                for i in range(3):
                    with Row(mt) as row:
                        row.set_i64(0, i + 1)
                        row.set_string(1, f"Customer {i + 1}")
                        row.set_i32(2, 30 + i)
                        
                        if not row.validate():
                            raise FlintDBError("Row validation failed")
                        
                        tbl.apply(row, check_dup=False)
        
        print("Successfully created table and inserted data.\n")
        return 0
        
    except FlintDBError as e:
        print(f"Error in tutorial_table_create: {e}", file=sys.stderr)
        return -1


def tutorial_table_find():
    """Demonstrates how to find and read data from an existing table."""
    print("--- Running tutorial_table_find ---")
    
    tablename = "./temp/tutorial_customer.flintdb"
    
    try:
        # 1. Open the table in read-only mode
        with Table(tablename, FLINTDB_RDONLY) as tbl:
            # 2. Find data using a WHERE clause
            print("Finding rows where age >= 31:")
            with tbl.find("WHERE age >= 31") as cursor:
                # 3. Iterate through the cursor
                for rowid in cursor:
                    row = tbl.read(rowid)
                    print_row(row)
        
        print("\nSuccessfully found and read data.\n")
        return 0
        
    except FlintDBError as e:
        print(f"Error in tutorial_table_find: {e}", file=sys.stderr)
        return -1


def tutorial_tsv_create():
    """Demonstrates how to create a TSV file and write data to it."""
    print("--- Running tutorial_tsv_create ---")
    
    filepath = "./temp/tutorial_products.tsv"
    GenericFile.drop(filepath)
    
    try:
        # 1. Define the schema for the TSV file
        with Meta(filepath) as mt:
            mt.add_column("product_id", VARIANT_INT32, spec=SPEC_NOT_NULL)
            mt.add_column("product_name", VARIANT_STRING, size=100, spec=SPEC_NOT_NULL)
            mt.add_column("price", VARIANT_DOUBLE, spec=SPEC_NOT_NULL)
            
            # 2. Open the generic file with TSV format
            with GenericFile(filepath, FLINTDB_RDWR, mt) as gf:
                # 3. Write data rows
                print("Writing 3 rows to TSV...")
                for i in range(3):
                    with Row(mt) as row:
                        row.set_i32(mt.column_at("product_id"), 101 + i)
                        row.set_string(mt.column_at("product_name"), f"Product-{chr(ord('A') + i)}")
                        row.set_f64(mt.column_at("price"), 9.99 * (i + 1))
                        gf.write(row)
        
        print("Successfully created TSV file.\n")
        return 0
        
    except FlintDBError as e:
        print(f"Error in tutorial_tsv_create: {e}", file=sys.stderr)
        return -1


def tutorial_tsv_find():
    """Demonstrates how to read data from a TSV file."""
    print("--- Running tutorial_tsv_find ---")
    
    filepath = "./temp/tutorial_products.tsv"
    
    try:
        # 1. Open the TSV file in read-only mode
        with GenericFile(filepath, FLINTDB_RDONLY) as gf:
            # 2. Find rows
            print("Reading rows from TSV:")
            with gf.find("WHERE product_id >= 102") as cursor:
                # 3. Iterate through the cursor
                for row in cursor:
                    print_row(row)
        
        print("\nSuccessfully read from TSV file.\n")
        return 0
        
    except FlintDBError as e:
        print(f"Error in tutorial_tsv_find: {e}", file=sys.stderr)
        return -1


def tutorial_table_update_delete():
    """Demonstrates how to update and delete rows in a table."""
    print("--- Running tutorial_table_update_delete ---")
    
    tablename = "./temp/tutorial_customer.flintdb"
    
    try:
        with Table(tablename, FLINTDB_RDWR) as tbl:
            # Find and update a row
            print("Finding and updating Customer with age = 30:")
            with tbl.find("WHERE age = 30") as cursor:
                rowid = cursor.next()
                if rowid > -1:
                    old_row = tbl.read(rowid)
                    print("Before update:")
                    print_row(old_row)
                    
                    # Create updated row (Note: This is simplified - full implementation needs to copy old values)
                    print("Note: Full update requires reading old values and creating new row")
                    print("See C/C++ tutorials for complete implementation.\n")
            
            # Delete a row
            print("Deleting Customer with id = 3:")
            with tbl.find("WHERE id = 3") as cursor:
                rowid = cursor.next()
                if rowid > -1:
                    tbl.delete_at(rowid)
                    print(f"Successfully deleted row at rowid {rowid}\n")
            
            # Show remaining rows
            print("Remaining customers:")
            with tbl.find("") as cursor:
                for rowid in cursor:
                    row = tbl.read(rowid)
                    print_row(row)
        
        print("\nSuccessfully updated and deleted rows.\n")
        return 0
        
    except FlintDBError as e:
        print(f"Error in tutorial_table_update_delete: {e}", file=sys.stderr)
        return -1


def tutorial_filesort():
    """Demonstrates how to use filesort for external sorting of rows."""
    print("--- Running tutorial_filesort ---")

    filepath = "./temp/tutorial_sort.dat"
    # Clean up old run artifacts (best-effort)
    for p in (filepath, filepath + ".desc"):
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    try:
        with Meta(filepath) as mt:
            mt.add_column("value", VARIANT_INT32, spec=SPEC_NOT_NULL, default="0", comment="Sort value")
            mt.add_column("label", VARIANT_STRING, size=20, spec=SPEC_NOT_NULL, default="", comment="Label")

            with FileSort(filepath, mt) as fs:
                print("Adding unsorted rows...")
                values = [5, 2, 8, 1, 9, 3]
                for v in values:
                    with Row(mt) as r:
                        r.set_i32(0, v)
                        r.set_string(1, f"Item-{v}")
                        fs.add(r)

                def compare_by_value(_ctx, a, b):
                    # Match tutorial.c: compare integer value at column 0
                    return int(a.get_i32(0) - b.get_i32(0))

                fs.sort(compare_by_value)

                print("Reading sorted rows:")
                count = fs.rows()
                for i in range(count):
                    with fs.read(i) as r:
                        print_row(r)

        print(f"\nSuccessfully sorted {count} rows.\n")
        return 0

    except FlintDBError as e:
        print(f"Error in tutorial_filesort: {e}", file=sys.stderr)
        return -1


def tutorial_aggregate():
    """Demonstrates how to use aggregate functions for grouping and summarization."""
    print("--- Running tutorial_aggregate ---")

    tablename = "./temp/tutorial_sales.flintdb"
    Table.drop(tablename)

    try:
        with Meta(tablename) as mt:
            mt.add_column("product", VARIANT_STRING, size=20, spec=SPEC_NOT_NULL, default="", comment="Product name")
            mt.add_column("category", VARIANT_STRING, size=20, spec=SPEC_NOT_NULL, default="", comment="Category")
            mt.add_column("quantity", VARIANT_INT32, spec=SPEC_NOT_NULL, default="0", comment="Quantity sold")
            mt.add_column("price", VARIANT_DOUBLE, spec=SPEC_NOT_NULL, default="0.0", comment="Unit price")
            mt.add_index(PRIMARY_NAME.decode(), None, ["product"])

            with Table(tablename, FLINTDB_RDWR, mt) as tbl:
                print("Inserting sales data...")
                sales = [
                    ("Apple", "Fruit", 10, 1.50),
                    ("Banana", "Fruit", 15, 0.80),
                    ("Carrot", "Vegetable", 8, 1.20),
                    ("Tomato", "Vegetable", 12, 2.00),
                    ("Orange", "Fruit", 7, 1.80),
                ]

                for product, category, quantity, price in sales:
                    with Row(mt) as r:
                        r.set_string(0, product)
                        r.set_string(1, category)
                        r.set_i32(2, quantity)
                        r.set_f64(3, price)
                        tbl.apply(r, check_dup=False)

                groupbys = [groupby_new("category", "category", VARIANT_STRING)]
                funcs = [
                    func_count("*", "count", VARIANT_INT64),
                    func_sum("quantity", "total_quantity", VARIANT_DECIMAL),
                    func_avg("price", "avg_price", VARIANT_DECIMAL),
                ]

                with Aggregate.build("sales_by_category", groupbys, funcs) as agg:
                    with tbl.find("") as c:
                        for rowid in c:
                            agg.row(tbl.read(rowid))

                    print("\nAggregation results (by category):")
                    for r in agg.compute():
                        print_row(r)
                        r.close()

        print("\nSuccessfully performed aggregation.\n")
        return 0

    except FlintDBError as e:
        print(f"Error in tutorial_aggregate: {e}", file=sys.stderr)
        return -1


def tutorial_flintdb_sql_exec():
    """Demonstrates how to execute SQL queries."""
    print("--- Running tutorial_flintdb_sql_exec ---")
    
    tablename = "./temp/tutorial_employees.flintdb"
    Table.drop(tablename)
    
    try:
        # 1. Create table using API
        print("Creating table with API...")
        with Meta(tablename) as mt:
            mt.add_column("id", VARIANT_INT64, spec=SPEC_NOT_NULL, default="0", comment="Employee ID")
            mt.add_column("name", VARIANT_STRING, size=50, spec=SPEC_NOT_NULL, comment="Employee name")
            mt.add_column("department", VARIANT_STRING, size=30, spec=SPEC_NOT_NULL, comment="Department")
            mt.add_column("salary", VARIANT_DOUBLE, spec=SPEC_NOT_NULL, default="0.0", comment="Salary")
            mt.add_index(PRIMARY_NAME.decode(), None, ["id"])
            
            # Open and close table to create it
            with Table(tablename, FLINTDB_RDWR, mt):
                pass
        
        # 2. Insert data via SQL
        print("Executing SQL INSERT...")
        insert_sqls = [
            "INSERT INTO ./temp/tutorial_employees.flintdb VALUES (1, 'Alice', 'Engineering', 75000.0)",
            "INSERT INTO ./temp/tutorial_employees.flintdb VALUES (2, 'Bob', 'Sales', 65000.0)",
            "INSERT INTO ./temp/tutorial_employees.flintdb VALUES (3, 'Charlie', 'Engineering', 80000.0)",
        ]
        
        total_affected = 0
        for sql in insert_sqls:
            with sql_exec(sql) as res:
                total_affected += res.affected

        print(f"Affected rows: {total_affected}")
        
        # 3. Query data via SQL
        print("\nExecuting SQL SELECT...")
        select_sql = "SELECT * FROM ./temp/tutorial_employees.flintdb WHERE department = 'Engineering'"
        with sql_exec(select_sql) as res:
            print("Columns: " + ", ".join(res.column_names))
            for row_ptr in res.iter_rows():
                print_row(row_ptr)
        
        print("\nSuccessfully executed SQL statements.\n")
        return 0
        
    except FlintDBError as e:
        print(f"Error in tutorial_flintdb_sql_exec: {e}", file=sys.stderr)
        return -1


def main():
    """Main function to run all tutorials"""
    ensure_temp_dir()
    
    tutorials = [
        tutorial_table_create,
        tutorial_table_find,
        tutorial_table_update_delete,
        tutorial_tsv_create,
        tutorial_tsv_find,
        tutorial_filesort,
        tutorial_aggregate,
        tutorial_flintdb_sql_exec,
    ]
    
    for tutorial in tutorials:
        result = tutorial()
        if result != 0:
            print(f"Tutorial {tutorial.__name__} failed!", file=sys.stderr)
            return 1
    
    print("All tutorial steps completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
