mod flintdb;

use flintdb::*;
use std::fs;

// RAII guard for automatic cleanup
struct CleanupGuard;

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        flintdb::cleanup();
    }
}

fn tutorial_table_create() -> Result<(), String> {
    println!("--- Running tutorial_table_create ---");

    let tablename = "./temp/tutorial_customer.flintdb";
    let _ = Table::drop_table(tablename);

    // 1. Define the table schema
    let mut mt = Meta::new(tablename)?;
    mt.add_column("id", VAR_INT64, 0, 0, 1, "0", "PRIMARY KEY")?;
    mt.add_column("name", VAR_STRING, 50, 0, 1, "", "Customer name")?;
    mt.add_column("age", VAR_INT32, 0, 0, 1, "0", "Customer age")?;

    mt.add_index("primary", &["id"])?;
    mt.add_index("ix_age", &["age"])?;

    let sql = mt.to_sql_string()?;
    println!("Table schema SQL:\n{}\n", sql);

    // 2. Open the table with the defined schema
    let mut tbl = Table::open(tablename, RDWR, Some(&mt))?;

    // 3. Insert data rows
    println!("Inserting 3 rows...");
    for i in 0..3 {
        let mut r = Row::new(&mut mt)?;
        r.set_i64(0, (i + 1) as i64)?;
        r.set_string(1, &format!("Customer {}", i + 1))?;
        r.set_i32(2, 30 + i)?;

        let rowid = tbl.apply(&mut r)?;
        if rowid < 0 {
            return Err("Failed to insert row".to_string());
        }
    }

    println!("Successfully created table and inserted data.\n");
    Ok(())
}

fn tutorial_table_find() -> Result<(), String> {
    println!("--- Running tutorial_table_find ---");

    let tablename = "./temp/tutorial_customer.flintdb";

    // 1. Open the table in read-only mode
    let mut tbl = Table::open(tablename, RDONLY, None)?;

    // 2. Find data using a WHERE clause
    println!("Finding rows where age >= 31:");
    let mut cursor = tbl.find("WHERE age >= 31")?;

    // 3. Iterate through the cursor to get rowids
    while let Some(rowid) = cursor.next()? {
        let row = tbl.read(rowid)?;
        print_row_safe(row.ptr);
    }

    println!("\nSuccessfully found and read data.\n");
    Ok(())
}

fn tutorial_tsv_create() -> Result<(), String> {
    println!("--- Running tutorial_tsv_create ---");

    let filepath = "./temp/tutorial_products.tsv";
    let _ = GenericFile::drop_file(filepath);

    // 1. Define the schema for the TSV file
    let mut mt = Meta::new(filepath)?;
    mt.add_column("product_id", VAR_INT32, 0, 0, 1, "", "")?;
    mt.add_column("product_name", VAR_STRING, 100, 0, 1, "", "")?;
    mt.add_column("price", VAR_DOUBLE, 0, 0, 1, "", "")?;

    // 2. Open the generic file with the TSV format
    let mut f = GenericFile::open(filepath, RDWR, Some(&mt))?;

    // 3. Write data rows
    println!("Writing 3 rows to TSV...");
    for i in 0..3 {
        let mut r = Row::new(&mut mt)?;
        r.set_i32(get_column_index(&mut mt, "product_id"), 101 + i)?;
        r.set_string(
            get_column_index(&mut mt, "product_name"),
            &format!("Product-{}", (b'A' + i as u8) as char),
        )?;
        r.set_f64(get_column_index(&mut mt, "price"), 9.99 * (i as f64 + 1.0))?;

        f.write(&mut r)?;
    }

    println!("Successfully created TSV file.\n");
    Ok(())
}

fn tutorial_tsv_find() -> Result<(), String> {
    println!("--- Running tutorial_tsv_find ---");

    let filepath = "./temp/tutorial_products.tsv";

    // 1. Open the TSV file in read-only mode
    let mut f = GenericFile::open(filepath, RDONLY, None)?;

    // 2. Find rows matching the WHERE clause
    println!("Reading rows where product_id >= 102:");
    let mut cursor = f.find("WHERE product_id >= 102")?;

    // 3. Iterate through the cursor
    while let Some(row) = cursor.next()? {
        print_row_safe(row.ptr);
    }

    println!("\nSuccessfully read from TSV file.\n");
    Ok(())
}

fn tutorial_table_update_delete() -> Result<(), String> {
    println!("--- Running tutorial_table_update_delete ---");

    let tablename = "./temp/tutorial_customer.flintdb";

    // 1. Open the table in read-write mode
    let mut tbl = Table::open(tablename, RDWR, None)?;

    // 2. Find a row to update
    println!("Finding and updating Customer with age = 30:");
    let mut cursor = tbl.find("WHERE age = 30")?;

    if let Some(rowid) = cursor.next()? {
        let old_row = tbl.read(rowid)?;
        println!("Before update:");
        print_row_safe(old_row.ptr);

        // Note: Full update implementation requires apply_at function
        println!("(Update operations require additional binding implementation)");
    }
    drop(cursor);

    // 3. Delete a row
    println!("\nDelete operations require additional binding implementation");

    // 4. Show remaining customers
    println!("\nCurrent customers:");
    let mut cursor2 = tbl.find("")?;
    while let Some(rowid) = cursor2.next()? {
        let row = tbl.read(rowid)?;
        print_row_safe(row.ptr);
    }

    println!("\nTable operations demonstrated.\n");
    Ok(())
}

fn tutorial_filesort() -> Result<(), String> {
    println!("--- Filesort feature available in C API ---");
    println!("(Rust bindings for filesort require additional implementation)\n");
    Ok(())
}

fn main() {
    // Setup automatic cleanup on scope exit (like Go's defer)
    let _cleanup = CleanupGuard;
    
    // Create a temp directory for database files if it doesn't exist
    let _ = fs::create_dir_all("./temp");

    if let Err(e) = tutorial_table_create() {
        eprintln!("Error in tutorial_table_create: {}", e);
        return;
    }

    if let Err(e) = tutorial_table_find() {
        eprintln!("Error in tutorial_table_find: {}", e);
        return;
    }

    if let Err(e) = tutorial_table_update_delete() {
        eprintln!("Error in tutorial_table_update_delete: {}", e);
        return;
    }

    if let Err(e) = tutorial_tsv_create() {
        eprintln!("Error in tutorial_tsv_create: {}", e);
        return;
    }

    if let Err(e) = tutorial_tsv_find() {
        eprintln!("Error in tutorial_tsv_find: {}", e);
        return;
    }

    if let Err(e) = tutorial_filesort() {
        eprintln!("Error in tutorial_filesort: {}", e);
        return;
    }

    println!("All tutorial steps completed successfully.");
    
    // Cleanup will be called automatically when _cleanup goes out of scope
}
