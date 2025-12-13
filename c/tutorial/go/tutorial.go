// FlintDB Go Tutorial
package main

import (
	"fmt"
	"os"

	flintdb "flintdb-tutorial/flintdb"
)

// tutorialTableCreate demonstrates how to create a table, define a schema, and insert data
func tutorialTableCreate() error {
	fmt.Println("--- Running tutorialTableCreate ---")

	tablename := "./temp/tutorial_customer.flintdb"
	flintdb.TableDrop(tablename)

	// 1. Define the table schema
	meta, err := flintdb.NewMeta(tablename)
	if err != nil {
		return err
	}
	defer meta.Close()

	if err := meta.AddColumn("id", flintdb.VARIANT_INT64, 0, 0, flintdb.SPEC_NOT_NULL, "0", "PRIMARY KEY"); err != nil {
		return err
	}
	if err := meta.AddColumn("name", flintdb.VARIANT_STRING, 50, 0, flintdb.SPEC_NOT_NULL, "", "Customer name"); err != nil {
		return err
	}
	if err := meta.AddColumn("age", flintdb.VARIANT_INT32, 0, 0, flintdb.SPEC_NOT_NULL, "0", "Customer age"); err != nil {
		return err
	}

	if err := meta.AddIndex(flintdb.PRIMARY_NAME, []string{"id"}); err != nil {
		return err
	}
	if err := meta.AddIndex("ix_age", []string{"age"}); err != nil {
		return err
	}

	sql, err := meta.ToSQL()
	if err != nil {
		return err
	}
	fmt.Printf("Table schema SQL:\n%s\n\n", sql)

	// 2. Open the table
	table, err := flintdb.TableOpen(tablename, flintdb.FLINTDB_RDWR, meta)
	if err != nil {
		return err
	}
	defer table.Close()

	// 3. Insert data rows
	fmt.Println("Inserting 3 rows...")
	for i := 0; i < 3; i++ {
		row, err := table.CreateRow()
		if err != nil {
			return err
		}

		if err := row.SetInt64ByName("id", int64(i+1)); err != nil {
			row.Free()
			return err
		}

		name := fmt.Sprintf("Customer %d", i+1)
		if err := row.SetStringByName("name", name); err != nil {
			row.Free()
			return err
		}

		if err := row.SetInt32ByName("age", int32(30+i)); err != nil {
			row.Free()
			return err
		}

		rowid, err := table.Insert(row)
		row.Free()
		if err != nil {
			return err
		}
		_ = rowid
	}

	fmt.Println("Successfully created table and inserted data.\n")
	return nil
}

// tutorialTableFind demonstrates how to find and read data from an existing table
func tutorialTableFind() error {
	fmt.Println("--- Running tutorialTableFind ---")

	tablename := "./temp/tutorial_customer.flintdb"

	// 1. Open the table in read-only mode
	table, err := flintdb.TableOpen(tablename, flintdb.FLINTDB_RDONLY, nil)
	if err != nil {
		return err
	}
	defer table.Close()

	// 2. Find data using a WHERE clause
	fmt.Println("Finding rows where age >= 31:")
	cursor, err := table.Find("WHERE age >= 31")
	if err != nil {
		return err
	}
	defer cursor.Close()

	// 3. Iterate through the cursor
	for {
		rowid, err := cursor.Next()
		if err != nil {
			return err
		}
		if rowid < 0 {
			break
		}

		row, err := table.Read(rowid)
		if err != nil {
			return err
		}

		row.Print()
	}

	fmt.Println("\nSuccessfully found and read data.\n")
	return nil
}

// tutorialTsvCreate demonstrates how to create a TSV file and write data to it
func tutorialTsvCreate() error {
	fmt.Println("--- Running tutorialTsvCreate ---")

	filepath := "./temp/tutorial_products.tsv"
	flintdb.GenericFileDrop(filepath)

	// 1. Define the schema for the TSV file
	meta, err := flintdb.NewMeta(filepath)
	if err != nil {
		return err
	}
	defer meta.Close()

	// Configure for TSV
	meta.SetFormatTSV()

	if err := meta.AddColumn("product_id", flintdb.VARIANT_INT32, 0, 0, flintdb.SPEC_NOT_NULL, "", ""); err != nil {
		return err
	}
	if err := meta.AddColumn("product_name", flintdb.VARIANT_STRING, 100, 0, flintdb.SPEC_NOT_NULL, "", ""); err != nil {
		return err
	}
	if err := meta.AddColumn("price", flintdb.VARIANT_DOUBLE, 0, 0, flintdb.SPEC_NOT_NULL, "", ""); err != nil {
		return err
	}

	// 2. Open the generic file
	file, err := flintdb.GenericFileOpen(filepath, flintdb.FLINTDB_RDWR, meta)
	if err != nil {
		return err
	}
	defer file.Close()

	// 3. Write data rows
	fmt.Println("Writing 3 rows to TSV...")
	for i := 0; i < 3; i++ {
		row, err := file.CreateRow()
		if err != nil {
			return err
		}

		if err := row.SetInt32ByName("product_id", int32(101+i)); err != nil {
			row.Free()
			return err
		}

		name := fmt.Sprintf("Product-%c", 'A'+i)
		if err := row.SetStringByName("product_name", name); err != nil {
			row.Free()
			return err
		}

		if err := row.SetDoubleByName("price", 9.99*float64(i+1)); err != nil {
			row.Free()
			return err
		}

		if err := file.Write(row); err != nil {
			row.Free()
			return err
		}
		row.Free()
	}

	fmt.Println("Successfully created TSV file.\n")
	return nil
}

// tutorialTsvFind demonstrates how to read data from a TSV file
func tutorialTsvFind() error {
	fmt.Println("--- Running tutorialTsvFind ---")

	filepath := "./temp/tutorial_products.tsv"

	// 1. Open the TSV file in read-only mode
	file, err := flintdb.GenericFileOpen(filepath, flintdb.FLINTDB_RDONLY, nil)
	if err != nil {
		return err
	}
	defer file.Close()

	// 2. Find rows
	fmt.Println("Reading rows from TSV where product_id >= 102:")
	cursor, err := file.Find("WHERE product_id >= 102")
	if err != nil {
		return err
	}
	defer cursor.Close()

	// 3. Iterate through the cursor
	for {
		row, err := cursor.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}

		row.Print()
		row.Free()
	}

	fmt.Println("\nSuccessfully read from TSV file.\n")
	return nil
}

// tutorialTableUpdateDelete demonstrates how to update and delete rows in a table
func tutorialTableUpdateDelete() error {
	fmt.Println("--- Running tutorialTableUpdateDelete ---")

	tablename := "./temp/tutorial_customer.flintdb"

	// 1. Open the table in read-write mode
	table, err := flintdb.TableOpen(tablename, flintdb.FLINTDB_RDWR, nil)
	if err != nil {
		return err
	}
	defer table.Close()

	// 2. Find a row to update
	fmt.Println("Finding and updating Customer with age = 30:")
	cursor, err := table.Find("WHERE age = 30")
	if err != nil {
		return err
	}
	defer cursor.Close()

	rowid, err := cursor.Next()
	if err != nil {
		return err
	}

	if rowid > -1 {
		oldRow, err := table.Read(rowid)
		if err != nil {
			return err
		}

		fmt.Println("Before update:")
		oldRow.Print()

		// Create updated row
		newRow, err := table.CreateRow()
		if err != nil {
			return err
		}
		defer newRow.Free()

		// Copy and modify
		if err := newRow.SetInt64ByName("id", 1); err != nil {
			return err
		}
		if err := newRow.SetStringByName("name", "Updated Customer"); err != nil {
			return err
		}
		if err := newRow.SetInt32ByName("age", 35); err != nil {
			return err
		}

		// Update at specific rowid
		if err := table.UpdateAt(rowid, newRow); err != nil {
			return err
		}

		fmt.Println("After update:")
		updatedRow, err := table.Read(rowid)
		if err != nil {
			return err
		}
		updatedRow.Print()
	}

	// 3. Delete a row
	fmt.Println("\nDeleting Customer with id = 3:")
	cursor2, err := table.Find("WHERE id = 3")
	if err != nil {
		return err
	}
	defer cursor2.Close()

	rowid, err = cursor2.Next()
	if err != nil {
		return err
	}

	if rowid > -1 {
		if err := table.DeleteAt(rowid); err != nil {
			return err
		}
		fmt.Printf("Successfully deleted row at rowid %d\n", rowid)
	}

	// 4. Verify deletion
	fmt.Println("\nRemaining customers:")
	cursor3, err := table.Find("")
	if err != nil {
		return err
	}
	defer cursor3.Close()

	for {
		rowid, err := cursor3.Next()
		if err != nil {
			return err
		}
		if rowid < 0 {
			break
		}

		row, err := table.Read(rowid)
		if err != nil {
			return err
		}
		row.Print()
	}

	fmt.Println("\nSuccessfully updated and deleted rows.\n")
	return nil
}

// tutorialFilesort demonstrates how to use filesort for external sorting
// Note: Full filesort bindings require more complex CGo setup
// This is a placeholder for the feature
func tutorialFilesort() error {
	fmt.Println("--- Filesort feature available in C API ---")
	fmt.Println("(Go bindings for filesort require additional CGo setup)\n")
	return nil
}

func main() {
	// Create temp directory if it doesn't exist
	if _, err := os.Stat("./temp"); os.IsNotExist(err) {
		os.Mkdir("./temp", 0755)
	}

	if err := tutorialTableCreate(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := tutorialTableFind(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := tutorialTableUpdateDelete(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := tutorialTsvCreate(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := tutorialTsvFind(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := tutorialFilesort(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	fmt.Println("All tutorial steps completed successfully.")
}
