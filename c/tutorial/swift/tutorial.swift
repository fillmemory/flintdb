// FlintDB Swift Tutorial
//
// This tutorial mirrors the core flow in `c/tutorial/c/tutorial.c`:
//  - Create a table with schema + indexes
//  - Insert rows
//  - Query rows
//  - Update + delete rows
//  - Create/read a TSV file via GenericFile
//
// Build (from this directory):
//   ./build.sh
// Run:
//   ./tutorial
// If you prefer, you can also run with:
//   DYLD_LIBRARY_PATH=../../lib ./tutorial

import Foundation
import Darwin

// Reusable wrapper layer (Meta/Table/Row/GenericFile)
// See FlintDBSwift.swift

private enum CustomerCol: String, FlintColumnName {
	case id
	case name
	case age
}

private enum CustomerSchema {
	static func makeMeta(_ path: String) throws -> Meta {
		let mt = try Meta(path)
		mt.setCache(bytes: 256 * 1024)
		try mt.addColumn(CustomerCol.id, type: FLINTDB_SWIFT_VARIANT_INT64, bytes: 0, precision: 0, notNull: true, defaultValue: "0", comment: "PRIMARY KEY")
		try mt.addColumn(CustomerCol.name, type: FLINTDB_SWIFT_VARIANT_STRING, bytes: 50, precision: 0, notNull: true, defaultValue: "", comment: "Customer name")
		try mt.addColumn(CustomerCol.age, type: FLINTDB_SWIFT_VARIANT_INT32, bytes: 0, precision: 0, notNull: true, defaultValue: "0", comment: "Customer age")
		try mt.addIndex("primary", keys: [CustomerCol.id])
		try mt.addIndex("ix_age", keys: [CustomerCol.age])
		return mt
	}
}

private enum ProductCol: String, FlintColumnName {
	case product_id
	case product_name
	case price
}

private enum ProductSchema {
	static func makeMeta(_ path: String) throws -> Meta {
		let mt = try Meta(path)
		mt.setTextFormat(absentHeader: true, delimiter: "\t", format: "tsv")
		try mt.addColumn(ProductCol.product_id, type: FLINTDB_SWIFT_VARIANT_INT32, bytes: 0, precision: 0, notNull: true)
		try mt.addColumn(ProductCol.product_name, type: FLINTDB_SWIFT_VARIANT_STRING, bytes: 100, precision: 0, notNull: true)
		try mt.addColumn(ProductCol.price, type: FLINTDB_SWIFT_VARIANT_DOUBLE, bytes: 0, precision: 0, notNull: true)
		return mt
	}
}

private func ensureTempDir() throws {
	let url = URL(fileURLWithPath: "./temp", isDirectory: true)
	if FileManager.default.fileExists(atPath: url.path) { return }
	do {
		try FileManager.default.createDirectory(
			at: url,
			withIntermediateDirectories: true,
			attributes: [FileAttributeKey.posixPermissions: 0o755]
		)
	} catch {
		throw FlintSwiftError.flintdb("Failed to create ./temp: \(error)")
	}
}

// MARK: - Table

private func tutorialTableCreate() throws {
	print("--- Running tutorialTableCreate ---")

	let tablename = "./temp/tutorial_customer.flintdb"

	Table.drop(tablename)

	// Build schema like tutorial.cpp (reusable Meta builder)
	let mt = try CustomerSchema.makeMeta(tablename)

	print("Table schema SQL:\n\(try mt.toSQLString())\n")

	let tbl = try Table.create(tablename, meta: mt)

	print("Inserting 3 rows...")
	for i in 0..<3 {
		let r = try Row(meta: mt)
		try r.setI64(0, Int64(i + 1))
		try r.setString(1, "Customer \(i + 1)")
		try r.setI32(2, Int32(30 + i))
		guard try r.validate() else { throw FlintSwiftError.flintdb("row validation failed") }

		let rowid = try tbl.apply(r)
		if rowid < 0 {
			throw FlintSwiftError.flintdb("table.apply: returned negative rowid")
		}
	}

	print("Successfully created table and inserted data.\n")
}

private func tutorialTableFind() throws {
	print("--- Running tutorialTableFind ---")

	let tablename = "./temp/tutorial_customer.flintdb"
	let tbl = try Table.openReadOnly(tablename)

	print("Finding rows where age >= 31:")
	let c = try tbl.find("WHERE age >= 31")
	while let rowid = try c.next() {
		let r = try tbl.read(rowid)
		flintPrintRow(r)
	}

	print("\nSuccessfully found and read data.\n")
}

private func tutorialTableUpdateDelete() throws {
	print("--- Running tutorialTableUpdateDelete ---")

	let tablename = "./temp/tutorial_customer.flintdb"
	let tbl = try Table.openReadWrite(tablename)
	let mt = try Meta.open(tablename + ".desc")

	print("Finding and updating Customer with age = 30:")
	let c = try tbl.find("WHERE age = 30")
	if let rowid = try c.next() {
		let oldRow = try tbl.read(rowid)
		print("Before update:")
		flintPrintRow(oldRow)

		let newRow = try Row(meta: mt)
		try newRow.setI64(CustomerCol.id, 1)
		try newRow.setString(CustomerCol.name, "Updated Customer")
		try newRow.setI32(CustomerCol.age, 35)
		guard try newRow.validate() else { throw FlintSwiftError.flintdb("row validation failed") }
		try tbl.applyAt(rowid, newRow)

		print("After update:")
		flintPrintRow(try tbl.read(rowid))
	}

	print("\nDeleting Customer with id = 3:")
	let c2 = try tbl.find("WHERE id = 3")
	if let rowid = try c2.next() {
		try tbl.deleteAt(rowid)
		print("Successfully deleted row at rowid \(rowid)")
	}

	print("\nRemaining customers:")
	let c3 = try tbl.find("")
	while let rowid = try c3.next() {
		flintPrintRow(try tbl.read(rowid))
	}

	print("\nSuccessfully updated and deleted rows.\n")
}

// MARK: - TSV (GenericFile)

private func tutorialTsvCreate() throws {
	print("--- Running tutorialTsvCreate ---")

	let filepath = "./temp/tutorial_products.tsv"
	GenericFile.drop(filepath)

	let mt = try ProductSchema.makeMeta(filepath)

	let f = try GenericFile.create(filepath, meta: mt)

	print("Writing 3 rows to TSV...")
	let idxProductId = try mt.columnIndex(ProductCol.product_id)
	let idxProductName = try mt.columnIndex(ProductCol.product_name)
	let idxPrice = try mt.columnIndex(ProductCol.price)

	for i in 0..<3 {
		let r = try Row(meta: mt)
		try r.setI32(UInt16(idxProductId), Int32(101 + i))

		let name = "Product-\(UnicodeScalar(Int(Character("A").asciiValue!) + i)!)"
		try r.setString(UInt16(idxProductName), name)

		try r.setF64(UInt16(idxPrice), Double(9.99 * Double(i + 1)))
		try f.write(r)
	}

	print("Successfully created TSV file.\n")
}

private func tutorialTsvFind() throws {
	print("--- Running tutorialTsvFind ---")

	let filepath = "./temp/tutorial_products.tsv"
	let f = try GenericFile.openReadOnly(filepath)

	print("Reading rows from TSV where product_id >= 102:")
	let c = try f.find("WHERE product_id >= 102")
	while let r = try c.next() {
		flintPrintRowBorrowed(r)
	}

	print("\nSuccessfully read from TSV file.\n")
}

// MARK: - Main

func runTutorial() throws {
	try ensureTempDir()
	try tutorialTableCreate()
	try tutorialTableFind()
	try tutorialTableUpdateDelete()
	try tutorialTsvCreate()
	try tutorialTsvFind()
}
