import Foundation
import Darwin
import FlintDB

// Swift doesn't reliably import C preprocessor macros from a Clang module.
// Keep the numeric values aligned with flintdb_swift_shim.h.
let FLINTDB_SWIFT_VARIANT_INT32: Int32 = 2
let FLINTDB_SWIFT_VARIANT_INT64: Int32 = 8
let FLINTDB_SWIFT_VARIANT_DOUBLE: Int32 = 9
let FLINTDB_SWIFT_VARIANT_STRING: Int32 = 11

let FLINTDB_SWIFT_SPEC_NULLABLE: Int32 = 0
let FLINTDB_SWIFT_SPEC_NOT_NULL: Int32 = 1

// Typed column names (so tutorials can avoid raw stringly-typed columns)
protocol FlintColumnName {
	var flintName: String { get }
}

extension FlintColumnName where Self: RawRepresentable, Self.RawValue == String {
	var flintName: String { rawValue }
}

enum FlintSwiftError: Error, CustomStringConvertible {
	case flintdb(String)
	case invalidArgument(String)

	var description: String {
		switch self {
		case .flintdb(let s):
			return s
		case .invalidArgument(let s):
			return s
		}
	}
}

@inline(__always)
func flintTakeError(_ e: inout UnsafeMutablePointer<CChar>?) -> String? {
	guard let ptr = e else { return nil }
	defer {
		free(ptr)
		e = nil
	}
	return String(cString: ptr)
}

@inline(__always)
func flintCheck(_ e: inout UnsafeMutablePointer<CChar>?, _ context: String) throws {
	if let msg = flintTakeError(&e) {
		throw FlintSwiftError.flintdb("\(context): \(msg)")
	}
}

final class Meta {
	private let ptr: OpaquePointer

	init(_ name: String) throws {
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let p = flintdb_swift_meta_new(name, &e) else {
			try flintCheck(&e, "meta_new")
			throw FlintSwiftError.flintdb("meta_new: returned NULL")
		}
		try flintCheck(&e, "meta_new")
		self.ptr = p
	}

	static func open(_ filename: String) throws -> Meta {
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let p = flintdb_swift_meta_open(filename, &e) else {
			try flintCheck(&e, "meta_open")
			throw FlintSwiftError.flintdb("meta_open: returned NULL")
		}
		try flintCheck(&e, "meta_open")
		return Meta(owning: p)
	}

	private init(owning p: OpaquePointer) {
		self.ptr = p
	}

	deinit {
		flintdb_swift_meta_free(ptr)
	}

	func raw() -> OpaquePointer { ptr }

	func setCache(bytes: Int) {
		flintdb_swift_meta_set_cache(ptr, Int32(bytes))
	}

	func setTextFormat(absentHeader: Bool, delimiter: Character, format: String) {
		let delim = delimiter.unicodeScalars.first.map { Int8($0.value) } ?? 0
		flintdb_swift_meta_set_text_format(ptr, absentHeader ? 1 : 0, CChar(delim), format)
	}

	func addColumn(
		_ name: String,
		type: Int32,
		bytes: Int32 = 0,
		precision: Int16 = 0,
		notNull: Bool,
		defaultValue: String? = nil,
		comment: String? = nil
	) throws {
		var e: UnsafeMutablePointer<CChar>? = nil
		flintdb_swift_meta_columns_add(
			ptr,
			name,
			type,
			bytes,
			precision,
			notNull ? FLINTDB_SWIFT_SPEC_NOT_NULL : FLINTDB_SWIFT_SPEC_NULLABLE,
			defaultValue,
			comment,
			&e
		)
		try flintCheck(&e, "meta_columns_add")
	}

	func addColumn<C: FlintColumnName>(
		_ col: C,
		type: Int32,
		bytes: Int32,
		precision: Int16,
		notNull: Bool,
		defaultValue: String? = nil,
		comment: String? = nil
	) throws {
		try addColumn(
			col.flintName,
			type: type,
			bytes: bytes,
			precision: precision,
			notNull: notNull,
			defaultValue: defaultValue,
			comment: comment
		)
	}

	func addIndex(_ name: String, algorithm: String? = nil, keys: [String]) throws {
		var e: UnsafeMutablePointer<CChar>? = nil
		let cStrings = keys.map { strdup($0) }
		defer { cStrings.forEach { free($0) } }

		var ptrs: [UnsafePointer<CChar>?] = cStrings.map { UnsafePointer<CChar>($0!) }
		ptrs.withUnsafeMutableBufferPointer { bp in
			flintdb_swift_meta_indexes_add(ptr, name, algorithm, bp.baseAddress, UInt16(keys.count), &e)
		}
		try flintCheck(&e, "meta_indexes_add")
	}

	func addIndex<C: FlintColumnName>(_ name: String, algorithm: String? = nil, keys: [C]) throws {
		try addIndex(name, algorithm: algorithm, keys: keys.map { $0.flintName })
	}

	func columnIndex(_ name: String) throws -> Int32 {
		let idx = flintdb_swift_column_at(ptr, name)
		if idx < 0 { throw FlintSwiftError.invalidArgument("Unknown column: \(name)") }
		return idx
	}

	func columnIndex<C: FlintColumnName>(_ col: C) throws -> Int32 {
		try columnIndex(col.flintName)
	}

	func toSQLString() throws -> String {
		var e: UnsafeMutablePointer<CChar>? = nil
		var buf = Array<CChar>(repeating: 0, count: 4096)
		let rc = buf.withUnsafeMutableBufferPointer { bp in
			flintdb_swift_meta_to_sql_string(ptr, bp.baseAddress, Int32(bp.count), &e)
		}
		if rc != 0 {
			try flintCheck(&e, "meta_to_sql_string")
			throw FlintSwiftError.flintdb("meta_to_sql_string: failed")
		}
		try flintCheck(&e, "meta_to_sql_string")
		return String(cString: buf)
	}
}

final class Row {
	private let ptr: OpaquePointer
	private let meta: Meta

	init(meta: Meta) throws {
		self.meta = meta
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let p = flintdb_swift_row_new(meta.raw(), &e) else {
			try flintCheck(&e, "row_new")
			throw FlintSwiftError.flintdb("row_new: returned NULL")
		}
		try flintCheck(&e, "row_new")
		self.ptr = p
	}

	deinit {
		flintdb_swift_row_free(ptr)
	}

	func raw() -> OpaquePointer { ptr }

	func setI64(_ i: UInt16, _ v: Int64) throws {
		var e: UnsafeMutablePointer<CChar>? = nil
		flintdb_swift_row_set_i64(ptr, i, v, &e)
		try flintCheck(&e, "row_set_i64")
	}

	func setI32(_ i: UInt16, _ v: Int32) throws {
		var e: UnsafeMutablePointer<CChar>? = nil
		flintdb_swift_row_set_i32(ptr, i, v, &e)
		try flintCheck(&e, "row_set_i32")
	}

	func setF64(_ i: UInt16, _ v: Double) throws {
		var e: UnsafeMutablePointer<CChar>? = nil
		flintdb_swift_row_set_f64(ptr, i, v, &e)
		try flintCheck(&e, "row_set_f64")
	}

	func setString(_ i: UInt16, _ s: String) throws {
		var e: UnsafeMutablePointer<CChar>? = nil
		s.withCString { cstr in
			flintdb_swift_row_set_string(ptr, i, cstr, &e)
		}
		try flintCheck(&e, "row_set_string")
	}

	func setI64(_ name: String, _ v: Int64) throws { try setI64(UInt16(try meta.columnIndex(name)), v) }
	func setI32(_ name: String, _ v: Int32) throws { try setI32(UInt16(try meta.columnIndex(name)), v) }
	func setF64(_ name: String, _ v: Double) throws { try setF64(UInt16(try meta.columnIndex(name)), v) }
	func setString(_ name: String, _ v: String) throws { try setString(UInt16(try meta.columnIndex(name)), v) }

	func setI64<C: FlintColumnName>(_ col: C, _ v: Int64) throws { try setI64(col.flintName, v) }
	func setI32<C: FlintColumnName>(_ col: C, _ v: Int32) throws { try setI32(col.flintName, v) }
	func setF64<C: FlintColumnName>(_ col: C, _ v: Double) throws { try setF64(col.flintName, v) }
	func setString<C: FlintColumnName>(_ col: C, _ v: String) throws { try setString(col.flintName, v) }

	func validate() throws -> Bool {
		var e: UnsafeMutablePointer<CChar>? = nil
		let ok = flintdb_swift_row_validate(ptr, &e)
		try flintCheck(&e, "row_validate")
		return ok != 0
	}
}

final class CursorI64 {
	private var ptr: OpaquePointer?

	init(_ p: OpaquePointer) {
		self.ptr = p
	}

	deinit {
		close()
	}

	func next() throws -> Int64? {
		guard let p = ptr else { return nil }
		var e: UnsafeMutablePointer<CChar>? = nil
		let v = flintdb_swift_cursor_i64_next(p, &e)
		try flintCheck(&e, "cursor_next")
		return v >= 0 ? v : nil
	}

	func close() {
		if let p = ptr {
			flintdb_swift_cursor_i64_close(p)
			ptr = nil
		}
	}
}

final class Table {
	private let ptr: OpaquePointer
	private let meta: Meta?

	private init(_ p: OpaquePointer, meta: Meta?) {
		self.ptr = p
		self.meta = meta
	}

	deinit {
		flintdb_swift_table_close(ptr)
	}

	static func create(_ file: String, meta: Meta) throws -> Table {
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let p = flintdb_swift_table_open_rdwr(file, meta.raw(), &e) else {
			try flintCheck(&e, "table_open")
			throw FlintSwiftError.flintdb("table_open: returned NULL")
		}
		try flintCheck(&e, "table_open")
		return Table(p, meta: meta)
	}

	static func openReadOnly(_ file: String) throws -> Table {
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let p = flintdb_swift_table_open_rdonly(file, &e) else {
			try flintCheck(&e, "table_open")
			throw FlintSwiftError.flintdb("table_open: returned NULL")
		}
		try flintCheck(&e, "table_open")
		return Table(p, meta: nil)
	}

	static func openReadWrite(_ file: String) throws -> Table {
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let p = flintdb_swift_table_open_rdwr(file, nil, &e) else {
			try flintCheck(&e, "table_open")
			throw FlintSwiftError.flintdb("table_open: returned NULL")
		}
		try flintCheck(&e, "table_open")
		return Table(p, meta: nil)
	}

	func apply(_ row: Row, upsert: Bool = false) throws -> Int64 {
		var e: UnsafeMutablePointer<CChar>? = nil
		let rowid = flintdb_swift_table_apply(ptr, row.raw(), upsert ? 1 : 0, &e)
		try flintCheck(&e, "table_apply")
		return rowid
	}

	func applyAt(_ rowid: Int64, _ row: Row) throws {
		var e: UnsafeMutablePointer<CChar>? = nil
		_ = flintdb_swift_table_apply_at(ptr, rowid, row.raw(), &e)
		try flintCheck(&e, "table_apply_at")
	}

	func deleteAt(_ rowid: Int64) throws {
		var e: UnsafeMutablePointer<CChar>? = nil
		_ = flintdb_swift_table_delete_at(ptr, rowid, &e)
		try flintCheck(&e, "table_delete_at")
	}

	func find(_ whereClause: String) throws -> CursorI64 {
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let c = flintdb_swift_table_find(ptr, whereClause, &e) else {
			try flintCheck(&e, "table_find")
			throw FlintSwiftError.flintdb("table_find: returned NULL")
		}
		try flintCheck(&e, "table_find")
		return CursorI64(c)
	}

	func read(_ rowid: Int64) throws -> OpaquePointer {
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let r = flintdb_swift_table_read(ptr, rowid, &e) else {
			try flintCheck(&e, "table_read")
			throw FlintSwiftError.flintdb("table_read: returned NULL")
		}
		try flintCheck(&e, "table_read")
		return r
	}

	static func drop(_ file: String) {
		var e: UnsafeMutablePointer<CChar>? = nil
		_ = flintdb_swift_table_drop(file, &e)
		_ = flintTakeError(&e)
	}
}

final class CursorRow {
	private var ptr: OpaquePointer?

	init(_ p: OpaquePointer) {
		self.ptr = p
	}

	deinit {
		close()
	}

	func next() throws -> OpaquePointer? {
		guard let p = ptr else { return nil }
		var e: UnsafeMutablePointer<CChar>? = nil
		let r = flintdb_swift_cursor_row_next(p, &e)
		try flintCheck(&e, "cursor_row_next")
		return r
	}

	func close() {
		if let p = ptr {
			flintdb_swift_cursor_row_close(p)
			ptr = nil
		}
	}
}

final class GenericFile {
	private let ptr: OpaquePointer
	private let meta: Meta?

	private init(_ p: OpaquePointer, meta: Meta?) {
		self.ptr = p
		self.meta = meta
	}

	deinit {
		flintdb_swift_genericfile_close(ptr)
	}

	static func create(_ file: String, meta: Meta) throws -> GenericFile {
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let p = flintdb_swift_genericfile_open_rdwr(file, meta.raw(), &e) else {
			try flintCheck(&e, "genericfile_open")
			throw FlintSwiftError.flintdb("genericfile_open: returned NULL")
		}
		try flintCheck(&e, "genericfile_open")
		return GenericFile(p, meta: meta)
	}

	static func openReadOnly(_ file: String) throws -> GenericFile {
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let p = flintdb_swift_genericfile_open_rdonly(file, &e) else {
			try flintCheck(&e, "genericfile_open")
			throw FlintSwiftError.flintdb("genericfile_open: returned NULL")
		}
		try flintCheck(&e, "genericfile_open")
		return GenericFile(p, meta: nil)
	}

	func write(_ row: Row) throws {
		var e: UnsafeMutablePointer<CChar>? = nil
		_ = flintdb_swift_genericfile_write(ptr, row.raw(), &e)
		try flintCheck(&e, "genericfile_write")
	}

	func find(_ whereClause: String) throws -> CursorRow {
		var e: UnsafeMutablePointer<CChar>? = nil
		guard let c = flintdb_swift_genericfile_find(ptr, whereClause, &e) else {
			try flintCheck(&e, "genericfile_find")
			throw FlintSwiftError.flintdb("genericfile_find: returned NULL")
		}
		try flintCheck(&e, "genericfile_find")
		return CursorRow(c)
	}

	static func drop(_ file: String) {
		var e: UnsafeMutablePointer<CChar>? = nil
		flintdb_swift_genericfile_drop(file, &e)
		_ = flintTakeError(&e)
	}
}

@inline(__always)
func flintPrintRow(_ r: OpaquePointer) {
	flintdb_swift_print_row(r)
}

@inline(__always)
func flintPrintRowBorrowed(_ r: OpaquePointer) {
	flintdb_swift_print_row(r)
}
