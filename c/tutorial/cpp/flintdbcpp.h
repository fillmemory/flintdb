#pragma once

#include "flintdb.h"

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace flintdbcpp {

class Table;
class TableRowRange;

class Error final : public std::runtime_error {
public:
	explicit Error(const std::string& message);
};

[[noreturn]] void throw_error(const char* where, const char* err);
void throw_if_error(const char* where, char* err);

class Meta final {
public:
	Meta();
	explicit Meta(const std::string& name);
	Meta(const Meta&) = delete;
	Meta& operator=(const Meta&) = delete;
	Meta(Meta&& other) noexcept;
	Meta& operator=(Meta&& other) noexcept;
	~Meta();

	void add_column(const std::string& name,
					flintdb_variant_type type,
					i32 bytes,
					i16 precision,
					flintdb_null_spec nullspec,
					const char* default_value,
					const char* comment);

	void add_index(const std::string& name,
				   const char* algorithm,
				   const std::vector<std::string>& keys);

	int column_at(const std::string& name);

	void set_format(const std::string& format);
	void set_delimiter(char delimiter);

	std::string to_sql_string() const;

	flintdb_meta* raw();
	const flintdb_meta* raw() const;

private:
	void reset() noexcept;

	flintdb_meta meta_{};
	bool open_{false};
};

class RowView;

class Row final {
public:
	Row() = default;
	explicit Row(Meta& meta);
	explicit Row(flintdb_meta* meta);
	static Row adopt(flintdb_row* row);

	Row(const Row&) = delete;
	Row& operator=(const Row&) = delete;
	Row(Row&& other) noexcept;
	Row& operator=(Row&& other) noexcept;
	~Row();

	void set_i64(u16 col, i64 v);
	void set_i32(u16 col, i32 v);
	void set_f64(u16 col, f64 v);
	void set_string(u16 col, const std::string& v);

	void set_i64(const std::string& col, i64 v);
	void set_i32(const std::string& col, i32 v);
	void set_f64(const std::string& col, f64 v);
	void set_string(const std::string& col, const std::string& v);

	bool validate() const;

	flintdb_row* raw();
	const flintdb_row* raw() const;

private:
	u16 col_index_or_throw(const std::string& name) const;

	explicit Row(flintdb_row* row, bool adopt);
	void reset() noexcept;

	flintdb_row* row_{nullptr};
};

class RowView final {
public:
	explicit RowView(const flintdb_row* row);

	i64 get_i64(u16 col) const;
	i32 get_i32(u16 col) const;
	f64 get_f64(u16 col) const;
	std::string get_string(u16 col) const;

	i64 get_i64(const std::string& col) const;
	i32 get_i32(const std::string& col) const;
	f64 get_f64(const std::string& col) const;
	std::string get_string(const std::string& col) const;

	const flintdb_row* raw() const;

private:
	u16 col_index_or_throw(const std::string& name) const;

	const flintdb_row* row_{nullptr};
};

class CursorI64 final {
public:
	CursorI64() = default;
	explicit CursorI64(flintdb_cursor_i64* cursor);
	CursorI64(const CursorI64&) = delete;
	CursorI64& operator=(const CursorI64&) = delete;
	CursorI64(CursorI64&& other) noexcept;
	CursorI64& operator=(CursorI64&& other) noexcept;
	~CursorI64();

	// Returns next rowid, or -1 when exhausted.
	i64 next();
	void close();

private:
	void reset() noexcept;
	flintdb_cursor_i64* cursor_{nullptr};
};

class CursorRow final {
public:
	CursorRow() = default;
	explicit CursorRow(flintdb_cursor_row* cursor, bool owning = true);
	CursorRow(const CursorRow&) = delete;
	CursorRow& operator=(const CursorRow&) = delete;
	CursorRow(CursorRow&& other) noexcept;
	CursorRow& operator=(CursorRow&& other) noexcept;
	~CursorRow();

	// Returns BORROWED row owned by the cursor (valid until next() or close())
	flintdb_row* next_borrowed();
	void close();

private:
	void reset() noexcept;
	flintdb_cursor_row* cursor_{nullptr};
	bool owning_{true};
};

class RowIdRange final {
public:
	explicit RowIdRange(CursorI64&& cursor);

	class iterator final {
	public:
		iterator() = default;
		i64 operator*() const;
		iterator& operator++();
		bool operator!=(const iterator& other) const;

	private:
		friend class RowIdRange;
		explicit iterator(CursorI64* cursor, i64 current, bool end);

		CursorI64* cursor_{nullptr};
		i64 current_{-1};
		bool end_{true};
	};

	iterator begin();
	iterator end();

private:
	CursorI64 cursor_{};
};

class RowRange final {
public:
	explicit RowRange(CursorRow&& cursor);

	class iterator final {
	public:
		iterator() = default;
		flintdb_row* operator*() const;
		iterator& operator++();
		bool operator!=(const iterator& other) const;

	private:
		friend class RowRange;
		explicit iterator(CursorRow* cursor, flintdb_row* current, bool end);

		CursorRow* cursor_{nullptr};
		flintdb_row* current_{nullptr};
		bool end_{true};
	};

	iterator begin();
	iterator end();

private:
	CursorRow cursor_{};
};

class Table final {
public:
	Table() = default;
	Table(const std::string& file, flintdb_open_mode mode, const Meta* meta);
	static Table open(const std::string& file, flintdb_open_mode mode);
	static Table create(const std::string& file, flintdb_open_mode mode, const Meta& meta);

	Table(const Table&) = delete;
	Table& operator=(const Table&) = delete;
	Table(Table&& other) noexcept;
	Table& operator=(Table&& other) noexcept;
	~Table();

	i64 apply(Row& row, bool upsert);
	i64 apply_at(i64 rowid, Row& row);
	i64 delete_at(i64 rowid);

	CursorI64 find(const std::string& where);
	RowIdRange rowids(const std::string& where);
	TableRowRange rows(const std::string& where);
	const flintdb_row* read(i64 rowid);
	const flintdb_meta* meta() const;

	void close();

private:
	void reset() noexcept;
	flintdb_table* table_{nullptr};
};

class TableRowRange final {
public:
	TableRowRange(Table* table, RowIdRange&& rowids);

	class iterator final {
	public:
		iterator() = default;
		RowView operator*() const;
		iterator& operator++();
		bool operator!=(const iterator& other) const;

	private:
		friend class TableRowRange;
		iterator(Table* table, RowIdRange::iterator it, RowIdRange::iterator end);

		Table* table_{nullptr};
		RowIdRange::iterator it_{};
		RowIdRange::iterator end_{};
		const flintdb_row* current_{nullptr};
	};

	iterator begin();
	iterator end();

private:
	Table* table_{nullptr};
	RowIdRange rowids_;
};

class GenericFile final {
public:
	GenericFile() = default;
	GenericFile(const std::string& file, flintdb_open_mode mode, const Meta* meta);
	static GenericFile open(const std::string& file, flintdb_open_mode mode);
	static GenericFile create(const std::string& file, flintdb_open_mode mode, const Meta& meta);

	GenericFile(const GenericFile&) = delete;
	GenericFile& operator=(const GenericFile&) = delete;
	GenericFile(GenericFile&& other) noexcept;
	GenericFile& operator=(GenericFile&& other) noexcept;
	~GenericFile();

	i64 write(Row& row);
	CursorRow find(const std::string& where) const;
	RowRange rows(const std::string& where) const;
	void close();

private:
	void reset() noexcept;
	flintdb_genericfile* file_{nullptr};
};

class FileSort final {
public:
	FileSort() = default;
	FileSort(const std::string& file, const Meta& meta);

	FileSort(const FileSort&) = delete;
	FileSort& operator=(const FileSort&) = delete;
	FileSort(FileSort&& other) noexcept;
	FileSort& operator=(FileSort&& other) noexcept;
	~FileSort();

	i64 add(Row& row);
	i64 sort(int (*cmpr)(const void* obj, const flintdb_row* a, const flintdb_row* b), const void* ctx);
	i64 rows() const;
	Row read(i64 index) const;
	void close();

private:
	void reset() noexcept;
	flintdb_filesort* sorter_{nullptr};
};

class Aggregate final {
public:
	Aggregate() = default;
	explicit Aggregate(flintdb_aggregate* agg);
	Aggregate(const Aggregate&) = delete;
	Aggregate& operator=(const Aggregate&) = delete;
	Aggregate(Aggregate&& other) noexcept;
	Aggregate& operator=(Aggregate&& other) noexcept;
	~Aggregate();

	static Aggregate create(const std::string& id,
							std::vector<flintdb_aggregate_groupby*>&& groupby,
							std::vector<flintdb_aggregate_func*>&& funcs);

	void row(const flintdb_row* r);
	std::vector<Row> compute();
	void close();

private:
	void reset() noexcept;
	flintdb_aggregate* agg_{nullptr};
};

class SqlResult final {
public:
	SqlResult() = default;
	explicit SqlResult(flintdb_sql_result* result);
	SqlResult(const SqlResult&) = delete;
	SqlResult& operator=(const SqlResult&) = delete;
	SqlResult(SqlResult&& other) noexcept;
	SqlResult& operator=(SqlResult&& other) noexcept;
	~SqlResult();

	i64 affected() const;
	int column_count() const;
	const char* column_name(int i) const;
	CursorRow& row_cursor();
	RowRange rows();

	void close();

private:
	void reset() noexcept;
	flintdb_sql_result* result_{nullptr};
	CursorRow cursor_{};
};

SqlResult sql_exec(const std::string& sql);

} // namespace flintdbcpp

