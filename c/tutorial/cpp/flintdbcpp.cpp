#include "flintdbcpp.h"

#include <cstdlib>
#include <cstring>
#include <sstream>

namespace flintdbcpp {

Error::Error(const std::string& message) : std::runtime_error(message) {}

[[noreturn]] void throw_error(const char* where, const char* err) {
	std::ostringstream oss;
	oss << where;
	if (err && err[0] != '\0') {
		oss << ": " << err;
	}
	throw Error(oss.str());
}

void throw_if_error(const char* where, char* err) {
	if (err) {
		throw_error(where, err);
	}
}

// ---- Meta ----

Meta::Meta() = default;

Meta::Meta(const std::string& name) {
	char* e = nullptr;
	meta_ = flintdb_meta_new(name.c_str(), &e);
	open_ = true;
	throw_if_error("flintdb_meta_new", e);
}

Meta::Meta(Meta&& other) noexcept {
	meta_ = other.meta_;
	open_ = other.open_;
	other.reset();
}

Meta& Meta::operator=(Meta&& other) noexcept {
	if (this == &other) return *this;
	if (open_) {
		flintdb_meta_close(&meta_);
	}
	meta_ = other.meta_;
	open_ = other.open_;
	other.reset();
	return *this;
}

Meta::~Meta() {
	if (open_) {
		flintdb_meta_close(&meta_);
	}
}

void Meta::reset() noexcept {
	std::memset(&meta_, 0, sizeof(meta_));
	open_ = false;
}

void Meta::add_column(const std::string& name,
					  flintdb_variant_type type,
					  i32 bytes,
					  i16 precision,
					  flintdb_null_spec nullspec,
					  const char* default_value,
					  const char* comment) {
	char* e = nullptr;
	flintdb_meta_columns_add(&meta_, name.c_str(), type, bytes, precision, nullspec, default_value, comment, &e);
	throw_if_error("flintdb_meta_columns_add", e);
}

void Meta::add_index(const std::string& name,
					 const char* algorithm,
					 const std::vector<std::string>& keys) {
	if (keys.empty()) {
		throw Error("Meta::add_index: keys must not be empty");
	}
	if (keys.size() > MAX_INDEX_KEYS_LIMIT) {
		throw Error("Meta::add_index: too many keys");
	}

	char keybuf[MAX_INDEX_KEYS_LIMIT][MAX_COLUMN_NAME_LIMIT];
	std::memset(keybuf, 0, sizeof(keybuf));
	for (size_t i = 0; i < keys.size(); ++i) {
		std::strncpy(keybuf[i], keys[i].c_str(), MAX_COLUMN_NAME_LIMIT - 1);
	}

	char* e = nullptr;
	flintdb_meta_indexes_add(&meta_, name.c_str(), algorithm, keybuf, static_cast<u16>(keys.size()), &e);
	throw_if_error("flintdb_meta_indexes_add", e);
}

int Meta::column_at(const std::string& name) {
	return flintdb_column_at(&meta_, name.c_str());
}

void Meta::set_format(const std::string& format) {
	std::memset(meta_.format, 0, sizeof(meta_.format));
	std::strncpy(meta_.format, format.c_str(), sizeof(meta_.format) - 1);
}

void Meta::set_delimiter(char delimiter) {
	meta_.delimiter = delimiter;
}

std::string Meta::to_sql_string() const {
	char buf[4096];
	std::memset(buf, 0, sizeof(buf));
	char* e = nullptr;
	int rc = flintdb_meta_to_sql_string(&meta_, buf, static_cast<i32>(sizeof(buf)), &e);
	throw_if_error("flintdb_meta_to_sql_string", e);
	if (rc != 0) {
		throw Error("flintdb_meta_to_sql_string failed");
	}
	return std::string(buf);
}

flintdb_meta* Meta::raw() { return &meta_; }
const flintdb_meta* Meta::raw() const { return &meta_; }

// ---- Row ----

Row::Row(Meta& meta) : Row(meta.raw()) {}

Row::Row(flintdb_meta* meta) {
	char* e = nullptr;
	row_ = flintdb_row_new(meta, &e);
	throw_if_error("flintdb_row_new", e);
	if (!row_) {
		throw Error("flintdb_row_new returned NULL");
	}
}

Row::Row(flintdb_row* row, bool /*adopt*/) : row_(row) {}

Row Row::adopt(flintdb_row* row) {
	if (!row) {
		throw Error("Row::adopt: row is NULL");
	}
	return Row(row, true);
}

Row::Row(Row&& other) noexcept {
	row_ = other.row_;
	other.row_ = nullptr;
}

Row& Row::operator=(Row&& other) noexcept {
	if (this == &other) return *this;
	reset();
	row_ = other.row_;
	other.row_ = nullptr;
	return *this;
}

Row::~Row() { reset(); }

void Row::reset() noexcept {
	if (row_) {
		row_->free(row_);
		row_ = nullptr;
	}
}

void Row::set_i64(u16 col, i64 v) {
	char* e = nullptr;
	row_->i64_set(row_, col, v, &e);
	throw_if_error("row->i64_set", e);
}

void Row::set_i32(u16 col, i32 v) {
	char* e = nullptr;
	row_->i32_set(row_, col, v, &e);
	throw_if_error("row->i32_set", e);
}

void Row::set_f64(u16 col, f64 v) {
	char* e = nullptr;
	row_->f64_set(row_, col, v, &e);
	throw_if_error("row->f64_set", e);
}

void Row::set_string(u16 col, const std::string& v) {
	char* e = nullptr;
	row_->string_set(row_, col, v.c_str(), &e);
	throw_if_error("row->string_set", e);
}

u16 Row::col_index_or_throw(const std::string& name) const {
	if (!row_ || !row_->meta) {
		throw Error("Row: meta is not available");
	}
	int idx = flintdb_column_at(row_->meta, name.c_str());
	if (idx < 0) {
		throw Error(std::string("Unknown column: ") + name);
	}
	return static_cast<u16>(idx);
}

void Row::set_i64(const std::string& col, i64 v) { set_i64(col_index_or_throw(col), v); }
void Row::set_i32(const std::string& col, i32 v) { set_i32(col_index_or_throw(col), v); }
void Row::set_f64(const std::string& col, f64 v) { set_f64(col_index_or_throw(col), v); }
void Row::set_string(const std::string& col, const std::string& v) { set_string(col_index_or_throw(col), v); }

bool Row::validate() const {
	char* e = nullptr;
	i8 ok = row_->validate(row_, &e);
	throw_if_error("row->validate", e);
	return ok != 0;
}

flintdb_row* Row::raw() { return row_; }
const flintdb_row* Row::raw() const { return row_; }

// ---- RowView ----

RowView::RowView(const flintdb_row* row) : row_(row) {
	if (!row_) {
		throw Error("RowView: row is NULL");
	}
}

i64 RowView::get_i64(u16 col) const {
	char* e = nullptr;
	i64 v = row_->i64_get(row_, col, &e);
	throw_if_error("row->i64_get", e);
	return v;
}

i32 RowView::get_i32(u16 col) const {
	char* e = nullptr;
	i32 v = row_->i32_get(row_, col, &e);
	throw_if_error("row->i32_get", e);
	return v;
}

f64 RowView::get_f64(u16 col) const {
	char* e = nullptr;
	f64 v = row_->f64_get(row_, col, &e);
	throw_if_error("row->f64_get", e);
	return v;
}

std::string RowView::get_string(u16 col) const {
	char* e = nullptr;
	const char* s = row_->string_get(row_, col, &e);
	throw_if_error("row->string_get", e);
	return s ? std::string(s) : std::string();
}

u16 RowView::col_index_or_throw(const std::string& name) const {
	if (!row_ || !row_->meta) {
		throw Error("RowView: meta is not available");
	}
	int idx = flintdb_column_at(row_->meta, name.c_str());
	if (idx < 0) {
		throw Error(std::string("Unknown column: ") + name);
	}
	return static_cast<u16>(idx);
}

i64 RowView::get_i64(const std::string& col) const { return get_i64(col_index_or_throw(col)); }
i32 RowView::get_i32(const std::string& col) const { return get_i32(col_index_or_throw(col)); }
f64 RowView::get_f64(const std::string& col) const { return get_f64(col_index_or_throw(col)); }
std::string RowView::get_string(const std::string& col) const { return get_string(col_index_or_throw(col)); }

// ---- RowIdRange ----

RowIdRange::RowIdRange(CursorI64&& cursor) : cursor_(std::move(cursor)) {}

RowIdRange::iterator::iterator(CursorI64* cursor, i64 current, bool end)
	: cursor_(cursor), current_(current), end_(end) {}

i64 RowIdRange::iterator::operator*() const { return current_; }

RowIdRange::iterator& RowIdRange::iterator::operator++() {
	if (!cursor_ || end_) return *this;
	current_ = cursor_->next();
	if (current_ < 0) {
		end_ = true;
	}
	return *this;
}

bool RowIdRange::iterator::operator!=(const iterator& other) const { return end_ != other.end_; }

RowIdRange::iterator RowIdRange::begin() {
	i64 first = cursor_.next();
	if (first < 0) {
		return iterator(nullptr, -1, true);
	}
	return iterator(&cursor_, first, false);
}

RowIdRange::iterator RowIdRange::end() { return iterator(nullptr, -1, true); }

// ---- RowRange ----

RowRange::RowRange(CursorRow&& cursor) : cursor_(std::move(cursor)) {}

RowRange::iterator::iterator(CursorRow* cursor, flintdb_row* current, bool end)
	: cursor_(cursor), current_(current), end_(end) {}

flintdb_row* RowRange::iterator::operator*() const { return current_; }

RowRange::iterator& RowRange::iterator::operator++() {
	if (!cursor_ || end_) return *this;
	current_ = cursor_->next_borrowed();
	if (!current_) {
		end_ = true;
	}
	return *this;
}

bool RowRange::iterator::operator!=(const iterator& other) const { return end_ != other.end_; }

RowRange::iterator RowRange::begin() {
	flintdb_row* first = cursor_.next_borrowed();
	if (!first) {
		return iterator(nullptr, nullptr, true);
	}
	return iterator(&cursor_, first, false);
}

RowRange::iterator RowRange::end() { return iterator(nullptr, nullptr, true); }

const flintdb_row* RowView::raw() const { return row_; }

// ---- CursorI64 ----

CursorI64::CursorI64(flintdb_cursor_i64* cursor) : cursor_(cursor) {}

CursorI64::CursorI64(CursorI64&& other) noexcept {
	cursor_ = other.cursor_;
	other.cursor_ = nullptr;
}

CursorI64& CursorI64::operator=(CursorI64&& other) noexcept {
	if (this == &other) return *this;
	close();
	cursor_ = other.cursor_;
	other.cursor_ = nullptr;
	return *this;
}

CursorI64::~CursorI64() { close(); }

void CursorI64::reset() noexcept { cursor_ = nullptr; }

i64 CursorI64::next() {
	if (!cursor_) return -1;
	char* e = nullptr;
	i64 rowid = cursor_->next(cursor_, &e);
	throw_if_error("cursor_i64->next", e);
	return rowid;
}

void CursorI64::close() {
	if (cursor_) {
		cursor_->close(cursor_);
		cursor_ = nullptr;
	}
}

// ---- CursorRow ----

CursorRow::CursorRow(flintdb_cursor_row* cursor, bool owning) : cursor_(cursor), owning_(owning) {}

CursorRow::CursorRow(CursorRow&& other) noexcept {
	cursor_ = other.cursor_;
	owning_ = other.owning_;
	other.cursor_ = nullptr;
}

CursorRow& CursorRow::operator=(CursorRow&& other) noexcept {
	if (this == &other) return *this;
	close();
	cursor_ = other.cursor_;
	owning_ = other.owning_;
	other.cursor_ = nullptr;
	return *this;
}

CursorRow::~CursorRow() { close(); }

void CursorRow::reset() noexcept { cursor_ = nullptr; }

flintdb_row* CursorRow::next_borrowed() {
	if (!cursor_) return nullptr;
	char* e = nullptr;
	flintdb_row* r = cursor_->next(cursor_, &e);
	throw_if_error("cursor_row->next", e);
	return r;
}

void CursorRow::close() {
	if (cursor_ && owning_) {
		cursor_->close(cursor_);
	}
	cursor_ = nullptr;
	owning_ = true;
}

// ---- Table ----

Table::Table(const std::string& file, flintdb_open_mode mode, const Meta* meta) {
	char* e = nullptr;
	table_ = flintdb_table_open(file.c_str(), mode, meta ? meta->raw() : nullptr, &e);
	throw_if_error("flintdb_table_open", e);
	if (!table_) {
		throw Error("flintdb_table_open returned NULL");
	}
}

Table Table::open(const std::string& file, flintdb_open_mode mode) {
	return Table(file, mode, nullptr);
}

Table Table::create(const std::string& file, flintdb_open_mode mode, const Meta& meta) {
	return Table(file, mode, &meta);
}

Table::Table(Table&& other) noexcept {
	table_ = other.table_;
	other.table_ = nullptr;
}

Table& Table::operator=(Table&& other) noexcept {
	if (this == &other) return *this;
	close();
	table_ = other.table_;
	other.table_ = nullptr;
	return *this;
}

Table::~Table() { close(); }

void Table::reset() noexcept { table_ = nullptr; }

i64 Table::apply(Row& row, bool upsert) {
	char* e = nullptr;
	i64 rowid = table_->apply(table_, row.raw(), upsert ? 1 : 0, &e);
	throw_if_error("table->apply", e);
	return rowid;
}

i64 Table::apply_at(i64 rowid, Row& row) {
	char* e = nullptr;
	i64 out = table_->apply_at(table_, rowid, row.raw(), &e);
	throw_if_error("table->apply_at", e);
	return out;
}

i64 Table::delete_at(i64 rowid) {
	char* e = nullptr;
	i64 out = table_->delete_at(table_, rowid, &e);
	throw_if_error("table->delete_at", e);
	return out;
}

CursorI64 Table::find(const std::string& where) {
	char* e = nullptr;
	flintdb_cursor_i64* c = table_->find(table_, where.c_str(), &e);
	throw_if_error("table->find", e);
	if (!c) {
		throw Error("table->find returned NULL");
	}
	return CursorI64(c);
}

RowIdRange Table::rowids(const std::string& where) {
	CursorI64 c = find(where);
	return RowIdRange(std::move(c));
}

TableRowRange Table::rows(const std::string& where) {
	return TableRowRange(this, rowids(where));
}

const flintdb_row* Table::read(i64 rowid) {
	char* e = nullptr;
	const flintdb_row* r = table_->read(table_, rowid, &e);
	throw_if_error("table->read", e);
	return r;
}

const flintdb_meta* Table::meta() const {
	char* e = nullptr;
	const flintdb_meta* m = table_->meta(table_, &e);
	throw_if_error("table->meta", e);
	return m;
}

void Table::close() {
	if (table_) {
		table_->close(table_);
		table_ = nullptr;
	}
}

// ---- TableRowRange ----

TableRowRange::TableRowRange(Table* table, RowIdRange&& rowids)
	: table_(table), rowids_(std::move(rowids)) {}

TableRowRange::iterator::iterator(Table* table, RowIdRange::iterator it, RowIdRange::iterator end)
	: table_(table), it_(it), end_(end) {
	if (table_ && (it_ != end_)) {
		current_ = table_->read(*it_);
	}
}

RowView TableRowRange::iterator::operator*() const {
	return RowView(current_);
}

TableRowRange::iterator& TableRowRange::iterator::operator++() {
	if (!table_) return *this;
	++it_;
	if (it_ != end_) {
		current_ = table_->read(*it_);
	} else {
		current_ = nullptr;
	}
	return *this;
}

bool TableRowRange::iterator::operator!=(const iterator& other) const {
	return it_ != other.it_;
}

TableRowRange::iterator TableRowRange::begin() {
	if (!table_) {
		return end();
	}
	return iterator(table_, rowids_.begin(), rowids_.end());
}

TableRowRange::iterator TableRowRange::end() {
	return iterator(nullptr, RowIdRange::iterator(), RowIdRange::iterator());
}

// ---- GenericFile ----

GenericFile::GenericFile(const std::string& file, flintdb_open_mode mode, const Meta* meta) {
	char* e = nullptr;
	file_ = flintdb_genericfile_open(file.c_str(), mode, meta ? meta->raw() : nullptr, &e);
	throw_if_error("flintdb_genericfile_open", e);
	if (!file_) {
		throw Error("flintdb_genericfile_open returned NULL");
	}
}

GenericFile GenericFile::open(const std::string& file, flintdb_open_mode mode) {
	return GenericFile(file, mode, nullptr);
}

GenericFile GenericFile::create(const std::string& file, flintdb_open_mode mode, const Meta& meta) {
	return GenericFile(file, mode, &meta);
}

GenericFile::GenericFile(GenericFile&& other) noexcept {
	file_ = other.file_;
	other.file_ = nullptr;
}

GenericFile& GenericFile::operator=(GenericFile&& other) noexcept {
	if (this == &other) return *this;
	close();
	file_ = other.file_;
	other.file_ = nullptr;
	return *this;
}

GenericFile::~GenericFile() { close(); }

void GenericFile::reset() noexcept { file_ = nullptr; }

i64 GenericFile::write(Row& row) {
	char* e = nullptr;
	i64 out = file_->write(file_, row.raw(), &e);
	throw_if_error("genericfile->write", e);
	return out;
}

CursorRow GenericFile::find(const std::string& where) const {
	char* e = nullptr;
	flintdb_cursor_row* c = file_->find(file_, where.c_str(), &e);
	throw_if_error("genericfile->find", e);
	if (!c) {
		throw Error("genericfile->find returned NULL");
	}
	return CursorRow(c);
}

RowRange GenericFile::rows(const std::string& where) const {
	CursorRow c = find(where);
	return RowRange(std::move(c));
}

void GenericFile::close() {
	if (file_) {
		file_->close(file_);
		file_ = nullptr;
	}
}

// ---- FileSort ----

FileSort::FileSort(const std::string& file, const Meta& meta) {
	char* e = nullptr;
	sorter_ = flintdb_filesort_new(file.c_str(), meta.raw(), &e);
	throw_if_error("flintdb_filesort_new", e);
	if (!sorter_) {
		throw Error("flintdb_filesort_new returned NULL");
	}
}

FileSort::FileSort(FileSort&& other) noexcept {
	sorter_ = other.sorter_;
	other.sorter_ = nullptr;
}

FileSort& FileSort::operator=(FileSort&& other) noexcept {
	if (this == &other) return *this;
	close();
	sorter_ = other.sorter_;
	other.sorter_ = nullptr;
	return *this;
}

FileSort::~FileSort() { close(); }

void FileSort::reset() noexcept { sorter_ = nullptr; }

i64 FileSort::add(Row& row) {
	char* e = nullptr;
	i64 out = sorter_->add(sorter_, row.raw(), &e);
	throw_if_error("filesort->add", e);
	return out;
}

i64 FileSort::sort(int (*cmpr)(const void* obj, const flintdb_row* a, const flintdb_row* b), const void* ctx) {
	char* e = nullptr;
	i64 out = sorter_->sort(sorter_, cmpr, ctx, &e);
	throw_if_error("filesort->sort", e);
	return out;
}

i64 FileSort::rows() const {
	return sorter_->rows(sorter_);
}

Row FileSort::read(i64 index) const {
	char* e = nullptr;
	flintdb_row* r = sorter_->read(sorter_, index, &e);
	throw_if_error("filesort->read", e);
	if (!r) {
		throw Error("filesort->read returned NULL");
	}
	return Row::adopt(r);
}

void FileSort::close() {
	if (sorter_) {
		sorter_->close(sorter_);
		sorter_ = nullptr;
	}
}

// ---- Aggregate ----

Aggregate::Aggregate(flintdb_aggregate* agg) : agg_(agg) {}

Aggregate::Aggregate(Aggregate&& other) noexcept {
	agg_ = other.agg_;
	other.agg_ = nullptr;
}

Aggregate& Aggregate::operator=(Aggregate&& other) noexcept {
	if (this == &other) return *this;
	close();
	agg_ = other.agg_;
	other.agg_ = nullptr;
	return *this;
}

Aggregate::~Aggregate() { close(); }

void Aggregate::reset() noexcept { agg_ = nullptr; }

Aggregate Aggregate::create(const std::string& id,
							std::vector<flintdb_aggregate_groupby*>&& groupby,
							std::vector<flintdb_aggregate_func*>&& funcs) {
	if (groupby.empty()) {
		throw Error("Aggregate::create: groupby must not be empty");
	}
	if (funcs.empty()) {
		throw Error("Aggregate::create: funcs must not be empty");
	}

	auto* gb = static_cast<flintdb_aggregate_groupby**>(
		std::calloc(groupby.size(), sizeof(flintdb_aggregate_groupby*)));
	auto* fn = static_cast<flintdb_aggregate_func**>(
		std::calloc(funcs.size(), sizeof(flintdb_aggregate_func*)));
	if (!gb || !fn) {
		if (gb) std::free(gb);
		if (fn) std::free(fn);
		throw Error("Aggregate::create: allocation failed");
	}

	for (size_t i = 0; i < groupby.size(); ++i) gb[i] = groupby[i];
	for (size_t i = 0; i < funcs.size(); ++i) fn[i] = funcs[i];

	char* e = nullptr;
	flintdb_aggregate* agg = aggregate_new(
		id.c_str(), gb, static_cast<u16>(groupby.size()), fn, static_cast<u16>(funcs.size()), &e);
	if (e || !agg) {
		// aggregate_new takes ownership only on success, so free here
		for (size_t i = 0; i < groupby.size(); ++i) {
			if (gb[i]) gb[i]->free(gb[i]);
		}
		for (size_t i = 0; i < funcs.size(); ++i) {
			if (fn[i]) fn[i]->free(fn[i]);
		}
		std::free(gb);
		std::free(fn);
		throw_if_error("aggregate_new", e);
		throw Error("aggregate_new returned NULL");
	}

	return Aggregate(agg);
}

void Aggregate::row(const flintdb_row* r) {
	char* e = nullptr;
	agg_->row(agg_, r, &e);
	throw_if_error("aggregate->row", e);
}

std::vector<Row> Aggregate::compute() {
	char* e = nullptr;
	flintdb_row** out_rows = nullptr;
	int count = agg_->compute(agg_, &out_rows, &e);
	throw_if_error("aggregate->compute", e);
	if (count < 0) {
		throw Error("aggregate->compute returned negative count");
	}

	std::vector<Row> rows;
	rows.reserve(static_cast<size_t>(count));
	for (int i = 0; i < count; ++i) {
		rows.push_back(Row::adopt(out_rows[i]));
	}
	std::free(out_rows);
	return rows;
}

void Aggregate::close() {
	if (agg_) {
		agg_->free(agg_);
		agg_ = nullptr;
	}
}

// ---- SqlResult ----

SqlResult::SqlResult(flintdb_sql_result* result) : result_(result) {
	if (result_ && result_->row_cursor) {
		// Non-owning view: flintdb_sql_result::close() owns/cleans up the cursor.
		cursor_ = CursorRow(result_->row_cursor, false);
	}
}

SqlResult::SqlResult(SqlResult&& other) noexcept {
	result_ = other.result_;
	cursor_ = std::move(other.cursor_);
	other.result_ = nullptr;
}

SqlResult& SqlResult::operator=(SqlResult&& other) noexcept {
	if (this == &other) return *this;
	close();
	result_ = other.result_;
	cursor_ = std::move(other.cursor_);
	other.result_ = nullptr;
	return *this;
}

SqlResult::~SqlResult() { close(); }

void SqlResult::reset() noexcept { result_ = nullptr; }

i64 SqlResult::affected() const { return result_ ? result_->affected : 0; }
int SqlResult::column_count() const { return result_ ? result_->column_count : 0; }

const char* SqlResult::column_name(int i) const {
	if (!result_ || i < 0 || i >= result_->column_count) return nullptr;
	return result_->column_names ? result_->column_names[i] : nullptr;
}

CursorRow& SqlResult::row_cursor() { return cursor_; }

RowRange SqlResult::rows() {
	if (!result_ || !result_->row_cursor) {
		return RowRange(CursorRow());
	}
	return RowRange(CursorRow(result_->row_cursor, false));
}

void SqlResult::close() {
	if (result_) {
		result_->close(result_);
		result_ = nullptr;
		cursor_ = CursorRow();
	}
}

SqlResult sql_exec(const std::string& sql) {
	char* e = nullptr;
	flintdb_sql_result* r = flintdb_sql_exec(sql.c_str(), nullptr, &e);
	throw_if_error("flintdb_sql_exec", e);
	if (!r) {
		throw Error("flintdb_sql_exec returned NULL");
	}
	return SqlResult(r);
}

} // namespace flintdbcpp

