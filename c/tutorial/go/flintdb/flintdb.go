// FlintDB Go Wrapper
package flintdb

/*
#cgo CFLAGS: -I../../../src
#cgo LDFLAGS: -L../../../lib -lflintdb
#include "flintdb.h"
#include <stdlib.h>
#include <string.h>

static void row_free_wrapper(struct flintdb_row *r) {
    if (r && r->free) r->free(r);
}

static void row_i32_set_wrapper(struct flintdb_row *r, int col_idx, int value, char **e) {
    if (r && r->i32_set) r->i32_set(r, col_idx, value, e);
}

static void row_i64_set_wrapper(struct flintdb_row *r, int col_idx, long long value, char **e) {
    if (r && r->i64_set) r->i64_set(r, col_idx, value, e);
}

static void row_string_set_wrapper(struct flintdb_row *r, int col_idx, const char *value, char **e) {
    if (r && r->string_set) r->string_set(r, col_idx, value, e);
}

static void row_f64_set_wrapper(struct flintdb_row *r, int col_idx, double value, char **e) {
    if (r && r->f64_set) r->f64_set(r, col_idx, value, e);
}

static void table_close_wrapper(struct flintdb_table *t) {
    if (t && t->close) t->close(t);
}

static long long table_apply_wrapper(struct flintdb_table *t, struct flintdb_row *r, i8 upsert, char **e) {
    if (t && t->apply) return t->apply(t, r, upsert, e);
    return -1;
}

static long long table_apply_at_wrapper(struct flintdb_table *t, long long rowid, struct flintdb_row *r, char **e) {
    if (t && t->apply_at) return t->apply_at(t, rowid, r, e);
    return -1;
}

static long long table_delete_at_wrapper(struct flintdb_table *t, long long rowid, char **e) {
    if (t && t->delete_at) return t->delete_at(t, rowid, e);
    return -1;
}

static const struct flintdb_row* table_read_wrapper(struct flintdb_table *t, long long rowid, char **e) {
    if (t && t->read) return t->read(t, rowid, e);
    return NULL;
}

static struct flintdb_cursor_i64* table_find_wrapper(struct flintdb_table *t, const char *query, char **e) {
    if (t && t->find) return t->find(t, query, e);
    return NULL;
}

static const struct flintdb_meta* table_meta_wrapper(const struct flintdb_table *t, char **e) {
    if (t && t->meta) return t->meta(t, e);
    return NULL;
}

static void cursor_i64_close_wrapper(struct flintdb_cursor_i64 *c) {
    if (c && c->close) c->close(c);
}

static long long cursor_i64_next_wrapper(struct flintdb_cursor_i64 *c, char **e) {
    if (c && c->next) return c->next(c, e);
    return -1;
}

static void genericfile_close_wrapper(struct flintdb_genericfile *f) {
    if (f && f->close) f->close(f);
}

static int genericfile_write_wrapper(struct flintdb_genericfile *f, struct flintdb_row *r, char **e) {
    if (f && f->write) return f->write(f, r, e);
    return -1;
}

static struct flintdb_cursor_row* genericfile_find_wrapper(struct flintdb_genericfile *f, const char *query, char **e) {
    if (f && f->find) return f->find(f, query, e);
    return NULL;
}

static const struct flintdb_meta* genericfile_meta_wrapper(const struct flintdb_genericfile *f, char **e) {
    if (f && f->meta) return f->meta(f, e);
    return NULL;
}

static void cursor_row_close_wrapper(struct flintdb_cursor_row *c) {
    if (c && c->close) c->close(c);
}

static struct flintdb_row* cursor_row_next_wrapper(struct flintdb_cursor_row *c, char **e) {
    if (c && c->next) return c->next(c, e);
    return NULL;
}
*/
import "C"
import (
	"fmt"
	"unsafe"
)

type FlintDBError struct {
	Message string
}

func (e *FlintDBError) Error() string {
	return fmt.Sprintf("FlintDB error: %s", e.Message)
}

func checkError(e *C.char) error {
	if e != nil {
		msg := C.GoString(e)
		return &FlintDBError{Message: msg}
	}
	return nil
}

const (
	VARIANT_INT32  = C.VARIANT_INT32
	VARIANT_INT64  = C.VARIANT_INT64
	VARIANT_STRING = C.VARIANT_STRING
	VARIANT_DOUBLE = C.VARIANT_DOUBLE
	VARIANT_FLOAT  = C.VARIANT_FLOAT
)

const (
	FLINTDB_RDONLY = C.FLINTDB_RDONLY
	FLINTDB_RDWR   = C.FLINTDB_RDWR
)

const (
	SPEC_NULLABLE = 0
	SPEC_NOT_NULL = 1
)

const PRIMARY_NAME = C.PRIMARY_NAME

type Meta struct {
	inner C.struct_flintdb_meta
}

func NewMeta(path string) (*Meta, error) {
	var e *C.char
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	meta := C.flintdb_meta_new(cpath, &e)
	if err := checkError(e); err != nil {
		return nil, err
	}

	return &Meta{inner: meta}, nil
}

func (m *Meta) Close() {
	C.flintdb_meta_close(&m.inner)
}

func (m *Meta) AddColumn(name string, variantType int, size int, precision int, nullspec uint32, defaultVal string, comment string) error {
	var e *C.char
	cname := C.CString(name)
	cdefault := C.CString(defaultVal)
	ccomment := C.CString(comment)
	defer C.free(unsafe.Pointer(cname))
	defer C.free(unsafe.Pointer(cdefault))
	defer C.free(unsafe.Pointer(ccomment))

	C.flintdb_meta_columns_add(&m.inner, cname, C.enum_flintdb_variant_type(variantType), C.i32(size), C.i16(precision), C.enum_flintdb_null_spec(nullspec), cdefault, ccomment, &e)
	return checkError(e)
}

func (m *Meta) AddIndex(name string, columns []string) error {
	var e *C.char
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	var keys [8][C.MAX_COLUMN_NAME_LIMIT]C.char
	for i, col := range columns {
		ccol := C.CString(col)
		C.strncpy(&keys[i][0], ccol, C.MAX_COLUMN_NAME_LIMIT-1)
		C.free(unsafe.Pointer(ccol))
	}

	C.flintdb_meta_indexes_add(&m.inner, cname, nil, (*[C.MAX_COLUMN_NAME_LIMIT]C.char)(unsafe.Pointer(&keys[0])), C.u16(len(columns)), &e)
	return checkError(e)
}

func (m *Meta) ToSQL() (string, error) {
	var e *C.char
	var sql [2048]C.char

	if C.flintdb_meta_to_sql_string(&m.inner, &sql[0], 2048, &e) != 0 {
		if err := checkError(e); err != nil {
			return "", err
		}
	}

	return C.GoString(&sql[0]), nil
}

func (m *Meta) ColumnAt(name string) int {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	return int(C.flintdb_column_at(&m.inner, cname))
}

func (m *Meta) SetFormatTSV() {
	m.inner.format[0] = 't'
	m.inner.format[1] = 's'
	m.inner.format[2] = 'v'
	m.inner.format[3] = 0
	m.inner.delimiter = '\t'
}

type Row struct {
	inner *C.struct_flintdb_row
	meta  *C.struct_flintdb_meta
}

func (r *Row) Free() {
	if r.inner != nil {
		C.row_free_wrapper(r.inner)
	}
}

func (r *Row) SetInt32(colIdx int, value int32) error {
	var e *C.char
	C.row_i32_set_wrapper(r.inner, C.int(colIdx), C.int(value), &e)
	return checkError(e)
}

func (r *Row) SetInt64(colIdx int, value int64) error {
	var e *C.char
	C.row_i64_set_wrapper(r.inner, C.int(colIdx), C.longlong(value), &e)
	return checkError(e)
}

func (r *Row) SetString(colIdx int, value string) error {
	var e *C.char
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cvalue))
	C.row_string_set_wrapper(r.inner, C.int(colIdx), cvalue, &e)
	return checkError(e)
}

func (r *Row) SetDouble(colIdx int, value float64) error {
	var e *C.char
	C.row_f64_set_wrapper(r.inner, C.int(colIdx), C.double(value), &e)
	return checkError(e)
}

func (r *Row) SetInt32ByName(colName string, value int32) error {
	cname := C.CString(colName)
	defer C.free(unsafe.Pointer(cname))
	idx := int(C.flintdb_column_at(r.meta, cname))
	return r.SetInt32(idx, value)
}

func (r *Row) SetInt64ByName(colName string, value int64) error {
	cname := C.CString(colName)
	defer C.free(unsafe.Pointer(cname))
	idx := int(C.flintdb_column_at(r.meta, cname))
	return r.SetInt64(idx, value)
}

func (r *Row) SetStringByName(colName string, value string) error {
	cname := C.CString(colName)
	defer C.free(unsafe.Pointer(cname))
	idx := int(C.flintdb_column_at(r.meta, cname))
	return r.SetString(idx, value)
}

func (r *Row) SetDoubleByName(colName string, value float64) error {
	cname := C.CString(colName)
	defer C.free(unsafe.Pointer(cname))
	idx := int(C.flintdb_column_at(r.meta, cname))
	return r.SetDouble(idx, value)
}

func (r *Row) Print() {
	C.flintdb_print_row(r.inner)
}

type Table struct {
	inner *C.struct_flintdb_table
	meta  *C.struct_flintdb_meta
}

func TableOpen(path string, mode uint32, meta *Meta) (*Table, error) {
	var e *C.char
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	var metaPtr *C.struct_flintdb_meta
	if meta != nil {
		metaPtr = &meta.inner
	}

	tbl := C.flintdb_table_open(cpath, C.enum_flintdb_open_mode(mode), metaPtr, &e)
	if err := checkError(e); err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, &FlintDBError{Message: "failed to open table"}
	}

	var tableMeta *C.struct_flintdb_meta
	if metaPtr != nil {
		tableMeta = metaPtr
	} else {
		tableMeta = (*C.struct_flintdb_meta)(C.table_meta_wrapper(tbl, &e))
		if err := checkError(e); err != nil {
			return nil, err
		}
	}

	return &Table{inner: tbl, meta: tableMeta}, nil
}

func (t *Table) Close() {
	if t.inner != nil {
		C.table_close_wrapper(t.inner)
	}
}

func TableDrop(path string) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	C.flintdb_table_drop(cpath, nil)
}

func (t *Table) CreateRow() (*Row, error) {
	var e *C.char
	row := C.flintdb_row_new(t.meta, &e)
	if err := checkError(e); err != nil {
		return nil, err
	}
	if row == nil {
		return nil, &FlintDBError{Message: "failed to create row"}
	}

	return &Row{inner: row, meta: t.meta}, nil
}

func (t *Table) Insert(row *Row) (int64, error) {
	var e *C.char
	rowid := C.table_apply_wrapper(t.inner, row.inner, 0, &e)
	if err := checkError(e); err != nil {
		return -1, err
	}
	if rowid < 0 {
		return -1, &FlintDBError{Message: "failed to insert row"}
	}
	return int64(rowid), nil
}

func (t *Table) UpdateAt(rowid int64, row *Row) error {
	var e *C.char
	result := C.table_apply_at_wrapper(t.inner, C.longlong(rowid), row.inner, &e)
	if err := checkError(e); err != nil {
		return err
	}
	if result < 0 {
		return &FlintDBError{Message: "failed to update row"}
	}
	return nil
}

func (t *Table) DeleteAt(rowid int64) error {
	var e *C.char
	result := C.table_delete_at_wrapper(t.inner, C.longlong(rowid), &e)
	if err := checkError(e); err != nil {
		return err
	}
	if result < 0 {
		return &FlintDBError{Message: "failed to delete row"}
	}
	return nil
}

func (t *Table) Read(rowid int64) (*Row, error) {
	var e *C.char
	row := C.table_read_wrapper(t.inner, C.longlong(rowid), &e)
	if err := checkError(e); err != nil {
		return nil, err
	}
	if row == nil {
		return nil, &FlintDBError{Message: "row not found"}
	}
	return &Row{inner: (*C.struct_flintdb_row)(unsafe.Pointer(row)), meta: t.meta}, nil
}

type CursorInt64 struct {
	inner *C.struct_flintdb_cursor_i64
}

func (t *Table) Find(query string) (*CursorInt64, error) {
	var e *C.char
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))

	cursor := C.table_find_wrapper(t.inner, cquery, &e)
	if err := checkError(e); err != nil {
		return nil, err
	}
	if cursor == nil {
		return nil, &FlintDBError{Message: "failed to create cursor"}
	}

	return &CursorInt64{inner: cursor}, nil
}

func (c *CursorInt64) Next() (int64, error) {
	var e *C.char
	rowid := C.cursor_i64_next_wrapper(c.inner, &e)
	if err := checkError(e); err != nil {
		return -1, err
	}
	return int64(rowid), nil
}

func (c *CursorInt64) Close() {
	if c.inner != nil {
		C.cursor_i64_close_wrapper(c.inner)
	}
}

type GenericFile struct {
	inner *C.struct_flintdb_genericfile
	meta  *C.struct_flintdb_meta
}

func GenericFileOpen(path string, mode uint32, meta *Meta) (*GenericFile, error) {
	var e *C.char
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	var metaPtr *C.struct_flintdb_meta
	if meta != nil {
		metaPtr = &meta.inner
	}

	file := C.flintdb_genericfile_open(cpath, C.enum_flintdb_open_mode(mode), metaPtr, &e)
	if err := checkError(e); err != nil {
		return nil, err
	}
	if file == nil {
		return nil, &FlintDBError{Message: "failed to open generic file"}
	}

	var fileMeta *C.struct_flintdb_meta
	if metaPtr != nil {
		fileMeta = metaPtr
	} else {
		fileMeta = (*C.struct_flintdb_meta)(C.genericfile_meta_wrapper(file, &e))
		if err := checkError(e); err != nil {
			return nil, err
		}
	}

	return &GenericFile{inner: file, meta: fileMeta}, nil
}

func (f *GenericFile) Close() {
	if f.inner != nil {
		C.genericfile_close_wrapper(f.inner)
	}
}

func GenericFileDrop(path string) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	C.flintdb_genericfile_drop(cpath, nil)
}

func (f *GenericFile) CreateRow() (*Row, error) {
	var e *C.char
	row := C.flintdb_row_new(f.meta, &e)
	if err := checkError(e); err != nil {
		return nil, err
	}
	if row == nil {
		return nil, &FlintDBError{Message: "failed to create row"}
	}

	return &Row{inner: row, meta: f.meta}, nil
}

func (f *GenericFile) Write(row *Row) error {
	var e *C.char
	ret := C.genericfile_write_wrapper(f.inner, row.inner, &e)
	if err := checkError(e); err != nil {
		return err
	}
	if ret != 0 {
		return &FlintDBError{Message: "failed to write row"}
	}
	return nil
}

type CursorRow struct {
	inner *C.struct_flintdb_cursor_row
	meta  *C.struct_flintdb_meta
}

func (f *GenericFile) Find(query string) (*CursorRow, error) {
	var e *C.char
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))

	cursor := C.genericfile_find_wrapper(f.inner, cquery, &e)
	if err := checkError(e); err != nil {
		return nil, err
	}
	if cursor == nil {
		return nil, &FlintDBError{Message: "failed to create cursor"}
	}

	return &CursorRow{inner: cursor, meta: f.meta}, nil
}

func (c *CursorRow) Next() (*Row, error) {
	var e *C.char
	row := C.cursor_row_next_wrapper(c.inner, &e)
	if err := checkError(e); err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return &Row{inner: row, meta: c.meta}, nil
}

func (c *CursorRow) Close() {
	if c.inner != nil {
		C.cursor_row_close_wrapper(c.inner)
	}
}
