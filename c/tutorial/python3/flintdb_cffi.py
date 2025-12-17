#!/usr/bin/env python3
"""FlintDB Python bindings using cffi.

This module intentionally models only the API surface used by `tutorial.py`.
It uses opaque pointers where possible, and accesses vtable-style function
pointers via struct definitions where necessary (Row/Table/FileSort/etc.).

Notes on ownership:
- Rows returned by Table.read() and CursorRow.next() are BORROWED.
- Rows returned by FileSort.read() and Aggregate.compute() are OWNED and must be freed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Sequence, Union

from cffi import FFI


ffi = FFI()

ffi.cdef(
    r"""
    typedef signed char i8;
    typedef unsigned char u8;
    typedef short i16;
    typedef unsigned short u16;
    typedef int i32;
    typedef unsigned int u32;
    typedef long long i64;
    typedef double f64;
    typedef unsigned long size_t;
    typedef long time_t;

    struct flintdb_meta;

    struct flintdb_cursor_i64 {
        void *p;
        i64 (*next)(struct flintdb_cursor_i64 *c, char **e);
        void (*close)(struct flintdb_cursor_i64 *c);
    };

    struct flintdb_cursor_row;

    enum flintdb_variant_type {
        VARIANT_NULL = 0,
        VARIANT_ZERO = 1,
        VARIANT_INT32 = 2,
        VARIANT_UINT32 = 3,
        VARIANT_INT8 = 4,
        VARIANT_UINT8 = 5,
        VARIANT_INT16 = 6,
        VARIANT_UINT16 = 7,
        VARIANT_INT64 = 8,
        VARIANT_DOUBLE = 9,
        VARIANT_FLOAT = 10,
        VARIANT_STRING = 11,
        VARIANT_DECIMAL = 12,
        VARIANT_BYTES = 13,
        VARIANT_DATE = 14,
        VARIANT_TIME = 15,
        VARIANT_UUID = 16,
        VARIANT_IPV6 = 17,
        VARIANT_BLOB = 18,
        VARIANT_OBJECT = 31
    };

    enum flintdb_null_spec {
        SPEC_NULLABLE = 0,
        SPEC_NOT_NULL = 1
    };

    struct flintdb_decimal {
        u8 sign;
        u8 scale;
        u8 reserved[2];
        u32 length;
        char data[16];
    };

    struct flintdb_variant {
        enum flintdb_variant_type type;
        union {
            i64 i;
            f64 f;
            struct flintdb_decimal d;
            struct {
                u8 owned;
                u8 sflag;
                u8 reserved[2];
                u32 length;
                char *data;
            } b;
            time_t t;
        } value;
    };

    struct flintdb_row {
        struct flintdb_variant *array;
        int length;
        void *priv;

        struct flintdb_meta *meta;
        i64 rowid;
        int refcount;

        void (*free)(struct flintdb_row *r);
        struct flintdb_row * (*retain)(struct flintdb_row *r);

        i64 (*id)(const struct flintdb_row *r);
        struct flintdb_variant * (*get)(const struct flintdb_row *r, u16 i, char **e);
        void (*set)(struct flintdb_row *r, u16 i, struct flintdb_variant *v, char **e);
        i8 (*is_nil)(const struct flintdb_row *r, u16 i, char **e);

        void (*string_set)(struct flintdb_row *r, u16 i, const char *str, char **e);
        void (*i64_set)(struct flintdb_row *r, u16 i, i64 val, char **e);
        void (*f64_set)(struct flintdb_row *r, u16 i, f64 val, char **e);
        void (*u8_set)(struct flintdb_row *r, u16 i, u8 val, char **e);
        void (*i8_set)(struct flintdb_row *r, u16 i, i8 val, char **e);
        void (*u16_set)(struct flintdb_row *r, u16 i, u16 val, char **e);
        void (*i16_set)(struct flintdb_row *r, u16 i, i16 val, char **e);
        void (*u32_set)(struct flintdb_row *r, u16 i, u32 val, char **e);
        void (*i32_set)(struct flintdb_row *r, u16 i, i32 val, char **e);
        void (*bytes_set)(struct flintdb_row *r, u16 i, const char *data, u32 length, char **e);
        void (*date_set)(struct flintdb_row *r, u16 i, time_t val, char **e);
        void (*time_set)(struct flintdb_row *r, u16 i, time_t val, char **e);
        void (*uuid_set)(struct flintdb_row *r, u16 i, const char *data, u32 length, char **e);
        void (*ipv6_set)(struct flintdb_row *r, u16 i, const char *data, u32 length, char **e);
        void (*decimal_set)(struct flintdb_row *r, u16 i, struct flintdb_decimal data, char **e);

        const char * (*string_get)(const struct flintdb_row *r, u16 i, char **e);
        i8 (*i8_get)(const struct flintdb_row *r, u16 i, char **e);
        u8 (*u8_get)(const struct flintdb_row *r, u16 i, char **e);
        i16 (*i16_get)(const struct flintdb_row *r, u16 i, char **e);
        u16 (*u16_get)(const struct flintdb_row *r, u16 i, char **e);
        i32 (*i32_get)(const struct flintdb_row *r, u16 i, char **e);
        u32 (*u32_get)(const struct flintdb_row *r, u16 i, char **e);
        i64 (*i64_get)(const struct flintdb_row *r, u16 i, char **e);
        f64 (*f64_get)(const struct flintdb_row *r, u16 i, char **e);
        struct flintdb_decimal (*decimal_get)(const struct flintdb_row *r, u16 i, char **e);
        const char * (*bytes_get)(const struct flintdb_row *r, u16 i, u32 *length, char **e);
        time_t (*date_get)(const struct flintdb_row *r, u16 i, char **e);
        time_t (*time_get)(const struct flintdb_row *r, u16 i, char **e);
        const char * (*uuid_get)(const struct flintdb_row *r, u16 i, u32 *length, char **e);
        const char * (*ipv6_get)(const struct flintdb_row *r, u16 i, u32 *length, char **e);

        i8 (*is_zero)(const struct flintdb_row *r, u16 i, char **e);
        i8 (*equals)(const struct flintdb_row *r, const struct flintdb_row *o);
        i8 (*compare)(const struct flintdb_row *r, const struct flintdb_row *o, int (*cmp)(const struct flintdb_row*, const struct flintdb_row*));
        struct flintdb_row * (*copy)(const struct flintdb_row *r, char **e);

        i8 (*validate)(const struct flintdb_row *r, char **e);
    };

    struct flintdb_cursor_row {
        void *p;
        struct flintdb_row * (*next)(struct flintdb_cursor_row *c, char **e);
        void (*close)(struct flintdb_cursor_row *c);
    };

    struct flintdb_table {
        i64 (*rows)(const struct flintdb_table *me, char **e);
        i64 (*bytes)(const struct flintdb_table *me, char **e);
        const struct flintdb_meta * (*meta)(const struct flintdb_table *me, char **e);

        i64 (*apply)(struct flintdb_table *me, struct flintdb_row *r, i8 upsert, char **e);
        i64 (*apply_at)(struct flintdb_table *me, i64 rowid, struct flintdb_row *r, char **e);
        i64 (*delete_at)(struct flintdb_table *me, i64 rowid, char **e);
        struct flintdb_cursor_i64 * (*find)(const struct flintdb_table *me, const char *where, char **e);
        const struct flintdb_row * (*one)(const struct flintdb_table *me, i8 index, u16 argc, const char **argv, char **e);
        const struct flintdb_row * (*read)(struct flintdb_table *me, i64 rowid, char **e);
        int (*read_stream)(struct flintdb_table *me, i64 rowid, struct flintdb_row *dest, char **e);
        void (*close)(struct flintdb_table *me);
        void *priv;
    };

    struct flintdb_genericfile {
        i64 (*rows)(const struct flintdb_genericfile *me, char **e);
        i64 (*bytes)(const struct flintdb_genericfile *me, char **e);
        const struct flintdb_meta * (*meta)(const struct flintdb_genericfile *me, char **e);

        i64 (*write)(struct flintdb_genericfile *me, struct flintdb_row *r, char **e);
        struct flintdb_cursor_row * (*find)(const struct flintdb_genericfile *me, const char *where, char **e);
        void (*close)(struct flintdb_genericfile *me);
        void *priv;
    };

    struct flintdb_filesort {
        void (*close)(struct flintdb_filesort *me);
        i64 (*rows)(const struct flintdb_filesort *me);
        i64 (*add)(struct flintdb_filesort *me, struct flintdb_row *r, char **e);
        struct flintdb_row * (*read)(const struct flintdb_filesort *me, i64 i, char **e);
        i64 (*sort)(struct flintdb_filesort *me,
                    int (*cmpr)(const void *obj, const struct flintdb_row *a, const struct flintdb_row *b),
                    const void *ctx,
                    char **e);
        void *priv;
    };

    struct flintdb_aggregate_groupkey;

    struct flintdb_aggregate {
        void *priv;
        void (*free)(struct flintdb_aggregate *agg);
        void (*row)(struct flintdb_aggregate *agg, const struct flintdb_row *r, char **e);
        int (*compute)(struct flintdb_aggregate *agg, struct flintdb_row ***out_rows, char **e);
    };

    struct flintdb_aggregate_groupby {
        void *priv;
        void (*free)(struct flintdb_aggregate_groupby *gb);
        const char * (*alias)(const struct flintdb_aggregate_groupby *gb);
        const char * (*column)(const struct flintdb_aggregate_groupby *gb);
        enum flintdb_variant_type (*type)(const struct flintdb_aggregate_groupby *gb);
        struct flintdb_variant * (*get)(const struct flintdb_aggregate_groupby *gb, const struct flintdb_row *r, char **e);
    };

    struct flintdb_aggregate_condition {
        i8 (*ok)(const struct flintdb_aggregate_condition *cond, const struct flintdb_row *r, char **e);
    };

    struct flintdb_aggregate_func {
        void *priv;
        void (*free)(struct flintdb_aggregate_func *f);
        const char * (*name)(const struct flintdb_aggregate_func *f);
        const char * (*alias)(const struct flintdb_aggregate_func *f);
        enum flintdb_variant_type (*type)(const struct flintdb_aggregate_func *f);
        int (*precision)(const struct flintdb_aggregate_func *f);
        const struct flintdb_aggregate_condition * (*condition)(const struct flintdb_aggregate_func *f);
        void (*row)(struct flintdb_aggregate_func *f, const struct flintdb_aggregate_groupkey *gk, const struct flintdb_row *r, char **e);
        void (*compute)(struct flintdb_aggregate_func *f, const struct flintdb_aggregate_groupkey *gk, char **e);
        const struct flintdb_variant * (*result)(const struct flintdb_aggregate_func *f, const struct flintdb_aggregate_groupkey *gk, char **e);
    };

    struct flintdb_sql_result {
        i64 affected;
        char **column_names;
        int column_count;
        struct flintdb_cursor_row *row_cursor;
        void *transaction;
        void (*close)(struct flintdb_sql_result *me);
    };

    // API functions
    struct flintdb_meta* flintdb_meta_new_ptr(const char *name, char **e);
    struct flintdb_meta* flintdb_meta_open_ptr(const char *filename, char **e);
    void flintdb_meta_free_ptr(struct flintdb_meta *m);

    int flintdb_meta_to_sql_string(const struct flintdb_meta *m, char *s, i32 len, char **e);
    void flintdb_meta_columns_add(struct flintdb_meta *m, const char *name, enum flintdb_variant_type type,
                                 i32 bytes, i16 precision, enum flintdb_null_spec nullspec,
                                 const char *value, const char *comment, char **e);
    void flintdb_meta_indexes_add(struct flintdb_meta *m, const char *name, const char *algorithm,
                                 const char keys[][40], u16 key_count, char **e);
    int flintdb_column_at(struct flintdb_meta *m, const char *name);

    struct flintdb_row * flintdb_row_new(struct flintdb_meta *meta, char **e);
    void flintdb_print_row(const struct flintdb_row *r);

    struct flintdb_table * flintdb_table_open(const char *file, int mode, const struct flintdb_meta *meta, char **e);
    int flintdb_table_drop(const char *file, char **e);

    struct flintdb_genericfile * flintdb_genericfile_open(const char *file, int mode, const struct flintdb_meta *meta, char **e);
    void flintdb_genericfile_drop(const char *file, char **e);

    struct flintdb_filesort * flintdb_filesort_new(const char *file, const struct flintdb_meta *m, char **e);

    struct flintdb_aggregate* aggregate_new(const char *id, struct flintdb_aggregate_groupby **groupby, u16 groupby_count,
                                           struct flintdb_aggregate_func **funcs, u16 func_count, char **e);
    struct flintdb_aggregate_groupby* groupby_new(const char *alias, const char *column, enum flintdb_variant_type type, char **e);

    struct flintdb_aggregate_func * flintdb_func_count(const char *name, const char *alias, enum flintdb_variant_type type,
                                                      struct flintdb_aggregate_condition cond, char **e);
    struct flintdb_aggregate_func * flintdb_func_sum(const char *name, const char *alias, enum flintdb_variant_type type,
                                                    struct flintdb_aggregate_condition cond, char **e);
    struct flintdb_aggregate_func * flintdb_func_avg(const char *name, const char *alias, enum flintdb_variant_type type,
                                                    struct flintdb_aggregate_condition cond, char **e);

    struct flintdb_sql_result* flintdb_sql_exec(const char *sql, const void *transaction, char **e);
    """
)


# libc malloc/free for ownership-transfer cases (e.g., aggregate_new consumes arrays)
ffi.cdef("void *malloc(size_t size); void free(void *ptr);")


class FlintDBError(Exception):
    pass


def _load_library():
    lib_paths = [
        "../../lib/libflintdb.so",
        "../../lib/libflintdb.dylib",
        "../../lib/libflintdb.dll",
        "../../bin/flintdb",
    ]
    base = os.path.dirname(__file__)
    for rel in lib_paths:
        full = os.path.join(base, rel)
        if os.path.exists(full):
            return ffi.dlopen(full)
    raise RuntimeError("Could not find FlintDB shared library")


_lib = _load_library()
_libc = ffi.dlopen(None)


# Constants (match tutorial expectations)
FLINTDB_RDONLY = os.O_RDONLY
FLINTDB_RDWR = os.O_RDWR | os.O_CREAT

VARIANT_INT32 = 2
VARIANT_INT64 = 8
VARIANT_DOUBLE = 9
VARIANT_STRING = 11
VARIANT_DECIMAL = 12

SPEC_NOT_NULL = 1
SPEC_PRIMARY_KEY = 2

MAX_COLUMN_NAME_LIMIT = 40
PRIMARY_NAME = b"primary"


def _err_ptr():
    return ffi.new("char **")


def _raise_if_err(err) -> None:
    if err[0] != ffi.NULL:
        msg = ffi.string(err[0]).decode("utf-8", "replace")
        raise FlintDBError(msg)


def _to_cstr(s: Union[str, bytes]) -> bytes:
    return s.encode() if isinstance(s, str) else s


class Meta:
    def __init__(self, filepath: str):
        err = _err_ptr()
        self._path = _to_cstr(filepath)
        self._m = _lib.flintdb_meta_new_ptr(self._path, err)
        _raise_if_err(err)
        if self._m == ffi.NULL:
            raise FlintDBError("meta_new_ptr returned NULL")

    def add_column(
        self,
        name: str,
        variant_type: int,
        size: int = 0,
        precision: int = 0,
        spec: int = 0,
        default: str = "",
        comment: str = "",
    ) -> None:
        err = _err_ptr()
        _lib.flintdb_meta_columns_add(
            self._m,
            _to_cstr(name),
            variant_type,
            int(size),
            int(precision),
            int(spec),
            _to_cstr(default),
            _to_cstr(comment),
            err,
        )
        _raise_if_err(err)

    def add_index(self, name: str, algorithm: Optional[str], keys: Sequence[str]) -> None:
        err = _err_ptr()
        key_count = len(keys)
        keys_arr = ffi.new(f"char[{key_count}][{MAX_COLUMN_NAME_LIMIT}]")
        for i, k in enumerate(keys):
            kb = _to_cstr(k)
            n = min(len(kb), MAX_COLUMN_NAME_LIMIT - 1)
            ffi.memmove(keys_arr[i], kb, n)


        algo_b = ffi.NULL if algorithm is None else _to_cstr(algorithm)
        _lib.flintdb_meta_indexes_add(self._m, _to_cstr(name), algo_b, keys_arr, key_count, err)
        _raise_if_err(err)

    def to_sql_string(self) -> str:
        err = _err_ptr()
        buf = ffi.new("char[]", 8192)
        rc = _lib.flintdb_meta_to_sql_string(self._m, buf, 8192, err)
        _raise_if_err(err)
        if rc < 0:
            raise FlintDBError("flintdb_meta_to_sql_string failed")
        return ffi.string(buf).decode("utf-8", "replace")

    def column_at(self, name: str) -> int:
        return int(_lib.flintdb_column_at(self._m, _to_cstr(name)))

    def close(self) -> None:
        if getattr(self, "_m", ffi.NULL) != ffi.NULL:
            _lib.flintdb_meta_free_ptr(self._m)
            self._m = ffi.NULL

    def __enter__(self) -> "Meta":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class Row:
    def __init__(self, meta: Meta, *, _ptr=None, _owned: bool = True):
        self._owned = _owned
        if _ptr is not None:
            self._r = _ptr
            return

        err = _err_ptr()
        self._r = _lib.flintdb_row_new(meta._m, err)
        _raise_if_err(err)
        if self._r == ffi.NULL:
            raise FlintDBError("row_new returned NULL")

    @staticmethod
    def borrowed(ptr) -> "Row":
        return Row.__new_from_ptr(ptr, owned=False)

    @staticmethod
    def owned(ptr) -> "Row":
        return Row.__new_from_ptr(ptr, owned=True)

    @staticmethod
    def __new_from_ptr(ptr, owned: bool) -> "Row":
        obj = Row.__new__(Row)
        Row.__init__(obj, meta=None, _ptr=ptr, _owned=owned)  # type: ignore[arg-type]
        return obj

    def _ptr(self):
        return self._r

    def set_string(self, idx: int, value: str) -> None:
        err = _err_ptr()
        self._r.string_set(self._r, idx, _to_cstr(value), err)
        _raise_if_err(err)

    def set_i32(self, idx: int, value: int) -> None:
        err = _err_ptr()
        self._r.i32_set(self._r, idx, int(value), err)
        _raise_if_err(err)

    def set_i64(self, idx: int, value: int) -> None:
        err = _err_ptr()
        self._r.i64_set(self._r, idx, int(value), err)
        _raise_if_err(err)

    def set_f64(self, idx: int, value: float) -> None:
        err = _err_ptr()
        self._r.f64_set(self._r, idx, float(value), err)
        _raise_if_err(err)

    def get_i32(self, idx: int) -> int:
        err = _err_ptr()
        v = self._r.i32_get(self._r, idx, err)
        _raise_if_err(err)
        return int(v)

    def validate(self) -> bool:
        err = _err_ptr()
        ok = self._r.validate(self._r, err)
        _raise_if_err(err)
        return bool(ok)

    def close(self) -> None:
        if getattr(self, "_r", ffi.NULL) != ffi.NULL and self._owned:
            self._r.free(self._r)
        self._r = ffi.NULL

    def __enter__(self) -> "Row":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class CursorI64:
    def __init__(self, cptr):
        self._c = cptr

    def next(self) -> int:
        err = _err_ptr()
        v = self._c.next(self._c, err)
        _raise_if_err(err)
        return int(v)

    def close(self) -> None:
        if getattr(self, "_c", ffi.NULL) != ffi.NULL:
            self._c.close(self._c)
            self._c = ffi.NULL

    def __iter__(self):
        return self

    def __next__(self) -> int:
        v = self.next()
        if v < 0:
            raise StopIteration
        return v

    def __enter__(self) -> "CursorI64":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class CursorRow:
    def __init__(self, cptr):
        self._c = cptr

    def next(self) -> Optional[Row]:
        err = _err_ptr()
        r = self._c.next(self._c, err)
        _raise_if_err(err)
        if r == ffi.NULL:
            return None
        return Row.borrowed(r)

    def close(self) -> None:
        if getattr(self, "_c", ffi.NULL) != ffi.NULL:
            self._c.close(self._c)
            self._c = ffi.NULL

    def __iter__(self):
        return self

    def __next__(self) -> Row:
        r = self.next()
        if r is None:
            raise StopIteration
        return r

    def __enter__(self) -> "CursorRow":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class Table:
    def __init__(self, filepath: str, mode: int, meta: Optional[Meta] = None):
        err = _err_ptr()
        meta_ptr = ffi.NULL if meta is None else meta._m
        self._t = _lib.flintdb_table_open(_to_cstr(filepath), mode, meta_ptr, err)
        _raise_if_err(err)
        if self._t == ffi.NULL:
            raise FlintDBError("table_open returned NULL")

    def apply(self, row: Row, check_dup: bool = False) -> int:
        err = _err_ptr()
        rowid = self._t.apply(self._t, row._ptr(), 1 if check_dup else 0, err)
        _raise_if_err(err)
        if rowid < 0:
            raise FlintDBError("apply returned < 0")
        return int(rowid)

    def delete_at(self, rowid: int) -> int:
        err = _err_ptr()
        r = self._t.delete_at(self._t, int(rowid), err)
        _raise_if_err(err)
        return int(r)

    def find(self, where: str = "") -> CursorI64:
        err = _err_ptr()
        c = self._t.find(self._t, _to_cstr(where), err)
        _raise_if_err(err)
        if c == ffi.NULL:
            raise FlintDBError("find returned NULL")
        return CursorI64(c)

    def read(self, rowid: int) -> Row:
        err = _err_ptr()
        r = self._t.read(self._t, int(rowid), err)
        _raise_if_err(err)
        if r == ffi.NULL:
            raise FlintDBError("read returned NULL")
        return Row.borrowed(ffi.cast("struct flintdb_row *", r))

    def close(self) -> None:
        if getattr(self, "_t", ffi.NULL) != ffi.NULL:
            self._t.close(self._t)
            self._t = ffi.NULL

    @staticmethod
    def drop(filepath: str) -> None:
        _lib.flintdb_table_drop(_to_cstr(filepath), ffi.NULL)

    def __enter__(self) -> "Table":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class GenericFile:
    def __init__(self, filepath: str, mode: int, meta: Optional[Meta] = None):
        err = _err_ptr()
        meta_ptr = ffi.NULL if meta is None else meta._m
        self._g = _lib.flintdb_genericfile_open(_to_cstr(filepath), mode, meta_ptr, err)
        _raise_if_err(err)
        if self._g == ffi.NULL:
            raise FlintDBError("genericfile_open returned NULL")

    def write(self, row: Row) -> int:
        err = _err_ptr()
        n = self._g.write(self._g, row._ptr(), err)
        _raise_if_err(err)
        return int(n)

    def find(self, where: str = "") -> CursorRow:
        err = _err_ptr()
        c = self._g.find(self._g, _to_cstr(where), err)
        _raise_if_err(err)
        if c == ffi.NULL:
            raise FlintDBError("genericfile.find returned NULL")
        return CursorRow(c)

    def close(self) -> None:
        if getattr(self, "_g", ffi.NULL) != ffi.NULL:
            self._g.close(self._g)
            self._g = ffi.NULL

    @staticmethod
    def drop(filepath: str) -> None:
        _lib.flintdb_genericfile_drop(_to_cstr(filepath), ffi.NULL)

    def __enter__(self) -> "GenericFile":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class FileSort:
    def __init__(self, filepath: str, meta: Meta):
        err = _err_ptr()
        self._fs = _lib.flintdb_filesort_new(_to_cstr(filepath), meta._m, err)
        _raise_if_err(err)
        if self._fs == ffi.NULL:
            raise FlintDBError("filesort_new returned NULL")

    def add(self, row: Row) -> int:
        err = _err_ptr()
        n = self._fs.add(self._fs, row._ptr(), err)
        _raise_if_err(err)
        return int(n)

    def rows(self) -> int:
        return int(self._fs.rows(self._fs))

    def read(self, i: int) -> Row:
        err = _err_ptr()
        r = self._fs.read(self._fs, int(i), err)
        _raise_if_err(err)
        if r == ffi.NULL:
            raise FlintDBError("filesort.read returned NULL")
        return Row.owned(r)

    def sort(self, compare: Callable[[object, Row, Row], int], ctx: object = None) -> int:
        err = _err_ptr()

        @ffi.callback("int(const void *, const struct flintdb_row *, const struct flintdb_row *)")
        def _cmpr(_ctx, a, b):
            ra = Row.borrowed(ffi.cast("struct flintdb_row *", a))
            rb = Row.borrowed(ffi.cast("struct flintdb_row *", b))
            return int(compare(ctx, ra, rb))

        n = self._fs.sort(self._fs, _cmpr, ffi.NULL, err)
        _raise_if_err(err)
        return int(n)

    def close(self) -> None:
        if getattr(self, "_fs", ffi.NULL) != ffi.NULL:
            self._fs.close(self._fs)
            self._fs = ffi.NULL

    def __enter__(self) -> "FileSort":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class GroupBy:
    def __init__(self, ptr, owned: bool = True):
        self._gb = ptr
        self._owned = owned

    def close(self) -> None:
        if self._owned and getattr(self, "_gb", ffi.NULL) != ffi.NULL:
            self._gb.free(self._gb)
            self._gb = ffi.NULL


class AggFunc:
    def __init__(self, ptr, owned: bool = True):
        self._f = ptr
        self._owned = owned

    def close(self) -> None:
        if self._owned and getattr(self, "_f", ffi.NULL) != ffi.NULL:
            self._f.free(self._f)
            self._f = ffi.NULL


def groupby_new(alias: str, column: str, variant_type: int) -> GroupBy:
    err = _err_ptr()
    gb = _lib.groupby_new(_to_cstr(alias), _to_cstr(column), variant_type, err)
    _raise_if_err(err)
    if gb == ffi.NULL:
        raise FlintDBError("groupby_new returned NULL")
    return GroupBy(gb, owned=True)


def _empty_cond():
    c = ffi.new("struct flintdb_aggregate_condition *")
    c.ok = ffi.NULL
    return c[0]


def func_count(name: str, alias: str, variant_type: int) -> AggFunc:
    err = _err_ptr()
    f = _lib.flintdb_func_count(_to_cstr(name), _to_cstr(alias), variant_type, _empty_cond(), err)
    _raise_if_err(err)
    if f == ffi.NULL:
        raise FlintDBError("func_count returned NULL")
    return AggFunc(f, owned=True)


def func_sum(name: str, alias: str, variant_type: int) -> AggFunc:
    err = _err_ptr()
    f = _lib.flintdb_func_sum(_to_cstr(name), _to_cstr(alias), variant_type, _empty_cond(), err)
    _raise_if_err(err)
    if f == ffi.NULL:
        raise FlintDBError("func_sum returned NULL")
    return AggFunc(f, owned=True)


def func_avg(name: str, alias: str, variant_type: int) -> AggFunc:
    err = _err_ptr()
    f = _lib.flintdb_func_avg(_to_cstr(name), _to_cstr(alias), variant_type, _empty_cond(), err)
    _raise_if_err(err)
    if f == ffi.NULL:
        raise FlintDBError("func_avg returned NULL")
    return AggFunc(f, owned=True)


class Aggregate:
    def __init__(self, ptr):
        self._agg = ptr

    @staticmethod
    def build(agg_id: str, groupbys: Sequence[GroupBy], funcs: Sequence[AggFunc]) -> "Aggregate":
        err = _err_ptr()

        gb_count = len(groupbys)
        fn_count = len(funcs)

        # Allocate pointer arrays with libc malloc because aggregate_new takes ownership and frees them.
        gb_mem = _libc.malloc(ffi.sizeof("struct flintdb_aggregate_groupby *") * gb_count) if gb_count else ffi.NULL
        fn_mem = _libc.malloc(ffi.sizeof("struct flintdb_aggregate_func *") * fn_count) if fn_count else ffi.NULL

        gb_arr = ffi.cast("struct flintdb_aggregate_groupby **", gb_mem) if gb_count else ffi.NULL
        fn_arr = ffi.cast("struct flintdb_aggregate_func **", fn_mem) if fn_count else ffi.NULL

        for i, gb in enumerate(groupbys):
            gb_arr[i] = gb._gb
            gb._owned = False  # ownership transferred to Aggregate

        for i, f in enumerate(funcs):
            fn_arr[i] = f._f
            f._owned = False  # ownership transferred to Aggregate

        agg = _lib.aggregate_new(_to_cstr(agg_id), gb_arr, gb_count, fn_arr, fn_count, err)
        _raise_if_err(err)
        if agg == ffi.NULL:
            raise FlintDBError("aggregate_new returned NULL")
        return Aggregate(agg)

    def row(self, r: Union[Row, "ffi.CData"]) -> None:
        err = _err_ptr()
        ptr = r._ptr() if isinstance(r, Row) else r
        self._agg.row(self._agg, ptr, err)
        _raise_if_err(err)

    def compute(self) -> List[Row]:
        err = _err_ptr()
        out = ffi.new("struct flintdb_row ***")
        n = self._agg.compute(self._agg, out, err)
        _raise_if_err(err)
        if n <= 0 or out[0] == ffi.NULL:
            return []
        rows = out[0]
        result: List[Row] = []
        for i in range(int(n)):
            if rows[i] != ffi.NULL:
                result.append(Row.owned(rows[i]))
        # Best-effort free for the array returned by CALLOC.
        _libc.free(rows)
        return result

    def close(self) -> None:
        if getattr(self, "_agg", ffi.NULL) != ffi.NULL:
            self._agg.free(self._agg)
            self._agg = ffi.NULL

    def __enter__(self) -> "Aggregate":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class SqlResult:
    def __init__(self, ptr):
        self._res = ptr

    @property
    def affected(self) -> int:
        return int(self._res.affected)

    @property
    def column_names(self) -> List[str]:
        names: List[str] = []
        for i in range(int(self._res.column_count)):
            p = self._res.column_names[i]
            if p != ffi.NULL:
                names.append(ffi.string(p).decode("utf-8", "replace"))
        return names

    def iter_rows(self) -> Iterable[Row]:
        c = self._res.row_cursor
        if c == ffi.NULL:
            return []
        return CursorRow(c)

    def close(self) -> None:
        if getattr(self, "_res", ffi.NULL) != ffi.NULL:
            self._res.close(self._res)
            self._res = ffi.NULL

    def __enter__(self) -> "SqlResult":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def sql_exec(sql: str) -> SqlResult:
    err = _err_ptr()
    res = _lib.flintdb_sql_exec(_to_cstr(sql), ffi.NULL, err)
    _raise_if_err(err)
    if res == ffi.NULL:
        raise FlintDBError("sql_exec returned NULL")
    return SqlResult(res)


def print_row(r: Union[Row, "ffi.CData"]) -> None:
    ptr = r._ptr() if isinstance(r, Row) else r
    _lib.flintdb_print_row(ptr)
