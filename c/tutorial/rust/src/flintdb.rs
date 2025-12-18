#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

include!("bindings.rs");

use std::ffi::{CStr, CString};
use std::ptr;

// Type aliases for shorter names
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_NULL as VAR_NULL;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_ZERO as VAR_ZERO;
pub use flintdb_variant_type_VARIANT_INT32 as VAR_INT32;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_UINT32 as VAR_UINT32;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_INT8 as VAR_INT8;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_UINT8 as VAR_UINT8;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_INT16 as VAR_INT16;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_UINT16 as VAR_UINT16;
pub use flintdb_variant_type_VARIANT_INT64 as VAR_INT64;
pub use flintdb_variant_type_VARIANT_DOUBLE as VAR_DOUBLE;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_FLOAT as VAR_FLOAT;
pub use flintdb_variant_type_VARIANT_STRING as VAR_STRING;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_DECIMAL as VAR_DECIMAL;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_BYTES as VAR_BYTES;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_DATE as VAR_DATE;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_TIME as VAR_TIME;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_UUID as VAR_UUID;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_IPV6 as VAR_IPV6;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_BLOB as VAR_BLOB;
#[allow(unused_imports)]
pub use flintdb_variant_type_VARIANT_OBJECT as VAR_OBJECT;

// Open mode aliases
pub use flintdb_open_mode_FLINTDB_RDONLY as RDONLY;
pub use flintdb_open_mode_FLINTDB_RDWR as RDWR;

// Safe Rust wrappers
pub struct Meta {
    inner: flintdb_meta,
}

impl Meta {
    pub fn new(flintdb_tablename: &str) -> Result<Self, String> {
        let c_flintdb_tablename = CString::new(flintdb_tablename).unwrap();
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let inner = unsafe { flintdb_meta_new(c_flintdb_tablename.as_ptr(), &mut err) };
        check_error(err)?;
        Ok(Meta { inner })
    }

    pub fn add_column(
        &mut self,
        name: &str,
        vtype: flintdb_variant_type,
        size: i32,
        scale: i16,
        nullspec: u32,
        default_value: &str,
        desc: &str,
    ) -> Result<(), String> {
        let c_name = CString::new(name).unwrap();
        let c_default = CString::new(default_value).unwrap();
        let c_desc = CString::new(desc).unwrap();
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        unsafe {
            flintdb_meta_columns_add(
                &mut self.inner,
                c_name.as_ptr(),
                vtype,
                size,
                scale,
                nullspec,
                c_default.as_ptr(),
                c_desc.as_ptr(),
                &mut err,
            );
        }
        check_error(err)
    }

    pub fn add_index(&mut self, name: &str, keys: &[&str]) -> Result<(), String> {
        let c_name = CString::new(name).unwrap();
        let mut c_keys: Vec<[::std::os::raw::c_char; 40]> = Vec::new();
        for key in keys {
            let mut arr = [0 as ::std::os::raw::c_char; 40];
            let c_key = CString::new(*key).unwrap();
            let bytes = c_key.as_bytes_with_nul();
            let len = bytes.len().min(40);
            arr[..len].copy_from_slice(unsafe { std::mem::transmute(&bytes[..len]) });
            c_keys.push(arr);
        }
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        unsafe {
            flintdb_meta_indexes_add(
                &mut self.inner,
                c_name.as_ptr(),
                ptr::null(),
                c_keys.as_ptr(),
                keys.len() as u16,
                &mut err,
            );
        }
        check_error(err)
    }

    pub fn to_sql_string(&self) -> Result<String, String> {
        let mut buffer = vec![0u8; 2048];
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        unsafe {
            flintdb_meta_to_sql_string(
                &self.inner,
                buffer.as_mut_ptr() as *mut ::std::os::raw::c_char,
                buffer.len() as i32,
                &mut err,
            );
        }
        check_error(err)?;
        let sql = unsafe { CStr::from_ptr(buffer.as_ptr() as *const ::std::os::raw::c_char) };
        Ok(sql.to_string_lossy().into_owned())
    }

    pub fn as_ptr(&self) -> *const flintdb_meta {
        &self.inner as *const flintdb_meta
    }

    pub fn as_mut_ptr(&mut self) -> *mut flintdb_meta {
        &mut self.inner as *mut flintdb_meta
    }

    fn as_mut_ptr_for_row(&mut self) -> *mut flintdb_meta {
        &mut self.inner as *mut flintdb_meta
    }
}

impl Drop for Meta {
    fn drop(&mut self) {
        unsafe { flintdb_meta_close(&mut self.inner) };
    }
}

pub struct Table {
    ptr: *mut flintdb_table,
}

impl Table {
    pub fn open(flintdb_tablename: &str, mode: flintdb_open_mode, mt: Option<&Meta>) -> Result<Self, String> {
        let c_flintdb_tablename = CString::new(flintdb_tablename).unwrap();
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let flintdb_meta_ptr = mt.map_or(ptr::null(), |m| m.as_ptr());
        let ptr = unsafe { flintdb_table_open(c_flintdb_tablename.as_ptr(), mode, flintdb_meta_ptr, &mut err) };
        check_error(err)?;
        if ptr.is_null() {
            return Err("Failed to open table".to_string());
        }
        Ok(Table { ptr })
    }

    pub fn drop_table(flintdb_tablename: &str) -> Result<(), String> {
        let c_flintdb_tablename = CString::new(flintdb_tablename).unwrap();
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        unsafe { flintdb_table_drop(c_flintdb_tablename.as_ptr(), &mut err) };
        check_error(err)
    }

    pub fn apply(&mut self, row: &mut Row) -> Result<i64, String> {
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let tbl = unsafe { &mut *self.ptr };
        let rowid = unsafe {
            if let Some(apply_fn) = tbl.apply {
                apply_fn(self.ptr, row.ptr, 0, &mut err)
            } else {
                return Err("apply function not available".to_string());
            }
        };
        check_error(err)?;
        Ok(rowid)
    }

    pub fn read(&self, rowid: i64) -> Result<Row, String> {
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let tbl = unsafe { &*self.ptr };
        let row_ptr = unsafe {
            if let Some(read_fn) = tbl.read {
                read_fn(self.ptr, rowid, &mut err)
            } else {
                return Err("read function not available".to_string());
            }
        };
        check_error(err)?;
        if row_ptr.is_null() {
            return Err("Failed to read row".to_string());
        }
        // read() returns a borrowed row owned by the table
        Ok(unsafe { Row::borrowed(row_ptr as *mut flintdb_row) })
    }

    pub fn find(&mut self, where_clause: &str) -> Result<CursorI64, String> {
        let c_where = CString::new(where_clause).unwrap();
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let tbl = unsafe { &mut *self.ptr };
        let ptr = unsafe {
            if let Some(find_fn) = tbl.find {
                find_fn(self.ptr, c_where.as_ptr(), &mut err)
            } else {
                return Err("find function not available".to_string());
            }
        };
        check_error(err)?;
        if ptr.is_null() {
            return Err("Failed to create cursor".to_string());
        }
        Ok(CursorI64 { ptr })
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        unsafe {
            if let Some(close_fn) = (*self.ptr).close {
                close_fn(self.ptr);
            }
        }
    }
}

pub struct GenericFile {
    ptr: *mut flintdb_genericfile,
}

impl GenericFile {
    pub fn open(filepath: &str, mode: flintdb_open_mode, mt: Option<&Meta>) -> Result<Self, String> {
        let c_filepath = CString::new(filepath).unwrap();
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let flintdb_meta_ptr = mt.map_or(ptr::null(), |m| m.as_ptr());
        let ptr = unsafe { flintdb_genericfile_open(c_filepath.as_ptr(), mode, flintdb_meta_ptr, &mut err) };
        check_error(err)?;
        if ptr.is_null() {
            return Err("Failed to open file".to_string());
        }
        Ok(GenericFile { ptr })
    }

    pub fn drop_file(filepath: &str) -> Result<(), String> {
        let c_filepath = CString::new(filepath).unwrap();
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        unsafe { flintdb_genericfile_drop(c_filepath.as_ptr(), &mut err) };
        check_error(err)
    }

    pub fn write(&mut self, row: &mut Row) -> Result<(), String> {
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let f = unsafe { &mut *self.ptr };
        let result = unsafe {
            if let Some(write_fn) = f.write {
                write_fn(self.ptr, row.ptr, &mut err)
            } else {
                return Err("write function not available".to_string());
            }
        };
        check_error(err)?;
        if result != 0 {
            return Err("Failed to write row".to_string());
        }
        Ok(())
    }

    pub fn find(&mut self, where_clause: &str) -> Result<CursorRow, String> {
        let c_where = CString::new(where_clause).unwrap();
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let f = unsafe { &mut *self.ptr };
        let ptr = unsafe {
            if let Some(find_fn) = f.find {
                find_fn(self.ptr, c_where.as_ptr(), &mut err)
            } else {
                return Err("find function not available".to_string());
            }
        };
        check_error(err)?;
        if ptr.is_null() {
            return Err("Failed to create cursor".to_string());
        }
        Ok(CursorRow { ptr })
    }
}

impl Drop for GenericFile {
    fn drop(&mut self) {
        unsafe {
            if let Some(close_fn) = (*self.ptr).close {
                close_fn(self.ptr);
            }
        }
    }
}

pub struct Row {
    pub ptr: *mut flintdb_row,
    owned: bool,  // true if we own the row and should free it
}

impl Row {
    pub fn new(mt: &mut Meta) -> Result<Self, String> {
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let ptr = unsafe { flintdb_row_new(mt.as_mut_ptr(), &mut err) };
        check_error(err)?;
        if ptr.is_null() {
            return Err("Failed to create row".to_string());
        }
        Ok(Row { ptr, owned: true })
    }
    
    // Create a borrowed row (not owned, won't be freed)
    unsafe fn borrowed(ptr: *mut flintdb_row) -> Self {
        Row { ptr, owned: false }
    }

    pub fn set_i32(&mut self, col: u16, value: i32) -> Result<(), String> {
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        unsafe {
            if let Some(set_fn) = (*self.ptr).i32_set {
                set_fn(self.ptr, col, value, &mut err);
            } else {
                return Err("i32_set function not available".to_string());
            }
        }
        check_error(err)
    }

    pub fn set_i64(&mut self, col: u16, value: i64) -> Result<(), String> {
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        unsafe {
            if let Some(set_fn) = (*self.ptr).i64_set {
                set_fn(self.ptr, col, value, &mut err);
            } else {
                return Err("i64_set function not available".to_string());
            }
        }
        check_error(err)
    }

    pub fn set_f64(&mut self, col: u16, value: f64) -> Result<(), String> {
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        unsafe {
            if let Some(set_fn) = (*self.ptr).f64_set {
                set_fn(self.ptr, col, value, &mut err);
            } else {
                return Err("f64_set function not available".to_string());
            }
        }
        check_error(err)
    }

    pub fn set_string(&mut self, col: u16, value: &str) -> Result<(), String> {
        let c_value = CString::new(value).unwrap();
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        unsafe {
            if let Some(set_fn) = (*self.ptr).string_set {
                set_fn(self.ptr, col, c_value.as_ptr(), &mut err);
            } else {
                return Err("string_set function not available".to_string());
            }
        }
        check_error(err)
    }
}

impl Drop for Row {
    fn drop(&mut self) {
        // Only free if we own the row
        if self.owned {
            unsafe {
                if let Some(free_fn) = (*self.ptr).free {
                    free_fn(self.ptr);
                }
            }
        }
    }
}

pub struct CursorI64 {
    ptr: *mut flintdb_cursor_i64,
}

impl CursorI64 {
    pub fn next(&mut self) -> Result<Option<i64>, String> {
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let cursor = unsafe { &mut *self.ptr };
        let rowid = unsafe {
            if let Some(next_fn) = cursor.next {
                next_fn(self.ptr, &mut err)
            } else {
                return Err("next function not available".to_string());
            }
        };
        check_error(err)?;
        if rowid == -1 {
            Ok(None)
        } else {
            Ok(Some(rowid))
        }
    }
}

impl Drop for CursorI64 {
    fn drop(&mut self) {
        unsafe {
            if let Some(close_fn) = (*self.ptr).close {
                close_fn(self.ptr);
            }
        }
    }
}

pub struct CursorRow {
    ptr: *mut flintdb_cursor_row,
}

impl CursorRow {
    pub fn next(&mut self) -> Result<Option<Row>, String> {
        let mut err: *mut ::std::os::raw::c_char = ptr::null_mut();
        let cursor = unsafe { &mut *self.ptr };
        let row_ptr = unsafe {
            if let Some(next_fn) = cursor.next {
                next_fn(self.ptr, &mut err)
            } else {
                return Err("next function not available".to_string());
            }
        };
        check_error(err)?;
        if row_ptr.is_null() {
            Ok(None)
        } else {
            // Return borrowed row - cursor owns it, don't free
            Ok(Some(unsafe { Row::borrowed(row_ptr) }))
        }
    }
}

impl Drop for CursorRow {
    fn drop(&mut self) {
        unsafe {
            if let Some(close_fn) = (*self.ptr).close {
                close_fn(self.ptr);
            }
        }
    }
}

// Utility functions
pub fn print_row_safe(row: *const flintdb_row) {
    unsafe { flintdb_print_row(row) };
}

pub fn get_column_index(mt: &mut Meta, name: &str) -> u16 {
    let c_name = CString::new(name).unwrap();
    unsafe { flintdb_column_at(mt.as_mut_ptr(), c_name.as_ptr()) as u16 }
}

fn check_error(err: *mut ::std::os::raw::c_char) -> Result<(), String> {
    if !err.is_null() {
        let c_str = unsafe { CStr::from_ptr(err) };
        let error_msg = c_str.to_string_lossy().into_owned();
        Err(error_msg)
    } else {
        Ok(())
    }
}

/// Cleanup all FlintDB resources
pub fn cleanup() {
    unsafe { flintdb_cleanup(ptr::null_mut()) };
}
