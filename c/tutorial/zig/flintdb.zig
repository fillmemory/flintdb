/// FlintDB Zig Wrapper
const std = @import("std");
const c = @cImport({
    @cInclude("flintdb.h");
});

pub const Error = error{
    FlintDBError,
    TableNotOpen,
    RowNotCreated,
    CursorNotCreated,
};

/// Helper to check C API errors
fn checkError(e: ?*u8) Error!void {
    if (e) |msg| {
        const cstr: [*:0]const u8 = @ptrCast(msg);
        const slice: []const u8 = std.mem.span(cstr);
        std.debug.print("FlintDB Error: {s}\n", .{slice});
        return Error.FlintDBError;
    }
}

/// Table metadata builder
pub const Meta = struct {
    inner: c.struct_flintdb_meta,

    pub fn init(path: []const u8) Error!Meta {
        var e: ?*u8 = null;
        var path_buf: [256]u8 = undefined;
        @memcpy(path_buf[0..path.len], path);
        path_buf[path.len] = 0;
        const path_z: [*:0]const u8 = @ptrCast(&path_buf);

        const meta = c.flintdb_meta_new(path_z, &e);
        try checkError(e);

        return .{ .inner = meta };
    }

    pub fn deinit(self: *Meta) void {
        c.flintdb_meta_close(&self.inner);
    }

    pub fn addColumn(
        self: *Meta,
        name: []const u8,
        variant_type: c_int,
        size: usize,
        precision: i16,
        nullspec: u32,
        default: []const u8,
        comment: []const u8,
    ) Error!void {
        var e: ?*u8 = null;
        var name_buf: [c.MAX_COLUMN_NAME_LIMIT]u8 = undefined;
        var default_buf: [256]u8 = undefined;
        var comment_buf: [256]u8 = undefined;

        @memcpy(name_buf[0..name.len], name);
        name_buf[name.len] = 0;
        @memcpy(default_buf[0..default.len], default);
        default_buf[default.len] = 0;
        @memcpy(comment_buf[0..comment.len], comment);
        comment_buf[comment.len] = 0;

        c.flintdb_meta_columns_add(
            &self.inner,
            @ptrCast(&name_buf),
            @intCast(variant_type),
            @intCast(size),
            precision,
            nullspec,
            @ptrCast(&default_buf),
            @ptrCast(&comment_buf),
            &e,
        );
        try checkError(e);
    }

    pub fn addIndex(self: *Meta, name: []const u8, columns: []const []const u8) Error!void {
        var e: ?*u8 = null;
        var name_buf: [c.MAX_COLUMN_NAME_LIMIT]u8 = undefined;
        @memcpy(name_buf[0..name.len], name);
        name_buf[name.len] = 0;

        // Build column key array
        var keys: [8][c.MAX_COLUMN_NAME_LIMIT]u8 = undefined;
        for (columns, 0..) |col, i| {
            @memset(keys[i][0..c.MAX_COLUMN_NAME_LIMIT], 0);
            @memcpy(keys[i][0..col.len], col);
            keys[i][col.len] = 0;
        }

        c.flintdb_meta_indexes_add(
            &self.inner,
            @ptrCast(&name_buf),
            null,
            &keys,
            @intCast(columns.len),
            &e,
        );
        try checkError(e);
    }

    pub fn toSQL(self: *Meta, buf: []u8) Error![]const u8 {
        var e: ?*u8 = null;
        if (c.flintdb_meta_to_sql_string(&self.inner, buf.ptr, @intCast(buf.len), &e) != 0) {
            try checkError(e);
        }
        return std.mem.sliceTo(buf, 0);
    }
};

/// Row wrapper for easier value access
pub const Row = struct {
    inner: *c.struct_flintdb_row,
    meta: *const c.struct_flintdb_meta,

    pub fn free(self: Row) void {
        if (self.inner.free) |free_fn| {
            free_fn(self.inner);
        }
    }

    pub fn setInt64(self: Row, col_idx: usize, value: i64) Error!void {
        var e: ?*u8 = null;
        if (self.inner.i64_set) |set_fn| {
            set_fn(self.inner, @intCast(col_idx), value, &e);
            try checkError(e);
        }
    }

    pub fn setInt32(self: Row, col_idx: usize, value: i32) Error!void {
        var e: ?*u8 = null;
        if (self.inner.i32_set) |set_fn| {
            set_fn(self.inner, @intCast(col_idx), value, &e);
            try checkError(e);
        }
    }

    pub fn setString(self: Row, col_idx: usize, value: []const u8) Error!void {
        var e: ?*u8 = null;
        var buf: [1024]u8 = undefined;
        @memcpy(buf[0..value.len], value);
        buf[value.len] = 0;

        if (self.inner.string_set) |set_fn| {
            set_fn(self.inner, @intCast(col_idx), &buf, &e);
            try checkError(e);
        }
    }

    pub fn setDouble(self: Row, col_idx: usize, value: f64) Error!void {
        var e: ?*u8 = null;
        if (self.inner.f64_set) |set_fn| {
            set_fn(self.inner, @intCast(col_idx), value, &e);
            try checkError(e);
        }
    }

    pub fn getInt64(self: Row, col_idx: usize) Error!i64 {
        var e: ?*u8 = null;
        if (self.inner.i64_get) |get_fn| {
            const val = get_fn(self.inner, @intCast(col_idx), &e);
            try checkError(e);
            return val;
        }
        return 0;
    }

    pub fn getInt32(self: Row, col_idx: usize) Error!i32 {
        var e: ?*u8 = null;
        if (self.inner.i32_get) |get_fn| {
            const val = get_fn(self.inner, @intCast(col_idx), &e);
            try checkError(e);
            return val;
        }
        return 0;
    }

    pub fn getString(self: Row, col_idx: usize, buf: []u8) Error![]const u8 {
        var e: ?*u8 = null;
        if (self.inner.string_get) |get_fn| {
            const ptr = get_fn(self.inner, @intCast(col_idx), &e);
            try checkError(e);
            if (ptr) |p| {
                const cstr: [*:0]const u8 = @ptrCast(p);
                const slice = std.mem.span(cstr);
                @memcpy(buf[0..slice.len], slice);
                return buf[0..slice.len];
            }
        }
        return "";
    }

    pub fn getDouble(self: Row, col_idx: usize) Error!f64 {
        var e: ?*u8 = null;
        if (self.inner.f64_get) |get_fn| {
            const val = get_fn(self.inner, @intCast(col_idx), &e);
            try checkError(e);
            return val;
        }
        return 0.0;
    }

    // Helper to get column index by name
    fn getColumnIndex(self: Row, col_name: []const u8) Error!usize {
        var name_buf: [c.MAX_COLUMN_NAME_LIMIT]u8 = undefined;
        @memcpy(name_buf[0..col_name.len], col_name);
        name_buf[col_name.len] = 0;
        const name_z: [*:0]const u8 = @ptrCast(&name_buf);

        const idx = c.flintdb_column_at(@constCast(self.meta), name_z);
        if (idx < 0) {
            std.debug.print("Column '{s}' not found\n", .{col_name});
            return Error.FlintDBError;
        }
        return @intCast(idx);
    }

    // Column name-based setters
    pub fn setByName(self: Row, col_name: []const u8, comptime T: type, value: T) Error!void {
        const idx = try self.getColumnIndex(col_name);
        switch (T) {
            i64 => try self.setInt64(idx, value),
            i32 => try self.setInt32(idx, value),
            f64 => try self.setDouble(idx, value),
            []const u8 => try self.setString(idx, value),
            else => @compileError("Unsupported type for setByName"),
        }
    }

    pub fn setInt64ByName(self: Row, col_name: []const u8, value: i64) Error!void {
        const idx = try self.getColumnIndex(col_name);
        try self.setInt64(idx, value);
    }

    pub fn setInt32ByName(self: Row, col_name: []const u8, value: i32) Error!void {
        const idx = try self.getColumnIndex(col_name);
        try self.setInt32(idx, value);
    }

    pub fn setStringByName(self: Row, col_name: []const u8, value: []const u8) Error!void {
        const idx = try self.getColumnIndex(col_name);
        try self.setString(idx, value);
    }

    pub fn setDoubleByName(self: Row, col_name: []const u8, value: f64) Error!void {
        const idx = try self.getColumnIndex(col_name);
        try self.setDouble(idx, value);
    }

    // Column name-based getters
    pub fn getInt64ByName(self: Row, col_name: []const u8) Error!i64 {
        const idx = try self.getColumnIndex(col_name);
        return try self.getInt64(idx);
    }

    pub fn getInt32ByName(self: Row, col_name: []const u8) Error!i32 {
        const idx = try self.getColumnIndex(col_name);
        return try self.getInt32(idx);
    }

    pub fn getStringByName(self: Row, col_name: []const u8, buf: []u8) Error![]const u8 {
        const idx = try self.getColumnIndex(col_name);
        return try self.getString(idx, buf);
    }

    pub fn getDoubleByName(self: Row, col_name: []const u8) Error!f64 {
        const idx = try self.getColumnIndex(col_name);
        return try self.getDouble(idx);
    }

    pub fn print(self: Row) void {
        c.flintdb_print_row(self.inner);
    }
};

/// Table cursor for iterating query results
pub const Cursor = struct {
    inner: *c.struct_flintdb_cursor_i64,

    pub fn close(self: Cursor) void {
        if (self.inner.close) |close_fn| {
            close_fn(self.inner);
        }
    }

    pub fn next(self: Cursor) Error!?i64 {
        var e: ?*u8 = null;
        if (self.inner.next) |next_fn| {
            const rowid = next_fn(self.inner, &e);
            try checkError(e);
            if (rowid < 0) return null;
            return rowid;
        }
        return null;
    }
};

/// Main table wrapper
pub const Table = struct {
    inner: *c.struct_flintdb_table,
    meta: *const c.struct_flintdb_meta,

    pub const Mode = enum {
        FLINTDB_RDONLY,
        FLINTDB_RDWR,
    };

    pub fn open(path: []const u8, mode: Mode, meta: ?*Meta) Error!Table {
        var e: ?*u8 = null;
        var path_buf: [256]u8 = undefined;
        @memcpy(path_buf[0..path.len], path);
        path_buf[path.len] = 0;
        const path_z: [*:0]const u8 = @ptrCast(&path_buf);

        const c_mode: c_uint = switch (mode) {
            .FLINTDB_RDONLY => @intCast(c.FLINTDB_RDONLY),
            .FLINTDB_RDWR => @intCast(c.FLINTDB_RDWR),
        };

        const meta_ptr = if (meta) |m| &m.inner else null;
        const tbl = c.flintdb_table_open(path_z, c_mode, meta_ptr, &e);
        try checkError(e);

        if (tbl == null) return Error.TableNotOpen;

        // Get meta by calling the meta() function
        var meta_err: ?*u8 = null;
        const tbl_meta = if (meta_ptr) |m| m else tbl.?.*.meta.?(tbl.?, &meta_err);
        try checkError(meta_err);

        return .{
            .inner = tbl.?,
            .meta = tbl_meta,
        };
    }

    pub fn close(self: Table) void {
        if (self.inner.close) |close_fn| {
            close_fn(self.inner);
        }
    }

    pub fn drop(path: []const u8) void {
        var path_buf: [256]u8 = undefined;
        @memcpy(path_buf[0..path.len], path);
        path_buf[path.len] = 0;
        const path_z: [*:0]const u8 = @ptrCast(&path_buf);
        _ = c.flintdb_table_drop(path_z, null);
    }

    pub fn createRow(self: Table) Error!Row {
        var e: ?*u8 = null;
        const row = c.flintdb_row_new(@constCast(self.meta), &e);
        try checkError(e);
        if (row == null) return Error.RowNotCreated;

        return .{ .inner = row.?, .meta = self.meta };
    }

    pub fn insert(self: Table, row: Row) Error!i64 {
        var e: ?*u8 = null;
        if (self.inner.apply) |apply_fn| {
            const rowid = apply_fn(self.inner, row.inner, 0, &e);
            try checkError(e);
            if (rowid < 0) return Error.FlintDBError;
            return rowid;
        }
        return Error.FlintDBError;
    }

    pub fn read(self: Table, rowid: i64) Error!Row {
        var e: ?*u8 = null;
        if (self.inner.read) |read_fn| {
            const row = read_fn(self.inner, rowid, &e);
            try checkError(e);
            if (row == null) return Error.RowNotCreated;
            // Note: read returns const row, so we cast away const carefully
            return .{ .inner = @constCast(row.?), .meta = self.meta };
        }
        return Error.RowNotCreated;
    }

    pub fn find(self: Table, query: []const u8) Error!Cursor {
        var e: ?*u8 = null;
        var query_buf: [1024]u8 = undefined;
        @memcpy(query_buf[0..query.len], query);
        query_buf[query.len] = 0;
        const query_z: [*:0]const u8 = @ptrCast(&query_buf);

        if (self.inner.find) |find_fn| {
            const cursor = find_fn(self.inner, query_z, &e);
            try checkError(e);
            if (cursor == null) return Error.CursorNotCreated;
            return .{ .inner = cursor.? };
        }
        return Error.CursorNotCreated;
    }
};

/// GenericFile for TSV/CSV support
pub const GenericFile = struct {
    inner: *c.struct_flintdb_genericfile,
    meta: *const c.struct_flintdb_meta,

    pub fn open(path: []const u8, mode: Table.Mode, meta: ?*Meta) Error!GenericFile {
        var e: ?*u8 = null;
        var path_buf: [256]u8 = undefined;
        @memcpy(path_buf[0..path.len], path);
        path_buf[path.len] = 0;
        const path_z: [*:0]const u8 = @ptrCast(&path_buf);

        const c_mode: c_uint = switch (mode) {
            .FLINTDB_RDONLY => @intCast(c.FLINTDB_RDONLY),
            .FLINTDB_RDWR => @intCast(c.FLINTDB_RDWR),
        };

        const meta_ptr = if (meta) |m| &m.inner else null;
        const file = c.flintdb_genericfile_open(path_z, c_mode, meta_ptr, &e);
        try checkError(e);

        if (file == null) return Error.TableNotOpen;

        // Get meta by calling the meta() function
        var meta_err: ?*u8 = null;
        const file_meta = if (meta_ptr) |m| m else file.?.*.meta.?(file.?, &meta_err);
        try checkError(meta_err);

        return .{
            .inner = file.?,
            .meta = file_meta,
        };
    }

    pub fn close(self: GenericFile) void {
        if (self.inner.close) |close_fn| {
            close_fn(self.inner);
        }
    }

    pub fn drop(path: []const u8) void {
        var path_buf: [256]u8 = undefined;
        @memcpy(path_buf[0..path.len], path);
        path_buf[path.len] = 0;
        const path_z: [*:0]const u8 = @ptrCast(&path_buf);
        _ = c.flintdb_genericfile_drop(path_z, null);
    }

    pub fn createRow(self: GenericFile) Error!Row {
        var e: ?*u8 = null;
        const row = c.flintdb_row_new(@constCast(self.meta), &e);
        try checkError(e);
        if (row == null) return Error.RowNotCreated;

        return .{ .inner = row.?, .meta = self.meta };
    }

    pub fn write(self: GenericFile, row: Row) Error!void {
        var e: ?*u8 = null;
        if (self.inner.write) |write_fn| {
            if (write_fn(self.inner, row.inner, &e) != 0) {
                try checkError(e);
                return Error.FlintDBError;
            }
            try checkError(e);
        }
    }
};

// Export common constants
pub const VariantType = struct {
    pub const INT32 = c.VARIANT_INT32;
    pub const INT64 = c.VARIANT_INT64;
    pub const STRING = c.VARIANT_STRING;
    pub const DOUBLE = c.VARIANT_DOUBLE;
    pub const FLOAT = c.VARIANT_FLOAT;
    pub const BOOL = c.VARIANT_BOOL;
};

pub const PRIMARY_KEY = c.PRIMARY_NAME;
pub const MAX_COLUMN_NAME = c.MAX_COLUMN_NAME_LIMIT;

/// Cleanup all FlintDB resources
pub fn cleanup() void {
    var e: ?*u8 = null;
    c.flintdb_cleanup(&e);
}
