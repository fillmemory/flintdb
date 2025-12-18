/// FlintDB Zig Tutorial
const std = @import("std");
const flintdb = @import("flintdb.zig");

// const c = @cImport({
//     @cInclude("flintdb.h");
// });

fn tutorialTableCreate() !void {
    std.debug.print("--- Creating Customer Table ---\n", .{});

    const tablename = "./temp/tutorial_customer.flintdb";
    flintdb.Table.drop(tablename);

    // Create metadata
    var meta = try flintdb.Meta.init(tablename);
    defer meta.deinit();

    // Add columns
    try meta.addColumn("id", flintdb.VariantType.INT64, 0, 0, 1, "0", "PRIMARY KEY");
    try meta.addColumn("name", flintdb.VariantType.STRING, 50, 0, 1, "", "Customer name");
    try meta.addColumn("age", flintdb.VariantType.INT32, 0, 0, 1, "0", "Customer age");

    // Add indexes
    try meta.addIndex(flintdb.PRIMARY_KEY, &[_][]const u8{"id"});
    try meta.addIndex("ix_age", &[_][]const u8{"age"});

    // Print schema
    var sql_buf: [2048]u8 = undefined;
    const sql = try meta.toSQL(&sql_buf);
    std.debug.print("Schema SQL:\n{s}\n\n", .{sql});

    // Open table and insert data
    var table = try flintdb.Table.open(tablename, .FLINTDB_RDWR, &meta);
    defer table.close();

    std.debug.print("Inserting 3 customers (using column names)...\n", .{});
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        var row = try table.createRow();
        defer row.free();

        // Using column names instead of indices!
        try row.setInt64ByName("id", @intCast(i + 1));

        var name_buf: [50]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buf, "Customer {d}", .{i + 1});
        try row.setStringByName("name", name);

        try row.setInt32ByName("age", @intCast(30 + i));

        const rowid = try table.insert(row);
        std.debug.print("  Inserted rowid: {d}\n", .{rowid});
    }
    std.debug.print("✓ Table created successfully\n\n", .{});
}

fn tutorialTableFind() !void {
    std.debug.print("--- Finding Customers (age >= 31) ---\n", .{});

    const tablename = "./temp/tutorial_customer.flintdb";
    var table = try flintdb.Table.open(tablename, .FLINTDB_RDONLY, null);
    defer table.close();

    var cursor = try table.find("WHERE age >= 31");
    defer cursor.close();

    while (try cursor.next()) |rowid| {
        const row = try table.read(rowid);
        row.print();
    }

    std.debug.print("✓ Query completed\n\n", .{});
}

fn tutorialTsvCreate() !void {
    std.debug.print("--- Creating TSV File ---\n", .{});

    const filepath = "./temp/tutorial_products.tsv";
    flintdb.GenericFile.drop(filepath);

    var meta = try flintdb.Meta.init(filepath);
    defer meta.deinit();

    // Configure for TSV
    const tsv = "tsv";
    @memcpy(meta.inner.format[0..tsv.len], tsv);
    meta.inner.format[tsv.len] = 0;
    meta.inner.delimiter = '\t';
    meta.inner.absent_header = 1;

    // Add columns
    try meta.addColumn("product_id", flintdb.VariantType.INT32, 0, 0, 1, "", "");
    try meta.addColumn("product_name", flintdb.VariantType.STRING, 100, 0, 1, "", "");
    try meta.addColumn("price", flintdb.VariantType.DOUBLE, 0, 0, 1, "", "");

    var file = try flintdb.GenericFile.open(filepath, .FLINTDB_RDWR, &meta);
    defer file.close();

    std.debug.print("Writing 3 products (using column names)...\n", .{});
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        var row = try file.createRow();
        defer row.free();

        // Use column names like C example
        try row.setInt32ByName("product_id", @intCast(101 + i));

        var name_buf: [100]u8 = undefined;
        const ch: u8 = @intCast('A' + i);
        const name = try std.fmt.bufPrint(&name_buf, "Product-{c}", .{ch});
        try row.setStringByName("product_name", name);

        const price = 9.99 * @as(f64, @floatFromInt(i + 1));
        try row.setDoubleByName("price", price);

        try file.write(row);
        std.debug.print("  Written: Product-{c}, ${d:.2}\n", .{ ch, price });
    }
    std.debug.print("✓ TSV file created\n\n", .{});
}

fn tutorialTableQuery() !void {
    std.debug.print("--- Advanced Query Examples ---\n", .{});

    const tablename = "./temp/tutorial_customer.flintdb";
    var table = try flintdb.Table.open(tablename, .FLINTDB_RDONLY, null);
    defer table.close();

    // Query: All customers (using column names!)
    std.debug.print("All customers (accessed by column names):\n", .{});
    var cursor1 = try table.find("");
    defer cursor1.close();
    while (try cursor1.next()) |rowid| {
        const row = try table.read(rowid);

        var name_buf: [50]u8 = undefined;
        // Access by column name instead of index!
        const id = try row.getInt64ByName("id");
        const name = try row.getStringByName("name", &name_buf);
        const age = try row.getInt32ByName("age");

        std.debug.print("  ID={d}, Name={s}, Age={d}\n", .{ id, name, age });
    }

    std.debug.print("\n✓ Queries completed\n\n", .{});
}

fn tutorialTableUpdateDelete() !void {
    std.debug.print("--- Table Update/Delete Operations ---\n", .{});

    const tablename = "./temp/tutorial_customer.flintdb";
    var table = try flintdb.Table.open(tablename, .FLINTDB_RDWR, null);
    defer table.close();

    std.debug.print("(Update/delete operations require additional binding implementation)\n", .{});
    std.debug.print("✓ Feature available in C API\n\n", .{});
}

fn tutorialFilesort() !void {
    std.debug.print("--- Filesort for External Sorting ---\n", .{});
    std.debug.print("(Filesort bindings require additional Zig implementation)\n", .{});
    std.debug.print("✓ Feature available in C API\n\n", .{});
}

pub fn main() !void {
    defer flintdb.cleanup();

    // Ensure temp directory exists
    var cwd = std.fs.cwd();
    cwd.makeDir("temp") catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    try tutorialTableCreate();
    try tutorialTableFind();
    try tutorialTableQuery();
    try tutorialTableUpdateDelete();
    try tutorialTsvCreate();
    try tutorialFilesort();

    std.debug.print("✨ All tutorials completed successfully!\n", .{});
}
