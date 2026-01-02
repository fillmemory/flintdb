const std = @import("std");

extern fn flintdb_main(argc: i32, argv: [*:null]?[*:0]u8) i32;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // C expects argv to be null-terminated (argv[argc] == null) in many environments.
    var c_argv = try allocator.alloc(?[*:0]u8, args.len + 1);
    defer allocator.free(c_argv);

    for (args, 0..) |a, i| {
        c_argv[i] = a.ptr;
    }
    c_argv[args.len] = null;

    const argc: i32 = @intCast(args.len);
    const argv: [*:null]?[*:0]u8 = @ptrCast(c_argv.ptr);
    const rc: i32 = flintdb_main(argc, argv);
    if (rc != 0) {
        const code: u8 = if (rc <= 0) 1 else if (rc > 255) 255 else @intCast(rc);
        std.process.exit(code);
    }
}
