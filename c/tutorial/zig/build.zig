const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "tutorial",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tutorial.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Header include path (matches C Makefiles which use -I ../../src)
    exe.addIncludePath(b.path("../../src"));

    // Library search path and link to libflintdb
    exe.addLibraryPath(b.path("../../lib"));
    exe.linkSystemLibrary("flintdb");

    // Ensure loader can find the dylib at runtime on macOS
    exe.addRPath(b.path("../../lib"));

    // Install the built executable as the default install step
    b.installArtifact(exe);
}
