// FlintDB C Core Build Script (Zig)
//
// 사용법 (Usage):
//   zig build                     # 기본 빌드: CLI + shared + static lib (zig-out/)
//   zig build cli                 # CLI만 빌드
//   zig build shared              # libflintdb.{so,dylib,dll} 만 빌드
//   zig build static              # libflintdb.a 만 빌드
//
// 빌드 옵션 (Build Options):
//   -Dtarget=<triple>             # 크로스 컴파일 타겟 (예: x86_64-linux-musl, aarch64-macos, x86_64-windows-gnu)
//   -Doptimize=<mode>             # Debug, ReleaseSafe, ReleaseFast, ReleaseSmall (또는 --release=fast/safe/small)
//   -Dversion=<string>            # 버전 문자열 (기본: 0.1.0)
//   -Dembed_html=<bool>           # WebUI HTML 임베드 여부 (기본: true)
//   -Dndebug=<bool>               # NDEBUG 정의 (기본: true)
//   -Dvariant_strpool=<bool>      # variant.c 문자열 풀 사용 (기본: true)
//   -Dstrpool_size=<u32>          # 문자열 풀 블록 크기 (기본: 1024)
//   -Dstrpool_capacity=<u32>      # 문자열 풀 용량 (기본: 1024)
//   -Dstorage_dio_use_buffer_pool=<u32>  # DIO 버퍼 풀 크기 (기본: 64, 0=비활성화)
//   -Dallocator=<name>            # 대체 allocator: none|jemalloc|tcmalloc (기본: none)
//   -Dlink_static=<bool>          # CLI를 static lib에 링크 (기본: false, shared lib 사용)
//
// 크로스 컴파일 옵션 (Cross-Compilation Options):
//   -Dzlib_name=<string>          # zlib 시스템 라이브러리 이름 (기본: z)
//   -Dsysroot=<path>              # sysroot 경로 (include/와 lib/ 하위에 헤더/라이브러리)
//   -Dmingw_sysroot=<path>        # MinGW sysroot (Windows 크로스 컴파일용)
//   -Dextra_include=<path>        # 추가 include 디렉토리
//   -Dextra_lib=<path>            # 추가 library 디렉토리
//
// 크로스 컴파일 예시 (Cross-Compilation Examples):
//   # Windows (x86_64)
//   zig build -Dtarget=x86_64-windows-gnu -Dmingw_sysroot=/usr/x86_64-w64-mingw32
//
//   # Linux (musl)
//   zig build -Dtarget=x86_64-linux-musl -Dsysroot=/path/to/musl-sysroot
//
//   # macOS (ARM64, from x86_64 host)
//   zig build -Dtarget=aarch64-macos
//
//   # Release 빌드 (속도 최적화, Makefile의 -O3와 유사)
//   zig build --release=fast
//   # 또는
//   zig build -Doptimize=ReleaseFast
//
//   # 용량 최적화
//   zig build --release=small
//
// 주의사항 (Notes):
//   - c/src/compress.c가 zlib을 필수로 사용하므로, 크로스 컴파일 시 타겟용 zlib.h와 libz가 필요
//   - 타겟에 zlib이 없으면 -Dsysroot 또는 -Dmingw_sysroot로 헤더/라이브러리 경로 제공
//   - CLI는 Zig wrapper (zig/cli_main.zig)를 통해 C main (flintdb_main)을 호출
//
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    // Zig 0.15 uses -Drelease=[bool] instead of -Doptimize
    // Default to ReleaseFast to match Makefile's -O3 behavior
    const optimize = b.standardOptimizeOption(.{});

    const version = b.option([]const u8, "version", "Version string embedded into CLI (default: 0.1.0)") orelse "0.1.0";
    const embed_html = b.option(bool, "embed_html", "Define EMBED_HTML (default: true)") orelse true;
    const ndebug = b.option(bool, "ndebug", "Define NDEBUG (default: true)") orelse true;

    const variant_strpool = b.option(bool, "variant_strpool", "Enable small-string pooling in variant (default: true)") orelse true;
    const strpool_size = b.option(u32, "strpool_size", "VARIANT_STRPOOL_STR_SIZE (default: 1024)") orelse 1024;
    const strpool_capacity = b.option(u32, "strpool_capacity", "VARIANT_STRPOOL_CAPACITY (default: 1024)") orelse 1024;
    const storage_dio_use_buffer_pool = b.option(u32, "storage_dio_use_buffer_pool", "STORAGE_DIO_USE_BUFFER_POOL (default: 64; 0 disables)") orelse 64;

    const allocator_opt = b.option([]const u8, "allocator", "Alternative allocator: none|jemalloc|tcmalloc (default: none)") orelse "none";
    const link_static = b.option(bool, "link_static", "Link CLI to the static library instead of shared (default: false)") orelse false;

    const zlib_name = b.option([]const u8, "zlib_name", "System library name for zlib (default: z)") orelse "z";
    const sysroot = b.option([]const u8, "sysroot", "Sysroot that contains include/ and lib/ (optional, helps cross-compiles)");
    const mingw_sysroot = b.option([]const u8, "mingw_sysroot", "MinGW sysroot (contains include/ and lib/) for Windows cross-compiles");
    const extra_include = b.option([]const u8, "extra_include", "Extra include directory to add for C compilation (optional)");
    const extra_lib = b.option([]const u8, "extra_lib", "Extra library directory to add for linking (optional)");

    const build_time = formatBuildTime(b.allocator) catch "unknown";

    // Common C flags approximating c/Makefile defaults (kept intentionally minimal).
    const common_c_flags = [_][]const u8{
        "-std=c11",
        "-Wall",
        "-Wextra",
        "-Wformat=0",
        "-Wno-unused-parameter",
        "-Wno-strict-aliasing",
        "-D_GNU_SOURCE",
    };

    const cli_c_flags = [_][]const u8{
        "-std=c11",
        "-Wall",
        "-Wextra",
        "-Wformat=0",
        "-Wno-unused-parameter",
        "-Wno-strict-aliasing",
        "-D_GNU_SOURCE",
        "-Dmain=flintdb_main",
    };

    const include_paths = [_]std.Build.LazyPath{
        b.path("../c"),
        b.path("../c/src"),
    };

    const c_mod_shared = b.createModule(.{
        .root_source_file = b.path("src/empty.zig"),
        .target = target,
        .optimize = optimize,
    });
    const c_mod_static = b.createModule(.{
        .root_source_file = b.path("src/empty.zig"),
        .target = target,
        .optimize = optimize,
    });

    const lib_shared = b.addLibrary(.{
        .name = "flintdb",
        .root_module = c_mod_shared,
        .linkage = .dynamic,
    });
    const lib_static = b.addLibrary(.{
        .name = "flintdb",
        .root_module = c_mod_static,
        .linkage = .static,
    });

    configureCore(b, target, lib_shared, true, &common_c_flags, &include_paths, embed_html, ndebug, variant_strpool, strpool_size, strpool_capacity, storage_dio_use_buffer_pool, allocator_opt, build_time);
    configureCore(b, target, lib_static, false, &common_c_flags, &include_paths, embed_html, ndebug, variant_strpool, strpool_size, strpool_capacity, storage_dio_use_buffer_pool, allocator_opt, build_time);

    // CLI executable
    const exe = b.addExecutable(.{
        .name = "flintdb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/cli_main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.linkLibC();

    for (include_paths) |p| exe.addIncludePath(p);
    exe.addCSourceFile(.{ .file = b.path("../c/src/cli.c"), .flags = &cli_c_flags });

    exe.root_module.addCMacro("VERSION", quoteCString(b.allocator, version) catch "\"0.1.0\"");
    exe.root_module.addCMacro("BUILD_TIME", quoteCString(b.allocator, build_time) catch "\"unknown\"");

    if (link_static) exe.linkLibrary(lib_static) else {
        exe.linkLibrary(lib_shared);
        addRuntimeRPath(exe);
    }

    // Optional sysroot / extra include+lib search paths for cross-compiles.
    applySearchPaths(b, lib_shared, sysroot, mingw_sysroot, extra_include, extra_lib);
    applySearchPaths(b, lib_static, sysroot, mingw_sysroot, extra_include, extra_lib);
    applySearchPaths(b, exe, sysroot, mingw_sysroot, extra_include, extra_lib);

    linkCommonSystemLibs(target, zlib_name, lib_shared, allocator_opt);
    linkCommonSystemLibs(target, zlib_name, lib_static, allocator_opt);
    linkCommonSystemLibs(target, zlib_name, exe, allocator_opt);

    // Install artifacts
    b.installArtifact(lib_shared);
    b.installArtifact(lib_static);
    b.installArtifact(exe);

    // Convenience aliases
    const step_cli = b.step("cli", "Build the flintdb CLI");
    step_cli.dependOn(&exe.step);

    const step_shared = b.step("shared", "Build the shared libflintdb");
    step_shared.dependOn(&lib_shared.step);

    const step_static = b.step("static", "Build the static libflintdb");
    step_static.dependOn(&lib_static.step);
}

fn configureCore(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    artifact: anytype,
    is_shared: bool,
    common_c_flags: []const []const u8,
    include_paths: []const std.Build.LazyPath,
    embed_html: bool,
    ndebug: bool,
    variant_strpool: bool,
    strpool_size: u32,
    strpool_capacity: u32,
    storage_dio_use_buffer_pool: u32,
    allocator_opt: []const u8,
    build_time: []const u8,
) void {
    artifact.linkLibC();

    for (include_paths) |p| artifact.addIncludePath(p);

    if (is_shared) artifact.root_module.addCMacro("FLINTDB_SHARED", "1");
    artifact.root_module.addCMacro("FLINTDB_BUILD", "1");

    if (embed_html) artifact.root_module.addCMacro("EMBED_HTML", "1");
    if (ndebug) artifact.root_module.addCMacro("NDEBUG", "1");

    artifact.root_module.addCMacro("BUILD_TIME", quoteCString(b.allocator, build_time) catch "\"unknown\"");

    if (variant_strpool) {
        artifact.root_module.addCMacro("VARIANT_USE_STRPOOL", "1");
        artifact.root_module.addCMacro("VARIANT_STRPOOL_STR_SIZE", u32ToCString(b.allocator, strpool_size) catch "1024u");
        artifact.root_module.addCMacro("VARIANT_STRPOOL_CAPACITY", u32ToCString(b.allocator, strpool_capacity) catch "1024u");
    }

    if (storage_dio_use_buffer_pool != 0) {
        artifact.root_module.addCMacro("STORAGE_DIO_USE_BUFFER_POOL", u32ToCString(b.allocator, storage_dio_use_buffer_pool) catch "64u");
    }

    // Platform-ish defines used in the Makefile's Windows/MSYS build.
    if (target.result.os.tag == .windows) {
        artifact.root_module.addCMacro("PATH_MAX", "4096");
        artifact.root_module.addCMacro("_WIN32", "1");
        artifact.root_module.addCMacro("_POSIX_", "1");
        // Char literal: '\\'
        artifact.root_module.addCMacro("PATH_SEPARATOR", "'\\\\'");
    }

    // Collect C sources for the core library.
    const core_sources = collectCoreSources(b, target.result.os.tag) catch {
        @panic("failed to enumerate c/src/*.c");
    };
    artifact.addCSourceFiles(.{ .files = core_sources, .flags = common_c_flags });

    _ = allocator_opt; // handled in linkCommonSystemLibs
}

fn collectCoreSources(b: *std.Build, os_tag: std.Target.Os.Tag) ![]const []const u8 {
    var arena = std.heap.ArenaAllocator.init(b.allocator);
    errdefer arena.deinit();

    var list: std.ArrayList([]const u8) = .{};
    defer list.deinit(arena.allocator());

    var dir = try std.fs.cwd().openDir(b.pathFromRoot("../c/src"), .{ .iterate = true });
    defer dir.close();

    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".c")) continue;

        // Exclude test harness sources from the library surface.
        if (std.mem.eql(u8, entry.name, "testcase.c")) continue;
        if (std.mem.eql(u8, entry.name, "testcase_filter.c")) continue;

        // Keep the CLI as a separate executable target.
        if (std.mem.eql(u8, entry.name, "cli.c")) continue;

        // Choose the right runtime shim for the target OS.
        if (std.mem.eql(u8, entry.name, "runtime_win32.c") and os_tag != .windows) continue;
        if (std.mem.eql(u8, entry.name, "runtime.c") and os_tag == .windows) continue;

        const rel = try std.fmt.allocPrint(arena.allocator(), "../c/src/{s}", .{entry.name});
        try list.append(arena.allocator(), rel);
    }

    // Sort for stable build graphs.
    std.mem.sortUnstable([]const u8, list.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b2: []const u8) bool {
            return std.mem.lessThan(u8, a, b2);
        }
    }.lessThan);

    return try b.allocator.dupe([]const u8, list.items);
}

fn linkCommonSystemLibs(target: std.Build.ResolvedTarget, zlib_name: []const u8, artifact: anytype, allocator_opt: []const u8) void {
    artifact.linkLibC();

    // zlib is required by c/src/compress.c.
    artifact.linkSystemLibrary(zlib_name);

    switch (target.result.os.tag) {
        .windows => {
            artifact.linkSystemLibrary("ws2_32");
        },
        .linux => {
            artifact.linkSystemLibrary("m");
            artifact.linkSystemLibrary("pthread");
            artifact.linkSystemLibrary("dl");
        },
        .macos => {
            artifact.linkSystemLibrary("m");
        },
        else => {
            // Best-effort: many targets still provide libm; pthread/dl may not.
            artifact.linkSystemLibrary("m");
        },
    }

    if (std.mem.eql(u8, allocator_opt, "jemalloc")) {
        artifact.linkSystemLibrary("jemalloc");
    } else if (std.mem.eql(u8, allocator_opt, "tcmalloc")) {
        artifact.linkSystemLibrary("tcmalloc");
    }
}

fn applySearchPaths(
    b: *std.Build,
    artifact: anytype,
    sysroot: ?[]const u8,
    mingw_sysroot: ?[]const u8,
    extra_include: ?[]const u8,
    extra_lib: ?[]const u8,
) void {
    if (extra_include) |p| artifact.addIncludePath(.{ .cwd_relative = p });
    if (extra_lib) |p| artifact.addLibraryPath(.{ .cwd_relative = p });

    // Generic sysroot (include/ + lib/).
    if (sysroot) |sr| {
        const inc = b.pathJoin(&.{ sr, "include" });
        const lib = b.pathJoin(&.{ sr, "lib" });
        artifact.addIncludePath(.{ .cwd_relative = inc });
        artifact.addLibraryPath(.{ .cwd_relative = lib });
    }

    // MinGW sysroot (include/ + lib/), convenient for Windows cross-builds.
    if (mingw_sysroot) |sr| {
        const inc = b.pathJoin(&.{ sr, "include" });
        const lib = b.pathJoin(&.{ sr, "lib" });
        artifact.addIncludePath(.{ .cwd_relative = inc });
        artifact.addLibraryPath(.{ .cwd_relative = lib });
    }
}

fn addRuntimeRPath(exe: *std.Build.Step.Compile) void {
    // Match the Makefile behavior: look for ../lib next to the installed executable.
    const os_tag = exe.rootModuleTarget().os.tag;
    switch (os_tag) {
        .linux => exe.addRPath(.{ .cwd_relative = "$ORIGIN/../lib" }),
        .macos => exe.addRPath(.{ .cwd_relative = "@executable_path/../lib" }),
        else => {},
    }
}

fn quoteCString(alloc: std.mem.Allocator, s: []const u8) ![]const u8 {
    return std.fmt.allocPrint(alloc, "\"{s}\"", .{s});
}

fn u32ToCString(alloc: std.mem.Allocator, v: u32) ![]const u8 {
    return std.fmt.allocPrint(alloc, "{d}u", .{v});
}

fn formatBuildTime(alloc: std.mem.Allocator) ![]const u8 {
    // Keep this stable across Zig versions: use unix epoch seconds as the build stamp.
    // (The Makefile uses YYYYMMDDHHMMSS, but this is sufficient for display/debugging.)
    const ts: i64 = std.time.timestamp();
    return std.fmt.allocPrint(alloc, "{d}", .{ts});
}
