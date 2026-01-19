const std = @import("std");

const LazyPath = std.Build.LazyPath;

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const system_libduckdb = b.option(bool, "system_libduckdb", "link with system libduckdb") orelse true;
    const debug_duckdb = b.option(bool, "debug_duckdb", "compile duckdb with DUCKDB_DEBUG_STACKTRACE") orelse false;

    const zuckdb = b.addModule("zuckdb", .{
        .root_source_file = b.path("src/zuckdb.zig"),
        .target = target,
        .optimize = optimize,
    });

    const c_header: LazyPath, const lib = blk: {
        if (system_libduckdb) {
            const lib_path = b.path("lib");
            zuckdb.addIncludePath(lib_path);
            zuckdb.linkSystemLibrary("duckdb", .{});
            break :blk .{ b.path("lib/duckdb.h"), null };
        } else {
            const c_dep = b.lazyDependency("duckdb", .{}) orelse return;
            const root_module = b.createModule(.{
                .target = target,
                .optimize = optimize,
            });

            const c_lib = b.addLibrary(.{
                .name = "duckdb",
                .root_module = root_module,
            });

            c_lib.linkLibCpp();
            const lib_path = c_dep.path("");
            c_lib.addIncludePath(lib_path);
            c_lib.addCSourceFiles(.{
                .files = &.{"duckdb.cpp"},
                .root = c_dep.path(""),
                .flags = &.{"-Wno-date-time"},
            });
            if (debug_duckdb) {
                c_lib.root_module.addCMacro("DUCKDB_DEBUG_STACKTRACE", "");
            }
            c_lib.root_module.addCMacro("DUCKDB_STATIC_BUILD", "");
            // json tests fail because extension loading does not work
            // on the self built version. TODO: statically link core extensions:
            // c_lib.root_module.addCMacro("DUCKDB_EXTENSION_JSON_LINKED", "true");
            b.installArtifact(c_lib);
            b.default_step.dependOn(&b.addInstallHeaderFile(
                c_dep.path("duckdb.h"),
                "duckdb.h",
            ).step);

            zuckdb.linkLibrary(c_lib);
            break :blk .{ c_dep.path("duckdb.h"), c_lib };
        }
    };

    zuckdb.addImport("duckdb_clib", b.addTranslateC(.{
        .root_source_file = c_header,
        .target = target,
        .optimize = optimize,
    }).createModule());

    {
        // Setup Tests
        const lib_test = b.addTest(.{
            .root_module = zuckdb,
            .filters = b.option(
                []const []const u8,
                "test-filter",
                "test-filter",
            ) orelse &.{},
            .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .simple },
        });
        lib_test.addRPath(b.path("lib"));
        lib_test.addIncludePath(b.path("lib"));
        lib_test.addLibraryPath(b.path("lib"));
        if (system_libduckdb) {
            lib_test.linkSystemLibrary("duckdb");
        } else {
            lib_test.linkLibrary(lib.?);
        }

        const run_test = b.addRunArtifact(lib_test);
        run_test.has_side_effects = true;

        const test_step = b.step("test", "Run unit tests");
        test_step.dependOn(&run_test.step);
    }
}
