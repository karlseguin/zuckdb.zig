const std = @import("std");

pub fn build(b: *std.Build) !void {
	const target = b.standardTargetOptions(.{});
	const optimize = b.standardOptimizeOption(.{});

	_ = b.addModule("zuckdb", .{
		.source_file = .{ .path = "zuckdb.zig" },
	});

	const lib_test = b.addTest(.{
		.root_source_file = .{ .path = "zuckdb.zig" },
		.target = target,
		.optimize = optimize,
	});
	lib_test.addRPath("lib");
	lib_test.addLibraryPath("lib");
	lib_test.addIncludePath("lib/");
	lib_test.linkSystemLibrary("duckdb");

	const run_test = b.addRunArtifact(lib_test);
	run_test.has_side_effects = true;

	const test_step = b.step("test", "Run unit tests");
	test_step.dependOn(&run_test.step);
}
