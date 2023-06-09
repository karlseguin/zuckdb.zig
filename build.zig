const std = @import("std");

pub fn build(b: *std.Build) !void {
	const target = b.standardTargetOptions(.{});
	const optimize = b.standardOptimizeOption(.{});

	const typed_module = b.dependency("typed", .{
		.target = target,
		.optimize = optimize,
	}).module("typed");

	_ = b.addModule("zuckdb", .{
		.source_file = .{ .path = "src/zuckdb.zig" },
		.dependencies = &.{ .{.name = "typed", .module = typed_module }},
	});

	const lib_test = b.addTest(.{
		.root_source_file = .{ .path = "src/zuckdb.zig" },
		.target = target,
		.optimize = optimize,
	});

	lib_test.addRPath("lib");
	lib_test.addLibraryPath("lib");
	lib_test.addIncludePath("lib/");
	lib_test.linkSystemLibrary("duckdb");
	lib_test.addModule("typed", typed_module);

	const run_test = b.addRunArtifact(lib_test);
	run_test.has_side_effects = true;

	const test_step = b.step("test", "Run unit tests");
	test_step.dependOn(&run_test.step);
}
