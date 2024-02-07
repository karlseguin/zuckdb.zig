const std = @import("std");

const LazyPath = std.Build.LazyPath;

pub fn build(b: *std.Build) !void {
	const target = b.standardTargetOptions(.{});
	const optimize = b.standardOptimizeOption(.{});

	const lib_path = LazyPath.relative("lib");

	_ = b.addModule("zuckdb", .{
		.root_source_file = .{ .path = "src/zuckdb.zig" },
	});

	{
		// Setup Tests
		const lib_test = b.addTest(.{
			.root_source_file = .{ .path = "src/zuckdb.zig" },
			.target = target,
			.optimize = optimize,
		});

		lib_test.addRPath(lib_path);
		lib_test.addIncludePath(lib_path);
		lib_test.addLibraryPath(lib_path);
		lib_test.linkSystemLibrary("duckdb");

		const run_test = b.addRunArtifact(lib_test);
		run_test.has_side_effects = true;

		const test_step = b.step("test", "Run unit tests");
		test_step.dependOn(&run_test.step);
	}
}
