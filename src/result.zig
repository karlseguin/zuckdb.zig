const std = @import("std");
const c = @cImport(@cInclude("zuckdb.h"));

const Stmt = @import("stmt.zig").Stmt;

const Allocator = std.mem.Allocator;

const RESULT_SIZEOF = c.result_sizeof;
const STATEMENT_SIZEOF = c.statement_sizeof;

const Tag = enum {
	ok,
	err,
};

pub fn Result(comptime T: type) type {
	return union(Tag) {
		ok: T,
		err: Err,

		const Self = @This();

		pub fn deinit(self: Self) void {
			switch (self) {
				.ok => |ok| {
					// better option??
					if (@typeInfo(T) == .Optional) {
						if (ok) |okok| okok.deinit();
					} else {
						ok.deinit();
					}
				},
				.err => |err| err.deinit(),
			}
		}

		pub fn unwrap(self: Self) !T {
			switch(self) {
				.ok => |ok| return ok,
				.err => |err| {
					defer err.deinit();
					std.log.err("zuckdb unwrap error: {s}\n", .{err.desc});
					return err.err;
				}
			}
		}

		const Ownership = struct {
			stmt: ?Stmt = null,
			result: ?*c.duckdb_result = null,
		};

		pub fn allocErr(err: anyerror, own: Ownership) Self {
			return staticErr(err, "OOM", own);
		}

		pub fn staticErr(err: anyerror, desc: [:0]const u8, own: Ownership) Self {
			return .{.err = .{.err = err, .desc = desc, .stmt = own.stmt, .result = own.result}};
		}

		pub fn resultErr(allocator: Allocator, stmt: ?Stmt, result: *c.duckdb_result) Self {
			return .{.err = .{
				.stmt = stmt,
				.result = result,
				.err = error.InvalidSQL,
				.allocator = allocator,
				.desc = std.mem.span(c.duckdb_result_error(result)),
			}};
		}
	};
}

pub const Err = struct {
	err: anyerror,
	desc: []const u8,
	allocator: Allocator = undefined,
	result: ?*c.duckdb_result = null,
	stmt: ?Stmt = null,

	pub fn deinit(self: Err) void {
		if (self.result) |r| {
			c.duckdb_destroy_result(r);
			self.allocator.free(@ptrCast([*]u8, r)[0..RESULT_SIZEOF]);
		}

		if (self.stmt) |s| {
			s.deinit();
		}
	}

	pub fn isDuplicate(self: Err) bool {
		// no better way right nows
		return std.mem.startsWith(u8, self.desc, "Constraint Error: Duplicate key");
	}
};
