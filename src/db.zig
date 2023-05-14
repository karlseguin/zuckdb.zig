const std = @import("std");
const c = @cImport(@cInclude("zuckdb.h"));

const Conn = @import("conn.zig").Conn;
const Pool = @import("pool.zig").Pool;

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

const CONFIG_SIZEOF = c.config_sizeof;
const CONFIG_ALIGNOF = c.config_alignof;
const DB_SIZEOF = c.database_sizeof;
const DB_ALIGNOF = c.database_alignof;

pub const DB = struct{
	allocator: Allocator,
	db: *c.duckdb_database,

	pub fn init(allocator: Allocator, path: [*:0]const u8) Result(DB) {
		var config_slice = allocator.alignedAlloc(u8, CONFIG_ALIGNOF, CONFIG_SIZEOF) catch |err| {
			return Result(DB).staticErr(err, "OOM");
		};

		defer allocator.free(config_slice);
		const config = @ptrCast(*c.duckdb_config, config_slice.ptr);

		if (c.duckdb_create_config(config) == DuckDBError) {
			return Result(DB).staticErr(error.CreateConfig, "error creating database config");
		}

		// if (c.duckdb_set_config(config, "enable_external_access", "false") == DuckDBError) {
		//  return error.DBConfigExternal;
		// }

		var db_slice = allocator.alignedAlloc(u8, DB_ALIGNOF, DB_SIZEOF) catch |err| {
			return Result(DB).staticErr(err, "OOM");
		};
		const db = @ptrCast(*c.duckdb_database, db_slice.ptr);

		var out_err: [*c]u8 = undefined;
		if (c.duckdb_open_ext(path, db, config.*, &out_err) == DuckDBError) {
			allocator.free(db_slice);
			return .{.err = .{
				.c_err = out_err,
				.err = error.DBOpen,
				.desc = std.mem.span(out_err),
			}};
		}

		return .{.ok = .{.db = db, .allocator = allocator}};
	}

	pub fn deinit(self: *const DB) void {
		const db = self.db;
		c.duckdb_close(db);
		self.allocator.free(@ptrCast([*]u8, db)[0..DB_SIZEOF]);
	}

	pub fn conn(self: DB) !Conn {
		return Conn.open(self);
	}

	pub fn pool(self: DB, config: Pool.Config) Result(Pool) {
		return Pool.init(self, config);
	}
};

const ResultTag = enum {
	ok,
	err,
};

// T can be a DB or a Pol
pub fn Result(comptime T: type) type {
	return union(ResultTag) {
		ok: T,
		err: Err,

		const Self = @This();
		pub fn deinit(self: Self) void {
			switch (self) {
				inline else => |case| case.deinit(),
			}
		}

		pub fn staticErr(err: anyerror, desc: []const u8) Self {
			return .{.err = .{.err = err, .desc = desc}};
		}
	};
}

const Err = struct {
	err: anyerror,
	desc: []const u8,
	c_err: ?[*c]u8 = null,

	pub fn deinit(self: Err) void {
		if (self.c_err) |err| {
			c.duckdb_free(err);
		}
	}
};

const t = std.testing;
test "open invalid path" {
	const res = DB.init(t.allocator, "/tmp/zuckdb.zig/doesnotexist").err;
	defer res.deinit();
	try t.expectEqualStrings("IO Error: Cannot open file \"/tmp/zuckdb.zig/doesnotexist\": No such file or directory", res.desc);
}
