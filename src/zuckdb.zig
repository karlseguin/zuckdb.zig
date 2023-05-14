const std = @import("std");
const c = @cImport(@cInclude("zuckdb.h"));

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

pub const DB = @import("db.zig").DB;
pub const Conn = @import("conn.zig").Conn;
pub const Pool = @import("pool.zig").Pool;

pub const Date = c.duckdb_date_struct;
pub const Time = c.duckdb_time_struct;
pub const Interval = c.duckdb_interval;
pub const UUID = [36]u8;

pub const ParameterType = enum {
	unknown,
	bool,
	i8,
	i16,
	i32,
	i64,
	u8,
	u16,
	u32,
	u64,
	f32,
	f64,
	timestamp,
	date,
	time,
	interval,
	i128,
	varchar,
	blob,
	decimal,

	pub fn jsonStringify(self: ParameterType, options: std.json.StringifyOptions, out: anytype) !void {
		return std.json.encodeJsonString(@tagName(self), options, out);
	}
};

const t = std.testing;
test {
	t.refAllDecls(@This());
}
