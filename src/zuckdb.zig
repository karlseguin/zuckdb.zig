const std = @import("std");
const c = @cImport(@cInclude("zuckdb.h"));

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;
const row = @import("row.zig");

pub const DB = @import("db.zig").DB;
pub const Row = row.Row;
pub const OwningRow = row.OwningRow;

pub const Rows = @import("rows.zig").Rows;
pub const Conn = @import("conn.zig").Conn;
pub const Pool = @import("pool.zig").Pool;
pub const Stmt = @import("stmt.zig").Stmt;
pub const Err = @import("result.zig").Err;

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
	@"enum",
	list,
	uuid,

	pub fn jsonStringify(self: ParameterType, options: std.json.StringifyOptions, out: anytype) !void {
		return std.json.encodeJsonString(@tagName(self), options, out);
	}

	pub fn fromDuckDBType(dt: c.duckdb_type) ParameterType{
		return switch (dt) {
			1 => .bool,
			2 => .i8,
			3 => .i16,
			4 => .i32,
			5 => .i64,
			6 => .u8,
			7 => .u16,
			8 => .u32,
			9 => .u64,
			10 => .f32,
			11 => .f64,
			12 => .timestamp,
			13 => .date,
			14 => .time,
			15 => .interval,
			16 => .i128,
			17 => .varchar,
			18 => .blob,
			19 => .decimal,
			23 => .@"enum",
			24 => .list,
			27 => .uuid,
			else => .unknown
		};
	}
};

pub fn StaticState(comptime N: usize) type {
	const ColumnData = @import("column_data.zig").ColumnData;
	return struct {
		columns: [N]ColumnData = undefined,
		column_types: [N]c.duckdb_type = undefined,

		const Self = @This();

		pub fn getColumns(self: *Self, count: usize) ![]ColumnData {
			std.debug.assert(count == N);
			return self.columns[0..count];
		}

		pub fn getColumnTypes(self: *Self, count: usize) ![]c.duckdb_type {
			std.debug.assert(count == N);
			return self.column_types[0..count];
		}
	};
}

// tested in stmt's bit binding test
pub fn bitToString(allocator: Allocator, data: []const u8) ![]u8 {
	const shl = std.math.shl;

	var i: usize = 0;
	var padding = data[0];
	var out = try allocator.alloc(u8, 8 - padding + (8 * (data.len - 2)));
	// std.debug.print("{any} {d}\n", .{data, padding});

	while (padding < 8) : (padding += 1) {
		out[i] = if (data[1] & shl(u8, 1, (7 - padding)) != 0) '1' else '0';
		i += 1;
	}

	for (data[2..]) |byte| {
		for (0..8) |bit| {
			out[i] = if (byte & shl(u8, 1, (7 - bit)) != 0) '1' else '0';
			i += 1;
		}
	}

	return out;
}

const t = std.testing;
test {
	t.refAllDecls(@This());
}
