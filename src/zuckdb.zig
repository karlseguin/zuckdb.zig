const std = @import("std");
const lib = @import("lib.zig");

pub const c = lib.c;
pub const DB = lib.DB;
pub const Row = lib.Row;
pub const List = lib.List;
pub const Enum = lib.Enum;
pub const Rows = lib.Rows;
pub const Conn = lib.Conn;
pub const Pool = lib.Pool;
pub const Stmt = lib.Stmt;
pub const Appender = lib.Appender;
pub const LazyList = lib.LazyList;
pub const OwningRow = lib.OwningRow;

pub const UUID = lib.UUID;
pub const Date = lib.Date;
pub const Time = lib.Time;
pub const Interval = lib.Interval;
pub const DataType = lib.DataType;

pub fn StaticState(comptime N: usize) type {
	const ColumnData = @import("column_data.zig").ColumnData;
	return struct {
		columns: [N]lib.ColumnData = undefined,
		column_types: [N]lib.c.duckdb_type = undefined,

		const Self = @This();

		pub fn getColumns(self: *Self, count: usize) ![]ColumnData {
			std.debug.assert(count == N);
			return self.columns[0..count];
		}

		pub fn getColumnTypes(self: *Self, count: usize) ![]lib.c.duckdb_type {
			std.debug.assert(count == N);
			return self.column_types[0..count];
		}
	};
}

// tested in stmt's bit binding test
pub fn bitToString(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
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

pub fn isDuplicate(err: []const u8) bool {
	// no better way right nows
	return std.mem.startsWith(u8, err, "Constraint Error: Duplicate key");
}

test {
	std.testing.refAllDecls(@This());
}
