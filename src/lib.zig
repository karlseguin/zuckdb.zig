pub const c = @cImport(@cInclude("duckdb.h"));
pub const DB = @import("db.zig").DB;
pub const Row = @import("row.zig").Row;
pub const Enum = @import("row.zig").Enum;
pub const Rows = @import("rows.zig").Rows;
pub const Pool = @import("pool.zig").Pool;
pub const Stmt = @import("stmt.zig").Stmt;
pub const Conn = @import("conn.zig").Conn;
pub const OwningRow = @import("row.zig").OwningRow;
pub const ColumnData = @import("column_data.zig").ColumnData;

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
	bitstring,

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
			29 => .bitstring,
			else => .unknown
		};
	}
};

const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn hugeInt(value: i128) c.duckdb_hugeint {
	return .{
		.lower = @intCast(@mod(value, 18446744073709551616)),
		.upper = @intCast(@divFloor(value, 18446744073709551616)),
	};
}

pub const StringZ = struct {
	z: [:0]const u8,
	duped: bool,

	pub fn deinit(self: StringZ, allocator: Allocator) void {
		if (self.duped) {
			allocator.free(self.z);
		}
	}
};

pub fn stringZ(str: anytype, allocator: Allocator) !StringZ {
	const T = @TypeOf(str);
	if (comptime isNullTerminatedString(T)) {
		return .{.duped = false, .z = str};
	}
	if (comptime isStringSlice(T)) {
		return .{.duped = true, .z = try allocator.dupeZ(u8, str)};
	}
	if (comptime isStringArray(T)) {
		return .{.duped = true, .z = try allocator.dupeZ(u8, &str)};
	}
	@compileError("Expected a string, got: {}" ++ @typeName(T));
}

fn isNullTerminatedString(comptime T: type) bool {
	switch (@typeInfo(T)) {
		.Pointer => |ptr| switch (ptr.size) {
			.One => return isNullTerminatedString(ptr.child),
			.Slice => {
				if (ptr.child == u8) {
					if (std.meta.sentinel(T)) |s| return s == 0;
				}
				return false;
			},
			else => return false,
		},
		.Array => |arr| {
			if (arr.child == u8) {
				if (std.meta.sentinel(T)) |s| return s == 0;
			}
			return false;
		},
		else => return false,
	}
}

fn isStringSlice(comptime T: type) bool {
	switch (@typeInfo(T)) {
		.Pointer => |ptr| switch (ptr.size) {
			.Slice => return ptr.child == u8 and ptr.sentinel == null,
			else => {},
		},
		else => {},
	}
	return false;
}

fn isStringArray(comptime T: type) bool {
	switch (@typeInfo(T)) {
		.Array => |arr| return arr.child == u8,
		else => return false,
	}
}


const root = @import("root");
const _assert = blk: {
	if (@hasDecl(root, "zuckdb_assert")) {
		break :blk root.pg_assert;
	}
	switch (@import("builtin").mode) {
		.ReleaseFast, .ReleaseSmall => break :blk false,
		else => break: blk true,
	}
};

pub fn assert(ok: bool) void {
	if (comptime _assert) {
		std.debug.assert(ok);
	}
}
