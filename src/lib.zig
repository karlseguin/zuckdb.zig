pub const c = @cImport(@cInclude("duckdb.h"));

const row = @import("row.zig");
pub const Row = row.Row;
pub const List = row.List;
pub const Enum = row.Enum;
pub const LazyList = row.LazyList;
pub const OwningRow = row.OwningRow;

pub const DB = @import("db.zig").DB;
pub const Rows = @import("rows.zig").Rows;
pub const Pool = @import("pool.zig").Pool;
pub const Stmt = @import("stmt.zig").Stmt;
pub const Conn = @import("conn.zig").Conn;
pub const Vector = @import("vector.zig").Vector;
pub const Appender = @import("appender.zig").Appender;

pub const Date = c.duckdb_date_struct;
pub const Time = c.duckdb_time_struct;
pub const Interval = c.duckdb_interval;
pub const UUID = [36]u8;

pub const DataType = enum {
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
	u128,
	varchar,
	blob,
	decimal,
	@"enum",
	list,
	uuid,
	bitstring,
	timetz,
	timestamptz,

	pub fn jsonStringify(self: DataType, options: std.json.StringifyOptions, out: anytype) !void {
		return std.json.encodeJsonString(@tagName(self), options, out);
	}

	pub fn fromDuckDBType(dt: c.duckdb_type) DataType{
		return switch (dt) {
			c.DUCKDB_TYPE_BOOLEAN => .bool,
			c.DUCKDB_TYPE_TINYINT => .i8,
			c.DUCKDB_TYPE_SMALLINT => .i16,
			c.DUCKDB_TYPE_INTEGER => .i32,
			c.DUCKDB_TYPE_BIGINT => .i64,
			c.DUCKDB_TYPE_UTINYINT => .u8,
			c.DUCKDB_TYPE_USMALLINT => .u16,
			c.DUCKDB_TYPE_UINTEGER => .u32,
			c.DUCKDB_TYPE_UBIGINT => .u64,
			c.DUCKDB_TYPE_FLOAT => .f32,
			c.DUCKDB_TYPE_DOUBLE => .f64,
			c.DUCKDB_TYPE_TIMESTAMP => .timestamp,
			c.DUCKDB_TYPE_DATE => .date,
			c.DUCKDB_TYPE_TIME => .time,
			c.DUCKDB_TYPE_INTERVAL => .interval,
			c.DUCKDB_TYPE_HUGEINT => .i128,
			c.DUCKDB_TYPE_UHUGEINT => .u128,
			c.DUCKDB_TYPE_VARCHAR => .varchar,
			c.DUCKDB_TYPE_BLOB => .blob,
			c.DUCKDB_TYPE_DECIMAL => .decimal,
			c.DUCKDB_TYPE_ENUM => .@"enum",
			c.DUCKDB_TYPE_LIST => .list,
			c.DUCKDB_TYPE_UUID => .uuid,
			c.DUCKDB_TYPE_BIT => .bitstring,
			c.DUCKDB_TYPE_TIME_TZ => .timetz,
			c.DUCKDB_TYPE_TIMESTAMP_TZ => .timestamptz,
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

pub fn uhugeInt(value: u128) c.duckdb_uhugeint {
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

pub const TINYINT_MIN = -128;
pub const TINYINT_MAX = 127;
pub const UTINYINT_MIN = 0;
pub const UTINYINT_MAX = 255;

pub const SMALLINT_MIN = -32768;
pub const SMALLINT_MAX = 32767;
pub const USMALLINT_MIN = 0;
pub const USMALLINT_MAX = 65535;

pub const INTEGER_MIN = -2147483648;
pub const INTEGER_MAX = 2147483647;
pub const UINTEGER_MIN = 0;
pub const UINTEGER_MAX = 4294967295;

pub const BIGINT_MIN = -9223372036854775808;
pub const BIGINT_MAX = 9223372036854775807;
pub const UBIGINT_MIN = 0;
pub const UBIGINT_MAX = 18446744073709551615;

pub const HUGEINT_MIN = -170141183460469231731687303715884105728;
pub const HUGEINT_MAX = 170141183460469231731687303715884105727;
pub const UHUGEINT_MIN = 0;
pub const UHUGEINT_MAX = 340282366920938463463374607431768211455;


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
