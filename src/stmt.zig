const std = @import("std");
const zuckdb = @import("zuckdb.zig");
const c = @cImport(@cInclude("zuckdb.h"));

const DB = @import("db.zig").DB;
const Rows = @import("rows.zig").Rows;
const Result = @import("result.zig").Result;

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

const hugeInt = @import("row.zig").hugeInt;

const RESULT_SIZEOF = c.result_sizeof;
const RESULT_ALIGNOF = c.result_alignof;
const STATEMENT_SIZEOF = c.statement_sizeof;

const UUID = zuckdb.UUID;
const Time = zuckdb.Time;
const Date = zuckdb.Date;
const Interval = zuckdb.Interval;

pub const Stmt = struct {
	cached: bool,
	allocator: Allocator,
	stmt: *c.duckdb_prepared_statement,

	pub fn init(allocator: Allocator, stmt: *c.duckdb_prepared_statement, cached: bool) Stmt {
		return .{
			.stmt = stmt,
			.cached = cached,
			.allocator = allocator,
		};
	}

	pub fn deinit(self: Stmt) void {
		const stmt = self.stmt;
		if (self.cached) {
			_ = c.duckdb_clear_bindings(stmt.*);
		} else {
			c.duckdb_destroy_prepare(stmt);
			self.allocator.free(@ptrCast([*]u8, stmt)[0..STATEMENT_SIZEOF]);
		}
	}

	pub fn bind(self: Stmt, values: anytype) !void {
		const stmt = self.stmt.*;
		inline for (values, 0..) |value, i| {
			_ = try bindValue(@TypeOf(value), stmt, value, i + 1);
		}
	}

	pub fn bindDynamic(self: Stmt, i: usize, value: anytype) !void {
		_ = try bindValue(@TypeOf(value), self.stmt.*, value, i+1);
	}

	pub fn execute(self: Stmt, state: anytype) Result(Rows) {
		const allocator = self.allocator;
		var slice = allocator.alignedAlloc(u8, RESULT_ALIGNOF, RESULT_SIZEOF) catch |err| {
			return Result(Rows).allocErr(err, .{.stmt = self});
		};

		const result = @ptrCast(*c.duckdb_result, slice.ptr);
		if (c.duckdb_execute_prepared(self.stmt.*, result) == DuckDBError) {
			return Result(Rows).resultErr(allocator, self, result);
		}
		return Rows.init(allocator, self, result, state);
	}

	pub fn numberOfParameters(self: Stmt) usize {
		return c.duckdb_nparams(self.stmt.*);
	}

	pub fn parameterTypeC(self: Stmt, i: usize) usize {
		return c.duckdb_param_type(self.stmt.*, i+1);
	}

	pub fn parameterType(self: Stmt, i: usize) zuckdb.ParameterType {
		return switch (self.parameterTypeC(i)) {
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
			else => .unknown
		};
	}
};

fn bindValue(comptime T: type, stmt: c.duckdb_prepared_statement, value: anytype, bind_index: usize) !c_uint {
	var rc: c_uint = 0;
	switch (@typeInfo(T)) {
		.Null => rc = c.duckdb_bind_null(stmt, bind_index),
		.ComptimeInt => rc = bindI64(stmt, bind_index, @intCast(i64, value)),
		.ComptimeFloat => rc = c.duckdb_bind_double(stmt, bind_index, @floatCast(f64, value)),
		.Int => |int| {
			if (int.signedness == .signed) {
				switch (int.bits) {
					1...8 => rc = c.duckdb_bind_int8(stmt, bind_index, @intCast(i8, value)),
					9...16 => rc = c.duckdb_bind_int16(stmt, bind_index, @intCast(i16, value)),
					17...32 => rc = c.duckdb_bind_int32(stmt, bind_index, @intCast(i32, value)),
					33...63 => rc = c.duckdb_bind_int64(stmt, bind_index, @intCast(i64, value)),
					64 => rc = bindI64(stmt, bind_index, value),
					65...128 => rc = c.duckdb_bind_hugeint(stmt, bind_index, hugeInt(@intCast(i128, value))),
					else => bindError(T),
				}
			} else {
				switch (int.bits) {
					1...8 => rc = c.duckdb_bind_uint8(stmt, bind_index, @intCast(u8, value)),
					9...16 => rc = c.duckdb_bind_uint16(stmt, bind_index, @intCast(u16, value)),
					17...32 => rc = c.duckdb_bind_uint32(stmt, bind_index, @intCast(u32, value)),
					33...64 => rc = c.duckdb_bind_uint64(stmt, bind_index, @intCast(u64, value)),
					// duckdb doesn't support u128
					else => bindError(T),
				}
			}
		},
		.Float => |float| {
			switch (float.bits) {
				1...32 => rc = c.duckdb_bind_float(stmt, bind_index, @floatCast(f32, value)),
				33...64 => rc = c.duckdb_bind_double(stmt, bind_index, @floatCast(f64, value)),
				else => bindError(T),
			}
		},
		.Bool => rc = c.duckdb_bind_boolean(stmt, bind_index, value),
		.Pointer => |ptr| {
			switch (ptr.size) {
				.One => rc = try bindValue(ptr.child, stmt, value, bind_index),
				.Slice => switch (ptr.child) {
					u8 => rc = bindByteArray(stmt, bind_index, value.ptr, value.len),
					else => bindError(T),
				},
				else => bindError(T),
			}
		},
		.Array => |arr| switch (arr.child) {
			u8 => rc = bindByteArray(stmt, bind_index, value, value.len),
			else => bindError(T),
		},
		.Optional => |opt| {
			if (value) |v| {
				rc = try bindValue(opt.child, stmt, v, bind_index);
			} else {
				rc = c.duckdb_bind_null(stmt, bind_index);
			}
		},
		.Struct => {
			if (T == Date) {
				rc = c.duckdb_bind_date(stmt, bind_index, c.duckdb_to_date(value));
			} else if (T == Time) {
				rc = c.duckdb_bind_time(stmt, bind_index, c.duckdb_to_time(value));
			} else if (T == Interval) {
				rc = c.duckdb_bind_interval(stmt, bind_index, value);
			} else {
				bindError(T);
			}
		},
		else => bindError(T),
	}

	if (rc == DuckDBError) {
		return error.Bind;
	}
	return rc;
}

fn bindI64(stmt: c.duckdb_prepared_statement, bind_index: usize, value: i64) c_uint {
	switch (c.duckdb_param_type(stmt, bind_index)) {
		c.DUCKDB_TYPE_TIMESTAMP => return c.duckdb_bind_timestamp(stmt, bind_index, .{.micros = value}),
		else => return c.duckdb_bind_int64(stmt, bind_index, value),
	}
}

fn bindByteArray(stmt: c.duckdb_prepared_statement, bind_index: usize, value: [*c]const u8, len: usize) c_uint {
	switch (c.duckdb_param_type(stmt, bind_index)) {
		c.DUCKDB_TYPE_VARCHAR => return c.duckdb_bind_varchar_length(stmt, bind_index, value, len),
		c.DUCKDB_TYPE_BLOB => return c.duckdb_bind_blob(stmt, bind_index, @ptrCast([*c]const u8, value), len),
		c.DUCKDB_TYPE_UUID => {
			if (len != 36) return DuckDBError;
			return c.duckdb_bind_varchar_length(stmt, bind_index, value, 36);
		},
		// this one is weird, but duckdb will return DUCKDB_TYPE_INVALID if it doesn't
		// know the type, such as: "select $1", but binding will still work
		c.DUCKDB_TYPE_INVALID => return c.duckdb_bind_varchar_length(stmt, bind_index, value, len),
		else => return DuckDBError,
	}
}

fn bindError(comptime T: type) void {
	@compileError("cannot bind value of type " ++ @typeName(T));
}

const t = std.testing;
test "binding: basic types" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	var rows = conn.query("select $1, $2, $3, $4, $5, $6", .{
		99,
		-32.01,
		true,
		false,
		@as(?i32, null),
		@as(?i32, 44),
	}).ok;
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(@as(i64, 99), row.get(i64, 0).?);
	try t.expectEqual(@as(f64, -32.01), row.get(f64, 1).?);
	try t.expectEqual(true, row.get(bool, 2).?);
	try t.expectEqual(false, row.get(bool, 3).?);
	try t.expectEqual(@as(?i32, null), row.get(i32, 4));
	try t.expectEqual(@as(i32, 44), row.get(i32, 5).?);
}

test "binding: int" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		var rows = conn.query("select $1, $2, $3, $4, $5, $6::hugeint", .{
			99,
			@as(i8, 2),
			@as(i16, 3),
			@as(i32, 4),
			@as(i64, 5),
			@as(i128, -9955340232221457974987)
		}).ok;
		defer rows.deinit();

		const row = (try rows.next()).?;
		try t.expectEqual(@as(i64, 99), row.get(i64, 0).?);
		try t.expectEqual(@as(i8, 2), row.get(i8, 1).?);
		try t.expectEqual(@as(i16, 3), row.get(i16,2).?);
		try t.expectEqual(@as(i32, 4), row.get(i32, 3).?);
		try t.expectEqual(@as(i64, 5), row.get(i64, 4).?);
		try t.expectEqual(@as(i128, -9955340232221457974987), row.get(i128, 5).?);
	}

	{
		// positive limit
		var rows = conn.query("select $1, $2, $3, $4, $5", .{
			@as(i8, 127),
			@as(i16, 32767),
			@as(i32, 2147483647),
			@as(i64, 9223372036854775807),
			@as(i128, 170141183460469231731687303715884105727)
		}).ok;
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqual(@as(i8, 127), row.get(i8, 0).?);
		try t.expectEqual(@as(i16, 32767), row.get(i16,1).?);
		try t.expectEqual(@as(i32, 2147483647), row.get(i32, 2).?);
		try t.expectEqual(@as(i64, 9223372036854775807), row.get(i64, 3).?);
		try t.expectEqual(@as(i128, 170141183460469231731687303715884105727), row.get(i128, 4).?);
	}

	{
		// negative limit
		var rows = conn.query("select $1, $2, $3, $4, $5", .{
			@as(i8, -127),
			@as(i16, -32767),
			@as(i32, -2147483647),
			@as(i64, -9223372036854775807),
			@as(i128, -170141183460469231731687303715884105727)
		}).ok;
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqual(@as(i8, -127), row.get(i8, 0).?);
		try t.expectEqual(@as(i16, -32767), row.get(i16,1).?);
		try t.expectEqual(@as(i32, -2147483647), row.get(i32, 2).?);
		try t.expectEqual(@as(i64, -9223372036854775807), row.get(i64, 3).?);
		try t.expectEqual(@as(i128, -170141183460469231731687303715884105727), row.get(i128, 4).?);
	}

	{
		// unsigned positive limit
		var rows = conn.query("select $1, $2, $3, $4", .{
			@as(u8, 255),
			@as(u16, 65535),
			@as(u32, 4294967295),
			@as(u64, 18446744073709551615),
		}).ok;
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqual(@as(u8, 255), row.get(u8, 0).?);
		try t.expectEqual(@as(u16, 65535), row.get(u16, 1).?);
		try t.expectEqual(@as(u32, 4294967295), row.get(u32, 2).?);
		try t.expectEqual(@as(u64, 18446744073709551615), row.get(u64, 3).?);
	}
}

test "binding: floats" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	// floats
	var rows = conn.query("select $1, $2, $3", .{
		99.88, // $1
		@as(f32, -3.192), // $2
		@as(f64, 999.182), // $3
	}).ok;
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(@as(f64, 99.88), row.get(f64, 0).?);
	try t.expectEqual(@as(f32, -3.192), row.get(f32, 1).?);
	try t.expectEqual(@as(f64, 999.182), row.get(f64, 2).?);
}

test "binding: decimal" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	// decimal
	var rows = conn.query("select $1::decimal(3,2), $2::decimal(18,6)", .{
		1.23, // $1
		-0.3291484 // $2
	}).ok;
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(@as(f64, 1.23), row.get(f64, 0).?);
	try t.expectEqual(@as(f64, -0.329148), row.get(f64, 1).?);
}

test "binding: uuid" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	// uuid
	var rows = conn.query("select $1::uuid, $2::uuid, $3::uuid, $4::uuid", .{"578D0DF0-A76F-4A8E-A463-42F8A4F133C8", "00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff", "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"}).ok;
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqualStrings("578d0df0-a76f-4a8e-a463-42f8a4f133c8", &(row.get(UUID, 0).?));
	try t.expectEqualStrings("00000000-0000-0000-0000-000000000000", &(row.get(UUID, 1).?));
	try t.expectEqualStrings("ffffffff-ffff-ffff-ffff-ffffffffffff", &(row.get(UUID, 2).?));
	try t.expectEqualStrings("ffffffff-ffff-ffff-ffff-ffffffffffff", &(row.get(UUID, 3).?));
}

test "binding: text" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		var result = conn.query("select $1", .{"hello world"});
		defer result.deinit();
		var rows = switch(result) {
			.ok => |rows| rows,
			.err => |err| @panic(err.desc),
		};

		const row = (try rows.next()).?;
		try t.expectEqualStrings("hello world", row.get([]u8, 0).?);
	}

	{
		// runtime varchar
		var list = std.ArrayList([]const u8).init(t.allocator);
		defer list.deinit();
		try list.append("i love keemun");

		var rows = conn.query("select $1::varchar", .{list.items[0]}).ok;
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqualStrings("i love keemun", row.get([]const u8, 0).?);
	}

	{
		// blob
		var rows = conn.query("select $1::blob", .{&[_]u8{0, 1, 2}}).ok;
		defer rows.deinit();

		const row = (try rows.next()).?;
		try t.expectEqualStrings(&[_]u8{0, 1, 2}, row.get([]const u8, 0).?);
	}

	{
		// runtime blob
		var list = std.ArrayList([]const u8).init(t.allocator);
		defer list.deinit();
		try list.append("i love keemun2");

		var rows = conn.query("select $1::blob", .{list.items[0]}).ok;
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqualStrings("i love keemun2", row.get([]const u8, 0).?);
	}
}

test "binding: date/time" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	// date & time
	const date = Date{.year = 2023, .month = 5, .day = 10};
	const time = Time{.hour = 21, .min = 4, .sec = 49, .micros = 123456};
	const interval = Interval{.months = 3, .days = 7, .micros = 982810};
	var rows = conn.query("select $1::date, $2::time, $3::timestamp, $4::interval", .{date, time, 751203002000000, interval}).ok;
	defer rows.deinit();
	const row = (try rows.next()).?;
	try t.expectEqual(date, row.get(Date, 0).?);
	try t.expectEqual(time, row.get(Time, 1).?);
	try t.expectEqual(@as(i64, 751203002000000), row.get(i64, 2).?);
	try t.expectEqual(interval, row.get(Interval, 3).?);
}

test "query parameters" {
	const ParameterType = zuckdb.ParameterType;

	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	const stmt = conn.prepareZ(\\select
		\\ $1::bool,
		\\ $2::tinyint, $3::smallint, $4::integer, $5::bigint, $6::hugeint,
		\\ $7::utinyint, $8::usmallint, $9::uinteger, $10::ubigint,
		\\ $11::real, $12::double, $13::decimal,
		\\ $14::timestamp, $15::date, $16::time, $17::interval,
		\\ $18::varchar, $19::blob
	).ok;

	defer stmt.deinit();

	try t.expectEqual(@as(usize, 19), stmt.numberOfParameters());

	// bool
	try t.expectEqual(@as(usize, 1), stmt.parameterTypeC(0));
	try t.expectEqual(ParameterType.bool, stmt.parameterType(0));

	// int
	try t.expectEqual(@as(usize, 2), stmt.parameterTypeC(1));
	try t.expectEqual(ParameterType.i8, stmt.parameterType(1));
	try t.expectEqual(@as(usize, 3), stmt.parameterTypeC(2));
	try t.expectEqual(ParameterType.i16, stmt.parameterType(2));
	try t.expectEqual(@as(usize, 4), stmt.parameterTypeC(3));
	try t.expectEqual(ParameterType.i32, stmt.parameterType(3));
	try t.expectEqual(@as(usize, 5), stmt.parameterTypeC(4));
	try t.expectEqual(ParameterType.i64, stmt.parameterType(4));
	try t.expectEqual(@as(usize, 16), stmt.parameterTypeC(5));
	try t.expectEqual(ParameterType.i128, stmt.parameterType(5));

	// uint
	try t.expectEqual(@as(usize, 6), stmt.parameterTypeC(6));
	try t.expectEqual(ParameterType.u8, stmt.parameterType(6));
	try t.expectEqual(@as(usize, 7), stmt.parameterTypeC(7));
	try t.expectEqual(ParameterType.u16, stmt.parameterType(7));
	try t.expectEqual(@as(usize, 8), stmt.parameterTypeC(8));
	try t.expectEqual(ParameterType.u32, stmt.parameterType(8));
	try t.expectEqual(@as(usize, 9), stmt.parameterTypeC(9));
	try t.expectEqual(ParameterType.u64, stmt.parameterType(9));

	// float & decimal
	try t.expectEqual(@as(usize, 10), stmt.parameterTypeC(10));
	try t.expectEqual(ParameterType.f32, stmt.parameterType(10));
	try t.expectEqual(@as(usize, 11), stmt.parameterTypeC(11));
	try t.expectEqual(ParameterType.f64, stmt.parameterType(11));
	try t.expectEqual(@as(usize, 19), stmt.parameterTypeC(12));
	try t.expectEqual(ParameterType.decimal, stmt.parameterType(12));

	// time
	try t.expectEqual(@as(usize, 12), stmt.parameterTypeC(13));
	try t.expectEqual(ParameterType.timestamp, stmt.parameterType(13));
	try t.expectEqual(@as(usize, 13), stmt.parameterTypeC(14));
	try t.expectEqual(ParameterType.date, stmt.parameterType(14));
	try t.expectEqual(@as(usize, 14), stmt.parameterTypeC(15));
	try t.expectEqual(ParameterType.time, stmt.parameterType(15));
	try t.expectEqual(@as(usize, 15), stmt.parameterTypeC(16));
	try t.expectEqual(ParameterType.interval, stmt.parameterType(16));

	// varchar & blob
	try t.expectEqual(@as(usize, 17), stmt.parameterTypeC(17));
	try t.expectEqual(ParameterType.varchar, stmt.parameterType(17));
	try t.expectEqual(@as(usize, 18), stmt.parameterTypeC(18));
	try t.expectEqual(ParameterType.blob, stmt.parameterType(18));
}

test "bindDynamic" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	const stmt = conn.prepareZ("select $1::int, $2::varchar, $3::smallint").ok;
	errdefer stmt.deinit();
	try stmt.bindDynamic(0, null);
	try stmt.bindDynamic(1, "over");
	try stmt.bindDynamic(2, 9000);

	var rows = stmt.execute(null).ok;
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(@as(?i32, null), row.get(i32, 0));
	try t.expectEqualStrings("over", row.get([]u8, 1).?);
	try t.expectEqual(@as(i16, 9000), row.get(i16, 2).?);
}
