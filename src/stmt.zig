const std = @import("std");
const lib = @import("lib.zig");

const M = @This();

const c = lib.c;
const Conn = lib.Conn;
const Rows = lib.Rows;

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

const UUID = lib.UUID;
const Time = lib.Time;
const Date = lib.Date;
const Interval = lib.Interval;
const ParameterType = lib.ParameterType;

pub const Stmt = struct {
	conn: *Conn,
	auto_release: bool,
	stmt: *c.duckdb_prepared_statement,

	pub const Opts = struct {
		auto_release: bool = false,
	};

	pub fn init(stmt: *c.duckdb_prepared_statement, conn: *Conn, opts: Opts) Stmt {
		return .{
			.conn = conn,
			.stmt = stmt,
			.auto_release = opts.auto_release,
		};
	}

	pub fn deinit(self: Stmt) void {
		const stmt = self.stmt;
		c.duckdb_destroy_prepare(stmt);
		self.conn.allocator.destroy(stmt);
	}

	pub fn clearBindings(self: *const Stmt) !void {
		if (c.duckdb_clear_bindings(self.stmt.*) == DuckDBError) {
			return error.DuckDBError;
		}
	}

	pub fn bind(self: *const Stmt, values: anytype) !void {
		const stmt = self.stmt.*;
		inline for (values, 0..) |value, i| {
			_ = try M.bindValue(@TypeOf(value), stmt, value, i + 1);
		}
	}

	pub fn bindValue(self: *const Stmt, i: usize, value: anytype) !void {
		_ = try M.bindValue(@TypeOf(value), self.stmt.*, value, i+1);
	}

	pub fn exec(self: *const Stmt) !usize {
		const result = try self.getResult();
		defer {
			c.duckdb_destroy_result(result);
			self.conn.allocator.destroy(result);
		}
		return c.duckdb_rows_changed(result);
	}

	pub fn query(self: *const Stmt, state: anytype) !Rows {
		const result = try self.getResult();
		return Rows.init(self.conn.allocator, if (self.auto_release) self.stmt else null, result, state);
	}

	pub fn getResult(self: *const Stmt) !*c.duckdb_result {
		const conn = self.conn;
		const allocator = conn.allocator;

		const result = try allocator.create(c.duckdb_result);
		errdefer allocator.destroy(result);

		if (c.duckdb_execute_prepared(self.stmt.*, result) == DuckDBError) {
			try self.conn.duckdbError(c.duckdb_result_error(result));
			return error.DuckDBError;
		}
		return result;
	}

	pub fn numberOfParameters(self: Stmt) usize {
		return c.duckdb_nparams(self.stmt.*);
	}

	pub fn parameterTypeC(self: Stmt, i: usize) c.duckdb_type {
		return c.duckdb_param_type(self.stmt.*, i+1);
	}

	pub fn parameterType(self: Stmt, i: usize) ParameterType {
		return ParameterType.fromDuckDBType(self.parameterTypeC(i));
	}
};

fn bindValue(comptime T: type, stmt: c.duckdb_prepared_statement, value: anytype, bind_index: usize) !c_uint {
	var rc: c_uint = 0;
	switch (@typeInfo(T)) {
		.Null => rc = c.duckdb_bind_null(stmt, bind_index),
		.ComptimeInt => rc = bindI64(stmt, bind_index, @intCast(value)),
		.ComptimeFloat => rc = c.duckdb_bind_double(stmt, bind_index, @floatCast(value)),
		.Int => |int| {
			if (int.signedness == .signed) {
				switch (int.bits) {
					1...8 => rc = c.duckdb_bind_int8(stmt, bind_index, @intCast(value)),
					9...16 => rc = c.duckdb_bind_int16(stmt, bind_index, @intCast(value)),
					17...32 => rc = c.duckdb_bind_int32(stmt, bind_index, @intCast(value)),
					33...63 => rc = c.duckdb_bind_int64(stmt, bind_index, @intCast(value)),
					64 => rc = bindI64(stmt, bind_index, value),
					65...128 => rc = c.duckdb_bind_hugeint(stmt, bind_index, lib.hugeInt(@intCast(value))),
					else => bindError(T),
				}
			} else {
				switch (int.bits) {
					1...8 => rc = c.duckdb_bind_uint8(stmt, bind_index, @intCast(value)),
					9...16 => rc = c.duckdb_bind_uint16(stmt, bind_index, @intCast(value)),
					17...32 => rc = c.duckdb_bind_uint32(stmt, bind_index, @intCast(value)),
					33...64 => rc = c.duckdb_bind_uint64(stmt, bind_index, @intCast(value)),
					65...128 => rc = c.duckdb_bind_uhugeint(stmt, bind_index, lib.uhugeInt(@intCast(value))),
					else => bindError(T),
				}
			}
		},
		.Float => |float| {
			switch (float.bits) {
				1...32 => rc = c.duckdb_bind_float(stmt, bind_index, @floatCast(value)),
				33...64 => rc = c.duckdb_bind_double(stmt, bind_index, @floatCast(value)),
				else => bindError(T),
			}
		},
		.Bool => rc = c.duckdb_bind_boolean(stmt, bind_index, value),
		.Pointer => |ptr| {
			switch (ptr.size) {
				.Slice => {
					if (ptr.is_const) {
						rc = bindSlice(stmt, bind_index, @as([]const ptr.child, value));
					} else {
						rc = bindSlice(stmt, bind_index, @as([]ptr.child, value));
					}
				},
				.One => switch (@typeInfo(ptr.child)) {
					.Array => {
						const Slice = []const std.meta.Elem(ptr.child);
						rc = bindSlice(stmt, bind_index, @as(Slice, value));
					},
					else => bindError(T),
				},
				else => bindError(T),
			}
		},
		.Array => rc = try bindValue(@TypeOf(&value), stmt, &value, bind_index),
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
		c.DUCKDB_TYPE_VARCHAR, c.DUCKDB_TYPE_ENUM, c.DUCKDB_TYPE_INTERVAL, c.DUCKDB_TYPE_BIT => return c.duckdb_bind_varchar_length(stmt, bind_index, value, len),
		c.DUCKDB_TYPE_BLOB => return c.duckdb_bind_blob(stmt, bind_index, @ptrCast(value), len),
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

fn bindSlice(stmt: c.duckdb_prepared_statement, bind_index: usize, value: anytype) c_uint {
	const T = @TypeOf(value);
	if (T == []u8 or T == []const u8) {
		// this slice is just a string, it maps to a duckdb text, not a list
		return bindByteArray(stmt, bind_index, value.ptr, value.len);
	}

	// https://github.com/duckdb/duckdb/discussions/7482
	// DuckDB doesn't expose an API for binding arrays.
	bindError(T);
}

fn bindError(comptime T: type) void {
	@compileError("cannot bind value of type " ++ @typeName(T));
}

const t = std.testing;
const DB = @import("db.zig").DB;
test "bind: basic types" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	var rows = try conn.query("select $1, $2, $3, $4, $5, $6", .{
		99,
		-32.01,
		true,
		false,
		@as(?i32, null),
		@as(?i32, 44),
	});
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(99, row.get(i64, 0));
	try t.expectEqual(-32.01, row.get(f64, 1));
	try t.expectEqual(true, row.get(bool, 2));
	try t.expectEqual(false, row.get(bool, 3));
	try t.expectEqual(null, row.get(?i32, 4));
	try t.expectEqual(44, row.get(i32, 5));
}

test "bind: int" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	{
		var rows = try conn.query("select $1, $2, $3, $4, $5, $6::hugeint, $7::uhugeint", .{
			99,
			@as(i8, 2),
			@as(i16, 3),
			@as(i32, 4),
			@as(i64, 5),
			@as(i128, -9955340232221457974987),
			@as(u128, 1267650600228229401496703205376),
		});
		defer rows.deinit();

		const row = (try rows.next()).?;
		try t.expectEqual(99, row.get(i64, 0));
		try t.expectEqual(2, row.get(i8, 1));
		try t.expectEqual(3, row.get(i16,2));
		try t.expectEqual(4, row.get(i32, 3));
		try t.expectEqual(5, row.get(i64, 4));
		try t.expectEqual(-9955340232221457974987, row.get(i128, 5));
		try t.expectEqual(1267650600228229401496703205376, row.get(u128, 6));
	}

	{
		// positive limit
		var rows = try conn.query("select $1, $2, $3, $4, $5", .{
			@as(i8, 127),
			@as(i16, 32767),
			@as(i32, 2147483647),
			@as(i64, 9223372036854775807),
			@as(i128, 170141183460469231731687303715884105727)
		});
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqual(127, row.get(i8, 0));
		try t.expectEqual(32767, row.get(i16,1));
		try t.expectEqual(2147483647, row.get(i32, 2));
		try t.expectEqual(9223372036854775807, row.get(i64, 3));
		try t.expectEqual(170141183460469231731687303715884105727, row.get(i128, 4));
	}

	{
		// negative limit
		var rows = try conn.query("select $1, $2, $3, $4, $5", .{
			@as(i8, -127),
			@as(i16, -32767),
			@as(i32, -2147483647),
			@as(i64, -9223372036854775807),
			@as(i128, -170141183460469231731687303715884105727)
		});
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqual(-127, row.get(i8, 0));
		try t.expectEqual(-32767, row.get(i16,1));
		try t.expectEqual(-2147483647, row.get(i32, 2));
		try t.expectEqual(-9223372036854775807, row.get(i64, 3));
		try t.expectEqual(-170141183460469231731687303715884105727, row.get(i128, 4));
	}

	{
		// unsigned positive limit
		var rows = try conn.query("select $1, $2, $3, $4", .{
			@as(u8, 255),
			@as(u16, 65535),
			@as(u32, 4294967295),
			@as(u64, 18446744073709551615),
		});
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqual(255, row.get(u8, 0));
		try t.expectEqual(65535, row.get(u16, 1));
		try t.expectEqual(4294967295, row.get(u32, 2));
		try t.expectEqual(18446744073709551615, row.get(u64, 3));
	}
}

test "bind: floats" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	// floats
	var rows = try conn.query("select $1, $2, $3", .{
		99.88, // $1
		@as(f32, -3.192), // $2
		@as(f64, 999.182), // $3
	});
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(99.88, row.get(f64, 0));
	try t.expectEqual(-3.192, row.get(f32, 1));
	try t.expectEqual(999.182, row.get(f64, 2));
}

test "bind: decimal" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	// decimal
	var rows = try conn.query("select $1::decimal(3,2), $2::decimal(18,6)", .{
		1.23, // $1
		-0.3291484 // $2
	});
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(1.23, row.get(f64, 0));
	try t.expectEqual(-0.329148, row.get(f64, 1));
}

test "bind: uuid" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	// uuid
	var rows = try conn.query("select $1::uuid, $2::uuid, $3::uuid, $4::uuid", .{"578D0DF0-A76F-4A8E-A463-42F8A4F133C8", "00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff", "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"});
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqualStrings("578d0df0-a76f-4a8e-a463-42f8a4f133c8", &(row.get(UUID, 0)));
	try t.expectEqualStrings("00000000-0000-0000-0000-000000000000", &(row.get(UUID, 1)));
	try t.expectEqualStrings("ffffffff-ffff-ffff-ffff-ffffffffffff", &(row.get(UUID, 2)));
	try t.expectEqualStrings("ffffffff-ffff-ffff-ffff-ffffffffffff", &(row.get(UUID, 3)));
}

test "bind: text" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	{
		var rows = try conn.query("select $1", .{"hello world"});
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqualStrings("hello world", row.get([]u8, 0));
	}

	{
		// runtime varchar
		var list = std.ArrayList([]const u8).init(t.allocator);
		defer list.deinit();
		try list.append("i love keemun");

		var rows = try conn.query("select $1::varchar", .{list.items[0]});
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqualStrings("i love keemun", row.get([]const u8, 0));
	}

	{
		// blob
		var rows = try conn.query("select $1::blob", .{&[_]u8{0, 1, 2}});
		defer rows.deinit();

		const row = (try rows.next()).?;
		try t.expectEqualStrings(&[_]u8{0, 1, 2}, row.get([]const u8, 0));
	}

	{
		// runtime blob
		var list = std.ArrayList([]const u8).init(t.allocator);
		defer list.deinit();
		try list.append("i love keemun2");

		var rows = try conn.query("select $1::blob", .{list.items[0]});
		defer rows.deinit();
		const row = (try rows.next()).?;
		try t.expectEqualStrings("i love keemun2", row.get([]const u8, 0));
	}
}

test "bind: date/time" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	// date & time
	const date = Date{.year = 2023, .month = 5, .day = 10};
	const time = Time{.hour = 21, .min = 4, .sec = 49, .micros = 123456};
	const interval = Interval{.months = 3, .days = 7, .micros = 982810};
	var rows = try conn.query("select $1::date, $2::time, $3::timestamp, $4::interval, $5::interval", .{date, time, 751203002000000, interval, "9298392 days"});
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(date, row.get(Date, 0));
	try t.expectEqual(time, row.get(Time, 1));
	try t.expectEqual(751203002000000, row.get(i64, 2));
	try t.expectEqual(interval, row.get(Interval, 3));
	try t.expectEqual(Interval{.months = 0, .days = 9298392, .micros = 0}, row.get(Interval, 4));
}

test "bind: enum" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec("create type my_type as enum ('type_a', 'type_b')", .{});
	_ = try conn.exec("create type tea_type as enum ('keemun', 'silver_needle')", .{});

	var rows = try conn.query("select $1::my_type, $2::tea_type, $3::my_type", .{"type_a", "keemun", null});
	defer rows.deinit();
	const row = (try rows.next()).?;
	try t.expectEqualStrings("type_a", std.mem.span(row.get(lib.Enum, 0).raw()));
	try t.expectEqualStrings("keemun", try row.get(?lib.Enum, 1).?.rowCache());
	try t.expectEqual(null, row.get(?lib.Enum, 2));
}

test "bind: bistring" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	var rows = try conn.query(
		\\ select $1::bit, $1::bit::varchar union all
		\\ select $2::bit, $2::bit::varchar union all
		\\ select $3::bit, $3::bit::varchar union all
		\\ select $4::bit, $4::bit::varchar union all
		\\ select $5::bit, $5::bit::varchar union all
		\\ select $6::bit, $6::bit::varchar union all
		\\ select $7::bit, $7::bit::varchar union all
		\\ select $8::bit, $8::bit::varchar union all
		\\ select $9::bit, $9::bit::varchar
	, .{"0", "1", "0001111", "010", "101", "1111111110", "101010101010010101010100000101001001", "00000000000000000", "11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111011111111111111111111111111111111111"});
	defer rows.deinit();

	// check that our toString is the same as duckdb's
	while (try rows.next()) |row| {
		const converted = try @import("zuckdb.zig").bitToString(t.allocator, row.get([]u8, 0));
		defer t.allocator.free(converted);
		try t.expectEqualStrings(row.get([]u8, 1), converted);
	}
}

test "bind: dynamic" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	var stmt = try conn.prepare("select $1::int, $2::varchar, $3::smallint", .{});
	defer stmt.deinit();
	try stmt.bindValue(0, null);
	try stmt.bindValue(1, "over");
	try stmt.bindValue(2, 9000);

	var rows = try stmt.query(null);
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(null, row.get(?i32, 0));
	try t.expectEqualStrings("over", row.get([]u8, 1));
	try t.expectEqual(9000, row.get(i16, 2));
}

test "query parameters" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	const stmt = try conn.prepare(\\select
		\\ $1::bool,
		\\ $2::tinyint, $3::smallint, $4::integer, $5::bigint, $6::hugeint,
		\\ $7::utinyint, $8::usmallint, $9::uinteger, $10::ubigint,
		\\ $11::real, $12::double, $13::decimal,
		\\ $14::timestamp, $15::date, $16::time, $17::interval,
		\\ $18::varchar, $19::blob
	, .{.auto_release = false});
	defer stmt.deinit();

	try t.expectEqual(19, stmt.numberOfParameters());

	// bool
	try t.expectEqual(1, stmt.parameterTypeC(0));
	try t.expectEqual(ParameterType.bool, stmt.parameterType(0));

	// int
	try t.expectEqual(2, stmt.parameterTypeC(1));
	try t.expectEqual(ParameterType.i8, stmt.parameterType(1));
	try t.expectEqual(3, stmt.parameterTypeC(2));
	try t.expectEqual(ParameterType.i16, stmt.parameterType(2));
	try t.expectEqual(4, stmt.parameterTypeC(3));
	try t.expectEqual(ParameterType.i32, stmt.parameterType(3));
	try t.expectEqual(5, stmt.parameterTypeC(4));
	try t.expectEqual(ParameterType.i64, stmt.parameterType(4));
	try t.expectEqual(16, stmt.parameterTypeC(5));
	try t.expectEqual(ParameterType.i128, stmt.parameterType(5));

	// uint
	try t.expectEqual(6, stmt.parameterTypeC(6));
	try t.expectEqual(ParameterType.u8, stmt.parameterType(6));
	try t.expectEqual(7, stmt.parameterTypeC(7));
	try t.expectEqual(ParameterType.u16, stmt.parameterType(7));
	try t.expectEqual(8, stmt.parameterTypeC(8));
	try t.expectEqual(ParameterType.u32, stmt.parameterType(8));
	try t.expectEqual(9, stmt.parameterTypeC(9));
	try t.expectEqual(ParameterType.u64, stmt.parameterType(9));

	// float & decimal
	try t.expectEqual(10, stmt.parameterTypeC(10));
	try t.expectEqual(ParameterType.f32, stmt.parameterType(10));
	try t.expectEqual(11, stmt.parameterTypeC(11));
	try t.expectEqual(ParameterType.f64, stmt.parameterType(11));
	try t.expectEqual(20, stmt.parameterTypeC(12));
	try t.expectEqual(ParameterType.decimal, stmt.parameterType(12));

	// time
	try t.expectEqual(12, stmt.parameterTypeC(13));
	try t.expectEqual(ParameterType.timestamp, stmt.parameterType(13));
	try t.expectEqual(13, stmt.parameterTypeC(14));
	try t.expectEqual(ParameterType.date, stmt.parameterType(14));
	try t.expectEqual(14, stmt.parameterTypeC(15));
	try t.expectEqual(ParameterType.time, stmt.parameterType(15));
	try t.expectEqual(15, stmt.parameterTypeC(16));
	try t.expectEqual(ParameterType.interval, stmt.parameterType(16));

	// varchar & blob
	try t.expectEqual(18, stmt.parameterTypeC(17));
	try t.expectEqual(ParameterType.varchar, stmt.parameterType(17));
	try t.expectEqual(19, stmt.parameterTypeC(18));
	try t.expectEqual(ParameterType.blob, stmt.parameterType(18));
}

test "Stmt: exec" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	{
		const stmt = try conn.prepare("create table exec(id integer)", .{});
		defer stmt.deinit();
		try t.expectEqual(0, try stmt.exec());
	}

	{
		const stmt = try conn.prepare("insert into exec (id) values ($1)", .{});
		defer stmt.deinit();
		try stmt.bindValue(0, 2);
		try t.expectEqual(1, try stmt.exec());

		try stmt.clearBindings();

		try stmt.bindValue(0, 3);
		try t.expectEqual(1, try stmt.exec());
	}

	var rows = try conn.query("select id from exec order by id", .{});
	defer rows.deinit();

	try t.expectEqual(2, (try rows.next()).?.get(i32, 0));
	try t.expectEqual(3, (try rows.next()).?.get(i32, 0));
	try t.expectEqual(null, try rows.next());
}
