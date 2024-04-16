const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const Conn = lib.Conn;
const Time = lib.Time;
const Date = lib.Date;
const Interval = lib.Interval;
const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

pub const Appender = struct {
	column_index: usize,
	allocator: Allocator,
	appender: *c.duckdb_appender,
	type_lookup: std.AutoHashMapUnmanaged(usize, c.duckdb_type),

	pub fn init(allocator: Allocator, appender: *c.duckdb_appender) Appender {
		return .{
			.column_index = 0,
			.appender = appender,
			.allocator = allocator,
			.type_lookup = std.AutoHashMapUnmanaged(usize, c.duckdb_type){},
		};
	}

	pub fn deinit(self: *Appender) void {
		const appender = self.appender;
		_ = c.duckdb_appender_destroy(appender);
		self.allocator.destroy(appender);
		self.type_lookup.deinit(self.allocator);
	}

	pub fn flush(self: Appender) !void {
		if (c.duckdb_appender_flush(self.appender.*) == DuckDBError) {
			return error.DuckDBError;
		}
	}

	pub fn close(self: Appender) !void {
		if (c.duckdb_appender_close(self.appender.*) == DuckDBError) {
			return error.DuckDBError;
		}
	}

	pub fn endRow(self: *Appender) !void {
		self.column_index = 0;
		if (c.duckdb_appender_end_row(self.appender.*) == DuckDBError) {
			return error.DuckDBError;
		}
	}

	pub fn err(self: Appender) ?[]const u8 {
		const c_err = c.duckdb_appender_error(self.appender.*);
		return if (c_err) |e| return std.mem.span(e) else null;
	}

	pub fn getType(self: *Appender) !c.duckdb_type {
		const gop = try self.type_lookup.getOrPut(self.allocator, self.column_index);
		if (gop.found_existing == false) {
			var logical_type = c.duckdb_appender_column_type(self.appender.*, self.column_index);
			defer c.duckdb_destroy_logical_type(&logical_type);
			gop.value_ptr.* = c.duckdb_get_type_id(logical_type);
		}
		return gop.value_ptr.*;
	}

	pub fn append(self: *Appender, value: anytype) !void {
		const column_index = self.column_index;
		defer self.column_index = column_index + 1;
		return self.appendValue(value);
	}

	fn appendValue(self: *Appender, value: anytype) !void {
		var rc: c_uint = 0;
		const T = @TypeOf(value);

		const appender = self.appender.*;
		switch (@typeInfo(T)) {
			.Null => rc = c.duckdb_append_null(appender),
			.ComptimeInt => rc = try self.appendI64(@intCast(value)),
			.ComptimeFloat => rc = c.duckdb_append_double(appender, @floatCast(value)),
			.Int => |int| {
				if (int.signedness == .signed) {
					switch (int.bits) {
						1...8 => rc = c.duckdb_append_int8(appender, @intCast(value)),
						9...16 => rc = c.duckdb_append_int16(appender, @intCast(value)),
						17...32 => rc = c.duckdb_append_int32(appender, @intCast(value)),
						33...63 => rc = c.duckdb_append_int64(appender, @intCast(value)),
						64 => rc = try self.appendI64(value),
						65...128 => rc = c.duckdb_append_hugeint(appender, lib.hugeInt(@intCast(value))),
						else => appendTypeError(T),
					}
				} else {
					switch (int.bits) {
						1...8 => rc = c.duckdb_append_uint8(appender, @intCast(value)),
						9...16 => rc = c.duckdb_append_uint16(appender, @intCast(value)),
						17...32 => rc = c.duckdb_append_uint32(appender, @intCast(value)),
						33...64 => rc = c.duckdb_append_uint64(appender, @intCast(value)),
						65...128 => rc = c.duckdb_append_uhugeint(appender, lib.uhugeInt(@intCast(value))),
						else => appendTypeError(T),
					}
				}
			},
			.Float => |float| {
				switch (float.bits) {
					1...32 => rc = c.duckdb_append_float(appender,  @floatCast(value)),
					33...64 => rc = c.duckdb_append_double(appender,  @floatCast(value)),
					else => appendTypeError(T),
				}
			},
			.Bool => rc = c.duckdb_append_bool(appender, value),
			.Pointer => |ptr| {
				switch (ptr.size) {
					.Slice => rc = try self.appendSlice(@as([]const ptr.child, value)),
					.One => switch (@typeInfo(ptr.child)) {
						.Array => {
							const Slice = []const std.meta.Elem(ptr.child);
							rc = try self.appendSlice(@as(Slice, value));
						},
						else => appendTypeError(T),
					},
					else => appendTypeError(T),
				}
			},
			.Array => try self.appendValue(&value),
			.Optional => {
				if (value) |v| {
					try self.appendValue(v);
				} else {
					rc = c.duckdb_append_null(appender);
				}
			},
			.Struct => {
				if (T == Date) {
					rc = c.duckdb_append_date(appender, c.duckdb_to_date(value));
				} else if (T == Time) {
					rc = c.duckdb_append_time(appender, c.duckdb_to_time(value));
				} else if (T == Interval) {
					rc = c.duckdb_append_interval(appender, value);
				} else {
					appendTypeError(T);
				}
			},
			else => appendTypeError(T),
		}

		if (rc == DuckDBError) {
			return error.Bind;
		}
	}

	fn appendI64(self: *Appender, value: i64) !c_uint {
		switch (try self.getType()) {
			c.DUCKDB_TYPE_TIMESTAMP => return c.duckdb_append_timestamp(self.appender.*, .{.micros = value}),
			else => return c.duckdb_append_int64(self.appender.*, value),
		}
	}

	fn appendSlice(self: *Appender, value: anytype) !c_uint {
		const T = @TypeOf(value);
		if (T == []u8 or T == []const u8) {
			// this slice is just a string, it maps to a duckdb text, not a list
			return self.appendByteArray(value.ptr, value.len);
		}

		// https://github.com/duckdb/duckdb/discussions/7482
		// DuckDB doesn't expose an API for appending arrays.
		appendTypeError(T);
	}

	fn appendByteArray(self: *Appender, value: [*c]const u8, len: usize) !c_uint {
		const appender = self.appender.*;
		switch (try self.getType()) {
			c.DUCKDB_TYPE_VARCHAR, c.DUCKDB_TYPE_ENUM, c.DUCKDB_TYPE_INTERVAL, c.DUCKDB_TYPE_BIT => return c.duckdb_append_varchar_length(appender, value, len),
			c.DUCKDB_TYPE_BLOB => return c.duckdb_append_blob(appender, @ptrCast(value), len),
			c.DUCKDB_TYPE_UUID => {
				if (len != 36) return DuckDBError;
				return c.duckdb_append_varchar_length(appender, value, 36);
			},
			// this one is weird, but duckdb will return DUCKDB_TYPE_INVALID if it doesn't
			// know the type, such as: "select $1", but binding will still work
			c.DUCKDB_TYPE_INVALID => return c.duckdb_append_varchar_length(appender, value, len),
			else => return DuckDBError,
		}
	}
};

fn appendTypeError(comptime T: type) void {
	@compileError("cannot append value of type " ++ @typeName(T));
}

const t = std.testing;
const DB = lib.DB;
test "Appender: basic types" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec(
		\\ create table x (
		\\   col_tinyint tinyint,
		\\   col_smallint smallint,
		\\   col_integer integer,
		\\   col_bigint bigint,
		\\   col_hugeint hugeint,
		\\   col_utinyint utinyint,
		\\   col_usmallint usmallint,
		\\   col_uinteger uinteger,
		\\   col_ubigint ubigint,
		\\   col_uhugeint uhugeint,
		\\   col_real real,
		\\   col_double double,
		\\   col_bool bool,
		\\   col_text text,
		\\   col_blob blob,
		\\   col_uuid uuid,
		\\   col_date date,
		\\   col_time time,
		\\   col_interval interval,
		\\   col_timestamp timestamp,
		\\   col_integer_null integer null
		\\ )
	, .{});

	var appender = try conn.appender(null, "x");
	try appender.append(@as(i8, -128));
	try appender.append(@as(i16, -32768));
	try appender.append(@as(i32, -2147483648));
	try appender.append(@as(i64, -9223372036854775808));
	try appender.append(@as(i128, -170141183460469231731687303715884105728));
	try appender.append(@as(u8, 255));
	try appender.append(@as(u16, 65535));
	try appender.append(@as(u32, 4294967295));
	try appender.append(@as(u64, 18446744073709551615));
	try appender.append(@as(u128, 340282366920938463463374607431768211455));
	try appender.append(@as(f32, -1.23));
	try appender.append(@as(f64, 1994.848288123));
	try appender.append(true);
	try appender.append("over 9000!");
	try appender.append(&[_]u8{1, 2, 3, 254});
	try appender.append("34c667cd-638e-40c2-b256-0f78ccab7013");
	try appender.append(Date{.year = 2023, .month = 5, .day = 10});
	try appender.append(Time{.hour = 21, .min = 4, .sec = 49, .micros = 123456});
	try appender.append(Interval{.months = 3, .days = 7, .micros = 982810});
	try appender.append(1711506018088167);
	try appender.append(null);
	try appender.endRow();

	try t.expectEqual(null, appender.err());
	appender.deinit();

	var row = (try conn.row("select * from x", .{})).?;
	defer row.deinit();
	try t.expectEqual(-128, row.get(i8, 0));
	try t.expectEqual(-32768, row.get(i16, 1));
	try t.expectEqual(-2147483648, row.get(i32, 2));
	try t.expectEqual(-9223372036854775808, row.get(i64, 3));
	try t.expectEqual(-170141183460469231731687303715884105728, row.get(i128, 4));
	try t.expectEqual(255, row.get(u8, 5));
	try t.expectEqual(65535, row.get(u16, 6));
	try t.expectEqual(4294967295, row.get(u32, 7));
	try t.expectEqual(18446744073709551615, row.get(u64, 8));
	try t.expectEqual(340282366920938463463374607431768211455, row.get(u128, 9));
	try t.expectEqual(-1.23, row.get(f32, 10));
	try t.expectEqual(1994.848288123, row.get(f64, 11));
	try t.expectEqual(true, row.get(bool, 12));
	try t.expectEqualStrings("over 9000!", row.get([]u8, 13));
	try t.expectEqualStrings(&[_]u8{1, 2, 3, 254}, row.get([]u8, 14));
	try t.expectEqualStrings("34c667cd-638e-40c2-b256-0f78ccab7013", &row.get(lib.UUID, 15));
	try t.expectEqual(Date{.year = 2023, .month = 5, .day = 10}, row.get(Date, 16));
	try t.expectEqual(Time{.hour = 21, .min = 4, .sec = 49, .micros = 123456}, row.get(Time, 17));
	try t.expectEqual(Interval{.months = 3, .days = 7, .micros = 982810}, row.get(Interval, 18));
	try t.expectEqual(1711506018088167, row.get(i64, 19));
	try t.expectEqual(null, row.get(?i64, 20));
}
