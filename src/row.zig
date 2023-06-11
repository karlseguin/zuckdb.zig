const std = @import("std");
const zuckdb = @import("zuckdb.zig");
const c = @cImport(@cInclude("zuckdb.h"));

const DB = @import("db.zig").DB;
const Rows = @import("rows.zig").Rows;
const ColumnData = @import("column_data.zig").ColumnData;

const UUID = zuckdb.UUID;
const Time = zuckdb.Time;
const Date = zuckdb.Date;
const Interval = zuckdb.Interval;

pub const Row = struct {
	index: usize,
	columns: []ColumnData,

	pub fn get(self: Row, comptime T: type, col: usize) ?scalarReturn(T) {
		const index = self.index;
		const column = self.columns[col];
		if (isNull(column.validity, index)) return null;

		switch (column.data) {
			.scalar => |scalar| return getScalar(T, scalar, index),
			else => return null,
		}
	}

	pub fn list(self: Row, comptime T: type, col: usize) ?List(T) {
		const index = self.index;
		const column = self.columns[col];
		if (isNull(column.validity, index)) return null;

		switch (column.data) {
			.container => |container| switch (container) {
				.list => |vc| {
					const entry = vc.entries[index];
					return List(T).init(vc.child, vc.validity, entry.offset, entry.length);
				},
			},
			else => return null,
		}
	}
};

// Returned by conn.row / rowZ, wraps a row and rows, the latter so that
// it can be deinit'd
pub const OwningRow = struct {
	row: Row,
	rows: Rows,

	pub fn get(self: OwningRow, comptime T: type, col: usize) ?scalarReturn(T) {
		return self.row.get(T, col);
	}

	pub fn list(self: OwningRow, comptime T: type, col: usize) ?List(T) {
		return self.row.list(T, col);
	}

	pub fn deinit(self: OwningRow) void {
		self.rows.deinit();
	}
};

fn isNull(validity: [*c]u64, index: usize) bool {
	const entry_index = index / 64;
	const entry_mask = index % 64;
	return validity[entry_index] & std.math.shl(u64, 1, entry_mask) == 0;
}

fn scalarReturn(comptime T: type) type {
	return switch (T) {
		[]u8 => []const u8,
		else => T
	};
}

fn getScalar(comptime T: type, scalar: ColumnData.Scalar, index: usize) ?scalarReturn(T) {
	switch (T) {
		[]u8, []const u8 => return getBlob(scalar, index),
		i8 => return getI8(scalar, index),
		i16 => return getI16(scalar, index),
		i32 => return getI32(scalar, index),
		i64 => return getI64(scalar, index),
		i128 => return getI128(scalar, index),
		u8 => return getU8(scalar, index),
		u16 => return getU16(scalar, index),
		u32 => return getU32(scalar, index),
		u64 => return getU64(scalar, index),
		f32 => return getF32(scalar, index),
		f64 => return getF64(scalar, index),
		bool => return getBool(scalar, index),
		Date => return getDate(scalar, index),
		Time => return getTime(scalar, index),
		Interval => return getInterval(scalar, index),
		UUID => return getUUID(scalar, index),
		else => @compileError("Cannot get value of type " ++ @typeName(T)),
	}
}

fn getBlob(scalar: ColumnData.Scalar, index: usize) ?[]const u8 {
	switch (scalar) {
		.blob => |vc| {
			// This sucks. This is an untagged union. But both versions (inlined and pointer)
			// have the same leading 8 bytes, including the length which is the first 4 bytes.
			// There is a c.duckdb_string_is_inlined that we could use instead of hard-coding
			// the 12, but that requires dereferencing value, which I'd like to avoid.
			// For one reason, when inlined, it's easy to accidently pass the address of the local copy
			const value = &vc[index];
			const len = value.value.inlined.length;
			if (len <= 12) {
				return value.value.inlined.inlined[0..len];
			}
			const pointer = value.value.pointer;
			return pointer.ptr[0..len];
		},
		else => return null,
	}
}

fn getI8(scalar: ColumnData.Scalar, index: usize) ?i8 {
	switch (scalar) {
		.i8 => |vc| return vc[index],
		else => return null,
	}
}

fn getI16(scalar: ColumnData.Scalar, index: usize) ?i16 {
	switch (scalar) {
		.i16 => |vc| return vc[index],
		else => return null,
	}
}

fn getI32(scalar: ColumnData.Scalar, index: usize) ?i32 {
	switch (scalar) {
		.i32 => |vc| return vc[index],
		else => return null,
	}
}

fn getI64(scalar: ColumnData.Scalar, index: usize) ?i64 {
	switch (scalar) {
		.i64 => |vc| return vc[index],
		.timestamp => |vc| return vc[index].micros,
		else => return null,
	}
}

fn getI128(scalar: ColumnData.Scalar, index: usize) ?i128 {
	switch (scalar) {
		.i128 => |vc| return vc[index],
		else => return null,
	}
}

// largely taken from duckdb's uuid type
fn getUUID(scalar: ColumnData.Scalar, index: usize) ?UUID {
	const hex = "0123456789abcdef";
	const n = getI128(scalar, index) orelse return null;

	const h = hugeInt(n);

	const u = h.upper ^ (@as(i64, 1) << 63);
	const l = h.lower;

	var buf: [36]u8 = undefined;

	const b1 = @intCast(u8, (u >> 56) & 0xFF);
	buf[0] = hex[b1 >> 4];
	buf[1] = hex[b1 & 0x0f];

	const b2 = @intCast(u8, (u >> 48) & 0xFF);
	buf[2] = hex[b2 >> 4];
	buf[3] = hex[b2 & 0x0f];

	const b3 = @intCast(u8, (u >> 40) & 0xFF);
	buf[4] = hex[b3 >> 4];
	buf[5] = hex[b3 & 0x0f];

	const b4 = @intCast(u8, (u >> 32) & 0xFF);
	buf[6] = hex[b4 >> 4];
	buf[7] = hex[b4 & 0x0f];

	buf[8] = '-';

	const b5 = @intCast(u8, (u >> 24) & 0xFF);
	buf[9] = hex[b5 >> 4];
	buf[10] = hex[b5 & 0x0f];

	const b6 = @intCast(u8, (u >> 16) & 0xFF);
	buf[11] = hex[b6 >> 4];
	buf[12] = hex[b6 & 0x0f];

	buf[13] = '-';

	const b7 = @intCast(u8, (u >> 8) & 0xFF);
	buf[14] = hex[b7 >> 4];
	buf[15] = hex[b7 & 0x0f];

	const b8 = @intCast(u8, u & 0xFF);
	buf[16] = hex[b8 >> 4];
	buf[17] = hex[b8 & 0x0f];

	buf[18] = '-';

	const b9 = @intCast(u8, (l >> 56) & 0xFF);
	buf[19] = hex[b9 >> 4];
	buf[20] = hex[b9 & 0x0f];

	const b10 = @intCast(u8, (l >> 48) & 0xFF);
	buf[21] = hex[b10 >> 4];
	buf[22] = hex[b10 & 0x0f];

	buf[23] = '-';

	const b11 = @intCast(u8, (l >> 40) & 0xFF);
	buf[24] = hex[b11 >> 4];
	buf[25] = hex[b11 & 0x0f];

	const b12 = @intCast(u8, (l >> 32) & 0xFF);
	buf[26] = hex[b12 >> 4];
	buf[27] = hex[b12 & 0x0f];

	const b13 = @intCast(u8, (l >> 24) & 0xFF);
	buf[28] = hex[b13 >> 4];
	buf[29] = hex[b13 & 0x0f];

	const b14 = @intCast(u8, (l >> 16) & 0xFF);
	buf[30] = hex[b14 >> 4];
	buf[31] = hex[b14 & 0x0f];

	const b15 = @intCast(u8, (l >> 8) & 0xFF);
	buf[32] = hex[b15 >> 4];
	buf[33] = hex[b15 & 0x0f];

	const b16 = @intCast(u8, l & 0xFF);
	buf[34] = hex[b16 >> 4];
	buf[35] = hex[b16 & 0x0f];

	return buf;
}

fn getU8(scalar: ColumnData.Scalar, index: usize) ?u8 {
	switch (scalar) {
		.u8 => |vc| return vc[index],
		else => return null,
	}
}

fn getU16(scalar: ColumnData.Scalar, index: usize) ?u16 {
	switch (scalar) {
		.u16 => |vc| return vc[index],
		else => return null,
	}
}

fn getU32(scalar: ColumnData.Scalar, index: usize) ?u32 {
	switch (scalar) {
		.u32 => |vc| return vc[index],
		else => return null,
	}
}

fn getU64(scalar: ColumnData.Scalar, index: usize) ?u64 {
	switch (scalar) {
		.u64 => |vc| return vc[index],
		else => return null,
	}
}

fn getBool(scalar: ColumnData.Scalar, index: usize) ?bool {
	switch (scalar) {
		.bool => |vc| return vc[index],
		else => return null,
	}
}

fn getF32(scalar: ColumnData.Scalar, index: usize) ?f32 {
	switch (scalar) {
		.f32 => |vc| return vc[index],
		else => return null,
	}
}

fn getF64(scalar: ColumnData.Scalar, index: usize) ?f64 {
	switch (scalar) {
		.f64 => |vc| return vc[index],
		.decimal => |vc| {
			const value = switch (vc.internal) {
				inline else => |internal| hugeInt(internal[index]),
			};
			return c.duckdb_decimal_to_double(c.duckdb_decimal{
				.width = vc.width,
				.scale = vc.scale,
				.value = value,
			});
		},
		else => return null,
	}
}

fn getDate(scalar: ColumnData.Scalar, index: usize) ?Date {
	switch (scalar) {
		.date => |vc| return c.duckdb_from_date(vc[index]),
		else => return null,
	}
}

fn getTime(scalar: ColumnData.Scalar, index: usize) ?Time {
	switch (scalar) {
		.time => |vc| return c.duckdb_from_time(vc[index]),
		else => return null,
	}
}

fn getInterval(scalar: ColumnData.Scalar, index: usize) ?Interval {
	switch (scalar) {
		.interval => |vc| return vc[index],
		else => return null,
	}
}

pub fn List(comptime T: type) type {
	return struct{
		len: usize,
		_validity: [*c]u64,
		_offset: usize,
		_scalar: ColumnData.Scalar,

		const Self = @This();

		fn init(scalar: ColumnData.Scalar, validity: [*c]u64, offset: usize, length: usize) Self {
			return .{
				.len = length,
				._offset = offset,
				._scalar = scalar,
				._validity = validity,
			};
		}

		pub fn get(self: Self, i: usize) ?T {
			const index = i + self._offset;
			if (isNull(self._validity, index)) return null;
			return getScalar(T, self._scalar, index);
		}
	};
}

pub fn hugeInt(value: i128) c.duckdb_hugeint {
	return .{
		.lower = @intCast(u64, @mod(value, 18446744073709551616)),
		.upper = @intCast(i64, @divFloor(value, 18446744073709551616)),
	};
}

const t = std.testing;
// Test this specifically since there's special handling based on the length
// of the column (inlined vs pointer)
test "read varchar" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		var rows = conn.queryZ(\\
			\\ select '1' union all
			\\ select '12345' union all
			\\ select '123456789A' union all
			\\ select '123456789AB' union all
			\\ select '123456789ABC' union all
			\\ select '123456789ABCD' union all
			\\ select '123456789ABCDE' union all
			\\ select '123456789ABCDEF' union all
			\\ select null
		, .{}).ok;
		defer rows.deinit();

		try t.expectEqualStrings("1", (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqualStrings("12345", (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqualStrings("123456789A", (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqualStrings("123456789AB", (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqualStrings("123456789ABC", (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqualStrings("123456789ABCD", (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqualStrings("123456789ABCDE", (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqualStrings("123456789ABCDEF", (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqual(@as(?[]const u8, null), (try rows.next()).?.get([]const u8, 0));
		try t.expectEqual(@as(?Row, null), try rows.next());
	}
}

// Test this specifically since there's special handling based on the length
// of the column (inlined vs pointer)
test "read blob" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		var rows = conn.queryZ(\\
			\\ select '\xAA'::blob union all
			\\ select '\xAA\xAA\xAA\xAA\xAB'::blob union all
			\\ select '\xAA\xAA\xAA\xAA\xAB\xAA\xAA\xAA\xAA\xAB\xAA\xAA\xAA\xAA\xAB'::blob union all
			\\ select null
		, .{}).ok;
		defer rows.deinit();

		try t.expectEqualSlices(u8, @as([]const u8, &.{170}), (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqualSlices(u8, @as([]const u8, &.{170, 170, 170, 170, 171}), (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqualSlices(u8, @as([]const u8, &.{170, 170, 170, 170, 171, 170, 170, 170, 170, 171, 170, 170, 170, 170, 171}), (try rows.next()).?.get([]const u8, 0).?);
		try t.expectEqual(@as(?[]const u8, null), (try rows.next()).?.get([]const u8, 0));
		try t.expectEqual(@as(?Row, null), try rows.next());
	}
}

test "read ints" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		var rows = conn.queryZ(\\
			\\ select 0::tinyint, 0::smallint, 0::integer, 0::bigint, 0::hugeint, 0::utinyint, 0::usmallint, 0::uinteger, 0::ubigint
			\\ union all
			\\ select 127::tinyint, 32767::smallint, 2147483647::integer, 9223372036854775807::bigint, 170141183460469231731687303715884105727::hugeint, 255::utinyint, 65535::usmallint, 4294967295::uinteger, 18446744073709551615::ubigint
			\\ union all
			\\ select -127::tinyint, -32767::smallint, -2147483647::integer, -9223372036854775807::bigint, -170141183460469231731687303715884105727::hugeint, 0::utinyint, 0::usmallint, 0::uinteger, 0::ubigint
			\\ union all
			\\ select null, null, null, null, null, null, null, null, null
		, .{}).ok;
		defer rows.deinit();

		var row = (try rows.next()) orelse unreachable;
		try t.expectEqual(@as(i8, 0), row.get(i8, 0).?);
		try t.expectEqual(@as(i16, 0), row.get(i16,1).?);
		try t.expectEqual(@as(i32, 0), row.get(i32, 2).?);
		try t.expectEqual(@as(i64, 0), row.get(i64, 3).?);
		try t.expectEqual(@as(i128, 0), row.get(i128, 4).?);
		try t.expectEqual(@as(u8, 0), row.get(u8, 5).?);
		try t.expectEqual(@as(u16, 0), row.get(u16, 6).?);
		try t.expectEqual(@as(u32, 0), row.get(u32, 7).?);
		try t.expectEqual(@as(u64, 0), row.get(u64, 8).?);

		row = (try rows.next()) orelse unreachable;
		try t.expectEqual(@as(i8, 127), row.get(i8, 0).?);
		try t.expectEqual(@as(i16, 32767), row.get(i16,1).?);
		try t.expectEqual(@as(i32, 2147483647), row.get(i32, 2).?);
		try t.expectEqual(@as(i64, 9223372036854775807), row.get(i64, 3).?);
		try t.expectEqual(@as(i128, 170141183460469231731687303715884105727), row.get(i128, 4).?);
		try t.expectEqual(@as(u8, 255), row.get(u8, 5).?);
		try t.expectEqual(@as(u16, 65535), row.get(u16, 6).?);
		try t.expectEqual(@as(u32, 4294967295), row.get(u32, 7).?);
		try t.expectEqual(@as(u64, 18446744073709551615), row.get(u64, 8).?);

		row = (try rows.next()) orelse unreachable;
		try t.expectEqual(@as(i8, -127), row.get(i8, 0).?);
		try t.expectEqual(@as(i16, -32767), row.get(i16,1).?);
		try t.expectEqual(@as(i32, -2147483647), row.get(i32, 2).?);
		try t.expectEqual(@as(i64, -9223372036854775807), row.get(i64, 3).?);
		try t.expectEqual(@as(i128, -170141183460469231731687303715884105727), row.get(i128, 4).?);

		row = (try rows.next()) orelse unreachable;
		try t.expectEqual(@as(?i8, null), row.get(i8, 0));
		try t.expectEqual(@as(?i16, null), row.get(i16,1));
		try t.expectEqual(@as(?i32, null), row.get(i32, 2));
		try t.expectEqual(@as(?i64, null), row.get(i64, 3));
		try t.expectEqual(@as(?i128, null), row.get(i128, 4));
		try t.expectEqual(@as(?u8, null), row.get(u8, 5));
		try t.expectEqual(@as(?u16, null), row.get(u16, 6));
		try t.expectEqual(@as(?u32, null), row.get(u32, 7));
		try t.expectEqual(@as(?u64, null), row.get(u64, 8));

		try t.expectEqual(@as(?Row, null), try rows.next());
	}
}

test "read bool" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		var rows = conn.queryZ("select 0::bool, 1::bool, null::bool", .{}).ok;
		defer rows.deinit();

		var row = (try rows.next()) orelse unreachable;
		try t.expectEqual(false, row.get(bool, 0).?);
		try t.expectEqual(true, row.get(bool, 1).?);
		try t.expectEqual(@as(?bool, null), row.get(bool, 2));

		try t.expectEqual(@as(?Row, null), try rows.next());
	}
}

test "read float" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		var rows = conn.queryZ("select 32.329::real, -0.29291::double, null::real, null::double", .{}).ok;
		defer rows.deinit();

		var row = (try rows.next()) orelse unreachable;
		try t.expectEqual(@as(f32, 32.329), row.get(f32, 0).?);
		try t.expectEqual(@as(f64, -0.29291), row.get(f64, 1).?);
		try t.expectEqual(@as(?f32, null), row.get(f32, 2));
		try t.expectEqual(@as(?f64, null), row.get(f64, 3));

		try t.expectEqual(@as(?Row, null), try rows.next());
	}
}

test "read decimal" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		// decimals (representation is different based on the width)
		var rows = conn.query("select 1.23::decimal(3,2), 1.24::decimal(8, 4), 1.25::decimal(12, 5), 1.26::decimal(18, 3), 1.27::decimal(35, 4)", .{}).ok;
		defer rows.deinit();

		const row = (try rows.next()).?;
		try t.expectEqual(@as(f64, 1.23), row.get(f64, 0).?);
		try t.expectEqual(@as(f64, 1.24), row.get(f64, 1).?);
		try t.expectEqual(@as(f64, 1.25), row.get(f64, 2).?);
		try t.expectEqual(@as(f64, 1.26), row.get(f64, 3).?);
		try t.expectEqual(@as(f64, 1.27), row.get(f64, 4).?);
	}
}

test "read date & time" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		var rows = conn.queryZ("select date '1992-09-20', time '14:21:13.332', timestamp '1993-10-21 11:30:02'", .{}).ok;
		defer rows.deinit();

		var row = (try rows.next()) orelse unreachable;
		try t.expectEqual(Date{.year = 1992, .month = 9, .day = 20}, row.get(Date, 0).?);
		try t.expectEqual(Time{.hour = 14, .min = 21, .sec = 13, .micros = 332000}, row.get(Time, 1).?);
		try t.expectEqual(@as(?i64, 751203002000000), row.get(i64, 2).?);
	}
}

test "list" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		var rows = conn.queryZ("select [1, 32, 99, null, -4]::int[]", .{}).ok;
		defer rows.deinit();

		var row = (try rows.next()) orelse unreachable;
		const list = row.list(i32, 0).?;
		try t.expectEqual(@as(usize, 5), list.len);
		try t.expectEqual(@as(i32, 1), list.get(0).?);
		try t.expectEqual(@as(i32, 32), list.get(1).?);
		try t.expectEqual(@as(i32, 99), list.get(2).?);
		try t.expectEqual(@as(?i32, null), list.get(3));
		try t.expectEqual(@as(i32, -4), list.get(4).?);
	}
}

test "owning row" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		// error case
		const result = conn.row("select x", .{});
		defer result.deinit();
		try t.expectEqualStrings("Binder Error: Referenced column \"x\" not found in FROM clause!\nLINE 1: select x\n               ^", result.err.desc);
	}

	{
		// null
		const result = conn.row("select 1 where false", .{});
		defer result.deinit();
		try t.expectEqual(@as(?OwningRow, null), result.ok);
	}

	{
		const row = (try conn.rowZ("select $1::bigint", .{-991823891832}).unwrap()).?;
		defer row.deinit();
		try t.expectEqual(@as(i64, -991823891832), row.get(i64, 0).?);
	}
}
