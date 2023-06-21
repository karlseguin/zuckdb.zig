const std = @import("std");
const typed = @import("typed");
const zuckdb = @import("zuckdb.zig");
const c = @cImport(@cInclude("zuckdb.h"));

const DB = @import("db.zig").DB;
const Rows = @import("rows.zig").Rows;
const ColumnData = @import("column_data.zig").ColumnData;

const UUID = zuckdb.UUID;
const Time = zuckdb.Time;
const Date = zuckdb.Date;
const Interval = zuckdb.Interval;
const ParameterType = zuckdb.ParameterType;

const Allocator = std.mem.Allocator;

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

	pub fn getList(self: Row, col: usize) ?List {
		const index = self.index;
		const column = self.columns[col];
		if (isNull(column.validity, index)) return null;

		switch (column.data) {
			.container => |container| switch (container) {
				.list => |vc| {
					const entry = vc.entries[index];
					return List.init(col, vc.type, vc.child, vc.validity, entry.offset, entry.length);
				},
			},
			else => return null,
		}
	}

	pub fn getEnum(self: Row, col: usize) !?[]const u8 {
		const index = self.index;
		const column = self.columns[col];
		if (isNull(column.validity, index)) return null;

		switch (column.data) {
			.scalar => |scalar| return _getEnum(scalar, col, index),
			else => return null,
		}
	}

	pub fn toMap(self: Row, builder: *MapBuilder) !typed.Map {
		const types = builder.types;

		var aa = builder.arena.allocator();
		var map = typed.Map.init(aa);
		try map.ensureTotalCapacity(@intCast(u32, types.len));
		errdefer map.deinit();

		for (types, builder.names, 0..) |tpe, name, i| {
			switch (tpe) {
				.varchar, .blob => {
					if (self.get([]u8, i)) |value| {
						map.putAssumeCapacity(name, try aa.dupe(u8, value));
					} else {
						map.putAssumeCapacity(name, null);
					}
				},
				.bool => map.putAssumeCapacity(name, self.get(bool, i)),
				.i8 => map.putAssumeCapacity(name, self.get(i8, i)),
				.i16 => map.putAssumeCapacity(name, self.get(i16, i)),
				.i32 => map.putAssumeCapacity(name, self.get(i32, i)),
				.i64 => map.putAssumeCapacity(name, self.get(i64, i)),
				.u8 => map.putAssumeCapacity(name, self.get(u8, i)),
				.u16 => map.putAssumeCapacity(name, self.get(u16, i)),
				.u32 => map.putAssumeCapacity(name, self.get(u32, i)),
				.u64 => map.putAssumeCapacity(name, self.get(u64, i)),
				.f32 => map.putAssumeCapacity(name, self.get(f32, i)),
				.f64, .decimal => map.putAssumeCapacity(name, self.get(f64, i)),
				.i128 => map.putAssumeCapacity(name, self.get(i128, i)),
				.uuid => {
					if (self.get(zuckdb.UUID, i)) |uuid| {
						map.putAssumeCapacity(name, try aa.dupe(u8, &uuid));
					} else {
						map.putAssumeCapacity(name, null);
					}
				},
				.date => {
					if (self.get(zuckdb.Date, i)) |date| {
						map.putAssumeCapacity(name, typed.Date{
							.year = @intCast(i16, date.year),
							.month = @intCast(u8, date.month),
							.day = @intCast(u8, date.day),
						});
					} else {
						map.putAssumeCapacity(name, null);
					}
				},
				.time => {
					if (self.get(zuckdb.Time, i)) |time| {
						map.putAssumeCapacity(name, typed.Time{
							.hour = @intCast(u8, time.hour),
							.min =  @intCast(u8, time.min),
							.sec =  @intCast(u8, time.sec),
						});
					} else {
						map.putAssumeCapacity(name, null);
					}
				},
				.timestamp => {
					if (self.get(i64, i)) |micros| {
						map.putAssumeCapacity(name, typed.Timestamp{.micros = micros});
					} else {
						map.putAssumeCapacity(name, null);
					}
				},
				else => return error.UnsupportedMapType,
			}
		}
		return map;
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

	pub fn getEnum(self: OwningRow, col: usize) !?[]const u8 {
		return self.row.getEnum(col);
	}

	pub fn getList(self: OwningRow, comptime T: type, col: usize) ?List(scalarReturn(T)) {
		return self.row.getList(T, col);
	}

	pub fn deinit(self: OwningRow) void {
		self.rows.deinit();
	}

	pub fn mapBuilder(self: OwningRow, allocator: Allocator) !MapBuilder {
		return try self.rows.mapBuilder(allocator);
	}

	pub fn toMap(self: OwningRow, builder: *MapBuilder) !typed.Map {
		return self.row.toMap(builder);
	}
};

pub const MapBuilder = struct {
	arena: std.heap.ArenaAllocator,
	types: []ParameterType,
	names: [][]const u8,

	pub fn deinit(self: MapBuilder) void {
		self.arena.deinit();
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

fn _getEnum(scalar: ColumnData.Scalar, col: usize, index: usize) !?[]const u8 {
	switch (scalar) {
		.@"enum" => |enm| {
			const enum_index = switch (enm.internal) {
				.u8 => |internal| internal[index],
				.u16 => |internal| internal[index],
				.u32 => |internal| internal[index],
				.u64 => |internal| internal[index],
			};
			var rows = enm.rows;
			var gop1 = try rows.enum_name_cache.getOrPut(col);
			if (!gop1.found_existing) {
				gop1.value_ptr.* = std.AutoHashMap(u64, []const u8).init(rows.arena);
			}

			var gop2 = try gop1.value_ptr.getOrPut(enum_index);
			if (!gop2.found_existing) {
				const string_value = c.duckdb_enum_dictionary_value(enm.logical_type, enum_index);
				gop2.value_ptr.* = try rows.arena.dupe(u8, std.mem.span(string_value));
			}
			return gop2.value_ptr.*;
		},
		else => return null,
	}
}

pub const List = struct {
	len: usize,
	col: usize,
	type: ParameterType,
	_validity: [*c]u64,
	_offset: usize,
	_scalar: ColumnData.Scalar,

	fn init(col: usize, parameter_type: ParameterType, scalar: ColumnData.Scalar, validity: [*c]u64, offset: usize, length: usize) List {
		return .{
			.col = col,
			.len = length,
			.type = parameter_type,
			._offset = offset,
			._scalar = scalar,
			._validity = validity,
		};
	}

	pub fn get(self: List, comptime T: type, i: usize) ?scalarReturn(T) {
		const index = i + self._offset;
		if (isNull(self._validity, index)) return null;
		return getScalar(T, self._scalar, index);
	}

	pub fn getEnum(self: List, i: usize) !?[]const u8 {
		const index = i + self._offset;
		if (isNull(self._validity, index)) return null;
		return _getEnum(self._scalar, self.col, index);
	}
};

pub const ListInfo = struct {
	len: usize,
	type: ParameterType,
};

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

test "read list" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	conn.execZ("create type my_type as enum ('type_a', 'type_b')") catch unreachable;

	{
		var rows = conn.queryZ("select [1, 32, 99, null, -4]::int[]", .{}).ok;
		defer rows.deinit();

		var row = (try rows.next()) orelse unreachable;

		const list = row.getList(0).?;
		try t.expectEqual(ParameterType.i32, list.type);
		try t.expectEqual(@as(usize, 5), list.len);
		try t.expectEqual(@as(i32, 1), list.get(i32, 0).?);
		try t.expectEqual(@as(i32, 32), list.get(i32, 1).?);
		try t.expectEqual(@as(i32, 99), list.get(i32, 2).?);
		try t.expectEqual(@as(?i32, null), list.get(i32, 3));
		try t.expectEqual(@as(i32, -4), list.get(i32, 4).?);
	}

	{
		var rows = conn.queryZ("select ['tag1', null, 'tag2']::varchar[]", .{}).ok;
		defer rows.deinit();

		var row = (try rows.next()) orelse unreachable;
		const list = row.getList(0).?;
		try t.expectEqual(ParameterType.varchar, list.type);
		try t.expectEqual(@as(usize, 3), list.len);
		try t.expectEqualStrings("tag1", list.get([]u8, 0).?);
		try t.expectEqual(@as(?[]const u8, null), list.get([]u8, 1));
		try t.expectEqualStrings("tag2", list.get([]u8, 2).?);
	}

	{
		var rows = conn.queryZ("select ['tag1', null, 'tag2']::varchar[]", .{}).ok;
		defer rows.deinit();

		var row = (try rows.next()) orelse unreachable;
		const list = row.getList(0).?;
		try t.expectEqual(ParameterType.varchar, list.type);
		try t.expectEqual(@as(usize, 3), list.len);
		try t.expectEqualStrings("tag1", list.get([]u8, 0).?);
		try t.expectEqual(@as(?[]const u8, null), list.get([]u8, 1));
		try t.expectEqualStrings("tag2", list.get([]u8, 2).?);
	}

	{
		var rows = conn.queryZ("select ['type_a', null, 'type_b', 'type_a']::my_type[]", .{}).ok;
		defer rows.deinit();

		var row = (try rows.next()) orelse unreachable;
		const list = row.getList(0).?;
		try t.expectEqual(ParameterType.@"enum", list.type);
		try t.expectEqual(@as(usize, 4), list.len);
		try t.expectEqualStrings("type_a", (try list.getEnum(0)).?);
		try t.expectEqual(@as(?[]const u8, null), try list.getEnum(1));
		try t.expectEqualStrings("type_b", (try list.getEnum(2)).?);
		try t.expectEqualStrings("type_a", (try list.getEnum(3)).?);
	}
}

// There's some internal caching with this, so we need to test mulitple rows
test "read enum" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	conn.execZ("create type my_type as enum ('type_a', 'type_b')") catch unreachable;
	conn.execZ("create type tea_type as enum ('keemun', 'silver_needle')") catch unreachable;

	var rows = conn.queryZ(
		\\ select 'type_a'::my_type, 'type_b'::my_type, null::my_type, 'type_a'::my_type, 'keemun'::tea_type, 'silver_needle'::tea_type, null::tea_type, 'silver_needle'::tea_type
		\\ union all
		\\ select 'type_b'::my_type, null::my_type, 'type_a'::my_type, 'type_b'::my_type, 'keemun'::tea_type, 'silver_needle'::tea_type, null::tea_type, 'silver_needle'::tea_type
	, .{}).ok;
	defer rows.deinit();

	var row = (try rows.next()) orelse unreachable;
	try t.expectEqualStrings("type_a", (try row.getEnum(0)).?);
	try t.expectEqualStrings("type_b", (try row.getEnum(1)).?);
	try t.expectEqual(@as(?[]const u8, null), (try row.getEnum(2)));
	try t.expectEqualStrings("type_a", (try row.getEnum(3)).?);
	try t.expectEqualStrings("keemun", (try row.getEnum(4)).?);
	try t.expectEqualStrings("silver_needle", (try row.getEnum(5)).?);
	try t.expectEqual(@as(?[]const u8, null), (try row.getEnum(6)));
	try t.expectEqualStrings("silver_needle", (try row.getEnum(7)).?);

	row = (try rows.next()) orelse unreachable;
	try t.expectEqualStrings("type_b", (try row.getEnum(0)).?);
	try t.expectEqual(@as(?[]const u8, null), (try row.getEnum(1)));
	try t.expectEqualStrings("type_a", (try row.getEnum(2)).?);
	try t.expectEqualStrings("type_b", (try row.getEnum(3)).?);
	try t.expectEqualStrings("keemun", (try row.getEnum(4)).?);
	try t.expectEqualStrings("silver_needle", (try row.getEnum(5)).?);
	try t.expectEqual(@as(?[]const u8, null), (try row.getEnum(6)));
	try t.expectEqualStrings("silver_needle", (try row.getEnum(7)).?);
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

test "row: toMap" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		var rows = conn.queryZ(
			\\ select true as the_truth, false as not_the_truth, null::bool as n_truth,
			\\
			\\   32.329::real as a, -0.29291::double as b, null::real as c, null::double as d,
			\\
			\\   127::tinyint as ti, -32767::smallint as si, 2147483647::integer as nn,
			\\   9223372036854775807::bigint as bi,
			\\   170141183460469231731687303715884105727::hugeint as hugn,
			\\
			\\   255::utinyint as uti, 65535::usmallint as usi, 4294967295::uinteger as unn,
			\\   18446744073709551615::ubigint as ubi,
			\\
			\\   null::tinyint as nti, null::smallint as nsi, null::integer as nnn,
			\\   null::bigint as nbi, null::hugeint as nhubn,
			\\
			\\   null::utinyint as nuti, null::usmallint as nusi, null::uinteger as nunn, null::ubigint as nubi,
			\\
			\\   '790162b2-8a73-4cd7-9be8-acc7475655d6'::uuid as col_uuid, null::uuid as zz,
			\\
			\\   'over 9000' as power, null::varchar as null_power,
			\\
			\\   '2023-06-19'::date as dte, null::date as ndate,
			\\
			\\   '23:03:45'::time as tme, null::time as ntme,
			\\
			\\   '2023-06-18 22:24:42.123Z'::timestamp as tz, null::timestamp as ntz
		, .{}).ok;
		defer rows.deinit();

		var mp = try rows.mapBuilder(t.allocator);
		defer mp.deinit();

		var row = (try rows.next()).?;
		var m = try row.toMap(&mp);

		try t.expectEqual(true, m.get(bool, "the_truth").?);
		try t.expectEqual(false, m.get(bool, "not_the_truth").?);
		try t.expectEqual(@as(?bool, null), m.get(bool, "n_truth"));

		try t.expectEqual(@as(f32, 32.329), m.get(f32, "a").?);
		try t.expectEqual(@as(f64, -0.29291), m.get(f64, "b").?);
		try t.expectEqual(@as(?f32, null), m.get(f32, "c"));
		try t.expectEqual(@as(?f64, null), m.get(f64, "d"));

		try t.expectEqual(@as(i8, 127), m.get(i8, "ti").?);
		try t.expectEqual(@as(i16, -32767), m.get(i16, "si").?);
		try t.expectEqual(@as(i32, 2147483647), m.get(i32, "nn").?);
		try t.expectEqual(@as(i64, 9223372036854775807), m.get(i64, "bi").?);
		try t.expectEqual(@as(i128, 170141183460469231731687303715884105727), m.get(i128, "hugn").?);

		try t.expectEqual(@as(u8, 255), m.get(u8, "uti").?);
		try t.expectEqual(@as(u16, 65535), m.get(u16, "usi").?);
		try t.expectEqual(@as(u32, 4294967295), m.get(u32, "unn").?);
		try t.expectEqual(@as(u64, 18446744073709551615), m.get(u64, "ubi").?);

		try t.expectEqual(@as(?i8, null), m.get(i8, "nti"));
		try t.expectEqual(@as(?i16, null), m.get(i16, "nsi"));
		try t.expectEqual(@as(?i32, null), m.get(i32, "nnn"));
		try t.expectEqual(@as(?i64, null), m.get(i64, "nbi"));
		try t.expectEqual(@as(?i128, null), m.get(i128, "nhugn"));

		try t.expectEqual(@as(?u8, null), m.get(u8, "nuti"));
		try t.expectEqual(@as(?u16, null), m.get(u16, "nusi"));
		try t.expectEqual(@as(?u32, null), m.get(u32, "nunn"));
		try t.expectEqual(@as(?u64, null), m.get(u64, "nubi"));

		try t.expectEqualStrings("790162b2-8a73-4cd7-9be8-acc7475655d6", m.get([]u8, "col_uuid").?);
		try t.expectEqual(@as(?[]const u8, null), m.get([]u8, "zz"));

		try t.expectEqualStrings("over 9000", m.get([]u8, "power").?);
		try t.expectEqual(@as(?[]const u8, null), m.get([]u8, "null_power"));

		try t.expectEqual(try typed.Date.parse("2023-06-19"), m.get(typed.Date, "dte").?);
		try t.expectEqual(@as(?typed.Date, null), m.get(typed.Date, "ndate"));

		try t.expectEqual(try typed.Time.parse("23:03:45"), m.get(typed.Time, "tme").?);
		try t.expectEqual(@as(?typed.Time, null), m.get(typed.Time, "ntme"));

		try t.expectEqual(@as(i64, 1687127082123000), m.get(typed.Timestamp, "tz").?.micros);
		try t.expectEqual(@as(?typed.Timestamp, null), m.get(typed.Timestamp, "ntz"));
	}
}
