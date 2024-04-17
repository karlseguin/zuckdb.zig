const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const Date = lib.Date;
const Time = lib.Time;
const Interval = lib.Interval;
const Vector = lib.Vector;
const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

pub const Appender = struct {
	// Error message, if any
	err: ?[]const u8,

	// the row of the current chunk that we're writing at
	row_index: usize,

	// c.duckdb_vector_size (2048)..when row_index == 2047, we flush and create
	// a new chunk
	vector_size: usize,

	allocator: Allocator,

	// 1 vector per column. Part of the vector data is initialied upfront (the
	// type information). Part of it is initialized for each data_chunk (the
	// underlying duckdb vector data and the validity data).
	vectors: []Vector,

	// This is duplicate of data available from vectors, but we need it as a slice
	// to pass to c.duckdb_create_data_chunk
	types: []c.duckdb_logical_type,

	// The collection of vectors for the appender. While we store data directly
	// in the vector, most operations (e.g. flush) happen on the data chunk.
	data_chunk: ?c.duckdb_data_chunk,
	appender: *c.duckdb_appender,

	pub fn init(allocator: Allocator, appender: *c.duckdb_appender) !Appender {
		const column_count = c.duckdb_appender_column_count(appender.*);

		var types = try allocator.alloc(c.duckdb_logical_type, column_count);
		errdefer allocator.free(types);

		var vectors = try allocator.alloc(Vector, column_count);
		errdefer allocator.free(vectors);

		var initialized: usize = 0;
		errdefer for (0..initialized) |i| {
			vectors[i].deinit();
		};

		for (0..column_count) |i| {
			const logical_type = c.duckdb_appender_column_type(appender.*, i);
			types[i] = logical_type;
			vectors[i] = try Vector.init(undefined, logical_type);
			initialized += 1;
		}

		return .{
			.err = null,
			.row_index = 0,
			.types = types,
			.vectors = vectors,
			.appender = appender,
			.allocator = allocator,
			.data_chunk = null,
			.vector_size = c.duckdb_vector_size(),
		};
	}

	pub fn deinit(self: *Appender) void {
		for (self.vectors) |*v| {
			v.deinit();
		}

		const allocator = self.allocator;
		allocator.free(self.types);
		allocator.free(self.vectors);

		if (self.data_chunk) |*data_chunk| {
			_ = c.duckdb_destroy_data_chunk(data_chunk);
		}


		const appender = self.appender;
		_ = c.duckdb_appender_destroy(appender);
		allocator.destroy(appender);
	}

	fn newDataChunk(types: []c.duckdb_logical_type, vectors: []Vector) c.duckdb_data_chunk {
		const data_chunk = c.duckdb_create_data_chunk(types.ptr, types.len);

		for (0..types.len) |i| {
			const v = c.duckdb_data_chunk_get_vector(data_chunk, i);
			const vector = &vectors[i];
			vector.loadVector(v);
			vector.validity = null;
		}
		return data_chunk;
	}

	pub fn flush(self: *Appender) !void {
		var data_chunk = self.data_chunk orelse return;
		// if (self.row_index < self.vector_size) {
			c.duckdb_data_chunk_set_size(data_chunk, self.row_index);
		// }

		const appender = self.appender;
		if (c.duckdb_append_data_chunk(appender.*, data_chunk) == DuckDBError) {
			if (c.duckdb_appender_error(appender.*)) |c_err| {
				self.err = std.mem.span(c_err);
			}
			return error.DuckDBError;
		}

		if (c.duckdb_appender_flush(self.appender.*) == DuckDBError) {
			if (c.duckdb_appender_error(appender.*)) |c_err| {
				self.err = std.mem.span(c_err);
			}
			return error.DuckDBError;
		}

		c.duckdb_destroy_data_chunk(&data_chunk);
		self.data_chunk = null;
	}

	pub fn appendRow(self: *Appender, values: anytype) !void {
		self.beginRow();

		inline for (values, 0..) |value, i| {
			try self.appendValue(value, i);
		}
		try self.endRow();
	}

	// The appender has two apis. The simplest is to call appendRow, passing the full
	// row. When using appendRow, things mostly just work.
	// It's also possible to call appendValue for each column. This API is used
	// when the "row" isn't known at comptime - the app has no choice but to
	// call appendValue for each column. In such cases, we require an explicit
	// call to beginRow, bindValue and endRow.
	pub fn beginRow(self: *Appender) void {
		if (self.data_chunk == null) {
			self.data_chunk = newDataChunk(self.types, self.vectors);
			self.row_index = 0;
		}
	}

	pub fn endRow(self: *Appender) !void {
		const row_index = self.row_index  + 1;
		self.row_index = row_index;
		if (row_index == self.vector_size) {
			try self.flush();
		}
	}

	pub fn appendValue(self: *Appender, value: anytype, column: usize) !void {
		var vector = &self.vectors[column];
		const row_index = self.row_index;

		const T = @TypeOf(value);
		const type_info = @typeInfo(T);
		switch (type_info) {
			.Null => {
				const validity = vector.validity orelse blk: {
					c.duckdb_vector_ensure_validity_writable(vector.vector);
					const v = c.duckdb_vector_get_validity(vector.vector);
					vector.validity = v;
					break :blk v;
				};
				c.duckdb_validity_set_row_invalid(validity, row_index);
				return;
			},
			.Optional => return self.appendValue(if (value) |v| v else null, column),
			.Pointer => |ptr| {
				switch (ptr.size) {
					.Slice => return self.appendSlice(vector, @as([]const ptr.child, value), row_index),
					.One => switch (@typeInfo(ptr.child)) {
						.Array => {
							const Slice = []const std.meta.Elem(ptr.child);
							return self.appendSlice(vector, @as(Slice, value), row_index);
						},
						else => appendError(T),
					},
					else => appendError(T),
				}
			},
			.Array => return self.appendValue(&value, column),
			else => {},
		}

		switch (vector.data) {
			.container => return self.appendTypeError("container", T),
			.scalar => |scalar| switch (scalar) {
				.bool => |data| {
					switch (type_info) {
						.Bool => data[row_index] = value,
						else => return self.appendTypeError("boolean", T)
					}
				},
				.i8 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < -128 or value > 127) return self.appendIntRangeError("tinyint");
							data[row_index] = @intCast(value);
						},
						else => return self.appendTypeError("tinyint", T)
					}
				},
				.i16 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < -32768 or value > 32767) return self.appendIntRangeError("smallint");
							data[row_index] = @intCast(value);
						},
						else => return self.appendTypeError("smallint", T)
					}
				},
				.i32 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < -2147483648 or value > 2147483647) return self.appendIntRangeError("integer");
							data[row_index] = @intCast(value);
						},
						else => return self.appendTypeError("integer", T)
					}
				},
				.i64 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < -9223372036854775808 or value > 9223372036854775807) return self.appendIntRangeError("bigint");
							data[row_index] = @intCast(value);
						},
						else => return self.appendTypeError("bigint", T)
					}
				},
				.i128 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < -170141183460469231731687303715884105728 or value > 170141183460469231731687303715884105727) return self.appendIntRangeError("hugeint");
							data[row_index] = @intCast(value);
						},
						else => return self.appendTypeError("hugeint", T)
					}
				},
				.u8 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < 0 or value > 255) return self.appendIntRangeError("utinyint");
							data[row_index] = @intCast(value);
						},
						else => return self.appendTypeError("utinyint", T)
					}
				},
				.u16 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < 0 or value > 65535) return self.appendIntRangeError("usmallint");
							data[row_index] = @intCast(value);
						},
						else => return self.appendTypeError("usmallint", T)
					}
				},
				.u32 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < 0 or value > 4294967295) return self.appendIntRangeError("uinteger");
							data[row_index] = @intCast(value);
						},
						else => return self.appendTypeError("uinteger", T)
					}
				},
				.u64 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < 0 or value > 18446744073709551615) return self.appendIntRangeError("ubingint");
							data[row_index] = @intCast(value);
						},
						else => return self.appendTypeError("ubingint", T)
					}
				},
				.u128 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < 0 or value > 340282366920938463463374607431768211455) return self.appendIntRangeError("uhugeint");
							data[row_index] = @intCast(value);
						},
						else => return self.appendTypeError("uhugeint", T)
					}
				},
				.f32 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => data[row_index] = @floatFromInt(value),
						.Float, .ComptimeFloat => data[row_index] = @floatCast(value),
						else => return self.appendTypeError("real", T)
					}
				},
				.f64 => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => data[row_index] = @floatFromInt(value),
						.Float, .ComptimeFloat => data[row_index] = @floatCast(value),
						else => return self.appendTypeError("double", T)
					}
				},
				.date => |data| if (T == Date) {
					data[row_index] = c.duckdb_to_date(value);
				} else {
					return self.appendTypeError("date", T);
				},
				.time => |data| if (T == Time) {
					data[row_index] = c.duckdb_to_time(value);
				} else {
					return self.appendTypeError("time", T);
				},
				.interval => |data| if (T == Interval) {
					data[row_index] = value;
				} else {
					return self.appendTypeError("interval", T);
				},
				.timestamp => |data| {
					switch (type_info) {
						.Int, .ComptimeInt => {
							if (value < -9223372036854775808 or value > 9223372036854775807) return self.appendIntRangeError("i64");
							data[row_index] = .{.micros = @intCast(value)};
						},
						else => return self.appendTypeError("timestamp", T)
					}
				},
				else => unreachable,
			}
		}
	}

	fn appendSlice(self: *Appender, vector: *Vector, value: anytype, row_index: usize) !void {
		const T = @TypeOf(value);
		if (T == []u8 or T == []const u8) {
			return self.appendString(vector, value, row_index);
		}
		// TODO
		appendError(T);
	}

	fn appendString(self: *Appender, vector: *Vector, value: anytype, row_index: usize) !void {
		switch (vector.data) {
			.container => return self.appendTypeError("container", @TypeOf(value)),
			.scalar => |scalar| switch (scalar) {
				.blob => c.duckdb_vector_assign_string_element_len(vector.vector, row_index, value.ptr, value.len),
				.i128 => |data| {
					var n: i128 = 0;
					if (value.len == 36) {
						n = try uuidToInt(value);
					} else if (value.len == 16) {
						n = std.mem.readInt(i128, value[0..16], .big);
					} else {
						return error.InvalidUUID;
					}
					data[row_index] = n ^ (@as(i128, 1) << 127);
				},
				else => {
					// we don't want to allocate, and we don't know the vector type at compile time, so we use ???. Fail.
					return self.appendTypeError("???", lib.UUID);
				},
			}
		}
	}

	fn appendTypeError(self: *Appender, comptime data_type: []const u8, value_type: type) error{AppendError} {
		self.err = "cannot bind a " ++ @typeName(value_type) ++ " to a column of type " ++ data_type;
		return error.AppendError;
	}

	fn appendIntRangeError(self: *Appender, comptime data_type: []const u8) error{AppendError} {
		self.err = "value is outside of range for a column of type " ++ data_type;
		return error.AppendError;
	}
};

fn appendError(comptime T: type) void {
	@compileError("cannot append value of type " ++ @typeName(T));
}

// // The Vector is initially initialized with just its data_type and, in the case
// // of lists and struct, the empty children.  When a new data chunk is created
// // the Vector's underlying duckdb_vector and data pointer are initialized. This
// // allows us to re-use some parts of the Vector across multple data chunks.
// const Vector = struct {
// 	data_type: c.duckdb_type,

// 	// initialized when a new data_chunk is created
// 	vector: c.duckdb_vector = undefined,

// 	// initialized when a new data_chunk is created, a typed pointer to the underlying
// 	// vector data (which is a void * in C).
// 	data: TypedData = undefined,

// 	// initialized when we first try to set null, reset to null on each new chunk
// 	validity: ?[*c]u64 = null,

// 	fn init(logical_type: c.duckdb_logical_type, col: usize) !Vector {
// 		_ = col;

// 		const tp = c.duckdb_get_type_id(logical_type);
// 		switch (tp) {
// 			c.DUCKDB_TYPE_BOOLEAN,
// 			c.DUCKDB_TYPE_TINYINT,
// 			c.DUCKDB_TYPE_SMALLINT,
// 			c.DUCKDB_TYPE_INTEGER,
// 			c.DUCKDB_TYPE_BIGINT,
// 			c.DUCKDB_TYPE_UTINYINT,
// 			c.DUCKDB_TYPE_USMALLINT,
// 			c.DUCKDB_TYPE_UINTEGER,
// 			c.DUCKDB_TYPE_UBIGINT,
// 			c.DUCKDB_TYPE_HUGEINT,
// 			c.DUCKDB_TYPE_UHUGEINT,
// 			c.DUCKDB_TYPE_FLOAT,
// 			c.DUCKDB_TYPE_DOUBLE,
// 			c.DUCKDB_TYPE_VARCHAR,
// 			c.DUCKDB_TYPE_BLOB,
// 			c.DUCKDB_TYPE_TIMESTAMP,
// 			c.DUCKDB_TYPE_DATE,
// 			c.DUCKDB_TYPE_TIME,
// 			c.DUCKDB_TYPE_INTERVAL,
// 			c.DUCKDB_TYPE_BIT,
// 			c.DUCKDB_TYPE_TIME_TZ,
// 			c.DUCKDB_TYPE_TIMESTAMP_TZ,
// 			c.DUCKDB_TYPE_DECIMAL,
// 			c.DUCKDB_TYPE_UUID,
// 			c.DUCKDB_TYPE_ENUM => return .{.data_type = tp},
// 			// c.DUCKDB_TYPE_LIST => .list,
// 			else => return error.UnsupportedAppendColumnType,
// 		}
// 	}
// };

// const DataType = enum {
// 	i8,
// 	i16,
// 	i32,
// 	i64,
// 	i128,
// 	u128,
// 	u8,
// 	u16,
// 	u32,
// 	u64,
// 	bool,
// 	f32,
// 	f64,
// 	blob,
// 	varchar,
// 	date,
// 	time,
// 	timestamp,
// 	interval,
// 	uuid,
// };

// const TypedData = union(DataType) {
// 	i8: [*c]i8,
// 	i16: [*c]i16,
// 	i32: [*c]i32,
// 	i64: [*c]i64,
// 	i128: [*c]i128,
// 	u128: [*c]u128,
// 	u8: [*c]u8,
// 	u16: [*c]u16,
// 	u32: [*c]u32,
// 	u64: [*c]u64,
// 	bool: [*c]bool,
// 	f32: [*c]f32,
// 	f64: [*c]f64,
// 	blob: void,
// 	varchar: void,
// 	date: [*]c.duckdb_date,
// 	time: [*]c.duckdb_time,
// 	timestamp: [*]i64,
// 	interval: [*]c.duckdb_interval,
// 	uuid: [*c]i128,
// 	list: [*c]cduckdb_list_entry,
// };

fn uuidToInt(hex: []const u8) !i128 {
	var bin: [16]u8 = undefined;

	std.debug.assert(hex.len == 36);
	if (hex[8] != '-' or hex[13] != '-' or hex[18] != '-' or hex[23] != '-') {
		return error.InvalidUUID;
	}

	inline for (encoded_pos, 0..) |i, j| {
		const hi = hex_to_nibble[hex[i + 0]];
		const lo = hex_to_nibble[hex[i + 1]];
		if (hi == 0xff or lo == 0xff) {
			return error.InvalidUUID;
		}
		bin[j] = hi << 4 | lo;
	}
	return std.mem.readInt(i128, &bin, .big);
}

const encoded_pos = [16]u8{ 0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34 };
const hex_to_nibble = [_]u8{0xff} ** 48 ++ [_]u8{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0xff,
} ++ [_]u8{0xff} ** 152;

const t = std.testing;
const DB = lib.DB;
test "Appender: bind errors" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec("create table x (a integer)", .{});
	{
		var appender = try conn.appender(null, "x");
		defer appender.deinit();
		try t.expectError(error.AppendError, appender.appendRow(.{true}));
		try t.expectEqualStrings("cannot bind a bool to a column of type integer", appender.err.?);
	}

	{
		var appender = try conn.appender(null, "x");
		defer appender.deinit();
		try t.expectError(error.AppendError, appender.appendRow(.{9147483647}));
		try t.expectEqualStrings("value is outside of range for a column of type integer", appender.err.?);
	}
}

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
		\\   col_bool bool,
		\\   col_real real,
		\\   col_double double,
		\\   col_text text,
		\\   col_blob blob,
		\\   col_uuid uuid,
		\\   col_date date,
		\\   col_time time,
		\\   col_interval interval,
		\\   col_timestamp timestamp,
		\\ )
	, .{});

	{
		var appender = try conn.appender(null, "x");
		defer appender.deinit();
		try appender.appendRow(.{
			-128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728,
			255, 65535, 4294967295, 18446744073709551615, 340282366920938463463374607431768211455,
			true, -1.23, 1994.848288123, "over 9000!", &[_]u8{1, 2, 3, 254}, "34c667cd-638e-40c2-b256-0f78ccab7013",
			Date{.year = 2023, .month = 5, .day = 10}, Time{.hour = 21, .min = 4, .sec = 49, .micros = 123456},
			Interval{.months = 3, .days = 7, .micros = 982810}, 1711506018088167,
		});
		try appender.flush();

		try t.expectEqual(null, appender.err);

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
		try t.expectEqual(true, row.get(bool, 10));
		try t.expectEqual(-1.23, row.get(f32, 11));
		try t.expectEqual(1994.848288123, row.get(f64, 12));
		try t.expectEqualStrings("over 9000!", row.get([]u8, 13));
		try t.expectEqualStrings(&[_]u8{1, 2, 3, 254}, row.get([]u8, 14));
		try t.expectEqualStrings("34c667cd-638e-40c2-b256-0f78ccab7013", &row.get(lib.UUID, 15));
		try t.expectEqual(Date{.year = 2023, .month = 5, .day = 10}, row.get(Date, 16));
		try t.expectEqual(Time{.hour = 21, .min = 4, .sec = 49, .micros = 123456}, row.get(Time, 17));
		try t.expectEqual(Interval{.months = 3, .days = 7, .micros = 982810}, row.get(Interval, 18));
		try t.expectEqual(1711506018088167, row.get(i64, 19));
	}
}

test "Appender: basic variants" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec(
		\\ create table x (
		\\   id integer,
		\\   col_bool bool,
		\\   col_uuid uuid
		\\ )
	, .{});

	var appender = try conn.appender(null, "x");
	defer appender.deinit();
	try appender.appendRow(.{1, false, &[_]u8{0xf9,0x3b,0x64,0xe0,0x91,0x62,0x40,0xf5,0xaa,0xb8,0xa0,0x1f,0x5c,0xe9,0x90,0x32}});
	try appender.appendRow(.{2, null, null});
	try appender.flush();

	try t.expectEqual(null, appender.err);

	{
		var row = (try conn.row("select * from x where id = 1", .{})).?;
		defer row.deinit();
		try t.expectEqual(false, row.get(bool, 1));
		try t.expectEqualStrings("f93b64e0-9162-40f5-aab8-a01f5ce99032", &row.get(lib.UUID, 2));
	}

	{
		var row = (try conn.row("select * from x where id = 2", .{})).?;
		defer row.deinit();
		try t.expectEqual(null, row.get(?bool, 1));
		try t.expectEqual(null, row.get(?lib.UUID, 2));
	}
}

test "Appender: multiple chunks" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec("create table x (a integer, b integer)", .{});

	{
		var appender = try conn.appender(null, "x");
		defer appender.deinit();

		for (0..1000) |i| {
			appender.beginRow();
			try appender.appendValue(i, 0);
			if (@mod(i, 3) == 0) {
				try appender.appendValue(null, 1);
			} else {
				try appender.appendValue(i * 2, 1);
			}
			try appender.endRow();
		}
		try appender.flush();
	}

	var rows = try conn.query("select * from x order by a", .{});
	defer rows.deinit();

	var i: i32 = 0;
	while (try rows.next()) |row| {
		try t.expectEqual(i, row.get(i32, 0));

		if (@mod(i, 3) == 0) {
			try t.expectEqual(null, row.get(?i32, 1));
		} else {
			try t.expectEqual(i*2, row.get(i32, 1));
		}
		i += 1;
	}
	try t.expectEqual(1000, i);
}

test "Appender: implicit and explicit flush" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec("create table x (a integer)", .{});

	{
		var appender = try conn.appender(null, "x");
		defer appender.deinit();

		try appender.appendRow(.{0});

		appender.beginRow();
		try appender.appendValue(1, 0);
		try appender.endRow();

		try appender.flush();

		for (2..5000) |i| {
			appender.beginRow();
			try appender.appendValue(i, 0);
			try appender.endRow();
		}

		try appender.appendRow(.{5000});

		appender.beginRow();
		try appender.appendValue(5001, 0);
		try appender.endRow();

		try appender.flush();
	}

	var rows = try conn.query("select * from x order by a", .{});
	defer rows.deinit();

	var i: i32 = 0;
	while (try rows.next()) |row| {
		try t.expectEqual(i, row.get(i32, 0));
		i += 1;
	}
	try t.expectEqual(5002, i);
}

test "Appender: hugeint" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec("create table x (a hugeint)", .{});

	var expected: [1000]i128 = undefined;
	{
		var seed: u64 = undefined;
		std.posix.getrandom(std.mem.asBytes(&seed)) catch unreachable;
		var prng = std.rand.DefaultPrng.init(seed);

		const random = prng.random();


		var appender = try conn.appender(null, "x");
		defer appender.deinit();

		for (0..1000) |i| {
			const value = random.int(i128);
			expected[i] = value;
			try appender.appendRow(.{value});
		}
		try appender.flush();
	}

	var rows = try conn.query("select * from x", .{});
	defer rows.deinit();

	var i: i32 = 0;
	while (try rows.next()) |row| {
		try t.expectEqual(expected[@intCast(i)], row.get(i128, 0));
		i += 1;
	}
	try t.expectEqual(1000, i);
}
