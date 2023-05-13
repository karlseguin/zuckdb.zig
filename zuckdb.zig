const std = @import("std");
const c = @cImport(@cInclude("zuckdb.h"));

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

const CONFIG_SIZEOF = c.config_sizeof;
const CONFIG_ALIGNOF = c.config_alignof;
const DB_SIZEOF = c.database_sizeof;
const DB_ALIGNOF = c.database_alignof;
const CONN_SIZEOF = c.connection_sizeof;
const CONN_ALIGNOF = c.connection_alignof;
const RESULT_SIZEOF = c.result_sizeof;
const RESULT_ALIGNOF = c.result_alignof;
const STATEMENT_SIZEOF = c.statement_sizeof;
const STATEMENT_ALIGNOF = c.statement_alignof;

pub const Date = c.duckdb_date_struct;
pub const Time = c.duckdb_time_struct;
pub const Interval = c.duckdb_interval;
pub const UUID = [36]u8;

pub const DB = struct{
	allocator: Allocator,
	db: *c.duckdb_database,

	pub fn init(allocator: Allocator, path: [*:0]const u8) DBResult(DB) {
		var config_slice = allocator.alignedAlloc(u8, CONFIG_ALIGNOF, CONFIG_SIZEOF) catch |err| {
			return .{.err = DBErr.static(err, "OOM") };
		};

		defer allocator.free(config_slice);
		const config = @ptrCast(*c.duckdb_config, config_slice.ptr);

		if (c.duckdb_create_config(config) == DuckDBError) {
			return .{.err = DBErr.static(error.CreateConfig, "error creating database config") };
		}

		// if (c.duckdb_set_config(config, "enable_external_access", "false") == DuckDBError) {
		//  return error.DBConfigExternal;
		// }

		var db_slice = allocator.alignedAlloc(u8, DB_ALIGNOF, DB_SIZEOF) catch |err| {
			return .{.err = DBErr.static(err, "OOM") };
		};
		const db = @ptrCast(*c.duckdb_database, db_slice.ptr);

		var out_err: [*c]u8 = undefined;
		if (c.duckdb_open_ext(path, db, config.*, &out_err) == DuckDBError) {
			allocator.free(db_slice);
			return .{.err = .{
				.c_err = out_err,
				.err = error.DBOpen,
				.desc = std.mem.span(out_err),
			}};
		}

		return .{.ok = .{.db = db, .allocator = allocator}};
	}

	pub fn deinit(self: *const DB) void {
		const db = self.db;
		c.duckdb_close(db);
		self.allocator.free(@ptrCast([*]u8, db)[0..DB_SIZEOF]);
	}

	pub fn conn(self: DB) !Conn {
		return Conn.open(self);
	}

	pub fn pool(self: DB, config: Pool.Config) DBResult(Pool) {
		return Pool.init(self, config);
	}
};

pub const Conn = struct {
	allocator: Allocator,
	conn: *c.duckdb_connection,

	fn open(db: DB) !Conn {
		const allocator = db.allocator;

		var slice = try allocator.alignedAlloc(u8, CONN_ALIGNOF, CONN_SIZEOF);
		errdefer allocator.free(slice);
		const conn = @ptrCast(*c.duckdb_connection, slice.ptr);

		if (c.duckdb_connect(db.db.*, conn) == DuckDBError) {
			return error.ConnectFail;
		}

		return .{
			.conn = conn,
			.allocator = allocator,
		};
	}

	pub fn deinit(self: Conn) void {
		const conn = self.conn;
		const allocator = self.allocator;

		c.duckdb_disconnect(conn);
		allocator.free(@ptrCast([*]u8, conn)[0..CONN_SIZEOF]);
	}

	pub fn begin(self: Conn) !void {
		return self.execZ("begin transaction");
	}

	pub fn commit(self: Conn) !void {
		return self.execZ("commit");
	}

	pub fn rollback(self: Conn) !void {
		return self.execZ("rollback");
	}

	pub fn exec(self: Conn, sql: []const u8) !void {
		const zql = try self.allocator.dupeZ(u8, sql);
		defer self.allocator.free(zql);
		return self.execZ(zql);
	}

	pub fn execZ(self: Conn, sql: [:0]const u8) !void {
		if (c.duckdb_query(self.conn.*, sql, null) == DuckDBError) {
			return error.ExecFailed;
		}
	}

	pub fn query(self: Conn, sql: []const u8, values: anytype) Result(Rows) {
		const zql = self.allocator.dupeZ(u8, sql) catch |err| {
			return .{.err = ResultErr.fromAllocator(err, .{}) };
		};
		defer self.allocator.free(zql);
		return self.queryZ(zql, values);
	}

	pub fn queryZ(self: Conn, sql: [:0]const u8, values: anytype) Result(Rows) {
		if (values.len == 0) {
				const allocator = self.allocator;
				var slice = allocator.alignedAlloc(u8, RESULT_ALIGNOF, RESULT_SIZEOF) catch |err| {
				return .{.err = ResultErr.fromAllocator(err, .{}) };
			};
			const result = @ptrCast(*c.duckdb_result, slice.ptr);
			if (c.duckdb_query(self.conn.*, sql, result) == DuckDBError) {
				return .{.err = ResultErr.fromResult(allocator, null, result) };
			}
			return Rows.init(allocator, null, result);
		}

		const prepare_result = self.prepareZ(sql);
		const stmt = switch (prepare_result) {
			.ok => |stmt| stmt,
			.err => |err| return .{.err = err},
		};

		stmt.bind(values) catch |err| {
			return .{.err = .{
				.err = err,
				.stmt = stmt.stmt,
				.desc = "bind error",
				.allocator = self.allocator,
			}};
		};
		return stmt.execute();
	}

	pub fn row(self: Conn, sql: []const u8, values: anytype) !?OwningRow {
		const zql = try self.allocator.dupeZ(u8, sql);
		defer self.allocator.free(zql);
		return self.rowZ(zql, values);
	}

	pub fn rowZ(self: Conn, sql: [:0]const u8, values: anytype) !?OwningRow
	 {
		const query_result = self.queryZ(sql, values);
		errdefer query_result.deinit();
		var rows = switch (query_result) {
			.ok => |rows| rows,
			.err => |err| {
				std.log.err("zuckdb conn.row error: {s}\n", .{err.desc});
				return err.err;
			},
		};

		const r = (try rows.next()) orelse {
			query_result.deinit();
			return null;
		};
		return .{.row = r, .rows = rows};
	}

	pub fn prepare(self: *const Conn, sql: []const u8) Result(Stmt) {
		const zql = self.allocator.dupeZ(u8, sql) catch |err| {
			return .{.err = ResultErr.fromAllocator(err, .{}) };
		};
		defer self.allocator.free(zql);
		return self.prepareZ(zql);
	}

	pub fn prepareZ(self: *const Conn, sql: [:0]const u8) Result(Stmt) {
		const allocator = self.allocator;
		var slice = allocator.alignedAlloc(u8, STATEMENT_ALIGNOF, STATEMENT_SIZEOF) catch |err| {
			return .{.err = ResultErr.fromAllocator(err, .{}) };
		};

		const stmt = @ptrCast(*c.duckdb_prepared_statement, slice.ptr);
		if (c.duckdb_prepare(self.conn.*, sql, stmt) == DuckDBError) {
			return .{.err = .{
				.err = error.Prepare,
				.stmt = stmt,
				.desc = std.mem.span(c.duckdb_prepare_error(stmt.*)),
				.allocator = allocator,
			}};
		}

		return .{.ok = .{.stmt = stmt, .allocator = allocator}};
	}
};

pub const Rows = struct {
	allocator: Allocator,

	// The statement that created this result. A result can be created directly
	// without a prepared statement. Also, a prepared statement's lifetime can
	// be longr than the result. When stmt != null, it means we now own the stmt
	// and must clean it up when we're done. This is used in simple APIs (e.g.
	// query with args).
	stmt: ?Stmt,

	// The underlying duckdb result
	result: *c.duckdb_result,

	// The number of chunks in this result
	chunk_count: usize = 0,

	// The next chunk to laod
	chunk_index: usize = 0,

	// The number of columns in this rersult
	column_count: usize = 0,

	// The current chunk object
	chunk: ?c.duckdb_data_chunk = null,

	// The number of rows in the current chunk
	row_count: usize = 0,

	// The row index, within the current chunk
	row_index: usize = 0,

	// Vector data + validity for the current chunk
	columns: []ColumnData = undefined,

	// the type of each column, this is loaded once on init
	column_types: []c.duckdb_type = undefined,

	pub fn init(allocator: Allocator, stmt: ?Stmt, result: *c.duckdb_result) Result(Rows) {
		const r = result.*;
		const chunk_count = c.duckdb_result_chunk_count(r);
		const column_count = c.duckdb_column_count(result);
		if (chunk_count == 0) {
			// no chunk, we don't need to load everything else
			return .{.ok = .{
				.stmt = stmt,
				.result = result,
				.chunk_count = 0,
				.allocator = allocator,
				.column_count = column_count,
			}};
		}

		const column_types = allocator.alloc(c.duckdb_type, column_count) catch |err| {
			return .{.err = ResultErr.fromAllocator(err, .{
				.stmt = if (stmt != null) stmt.?.stmt else null,
				.result = result,
			})};
		};

		for (0..column_count) |i| {
			column_types[i] = c.duckdb_column_type(result, i);
		}

		const columns = allocator.alloc(ColumnData, column_count) catch |err| {
			return .{.err = ResultErr.fromAllocator(err, .{
				.stmt = if (stmt != null) stmt.?.stmt else null,
				.result = result,
			})};
		};

		return .{.ok = .{
			.stmt = stmt,
			.result = result,
			.columns = columns,
			.allocator = allocator,
			.chunk_count = chunk_count,
			.column_count = column_count,
			.column_types = column_types,
		}};
	}

	pub fn deinit(self: Rows) void {
		const allocator = self.allocator;

		if (self.chunk_count != 0) {
			// these are only allocated if we have data
			allocator.free(self.columns);
			allocator.free(self.column_types);
		}

		const result = self.result;
		c.duckdb_destroy_result(result);
		allocator.free(@ptrCast([*]u8, result)[0..RESULT_SIZEOF]);

		if (self.stmt) |stmt| {
			stmt.deinit();
		}
	}

	pub fn changed(self: Rows) usize {
		return c.duckdb_rows_changed(self.result);
	}

	pub fn count(self: Rows) usize {
		return c.duckdb_row_count(self.result);
	}

	pub fn columnName(self: Rows, i: usize) [*c]const u8 {
		return c.duckdb_column_name(self.result, i);
	}

	pub fn next(self: *Rows) !?Row {
		var row_index = self.row_index;
		if (row_index == self.row_count) {
			if (try self.loadNextChunk() == false) {
				return null;
			}
			row_index = 0;
		}

		self.row_index = row_index + 1;

		return .{
			.index = row_index,
			.columns = self.columns,
		};
	}

	fn loadNextChunk(self: *Rows) !bool {
		const result = self.result.*;
		const chunk_count = self.chunk_count;
		const column_count = self.column_count;

		if (self.chunk) |*chunk| {
			c.duckdb_destroy_data_chunk(chunk);
			self.chunk = null;
		}

		var chunk_index = self.chunk_index;

		while (true) {
			if (chunk_index == chunk_count) return false;

			var chunk = c.duckdb_result_get_chunk(result, chunk_index);
			chunk_index += 1;

			const row_count = c.duckdb_data_chunk_get_size(chunk);
			if (row_count == 0) {
				c.duckdb_destroy_data_chunk(&chunk);
				continue;
			}

			var columns = self.columns;
			const column_types = self.column_types;

			for (0..column_count) |col| {
				const column_type = column_types[col];
				const vector = c.duckdb_data_chunk_get_vector(chunk, col);

				// we split up Scalar and Complex because, for now atleast, our container types
				// (like lists) can only contain scalar types. So we need an explicit function
				// for loading Scalar data which we can re-use for lists.
				var data: ColumnData.Data = undefined;
				if (generateScalarColumnData(vector, column_type)) |scalar| {
					data = .{.scalar = scalar};
				} else {
					if (generateContainerColumnData(vector, column_type)) |container| {
						data = .{.container = container};
					} else {
						return error.UnknownDataType;
					}
				}
				columns[col] = ColumnData{
					.data = data,
					.validity = c.duckdb_vector_get_validity(vector),
				};
			}

			self.chunk = chunk;
			self.row_count = row_count;
			self.chunk_index = chunk_index;

			return true;
		}
		unreachable;
	}
};

// DuckDB exposes data as "vectors", which is essentially a pointer to memory
// that holds data based on the column type (a vector is data for a column, not
// a row). Our ColumnData is a typed wrapper to the (a) data and (b) the validity
// mask (null) of a vector.
const ColumnData = struct {
	data: Data,
	validity: [*c]u64,

	const Data = union(enum) {
		scalar: Scalar,
		container: Container,
	};

	const Scalar = union(enum) {
		i8: [*c]i8,
		i16: [*c]i16,
		i32: [*c]i32,
		i64: [*c]i64,
		i128: [*c]i128,
		u8: [*c]u8,
		u16: [*c]u16,
		u32: [*c]u32,
		u64: [*c]u64,
		bool: [*c]bool,
		f32: [*c]f32,
		f64: [*c]f64,
		blob: [*]c.duckdb_string_t,
		varchar: [*]c.duckdb_string_t,
		date: [*]c.duckdb_date,
		time: [*]c.duckdb_time,
		timestamp: [*]c.duckdb_timestamp,
		interval: [*]c.duckdb_interval,
		decimal: ColumnData.Decimal,
		uuid: [*c]i128,
	};

	const Container = union(enum) {
		list: ColumnData.List,
	};

	const Decimal = struct {
		width: u8,
		scale: u8,
		internal: Internal,

		const Internal = union(enum) {
			i16: [*c]i16,
			i32: [*c]i32,
			i64: [*c]i64,
			i128: [*c]i128,
		};
	};

	const List = struct {
		child: Scalar,
		validity: [*c]u64,
		entries: [*]c.duckdb_list_entry,
	};
};

fn generateScalarColumnData(vector: c.duckdb_vector, column_type: usize) ?ColumnData.Scalar {
	const raw_data = c.duckdb_vector_get_data(vector);
	switch (column_type) {
		c.DUCKDB_TYPE_BLOB, c.DUCKDB_TYPE_VARCHAR => return .{.blob = @ptrCast([*]c.duckdb_string_t, @alignCast(8, raw_data))},
		c.DUCKDB_TYPE_TINYINT => return .{.i8 = @ptrCast([*c]i8, raw_data)},
		c.DUCKDB_TYPE_SMALLINT => return .{.i16 = @ptrCast([*c]i16, @alignCast(2, raw_data))},
		c.DUCKDB_TYPE_INTEGER => return .{.i32 = @ptrCast([*c]i32, @alignCast(4, raw_data))},
		c.DUCKDB_TYPE_BIGINT => return .{.i64 = @ptrCast([*c]i64, @alignCast(8, raw_data))},
		c.DUCKDB_TYPE_HUGEINT, c.DUCKDB_TYPE_UUID => return .{.i128 = @ptrCast([*c]i128, @alignCast(16, raw_data))},
		c.DUCKDB_TYPE_UTINYINT => return .{.u8 = @ptrCast([*c]u8, raw_data)},
		c.DUCKDB_TYPE_USMALLINT => return .{.u16 = @ptrCast([*c]u16, @alignCast(2, raw_data))},
		c.DUCKDB_TYPE_UINTEGER => return .{.u32 = @ptrCast([*c]u32, @alignCast(4, raw_data))},
		c.DUCKDB_TYPE_UBIGINT => return .{.u64 = @ptrCast([*c]u64, @alignCast(8, raw_data))},
		c.DUCKDB_TYPE_BOOLEAN => return .{.bool = @ptrCast([*c]bool, raw_data)},
		c.DUCKDB_TYPE_FLOAT => return .{.f32 = @ptrCast([*c]f32, @alignCast(4, raw_data))},
		c.DUCKDB_TYPE_DOUBLE => return .{.f64 = @ptrCast([*c]f64, @alignCast(8, raw_data))},
		c.DUCKDB_TYPE_DATE => return .{.date = @ptrCast([*c]c.duckdb_date, @alignCast(@alignOf(c.duckdb_date), raw_data))},
		c.DUCKDB_TYPE_TIME => return .{.time = @ptrCast([*c]c.duckdb_time, @alignCast(@alignOf(c.duckdb_time), raw_data))},
		c.DUCKDB_TYPE_TIMESTAMP => return .{.timestamp = @ptrCast([*c]c.duckdb_timestamp, @alignCast(@alignOf(c.duckdb_timestamp), raw_data))},
		c.DUCKDB_TYPE_INTERVAL => return .{.interval = @ptrCast([*c]c.duckdb_interval, @alignCast(@alignOf(c.duckdb_interval), raw_data))},
		c.DUCKDB_TYPE_DECIMAL => {
			// decimal's storage is based on the width
			const logical_type = c.duckdb_vector_get_column_type(vector);
			const scale = c.duckdb_decimal_scale(logical_type);
			const width = c.duckdb_decimal_width(logical_type);
			const internal: ColumnData.Decimal.Internal = switch (c.duckdb_decimal_internal_type(logical_type)) {
				c.DUCKDB_TYPE_SMALLINT => .{.i16 = @ptrCast([*c]i16, @alignCast(2, raw_data))},
				c.DUCKDB_TYPE_INTEGER => .{.i32 = @ptrCast([*c]i32, @alignCast(4, raw_data))},
				c.DUCKDB_TYPE_BIGINT => .{.i64 = @ptrCast([*c]i64, @alignCast(8, raw_data))},
				c.DUCKDB_TYPE_HUGEINT => .{.i128 = @ptrCast([*c]i128, @alignCast(16, raw_data))},
				else => unreachable,
			};
			return .{.decimal = .{.width = width, .scale = scale, .internal = internal}};
		},
		else => return null,
	}
}

fn generateContainerColumnData(vector: c.duckdb_vector, column_type: usize) ?ColumnData.Container {
	const raw_data = c.duckdb_vector_get_data(vector);
	switch (column_type) {
		c.DUCKDB_TYPE_LIST => {
			const child_vector = c.duckdb_list_vector_get_child(vector);
			const child_type = c.duckdb_get_type_id(c.duckdb_vector_get_column_type(child_vector));
			const child_data = generateScalarColumnData(child_vector, child_type) orelse return null;
			const child_validity = c.duckdb_vector_get_validity(child_vector);
			return .{.list = .{
				.child = child_data,
				.validity = child_validity,
				.entries = @ptrCast([*c]c.duckdb_list_entry, @alignCast(@alignOf(c.duckdb_list_entry), raw_data)),
			}};
		},
		else => return null,
	}
}

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

		switch (column.data)  {
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

		fn get(self: Self, i: usize) ?T {
			const index = i + self._offset;
			if (isNull(self._validity, index)) return null;
			return getScalar(T, self._scalar, index);
		}
	};
}

pub const Stmt = struct {
	allocator: Allocator,
	stmt: *c.duckdb_prepared_statement,

	pub fn deinit(self: Stmt) void {
		const stmt = self.stmt;
		c.duckdb_destroy_prepare(stmt);
		self.allocator.free(@ptrCast([*]u8, stmt)[0..STATEMENT_SIZEOF]);
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

	pub fn execute(self: Stmt) Result(Rows) {
		const stmt = self.stmt;
		const allocator = self.allocator;
		var slice = allocator.alignedAlloc(u8, RESULT_ALIGNOF, RESULT_SIZEOF) catch |err| {
			return .{.err = ResultErr.fromAllocator(err, .{.stmt = stmt})};
		};

		const result = @ptrCast(*c.duckdb_result, slice.ptr);
		if (c.duckdb_execute_prepared(stmt.*, result) == DuckDBError) {
			return .{.err = ResultErr.fromResult(allocator, stmt, result) };
		}
		return Rows.init(allocator, self, result);
	}

	pub fn numberOfParameters(self: Stmt) usize {
		return c.duckdb_nparams(self.stmt.*);
	}

	pub fn parameterTypeC(self: Stmt, i: usize) usize {
		return c.duckdb_param_type(self.stmt.*, i+1);
	}

	pub fn parameterType(self: Stmt, i: usize) ParameterType {
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
					u8 => rc = bindVarcharOrBlob(stmt, bind_index, value.ptr, value.len),
					else => bindError(T),
				},
				else => bindError(T),
			}
		},
		.Array => |arr| switch (arr.child) {
			u8 => rc = c.duckdb_bind_varchar_length(stmt, bind_index, value, value.len),
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

fn bindVarcharOrBlob(stmt: c.duckdb_prepared_statement, bind_index: usize, value: [*c]const u8, len: usize) c_uint {
	switch (c.duckdb_param_type(stmt, bind_index)) {
		c.DUCKDB_TYPE_VARCHAR => return c.duckdb_bind_varchar_length(stmt, bind_index, value, len),
		c.DUCKDB_TYPE_BLOB => return c.duckdb_bind_blob(stmt, bind_index, @ptrCast([*c]const u8, value), len),
		c.DUCKDB_TYPE_UUID => {
			if (len != 36) return DuckDBError;
			return c.duckdb_bind_varchar_length(stmt, bind_index, value, 36);
		},
		else => return DuckDBError,
	}
}

fn bindError(comptime T: type) void {
	@compileError("cannot bind value of type " ++ @typeName(T));
}

fn hugeInt(value: i128) c.duckdb_hugeint {
	return .{
		.lower = @intCast(u64, @mod(value, 18446744073709551616)),
		.upper = @intCast(i64, @divFloor(value, 18446744073709551616)),
	};
}

pub const Pool = struct {
	db: DB,
	mutex: std.Thread.Mutex,
	cond: std.Thread.Condition,
	conns: []Conn,
	available: usize,
	allocator: Allocator,

	pub const Config = struct {
		size: usize = 5,
		on_connection: ?*const fn(conn: Conn) anyerror!void = null,
		on_first_connection: ?*const fn(conn: Conn) anyerror!void = null,
	};

	pub fn init(db: DB, config: Config) DBResult(Pool) {
		const size = config.size;
		const allocator = db.allocator;
		const conns = allocator.alloc(Conn, size) catch |err| {
			return .{.err = DBErr.static(err, "OOM") };
		};

		// if something fails while we're setting up the pool, we need to close
		// any connection that we've initialized
		var init_count: usize = 0;
		const on_connection = config.on_connection;
		for (0..size) |i| {
			var conn = db.conn() catch |err| {
				poolInitFailCleanup(allocator, conns, init_count);
				return .{.err = DBErr.static(err, "open connection failure") };
			};
			init_count += 1;
			if (i == 0) {
				if (config.on_first_connection) |f| {
					f(conn) catch |err| {
						poolInitFailCleanup(allocator, conns, init_count);
						return .{.err = DBErr.static(err, "on_first_connection failure") };
					};
				}
			}
			if (on_connection) |f| {
				f(conn) catch |err| {
					poolInitFailCleanup(allocator, conns, init_count);
					return .{.err = DBErr.static(err, "on_connection failure") };
				};
			}
			conns[i] = conn;
		}

		return .{.ok = .{
			.db = db,
			.conns = conns,
			.available = size,
			.allocator = allocator,
			.mutex = std.Thread.Mutex{},
			.cond = std.Thread.Condition{},
		}};
	}

	pub fn deinit(self: *const Pool) void {
		const allocator = self.allocator;
		for (self.conns) |*conn| {
			conn.deinit();
		}
		allocator.free(self.conns);
		self.db.deinit();
	}

	pub fn acquire(self: *Pool) Conn {
		self.mutex.lock();
		while (true) {
			const conns = self.conns;
			const available = self.available;
			if (available == 0) {
				self.cond.wait(&self.mutex);
				continue;
			}
			const index = available - 1;
			const conn = conns[index];
			self.available = index;
			self.mutex.unlock();
			return conn;
		}
	}

	pub fn release(self: *Pool, conn: Conn) void {
		self.mutex.lock();

		var conns = self.conns;
		const available = self.available;
		conns[available] = conn;
		self.available = available + 1;
		self.mutex.unlock();
		self.cond.signal();
	}
};

fn poolInitFailCleanup(allocator: Allocator, conns: []Conn, count: usize) void {
	for (0..count) |i| {
		conns[i].deinit();
	}
	allocator.free(conns);
}

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

	pub fn jsonStringify(self: ParameterType, options: std.json.StringifyOptions, out: anytype) !void {
		return std.json.encodeJsonString(@tagName(self), options, out);
	}
};

const DBResultTag = enum {
	ok,
	err,
};

// T can be a DB or a Pol
fn DBResult(comptime T: type) type {
	return union(DBResultTag) {
		ok: T,
		err: DBErr,

		const Self = @This();
		pub fn deinit(self: Self) void {
			switch (self) {
				inline else => |case| case.deinit(),
			}
		}
	};
}

const DBErr = struct {
	err: anyerror,
	desc: []const u8,
	c_err: ?[*c]u8 = null,

	pub fn deinit(self: DBErr) void {
		if (self.c_err) |err| {
			c.duckdb_free(err);
		}
	}

	fn static(err: anyerror, desc: [:0]const u8) DBErr {
		return .{.err = err, .desc = desc};
	}
};

pub const ResultTag = enum {
	ok,
	err,
};

fn Result(comptime T: type) type {
	return union(ResultTag) {
		ok: T,
		err: ResultErr,

		const Self = @This();
		pub fn deinit(self: Self) void {
			switch (self) {
				inline else => |case| case.deinit(),
			}
		}
	};
}

pub const ResultErr = struct {
	err: anyerror,
	desc: []const u8,
	allocator: Allocator = undefined,
	result: ?*c.duckdb_result = null,
	stmt: ?*c.duckdb_prepared_statement = null,

	const Ownership = struct {
		stmt: ?*c.duckdb_prepared_statement = null,
		result: ?*c.duckdb_result = null,
	};

	fn fromAllocator(err: anyerror, own: Ownership) ResultErr {
		return static(err, "OOM", own);
	}

	fn static(err: anyerror, desc: [:0]const u8, own: Ownership) ResultErr {
		return .{.err = err, .desc = desc, .stmt = own.stmt, .result = own.result};
	}

	fn fromResult(allocator: Allocator, stmt: ?*c.duckdb_prepared_statement, result: *c.duckdb_result) ResultErr {
		return .{
			.stmt = stmt,
			.result = result,
			.err = error.InvalidSQL,
			.allocator = allocator,
			.desc = std.mem.span(c.duckdb_result_error(result)),
		};
	}

	pub fn deinit(self: ResultErr) void {
		if (self.result) |r| {
			c.duckdb_destroy_result(r);
			self.allocator.free(@ptrCast([*]u8, r)[0..RESULT_SIZEOF]);
		}
		if (self.stmt) |s| {
			c.duckdb_destroy_prepare(s);
			self.allocator.free(@ptrCast([*]u8, s)[0..STATEMENT_SIZEOF]);
		}
	}
};

const t = std.testing;
test "open invalid path" {
	const res = DB.init(t.allocator, "/tmp/zuckdb.zig/doesnotexist").err;
	defer res.deinit();
	try t.expectEqualStrings("IO Error: Cannot open file \"/tmp/zuckdb.zig/doesnotexist\": No such file or directory", res.desc);
}

test "exec error" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	try t.expectError(error.ExecFailed, conn.exec("select from x"));
}

test "exec success" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	try conn.exec("create table t (id int)");
	try conn.exec("insert into t (id) values (39)");

	var rows = conn.queryZ("select * from t", .{}).ok;
	defer rows.deinit();
	try t.expectEqual(@as(i64, 39), (try rows.next()).?.get(i32, 0).?);
}

test "query error" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	const err = conn.queryZ("select from x", .{}).err;
	defer err.deinit();
	try t.expectEqualStrings("Parser Error: SELECT clause without selection list", err.desc);
}

test "query select ok" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	var res = conn.queryZ("select 39213", .{});
	defer res.deinit();
	var rows = switch (res) {
		.err => unreachable,
		.ok => |rows| rows,
	};

	const row = (try rows.next()).?;
	try t.expectEqual(@as(i32, 39213), row.get(i32, 0).?);
	try t.expectEqual(@as(?Row, null), try rows.next());
}

test "query empty" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	var res = conn.queryZ("select 1 where false", .{});
	defer res.deinit();
	var rows = switch (res) {
		.err => unreachable,
		.ok => |rows| rows,
	};
	try t.expectEqual(@as(?Row, null), try rows.next());
}

test "query mutate ok" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		const rows = conn.query("create table test(id integer);", .{}).ok;
		defer rows.deinit();
		try t.expectEqual(@as(usize, 0), rows.count());
		try t.expectEqual(@as(usize, 0), rows.changed());
	}

	{
		const rows = conn.queryZ("insert into test (id) values (9001);", .{}).ok;
		defer rows.deinit();

		try t.expectEqual(@as(usize, 1), rows.count());
		try t.expectEqual(@as(usize, 1), rows.changed());
	}
}

test "query column names" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	try conn.execZ("create table test(id integer, name varchar);");
	const rows = conn.queryZ("select id, name from test", .{}).ok;
	defer rows.deinit();
	try t.expectEqual(@as(usize, 2), rows.column_count);
	try t.expectEqualStrings("id", std.mem.span(rows.columnName(0)));
	try t.expectEqualStrings("name", std.mem.span(rows.columnName(1)));

}

test "prepare error" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	const stmt = conn.prepare("select x");
	defer stmt.deinit();

	switch (stmt) {
		.ok => unreachable,
		.err => |err| {
			try t.expectEqualStrings("Binder Error: Referenced column \"x\" not found in FROM clause!\nLINE 1: select x\n               ^", err.desc);
		}
	}
}

test "binding" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		// basic types
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

	{
		// int basic signed
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
		// int signed positive limit
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
		// int signed negative limit
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
		// int unsigned positive limit
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

	{
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

	{
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

	{
		// uuid
		var rows = conn.query("select $1::uuid, $2::uuid, $3::uuid, $4::uuid", .{"578D0DF0-A76F-4A8E-A463-42F8A4F133C8", "00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff", "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"}).ok;
		defer rows.deinit();

		const row = (try rows.next()).?;
		try t.expectEqualStrings("578d0df0-a76f-4a8e-a463-42f8a4f133c8", &(row.get(UUID, 0).?));
		try t.expectEqualStrings("00000000-0000-0000-0000-000000000000", &(row.get(UUID, 1).?));
		try t.expectEqualStrings("ffffffff-ffff-ffff-ffff-ffffffffffff", &(row.get(UUID, 2).?));
		try t.expectEqualStrings("ffffffff-ffff-ffff-ffff-ffffffffffff", &(row.get(UUID, 3).?));
	}

	{
		// text
		var rows = conn.query("select $1", .{"hello world",}).ok;
		defer rows.deinit();

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
		var rows = conn.query("select $1", .{&[_]u8{0, 1, 2}}).ok;
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

	{
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
}

// Test this specifically since there's special handling based on the length
// of the column (inlined vs pointer)
test "read varchar" {
	const db = DB.init(t.allocator, ":memory:").ok;
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
test "Row: read blob" {
	const db = DB.init(t.allocator, ":memory:").ok;
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

test "Row: read ints" {
	const db = DB.init(t.allocator, ":memory:").ok;
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

test "Row: read bool" {
	const db = DB.init(t.allocator, ":memory:").ok;
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

test "Row: read float" {
	const db = DB.init(t.allocator, ":memory:").ok;
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

test "Row: read decimal" {
	const db = DB.init(t.allocator, ":memory:").ok;
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

test "Row: read date & time" {
	const db = DB.init(t.allocator, ":memory:").ok;
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

test "Row: list" {
	const db = DB.init(t.allocator, ":memory:").ok;
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

test "transaction" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		//rollback
		try conn.execZ("create table t (id int)");
		try conn.begin();
		try conn.execZ("insert into t (id) values (1)");
		try conn.rollback();

		var rows = conn.queryZ("select * from t", .{}).ok;
		defer rows.deinit();
		try t.expectEqual(@as(?Row, null), try rows.next());
	}

	{
		// commit
		try conn.begin();
		try conn.execZ("insert into t (id) values (1)");
		try conn.commit();

		var rows = conn.queryZ("select * from t", .{}).ok;
		defer rows.deinit();
		try t.expectEqual(@as(i32, 1), (try rows.next()).?.get(i32, 0).?);
	}
}

test "query parameters" {
	const db = DB.init(t.allocator, ":memory:").ok;
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
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	const stmt = conn.prepareZ("select $1::int, $2, $3::smallint").ok;
	errdefer stmt.deinit();
	try stmt.bindDynamic(0, null);
	try stmt.bindDynamic(1, "over");
	try stmt.bindDynamic(2, 9000);

	var rows = stmt.execute().ok;
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(@as(?i32, null), row.get(i32, 0));
	try t.expectEqualStrings("over", row.get([]u8, 1).?);
	try t.expectEqual(@as(i16, 9000), row.get(i16, 2).?);
}

test "owning row" {
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	const conn = try db.conn();
	defer conn.deinit();

	{
		// error case
		try t.expectError(error.InvalidSQL, conn.row("select x", .{}));
	}

	{
		// null
		try t.expectEqual(@as(?OwningRow, null), try conn.row("select 1 where false", .{}));
	}

	{
		const row = (try conn.rowZ("select $1::bigint", .{-991823891832})).?;
		defer row.deinit();
		try t.expectEqual(@as(i64, -991823891832), row.get(i64, 0).?);
	}
}

test "Pool" {
	const db = DB.init(t.allocator, "/tmp/duckdb.zig.test").ok;
	var pool = db.pool(.{
		.size = 2,
		.on_first_connection = &testPoolFirstConnection,
	}).ok;
	defer pool.deinit();

	const t1 = try std.Thread.spawn(.{}, testPool, .{&pool});
	const t2 = try std.Thread.spawn(.{}, testPool, .{&pool});
	const t3 = try std.Thread.spawn(.{}, testPool, .{&pool});

	t1.join(); t2.join(); t3.join();

	const c1 = pool.acquire();
	defer pool.release(c1);

	const result = c1.queryZ("delete from pool_test", .{});
	defer result.deinit();
	try t.expectEqual(@as(usize, 3000), result.ok.changed());
}

fn testPool(p: *Pool) void {
	for (0..1000) |i| {
		const conn = p.acquire();
		conn.queryZ("insert into pool_test (id) values ($1)", .{i}).ok.deinit();
		p.release(conn);
	}
}

fn testPoolFirstConnection(conn: Conn) !void {
	conn.queryZ("drop table if exists pool_test", .{}).ok.deinit();
	conn.queryZ("create table pool_test (id uint16 not null)", .{}).ok.deinit();
}
