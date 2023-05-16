const std = @import("std");
const c = @cImport(@cInclude("zuckdb.h"));

const DB = @import("db.zig").DB;
const Row = @import("row.zig").Row;
const Stmt = @import("stmt.zig").Stmt;
const Result = @import("result.zig").Result;
const ColumnData = @import("column_data.zig").ColumnData;

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

const RESULT_SIZEOF = c.result_sizeof;

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

	column_provider: ?ColumnProvider,

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
				.column_provider = null,
			}};
		}

		return .{.ok = .{
			.stmt = stmt,
			.result = result,
			.allocator = allocator,
			.chunk_count = chunk_count,
			.column_count = column_count,
			.column_provider = null,
		}};
	}

	pub fn deinit(self: Rows) void {
		const allocator = self.allocator;
		if (self.column_provider) |cp| {
			cp.deinit(allocator);
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
		if (self.column_provider == null) {
			const result = self.result;
			const column_count = self.column_count;

			self.column_provider = try ColumnProvider.init(self.allocator, column_count);
			switch (self.column_provider.?) {
				.static => |*s| {
					self.columns = s.columns[0..column_count];
					self.column_types = s.column_types[0..column_count];
				},
				.dynamic => |d| {
					self.columns = d.columns;
					self.column_types = d.column_types;
				},
			}

			var column_types = self.column_types;
			for (0..column_count) |i| {
				column_types[i] = c.duckdb_column_type(result, i);
			}
		}

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

const ColumnProvider = union(enum) {
	static: StaticProvider,
	dynamic: DynamicProvider,

	fn init(allocator: Allocator, count: usize) !ColumnProvider {
		switch (count) {
			0 => unreachable,
			1...10 => return .{.static = StaticProvider{}},
			else => return .{.dynamic = try DynamicProvider.init(allocator, count)},
		}
	}

	pub fn deinit(self: ColumnProvider, allocator: Allocator) void {
		switch (self) {
			.static => {},
			.dynamic => |dyn| dyn.deinit(allocator),
		}
	}

	const DynamicProvider = struct{
		columns: []ColumnData = undefined,
		column_types: []c.duckdb_type = undefined,

		fn init(allocator: Allocator, count: usize) !DynamicProvider {
			return .{
				.columns = try allocator.alloc(ColumnData, count),
				.column_types = try allocator.alloc(c.duckdb_type, count),
			};
		}

		fn deinit(self: DynamicProvider, allocator: Allocator) void {
			allocator.free(self.columns);
			allocator.free(self.column_types);
		}
	};

	const StaticProvider = struct{
		columns: [10]ColumnData = undefined,
		column_types: [10]c.duckdb_type = undefined,
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

const t = std.testing;
test "query column names" {
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
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
	const db = DB.init(t.allocator, ":memory:", .{}).ok;
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
