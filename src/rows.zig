const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const Row = lib.Row;
const MapBuilder = lib.MapBuilder;
const ParameterType = lib.ParameterType;
const ColumnData = lib.ColumnData;

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

pub const Rows = struct {
	allocator: Allocator,

	// When not null, the rows owns the stmt and is responsible for freeing it.
	stmt: ?*c.duckdb_prepared_statement,

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

	// The duckdb gives us the enum name, but as a C string we need to free. This
	// is potentially both expensive and awkward. We're going to intern the enums
	// and manage the strings in an arena.
	_arena: *std.heap.ArenaAllocator,
	arena: std.mem.Allocator,

	// We might have more than 1 enum column. Our cache is:
	//   column_index =>  internal_enum_integer => string
	enum_name_cache: std.AutoHashMap(u64, std.AutoHashMap(u64, []const u8)),

	pub fn init(allocator: Allocator, stmt: ?*c.duckdb_prepared_statement, result: *c.duckdb_result, state: anytype) !Rows {
		const r = result.*;
		const chunk_count = c.duckdb_result_chunk_count(r);
		const column_count = c.duckdb_column_count(result);
		if (chunk_count == 0) {
			// no chunk, we don't need to load everything else
			return .{
				.arena = undefined,
				._arena = undefined,
				.stmt = stmt,
				.result = result,
				.chunk_count = 0,
				.allocator = allocator,
				.column_count = column_count,
				.enum_name_cache = std.AutoHashMap(u64, std.AutoHashMap(u64, []const u8)).init(undefined),
			};
		}

		const arena = try allocator.create(std.heap.ArenaAllocator);
		errdefer arena.deinit();
		arena.* = std.heap.ArenaAllocator.init(allocator);

		const aa = arena.allocator();

		var columns: []ColumnData = undefined;
		var column_types: []c.duckdb_type = undefined;

		if (@TypeOf(state) == @TypeOf(null)) {
			columns = try aa.alloc(ColumnData, column_count);
			column_types = try aa.alloc(c.duckdb_type, column_count);
		} else {
			columns = try state.getColumns(column_count);
			column_types = try state.getColumnTypes(column_count);
		}

		for (0..column_count) |i| {
			column_types[i] = c.duckdb_column_type(result, i);
		}

		return .{
			.stmt = stmt,
			._arena = arena,
			.arena = aa,
			.result = result,
			.columns = columns,
			.allocator = allocator,
			.chunk_count = chunk_count,
			.column_count = column_count,
			.column_types = column_types,
			.enum_name_cache = std.AutoHashMap(u64, std.AutoHashMap(u64, []const u8)).init(aa),
		};
	}

	pub fn deinit(self: Rows) void {
		const allocator = self.allocator;

		{
			const result = self.result;
			c.duckdb_destroy_result(result);
			allocator.destroy(result);
		}

		if (self.stmt) |stmt| {
			c.duckdb_destroy_prepare(stmt);
			allocator.destroy(stmt);
		}

		if (self.chunk_count == 0) {
			return;
		}

		self._arena.deinit();
		allocator.destroy(self._arena);
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

	pub fn columnType(self: Rows, i: usize) ParameterType {
		return ParameterType.fromDuckDBType(self.column_types[i]);
	}

	pub fn mapBuilder(self: Rows, allocator: Allocator) !MapBuilder {
		const column_count = self.column_count;

		var arena = std.heap.ArenaAllocator.init(allocator);
		errdefer arena.deinit();

		const aa = arena.allocator();
		var types = try aa.alloc(ParameterType, column_count);
		var names = try aa.alloc([]const u8, column_count);

		for (self.column_types, 0..) |ctype, i| {
			types[i] = ParameterType.fromDuckDBType(ctype);
			names[i] = try aa.dupe(u8, std.mem.span(self.columnName(i)));
		}

		return .{
			.types = types,
			.names = names,
			.arena = arena,
		};
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
				if (generateScalarColumnData(self, vector, column_type)) |scalar| {
					data = .{ .scalar = scalar };
				} else {
					if (generateContainerColumnData(self, vector, column_type)) |container| {
						data = .{ .container = container };
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

fn generateScalarColumnData(rows: *Rows, vector: c.duckdb_vector, column_type: usize) ?ColumnData.Scalar {
	const raw_data = c.duckdb_vector_get_data(vector);
	switch (column_type) {
		c.DUCKDB_TYPE_BLOB, c.DUCKDB_TYPE_VARCHAR, c.DUCKDB_TYPE_BIT => return .{ .blob = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_TINYINT => return .{ .i8 = @ptrCast(raw_data) },
		c.DUCKDB_TYPE_SMALLINT => return .{ .i16 = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_INTEGER => return .{ .i32 = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_BIGINT => return .{ .i64 = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_HUGEINT, c.DUCKDB_TYPE_UUID => return .{ .i128 = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_UTINYINT => return .{ .u8 = @ptrCast(raw_data) },
		c.DUCKDB_TYPE_USMALLINT => return .{ .u16 = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_UINTEGER => return .{ .u32 = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_UBIGINT => return .{ .u64 = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_BOOLEAN => return .{ .bool = @ptrCast(raw_data) },
		c.DUCKDB_TYPE_FLOAT => return .{ .f32 = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_DOUBLE => return .{ .f64 = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_DATE => return .{ .date = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_TIME => return .{ .time = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_TIMESTAMP => return .{ .timestamp = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_INTERVAL => return .{ .interval = @ptrCast(@alignCast(raw_data)) },
		c.DUCKDB_TYPE_DECIMAL => {
			// decimal's storage is based on the width
			const logical_type = c.duckdb_vector_get_column_type(vector);
			const scale = c.duckdb_decimal_scale(logical_type);
			const width = c.duckdb_decimal_width(logical_type);
			const internal: ColumnData.Decimal.Internal = switch (c.duckdb_decimal_internal_type(logical_type)) {
				c.DUCKDB_TYPE_SMALLINT => .{ .i16 = @ptrCast(@alignCast(raw_data)) },
				c.DUCKDB_TYPE_INTEGER => .{ .i32 = @ptrCast(@alignCast(raw_data)) },
				c.DUCKDB_TYPE_BIGINT => .{ .i64 = @ptrCast(@alignCast(raw_data)) },
				c.DUCKDB_TYPE_HUGEINT => .{ .i128 = @ptrCast(@alignCast(raw_data)) },
				else => unreachable,
			};
			return .{ .decimal = .{ .width = width, .scale = scale, .internal = internal } };
		},
		c.DUCKDB_TYPE_ENUM => {
			const logical_type = c.duckdb_vector_get_column_type(vector);
			const internal_type = c.duckdb_enum_internal_type(logical_type);
			return .{
				.@"enum" = .{
					.rows = rows,
					.logical_type = logical_type,
					.internal = switch (internal_type) {
						c.DUCKDB_TYPE_UTINYINT => .{ .u8 = @ptrCast(raw_data) },
						c.DUCKDB_TYPE_USMALLINT => .{ .u16 = @ptrCast(@alignCast(raw_data)) },
						c.DUCKDB_TYPE_UINTEGER => .{ .u32 = @ptrCast(@alignCast(raw_data)) },
						c.DUCKDB_TYPE_UBIGINT => .{ .u64 = @ptrCast(@alignCast(raw_data)) },
						else => @panic("Unsupported enum internal storage type"), // I don't think this can happen, but if it can, I want to know about it
					},
				},
			};
		},
		else => return null,
	}
}

fn generateContainerColumnData(rows: *Rows, vector: c.duckdb_vector, column_type: usize) ?ColumnData.Container {
	const raw_data = c.duckdb_vector_get_data(vector);
	switch (column_type) {
		c.DUCKDB_TYPE_LIST => {
			const child_vector = c.duckdb_list_vector_get_child(vector);
			const child_type = c.duckdb_get_type_id(c.duckdb_vector_get_column_type(child_vector));
			const child_data = generateScalarColumnData(rows, child_vector, child_type) orelse return null;
			const child_validity = c.duckdb_vector_get_validity(child_vector);
			return .{ .list = .{
				.child = child_data,
				.validity = child_validity,
				.type = ParameterType.fromDuckDBType(child_type),
				.entries = @ptrCast(@alignCast(raw_data)),
			} };
		},
		else => return null,
	}
}

const t = std.testing;
const DB = lib.DB;
test "query column names" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec("create table test(id integer, name varchar);", .{});

	const rows = try conn.query("select id, name from test", .{});
	defer rows.deinit();

	try t.expectEqual(2, rows.column_count);
	try t.expectEqualStrings("id", std.mem.span(rows.columnName(0)));
	try t.expectEqualStrings("name", std.mem.span(rows.columnName(1)));
}
