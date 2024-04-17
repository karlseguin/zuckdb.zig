const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const Row = lib.Row;
const Vector = lib.Vector;
const DataType = lib.DataType;

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

	// Vector data + validity  + type info for the current chunk
	vectors: []Vector = undefined,

	// The duckdb gives us the enum name, but as a C string we need to free. This
	// is potentially both expensive and awkward. We're going to intern the enums
	// and manage the strings in an arena.
	_arena: *std.heap.ArenaAllocator,
	arena: std.mem.Allocator,

	// We might have more than 1 enum column. Our cache is:
	//   column_index =>  internal_enum_integer => string
	enum_name_cache: std.AutoHashMap(u64, std.AutoHashMap(u64, []const u8)),

	pub fn init(allocator: Allocator, stmt: ?*c.duckdb_prepared_statement, result: *c.duckdb_result, state: anytype) !Rows {
		errdefer if (stmt) |s| {
			c.duckdb_destroy_prepare(s);
			allocator.destroy(s);
		};

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
				.vectors = &[_]Vector{},
				.enum_name_cache = std.AutoHashMap(u64, std.AutoHashMap(u64, []const u8)).init(undefined),
			};
		}

		const arena = try allocator.create(std.heap.ArenaAllocator);
		errdefer arena.deinit();
		arena.* = std.heap.ArenaAllocator.init(allocator);

		const aa = arena.allocator();

		var vectors: []Vector = undefined;

		if (@TypeOf(state) == @TypeOf(null)) {
			vectors = try aa.alloc(Vector, column_count);
		} else {
			vectors = try state.getVectors(column_count);
		}

		for (0..column_count) |i| {
			vectors[i] = .{
				// loaded when we actually read a chunk
				.data = undefined,
				// loaded when we actually read a chunk
				.validity = undefined,

				// loaded once and re-used for each chunk
				.type = try Vector.Type.init(aa, c.duckdb_column_logical_type(result, i)),
			};
		}

		return .{
			.stmt = stmt,
			._arena = arena,
			.arena = aa,
			.result = result,
			.allocator = allocator,
			.chunk_count = chunk_count,
			.column_count = column_count,
			.vectors = vectors,
			.enum_name_cache = std.AutoHashMap(u64, std.AutoHashMap(u64, []const u8)).init(aa),
		};
	}

	pub fn deinit(self: Rows) void {
		for (self.vectors) |*v| {
			v.type.deinit();
		}

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

	pub fn columnType(self: Rows, i: usize) DataType {
		return DataType.fromDuckDBType(self.column_types[i]);
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
			.vectors = self.vectors,
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

			var vectors = self.vectors;

			for (0..column_count) |col| {
				const vector = &vectors[col];
				const real_vector = c.duckdb_data_chunk_get_vector(chunk, col);

				vector.data = switch (vector.type) {
					.list => |*l| .{.container = generateListData(l, real_vector)},
					.scalar => |*s| .{.scalar = generateScalarData(s, real_vector)},
				};

				vector.validity = c.duckdb_vector_get_validity(real_vector);
			}

			self.chunk = chunk;
			self.row_count = row_count;
			self.chunk_index = chunk_index;

			return true;
		}
		unreachable;
	}
};

fn generateScalarData(scalar_type: *Vector.Type.Scalar, real_vector: c.duckdb_vector) Vector.Scalar {
	const raw_data = c.duckdb_vector_get_data(real_vector);
	switch (scalar_type.*) {
		.@"enum" => |*e| {
			return .{.@"enum" = .{
				.cache = &e.cache,
				.logical_type = e.logical_type,
				.internal = switch (e.type) {
					.u8 => .{ .u8 = @ptrCast(raw_data) },
					.u16 => .{ .u16 = @ptrCast(@alignCast(raw_data)) },
					.u32 => .{ .u32 = @ptrCast(@alignCast(raw_data)) },
					.u64 => .{ .u64 = @ptrCast(@alignCast(raw_data)) },
				},
			}};
		},
		.decimal => |d| {
			return .{.decimal = .{
				.width = d.width,
				.scale = d.scale,
				.internal = switch (d.type) {
					.i16 => .{ .i16 = @ptrCast(@alignCast(raw_data)) },
					.i32 => .{ .i32 = @ptrCast(@alignCast(raw_data)) },
					.i64 => .{ .i64 = @ptrCast(@alignCast(raw_data)) },
					.i128 => .{ .i128 = @ptrCast(@alignCast(raw_data)) },
				},
			}};
		},
		.simple => |s| switch (s) {
			c.DUCKDB_TYPE_BLOB, c.DUCKDB_TYPE_VARCHAR, c.DUCKDB_TYPE_BIT => return .{ .blob = @ptrCast(@alignCast(raw_data)) },
			c.DUCKDB_TYPE_TINYINT => return .{ .i8 = @ptrCast(raw_data) },
			c.DUCKDB_TYPE_SMALLINT => return .{ .i16 = @ptrCast(@alignCast(raw_data)) },
			c.DUCKDB_TYPE_INTEGER => return .{ .i32 = @ptrCast(@alignCast(raw_data)) },
			c.DUCKDB_TYPE_BIGINT => return .{ .i64 = @ptrCast(@alignCast(raw_data)) },
			c.DUCKDB_TYPE_HUGEINT, c.DUCKDB_TYPE_UUID => return .{ .i128 = @ptrCast(@alignCast(raw_data)) },
			c.DUCKDB_TYPE_UHUGEINT => return .{ .u128 = @ptrCast(@alignCast(raw_data)) },
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
			c.DUCKDB_TYPE_TIMESTAMP_TZ => return .{ .timestamp = @ptrCast(@alignCast(raw_data)) },
			c.DUCKDB_TYPE_INTERVAL => return .{ .interval = @ptrCast(@alignCast(raw_data)) },
			else => unreachable,
		}
	}
}

fn generateListData(child_type: *Vector.Type.Scalar, real_vector: c.duckdb_vector) Vector.Container {
	const raw_data = c.duckdb_vector_get_data(real_vector);

	const child_vector = c.duckdb_list_vector_get_child(real_vector);
	const child_data = generateScalarData(child_type, child_vector);
	const child_validity = c.duckdb_vector_get_validity(child_vector);

	return .{.list = .{
		.child = child_data,
		.validity = child_validity,
		.entries = @ptrCast(@alignCast(raw_data)),
		.type = switch (child_type.*) {
			.@"enum" => c.DUCKDB_TYPE_ENUM,
			.decimal => c.DUCKDB_TYPE_DECIMAL,
			.simple => |s| s,
		},
	}};
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
