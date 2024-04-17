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
			vectors[i] = try Vector.init(aa, c.duckdb_column_logical_type(result, i));
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
			v.deinit();
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
				vector.loadVector(real_vector);
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
