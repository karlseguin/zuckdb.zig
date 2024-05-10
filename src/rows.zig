const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const Row = lib.Row;
const Conn = lib.Conn;
const Vector = lib.Vector;
const DataType = lib.DataType;

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

pub const Rows = struct {
	// Depending on how it's executed, Rows might own a prepared statement and/or
	// a connection (or neither). By "own", we mean that when rows.deinit() is
	// called, it must free/release whatever it owns.
	own: Own,

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

	arena: *std.heap.ArenaAllocator,

	const Own = struct {
		conn: ?*Conn = null,
		stmt: ?*c.duckdb_prepared_statement = null,
	};

	pub fn init(allocator: Allocator, result: *c.duckdb_result, state: anytype, own: Own) !Rows {
		errdefer {
			if (own.stmt) |s| {
				c.duckdb_destroy_prepare(s);
				allocator.destroy(s);
			}
			if (own.conn) |conn| {
				conn.release();
			}
		}

		const r = result.*;
		const chunk_count = c.duckdb_result_chunk_count(r);
		const column_count = c.duckdb_column_count(result);

		var arena = try allocator.create(std.heap.ArenaAllocator);
		errdefer allocator.destroy(arena);

		arena.* = std.heap.ArenaAllocator.init(allocator);
		errdefer arena.deinit();

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
			.own = own,
			.arena = arena,
			.result = result,
			.vectors = vectors,
			.chunk_count = chunk_count,
			.column_count = column_count,
		};
	}

	pub fn deinit(self: *const Rows) void {
		for (self.vectors) |*v| {
			v.deinit();
		}

		const allocator = self.arena.child_allocator;
		{
			const result = self.result;
			c.duckdb_destroy_result(result);
			allocator.destroy(result);
		}

		const own = self.own;
		if (own.stmt) |stmt| {
			c.duckdb_destroy_prepare(stmt);
			allocator.destroy(stmt);
		}

		if (own.conn) |conn| {
			conn.release();
		}

		self.arena.deinit();
		allocator.destroy(self.arena);
	}

	pub fn changed(self: *const Rows) usize {
		return c.duckdb_rows_changed(self.result);
	}

	pub fn count(self: *const Rows) usize {
		return c.duckdb_row_count(self.result);
	}

	pub fn columnName(self: *const Rows, i: usize) [*c]const u8 {
		return c.duckdb_column_name(self.result, i);
	}

	pub fn columnType(self: *const Rows, i: usize) DataType {
		switch (self.vectors[i].type) {
			.list => return .list,
			.scalar => |s| switch (s) {
				.@"enum" => return .@"enum",
				.decimal => return .decimal,
				.simple => |duckdb_type| return DataType.fromDuckDBType(duckdb_type),
			}
		}
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
test "rows: introspect" {
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

	try t.expectEqual(.integer, rows.columnType(0));
	try t.expectEqual(.varchar, rows.columnType(1));
}

test "rows: introspect empty" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec("create table test(id integer, name varchar);", .{});

	const rows = try conn.query("select id, name from test where false", .{});
	defer rows.deinit();

	try t.expectEqual(2, rows.column_count);
	try t.expectEqualStrings("id", std.mem.span(rows.columnName(0)));
	try t.expectEqualStrings("name", std.mem.span(rows.columnName(1)));

	try t.expectEqual(.integer, rows.columnType(0));
	try t.expectEqual(.varchar, rows.columnType(1));
}
