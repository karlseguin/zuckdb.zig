const std = @import("std");
const c = @cImport(@cInclude("zuckdb.h"));

const DB = @import("db.zig").DB;
const Row = @import("row.zig").Row;
const Rows = @import("rows.zig").Rows;
const Stmt = @import("stmt.zig").Stmt;
const Result = @import("result.zig").Result;
const OwningRow = @import("row.zig").OwningRow;

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

const CONN_SIZEOF = c.connection_sizeof;
const CONN_ALIGNOF = c.connection_alignof;
const RESULT_SIZEOF = c.result_sizeof;
const RESULT_ALIGNOF = c.result_alignof;
const STATEMENT_SIZEOF = c.statement_sizeof;
const STATEMENT_ALIGNOF = c.statement_alignof;

pub const Conn = struct {
	version: u32 = 0,
	allocator: Allocator,
	conn: *c.duckdb_connection,
	stmt_cache: std.StringHashMap(*c.duckdb_prepared_statement),

	pub fn open(db: DB) !Conn {
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
			.stmt_cache = std.StringHashMap(*c.duckdb_prepared_statement).init(allocator),
		};
	}

	pub fn deinit(self: Conn) void {
		const conn = self.conn;
		const allocator = self.allocator;

		self.clearStatementCache();
		var stmt_cache = self.stmt_cache;
		stmt_cache.deinit();

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
			return Result(Rows).allocErr(err, .{});
		};
		defer self.allocator.free(zql);
		return self.queryZ(zql, values);
	}

	pub fn queryZ(self: Conn, sql: [:0]const u8, values: anytype) Result(Rows) {
		if (values.len == 0) {
				const allocator = self.allocator;
				var slice = allocator.alignedAlloc(u8, RESULT_ALIGNOF, RESULT_SIZEOF) catch |err| {
				return Result(Rows).allocErr(err, .{});
			};
			const result = @ptrCast(*c.duckdb_result, slice.ptr);
			if (c.duckdb_query(self.conn.*, sql, result) == DuckDBError) {
				return Result(Rows).resultErr(allocator, null, result);
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

	pub fn rowZ(self: Conn, sql: [:0]const u8, values: anytype) !?OwningRow {
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
			return Result(Stmt).allocErr(err, .{});
		};
		defer self.allocator.free(zql);
		return self.prepareZ(zql);
	}

	pub fn prepareZ(self: *const Conn, sql: [:0]const u8) Result(Stmt) {
		const allocator = self.allocator;
		var slice = allocator.alignedAlloc(u8, STATEMENT_ALIGNOF, STATEMENT_SIZEOF) catch |err| {
			return Result(Stmt).allocErr(err, .{});
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

	pub fn queryCache(self: *Conn, name: []const u8, sql: []const u8, values: anytype) Result(Rows) {
		const zql = self.allocator.dupeZ(u8, sql) catch |err| {
			return Result(Rows).allocErr(err, .{});
		};
		defer self.allocator.free(zql);
		return self.queryCacheZ(name, zql, values);
	}

	pub fn queryCacheZ(self: *Conn, name: []const u8, sql: [:0]const u8, values: anytype) Result(Rows) {
		const allocator = self.allocator;

		var stmt: Stmt = undefined;
		if (self.stmt_cache.get(name)) |c_stmt| {
			stmt = Stmt{.allocator = allocator, .stmt = c_stmt};
		} else {
			const prepare_result = self.prepareZ(sql);
			stmt = switch (prepare_result) {
				.ok => |s| s,
				.err => |err| return .{.err = err},
			};

			const owned_name = allocator.dupe(u8, name) catch |err| {
				return Result(Rows).allocErr(err, .{.stmt = stmt.stmt});
			};
			self.stmt_cache.put(owned_name, stmt.stmt) catch |err| {
				return Result(Rows).allocErr(err, .{.stmt = stmt.stmt});
			};
		}

		stmt.bind(values) catch |err| {
			return .{.err = .{
				.err = err,
				.stmt = null, // don't want to free this, as its meant to be cached
				.desc = "bind error",
				.allocator = self.allocator,
			}};
		};

		return stmt.executeReleaseable(false);
	}

	pub fn clearStatementCache(self: Conn) void {
		const allocator = self.allocator;
		var stmt_cache = self.stmt_cache;

		var it = stmt_cache.iterator();
		while (it.next()) |entry| {
			allocator.free(entry.key_ptr.*);
			const stmt = entry.value_ptr.*;
			c.duckdb_destroy_prepare(stmt);
			allocator.free(@ptrCast([*]u8, stmt)[0..STATEMENT_SIZEOF]);
		}
		stmt_cache.clearRetainingCapacity();
	}
};

const t = std.testing;
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

test "queryCache" {
	std.debug.print("HERE\n", .{});
	const db = DB.init(t.allocator, ":memory:").ok;
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	{
		//initial query
		var rows = conn.queryCache("q1", "select $1::int", .{334}).ok;
		defer rows.deinit();
		try t.expectEqual(@as(i32, 334), (try rows.next()).?.get(i32, 0).?);
	}

	{
		// from cache
		var rows = conn.queryCache("q1", "", .{999}).ok;
		defer rows.deinit();
		try t.expectEqual(@as(i32, 999), (try rows.next()).?.get(i32, 0).?);
	}

	{
		// separate query
		var rows = conn.queryCache("q2", "select $1::varchar", .{"teg"}).ok;
		defer rows.deinit();
		try t.expectEqualStrings("teg", (try rows.next()).?.get([]u8, 0).?);
	}

	conn.clearStatementCache();
	{
		// q1 should not be loaded from the cache, since we cleared
		var rows = conn.queryCache("q1", "select $1::int+1000", .{334}).ok;
		defer rows.deinit();
		try t.expectEqual(@as(i32, 1334), (try rows.next()).?.get(i32, 0).?);
	}
}
