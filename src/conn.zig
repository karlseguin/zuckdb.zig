const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const DB = lib.DB;
const Row = lib.Row;
const Rows = lib.Rows;
const Stmt = lib.Stmt;
const OwningRow = lib.OwningRow;

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

pub const Conn = struct {
	err: ?[]const u8,
	allocator: Allocator,
	conn: *c.duckdb_connection,

	pub fn open(db: DB) !Conn {
		const allocator = db.allocator;

		const conn = try allocator.create(c.duckdb_connection);
		errdefer allocator.destroy(conn);

		if (c.duckdb_connect(db.db.*, conn) == DuckDBError) {
			return error.ConnectFail;
		}

		return .{
			.err = null,
			.conn = conn,
			.allocator = allocator,
		};
	}

	pub fn deinit(self: Conn) void {
		const allocator = self.allocator;

		if (self.err) |e| {
			allocator.free(e);
		}

		const conn = self.conn;
		c.duckdb_disconnect(conn);
		allocator.destroy(conn);
	}

	pub fn begin(self: *Conn) !void {
		_ = try self.exec("begin transaction", .{});
	}

	pub fn commit(self: *Conn) !void {
		_ = try self.exec("commit", .{});
	}

	pub fn rollback(self: *Conn) !void {
		_ = try self.exec("rollback", .{});
	}

	pub fn exec(self: *Conn, sql: anytype, values: anytype) !usize {
		const rows = try self.query(sql, values);
		defer rows.deinit();
		return rows.changed();
	}

	pub fn query(self: *Conn, sql: anytype, values: anytype) !Rows {
		return self.queryWithState(sql, values, null);
	}

	pub fn queryWithState(self: *Conn, sql: anytype, values: anytype, state: anytype) !Rows {
		if (values.len > 0) {
			var stmt = try self.prepare(sql, .{.auto_release = true});
			errdefer stmt.deinit();
			try stmt.bind(values);
			return stmt.execute(state);
		}

		const allocator = self.allocator;
		const str = try lib.stringZ(sql, allocator);
		defer str.deinit(allocator);

		const result = try allocator.create(c.duckdb_result);
		errdefer allocator.destroy(result);

		if (c.duckdb_query(self.conn.*, str.z, result) == DuckDBError) {
			try self.duckdbError(c.duckdb_result_error(result));
			return error.DuckDBError;
		}
		return Rows.init(allocator, null, result, state);
	}

	pub fn row(self: *Conn, sql: anytype, values: anytype) !?OwningRow {
		return self.rowWithState(sql, values, null);
	}

	pub fn rowWithState(self: *Conn, sql: anytype, values: anytype, state: anytype) !?OwningRow {
		var rows = try self.queryWithState(sql, values, state);
		errdefer rows.deinit();

		const r = (try rows.next()) orelse {
			rows.deinit();
			return null;
		};

		return .{
			.row = r,
			.rows = rows,
		};
	}

	pub fn prepare(self: *Conn, sql: anytype, opts: Stmt.Opts) !Stmt {
		const allocator = self.allocator;
		const str = try lib.stringZ(sql, allocator);
		defer str.deinit(allocator);

		const stmt = try allocator.create(c.duckdb_prepared_statement);
		errdefer allocator.destroy(stmt);
		if (c.duckdb_prepare(self.conn.*, str.z, stmt) == DuckDBError) {
			try self.duckdbError(c.duckdb_prepare_error(stmt.*));
			return error.DuckDBError;
		}

		return Stmt.init(stmt, self, opts);
	}

	pub fn duckdbError(self: *Conn, err: [*c]const u8) !void {
		const allocator = self.allocator;
		if (self.err) |e| {
			allocator.free(e);
		}
		self.err = try allocator.dupe(u8, std.mem.span(err));
	}
};

const t = std.testing;
test "conn: exec error" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	try t.expectError(error.DuckDBError, conn.exec("select from x", .{}));
	try t.expectEqualStrings("Parser Error: SELECT clause without selection list", conn.err.?);
}

test "conn: exec success" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec("create table t (id int)", .{});
	_ = try conn.exec("insert into t (id) values (39)", .{});

	var rows = try conn.query("select * from t", .{});
	defer rows.deinit();
	try t.expectEqual(39, (try rows.next()).?.get(i32, 0).?);
}

test "conn: query error" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	try t.expectError(error.DuckDBError, conn.query("select from x", .{}));
	try t.expectEqualStrings("Parser Error: SELECT clause without selection list", conn.err.?);
}

test "conn: query select ok" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	var rows = try conn.query("select 39213", .{});
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(39213, row.get(i32, 0).?);
	try t.expectEqual(null, try rows.next());
}

test "conn: query empty" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	var rows = try conn.query("select 1 where false", .{});
	defer rows.deinit();
	try t.expectEqual(null, try rows.next());
}

test "conn: query mutate ok" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	{
		const rows = try conn.query("create table test(id integer);", .{});
		defer rows.deinit();
		try t.expectEqual(0, rows.count());
		try t.expectEqual(0, rows.changed());
	}

	{
		const rows = try conn.query("insert into test (id) values (9001);", .{});
		defer rows.deinit();

		try t.expectEqual(1, rows.count());
		try t.expectEqual(1, rows.changed());
	}
}

test "conn: transaction" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	{
		//rollback
		_ = try conn.exec("create table t (id int)", .{});
		try conn.begin();
		_ = try conn.exec("insert into t (id) values (1)", .{});
		try conn.rollback();

		var rows = try conn.query("select * from t", .{});
		defer rows.deinit();
		try t.expectEqual(null, try rows.next());
	}

	{
		// commit
		try conn.begin();
		_ = try conn.exec("insert into t (id) values (1)", .{});
		try conn.commit();

		var rows = try conn.query("select * from t", .{});
		defer rows.deinit();
		try t.expectEqual(1, (try rows.next()).?.get(i32, 0).?);
	}
}

test "conn: query with explicit state" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	var state = @import("zuckdb.zig").StaticState(2){};
	var rows = try conn.queryWithState("select $1::int, $2::varchar", .{9392, "teg"}, &state);
	defer rows.deinit();
	const row = (try rows.next()).?;
	try t.expectEqual(9392, row.get(i32, 0).?);
	try t.expectEqualStrings("teg", row.get([]u8, 1).?);
}

test "conn: sql with different string types" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	const literal = "select $1::int, $2::varchar";
	try testSQLStringType(&conn, literal);

	{
		const sql = try t.allocator.dupe(u8, literal);
		defer t.allocator.free(sql);
		try testSQLStringType(&conn, sql);
	}

	{
		const sql = try t.allocator.dupeZ(u8, literal);
		defer t.allocator.free(sql);
		try testSQLStringType(&conn, sql);
	}

	{
		const sql = [_]u8{'s', 'e', 'l', 'e', 'c', 't', ' ', '$', '1', ':', ':', 'i', 'n', 't', ',', ' ', '$', '2', ':', ':', 'v', 'a', 'r', 'c', 'h', 'a', 'r'};
		try testSQLStringType(&conn, sql);
	}
}

test "conn: constraint errors" {
	const zuckdb = @import("zuckdb.zig");

	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	_ = try conn.exec("create table t (name varchar not null)", .{});
	_ = try conn.exec("create unique index t_name on t (name)", .{});
	_ = try conn.exec("insert into t (name) values ('leto')", .{});

	{
		// not a duplicate error
		try t.expectError(error.DuckDBError, conn.query("create table t (name varchar not null)", .{}));
		try t.expectEqual(false, zuckdb.isDuplicate(conn.err.?));
	}

	{
		try t.expectError(error.DuckDBError, conn.query("insert into t (name) values ('leto')", .{}));
		try t.expectEqual(true, zuckdb.isDuplicate(conn.err.?));
	}
}

test "conn: prepare error" {
	const db = try DB.init(t.allocator, ":memory:", .{});
	defer db.deinit();

	var conn = try db.conn();
	defer conn.deinit();

	try t.expectError(error.DuckDBError, conn.prepare("select x", .{}));
	try t.expectEqualStrings("Binder Error: Referenced column \"x\" not found in FROM clause!\nLINE 1: select x\n               ^", conn.err.?);
}

fn testSQLStringType(conn: *Conn, sql: anytype) !void {
	var rows = try conn.query(sql, .{9392, "teg"});
	defer rows.deinit();

	const row = (try rows.next()).?;
	try t.expectEqual(9392, row.get(i32, 0).?);
	try t.expectEqualStrings("teg", row.get([]u8, 1).?);
}
