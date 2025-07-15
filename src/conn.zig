const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const DB = lib.DB;
const Row = lib.Row;
const Rows = lib.Rows;
const Pool = lib.Pool;
const Stmt = lib.Stmt;
const OwningRow = lib.OwningRow;

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

pub const Conn = struct {
    pool: ?*Pool = null,
    err: ?[]const u8,
    allocator: Allocator,
    conn: *c.duckdb_connection,
    query_cache: std.StringHashMapUnmanaged(CachedStmt),

    pub fn open(db: *const DB) !Conn {
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
            .query_cache = std.StringHashMapUnmanaged(CachedStmt){},
        };
    }

    pub fn deinit(self: *Conn) void {
        const allocator = self.allocator;

        if (self.err) |e| {
            allocator.free(e);
        }

        var it = self.query_cache.iterator();
        while (it.next()) |kv| {
            allocator.free(kv.key_ptr.*);
            kv.value_ptr.stmt.deinit();
        }
        self.query_cache.deinit(allocator);

        const conn = self.conn;
        c.duckdb_disconnect(conn);
        allocator.destroy(conn);
    }

    pub fn release(self: *Conn) void {
        if (self.pool) |p| {
            p.release(self);
        }
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
        const r = try self.getResult(sql, values);
        if (r.stmt) |s| {
            c.duckdb_destroy_prepare(s);
            self.allocator.destroy(s);
        }
        return self.execResult(r.result);
    }

    pub fn execCache(self: *Conn, name: []const u8, version: u32, sql: anytype, values: anytype) !usize {
        var s = try self.getCachedStmt(name, version, sql, values);
        defer _ = c.duckdb_clear_bindings(s.stmt.*);
        return self.execResult(try s.getResult());
    }

    fn execResult(self: *Conn, result: *c.duckdb_result) !usize {
        defer {
            c.duckdb_destroy_result(result);
            self.allocator.destroy(result);
        }
        return c.duckdb_rows_changed(result);
    }

    pub fn query(self: *Conn, sql: anytype, values: anytype) !Rows {
        return self.queryWithState(sql, values, null);
    }

    pub fn queryCache(self: *Conn, name: []const u8, version: u32, sql: anytype, values: anytype) !Rows {
        self.queryCacheWithState(name, version, sql, values, null);
    }

    pub fn queryWithState(self: *Conn, sql: anytype, values: anytype, state: anytype) !Rows {
        const result = try self.getResult(sql, values);
        return Rows.init(self.allocator, result.result, state, .{ .stmt = result.stmt });
    }

    pub fn queryCacheWithState(self: *Conn, name: []const u8, version: u32, sql: anytype, values: anytype, state: anytype) !Rows {
        var stmt = try self.getCachedStmt(name, version, sql, values);
        return stmt.query(state);
    }

    pub fn row(self: *Conn, sql: anytype, values: anytype) !?OwningRow {
        return self.rowWithState(sql, values, null);
    }

    pub fn rowCache(self: *Conn, name: []const u8, version: u32, sql: anytype, values: anytype) !?OwningRow {
        return self.rowCacheWithState(name, version, sql, values, null);
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

    pub fn rowCacheWithState(self: *Conn, name: []const u8, version: u32, sql: anytype, values: anytype, state: anytype) !?OwningRow {
        var rows = try self.queryCacheWithState(name, version, sql, values, state);
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

    pub fn appender(self: *Conn, schema: ?[]const u8, table: []const u8) !lib.Appender {
        const allocator = self.allocator;

        var schema_z: ?lib.StringZ = null;

        defer if (schema_z) |sz| sz.deinit(allocator);
        if (schema) |s| {
            schema_z = try lib.stringZ(s, allocator);
        }

        const table_z = try lib.stringZ(table, allocator);
        defer table_z.deinit(allocator);

        const a = try allocator.create(c.duckdb_appender);
        errdefer allocator.destroy(a);
        if (c.duckdb_appender_create(self.conn.*, if (schema_z) |s| s.z else null, table_z.z, a) == DuckDBError) {
            return error.DuckDBError;
        }
        return lib.Appender.init(self.allocator, a);
    }

    pub fn duckdbError(self: *Conn, err: [*c]const u8) !void {
        const allocator = self.allocator;
        if (self.err) |e| {
            allocator.free(e);
        }
        self.err = try allocator.dupe(u8, std.mem.span(err));
    }

    const Result = struct {
        stmt: ?*c.duckdb_prepared_statement,
        result: *c.duckdb_result,
    };

    pub fn getResult(self: *Conn, sql: anytype, values: anytype) !Result {
        if (values.len > 0) {
            var stmt = try self.prepare(sql, .{ .auto_release = true });
            errdefer stmt.deinit();
            try stmt.bind(values);

            return .{
                .stmt = stmt.stmt,
                .result = try stmt.getResult(),
            };
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

        return .{
            .stmt = null,
            .result = result,
        };
    }

    fn getCachedStmt(self: *Conn, name: []const u8, version: u32, sql: anytype, values: anytype) !Stmt {
        var stmt: ?Stmt = null;
        const gop = try self.query_cache.getOrPut(self.allocator, name);
        if (gop.found_existing) {
            const cached = gop.value_ptr;
            if (cached.version == version) {
                stmt = cached.stmt;
            } else {
                cached.stmt.deinit();
            }
        }

        if (stmt == null) {
            errdefer if (gop.found_existing) {
                // if we're here, we then we must have called deinit() on the cached statement
                // but the entry is still in the cache (because we were expecting to replace it)
                // if we fail, we need to remove it so that subsequent calls don't get the
                // deinitialize statement.
                const removed = self.query_cache.fetchRemove(name);
                self.allocator.free(removed.?.key);
            };

            stmt = try self.prepare(sql, .{ .auto_release = false });
            if (gop.found_existing == false) {
                gop.key_ptr.* = try self.allocator.dupe(u8, name);
            }
            gop.value_ptr.* = .{ .stmt = stmt.?, .version = version };
        }

        var s = stmt.?;
        try s.bind(values);
        return s;
    }
};

const CachedStmt = struct {
    stmt: Stmt,
    version: u32,
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
    try t.expectEqual(1, try conn.exec("insert into t (id) values (39)", .{}));

    var rows = try conn.query("select * from t", .{});
    defer rows.deinit();
    try t.expectEqual(39, (try rows.next()).?.get(i32, 0));
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
    try t.expectEqual(39213, row.get(i32, 0));
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
        try t.expectEqual(1, (try rows.next()).?.get(i32, 0));
    }
}

test "conn: query with explicit state" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    var state = @import("zuckdb.zig").StaticState(2){};
    var rows = try conn.queryWithState("select $1::int, $2::varchar", .{ 9392, "teg" }, &state);
    defer rows.deinit();
    const row = (try rows.next()).?;
    try t.expectEqual(9392, row.get(i32, 0));
    try t.expectEqualStrings("teg", row.get([]u8, 1));
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
        const sql = [_]u8{ 's', 'e', 'l', 'e', 'c', 't', ' ', '$', '1', ':', ':', 'i', 'n', 't', ',', ' ', '$', '2', ':', ':', 'v', 'a', 'r', 'c', 'h', 'a', 'r' };
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
    try t.expectEqualStrings("Binder Error: Referenced column \"x\" not found in FROM clause!\n\nLINE 1: select x\n               ^", conn.err.?);
}

test "conn: query cache" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    {
        const row = (try conn.rowCache("q1", 1, "select $1::int", .{2})) orelse unreachable;
        defer row.deinit();
        try t.expectEqual(2, row.get(i32, 0));
    }

    {
        const row = (try conn.rowCache("q1", 1, "select $1::int", .{33})) orelse unreachable;
        defer row.deinit();
        try t.expectEqual(33, row.get(i32, 0));
    }

    {
        const row = (try conn.rowCache("q1", 2, "select $1::text", .{"hello"})) orelse unreachable;
        defer row.deinit();
        try t.expectEqualStrings("hello", row.get([]u8, 0));
    }

    {
        const row = (try conn.rowCache("q2", 1, "select $1::bool", .{true})) orelse unreachable;
        defer row.deinit();
        try t.expectEqual(true, row.get(bool, 0));
    }

    {
        // make sure that a failure to prepare a new version doesn't leak anything
        try t.expectError(error.DuckDBError, conn.rowCache("q2", 2, "select $1::fail", .{true}));

        const row = (try conn.rowCache("q2", 3, "select $1::utinyint", .{255})) orelse unreachable;
        defer row.deinit();
        try t.expectEqual(255, row.get(u8, 0));
    }
}

test "conn: exec cache" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table t1 (id integer)", .{});

    try t.expectEqual(1, try conn.execCache("q1", 1, "insert into t1 values ($1)", .{2}));
    try t.expectEqual(1, try conn.execCache("q1", 1, "insert into t1 values ($1)", .{3}));
    try t.expectEqual(1, try conn.execCache("q1", 2, "insert into t1 values (5)", .{}));
    try t.expectEqual(1, try conn.execCache("q2", 1, "insert into t1 values (0)", .{}));

    var rows = try conn.query("select * from t1 order by id", .{});
    defer rows.deinit();
    try t.expectEqual(0, (try rows.next()).?.get(i32, 0));
    try t.expectEqual(2, (try rows.next()).?.get(i32, 0));
    try t.expectEqual(3, (try rows.next()).?.get(i32, 0));
    try t.expectEqual(5, (try rows.next()).?.get(i32, 0));
    try t.expectEqual(null, rows.next());
}

fn testSQLStringType(conn: *Conn, sql: anytype) !void {
    var rows = try conn.query(sql, .{ 9392, "teg" });
    defer rows.deinit();

    const row = (try rows.next()).?;
    try t.expectEqual(9392, row.get(i32, 0));
    try t.expectEqualStrings("teg", row.get([]u8, 1));
}
