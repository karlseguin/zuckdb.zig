const std = @import("std");
const lib = @import("lib.zig");

const DB = lib.DB;
const Conn = lib.Conn;
const Rows = lib.Rows;
const OwningRow = lib.OwningRow;

const Allocator = std.mem.Allocator;

pub const Pool = struct {
    db: DB,
    timeout: u64,
    conns: []*Conn,
    shutdown: bool,
    available: usize,
    mutex: std.Thread.Mutex,
    cond: std.Thread.Condition,
    allocator: Allocator,

    pub const Config = struct {
        size: usize = 5,
        timeout: u32 = 10 * std.time.ms_per_s,
        on_connection: ?*const fn (conn: *Conn) anyerror!void = null,
        on_first_connection: ?*const fn (conn: *Conn) anyerror!void = null,
    };

    pub fn init(db: DB, config: Config) !*Pool {
        const size = config.size;
        const allocator = db.allocator;

        const pool = try allocator.create(Pool);
        errdefer allocator.destroy(pool);

        const conns = try allocator.alloc(*Conn, size);
        errdefer allocator.free(conns);

        // if something fails while we're setting up the pool, we need to close
        // any connection that we've initialized
        var initialized: usize = 0;
        errdefer {
            for (0..initialized) |i| {
                conns[i].deinit();
                allocator.destroy(conns[i]);
            }
        }

        const on_connection = config.on_connection;

        for (0..size) |i| {
            const conn = try allocator.create(Conn);
            errdefer allocator.destroy(conn);

            conn.* = try db.conn();
            conns[i] = conn;
            conn.pool = pool;
            initialized += 1;

            if (i == 0) {
                if (config.on_first_connection) |f| {
                    try f(conn);
                }
            }

            if (on_connection) |f| {
                try f(conn);
            }
        }

        pool.* = .{
            .db = db,
            .cond = .{},
            .mutex = .{},
            .conns = conns,
            .shutdown = false,
            .available = size,
            .allocator = allocator,
            .timeout = @as(u64, @intCast(config.timeout)) * std.time.ns_per_ms,
        };
        return pool;
    }

    // blocks until all connections can be safely removed from the pool
    pub fn deinit(self: *Pool) void {
        const conns = self.conns;
        self.mutex.lock();
        self.shutdown = true;
        // any thread blocked in acquire() will unblock, check self.shutdown
        // and return an error
        self.cond.broadcast();

        while (true) {
            if (self.available == conns.len) {
                break;
            }
            self.cond.wait(&self.mutex);
        }

        // Don't need to lock this while we deallocate, as any calls to acquire
        // will see the shutdown = true;
        self.mutex.unlock();

        const allocator = self.allocator;
        for (conns) |conn| {
            conn.deinit();
            allocator.destroy(conn);
        }
        allocator.free(self.conns);
        self.db.deinit();

        allocator.destroy(self);
    }

    pub fn acquire(self: *Pool) !*Conn {
        const conns = self.conns;

        self.mutex.lock();
        errdefer self.mutex.unlock();
        while (true) {
            if (self.shutdown) {
                return error.PoolShuttingDown;
            }
            const available = self.available;
            if (available == 0) {
                try self.cond.timedWait(&self.mutex, self.timeout);
                continue;
            }
            const index = available - 1;
            const conn = conns[index];
            self.available = index;
            self.mutex.unlock();

            return conn;
        }
    }

    pub fn release(self: *Pool, conn: *Conn) void {
        var conns = self.conns;
        if (conn.err) |err| {
            conn.allocator.free(err);
            conn.err = null;
        }

        self.mutex.lock();
        const available = self.available;
        conns[available] = conn;
        self.available = available + 1;
        self.mutex.unlock();

        self.cond.signal();
    }

    pub fn exec(self: *Pool, sql: anytype, values: anytype) !usize {
        var conn = try self.acquire();
        defer self.release(conn);
        return conn.exec(sql, values);
    }

    pub fn query(self: *Pool, sql: anytype, values: anytype) !Rows {
        return self.queryWithState(sql, values, null);
    }

    pub fn queryWithState(self: *Pool, sql: anytype, values: anytype, state: anytype) !Rows {
        const conn = try self.acquire();
        errdefer self.release(conn);

        const result = try conn.getResult(sql, values);
        return Rows.init(self.allocator, result.result, state, .{
            .conn = conn,
            .stmt = result.stmt,
        });
    }

    pub fn row(self: *Pool, sql: anytype, values: anytype) !?OwningRow {
        return self.rowWithState(sql, values, null);
    }

    pub fn rowWithState(self: *Pool, sql: anytype, values: anytype, state: anytype) !?OwningRow {
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

    pub fn newConn(self: *Pool) !Conn {
        return self.db.conn();
    }
};

const t = std.testing;
test "Pool: thread-safety" {
    const db = try DB.init(t.allocator, "/tmp/duckdb.zig.test", .{});
    var pool = try db.pool(.{
        .size = 2,
        .on_first_connection = &testPoolFirstConnection,
    });
    defer pool.deinit();

    const t1 = try std.Thread.spawn(.{}, testPool, .{pool});
    const t2 = try std.Thread.spawn(.{}, testPool, .{pool});
    const t3 = try std.Thread.spawn(.{}, testPool, .{pool});

    t1.join();
    t2.join();
    t3.join();

    var c1 = try pool.acquire();
    defer pool.release(c1);

    const count = try c1.exec("delete from pool_test", .{});
    try t.expectEqual(6000, count);
}

test "Pool: exec/query/row" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    var pool = try db.pool(.{ .size = 1 });
    defer pool.deinit();

    _ = try pool.exec("create table pool_test (id integer)", .{});
    try t.expectEqual(3, try pool.exec("insert into pool_test (id) values ($1), ($2), ($3)", .{ 1, 20, 300 }));

    {
        var rows = try pool.query("select * from pool_test where id != $1 order by id", .{20});
        defer rows.deinit();

        try t.expectEqual(1, (try rows.next()).?.get(i32, 0));
        try t.expectEqual(300, (try rows.next()).?.get(i32, 0));
        try t.expectEqual(null, rows.next());
    }

    {
        var row = (try pool.row("select * from pool_test where id = $1", .{300})) orelse unreachable;
        defer row.deinit();
        try t.expectEqual(300, row.get(i32, 0));
    }

    {
        const row = try pool.row("select * from pool_test where id = $1", .{400});
        try t.expectEqual(null, row);
    }

    try t.expectEqual(3, try pool.exec("delete from pool_test", .{}));
}

fn testPool(p: *Pool) void {
    for (0..2000) |i| {
        var conn = p.acquire() catch unreachable;
        _ = conn.exec("insert into pool_test (id) values ($1)", .{i}) catch unreachable;
        conn.release();
    }
}

fn testPoolFirstConnection(conn: *Conn) !void {
    _ = try conn.exec("drop table if exists pool_test", .{});
    _ = try conn.exec("create table pool_test (id uint16 not null)", .{});
}
