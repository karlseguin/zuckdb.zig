const std = @import("std");

const DB = @import("db.zig").DB;
const Conn = @import("conn.zig").Conn;
const Result = @import("db.zig").Result;

const Allocator = std.mem.Allocator;

pub const Pool = struct {
	db: DB,
	version: u32,
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

	pub fn init(db: DB, config: Config) Result(Pool) {
		const size = config.size;
		const allocator = db.allocator;
		const conns = allocator.alloc(Conn, size) catch |err| {
			return Result(Pool).staticErr(err, "OOM");
		};

		// if something fails while we're setting up the pool, we need to close
		// any connection that we've initialized
		var init_count: usize = 0;
		const on_connection = config.on_connection;
		for (0..size) |i| {
			var conn = db.conn() catch |err| {
				poolInitFailCleanup(allocator, conns, init_count);
				return Result(Pool).staticErr(err, "open connection failure");
			};
			init_count += 1;
			if (i == 0) {
				if (config.on_first_connection) |f| {
					f(conn) catch |err| {
						poolInitFailCleanup(allocator, conns, init_count);
						return Result(Pool).staticErr(err, "on_first_connection failure");
					};
				}
			}
			if (on_connection) |f| {
				f(conn) catch |err| {
					poolInitFailCleanup(allocator, conns, init_count);
					return Result(Pool).staticErr(err, "on_connection failure");
				};
			}
			conns[i] = conn;
		}

		return .{.ok = .{
			.db = db,
			.conns = conns,
			.version = 0,
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
			var conn = conns[index];
			self.available = index;
			const pv = self.version;
			self.mutex.unlock();

			if (conn.version != pv) {
				conn.clearStatementCache();
				conn.version = pv;
			}
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

	pub fn incrementVersion(self: *Pool) void {
		self.mutex.lock();
		self.version += 1;
		self.mutex.unlock();
	}
};

fn poolInitFailCleanup(allocator: Allocator, conns: []Conn, count: usize) void {
	for (0..count) |i| {
		conns[i].deinit();
	}
	allocator.free(conns);
}

const t = std.testing;
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
	try t.expectEqual(@as(usize, 6000), result.ok.changed());
}

test "Pool: versioning" {
	const db = DB.init(t.allocator, ":memory:").ok;
	var pool = db.pool(.{.size = 2,}).ok;
	defer pool.deinit();

	try t.expectEqual(@as(u32, 0), pool.version);

	pool.incrementVersion();
	try t.expectEqual(@as(u32, 1), pool.version);

	pool.incrementVersion();
	try t.expectEqual(@as(u32, 2), pool.version);
}

test "Pool: conn version management" {
	const db = DB.init(t.allocator, ":memory:").ok;
	var pool = db.pool(.{.size = 2,}).ok;
	defer pool.deinit();

	{
		const c1 = pool.acquire();
		const c2 = pool.acquire();
		try t.expectEqual(@as(u32, 0), c1.version);
		try t.expectEqual(@as(u32, 0), c1.version);
		pool.release(c1);
		pool.release(c2);
	}

	{
		pool.incrementVersion();
		const c1 = pool.acquire();
		const c2 = pool.acquire();
		try t.expectEqual(@as(u32, 1), c1.version);
		try t.expectEqual(@as(u32, 1), c2.version);
		pool.release(c1);
		pool.release(c2);
	}

	{
		pool.incrementVersion();
		const c1 = pool.acquire();
		const c2 = pool.acquire();
		try t.expectEqual(@as(u32, 2), c1.version);
		try t.expectEqual(@as(u32, 2), c2.version);
		pool.release(c1);
		pool.release(c2);
	}

	{
		// c1 acquired before increment
		const c1 = pool.acquire();
		pool.incrementVersion();
		const c2 = pool.acquire();
		try t.expectEqual(@as(u32, 2), c1.version);
		try t.expectEqual(@as(u32, 3), c2.version);
		pool.release(c1);
		pool.release(c2);
	}

	{
		const c1 = pool.acquire();
		const c2 = pool.acquire();
		try t.expectEqual(@as(u32, 3), c1.version);
		try t.expectEqual(@as(u32, 3), c2.version);
		pool.release(c1);
		pool.release(c2);
	}
}

fn testPool(p: *Pool) void {
	for (0..2000) |i| {
		const conn = p.acquire();
		conn.queryZ("insert into pool_test (id) values ($1)", .{i}).ok.deinit();
		p.release(conn);
	}
}

fn testPoolFirstConnection(conn: Conn) !void {
	conn.queryZ("drop table if exists pool_test", .{}).ok.deinit();
	conn.queryZ("create table pool_test (id uint16 not null)", .{}).ok.deinit();
}
