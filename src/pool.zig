const std = @import("std");
const lib = @import("lib.zig");

const DB = lib.DB;
const Conn = lib.Conn;

const Allocator = std.mem.Allocator;

pub const Pool = struct {
	db: DB,
	conns: []*Conn,
	shutdown: bool,
	available: usize,
	mutex: std.Thread.Mutex,
	cond: std.Thread.Condition,
	allocator: Allocator,

	pub const Config = struct {
		size: usize = 5,
		on_connection: ?*const fn(conn: *Conn) anyerror!void = null,
		on_first_connection: ?*const fn(conn: *Conn) anyerror!void = null,
	};

	pub fn init(db: DB, config: Config) !Pool {
		const size = config.size;
		const allocator = db.allocator;

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
			conns[i] = try allocator.create(Conn);
			errdefer allocator.destroy(conns[i]);

			conns[i].* = try db.conn();
			initialized += 1;

			if (i == 0) {
				if (config.on_first_connection) |f| {
					try f(conns[i]);
				}
 			}

			if (on_connection) |f| {
				try f(conns[i]);
			}
		}

		return .{
			.db = db,
			.cond = .{},
			.mutex = .{},
			.conns = conns,
			.shutdown = false,
			.available = size,
			.allocator = allocator,
		};
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
	}

	pub fn acquire(self: *Pool) !*Conn {
		const conns = self.conns;

		self.mutex.lock();
		while (true) {
			if (self.shutdown) {
				return error.PoolShuttingDown;
			}
			const available = self.available;
			if (available == 0) {
				self.cond.wait(&self.mutex);
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
};

const t = std.testing;
test "Pool" {
	const db = try DB.init(t.allocator, "/tmp/duckdb.zig.test", .{});
	var pool = try db.pool(.{
		.size = 2,
		.on_first_connection = &testPoolFirstConnection,
	});
	defer pool.deinit();

	const t1 = try std.Thread.spawn(.{}, testPool, .{&pool});
	const t2 = try std.Thread.spawn(.{}, testPool, .{&pool});
	const t3 = try std.Thread.spawn(.{}, testPool, .{&pool});

	t1.join(); t2.join(); t3.join();

	var c1 = try pool.acquire();
	defer pool.release(c1);

	const count = try c1.exec("delete from pool_test", .{});
	try t.expectEqual(6000, count);
}

fn testPool(p: *Pool) void {
	for (0..2000) |i| {
		var conn = p.acquire() catch unreachable;
		_ = conn.exec("insert into pool_test (id) values ($1)", .{i}) catch unreachable;
		p.release(conn);
	}
}

fn testPoolFirstConnection(conn: *Conn) !void {
	_ = try conn.exec("drop table if exists pool_test", .{});
	_ = try conn.exec("create table pool_test (id uint16 not null)", .{});
}
