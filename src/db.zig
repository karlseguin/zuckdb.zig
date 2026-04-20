const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const Conn = lib.Conn;
const Pool = lib.Pool;

const Io = std.Io;
const Allocator = std.mem.Allocator;

const DuckDBError = c.DuckDBError;
const CONFIG_SIZEOF = c.config_sizeof;
const CONFIG_ALIGNOF = c.config_alignof;
const DB_SIZEOF = c.database_sizeof;
const DB_ALIGNOF = c.database_alignof;

pub const DB = struct {
    io: Io,
    allocator: Allocator,
    db: *c.duckdb_database,

    pub const Config = struct {
        enable_external_access: bool = true,
        access_mode: AccessMode = .automatic,

        const AccessMode = enum {
            automatic,
            read_only,
            read_write,
        };
    };

    pub fn init(io: Io, allocator: Allocator, path: anytype, db_config: Config) !DB {
        return initWithErr(io, allocator, path, db_config, null);
    }

    pub fn initWithErr(io: Io, allocator: Allocator, path: anytype, db_config: Config, err: ?*?[]u8) !DB {
        const str = try lib.stringZ(path, allocator);
        defer str.deinit(allocator);

        const config = try allocator.create(c.duckdb_config);
        defer allocator.destroy(config);

        if (c.duckdb_create_config(config) == DuckDBError) {
            return error.ConfigCreate;
        }

        if (db_config.access_mode != .automatic) {
            if (c.duckdb_set_config(config.*, "access_mode", @tagName(db_config.access_mode)) == DuckDBError) {
                return error.ConfigAccessMode;
            }
        }

        if (db_config.enable_external_access == false) {
            if (c.duckdb_set_config(config.*, "enable_external_access", "false") == DuckDBError) {
                return error.ConfigExternalAccess;
            }
        }

        const db = try allocator.create(c.duckdb_database);
        errdefer allocator.destroy(db);

        var out_err: [*c]u8 = undefined;
        if (c.duckdb_open_ext(str.z, db, config.*, &out_err) == DuckDBError) {
            defer c.duckdb_free(out_err);
            if (err) |e| {
                e.* = try allocator.dupe(u8, std.mem.span(out_err));
            }
            return error.OpenDB;
        }

        return .{ .io = io, .db = db, .allocator = allocator };
    }

    pub fn deinit(self: *const DB) void {
        const db = self.db;
        c.duckdb_close(db);
        self.allocator.destroy(db);
    }

    pub fn conn(self: *const DB) !Conn {
        return Conn.open(self);
    }

    pub fn pool(self: DB, config: Pool.Config) !*Pool {
        return Pool.init(self, config);
    }
};

const t = std.testing;
test "DB: error" {
    var err: ?[]u8 = null;
    try t.expectError(error.OpenDB, DB.initWithErr(t.io, t.allocator, "/tmp/does/not/exist/zuckdb", .{}, &err));
    try t.expectEqual(true, std.mem.find(u8, err.?, "IO Error: Cannot open file") != null);
    try t.expectEqual(true, std.mem.find(u8, err.?, "No such file or directory") != null);
    t.allocator.free(err.?);
}
