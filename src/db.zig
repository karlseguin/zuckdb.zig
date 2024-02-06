const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const Conn = lib.Conn;
const Pool = lib.Pool;

const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

const CONFIG_SIZEOF = c.config_sizeof;
const CONFIG_ALIGNOF = c.config_alignof;
const DB_SIZEOF = c.database_sizeof;
const DB_ALIGNOF = c.database_alignof;

const log = std.log.scoped(.zuckdb);

pub const DB = struct{
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

	pub fn init(allocator: Allocator, path: anytype, db_config: Config) !DB {
		const str = try lib.stringZ(path, allocator);
		defer str.deinit(allocator);

		const config_slice = try allocator.alignedAlloc(u8, CONFIG_ALIGNOF, CONFIG_SIZEOF);
		defer allocator.free(config_slice);
		const config: *c.duckdb_config = @ptrCast(config_slice.ptr);

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

		const db_slice = try allocator.alignedAlloc(u8, DB_ALIGNOF, DB_SIZEOF);
		errdefer allocator.free(db_slice);
		const db: *c.duckdb_database = @ptrCast(db_slice.ptr);

		var out_err: [*c]u8 = undefined;
		if (c.duckdb_open_ext(str.z, db, config.*, &out_err) == DuckDBError) {
			log.err("zudkdb failed to open database: {s}", .{std.mem.span(out_err)});
			c.duckdb_free(out_err);
			return error.OpenDB;
		}

		return .{
			.db = db,
			.allocator = allocator
		};
	}

	pub fn deinit(self: *const DB) void {
		const db = self.db;
		c.duckdb_close(db);

		const ptr: [*]align(DB_ALIGNOF) u8 = @ptrCast(db);
		const slice = ptr[0..DB_SIZEOF];
		self.allocator.free(slice);
	}

	pub fn conn(self: DB) !Conn {
		return Conn.open(self);
	}

	pub fn pool(self: DB, config: Pool.Config) !Pool {
		return Pool.init(self, config);
	}
};
