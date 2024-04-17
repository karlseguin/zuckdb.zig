const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const Rows = lib.Rows;
const DataType = lib.DataType;
const Allocator = std.mem.Allocator;

// DuckDB exposes data as "vectors", which is essentially a pointer to memory
// that holds data based on the column type (a vector is data for a column, not
// a row). Our ColumnData is a typed wrapper to the (a) data and (b) the validity
// mask (null) of a vector.
pub const Vector = struct {
	type: Type,
	data: Data,
	validity: [*c]u64,

	pub const Type = union(enum) {
		list: Vector.Type.Scalar,
		scalar: Vector.Type.Scalar,

		// We expect allocator to be an Arena. Currently, we only need allocator for
		// our Enum cache.
		pub fn init(allocator: Allocator, logical_type: c.duckdb_logical_type) !Type {
			const type_id = c.duckdb_get_type_id(logical_type);
			switch (type_id) {
				c.DUCKDB_TYPE_LIST => {
					const child_type = c.duckdb_list_type_child_type(logical_type);
					return .{.list = try initScalar(allocator, child_type) };
				},
				else => return .{.scalar = try initScalar(allocator, logical_type)},
			}
		}

		pub fn deinit(self: *Type) void {
			switch (self.*) {
				.list => |*scalar| scalar.deinit(),
				.scalar => |*scalar| scalar.deinit(),
			}
		}

		fn initScalar(allocator: Allocator, logical_type: c.duckdb_logical_type) !Vector.Type.Scalar {
			const type_id = c.duckdb_get_type_id(logical_type);
			switch (type_id) {
				c.DUCKDB_TYPE_ENUM => {
					const internal_type: Vector.Type.Enum.Type = switch (c.duckdb_enum_internal_type(logical_type)) {
						c.DUCKDB_TYPE_UTINYINT => .u8,
						c.DUCKDB_TYPE_USMALLINT => .u16,
						c.DUCKDB_TYPE_UINTEGER => .u32,
						c.DUCKDB_TYPE_UBIGINT => .u64,
						else => unreachable,
					};
					return .{.@"enum" = .{
						.type = internal_type,
						.logical_type = logical_type,
						.cache = std.AutoHashMap(u64, []const u8).init(allocator),
					}};
				},
				c.DUCKDB_TYPE_DECIMAL => {
					const scale = c.duckdb_decimal_scale(logical_type);
					const width = c.duckdb_decimal_width(logical_type);
					const internal_type: Vector.Type.Decimal.Type = switch (c.duckdb_decimal_internal_type(logical_type)) {
						c.DUCKDB_TYPE_SMALLINT => .i16,
						c.DUCKDB_TYPE_INTEGER => .i32,
						c.DUCKDB_TYPE_BIGINT => .i64,
						c.DUCKDB_TYPE_HUGEINT => .i128,
						else => unreachable,
					};
					return .{.decimal = .{.width = width, .scale = scale, .type = internal_type}};
				},
				c.DUCKDB_TYPE_BLOB,
				c.DUCKDB_TYPE_VARCHAR,
				c.DUCKDB_TYPE_BIT,
				c.DUCKDB_TYPE_TINYINT,
				c.DUCKDB_TYPE_SMALLINT,
				c.DUCKDB_TYPE_INTEGER,
				c.DUCKDB_TYPE_BIGINT,
				c.DUCKDB_TYPE_HUGEINT, c.DUCKDB_TYPE_UUID,
				c.DUCKDB_TYPE_UHUGEINT,
				c.DUCKDB_TYPE_UTINYINT,
				c.DUCKDB_TYPE_USMALLINT,
				c.DUCKDB_TYPE_UINTEGER,
				c.DUCKDB_TYPE_UBIGINT,
				c.DUCKDB_TYPE_BOOLEAN,
				c.DUCKDB_TYPE_FLOAT,
				c.DUCKDB_TYPE_DOUBLE,
				c.DUCKDB_TYPE_DATE,
				c.DUCKDB_TYPE_TIME,
				c.DUCKDB_TYPE_TIMESTAMP,
				c.DUCKDB_TYPE_TIMESTAMP_TZ,
				c.DUCKDB_TYPE_INTERVAL => return .{ .simple = type_id },
				else => return error.UnknownDataType,
			}
		}

		pub const Scalar = union(enum) {
			simple: c.duckdb_type,
			@"enum": Vector.Type.Enum,
			decimal: Vector.Type.Decimal,

			fn deinit(self: *@This()) void {
				switch (self.*) {
					.simple, .decimal => {},
					.@"enum" => |*e| c.duckdb_destroy_logical_type(&e.logical_type),
				}
			}
		};

		const Decimal = struct {
			width: u8,
			scale: u8,
			type: Vector.Type.Decimal.Type,

			const Type = enum {
				i16, i32, i64, i128
			};
		};

		const Enum = struct {
			type: Vector.Type.Enum.Type,
			logical_type: c.duckdb_logical_type,
			cache: std.AutoHashMap(u64, []const u8),

			const Type = enum {
				u8, u16, u32, u64
			};
		};
	};

	pub const Data = union(enum) {
		scalar: Scalar,
		container: Container,
	};

	pub const Scalar = union(enum) {
		i8: [*c]i8,
		i16: [*c]i16,
		i32: [*c]i32,
		i64: [*c]i64,
		i128: [*c]i128,
		u128: [*c]u128,
		u8: [*c]u8,
		u16: [*c]u16,
		u32: [*c]u32,
		u64: [*c]u64,
		bool: [*c]bool,
		f32: [*c]f32,
		f64: [*c]f64,
		blob: [*]c.duckdb_string_t,
		varchar: [*]c.duckdb_string_t,
		date: [*]c.duckdb_date,
		time: [*]c.duckdb_time,
		timestamp: [*]c.duckdb_timestamp,
		interval: [*]c.duckdb_interval,
		decimal: Vector.Decimal,
		uuid: [*c]i128,
		@"enum": Vector.Enum,
	};

	pub const Container = union(enum) {
		list: Vector.List,
	};

	pub const Decimal = struct {
		width: u8,
		scale: u8,
		internal: Internal,

		pub const Internal = union(Vector.Type.Decimal.Type) {
			i16: [*c]i16,
			i32: [*c]i32,
			i64: [*c]i64,
			i128: [*c]i128,
		};
	};

	pub const List = struct {
		child: Scalar,
		validity: [*c]u64,
		type: c.duckdb_type,
		entries: [*]c.duckdb_list_entry,
	};

	pub const Enum = struct {
		internal: Internal,
		logical_type: c.duckdb_logical_type,
		cache: *std.AutoHashMap(u64, []const u8),

		pub const Internal = union(Vector.Type.Enum.Type) {
			u8: [*c]u8,
			u16: [*c]u16,
			u32: [*c]u32,
			u64: [*c]u64,
		};
	};
};
