const lib = @import("lib.zig");

const c = lib.c;
const Rows = lib.Rows;
const ParameterType = lib.ParameterType;

// DuckDB exposes data as "vectors", which is essentially a pointer to memory
// that holds data based on the column type (a vector is data for a column, not
// a row). Our ColumnData is a typed wrapper to the (a) data and (b) the validity
// mask (null) of a vector.
pub const ColumnData = struct {
	data: Data,
	validity: [*c]u64,

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
		decimal: ColumnData.Decimal,
		uuid: [*c]i128,
		@"enum": ColumnData.Enum,
	};

	pub const Container = union(enum) {
		list: ColumnData.List,
	};

	pub const Decimal = struct {
		width: u8,
		scale: u8,
		internal: Internal,

		pub const Internal = union(enum) {
			i16: [*c]i16,
			i32: [*c]i32,
			i64: [*c]i64,
			i128: [*c]i128,
		};
	};

	pub const List = struct {
		child: Scalar,
		validity: [*c]u64,
		entries: [*]c.duckdb_list_entry,
	};

	pub const Enum = struct {
		rows: *Rows,
		internal: Internal,
		logical_type: c.duckdb_logical_type,

		pub const Internal = union(enum) {
			u8: [*c]u8,
			u16: [*c]u16,
			u32: [*c]u32,
			u64: [*c]u64,
		};
	};
};
