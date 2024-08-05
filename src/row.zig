const std = @import("std");
const typed = @import("typed");
const lib = @import("lib.zig");

const c = lib.c;
const DB = lib.DB;
const Rows = lib.Rows;
const Vector = lib.Vector;

const UUID = lib.UUID;
const Time = lib.Time;
const Date = lib.Date;
const Interval = lib.Interval;
const DataType = lib.DataType;

const Allocator = std.mem.Allocator;

pub const Row = struct {
    index: usize,
    vectors: []Vector,

    pub fn get(self: Row, comptime T: type, col: usize) T {
        const index = self.index;
        const vector = self.vectors[col];

        const TT = switch (@typeInfo(T)) {
            .Optional => |opt| blk: {
                if (_isNull(vector.validity.?, index)) return null;
                break :blk opt.child;
            },
            else => blk: {
                lib.assert(_isNull(vector.validity.?, index) == false);
                break :blk T;
            },
        };

        switch (vector.data) {
            .scalar => |scalar| return getScalar(TT, scalar, index, col),
            else => unreachable,
        }
    }

    pub fn list(self: Row, comptime T: type, col: usize) ?List(T) {
        const index = self.index;
        const vector = self.vectors[col];
        if (_isNull(vector.validity.?, index)) return null;

        const vc = vector.data.list;
        const entry = vc.entries[index];
        return List(T).init(col, vc.child, vc.validity, entry.offset, entry.length);
    }

    pub fn lazyList(self: Row, col: usize) ?LazyList {
        const index = self.index;
        const vector = self.vectors[col];
        if (_isNull(vector.validity.?, index)) return null;

        const vc = vector.data.list;
        const entry = vc.entries[index];
        return LazyList.init(col, vc.child, vc.validity, entry.offset, entry.length);
    }

    pub fn listItemType(self: Row, col: usize) DataType {
        // (⌐■_■)
        return lib.DataType.fromDuckDBType(self.vectors[col].data.list.type);
    }

    pub fn isNull(self: Row, col: usize) bool {
        return _isNull(self.vectors[col].validity.?, self.index);
    }
};

// Returned by conn.row, wraps a row and rows, the latter so that
// it can be deinit'd
pub const OwningRow = struct {
    row: Row,
    rows: Rows,

    pub fn get(self: OwningRow, comptime T: type, col: usize) T {
        return self.row.get(T, col);
    }

    pub fn list(self: OwningRow, comptime T: type, col: usize) ?List(T) {
        return self.row.list(T, col);
    }

    pub fn deinit(self: OwningRow) void {
        self.rows.deinit();
    }
};

pub const Enum = struct {
    idx: usize,
    _col: usize,
    _logical_type: c.duckdb_logical_type,
    _cache: *std.AutoHashMap(u64, []const u8),

    pub fn rowCache(self: Enum) ![]const u8 {
        const gop = try self._cache.getOrPut(self.idx);
        if (gop.found_existing) {
            return gop.value_ptr.*;
        }

        const string_value = c.duckdb_enum_dictionary_value(self._logical_type, self.idx);

        // Using self._cache.allocator is pretty bad. I _know_ this is the rows
        // ArenaAllocator, so it's right, but it is  ugly and error prone should
        // anything ever change
        const value = try self._cache.allocator.dupe(u8, std.mem.span(string_value));
        c.duckdb_free(string_value);
        gop.value_ptr.* = value;
        return value;
    }

    pub fn raw(self: Enum) [*c]const u8 {
        return c.duckdb_enum_dictionary_value(self._logical_type, self.idx);
    }
};

pub fn List(comptime T: type) type {
    return struct {
        len: usize,
        col: usize,
        _validity: [*c]u64,
        _offset: usize,
        _scalar: Vector.Scalar,

        const Self = @This();

        fn init(col: usize, scalar: Vector.Scalar, validity: [*c]u64, offset: usize, length: usize) Self {
            return .{
                .col = col,
                .len = length,
                ._offset = offset,
                ._scalar = scalar,
                ._validity = validity,
            };
        }

        pub fn get(self: *const Self, i: usize) T {
            const index = i + self._offset;

            const TT = switch (@typeInfo(T)) {
                .Optional => |opt| blk: {
                    if (_isNull(self._validity, index)) return null;
                    break :blk opt.child;
                },
                else => blk: {
                    lib.assert(_isNull(self._validity, index) == false);
                    break :blk T;
                },
            };

            return getScalar(TT, self._scalar, index, self.col);
        }

        pub fn alloc(self: *const Self, allocator: Allocator) ![]T {
            const arr = try allocator.alloc(T, self.len);
            self.fill(arr);
            return arr;
        }

        pub fn fill(self: *const Self, into: []T) void {
            const limit = @min(into.len, self.len);
            for (0..limit) |i| {
                into[i] = self.get(i);
            }
        }
    };
}

// A list who's type isn't known at compile-time
pub const LazyList = struct {
    len: usize,
    col: usize,
    _validity: [*c]u64,
    _offset: usize,
    _scalar: Vector.Scalar,

    fn init(col: usize, scalar: Vector.Scalar, validity: [*c]u64, offset: usize, length: usize) LazyList {
        return .{
            .col = col,
            .len = length,
            ._offset = offset,
            ._scalar = scalar,
            ._validity = validity,
        };
    }

    pub fn get(self: *const LazyList, comptime T: type, i: usize) T {
        const index = i + self._offset;

        const TT = switch (@typeInfo(T)) {
            .Optional => |opt| blk: {
                if (_isNull(self._validity, index)) return null;
                break :blk opt.child;
            },
            else => blk: {
                lib.assert(_isNull(self._validity, index) == false);
                break :blk T;
            },
        };
        return getScalar(TT, self._scalar, index, self.col);
    }

    pub fn isNull(self: *const LazyList, i: usize) bool {
        return _isNull(self._validity, i);
    }
};

inline fn _isNull(validity: [*c]u64, index: usize) bool {
    const entry_index = index / 64;
    const entry_mask = index % 64;
    return validity[entry_index] & std.math.shl(u64, 1, entry_mask) == 0;
}

fn getScalar(comptime T: type, scalar: Vector.Scalar, index: usize, col: usize) T {
    switch (T) {
        []u8, []const u8 => return getBlob(scalar, index),
        i8 => return scalar.i8[index],
        i16 => return scalar.i16[index],
        i32 => return scalar.i32[index],
        i64 => switch (scalar) {
            .i64 => |vc| return vc[index],
            .timestamp => |vc| return vc[index],
            else => unreachable,
        },
        i128 => return scalar.i128[index],
        u128 => return scalar.u128[index],
        u8 => return scalar.u8[index],
        u16 => return scalar.u16[index],
        u32 => return scalar.u32[index],
        u64 => return scalar.u64[index],
        f32 => return scalar.f32[index],
        f64 => switch (scalar) {
            .f64 => |vc| return vc[index],
            .decimal => |vc| {
                const value = switch (vc.internal) {
                    inline else => |internal| lib.hugeInt(internal[index]),
                };
                return c.duckdb_decimal_to_double(c.duckdb_decimal{
                    .width = vc.width,
                    .scale = vc.scale,
                    .value = value,
                });
            },
            else => unreachable,
        },
        bool => return scalar.bool[index],
        Date => return c.duckdb_from_date(scalar.date[index]),
        Time => return c.duckdb_from_time(scalar.time[index]),
        Interval => return scalar.interval[index],
        Enum => {
            const e = scalar.@"enum";
            return Enum{
                .idx = switch (e.internal) {
                    inline else => |internal| internal[index],
                },
                ._col = col,
                ._cache = e.cache,
                ._logical_type = e.logical_type,
            };
        },
        UUID => return getUUID(scalar, index),
        else => @compileError("Cannot get value of type " ++ @typeName(T)),
    }
}

fn getBlob(scalar: Vector.Scalar, index: usize) []u8 {
    switch (scalar) {
        .blob, .varchar => |vc| {
            // This sucks. This is an untagged union. But both versions (inlined and pointer)
            // have the same leading 8 bytes, including the length which is the first 4 bytes.
            // There is a c.duckdb_string_is_inlined that we could use instead of hard-coding
            // the 12, but that requires dereferencing value, which I'd like to avoid.
            // For one reason, when inlined, it's easy to accidently pass the address of the local copy
            const value = &vc[index];
            const len = value.value.inlined.length;
            if (len <= 12) {
                return value.value.inlined.inlined[0..len];
            }
            const pointer = value.value.pointer;
            return pointer.ptr[0..len];
        },
        else => unreachable,
    }
}

// largely taken from duckdb's uuid type
fn getUUID(scalar: Vector.Scalar, index: usize) UUID {
    const hex = "0123456789abcdef";
    const n = scalar.uuid[index];

    const h = lib.hugeInt(n);

    const u = h.upper ^ (@as(i64, 1) << 63);
    const l = h.lower;

    var buf: [36]u8 = undefined;

    const b1: u8 = @intCast((u >> 56) & 0xFF);
    buf[0] = hex[b1 >> 4];
    buf[1] = hex[b1 & 0x0f];

    const b2: u8 = @intCast((u >> 48) & 0xFF);
    buf[2] = hex[b2 >> 4];
    buf[3] = hex[b2 & 0x0f];

    const b3: u8 = @intCast((u >> 40) & 0xFF);
    buf[4] = hex[b3 >> 4];
    buf[5] = hex[b3 & 0x0f];

    const b4: u8 = @intCast((u >> 32) & 0xFF);
    buf[6] = hex[b4 >> 4];
    buf[7] = hex[b4 & 0x0f];

    buf[8] = '-';

    const b5: u8 = @intCast((u >> 24) & 0xFF);
    buf[9] = hex[b5 >> 4];
    buf[10] = hex[b5 & 0x0f];

    const b6: u8 = @intCast((u >> 16) & 0xFF);
    buf[11] = hex[b6 >> 4];
    buf[12] = hex[b6 & 0x0f];

    buf[13] = '-';

    const b7: u8 = @intCast((u >> 8) & 0xFF);
    buf[14] = hex[b7 >> 4];
    buf[15] = hex[b7 & 0x0f];

    const b8: u8 = @intCast(u & 0xFF);
    buf[16] = hex[b8 >> 4];
    buf[17] = hex[b8 & 0x0f];

    buf[18] = '-';

    const b9: u8 = @intCast((l >> 56) & 0xFF);
    buf[19] = hex[b9 >> 4];
    buf[20] = hex[b9 & 0x0f];

    const b10: u8 = @intCast((l >> 48) & 0xFF);
    buf[21] = hex[b10 >> 4];
    buf[22] = hex[b10 & 0x0f];

    buf[23] = '-';

    const b11: u8 = @intCast((l >> 40) & 0xFF);
    buf[24] = hex[b11 >> 4];
    buf[25] = hex[b11 & 0x0f];

    const b12: u8 = @intCast((l >> 32) & 0xFF);
    buf[26] = hex[b12 >> 4];
    buf[27] = hex[b12 & 0x0f];

    const b13: u8 = @intCast((l >> 24) & 0xFF);
    buf[28] = hex[b13 >> 4];
    buf[29] = hex[b13 & 0x0f];

    const b14: u8 = @intCast((l >> 16) & 0xFF);
    buf[30] = hex[b14 >> 4];
    buf[31] = hex[b14 & 0x0f];

    const b15: u8 = @intCast((l >> 8) & 0xFF);
    buf[32] = hex[b15 >> 4];
    buf[33] = hex[b15 & 0x0f];

    const b16: u8 = @intCast(l & 0xFF);
    buf[34] = hex[b16 >> 4];
    buf[35] = hex[b16 & 0x0f];

    return buf;
}

const t = std.testing;
// Test this specifically since there's special handling based on the length
// of the column (inlined vs pointer)
test "read varchar" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    {
        var rows = try conn.query(
            \\
            \\ select '1' union all
            \\ select '12345' union all
            \\ select '123456789A' union all
            \\ select '123456789AB' union all
            \\ select '123456789ABC' union all
            \\ select '123456789ABCD' union all
            \\ select '123456789ABCDE' union all
            \\ select '123456789ABCDEF' union all
            \\ select null
        , .{});
        defer rows.deinit();

        {
            const row = (try rows.next()).?;
            try t.expectEqualStrings("1", row.get([]u8, 0));
            try t.expectEqualStrings("1", row.get(?[]u8, 0).?);
            try t.expectEqualStrings("1", row.get([]const u8, 0));
            try t.expectEqualStrings("1", row.get(?[]const u8, 0).?);
            try t.expectEqual(false, row.isNull(0));
        }

        try t.expectEqualStrings("12345", (try rows.next()).?.get([]const u8, 0));
        try t.expectEqualStrings("123456789A", (try rows.next()).?.get([]const u8, 0));
        try t.expectEqualStrings("123456789AB", (try rows.next()).?.get([]const u8, 0));
        try t.expectEqualStrings("123456789ABC", (try rows.next()).?.get([]const u8, 0));
        try t.expectEqualStrings("123456789ABCD", (try rows.next()).?.get([]const u8, 0));
        try t.expectEqualStrings("123456789ABCDE", (try rows.next()).?.get([]const u8, 0));

        {
            const row = (try rows.next()).?;
            try t.expectEqualStrings("123456789ABCDEF", row.get([]u8, 0));
            try t.expectEqualStrings("123456789ABCDEF", row.get(?[]u8, 0).?);
            try t.expectEqualStrings("123456789ABCDEF", row.get([]const u8, 0));
            try t.expectEqualStrings("123456789ABCDEF", row.get(?[]const u8, 0).?);
        }

        {
            const row = (try rows.next()).?;
            try t.expectEqual(null, row.get(?[]u8, 0));
            try t.expectEqual(null, row.get(?[]const u8, 0));
            try t.expectEqual(true, row.isNull(0));
        }

        try t.expectEqual(null, try rows.next());
    }
}

// Test this specifically since there's special handling based on the length
// of the column (inlined vs pointer)
test "read blob" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    {
        var rows = try conn.query(
            \\
            \\ select '\xAA'::blob union all
            \\ select '\xAA\xAA\xAA\xAA\xAB'::blob union all
            \\ select '\xAA\xAA\xAA\xAA\xAB\xAA\xAA\xAA\xAA\xAB\xAA\xAA\xAA\xAA\xAB'::blob union all
            \\ select null
        , .{});
        defer rows.deinit();

        try t.expectEqualSlices(u8, @as([]const u8, &.{170}), (try rows.next()).?.get([]const u8, 0));
        try t.expectEqualSlices(u8, @as([]const u8, &.{ 170, 170, 170, 170, 171 }), (try rows.next()).?.get([]const u8, 0));
        try t.expectEqualSlices(u8, @as([]const u8, &.{ 170, 170, 170, 170, 171, 170, 170, 170, 170, 171, 170, 170, 170, 170, 171 }), (try rows.next()).?.get([]const u8, 0));
        try t.expectEqual(null, (try rows.next()).?.get(?[]const u8, 0));
        try t.expectEqual(null, try rows.next());
    }
}

test "read ints" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    {
        var rows = try conn.query(
            \\
            \\ select 0::tinyint, 0::smallint, 0::integer, 0::bigint, 0::hugeint, 0::utinyint, 0::usmallint, 0::uinteger, 0::ubigint
            \\ union all
            \\ select 127::tinyint, 32767::smallint, 2147483647::integer, 9223372036854775807::bigint, 170141183460469231731687303715884105727::hugeint, 255::utinyint, 65535::usmallint, 4294967295::uinteger, 18446744073709551615::ubigint
            \\ union all
            \\ select -127::tinyint, -32767::smallint, -2147483647::integer, -9223372036854775807::bigint, -170141183460469231731687303715884105727::hugeint, 0::utinyint, 0::usmallint, 0::uinteger, 0::ubigint
            \\ union all
            \\ select null, null, null, null, null, null, null, null, null
        , .{});
        defer rows.deinit();

        var row = (try rows.next()) orelse unreachable;
        try t.expectEqual(0, row.get(i8, 0));
        try t.expectEqual(0, row.get(i16, 1));
        try t.expectEqual(0, row.get(i32, 2));
        try t.expectEqual(0, row.get(i64, 3));
        try t.expectEqual(0, row.get(i128, 4));
        try t.expectEqual(0, row.get(u8, 5));
        try t.expectEqual(0, row.get(u16, 6));
        try t.expectEqual(0, row.get(u32, 7));
        try t.expectEqual(0, row.get(u64, 8));

        row = (try rows.next()) orelse unreachable;
        try t.expectEqual(127, row.get(i8, 0));
        try t.expectEqual(32767, row.get(i16, 1));
        try t.expectEqual(2147483647, row.get(i32, 2));
        try t.expectEqual(9223372036854775807, row.get(i64, 3));
        try t.expectEqual(170141183460469231731687303715884105727, row.get(i128, 4));
        try t.expectEqual(255, row.get(u8, 5));
        try t.expectEqual(65535, row.get(u16, 6));
        try t.expectEqual(4294967295, row.get(u32, 7));
        try t.expectEqual(18446744073709551615, row.get(u64, 8));

        row = (try rows.next()) orelse unreachable;
        try t.expectEqual(-127, row.get(i8, 0));
        try t.expectEqual(-32767, row.get(i16, 1));
        try t.expectEqual(-2147483647, row.get(i32, 2));
        try t.expectEqual(-9223372036854775807, row.get(i64, 3));
        try t.expectEqual(-170141183460469231731687303715884105727, row.get(i128, 4));

        row = (try rows.next()) orelse unreachable;
        try t.expectEqual(null, row.get(?i8, 0));
        try t.expectEqual(null, row.get(?i16, 1));
        try t.expectEqual(null, row.get(?i32, 2));
        try t.expectEqual(null, row.get(?i64, 3));
        try t.expectEqual(null, row.get(?i128, 4));
        try t.expectEqual(null, row.get(?u8, 5));
        try t.expectEqual(null, row.get(?u16, 6));
        try t.expectEqual(null, row.get(?u32, 7));
        try t.expectEqual(null, row.get(?u64, 8));

        try t.expectEqual(null, try rows.next());
    }
}

test "read bool" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    {
        var rows = try conn.query("select 0::bool, 1::bool, null::bool", .{});
        defer rows.deinit();

        var row = (try rows.next()) orelse unreachable;
        try t.expectEqual(false, row.get(bool, 0));
        try t.expectEqual(true, row.get(bool, 1));
        try t.expectEqual(null, row.get(?bool, 2));

        try t.expectEqual(null, try rows.next());
    }
}

test "read float" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    {
        var rows = try conn.query("select 32.329::real, -0.29291::double, null::real, null::double", .{});
        defer rows.deinit();

        var row = (try rows.next()) orelse unreachable;
        try t.expectEqual(32.329, row.get(f32, 0));
        try t.expectEqual(-0.29291, row.get(f64, 1));
        try t.expectEqual(null, row.get(?f32, 2));
        try t.expectEqual(null, row.get(?f64, 3));

        try t.expectEqual(null, try rows.next());
    }
}

test "read decimal" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    {
        // decimals (representation is different based on the width)
        var rows = try conn.query("select 1.23::decimal(3,2), 1.24::decimal(8, 4), 1.25::decimal(12, 5), 1.26::decimal(18, 3), 1.27::decimal(35, 4)", .{});
        defer rows.deinit();

        const row = (try rows.next()).?;
        try t.expectEqual(1.23, row.get(f64, 0));
        try t.expectEqual(1.24, row.get(f64, 1));
        try t.expectEqual(1.25, row.get(f64, 2));
        try t.expectEqual(1.26, row.get(f64, 3));
        try t.expectEqual(1.27, row.get(f64, 4));
    }
}

test "read date & time" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    {
        var rows = try conn.query("select date '1992-09-20', time '14:21:13.332', timestamp '1993-10-21 11:30:02'", .{});
        defer rows.deinit();

        var row = (try rows.next()) orelse unreachable;
        try t.expectEqual(Date{ .year = 1992, .month = 9, .day = 20 }, row.get(Date, 0));
        try t.expectEqual(Time{ .hour = 14, .min = 21, .sec = 13, .micros = 332000 }, row.get(Time, 1));
        try t.expectEqual(751203002000000, row.get(i64, 2));
    }
}

test "read list" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create type my_type as enum ('type_a', 'type_b')", .{});

    {
        var rows = try conn.query("select [1, 32, 99, null, -4]::int[]", .{});
        defer rows.deinit();

        var row = (try rows.next()) orelse unreachable;

        const list = row.list(?i32, 0).?;
        try t.expectEqual(5, list.len);
        try t.expectEqual(1, list.get(0).?);
        try t.expectEqual(32, list.get(1).?);
        try t.expectEqual(99, list.get(2).?);
        try t.expectEqual(null, list.get(3));
        try t.expectEqual(-4, list.get(4).?);

        const arr = try list.alloc(t.allocator);
        defer t.allocator.free(arr);
        try t.expectEqualSlices(?i32, &.{ 1, 32, 99, null, -4 }, arr);
    }

    {
        var rows = try conn.query("select ['tag1', null, 'tag2']::varchar[]", .{});
        defer rows.deinit();

        var row = (try rows.next()) orelse unreachable;
        const list = row.list(?[]u8, 0).?;
        try t.expectEqual(3, list.len);
        try t.expectEqualStrings("tag1", list.get(0).?);
        try t.expectEqual(null, list.get(1));
        try t.expectEqualStrings("tag2", list.get(2).?);
    }

    {
        var rows = try conn.query("select ['tag1', null, 'tag2']::varchar[]", .{});
        defer rows.deinit();

        var row = (try rows.next()) orelse unreachable;
        const list = row.lazyList(0).?;
        try t.expectEqual(3, list.len);
        try t.expectEqualStrings("tag1", list.get([]const u8, 0));
        try t.expectEqual(null, list.get(?[]const u8, 1));
        try t.expectEqualStrings("tag2", list.get(?[]const u8, 2).?);
    }

    {
        var rows = try conn.query("select ['tag1', null, 'tag2']::varchar[]", .{});
        defer rows.deinit();

        var row = (try rows.next()) orelse unreachable;
        const list = row.list(?[]const u8, 0).?;
        try t.expectEqual(3, list.len);
        try t.expectEqualStrings("tag1", list.get(0).?);
        try t.expectEqual(null, list.get(1));
        try t.expectEqualStrings("tag2", list.get(2).?);
    }

    {
        var rows = try conn.query("select ['type_a', null, 'type_b', 'type_a']::my_type[]", .{});
        defer rows.deinit();

        var row = (try rows.next()) orelse unreachable;
        const list = row.list(?Enum, 0).?;
        try t.expectEqual(.@"enum", row.listItemType(0));
        try t.expectEqual(4, list.len);
        try t.expectEqualStrings("type_a", try list.get(0).?.rowCache());
        try t.expectEqual(null, list.get(1));
        try t.expectEqualStrings("type_b", try list.get(2).?.rowCache());
        try t.expectEqualStrings("type_a", try list.get(3).?.rowCache());
    }
}

// There's some internal caching with this, so we need to test mulitple rows
test "read enum" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create type my_type as enum ('type_a', 'type_b')", .{});
    _ = try conn.exec("create type tea_type as enum ('keemun', 'silver_needle')", .{});

    var rows = try conn.query(
        \\ select 'type_a'::my_type, 'type_b'::my_type, null::my_type, 'type_a'::my_type, 'keemun'::tea_type, 'silver_needle'::tea_type, null::tea_type, 'silver_needle'::tea_type
        \\ union all
        \\ select 'type_b'::my_type, null::my_type, 'type_a'::my_type, 'type_b'::my_type, 'keemun'::tea_type, 'silver_needle'::tea_type, null::tea_type, 'silver_needle'::tea_type
    , .{});
    defer rows.deinit();

    var row = (try rows.next()) orelse unreachable;
    try t.expectEqualStrings("type_a", std.mem.span(row.get(Enum, 0).raw()));
    try t.expectEqualStrings("type_a", try row.get(Enum, 0).rowCache());
    try t.expectEqualStrings("type_a", try row.get(Enum, 0).rowCache());
    try t.expectEqualStrings("type_b", try row.get(Enum, 1).rowCache());
    try t.expectEqualStrings("type_b", try row.get(?Enum, 1).?.rowCache());
    try t.expectEqual(null, row.get(?Enum, 2));
    try t.expectEqualStrings("type_a", try row.get(Enum, 3).rowCache());
    try t.expectEqualStrings("keemun", try row.get(Enum, 4).rowCache());
    try t.expectEqualStrings("silver_needle", std.mem.span(row.get(Enum, 5).raw()));
    try t.expectEqual(null, row.get(?Enum, 6));
    try t.expectEqualStrings("silver_needle", try row.get(Enum, 7).rowCache());

    row = (try rows.next()) orelse unreachable;
    try t.expectEqualStrings("type_b", try row.get(Enum, 0).rowCache());
    try t.expectEqual(null, row.get(?Enum, 1));
    try t.expectEqualStrings("type_a", try row.get(Enum, 2).rowCache());
    try t.expectEqualStrings("type_b", try row.get(Enum, 3).rowCache());
    try t.expectEqualStrings("keemun", try row.get(?Enum, 4).?.rowCache());
    try t.expectEqualStrings("silver_needle", try row.get(Enum, 5).rowCache());
    try t.expectEqual(null, row.get(?Enum, 6));
    try t.expectEqualStrings("silver_needle", try row.get(Enum, 7).rowCache());
}

test "owning row" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    {
        // error case
        try t.expectError(error.DuckDBError, conn.row("select x", .{}));
        try t.expectEqualStrings("Binder Error: Referenced column \"x\" not found in FROM clause!\nLINE 1: select x\n               ^", conn.err.?);
    }

    {
        // null
        const row = try conn.row("select 1 where false", .{});
        try t.expectEqual(null, row);
    }

    {
        const row = (try conn.row("select $1::bigint", .{-991823891832})) orelse unreachable;
        defer row.deinit();
        try t.expectEqual(-991823891832, row.get(i64, 0));
    }

    {
        const row = (try conn.row("select [1, 32, 99, null, -4]::int[]", .{})) orelse unreachable;
        defer row.deinit();

        const list = row.list(?i32, 0).?;
        try t.expectEqual(5, list.len);
        try t.expectEqual(1, list.get(0).?);
        try t.expectEqual(32, list.get(1).?);
        try t.expectEqual(99, list.get(2).?);
        try t.expectEqual(null, list.get(3));
        try t.expectEqual(-4, list.get(4).?);
    }
}
