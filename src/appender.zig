const std = @import("std");
const lib = @import("lib.zig");

const c = lib.c;
const Date = lib.Date;
const Time = lib.Time;
const UUID = lib.UUID;
const Interval = lib.Interval;
const Vector = lib.Vector;
const DuckDBError = c.DuckDBError;
const Allocator = std.mem.Allocator;

pub const Appender = struct {
    // Error message, if any
    err: ?[]const u8,

    // whether or not we own the error. The underlying duckdb appender error is
    // owned by the the duckdb appender itself, so we don't need to manage it.
    // but sometimes we create our own error message, in which case we have to
    // free it.
    own_err: bool,

    // the row of the current chunk that we're writing at
    row_index: usize,

    // c.duckdb_vector_size (2048)..when row_index == 2047, we flush and create
    // a new chunk
    vector_size: usize,

    allocator: Allocator,

    // 1 vector per column. Part of the vector data is initialied upfront (the
    // type information). Part of it is initialized for each data_chunk (the
    // underlying duckdb vector data and the validity data).
    vectors: []Vector,

    // This is duplicate of data available from vectors, but we need it as a slice
    // to pass to c.duckdb_create_data_chunk
    types: []c.duckdb_logical_type,

    // The collection of vectors for the appender. While we store data directly
    // in the vector, most operations (e.g. flush) happen on the data chunk.
    data_chunk: ?c.duckdb_data_chunk,
    appender: *c.duckdb_appender,

    pub fn init(allocator: Allocator, appender: *c.duckdb_appender) !Appender {
        const column_count = c.duckdb_appender_column_count(appender.*);

        var types = try allocator.alloc(c.duckdb_logical_type, column_count);
        errdefer allocator.free(types);

        var vectors = try allocator.alloc(Vector, column_count);
        errdefer allocator.free(vectors);

        var initialized: usize = 0;
        errdefer for (0..initialized) |i| {
            vectors[i].deinit();
        };

        for (0..column_count) |i| {
            const logical_type = c.duckdb_appender_column_type(appender.*, i);
            types[i] = logical_type;
            vectors[i] = try Vector.init(undefined, logical_type);
            initialized += 1;

            switch (vectors[i].type) {
                .list => {},
                .scalar => |scalar| switch (scalar) {
                    .simple => {},
                    .decimal => {},
                    .@"enum" => return error.CannotAppendToEnum, // https://github.com/duckdb/duckdb/pull/11704
                },
            }
        }

        return .{
            .err = null,
            .own_err = false,
            .row_index = 0,
            .types = types,
            .vectors = vectors,
            .data_chunk = null,
            .appender = appender,
            .allocator = allocator,
            .vector_size = c.duckdb_vector_size(),
        };
    }

    pub fn deinit(self: *Appender) void {
        for (self.vectors) |*v| {
            v.deinit();
        }

        const allocator = self.allocator;
        allocator.free(self.types);
        allocator.free(self.vectors);

        if (self.data_chunk) |*data_chunk| {
            _ = c.duckdb_destroy_data_chunk(data_chunk);
        }

        const appender = self.appender;
        _ = c.duckdb_appender_destroy(appender);
        allocator.destroy(appender);
    }

    fn newDataChunk(types: []c.duckdb_logical_type, vectors: []Vector) c.duckdb_data_chunk {
        const data_chunk = c.duckdb_create_data_chunk(types.ptr, types.len);

        for (0..types.len) |i| {
            const v = c.duckdb_data_chunk_get_vector(data_chunk, i);
            const vector = &vectors[i];
            vector.loadVector(v);
            vector.validity = null;
        }
        return data_chunk;
    }

    pub fn flush(self: *Appender) !void {
        var data_chunk = self.data_chunk orelse return;
        c.duckdb_data_chunk_set_size(data_chunk, self.row_index);

        const appender = self.appender;
        if (c.duckdb_append_data_chunk(appender.*, data_chunk) == DuckDBError) {
            if (c.duckdb_appender_error(appender.*)) |c_err| {
                self.setErr(std.mem.span(c_err), false);
            }
            return error.DuckDBError;
        }

        if (c.duckdb_appender_flush(self.appender.*) == DuckDBError) {
            if (c.duckdb_appender_error(appender.*)) |c_err| {
                self.setErr(std.mem.span(c_err), false);
            }
            return error.DuckDBError;
        }

        c.duckdb_destroy_data_chunk(&data_chunk);
        self.data_chunk = null;
    }

    pub fn appendRow(self: *Appender, values: anytype) !void {
        // This is to help the caller make sure they set all the values. Not setting
        // a value is an undefined behavior.
        std.debug.assert(values.len == self.vectors.len);

        self.beginRow();

        inline for (values, 0..) |value, i| {
            try self.appendValue(value, i);
        }
        try self.endRow();
    }

    // The appender has two apis. The simplest is to call appendRow, passing the full
    // row. When using appendRow, things mostly just work.
    // It's also possible to call appendValue for each column. This API is used
    // when the "row" isn't known at comptime - the app has no choice but to
    // call appendValue for each column. In such cases, we require an explicit
    // call to beginRow, bindValue and endRow.
    pub fn beginRow(self: *Appender) void {
        if (self.data_chunk == null) {
            self.data_chunk = newDataChunk(self.types, self.vectors);
            self.row_index = 0;
        }
    }

    pub fn endRow(self: *Appender) !void {
        const row_index = self.row_index + 1;
        self.row_index = row_index;
        if (row_index == self.vector_size) {
            try self.flush();
        }
    }

    pub fn appendValue(self: *Appender, value: anytype, column: usize) !void {
        var vector = &self.vectors[column];
        const row_index = self.row_index;

        const T = @TypeOf(value);
        const type_info = @typeInfo(T);
        switch (type_info) {
            .Null => {
                const validity = vector.validity orelse blk: {
                    c.duckdb_vector_ensure_validity_writable(vector.vector);
                    const v = c.duckdb_vector_get_validity(vector.vector);
                    vector.validity = v;
                    break :blk v;
                };
                c.duckdb_validity_set_row_invalid(validity, row_index);
                return;
            },
            .Optional => {
                if (value) |v| {
                    return self.appendValue(v, column);
                } else {
                    return self.appendValue(null, column);
                }
            },
            .Pointer => |ptr| {
                switch (ptr.size) {
                    .Slice => return self.appendSlice(vector, @as([]const ptr.child, value), row_index),
                    .One => switch (@typeInfo(ptr.child)) {
                        .Array => {
                            const Slice = []const std.meta.Elem(ptr.child);
                            return self.appendSlice(vector, @as(Slice, value), row_index);
                        },
                        else => appendError(T),
                    },
                    else => appendError(T),
                }
            },
            .Array => return self.appendValue(&value, column),
            else => {},
        }

        switch (vector.data) {
            .list => return self.appendTypeError("list", T),
            .scalar => |scalar| switch (scalar) {
                .bool => |data| {
                    switch (type_info) {
                        .Bool => data[row_index] = value,
                        else => return self.appendTypeError("boolean", T),
                    }
                },
                .i8 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.TINYINT_MIN or value > lib.TINYINT_MAX) return self.appendIntRangeError("tinyint");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("tinyint", T),
                    }
                },
                .i16 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.SMALLINT_MIN or value > lib.SMALLINT_MAX) return self.appendIntRangeError("smallint");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("smallint", T),
                    }
                },
                .i32 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.INTEGER_MIN or value > lib.INTEGER_MAX) return self.appendIntRangeError("integer");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("integer", T),
                    }
                },
                .i64 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.BIGINT_MIN or value > lib.BIGINT_MAX) return self.appendIntRangeError("bigint");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("bigint", T),
                    }
                },
                .i128 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.HUGEINT_MIN or value > lib.HUGEINT_MAX) return self.appendIntRangeError("hugeint");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("hugeint", T),
                    }
                },
                .u8 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.UTINYINT_MIN or value > lib.UTINYINT_MAX) return self.appendIntRangeError("utinyint");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("utinyint", T),
                    }
                },
                .u16 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.USMALLINT_MIN or value > lib.USMALLINT_MAX) return self.appendIntRangeError("usmallint");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("usmallint", T),
                    }
                },
                .u32 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.UINTEGER_MIN or value > lib.UINTEGER_MAX) return self.appendIntRangeError("uinteger");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("uinteger", T),
                    }
                },
                .u64 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.UBIGINT_MIN or value > lib.UBIGINT_MAX) return self.appendIntRangeError("ubingint");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("ubingint", T),
                    }
                },
                .u128 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.UHUGEINT_MIN or value > lib.UHUGEINT_MAX) return self.appendIntRangeError("uhugeint");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("uhugeint", T),
                    }
                },
                .f32 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => data[row_index] = @floatFromInt(value),
                        .Float, .ComptimeFloat => data[row_index] = @floatCast(value),
                        else => return self.appendTypeError("real", T),
                    }
                },
                .f64 => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => data[row_index] = @floatFromInt(value),
                        .Float, .ComptimeFloat => data[row_index] = @floatCast(value),
                        else => return self.appendTypeError("double", T),
                    }
                },
                .date => |data| if (T == Date) {
                    data[row_index] = c.duckdb_to_date(value);
                } else {
                    return self.appendTypeError("date", T);
                },
                .time => |data| if (T == Time) {
                    data[row_index] = c.duckdb_to_time(value);
                } else {
                    return self.appendTypeError("time", T);
                },
                .interval => |data| if (T == Interval) {
                    data[row_index] = value;
                } else {
                    return self.appendTypeError("interval", T);
                },
                .timestamp => |data| {
                    switch (type_info) {
                        .Int, .ComptimeInt => {
                            if (value < lib.BIGINT_MIN or value > lib.BIGINT_MAX) return self.appendIntRangeError("i64");
                            data[row_index] = @intCast(value);
                        },
                        else => return self.appendTypeError("timestamp", T),
                    }
                },
                .decimal => |data| return self.setDecimal(value, data, row_index),
                .varchar => {
                    var buf: [std.fmt.format_float.min_buffer_size]u8 = undefined;
                    const v = switch (type_info) {
                        .Int, .ComptimeInt => blk: {
                            const n = std.fmt.formatIntBuf(&buf, value, 10, .lower, .{});
                            break :blk buf[0..n];
                        },
                        .Float, .ComptimeFloat => try std.fmt.formatFloat(&buf, value, .{}),
                        .Bool => if (value == true) "true" else "false",
                        else => {
                            const err = try std.fmt.allocPrint(self.allocator, "cannot bind a {any} (type {s}) to a varchar column", .{ value, @typeName(T) });
                            self.setErr(err, true);
                            return error.AppendError;
                        },
                    };
                    c.duckdb_vector_assign_string_element_len(vector.vector, row_index, v.ptr, v.len);
                },
                .uuid => |data| {
                    switch (type_info) {
                        .Int => |int| {
                            if (int.signedness == .signed and int.bits == 128) {
                                data[row_index] = value;
                                return;
                            }
                        },
                        else => {},
                    }
                    return self.appendTypeError("uuid", T);
                },
                else => {
                    const err = try std.fmt.allocPrint(self.allocator, "cannot bind a {any} (type {s}) to a column of type {s}", .{ value, @typeName(T), @tagName(std.meta.activeTag(scalar)) });
                    self.setErr(err, true);
                    return error.AppendError;
                },
            },
        }
    }

    pub fn clearError(self: *Appender) !void {
        const e = self.err orelse return;
        if (self.own_err) {
            self.allocator.free(e);
        }
        self.err = null;
        self.own_err = false;
    }

    fn appendSlice(self: *Appender, vector: *Vector, values: anytype, row_index: usize) !void {
        const T = @TypeOf(values);
        switch (vector.data) {
            .list => |*list| {
                const size = list.size;
                const new_size = try self.setListSize(vector.vector, list, values.len, row_index);
                switch (list.child) {
                    .i8 => |data| return self.setListDirect(i8, "tinyint[]", list, values, data[size..new_size]),
                    .i16 => |data| return self.setListDirect(i16, "smallint[]", list, values, data[size..new_size]),
                    .i32 => |data| return self.setListDirect(i32, "integer[]", list, values, data[size..new_size]),
                    .i64 => |data| return self.setListDirect(i64, "bigint[]", list, values, data[size..new_size]),
                    .i128 => |data| return self.setListDirect(i128, "hugeint[]", list, values, data[size..new_size]),
                    .u8 => |data| return self.setListDirect(u8, "utinyint[]", list, values, data[size..new_size]),
                    .u16 => |data| return self.setListDirect(u16, "usmallint[]", list, values, data[size..new_size]),
                    .u32 => |data| return self.setListDirect(u32, "uinteger[]", list, values, data[size..new_size]),
                    .u64 => |data| return self.setListDirect(u64, "ubigint[]", list, values, data[size..new_size]),
                    .u128 => |data| return self.setListDirect(u128, "uhugeint[]", list, values, data[size..new_size]),
                    .f32 => |data| return self.setListDirect(f32, "real[]", list, values, data[size..new_size]),
                    .f64 => |data| return self.setListDirect(f64, "doule[]", list, values, data[size..new_size]),
                    .bool => |data| return self.setListDirect(bool, "bool[]", list, values, data[size..new_size]),
                    .timestamp => |data| return self.setListDirect(i64, "timestamp[]", list, values, data[size..new_size]),
                    .interval => |data| return self.setListDirect(Interval, "interval[]", list, values, data[size..new_size]),
                    .date => |data| return self.setListTransform(Date, c.duckdb_date, "date", list, values, data[size..new_size], c.duckdb_to_date),
                    .time => |data| return self.setListTransform(Time, c.duckdb_time, "time", list, values, data[size..new_size], c.duckdb_to_time),
                    .blob, .varchar => if (T == []const []const u8 or T == []const []u8) {
                        const child_vector = list.child_vector;
                        for (values, size..) |value, i| {
                            c.duckdb_vector_assign_string_element_len(child_vector, i, value.ptr, value.len);
                        }
                    } else if (T == []const ?[]const u8 or T == []const ?[]u8) {
                        const child_vector = list.child_vector;
                        const child_validity = list.childValidity();
                        for (values, size..) |value, i| {
                            if (value) |v| {
                                c.duckdb_vector_assign_string_element_len(child_vector, i, v.ptr, v.len);
                            } else {
                                c.duckdb_validity_set_row_invalid(child_validity, i);
                            }
                        }
                    } else {
                        return self.appendTypeError("text[] / blob[]", T);
                    },
                    .uuid => |data| if (T == []const []const u8 or T == []const []u8) {
                        for (values, size..) |value, i| {
                            data[i] = try encodeUUID(value);
                        }
                    } else if (T == []const ?[]const u8 or T == []const ?[]u8) {
                        const child_validity = list.childValidity();
                        for (values, size..) |value, i| {
                            if (value) |v| {
                                data[i] = try encodeUUID(v);
                            } else {
                                c.duckdb_validity_set_row_invalid(child_validity, i);
                            }
                        }
                    } else {
                        return self.appendTypeError("uuid[]", T);
                    },
                    .decimal => |data| {
                        if (T == []const i64 or T == []const f32 or T == []const f64) {
                            for (values, size..) |value, i| {
                                try self.setDecimal(value, data, i);
                            }
                        } else if (T == []const ?i64 or T == []const ?f32 or T == []const ?f64) {
                            const child_validity = list.childValidity();
                            for (values, size..) |value, i| {
                                if (value) |v| {
                                    try self.setDecimal(v, data, i);
                                } else {
                                    c.duckdb_validity_set_row_invalid(child_validity, i);
                                }
                            }
                        } else {
                            return self.appendTypeError("decimal[]", T);
                        }
                    },
                    inline else => |_, tag| return self.appendTypeError(@tagName(tag), T),
                }
            },
            .scalar => |scalar| switch (scalar) {
                .varchar, .blob => {
                    // We have a []u8 or []const u8. This could either be a text value
                    // or a utinyint[]. The type of the vector resolves the ambiguity.
                    if (T == []const u8) {
                        c.duckdb_vector_assign_string_element_len(vector.vector, row_index, values.ptr, values.len);
                    } else {
                        return self.appendTypeError("varchar/blob", T);
                    }
                },
                .uuid => |data| {
                    // maybe we have a []u8 that represents a UUID (either in binary or hex)
                    if (T == []const u8) {
                        data[row_index] = try encodeUUID(values);
                    } else {
                        return self.appendTypeError("uuid", T);
                    }
                },
                inline else => |_, tag| return self.appendTypeError(@tagName(tag), T),
            },
        }
    }

    fn setDecimal(self: *Appender, value: anytype, vector: Vector.Decimal, row_index: usize) !void {
        const T = @TypeOf(value);
        switch (@typeInfo(T)) {
            .Int, .ComptimeInt => switch (vector.internal) {
                .i16 => |data| {
                    if (value < lib.SMALLINT_MIN or value > lib.SMALLINT_MAX) return self.appendIntRangeError("smallint");
                    data[row_index] = @intCast(value);
                },
                .i32 => |data| {
                    if (value < lib.INTEGER_MIN or value > lib.INTEGER_MAX) return self.appendIntRangeError("integer");
                    data[row_index] = @intCast(value);
                },
                .i64 => |data| {
                    if (value < lib.BIGINT_MIN or value > lib.BIGINT_MAX) return self.appendIntRangeError("bigint");
                    data[row_index] = @intCast(value);
                },
                .i128 => |data| {
                    if (value < lib.HUGEINT_MIN or value > lib.HUGEINT_MAX) return self.appendIntRangeError("hugeint");
                    data[row_index] = @intCast(value);
                },
            },
            .Float, .ComptimeFloat => {
                // YES, there's a lot of duplication going on. But, I don't think the float and int codepaths can be merged
                // without forcing int value to an i128, which seems wasteful to me.
                const huge: i128 = switch (vector.scale) {
                    0 => @intFromFloat(value),
                    1 => @intFromFloat(value * 10),
                    2 => @intFromFloat(value * 100),
                    3 => @intFromFloat(value * 1000),
                    4 => @intFromFloat(value * 10000),
                    5 => @intFromFloat(value * 100000),
                    6 => @intFromFloat(value * 1000000),
                    7 => @intFromFloat(value * 10000000),
                    8 => @intFromFloat(value * 100000000),
                    9 => @intFromFloat(value * 1000000000),
                    10 => @intFromFloat(value * 10000000000),
                    else => |n| @intFromFloat(value * std.math.pow(f64, 10, @floatFromInt(n))),
                };
                switch (vector.internal) {
                    .i16 => |data| {
                        if (huge < lib.SMALLINT_MIN or huge > lib.SMALLINT_MAX) return self.appendIntRangeError("smallint");
                        data[row_index] = @intCast(huge);
                    },
                    .i32 => |data| {
                        if (huge < lib.INTEGER_MIN or huge > lib.INTEGER_MAX) return self.appendIntRangeError("integer");
                        data[row_index] = @intCast(huge);
                    },
                    .i64 => |data| {
                        if (huge < lib.BIGINT_MIN or huge > lib.BIGINT_MAX) return self.appendIntRangeError("bigint");
                        data[row_index] = @intCast(huge);
                    },
                    .i128 => |data| {
                        if (huge < lib.HUGEINT_MIN or huge > lib.HUGEINT_MAX) return self.appendIntRangeError("hugeint");
                        data[row_index] = @intCast(huge);
                    },
                }
            },
            else => return self.appendTypeError("decimal", T),
        }
    }

    fn setListDirect(self: *Appender, comptime T: type, comptime column_type: []const u8, list: *Vector.List, values: anytype, data: anytype) !void {
        if (@TypeOf(values) == []const T) {
            @memcpy(data, values);
            return;
        }

        if (@TypeOf(values) == []const ?T) {
            const validity = list.childValidity();
            const validity_start = list.size - values.len;

            for (values, 0..) |value, i| {
                if (value) |v| {
                    data[i] = v;
                } else {
                    c.duckdb_validity_set_row_invalid(validity, validity_start + i);
                }
            }
            return;
        }

        return self.appendTypeError(column_type, @TypeOf(values));
    }

    fn setListTransform(self: *Appender, comptime T: type, comptime D: type, comptime column_type: []const u8, list: *Vector.List, values: anytype, data: anytype, transform: *const fn (T) callconv(.C) D) !void {
        if (@TypeOf(values) == []const T) {
            for (values, 0..) |value, i| {
                data[i] = transform(value);
            }
            return;
        }

        if (@TypeOf(values) == []const ?T) {
            const validity = list.childValidity();
            const validity_start = list.size - values.len;

            for (values, 0..) |value, i| {
                if (value) |v| {
                    data[i] = transform(v);
                } else {
                    c.duckdb_validity_set_row_invalid(validity, validity_start + i);
                }
            }
            return;
        }

        return self.appendTypeError(column_type, @TypeOf(values));
    }

    pub fn appendListMap(self: *Appender, comptime T: type, comptime D: type, column: usize, values: []const T, transform: *const fn (T) OptionalType(D)) !void {
        var vector = &self.vectors[column];
        switch (vector.data) {
            .list => |*list| {
                const size = list.size;
                const new_size = try self.setListSize(vector.vector, list, values.len, self.row_index);
                const validity = list.childValidity();
                const validity_start = list.size - values.len;
                const child = list.child;
                switch (D) {
                    i8 => try appendListMapInto(T, i8, child.i8[size..new_size], values, transform, validity, validity_start),
                    i16 => try appendListMapInto(T, i16, child.i16[size..new_size], values, transform, validity, validity_start),
                    i32 => try appendListMapInto(T, i32, child.i32[size..new_size], values, transform, validity, validity_start),
                    i64 => switch (child) {
                        .i64 => |cc| try appendListMapInto(T, i64, cc[size..new_size], values, transform, validity, validity_start),
                        .timestamp => |cc| try appendListMapInto(T, i64, cc[size..new_size], values, transform, validity, validity_start),
                        else => unreachable,
                    },
                    i128 => switch (child) {
                        .uuid => |cc| try appendListMapInto(T, i128, cc[size..new_size], values, transform, validity, validity_start),
                        .i128 => |cc| try appendListMapInto(T, i128, cc[size..new_size], values, transform, validity, validity_start),
                        else => unreachable,
                    },
                    u8 => try appendListMapInto(T, u8, child.u8[size..new_size], values, transform, validity, validity_start),
                    u16 => try appendListMapInto(T, u16, child.u16[size..new_size], values, transform, validity, validity_start),
                    u32 => try appendListMapInto(T, u32, child.u32[size..new_size], values, transform, validity, validity_start),
                    u64 => try appendListMapInto(T, u64, child.u64[size..new_size], values, transform, validity, validity_start),
                    u128 => try appendListMapInto(T, u128, child.u128[size..new_size], values, transform, validity, validity_start),
                    bool => try appendListMapInto(T, bool, child.bool[size..new_size], values, transform, validity, validity_start),
                    f32 => try appendListMapInto(T, f32, child.f32[size..new_size], values, transform, validity, validity_start),
                    f64 => try appendListMapInto(T, f64, child.f64[size..new_size], values, transform, validity, validity_start),
                    []u8, []const u8 => switch (child) {
                        .uuid => |cc| try appendUUIDListMapInto(T, cc[size..new_size], values, transform, validity, validity_start),
                        .blob, .varchar => try appendTextListMapInto(T, list.child_vector, size, values, transform, validity),
                        else => unreachable,
                    },
                    Date => try appendDateListMapInto(T, child.date[size..new_size], values, transform, validity, validity_start),
                    Time => try appendTimeListMapInto(T, child.time[size..new_size], values, transform, validity, validity_start),
                    // interval: [*]c.duckdb_interval,
                    // decimal: Vector.Decimal,
                    else => {
                        const err = try std.fmt.allocPrint(self.allocator, "appendListMap does not support {s} lists", .{@typeName(D)});
                        return self.setErr(err, true);
                    },
                }
            },
            else => {
                const err = try std.fmt.allocPrint(self.allocator, "column {d} is not a list", .{column});
                return self.setErr(err, true);
            },
        }
    }

    fn appendListMapInto(comptime T: type, comptime D: type, data: []D, values: anytype, transform: *const fn (T) OptionalType(D), validity: [*c]u64, validity_start: usize) !void {
        for (values, 0..) |value, i| {
            if (transform(value)) |v| {
                data[i] = v;
            } else {
                c.duckdb_validity_set_row_invalid(validity, validity_start + i);
            }
        }
    }

    fn appendTextListMapInto(comptime T: type, child_vector: c.duckdb_vector, start: usize, values: anytype, transform: *const fn (T) ?[]const u8, validity: [*c]u64) !void {
        for (values, start..) |value, i| {
            if (transform(value)) |v| {
                c.duckdb_vector_assign_string_element_len(child_vector, i, v.ptr, v.len);
            } else {
                c.duckdb_validity_set_row_invalid(validity, i);
            }
        }
    }

    fn appendDateListMapInto(comptime T: type, data: []c.duckdb_date, values: anytype, transform: *const fn (T) ?Date, validity: [*c]u64, validity_start: usize) !void {
        for (values, 0..) |value, i| {
            if (transform(value)) |v| {
                data[i] = c.duckdb_to_date(v);
            } else {
                c.duckdb_validity_set_row_invalid(validity, validity_start + i);
            }
        }
    }

    fn appendTimeListMapInto(comptime T: type, data: []c.duckdb_time, values: anytype, transform: *const fn (T) ?Time, validity: [*c]u64, validity_start: usize) !void {
        for (values, 0..) |value, i| {
            if (transform(value)) |v| {
                data[i] = c.duckdb_to_time(v);
            } else {
                c.duckdb_validity_set_row_invalid(validity, validity_start + i);
            }
        }
    }

    fn appendUUIDListMapInto(comptime T: type, data: []i128, values: anytype, transform: *const fn (T) ?[]const u8, validity: [*c]u64, validity_start: usize) !void {
        for (values, 0..) |value, i| {
            if (transform(value)) |v| {
                data[i] = try encodeUUID(v);
            } else {
                c.duckdb_validity_set_row_invalid(validity, validity_start + i);
            }
        }
    }

    fn setListSize(self: *Appender, vector: c.duckdb_vector, list: *Vector.List, value_count: usize, row_index: usize) !usize {
        const size = list.size;
        const new_size = size + value_count;
        list.size = new_size;

        list.entries[row_index] = .{
            .offset = size,
            .length = value_count,
        };

        if (c.duckdb_list_vector_set_size(vector, new_size) == DuckDBError) {
            self.setErr("failed to set vector size", false);
            return error.DuckDBError;
        }
        if (c.duckdb_list_vector_reserve(vector, new_size) == DuckDBError) {
            self.setErr("failed to reserve vector space", false);
            return error.DuckDBError;
        }
        return new_size;
    }

    fn appendTypeError(self: *Appender, comptime data_type: []const u8, value_type: type) error{AppendError} {
        self.setErr("cannot bind a " ++ @typeName(value_type) ++ " to a column of type " ++ data_type, false);
        return error.AppendError;
    }

    fn appendIntRangeError(self: *Appender, comptime data_type: []const u8) error{AppendError} {
        self.setErr("value is outside of range for a column of type " ++ data_type, false);
        return error.AppendError;
    }

    fn setErr(self: *Appender, err: []const u8, own: bool) void {
        // free any previous owned error we have
        if (self.own_err) {
            self.allocator.free(self.err.?);
        }
        self.err = err;
        self.own_err = own;
    }
};

fn OptionalType(comptime T: type) type {
    switch (@typeInfo(T)) {
        .Optional => return T,
        else => return ?T,
    }
}

fn appendError(comptime T: type) void {
    @compileError("cannot append value of type " ++ @typeName(T));
}

pub fn encodeUUID(value: []const u8) !i128 {
    if (value.len == 16) {
        const n = std.mem.readInt(i128, value[0..16], .big);
        return n ^ (@as(i128, 1) << 127);
    }
    return lib.encodeUUID(value);
}

const t = std.testing;
const DB = lib.DB;
test "Appender: bind errors" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table x (a integer)", .{});
    {
        var appender = try conn.appender(null, "x");
        defer appender.deinit();
        try t.expectError(error.AppendError, appender.appendRow(.{true}));
        try t.expectEqualStrings("cannot bind a bool to a column of type integer", appender.err.?);
    }

    {
        var appender = try conn.appender(null, "x");
        defer appender.deinit();
        try t.expectError(error.AppendError, appender.appendRow(.{9147483647}));
        try t.expectEqualStrings("value is outside of range for a column of type integer", appender.err.?);
    }
}

test "CannotAppendToDecimal" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec(
        \\ create table x (
        \\   col_tinyint tinyint,
        \\   col_smallint smallint,
        \\   col_integer integer,
        \\   col_bigint bigint,
        \\   col_hugeint hugeint,
        \\   col_utinyint utinyint,
        \\   col_usmallint usmallint,
        \\   col_uinteger uinteger,
        \\   col_ubigint ubigint,
        \\   col_uhugeint uhugeint,
        \\   col_bool bool,
        \\   col_real real,
        \\   col_double double,
        \\   col_text text,
        \\   col_blob blob,
        \\   col_uuid uuid,
        \\   col_date date,
        \\   col_time time,
        \\   col_interval interval,
        \\   col_timestamp timestamp,
        \\   col_decimal decimal(18, 6),
        \\ )
    , .{});

    {
        var appender = try conn.appender(null, "x");
        defer appender.deinit();
        try appender.appendRow(.{ -128, lib.SMALLINT_MIN, lib.INTEGER_MIN, lib.BIGINT_MIN, lib.HUGEINT_MIN, lib.UTINYINT_MAX, lib.USMALLINT_MAX, lib.UINTEGER_MAX, lib.UBIGINT_MAX, lib.UHUGEINT_MAX, true, -1.23, 1994.848288123, "over 9000!", &[_]u8{ 1, 2, 3, 254 }, "34c667cd-638e-40c2-b256-0f78ccab7013", Date{ .year = 2023, .month = 5, .day = 10 }, Time{ .hour = 21, .min = 4, .sec = 49, .micros = 123456 }, Interval{ .months = 3, .days = 7, .micros = 982810 }, 1711506018088167, 39858392.36212 });
        try appender.flush();

        try t.expectEqual(null, appender.err);

        var row = (try conn.row("select * from x", .{})).?;
        defer row.deinit();
        try t.expectEqual(-128, row.get(i8, 0));
        try t.expectEqual(lib.SMALLINT_MIN, row.get(i16, 1));
        try t.expectEqual(lib.INTEGER_MIN, row.get(i32, 2));
        try t.expectEqual(lib.BIGINT_MIN, row.get(i64, 3));
        try t.expectEqual(lib.HUGEINT_MIN, row.get(i128, 4));
        try t.expectEqual(lib.UTINYINT_MAX, row.get(u8, 5));
        try t.expectEqual(lib.USMALLINT_MAX, row.get(u16, 6));
        try t.expectEqual(lib.UINTEGER_MAX, row.get(u32, 7));
        try t.expectEqual(lib.UBIGINT_MAX, row.get(u64, 8));
        try t.expectEqual(lib.UHUGEINT_MAX, row.get(u128, 9));
        try t.expectEqual(true, row.get(bool, 10));
        try t.expectEqual(-1.23, row.get(f32, 11));
        try t.expectEqual(1994.848288123, row.get(f64, 12));
        try t.expectEqualStrings("over 9000!", row.get([]u8, 13));
        try t.expectEqualStrings(&[_]u8{ 1, 2, 3, 254 }, row.get([]u8, 14));
        try t.expectEqualStrings("34c667cd-638e-40c2-b256-0f78ccab7013", &row.get(lib.UUID, 15));
        try t.expectEqual(Date{ .year = 2023, .month = 5, .day = 10 }, row.get(Date, 16));
        try t.expectEqual(Time{ .hour = 21, .min = 4, .sec = 49, .micros = 123456 }, row.get(Time, 17));
        try t.expectEqual(Interval{ .months = 3, .days = 7, .micros = 982810 }, row.get(Interval, 18));
        try t.expectEqual(1711506018088167, row.get(i64, 19));
        try t.expectEqual(39858392.36212, row.get(f64, 20));
    }
}

test "Appender: basic variants" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec(
        \\ create table x (
        \\   id integer,
        \\   col_bool bool,
        \\   col_uuid uuid
        \\ )
    , .{});

    var appender = try conn.appender(null, "x");
    defer appender.deinit();
    try appender.appendRow(.{ 1, false, &[_]u8{ 0xf9, 0x3b, 0x64, 0xe0, 0x91, 0x62, 0x40, 0xf5, 0xaa, 0xb8, 0xa0, 0x1f, 0x5c, 0xe9, 0x90, 0x32 } });
    try appender.appendRow(.{ 2, null, null });
    try appender.appendRow(.{ 3, null, @as(i128, 96426444282114970045097725006964541666) });
    try appender.flush();

    try t.expectEqual(null, appender.err);

    {
        var row = (try conn.row("select * from x where id = 1", .{})).?;
        defer row.deinit();
        try t.expectEqual(false, row.get(bool, 1));
        try t.expectEqualStrings("f93b64e0-9162-40f5-aab8-a01f5ce99032", &row.get(lib.UUID, 2));
    }

    {
        var row = (try conn.row("select * from x where id = 2", .{})).?;
        defer row.deinit();
        try t.expectEqual(null, row.get(?bool, 1));
        try t.expectEqual(null, row.get(?lib.UUID, 2));
    }

    {
        var row = (try conn.row("select col_uuid from x where id = 3", .{})).?;
        defer row.deinit();
        try t.expectEqualStrings("c88b0ec1-fa66-40fc-8eb6-09867e9f48e2", &row.get(lib.UUID, 0));
    }
}

test "Appender: int/float/bool into varchar and json" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec(
        \\ create table x (
        \\   id integer,
        \\   a varchar,
        \\   b json
        \\ )
    , .{});

    var appender = try conn.appender(null, "x");
    defer appender.deinit();
    try appender.appendRow(.{ 1, @as(i32, -39991), @as(f64, 3.14159) });
    try appender.appendRow(.{ 2, @as(f32, 0.991), @as(u16, 1025) });
    try appender.appendRow(.{ 3, 1234, 5.6789 });
    try appender.appendRow(.{ 4, -987.65, -5432 });
    try appender.appendRow(.{ 5, true, false });
    try appender.flush();

    try t.expectEqual(null, appender.err);

    {
        var row = (try conn.row("select a, b from x where id = 1", .{})).?;
        defer row.deinit();
        try t.expectEqualStrings("-39991", row.get([]u8, 0));
        try t.expectEqualStrings("3.14159e0", row.get([]u8, 1));
    }

    {
        var row = (try conn.row("select a, b from x where id = 2", .{})).?;
        defer row.deinit();
        try t.expectEqualStrings("9.91e-1", row.get([]u8, 0));
        try t.expectEqualStrings("1025", row.get([]u8, 1));
    }

    {
        var row = (try conn.row("select a, b from x where id = 3", .{})).?;
        defer row.deinit();
        try t.expectEqualStrings("1234", row.get([]u8, 0));
        try t.expectEqualStrings("5.6789e0", row.get([]u8, 1));
    }

    {
        var row = (try conn.row("select a, b from x where id = 4", .{})).?;
        defer row.deinit();
        try t.expectEqualStrings("-9.8765e2", row.get([]u8, 0));
        try t.expectEqualStrings("-5432", row.get([]u8, 1));
    }

    {
        var row = (try conn.row("select a, b from x where id = 5", .{})).?;
        defer row.deinit();
        try t.expectEqualStrings("true", row.get([]u8, 0));
        try t.expectEqualStrings("false", row.get([]u8, 1));
    }
}

test "Appender: multiple chunks" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table x (a integer, b integer)", .{});

    {
        var appender = try conn.appender(null, "x");
        defer appender.deinit();

        for (0..1000) |i| {
            appender.beginRow();
            try appender.appendValue(i, 0);
            if (@mod(i, 3) == 0) {
                try appender.appendValue(null, 1);
            } else {
                try appender.appendValue(i * 2, 1);
            }
            try appender.endRow();
        }
        try appender.flush();
    }

    var rows = try conn.query("select * from x order by a", .{});
    defer rows.deinit();

    var i: i32 = 0;
    while (try rows.next()) |row| {
        try t.expectEqual(i, row.get(i32, 0));

        if (@mod(i, 3) == 0) {
            try t.expectEqual(null, row.get(?i32, 1));
        } else {
            try t.expectEqual(i * 2, row.get(i32, 1));
        }
        i += 1;
    }
    try t.expectEqual(1000, i);
}

test "Appender: implicit and explicit flush" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table x (a integer)", .{});

    {
        var appender = try conn.appender(null, "x");
        defer appender.deinit();

        try appender.appendRow(.{0});

        appender.beginRow();
        try appender.appendValue(1, 0);
        try appender.endRow();

        try appender.flush();

        for (2..5000) |i| {
            appender.beginRow();
            try appender.appendValue(i, 0);
            try appender.endRow();
        }

        try appender.appendRow(.{5000});

        appender.beginRow();
        try appender.appendValue(5001, 0);
        try appender.endRow();

        try appender.flush();
    }

    var rows = try conn.query("select * from x order by a", .{});
    defer rows.deinit();

    var i: i32 = 0;
    while (try rows.next()) |row| {
        try t.expectEqual(i, row.get(i32, 0));
        i += 1;
    }
    try t.expectEqual(5002, i);
}

test "Appender: hugeint" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table x (a hugeint)", .{});

    const COUNT = 1000;
    var expected: [COUNT]i128 = undefined;
    {
        var seed: u64 = undefined;
        std.posix.getrandom(std.mem.asBytes(&seed)) catch unreachable;
        var prng = std.Random.DefaultPrng.init(seed);

        const random = prng.random();

        var appender = try conn.appender(null, "x");
        defer appender.deinit();

        for (0..COUNT) |i| {
            const value = random.int(i128);
            expected[i] = value;
            try appender.appendRow(.{value});
        }
        try appender.flush();
    }

    var rows = try conn.query("select * from x", .{});
    defer rows.deinit();

    var i: i32 = 0;
    while (try rows.next()) |row| {
        try t.expectEqual(expected[@intCast(i)], row.get(i128, 0));
        i += 1;
    }
    try t.expectEqual(COUNT, i);
}

test "Appender: decimal" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table appdec (id integer, d decimal(8, 4))", .{});

    {
        var appender = try conn.appender(null, "appdec");
        defer appender.deinit();
        try appender.appendRow(.{ 1, 12345678 });
        try appender.flush();

        var row = (try conn.row("select d from appdec where id = 1", .{})).?;
        defer row.deinit();
        try t.expectEqual(1234.5678, row.get(f64, 0));
    }

    {
        var appender = try conn.appender(null, "appdec");
        defer appender.deinit();
        try appender.appendRow(.{ 2, 5323.224 });
        try appender.flush();

        var row = (try conn.row("select d from appdec where id = 2", .{})).?;
        defer row.deinit();
        try t.expectEqual(5323.224, row.get(f64, 0));
    }
}

test "Appender: decimal fuzz" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table appdec (d1 decimal(3, 1), d2 decimal(9, 3), d3 decimal(17, 5), d4 decimal(30, 10))", .{});

    const COUNT = 1000;
    var expected_i16: [COUNT]f64 = undefined;
    var expected_i32: [COUNT]f64 = undefined;
    var expected_i64: [COUNT]f64 = undefined;
    var expected_i128: [COUNT]f64 = undefined;
    {
        var seed: u64 = undefined;
        std.posix.getrandom(std.mem.asBytes(&seed)) catch unreachable;
        var prng = std.Random.DefaultPrng.init(seed);
        const random = prng.random();

        var appender = try conn.appender(null, "appdec");
        defer appender.deinit();

        for (0..COUNT) |i| {
            const d1 = @trunc(random.float(f64) * 100) / 10;
            expected_i16[i] = d1;
            const d2 = @trunc(random.float(f64) * 100000000) / 1000;
            expected_i32[i] = d2;
            const d3 = @trunc(random.float(f64) * 10000000000000000) / 100000;
            expected_i64[i] = d3;
            const d4 = @trunc(random.float(f64) * 100000000000000000000000000000) / 10000000000;
            expected_i128[i] = d4;

            try appender.appendRow(.{ d1, d2, d3, d4 });
        }
        try appender.flush();
    }

    var rows = try conn.query("select * from appdec", .{});
    defer rows.deinit();

    var i: i32 = 0;
    while (try rows.next()) |row| {
        try t.expectApproxEqRel(expected_i16[@intCast(i)], row.get(f64, 0), 0.01);
        try t.expectApproxEqRel(expected_i32[@intCast(i)], row.get(f64, 1), 0.001);
        try t.expectApproxEqRel(expected_i64[@intCast(i)], row.get(f64, 2), 0.00001);
        try t.expectApproxEqRel(expected_i128[@intCast(i)], row.get(f64, 3), 0.0000000001);
        i += 1;
    }
    try t.expectEqual(COUNT, i);
}

test "Appender: optional values" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    errdefer std.log.err("conn: {?s}", .{conn.err});

    _ = try conn.exec("create table optionals (id integer, non_null_data integer, null_data integer)", .{});

    {
        var appender = try conn.appender(null, "optionals");
        defer appender.deinit();

        var i: i32 = 0;
        while (i < 10) : (i += 1) {
            const non_null_data: ?i32 = i;
            const null_data: ?i32 = null;

            try appender.appendRow(.{ i, non_null_data, null_data});
        }
        try appender.flush();
    }

    var rows = try conn.query("select id, non_null_data, null_data from optionals order by id", .{});
    defer rows.deinit();

    var i: i32 = 0;
    while (try rows.next()) |row| {
        try t.expectEqual(i, row.get(?i32, 0));
        try t.expectEqual(i, row.get(?i32, 1));
        try t.expectEqual(null, row.get(?i32, 2));
        i += 1;
    }
    try t.expectEqual(10, i);
}

test "Appender: json" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table aj (id integer, data json)", .{});

    {
        var appender = try conn.appender(null, "aj");
        defer appender.deinit();
        try appender.appendRow(.{ 1, "{\"id\":1,\"x\":true}" });
        try appender.flush();

        var row = (try conn.row("select data from aj where id = 1", .{})).?;
        defer row.deinit();
        try t.expectEqualStrings("{\"id\":1,\"x\":true}", row.get([]u8, 0));
    }
}

test "Appender: list simple types" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec(
        \\ create table applist (
        \\  id integer,
        \\  col_tinyint tinyint[],
        \\  col_smallint smallint[],
        \\  col_integer integer[],
        \\  col_bigint bigint[],
        \\  col_hugeint hugeint[],
        \\  col_utinyint utinyint[],
        \\  col_usmallint usmallint[],
        \\  col_uinteger uinteger[],
        \\  col_ubigint ubigint[],
        \\  col_uhugeint uhugeint[],
        \\  col_real real[],
        \\  col_double double[],
        \\  col_bool bool[],
        \\  col_text text[],
        \\  col_uuid uuid[],
        \\  col_date date[],
        \\  col_time time[],
        \\  col_timestamp timestamp[],
        \\  col_interval interval[],
        \\  col_decimal decimal(18, 6)[],
        \\ )
    , .{});

    {
        var appender = try conn.appender(null, "applist");
        defer appender.deinit();

        try appender.appendRow(.{
            1,
            &[_]i8{ -128, 0, 100, 127 },
            &[_]i16{ -32768, 0, -299, 32767 },
            &[_]i32{ -2147483648, -4933, 0, 2147483647 },
            &[_]i64{ -9223372036854775808, -8223372036854775800, 0, 9223372036854775807 },
            &[_]i128{ -170141183460469231731687303715884105728, -1, 2, 170141183460469231731687303715884105727 },
            &[_]u8{ 0, 200, 255 },
            &[_]u16{ 0, 65535 },
            &[_]u32{ 0, 4294967294, 4294967295 },
            &[_]u64{ 0, 18446744073709551615 },
            &[_]u128{ 0, 99999999999999999999998, 340282366920938463463374607431768211455 },
            &[_]f32{ -1.0, 3.44, 0.0, 99.9991 },
            &[_]f64{ -1.02, 9999.1303, 0.0, -8288133.11 },
            &[_]bool{ true, false, true, true, false },
            &[_][]const u8{ "hello", "world" },
            [2][]const u8{ "eadc5eb8-dd6b-4c55-9c9b-b19c76048c32", &[_]u8{ 204, 193, 82, 169, 150, 64, 52, 71, 92, 228, 173, 248, 223, 220, 70, 252 } },
            &[_]lib.Date{ .{ .year = 2023, .month = 5, .day = 10 }, .{ .year = 1901, .month = 2, .day = 3 } },
            &[_]lib.Time{ .{ .hour = 10, .min = 44, .sec = 23, .micros = 123456 }, .{ .hour = 20, .min = 2, .sec = 5, .micros = 2 } },
            &[_]i64{ 0, -1, 17135818900221234 },
            &[_]lib.Interval{ .{ .months = 3, .days = 7, .micros = 982810 }, .{ .months = 1, .days = 2, .micros = 3 } },
            &[_]f64{ 3.1234, -0.00002 },
        });
        try appender.appendRow(.{ 2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null });
        try appender.appendRow(.{
            3,
            &[_]?i8{ -1, null, 9 },
            &[_]?i16{ -32768, null, 32767 },
            &[_]?i32{ -200000000, null, 200000001 },
            &[_]?i64{ -40000000000000000, null, 4000000000000001 },
            &[_]?i128{ -999999999999999999999, null, 8888888888888888888 },
            &[_]?u8{ 254, null },
            &[_]?u16{ 65534, null },
            &[_]?u32{ null, 4294967255, 0 },
            &[_]?u64{ 1, null, null },
            &[_]?u128{ null, null, 0, null, null },
            &[_]?f32{ 0.0, null, 0.1 },
            &[_]?f64{ 0.9999, null, null },
            &[_]?bool{ false, null, true },
            &[_]?[]const u8{ "hello", null, "world", null },
            [_]?[]const u8{ "eadc5eb8-dd6b-4c55-9c9b-b19c76048c3d", null, "FFFFFFFF-dd6b-4c55-9c9b-aaaaaaaaaaaa", &[_]u8{ 2, 193, 82, 169, 150, 64, 52, 71, 92, 228, 173, 248, 223, 220, 70, 1 }, null },
            &[_]?lib.Date{ .{ .year = 2023, .month = 5, .day = 10 }, null },
            &[_]?lib.Time{ null, null, .{ .hour = 20, .min = 2, .sec = 5, .micros = 2 } },
            &[_]?i64{ 0, null, null, 27135818900221234 },
            &[_]?lib.Interval{ .{ .months = 3, .days = 7, .micros = 982810 }, null, null },
            &[_]?f64{ 3.1234, null },
        });
        try appender.flush();
    }

    {
        var row = (try conn.row("select * from applist where id = 1", .{})).?;
        defer row.deinit();

        try assertList(&[_]i8{ -128, 0, 100, 127 }, row.list(i8, 1).?);
        try assertList(&[_]i16{ -32768, 0, -299, 32767 }, row.list(i16, 2).?);
        try assertList(&[_]i32{ -2147483648, -4933, 0, 2147483647 }, row.list(i32, 3).?);
        try assertList(&[_]i64{ -9223372036854775808, -8223372036854775800, 0, 9223372036854775807 }, row.list(i64, 4).?);
        try assertList(&[_]i128{ -170141183460469231731687303715884105728, -1, 2, 170141183460469231731687303715884105727 }, row.list(i128, 5).?);
        try assertList(&[_]u8{ 0, 200, 255 }, row.list(u8, 6).?);
        try assertList(&[_]u16{ 0, 65535 }, row.list(u16, 7).?);
        try assertList(&[_]u32{ 0, 4294967294, 4294967295 }, row.list(u32, 8).?);
        try assertList(&[_]u64{ 0, 18446744073709551615 }, row.list(u64, 9).?);
        try assertList(&[_]u128{ 0, 99999999999999999999998, 340282366920938463463374607431768211455 }, row.list(u128, 10).?);
        try assertList(&[_]f32{ -1.0, 3.44, 0.0, 99.9991 }, row.list(f32, 11).?);
        try assertList(&[_]f64{ -1.02, 9999.1303, 0.0, -8288133.11 }, row.list(f64, 12).?);
        try assertList(&[_]bool{ true, false, true, true, false }, row.list(bool, 13).?);

        const list_texts = row.list([]u8, 14).?;
        try t.expectEqualStrings("hello", list_texts.get(0));
        try t.expectEqualStrings("world", list_texts.get(1));

        const list_uuids = row.list(lib.UUID, 15).?;
        try t.expectEqualStrings("eadc5eb8-dd6b-4c55-9c9b-b19c76048c32", &list_uuids.get(0));
        try t.expectEqualStrings("ccc152a9-9640-3447-5ce4-adf8dfdc46fc", &list_uuids.get(1));

        try assertList(&[_]lib.Date{ .{ .year = 2023, .month = 5, .day = 10 }, .{ .year = 1901, .month = 2, .day = 3 } }, row.list(lib.Date, 16).?);
        try assertList(&[_]lib.Time{ .{ .hour = 10, .min = 44, .sec = 23, .micros = 123456 }, .{ .hour = 20, .min = 2, .sec = 5, .micros = 2 } }, row.list(lib.Time, 17).?);

        // timestamp
        try assertList(&[_]i64{ 0, -1, 17135818900221234 }, row.list(i64, 18).?);
        try assertList(&[_]lib.Interval{ .{ .months = 3, .days = 7, .micros = 982810 }, .{ .months = 1, .days = 2, .micros = 3 } }, row.list(lib.Interval, 19).?);
        try assertList(&[_]f64{ 3.1234, -0.00002 }, row.list(f64, 20).?);
    }

    {
        var row = (try conn.row("select * from applist where id = 2", .{})).?;
        defer row.deinit();
        try t.expectEqual(null, row.list(?i8, 1));
        try t.expectEqual(null, row.list(?i16, 2));
        try t.expectEqual(null, row.list(?i32, 3));
        try t.expectEqual(null, row.list(?i64, 4));
        try t.expectEqual(null, row.list(?i128, 5));
        try t.expectEqual(null, row.list(?u8, 6));
        try t.expectEqual(null, row.list(?u16, 7));
        try t.expectEqual(null, row.list(?u32, 8));
        try t.expectEqual(null, row.list(?u64, 9));
        try t.expectEqual(null, row.list(?u128, 10));
        try t.expectEqual(null, row.list(?f32, 11));
        try t.expectEqual(null, row.list(?f64, 12));
        try t.expectEqual(null, row.list(?bool, 13));
        try t.expectEqual(null, row.list(?[]const u8, 14));
        try t.expectEqual(null, row.list(?lib.UUID, 15));
        try t.expectEqual(null, row.list(?lib.Date, 16));
        try t.expectEqual(null, row.list(?lib.Time, 17));
        try t.expectEqual(null, row.list(?i64, 18));
        try t.expectEqual(null, row.list(?i64, 19));
        try t.expectEqual(null, row.list(?f64, 20));
    }

    {
        var row = (try conn.row("select * from applist where id = 3", .{})).?;
        defer row.deinit();

        try assertList(&[_]?i8{ -1, null, 9 }, row.list(?i8, 1).?);
        try assertList(&[_]?i16{ -32768, null, 32767 }, row.list(?i16, 2).?);
        try assertList(&[_]?i32{ -200000000, null, 200000001 }, row.list(?i32, 3).?);
        try assertList(&[_]?i64{ -40000000000000000, null, 4000000000000001 }, row.list(?i64, 4).?);
        try assertList(&[_]?i128{ -999999999999999999999, null, 8888888888888888888 }, row.list(?i128, 5).?);
        try assertList(&[_]?u8{ 254, null }, row.list(?u8, 6).?);
        try assertList(&[_]?u16{ 65534, null }, row.list(?u16, 7).?);
        try assertList(&[_]?u32{ null, 4294967255, 0 }, row.list(?u32, 8).?);
        try assertList(&[_]?u64{ 1, null, null }, row.list(?u64, 9).?);
        try assertList(&[_]?u128{ null, null, 0, null, null }, row.list(?u128, 10).?);
        try assertList(&[_]?f32{ 0.0, null, 0.1 }, row.list(?f32, 11).?);
        try assertList(&[_]?f64{ 0.9999, null, null }, row.list(?f64, 12).?);
        try assertList(&[_]?bool{ false, null, true }, row.list(?bool, 13).?);

        const list_texts = row.list(?[]u8, 14).?;
        try t.expectEqualStrings("hello", list_texts.get(0).?);
        try t.expectEqual(null, list_texts.get(1));
        try t.expectEqualStrings("world", list_texts.get(2).?);
        try t.expectEqual(null, list_texts.get(3));

        const list_uuids = row.list(?lib.UUID, 15).?;
        try t.expectEqualStrings("eadc5eb8-dd6b-4c55-9c9b-b19c76048c3d", &(list_uuids.get(0).?));
        try t.expectEqual(null, list_uuids.get(1));
        try t.expectEqualStrings("ffffffff-dd6b-4c55-9c9b-aaaaaaaaaaaa", &(list_uuids.get(2).?));
        try t.expectEqualStrings("02c152a9-9640-3447-5ce4-adf8dfdc4601", &(list_uuids.get(3).?));
        try t.expectEqual(null, list_uuids.get(4));

        try assertList(&[_]?lib.Date{ .{ .year = 2023, .month = 5, .day = 10 }, null }, row.list(?lib.Date, 16).?);
        try assertList(&[_]?lib.Time{ null, null, .{ .hour = 20, .min = 2, .sec = 5, .micros = 2 } }, row.list(?lib.Time, 17).?);
        try assertList(&[_]?i64{ 0, null, null, 27135818900221234 }, row.list(?i64, 18).?);
        try assertList(&[_]?lib.Interval{ .{ .months = 3, .days = 7, .micros = 982810 }, null, null }, row.list(?lib.Interval, 19).?);
        try assertList(&[_]?f64{ 3.1234, null }, row.list(?f64, 20).?);
    }
}

test "Appender: list multiple" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table applist (id integer, data integer[])", .{});

    {
        var appender = try conn.appender(null, "applist");
        defer appender.deinit();

        var i: i32 = 0;
        while (i < 10) : (i += 1) {
            try appender.appendRow(.{ i, &[_]i32{ i, i + 1, i + 2 } });
        }
        try appender.flush();
    }

    var rows = try conn.query("select data from applist order by id", .{});
    defer rows.deinit();

    var i: i32 = 0;
    while (try rows.next()) |row| {
        try assertList(&[_]i32{ i, i + 1, i + 2 }, row.list(i32, 0).?);
        i += 1;
    }
    try t.expectEqual(10, i);
}

test "Appender: appendListMap int" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table applist (id integer, data integer[])", .{});

    {
        var appender = try conn.appender(null, "applist");
        defer appender.deinit();

        appender.beginRow();
        try appender.appendValue(1, 0);
        try appender.appendListMap([]const u8, i32, 1, &[_][]const u8{ "123", "0", "999" }, testAtoi);
        try appender.endRow();
        try appender.flush();

        var row = (try conn.row("select data from applist where id = 1", .{})).?;
        defer row.deinit();
        try assertList(&[_]?i32{ 123, null, 999 }, row.list(?i32, 0).?);
    }
}

test "Appender: appendListMap text" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table applist (id integer, data varchar[])", .{});

    {
        var appender = try conn.appender(null, "applist");
        defer appender.deinit();

        appender.beginRow();
        try appender.appendValue(1, 0);
        try appender.appendListMap(i64, []const u8, 1, &[_]i64{ -3929, 999, 0 }, testItoa);
        try appender.endRow();
        try appender.flush();

        var row = (try conn.row("select data from applist where id = 1", .{})).?;
        defer row.deinit();
        const list_texts = row.list(?[]u8, 0).?;
        try t.expectEqualStrings("-3929", list_texts.get(0).?);
        try t.expectEqualStrings("999", list_texts.get(1).?);
        try t.expectEqual(null, list_texts.get(2));
    }
}

test "Appender: appendListMap date" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table applist (id integer, data date[])", .{});

    {
        var appender = try conn.appender(null, "applist");
        defer appender.deinit();

        appender.beginRow();
        try appender.appendValue(1, 0);
        try appender.appendListMap(u8, Date, 1, &[_]u8{ 1, 0, 2 }, testMapDate);
        try appender.endRow();
        try appender.flush();

        var row = (try conn.row("select data from applist where id = 1", .{})).?;
        defer row.deinit();
        try assertList(&[_]?Date{ .{ .year = 2005, .month = 8, .day = 10 }, null, .{ .year = -8, .month = 1, .day = 30 } }, row.list(?Date, 0).?);
    }
}

test "Appender: appendListMap time" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table applist (id integer, data time[])", .{});

    {
        var appender = try conn.appender(null, "applist");
        defer appender.deinit();

        appender.beginRow();
        try appender.appendValue(1, 0);
        try appender.appendListMap(u8, Time, 1, &[_]u8{ 1, 0, 2 }, testMapTime);
        try appender.endRow();
        try appender.flush();

        var row = (try conn.row("select data from applist where id = 1", .{})).?;
        defer row.deinit();
        try assertList(&[_]?Time{
            .{ .hour = 10, .min = 56, .sec = 21, .micros = 123456 },
            null,
            .{ .hour = 22, .min = 8, .sec = 1, .micros = 0 },
        }, row.list(?Time, 0).?);
    }
}

test "Appender: appendListMap timestamp" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table applist (id integer, data timestamptz[])", .{});

    {
        var appender = try conn.appender(null, "applist");
        defer appender.deinit();

        appender.beginRow();
        try appender.appendValue(1, 0);
        try appender.appendListMap(u8, i64, 1, &[_]u8{ 1, 0, 2 }, testMapTimeStamp);
        try appender.endRow();
        try appender.flush();

        var row = (try conn.row("select data from applist where id = 1", .{})).?;
        defer row.deinit();
        try assertList(&[_]?i64{ 1715854289547193, null, -2775838248400000 }, row.list(?i64, 0).?);
    }
}

test "Appender: appendListMap UUID" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table applist (id integer, data uuid[])", .{});

    {
        var appender = try conn.appender(null, "applist");
        defer appender.deinit();

        appender.beginRow();
        try appender.appendValue(392, 0);
        try appender.appendListMap(u8, []const u8, 1, &[_]u8{ 2, 1, 0 }, testMapUUID);
        try appender.endRow();
        try appender.flush();

        var row = (try conn.row("select data from applist where id = 392", .{})).?;
        defer row.deinit();
        const list = row.list(?UUID, 0).?;
        try t.expectEqualStrings("e188523a-9650-41ef-8cb3-d7e3cd4833b9", &(list.get(0).?));
        try t.expectEqualStrings("61cdea17-71fd-44f2-898d-6756d6b63a97", &(list.get(1).?));
        try t.expectEqual(null, list.get(2));
    }

    // as i128s
    {
        var appender = try conn.appender(null, "applist");
        defer appender.deinit();

        appender.beginRow();
        try appender.appendValue(991, 0);
        try appender.appendListMap(u8, i128, 1, &[_]u8{ 0, 1, 2 }, testMapUUID2);
        try appender.endRow();
        try appender.flush();

        var row = (try conn.row("select data from applist where id = 991", .{})).?;
        defer row.deinit();
        const list = row.list(?UUID, 0).?;
        try t.expectEqual(null, list.get(0));
        try t.expectEqualStrings("db052c3d-eb85-4259-b59c-532d47f185a1", &(list.get(1).?));
        try t.expectEqualStrings("c88b0ec1-fa66-40fc-8eb6-09867e9f48e2", &(list.get(2).?));
    }
}

test "Appender: incomplete row" {
    const db = try DB.init(t.allocator, ":memory:", .{});
    defer db.deinit();

    var conn = try db.conn();
    defer conn.deinit();

    _ = try conn.exec("create table app_x (id integer)", .{});

    {
        var appender = try conn.appender(null, "app_x");
        defer appender.deinit();

        appender.beginRow();
        try appender.appendValue(1, 0);
        try appender.endRow();

        appender.beginRow();
        try appender.appendValue(2, 0);
        try appender.endRow();

        appender.beginRow();
        try appender.appendValue(3, 0);

        // we fllush without finishing the above row. That's fine, the row should
        // be abandoned.
        try appender.flush();
    }

    var rows = try conn.query("select id from app_x order by id", .{});
    defer rows.deinit();

    var i: i32 = 0;
    while (try rows.next()) |row| {
        try t.expectEqual(i + 1, row.get(i32, 0));
        i += 1;
    }
    try t.expectEqual(2, i);
}

fn testAtoi(str: []const u8) ?i32 {
    if (std.mem.eql(u8, str, "123")) return 123; // (•_•)
    if (std.mem.eql(u8, str, "0")) return null; // ( •_•)>⌐■-■
    if (std.mem.eql(u8, str, "999")) return 999; // (⌐■_■)
    unreachable;
}

fn testItoa(value: i64) ?[]const u8 {
    if (value == 0) return null; // (•_•)
    if (value == 999) return "999"; // ( •_•)>⌐■-■
    if (value == -3929) return "-3929"; // (⌐■_■)
    unreachable;
}

fn testMapDate(value: u8) ?Date {
    if (value == 0) return null;
    if (value == 1) return Date{ .year = 2005, .month = 8, .day = 10 };
    if (value == 2) return Date{ .year = -8, .month = 1, .day = 30 };
    unreachable;
}

fn testMapTime(value: u8) ?Time {
    if (value == 0) return null;
    if (value == 1) return Time{ .hour = 10, .min = 56, .sec = 21, .micros = 123456 };
    if (value == 2) return Time{ .hour = 22, .min = 8, .sec = 1, .micros = 0 };
    unreachable;
}

fn testMapTimeStamp(value: u8) ?i64 {
    if (value == 0) return null;
    if (value == 1) return 1715854289547193;
    if (value == 2) return -2775838248400000;
    unreachable;
}

fn testMapUUID(value: u8) ?[]const u8 {
    if (value == 0) return null;
    if (value == 1) return "61cdea17-71fd-44f2-898d-6756d6b63a97";
    if (value == 2) return "e188523a-9650-41ef-8cb3-d7e3cd4833b9";
    unreachable;
}

fn testMapUUID2(value: u8) ?i128 {
    if (value == 0) return null;
    if (value == 1) return 120986606432550570389071696710866142625;
    if (value == 2) return 96426444282114970045097725006964541666;
    unreachable;
}

fn assertList(expected: anytype, actual: anytype) !void {
    try t.expectEqual(expected.len, actual.len);
    for (expected, 0..) |e, i| {
        try t.expectEqual(e, actual.get(i));
    }
}
