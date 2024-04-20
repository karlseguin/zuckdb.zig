# Zig driver for DuckDB.

## Quick Example
```zig
const db = try zuckdb.DB.init(allocator, "/tmp/db.duck", .{});
defer db.deinit();

var conn = try db.conn();
defer conn.deinit();

// for insert/update/delete returns the # changed rows
// returns 0 for other statements
_ = try conn.exec("create table users(id int)", .{});

var rows = try conn.query("select * from users", .{});
defer rows.deinit();

while (try rows.next()) |row| {
    // get the 0th column of the current row
    const id = row.get(i32, 0);
    std.debug.print("The id is: {d}", .{id});
}
```

Any non-primitive value that you get from the `row` are valid only until the next call to `next` or `deinit`.

## Install
This library is tested with DuckDB 0.10.1.

1) Add into `dependencies` at `build.zig.zon`:
```zig
.dependencies = .{
    ...
    .zuckdb = .{
        .url = "git+https://github.com/karlseguin/zuckdb.zig#master",
        .hash = "{{ actual_hash string, remove this line before 'zig build' to get actual hash }}",
    },
},
```

2) Download the libduckdb from the <a href="https://duckdb.org/docs/installation/index.html?version=latest&environment=cplusplus&installer=binary">DuckDB download page</a>. 

3) Place the `duckdb.h` file and the `libduckdb.so` (linux) or `libduckdb.dylib` (mac) in your project's `lib` folder.

4) Add this in `build.zig`:

```zig
const zuckdb = b.dependency("zuckdb", .{
    .target = target,
    .optimize = optimize,
}).module("zuckdb");

// tell zuckdb.zig where to find the duckdb.h file
zuckdb.addIncludePath(LazyPath.relative("lib/"));

// Your app's program
const exe = b.addExecutable(.{
    .name = "run",
    .root_source_file = .{ .path = "test.zig" },
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zuckdb", zuckdb);

// link to libduckdb
exe.linkSystemLibrary("duckdb"); 

// tell the linker where to find libduckdb.so (linux) or libduckdb.dylib (macos)
exe.addLibraryPath(LazyPath.relative("lib/"));

// add other imports
```

### Static Linking
It's also possible to statically link DuckDB. In order to do this, you must build DuckDB yourself, in order to [compile it using Zig C++](https://github.com/ziglang/zig/issues/9832#issuecomment-926832810) and using the [bundle-library](https://github.com/duckdb/duckdb/issues/9475) target

```
git clone -b 0.10.1 --single-branch https://github.com/duckdb/duckdb.git
cd duckdb
export CXX="zig c++"
DUCKDB_EXTENSIONS='json' make bundle-library
```

When this finished (it will take several minutes), you can copy `build/release/libduckdb_bundle.a` and `src/include/duckdb.h` to your project's `lib` folder. Rename `libduckdb_bundle.a` to `libduckdb.a`.

Finally, Add the following to your `build.zig`:

```zig
exe.linkSystemLibrary("duckdb");
exe.linkSystemLibrary("stdc++");
exe.addLibraryPath(LazyPath.relative("lib/"));
```

# DB
The `DB` is used to initialize the database, open connections and, optionally, create a connection pool.

## init
Creates or opens the database.

```zig
// can use the special path ":memory:" for an in-memory database
const db = try DB.init(t.allocator, "/tmp/db.duckdb", .{});
defer db.deinit();
```

The 3rd parameter is for options. The available options, with their default, are:

* `access_mode` - Sets the `access_mode` DuckDB configuration. Defaults to `.automatic`. Valid options are: `.automatic`, `.read_only` or `.read_write`.
* `enable_external_access` - Sets the `enable_external_access` DuckDB configuration. Defaults to `true`.

## initWithErr
Same as `init`, but takes a 4th output parameter. On open failure, the output parameter will be set to the error message. This parameter must be freed if set.

```zig
var open_err: ?[]u8 = null;
const db = DB.initWithErr(allocator, "/does/not/exist", .{}, &open_err) catch |err| {
    if (err == error.OpenDB) {
        defer allocator.free(open_err.?);
        std.debug.print("DB open: {}", .{open_err.?});
    }
    return err;
};
```

## deinit
Closes the database.

## conn
Returns a new [connection](#conn-1) object. 

```zig
var conn = try db.conn();
defer conn.deinit();
...
```

## pool
Initializes a [pool](#pool-1) of connections to the DB. 

```zig
var pool = try db.pool(.{.size = 2});

// the pool owns the `db`, so pool.deinit will call `db.deinit`.
defer pool.deinit();

var conn = try pool.acquire();
defer pool.release(conn);
```

The `pool` method takes an options parameter:
* `size: usize` - The number of connections to keep in the pool. Defaults to `5`
* `timeout: u64` - The time, in milliseconds, to wait for a connetion to be available when calling `pool.acquire()`. Defaults to `10_000`.
* `on_connection: ?*const fn(conn: *Conn) anyerror!void` - The function to call when the pool first establishes the connection. Defaults to `null`.
* `on_first_connection: ?*const fn(conn: *Conn) anyerror!void` - The function to call on the first connection opened by the pool. Defaults to `null`.

# Conn

## query
Use `conn.query(sql, args) !Rows` to query the database and return a `zuckdb.Rows` which can be iterated. You must call `deinit` on the returned rows.

```zig
var rows = try conn.query("select * from users where power > $1", .{9000});
defer rows.deinit();
while (try rows.next()) |row| {
    // ...
}
```

## exec
`conn.exec(sql, args) !usize` is a wrapper around `query` which returns the number of affected rows for insert, updates or deletes.

## row
`conn.row(sql, args) !?OwningRow` is a wrapper around `query` which returns a single optional row. You must call `deinit` on the returned row:

```zig
var row = (try conn.query("select * from users where id = $1", .{22})) orelse return null;;
defer row.deinit();
// ...
```

## begin/commit/rollback
The `conn.begin()`, `conn.commit()` and `conn.rollback()` calls are wrappers around `exec`, e.g.: `conn.exec("begin", .{})`.

## prepare
`conn.prepare(sql) !Stmt` prepares the given SQL and returns a `zuckdb.Stmt`. You should prefer using `query`, `exec` or `row` which wrap `prepare` and then call `stmt.bind(values)` and finally `stmt.execute()`.

## err
If a method of `conn` returns `error.DuckDBError`, `conn.err` will be set:

```zig
var rows = conn.query("....", .{}) catch |err| {
  if (err == error.DuckDBError) {
    if (conn.err) |derr| {
      std.log.err("DuckDB {s}\n", .{derr});
    }
  }
  return err;
}
```

In the above snippet, it's possible to skip the `if (err == error.DuckDBError)`check, but in that case conn.err could be set from some previous command (conn.err is always reset when acquired from the pool).

## release
`conn.release()` will release the connection back to the pool. This does nothing if the connection did not come from the pool (i.e. `pool.acquire()`). This is the same as calling `pool.release(conn)`.

# Rows
The `rows` returned from `conn.query` exposes the following methods:

* `count()` - the number of rows in the result
* `changed()` - the number of updated/deleted/inserted rows
* `columnName(i: usize)` - the column name at position `i` in a result
* `deinit()` - must be called to free resources associated with the result
* `next() !?Row` - returns the next row

The most important method on `rows` is `next()` which is used to iterate the results. `next()` is a typical Zig iterator and returns a `?Row` which will be null when no more rows exist to be iterated.

# Row

## get
`Row` exposes a `get(T, index) T` function. This function trusts you! If you ask for an <code>i32</code> the library will crash if the column is not an <code>int4</code>. Similarly, if the value can be null, you must use the optional type, e.g. <code>?i32</code>.

The supported types for `get`, are:
* `[]u8`, 
* `[]const u8`
* `i8`
* `i16`
* `i32`
* `i64`
* `i128`
* `u8`
* `u16`
* `u32`
* `u64`
* `f32`
* `f64`
* `bool`
* `zuckdb.Date`
* `zuckdb.Time`
* `zuckdb.Interval`
* `zuckdb.UUID`
* `zudkdb.Enum`

Optional version of the above are all supported **and must be used** if it's possible the value is null.

String values and enums are only valid until the next call to `next()` or `deinit`. You must dupe the values if you want them to outlive the row.

## list
`Row` exposes a `list` method which behaves similar to `get` but returns a `zuckdb.List(T)`.

```zig
const row = (try conn.row("select [1, 32, 99, null, -4]::int[]", .{})) orelse unreachable;
defer row.deinit();

const list = row.list(?i32, 0).?;
try t.expectEqual(5, list.len);
try t.expectEqual(1, list.get(0).?);
try t.expectEqual(32, list.get(1).?);
try t.expectEqual(99, list.get(2).?);
try t.expectEqual(null, list.get(3));
try t.expectEqual(-4, list.get(4).?);
```

`list()` always returns a nullable, i.e. `?zuckdb.List(T)`. Besides the `len` field, `get` is used on the provided list to return a value at a specific index. `row.list(T, col).get(idx)` works with any of the types supported by `row.get(col)`.

a `List(T)` also has a `alloc(allocator: Allocator) ![]T` method. This will allocate a `[]T` and fill it with the list values. It is the caller's responsibility to free the returned slice.

Alternatively, `fill(into: []T) void` can be used used to populate `into` with items from the list. This will fill `@min(into.len, list.len)` values.

# zuckdb.Enum
The `zuckdb.Enum` is a special type which exposes two functions: `raw() [*c]const u8` and `rowCache() ![]const u8`.

`raw()` directly returns the DuckDB enum string value. If you want to turn this into a `[]const u8`, you'll need to wrap it in `std.mem.span`. The value returned by `raw()` is only valid until the next iteration.

`rowCache()` takes the result of `raw()`, and dupes it, giving ownership to the Rows. Thus, the string returned by `rowCache()` outlives the current row iteration and is valid until `rows.deinit()` is called. Essentially, it is an interned string representation of the enum value (which DuckDB internally represents as an integer).

# Pool
The `zuckdb.Pool` is a thread-safe connection pool:

```zig
const db = try zuckdb.DB.init(allocator, "/tmp/duckdb.zig.test", .{});
var pool = db.pool(.{
    .size = 2,
    .on_connection = &connInit,
    .on_first_connection = &poolInit,
});
defer pool.deinit();

var conn = try pool.acquire();
defer conn.release();
```

The Pool takes ownership of the DB object, thus `db.deinit` does not need to be called The `on_connection` and `on_first_connection` are optional callbacks. They both have the same signature:

```zig
?*const fn(conn: *Conn) anyerror!void
```

If both are specific, the first initialized connection will first be passed to `on_first_connection` and then to `on_connection`.

## newConn() !Conn
Besides using `acquire()` to get a `!*Conn` from the pool, it's possible to create a new connection detached from the pool using `pool.newConn()`.  This is the same as calling `db.conn()` but, on the pool. Again, this connection will not be part of the pool and `release()` should not be called on it (but `deinit()` should).

# Query Optimizations
In very tight loops, performance might be improved by providing a stack-based state for the query logic to use. The `query` and `row` functions all have a `WithState` alternative, e.g.: `queryWithState`. These functions take 1 additional "query state" parameter:

```zig
var state = zuckdb.StaticState(2){};
var rows = try conn.queryWithState(SQL, .{ARGS}, &state);
// use rows normally
```

The value passed to `zuckdb.StaticState` is the number of columns returned by the query. The `state` must remain valid until `rows.deinit()` is called.

# Appender
The fastest way to insert a large amount of data is to use the appender:

```zig
// the first parameter is the schema, or null to use the default schema
var appender = try conn.appender(null, "my_table");
defer appender.deinit();

for (...) {
    try appender.appendRow(.{"over", 9001, true});
}
// The appender auto-flushes, but it should be called once at the end.
try appender.flush();
```

The order of the values used in `appendRow` is the order of the columns as they are defined in the table (e.g. the order that `describe $table` returns).

## Appender per-column append
The `appender.appendRow` function depends on the fact that you have comptime knowledge of the underlying table. If you are dealing with dynamic (e.g. user-defined) schemas, that won't always be the case. Instead, use the more explicit `beginRow()`, `appendValue()` and `endRow()` methods. When using this api. `endRow` will call `flush()` as needed, so using this API doesn't require a final `flush()`.

```zig
for (...) {
    appender.beginRow();
    appender.appendValue("over", 0);
    appender.appendValue(9001, 1);
    appender.appendValue(true, 2);
    try appender.endRow();
}
```

The `appendRow()` call translates to the above, more explicit, dance.

## Appender Type Support
The appender writes directly to the underlying storage and thus cannot leverage default column values. `appendRow` asserts that the # of values matches the number of columns. However, when using the explicit `beginRow` + `appendValue` + `endRow`, you must make sure to append a value for each column, else the behavior is undefined. 

Enums aren't supporting, due to [limitations in the DuckDB C API](https://github.com/duckdb/duckdb/pull/11704).

Decimals are supported, but be careful! When appending a float, the value will truncated to the decimal place specified by the scale of the column (i.e. a decimal(8, 3) will have the float truncated with 3 decimal places). When appending an int, the library assumes that you have already converted the decimal to the DuckDB internal representation. While surprising, this provides callers with precise control.

When dealing with ints, floats and decimals, appending a single value tends to be flexible. In other words, you can append an `i64` to a `tinyint` column, so long as the value fits (i.e. there's a runtime check). However, when dealing with lists (e.g. `integer[]`), the exact type is required. Thus, only a `[]u16` can be bound to a `usmallint[]` column. `decimal[]` can bind to a `[]i64`, `[]f32` or `[]f64`.

List columns support null values, and thus can be bound to either a `[]const T` or a `[]const ?T`.


## Appender Error
If any of the appender methods return an error, you can see if the optional `appender.err` has an error description. This is a `?[]const u8` field. On error, you **should not** assume that this value is set, there are error cases where DuckDB doesn't provide an error description.
