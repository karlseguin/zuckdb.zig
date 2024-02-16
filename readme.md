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


const rows = try conn.query("select * from users", .{});
defer rows.deinit();

while (try rows.next()) |row| {
    // get the 0th column of the current row
    const id = row.get(i32, 0);
    std.debug.print("The id is: {d}", .{id});
}
```

Any non-primitive value that you get from the `row` are valid only until the next call to `next` or `deinit`.

## Install
This library is tested with DuckDB 0.10.0.

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

# DB
The `DB` is used to intialize the database, open connections and, optionally, create a connection pool.

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
Returns a new connection object. 

```zig
var conn = try db.conn();
defer conn.deinit();
...
```

## pool
Initializes a pool of connections to the DB.

```zig
var pool = db.pool(.{.size = 2});
defer pool.deinit();

var conn = try pool.acquire();
defer pool.release(conn);
```

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
const rows = conn.query("....", .{}) catch |err| {
  if (err == error.DuckDBError) {
    if (conn.err) |derr| {
      std.log.err("DuckDB {s}\n", .{derr});
    }
  }
  return err;
}
```

In the above snippet, it's possible to skip the `if (err == error.DuckDBError)`check, but in that case conn.err could be set from some previous command (conn.err is always reset when acquired from the pool).

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
`Row` exposes a `get(T, index) T` function. This function trusts you! If you ask for an <code>i32</code> the library will crash if the column is not an <code>int4</code>. Similarl, if the value can be null, you must use the optional type, e.g. <code>?i32</code>.

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

String values and enums are only valid until the next call to `next()` or `deinit`. You must dupe the values if you want them to outlive the row.s

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

`list()` always returns a nullable, i.e. `?zuckdb.List(T)`. Besides the `len` field, `get` is used on the provided list to return a value at a specific index. `row.list(T, col).get(idz)` works with any of the types supported by `row.get(col)`.

a `List(T)` also has a `alloc(allocator: Allocator) ![]T` method. This will allocate a `[]T` and fill it with the list values. It is the caller's responsibility to free the returned slice.

Alternatively, `fill(into: []T) void` can be used used to populate `into` with items from the list. This will fill `@min(into.len, list.len)` values.

# zuckdb.Enum
The `zuckdb.Enum` is a special type which exposes two functions: `raw() [*c]const u8` and `rowCache() ![]const u8`.

`raw()` returns a C string directly to the DuckDB enum string value. If you want to turn this into a `[]const u8`, you'll need to wrap it in `std.mem.span`. The value returned `raw` is only valid untli the next iteration.

`rowCache()` takes the result of `raw()`, and dupes it, giving ownerning to the Rows. Thus, the string returned by `rowCache()` outlives the current row iteration and is valid until `rows.deinit()` is called. Essentially, it is an interned string representation fo the enum value (which DuckDB internally represents as an integer).


## Pool
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
defer pool.release(conn);
```

The Pool takes ownership of the DB object, thus `db.deinit` does not need to be called.The `on_connection` and `on_first_connection` are optional callbacks. They both have the same signature:

```zig
?*const fn(conn: *Conn) anyerror!void
```

If both are specific, the first initialized connection will first be passed to `on_first_connection` and then to `on_connection`.

# Query Optimizations
In very tight loops, performance might be improved by providing a stack-based state for the query logic to use. The `query` and `row` functions all have a `WithState` alternative, e.g.: `queryWithState`. These functions take 1 additional "query state" parameter:

```zig
var state = zuckdb.StaticState(2){};
var rows = try conn.queryWithState(SQL, .{ARGS}, &state);
// use rows normally
```

The value passed to `zuckdb.StaticState` is the number of columns returned by the query. The `state` must remain valid while the query is used.
