Zig driver for DuckDB.

# Quick Example
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
1) Add into `dependencies` at `build.zig.zon`:
```zig
.dependencies = .{
    ...
    .zqlite = .{
        .url = "git+https://github.com/karlseguin/zqlite.zig#master",
        .hash = {{ actual_hash string, remove this line before 'zig build' to get actual hash }},
    },
},
```
2) Add this in `build.zig`:
```zig
const zqlite = b.dependency("zqlite", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zqlite", zqlite.module("pg"));
```

## Errors

### DB.init
Rather than calling `DB.init`, you can call `DB.initWithErr` and pass an optional string pointer. If the function returns `error.OpenDB` then the error string will be set. You must free this string when done:

```zig
var err: ?[]u8 = null;
const db = DB.initWithErr(allocator, "/tmp/does/not/exist/zuckdb", .{}, &err) catch |err| {
    if (err == error.OpenDB) {
        defer allocator.free(err.?);
        std.debug.print("DB open: {}", .{err.?});
    }
    return err;
};
try t.expectEqualStrings("IO Error: Cannot open file \"/tmp/does/not/exist/zuckdb\": No such file or directory", err.?);
t.allocator.free(err.?);


A call that returns an `error.DuckDBError` will have the `conn.err` field set to an error description:

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

In the above snippet, it's possible to skip the if (err == error.DuckDBError) check, but in that case conn.err could be set from some previous command (conn.err is always reset when acquired from the pool).

If error.DuckDBError is returned from a non-connection object, like a query result, the associated connection will have its conn.err set. In other words, conn.err is the only thing you ever have to check.

## Rows and Row
The `rows` returned from `conn.squery` exposes the following methods:

* `count()` - the number of rows in the result
* `changed()` - the number of updated/deleted/inserted rows
* `columnName(i: usize)` - the column name at position `i` in a result

The most important method on `rows` is `next()` which is used to iterate the results. `next()` is a typical Zig iterator and returns a `?Row` which will be null when no more rows exist to be iterated.

`Row` exposes a `get(T, index) ?T`, `getList(index) ?List` and `getEnum(index) !?[]const u8` function.

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

There are a few important notes. First calling `get` with the wrong type will result in `null` being returned. Second, `get(T, i)` usually returns a `?T`. The only exception to this is `get([]u8, i)` which returns a `[]const u8` - I just didn't want to type `get([]const u8)` all the time. Strings returned by get are only valid until the next call to `next()` or `rows.deinit()`.

`row.getList(i)` is used to fetch a list from duckdb. It returns a type that exposes a `len` and `get(T, i) ?T` function:


```zig
const tags = row.getList(3) orelse unreachable; // TODO handle null properly
for (0..tags.len) |i| {
    const tag = tags.get([]u8, i);
    // get returns a nullable, so above, tag could be null
}
```

`row.getEnum(i)` returns the string representation of an enum. Unlike strings returned from `row.get([]u8, i)`, the returned string is valid until `rows.deinit()` (the enum string value is interned within the `rows`, thus it survives calls to `rows.next()`).

```zig
const category = (try row.getEnum(3)) orelse "default";
```

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

var conn = pool.acquire();
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
