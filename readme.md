Zig driver for DuckDB.

# Quick Example
```zig
const db = switch (zuckdb.DB.init(allocator, "/tmp/db.duck", .{})) {
    .ok => |db| db,
    .err => |err| {
        std.debug.print("Failed to open DB: {s}\n", .{err.desc});
        err.deinit();
        return error.DBOpenFailure;
    }
}
defer db.deinit();

const conn = try db.conn();
defer conn.deinit();

{
    // exec takes a null terminated string and does not support parameters
    // it is micro-optimized for simple cases.
    try conn.exec("create table users(id int)");
}

{
    const rows = switch (conn.query("insert into users (id) values ($1)", .{1})) {
        ok => |rows| rows,
        err => |err| => {
            std.debug.print("failed to insert: {s}\n", .{err.desc});
            // Important to note that even the error needs to be deinit'd
            err.deinit();
            return error.Failure;
        },
    }
    defer rows.deinit();

    // Since this is an insert, only rows.changed() is useful:
    std.debug.print("inserted {d} row(s)\n", .{rows.rowsChanged()});
}


{
    const rows = switch (conn.query("select * from users", .{});) {
        ok => |rows| rows,
        err => |err| => {
            std.debug.print("failed to select: {s}\n", .{err.desc});
            err.deinit();
            return error.Failure;
        },
    }
    defer rows.deinit();
    while (try rows.next()) |row| {
        // get the 0th column of the current row
        const id = row.get(i32, 0);
        std.debug.print("The id is: {d}", .{id});
    }
}
```

When fetching data from a `row`, data is only valid until the next call to `next` or `deinit`:

```zig
// assume we have rows from a "select name from users";

defer rows.deinit();

// we'll collect the names in here
const names = std.ArrayList([]const u8).init(allocator);

while (rows.next()) |row| {
    // name is not null
    const name = row.get([]u8, 0).?; 

    // we need to dupe the string value to own it beyond this block
    try names.append(try allocator.dupe(u8, name));
}
```


## Query and Exec

`query` and `exec` are the two primary functions used to run queries. The `queryZ` and `execZ` versions are optimized when the 1st parameter (the sql string) is known to be null-terminated (so you should favor using `execZ` or `queryZ` if you have a static SQL string). `exec` and `execZ` do not accept SQL parameters and only returns an `!void` (i.e. no detailed message, no rows), but will run slightly faster.

You'll notice that the library does not use idiomatic error sets, but rather Rust-like results. This was done so that the error message could be exposed. As a consequence, you'll also notice that `deinit` must be called on BOTH the `ok` and `err` values. There are two options to deal with this. In the above example, `deinit` is called in both cases:

```zig
const rows = switch(conn.query("...", .{})) {
    .ok => |rows| rows,
    .err => {
        std.debug.print("{s}\n", .{err.desc});
        // err result is freed here, _after_ err.desc is used
        err.deinit();
        return error.Fail;
    }
}
// ok result is freed here
defer rows.deinit()
```

Alternatively, `deinit` can be called on the result itself:

```zig
const result = conn.query("...", .{});
// CALLED HERE
defer result.deinit();

const rows = switch() {
    .ok => |rows| rows,
    .err => {
        std.debug.print("{s}\n", .{err.desc});
        return error.Fail;
    }
}

// should NOT call rows.deinit()
```

## Rows and Row
The `rows` returned from the `ok` case of a `query` exposes the following methods:

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
const db = zuckdb.DB.init(allocator, "/tmp/duckdb.zig.test", .{}).ok;
var pool = db.pool(.{
    .size = 2,
    .on_connection = &connInit,
    .on_first_connection = &poolInit,
}).ok;
defer pool.deinit();

const conn = pool.acquire();
defer pool.release(conn);
```

The Pool takes ownership of the DB object, thus `db.deinit` does not need to be called. In the above code, the DB result and Pool result are dangerously unwrapped directly into `ok` - this will panic if either returns an `.err`.

The `on_connection` and `on_first_connection` are optional callbacks. They both have the same signature:

```zig
?*const fn(conn: Conn) anyerror!void
```

If both are specific, the first initialized connection will first be passed to `on_first_connection` and then to `on_connection`.

# Query Optimizations
In very tight loops, performance might be improved by providing a stack-based state for the query logic to use. The `query`, `queryZ`, `row`, `rowZ`, `queryCache` and `queryCacheZ` functions all have a `WithState` alternative, e.g.: `queryZWithState`. These functions take 1 additional "query state" parameter:

```zig
var state = zuckdb.StaticState(2){};
var query_result = conn.queryCacheWithState(SQL, .{ARGS}, &state);
// use query_result normally
```

The value passed to `zuckdb.StaticState` is the number of columns returned by the query. The `state` must remain valid while the query is used.
