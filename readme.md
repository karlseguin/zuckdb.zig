Zig driver for DuckDB.

This API mixes idiomatic Zig error handling with a Result-based approach (think Rust). This was done for two reasons. The first is to expose the DuckDB error message for various actions (e.g. trying to execute an invalid SQL). The second is that, even in error cases, DuckDB can retain memory, and thus even the error-case requires a deinit. In other words, a Result is used whenever data (error message) and/or behavior (deinit) is needed in the error case.

# Quick Example
```zig
const db = try zuckdb.DB.init(allocator, "/tmp/db.duck");
defer db.deinit();

const conn = db.open();
defer conn.deinit();

{
    // execZ takes a null terminated string and does not support parameters
    // it is micro-optimized for simple cases.
    
    try conn.execZ("create table users(id int)");
}

{
    // queryZ takes a null terminated string and supports parameters. Instead
    // of an error union it returns a tagged-union with `ok` and `err`.
    // deinit must be called on both the success (ok) and error (err) return value
    // query is the same but takes a []const u8
    const rows = switch (conn.queryZ("insert into users (id) values ($1)", .{1})) {
        ok => |rows| rows,
        err => |err| => {
            // Important to note that even the error needs to be deinit'd
            defer err.deinit();
            std.debug.print("failed to insert: {s}\n", .{err.desc});
            return error.Failure;
        },
    }
    defer rows.deinit();

    // Since this is an insert, only rows.changed() is useful:
    std.debug.print("inserted {d} row(s)\n", .{rows.rowsChanged()});
}


{
    const result = conn.query("select * from users", .{});
    // deinit can also be called on the result itself, rather than
    // on the ok/err value.
    defer result.deinit();
    const rows = switch (result) {
        ok => |rows| rows,
        err => |err| => {
            std.debug.print("failed to select: {s}\n", .{err.desc});
            return error.Failure;
        },
    }
    while (rows.next()) |row| {
        // get the 0th column of the current row
        const id = row.get(i32, 0);
        std.debug.print("The id is: {d}", .{id});
    }
}
```

