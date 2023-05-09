Zig driver for DuckDB.

This API mixes idiomatic Zig error handling with a Result-based approach (think Rust). This was done for two reasons. The first is to expose the DuckDB error message for various actions (e.g. trying to execute an invalid SQL). The second is that, even in error cases, DuckDB can retain memory, and thus even the error-case requires a deinit. In other words, a Result is used whenever data (error message) and/or behavior (deinit) is needed in the error case.
