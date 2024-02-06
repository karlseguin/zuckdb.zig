#include "duckdb.h"
#include <stdalign.h>

const size_t config_sizeof = sizeof(duckdb_config);
const size_t config_alignof = alignof(duckdb_config);

const size_t database_sizeof = sizeof(duckdb_database);
const size_t database_alignof = alignof(duckdb_database);

const size_t connection_sizeof = sizeof(duckdb_connection);
const size_t connection_alignof = alignof(duckdb_connection);

const size_t result_sizeof = sizeof(duckdb_result);
const size_t result_alignof = 8;
// THIS crashes the zig compiler?
// const size_t result_alignof = alignof(duckdb_result);

const size_t statement_sizeof = sizeof(duckdb_prepared_statement);
const size_t statement_alignof = alignof(duckdb_prepared_statement);

const size_t appender_sizeof = sizeof(duckdb_appender);
const size_t appender_alignof = alignof(duckdb_appender);
