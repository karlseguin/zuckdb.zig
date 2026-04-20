F=
.PHONY: t
t:
	TEST_FILTER="${F}" zig build -Dsystem_libduckdb=false test --summary all -freference-trace
