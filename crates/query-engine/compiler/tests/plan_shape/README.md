# Plan-shape scenarios

Run `mise test:plan-shape`. The runner reads the YAML files in
`crates/integration-tests/tests/compiler/plan_shape/` without building the server.

Each scenario requires an explicit `skip: true` or `skip: false`.
Skipped scenarios retain their queries and expectations. Use `skip_reason` to
record a known blocker. The runner reports every skip and fails if no scenario runs.

Enabled scenarios check the bound logical tree and the selected physical program.
ClickHouse selection includes restriction and scope preparation. DuckDB uses its
own catalog. Both physical paths verify and lower the selected program.
These tests check plan shape; database execution belongs to the behavioral suites.

Patterns match structured S-expressions anywhere in the tree:

- `_` matches one atom or subtree.
- `...` matches zero or more siblings in a list.
- Quoted atoms use JSON string escaping.
- `expect` requires a match; `reject` forbids one.

For example, `(Join Inner (Equal _ _) ... )` requires an inner equality join.
`(Scan gl_edge Snapshot ...)` requires that physical table and read mode.

A backend block can use `skip: true` while another backend runs, or `error` to
assert that binding rejects unavailable data. Unknown scenario fields and backend
names fail the run. Update expressions when representation changes, but retain
assertions for required optimizations until those rules work.
