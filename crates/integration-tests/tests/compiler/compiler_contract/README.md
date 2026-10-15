# Compiler contract fixtures

Run `mise test:compiler-contract`. The runner discovers YAML files recursively
under `fixtures/` and needs no Docker. Each case runs a query through one
compiler stage and checks typed outputs: errors, the normalized input,
parameters, settings, hydration, result metadata, pagination, and schema calls.
Plan decisions belong in [plan-shape fixtures](../plan_shape/README.md).

A file can hold several cases in a `cases` list. Every other top-level key is a
default for each case, and nested maps merge key by key.

## Case fields

| Field | Description |
|---|---|
| `name` | Required and unique within the file |
| `query` | `json` and `gql` strings. `gql` may be a list of equivalent spellings |
| `missing_frontends` | Reason a frontend is absent. Required when a stage frontend has no query |
| `stage` | `decode` (JSON deserialization or GQL lowering), `coerce` (decode, then normalize without validation), `parse` (frontend with schema checks), `normalize` (validation and normalization), `compile` (default), or `prepare` (GQL schema calls) |
| `backend` | `clickhouse` (default) or `duckdb` for local compilation |
| `scope` | `all` (default) or `local` for `prepare` |
| `security` | `default`, `admin`, or `{paths, authorized_paths, access_level, org_id, admin, scope_proofs}` |
| `ontology` | `{overlay, schema_prefix, path_scopable, rename_tables, fields}` on the embedded catalog, or `{nodes, edges, edge_columns}` for a synthetic catalog. `fields` maps `Node.field` to `{filterable, like_allowed, virtual}` |
| `cursor` | `{from: {json\|gql: query}, keys: [...]}` mints a token for `{{cursor}}` in queries |
| `parity` | Defaults to on for `compile`: every query must produce the same SQL, parameters, and hydration, or the same rejection reason |
| `same_output` | Other queries that must compile identically to the first query |
| `same_hash`, `different_hash` | Other queries whose statement hash must match or differ from the first query in the same frontend |

Queries can generate long inputs with
`{{for i in 0..4000 sep ","}}(a{i}){{end}}`. Use `..=` for an inclusive range.

## Expectations

`expect` applies to every query. `expect.json` and `expect.gql` add
frontend-specific checks.

| Field | Checks |
|---|---|
| `error` | `reason` (the rejection reason), `client_safe`, `message` (exact), `ends_with`, `contains`, `not_contains` |
| `input` | Subset of the normalized input view |
| `params` | `contains: [{value, type}]`, `absent: [value]`, `inlined_absent: [text]` (text must not appear in the parameterized SQL) |
| `settings` | Subset of the effective ClickHouse settings. `null` means absent |
| `hydration` | Subset of the hydration plan view, keyed by node alias or entity |
| `result` | Subset of the result context: aliases, identity columns, entity auth, and edges |
| `pagination` | `query_hash` (`zero` or `nonzero`) and `key_count` |
| `schema` | `full`, `domains`, `nodes`, `expanded_with_properties`, `edges`, and `edges_touch_expanded_nodes` |

Subset matching requires every listed key and exact list lengths. Use
`{$contains: [...]}` or `{$excludes: [...]}` to match list members. Every
ClickHouse compile also checks that rendering binds every placeholder.

## Debugging

`CONTRACT_FILTER=<text>` runs matching cases. `CONTRACT_PRINT=<text>` prints the
outcome, input view, parameters, settings, hydration, result context, and
rendered SQL for matching queries.

## Keep in Rust

| Tests | Reason |
|---|---|
| `compiler::tests::compile_rejections_increment_the_rejection_counter` | Reads a test-only metrics counter |
| `compiler::tests::native_conditions_need_a_column`, `validate::tests::native_predicate_trees_are_bounded_before_planning` | Build inputs that no frontend can produce |
| `codegen::{clickhouse,duckdb}::tests::time_buckets_*` | Check dialect function rendering for each truncation unit |
| `validate::tests::rejects_inputs_the_frontends_block_first` | The JSON schema and GQL lowering reject these inputs first, so only inputs built in code reach the validator branches |
| `validate::tests::edge_column_type_covers_all_reserved_columns`, `json_dsl::tests::accepts_declared_columns_and_only_filterable_fields` | Sweep the whole embedded catalog |
| `input::tests` for hops, `order_by`, `aggregation_sort`, and `group_by` keys | Deserialize single JSON fields, not queries |
