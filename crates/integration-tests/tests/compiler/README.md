# Compiler fixtures

Run `mise test:compiler`. The runner discovers YAML files recursively under
`fixtures/`. Each fixture runs the production compiler pipeline for each
frontend and backend, and checks the state that named phases leave behind. Most
fixtures run through both JSON and GQL.

Assertions live under `physical.<backend>.<phase>.<state field>`:

```yaml
physical:
  clickhouse:
    normalize:
      input: {expect: ["(Limit 2 (Input traversal (...)))"]}
    plan_clickhouse:
      query_plan: {expect: ["(Scan Table(gl_user) AS u)"], point_lookup: true}
    response_policy:
      node: {expect: ["(Limit 3 (Project u.username AS u_username (_)))"]}
    settings:
      query_config: {equals: {transform_null_in: "0"}, absent: [join_order_algorithm]}
    result_contract:
      result_ctx: {query_type: traversal, nodes: {u: {entity_type: User}}}
    hydrate_plan:
      hydration_plan: static
    security:
      node: {expect: ["(Filter false, ... (_))"]}
    codegen:
      output:
        sql: {contains: [gl_merge_request], not_contains: [gl_edge]}
        parameterized_sql: {contains: ["{p"], not_contains: [opened]}
        params: [{value: opened, type: String}]
        indexes: [{table: gl_merge_request, name: idx_title}]
```

Common targets have aliases, declared in one table in
`integration-testkit/src/compiler/aliases.rs`:

| Alias | Target |
|---|---|
| `logical` (top level) | `normalize.input`, or `plan_clickhouse.input` for hydration fixtures |
| `planned`, `point_lookup` | `plan_clickhouse.query_plan` or `plan_duckdb.query_plan` |
| `emitted` | `response_policy.node` on ClickHouse, `paginate.node` on DuckDB |
| `final_ast` | `security.node` |
| `hydration` | `hydrate_plan.hydration_plan` |
| `result_context` | `result_contract.result_ctx` on ClickHouse |
| `settings` | `settings.query_config` |
| `sql`, `parameterized_sql`, `params`, `indexes` | `codegen.output` on ClickHouse |

An alias and an explicit phase entry cannot set the same state field.

A phase key must name a phase of that pipeline, and each state field must be one
the phase writes. The runner rejects other keys. Each state field has one
contract:

| State field | Contract |
|---|---|
| `input` | Patterns on the logical tree |
| `tp_bounds` | The traversal path bound of each listed node alias |
| `query_plan` | Patterns on the planned tree, and `point_lookup` |
| `node` | Patterns on the SQL AST |
| `query_config` | `exact`, `equals`, and `absent` on the resolved ClickHouse settings |
| `hydration_plan` | `none`, `static`, or `dynamic`, or the templates of a static or dynamic plan |
| `result_ctx` | The query type, node entries, neighbors center, and entity labels of the result context |
| `output` | `sql` and `parameterized_sql` text checks, bound `params`, and `indexes` that the layout declares and the final query scans |

In `input`, the `Input` head names the query type, then `include_debug_sql`
and `dynamic_columns=*` when the query sets them. A head without them asserts
the defaults.

`hydration_plan` also takes a list of templates under the plan kind. Each
listed template must match one template of the plan on every field it names.
Fields match like a format `value` check: a map matches on the keys it lists,
a list matches by length and position, and other values compare exactly. A
field the template does not have is reported missing. Static templates
have `entity_type`, `node_alias`, `destination_table`, `columns`,
`virtual_columns`, `injected_columns`, `has_traversal_path`, `virtual_filters`,
and `filter_injected_columns`. Dynamic templates have the same fields without
`node_alias`, `virtual_filters`, and `filter_injected_columns`.

```yaml
hydration:
  static:
    - node_alias: f
      virtual_columns: [{column_name: content, service: gitaly, lookup: blob_content}]
      virtual_filters: [{property: content, op: contains, value: needle}]
      filter_injected_columns: [content]
```

`result_ctx` checks only the keys it lists. `nodes` must list every node alias
of the result context, and each entry checks the fields it names:
`entity_type`, `pk_column`, `id_column`, and `type_column`. `neighbors_center`
takes `{entity, id}`, or `null` for no center. `labels` maps an entity to its
label property, or to `null` for no label.

```yaml
result_context:
  neighbors_center: {entity: User, id: 1}
  labels: {User: username}
```

`tp_bounds` maps a node alias to the predicate that bounds its traversal path,
or to `null` for no bound. A trailing `...` matches a prefix of the predicate.

```yaml
tp_bounds:
  tp_bounds:
    nodes:
      a: StartsWith(a.traversal_path, scalar(gl_project AS _orbit_scope WHERE _orbit_scope.id = 1)) ...
      b: null
```

`sql` checks the rendered SQL, with parameter values inlined.
`parameterized_sql` checks the SQL that is sent with bound parameters. Each
`params` entry must match one bound parameter by its value and its ClickHouse
type name.

Every case that compiles gets two more checks without an assertion. DuckDB
runs the rendered SQL of each DuckDB case on an empty database of the local
schema. For each ClickHouse case, the query and each CTE body, emitted as a
statement of its own for EXPLAIN, must define every CTE that their AST reads
and render each read under that CTE's name. The run fails if no case has a CTE
body that reads another CTE.

The compiler fixtures binary loads the embedded query config, so `settings` sees the
production settings, such as the query cache. The container suite runs EXPLAIN
on the seeded ClickHouse for every fixture with `indexes`. It first adds an older
decoy copy of each row in its own part, with string values that no fixture
matches. With the table's other skip indexes ignored, each listed skip index must
apply to a read of its table and prune granules. Other fixtures need no ClickHouse.

After `paginate`, `Limit` shows the fetched rows (the page plus one probe row)
and `Offset` shows the lowered SKIP. In `input`, `Limit` and `Offset` show the
requested LIMIT and SKIP. `security` sets the user, with the same shape as a
query scenario's `config.security`. Hydration fixtures run the hydration
pipeline, which has no `normalize` or `paginate` phase. Its `hydration` setup
takes `dynamic`, `path_segment_budget`, the `paths` of each node alias, and the
`columns` of a node alias, which replace the columns that the query selects.

Set `ontology_overlay: denorm_approved` to load `config/seeds/overlays/denorm_approved/`
for one fixture. Other fixtures keep the default ontology. An overlay merges
its YAML files over the ontology files of the same path, so it can add a
property, change a table, or add an edge table. Set `schema_version_prefix: v1_`
to prefix every physical table name, as a schema version does. It combines with
`ontology_overlay`.

```yaml
ontology_overlay: renamed_user
schema_version_prefix: v1_
```

| Overlay | Change |
|---|---|
| `scoped_user` | `User` is scoped by a traversal path instead of global |
| `renamed_user` | `User` is stored in `gl_renamed_user` |
| `property_types` | `Group` has a float `score` and a visible `private_note` that cannot be filtered |
| `string_edge_source` | A `NOTED` edge from `User` to `Note` in a table whose `source_id` is a string |
| `local_dates` | The local `Definition` has a `created_at` timestamp |
| `local_nulls` | The local `Definition` has a nullable `rank` |

Both `query.json` and `query.gql` are required. If a frontend cannot express the
tested plan, declare its reason under `missing_frontends`. The runner rejects
missing, blank, unknown, or redundant exception entries.

```yaml
missing_frontends:
  json: The JSON frontend does not expose property-to-property comparisons.
```

When every language of a case compiles on a backend, the runner compares each
language's compiled output with that of the first language, in name order,
without a `language_differs` entry: the parameterized SQL,
the bound parameter values and types, the hydration plan, and the result
context. A failure names the first part that differs. If a language compiles to
different output by design, declare its reason under `language_differs`. The
runner rejects an entry that names no query language, has a blank reason, or
names a language whose output matches on every backend.

```yaml
language_differs:
  gql: GQL returns properties in RETURN order, which JSON cannot express.
```

A file can hold several fixtures in a `cases` list. Every other top-level key
is a default for each case, and each case needs its own `name`.

A case with `generate` expands before it runs. `kind: named_queries` makes one
case per embedded named query. Each case renders the query in JSON and GQL with
its example parameters, keeps every other key of the case, and appends the
query name to the case name. `kind: repeat` replaces `placeholder` in every
string of the case with `repeat` repeated `count` times and then `suffix`, the
same expansion as a scenario's `{repeat, count, suffix}` value, so a fixture
can check a large input without embedding it.

```yaml
- name: A query past the byte limit is rejected before parsing
  generate: {kind: repeat, placeholder: LONG_NAME, repeat: x, count: 40000}
  query:
    gql: |
      MATCH (u:User {username: 'LONG_NAME'}) RETURN u
```

The fixtures cover access-path choices and their important eligibility guards.

A fixture that the compiler must reject sets `rejected` under a backend:

```yaml
physical:
  clickhouse:
    rejected:
      phase: gql_parse
      reason: schema
      contains: ["expected end of query, ORDER BY, SKIP, LIMIT, or DEBUG"]
      not_contains: [EOI, function]
```

`phase` is the phase that returns the error, and `reason` is its rejection
reason. `contains` and `not_contains` check the error message. Each field takes
one value or a `{json, gql}` map. Phases at or after the rejecting phase cannot
have assertions. Every rejection is also checked for a traversal path, an email
address, a physical table name, and the private fixture markers.

`rejections/` holds the error-message checks. Data-correctness scenarios check
the rejection reason and leak checks, never the wording.

`scenarios/` holds the compiler checks for the data-correctness scenarios. It
mirrors the scenario tree: `scenarios/search/filter_starts_with.yaml` checks the
query in `server/data_correctness/scenarios/search/filter_starts_with.yaml`.
Production route cases stay in `edge_scans/production_variant_routes.yaml`.

CTE assertions refer to top-level named query definitions, not execution order.
Use one of these forms in a pattern block:

```yaml
ctes:
  absent: true
```

```yaml
ctes:
  exact_order: [_candidate_p, _candidate_mr]
```

The exact order must list every top-level CTE and can use bound captures.
Omitting `ctes` leaves definitions unchecked. Empty blocks, empty orders,
`absent: false`, and combining both forms are rejected.

## Schema calls

`schema/` holds the checks for `CALL db.schema()`, which `gql::prepare` answers
with a schema response instead of compiling a query. Each case names the
`call`, the introspection `scope` (`all` or `local`), and one of `expect` or
`rejected`. `expect` takes `query` for a statement that prepares as a query,
`overview` for the compact overview of the scope, or `{node: Name}` for one
domain with that node, its properties, and no edges. `rejected` takes
`contains`, and every rejection must be safe to show a client.

```yaml
- name: A node outside the local scope is rejected
  call: "CALL db.schema('MergeRequest')"
  scope: local
  rejected: {contains: ["schema node 'MergeRequest' is unknown or unavailable in this scope"]}
```

## Coverage

| Area | Fixtures |
|---|---|
| Stored reads, routes, metadata, and node fallback | `edge_scans/`, `relations/` |
| FK-backed relationships and direction guards | `foreign_keys/` |
| Chains, branches, convergence, and relation reuse | `relations/` |
| Bounded expansion and incoming orientation | `variable_hops/` |
| Aggregation inputs and latest-row behavior | `aggregation/` |
| Neighbor access and routing | `neighbors/` |
| Path-finding frontiers and scope | `path_finding/` |
| Hydration projections and path filters | `hydration/` |
| Filters and result projections | `filters/`, `projections/` |
| SKIP/LIMIT windows, tie-breakers, and text budgets | `pagination/` |
| Parameter values inlined into rendered SQL | `rendering/` |
| DuckDB plans and generated SQL | `dialects/duckdb/` |

## Coverage boundaries

- JSON incoming variable hops have a dedicated fixture. GQL normalizes the
  equivalent pattern into outgoing edges, so it exercises a different plan.
- The testkit's `hydration_planning_selects_paths_before_sql_rendering` test
  checks 256/257 dynamic paths, 257 static paths, and budget-driven mode changes.
  The YAML fixture independently covers the large dynamic mode.
- The explain view flattens associative OR expressions. Static prefix fixtures
  verify predicate content, but do not assert the internal balanced-tree depth.
- Backend code-generation optimizations are outside this harness.

## Keep in Rust

These assertions exceed the current YAML contract. Keep them active; they are
not ignored tests or missing fixture migrations.

| Tests | Reason |
|---|---|
| `lower::hydration::tests::large_dynamic_tp_sets_emit_array_exists` | Checks the number and length of bound array parameters, which explain does not expose |
| `lower::hydration::tests::large_static_tp_sets_emit_or` | Pins 257 static predicates and rejects array mode; complements the dynamic YAML fixture |
| `lower::hydration::tests::{dynamic,static}_tp_filter_precedes_id_filter` | Checks serialized predicate order; partial YAML item matching is unordered |
| `compiler::hydration_planning_selects_paths_before_sql_rendering` | Exercises the 256/257 boundary and budget-driven mode selection together |
| `compiler::tests::hydration_widens_paths_to_segment_budget` | Checks bound path arrays, shared parameters, and ancestor coverage across generated paths |
| Compiler scope, role, settings, virtual-property, and telemetry tests | Exercise phases or observable outputs outside compiler fixture assertions |
| Compiler dialect and parameter-rendering tests | Check final backend SQL or parameter bindings, rather than the shared SQL AST |
| Input deserialization, normalization, schema-limit, validation-error, GQL hashing, hydration-metadata, and DDL tests | Check contracts that a valid query through the harness does not reach |

Keep a Rust test until a fixture checks every one of its assertions. For the
compiler fixture layer and its limits, see
[Plan-shape fixtures](../../../../docs/pages/engineering/testing/integration/compiler-and-code-graph.md#plan-tests).
