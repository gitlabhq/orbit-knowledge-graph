# Planner optimization coverage

Run `mise test:plan-shape`. The runner discovers YAML files recursively under
`fixtures/`. Each fixture checks normalized input, selected requirements, and/or
the emitted SQL AST. Most fixtures run through both JSON and GQL.

Set `ontology_overlay: denorm_approved` to load `config/seeds/overlays/denorm_approved/`
for one fixture. Other fixtures keep the default ontology.

Both `query.json` and `query.gql` are required. If a frontend cannot express the
tested plan, declare its reason under `missing_frontends`. The runner rejects
missing, blank, unknown, or redundant exception entries.

```yaml
missing_frontends:
  json: The JSON frontend does not expose property-to-property comparisons.
```

A file can hold several fixtures in a `cases` list. Every other top-level key
is a default for each case, and each case needs its own `name`.

The fixtures cover access-path choices and their important eligibility guards.

CTE assertions refer to top-level named query definitions, not execution order.
Use one of these forms in a planned or emitted assertion block:

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

## Coverage boundaries

- JSON incoming variable hops have a dedicated fixture. GQL normalizes the
  equivalent pattern into outgoing edges, so it exercises a different plan.
- The testkit's `hydration_planning_selects_paths_before_sql_rendering` test
  checks 256/257 dynamic paths, 257 static paths, and budget-driven mode changes.
  The YAML fixture independently covers the large dynamic mode.
- The explain view flattens associative OR expressions. Static prefix fixtures
  verify predicate content, but do not assert the internal balanced-tree depth.
- Scope enforcement, role scans, virtual-property hydration, cursor handling,
  and backend code-generation optimizations are outside this harness.

## Keep in Rust

These assertions exceed the current YAML contract. Keep them active; they are
not ignored tests or missing fixture migrations.

| Tests | Reason |
|---|---|
| `lower::hydration::tests::large_dynamic_tp_sets_emit_array_exists` | Checks the number and length of bound array parameters, which explain does not expose |
| `lower::hydration::tests::large_static_tp_sets_emit_or` | Pins 257 static predicates and rejects array mode; complements the dynamic YAML fixture |
| `lower::hydration::tests::{dynamic,static}_tp_filter_precedes_id_filter` | Checks serialized predicate order; partial YAML item matching is unordered |
| `plan_shape::hydration_planning_selects_paths_before_sql_rendering` | Exercises the 256/257 boundary and budget-driven mode selection together |
| `compiler::ontology::hydration_widens_paths_to_segment_budget` | Checks bound path arrays, shared parameters, and ancestor coverage across generated paths |
| Compiler cursor, scope, role, settings, virtual-property, and telemetry tests | Exercise phases or observable outputs outside plan-shape assertions |
| Compiler dialect and parameter-rendering tests | Check final backend SQL or parameter bindings, rather than the shared SQL AST |
| Input deserialization, normalization, schema-limit, validation-error, GQL hashing, cursor-token, hydration-metadata, and DDL tests | Check contracts that a valid query through the harness does not reach |

Keep a Rust test until a fixture checks every one of its assertions. For the
plan-shape layer and its limits, see
[Plan-shape fixtures](../../../../../docs/design-documents/testing.md#plan-shape-fixtures).
