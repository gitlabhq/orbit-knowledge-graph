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

## Edge scans and node access

| Old implementation | Behavior | Fixtures |
|---|---|---|
| `plan/edge_chain::reorder_by_selectivity` | Start at the more selective endpoint | `edge_scans/selective_end_reversal.yaml` |
| `determine_hydration` | Skip node reads when edges supply identity and filters | `edge_scans/plain_single_hop.yaml`, `edge_scans/denormalized_filter.yaml` |
| `determine_hydration` | Keep uncovered filters and property consumers table-backed | `edge_scans/uncovered_filter_fallback.yaml`, `narrowing/nonselective_join_guard.yaml`, `aggregation/property_inputs.yaml` |
| `lower/helpers::push_edge_predicates` | Push shared code columns once; retain endpoint ID meaning | `edge_scans/code_column_pushdown.yaml` |
| `emit_denorm_tags` | Use source/target tags, scalar/set membership, and one filter per shared node | `edge_scans/denormalized_filter.yaml`, `edge_scans/denormalized_incoming_set.yaml`, `edge_scans/denormalized_shared_node.yaml` |
| `lower/flat_chain` | Plain single-edge traversal; streaming FINAL for multiple edges | `edge_scans/plain_single_hop.yaml`, `narrowing/recursive_cascade.yaml` |
| `lower/flat_chain` | Single-edge aggregation uses latest-row dedup and conditional measures | `aggregation/conditional_count.yaml`, `aggregation/conditional_count_with_join.yaml` |
| `build_multi_hop_union` | Emit only requested depths and retain incoming orientation | `variable_hops/depth_arms.yaml`, `variable_hops/exact_hops.yaml`, `variable_hops/incoming.yaml` |

## Narrowing and foreign keys

| Old implementation | Behavior | Fixtures |
|---|---|---|
| `emit_filter_narrowing` | Candidate keys for pinned or high-selectivity joined nodes | `narrowing/selective_node_keys.yaml`, `narrowing/selective_property_keys.yaml` |
| `emit_filter_subquery` | Authoritative filter-only keys; share definitions in first-use order | `narrowing/shared_filter_keys.yaml`, `foreign_keys/filter_only_target.yaml` |
| `build_cascade_anchor` | Recursive frontier membership from pinned IDs, ranges, or filter keys | `narrowing/recursive_cascade.yaml`, `narrowing/id_range_anchor.yaml`, `narrowing/node_with_cascade.yaml` |
| `lower/flat_chain` | Place membership inside FINAL only for leading endpoint keys | `narrowing/leading_edge_key.yaml`, `narrowing/recursive_cascade.yaml` |
| `resolve_node_flags` | Edge-derived node narrowing; skip convergent targets | `narrowing/node_with_cascade.yaml`, `narrowing/convergent_target_guard.yaml` |
| `emit_node_join_with_narrowing` | Full replacement keys, inner immutable filters, outer mutable/deletion rechecks | `narrowing/mutable_recheck.yaml`, `narrowing/sort_key_pushdown.yaml` |
| Narrowing eligibility | Avoid rescans without sufficient selectivity | `narrowing/unselective_chain.yaml`, `narrowing/nonselective_join_guard.yaml` |
| `elide_hops` | Replace pinned targets with holder predicates, including filter-only multi-ID targets | `foreign_keys/elided_pinned_target.yaml`, `foreign_keys/multi_id_filter_elision.yaml` |
| `elide_hops` | Retain required identities, property comparisons, and elevated-role targets | `foreign_keys/multi_id_projection_guard.yaml`, `foreign_keys/property_comparison_guard.yaml`, `foreign_keys/elevated_role_guard.yaml` |
| `detect_fk_star`, `lower/fk::emit_star` | Direct node joins, candidate dependencies, target narrowing, aggregate rescan suppression | `foreign_keys/star_candidate_dependencies.yaml`, `foreign_keys/star_target_narrowing.yaml`, `foreign_keys/aggregation_avoids_target_rescan.yaml` |
| `detect_fk_chain`, `emit_chain` | Replace eligible scoped chains and global hubs with node joins | `foreign_keys/chain_traversal.yaml`, `foreign_keys/chain_aggregation.yaml` |
| FK-chain eligibility | Keep edge scans for pinned endpoints, edge filters, and cross-namespace links | `foreign_keys/chain_point_guard.yaml`, `foreign_keys/chain_edge_filter_guard.yaml`, `foreign_keys/cross_namespace_chain_guard.yaml` |
| FK direction | Preserve the physical holder for incoming self-relationships | `foreign_keys/incoming_self_relationship.yaml` |

## Family algorithms

| Old implementation | Behavior | Fixtures |
|---|---|---|
| `lower/neighbors` | Fuse eligible both-direction reads into one scan | `neighbors/fused.yaml`, `neighbors/denormalized_fused.yaml` |
| Neighbor eligibility | Use separate arms for center filters, indirect identity, or multiple routes | `neighbors/filtered_directional.yaml`, `neighbors/indirect_identity.yaml`, `neighbors/multiple_routes.yaml` |
| Neighbor routing | Push predicates into each table arm | `neighbors/multiple_routes.yaml` |
| Incoming neighbors | Pin the leading traversal-path key through a center lookup | `neighbors/incoming_path_lookup.yaml` |
| `plan/pathfinding` | Split forward/backward depths and omit the backward side at depth one | `path_finding/scoped_frontiers.yaml`, `path_finding/odd_depth.yaml`, `path_finding/direct_pinned.yaml` |
| `build_anchor` | Inline unscoped pinned IDs; bound filtered current-row anchors | `path_finding/direct_pinned.yaml`, `path_finding/filtered_anchor.yaml` |
| `build_scope_cte`, `build_frontier_arm` | Union endpoint paths; prune expansions and intersection by scope | `path_finding/scoped_frontiers.yaml`, `path_finding/odd_depth.yaml` |
| Wildcard paths | Restrict first-hop relationship kinds from endpoint metadata | `path_finding/wildcard_endpoint_pruning.yaml` |
| `lower/hydration` | Prune projected columns and use full-key latest-row reads | `hydration/projection_pruning.yaml`, `hydration/multiple_entities.yaml` |
| Hydration paths | Prune ancestors, widen to budget, and select prefix OR or array membership | `hydration/leaf_pruning.yaml`, `hydration/budget.yaml`, `hydration/static_prefixes.yaml`, `hydration/large_dynamic_paths.yaml` |
| Hydration fallback | Omit absent path and ID constraints | `hydration/projection_pruning.yaml`, `hydration/without_ids.yaml` |

## Coverage boundaries

- JSON incoming variable hops have a dedicated fixture. GQL normalizes the
  equivalent pattern into outgoing edges, so it exercises a different plan.
- The testkit's `hydration_planning_selects_paths_before_sql_rendering` test
  checks 256/257 dynamic paths, 257 static paths, and budget-driven mode changes.
  The YAML fixture independently covers the large dynamic mode.
- The explain view flattens associative OR expressions. Static prefix fixtures
  verify predicate content, but do not assert the internal balanced-tree depth.
- Compiler unit tests retain synthetic catalog guards that the embedded ontology
  cannot express, such as an FK chain made entirely of global hubs.
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
| `plan::edge_chain::tests::fk_chain_*` | Uses synthetic catalog facts, including all-global chains absent from the embedded ontology |
| `compiler::tests::cross_namespace_fk_chain_elides_to_node_joins` | Also checks authorization predicates after lowering |
| `compiler::tests::multi_hop_aggregation_generates_cascade_cte` | Also checks security injection after lowering |
| Compiler cursor, scope, role, settings, virtual-property, and telemetry tests | Exercise phases or observable outputs outside plan-shape assertions |
| Compiler dialect and parameter-rendering tests | Check final backend SQL or parameter bindings, rather than the shared SQL AST |
| Input deserialization, normalization, schema-limit, validation-error, GQL hashing, cursor-token, hydration-metadata, and DDL tests | Check contracts that a valid query through the harness does not reach |

Keep a Rust test until a fixture checks every one of its assertions. For the
plan-shape layer and its limits, see
[Plan-shape fixtures](../../../../../docs/design-documents/testing.md#plan-shape-fixtures).
