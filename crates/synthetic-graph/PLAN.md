# Datalake-Generator Wiring Plan

## Context

The datalake-generator uses a fundamentally different architecture from the simulator:

- **Schema-driven, table-by-table**: Reads ClickHouse schemas at runtime via `SchemaRegistry`, then generates rows per-table using `DirectBatchBuilder`. The simulator is ontology-driven and generates entities by type with `BatchBuilder`.
- **Siphon-native types**: `SiphonValue` has `Int8`, `DateTime64` (microseconds), and `generate_string_into` for zero-copy writes into Arrow `StringBuilder`. The simulator's `FakeValue` works at the ontology type level (millis, no Int8, no StringBuilder API).
- **Foundation + Layout pattern**: `Foundation` pre-computes user/group/project seeds with namespace IDs, organization IDs, and traversal paths. `ProjectEntityLayout` maps per-project entity counts. The simulator doesn't have this concept — it generates everything inline from the dependency graph.
- **ID block arithmetic**: Tables get non-overlapping ID ranges via `table_base_id` + `synthetic_row_id`. The simulator uses a global atomic counter.

## What's actually duplicated

Only the ID math functions in `layout.rs` are truly duplicated:

| datalake-generator function | synthetic-graph equivalent |
|---|---|
| `layout::synthetic_row_id(base, rows, proj_idx, ent_idx)` | `ids::synthetic_row_id(base, rows, proj_idx, ent_idx)` — identical |
| `layout::map_child_to_parent_index(child, children, parents)` | `ids::map_child_to_parent_index(child, children, parents)` — identical |
| `layout::table_base_id(name, foundation, layout)` | `ids::table_block_base(base_id, position, proj_count, max_rows)` — same math, different signature |

The `ColumnKind::classify` logic in `fake_values.rs` overlaps with `FieldKind::classify_column`, but they produce different enum types with different variant sets (15 vs 31 variants), and the generators produce different output types.

## What to replace

### `domain/layout.rs` — partial replacement

Replace the three functions with thin wrappers or re-exports from `synthetic_graph::ids`:

```rust
// synthetic_row_id and map_child_to_parent_index: re-export directly
pub use synthetic_graph::ids::{synthetic_row_id, map_child_to_parent_index};

// table_base_id: thin wrapper that calls catalog internally
pub fn table_base_id(table_name: &str, foundation: &Foundation, layout: ProjectEntityLayout) -> i64 {
    let project_count = foundation.projects.len();
    let max_rows_per_project = layout.max_rows_per_project().max(1);
    let table_position = catalog::project_table_position(table_name).unwrap_or(0);
    synthetic_graph::ids::table_block_base(
        foundation.next_entity_id,
        table_position,
        project_count,
        max_rows_per_project,
    )
}
```

Keep `ProjectEntityLayout` in place — it's config-specific to the datalake-generator.

### `domain/foundation.rs` — keep as-is

The `Foundation`, `UserSeed`, `GroupSeed`, `ProjectSeed` structs and `build_foundation()` are specific to the table-seeding pipeline. They carry fields (`namespace_id`, `parent_namespace_id`, `organization_id`) that the ontology-driven generator doesn't produce.

### `data_generation/fake_values.rs` — keep as-is

`SiphonFakeValueGenerator` and `SiphonValue` serve a different architectural layer (ClickHouse-native types, direct StringBuilder writes). Forcing these through `FakeValueGenerator`/`FakeValue` would require conversion layers with no actual deduplication benefit.

## What NOT to do

- Don't try to make `Foundation` use `EntityRegistry` — they serve different patterns (pre-computed seeds vs runtime accumulation).
- Don't try to unify `SiphonValue` with `FakeValue` — they target different type systems.
- Don't add synthetic-graph as a dependency if the only thing used is 3 ID math functions — the benefit doesn't justify the coupling. Consider whether it's worth it.

## Decision point

The actual deduplication for datalake-generator is minimal (3 functions, ~30 lines). Options:

1. **Add dependency, replace layout.rs functions** — Clean, but adds a build dependency for 30 lines of savings.
2. **Leave datalake-generator as-is** — The "duplication" is 3 trivial arithmetic functions. The rest is architecturally different.
3. **Move just the 3 ID functions to a tiny shared crate** — Over-engineering for 30 lines.

Recommendation: Option 1 if we want consistency, Option 2 if we're pragmatic. The original goal was to make synthetic-graph the "single source of truth for generating synthetic SDLC property graphs." The datalake-generator's table-seeding pipeline isn't really generating a property graph — it's filling ClickHouse tables with schema-driven fake data. The overlap is incidental, not architectural.
