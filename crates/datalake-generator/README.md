# Datalake Generator

> **Not for production use.** This tool generates synthetic data for development and testing only.

High-throughput seeding harness for ClickHouse tables used by the Knowledge Graph.

The generator has a single seeding architecture with:

- deterministic foundation generation (users, groups, projects),
- staged table writes with dependency ordering,
- optional continuous mode for ongoing insert/update/delete traffic.

## Quick start

```shell
cargo run --bin datalake-generate -- -c crates/datalake-generator/datalake-generator.yaml
```

## CLI

```shell
cargo run --bin datalake-generate -- [OPTIONS]
```

Options:

- `-c, --config <PATH>`: YAML config path (default `datalake-generator.yaml`)
- `--skip-seeding`: skip the initial seed and run from saved state

## Main flow

1. Build foundation entities from config.
2. Truncate stage tables that exist in the target ClickHouse schema.
3. Run staged writes in dependency order.
4. Persist state for continuous mode.
5. Optionally run continuous mode.
6. Write metrics report.

## Data generation pipeline

```plaintext
 OS threads (std::thread::scope)
┌──────────────────────────────────────────┐
│                                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐  │
│  │ Table A  │ │ Table B  │ │ Table C  │  │
│  │ producer │ │ producer │ │ producer │  │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘  │
│       │            │            │        │
└───────┼────────────┼────────────┼────────┘
        │            │            │
        ▼            ▼            ▼
  ┌────────────────────────────────────────┐
  │   sync_channel (bounded, capacity 16)  │
  │        Arrow RecordBatches             │
  └──────────────────┬─────────────────────┘
                     │
                     ▼
          ┌───────────────────────┐
          │  Consumer thread      │
          │  (spawn_blocking)     │
          │                       │
          │  tokio::spawn per     │
          │  batch -> ClickHouse  │
          │  HTTP insert          │
          └───────────────────────┘
```

Each stage (foundation, primary, secondary, leaf) runs this pipeline for its
tables. Within a stage, all tables generate rows in parallel.

## Graph structure

The generator builds a property graph that mirrors a GitLab instance's SDLC
data. The graph has three layers, each depending on the one above.

### Foundation layer

Built first, deterministically from config. These entities form the skeleton
that all project-scoped data hangs off of.

```plaintext
Organization (implicit, always id=1)
├── Users (flat list)
└── Groups (hierarchical)
    ├── Root Group 1
    │   ├── Subgroup 1a
    │   │   └── Subgroup 1a-i
    │   └── Subgroup 1b
    └── Root Group 2
        └── ...
    └── Projects (under each group)
```

Each group gets an entity ID, a namespace ID, and a traversal path
(`"1/2/3/"`) encoding its position in the hierarchy. Projects inherit
their parent group's namespace and path.

### Project-scoped entities

Generated per-project according to `per_project` counts in the config.
Every entity gets a synthetic ID, a `project_id`, a `namespace_id`,
and a `traversal_path` linking it back to its project.

Entities are written in four dependency-ordered stages:

| Stage | Tables | Depends on |
|-------|--------|------------|
| Foundation | `siphon_users`, `siphon_namespaces`, `siphon_namespace_details`, `namespace_traversal_paths`, `siphon_projects`, `project_namespace_traversal_paths`, `siphon_knowledge_graph_enabled_namespaces` | nothing |
| Primary | `hierarchy_merge_requests`, `hierarchy_work_items`, `siphon_issues`, `siphon_p_ci_pipelines`, `siphon_vulnerabilities`, `siphon_vulnerability_scanners`, `siphon_vulnerability_identifiers`, `siphon_vulnerability_occurrences`, `siphon_milestones`, `siphon_labels`, `siphon_members` | Foundation |
| Secondary | `siphon_notes`, `siphon_merge_request_diffs`, `siphon_p_ci_stages`, `siphon_security_scans`, `siphon_vulnerability_merge_request_links`, `siphon_merge_requests_closing_issues`, `siphon_work_item_parent_links`, `siphon_issue_links`, `siphon_vulnerability_occurrence_identifiers` | Primary |
| Leaf | `siphon_p_ci_builds`, `siphon_security_findings`, `siphon_merge_request_diff_files` | Secondary |

Within a stage, all tables generate in parallel. The next stage starts
only after the previous stage finishes.

### Relationships

Parent-child and cross-entity relationships are wired deterministically
using `map_child_to_parent_index`, which spreads children evenly across
parents: `parent_index = (child_index * parent_count) / child_count`.

Relationships expressed:

```plaintext
MergeRequest ──────── Project (target_project_id, source_project_id)
MergeRequest ──────── User (author_id)
WorkItem ──────────── User (author_id)
Note ─────────────┬── MergeRequest (noteable_id, split by ratio)
                  └── WorkItem (noteable_id)
MergeRequestDiff ──── MergeRequest (merge_request_id)
MergeRequestDiffFile ─ MergeRequestDiff (merge_request_diff_id)
Stage ─────────────── Pipeline (pipeline_id)
Job ───────────────── Stage (stage_id)
SecurityScan ──────── Pipeline + Job (pipeline_id, build_id)
SecurityFinding ───── SecurityScan + VulnerabilityScanner (scan_id, scanner_id)
Vulnerability ─────── User (author_id)
VulnerabilityOccurrence ─ Vulnerability + Scanner + Identifier
VulnerabilityMergeRequestLink ─ Vulnerability + MergeRequest
MergeRequestClosingIssue ──── MergeRequest + WorkItem
WorkItemParentLink ── WorkItem (parent) + WorkItem (child)
IssueLink ─────────── WorkItem (source) + WorkItem (target)
Member ────────────── Project + User (source_id, user_id)
```

Notes are split between MergeRequest and WorkItem parents proportionally
to their respective counts. Work item parent links form a flat hierarchy
(all children point to the first work item in the project).

## Synthetic ID scheme

IDs are computed arithmetically so that every row across every table gets a
globally unique, deterministic ID with no coordination or sequence counters.

```plaintext
table_id_base = next_entity_id + (table_position * block_size)
block_size    = project_count * max_rows_per_project + 1

row_id = table_id_base + (project_index * rows_per_project) + entity_index
```

`table_position` is a fixed ordinal for each project-scoped table (0 for
merge_requests, 1 for work_items, etc.), defined in `catalog.rs`. This
spreads each table's ID range into a non-overlapping block. Child tables
compute parent IDs using the parent table's base and the same formula,
so referential links are computed without lookups.

## Fake value generation

The generator auto-fills any column not explicitly set by a relationship
writer. Column names are classified by pattern matching into kinds, and
each kind produces plausible-looking synthetic data:

| Column pattern | Kind | Example output |
|---------------|------|----------------|
| `id`, `*_id` | Id | `42317` |
| `iid` | Iid | `1` through `10000` |
| `*email*` | Email | `user12ab@example.com` |
| `*url*` | Url | `https://example.com/a1b2/c3d4` |
| `*sha*`, `*hash*`, `*fingerprint*` | Sha | 40-char hex string |
| `*path*` | Path | `/p1a/d2b/c3d4e5f6` |
| `*name*`, `*title*` | Name | `alpha_a1b2c3d4` |
| `*description*`, `*body*`, `*note*` | Description | `Lorem ipsum dolor a1b2` |
| `*status*` | Status | `open`, `closed`, `merged`, `pending`, `active` |
| `*state*` | State | `pending`, `running`, `success`, `failed`, `canceled` |
| `*ref*`, `*branch*` | Branch | `feature/branch-a1b2` |
| `uuid`, `*_uuid` | Uuid | RFC 4122 format |
| `*_ids` | IdList | `[1, 2345, 6789]` |
| `*_at`, `created_at`, `updated_at` | DateTime | timestamp within last 5 years |
| anything else (string) | GenericString | `val<hex>` |

Nullable columns have a ~10% chance of being null.

### Field overrides

The `field_overrides` config section constrains generated values for
enum-like columns to valid domain values. Without overrides, status/state
columns pick from a small hardcoded pool. With overrides, the generator
picks uniformly from the provided list:

```yaml
field_overrides:
  MergeRequest:
    state_id: [1, 2, 3, 4]
    merge_status: ["unchecked", "can_be_merged", "cannot_be_merged"]
```

### Determinism

All generation is seeded. The base seed (default `42`) is XORed with a
per-table offset so each table gets a different random sequence while
remaining fully reproducible. The RNG is `SmallRng` seeded via
`seed_from_u64`, and the counter is mixed with the golden ratio hash
(`0x9e3779b97f4a7c15`) for better bit distribution.

## Limitations

**Single organization.** The generator always uses `organization_id = 1`.
Multi-org generation is not supported.

**No schema DDL.** The generator reads schemas from a running ClickHouse
instance. Tables must already exist; it does not create or migrate them.
Tables missing from ClickHouse are silently skipped.

**Uniform distributions.** Entities are spread evenly across parents.
Real GitLab data has power-law distributions (some projects have thousands
of MRs, most have few). The generator does not model this skew.

**Flat work item hierarchies.** Work item parent links all point to the
first work item in each project. Real work items form deeper trees.

**No cross-project relationships.** MergeRequest source and target
projects are always the same project. Forked-project MR workflows are
not modeled.

**No code data.** The generator covers SDLC entities only. It does not
produce code indexing data (call graphs, definitions, references).

**No temporal consistency.** Timestamps are generated independently per
column. A merge request's `merged_at` might precede its `created_at`.

**String content is synthetic.** Names, descriptions, and other text
fields are lorem-ipsum-style placeholders. They do not resemble real
GitLab content.

**Fixed column type support.** The generator handles `Int64`, `Int8`,
`Utf8`, `Boolean`, `Float64`, `Date32`, and `List<Int64>`. Any other
Arrow data type falls back to null.

**Truncation on re-run.** The seeding phase truncates all stage tables
before writing. Running the generator against a database with real data
would destroy it.

## Module structure

- `src/lib.rs`: top-level `run()` entrypoint that orchestrates the full pipeline
- `src/domain/`: domain model
  - `foundation.rs`: foundation entities and ID allocation
  - `layout.rs`: per-table row counts and synthetic ID helpers
- `src/seeding/`: seeding pipeline (what to generate and in what order)
  - `catalog.rs`: stage ordering and table metadata
  - `pipeline.rs`: concurrent batch generation and ClickHouse inserts
  - `state_builder.rs`: builds `HierarchyState` after seeding
- `src/data_generation/`: row-level building toolkit (how to construct Arrow batches)
  - `schema_registry.rs`: fetches and caches Arrow schemas from ClickHouse
  - `row_builder.rs`: `DirectBatchBuilder` for columnar Arrow array construction
  - `fake_values.rs`: deterministic fake value generation per column type
- `src/continuous.rs`: continuous insert/update/delete traffic after initial seed
- `src/state.rs`: compressed state persistence (`save` / `load`)

## Configuration

Use `crates/datalake-generator/datalake-generator.yaml` as the baseline.

Key sections:

- `datalake`: ClickHouse connection and database
- `generation`: batch size, root counts, per-project counts, field overrides
- `continuous`: continuous mode controls
- `metrics`: report output
- `state`: state directory

## Metrics output

When metrics are enabled, the generator writes:

- JSON report at `metrics.output_path`
- stdout summary with duration, table row counts, and resource usage
