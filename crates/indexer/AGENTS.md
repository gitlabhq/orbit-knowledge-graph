# Indexer

Message processing framework and domain modules for the GitLab Knowledge Graph. Consumes events from NATS JetStream, routes them through handlers, and writes property graph data to ClickHouse.

## Architecture

```plaintext
NATS JetStream → Engine → Handler Registry → ClickHouse
                    ↓
              Worker Pool
```

### Engine components

| Component | File | Purpose |
|-----------|------|---------|
| Engine | `engine.rs` | Message dispatch and lifecycle |
| HandlerRegistry | `handler.rs` | Handler-to-topic routing |
| NatsBroker | `nats/broker.rs` | JetStream connection |
| WorkerPool | `worker_pool.rs` | Concurrency control |
| Destination | `destination.rs` | Output abstraction |
| ClickHouse | `clickhouse/` | ClickHouse destination implementation |

### Domain modules

| Module | Directory | Purpose |
|--------|-----------|---------|
| `code::register_handlers` | `modules/code/` | Git repository indexing via Rails internal API, call graph extraction |
| `sdlc::register_handlers` | `modules/sdlc/` | SDLC entity indexing (projects, MRs, CI, issues, etc.) |
| `namespace_deletion::register_handlers` | `modules/namespace_deletion/` | Soft-deletes all graph data for a namespace across ontology-driven tables |

### Traits

- **Handler**: Message processor (`name`, `subscription`, `handle`)
- **Destination**: Provides BatchWriter or StreamWriter
- **Event**: Type-safe message serialization

### Schema migration

The **dispatcher** owns schema migration. At boot, `schema::migration::run_if_needed()` compares
the embedded `SCHEMA_VERSION` with the active version in ClickHouse. On a mismatch, it acquires a
NATS KV distributed lock, generates DDL from the ontology via `generate_graph_tables_with_prefix()`,
creates new-prefix ClickHouse tables, and marks the new version as `migrating`. Marking
`migrating` also opens a re-index **campaign** (`campaign::CampaignState`, in-memory) that
dispatchers stamp onto every request as `campaign_id` until completion clears it.

Indexers do not run DDL. Before consuming, the indexer calls `schema::version::wait_until_ready()`,
which polls `gkg_schema_version` with backoff until its version is `active`/`migrating`, exiting
non-zero (→ restart) if the budget is exhausted or the binary is outdated. All write paths
(checkpoints, namespace deletion, ontology-driven tables) use
`prefixed_table_name(table, SCHEMA_VERSION)` so they always target the current schema version's
table-set.

### Migration completion and dead-version GC

`migration_completion::MigrationCompletionChecker` runs as a scheduled task in DispatchIndexing
mode. It detects when all enabled namespaces have been re-indexed into new-prefix tables (by
comparing checkpoint entries against enabled namespaces), promotes the `migrating` version to
`active`, and retires the old active version. After promotion it clears the re-index campaign.

A single SQL query then enumerates all `v<N>_*` objects in `system.tables` whose version falls
outside a keep-set computed in the same query (active + newest retired within
`max_retained_versions` + migrating above active). Each candidate is validated against the
ontology before being dropped; unrecognized objects (e.g. rename-orphans) are logged and left
alone for a future migration framework to handle.

### Stale FK-edge reconciliation

`scheduler::StaleEdgeReconciliation` is a DispatchIndexing-mode `ScheduledTask` that tombstones
stale FK-derived edges. A mutable-FK "latest" edge (e.g. `HAS_LATEST_DIFF`) orphans its old row when
the FK changes, because `target_id` is part of the `ReplacingMergeTree` identity, so the prior
`(source, old_target)` row is never replaced. The task runs one idempotent `INSERT … SELECT` per
`(relationship_kind, FK-owner)` variant against the changed-owner set (`_version >= cursor`), pruned
to the changed set by a dual `IN` on the edge PK; the swept set is ontology-derived (edges marked
`mutable: true`), as is each variant's metadata. It runs directly in the dispatcher (not dispatched to indexer workers):
one cheap global sweep, off the insert hot path. See
`docs/design-documents/indexing/sdlc_indexing.md` ("Stale FK-edge reconciliation").

### Entry point

The `run()` function in `lib.rs` wires everything together: waits for the schema version to be
ready, connects to NATS and ClickHouse, registers handlers via `sdlc::register_handlers()`,
`code::register_handlers()`, and `namespace_deletion::register_handlers()`, builds the engine,
and runs until shutdown.

`IndexerConfig` holds all configuration (NATS, ClickHouse graph/datalake, engine concurrency, handler configs, GitLab client). Handler configs are typed via `HandlersConfiguration` in `configuration.rs` — no string-keyed lookups.

## Development

### Running tests

```shell
# Unit tests
cargo test --lib

# Integration tests (requires Docker, make sure to look for Colima if docker is not found)
cargo test --test '*'
```

### Test utilities

Located in `testkit/`:

- `MockNatsServices`, `MockDestination`, `MockHandler`
- `TestEngineBuilder` for integration tests
- `TestEnvelopeFactory` for message creation

## Common tasks

### Before writing a new handler: reuse existing infra

Most handler work re-derives infrastructure the crate already provides. Before scaffolding a
self-contained module, do an explicit "what does the codebase already give me?" pass. This is a
reuse-first **default**, not a hard rule — but in review it is the single most common class of
preventable feedback (see #2772, !1416). Check each of these first:

- **Paging + checkpoint + cursor:** `Pipeline::run_plan` and `EntityHandler`
  (`modules/sdlc/pipeline.rs`, `modules/sdlc/handler/entity.rs`) already provide windowed
  extraction, keyset cursor persistence (`cursor_values` / `to_checkpoint_values()`), and
  watermark advance. Reuse them before hand-rolling a page loop. For code indexing, reuse the
  checkpoint store in `modules/code/checkpoint.rs`.
- **Arrow extraction:** decode datalake `RecordBatch` rows with the `gkg_utils::arrow` helpers
  (`get_column`, `get_column_string`, `get_string_list`, `extract_row` in
  `crates/utils/src/arrow.rs`), not bespoke `col_i64` / `col_string` functions.
- **Edge/node `RecordBatch` specs:** derive column specs from the ontology — `edge_specs(ontology)`
  in `modules/code/arrow_converter.rs` (also `crates/duckdb-client/src/converter.rs`). Do not
  hardcode them; hardcoded specs silently drift from `config/graph.sql`.
- **Filtering:** push row filters into the extraction SQL (`WHERE`, `action IN (...)`) instead of
  post-filtering in Rust, so rows you discard never cross the wire.
- **Concurrency:** independent datalake lookups (routes / MR / work-item) should run concurrently
  (e.g. `tokio::try_join!`), not sequentially.
- **Constants:** prefer deriving values from the ontology or a typed config field over hardcoding
  magic numbers; if a value is environment-dependent, make it a `HandlersConfiguration` field.
- **Siphon columns:** hand-written datalake SQL must use
  `ontology::siphon_watermark_column()` and `ontology::siphon_deleted_column()`, never the
  literal column names. Both are derived at runtime from `schema.yaml`'s
  `settings.etl.default_watermark` / `default_deleted` via a `LazyLock<Ontology>`, so the
  ontology YAML is the single source of truth.

If none of the above fits and you genuinely need new infrastructure, prefer generalizing into a
shared place (`crates/utils/`, `modules/.../pipeline.rs`) over duplicating logic per handler.

Do not ship `#[allow(dead_code)]` to silence scaffold warnings — see the no-shipped-dead-code rule
below and in the root `AGENTS.md`.

### When a Rust transform is justified (ADR 015)

The SDLC transform stage is pluggable (`modules/sdlc/transform.rs`): the built-in `data_fusion`
transform is a row-wise SQL projection of one extracted block and is the **default** for every
node and standalone-edge plan. A hand-written `BlockTransform` (a derived entity's `etl.transform`)
is justified **only when the graph shape cannot be expressed as that SQL projection** — concretely,
when it needs:

- **multi-hop datalake reads** mid-transform (e.g. resolving GFM references to entity IDs via a
  second `IN`-list lookup against `siphon_routes` and entity tables), or
- **cross-row or free-text work** SQL can't do (parsing note bodies, fanning one source row into
  several edge kinds).

If the transform is a per-row projection of one extracted batch, express it as an ontology plan +
`data_fusion`, not Rust. SystemNote is the reference case for the Rust path (ADR 013). See
`docs/design-documents/decisions/015_pluggable_entity_pipelines.md`.

### Adding a handler

1. Run the **reuse-infra checklist above** before writing new code.
2. Define event type implementing `Event`
3. Create handler implementing `Handler` (`name`, `subscription`, `handle`)
4. Add topic config to `engine.topics` in `config/default.yaml` for retry/concurrency policy
5. If handler needs domain config, add a typed config field to `HandlersConfiguration` in `engine.rs`
6. Register in `sdlc::register_handlers()`, `code::register_handlers()`, or `namespace_deletion::register_handlers()`

### No `#[allow(dead_code)]` in shipped code

This crate denies bare `#[allow(..)]` attributes (`clippy::allow_attributes_without_reason = "deny"`
in `Cargo.toml`). Scaffold-era `#[allow(dead_code)]` markers must not survive into a merged MR:

- If a symbol is test-only, gate it with `#[cfg(test)]` instead of allowing dead code.
- If it is genuinely unused, delete it.
- If you must keep an exception, it has to carry an explicit `reason`
  (`#[allow(dead_code, reason = "…")]`) so the justification is reviewable, ideally linking an
  issue. Prefer `#[expect(dead_code, reason = "…")]`, which fails once the code becomes used and so
  self-cleans.

### No panics in the indexer data path

`handle`/transform/`emit` code processes untrusted production rows one at a time, so a `panic!`,
`unreachable!`, `unwrap`, or `expect` on a data-dependent branch can crash-loop the worker: one
malformed or unexpected row takes down every row behind it. Prefer **log-and-skip**
(`tracing::warn!` with enough context to debug — the unexpected value plus the relevant ids — then
`continue`) over panicking. Compute branch-dependent values in one fallible step that returns
`Option`/`Result` and skip on the unexpected case, rather than mirroring a match in two places and
needing an `unreachable!()` fallthrough in each (see the `contains_endpoints` helper in
`modules/sdlc/transform/system_notes/emit.rs`, the motivating case from !1559). Panicking is only
acceptable on a genuine programmer invariant that no production data can reach.

### New edge/action routing needs an end-to-end YAML scenario

When you route a new action (or noteable/edge kind) to a `gl_edge`, add an integration scenario
under `tests/integration-tests/tests/indexer/scenarios/sdlc/...` (run by `scenario_indexing`), not
just `emit.rs`/`parse.rs` unit tests. Seed the real siphon rows and assert the emitted `gl_edge`
rows — direction and `traversal_path`. Include any cross-namespace / `traversal_path` invariant
(e.g. a child in one namespace under a parent in another) so the partition-side property is guarded
end-to-end; unit tests that reuse one traversal path cannot catch a target-vs-source partition bug.
See `epic_and_task_hierarchy_emit_contains_edges.yaml` (!1559).

### Concurrency

- `max_concurrent_workers`: Global limit (default 16)
- Per-subscription concurrency groups configured via `engine.topics` in YAML
