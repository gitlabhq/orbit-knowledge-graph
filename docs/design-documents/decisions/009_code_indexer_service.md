---
title: "GKG ADR 009: Separate code indexer service"
creation-date: "2026-04-22"
authors: [ "@michaelangeloio" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-04-22

## Context

The `gkg-server --mode=indexer` process currently runs all three handler families in a single Engine: SDLC indexing, code indexing, and namespace deletion. They share one NATS consumer loop, one `WorkerPool`, and one set of resource limits.

Code indexing and SDLC indexing have fundamentally different resource profiles:

| Characteristic | SDLC indexing | Code indexing |
|---|---|---|
| CPU | Low (SQL transforms on ClickHouse data) | High (tree-sitter/OXC/Prism parsing, SSA analysis) |
| Memory | Moderate (Arrow batches) | High (full repository archives held in memory, AST structures) |
| Disk | None | Up to 10 Gi ephemeral (archive extraction, scratch space) |
| Network | ClickHouse queries | Rails archive downloads (large payloads), ClickHouse writes |
| Concurrency pattern | Many small fast tasks | Few large slow tasks |
| External dependencies | ClickHouse datalake + graph | GitLab Rails API + ClickHouse graph |
| Failure blast radius | A slow namespace blocks the concurrency group | A large repo OOM kills the pod, taking SDLC handlers with it |

Today the Helm chart allocates resources for the worst case (code indexing): 8 CPU, 16 Gi memory, 15 Gi ephemeral storage, and a 10 Gi `/tmp` emptyDir. SDLC indexing alone would need a fraction of that.

### Problems this creates

1. **Scaling is coupled.** Scaling up code indexing means scaling up SDLC indexing pods too, wasting resources on the SDLC side.
2. **Fault isolation.** A code indexing OOM (large repo archive, deep AST) kills the entire pod, including any in-flight SDLC work. NATS redelivers both, but the disruption is unnecessary.
3. **Deployment coupling.** Code-graph parser changes (new language support, OXC upgrades) require redeploying the same binary that runs SDLC indexing. A parser regression affects SDLC availability.
4. **Resource tuning.** The concurrency groups in `engine.concurrency_groups` are configured in one ConfigMap. Tuning code indexing parallelism (fewer workers, more memory per worker) requires redeploying the combined indexer.

## Decision

Split code indexing into a dedicated `--mode=code-indexer` service. The existing `--mode=indexer` continues to run SDLC and namespace deletion handlers only.

### What changes

#### 1. New `Mode::CodeIndexer` variant

Add `CodeIndexer` to the `Mode` enum in `crates/gkg-server/src/cli/mod.rs`. The CLI flag value would be `code-indexer`.

#### 2. New entry point in the indexer crate

Add `run_code_indexer()` to `crates/indexer/src/lib.rs`. This function:

- Connects to NATS and ClickHouse (graph DB only, no datalake needed)
- Connects to the GitLab Rails API (for archive downloads)
- Runs schema migration check (same as today)
- Calls only `code::register_handlers()` to build the `HandlerRegistry`
- Builds and runs the `Engine`

A new `CodeIndexerConfig` struct (or a subset of `IndexerConfig`) provides the configuration, including its own `EngineConfiguration` with code-specific concurrency groups.

#### 3. Remove code handlers from the SDLC indexer

The existing `run()` function stops calling `code::register_handlers()`. It only registers SDLC and namespace deletion handlers.

To support a transition period where operators may not have deployed the new service yet, this could be gated on a config flag (e.g., `indexer.code_indexing_enabled: false` as the new default). The flag would be removed once the migration is complete.

#### 4. Helm chart: new `codeIndexer` deployment

In `gkg-helm-charts`, add a new deployment under `templates/code-indexer/`:

| File | Purpose |
|---|---|
| `deployment.yaml` | Deployment with `--mode=code-indexer`, code-specific resources |
| `configmap.yaml` | Config with `nats`, `graph`, `gitlab`, `engine` (code concurrency groups only) |
| `podmonitor.yaml` | Prometheus scraping |

The new deployment would have:

- **Separate resource limits** tuned for code workloads (high CPU/memory, ephemeral storage for archives)
- **Independent replica count** (`codeIndexer.replicas`)
- **Its own concurrency groups** in the Engine config, sized for code work (fewer concurrent workers, more resources each)
- **The `/tmp` emptyDir volume** moves here (the SDLC indexer no longer needs it)
- **Same secret mounts** as the current indexer: `datalakePassword` (if checkpoint store needs it), `graphPassword`, `gitlabJwtSigningKey`

The existing `indexer/` templates would be updated to:

- Remove the `/tmp` emptyDir (or reduce its size)
- Lower resource limits (no longer needs 8 CPU / 16 Gi for code parsing)
- Remove the `gitlab` config section (SDLC indexing does not call Rails)
- Remove `gitlabJwtSigningKey` from secret mounts
- Remove code-specific concurrency groups from the Engine config

#### 5. Health-check targets

The health-check deployment's `targets` list in `values.yaml` would add the new `gkg-code-indexer` deployment so it is monitored alongside the existing indexer and dispatcher.

### What does not change

- **Dispatching stays as-is.** The DispatchIndexing mode already publishes code tasks independently. `SiphonCodeIndexingTaskDispatcher` and `NamespaceCodeBackfillDispatcher` publish to `code.task.indexing.requested.*.*` regardless of which service consumes them.
- **NATS subjects are already separate.** Code: `code.task.indexing.requested.*.*`. SDLC: `sdlc.global.indexing.requested`, `sdlc.namespace.indexing.requested.*.*`. No routing changes.
- **Namespace deletion stays in the SDLC indexer.** It deletes from all ontology tables (including code tables), but it is a lightweight ClickHouse `DELETE` operation that does not need code-specific resources. Moving it would add complexity for no benefit.
- **Schema migration coordination.** Both services check `schema_version` on startup via the same mechanism. The `NamespaceCodeBackfillDispatcher` triggers code backfill during migrations from the dispatch side. The code indexer just consumes those messages.
- **The `code` module in the indexer crate.** No structural changes to `crates/indexer/src/modules/code/`. It is already self-contained.

## Why not the alternatives

### Feature flag on the existing indexer

A config flag like `indexer.modules: [sdlc, code, namespace_deletion]` would let operators choose which handlers run in each pod. This works but pushes deployment topology decisions into application config. Separate `--mode` values keep the deployment intent explicit and match the existing pattern (`webserver`, `indexer`, `dispatch-indexing`, `health-check`).

### Separate binary

A fully separate binary (e.g., `gkg-code-indexer`) would provide the strongest isolation. But the code indexer shares the same `Engine`, `Handler` trait, NATS consumer, ClickHouse client, schema migration, and metrics infrastructure as the SDLC indexer. Duplicating or extracting all of that into a shared library adds build complexity for marginal isolation benefit. A new `--mode` on the same binary is consistent with the existing architecture.

### Move code indexing to a different crate

The `code` module could be extracted to its own crate. This is orthogonal to the service split: crate boundaries are a compile-time concern, mode selection is a runtime concern. The module is already well-isolated within the indexer crate. If compile times become an issue, extracting the module to a crate can be done independently.

## Consequences

**What improves:**

- Code and SDLC indexing scale independently. Operators can run 10 code indexer replicas and 3 SDLC indexer replicas based on actual workload.
- A code indexing OOM does not disrupt SDLC work.
- Resource limits are right-sized per workload. SDLC pods drop from 16 Gi to a fraction of that. Code pods keep the high limits.
- Code-graph parser changes (new language, OXC upgrade) can be deployed and tested on the code indexer without touching SDLC.
- Monitoring and alerting can distinguish code indexing health from SDLC indexing health at the pod level.

**What gets harder:**

- One more deployment to manage. Helm values grow by one top-level section (`codeIndexer`).
- During the transition, operators need to deploy both the updated indexer (without code handlers) and the new code indexer. The config flag on the old indexer mitigates this.
- Health-check targets need updating.
- Three deployments now share the `GKG_INDEXER` NATS stream (SDLC indexer, code indexer, namespace deletion in the SDLC indexer). NATS consumer groups already handle this, but operators should verify consumer names do not collide.

## Implementation plan

1. Add `Mode::CodeIndexer` to the CLI enum and wire it in `main.rs`
2. Add `run_code_indexer()` to `crates/indexer/src/lib.rs` with `CodeIndexerConfig`
3. Add a config flag to `run()` to optionally skip `code::register_handlers()`
4. Add `templates/code-indexer/` to the Helm chart (deployment, configmap, podmonitor)
5. Update `templates/indexer/` to remove code-specific config and reduce resources
6. Update `templates/health-check/configmap.yaml` to add the new deployment target
7. Update `AGENTS.md` / `CLAUDE.md` crate map and mode descriptions
8. Update `docs/design-documents/indexing/README.md` architecture diagram to show the split
9. Update `docs/design-documents/indexing/code_indexing.md` to reflect the new mode

## References

- [Code indexing design document](../indexing/code_indexing.md)
- [SDLC indexing design document](../indexing/sdlc_indexing.md)
- [Indexer crate: handler registry](../../../crates/indexer/src/handler.rs)
- [Server CLI: mode enum](../../../crates/gkg-server/src/cli/mod.rs)
