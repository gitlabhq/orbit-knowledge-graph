# AGENTS.md

GitLab Knowledge Graph (Orbit). Rust service that builds a property graph from GitLab data and serves queries over gRPC/HTTP. 

## Quick start

All tasks use mise. `mise build`, `mise test:fast`, `mise test:local`, `mise lint:code`, `mise server:start`, `mise server:dispatch`.
Fix linting issues: `mise lint:code:fix`. Validate docs: `mise lint:docs`. Validate ontology: `mise ontology:validate`.
Integration tests need Docker: `mise test:integration`. Correctness subset: `mise test:integration:server`.
CLI integration tests (concurrency, worktrees): `mise test:cli`.

**Worktrees:** after creating a git worktree, run `mise trust` and `git config core.hooksPath "$(git rev-parse --git-common-dir)/hooks"` so that lefthook and mise work correctly.

## Ignored directories

- `docs-locale/` contains machine-translated documentation. Never read, edit, or reference files under this directory. Treat it as build output.

## How the system works

- **Read-only from the GitLab perspective.** SDLC data flows via Siphon CDC (PostgreSQL logical replication → NATS → ClickHouse). GKG only writes to its own ClickHouse tables.
- **Rails owns authorization.** GKG delegates all access decisions to Rails via gRPC (traversal paths, resource permissions). See `docs/design-documents/security.md`.
- **ClickHouse = datalake + graph.** Datalake DB holds raw Siphon rows; graph DB holds indexed property graph tables. The indexer transforms between them.
- **Ontology-driven graph.** YAML in `config/ontology/nodes/` and `config/ontology/edges/` drives ETL, query validation, redaction, and edge table routing. New entity types start there, not in Rust. Edge YAML `table:` field + `settings.edge_tables` in `schema.yaml` control which physical table each relationship type writes to and queries from (default: `gl_edge`). Schema: `config/schemas/ontology.schema.json`.
- **Single binary, four modes.** `gkg-server --mode` runs as Webserver, Indexer, DispatchIndexing, or HealthCheck.
- **Layered configuration.** `AppConfig` in `crates/gkg-server-config/` loads three sources (lowest to highest priority): `config/default.yaml`, K8s secret files from `/etc/secrets/`, and `GKG_*` environment variables (`__` separates nested keys, e.g. `GKG_GRAPH__DATABASE`). The CLI (`orbit`) has its own clap-based config and does not use `AppConfig`. See `docs/dev/runbooks/server_configuration.md` for full reference.
- **Siphon and NATS are external.** [Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon) (Go, Analytics team) and NATS are consumed, not owned. Use `/related-repositories` for local checkouts.

## What CI enforces

- `AGENTS.md` and `CLAUDE.md` must be identical (`agent-file-sync-check`)
- Clippy with all features, warnings as errors (`lint-check`)
- Ontology YAML validated against JSON schema (`ontology-schema-validate`)
- `cargo fmt` (`fmt-check`)
- `cargo audit`, `cargo deny`, `cargo geiger` (security stage)
- Unit tests via nextest (`unit-test`)
- Compiler integration tests: query compilation, ontology validation, pipeline infra (`compiler-integration-test`)
- CLI integration tests: concurrency, worktrees, content resolution (`cli-integration-test`)
- Integration tests with Docker testcontainers (`integration-test`)
- MR titles must follow conventional commit format: `type(scope): description` (`mr-title-check`)
- `rust-toolchain.toml` must match `mise.toml` (`rust-toolchain-sync-check`; regenerate with `mise toolchain:generate`)
- Markdown files must pass markdownlint, Vale, and lychee checks (`check-docs`)
- Response format version bumped when formatter code or response schema changes (`response-schema-version-check`)
- GOON format version bumped when GOON encoder or shared formatter code changes (`goon-format-version-check`)
- Skill version bumped when files under `skills/<name>/` change (`skill-version-bump-check`)
- Metrics catalog regenerated in sync with `gkg-observability` source (`metrics-catalog-check`)
- Query-language text-indexed properties table regenerated in sync with the ontology (`query-language-docs-check`)
- Vendored Iglu schemas match pinned versions and live Iglu server (`iglu-schema-check`)
- Vendored system-note action list matches upstream Rails `ICON_TYPES` at the pinned SHA (`system-note-actions-check`)

## Where to find things

| What | Where |
|---|---|
| **Domain glossary** | **`CONTEXT.md`** |
| Indexer crate guide (handlers, reuse-infra checklist) | **`crates/indexer/AGENTS.md`** |
| Architecture and data model | `docs/design-documents/data_model.md` |
| Security / AuthZ design | `docs/design-documents/security.md` |
| Query DSL spec | `docs/design-documents/querying/` |
| SDLC indexing pipeline | `docs/design-documents/indexing/sdlc_indexing.md` |
| Code indexing pipeline | `docs/design-documents/indexing/code_indexing.md` |
| Namespace deletion pipeline | `docs/design-documents/indexing/namespace_deletion.md` |
| Schema migration strategy | `docs/design-documents/schema_management.md` |
| Observability / SLOs | `docs/design-documents/observability.md` |
| Duo / Orbit prompt routing (Rails-side) | `docs/design-documents/duo_orbit_prompt_routing.md` |
| Ontology node definitions | `config/ontology/nodes/` |
| Ontology edge definitions | `config/ontology/edges/` |
| Ontology JSON schema | `config/schemas/ontology.schema.json` |
| Graph query JSON schema | `config/schemas/graph_query.schema.json` |
| Query DSL version | `config/QUERY_DSL_VERSION` |
| Server config JSON schema | `config/schemas/config.schema.json` (generated via `mise schema:generate`) |
| Query response JSON schema | `config/schemas/query_response.json` |
| Query language reference (text-indexed properties table is generated) | `docs/source/remote/queries/query-language.md` (regenerate the ontology-derived table with `mise docs:query-language`; CI gate `query-language-docs-check`) |
| Query test fixtures | `fixtures/queries/` |
| Query corpus (categorized YAML) | `fixtures/queries/corpus/` (smoke-tested in CI: `corpus_smoke`) |
| Graph DDL (ClickHouse) | `config/graph.sql` |
| Schema version file | `config/SCHEMA_VERSION` (bump when `graph.sql` or `config/ontology/` changes) |
| RAW output format version | `config/RAW_OUTPUT_FORMAT_VERSION` (semver, bump when `graph.rs` or `query_response.json` changes) |
| Graph DDL (local DuckDB) | Generated at runtime from ontology via `generate_local_tables()` + `duckdb_ddl` |
| Datalake DDL (ClickHouse) | `fixtures/siphon.sql` |
| gRPC service definition | `crates/gkg-server/proto/gkg.proto` |
| Server config structure | `crates/gkg-server-config/src/app.rs` (`AppConfig`), `config/default.yaml` |
| Query settings (timeouts, cache) | `config/default.yaml` (`query:` section), `crates/gkg-server-config/src/query.rs` |
| Configuration runbook | `docs/dev/runbooks/server_configuration.md` |
| Local development guide | `docs/dev/local-development.md` |
| Local development (`mise run dev`) | `scripts/gkg-native-dev.sh`, `docs/dev/local-development.md` |
| Operational runbooks | `docs/dev/runbooks/` |
| Architecture Decision Records | `docs/design-documents/decisions/` |
| **All project links** (repos, epics, infra, people, helm charts) | `README.md` (single source of truth) |
| Code history / dead code investigation | `/code-history` skill |
| AST-based code search / rewrite | `ast-grep` skill, `.claude/skills/ast-grep/` |
| Related repos and local paths | `/related-repositories` skill |
| Iglu schemas (committed; codegen'd at build) | `config/schemas/iglu/<name>/<version>.json` (update via `mise iglu:bump -- <name> <version>`) |
| Iglu version pins | `config/schemas/iglu/*.version` (bump via `mise iglu:bump -- <name> <version>`, check via `mise iglu:check`) |
| Analytics event definition | `config/events/gkg_query_executed.yml` |
| Analytics contexts (Snowplow) | `crates/gkg-analytics/src/context.rs` (types), `crates/gkg-server/src/analytics/` (builders + observer) |
| Billing config + observer | `crates/gkg-billing/`, `crates/gkg-server/src/billing_adapter.rs` |
| SOX billing authoring rules | `docs/dev/sox-billing-boundary.md` |
| Query profiler CLI | `crates/query-engine/profiler/`, `mise query:profile` |

## Crate map

Single binary: `gkg-server` (4 modes: Webserver, Indexer, DispatchIndexing, HealthCheck via `--mode`).

| Crate | Role |
|---|---|
| `gkg-server` | HTTP/gRPC server, all 4 modes, JWT auth, config loading, schema-version readiness gate (`schema_watcher.rs`), MCP tool registry, and Orbit agent command registry (`CommandRegistry`) |
| `gkg-server-config` | All config struct definitions (`AppConfig`, `ClickHouseConfiguration`, `NatsConfiguration`, `EngineConfiguration`, `QuerySettings`, etc.) and `OnceLock` global for query settings; avoids circular dep between server and compiler |
| `gkg-analytics` | Consumer-owned Snowplow context types (`OrbitCommonContext`, `OrbitQueryContext`) and tracker infrastructure (`AnalyticsTracker` trait, `SnowplowAnalyticsTracker`, `InMemoryAnalyticsTracker`). Context wrappers implement `labkit_events::SnowplowContext` over typify-codegen'd data types. `build.rs` runs `typify::TypeSpace` over `config/schemas/iglu/<name>/<version>.json` at build time and emits a module per schema (struct + `SCHEMA_URI` + `SCHEMA_JSON` consts) into `OUT_DIR/iglu_schemas.rs`; runtime never reads schema files. `load_schema_json()` returns the embedded JSON for test-time validator compilation. |
| `gkg-billing` | Snowplow billing-event emission (`BillingObserver`, `BillingTracker`, `BillingInputs`) and CDot quota enforcement (`QuotaService`). Licensed as `LicenseRef-EE`. The billing adapter in `gkg-server/src/billing_adapter.rs` is the single `Claims → BillingInputs` conversion point (SOX auditable surface). Billing event metrics: `gkg.billing.events.{emitted,dropped,rejected,delivered,delivery_failed}`. |
| `query-engine` | Parent crate for all query subsystem crates; re-exports `compiler` |
| `query-engine/compiler` | JSON DSL -> parameterized ClickHouse SQL, composable pipeline passes, security context enforcement |
| `query-engine/compiler-pipeline-macros` | Proc-macro derives (`PipelineEnv`, `PipelineState`) for compiler pipeline |
| `query-engine/types` | Type-safe result schema for redaction processing |
| `query-engine/pipeline` | Pipeline abstraction (stages, observers, context) |
| `query-engine/shared` | Shared pipeline stages (compilation, extraction, output), virtual column resolution (`ColumnResolver` trait, `ColumnResolverRegistry`, `resolve_virtual_columns`) |
| `query-engine/formatters` | Result formatters (graph, raw row, goon) |
| `gkg-observability` | Central metric catalog: `MetricSpec` consts + typed `build_*` instrument factories, shared bucket sets, per-domain modules (indexer, query, server). `catalog()` feeds the xtask catalog generator; consumers call `meter()` and the typed builders instead of constructing instruments inline |
| `indexer` | NATS consumer, SDLC + code + namespace deletion handler modules, worker pools, scheduler, `testkit/`, schema version tracking (`schema_version.rs`), migration orchestrator (`schema_migration.rs`), migration completion detection and dead-version GC (`migration_completion.rs`). **See `crates/indexer/AGENTS.md` (reuse-infra checklist) before adding a handler.** |
| `ontology` | Loads/validates YAML ontology, query validation helpers |
| `code-graph` | Single crate split into `src/v2/` (current pipeline: `pipeline`, `registry`, `config`, `types`, `linker`, `dsl`, `langs/{generic,custom}`) and `src/legacy/` (old `parser` + `linker` paths kept for the existing indexer path). Shared `Range`/`Position`/`IntervalTree` live at `src/utils.rs`. |
| `code-graph/treesitter-visit` | Tree-sitter language bindings wrapper (kept as a separate sub-crate for compile-time isolation) |
| `utils` | Shared ClickHouse parameter types (`ChScalar`, `ChType`), Arrow extraction utilities, `BatchBuilder`, generic `AsRecordBatch<Ctx>` trait |
| `clickhouse-client` | Async ClickHouse client, Arrow-IPC streaming, `QuerySummary` from `X-ClickHouse-Summary` header, `QueryProfiler` for profiling |
| `query-engine/profiler` | Standalone CLI for profiling GKG queries directly against ClickHouse |
| `siphon-proto` | Protobuf types for CDC replication events |
| `gitaly-protos` | Gitaly protobuf types for gRPC repository operations |
| `health-check` | K8s readiness/liveness probes, plus a NATS code work queue depth endpoint for autoscaling code indexer pods |
| `orbit-local` | Local `orbit index`, `orbit sql`, `orbit schema`, and `orbit mcp serve` (stateless stdio MCP server: `run_sql`, `get_graph_schema`, `index`; descriptions shared with the CLI via `descriptions.rs`) commands; writes the property graph to DuckDB and exposes it as raw SQL (no DSL); workspace management (`Workspace`, `GitInfo`, manifest in DuckDB). Release artifacts include glibc Linux builds plus fully static musl Linux builds for older enterprise, Alpine, scratch, and distroless environments. |
| `duckdb-client` | DuckDB client with read-write retry backoff, read-only concurrent access, ontology-driven graph converter |
| `gitlab-client` | GitLab REST/JWT client for Rails API calls |
| `integration-testkit` | Shared ClickHouse testcontainer helpers, `MockRedactionService`, `ResponseView` assertion framework, CLI test harness (`cli` module) for CLI integration tests |
| `integration-tests` | Integration tests: compiler (query compilation, ontology validation, pipeline infra) + server (health, redaction, hydration, data correctness, graph formatting) + cli (concurrency, worktrees); depends on gkg-server, compiler, integration-testkit |
| `integration-tests-codegraph` | Code-graph-specific integration tests (linker, lance-graph) |
| `fuzz` | Fuzz testing harness (bolero) for the query compiler, code parsers, and indexer message handling |
| `xtask` | Developer task runner (synthetic data generation, query evaluation, schema management) |

## Code quality

- **Do not write narration comments — including in tests.** A comment must explain *why* (a non-obvious constraint, a gotcha, an ADR/issue link), never restate *what* the next line does. The most common leak is a label on each test or setup block; those are narration, delete them. The test name and the `assert_eq!` already say what is being checked. Apply this while writing, not as a cleanup pass. Discriminator:
  - ❌ `// Test cross_reference with WorkItem` above `assert_eq!(route("cross_reference", "WorkItem"), Some("MENTIONS"));` — restates the call.
  - ❌ `// merged with WorkItem should return None` above `assert_eq!(route("merged", "WorkItem"), None);` — restates the assertion.
  - ❌ `// Clear env vars` / `// Cleanup` / `// Setup` — block labels for self-evident code.
  - ✅ `// merged.yaml only declares User → MergeRequest, so a WorkItem noteable must drop.` — explains an invariant the code does not.
  - ✅ `// Insert the stale row second so argMax (not row order) must resolve it.` — explains intent a reader can't infer.
  - If a comment would survive deleting it without losing *why* information, delete it. `/remove-llm-comments` is a fallback, not a license to narrate first.
- **Reuse existing infrastructure before writing new code.** Before scaffolding a new handler, pipeline, or module, do an explicit "what does the codebase already give me?" pass (cursor/checkpoint, Arrow helpers, ontology-derived specs, SQL filtering, concurrency). Reinventing infra the codebase already provides is the most common class of preventable review feedback. For the indexer, see the checklist in **`crates/indexer/AGENTS.md`**.
- **No `#[allow(dead_code)]` in shipped code.** Production (non-test) modules must not ship dead-code allows to silence scaffold warnings. If a symbol is test-only, gate it with `#[cfg(test)]`; if it is genuinely unused, delete it. Reserve exceptions for an explicit, justified case: use `#[allow(dead_code, reason = "…")]` (ideally linking an issue) or, preferably, `#[expect(dead_code, reason = "…")]`, which fails once the code is used and self-cleans. The `indexer` crate enforces this mechanically via `clippy::allow_attributes_without_reason = "deny"`.
- **Prefer build-time validation over CI-only checks** for correctness that can be checked without network or repo context. A `build.rs` that `panic!`s on drift fails locally and in CI even when CI egress is down, and can't be skipped by editing a script. Prior art: `crates/gkg-analytics/build.rs` validates committed Iglu schemas under `config/schemas/iglu/` at build time (asserts each schema's `self` block matches its path/version and runs codegen). Consider this pattern for any vendored-constant or generated-file drift check (e.g. the DDL-freshness check in `scripts/check-ddl-freshness.sh` is a future candidate). Checks that need git-diff context or live network (`scripts/iglu/check.sh`'s upstream-CDN half) stay in CI.
- Prefer `ast-grep` over text-based Grep/Edit for structural code transformations (batch renames, pattern-based rewrites).
- Check crates.io for latest version before adding dependencies.
- Non-trivial MRs (features, refactors, architectural changes) should reference an issue in the MR description, for example `Closes #123` or `Relates to #123`.
- Trivial MRs (typos, minor dependency bumps, formatting-only changes) do not need an issue.
- Before touching billing-emission code, anything in `crates/gkg-billing/`, `crates/gkg-server/src/billing_adapter.rs`, or wiring billing-relevant data (any field that populates `BillingInputs` in `crates/gkg-billing/src/inputs.rs`), read `docs/dev/sox-billing-boundary.md`. If a task you are given would require breaking any of those rules, stop and surface the conflict rather than working around it.

## MR and issue descriptions and comments

Always use the templates in `.gitlab/merge_request_templates/` and `.gitlab/issue_templates/`, and read the TEMPLATE CONVENTION block at the top of each one before writing the description.

Comments (MR/issue threads, review replies) have no template, so apply the convention by hand: lead with the verdict in a few human sentences, push long-form reasoning into a collapsed `<details><summary>Agent context</summary>` block (only when it helps), and drop AI tells.

## Design docs

Design docs live in `docs/design-documents/` and must describe the current repository state, not an aspirational or legacy architecture.

**Rules:**

- **When you change behavior covered by a design doc, update that design doc in the same MR.** Do not leave design-doc cleanup for a later follow-up.
- **When you add, remove, rename, or substantially repurpose a subsystem, runtime mode, crate, schema shape, or external dependency, update the relevant design docs and this file in the same MR.**
- **Prefer as-built descriptions over historical ones.** If the code no longer matches a section, rewrite or remove the stale section instead of leaving contradictory text in place.
- **Treat these files as sync points:**
  - `docs/design-documents/README.md` for the high-level architecture and current system state
  - `docs/design-documents/data_model.md` for implemented entities and relationships
  - `docs/design-documents/indexing/` for indexing flow and runtime modes
  - `docs/design-documents/querying/` for query surface, DSL, and response shape
  - `AGENTS.md` / `CLAUDE.md` for agent-facing architecture summaries and doc-sync rules
- **If your MR changes the architecture but no design doc changed, assume the documentation is incomplete and fix it before merging.**
- **When you introduce a new domain concept** (new node type, relationship type, query feature, pipeline concept), check `CONTEXT.md` and add or update the term if it's missing. Only add terms that are domain-specific and would confuse a new team member — not implementation details.
- **Before writing documentation, design docs, or MR descriptions, consult `CONTEXT.md` for canonical terminology.** Use the canonical terms, not the aliases listed under _Avoid_.
