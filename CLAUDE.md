# AGENTS.md

GitLab Knowledge Graph (Orbit). Rust service that builds a property graph from GitLab data and serves queries over gRPC/HTTP. 

## Quick start

All tasks use mise. `mise build`, `mise test:fast`, `mise test:local`, `mise lint:code`, `mise server:start`, `mise server:dispatch`.
Fix linting issues: `mise lint:code:fix`. Validate docs: `mise lint:docs`. Validate ontology: `mise ontology:validate`.
Integration tests need Docker: `mise test:integration`. Correctness subset: `mise test:integration:server`.
CLI integration tests (concurrency, worktrees): `mise test:cli`.

**Worktrees:** after creating a git worktree, run `mise trust` and `git config core.hooksPath "$(git rev-parse --git-common-dir)/hooks"` so that lefthook and mise work correctly.

## E2E local development (native dev)

For E2E local development setup and workflow, see `docs/dev/local-development.md`.

## How the system works

- **Read-only from the GitLab perspective.** SDLC data flows via Siphon CDC (PostgreSQL logical replication → NATS → ClickHouse). GKG only writes to its own ClickHouse tables.
- **Rails owns authorization.** GKG delegates all access decisions to Rails via gRPC (traversal IDs, resource permissions). See `docs/design-documents/security.md`.
- **ClickHouse = datalake + graph.** Datalake DB holds raw Siphon rows; graph DB holds indexed property graph tables. The indexer transforms between them.
- **Ontology-driven graph.** YAML in `config/ontology/nodes/` drives ETL, query validation, and redaction. New entity types start there, not in Rust. Schema: `config/schemas/ontology.schema.json`.
- **Single binary, four modes.** `gkg-server --mode` runs as Webserver, Indexer, DispatchIndexing, or HealthCheck.
- **Siphon and NATS are external.** [Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon) (Go, Analytics team) and NATS are consumed, not owned. Use `/related-repositories` for local checkouts.

## What CI enforces

- `AGENTS.md` and `CLAUDE.md` must be identical (`agent-file-sync-check`)
- Clippy with all features, warnings as errors (`lint-check`)
- Ontology YAML validated against JSON schema (`ontology-schema-validate`)
- `cargo fmt` (`fmt-check`)
- `cargo audit`, `cargo deny`, `cargo geiger` (security stage)
- Unit tests via nextest, includes compiler tests (`unit-test`)
- CLI integration tests: concurrency, worktrees, content resolution (`cli-integration-test`)
- Integration tests with Docker testcontainers (`integration-test`)
- MR titles must follow conventional commit format: `type(scope): description` (`mr-title-check`)
- Markdown files must pass markdownlint, Vale, and lychee checks (`check-docs`)

## Where to find things

| What | Where |
|---|---|
| Architecture and data model | `docs/design-documents/data_model.md` |
| Security / AuthZ design | `docs/design-documents/security.md` |
| Query DSL spec | `docs/design-documents/querying/` |
| SDLC indexing pipeline | `docs/design-documents/indexing/sdlc_indexing.md` |
| Code indexing pipeline | `docs/design-documents/indexing/code_indexing.md` |
| Namespace deletion pipeline | `docs/design-documents/indexing/namespace_deletion.md` |
| Schema migration strategy | `docs/design-documents/schema_management.md` |
| Observability / SLOs | `docs/design-documents/observability.md` |
| Ontology node definitions | `config/ontology/nodes/` |
| Ontology edge definitions | `config/ontology/edges/` |
| Ontology JSON schema | `config/schemas/ontology.schema.json` |
| Graph query JSON schema | `config/schemas/graph_query.schema.json` |
| Server config JSON schema | `config/schemas/config.schema.json` (generated via `mise schema:generate`) |
| Query response JSON schema | `crates/gkg-server/schemas/query_response.json` |
| Query test fixtures | `fixtures/queries/` |
| Graph DDL (ClickHouse) | `config/graph.sql` |
| Schema version file | `config/SCHEMA_VERSION` (bump when `graph.sql` or `config/ontology/` changes) |
| Graph DDL (local DuckDB) | Generated at runtime from ontology via `generate_local_tables()` + `duckdb_ddl` |
| Datalake DDL (ClickHouse) | `fixtures/siphon.sql` |
| gRPC service definition | `crates/gkg-server/proto/gkg.proto` |
| Server config structure | `crates/gkg-server/src/config.rs` |
| Query settings (timeouts, cache) | `config/default.yaml` (`query:` section), `crates/gkg-server-config/` |
| Dev environment setup | `docs/dev/INFRASTRUCTURE.md` |
| Local development guide | `docs/dev/local-development.md` |
| Local development (`mise run dev`) | `scripts/gkg-native-dev.sh`, `docs/dev/local-development.md` |
| GitLab instance config | `docs/dev/GITLAB_INSTANCE.md` |
| Operational runbooks | `docs/dev/runbooks/` |
| Architecture Decision Records | `docs/design-documents/decisions/` |
| Helm charts | `helm/gkg/` (vendored via vendir) |
| **All project links** (repos, epics, infra, people, helm charts) | `README.md` (single source of truth) |
| Code history / dead code investigation | `/code-history` skill |
| AST-based code search / rewrite | `ast-grep` skill, `.claude/skills/ast-grep/` |
| Related repos and local paths | `/related-repositories` skill |
| Query profiler CLI | `crates/query-engine/profiler/`, `mise query:profile` |

## Crate map

Single binary: `gkg-server` (4 modes: Webserver, Indexer, DispatchIndexing, HealthCheck via `--mode`).

| Crate | Role |
|---|---|
| `gkg-server` | HTTP/gRPC server, all 4 modes, JWT auth, config loading |
| `gkg-server-config` | Shared query config types (`QueryConfig`, `QuerySettings`) and `OnceLock` global; avoids circular dep between server and compiler |
| `query-engine` | Parent crate for all query subsystem crates; re-exports `compiler` |
| `query-engine/compiler` | JSON DSL -> parameterized ClickHouse SQL, composable pipeline passes, security context enforcement |
| `query-engine/compiler-pipeline-macros` | Proc-macro derives (`PipelineEnv`, `PipelineState`) for compiler pipeline |
| `query-engine/types` | Type-safe result schema for redaction processing |
| `query-engine/pipeline` | Pipeline abstraction (stages, observers, context) |
| `query-engine/shared` | Shared pipeline stages (compilation, extraction, output), virtual column resolution (`ColumnResolver` trait, `ColumnResolverRegistry`, `resolve_virtual_columns`) |
| `query-engine/formatters` | Result formatters (graph, raw row, goon) |
| `indexer` | NATS consumer, SDLC + code + namespace deletion handler modules, worker pools, scheduler, `testkit/`, schema version tracking (`schema_version.rs`), migration orchestrator (`schema_migration.rs`), migration completion detection and table cleanup (`migration_completion.rs`) |
| `ontology` | Loads/validates YAML ontology, query validation helpers |
| `code-graph` | Parent crate for code parsing and graph construction; re-exports `treesitter-visit`, `parser-core`, `code-graph-linker` |
| `code-graph/treesitter-visit` | Tree-sitter language bindings wrapper |
| `code-graph/parser` | Multi-language parser (7 langs), tree-sitter + swc, extracts definitions/imports/references |
| `code-graph/linker` | Builds in-memory property graphs from parsed code, `AsRecordBatch<RowContext>` impls for all node types, `ResolvedEdge` for edge serialization |
| `utils` | Shared ClickHouse parameter types (`ChScalar`, `ChType`), Arrow extraction utilities, `BatchBuilder`, generic `AsRecordBatch<Ctx>` trait |
| `clickhouse-client` | Async ClickHouse client, Arrow-IPC streaming, `QuerySummary` from `X-ClickHouse-Summary` header, `QueryProfiler` for profiling |
| `query-engine/profiler` | Standalone CLI for profiling GKG queries directly against ClickHouse |
| `siphon-proto` | Protobuf types for CDC replication events |
| `labkit-rs` | Logging, correlation IDs, OpenTelemetry metrics |
| `health-check` | K8s readiness/liveness probes |
| `cli` | Local `orbit index`, `orbit query`, and `orbit compile` commands; DuckDB pipeline with hydration + virtual column resolution from filesystem; workspace management (`Workspace`, `GitInfo`, manifest in DuckDB) |
| `duckdb-client` | DuckDB client with read-write retry backoff, read-only concurrent access, ontology-driven graph converter |
| `gitlab-client` | GitLab REST/JWT client for Rails API calls |
| `integration-testkit` | Shared ClickHouse testcontainer helpers, `MockRedactionService`, `ResponseView` assertion framework, CLI test harness (`cli` module) for CLI integration tests |
| `integration-tests` | Integration tests: compiler (query compilation, ontology validation, pipeline infra) + server (health, redaction, hydration, data correctness, graph formatting) + cli (concurrency, worktrees); depends on gkg-server, compiler, integration-testkit |
| `xtask` | Developer task runner (data generation, query evaluation, ClickHouse management) |

## Code quality

- No narration comments. Keep only *why* comments. Use `/remove-llm-comments` to clean up.
- Prefer `ast-grep` over text-based Grep/Edit for structural code transformations (batch renames, pattern-based rewrites).
- Check crates.io for latest version before adding dependencies.
- Non-trivial MRs (features, refactors, architectural changes) should reference an issue in the MR description, for example `Closes #123` or `Relates to #123`.
- Trivial MRs (typos, minor dependency bumps, formatting-only changes) do not need an issue.

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
