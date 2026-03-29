# AGENTS.md

GitLab Knowledge Graph (Orbit). Rust service that builds a property graph from GitLab data and serves queries over gRPC/HTTP. 

## Quick start

All tasks use mise. `mise build`, `mise test:fast`, `mise test:local`, `mise lint:code`, `mise server:start`, `mise server:dispatch`.
Fix linting issues: `mise lint:code:fix`. Validate docs: `mise lint:docs`. Validate ontology: `mise ontology:validate`.
Integration tests need Docker: `mise test:integration`. Correctness subset: `mise test:integration:server`.

**Worktrees:** after creating a git worktree, run `mise trust` and `git config core.hooksPath "$(git rev-parse --git-common-dir)/hooks"` so that lefthook and mise work correctly.

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
| Query response JSON schema | `crates/gkg-server/schemas/query_response.json` |
| Query test fixtures | `fixtures/queries/` |
| Graph DDL (ClickHouse) | `config/graph.sql` |
| Datalake DDL (ClickHouse) | `fixtures/siphon.sql` |
| gRPC service definition | `crates/gkg-server/proto/gkg.proto` |
| Server config structure | `crates/gkg-server/src/config.rs` |
| Dev environment setup | `docs/dev/INFRASTRUCTURE.md` |
| Local development guide | `docs/dev/local-development.md` |
| GitLab instance config | `docs/dev/GITLAB_INSTANCE.md` |
| Operational runbooks | `docs/dev/runbooks/` |
| Architecture Decision Records | `docs/design-documents/decisions/` |
| Helm charts | `helm/gkg/` (vendored via vendir), `helm/local/` (dev Prometheus + Grafana) |
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
| `query-engine` | Parent crate for all query subsystem crates; re-exports `compiler` |
| `query-engine/compiler` | JSON DSL -> parameterized ClickHouse SQL, composable pipeline passes, security context enforcement |
| `query-engine/compiler-pipeline-macros` | Proc-macro derives (`PipelineEnv`, `PipelineState`) for compiler pipeline |
| `query-engine/types` | Type-safe result schema for redaction processing |
| `query-engine/pipeline` | Pipeline abstraction (stages, observers, context) |
| `query-engine/shared` | Shared pipeline stages (compilation, extraction, output) |
| `query-engine/formatters` | Result formatters (graph, raw row, goon) |
| `indexer` | NATS consumer, SDLC + code + namespace deletion handler modules, worker pools, scheduler, `testkit/` |
| `ontology` | Loads/validates YAML ontology, query validation helpers |
| `code-parser` | Multi-language parser (7 langs), tree-sitter + swc, extracts definitions/imports/references |
| `code-graph` | Builds in-memory property graphs from parsed code |
| `utils` | Shared ClickHouse parameter types (`ChScalar`, `ChType`) and Arrow extraction utilities |
| `clickhouse-client` | Async ClickHouse client, Arrow-IPC streaming, `QueryProfiler` for per-query stats |
| `query-engine/profiler` | Standalone CLI for profiling GKG queries directly against ClickHouse |
| `siphon-proto` | Protobuf types for CDC replication events |
| `labkit-rs` | Logging, correlation IDs, OpenTelemetry metrics |
| `health-check` | K8s readiness/liveness probes |
| `treesitter-visit` | Tree-sitter language bindings wrapper |
| `cli` | Local `orbit index` and `orbit query` commands |
| `gitlab-client` | GitLab REST/JWT client for Rails API calls |
| `integration-testkit` | Shared ClickHouse testcontainer helpers, `MockRedactionService`, and `ResponseView` assertion framework for integration tests |
| `integration-tests` | Integration tests: compiler (query compilation, ontology validation, pipeline infra) + server (health, redaction, hydration, data correctness, graph formatting); depends on gkg-server, compiler, integration-testkit |
| `xtask` | Developer task runner (data generation, query evaluation, ClickHouse management) |

## Code quality

- No narration comments. Keep only *why* comments. Use `/remove-llm-comments` to clean up.
- Prefer `ast-grep` over text-based Grep/Edit for structural code transformations (batch renames, pattern-based rewrites).
- Check crates.io for latest version before adding dependencies.
