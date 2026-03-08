# AGENTS.md

GitLab Knowledge Graph (Orbit). Rust service that builds a property graph from GitLab data and serves queries over gRPC/HTTP. 

## Quick start

All tasks use mise. `mise build`, `mise test:fast`, `mise lint:code`, `mise server:start`.
Fix linting issues: `mise lint:code:fix`. Validate docs: `mise lint:docs`. Validate ontology: `mise ontology:validate`.
Integration tests need Docker: `mise test:integration`.

## How the system works

- **Read-only from the GitLab perspective.** SDLC data flows via Siphon CDC (PostgreSQL logical replication → NATS → ClickHouse). Code data via Gitaly `GetArchive` gRPC. GKG only writes to its own ClickHouse tables.
- **Rails owns authorization.** GKG delegates all access decisions to Rails via gRPC (traversal IDs, resource permissions). See `docs/design-documents/security.md`.
- **ClickHouse = datalake + graph.** Datalake DB holds raw Siphon rows; graph DB holds indexed property graph tables. The indexer transforms between them.
- **Ontology-driven graph.** YAML in `fixtures/ontology/nodes/` drives ETL, query validation, and redaction. New entity types start there, not in Rust. Schema: `fixtures/ontology/ontology.schema.json`.
- **Single binary, four modes.** `gkg-server --mode` runs as Webserver, Indexer, DispatchIndexing, or HealthCheck.
- **Siphon and NATS are external.** [Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon) (Go, Analytics team) and NATS are consumed, not owned. Use `/related-repositories` for local checkouts.

## What CI enforces

- `AGENTS.md` and `CLAUDE.md` must be identical (`agent-file-sync-check`)
- Clippy with all features, warnings as errors (`lint-check`)
- Ontology YAML validated against JSON schema (`ontology-schema-validate`)
- `cargo fmt` (`fmt-check`)
- `cargo audit`, `cargo deny`, `cargo geiger` (security stage)
- Unit tests via nextest (`unit-test`)
- Integration tests with Docker testcontainers (`integration-test`)
- Gitaly integration tests with real Gitaly container (`gitaly-integration-test`)
- MR titles must follow conventional commit format: `type(scope): description` (`mr-title-check`)
- Markdown files must pass markdownlint, Vale, and lychee checks (`check-docs`)
- Helm chart linting and template validation (`helm-lint`)

## Where to find things

| What | Where |
|---|---|
| Architecture and data model | `docs/design-documents/data_model.md` |
| Security / AuthZ design | `docs/design-documents/security.md` |
| Query DSL spec | `docs/design-documents/querying/` |
| SDLC indexing pipeline | `docs/design-documents/indexing/sdlc_indexing.md` |
| Code indexing pipeline | `docs/design-documents/indexing/code_indexing.md` |
| Schema migration strategy | `docs/design-documents/schema_management.md` |
| Observability / SLOs | `docs/design-documents/observability.md` |
| Ontology node definitions | `fixtures/ontology/nodes/` |
| Ontology edge definitions | `fixtures/ontology/edges/` |
| Ontology JSON schema | `fixtures/ontology/ontology.schema.json` |
| Query test fixtures | `fixtures/queries/` |
| Schema fixtures | `fixtures/schema/` |
| gRPC service definition | `crates/gkg-server/proto/gkg.proto` |
| Server config structure | `crates/gkg-server/src/config.rs` |
| Dev environment setup | `docs/dev/INFRASTRUCTURE.md` |
| Local development guide | `docs/dev/local-development.md` |
| GitLab instance config | `docs/dev/GITLAB_INSTANCE.md` |
| Operational runbook | `docs/dev/RUNBOOK.md` |
| Helm charts (dev) | `helm-dev/gkg/`, `helm-dev/observability/` |
| **All project links** (repos, epics, infra, people, helm charts) | `README.md` (single source of truth) |
| Code history / dead code investigation | `/code-history` skill |
| Related repos and local paths | `/related-repositories` skill |

## Crate map

Single binary: `gkg-server` (4 modes: Webserver, Indexer, DispatchIndexing, HealthCheck via `--mode`).

| Crate | Role |
|---|---|
| `gkg-server` | HTTP/gRPC server, all 4 modes, JWT auth, config loading |
| `query-engine` | JSON DSL -> parameterized ClickHouse SQL, security context enforcement |
| `indexer` | NATS consumer, SDLC + code handler modules, worker pools, `testkit/` |
| `ontology` | Loads/validates YAML ontology, query validation helpers |
| `code-parser` | Multi-language parser (7 langs), tree-sitter + swc, extracts definitions/imports/references |
| `code-graph` | Builds in-memory property graphs from parsed code |
| `utils` | Shared ClickHouse parameter types (`ChScalar`, `ChType`) and Arrow extraction utilities |
| `clickhouse-client` | Async ClickHouse client, Arrow-IPC streaming |
| `gitaly-client` | Gitaly gRPC client, HMAC auth, GetArchive RPC |
| `siphon-proto` | Protobuf types for CDC replication events |
| `labkit-rs` | Logging, correlation IDs, OpenTelemetry metrics |
| `health-check` | K8s readiness/liveness probes |
| `treesitter-visit` | Tree-sitter language bindings wrapper |
| `cli` | Local `gkg index` and `gkg query` commands |
| `simulator` | Fake data generation + query correctness evaluation |
| `datalake-generator` | Synthetic GitLab data for load testing |

## Code quality

- No narration comments. Keep only *why* comments. Use `/remove-llm-comments` to clean up.
- Check crates.io for latest version before adding dependencies.
