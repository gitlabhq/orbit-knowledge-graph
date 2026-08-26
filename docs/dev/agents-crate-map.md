# Crate map

Single binary: `gkg-server` (4 modes: Webserver, Indexer, DispatchIndexing, HealthCheck via `--mode`).

| Crate | Role |
|---|---|
| `orbit-server` | HTTP/gRPC server, all 4 modes, JWT auth, config loading, schema-version readiness gate (`schema_watcher.rs`), MCP tool registry, and Orbit agent command registry (`CommandRegistry`) |
| `orbit-server-config` | All config struct definitions (`AppConfig`, `ClickHouseConfiguration`, `NatsConfiguration`, `EngineConfiguration`, `QuerySettings`, etc.) and `OnceLock` global for query settings; avoids circular dep between server and compiler |
| `orbit-analytics` | Consumer-owned Snowplow context types (`OrbitCommonContext`, `OrbitQueryContext`) and tracker infrastructure (`AnalyticsTracker` trait, `SnowplowAnalyticsTracker`, `InMemoryAnalyticsTracker`). Context wrappers implement `labkit_events::SnowplowContext` over typify-codegen'd data types. `build.rs` runs `typify::TypeSpace` over `config/schemas/iglu/<name>/<version>.json` at build time and emits a module per schema (struct + `SCHEMA_URI` + `SCHEMA_JSON` consts) into `OUT_DIR/iglu_schemas.rs`; runtime never reads schema files. `load_schema_json()` returns the embedded JSON for test-time validator compilation. |
| `orbit-billing` | Snowplow billing-event emission (`BillingObserver`, `BillingTracker`, `BillingInputs`) and CDot quota enforcement (`QuotaService`). Licensed as `LicenseRef-EE`. The billing adapter in `orbit-server/src/billing_adapter.rs` is the single `Claims → BillingInputs` conversion point (SOX auditable surface). Billing event metrics: `gkg.billing.events.{emitted,dropped,rejected,delivered,delivery_failed}`. |
| `orbit-prompts` | Embeds the versioned YAML prompts under `config/prompts/` via rust-embed and validates them (consumer build scripts fail the build on a malformed prompt) |
| `query-engine` | Parent crate for all query subsystem crates; re-exports `compiler` |
| `query-engine/compiler` | JSON DSL -> parameterized ClickHouse SQL, composable pipeline passes, security context enforcement |
| `query-engine/compiler-pipeline-macros` | Proc-macro derives (`PipelineEnv`, `PipelineState`) for compiler pipeline |
| `query-engine/types` | Type-safe result schema for redaction processing |
| `query-engine/pipeline` | Pipeline abstraction (stages, observers, context) |
| `query-engine/shared` | Shared pipeline stages (compilation, extraction, output), virtual column resolution (`ColumnResolver` trait, `ColumnResolverRegistry`, `resolve_virtual_columns`) |
| `query-engine/formatters` | Result formatters (graph, raw row, goon) |
| `orbit-observability` | Central metric catalog: `MetricSpec` consts + typed `build_*` instrument factories, shared bucket sets, per-domain modules (indexer, query, server). `catalog()` feeds the xtask catalog generator; consumers call `meter()` and the typed builders instead of constructing instruments inline |
| `indexer` | NATS consumer, SDLC + code + namespace deletion handler modules, worker pools, scheduler, `testkit/`, schema version tracking (`schema/version.rs`), migration orchestrator (`schema/migration.rs`), migration completion detection and dead-version GC (`orchestrator/scheduled/migration_completion.rs`). **See `crates/indexer/AGENTS.md` (reuse-infra checklist) before adding a handler.** |
| `ontology` | Loads/validates YAML ontology, query validation helpers |
| `code-graph` | Code parsing and linking pipeline under `src/v2/` (`pipeline`, `registry`, `config`, `types`, `linker`, `dsl`, `langs/{generic,custom}`); the old `src/legacy/` parser and linker have been removed. Shared `Range`/`Position`/`IntervalTree` live at `src/utils.rs`. |
| `code-graph/treesitter-visit` | Tree-sitter language bindings wrapper (kept as a separate sub-crate for compile-time isolation) |
| `utils` | Shared ClickHouse parameter types (`ChScalar`, `ChType`), Arrow extraction utilities, `BatchBuilder`, generic `AsRecordBatch<Ctx>` trait, strict-mode YAML parse/serialize helpers (`yaml`) |
| `clickhouse-client` | Async ClickHouse client, Arrow-IPC streaming, `QuerySummary` from `X-ClickHouse-Summary` header, `QueryProfiler` for profiling |
| `named-queries` | Named query templates: parses/validates YAML under `config/named_queries/`, embeds it via `rust-embed`, and renders `$binding` placeholders (server-derived `BindingValues`) and `$param` placeholders (client-supplied, JSON-schema-validated per template); used by `orbit-server`'s build script (compile-time validation with declared `example` values) and at runtime for `QUERY_TYPE_NAMED` execution |
| `nats-client` | Shared NATS client wrapper (`NatsClient`), KV bucket services (`KvServices`), circuit-breaking decorator (`CircuitBreakingNatsClient`), testkit feature |
| `circuit-breaker` | Generic circuit breaker (`CircuitBreaker`, `CircuitBreakerRegistry`, `CircuitBreakableError`) with observer hooks and per-service config |
| `query-engine/profiler` | Standalone CLI for profiling GKG queries directly against ClickHouse |
| `dispatch-profiler` | Standalone CLI that seeds a production-scale synthetic datalake in ClickHouse and profiles the dispatcher's code-backfill enumeration for peak memory |
| `gitaly-protos` | Gitaly protobuf types for gRPC repository operations |
| `health-check` | Networked aggregate health for configured Kubernetes workloads and ClickHouse, plus NATS code work queue depth for autoscaling code indexer pods |
| `orbit-local` | Local `orbit index`, `orbit sql`, `orbit schema`, and `orbit mcp serve` (stateless stdio MCP server: `run_sql`, `get_graph_schema`, `index`; descriptions shared with the CLI via `descriptions.rs`) commands; writes the property graph to DuckDB and exposes it as raw SQL (no DSL); workspace management (`Workspace`, `GitInfo`, manifest in DuckDB). Release artifacts include glibc Linux builds plus fully static musl Linux builds for older enterprise, Alpine, scratch, and distroless environments. |
| `orbit-search` | Store-agnostic graph search engine: identifier tokenizer with camelCase split + Snowball stem (`search_document` feeds `gl_definition.search_text`), stopword/decompounding query processing, tiered BM25 ranking with weak-match signaling, ObjectRank ranking over a bounded 2-hop neighborhood — per-term personalized PageRank combined by quorum consensus (`ppr`, `expand`), and the ask orchestration (`ask`) behind source traits a backend implements with a handful of SQL methods. Shared corpus policy in `corpus` |
| `duckdb-client` | DuckDB client with read-write retry backoff, read-only concurrent access, ontology-driven graph converter, shared SQL-literal/Arrow-column helpers |
| `gitlab-client` | GitLab REST/JWT client for Rails API calls |
| `integration-testkit` | Shared ClickHouse testcontainer helpers, `MockRedactionService`, `ResponseView` assertion framework, CLI test harness (`cli` module) for CLI integration tests |
| `integration-tests` | Integration tests: compiler (query compilation, ontology validation, pipeline infra) + server (health, redaction, hydration, data correctness, graph formatting) + cli (concurrency, worktrees); depends on orbit-server, compiler, integration-testkit |
| `integration-tests-codegraph` | Code-graph-specific integration tests (linker, lance-graph) |
| `fuzz` | Fuzz testing harness (bolero) for the query compiler, code parsers, and indexer message handling |
| `xtask` | Developer task runner (synthetic data generation, query evaluation, schema management) |
