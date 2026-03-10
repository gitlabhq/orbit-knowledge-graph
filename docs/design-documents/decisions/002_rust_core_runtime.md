---
title: "GKG ADR 002: Rust as the core runtime for the Knowledge Graph service"
creation-date: "2026-02-23"
authors: [ "@michaelangeloio" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-02-23

## Context

The Knowledge Graph already has a substantial Rust codebase. The code indexer -- a multi-language static analysis engine built on tree-sitter and SWC -- was written in Rust from day one because Rust has first-class tree-sitter bindings maintained by the tree-sitter organization, compiles to WebAssembly for client-side use (Language Server, Web IDE), and provides the performance needed to parse large repositories in-memory without GC pauses. The "One Parser" initiative ([gitlab-org/gitlab#534153](https://gitlab.com/gitlab-org/gitlab/-/issues/534153)) later formalized this choice, establishing Rust as the GitLab standard for static code analysis across the Knowledge Graph, embeddings, and language server features.

When the Knowledge Graph evolved from a local CLI tool into a server-side service, the question was whether to keep the core runtime in Rust or rewrite the non-parsing components in Go. We originally embedded the Rust code via FFI into the Go-based `gitlab-zoekt-indexer` to satisfy Omnibus packaging constraints. When GitLab committed to its segmentation strategy, that constraint went away and we evaluated FFI vs. standalone service in [#168](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/168). The decision to run as a dedicated Rust process made the language commitment permanent.

The service now builds a property graph from GitLab instance data and serves queries over gRPC. It runs as a single binary in four modes (webserver, indexer, scheduler, health-check), processes CDC events from NATS, compiles a JSON DSL into parameterized ClickHouse SQL, and handles bidirectional gRPC streaming for authorization exchanges with Rails. It is multi-tenant -- one deployment serves all of GitLab.com. Query workloads must finish under 300ms at p95 for 3-hop traversals.

Choosing Rust for the full runtime also aligns with data engineering trends in the broader ecosystem. The columnar data stack we depend on -- Arrow, DataFusion, and Parquet -- is Rust-native (the Apache Arrow Rust implementation is the reference for DataFusion and is used by systems like Polars, Delta Lake, and Databend). As throughput requirements grow with larger GitLab instances and more indexed entities, having the indexer, query compiler, and data pipeline in the same language as the underlying data processing libraries eliminates serialization boundaries and enables zero-copy data paths from ClickHouse through Arrow-IPC to gRPC responses.

## Decision

Use Rust as the core runtime language for the Knowledge Graph service.

The service is implemented as a Cargo workspace with 16 crates:

| Crate | Purpose |
|-------|---------|
| `gkg-server` | HTTP/gRPC server, JWT auth, config loading, 4 execution modes |
| `query-engine` | JSON DSL to parameterized ClickHouse SQL compilation |
| `indexer` | NATS consumer, SDLC + code indexing handlers, worker pools |
| `ontology` | YAML ontology loading, JSON schema validation, query validators |
| `code-parser` | Multi-language parser (7 langs) via tree-sitter + SWC |
| `code-graph` | In-memory property graph construction from parsed code |
| `clickhouse-client` | Async ClickHouse client with Arrow-IPC streaming |
| `gitaly-client` | Gitaly gRPC client with HMAC authentication |
| `siphon-proto` | Protobuf types for Siphon CDC replication events |
| `labkit-rs` | Structured logging, correlation IDs, OpenTelemetry metrics |
| `health-check` | Kubernetes readiness/liveness probes |
| `treesitter-visit` | Tree-sitter language bindings wrapper |
| `cli` | Local `gkg index` and `gkg query` commands |
| `simulator` | Fake data generation and query correctness evaluation |
| `datalake-generator` | Synthetic GitLab data for load testing |
| `xtask` | Build automation for e2e tests |

Runtime dependencies worth noting: Tokio (async runtime), Axum (HTTP), Tonic (gRPC), Arrow/DataFusion (columnar data), async-nats (message broker), tree-sitter (code parsing).

## Why Rust

### Memory safety without garbage collection

The service handles untrusted input at multiple boundaries (user queries, CDC events, Gitaly tar streams) in a multi-tenant environment. Rust's ownership model eliminates use-after-free, double-free, buffer overflows, and data races at compile time. We enforce `unsafe_code = "forbid"` across the workspace. A GC'd language provides memory safety but introduces unpredictable pause times during the gRPC bidirectional streaming redaction exchange, which blocks a Puma thread on the Rails side.

### Single binary deployment

`gkg-server` runs in four modes via a `--mode` flag: `Webserver`, `Indexer`, `DispatchIndexing`, `HealthCheck`. All modes share configuration and startup logic. The Helm chart (`gkg-helm-charts` v1.0.0) deploys the same container image for every workload type, differing only in the mode argument and resource limits.

Rust compiles to a statically linked binary with no runtime dependencies. The container image is scratch-based: just the binary, TLS certificates, and ontology fixtures. No interpreter, no VM, no shared library chain. This simplifies self-managed and Dedicated deployments where we do not control the host OS.

### Async concurrency model

Everything runs on Tokio:

- The indexer runs configurable worker pools that consume NATS messages, fetch Gitaly archives, parse code, and write to ClickHouse concurrently. `tokio-rayon` bridges CPU-bound tree-sitter parsing into the async runtime without blocking the executor.
- The webserver handles concurrent gRPC streams, each running a ClickHouse query and a redaction exchange. Tokio maps each stream to a lightweight task rather than an OS thread.
- The health-check mode uses the `kube` crate to watch Kubernetes pod status asynchronously.

Rust's `Send + Sync` trait bounds catch data races at compile time. Sharing state between async tasks (e.g., the mutex-protected gRPC stub cache in the Rails client) is verified by the compiler rather than by runtime testing.

### Tree-sitter and native FFI

The code parser supports seven languages (Ruby, Python, JavaScript, TypeScript, Go, Rust, Java) via tree-sitter grammars. Tree-sitter is a C library. Rust's FFI with C is straightforward -- the `treesitter-visit` crate wraps the C bindings with safe Rust types. No marshaling overhead, no JNI bridge, no cgo compilation step. The parser processes repository archives in-memory, extracting call graphs, definitions, imports, and references without writing intermediate files.

SWC (Rust-native JS/TS parser) runs alongside tree-sitter for JS/TS-specific analysis. Both parsers share the same allocator and compose without cross-language overhead.

### gRPC and protobuf ecosystem

We use Tonic for gRPC (client and server) and Prost for protobuf codegen. `gkg.proto` defines the service contract; `tonic-build` generates Rust server traits and client stubs at compile time. The same proto file generates the `gitlab-gkg-proto` Ruby gem for the Rails client, so the contract is shared between Rust and Ruby. Schema mismatches get caught at compile time on the Rust side.

The Gitaly client (`gitaly-client` crate) uses the same Tonic/Prost stack to call Gitaly's `GetArchive` RPC. This follows the same patterns GitLab uses for Gitaly (Go) and is consistent with GLAZ (also Rust).

### Columnar data processing

The `clickhouse-client` crate streams query results as Arrow-IPC batches using the `arrow` and `datafusion` crates. Arrow's columnar format avoids row-by-row deserialization for large result sets. The query engine uses DataFusion for SQL planning and validation. Both are Rust-native with no binding overhead. This matters for future throughput: as indexed entity counts grow, the zero-copy path from ClickHouse through Arrow-IPC to gRPC responses avoids serialization bottlenecks that would appear in a language with a separate runtime representation for columnar data.

### Compile-time correctness

Beyond memory safety, the type system catches whole categories of bugs before anything runs:

- Exhaustive `match` on protobuf `oneof` enums means every message variant must be handled. Add a new RPC message type to `gkg.proto` and the compiler forces you to update every handler.
- The ontology crate validates YAML schemas at startup and exposes typed accessors. Invalid ontology references break CI, not production queries.
- Clippy runs with all warnings as errors in CI. `cargo deny` checks licenses, `cargo audit` checks for known vulnerabilities, `cargo geiger` reports unsafe usage.

## Why not FFI into Go

Before we decided on a dedicated process, the Knowledge Graph was embedded via FFI in the Go-based `gitlab-zoekt-indexer`. We abandoned this approach for several reasons documented in [#168](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/168):

Querying through FFI required a multi-step dance: Go calls Rust to build a query, Go executes it, Go calls Rust again to post-process results (mapping integer relationship types back to strings). Every API change required updating Rust code, FFI bindings, and Go service code -- three maintenance points instead of one.

FFI shared the failure domain -- a Rust panic or memory leak would crash the entire Go process. The `unsafe` code required for FFI bindings undermined the safety guarantees that motivated choosing Rust in the first place.

Tokio cannot be embedded cleanly across FFI into Go's goroutine scheduler. Connection pooling, state management, and concurrent database access had to be plumbed through raw pointers and C-compatible types.

Observability could not be separated -- CPU and memory from the Rust FFI library were indistinguishable from the Go process. A dedicated container with its own resource limits, health checks, and metrics endpoints solved this.

A proof-of-concept converting FFI-based indexing to HTTP was built in one day and was significantly easier to work with. Other projects at GitLab had similar negative experiences with FFI ([gitlab-org/gitlab#392996](https://gitlab.com/gitlab-org/gitlab/-/issues/392996)), which reinforced the decision.

The question was never whether to rewrite the KG in Go, but how the existing Rust codebase should integrate with GitLab infrastructure. Once the Omnibus constraint went away, a standalone Rust service was the clear path forward.

## Why not Go

Go is the established language at GitLab for infrastructure services (Gitaly, Praefect, Siphon, Workhorse). It was the primary alternative considered. Go would have been a reasonable choice -- we have Go experience (Siphon is Go) and the operational patterns are well-established. The workload characteristics tipped the decision toward Rust.

GC pauses during bidirectional gRPC streams would extend the time a Puma thread is blocked on the Rails side. Rust's deterministic memory management avoids this.

Go's tree-sitter bindings require cgo, which complicates cross-compilation, increases build times, and introduces memory bugs outside the Go GC's control. The "One Parser" initiative ([gitlab-org/gitlab#534153](https://gitlab.com/gitlab-org/gitlab/-/issues/534153)) evaluated the Go tree-sitter ecosystem and found two blocking problems: Go's `js/wasm` target does not support cgo (ruling out WebAssembly for client-side use in Language Server and Web IDE), and the most widely used Go binding (`smacker/go-tree-sitter`) had low activity with only a subset of available grammars. The official `tree-sitter` Rust crate is maintained by the tree-sitter organization. The initiative established Rust as the GitLab standard for static code analysis.

Go lacks sum types. The protobuf `oneof` pattern in the gRPC message exchange maps to Rust enums with exhaustive matching. In Go, the same pattern requires interface type assertions with no compile-time exhaustiveness check.

Go's concurrency model (goroutines + channels) is simpler but provides no compile-time data race detection. `go vet` and the race detector catch races at runtime, not at compile time.

During the FFI evaluation, concerns were raised about Rust expertise gaps at GitLab (interviewing, code style guides, monitoring tools, on-call coverage). These tradeoffs are addressed in the Consequences section below.

## Why not Ruby

Ruby is the primary language at GitLab, and Rails already handles authorization, the MCP endpoint, and the gRPC client that calls GKG. The question is whether the KG service itself should also be Ruby.

The code parser needs to call tree-sitter (C library) and SWC (Rust library) with no serialization overhead, processing repository archives in-memory across seven languages. Ruby's C extension API can wrap tree-sitter, but the resulting code is harder to make memory-safe than Rust's FFI, and there is no path to SWC without shelling out or adding a Rust FFI layer anyway.

The indexer runs concurrent worker pools consuming NATS messages, fetching Gitaly archives, and writing to ClickHouse simultaneously. Ruby's GIL limits CPU-bound parallelism to forked processes, which increases memory usage and complicates shared state. The query path requires sub-300ms p95 latency for compiled SQL execution plus a gRPC redaction exchange -- Ruby's interpreter overhead and GC pauses make this harder to achieve.

The columnar data pipeline (Arrow-IPC streaming from ClickHouse, DataFusion for SQL validation) has no mature Ruby equivalent. We would need to wrap the Rust libraries via FFI from Ruby, which reintroduces the same problems we encountered embedding Rust in Go.

Ruby is the right choice for the Rails integration layer (authorization, MCP routing, gRPC client). It is not the right choice for the compute-intensive service behind it.

## Consequences

### Team composition

Rust has a steeper learning curve than Go or Python. We addressed this during the Feb 2026 offsite by investing in build infrastructure: a pre-compiled base image with tool versions tracked in `.tool-versions` via `mise`, sccache for dependency caching across CI runs, and Docker layer caching. Pipeline times are ~5 minutes for 400 dependencies (vs ~20 minutes without caching).

New contributors need Rust experience, which narrows the reviewer pool within GitLab compared to Go or Ruby. The [`labkit-rs`](https://gitlab.com/gitlab-org/rust/labkit-rs) library and CI patterns we have established can be reused by other Rust projects at GitLab (GLAZ is also Rust).

### Build infrastructure

Rust's compilation model (monomorphization, LLVM codegen) produces slower builds than Go. We maintain a separate `build-images` repository (`gitlab-org/orbit/build-images`) with pre-compiled tool versions and the sccache configuration described above. This is ongoing maintenance cost that Go would not require.

Cross-compilation for multiple architectures means managing target triples and linked C libraries (tree-sitter grammars, OpenSSL). The multi-arch Docker build pipeline handles this but it is not trivial to maintain.

### Ecosystem

The Rust ecosystem for gRPC (Tonic), HTTP (Axum), and async (Tokio) is production-ready. The `async-nats` crate is the official NATS client and is actively maintained. The `clickhouse` crate has been sufficient for our needs.

### Operational patterns

GitLab SRE has deep experience operating Go services and limited experience with Rust. The observability team identified this gap at the Feb 2026 offsite and committed to defining standard telemetry output for Rust services. To address this, we are building [`labkit-rs`](https://gitlab.com/gitlab-org/rust/labkit-rs) -- a Rust implementation of the LabKit observability library that provides structured logging, correlation ID propagation, and OpenTelemetry metrics, following the patterns from `labkit` (Go) and `labkit-ruby`. The project is being developed in five phases: foundation (fields, correlation, logging -- complete), HTTP/gRPC server layers, client propagation, OpenTelemetry integration, and masking/documentation.

The PREP (Production Readiness) review (MR !64) will evaluate Rust-specific operational characteristics: binary size, memory profile, crash behavior (panic vs abort), and core dump analysis.

## References

- [Knowledge Graph repository](https://gitlab.com/gitlab-org/orbit/knowledge-graph) - 16-crate Cargo workspace
- [Build images repository](https://gitlab.com/gitlab-org/orbit/build-images) - CI builder images with sccache
- [GKG Helm charts](https://gitlab.com/gitlab-org/orbit/gkg-helm-charts) - production Helm chart (v1.0.0)
- [ADR 001: gRPC communication protocol](001_grpc_communication.md)
- [ADR 003: API Design — Unified REST + GraphQL](003_api_design.md)
- [Design documents](../README.md) - architecture overview
- [PREP readiness review MR !64](https://gitlab.com/gitlab-org/architecture/readiness/-/merge_requests/64)
- [labkit-rs](https://gitlab.com/gitlab-org/rust/labkit-rs) - Rust implementation of LabKit observability library
- [GLAZ authorization service](https://gitlab.com/gitlab-org/architecture/auth-architecture/design-doc/-/merge_requests/74) - another Rust service at GitLab
- [Issue #168: FFI vs Dedicated Process](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/168) - decision to run KG as standalone
- [Issue #534153: One Parser proposal](https://gitlab.com/gitlab-org/gitlab/-/issues/534153) - Rust as the GitLab standard for static code analysis
- [Issue #392996: FFI experience at GitLab](https://gitlab.com/gitlab-org/gitlab/-/issues/392996) - prior art against FFI
- [Feb 2026 offsite: CI infrastructure session](https://gitlab.com/gitlab-org/orbit/documentation/orbit-artifacts) - build optimization discussion
