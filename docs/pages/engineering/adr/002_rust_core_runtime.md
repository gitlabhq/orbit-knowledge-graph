---
title: "GKG ADR 002: Rust as the core runtime for the Orbit service"
creation-date: "2026-02-23"
authors: [ "@michaelangeloio" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-02-23

## Context

The Orbit code indexer was written in Rust from the start.
The indexer is a static analysis engine for many languages, built on tree-sitter and SWC.
The tree-sitter organization maintains the Rust bindings for tree-sitter.
Rust compiles to WebAssembly for client-side use in the Language Server and the Web IDE.
Rust also parses large repositories in memory with no garbage collection (GC) pauses.
The One Parser initiative later made Rust the GitLab standard for static code analysis in Orbit, embeddings, and language server features.

Orbit then changed from a local command-line interface (CLI) tool into a server-side service.
We had to keep the core runtime in Rust or rewrite the components that do not parse code in Go.
At first, we embedded the Rust code in a Go-based indexer through a foreign function interface (FFI).
That choice satisfied a packaging constraint of the Linux package.
GitLab then committed to its segmentation strategy, and the constraint went away.
We compared FFI with a standalone service and chose a dedicated Rust process.
That choice made the language commitment permanent.

The service builds a property graph from GitLab instance data and serves queries over gRPC.
It runs as one binary in four modes: web server, indexer, scheduler, and health check.
It consumes change data capture (CDC) events from NATS and compiles a JSON query language into parameterized ClickHouse SQL.
It uses bidirectional gRPC streams for the authorization exchange with Rails.
One multi-tenant deployment serves all of GitLab.com.
A three-hop traversal must finish in less than 300 ms at p95.

The choice also follows the data engineering trend across the industry.
The columnar data stack that Orbit uses is native to Rust, and includes Arrow, DataFusion, and Parquet.
DataFusion builds on the Arrow Rust implementation, and Polars, Delta Lake, and Databend also use it.
Throughput needs grow with larger instances and more indexed entities.

## Decision

**We use Rust as the core runtime language for the Orbit service.**

The indexer, the query compiler, the data pipeline, and the web server are one Rust workspace.
They use the same language as the data processing libraries under them, so no serialization boundary separates them.
Data can move from ClickHouse through Arrow inter-process communication (IPC) batches to gRPC responses with no copy.
The service uses Rust libraries for the async runtime, HTTP, gRPC, columnar data, NATS, and tree-sitter.

## Benefits of this approach

### Memory safety without garbage collection

The service accepts untrusted user queries, CDC events, and repository archives in a multi-tenant environment.
The Rust ownership model prevents use-after-free, double-free, buffer overflow, and data race bugs at compile time.
We forbid unsafe code across the workspace.
A language with GC is also memory-safe, but it adds pauses of unknown length.
A pause during the bidirectional redaction exchange extends the time that a Puma thread waits on the Rails side.

### One binary for all workloads

All four modes share configuration and startup logic.
The Helm chart deploys the same container image for each workload, with a different mode argument and resource limits.
Rust compiles to a statically linked binary with no runtime dependencies.
The container image has only the binary, TLS certificates, and the ontology files.
The image has no interpreter, no virtual machine, and no shared library chain.
This makes GitLab Self-Managed and GitLab Dedicated deployments simpler, because we do not control the host operating system.

### Async concurrency

The indexer runs worker pools that consume NATS messages, fetch archives, parse code, and write to ClickHouse at the same time.
CPU-bound tree-sitter parsing runs in a separate pool, so it does not block the async executor.
The web server maps each gRPC stream to a lightweight task, not to an operating system thread.
The health check mode watches Kubernetes pod status asynchronously.
The compiler checks thread-safety bounds, so a data race between async tasks fails at compile time.

### Native FFI for tree-sitter

Tree-sitter is a C library, and Rust calls C directly through a thin layer of safe Rust types.
The call has no marshaling cost and needs no bridge such as JNI or cgo.
The parser reads repository archives in memory, with no intermediate files, and extracts definitions, imports, references, and call graphs.
SWC is a Rust-native JavaScript and TypeScript parser, and it runs next to tree-sitter.
Both parsers share one allocator and compose with no cross-language cost.

### Shared gRPC contract

One protocol file defines the service contract.
The build generates the Rust server interfaces from it, and the same file generates the Ruby gem for the Rails client.
The Rust compiler catches schema mismatches.

### Columnar data processing

The ClickHouse client streams results as Arrow IPC batches, so large results need no row-by-row deserialization.
The query engine uses DataFusion to plan and validate SQL.
Both libraries are native to Rust, so no binding layer adds cost.
A language with a separate runtime form for columnar data would add serialization cost as entity counts grow.

### Compile-time correctness

The type system catches whole classes of bugs before the code runs:

- Each protobuf `oneof` maps to a Rust enum, and the compiler requires a match arm for each variant.
- A new message type in the contract forces an update to every handler.
- The ontology loads and validates at startup and exposes typed accessors, so an invalid reference fails continuous integration (CI) and not a production query.
- CI treats lint warnings as errors and checks licenses, known vulnerabilities, and unsafe code.

## Consequences

Rust is harder to learn than Go or Python.
At the February 2026 offsite, we invested in build infrastructure to reduce this cost.
We added a prebuilt base image with pinned tool versions, a shared compiler cache, and Docker layer caching.
With caching, a pipeline took about five minutes for 400 dependencies, against about 20 minutes without it.

New contributors need Rust experience, so the reviewer pool at GitLab is smaller than for Go or Ruby.
Other Rust projects at GitLab can reuse our observability library and CI patterns.
GLAZ is one such Rust service.

Rust builds are slower than Go builds because of generic code expansion and LLVM code generation.
A shared repository holds the builder images for all Rust services at GitLab.
The Orbit image carries the prebuilt tools and the compiler cache configuration.
Go would not need this maintenance.
Multi-architecture builds need target triples and linked C libraries, such as tree-sitter grammars and OpenSSL.
The multi-architecture Docker pipeline handles this, but it takes work to maintain.

The Rust libraries for gRPC, HTTP, and the async runtime are ready for production.
The official NATS client for Rust is actively maintained, and the ClickHouse client library meets our needs.

GitLab site reliability engineering (SRE) has deep experience with Go services and little experience with Rust.
The observability team found this gap at the February 2026 offsite and committed to a standard telemetry output for Rust services.
We build a Rust implementation of LabKit to close the gap.
It gives structured logging, correlation ID propagation, and OpenTelemetry metrics.
It follows the patterns of the Go and Ruby libraries.
We planned it in phases, from the logging foundation to server layers, client propagation, OpenTelemetry, and masking.
The foundation phase was complete when we made this decision.

The Production Readiness review evaluates the binary size and the memory profile.
It also evaluates crash behavior on panic or abort, and core dump analysis.

## Why not the alternatives

### FFI into Go

Before the dedicated process, Orbit ran through FFI inside the Go-based indexer.
We stopped for these reasons:

- A query needed several steps: Go called Rust to build the query, ran it, then called Rust again to process the results.
- Each API change needed edits in the Rust code, the FFI bindings, and the Go service.
- A Rust panic or memory leak crashed the full Go process.
- The FFI bindings needed unsafe code, and that removed the safety that made us choose Rust.
- The async runtime does not embed cleanly in the Go scheduler, so pools and state went through raw pointers and C types.
- The CPU and memory use of the Rust library were not separate from the Go process.

A dedicated container gives its own resource limits, health checks, and metrics.
A proof of concept that moved indexing from FFI to HTTP took one day, and it was much easier to work with.
Other GitLab projects had similar problems with FFI.

We never planned to rewrite the service in Go.
The question was how the Rust code should connect to GitLab infrastructure.
When the packaging constraint went away, we moved the Rust code into its own service.

### Go

Go is the usual language at GitLab for infrastructure services, such as Gitaly, Praefect, Siphon, and Workhorse.
Go was the main alternative and a reasonable choice.
We know Go from Siphon, and its operational patterns are well established.
The workload made us choose Rust.

GC pauses during bidirectional gRPC streams would extend the time that a Puma thread waits in Rails.
The Go bindings for tree-sitter need cgo.
cgo makes cross-compilation harder, makes builds slower, and adds memory bugs that the Go GC cannot control.
The One Parser initiative evaluated the Go tree-sitter options and found two blockers.
The Go WebAssembly target does not support cgo, so client-side use in the Language Server and Web IDE is not possible.
The most used Go binding had low activity and only some of the grammars.

Go has no sum types.
In Go, the protobuf `oneof` pattern needs interface type assertions with no exhaustiveness check at compile time.
Goroutines and channels are simpler, but Go finds data races only at runtime with the race detector.

During the FFI evaluation, people raised concerns about Rust skills for interviews, style guides, monitoring, and on-call coverage.
The Consequences section addresses them.

### Ruby

Ruby is the main language at GitLab.
Rails already handles authorization, the Model Context Protocol (MCP) endpoint, and the gRPC client that calls Orbit.

The parser must call tree-sitter, a C library, and SWC, a Rust library, with no serialization cost.
A Ruby C extension can wrap tree-sitter, but that code is harder to make memory-safe than Rust FFI.
Ruby can reach SWC only through a separate process or a Rust FFI layer.

The indexer runs parallel worker pools for NATS, archive fetches, and ClickHouse writes.
The Ruby global interpreter lock limits CPU-bound parallelism to forked processes.
Forked processes use more memory and make shared state harder.
Interpreter overhead and GC pauses make the p95 query target harder to meet.

Ruby has no mature equivalent for Arrow IPC streaming or DataFusion.
We would have to wrap the Rust libraries through FFI from Ruby, with the same problems we had in Go.

We keep Ruby for the Rails integration layer, and Rust for the compute-heavy service behind it.

## References

- [Orbit repository](https://gitlab.com/gitlab-org/orbit/knowledge-graph)
- [Shared Rust builder images](https://gitlab.com/gitlab-org/rust/build-images)
- [Orbit Helm charts](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts)
- [ADR 001: gRPC communication protocol](001_grpc_communication.md)
- [ADR 003: API design](003_api_design.md)
- [Orbit design documents](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/README.md)
- [Production Readiness review for Orbit](https://gitlab.com/gitlab-org/architecture/readiness/-/merge_requests/64)
- [LabKit for Rust](https://gitlab.com/gitlab-org/rust/labkit-rs)
- [GLAZ authorization service design](https://gitlab.com/gitlab-org/architecture/auth-architecture/design-doc/-/merge_requests/74)
- [FFI compared with a dedicated process](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/168)
- [One Parser proposal](https://gitlab.com/gitlab-org/gitlab/-/issues/534153)
- [Prior FFI experience at GitLab](https://gitlab.com/gitlab-org/gitlab/-/issues/392996)
- [February 2026 offsite CI infrastructure session](https://gitlab.com/gitlab-org/orbit/documentation/orbit-artifacts)
