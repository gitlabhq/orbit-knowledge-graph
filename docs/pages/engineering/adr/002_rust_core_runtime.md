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

When Orbit changed from a local command-line interface (CLI) tool into a server-side service, we had to pick its main language.
The code indexer was already in Rust, so we could keep Rust everywhere or rewrite the parts that do not parse code in Go.

The indexer is a static analysis engine for many languages, built on the tree-sitter and SWC parsers.
The tree-sitter organization maintains the Rust bindings for tree-sitter.
Rust compiles to WebAssembly, so the same code runs in the Language Server and the Web IDE.
Rust also parses large repositories in memory with no garbage collection (GC) pauses.
The One Parser initiative later made Rust the GitLab standard for static code analysis in Orbit, embeddings, and language server features.

At first, a packaging constraint in the Linux package made us embed the Rust code in a Go indexer through a foreign function interface (FFI).
That constraint went away when GitLab committed to its segmentation strategy.
We then compared FFI with a standalone Rust service and chose the service, which made Rust a permanent choice.

Orbit builds a property graph from GitLab instance data and serves queries over gRPC.
It runs as one binary in four modes: web server, indexer, dispatcher, and health check.
It reads change data capture (CDC) events from NATS and compiles GQL queries into ClickHouse SQL.
It uses bidirectional gRPC streams for the authorization exchange with Rails.
One multi-tenant deployment serves all of GitLab.com, and a three-hop traversal must finish in less than 300 ms at p95.

The columnar data libraries that Orbit uses, Arrow, DataFusion, and Parquet, are native to Rust.
Polars, Delta Lake, and Databend also build on DataFusion and Arrow in Rust.
Orbit needs more throughput as instances grow and as we index more entities.

## Decision

**We write all of Orbit in Rust.**

The indexer, the query compiler, the data pipeline, and the web server are one Rust codebase.
They use the same language as the data libraries under them, so data does not need conversion between languages.
Data can move from ClickHouse through Arrow batches to gRPC responses with no copy.
Orbit uses Rust libraries for the async runtime, HTTP, gRPC, columnar data, NATS, and tree-sitter.

## Benefits of this approach

### Memory safety without garbage collection

Orbit accepts untrusted user queries, CDC events, and repository archives from many tenants.
The Rust ownership model prevents memory bugs and data races at compile time, and we forbid unsafe code everywhere.
A language with GC is also memory-safe, but it adds pauses of unknown length.
A pause during the redaction exchange makes a Puma thread in Rails wait longer.

### One binary for all workloads

All four modes share configuration and startup logic.
The Helm chart deploys the same container image for each workload, with a different mode and different resource limits.
Rust compiles to a static binary with no runtime dependencies.
The container image has only the binary, TLS certificates, and the ontology files, with no interpreter or shared libraries.
We do not control the host operating system on GitLab Self-Managed and GitLab Dedicated, so a self-contained image makes those deployments simpler.

### Async concurrency

The indexer runs worker pools that read NATS messages, fetch archives, parse code, and write to ClickHouse at the same time.
CPU-heavy parsing runs in a separate pool, so it does not block the async work.
The web server runs each gRPC stream as a lightweight task instead of an operating system thread.
The health check mode watches Kubernetes pod status asynchronously.
The compiler checks thread safety, so a data race between async tasks fails at compile time.

### Native FFI for tree-sitter

Tree-sitter is a C library, and Rust calls C directly with no conversion cost and no bridge such as JNI or cgo.
The parser reads repository archives in memory and extracts definitions, imports, references, and calls.
SWC is a Rust parser for JavaScript and TypeScript, and it runs next to tree-sitter in the same process.

### Shared gRPC contract

One protocol file defines the contract between Rails and Orbit.
The build generates the Rust server and the Ruby gem for the Rails client from it, so the compiler catches schema mismatches.

### Columnar data processing

The ClickHouse client streams results as Arrow batches, so Orbit does not decode large results row by row.
The query engine uses DataFusion to plan and validate SQL.
Both libraries are native to Rust, so there is no binding layer.
In another language, each conversion to and from columnar data would cost more as the graph grows.

### Compile-time correctness

The type system catches whole classes of bugs before the code runs:

- Each message that can take one of several shapes maps to a Rust enum, and the compiler requires a case for each shape.
- A new message type in the contract forces an update to every handler.
- The ontology, the schema of the graph, is checked at startup, so an invalid reference fails continuous integration (CI) before it reaches production.
- CI treats lint warnings as errors and checks licenses, known vulnerabilities, and unsafe code.

## Consequences

Rust is harder to learn than Go or Python, and its builds are slow.
At the February 2026 offsite, we added a prebuilt base image, a shared compiler cache, and Docker layer caching.
With caching, a pipeline took about five minutes instead of about 20.

New contributors need Rust experience, so GitLab has fewer reviewers for Rust than for Go or Ruby.
Other Rust projects at GitLab, such as the GLAZ authorization service, can reuse our observability library and CI patterns.

Rust builds are slower than Go builds.
A shared repository holds the builder images for all Rust services at GitLab, and Go would not need this maintenance.
Builds for several CPU architectures must also link C libraries, such as tree-sitter grammars and OpenSSL.
Our Docker pipeline handles this, but it takes work to maintain.

The Rust libraries for gRPC, HTTP, and the async runtime are ready for production.
The official NATS client for Rust is actively maintained, and the ClickHouse client library meets our needs.

GitLab site reliability engineering (SRE) has deep experience with Go services and little with Rust.
The observability team found this gap at the February 2026 offsite and committed to standard telemetry for Rust services.
To close the gap, we build a Rust version of LabKit, the GitLab observability library, on the patterns of the Go and Ruby versions.
It gives structured logging, correlation IDs, and OpenTelemetry metrics.
We planned it in phases, and the logging foundation was complete when we made this decision.

The Production Readiness review will evaluate the binary size, the memory profile, crash behavior on panic or abort, and core dump analysis.

## Why not the alternatives

### FFI into Go

Before the dedicated process, Orbit ran through FFI inside the Go indexer.
We stopped for these reasons:

- A query needed several steps: Go called Rust to build the query, ran it, then called Rust again to process the results.
- Each API change needed edits in the Rust code, the FFI bindings, and the Go service.
- A Rust panic or memory leak crashed the full Go process.
- The FFI bindings needed unsafe code, and that removed the safety that made us choose Rust.
- The Rust async runtime does not fit cleanly inside Go, so shared state went through raw pointers and C types.
- We could not see the CPU and memory use of the Rust code apart from the Go process.

A dedicated container has its own resource limits, health checks, and metrics.
A proof of concept that moved indexing from FFI to HTTP took one day, and it was much easier to work with.
Other GitLab projects had similar problems with FFI.

We never planned to rewrite Orbit in Go, and the open question was how the Rust code connects to GitLab infrastructure.
When the packaging constraint went away, we moved the Rust code into its own service.

### Go

Go is the usual language at GitLab for infrastructure services, such as Gitaly, Praefect, Siphon, and Workhorse.
It was the main alternative and a reasonable choice, and we know its operational patterns from Siphon.
The workload made us choose Rust.

GC pauses during the gRPC stream would make a Puma thread in Rails wait longer.
The Go bindings for tree-sitter need cgo, the Go bridge to C.
cgo makes cross-compilation harder and builds slower, and it adds memory bugs that the Go GC cannot catch.
The One Parser initiative evaluated the Go tree-sitter options and found two blockers.
Go cannot use cgo when it compiles to WebAssembly, so the parser could not run in the Language Server and Web IDE.
The most used Go binding also had low activity and only some of the grammars.

Go has no sum types, so a message that can take several shapes gets no completeness check at compile time.
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
Ruby can run CPU-heavy work in parallel only in forked processes, which use more memory and make shared state harder.
Interpreter overhead and GC pauses make the p95 query target harder to meet.

Ruby has no mature equivalent for Arrow streaming or DataFusion.
We would have to wrap the Rust libraries through FFI from Ruby, with the same problems we had in Go.

We keep Ruby for the Rails integration layer, and Rust for the compute-heavy service behind it.

## References

- [Orbit repository](https://gitlab.com/gitlab-org/orbit/knowledge-graph)
- [Shared Rust builder images](https://gitlab.com/gitlab-org/rust/build-images)
- [Orbit Helm charts](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts)
- [ADR 001: gRPC communication protocol](001_grpc_communication.md)
- [ADR 003: Orbit API design (unified REST and GraphQL)](003_api_design.md)
- [Orbit design documents](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/README.md)
- [Production Readiness review for Orbit](https://gitlab.com/gitlab-org/architecture/readiness/-/merge_requests/64)
- [LabKit for Rust](https://gitlab.com/gitlab-org/rust/labkit-rs)
- [GLAZ authorization service design](https://gitlab.com/gitlab-org/architecture/auth-architecture/design-doc/-/merge_requests/74)
- [FFI compared with a dedicated process](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/168)
- [One Parser proposal](https://gitlab.com/gitlab-org/gitlab/-/issues/534153)
- [Prior FFI experience at GitLab](https://gitlab.com/gitlab-org/gitlab/-/issues/392996)
- [February 2026 offsite CI infrastructure session](https://gitlab.com/gitlab-org/orbit/documentation/orbit-artifacts)
