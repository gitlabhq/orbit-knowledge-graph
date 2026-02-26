# Code indexer architecture

<!-- This document addresses https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/95 -->

This document describes the architecture of the code indexing pipeline as currently
implemented. It covers the parser, the graph builder, the server-side integration,
and the storage layer.

## Key concepts

These terms appear throughout the document. Readers already familiar with the
GKG ecosystem can skip to [Overview](#overview).

<!-- source: docs/design-documents/indexing/sdlc_indexing.md — Siphon architecture section -->
<!-- source: https://gitlab.com/gitlab-org/analytics-section/siphon — Siphon repository -->

**Siphon CDC** is a Go service maintained by the GitLab Analytics team. It connects
to a PostgreSQL logical replication slot, decodes Write-Ahead Log (WAL) changes into
protobuf messages (`LogicalReplicationEvents`), and publishes them to NATS JetStream.
For code indexing, the relevant events are `push_event_payloads`, which fire whenever
someone pushes commits to a repository. Siphon is external to GKG; we consume its
output but do not own or modify it. See the
[SDLC indexing design document](sdlc_indexing.md) for more detail on the CDC pipeline.

<!-- source: crates/indexer/src/nats/ — NATS broker implementation -->
<!-- source: crates/indexer/src/modules/code/push_event_handler.rs — NATS KV lock usage -->

**NATS JetStream** is the message broker that sits between Siphon and GKG. JetStream
provides durable message delivery with at-least-once semantics, meaning messages
survive broker restarts and are redelivered if a consumer fails to acknowledge them.
GKG also uses **NATS Key-Value (KV)** as a lightweight distributed lock store. During
code indexing, a lock keyed by `project_id:branch` prevents multiple indexer pods from
processing the same repository concurrently.

<!-- source: crates/gitaly-client/ — Gitaly gRPC client -->
<!-- source: crates/indexer/src/modules/code/gitaly.rs — RepositoryService trait, extract_repository -->

**Gitaly** is GitLab's Git storage service. All Git operations in GitLab go through
Gitaly's gRPC API rather than touching disk directly. The code indexer uses the
`GetArchive` RPC to download a tarball of the repository at a specific commit, which
is then extracted to a temporary directory for parsing. This avoids a full `git clone`
and gives us a point-in-time snapshot of the codebase. Authentication uses HMAC-signed
tokens. See the [gitaly-client crate](../../../crates/gitaly-client/) for the
implementation.

<!-- source: docs/design-documents/data_model.md — property graph data model -->
<!-- source: crates/clickhouse-client/ — ClickHouse client with Arrow IPC support -->

**ClickHouse** is the columnar database that stores both the raw Siphon data (datalake)
and the indexed property graph. ClickHouse was chosen because it handles analytical
queries over large datasets efficiently, supports native Arrow IPC ingestion (avoiding
serialization overhead), and scales horizontally. The property graph is stored as
separate node tables (`gl_directory`, `gl_file`, `gl_definition`,
`gl_imported_symbol`) and a shared `edges` table. See the
[data model design document](../data_model.md) for the full schema.

<!-- source: crates/indexer/src/modules/code/arrow_converter.rs — traversal_path in BaseColumnBuilders -->
<!-- source: docs/design-documents/security.md — authorization via traversal IDs -->

**Traversal path** is a string encoding the GitLab namespace hierarchy for a project
(e.g., `/gitlab-org/orbit/knowledge-graph`). Every row written to ClickHouse includes
a `traversal_path` column. At query time, the query engine filters results to only
the namespaces the requesting user has access to, as determined by Rails via gRPC.
This is the primary authorization mechanism. See the
[security design document](../security.md) for details.

<!-- source: crates/indexer/src/modules/code/arrow_converter.rs — ArrowConverter, RecordBatch construction -->

**Arrow IPC** is the serialization format used to transfer data from the indexer to
ClickHouse. The indexer builds Apache Arrow `RecordBatch` objects in memory (columnar,
zero-copy where possible) and serializes them using the Arrow IPC wire format.
ClickHouse can ingest Arrow IPC natively, which avoids row-by-row insertion overhead
and makes bulk writes efficient.

## Overview

The code indexer transforms source code from GitLab repositories into a property graph
stored in ClickHouse. The pipeline runs inside the `gkg-server` Indexer mode and is
triggered by Siphon CDC push events arriving over NATS JetStream.

<!-- source: crates/indexer/src/modules/code/push_event_handler.rs — PushEventHandler::handle -->
<!-- source: crates/indexer/src/modules/code/mod.rs — CodeModule registers PushEventHandler -->

```text
Siphon CDC (push_event_payloads)
        │
        ▼
  NATS JetStream
        │
        ▼
  gkg-server (Indexer mode)
        │
        ▼
  PushEventHandler
        │
        ├─ 1. Validate event (default branch only, not already indexed)
        ├─ 2. Acquire distributed lock via NATS KV
        ├─ 3. Fetch repository archive from Gitaly (GetArchive RPC)
        ├─ 4. Extract to temp directory
        ├─ 5. Run code-graph indexing pipeline
        │       ├─ File discovery (ignore crate, respects .gitignore)
        │       ├─ Async file read (bounded IO concurrency)
        │       ├─ CPU-bound parsing on Rayon pool (semaphore-bounded)
        │       └─ Analysis phase → GraphData
        ├─ 6. Convert GraphData → Arrow RecordBatches
        ├─ 7. Write batches to ClickHouse (5 tables)
        ├─ 8. Delete stale data from previous indexing run
        └─ 9. Update watermark, release lock
```

## Crate responsibilities

<!-- source: Cargo.toml — workspace members -->

| Crate | Role |
|---|---|
| `code-parser` | Multi-language parser: AST parsing, definition/import/reference extraction |
| `code-graph` | Streaming indexing pipeline, graph data model, analysis service |
| `indexer` | NATS consumer, push event handler, Arrow conversion, ClickHouse writes |
| `gitaly-client` | Gitaly gRPC client for `GetArchive` RPC |

## Parser architecture (`code-parser`)

### Language support matrix

<!-- source: crates/code-parser/src/parser.rs — define_languages! macro, lines ~130-175 -->

| Language | Extensions | Parser backend | References extracted? |
|---|---|---|---|
| Ruby | `rb`, `rbw`, `rake`, `gemspec` | `ruby_prism` (native Prism bindings) | Yes |
| TypeScript/JavaScript | `ts`, `js` (excludes `min.js`) | SWC (`swc_ecma_parser`) | Yes |
| Python | `py` | tree-sitter via `GenericParser` | Yes |
| Kotlin | `kt`, `kts` | tree-sitter via `GenericParser` | Yes |
| Java | `java` | tree-sitter via `GenericParser` | Yes |
| C# | `cs` | tree-sitter via `GenericParser` | No |
| Rust | `rs` | tree-sitter via `GenericParser` | No |

### Three parser backends

<!-- source: crates/code-parser/src/parser.rs — ParserType enum and ParserType::for_language -->

The `ParserType` enum dispatches to the correct backend based on language:

```rust
// crates/code-parser/src/parser.rs
pub enum ParserType {
    TreeSitter(GenericParser),          // Python, Kotlin, Java, C#, Rust
    Ruby(RubyParser),                   // ruby_prism
    TypeScript(TypeScriptParser),       // SWC
}

impl ParserType {
    pub fn for_language(language: SupportedLanguage) -> Self {
        match language {
            SupportedLanguage::Ruby => Self::Ruby(create_ruby_parser()),
            SupportedLanguage::TypeScript => Self::TypeScript(create_typescript_parser()),
            _ => Self::TreeSitter(GenericParser::new(language)),
        }
    }
}
```

Each backend produces a `UnifiedParseResult` variant that the downstream analyzer
consumes:

<!-- source: crates/code-parser/src/parser.rs — UnifiedParseResult enum -->

```rust
// crates/code-parser/src/parser.rs
pub enum UnifiedParseResult<'a> {
    TreeSitter(ParseResult<'a, Root<StrDoc<SupportLang>>>),
    Ruby(ParseResult<'a, ruby_prism::ParseResult<'a>>),
    TypeScript(ParseResult<'a, TypeScriptSwcAst>),
}
```

### Language detection

<!-- source: crates/code-parser/src/parser.rs — detect_language_from_extension, EXTENSION_MAP -->

Language detection uses a static `FxHashMap` mapping file extensions to
`SupportedLanguage` variants. The map is built at startup by the `define_languages!`
macro. There are 12 supported extensions total.

Files matching a language's `exclude_extensions` list (currently only `min.js` for
TypeScript) are skipped during processing.

<!-- source: crates/code-parser/src/parser.rs — SupportedLanguage::exclude_extensions -->

### Extraction types

The parser extracts three categories of information from each file:

<!-- source: crates/code-parser/src/definitions.rs — DefinitionInfo struct -->
<!-- source: crates/code-parser/src/imports.rs — ImportedSymbolInfo struct -->
<!-- source: crates/code-parser/src/references.rs — ReferenceInfo struct -->

**Definitions** (`DefinitionInfo<DefinitionType, FqnType>`): Classes, modules, methods,
functions, constants, interfaces, properties, constructors, enum entries, and lambdas.
Each definition has a fully qualified name (FQN), a source range, and a
language-specific definition type.

**Imported symbols** (`ImportedSymbolInfo<ImportType, FqnType>`): Import statements with
their import path, identifier (name + optional alias), source range, and scope. Import
types are language-specific (e.g., Python distinguishes `from` imports, wildcard imports,
relative imports, and future imports).

**References** (`ReferenceInfo<TargetResolutionType, ReferenceType>`): Call sites and
property accesses. Each reference has a target that can be `Resolved` (single
definition or import), `Ambiguous` (multiple candidates), or `Unresolved`. Target
resolution can point to a definition in the same file, an imported symbol, or a partial
expression chain to be resolved later during analysis.

### Per-language analyzers

<!-- source: crates/code-graph/src/parsing/processor.rs — FileProcessor::analyze_file -->

Each language has a dedicated analyzer in `crates/code-parser/src/<language>/analyzer.rs`:

| Language | Analyzer | Input |
|---|---|---|
| Ruby | `RubyAnalyzer::analyze_with_prism` | Prism AST |
| TypeScript/JS | `TypeScriptAnalyzer::analyze_swc` | SWC AST |
| Python | `PythonAnalyzer::analyze` | tree-sitter AST |
| Kotlin | `KotlinAnalyzer::analyze` | tree-sitter AST |
| Java | `JavaAnalyzer::analyze` | tree-sitter AST |
| C# | `CSharpAnalyzer::analyze` | tree-sitter AST |
| Rust | `RustAnalyzer::analyze` | tree-sitter AST |

### Stack safety

<!-- source: crates/code-parser/src/lib.rs — MINIMUM_STACK_REMAINING constant -->
<!-- source: crates/code-graph/src/lib.rs — MINIMUM_STACK_REMAINING constant -->

Both `code-parser` and `code-graph` define a `MINIMUM_STACK_REMAINING` constant of
128 KiB. Before each recursive call during AST traversal, `stacker::remaining_stack()`
is checked. If less than 128 KiB remains, the traversal bails out, trading completeness
for crash safety.

## Indexing pipeline (`code-graph`)

### Streaming architecture

<!-- source: crates/code-graph/src/indexer.rs — RepositoryIndexer::parse_file_stream -->

The indexer uses a fully streaming pipeline. Files are processed as they are discovered;
there is no upfront collection step.

```text
DirectoryFileSource::stream_files()
        │
        │  mpsc channel (capacity 256)
        ▼
  filter by supported extensions
        │
        ▼
  async file read (buffer_unordered, IO concurrency)
        │
        ▼
  CPU-bound parsing on Rayon (tokio_rayon::spawn, semaphore-bounded)
        │
        ▼
  collect results → AnalysisService::analyze_results()
        │
        ▼
  GraphData
```

### Concurrency model

<!-- source: crates/code-graph/src/indexer.rs — parse_file_stream, worker_count and io_concurrency -->

| Resource | Bound | Default |
|---|---|---|
| CPU workers (Rayon via `tokio_rayon::spawn`) | `Semaphore(worker_count)` | `max(num_cpus, 4)` |
| IO concurrency (async file reads) | `buffer_unordered(io_concurrency)` | `max(worker_count * 2, 8)` |
| Directory walker channel | `mpsc::channel(256)` | 256 |

The `IndexingConfig` struct controls these values:

<!-- source: crates/code-graph/src/indexer.rs — IndexingConfig struct -->

```rust
// crates/code-graph/src/indexer.rs
pub struct IndexingConfig {
    pub worker_threads: usize,   // 0 = auto-detect
    pub max_file_size: usize,    // default: 5 MB
    pub respect_gitignore: bool, // default: true
}
```

### File discovery

<!-- source: crates/code-graph/src/loading/mod.rs — DirectoryFileSource, stream_directory -->

`DirectoryFileSource` uses the `ignore` crate's `WalkBuilder` with parallel directory
traversal. It:

- Respects `.gitignore` rules (configurable)
- Skips `.git` directories
- Skips nested git repositories (directories containing `.git/` at depth > 0)
- Filters to supported file extensions (12 extensions across 7 languages)
- Supports custom exclusion patterns via gitignore syntax

Files are sent through a `tokio::sync::mpsc` channel (capacity 256) from a
`spawn_blocking` task running the parallel walker.

### File processing

<!-- source: crates/code-graph/src/parsing/processor.rs — FileProcessor::process -->

`FileProcessor::process()` handles a single file:

1. Detect language from pre-computed file extension
2. Check exclusion list (e.g., `min.js`)
3. Parse using `ParserType::for_language(language).parse()`
4. Analyze using the language-specific analyzer
5. Return `FileProcessingResult` containing `Definitions`, `ImportedSymbols`, and
   `References` enums

Each of these enums is a language-tagged wrapper:

<!-- source: crates/code-graph/src/parsing/processor.rs — Definitions, ImportedSymbols, References enums -->

```rust
// crates/code-graph/src/parsing/processor.rs
pub enum Definitions {
    Ruby(Vec<RubyDefinitionInfo>),
    Python(Vec<PythonDefinitionInfo>),
    // ... one variant per language
}
```

### Graph data model

<!-- source: crates/code-graph/src/analysis/types.rs — GraphData struct (inferred from arrow_converter.rs usage) -->

The `AnalysisService::analyze_results()` method groups file processing results by
language and builds a `GraphData` struct containing:

| Node type | Description |
|---|---|
| `DirectoryNode` | Directory in the repository tree |
| `FileNode` | Source file with path, language, extension |
| `DefinitionNode` | Code definition with FQN, type, source range, file path |
| `ImportedSymbolNode` | Import statement with path, type, identifier, source range |

Relationships between nodes are stored as a flat list with source/target indices and
a `RelationshipType` enum.

### Relationship types

<!-- source: crates/code-graph/src/graph.rs — RelationshipType enum -->

The `RelationshipType` enum defines 49 fine-grained relationship types organized into
categories:

| Category | Examples | Count |
|---|---|---|
| Directory structure | `DirContainsDir`, `DirContainsFile` | 2 |
| File to entity | `FileDefines`, `FileImports` | 2 |
| Definition hierarchy | `ClassToMethod`, `ModuleToClass`, `FunctionToFunction`, etc. | 30+ |
| Interface relationships | `InterfaceToMethod`, `InterfaceToProperty`, etc. | 6 |
| References | `Calls`, `AmbiguouslyCalls`, `PropertyReference` | 3 |
| Import resolution | `ImportedSymbolToDefinition`, `ImportedSymbolToFile`, etc. | 4 |

These fine-grained types are mapped to four ontology edge labels during Arrow
conversion:

<!-- source: crates/indexer/src/modules/code/arrow_converter.rs — edge_label function -->

| Ontology label | Mapped from |
|---|---|
| `CONTAINS` | `DirContainsDir`, `DirContainsFile` |
| `DEFINES` | All definition hierarchy types (30+) |
| `IMPORTS` | `FileImports`, `ImportedSymbolTo*` |
| `CALLS` | `Calls`, `AmbiguouslyCalls`, `PropertyReference` |

## Server integration (`indexer` crate)

### ETL engine and module system

<!-- source: crates/indexer/src/engine.rs — Engine, EngineBuilder -->
<!-- source: crates/indexer/src/module.rs — Module, Handler, ModuleRegistry, HandlerContext -->
<!-- source: crates/indexer/src/worker_pool.rs — WorkerPool, HandlerSlot -->
<!-- source: crates/indexer/src/destination.rs — Destination, BatchWriter traits -->

The indexer crate provides a general-purpose ETL engine that is not specific to code
indexing. Code and SDLC indexing are both implemented as modules plugged into this
engine.

The core abstractions are:

- **`Handler`** (trait): Processes messages from a single NATS topic. Defines `name()`,
  `topic()`, and `handle(context, message)`. For code indexing, the handler is
  `PushEventHandler`.
- **`Module`** (trait): Groups related handlers and entity definitions. Returns handlers
  via `handlers()`. For code indexing, the module is `CodeModule`, which registers
  `PushEventHandler`.
- **`ModuleRegistry`**: Collects handlers from all registered modules and provides
  topic-to-handler lookup. The engine queries the registry to know which handlers to
  invoke for each incoming message.
- **`HandlerContext`**: Passed to every handler invocation. Contains three shared
  resources: a `Destination` for writing data, `NatsServices` for publishing messages,
  and a `LockService` for distributed locking.
- **`Engine`**: Subscribes to all topics that have registered handlers, dispatches
  incoming messages through the worker pool, and manages ack/nack. Built via
  `EngineBuilder` which wires together the NATS broker, module registry, destination,
  and optional metrics.
- **`WorkerPool`**: Controls concurrency with two levels of semaphores. A global
  semaphore caps total concurrent handlers across the engine (default: 16). Optional
  per-module semaphores prevent one module from starving another when multiple indexers
  run in a single pod.

<!-- source: crates/indexer/src/configuration.rs — EngineConfiguration, ModuleConfiguration -->

```rust
// crates/indexer/src/configuration.rs
pub struct EngineConfiguration {
    pub max_concurrent_workers: usize,  // default: 16
    pub modules: HashMap<String, ModuleConfiguration>,
}

pub struct ModuleConfiguration {
    pub max_concurrency: Option<usize>,      // per-module limit
    pub max_retry_attempts: Option<u32>,      // before giving up
    pub retry_interval_secs: Option<u64>,     // nack delay
}
```

### Pluggable storage (`Destination` / `BatchWriter`)

<!-- source: crates/indexer/src/destination.rs — Destination, BatchWriter traits -->

The engine does not write to ClickHouse directly. Instead, handlers receive a
`Destination` through `HandlerContext` and create table-specific writers on demand:

```rust
// crates/indexer/src/destination.rs
#[async_trait]
pub trait BatchWriter: Send + Sync {
    async fn write_batch(&self, batch: &[RecordBatch])
        -> Result<(), DestinationError>;
}

#[async_trait]
pub trait Destination: Send + Sync {
    async fn new_batch_writer(&self, table: &str)
        -> Result<Box<dyn BatchWriter>, DestinationError>;
}
```

In production, the `Destination` implementation is backed by the `clickhouse-client`
crate's `ArrowClickHouseClient`, which serializes Arrow `RecordBatch` objects to IPC
format and sends them to ClickHouse. In tests, a `MockDestination` captures writes
for assertion. This separation means handlers never import ClickHouse-specific code
and the storage backend can be swapped without changing handler logic.

### Push event flow

<!-- source: crates/indexer/src/modules/code/push_event_handler.rs — PushEventHandler -->

The `PushEventHandler` implements the `Handler` trait and subscribes to the
`push_event_payloads` NATS subject. When a push event arrives:

1. **Decode**: Deserialize Siphon protobuf `LogicalReplicationEvents`
2. **Extract**: Pull `event_id`, `project_id`, `ref_type`, `action`, `ref_name`,
   `revision_after` from the CDC columns
3. **Validate**: Skip non-branch pushes and non-push actions
4. **Default branch check**: Query Gitaly `FindDefaultBranch` RPC to confirm the push
   is to the default branch
5. **Project lookup**: Verify the project exists in the knowledge graph (ClickHouse
   datalake `projects` table) and retrieve its `traversal_path`
6. **Watermark check**: Skip if `last_event_id >= current event_id`
7. **Distributed lock**: Acquire a NATS KV lock keyed by `project_id:branch`
8. **Repository extraction**: Call Gitaly `GetArchive` RPC, extract tarball to a
   `TempDir`
9. **Indexing**: Run `RepositoryIndexer::index_files()` with `DirectoryFileSource`
10. **ID assignment**: Call `graph_data.assign_node_ids(project_id, branch)` to generate
    deterministic node IDs
11. **Arrow conversion**: `ArrowConverter::convert_all()` produces 5 `RecordBatch` objects
12. **ClickHouse write**: Write batches to 5 tables
13. **Stale data cleanup**: Delete rows from previous indexing run
14. **Watermark update**: Record the new watermark
15. **Lock release**

### Arrow conversion

<!-- source: crates/indexer/src/modules/code/arrow_converter.rs — ArrowConverter -->

`ArrowConverter` transforms `GraphData` into Apache Arrow `RecordBatch` objects. Every
row includes base columns injected by the converter:

| Base column | Type | Description |
|---|---|---|
| `traversal_path` | `Utf8` | Namespace hierarchy path for authorization |
| `project_id` | `Int64` | GitLab project ID |
| `branch` | `Utf8` | Branch name |
| `_version` | `Timestamp(Microsecond, UTC)` | Indexing timestamp for stale data cleanup |

The converter produces five `RecordBatch` objects written to five ClickHouse tables:

<!-- source: crates/indexer/src/modules/code/push_event_handler.rs — write_graph_data method -->

| Table | Node type | Extra columns |
|---|---|---|
| `gl_directory` | Directories | `path`, `name` |
| `gl_file` | Files | `path`, `name`, `extension`, `language` |
| `gl_definition` | Definitions | `file_path`, `fqn`, `name`, `definition_type`, `start_line`, `end_line`, `start_byte`, `end_byte` |
| `gl_imported_symbol` | Imported symbols | `file_path`, `import_type`, `import_path`, `identifier_name`, `identifier_alias`, `start_line`, `end_line`, `start_byte`, `end_byte` |
| `edges` | Relationships | `source_id`, `source_kind`, `relationship_kind`, `target_id`, `target_kind` (shared with SDLC edges) |

### ClickHouse write path

<!-- source: crates/clickhouse-client/src/ — ArrowClickHouseClient (inferred from Cargo.toml deps) -->

Arrow `RecordBatch` objects are serialized to Arrow IPC format and sent to ClickHouse
via the `clickhouse-client` crate's `ArrowClickHouseClient`. The `Destination` /
`BatchWriter` trait pair provides a pluggable storage abstraction used by both code and
SDLC indexing modules.

## Differences from the original local tool

<!-- source: local-knowledge-graph/crates/database/ — lbug-based embedded graph DB -->
<!-- source: local-knowledge-graph/crates/indexer/ — local filesystem indexer -->

The original Knowledge Graph (at `gitlab-org/rust/knowledge-graph`) was a local desktop
tool. Key architectural differences:

| Aspect | Original (local) | Current (service) |
|---|---|---|
| Graph database | lbug (embedded) | ClickHouse |
| Code access | Local filesystem | Gitaly `GetArchive` RPC |
| Event trigger | Filesystem watcher (`watchexec`) | Siphon CDC push events via NATS |
| Storage format | Parquet → lbug bulk import | Arrow IPC → ClickHouse |
| Multi-tenancy | Single user, single repo | Namespace-scoped via `traversal_path` |
| Authorization | None (local tool) | Rails gRPC delegation |
| Parser crate | External `parser-core` dependency | In-tree `code-parser` (forked and evolved) |
| Graph builder | External `indexer` crate | In-tree `code-graph` |
| Concurrency | Same streaming model | Same streaming model (preserved) |

The streaming concurrency model (Rayon + semaphore + `buffer_unordered`) was carried
over from the original tool and remains largely unchanged.
