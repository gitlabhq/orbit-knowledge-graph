# GitLab Knowledge Graph (Orbit) - Technical Deep Dive

A comprehensive technical report on the ETL engine, indexing modules, and system architecture.

---

## Executive Summary

The GitLab Knowledge Graph (codename "Orbit") is a property graph database that indexes GitLab data (code, SDLC entities) and exposes it through a Cypher-like JSON DSL. The system uses a streaming ETL architecture built on NATS JetStream and ClickHouse.

**Key Components:**
- **ETL Engine**: Message-driven processing framework with two-level concurrency control
- **SDLC Module**: Indexes MRs, issues, pipelines, work items, and more
- **Code Module**: Indexes call graphs, definitions, and references from repositories
- **Mailbox**: Plugin system for customer-defined nodes and edges
- **GKG Server**: gRPC/HTTP server with query pipeline and authorization

---

## Part 1: System Architecture Overview

### Data Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   PostgreSQL    │────▶│  Siphon (CDC)   │────▶│  NATS JetStream │
│  (GitLab DB)    │     │    Producer     │     │    (Broker)     │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
          ┌──────────────────────────────────────────────┼──────────────────────────────────────────────┐
          │                         │                    │                         │                    │
          ▼                         ▼                    ▼                         ▼                    ▼
┌─────────────────┐       ┌─────────────────┐  ┌─────────────────┐       ┌─────────────────┐  ┌─────────────────┐
│  SDLC Indexer   │       │  Code Indexer   │  │ Mailbox Handler │       │   Dispatcher    │  │ External Plugins│
│    Module       │       │    Module       │  │    Module       │       │                 │  │   (HTTP API)    │
└────────┬────────┘       └────────┬────────┘  └────────┬────────┘       └─────────────────┘  └────────┬────────┘
         │                         │                    │                                              │
         │    ┌─────────────────┐  │                    │                                              │
         │    │     Gitaly      │◀─┤                    │◀─────────────────────────────────────────────┘
         │    │  (Git Storage)  │  │                    │
         │    └─────────────────┘  │                    │
         │                         │                    │
         ▼                         ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                      ClickHouse                                          │
│                                 (Property Graph Storage)                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────────┐ │
│  │ gl_* tables │  │ gl_edges    │  │ gl_file     │  │ gl_definition│ │ gl_plugin_*_*  │ │
│  │ (SDLC)      │  │             │  │ (Code)      │  │ (Code)      │  │ (Mailbox)      │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  └────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
              ┌───────────────────────────────────────────────────┐
              │                   GKG Server                       │
              │          (gRPC + HTTP Query Interface)             │
              └───────────────────────────────────────────────────┘
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Message Broker | NATS JetStream | Event streaming, distributed coordination |
| CDC | Siphon | PostgreSQL logical replication |
| Storage | ClickHouse | Property graph tables, analytical queries |
| Git Access | Gitaly | Repository fetching via gRPC |
| Server | Rust (Axum + Tonic) | HTTP and gRPC APIs |
| Query Engine | Custom DSL → SQL | JSON-based Cypher-like queries |

---

## Part 2: ETL Engine Deep Dive

### Core Architecture

The ETL engine implements a **registry-based, handler-driven architecture**:

```
┌─────────────────────────────────────────────────────────────────┐
│                         ETL Engine                               │
├─────────────────────────────────────────────────────────────────┤
│  NatsBroker ──▶ Engine ──▶ Destination (ClickHouse)             │
│                   │                                              │
│                   ▼                                              │
│             ModuleRegistry                                       │
│               └─ Module                                          │
│                   ├─ Handler (topic subscriber)                  │
│                   ├─ Handler                                     │
│                   └─ Entity definitions                          │
└─────────────────────────────────────────────────────────────────┘
```

### Key Traits

#### Handler Trait
```rust
#[async_trait]
pub trait Handler: Send + Sync {
    fn name(&self) -> &str;
    fn topic(&self) -> Topic;
    async fn handle(&self, context: HandlerContext, message: Envelope)
        -> Result<(), HandlerError>;
}
```
- Single responsibility: processes messages from one NATS topic
- Receives context with destination writer, metrics, and NATS services
- Errors trigger message nack for automatic retry

#### Module Trait
```rust
pub trait Module: Send + Sync {
    fn name(&self) -> &str;
    fn handlers(&self) -> Vec<Box<dyn Handler>>;
    fn entities(&self) -> Vec<Entity>;
}
```
- Groups related handlers and entity definitions
- Module name used for per-module concurrency control

#### Destination Trait
```rust
#[async_trait]
pub trait Destination: Send + Sync {
    async fn new_batch_writer(&self, table: &str)
        -> Result<Box<dyn BatchWriter>, DestinationError>;
}
```
- Factory for creating table writers
- Abstraction enables any storage backend

### Message Processing Flow

```
1. Engine::run() starts
   ├─ Retrieves topics from ModuleRegistry
   ├─ Ensures NATS streams exist (if auto_create enabled)
   └─ Spawns listener task per topic

2. Each listener subscribes via NatsBroker::subscribe()
   └─ Creates pull consumer with background fetch
       ├─ Fetches messages in batches (configurable)
       ├─ Converts NATS messages to Envelope objects
       └─ Streams through mpsc channel

3. For each message:
   ├─ Finds all handlers for that topic
   ├─ Creates HandlerContext
   ├─ Dispatches via worker pool (concurrency control)
   ├─ On success: acks message
   └─ On failure: nacks message for retry
```

### Two-Level Concurrency Control

The engine uses **two-level semaphores** for resource protection:

```
┌─────────────────────────────────────────────────────────────────┐
│                       WorkerPool                                 │
├─────────────────────────────────────────────────────────────────┤
│  Global Semaphore (always present)                              │
│  └─ Limits total concurrent handlers across all modules          │
│     └─ Protects: CPU, memory, database connections              │
│                                                                  │
│  Per-Module Semaphores (optional)                               │
│  └─ Limits concurrent handlers within a specific module          │
│     └─ Example: SDLC max 4, Code max 4, global max 6            │
└─────────────────────────────────────────────────────────────────┘
```

**Acquisition Flow:**
1. Acquire global permit first
2. Acquire module-specific permit (if configured)
3. Process message
4. Permits auto-released via RAII when dropped

**Configuration Example:**
```rust
EngineConfiguration {
    max_concurrent_workers: 16,  // Global limit
    modules: {
        "sdlc": ModuleConfiguration { max_concurrency: Some(4) },
        "code": ModuleConfiguration { max_concurrency: Some(4) },
    }
}
```

### NATS JetStream Integration

**Stream Management:**
- Auto-creates streams on startup (configurable)
- Configurable replicas (1 for dev, 3 for prod)
- File storage with retention policies

**Consumer Configuration:**
- Durable vs ephemeral consumers
- Batch fetching (default: 10 messages)
- Ack waiting: 30 seconds before redelivery
- Max redeliveries: 5 attempts

**KV Store:**
- Distributed coordination (locks, watermarks)
- Optimistic concurrency control with revisions
- TTL support for temporary keys

### Testkit Utilities

Mock implementations for testing without infrastructure:

| Mock | Purpose |
|------|---------|
| MockNatsServices | Records published messages, in-memory KV |
| MockDestination | Collects written batches |
| MockHandler | Configurable delay and error injection |
| MockMetricCollector | Records metric events |
| TestEngineBuilder | Fluent API for test setup |

---

## Part 3: SDLC Module

### Entities Indexed

**Global-Scoped:**
- User (username, email, name, type, admin status)

**Namespace-Scoped:**
- Group, Project
- MergeRequest, MergeRequestDiff, MergeRequestDiffFile
- Milestone, Label, Note
- WorkItem
- Pipeline, Stage, Job
- Vulnerability, Finding

### Two-Handler Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       SDLC Module                                │
├─────────────────────────────────────────────────────────────────┤
│  GlobalHandler                                                   │
│  ├─ Topic: sdlc.global.indexing.requested                       │
│  ├─ Receives: GlobalIndexingRequest (watermark)                 │
│  ├─ Processes: User entities                                    │
│  └─ Filters by: last_watermark → current_watermark              │
│                                                                  │
│  NamespaceHandler                                                │
│  ├─ Topic: sdlc.namespace.indexing.requested                    │
│  ├─ Receives: NamespaceIndexingRequest (org_id, namespace_id)   │
│  ├─ Processes: All namespace-scoped entities + edges            │
│  └─ Filters by: traversal_path prefix + watermark range         │
└─────────────────────────────────────────────────────────────────┘
```

### Processing Pipeline

```
NATS Topic
    │
    ▼
Handler (Global/Namespace)
    │
    ▼
Ontology-Driven Pipelines
    ├─ Entity Pipelines (extract → transform → write nodes)
    └─ Edge Pipelines (extract → transform → write relationships)
    │
    ▼
ClickHouse
    ├─ gl_* tables (nodes)
    ├─ gl_edges (relationships)
    └─ *_indexing_watermark tables
```

### Key Components

**Datalake:**
- Abstracts ClickHouse as `DatalakeQuery` trait
- Executes parameterized queries against siphon_* source tables
- Returns Arrow RecordBatch streams

**Pipelines:**
- `OntologyEntityPipeline`: Entity extraction and transformation
- `OntologyEdgePipeline`: Relationship extraction

**Transform Engine:**
- Enum transformation (int → string)
- Field renaming (source → ontology)
- Multi-value edge explosion (delimited fields → multiple rows)
- Edge filtering for polymorphic relationships

**Watermark Strategy:**
- Global: single timestamp for all-users processing
- Namespace: per-namespace timestamp
- Uses `argMax(watermark, _version)` for latest watermark
- Enables incremental processing

**Distributed Locking:**
- NATS KV store (`indexing_locks` bucket)
- Prevents concurrent indexing of same scope
- 5-minute TTL to prevent stuck locks

### Integration Tests

Tests verify:
- User processing with enum transformations
- Watermark-based incremental processing
- Merge request relationships (AUTHORED, ASSIGNED, IN_PROJECT)
- CI/CD pipeline processing
- Edge relationship creation

---

## Part 4: Code Indexing Module

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Code Indexing Pipeline                        │
├─────────────────────────────────────────────────────────────────┤
│  Siphon CDC Stream                                              │
│      │                                                           │
│      ▼                                                           │
│  NATS (events.push_event_payloads)                              │
│      │                                                           │
│      ▼                                                           │
│  EventCacheHandler + PushEventHandler                           │
│      │                                                           │
│      ▼                                                           │
│  Gitaly (GetArchive RPC)                                        │
│      │                                                           │
│      ▼                                                           │
│  RepositoryIndexer (streaming file processing)                  │
│      │                                                           │
│      ▼                                                           │
│  AnalysisService (language-specific parsing)                    │
│      │                                                           │
│      ▼                                                           │
│  ArrowConverter (serialization)                                 │
│      │                                                           │
│      ▼                                                           │
│  ClickHouse (property graph)                                    │
└─────────────────────────────────────────────────────────────────┘
```

### Repository Fetching (Gitaly)

**Connection Types:**
- Unix socket: `unix:/path/to/gitaly.socket`
- TCP: `tcp://hostname:port`

**Authentication:**
- HMAC-SHA256 v2 token-based auth

**Extraction Flow:**
1. Create `GetArchiveRequest` with repository reference
2. Stream TAR archive in chunks
3. Security hardening:
   - Validate symlinks don't escape target
   - Reject path traversal attempts
   - Sanitize absolute paths

**Hashed Storage Path:**
```rust
// GitLab's hashed storage format
fn compute_hashed_path(project_id: i64) -> String {
    let hash = sha256(project_id.to_string());
    format!("@hashed/{}/{}/{}.git", &hash[0..2], &hash[2..4], hash)
}
```

### Streaming File Processing

```
Directory Walker (ignore crate with gitignore support)
    │ (files streamed as discovered)
    ▼
Async File Read Layer
    │ (bounded by io_concurrency: max(workers * 2, 8))
    ▼
CPU-bound Parsing Layer (tokio_rayon + Semaphore)
    │ (bounded by num_cores, default 4)
    ▼
Results Collection
```

### Supported Languages

| Language | Definition Types |
|----------|-----------------|
| Java | Class, Interface, Method, Constructor, Field, Annotation, Enum |
| Python | Class, Function, Module |
| Ruby | Class, Module, Method, Singleton Method |
| Kotlin | Class, Interface, Function, Property, Enum Entry |
| TypeScript | Class, Function, Interface, Type Alias |
| C# | Class, Interface, Method, Property, Field, Enum |
| Rust | Struct, Enum, Function, Trait, Impl, Module |

### Data Stored in ClickHouse

**Tables:**

| Table | Content |
|-------|---------|
| `gl_directory` | Directory nodes with paths |
| `gl_file` | File nodes with language, extension |
| `gl_definition` | Definitions with FQN, type, line ranges |
| `gl_imported_symbol` | Import statements |
| `gl_edges` | Relationships between entities |

**Relationship Types:**
- **Structural:** DIR_CONTAINS_DIR, DIR_CONTAINS_FILE, FILE_DEFINES
- **Hierarchical:** CLASS_TO_METHOD, CLASS_TO_CLASS, MODULE_TO_METHOD
- **Call graphs:** CALLS, METHOD_TO_FUNCTION, FUNCTION_TO_CLASS
- **Imports:** FILE_IMPORTS, DEFINES_IMPORTED_SYMBOL

### PushEventHandler Flow

1. **Event Decoding**: Decompress zstd Siphon CDC messages
2. **Validation**: Only BRANCH pushes with PUSHED action
3. **Project Lookup**: Resolve project_id, get traversal_path
4. **Default Branch Check**: Only index default branch pushes
5. **Duplicate Detection**: Watermark-based event filtering
6. **Lock Acquisition**: NATS KV distributed lock (15-min TTL)
7. **Repository Extraction**: Gitaly GetArchive RPC
8. **Code Indexing**: Run RepositoryIndexer on extracted files
9. **Node ID Assignment**: Deterministic hashing for idempotency
10. **ClickHouse Write**: 5 batches (dirs, files, defs, imports, edges)
11. **Watermark Update**: Store progress for incremental processing

---

## Part 5: GKG Server

### Operational Modes

| Mode | Description |
|------|-------------|
| Webserver | HTTP API with health checks |
| Indexer | Consumes NATS events, writes to ClickHouse |
| Dispatcher | Routes indexing tasks to workers |
| HealthCheck | Monitors infrastructure health |
| TrelloSync | Syncs Trello data (secondary) |

### gRPC Service

**RPCs:**

| RPC | Description |
|-----|-------------|
| ListTools | Returns available tools with JSON schemas |
| ExecuteTool | Bidirectional stream for tool execution |
| ExecuteQuery | Bidirectional stream for raw queries |
| GetOntology | Returns complete graph schema |
| GetNamespaceOntology | Returns ontology with plugin schemas |
| GetClusterHealth | Returns component health status |

### Query Processing Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    Query Pipeline Stages                         │
├─────────────────────────────────────────────────────────────────┤
│  1. SecurityStage                                                │
│     └─ Create security context from JWT claims                   │
│     └─ Admin: org-wide access                                    │
│     └─ Non-admin: group traversal paths                          │
│                                                                  │
│  2. ExtractionStage                                              │
│     └─ Compile JSON DSL → ClickHouse SQL                        │
│     └─ Execute query, get Arrow RecordBatches                   │
│     └─ Identify entities needing authorization                   │
│                                                                  │
│  3. AuthorizationStage                                           │
│     └─ Bidirectional handshake with client                      │
│     └─ Send resource checks                                      │
│     └─ Receive authorization responses                           │
│                                                                  │
│  4. RedactionStage                                               │
│     └─ Filter results based on authorizations                    │
│                                                                  │
│  5. FormattingStage                                              │
│     └─ Convert Arrow rows to JSON                                │
│     └─ Filter out internal _gkg_* columns                        │
└─────────────────────────────────────────────────────────────────┘
```

### Tools System

**Built-in Tools:**
- `query_graph`: Execute graph queries (traversals, paths, aggregations)
- `get_graph_entities`: List schema with property expansion

### Authentication

- JWT validation with configurable clock skew
- Claims: user_id, admin flag, organization_id, group traversal paths
- Admin users: org-wide traversal path (`{org_id}/`)
- Non-admins: group-based paths (e.g., `{org_id}/22/`)

---

## Part 6: Mailbox Plugin System

### Purpose and Vision

The **Mailbox module** is a plugin system that extends the Knowledge Graph with custom, user-defined nodes and edges. It enables customers to ingest their own data—vulnerability scans, deployments, compliance metrics, third-party tools—into the Knowledge Graph via HTTP APIs, storing it in dynamically generated ClickHouse tables alongside core GitLab entities.

**Core mission**: Allow customers to seamlessly integrate external data sources without modifying core system tables or implementing custom solutions.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      HTTP API Layer                              │
│  POST /api/v1/mailbox/plugins          Register plugin schema    │
│  POST /api/v1/mailbox/messages         Ingest nodes and edges    │
│  GET  /api/v1/mailbox/plugins/:id      Get plugin info           │
│  GET  /api/v1/mailbox/namespaces/{ns}/plugins  List plugins     │
│  DELETE /api/v1/mailbox/plugins/:id    Deregister plugin        │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│              Validation Layer                                    │
│  SchemaValidator   - Validates plugin schemas                   │
│  MessageValidator  - Validates ingestion payloads               │
│  PluginAuth        - API key verification via Argon2            │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│              NATS JetStream Layer                                │
│  Stream: mailbox-stream                                          │
│  Subject: mailbox.messages                                       │
│  Message deduplication via NATS KV (24h TTL)                    │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│          MailboxHandler (etl-engine Module)                      │
│  - Message deserialization from NATS                            │
│  - Deduplication checking                                       │
│  - Deterministic ID generation                                  │
│  - Arrow batch building for dynamic schemas                     │
│  - Node/edge processing and soft deletion                       │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│              ClickHouse Storage                                  │
│  gl_mailbox_plugins       - Plugin metadata & schemas            │
│  gl_mailbox_migrations    - Migration history                   │
│  gl_plugin_*_*            - Dynamic per-node-kind tables        │
│  gl_edges                 - All relationships (system + plugin)  │
└─────────────────────────────────────────────────────────────────┘
```

### Plugin Registration Flow

1. Client submits `RegisterPluginRequest` with schema
2. `SchemaValidator` enforces naming and structure rules
3. API key hashed with Argon2 before storage
4. DDL generated and executed for each node type
5. Migration recorded in `gl_mailbox_migrations`

**Naming Convention Enforcement:**
```
Plugin: security-scanner → prefix: security_scanner_
Node name: security_scanner_Vulnerability
Edge kind: security_scanner_AFFECTS
Table name: gl_plugin_security_scanner_vulnerability
```

### Schema Definition

**Node Definition:**
```json
{
  "name": "security_scanner_Vulnerability",
  "properties": [
    { "name": "score", "property_type": "float", "nullable": false },
    { "name": "severity", "property_type": "enum", "enum_values": ["low", "medium", "high", "critical"] },
    { "name": "cve_id", "property_type": "string", "nullable": true }
  ]
}
```

**Edge Definition:**
```json
{
  "relationship_kind": "security_scanner_AFFECTS",
  "source_node_kind": "security_scanner_Vulnerability",
  "target_node_kind": "Project"
}
```

### Supported Property Types

| Type | ClickHouse | Arrow | Notes |
|------|-----------|-------|-------|
| `string` | String | Utf8 | Default text |
| `int64` | Int64 | Int64 | 64-bit signed integer |
| `float` | Float64 | Float64 | Double precision |
| `boolean` | Bool | Boolean | True/false |
| `date` | Date | Date32 | Calendar date |
| `timestamp` | DateTime64(6, 'UTC') | Timestamp(μs) | Microsecond precision |
| `enum` | String | Utf8 | Restricted to enum_values |

### Message Ingestion

**Message Structure:**
```json
{
  "message_id": "unique-client-id",
  "plugin_id": "security-scanner",
  "nodes": [
    {
      "external_id": "vuln-001",
      "node_kind": "security_scanner_Vulnerability",
      "properties": { "score": 8.5, "severity": "high" }
    }
  ],
  "edges": [
    {
      "external_id": "edge-001",
      "relationship_kind": "security_scanner_AFFECTS",
      "source": { "node_kind": "security_scanner_Vulnerability", "external_id": "vuln-001" },
      "target": { "node_kind": "Project", "external_id": "42" }
    }
  ],
  "delete_nodes": [...],
  "delete_edges": [...]
}
```

**Validation Rules:**
- Maximum 1000 node operations per message
- Maximum 1000 edge operations per message
- Property values must match declared types
- Enum values restricted to defined enum_values
- Required properties must be present

### Message Processing Pipeline

```
1. Deserialize message from NATS Envelope
2. Check deduplication via NATS KV (msg_{message_id})
3. Retrieve plugin metadata from ClickHouse
4. Validate message against schema
5. Resolve traversal_path from namespace_id
6. Group nodes by kind for batch processing
7. For each node kind:
   ├─ Build Arrow RecordBatch from JSON properties
   ├─ Generate deterministic node IDs via hash
   └─ Write to gl_plugin_{plugin_id}_{kind} table
8. For each edge:
   ├─ Generate deterministic edge IDs
   ├─ Resolve target IDs (plugin nodes: hash, system nodes: external_id)
   └─ Write to gl_edges table
9. For deletions:
   └─ Create soft-delete records (_deleted=true)
10. Mark message as processed in NATS KV (24h TTL)
```

### Deterministic ID Generation

Uses `std::collections::hash_map::DefaultHasher`:
- **Node ID**: `hash(plugin_id, namespace_id, node_kind, external_id)`
- **Edge ID**: `hash(plugin_id, namespace_id, "edge", relationship_kind, external_id)`

Ensures idempotent ingestion—same data produces same ID, and ReplacingMergeTree deduplicates.

### Dynamic Table Generation

**Generated DDL Example:**
```sql
CREATE TABLE IF NOT EXISTS gl_plugin_security_scanner_vulnerability (
    id Int64,
    traversal_path String,
    score Float64,
    cve_id Nullable(String),
    severity String,
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id)
```

### Authentication

**Registration:**
- Client provides API key as plaintext
- Key hashed with Argon2 before storage

**Message Ingestion:**
- `X-Plugin-Id` header: Plugin identifier
- `X-Plugin-Token` header: API key (plaintext)
- Server verifies against stored Argon2 hash

### Deduplication System

- **Bucket**: `mailbox-dedup` (NATS KV)
- **Key pattern**: `msg_{message_id}`
- **TTL**: 24 hours
- Network retries and duplicate submissions are idempotent

### Validation Framework

**SchemaValidator (at registration):**
- Node names prefixed with `{plugin_id}_`
- Edge kinds prefixed with `{plugin_id}_`
- Edge targets: base ontology, own nodes, or other namespaces
- No reserved property names (`id`, `traversal_path`, `_version`, `_deleted`)
- Enum properties must have `enum_values`

**MessageValidator (at ingestion):**
- Batch size limits (≤1000 nodes, ≤1000 edges)
- Plugin ID matches authenticated plugin
- Property types match schema
- Edge source/target kinds allowed by schema

### Storage Tables

| Table | Purpose |
|-------|---------|
| `gl_mailbox_plugins` | Plugin metadata, schemas, hashed API keys |
| `gl_mailbox_migrations` | Schema migration history |
| `gl_plugin_{id}_{kind}` | Dynamic per-node-kind tables |
| `gl_edges` | All relationships (shared with system) |

### ETL Engine Integration

- `MailboxModule` implements `etl_engine::module::Module` trait
- `MailboxHandler` subscribes to `Topic(mailbox-stream, mailbox.messages)`
- Writes via `HandlerContext::destination`
- Uses shared `NatsServices` for deduplication KV

### Key Files

| File | Purpose |
|------|---------|
| `crates/mailbox/src/module.rs` | Module trait implementation |
| `crates/mailbox/src/handler/mailbox_handler.rs` | Core message processing |
| `crates/mailbox/src/handler/id_generator.rs` | Deterministic ID hashing |
| `crates/mailbox/src/handler/arrow_converter.rs` | Dynamic Arrow batch building |
| `crates/mailbox/src/types/` | Core data types |
| `crates/mailbox/src/validation/` | Schema and message validation |
| `crates/mailbox/src/storage/` | ClickHouse persistence |
| `crates/mailbox/src/http/routes.rs` | HTTP endpoint handlers |
| `crates/mailbox/src/schema_generator/` | DDL and Arrow schema generation |

---

## Part 7: GDK Integration

### Development Environment Setup

**Required Components:**

```bash
# Enable in GDK
gdk config set clickhouse.enabled true
gdk config set nats.enabled true
gdk config set siphon.enabled true

# Enable PostgreSQL logical replication
# Edit postgresql/data/postgresql.conf: wal_level = logical

gdk reconfigure
gdk start
```

### Service Configuration

| Service | Port | Config File |
|---------|------|-------------|
| NATS | 4222 | support/nats/nats-server.conf |
| ClickHouse HTTP | 8123 | clickhouse/config.xml |
| ClickHouse TCP | 9001 | clickhouse/config.d/gdk.xml |
| Gitaly | Unix socket | gitaly/gitaly.config.toml |
| PostgreSQL | 5432 | postgresql/data/postgresql.conf |

### Siphon CDC Configuration

**Producer (config.yml):**
```yaml
database:
  host: "localhost"
  port: 5432
  database: "gitlabhq_development"

replication:
  publication_name: "siphon_publication_main_db"
  slot_name: "siphon_slot_main_db"

queueing:
  driver: "nats"
  url: "localhost:4222"
  stream_name: 'siphon_stream_main_db'
```

**Consumer (consumer.yml):**
```yaml
queueing:
  driver: "nats"
  stream_name: "siphon_stream_main_db"

clickhouse:
  host: localhost
  port: 9001
  database: gitlab_clickhouse_development
```

### Tables Replicated (47 tables)

Organized in dependency tiers:
- **Tier 1:** organizations, namespaces, users
- **Tier 2:** projects, knowledge_graph_enabled_namespaces
- **Tier 3-6:** Issues, MRs, work items, labels, members
- **Tier 9-11:** Vulnerabilities, security scans, events, push events

---

## Part 8: Key Design Principles

1. **Stateless Handlers**: No shared state between invocations
2. **Async-First**: All I/O operations use async/await
3. **Trait Abstraction**: Enables testing and multiple implementations
4. **RAII for Resources**: Permits auto-released via drop
5. **Concurrent-by-Default**: Two-level semaphores for scaling
6. **Mockable Dependencies**: Unit testing without infrastructure
7. **Message Ordering**: Per-topic (JetStream guarantees)
8. **At-Least-Once Delivery**: Nack/retry with idempotent handlers
9. **Watermark-Based Incremental Processing**: Efficient change tracking
10. **Distributed Locking**: Prevents concurrent indexing conflicts

---

## Appendix: File Reference

### ETL Engine
- `crates/etl-engine/src/engine.rs` - Core engine loop
- `crates/etl-engine/src/module.rs` - Handler/Module traits
- `crates/etl-engine/src/worker_pool.rs` - Concurrency control
- `crates/etl-engine/src/nats/broker.rs` - NATS integration
- `crates/etl-engine/src/testkit/` - Test utilities

### GKG Server
- `crates/gkg-server/src/main.rs` - Entry point
- `crates/gkg-server/src/grpc/service.rs` - gRPC implementation
- `crates/gkg-server/src/query_pipeline/` - Query processing
- `crates/gkg-server/src/indexer/modules/` - SDLC and Code modules

### Code Indexer
- `crates/code-indexer/src/indexer.rs` - Repository indexing
- `crates/code-indexer/src/analysis/` - Language analyzers

### Gitaly Client
- `crates/gitaly-client/src/client.rs` - Repository fetching

### Mailbox
- `crates/mailbox/src/module.rs` - Module trait implementation
- `crates/mailbox/src/handler/mailbox_handler.rs` - Message processing
- `crates/mailbox/src/handler/id_generator.rs` - Deterministic ID hashing
- `crates/mailbox/src/handler/arrow_converter.rs` - Dynamic Arrow batch building
- `crates/mailbox/src/types/` - Core data types
- `crates/mailbox/src/validation/` - Schema and message validation
- `crates/mailbox/src/storage/` - ClickHouse persistence
- `crates/mailbox/src/http/routes.rs` - HTTP endpoints
- `crates/mailbox/src/schema_generator/` - DDL and Arrow schema generation

---

*Report generated for Slidev presentation preparation*
