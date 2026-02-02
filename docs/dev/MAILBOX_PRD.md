# Mailbox Module PRD

## Problem Statement

Customers need a way to extend the GitLab Knowledge Graph with their own custom nodes and edges. Currently, the Knowledge Graph only indexes GitLab-native entities (MRs, pipelines, issues, etc.). Customers running security scanners, compliance tools, or internal services have valuable graph data that should integrate with GitLab entities but have no way to ingest it.

## Goals

1. Enable customers to define custom node and edge types with typed schemas
2. Provide an HTTP API for ingesting custom graph data
3. Automatically generate typed ClickHouse tables from schemas (no generic JSON blobs)
4. Allow custom nodes to connect to system nodes (User, Project, File, etc.)
5. Ensure idempotent, reliable message processing

## Non-Goals

- Real-time streaming API (batch HTTP only for v1)
- Schema backward compatibility enforcement (plugin owners manage their own compatibility)
- Multi-tenant plugin sharing (plugins are namespace-scoped)
- Custom query DSL extensions for plugin types

## User Stories

### Plugin Developer

**As a** security tool developer
**I want to** register a "Vulnerability" node type with severity, CVE ID, and score fields
**So that** I can link vulnerabilities to affected files and projects in the Knowledge Graph

**As a** plugin developer
**I want to** submit batches of nodes and edges via HTTP
**So that** my tool can integrate with the Knowledge Graph without managing message queues

**As a** plugin developer
**I want to** reference existing GitLab entities (Users, Projects, Files) in my edges
**So that** my custom data connects to the broader GitLab graph

### Platform Operator

**As a** GitLab administrator
**I want to** see which plugins are registered and their schemas
**So that** I can audit what custom data flows into the Knowledge Graph

**As a** platform operator
**I want** duplicate messages to be handled idempotently
**So that** retries from unreliable networks don't corrupt data

## Solution Overview

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌────────────┐
│   Plugin    │────▶│  HTTP API    │────▶│    NATS     │────▶│  Handler   │
│   Client    │     │  (gkg-server)│     │  JetStream  │     │            │
└─────────────┘     └──────────────┘     └─────────────┘     └─────┬──────┘
                           │                                       │
                           ▼                                       ▼
                    ┌──────────────┐                        ┌────────────┐
                    │   Plugin     │                        │ ClickHouse │
                    │   Registry   │◀───────────────────────│   Tables   │
                    └──────────────┘                        └────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   Schema     │
                    │  Migration   │
                    │   Engine     │
                    └──────────────┘
```

## Functional Requirements

### FR1: Plugin Registration

| ID | Requirement |
|----|-------------|
| FR1.1 | System shall accept plugin registration with a unique plugin ID, top-level namespace ID, API key, and schema definition |
| FR1.2 | System shall validate that all custom type names are prefixed with `{plugin_id}_` |
| FR1.3 | System shall store the provided API key as Argon2 hash |
| FR1.4 | System shall use namespace ID to construct traversal paths for all plugin entities |
| FR1.5 | System shall reject registration if plugin ID already exists in namespace |

**Trust model:** Registration establishes trust. The customer provides their plugin information and API key; we store and use it for authentication on subsequent requests.

**API:** `POST /api/v1/mailbox/plugins`

### FR2: Schema Definition

| ID | Requirement |
|----|-------------|
| FR2.1 | Schemas shall define custom node types with typed properties |
| FR2.2 | Schemas shall define custom edge types with allowed source/target node kinds |
| FR2.3 | Edge targets may reference: system nodes (base ontology), the plugin's own nodes, or nodes from plugins in other namespaces |
| FR2.4 | Edge targets shall NOT reference nodes from other plugins within the same namespace |
| FR2.5 | Properties shall support: string, int64, float, boolean, date, timestamp, enum |
| FR2.6 | Properties shall specify nullable or required |

**Example schema:**
```json
{
  "nodes": [
    {
      "name": "Vulnerability",
      "properties": [
        { "name": "severity", "type": "enum", "values": ["low", "medium", "high", "critical"] },
        { "name": "cve_id", "type": "string", "nullable": true },
        { "name": "score", "type": "float", "nullable": false }
      ]
    }
  ],
  "edges": [
    { "relationship_kind": "AFFECTS", "from_node_kinds": ["Vulnerability"], "to_node_kinds": ["File", "Project"] }
  ]
}
```

### FR3: Automatic Table Generation

| ID | Requirement |
|----|-------------|
| FR3.1 | Upon plugin registration, system shall generate ClickHouse DDL for each node type |
| FR3.2 | Tables shall be named `gl_plugin_{plugin_id}_{node_kind_lowercase}` |
| FR3.3 | Tables shall include standard columns: `id` (Int64), `traversal_path` (String), `_version`, `_deleted` |
| FR3.4 | Tables shall use ReplacingMergeTree engine for deduplication |
| FR3.5 | System shall track applied migrations in `gl_mailbox_migrations` table |

**Traversal path:** All plugin nodes use the format `{organization_id}/{namespace_id}`. The organization ID is resolved from `gl_groups` using the namespace ID provided during plugin registration.

**Type mapping:**

| Plugin Type | ClickHouse Type |
|-------------|-----------------|
| string | String |
| int64 | Int64 |
| float | Float64 |
| boolean | Bool |
| date | Date |
| timestamp | DateTime64(6, 'UTC') |
| enum | String |

### FR4: Message Ingestion

| ID | Requirement |
|----|-------------|
| FR4.1 | System shall accept batches of nodes and edges via HTTP POST |
| FR4.2 | Each message shall have a client-provided unique `message_id` |
| FR4.3 | System shall validate API token via `X-Plugin-Token` header |
| FR4.4 | System shall validate all nodes/edges against registered schema |
| FR4.5 | System shall publish valid messages to NATS stream `mailbox-stream` |
| FR4.6 | System shall return message ID for client tracking |

**API:** `POST /api/v1/mailbox/messages`

**Request format:**
```json
{
  "message_id": "unique-client-id",
  "plugin_id": "security-scanner",
  "nodes": [
    {
      "external_id": "vuln-001",
      "node_kind": "security-scanner_Vulnerability",
      "properties": { "severity": "high", "score": 8.5 }
    }
  ],
  "edges": [
    {
      "external_id": "edge-001",
      "relationship_kind": "security-scanner_AFFECTS",
      "source": { "node_kind": "security-scanner_Vulnerability", "external_id": "vuln-001" },
      "target": { "node_kind": "Project", "external_id": "42" }
    }
  ]
}
```

**Understanding `external_id`:**

`external_id` is the plugin's own identifier for an entity - the ID used in the plugin's system, not our internal ID.

| Context | Example | Meaning |
|---------|---------|---------|
| Plugin node | `"vuln-001"` | The plugin's internal identifier for this vulnerability |
| System node (target) | `"42"` | The actual GitLab entity ID (e.g., project ID 42) |
| Edge | `"edge-001"` | The plugin's identifier for this relationship |

**ID generation:** We hash `(plugin_id, namespace_id, kind, external_id)` to produce a deterministic `Int64` for storage. This ensures:
- Same `external_id` sent twice → same internal ID → idempotent updates via ReplacingMergeTree
- Plugins use their own naming scheme without knowing our internal IDs
- System node references resolve to the correct GitLab entities

### FR5: Message Processing

| ID | Requirement |
|----|-------------|
| FR5.1 | Handler shall deduplicate messages using NATS KV with 24h TTL |
| FR5.2 | Handler shall generate deterministic Int64 IDs: `int_hash(plugin_id, namespace_id, kind, external_id)` |
| FR5.3 | Handler shall resolve system node references using base ontology ID generation |
| FR5.4 | Handler shall verify referenced nodes exist before creating edges; reject with error if target node does not exist |
| FR5.5 | Handler shall write nodes to plugin-specific tables |
| FR5.6 | Handler shall write edges to `gl_edges` table |
| FR5.7 | Duplicate messages shall succeed silently (idempotent) |

### FR6: Schema Updates

| ID | Requirement |
|----|-------------|
| FR6.1 | System shall accept schema updates and apply them to ClickHouse tables |
| FR6.2 | System shall execute ALTER TABLE for additive changes (new columns) |
| FR6.3 | System shall reject updates that cause ClickHouse conflicts (e.g., incompatible type changes) with a clear error message explaining the conflict |
| FR6.4 | System shall track schema versions per plugin |

**Compatibility note:** Plugin owners are responsible for managing backward compatibility with their consumers. We apply schema changes as requested and reject only when ClickHouse cannot execute the change.

### FR7: Plugin Management

| ID | Requirement |
|----|-------------|
| FR7.1 | System shall provide endpoint to list all plugins for a namespace |
| FR7.2 | System shall provide endpoint to retrieve plugin info |
| FR7.3 | System shall provide endpoint to deregister plugin |
| FR7.4 | Upon deregistration, all plugin data (nodes, edges, tables) shall be deleted within 24 hours |

**APIs:**
- `GET /api/v1/mailbox/namespaces/{namespace_id}/plugins` - List all plugins for a namespace (for Rails ontology extension)
- `GET /api/v1/mailbox/plugins/{id}` - Get single plugin info
- `DELETE /api/v1/mailbox/plugins/{id}` - Deregister plugin

### FR8: Namespace Ontology Query

| ID | Requirement |
|----|-------------|
| FR8.1 | System shall provide a gRPC endpoint to retrieve the ontology for a namespace |
| FR8.2 | Response shall include base ontology nodes with `plugin_id` unset |
| FR8.3 | Response shall include plugin-defined nodes with `plugin_id` set to the source plugin |
| FR8.4 | Plugin nodes shall be grouped under a "plugins" domain |
| FR8.5 | Response shall include both base ontology edges and plugin-defined edges |

**gRPC API:** `GetNamespaceOntology(GetNamespaceOntologyRequest) returns (GetOntologyResponse)`

**Request:**
```protobuf
message GetNamespaceOntologyRequest {
  int64 namespace_id = 1;
}
```

**Response:** Uses the existing `GetOntologyResponse` message with plugin nodes merged:

```protobuf
message GetOntologyResponse {
  string schema_version = 1;
  repeated NodeDefinition nodes = 2;    // Base + plugin nodes
  repeated EdgeDefinition edges = 3;    // Base + plugin edges
  repeated DomainDefinition domains = 4; // Base + "plugins" domain
}

message NodeDefinition {
  string name = 1;
  string domain = 2;
  string description = 3;
  string primary_key = 4;
  string label_field = 5;
  repeated PropertyDefinition properties = 6;
  NodeStyle style = 7;
  optional string plugin_id = 8;  // Set for plugin nodes, empty for base ontology
}
```

**Example response with plugins:**
```json
{
  "schema_version": "1.0.0",
  "nodes": [
    {
      "name": "User",
      "domain": "core",
      "description": "A GitLab user account",
      "primary_key": "id",
      "properties": [...]
    },
    {
      "name": "security_scanner_Vulnerability",
      "domain": "plugins",
      "primary_key": "id",
      "properties": [
        { "name": "score", "data_type": "Float", "nullable": false },
        { "name": "severity", "data_type": "Enum", "enum_values": ["low", "medium", "high"] }
      ],
      "style": { "size": 30, "color": "#9333EA" },
      "plugin_id": "security-scanner"
    }
  ],
  "domains": [
    { "name": "core", "description": "Core entities", "node_names": ["User", "Project", ...] },
    { "name": "plugins", "description": "Custom nodes defined by plugins", "node_names": ["security_scanner_Vulnerability"] }
  ]
}
```

**Property type mapping (plugin → ontology):**

| Plugin PropertyType | Ontology DataType |
|---------------------|-------------------|
| string | String |
| int64 | Int |
| float | Float |
| boolean | Bool |
| date | Date |
| timestamp | DateTime |
| enum | Enum |

## Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR1 | Message processing latency: p99 < 500ms from API to ClickHouse |
| NFR2 | API availability: 99.9% uptime |
| NFR3 | Batch size limit: 1000 nodes + 1000 edges per request |
| NFR4 | Message retention: 24h in NATS before delivery required |

## Data Model

### gl_mailbox_plugins
```sql
CREATE TABLE gl_mailbox_plugins (
    plugin_id String,
    namespace_id Int64,            -- Top-level namespace, used in traversal paths
    api_key_hash String,           -- Argon2 hash of customer-provided API key
    schema String,                 -- JSON serialized PluginSchema
    created_at DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (namespace_id, plugin_id);
```

### gl_mailbox_migrations
```sql
CREATE TABLE gl_mailbox_migrations (
    plugin_id String,
    schema_version Int64,
    node_kind String,
    table_name String,
    ddl_hash String,
    applied_at DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (plugin_id, schema_version, node_kind);
```

### Generated plugin tables (example)
```sql
CREATE TABLE gl_plugin_security_scanner_vulnerability (
    id Int64,                                          -- Deterministic hash of (plugin_id, namespace_id, kind, external_id)
    traversal_path String,                             -- Format: "{organization_id}/{namespace_id}"
    severity String,
    cve_id Nullable(String),
    score Float64,
    detected_at DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id);
```

Example row: `id=8374628374, traversal_path="1/42", severity="high", ...` (organization 1, namespace 42)

## Security Considerations

1. **Authentication**: All message ingestion endpoints require valid API key (provided during registration)
2. **Authorization**: Plugins are scoped to namespaces; cross-namespace access prohibited
3. **Token storage**: Customer-provided API keys stored as Argon2 hashes
4. **Schema validation**: Strict validation prevents injection via property names
5. **Rate limiting**: TBD based on load testing

## Success Metrics

| Metric | Target |
|--------|--------|
| Plugin registrations (first 90 days) | 10+ |
| Messages processed/day | 100K+ |
| Schema validation error rate | < 1% |
| Duplicate message rate | < 5% |

## Code Quality Standards

**This is mission critical.** The implementation must read like a book—clear, top-to-bottom flow that impresses both junior developers and principal engineers.

### Readability Principles

1. **Top-to-bottom narrative:** Each file should tell a story. Public API at the top, implementation details below. A reader should understand the module's purpose within the first 20 lines.

2. **Descriptive names over comments:** If you need a comment to explain what code does, rename the function or variable instead. `validate_plugin_schema_against_registered_types()` needs no comment.

3. **Early returns:** Reduce nesting. Handle error cases first, then the happy path flows naturally.

4. **No abbreviations:** Use `configuration` not `cfg`, `repository` not `repo`, `message` not `msg`. Exceptions: `id`, `url`, universally understood terms.

5. **Explicit over clever:** A straightforward 10-line function beats a clever 3-line one that requires mental gymnastics.

### Comments Policy

**Avoid at all costs:**
- Narration of obvious code (`// Create a new vector`)
- Changelog comments (`// Fixed bug where X happened`)
- Section markers (`// === HELPERS ===`)
- Signature restatements (`// Takes X and returns Y`)
- TODO comments without actionable context

**Keep only:**
- *Why* something exists (business logic, gotchas)
- Links to specs, issues, or external documentation
- Non-obvious performance or safety constraints

### File Structure

Each module should follow this order:
1. Module-level doc comment (one sentence: what this module does)
2. Imports
3. Public types and traits
4. Public functions
5. Private helpers

### Example

```rust
//! Plugin schema validation for GRAFT registration.

use crate::ontology::BaseOntology;

pub struct SchemaValidator { /* ... */ }

impl SchemaValidator {
    pub fn validate(&self, schema: &PluginSchema) -> Result<(), ValidationError> {
        self.ensure_node_names_prefixed(schema)?;
        self.ensure_edge_targets_valid(schema)?;
        self.ensure_property_types_supported(schema)?;
        Ok(())
    }

    fn ensure_node_names_prefixed(&self, schema: &PluginSchema) -> Result<(), ValidationError> {
        // ...
    }
}
```

No comments needed. The code explains itself.

## Implementation Phases

### Phase 1: Core Infrastructure
- Plugin data types and storage
- Schema generator (plugin schema → Arrow → DDL)
- Migration engine

### Phase 2: Registration API
- Plugin registration endpoint
- API key storage and auth
- Schema validation

### Phase 3: Message Processing
- Message types and ID generation
- Arrow conversion for dynamic batches
- NATS handler implementation

### Phase 4: HTTP Ingestion
- Message submission endpoint
- Schema validation
- NATS publishing

### Phase 5: Testing & Hardening
- Integration tests
- Load testing
- Documentation
