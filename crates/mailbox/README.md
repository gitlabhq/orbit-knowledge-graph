# Mailbox Module

Plugin system for extending the GitLab Knowledge Graph with custom nodes and edges.

## Overview

The Mailbox module enables customers to define custom schemas and ingest data via HTTP. Plugin data is stored in dynamically created ClickHouse tables and can be queried alongside core Knowledge Graph entities.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          HTTP API Layer                                  │
│  POST /api/v1/mailbox/plugins       Register plugin with schema         │
│  POST /api/v1/mailbox/messages      Ingest nodes and edges              │
│  GET  /api/v1/mailbox/plugins/:id   Get plugin info                     │
└─────────────────┬───────────────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Validation Layer                                    │
│  SchemaValidator   Validates plugin schemas (prefixes, edge targets)    │
│  MessageValidator  Validates payloads against registered schemas        │
└─────────────────┬───────────────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       NATS JetStream                                     │
│  Stream: mailbox-stream                                                  │
│  Subject: mailbox.messages                                               │
└─────────────────┬───────────────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      MailboxHandler                                      │
│  - Deduplication via NATS KV (24h TTL)                                  │
│  - Deterministic ID generation                                          │
│  - Arrow batch building                                                  │
│  - Writes to gl_plugin_{plugin_id}_{node_kind} tables                   │
└─────────────────┬───────────────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        ClickHouse                                        │
│  gl_mailbox_plugins       Plugin metadata                               │
│  gl_mailbox_migrations    Schema version tracking                       │
│  gl_plugin_*_*            Dynamic per-node-kind tables                  │
│  gl_edges                 Plugin edge relationships                     │
└─────────────────────────────────────────────────────────────────────────┘
```

## Plugin Registration

### Schema Definition

Plugins define their schema with nodes and edges:

```json
{
  "plugin_id": "security-scanner",
  "namespace_id": 42,
  "api_key": "your-secret-key",
  "schema": {
    "nodes": [
      {
        "name": "security_scanner_Vulnerability",
        "properties": [
          { "name": "score", "type": "float" },
          { "name": "severity", "type": "enum", "enum_values": ["low", "medium", "high"] },
          { "name": "cve_id", "type": "string", "nullable": true }
        ]
      }
    ],
    "edges": [
      {
        "relationship_kind": "security_scanner_AFFECTS",
        "from_node_kinds": ["security_scanner_Vulnerability"],
        "to_node_kinds": ["Project", "File"]
      }
    ]
  }
}
```

### Naming Conventions

- Node names must be prefixed with `{plugin_id}_` (e.g., `security_scanner_Vulnerability`)
- Edge relationship kinds must be prefixed with `{plugin_id}_` (e.g., `security_scanner_AFFECTS`)
- Edge targets can reference:
  - Plugin's own nodes (with prefix)
  - System nodes from the base ontology (Project, User, File, etc.)
- Edges cannot target other plugins' nodes in the same namespace

### Supported Property Types

| Type | ClickHouse Type | JSON Value |
|------|-----------------|------------|
| `string` | String | `"text"` |
| `int64` | Int64 | `123` |
| `float` | Float64 | `1.5` |
| `boolean` | Bool | `true` |
| `date` | Date | `"2024-01-15"` |
| `timestamp` | DateTime64(6, 'UTC') | `"2024-01-15T10:30:00Z"` |
| `enum` | String | `"low"` |

## Message Ingestion

### Request Format

```json
{
  "message_id": "msg-001",
  "plugin_id": "security-scanner",
  "nodes": [
    {
      "external_id": "vuln-001",
      "node_kind": "security_scanner_Vulnerability",
      "properties": {
        "score": 8.5,
        "severity": "high",
        "cve_id": "CVE-2024-1234"
      }
    }
  ],
  "edges": [
    {
      "external_id": "edge-001",
      "relationship_kind": "security_scanner_AFFECTS",
      "source": {
        "node_kind": "security_scanner_Vulnerability",
        "external_id": "vuln-001"
      },
      "target": {
        "node_kind": "Project",
        "external_id": "42"
      }
    }
  ],
  "delete_nodes": [
    {
      "node_kind": "security_scanner_Vulnerability",
      "external_id": "old-vuln-001"
    }
  ],
  "delete_edges": [
    {
      "relationship_kind": "security_scanner_AFFECTS",
      "external_id": "old-edge-001"
    }
  ]
}
```

### Authentication

Requests to `/api/v1/mailbox/messages` require:
- `X-Plugin-Id` header with the plugin ID
- `X-Plugin-Token` header with the API key

### Batch Limits

- Maximum 1000 node operations per message (create + delete combined)
- Maximum 1000 edge operations per message (create + delete combined)

### Deleting Nodes and Edges

To delete nodes or edges, include them in the `delete_nodes` or `delete_edges` arrays:

- **delete_nodes**: Requires `node_kind` and `external_id` to identify the node
- **delete_edges**: Requires `relationship_kind` and `external_id` to identify the edge

Deletions use soft delete via the `_deleted` flag in ClickHouse's ReplacingMergeTree. The node/edge kinds must be defined in the plugin's schema.

## ID Generation

Node and edge IDs are deterministically generated from:
- Plugin ID
- Namespace ID
- Node/Edge kind
- External ID

This ensures idempotent ingestion - resubmitting the same data produces the same IDs.

## Table Schema

Plugin tables use ReplacingMergeTree for versioning:

```sql
CREATE TABLE gl_plugin_{plugin_id}_{node_kind} (
    id Int64,
    traversal_path String,          -- "{org_id}/{namespace_id}"
    {custom_properties},
    _version DateTime64(6, 'UTC'),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id)
```

## Extending the Mailbox Module

### Adding New Property Types

1. Add variant to `PropertyType` enum in `types/property_type.rs`
2. Implement `to_clickhouse_type()` and `to_arrow_data_type()` mappings
3. Add builder case in `ArrowConverter::build_property_column()`
4. Add validation in `MessageValidator::validate_property_value()`

### Adding New Endpoints

1. Add handler function in `http/routes.rs`
2. Register route in `create_mailbox_router()`
3. Add request/response types if needed

### Custom Validation Rules

Extend `SchemaValidator` or `MessageValidator` in the `validation/` module.

## Integration with gkg-server

To add mailbox routes to the webserver:

```rust
use mailbox::http::{create_mailbox_router, MailboxState};

let mailbox_state = MailboxState {
    plugin_store: plugin_store.clone(),
    migration_store: migration_store.clone(),
    nats: nats_services.clone(),
    ontology: ontology.clone(),
};

let router = Router::new()
    .nest("/api/v1/mailbox", create_mailbox_router(mailbox_state));
```

To register the handler module:

```rust
use mailbox::MailboxModule;

let module = MailboxModule::new(plugin_store, traversal_resolver);
engine.register_module(Box::new(module));
```

## Creating Your Own Plugin

This guide walks through building a complete mailbox plugin from scratch.

### Step 1: Design Your Schema

First, identify the data you want to add to the Knowledge Graph:

1. **Nodes**: What entities do you want to track? (e.g., vulnerabilities, deployments, incidents)
2. **Properties**: What attributes does each entity have?
3. **Edges**: How do your entities relate to existing Knowledge Graph nodes?

Example: A security scanner plugin that tracks vulnerabilities affecting projects.

### Step 2: Register Your Plugin

Register your plugin with a schema definition. All node and edge names must be prefixed with your plugin ID (with hyphens converted to underscores).

**Using curl:**

```bash
curl -X POST http://localhost:4200/api/v1/mailbox/plugins \
  -H "Content-Type: application/json" \
  -d '{
    "plugin_id": "security-scanner",
    "namespace_id": 42,
    "api_key": "your-secret-api-key-here",
    "schema": {
      "nodes": [
        {
          "name": "security_scanner_Vulnerability",
          "properties": [
            { "name": "title", "type": "string" },
            { "name": "description", "type": "string", "nullable": true },
            { "name": "score", "type": "float" },
            { "name": "severity", "type": "enum", "enum_values": ["critical", "high", "medium", "low"] },
            { "name": "cve_id", "type": "string", "nullable": true },
            { "name": "detected_at", "type": "timestamp" },
            { "name": "fixed", "type": "boolean" }
          ]
        },
        {
          "name": "security_scanner_ScanRun",
          "properties": [
            { "name": "started_at", "type": "timestamp" },
            { "name": "finished_at", "type": "timestamp", "nullable": true },
            { "name": "status", "type": "enum", "enum_values": ["running", "completed", "failed"] },
            { "name": "vulnerabilities_found", "type": "int64" }
          ]
        }
      ],
      "edges": [
        {
          "relationship_kind": "security_scanner_AFFECTS",
          "from_node_kinds": ["security_scanner_Vulnerability"],
          "to_node_kinds": ["Project", "File"]
        },
        {
          "relationship_kind": "security_scanner_FOUND_IN",
          "from_node_kinds": ["security_scanner_Vulnerability"],
          "to_node_kinds": ["security_scanner_ScanRun"]
        },
        {
          "relationship_kind": "security_scanner_SCANNED",
          "from_node_kinds": ["security_scanner_ScanRun"],
          "to_node_kinds": ["Project"]
        }
      ]
    }
  }'
```

**Using Python:**

```python
import requests

MAILBOX_URL = "http://localhost:4200/api/v1/mailbox"
NAMESPACE_ID = 42
PLUGIN_ID = "security-scanner"
API_KEY = "your-secret-api-key-here"

schema = {
    "nodes": [
        {
            "name": "security_scanner_Vulnerability",
            "properties": [
                {"name": "title", "type": "string"},
                {"name": "score", "type": "float"},
                {"name": "severity", "type": "enum", "enum_values": ["critical", "high", "medium", "low"]},
                {"name": "cve_id", "type": "string", "nullable": True},
                {"name": "detected_at", "type": "timestamp"},
                {"name": "fixed", "type": "boolean"},
            ],
        }
    ],
    "edges": [
        {
            "relationship_kind": "security_scanner_AFFECTS",
            "from_node_kinds": ["security_scanner_Vulnerability"],
            "to_node_kinds": ["Project"],
        }
    ],
}

response = requests.post(
    f"{MAILBOX_URL}/plugins",
    json={
        "plugin_id": PLUGIN_ID,
        "namespace_id": NAMESPACE_ID,
        "api_key": API_KEY,
        "schema": schema,
    },
)
print(f"Plugin registered: {response.json()}")
```

### Step 3: Ingest Data

Once registered, send messages to create nodes and edges. Each message requires authentication via headers.

**Using curl:**

```bash
curl -X POST http://localhost:4200/api/v1/mailbox/messages \
  -H "Content-Type: application/json" \
  -H "X-Plugin-Id: security-scanner" \
  -H "X-Plugin-Token: your-secret-api-key-here" \
  -d '{
    "message_id": "scan-run-001",
    "plugin_id": "security-scanner",
    "nodes": [
      {
        "external_id": "vuln-cve-2024-1234",
        "node_kind": "security_scanner_Vulnerability",
        "properties": {
          "title": "SQL Injection in login endpoint",
          "score": 9.8,
          "severity": "critical",
          "cve_id": "CVE-2024-1234",
          "detected_at": "2024-01-15T10:30:00Z",
          "fixed": false
        }
      },
      {
        "external_id": "vuln-cve-2024-5678",
        "node_kind": "security_scanner_Vulnerability",
        "properties": {
          "title": "XSS in comment field",
          "score": 6.1,
          "severity": "medium",
          "cve_id": "CVE-2024-5678",
          "detected_at": "2024-01-15T10:30:00Z",
          "fixed": false
        }
      }
    ],
    "edges": [
      {
        "external_id": "vuln-cve-2024-1234-affects-project-42",
        "relationship_kind": "security_scanner_AFFECTS",
        "source": {
          "node_kind": "security_scanner_Vulnerability",
          "external_id": "vuln-cve-2024-1234"
        },
        "target": {
          "node_kind": "Project",
          "external_id": "42"
        }
      },
      {
        "external_id": "vuln-cve-2024-5678-affects-project-42",
        "relationship_kind": "security_scanner_AFFECTS",
        "source": {
          "node_kind": "security_scanner_Vulnerability",
          "external_id": "vuln-cve-2024-5678"
        },
        "target": {
          "node_kind": "Project",
          "external_id": "42"
        }
      }
    ]
  }'
```

**Using Python:**

```python
import requests
import uuid

def send_vulnerabilities(vulnerabilities, project_id):
    """Send vulnerability data to the mailbox."""
    nodes = []
    edges = []

    for vuln in vulnerabilities:
        external_id = f"vuln-{vuln['cve_id']}"

        nodes.append({
            "external_id": external_id,
            "node_kind": "security_scanner_Vulnerability",
            "properties": {
                "title": vuln["title"],
                "score": vuln["score"],
                "severity": vuln["severity"],
                "cve_id": vuln["cve_id"],
                "detected_at": vuln["detected_at"],
                "fixed": vuln.get("fixed", False),
            },
        })

        edges.append({
            "external_id": f"{external_id}-affects-project-{project_id}",
            "relationship_kind": "security_scanner_AFFECTS",
            "source": {
                "node_kind": "security_scanner_Vulnerability",
                "external_id": external_id,
            },
            "target": {
                "node_kind": "Project",
                "external_id": str(project_id),
            },
        })

    response = requests.post(
        f"{MAILBOX_URL}/messages",
        headers={
            "X-Plugin-Id": PLUGIN_ID,
            "X-Plugin-Token": API_KEY,
        },
        json={
            "message_id": str(uuid.uuid4()),
            "plugin_id": PLUGIN_ID,
            "nodes": nodes,
            "edges": edges,
        },
    )
    return response.json()

# Example usage
vulnerabilities = [
    {
        "cve_id": "CVE-2024-1234",
        "title": "SQL Injection in login endpoint",
        "score": 9.8,
        "severity": "critical",
        "detected_at": "2024-01-15T10:30:00Z",
    },
    {
        "cve_id": "CVE-2024-5678",
        "title": "XSS in comment field",
        "score": 6.1,
        "severity": "medium",
        "detected_at": "2024-01-15T10:30:00Z",
    },
]

result = send_vulnerabilities(vulnerabilities, project_id=42)
print(f"Message accepted: {result}")
```

### Step 4: Update and Delete Data

To update existing nodes, send them again with the same `external_id`. The system uses the `_version` timestamp to keep the latest version.

To delete nodes or edges, include them in the delete arrays:

```python
def mark_vulnerability_fixed(cve_id, project_id):
    """Update a vulnerability as fixed and remove the AFFECTS edge."""
    response = requests.post(
        f"{MAILBOX_URL}/messages",
        headers={
            "X-Plugin-Id": PLUGIN_ID,
            "X-Plugin-Token": API_KEY,
        },
        json={
            "message_id": str(uuid.uuid4()),
            "plugin_id": PLUGIN_ID,
            "nodes": [
                {
                    "external_id": f"vuln-{cve_id}",
                    "node_kind": "security_scanner_Vulnerability",
                    "properties": {
                        "title": "...",  # Include all required properties
                        "score": 9.8,
                        "severity": "critical",
                        "cve_id": cve_id,
                        "detected_at": "2024-01-15T10:30:00Z",
                        "fixed": True,  # Updated value
                    },
                }
            ],
            "delete_edges": [
                {
                    "relationship_kind": "security_scanner_AFFECTS",
                    "external_id": f"vuln-{cve_id}-affects-project-{project_id}",
                }
            ],
        },
    )
    return response.json()
```

### Step 5: Query Your Data

Once ingested, your plugin data can be queried via the Knowledge Graph API alongside core entities:

```bash
# Find all critical vulnerabilities affecting a project
curl -X POST http://localhost:4200/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": [
        {"node": "v", "kind": "security_scanner_Vulnerability"},
        {"edge": "e", "kind": "security_scanner_AFFECTS", "from": "v", "to": "p"},
        {"node": "p", "kind": "Project"}
      ],
      "where": [
        {"field": "v.severity", "op": "=", "value": "critical"},
        {"field": "p.id", "op": "=", "value": 42}
      ],
      "return": ["v.title", "v.score", "v.cve_id"]
    }
  }'
```

### Complete Example: CI/CD Deployment Tracker

Here's a complete example of a plugin that tracks deployments:

```python
import requests
import uuid
from datetime import datetime

MAILBOX_URL = "http://localhost:4200/api/v1/mailbox"
PLUGIN_ID = "deploy-tracker"
NAMESPACE_ID = 42
API_KEY = "deployment-secret-key"

def register_deployment_plugin():
    """Register the deployment tracking plugin."""
    schema = {
        "nodes": [
            {
                "name": "deploy_tracker_Deployment",
                "properties": [
                    {"name": "environment", "type": "enum", "enum_values": ["production", "staging", "development"]},
                    {"name": "version", "type": "string"},
                    {"name": "deployed_at", "type": "timestamp"},
                    {"name": "deployed_by", "type": "string"},
                    {"name": "status", "type": "enum", "enum_values": ["pending", "in_progress", "succeeded", "failed", "rolled_back"]},
                    {"name": "rollback_of", "type": "string", "nullable": True},
                ],
            }
        ],
        "edges": [
            {
                "relationship_kind": "deploy_tracker_DEPLOYED_TO",
                "from_node_kinds": ["deploy_tracker_Deployment"],
                "to_node_kinds": ["Project"],
            },
            {
                "relationship_kind": "deploy_tracker_TRIGGERED_BY",
                "from_node_kinds": ["deploy_tracker_Deployment"],
                "to_node_kinds": ["Pipeline"],
            },
        ],
    }

    return requests.post(
        f"{MAILBOX_URL}/plugins",
        json={
            "plugin_id": PLUGIN_ID,
            "namespace_id": NAMESPACE_ID,
            "api_key": API_KEY,
            "schema": schema,
        },
    )

def record_deployment(project_id, pipeline_id, environment, version, deployed_by):
    """Record a new deployment."""
    deployment_id = f"deploy-{project_id}-{environment}-{version}"

    return requests.post(
        f"{MAILBOX_URL}/messages",
        headers={
            "X-Plugin-Id": PLUGIN_ID,
            "X-Plugin-Token": API_KEY,
        },
        json={
            "message_id": str(uuid.uuid4()),
            "plugin_id": PLUGIN_ID,
            "nodes": [
                {
                    "external_id": deployment_id,
                    "node_kind": "deploy_tracker_Deployment",
                    "properties": {
                        "environment": environment,
                        "version": version,
                        "deployed_at": datetime.utcnow().isoformat() + "Z",
                        "deployed_by": deployed_by,
                        "status": "in_progress",
                        "rollback_of": None,
                    },
                }
            ],
            "edges": [
                {
                    "external_id": f"{deployment_id}-to-project",
                    "relationship_kind": "deploy_tracker_DEPLOYED_TO",
                    "source": {"node_kind": "deploy_tracker_Deployment", "external_id": deployment_id},
                    "target": {"node_kind": "Project", "external_id": str(project_id)},
                },
                {
                    "external_id": f"{deployment_id}-from-pipeline",
                    "relationship_kind": "deploy_tracker_TRIGGERED_BY",
                    "source": {"node_kind": "deploy_tracker_Deployment", "external_id": deployment_id},
                    "target": {"node_kind": "Pipeline", "external_id": str(pipeline_id)},
                },
            ],
        },
    )

def update_deployment_status(project_id, environment, version, status):
    """Update deployment status (e.g., succeeded, failed)."""
    deployment_id = f"deploy-{project_id}-{environment}-{version}"

    return requests.post(
        f"{MAILBOX_URL}/messages",
        headers={
            "X-Plugin-Id": PLUGIN_ID,
            "X-Plugin-Token": API_KEY,
        },
        json={
            "message_id": str(uuid.uuid4()),
            "plugin_id": PLUGIN_ID,
            "nodes": [
                {
                    "external_id": deployment_id,
                    "node_kind": "deploy_tracker_Deployment",
                    "properties": {
                        "environment": environment,
                        "version": version,
                        "deployed_at": datetime.utcnow().isoformat() + "Z",
                        "deployed_by": "ci-bot",
                        "status": status,
                        "rollback_of": None,
                    },
                }
            ],
        },
    )

# Usage
if __name__ == "__main__":
    # Register plugin (only needed once)
    register_deployment_plugin()

    # Record a deployment
    record_deployment(
        project_id=42,
        pipeline_id=12345,
        environment="production",
        version="v2.1.0",
        deployed_by="jane.doe",
    )

    # Update status when deployment completes
    update_deployment_status(
        project_id=42,
        environment="production",
        version="v2.1.0",
        status="succeeded",
    )
```

### Best Practices

1. **Use descriptive external IDs**: Make them human-readable and deterministic (e.g., `vuln-CVE-2024-1234` not `uuid-abc123`)

2. **Batch your operations**: Send multiple nodes/edges in a single message when possible (up to 1000 each)

3. **Use idempotent message IDs**: If you retry a failed request, use the same `message_id` to prevent duplicates

4. **Handle errors gracefully**: Check response status codes and implement retry logic with exponential backoff

5. **Validate locally first**: Ensure your data matches the schema before sending to avoid validation errors

6. **Use appropriate property types**: Choose the right type for your data (e.g., `timestamp` for dates, `enum` for fixed values)

7. **Keep API keys secure**: Store them in environment variables or secret managers, never in code

## Testing

Run unit tests:
```bash
cargo test -p mailbox
```

Integration tests require Docker for NATS/ClickHouse testcontainers.
