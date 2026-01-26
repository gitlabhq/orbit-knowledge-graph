# Simulator

Streaming data generator for the GitLab Knowledge Graph. Generates fake SDLC data from ontology definitions and populates ClickHouse directly.

## Quick Start

```bash
# Start ClickHouse and populate with fake data
./scripts/run.sh populate

# Or with custom parameters
./scripts/run.sh populate --tenants 5 --nodes-per-type 500
```

## Prerequisites

- [Colima](https://github.com/abiosoft/colima) - Docker runtime for macOS
- Docker CLI

```bash
brew install colima docker
```

## Usage

### Script Commands

```bash
./scripts/run.sh start       # Start ClickHouse
./scripts/run.sh stop        # Stop ClickHouse
./scripts/run.sh clean       # Remove container and all data
./scripts/run.sh restart     # Restart ClickHouse
./scripts/run.sh status      # Show ClickHouse status and table stats
./scripts/run.sh populate    # Start ClickHouse and populate with data
```

### Populate Options

```bash
./scripts/run.sh populate --tenants 5                    # 5 tenants
./scripts/run.sh populate --nodes-per-type 500           # 500 nodes per type
./scripts/run.sh populate --node-count User=1000         # Override specific types
./scripts/run.sh populate --edges-per-source 5           # More edges per node
./scripts/run.sh populate --dry-run                      # Preview plan only
```

### Direct Binary

```bash
cargo run --bin simulate -- \
    --ontology-path fixtures/ontology \
    --clickhouse-url http://localhost:8123 \
    --tenants 2 \
    --nodes-per-type 100 \
    --node-count User=500 \
    --edges-per-source 3
```

### All CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--ontology-path` | `fixtures/ontology` | Path to ontology YAML files |
| `--clickhouse-url` | `http://localhost:8123` | ClickHouse HTTP URL |
| `--tenants` | 2 | Number of tenants to generate |
| `--nodes-per-type` | 100 | Default nodes per node type |
| `--node-count TYPE=N` | - | Override count for specific type (repeatable) |
| `--edges-per-source` | 3 | Edges to generate per source node |
| `--batch-size` | 10000 | Batch size for inserts |
| `--dry-run` | false | Print plan without executing |

## Architecture

The simulator is fully ontology-driven:

1. **Loading ontology** - Parses YAML files via the `ontology` crate
2. **Building Arrow schemas** - Dynamically generates Arrow schemas from `NodeEntity` definitions
3. **Generating fake data** - Uses the `fake` crate with field-name-aware generators
4. **Populating ClickHouse** - Batched inserts using `clickhouse-rs`

No hardcoded entity names - all node types, fields, and edge types come from the ontology.

### Generated Tables

For each node type in the ontology, a table `kg_{node_name}` is created with:
- `tenant_id` - Tenant identifier
- All fields from the ontology definition

A unified `kg_edges` table stores all relationships:
- `tenant_id` - Tenant identifier
- `relationship_kind` - Edge type (e.g., "AUTHORED", "CONTAINS")
- `source` - Source node ID
- `source_kind` - Source node type
- `target` - Target node ID
- `target_kind` - Target node type

## Querying Generated Data

```sql
-- Count nodes by type
SELECT 'users' as type, count() FROM kg_user
UNION ALL
SELECT 'groups', count() FROM kg_group
UNION ALL
SELECT 'projects', count() FROM kg_project;

-- Count edges by relationship type
SELECT relationship_kind, count() 
FROM kg_edges 
GROUP BY relationship_kind 
ORDER BY count() DESC;

-- Find all projects in a group
SELECT p.* 
FROM kg_project p
JOIN kg_edges e ON e.target = p.id AND e.target_kind = 'Project'
WHERE e.source_kind = 'Group' 
  AND e.relationship_kind = 'CONTAINS'
  AND e.source = 123;
```
