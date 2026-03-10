# Simulator

Streaming data generator and query evaluator for the GitLab Knowledge Graph. Generates fake SDLC data from ontology definitions, writes Parquet files, loads them into ClickHouse, and runs correctness tests against SDLC queries.

All three binaries are config-driven via a YAML file (default `simulator.yaml`).

## Quick Start

```shell
# Generate fake data to Parquet files
cargo run --bin generate

# Load Parquet data into ClickHouse
cargo run --bin load

# Run SDLC queries and collect statistics
cargo run --bin evaluate
```

## Prerequisites

- [Colima](https://github.com/abiosoft/colima) or Docker Desktop for ClickHouse
- Docker CLI

```shell
brew install colima docker
```

## Binaries

### generate

Generates fake SDLC data to Parquet files based on the ontology and generation config.

```shell
cargo run --bin generate                              # default config
cargo run --bin generate -- --config my-config.yaml   # custom config
cargo run --bin generate -- --dry-run                 # preview plan only
cargo run --bin generate -- --force                   # regenerate even if data exists
```

### load

Loads generated Parquet files into ClickHouse (creates tables, inserts data, adds indexes/projections).

```shell
cargo run --bin load                          # default config
cargo run --bin load -- --no-schema           # skip table creation (reload data only)
cargo run --bin load -- --no-data             # skip data loading (schema/indexes only)
cargo run --bin load -- --no-indexes          # skip index creation
cargo run --bin load -- --no-projections      # skip projection creation
cargo run --bin load -- --use-cli             # use clickhouse-client CLI for loading
```

### evaluate

Runs SDLC queries against the database and collects statistics. See [Query Evaluation](#query-evaluation) below.

## Architecture

The simulator is fully ontology-driven:

1. **Loading ontology** - Parses YAML files via the `ontology` crate
2. **Building Arrow schemas** - Dynamically generates Arrow schemas from `NodeEntity` definitions
3. **Generating fake data** - Uses the `fake` crate with field-name-aware generators
4. **Populating ClickHouse** - Batched inserts using `clickhouse-rs`

No hardcoded entity names - all node types, fields, and edge types come from the ontology.

### Generated Tables

For each node type in the ontology, a table `gl_{node_name}` is created with:

- `organization_id` - Organization identifier
- `traversal_path` - Hierarchical authorization path (e.g., "1/2/3")
- All fields from the ontology definition

A unified `gl_edge` table stores all relationships:

- `relationship_kind` - Edge type (e.g., "AUTHORED", "CONTAINS")
- `source` - Source node ID
- `source_kind` - Source node type
- `target` - Target node ID
- `target_kind` - Target node type

### Traversal IDs

Traversal IDs enable efficient row-level authorization using the GitLab namespace hierarchy:

- Format: `org_id/group1/group2/...` (e.g., `1/42/100`)
- Tables are ordered by `(organization_id, traversal_path, id)` for efficient range queries
- Query pattern: `WHERE traversal_path LIKE '1/42/%'` to get all entities in a subtree

## Query Evaluation

The `evaluate` binary runs SDLC queries against the database and collects statistics for correctness testing. All settings live in the YAML config file (default `simulator.yaml`).

### Usage

```shell
# Run with default config
cargo run --bin evaluate

# Custom config file
cargo run --bin evaluate -- --config simulator-slim.yaml

# Verbose logging
cargo run --bin evaluate -- --verbose
```

### Evaluation Config

Settings go under the `evaluation` key in the YAML config:

```yaml
evaluation:
  queries_path: fixtures/queries/sdlc_queries.yaml  # required
  sample_size: 100
  iterations: 1
  concurrency: 1       # >1 for concurrent load testing
  skip_cache_warm: false
  filter: "MR"         # only run queries matching pattern
  metadata_dir: metadata  # save query plans and params (optional)
  output:
    format: text       # text, json, markdown
    path: report.md    # stdout if omitted
```

### Query File Format

Queries are defined in YAML. Keys are stable identifiers (`q1`..`qN`), each with a `desc` and inline `query` (the raw GKG query DSL as a JSON string):

```yaml
q1:
  desc: List users in org
  query: '{"version":"1.1","query":{"nodes":[{"node_kind":"User","filters":[]}]}}'
```

### How It Works

1. **Parameter Sampling**: Queries with `"$sample"` placeholders in `node_ids` are automatically substituted with real IDs sampled from the database.

2. **Query Compilation**: JSON queries are compiled to SQL using the `query-engine` crate.

3. **Execution**: Each query is executed and statistics are collected:
   - Success/failure status
   - Row count
   - Execution time
   - Error messages for failures

4. **Reporting**: Results are formatted as text, JSON, or markdown with summary statistics.

## Querying Generated Data

```sql
-- Count nodes by type
SELECT 'users' as type, count() FROM gl_user
UNION ALL
SELECT 'groups', count() FROM gl_group
UNION ALL
SELECT 'projects', count() FROM gl_project;

-- Count edges by relationship type
SELECT relationship_kind, count() 
FROM gl_edge 
GROUP BY relationship_kind 
ORDER BY count() DESC;

-- Find all projects in a group
SELECT p.* 
FROM gl_project p
JOIN gl_edge e ON e.target = p.id AND e.target_kind = 'Project'
WHERE e.source_kind = 'Group' 
  AND e.relationship_kind = 'CONTAINS'
  AND e.source = 123;
```
