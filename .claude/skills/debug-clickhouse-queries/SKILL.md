---
name: debug-clickhouse-queries
description: Investigate query evaluation failures in the Knowledge Graph simulator. Use when queries fail or return unexpected results after running the evaluate binary.
---

# Investigating ClickHouse query failures

When the evaluate binary reports failures or empty results, the goal is figuring out whether the problem is in the generated data, the query compilation, or something else.

## Start by looking at the actual data

Connect to ClickHouse and check what's really there:

```bash
clickhouse client --query "YOUR_QUERY"
```

Some starting points:

```sql
-- What columns exist?
DESCRIBE TABLE kg_<entity>

-- What values does a field actually contain?
SELECT <field>, count(*) FROM kg_<entity> GROUP BY <field>

-- What edge types were generated?
SELECT relationship_kind, count(*) FROM kg_edges 
GROUP BY relationship_kind ORDER BY count(*) DESC

-- Sample some traversal paths
SELECT traversal_path FROM kg_<entity> LIMIT 10
```

## Inspect the generated SQL directly

The `gkg query` command lets you see what SQL the query engine produces without running the full simulator. The query input format is defined in `crates/ontology/schema.json`.

First, sample some traversal paths from your data:

```sql
SELECT DISTINCT traversal_path FROM kg_group LIMIT 5
```

Then pass them to the query command (org ID is parsed from the first segment):

```bash
# From the SDLC queries file
cargo run -p gkg -- query -t "1/2/3/" fixtures/queries/sdlc_queries.json

# Multiple traversal paths
cargo run -p gkg -- query -t "1/2/" -t "1/3/" fixtures/queries/sdlc_queries.json

# Single query inline
cargo run -p gkg -- query -t "1/" --json '{"test": {"nodes": [{"type": "Pipeline"}]}}'

# JSON output for scripting
cargo run -p gkg -- query -t "1/" --format json fixtures/queries/sdlc_queries.json
```

This shows each query's input JSON, generated SQL, and parameters. You can then run the SQL directly in ClickHouse to see what's happening and use it to guide your investigations.

## Hypothesis testing

The evaluate report shows the SQL and parameters for each query. Take a failing query and run it manually, then start removing predicates to narrow down what's breaking.

**If UNKNOWN_COLUMN**: Check if the column name in the query matches what's in the table. Could be a mismatch between the query engine and data generator.

**If 0 rows but data exists**: Compare what the query filters for against what values actually exist. The enum values might not match, or an edge type might not be generated.

**If traversal_path format error**: Look at some actual paths in the data. Malformed paths (like double slashes) would fail validation.

## Places to investigate

Column name definitions:
- `crates/query-engine/src/security.rs` - security filter column name
- `crates/simulator/src/arrow_schema.rs` - data generation schema

Enum value sources:
- `fixtures/ontology/nodes/**/*.yaml` - ontology definitions
- `crates/simulator/src/generator/fake_data.rs` - fake value generation logic

Edge configuration:
- `crates/simulator/simulator.yaml` - relationships and associations

Traversal path construction:
- `crates/simulator/src/generator/traversal.rs`
- `crates/simulator/src/generator/mod.rs`

## After changes, regenerate

```bash
cargo run --bin generate -- -c simulator.yaml
cargo run --bin load -- -c simulator.yaml
cargo run --bin evaluate -- -c simulator.yaml
```
