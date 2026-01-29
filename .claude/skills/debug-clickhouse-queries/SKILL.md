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

## Distinguishing bug types

Empty results can come from three sources:

1. **Data generation bugs** - Wrong values in the data (e.g., garbage enum values)
2. **Sampling issues** - Sampler picks narrow paths that don't contain matching data
3. **Query engine bugs** - TYPE_MISMATCH, UNKNOWN_COLUMN errors

To tell them apart, check if matching data exists globally:

```sql
-- Does matching data exist anywhere?
SELECT count(*) FROM kg_project 
WHERE visibility_level = 'public' AND star_count >= 100

-- How many entities are in the sampled path?
SELECT count(*) FROM kg_project 
WHERE startsWith(traversal_path, '3/514/522/523/524/')
```

If global count is high but sampled path has few entities, it's a sampling issue, not a data bug.

## Hypothesis testing

The evaluate report shows the SQL and parameters for each query. Take a failing query and run it manually, then start removing predicates to narrow down what's breaking.

**If UNKNOWN_COLUMN**: Check if the column name in the query matches what's in the table. Could be a mismatch between the query engine and data generator.

**If TYPE_MISMATCH**: Check if the query is comparing incompatible types (e.g., timestamp column vs string date literal).

**If 0 rows but data exists**: Compare what the query filters for against what values actually exist. The enum values might not match, or an edge type might not be generated.

**If traversal_path format error**: Look at some actual paths in the data. Malformed paths (like double slashes) would fail validation.

## Common data generation issues

### Garbage enum values

If you see values like `val188ebe1e3a382996` instead of proper enums, the fake data generator is falling back to random strings instead of using ontology values.

Quick diagnostic:

```sql
SELECT state, count(*) FROM kg_user GROUP BY state ORDER BY count(*) DESC
SELECT visibility_level, count(*) FROM kg_project GROUP BY visibility_level
SELECT user_type, count(*) FROM kg_user GROUP BY user_type
```

The ontology defines enum values in two ways:
- `type: enum` with `values:` - explicitly an enum
- `type: string` with `values:` - semantically an enum but typed as string

Both should use the ontology's `values:` mapping. If not, check `fake_data.rs` to ensure it checks `field.enum_values` before falling back to pattern-based generation.

### Traversal path mismatches

Root entities (User, Group) dont have traversal paths. Nested entities (Project, MergeRequest, etc.) have paths like `"1/2/3/4/"`.

If User queries return empty but Users exist, check if the sampled traversal path is too specific:

```sql
-- Projects have nested paths
SELECT DISTINCT traversal_path FROM kg_project LIMIT 5  
-- Returns: 1/2/3/, 1/2/4/, etc.
```

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
