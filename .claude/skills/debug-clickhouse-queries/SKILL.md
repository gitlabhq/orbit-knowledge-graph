---
name: debug-clickhouse-queries
description: Investigate query evaluation failures in the Knowledge Graph synthetic data pipeline. Use when queries fail or return unexpected results after running the evaluate binary.
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
DESCRIBE TABLE gl_<entity>

-- What values does a field actually contain?
SELECT <field>, count(*) FROM gl_<entity> GROUP BY <field>

-- What edge types were generated?
SELECT relationship_kind, count(*) FROM gl_edge 
GROUP BY relationship_kind ORDER BY count(*) DESC

-- Sample some traversal paths
SELECT traversal_path FROM gl_<entity> LIMIT 10
```

## Inspect the generated SQL directly

The `gkg query` command lets you see what SQL the query engine produces without running the full pipeline. The query input format is defined in `crates/ontology/schema.json`.

First, sample some traversal paths from your data:

```sql
SELECT DISTINCT traversal_path FROM gl_group LIMIT 5
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

The evaluation report shows sampling metadata for each empty result:

- `path-scoped (N entities in 'path/')` - IDs sampled from within the security context
- `global (N entities)` - Fell back to global sampling (path had no matching entities)
- `no sampling needed` - Query has no node_ids parameters

If you see `global` sampling with empty results, the sampled IDs likely don't exist in the security context path. If you see `path-scoped` with empty results, the data exists but query predicates filter it out.

To tell them apart, check if matching data exists globally:

```sql
-- Does matching data exist anywhere?
SELECT count(*) FROM gl_project 
WHERE visibility_level = 'public' AND star_count >= 100

-- How many entities are in the sampled path?
SELECT count(*) FROM gl_project 
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
SELECT state, count(*) FROM gl_user GROUP BY state ORDER BY count(*) DESC
SELECT visibility_level, count(*) FROM gl_project GROUP BY visibility_level
SELECT user_type, count(*) FROM gl_user GROUP BY user_type
```

The ontology defines enum values in two ways:
- `type: enum` with `values:` - explicitly an enum
- `type: string` with `values:` - semantically an enum but typed as string

Both should use the ontology's `values:` mapping. If not, check `fake_data.rs` to ensure it checks `field.enum_values` before falling back to pattern-based generation.

### Traversal path semantics

Traversal paths form a trie structure for access control:
- `1/2/` can access itself and descendants (`1/2/3/`, `1/2/3/4/`, etc.)
- `1/2/3/4/` can only access itself and descendants, NOT ancestors like `1/2/`
- More specific paths = more restricted access

The `startsWith(entity.traversal_path, security_context_path)` filter enforces this: entities must be at or below the security context level.

Root entities (User, Group) have shallow paths. Nested entities (Project, MergeRequest, etc.) have deeper paths like `"1/2/3/4/"`.

If queries return empty, check if the sampled path is too deep for the entities being queried:

```sql
-- Projects have nested paths
SELECT DISTINCT traversal_path FROM gl_project LIMIT 5  
-- Returns: 1/2/3/, 1/2/4/, etc.
```

## Simulator configuration impact

The synth config (`crates/xtask/simulator.yaml`) directly affects what data exists for queries to find.

### Edge types and directions

Check which edge variants are configured:

```sql
-- What edges were actually generated?
SELECT relationship_kind, source_kind, target_kind, count(*) 
FROM gl_edge 
GROUP BY relationship_kind, source_kind, target_kind 
ORDER BY count(*) DESC
```

If a query expects `MergeRequest -> Pipeline` edges but only `User -> Pipeline` exists, the query will return empty. Compare against `crates/xtask/simulator.yaml` associations section.

### Association iteration direction

Associations can iterate per-source or per-target:

```yaml
# Per-target (default): For each User, link 1 MR
AUTHORED:
  "User -> MergeRequest": 1

# Per-source: For each MR, maybe link a User  
MERGED_BY:
  "MergeRequest -> User":
    ratio: 0.3
    per: source
```

**Why this matters**: With 1000 Users and 100k MRs:
- `per: target` with ratio 1 → 1000 edges (1 per User)
- `per: source` with ratio 0.3 → 30k edges (30% of MRs)

If queries return empty for User-related edges, check:
1. Is the edge configured at all?
2. Is the iteration direction correct for the cardinality?
3. Are there enough edges generated?

### Edge direction in ontology vs queries

Queries specify edge direction: `{"type": "MERGED_BY", "from": "mr", "to": "user"}`

This translates to: `mr.id = edge.source AND edge.target = user.id`

The ontology must match. If ontology says `User -> MergeRequest` but query expects `MergeRequest -> User`, they're incompatible.

### Path compatibility for edges

Association edges must be queryable given the security filter rules. The generator uses `edge_is_queryable()`:

This matches the query engine's behavior (`crates/query-engine/src/security.rs`):
- **User** (exempt): Only filtered by relationships, not path. Edges just need same org.
- **Other entities**: Must be at or below the source's path level.

### Sampling fallback behavior

When path-scoped sampling returns no results, the sampler falls back to org-scoped sampling using `random_ids_in_org()`. This ensures sampled entities are at least in the correct organization.

If you still see mismatches, check that the sampled entity exists in the org:

```sql
-- Check if sampled user is in the right org
SELECT id, traversal_path FROM gl_user WHERE id = <sampled_user_id>
-- Path should start with the security context's org_id
```

## Places to investigate

Column name definitions:
- `crates/query-engine/src/security.rs` - security filter column name
- `crates/xtask/src/synth/arrow_schema.rs` - data generation schema

Enum value sources:
- `fixtures/ontology/nodes/**/*.yaml` - ontology definitions
- `crates/xtask/src/synth/generator/fake_data.rs` - fake value generation logic

Edge configuration:
- `crates/xtask/simulator.yaml` - relationships and associations
- `fixtures/ontology/edges/*.yaml` - edge type definitions (source/target kinds)

Traversal path construction:
- `crates/xtask/src/synth/generator/traversal.rs`
- `crates/xtask/src/synth/generator/mod.rs`

Association generation:
- `crates/xtask/src/synth/config.rs` - `AssociationConfig`, `IterationDirection`
- `crates/xtask/src/synth/generator/mod.rs` - `generate_association_edges()`

## Debugging checklist for empty results

1. **Check sampling metadata** - Is it path-scoped or global fallback?
2. **Verify edge exists in ontology** - Does `fixtures/ontology/edges/<type>.yaml` define the right direction?
3. **Verify edge configured** - Is it in `crates/xtask/simulator.yaml` associations?
4. **Check iteration direction** - Does `per: source` vs `per: target` match the cardinality?
5. **Check edge counts** - Do enough edges of this type exist?
6. **Check path compatibility** - Are source/target entities in the same traversal hierarchy?
7. **Check query predicates** - Are filters too restrictive for the generated data?

## After changes, regenerate

```bash
cargo xtask synth generate
cargo xtask synth load
cargo xtask synth evaluate
```
