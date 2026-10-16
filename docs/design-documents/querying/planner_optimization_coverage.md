# Planner optimization coverage

## Scope

This temporary inventory covers traversal and aggregation planning. Neighbors,
path finding, and hydration keep their specialized planners and lowerers.

The SQL is pseudocode. It omits authorization predicates, generated aliases,
response columns, and query settings.

## Execution model

Orbit uses edge-relation execution for graph matching:

```plaintext
bind one EdgeRelation per relationship occurrence
  -> route each relation to stored edge tables
  -> choose a directional equivalent layout when available
  -> join relations through typed shared endpoints
  -> join current node rows for uncovered match values
  -> project traversal rows or aggregate match rows
```

Fixed matching has three physical shapes:

| Shape | Use |
|---|---|
| One edge relation | One fixed relationship. |
| Joined edge relations | Fixed chains, branches, and convergent patterns. |
| Bounded edge expansion | Variable relationship depth. |

A current node read is not another graph shape. It supplies canonical filters,
groups, sorts, metric properties, or policy values that edge metadata does not
cover. Output-only traversal properties use late hydration after the base
query limit.

## Current coverage

| Optimization | Status | Physical effect |
|---|---|---|
| Pinned endpoint IDs and ranges | Active | Pushes selectors into edge endpoint columns. |
| Identity-only traversal | Active | Returns endpoint IDs from the edge relation without node reads. |
| Exact endpoint metadata | Active | Applies covered filters to indexed source or target tags. |
| Canonical node fallback | Active | Joins `FINAL` node rows for uncovered match values. |
| Late output hydration | Active | Fetches traversal output properties after matching and limiting. |
| Routed relationship tables | Active | Reads each relationship from its ontology route. |
| Normalized FK relations | Active | Reads one current FK-holder row and projects the canonical edge schema. |
| FK holder property reuse | Active | Reuses required holder columns for filtering, grouping, sorting, and output. |
| Multi-table alternatives | Active | Normalizes routed arms with `UNION ALL`. |
| Typed edge joins | Active | Compares endpoint ID and kind across fixed chains and branches. |
| Current stored edges | Active | Uses `FINAL` or explicit latest-row resolution before tombstone checks. |
| Single-relation aggregation | Active | Resolves current edges before evaluating aggregate conditions. |
| Bounded expansion | Active | Emits one typed edge-join arm per requested depth. |
| Reusable physical subplans | Active | Defines a selective edge relation once and references it by `SubplanId`. |
| Adjacent frontier | Active | Derives distinct reached IDs and filters the next edge scan with them. |
| Chain frontier propagation | Active | Materializes selective intermediate relations and derives the next frontier. |
| Branch frontier reuse | Active | One shared endpoint frontier narrows every later branch arm. |
| Exact endpoint keys | Active | Filter-only aggregation endpoints use one current matching ID set. |
| Candidate node narrowing | Active | Selective equality and `IN` scans narrow exact `FINAL` key reads, which repeat filters and tombstone checks. |
| Directional layout selection | Active when declared | Chooses the layout whose sort key reaches the selected endpoint first. |
| Connectivity-only ordering | Active | Orders relations by shared node occurrences only. |
| Required-column pruning | Partial | Some edge unions and current-row scopes still carry broad columns. |
| Union-arm predicate pushdown | Partial | Some predicates remain above routed unions. |
| Materialized segment selection | Not active | Declared multi-edge materializations are not selected. |
| Shard-aware routing | Not active | The compiler does not route frontiers to physical shards. |

## Safety rules

[MR 2820](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2820)
identified rewrites that could drop graph constraints. Edge-relation execution
keeps these rules:

| Obligation | Rule |
|---|---|
| Relationship filter or projection | Read the stored relationship row. |
| Variable relationship depth | Use bounded stored edge expansion. |
| Elevated-role or cross-scope endpoint | Keep the required policy provenance. |
| Nullable property count | Read the current property and retain its aggregate argument. |
| Incomplete metadata coverage | Join the current node instead of trusting missing tags. |
| Deleted relationship version | Resolve the current edge before `_deleted = false`. |
| Typed path identity | Compare both endpoint ID and kind. |
| Boolean condition group | Keep the group atomic when split placement would change null semantics. |

## Query examples

### One edge relation

```gql orbit-query
MATCH (a:Definition {id: 1})-[:CALLS]->(b:Definition)
RETURN a.id, b.id
```

```sql
SELECT edge.source_id, edge.target_id
FROM gl_code_edge AS edge FINAL
WHERE edge.relationship_kind = 'CALLS'
  AND edge.source_id = 1
  AND edge._deleted = false
```

### Exact edge metadata

```gql orbit-query
MATCH (u:User)-[:APPROVED]->(mr:MergeRequest {state: 'merged'})
RETURN u.id, mr.id
```

```sql
SELECT edge.source_id, edge.target_id
FROM gl_edge AS edge FINAL
WHERE edge.relationship_kind = 'APPROVED'
  AND has(edge.target_tags, 'state:merged')
  AND edge._deleted = false
```

### FK relation with an uncovered endpoint filter

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/orbit'})
RETURN mr.id, p.id
```

When an aggregation only needs endpoint membership, the planner can avoid the
node join:

```sql
WITH candidate_projects AS MATERIALIZED (
  SELECT id
  FROM gl_project
  WHERE full_path = 'gitlab-org/orbit'
),
exact_projects AS MATERIALIZED (
  SELECT id
  FROM gl_project FINAL
  WHERE id IN (SELECT id FROM candidate_projects)
    AND full_path = 'gitlab-org/orbit'
    AND _deleted = false
)
SELECT count()
FROM gl_merge_request FINAL
WHERE project_id IN (SELECT id FROM exact_projects)
  AND _deleted = false
```

Candidate IDs only narrow the exact read. The exact current-row query enforces
the property filter and deletion check.

```sql
SELECT mr.id, mr.project_id
FROM gl_merge_request AS mr FINAL
INNER JOIN gl_project AS p FINAL ON mr.project_id = p.id
WHERE p.full_path = 'gitlab-org/orbit'
  AND mr._deleted = false
  AND p._deleted = false
```

The FK-holder scan also projects required MergeRequest properties. The planner
does not read `gl_merge_request` again for filtering, grouping, sorting, or
output.

### Joined edge relations

```gql orbit-query
MATCH (u:User {id: 1})-[:APPROVED]->(mr:MergeRequest)
      -[:IN_PROJECT]->(p:Project)
RETURN p.name
```

```sql
WITH first_relation AS MATERIALIZED (
  SELECT *
  FROM gl_edge FINAL
  WHERE relationship_kind = 'APPROVED'
    AND source_id = 1
    AND _deleted = false
),
mr_frontier AS (
  SELECT DISTINCT target_id AS id
  FROM first_relation
)
SELECT in_project.target_id
FROM first_relation AS approved
INNER JOIN gl_edge AS in_project FINAL
  ON approved.target_id = in_project.source_id
WHERE in_project.relationship_kind = 'IN_PROJECT'
  AND in_project.source_id IN (SELECT id FROM mr_frontier)
  AND in_project._deleted = false
```

`first_relation` retains every match row. `mr_frontier` removes duplicate IDs
only for scan pruning, so relationship multiplicity is unchanged.

### Directional layout

```gql orbit-query
MATCH (caller:Definition)-[:CALLS]->(callee:Definition {id: 12001})
RETURN caller.id
```

When the ontology declares `gl_code_edge_by_target`, the planner uses it:

```sql
SELECT source_id
FROM gl_code_edge_by_target FINAL
WHERE relationship_kind = 'CALLS'
  AND target_id = 12001
  AND _deleted = false
```

### Bounded expansion

```gql orbit-query
MATCH (a:Definition {id: 1})-[:CALLS*1..3]->(b:Definition)
RETURN a.id, b.id
```

```sql
SELECT source_id, target_id, path_nodes, depth
FROM (
  SELECT e1.source_id, e1.target_id, [e1.target_id], 1
  FROM gl_code_edge AS e1 FINAL
  WHERE e1.source_id = 1

  UNION ALL

  SELECT e1.source_id, e2.target_id, [e1.target_id, e2.target_id], 2
  FROM gl_code_edge AS e1 FINAL
  INNER JOIN gl_code_edge AS e2 FINAL
    ON e1.target_id = e2.source_id
   AND e1.target_kind = e2.source_kind
  WHERE e1.source_id = 1
)
```

## Remaining work

The next measured improvements should stay inside this execution model:

1. Prune required columns in edge unions and current node scopes.
2. Push safe endpoint predicates into every routed union arm.
3. Select declared materialized segments only after profile data justifies it.
4. Add shard routing only with a concrete distributed storage design.

Do not restore FK star or chain dispatch, logical selectivity reversal,
recursive cascade rescans, or whole-query strategy selection.
