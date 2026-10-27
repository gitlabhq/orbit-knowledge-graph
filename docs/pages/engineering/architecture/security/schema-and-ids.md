---
title: Schema and identity
description: How the ontology declares access rules, and how internal columns connect each result row to a GitLab resource.
order: 3
authors:
  - michaelangeloio
  - dgruzd
---

## Access rules in the ontology

The ontology declares the access rules of each entity next to its properties.
The compiler reads those rules for tier 2, and the server reads them for tier 3.
A new entity gets its access rules from its declaration, with no new code.
The [data model](../data-model.md) explains the rest of the ontology.

## Entity access settings

Each entity declares a `redaction` block.
The loader rejects an entity without one, so no entity can reach a query without access rules.

```yaml
node_type: Vulnerability
redaction:
  resource_type: vulnerability
  id_column: id
  ability: read_vulnerability
  required_role: security_manager
```

| Key | Description |
| --- | ----------- |
| `resource_type` | The GitLab resource type that redaction checks. |
| `id_column` | The column that holds the ID of that resource. Most entities use `id`. |
| `ability` | The GitLab ability that redaction checks, such as `read_merge_request`. |
| `required_role` | The minimum role on a path for tier 2 to keep that path. The default is Reporter. |

A contract check compares the `redaction` blocks with an export of the GitLab permission policies.
The check fails on each of these conditions:

- The ability is not a `read_` ability.
- GitLab has no resource type with the name in `resource_type`.
- The GitLab policy of the resource cannot grant the ability.
- The `required_role` is below the lowest GitLab role that has the ability.
- Two nodes with the same `resource_type` declare different abilities.

### Property access

Two flags on a property limit who can see the property:

| Flag | Schema tool output | Query results |
| ---- | ------------------ | ------------- |
| `hidden` | Not listed | Not available to any caller |
| `admin_only` | Listed | Removed for callers without the `admin` claim |

The traversal path is a hidden property on each namespaced entity.
The email address of a user is an admin-only property.

```yaml
properties:
  email:
    type: string
    admin_only: true
```

## Entity identity

Redaction needs the GitLab ID of each returned entity, so each entity ID maps to a known resource:

- An entity of the software development lifecycle (SDLC) graph keeps the primary key of its source row in GitLab. A merge request with ID 11 in Orbit is merge request 11 in GitLab.
- A code entity gets an ID from a hash of its project, its branch, its kind, and its location in the code. The same code at the same location always gets the same ID, so a new index run does not change the ID.

## Internal columns

The compiler adds internal columns to the output of each query.
Each internal column name starts with the reserved prefix `_orbit_`, and the loader rejects an ontology property that uses the prefix.
The alias is the variable name of a node in the query, such as `mr` in `MATCH (mr:MergeRequest)`.
Each internal column name also holds the alias, so the server maps a column to a node by its name alone.

For a node with the alias `mr`, the compiler adds these columns:

| Column | Value | Use |
| ------ | ----- | --- |
| `_orbit_mr_id` | The value of the `id_column` of the entity | The ID that redaction checks |
| `_orbit_mr_type` | The entity name, as a constant | The key to the redaction settings |
| `_orbit_mr_pk` | The entity ID, only when `id_column` is not `id` | The key for [hydration](redaction.md#4-finish-the-result) |
| `_orbit_mr_tp` | The traversal path, except in aggregations | A narrow scope for the hydration query |

The query `MATCH (mr:MergeRequest) RETURN mr.title` compiles to SQL like this example:

```sql
SELECT
  mr.title,
  mr.id               AS _orbit_mr_id,
  {p0:String}        AS _orbit_mr_type,
  mr.traversal_path   AS _orbit_mr_tp
FROM gl_merge_request AS mr
WHERE startsWith(mr.traversal_path, {p1:String})
```

The entity name in `p0` is a bound parameter, like every other value.

Redaction reads `_orbit_mr_type`, finds the redaction settings for `MergeRequest`, and checks the value of `_orbit_mr_id`.
Nodes on a path have no fixed alias.
Each path row carries the type and the ID of each node on the path.
A node that redaction checks through its owner needs the owner as another node in the same row, or redaction denies the row.

### Owner checks

Some entities have no GitLab resource of their own.
For those entities, `id_column` names the owner, and redaction checks the owner:

| Entity | `id_column` | GitLab check |
| ------ | ----------- | ------------ |
| Merge request diff | `merge_request_id` | `read_merge_request` on the merge request |
| Definition, file, and other code entities | `project_id` | `read_code` on the project |

The `_orbit_<alias>_id` column then holds the owner ID, and the `_orbit_<alias>_pk` column holds the entity ID.

## Schema introspection

Schema introspection has two sides.
Callers read the schema of the graph, and the server reads the schema of each result.

### Graph schema

Callers read the graph schema through the schema tool of the Orbit API, the MCP server, and the CLI.
The tool reads the ontology of the active schema version and returns each node type, edge type, and listed property.
The schema is the same for each caller.
The compiler applies access when the query compiles, not when a caller reads the schema.
A caller without the `admin` claim can see that `User.email` exists, but no query returns the value to that caller.

### Result schema

The compiler knows which entity each output column belongs to, because it wrote the SQL.
The compiler records that knowledge in a result contract next to the SQL.
The contract lists each node that redaction must check, with the exact names of its internal columns.

The graph store returns the rows as Apache Arrow record batches.
Each batch carries its own schema: the name and the type of each column.
The server does not guess from column names.
It looks up the names from the contract in the schema of each batch:

```plaintext
contract = compile(query).result_contract
for each batch from the graph store:
    for each node in contract.nodes:
        ids   = batch.column(node.id_column)
        types = batch.column(node.type_column)
        for each row:
            add (types[row], ids[row]) to the entities of the row
    for each path column in contract.path_columns:
        for each row:
            for each (id, type) in batch.column(path column)[row]:
                add (type, id) to the entities of the row
```

The entities of each row then go to [step 1 of the redaction exchange](redaction.md#1-collect-the-checks).

Each query type puts its entities in different columns:

| Query type | Entities that redaction checks | Columns |
| ---------- | ------------------------------ | ------- |
| Traversal | Each node in the pattern | The internal columns of each alias. A hop with more than one step adds a list column of the nodes on the hop. |
| Aggregation | Only the nodes in the group key | The internal columns of each group node, with no traversal path column |
| Path finding | Each node on each path | One path column that holds a list of (ID, type) pairs |
| Neighbors | The center node and each neighbor | The internal columns of the center alias, and a neighbor ID and neighbor type column |

A node with no ID in a row makes the row unauthorized.
A path or neighbor column with no ID or type fails the whole query.

## Design rationale

We put the access rules in the ontology, because the ontology already names each entity, table, and property.
One declaration drives the indexer, the compiler, and redaction, so the three cannot disagree about an entity.
