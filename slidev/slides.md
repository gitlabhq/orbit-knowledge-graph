---
theme: seriph
background: https://cover.sli.dev
title: Query Engine Compiler
class: text-center
drawings:
  persist: false
transition: slide-left
mdc: true
---

# Query Engine Compiler

JSON → SQL compilation pipeline

<div class="abs-br m-6 text-sm opacity-50">
  GitLab Knowledge Graph
</div>

<!--
We're going to walk through how the query engine turns JSON graph queries into ClickHouse SQL. It's a straightforward pipeline with seven steps.
-->

---
layout: two-cols
layoutClass: gap-8
---

# Why not just write SQL?

<v-clicks>

- **Agent Friendly** - LLMs generate structured JSON reliably
- **Security** - No string concatenation, no injection
- **Portability** - Backend can change without breaking clients
- **Easy to Sync** - Schema derived from ontology

</v-clicks>

::right::

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "u",
      "entity": "User",
      "columns": ["username", "name"],
      "node_ids": [1]
    },
    {
      "id": "mr",
      "entity": "MergeRequest",
      "columns": ["iid", "title", "state", "source_branch"]
    },
    {
      "id": "p",
      "entity": "Project",
      "columns": ["name", "full_path"]
    }
  ],
  "relationships": [
    { "type": "AUTHORED", "from": "u", "to": "mr" },
    { "type": "IN_PROJECT", "from": "mr", "to": "p" }
  ],
  "limit": 50
}
```

<!--
Why not just write SQL? Four reasons. First, agents can generate this reliably - structured output is easier than freeform SQL. Second, there's no string interpolation so injection is impossible. Third, we can change the SQL backend without breaking API clients. Fourth, the schema comes from the ontology so it stays in sync automatically.
-->

---

# How

<div class="grid grid-cols-[1fr_auto_1fr] gap-4 items-center h-[80%]">

<div v-click="1" class="text-[0.55rem] leading-tight">

```json
{
  "query_type": "traversal",
  "nodes": [
    { "id": "u", "entity": "User",
      "columns": ["username", "name"],
      "node_ids": [1] },
    { "id": "mr", "entity": "MergeRequest",
      "columns": ["iid", "title", "state"] },
    { "id": "p", "entity": "Project",
      "columns": ["name", "full_path"] }
  ],
  "relationships": [
    { "type": "AUTHORED",
      "from": "u", "to": "mr" },
    { "type": "IN_PROJECT",
      "from": "mr", "to": "p" }
  ],
  "limit": 50
}
```

</div>

<div v-click="2" class="flex flex-col items-center gap-2">
  <div class="text-5xl text-red-500 font-bold">→</div>
  <div class="border-2 border-red-500 rounded px-4 py-2">
    <span v-if="$clicks < 3" class="text-4xl text-red-500 font-bold">?</span>
    <span v-else class="text-lg">compiler</span>
  </div>
  <div class="text-5xl text-red-500 font-bold">→</div>
</div>

<div v-click="3" class="text-[0.55rem] leading-tight">

```sql
SELECT
  u.username AS u_username,
  u.name AS u_name,
  mr.iid AS mr_iid,
  mr.title AS mr_title,
  mr.state AS mr_state,
  p.name AS p_name,
  p.full_path AS p_full_path
FROM gl_user AS u
INNER JOIN gl_edges AS e0
  ON u.id = e0.source_id
INNER JOIN gl_merge_request AS mr
  ON e0.target_id = mr.id
INNER JOIN gl_edges AS e1
  ON mr.id = e1.source_id
INNER JOIN gl_project AS p
  ON e1.target_id = p.id
WHERE u.id IN (1)
  AND e0.relationship_kind = 'AUTHORED'
  AND e1.relationship_kind = 'IN_PROJECT'
LIMIT 50
```

</div>

</div>

<!--
This is the transformation we're building. JSON query on the left, SQL on the right. The compiler in the middle is what we're going to walk through step by step.
-->

---
transition: fade-out
---

# The Compiler

```rust {all|2|3|4|5|6|7|8|9|10}
fn compile(json: &str, ontology: &Ontology, ctx: &SecurityContext) -> Result<SQL> {
    let value = validate_json(json)?;            // JSON structure ok?
    validate_ontology(&value, ontology)?;        // entities exist?
    let input = parse(value)?;                   // JSON → typed struct
    validate(&input, ontology)?;                 // references valid?
    let input = normalize(input, ontology);      // canonicalize
    let mut ast = lower(&input)?;                // build SQL AST
    let ctx = enforce_return(&mut ast, &input)?; // for redaction
    apply_security(&mut ast, ctx)?;              // tenant isolation
    codegen(&ast, ctx)                           // AST → SQL
}
```

<!--
Here's the compiler. Nine lines, each doing one thing. We'll walk through what each step does.
-->

---

# Step 1: Schema Validation

<div class="grid grid-cols-[1fr_auto_auto_auto_1fr] gap-4 items-center h-[85%]">

<div class="text-[0.5rem] leading-tight">

```json
{
  "required": ["query_type"],
  "properties": {
    "query_type": {
      "enum": ["traversal", "search",
               "aggregation", "path_finding"]
    },
    "nodes": {
      "type": "array",
      "items": { "$ref": "#/$defs/NodeSelector" }
    },
    "limit": {
      "type": "integer",
      "minimum": 1,
      "maximum": 1000
    }
  },
  "allOf": [{
    "if": { "query_type": "traversal" },
    "then": { "nodes": { "minItems": 2 } }
  }]
}
```

</div>

<div v-click="1" class="text-3xl">→</div>

<div v-click="1" class="text-[0.5rem] leading-tight">

```json
{
  "query_type": "traversal",
  "nodes": [
    { "id": "u", "entity": "User" },
    { "id": "mr", "entity": "MR" }
  ],
  "limit": 50
}
```

</div>

<div class="flex flex-col gap-6 text-sm">
  <div v-click="2" class="flex items-center gap-2">
    <span class="text-3xl text-red-500">↗</span>
    <span class="text-red-500 font-mono text-xs">
      "traversal" requires "nodes"<br/>with minItems: 2
    </span>
  </div>
  <div v-click="3" class="flex items-center gap-2">
    <span class="text-3xl text-green-500">↘</span>
    <span class="text-green-500 font-bold">Accept</span>
  </div>
</div>

</div>

<!--
The schema does structural validation. It checks required fields, types, enums, and conditional rules. If you say query_type is traversal, you need at least two nodes. Errors are specific - the message tells you exactly what's wrong.
-->

---

# Schema Validation Details

```rust
fn validate_json(json: &str) -> Result<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    collect_schema_errors(base_validator(), &value)?;
    Ok(value)
}

fn validate_ontology(value: &serde_json::Value, ontology: &Ontology) -> Result<()> {
    let schema = ontology.derive_json_schema(BASE_SCHEMA_JSON)?;
    let validator = jsonschema::validator_for(&schema)?;
    collect_schema_errors(&validator, value)
}
```

<v-click>

The ontology generates allowed values for `entity` and `relationship` fields at runtime.

</v-click>

<!--
The base schema is baked in, but the ontology derives a new schema with valid entity names. If you add a new node type to the ontology, it automatically becomes valid in queries.
-->

---

# Step 2-3: Parse + Semantic Validation

```rust {3-5}
pub fn compile(json_input: &str, ontology: &Ontology, ctx: &SecurityContext) -> Result<ParameterizedQuery> {
    let value = validate_json(json_input)?;
    validate_ontology(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    validate::validate(&input, ontology)?;
    let input = normalize::normalize(input, ontology);
    let mut node = lower::lower(&input)?;
    let result_context = enforce_return(&mut node, &input)?;
    apply_security_context(&mut node, ctx)?;
    codegen(&node, result_context)
}
```

<v-click>

**validate::validate checks:**
- Node references exist
- Relationship endpoints match declared nodes
- Filter columns exist on entities
- Aggregation targets are valid

</v-click>

<!--
After parsing into a typed Input struct, we run semantic validation. Schema validation only checks shape - this checks that node references actually point to declared nodes, that filter columns exist on those entities, and so on.
-->

---

# Step 4: Normalize

```rust {6}
pub fn compile(json_input: &str, ontology: &Ontology, ctx: &SecurityContext) -> Result<ParameterizedQuery> {
    let value = validate_json(json_input)?;
    validate_ontology(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    validate::validate(&input, ontology)?;
    let input = normalize::normalize(input, ontology);
    let mut node = lower::lower(&input)?;
    let result_context = enforce_return(&mut node, &input)?;
    apply_security_context(&mut node, ctx)?;
    codegen(&node, result_context)
}
```

<v-click>

**Transforms:**
- `"entity": "User"` → `"table": "gl_user"`
- `"columns": "*"` → `["id", "username", "email", ...]`
- Enum integers → string labels

</v-click>

<!--
Normalization puts the input in canonical form. Entity names become table names. Wildcard column selections expand to explicit lists. Enum filter values get coerced from integers to their string labels.
-->

---

# Step 5: Lower

```rust {7}
pub fn compile(json_input: &str, ontology: &Ontology, ctx: &SecurityContext) -> Result<ParameterizedQuery> {
    let value = validate_json(json_input)?;
    validate_ontology(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    validate::validate(&input, ontology)?;
    let input = normalize::normalize(input, ontology);
    let mut node = lower::lower(&input)?;
    let result_context = enforce_return(&mut node, &input)?;
    apply_security_context(&mut node, ctx)?;
    codegen(&node, result_context)
}
```

<v-click>

Input → AST (Query node with SELECT, FROM, WHERE, etc.)

</v-click>

<!--
Lowering is the big transformation. It takes the validated, normalized input and builds a SQL-oriented AST. The result is a Query node with select clauses, joins, where conditions, and so on.
-->

---

# Lower: Query Types

```rust
pub fn lower(input: &Input) -> Result<Node> {
    match input.query_type {
        QueryType::Traversal | QueryType::Search => lower_traversal(input),
        QueryType::Aggregation => lower_aggregation(input),
        QueryType::PathFinding => lower_path_finding(input),
        QueryType::Neighbors => lower_neighbors(input),
    }
}
```

<v-click>

Each query type has its own lowering strategy:
- **Traversal/Search** - JOIN chain
- **Aggregation** - GROUP BY
- **PathFinding** - Recursive CTE
- **Neighbors** - Edge table scan

</v-click>

<!--
Different query types get different SQL patterns. Traversals become join chains. Aggregations add GROUP BY. Path finding generates recursive CTEs for graph traversal.
-->

---

# Step 6: Enforce Return

```rust {8}
pub fn compile(json_input: &str, ontology: &Ontology, ctx: &SecurityContext) -> Result<ParameterizedQuery> {
    let value = validate_json(json_input)?;
    validate_ontology(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    validate::validate(&input, ontology)?;
    let input = normalize::normalize(input, ontology);
    let mut node = lower::lower(&input)?;
    let result_context = enforce_return(&mut node, &input)?;
    apply_security_context(&mut node, ctx)?;
    codegen(&node, result_context)
}
```

<v-click>

Adds mandatory columns: `_gkg_u_id`, `_gkg_u_type`

These enable post-query redaction based on user permissions.

</v-click>

<!--
The server needs to know which rows contain which entities so it can redact results the user shouldn't see. This step adds hidden ID and type columns for every node in the query.
-->

---

# Step 7: Security Context

```rust {9}
pub fn compile(json_input: &str, ontology: &Ontology, ctx: &SecurityContext) -> Result<ParameterizedQuery> {
    let value = validate_json(json_input)?;
    validate_ontology(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    validate::validate(&input, ontology)?;
    let input = normalize::normalize(input, ontology);
    let mut node = lower::lower(&input)?;
    let result_context = enforce_return(&mut node, &input)?;
    apply_security_context(&mut node, ctx)?;
    codegen(&node, result_context)
}
```

<v-click>

Injects `traversal_path` filters for tenant isolation:

```sql
WHERE startsWith(u.traversal_path, '1/')
  AND startsWith(p.traversal_path, '1/')
```

</v-click>

<!--
Multi-tenant isolation happens here. Every table scan gets a filter on traversal_path to ensure users only see data in their organization's namespace. The org_id is encoded in the path prefix.
-->

---

# Step 8: Codegen

```rust {10}
pub fn compile(json_input: &str, ontology: &Ontology, ctx: &SecurityContext) -> Result<ParameterizedQuery> {
    let value = validate_json(json_input)?;
    validate_ontology(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    validate::validate(&input, ontology)?;
    let input = normalize::normalize(input, ontology);
    let mut node = lower::lower(&input)?;
    let result_context = enforce_return(&mut node, &input)?;
    apply_security_context(&mut node, ctx)?;
    codegen(&node, result_context)
}
```

<v-click>

AST → Parameterized SQL

```rust
ParameterizedQuery {
    sql: "SELECT ... WHERE u.username = {p0:String}",
    params: {"p0": "admin"},
    result_context: ResultContext { ... }
}
```

</v-click>

<!--
Finally, codegen walks the AST and emits SQL. Values become named parameters - no string interpolation. The result includes the SQL, a map of parameter values, and metadata about which columns map to which entities.
-->

---
layout: center
class: text-center
---

# Summary

```text
JSON → Schema → Parse → Validate → Normalize → Lower → Return → Security → SQL
```

Each step does one thing. Errors surface early. SQL injection is impossible.

<!--
That's the whole pipeline. Each step has a single responsibility. Invalid input fails fast. And because codegen uses parameterized queries, SQL injection isn't possible.
-->
