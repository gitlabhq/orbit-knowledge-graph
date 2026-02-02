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

# Step 2: Ontology Validation + Parse

```rust {3-4}
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
Now we validate that the entities and relationships in the query actually exist in our ontology, then parse into a typed struct.
-->

---

# Step 2: Ontology Validation + Parse

<div class="flex items-center justify-center gap-3 h-[75%]">

<div class="text-[0.5rem] leading-tight">

```json
{
  "$defs": {
    "EntityType": { "enum": [] },
    "RelationshipTypeName": { "enum": [] },
    "NodeProperties": {}
  }
}
```

</div>

<div v-click="1" class="text-2xl">+</div>

<div v-click="1" class="text-[0.5rem] leading-tight">

```yaml
node_type: Project
properties:
  id: { type: int64 }
  name: { type: string }
```

</div>

<div v-click="2" class="text-2xl">→</div>

<div v-click="2" class="text-sm font-mono bg-gray-100 dark:bg-gray-800 rounded px-3 py-1">
  Validator
</div>

<div v-click="3" class="flex flex-col gap-1">
  <div class="flex items-center gap-1">
    <span class="text-2xl text-red-500">↗</span>
    <span class="text-red-500 font-mono text-xs">error</span>
  </div>
  <div class="flex items-center gap-1">
    <span class="text-2xl text-green-500">↘</span>
    <span class="text-green-500 font-mono text-xs">parse()</span>
  </div>
</div>

</div>

<div v-click="3" class="text-xs text-center text-gray-500">
  At runtime: enum: [] becomes enum: ["User", "Project", "MergeRequest", ...]
</div>

<!--
The ontology fills in the schema gaps. EntityType is a placeholder - at runtime we inject the actual node types from our YAML definitions. So if you try to query a "Foobar" entity that doesn't exist, the schema validator catches it before we even parse.
-->

---

# Step 3: Semantic Validation

```rust {5}
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

<v-clicks>

- Nodes exist and have entity types
- Relationship endpoints point to declared nodes
- Column names match the entity's schema
- Aggregation targets are real nodes
- Order by references valid node + property
- <span class="text-red-500">Goal: fold this into jsonschema so we have one validation pass</span>

</v-clicks>

<!--
Semantic validation catches things the schema can't. Does node "u" actually exist when you reference it in a relationship? Does the "foobar" column exist on the User entity? This is where we catch typos and logic errors.
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
Lowering builds a SQL AST from the validated input. This is where the magic happens.
-->

---

# Step 5: Lower

<div class="flex items-center justify-center gap-4 h-[80%]">

<div class="border rounded px-4 py-2 bg-blue-50 dark:bg-blue-900">
  Validated Input
</div>

<div class="text-2xl">→</div>

<div class="border rounded px-4 py-2 bg-green-50 dark:bg-green-900">
  AST
</div>

<div class="text-2xl">→</div>

<div v-click="1" class="text-[0.4rem] leading-tight">

```rust
Query {
  select: vec![
    SelectExpr::new(Expr::col("p", "path_ids"), "path_ids"),
    SelectExpr::new(
      Expr::func("arrayConcat", vec![
        Expr::col("p", "path"),
        Expr::func("array", vec![next_tuple])
      ]), "path"),
  ],
  from: TableRef::join(
    JoinType::Inner,
    TableRef::scan("paths", "p"),
    TableRef::scan("gl_edges", "e"),
    Expr::eq(Expr::col("p", "node_id"), Expr::col("e", "source_id"))
  ),
  where_clause: Expr::and(
    Expr::lt(Expr::col("p", "depth"), Expr::param("max_depth")),
    Expr::not(Expr::func("has", vec![...]))
  ),
  ..Default::default()
}
```

</div>

<div v-click="2" class="text-3xl text-red-500 font-bold ml-2">
  Scary!
</div>

</div>

<!--
The AST is verbose. That's the point - it captures everything needed to generate correct SQL. We'll see in a minute how codegen turns this into readable SQL.
-->

---

# It's just a query builder

<div class="flex justify-center gap-12 h-[75%] items-center">

<div v-click="1" class="text-center">
<div class="text-sm font-bold mb-2">Prisma</div>
<div class="text-[0.4rem] leading-tight">

```typescript
prisma.user.findMany({
  where: { email: { contains: "@" } },
  include: { posts: true }
})
```

</div>
</div>

<div v-click="2" class="text-center">
<div class="text-sm font-bold mb-2">Drizzle</div>
<div class="text-[0.4rem] leading-tight">

```typescript
db.select()
  .from(users)
  .leftJoin(posts, eq(id, authorId))
  .where(like(email, '%@%'))
```

</div>
</div>

<div v-click="3" class="text-center">
<div class="text-sm font-bold mb-2">Us</div>
<div class="text-[0.4rem] leading-tight">

```rust
Query {
  select: vec![...],
  from: TableRef::join(...),
  where_clause: Expr::and(...),
}
```

</div>
</div>

</div>

<div v-click="4" class="text-center text-gray-500 mt-4">
  Same idea: build a tree, emit SQL
</div>

<!--
This pattern is everywhere. Prisma builds an AST from its query objects. Drizzle chains methods to build a tree. We do the same thing - just in Rust with explicit struct construction.
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

<v-clicks>

- Walk the AST, find all SELECT clauses
- For each node in the query, inject `_gkg_{node}_id` and `_gkg_{node}_type`
- Server extracts a list of tuples like `[(102, "User"), (103, "Project")]` to check permissions
- **This works because the query is an AST - we can manipulate it with code!**

</v-clicks>

<!--
We walk the AST and inject hidden columns. The server reads these after the query runs to figure out which rows to redact. You can't do this with string concatenation.
-->

---

# Step 7: Security Context

```rust {9}
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

<v-clicks>

- Walk AST, find all table scans (skip edge table, skip `gl_users`)
- Inject `WHERE startsWith(traversal_path, $MY_TRAVERSAL_PATH)` for each table
- Multiple paths? Use `startsWith(LOWEST_COMMON_PREFIX) AND (p1 OR p2 OR ...)`

</v-clicks>

<!--
Same pattern as enforce_return. Walk the AST, find table scans, inject WHERE clauses. The path encodes the GitLab namespace hierarchy so you only see data in groups you have access to.
-->

---

# Step 8: Codegen

```rust {10}
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
Final step: walk the AST and emit SQL strings.
-->

---

# Step 8: Codegen

<div class="flex items-center justify-center gap-3 h-[85%]">

<div>
<div class="text-xs font-bold mb-1 text-center">AST</div>
<div class="text-[0.3rem] leading-tight text-left">

```rust
Query {
  select: vec![
    SelectExpr::new(
      Expr::col("u", "name"),
      "u_name"
    ),
  ],
  from: TableRef::scan(
    "gl_user", "u"
  ),
  where_clause: Expr::eq(
    Expr::col("u", "id"),
    Expr::param("p0")
  ),
  limit: Some(10),
}
```

</div>
</div>

<div v-click="1" class="text-xl">→</div>

<div v-click="1">
<div class="text-xs font-bold mb-1 text-center">codegen()</div>
<div class="text-[0.25rem] leading-tight text-left">

```rust
let select_items: Vec<_> = q.select
    .iter()
    .map(|sel| {
        let expr = self.emit_expr(&sel.expr);
        match &sel.alias {
            Some(a) => format!("{expr} AS {a}"),
            None => expr,
        }
    }).collect();
parts.push(format!("SELECT {}", 
    select_items.join(", ")));
parts.push(format!("FROM {}", from.sql));
if !where_parts.is_empty() {
    parts.push(format!("WHERE {}", 
        where_parts.join(" AND ")));
}
```

</div>
</div>

<div v-click="2" class="text-xl">→</div>

<div v-click="2">
<div class="text-xs font-bold mb-1 text-center">Parameterized SQL</div>
<div class="text-[0.3rem] leading-tight text-left">

```sql
SELECT 
  u.name AS u_name,
  u.id AS _gkg_u_id,
  'User' AS _gkg_u_type
FROM gl_user AS u
WHERE u.id = {p0:Int64}
  AND startsWith(
    u.traversal_path, '1/2/')
LIMIT 10
```

</div>
</div>

</div>

<!--
Codegen is just string formatting. Walk each part of the Query struct, emit the corresponding SQL fragment, join them together. Values become parameterized placeholders.
-->

---

# Summary

<div class="flex flex-col items-center gap-1">

<div class="border-2 border-red-400 bg-red-50 dark:bg-red-900/20 rounded px-6 py-1 text-sm">
  🔓 JSON Input (untrusted)
</div>

<div class="text-lg">↓</div>

<div class="flex gap-2 items-center text-xs">
  <div class="border rounded px-2 py-1 bg-yellow-50 dark:bg-yellow-900/20">schema</div>
  <span>→</span>
  <div class="border rounded px-2 py-1 bg-yellow-50 dark:bg-yellow-900/20">ontology</div>
  <span>→</span>
  <div class="border rounded px-2 py-1 bg-yellow-50 dark:bg-yellow-900/20">parse</div>
  <span>→</span>
  <div class="border rounded px-2 py-1 bg-yellow-50 dark:bg-yellow-900/20">validate</div>
  <span class="text-red-500 ml-2">→ ❌</span>
</div>

<div class="text-lg">↓</div>

<div class="border-2 border-green-400 bg-green-50 dark:bg-green-900/20 rounded px-4 py-2">
  <div class="text-xs text-center mb-1 font-bold">🔒 Trusted Zone</div>
  <div class="flex gap-2 items-center text-xs">
    <div class="border rounded px-2 py-1">normalize</div>
    <span>→</span>
    <div class="border rounded px-2 py-1">lower</div>
    <span>→</span>
    <div class="border-2 border-blue-400 bg-blue-50 dark:bg-blue-900/20 rounded px-2 py-1 font-bold">AST</div>
    <span>→</span>
    <div class="border rounded px-2 py-1">+return</div>
    <span>→</span>
    <div class="border rounded px-2 py-1">+security</div>
    <span>→</span>
    <div class="border rounded px-2 py-1">codegen</div>
  </div>
</div>

<div class="text-lg">↓</div>

<div class="border-2 border-green-500 bg-green-100 dark:bg-green-900/30 rounded px-6 py-1 text-sm font-bold">
  Parameterized SQL ✓
</div>

</div>

<div class="mt-4 text-sm">
<v-clicks>

- Validate early, fail fast - bad queries never touch the database
- After `validate()`, success is guaranteed
- Security policies injected via AST manipulation
- Parameterized SQL output - no injection possible

</v-clicks>
</div>

<!--
The funnel: untrusted JSON enters at top, validation gates reject bad queries, and only valid queries cross into the trusted zone where we build and manipulate the AST. Output is always safe parameterized SQL.
-->

---

# Vision

<div class="flex flex-col items-center gap-6 h-[80%] justify-center">

<!-- Inputs row -->
<div class="flex gap-8 items-center justify-center">
  <div v-click="2" class="border-2 border-dashed border-gray-400 rounded px-4 py-2 text-gray-500 min-w-24 text-center">Cypher</div>
  <div v-click="1" class="border-2 border-green-500 bg-green-50 dark:bg-green-900/20 rounded px-4 py-2 font-bold text-lg min-w-24 text-center">JSON DSL</div>
  <div v-click="2" class="border-2 border-dashed border-gray-400 rounded px-4 py-2 text-gray-500 min-w-24 text-center">SQL</div>
</div>

<!-- Arrows down -->
<div class="flex gap-8 items-center justify-center">
  <span v-click="2" class="text-xl text-gray-400 min-w-24 text-center">↘</span>
  <span v-click="1" class="text-2xl min-w-24 text-center">↓</span>
  <span v-click="2" class="text-xl text-gray-400 min-w-24 text-center">↙</span>
</div>

<!-- Compiler -->
<div class="border-4 border-blue-500 bg-blue-50 dark:bg-blue-900/20 rounded-full px-10 py-5 text-center">
  <div class="font-bold text-xl">Compiler</div>
  <div class="text-gray-600 dark:text-gray-400">AST</div>
</div>

<!-- Arrows down -->
<div class="flex gap-8 items-center justify-center">
  <span v-click="3" class="text-xl text-gray-400 min-w-24 text-center">↙</span>
  <span v-click="1" class="text-2xl min-w-24 text-center">↓</span>
  <span v-click="3" class="text-xl text-gray-400 min-w-24 text-center">↘</span>
</div>

<!-- Outputs row -->
<div class="flex gap-8 items-center justify-center">
  <div v-click="3" class="border-2 border-dashed border-gray-400 rounded px-4 py-2 text-gray-500 min-w-24 text-center">Postgres</div>
  <div v-click="1" class="border-2 border-green-500 bg-green-50 dark:bg-green-900/20 rounded px-4 py-2 font-bold text-lg min-w-24 text-center">ClickHouse</div>
  <div v-click="3" class="border-2 border-dashed border-gray-400 rounded px-4 py-2 text-gray-500 min-w-24 text-center">MySQL</div>
</div>

</div>

<div v-click="4" class="text-center text-gray-500 mt-8">
  The AST is the abstraction layer - add inputs or outputs without changing the core
</div>

<!--
The compiler and AST are the stable core. Today we parse JSON and emit ClickHouse SQL. Tomorrow we could add Cypher parsing, GraphQL, or target Postgres, MySQL, whatever. The AST is the abstraction layer.
-->
