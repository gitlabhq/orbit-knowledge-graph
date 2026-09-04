# openCypher frontend

Status: implemented in `crates/query-engine/opencypher`. Nothing outside the
compiler can send a statement yet; server, MCP, and consumer wiring are
tracked on issue #913, together with the token-cost and malformed-query
measurement that decides whether the text surface becomes the recommended
one.

## Overview

The query DSL is JSON. Agents compose a `graph_query` document, the compiler
validates it against `config/schemas/graph_query.schema.json` and the
ontology, deserializes it into `compiler::input::Input`, and lowers it to
parameterized ClickHouse SQL.

Issue #1242 identified the underlying problem: agents query Orbit badly
because no model has training distribution for the JSON DSL. This frontend
answers with a read-only openCypher 9 (M23) subset rather than a constrained
SQL grammar, because openCypher has the training distribution, a graph
pattern never exposes physical tables such as `gl_edge`, and hop caps and the
single linear chain fall out of the pattern grammar instead of needing join
counting on a SQL AST.

The frontend is read-only and at functional parity with the DSL: every DSL
field has a text surface, and nothing the DSL cannot express lowers
successfully.

## Approach

A statement is parsed with `pest` (`src/grammar.pest`) and lowered directly
to the compiler's `Input`. It never becomes DSL JSON and is never checked
against `graph_query.schema.json`. It enters the pipeline at
`compile_from_input`, the point where the JSON frontend has finished
deserializing, so the Input-level validators, normalize, restrict, plan,
lower, security, and codegen run unchanged.

There is no intermediate AST: the pest pair tree carries rule, text, span,
and children, so lowering walks it directly. The pattern is collected into
drafts first (`src/pattern.rs`) because the query type depends on the whole
statement, then `src/lower.rs` classifies and builds the `Input` through
constructors and JSON-visible fields only.

Design principles:

1. **openCypher 9 base, extended only for DSL parity.** Where the DSL has a
   feature openCypher 9 cannot spell, the grammar is extended (see
   [Extensions](#extensions)). Where openCypher 9 already has a spelling, no
   second one is added: GQL forms such as `{1,3}`, `ANY SHORTEST`,
   `IS Label`, `GROUP BY`, and `DATE '...'` are rejected with a hint naming
   the openCypher form.
2. **Read-only by construction.** The only statement shape is
   `MATCH ... RETURN`. Mutation, catalog, session, and transaction keywords
   are reserved so they produce a targeted error instead of an identifier.
3. **One statement, one DSL query.** The query type is inferred from the
   pattern shape and the return items. There is no `WITH`, `NEXT`, `UNION`,
   or subquery.
4. **Parity, not a superset.** Every construct lowers to an existing DSL
   field. Anything else is rejected at lowering with a hint that names the
   DSL limitation, never a parser state.
5. **Byte-equivalent SQL.** A statement and its DSL twin compile to the same
   SQL bytes and bound parameters. See [Testing](#testing).

## Grammar

The grammar is `crates/query-engine/opencypher/src/grammar.pest`; its
productions follow the M23 EBNF for the constructs kept. Points a reader of
the grammar cannot infer:

- Keywords are case-insensitive. Identifiers are case-sensitive because the
  ontology validates entity and relationship names as written, and after
  unquoting must match the DSL identifier rule
  `^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`.
- Labels, relationship types, and property keys are `SchemaName` as in the
  M23 EBNF, so a reserved word such as `CONTAINS` is a valid relationship
  type. A reserved word used as a variable (`end`) must be backticked, and
  the error says so.
- Every node needs a variable: the DSL needs a stable `id` per selector and
  uses it in output column names.
- Comma-separated pattern parts are accepted, and a variable mentioned again
  in a later part is the same node. This is how a star-shaped traversal is
  spelled; nodes are emitted in first-mention order and relationships in
  pattern order.
- Several constructs the DSL cannot express are still parsed where that is
  cheap (`OPTIONAL`, `DISTINCT`, `SKIP`, `OR`/`XOR`/`NOT`, `<>`, `count(*)`,
  unbounded ranges, GQL quantifiers, temporal literals, single-dash arrows,
  inline `WHERE`) so `syntax::reject_unsupported` can reject them with a
  hint. Tokens the grammar does not know (`WITH`, `CREATE`, `GROUP`, ...)
  get a hint from `syntax::hint_for` keyed on the offending token.

## Query type inference

| Shape | `query_type` |
|---|---|
| Pattern is wrapped in `shortestPath()` | `path_finding` |
| Exactly one edge and its far endpoint has no label | `neighbors` |
| `RETURN` contains an aggregate function | `aggregation` |
| Anything else | `traversal` |

An aggregate combined with `shortestPath()` or an unlabeled endpoint is
rejected, as is an unlabeled node in any other position.

## Lowering rules

### Common

- Each node pattern is one entry in `nodes` (`id` from the variable, `entity`
  from the label); each edge pattern is one entry in `relationships`.
- A left-pointing edge is the mirror image of a right-pointing one:
  `(a)<-[:T]-(b)` lowers to `from: b, to: a, direction: outgoing`, exactly as
  a JSON author writes it. The frontend never emits `direction: incoming`.
  `-[...]-` keeps pattern order and sets `direction: both`.
- On edges, `:A|B` lowers to a `type` array and no type lowers to the DSL
  wildcard `"*"`, which normalize specializes to the kinds valid between the
  endpoints.
- `*m..n`, `*n`, and `*..n` lower to `hops` (openCypher defaults the minimum
  to 1). `*`, `*1..`, and `*0..3` are rejected: the DSL has no unbounded or
  zero-length hops.
- `{k: v}` and `x.k = v` both lower to an `eq` filter. Predicates on one
  property AND-combine into the array form the DSL uses to repeat an
  operator. Filters are a `BTreeMap` keyed by property, and within a property
  entries are ordered by operator name, the order `serde_json`'s sorted map
  hands the JSON frontend an operator object in.
- `id` is special: `x.id = v`, `{id: v}`, and `x.id IN [...]` lower to
  `node_ids`; `x.id >= a AND x.id <= b` lowers to `id_range`. Plain filters
  would be result-equivalent but lose the planner's narrowing paths.
- `date('...')` and `datetime('...')` lower to their string payload; the
  compiler types the bound parameter from the ontology column.
- Parameters are substituted before lowering and may stand in for a value,
  an `IN` list, or the `LIMIT`. Labels, types, property names, and hop
  bounds must be literal.
- Labels, relationship types, returned columns, and filtered properties are
  checked against the ontology at lowering and reported as
  `AllowlistRejected`, the class the JSON frontend uses, with a
  case-insensitive near-miss suggestion and the full allow-list.

### Traversal

- `variable.property` items lower to that node's `columns`. A bare variable
  alone leaves `columns` unset (the DSL default: `id` only). `RETURN *` gives
  every node `columns: "*"`. openCypher's own `RETURN u` returns the whole
  node; the default won because it is what agents actually write.
- `AS` is rejected: the DSL has no column aliasing outside aggregation.
- `ORDER BY x.prop [DESC]` lowers to `order_by`.

### Aggregation

- Aggregate items lower to `aggregations`; `AS` lowers to `as`, otherwise the
  output name is the compiler's derived name (`count_mr`).
- Non-aggregate items are the group keys (openCypher's implicit grouping). A
  bare variable is a node key and leaves `columns` unset; `u.prop` items
  beside a bare `u` become its `columns` instead of separate keys, which is
  result-equivalent because the node determines its properties. `x.prop`
  without a bare `x` is a property key; `date_trunc('unit', x.prop)` is the
  truncated form.
- An undirected edge is rejected because the compiler refuses
  `direction: both` in aggregations (the OR join defeats index use).
- `ORDER BY` lowers to `aggregation_sort`; the key may be an alias, an
  output name, or a `RETURN` item repeated verbatim.

### Path finding

- Exactly two nodes and one directed edge; a left-pointing edge swaps `from`
  and `to`. The range's upper bound is `max_depth` (default 3); a lower
  bound other than 1 is rejected.
- A type is required and lowers to `rel_types`; the compiler rejects an
  untyped path search because the frontier fans out over every edge kind.
- A path variable may appear in `RETURN`; it emits nothing because the
  response always carries the path.

### Neighbors

- The labeled node is the center and the only entry in `nodes`. The arrow,
  read relative to the center, gives `neighbors.direction`; the edge's types
  give `neighbors.rel_types`.
- A range or a filter on the edge or the far endpoint is rejected. Property
  access on the neighbor or edge is rejected because their entity types are
  only known at runtime; column selection for them is
  `options.dynamic_columns`.

## Extensions

Everything the grammar accepts beyond openCypher 9, each tied to the DSL
feature that requires it.

| Extension | DSL feature | Why openCypher 9 has no spelling |
|---|---|---|
| `date_trunc('unit', x.prop)` in `RETURN` and `ORDER BY` | `group_by` `truncate` | openCypher 9 defines no temporal functions |
| `token_match(x.prop, v)`, `all_tokens(x.prop, v)`, `any_tokens(x.prop, v)` predicates | `FilterOp::TokenMatch`, `AllTokens`, `AnyTokens` | Text-index operators are Orbit-specific |
| An unlabeled endpoint turns the statement into a neighbors query | `query_type: neighbors` | openCypher has no neighborhood query; an interpretation rule, not new syntax |
| A bare variable plus its `variable.property` items selects that node's columns | `columns` on a node returned or grouped as an entity | openCypher 9 has no map projection |
| `x.id` comparisons lower to `node_ids` and `id_range` | `node_ids`, `id_range` | Plain filters lose the planner's narrowing paths |

Two DSL fields travel beside the statement as request fields: `cursor`,
because the `after` token is opaque and hash-bound, and `options`, because it
changes presentation, not semantics. `id_property` is never emitted.

## Parity matrix

| DSL field | openCypher frontend | Notes |
|---|---|---|
| `query_type` | Inferred | See query type inference |
| `nodes[].id` | Node variable | Required on every node |
| `nodes[].entity` | `:Label` | One label per node |
| `nodes[].columns` | `u.prop`, `u, u.prop`, `*` | Unset for a bare `u` and for nodes not returned |
| `nodes[].filters` | `{k: v}`, `WHERE` predicates | All operators, including token functions |
| `nodes[].node_ids` | `u.id = v`, `u.id IN [...]`, `{id: v}` | |
| `nodes[].id_range` | `u.id >= a AND u.id <= b` | Inclusive |
| `nodes[].id_property` | Not emitted | A filter on the property is result-equivalent |
| `relationships[].type` | `:A`, `:A\|B`, or no type for `"*"` | Wildcard specialized by normalize |
| `relationships[].from`, `to` | Pattern order | `<--` swaps them instead of emitting `incoming` |
| `relationships[].direction` | `-->`, `--` | |
| `relationships[].hops` | `*m..n`, `*n`, `*..n` | Bounded, 1 to 3 |
| `relationships[].filters` | `-[e:T {k: v}]->`, `WHERE e.k = v` | Traversal only |
| `aggregations[]` | `count`, `sum`, `avg`, `min`, `max`, `collect` with `AS` | `collect` still rejected by the compiler |
| `group_by` node key | Bare variable in `RETURN` | Implicit grouping |
| `group_by` property key | `u.prop` in `RETURN` | |
| `group_by` truncate | `date_trunc('unit', u.prop)` | |
| `group_by` alias | `AS name` | |
| `aggregation_sort` | `ORDER BY` in an aggregation | One key |
| `path.type` | `shortestPath()` | Only `shortest` exists |
| `path.from`, `path.to` | Endpoints in arrow order | |
| `path.max_depth` | Range upper bound | Default 3 |
| `path.rel_types` | Types on the path edge | Required |
| `neighbors.direction` | Arrow on the single edge | |
| `neighbors.rel_types` | Types on the single edge | |
| `limit` | `LIMIT n` | 1 to 1000, default 30 |
| `order_by` | `ORDER BY u.prop [DESC]` | One key |
| `cursor` | Request field beside the statement | Opaque token bound to the query hash |
| `options` | Request field beside the statement | Presentation only |

## Rejected constructs

Each rejection carries the span of the construct and a hint naming the DSL
limitation or the openCypher spelling. The hint text is the table in
`src/syntax.rs`; the classes are:

| Construct | Reason |
|---|---|
| `INSERT`, `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, `DETACH`, `DROP`, `ALTER` | Read-only |
| `CALL`, `YIELD`, `USE`, `SESSION`, transactions | No procedures, graph selection, or transactions |
| `WITH`, `UNWIND`, `NEXT`, `LET`, `FOR`, `FILTER`, `OPTIONAL MATCH`, a second `MATCH` | One statement, one MATCH |
| `UNION`, `EXCEPT`, `INTERSECT`, `OTHERWISE` | No composite queries |
| `EXISTS`, `CASE`, `COALESCE`, comprehensions, arithmetic, map projection | No expressions beyond predicates |
| `OR`, `XOR`, `NOT`, `<>`, `!=`, `=~`, comparing two properties | Filters are conjunctions of single-property comparisons |
| Anonymous node, multiple labels, `&`, `!`, `%` | One entity per selector |
| `*`, `*1..`, `*0..3`, `{m,n}`, `+` | Hops are bounded 1 to 3; GQL quantifier spelling |
| `DISTINCT`, `count(*)`, `count(DISTINCT x)` | No DSL equivalent |
| `OFFSET`, `SKIP`, multiple sort keys, `NULLS FIRST` | Keyset cursor pagination, single sort key |
| `allShortestPaths()`, `ANY SHORTEST`, path modes | Only single shortest path |
| `AS` outside aggregation, `GROUP BY`, `IS Label`, `DATE '...'`, `->`, `<-`, `-`, inline `WHERE`, `~` | No DSL equivalent or GQL spelling |
| `= NULL`, `NULL` as a value | Use `IS [NOT] NULL` |

## Validation

The JSON frontend validates in three layers: the base schema (structure and
caps), the ontology-derived schema (allow-lists), and the Input-level
validators (`Validator::check_references`, `annotate_filter_types`, cursor
decoding). The openCypher frontend skips the first two and reuses the third.

The compiler re-checks every cap the base schema also enforces (node,
relationship, hop, filter, column, and `node_ids` counts), so a drifted
frontend check surfaces as a compiler rejection. Four checks exist only in
the schema and are re-implemented against named constants in the
`opencypher` crate that mirror the schema's values: `limit` 1 to 1000,
`cursor.page_size` 1 to 1000, string literals at most 1024 characters, and
`IN` lists at most 100 values. The allow-list layer is re-implemented against
the same ontology API the derived schema is built from, and returned columns
are checked with `Ontology::validate_field` because the JSON frontend checks
column names nowhere else.

Errors reuse the compiler's variants so `failure_reason` metrics stay
comparable: allow-list misses are `AllowlistRejected`, caps `LimitExceeded`,
shape violations `Validation`, dangling variables `ReferenceError`. Parse
failures are the new client-safe `QueryError::Syntax`, whose message carries
line, column, the unexpected token, the expected tokens, and a hint.

## Compiler changes

- The former `validate` phase is split into `parse_json` (schema checks,
  canonical hash, deserialize; JSON only) and `validate_input` (cursor
  decoding, `check_references`, `annotate_filter_types`; shared). The
  `from_input` preset starts at `validate_input` and is exposed as
  `compile_from_input`.
- Types with `serde(skip)` fields gain constructors over their JSON-visible
  fields (`InputRelationship::new`, `InputPath::new`, `InputCursor::new`,
  `InputFilter::new`, `AggExpr::try_new`, `Input::with_query_hash`), so the
  frontend never names compiler-internal fields.
- Node and relationship `filters` are a `BTreeMap`. Two `HashMap`s with the
  same contents iterate in different orders, which made the JSON frontend's
  own SQL non-deterministic and byte equivalence impossible.
- The compiled output types derive `PartialEq` so twins can be compared.
- The cursor hash is FNV-1a over the token stream (keywords case-folded,
  identifiers unbackticked, strings unescaped), so whitespace and comments do
  not move it. A bound `$parameter` hashes as the literal it stands for, so a
  cursor issued for `LIMIT $n` under one binding is rejected under another.
  Tokens do not transfer between frontends.

## Parser hardening

The parser is an untrusted-input surface and a PEG parser recurses on
nested parentheses. Before parsing, the frontend checks the statement length
(`MAX_STATEMENT_BYTES`), bracket depth outside string and backtick literals
(`MAX_NESTING_DEPTH`), and the parameter map's count and serialized size.
None is configurable: they describe what a well-formed statement can look
like, and a cap that moved between environments would make the
byte-equivalence suite environment-dependent. A fuzz target beside
`fuzz_compile` in `crates/fuzz/` is deferred.

## Testing

Parity is proven at the SQL level. Because the openCypher path enters the
pipeline at `validate_input` and every later pass is shared, any byte that
differs can only come from the frontend building a different `Input`.

- Every entry in `fixtures/queries/corpus/` carries an `opencypher` twin, and
  `crates/integration-tests/tests/compiler/corpus.rs` asserts each compiles
  to the same `ParameterizedQuery`, query type, and hydration plan as its
  `query`, resolving `{{TOKEN}}` and `$sample` the way the smoke test does.
  An entry the frontend cannot spell must carry `opencypher_skip`. The JSON
  side is canonicalized first: an `id` equality or `in` filter becomes
  `node_ids`, the only spelling `{id: v}` has.
- `crates/integration-tests/tests/compiler/opencypher.rs` checks that where
  the JSON frontend rejects a query, the twin is rejected with the same
  diagnostic.
- The crate's unit tests (`src/tests.rs`) cover the lowering rules, every
  rejection hint, parameter substitution, the hash, and the caps.

The rule for new work: a compiler change that alters SQL updates the JSON
fixture once and the twin passes unchanged. A frontend change that alters
SQL for a twin without changing the JSON is a parity break.

## Examples

```opencypher
MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {state: 'merged'})
RETURN *
ORDER BY mr.merged_at DESC
LIMIT 25
```

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User", "columns": "*"},
    {"id": "mr", "entity": "MergeRequest", "columns": "*", "filters": {"state": "merged"}}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"}
  ],
  "limit": 25,
  "order_by": "-mr.merged_at"
}
```

```opencypher
MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {state: 'merged'})
RETURN u, u.id, u.username, count(mr) AS mr_count
ORDER BY mr_count DESC
LIMIT 10
```

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "u", "entity": "User", "columns": ["id", "username"]},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"}
  ],
  "group_by": ["u"],
  "aggregations": [
    { "count": "mr", "as": "mr_count" }
  ],
  "limit": 10,
  "aggregation_sort": "-mr_count"
}
```

## Scope

The frontend targets Orbit Remote only. `SqlDialect` has a single
`ClickHouse` variant and the DuckDB side of the compiler generates DDL only,
so `orbit local` keeps exposing raw SQL. If the compiler grows a DuckDB query
dialect, the frontend needs no change because it stops at `Input`.

Not yet wired, in order: a `QUERY_TYPE_OPENCYPHER` on the gRPC request and a
`language` parameter on the MCP query tool; a `query_language` field in the
`orbit_query` analytics context; Workhorse, Rails, and glab passing the new
type through; and the skill, prompt, and user-doc guidance. Whether the text
surface becomes the recommended one waits on the #913 measurement.

## DSL spellings without a text form

Every DSL field lowers from some statement, but a few spellings of the same
query are canonicalized away. They are candidates for removal from the DSL.

| DSL spelling | Frontend canonical form | Notes |
|---|---|---|
| `direction: incoming` | `from`/`to` swapped with `direction: outgoing` | Two spellings of one relationship |
| `filters: {"id": {"eq": v}}`, `{"id": {"in": [...]}}` | `node_ids` | The `node_ids` plan pushes the pinned FK onto the joined side; the filter form does not |
| `{"is_null": false}` | `is_not_null: true` | `IS [NOT] NULL` always emits `true` |
| `columns` on an aggregation node that is neither grouped nor returned | omitted | Ignored in SQL, but a virtual column there still drives hydration |
| `id_property` | not emitted | A filter on the property is result-equivalent |

## Open questions

1. Column selection for neighbor and path endpoints discovered at runtime
   stays request-level (`options.dynamic_columns`); a syntax form would need
   an extension.
2. `id_property` has no surface; whether any entity needs `node_ids` keyed by
   another property decides whether that matters.
3. `DISTINCT` could be accepted as a no-op on traversal statements.

## References

- openCypher M23 EBNF:
  <https://s3.amazonaws.com/artifacts.opencypher.org/M23/cypher.ebnf>
- openCypher TCK and grammar (Apache 2.0): <https://github.com/opencypher/openCypher>
- ISO/IEC 39075:2024 GQL: <https://www.iso.org/standard/76120.html>
- DSL design document:
  [`intermediary_llm_query_language.md`](./intermediary_llm_query_language.md)
- Issue #913, spike: evaluate a Cypher-family query frontend:
  <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/913>
- Issue #1242, offsite decision on the agent-facing grammar:
  <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1242>
