# Onboarding: a guided tour of the Knowledge Graph

A reading path for engineers joining the team. Not a reference - the
[crate map](../../AGENTS.md), [design documents](../design-documents/README.md),
and [CONTEXT.md](../../CONTEXT.md) own that. This page tells you what to read
in which order, corrects the mental-model mistakes newcomers actually make,
and gives you one exercise per section to verify your understanding against
the code.

One meta-rule before you start: when a summary and a detailed document
disagree, **the code wins, then the detailed document**. Summaries drift.
Several sections below exist because a summary said one thing and the code
did another. When you hit a contradiction, treat it as a documentation bug
and fix it (or file it), not as your misreading.

## 1. The big picture

Orbit is a **read-only OLAP property graph over the GitLab OLTP data**, stored
in ClickHouse, with all access-control decisions delegated to GitLab Rails.

```plaintext
PostgreSQL ──logical replication──> Siphon (external, Analytics team)
Siphon ──CDC events──> NATS JetStream ──> ClickHouse datalake (siphon_* tables)
gkg-server --mode DispatchIndexing ──per-namespace requests──> NATS
gkg-server --mode Indexer ──ontology-driven ETL──> ClickHouse graph (gl_* tables)
gkg-server --mode Webserver <──HTTP/gRPC/MCP── users, agents, Rails
```

Three corrections to the model you probably formed from the diagram:

- **One binary, four modes.** There is no separate indexer service.
  `gkg-server --mode {Webserver,Indexer,DispatchIndexing,HealthCheck}` - see
  the [mode breakdown](../design-documents/README.md).
- **Two databases inside one ClickHouse.** The *datalake* holds raw CDC rows
  (`siphon_*`); the *graph* holds indexed property-graph tables (`gl_*`).
  The indexer is the ETL between them, and that ETL is mostly SQL generated
  from YAML, not handwritten Rust.
- **Orbit never decides who sees what.** Tenancy is compiled into every query
  via [Traversal Path](../../CONTEXT.md) prefixes; per-row permission checks
  are delegated to Rails at query time. See
  [security design](../design-documents/security.md).

**Exercise**: start the stack ([local development](local-development.md)),
then name which of the four modes touches an `issues` row UPDATE first - and
what that mode does *not* do with it. Check your answer against section 3.

## 2. The ontology is the center of gravity

The most important non-obvious fact about this codebase:
**`config/ontology/` is the product; the Rust is machinery around it.**
One YAML file per node type (`config/ontology/nodes/`) and per relationship
type (`config/ontology/edges/`). A single node YAML drives five systems:

1. **ClickHouse DDL** - the `storage:` section generates the `gl_*` table
   (columns, codecs, indexes, projections).
2. **ETL** - the `etl:` section is the SQL template the indexer runs against
   the datalake.
3. **Query validation** - the webserver validates Query DSL requests against
   declared types and properties.
4. **Redaction** - the `redaction:` block names the Rails ability checked
   per row.
5. **Edge routing** - edge YAML `table:` selects the physical edge table.

Read `config/ontology/nodes/core/project.yaml` top to bottom with the
[data model design doc](../design-documents/data_model.md) open. Note
`storage.primary_key: [traversal_path, id]` - every node table is sorted
tenant-first; that single line is the multi-tenancy story at the storage
layer.

Practical consequence: "add an entity to the graph" usually means YAML plus a
`config/SCHEMA_VERSION` bump, not Rust. Rust changes only for new
*mechanisms* (see [ADR 015](../design-documents/decisions/015_pluggable_entity_pipelines.md)
for where that line sits).

**Exercise**: `Pipeline` nodes need a new `duration_seconds` property that
already exists in `siphon_ci_pipelines`. List every file you would touch.
Then check `git log` for a recent property-addition MR and compare.

## 3. The write path: SDLC indexing

Authoritative doc: [SDLC indexing](../design-documents/indexing/sdlc_indexing.md).
Two facts that newcomers reliably get wrong:

- **Dispatch is a scheduled sweep, not event processing.** CDC events flow
  Siphon → NATS → datalake and stop there. Separately, on a timer,
  `NamespaceDispatcher` (`crates/indexer/src/modules/sdlc/dispatch/namespace.rs`)
  enumerates enabled namespaces from the datalake and publishes one indexing
  request per namespace, deduplicated by NATS itself
  (`max_messages_per_subject: 1`). No CDC event ever flows *through* the
  dispatcher. This is what "OLAP, eventually consistent" means structurally:
  the graph converges on the next sweep.
- **There are no per-entity handlers.** One generic `EntityHandler`
  (`crates/indexer/src/modules/sdlc/handler/entity.rs`) is instantiated once
  per ontology entity with a *plan* - the compiled form of that entity's
  `etl:` YAML. Extract (windowed datalake SQL) → transform (SQL projection by
  default; Rust only when SQL can't express it, see
  [ADR 015](../design-documents/decisions/015_pluggable_entity_pipelines.md))
  → load (Arrow batches into `gl_*`).

Incrementality is two glossary terms - read their
[CONTEXT.md](../../CONTEXT.md) entries now: **Watermark** (the upper time
bound of a window, fixed at dispatch so the window is deterministic across
pages and retries) and **Checkpoint** (the persisted watermark + page cursor
that lets an interrupted run resume).

Schema migrations belong to the *dispatcher*, not the indexer: new prefixed
table-set, full re-index as a **Campaign**, cutover, GC. Indexers never run
DDL. Details in `crates/indexer/AGENTS.md` and
[schema management](../design-documents/schema_management.md).

**Exercise**: the dispatcher stamps `watermark: Utc::now()` *before* the
indexer runs. Predict what breaks if extraction instead used "everything
since the last checkpoint" with no upper bound, then check your answer
against `WatermarkFilter` in `crates/indexer/src/modules/sdlc/plan/mod.rs`
and the checkpoint save in `modules/sdlc/pipeline.rs`.

## 4. The read path: one query, end to end

Authoritative docs: [querying](../design-documents/querying/README.md) and
[security](../design-documents/security.md). The best way in is to trace one
real query. Take q3 from `fixtures/queries/corpus/sdlc.yaml` (open draft MRs
with their authors): two nodes, one `AUTHORED` relationship, a limit. Note
what the caller does **not** say - no tables, no joins, no tenancy, no
permissions. The pipeline adds all four.

The compiler is a sequence of named passes; the order lives in
`crates/query-engine/compiler/src/config.rs` and the pass modules in
`crates/query-engine/compiler/src/passes/`. The three security-relevant
passes are documented in [security.md](../design-documents/security.md):
`RestrictPass` (user filters may only narrow the JWT-granted scope),
`SecurityPass` (injects `startsWith(traversal_path, ?)` into every table
access), and `CheckPass` (refuses to emit SQL if any `gl_*` alias lacks a
tenancy predicate). Every user value becomes a typed ClickHouse parameter -
the compiler never interpolates input into SQL text.

Authorization is two-layered, and the layers answer different questions:

- **Compiled in**: traversal-path prefixes - coarse, hierarchical, free at
  query time because `traversal_path` leads every primary key (section 2).
- **At runtime**: Redaction - per-row `Ability.allowed?` calls to Rails for
  what ClickHouse cannot know: confidential flags, SAML/IP rules, role
  gates. Rows are dropped, not masked.

Why can't redaction be compiled in too? Read the global-table exception in
[security.md](../design-documents/security.md): `gl_user` and `gl_runner`
have no `traversal_path` at all and are protected purely by redaction plus
the fact that they are only reachable through edge joins that do carry the
filter. That paragraph is the whole two-layer model in miniature.

**Exercise**: run q3 against your local stack. Then break it three ways -
`"draft": "yes"` (wrong type), a 4-hop relationship chain, a filter on a
property that is not `filterable` - and read which pass each error message
names. Error provenance teaches the pass order better than any diagram.

## 5. Where to go deeper

| Topic | Read |
|---|---|
| Domain vocabulary (read first, write second) | [CONTEXT.md](../../CONTEXT.md) |
| Architecture, mode breakdown | [design README](../design-documents/README.md) |
| Entities and relationships | [data model](../design-documents/data_model.md) |
| Indexing pipelines (SDLC, code, deletion) | [indexing](../design-documents/indexing/README.md) |
| Query DSL and engine | [querying](../design-documents/querying/README.md) |
| AuthZ end to end | [security](../design-documents/security.md) |
| Decisions and their context | [ADRs](../design-documents/decisions/) |
| Day-to-day commands, CI, conventions | [AGENTS.md](../../AGENTS.md) |
| Local stack | [local development](local-development.md) |
| Writing your first query | [first query](contributor-guides/first-query.md) |

When a doc here contradicts the code, remember the meta-rule from the top:
the code wins - and the contradiction is itself your first contribution.
