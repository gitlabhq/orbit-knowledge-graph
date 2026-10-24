# Orbit as a catalog interaction graph

This is a design proposal grounded in main at `1ccbaa84d9faf77292f39c4f1abc83b8e5cd23c8`.
The [companion YAML](orbit-interaction-graph.yaml) is a proposed language, not configuration accepted by Orbit today.

## Terms

- Topology describes entities, properties, and directed relationship variants.
- A realization connects a logical declaration to stored data or a service lookup.
- An obligation records what one query occurrence needs.
- An implementation says how to build an operation and what it needs.
- Evidence records why that choice meets a requirement.

## The existing model

The shared model has three type parameters.
They cover storage, mapping, and auth.

```rust
DataModel<S, M: Mapping<S>, A>
```

It owns a `GraphCatalog`, a `Layout<S, M>`, and an authorization catalog.
`Mapping<S>::derive` connects graph declarations to a storage schema.
The document-model test in `derive.rs` builds a model without relational or GitLab contracts.

Orbit's two current aliases specialize that structure:

```text
ClickHouseDataModel
  GraphCatalog
  Relational<ClickHouse> + ClickHouseBindings
  GitLabAuthzCatalog

DuckDbDataModel
  GraphCatalog
  Relational<DuckDb> + DuckDbBindings
  TrustedLocalCatalog
```

The service-facing `OrbitQueryModel` trait is narrower.
It requires relational mapping and GitLab-policy interfaces.
The proposed graph belongs below that layer.
SQL operators remain one language within it.

## Import facts instead of maintaining a second ontology

The ontology already declares much of the model we discussed.
The new graph should derive those facts from the existing loader and catalogs.

| Existing fact. | Source. | Proposed graph relationship. |
| --- | --- | --- |
| Entity and property types. | Node YAML and GraphCatalog. | Entity owns property. |
| Source and target entity kinds. | Edge variants. | Variant connects endpoint types. |
| Physical table and column bindings. | Backend mapping. | Declaration realized by storage. |
| FK holder and referenced property. | ForeignKey. | Variant realized by FK binding. |
| Role floor and Rails ability. | GitLabAuthzCatalog. | Resource governed by policy. |
| Virtual lookup and dependencies. | PropertyRealization::Virtual. | Property supplied by service call. |
| Materialized edge tags. | Ontology derivation and DenormalizedCatalog. | Property covered by directional edge binding. |

The YAML uses `resolve` nodes to refer to these facts.
Its `expected` values are checks on the source facts.
They do not override them.
Table names come from the active schema, including its version prefix.

## Start with four real relationship variants

```text
User ── AUTHORED ──▶ MergeRequest ── IN_PROJECT ──▶ Project
                           │
                        HAS_DIFF
                           ▼
                    MergeRequestDiff

Definition ── CALLS ──▶ Definition
```

The FK direction differs across these relationships:

```yaml
foreign_keys:
  IN_PROJECT(MergeRequest, Project):
    holder: source
    property: MergeRequest.project_id
    references: Project.id

  AUTHORED(User, MergeRequest):
    holder: target
    property: MergeRequest.author_id
    references: User.id

  HAS_DIFF(MergeRequest, MergeRequestDiff):
    holder: target
    property: MergeRequestDiff.merge_request_id
    references: MergeRequest.id
```

These are facts in the current model, written here in proposed syntax.
An incoming query binds the endpoints in reverse order.
It must still read the FK from the endpoint that owns it.
`CALLS(Definition, Definition)` has no declared FK realization.
Its remote route is `gl_code_edge`.
`HAS_DIFF` uses `gl_diff_edge`; `IN_PROJECT` and `AUTHORED` use the default edge route.

The variants also have distinct scope facts:

```yaml
scope:
  IN_PROJECT(MergeRequest, Project): namespace_anchor
  AUTHORED(User, MergeRequest): prune_to_target
  HAS_DIFF(MergeRequest, MergeRequestDiff): same_namespace
  CALLS(Definition, Definition): same_namespace
```

These facts describe how scope travels across an edge.
They do not grant access.
An MR's project link does not replace its `read_merge_request` check with the project's policy.
Other CALLS variants involving ImportedSymbol have no scope annotation in the current YAML.
The loader must preserve that absence.
It cannot apply one variant's rule to all CALLS edges.

## Authorization supplies several separate dependencies

The current declarations resolve as follows:

| Entity. | Resource type and ability. | Authorization ID. |
| --- | --- | --- |
| Project. | project / read_code. | id. |
| MergeRequest. | merge_request / read_merge_request. | id. |
| MergeRequestDiff. | merge_request / read_merge_request. | merge_request_id. |
| Definition. | project / read_code. | project_id. |
| Vulnerability. | vulnerability / read_vulnerability. | id; minimum role 25. |

Project currently uses `read_code`, with an explanatory comment in its ontology file.
The proposal keeps that fact.

Remote reads need namespace guards where the storage is scoped.
Returned resources also carry identities for Rails redaction.
Each check needs its own evidence.

```text
MergeRequestDiff result occurrence
  ├─ graph identity ───────────▶ id
  ├─ redaction identity ───────▶ merge_request_id
  ├─ namespace guard ─────────▶ traversal_path
  └─ resource permission ─────▶ read_merge_request
```

The request's authorized paths are the security boundary.
`SecurityContext.org_id` is metadata; authorized paths may span organizations.
For Vulnerability, filter paths by role 25 before collapsing their prefixes.
A Reporter path at `1/` must not absorb a qualifying Security Manager path at `1/100/`.

Global entities such as User and Runner skip namespace filtering on their node tables.
They still rely on resource redaction.
Their edge tables still need namespace guards.

The graph records the obligations before planning.
The security pass can still materialize ordinary predicates after lowering.
Logical requirements do not require constructor-time authorization or privileged filter nodes.

## An implementation node is an AND boundary

A goal can have several ways to run.
Each choice has a set of needs.
All of them must hold.

```text
Traverse HAS_DIFF(mr, diff)                         OR
  ├─ FK join                                      AND
  │    ├─ one directed hop
  │    ├─ no relationship-property predicates
  │    ├─ stored diff.merge_request_id available
  │    ├─ stored mr.id available
  │    └─ preserve outstanding result and policy obligations
  └─ edge traversal                               AND
       ├─ route gl_diff_edge
       ├─ constrain relationship and endpoint kinds
       ├─ preserve edge version/deletion semantics
       └─ preserve outstanding result and policy obligations
```

The sample builds a join directly.
It does not create a second intermediate access plan.
The builder checks each reference and connection.

FK availability is only one eligibility fact.
The existing planner also handles star shape, selectivity, scope, output demand, and target existence.
The sample does not encode all those rules yet.
Each branch needs a named requirement or a hook into code.

In particular, an ETL FK declaration does not prove that a live target row exists.
Removing a target read must preserve its existence contract.
All its other requirements must still hold.
Same-namespace scope alone is not sufficient evidence.

## Gitaly makes this a real non-relational example

The ontology already declares this property:

```yaml
patch:
  type: string
  nullable: true
  virtual:
    service: gitaly
    lookup: raw_patch
    depends_on: [project_id, id]
```

It belongs to MergeRequestDiff.
The lookup needs the diff's project ID and diff ID.
Its redaction ID serves a different purpose.

```text
Read MergeRequestDiff.patch
  ├─ require project_id ──▶ stored property read
  ├─ require id ──────────▶ stored property read
  ├─ require permission ──▶ merge_request_id ──▶ Rails decision
  └─ resolve property ────▶ Gitaly raw_patch
```

The template places content lookup after redaction.
This is a proposed ordering rule.
This review does not prove that every current executor follows it.
Current shared code already has a `ColumnResolverRegistry` and a batch resolver interface.
It checks result-vector length against input rows and rejects oversized batches.

The graph can reuse that adapter.
It makes the required inputs explicit.

```yaml
patches:
  op: service.resolve_batch
  input: authorized_diffs
  service: gitaly
  lookup: raw_patch
  arguments: [project_id, id]
  correlate_by: [MergeRequestDiff, id]
```

Compiler and executor boundaries remain real.
The full interaction graph contains both SQL-plan fragments and later service operations.
The SQL renderer handles its own fragment.
The runtime schedules the service lookup.
That is how this design supports non-relational actions already present in Orbit.

## Definition calls show backend-specific realizations

Remote Definition reads use this full replacement key:

```yaml
definition:
  sort_key: [traversal_path, project_id, branch, id]

calls:
  table: gl_code_edge
  sort_key:
    - traversal_path
    - relationship_kind
    - project_id
    - branch
    - source_id
    - target_id
    - source_kind
    - target_kind
```

Latest-row selection must use the complete key.
Deletion and mutable-value checks must retain their required position relative to version selection.
Node kind is part of endpoint identity.
Equal integer IDs alone do not mean two endpoints are the same resource.

The local DuckDB catalog has a different realization.
Its configured entities are Directory, File, Definition, and ImportedSymbol, with `gl_edge` as the local edge table.
The local model uses `TrustedLocalCatalog` rather than remote GitLab authorization.
No generic rule should inject ClickHouse FINAL reads or remote Rails calls into that local plan.

The search state includes the backend and policy domain.
It cannot choose a local trusted implementation to satisfy a remote authorization requirement.

## Coverage must be evidence, not a property-name match

`schema.yaml` requests denormalization for properties such as MergeRequest.state and Definition.definition_type.
The ontology loader only derives coverage when the edge ETL materializes that property for the relevant direction.

One source regression is especially useful:

```text
MergeRequest.state on IN_PROJECT source tags: materialized
MergeRequest.state on HAS_DIFF source tags: not materialized
```

The declarations alone would make a broad tag-pushdown rule look plausible.
Derived coverage facts prevent that error.
For mixed relationship selections, the rule must prove coverage for every selected variant and direction.
Unknown coverage means the optimization is unavailable.

The graph should attach evidence to the exact property binding.
A hint for narrowing candidates is distinct from a final predicate.
Moving a mutable condition before latest-row selection still needs semantic justification.

## Walking one concrete request

Consider an MR, its project, and its diffs.
The result also requests the patch.

```text
mr:MergeRequest ─ IN_PROJECT → p:Project
mr:MergeRequest ─ HAS_DIFF → d:MergeRequestDiff

return mr.title, p.name, d.id, d.patch
```

The planner binds each relationship to an exact variant.
It records the values and policy checks each use requires.
It then follows the `implements` edges to explore choices.

The MR owns the IN_PROJECT FK.
The diff owns the HAS_DIFF FK.
These FKs have different holders.
They do not form a common-holder star.
The actual strategy rules must decide whether to join nodes or use edge storage.

Regardless of that choice, the plan must provide:

```yaml
required_values:
  public: [mr.title, p.name, d.id]
  content_arguments: [d.project_id, d.id]
  redaction:
    mr: mr.id
    p: p.id
    d: d.merge_request_id

required_decisions:
  mr: {resource: merge_request, ability: read_merge_request}
  p: {resource: project, ability: read_code}
  d: {resource: merge_request, ability: read_merge_request}
```

Requested values prevent some scan-elision choices.
Authorization identities and content arguments prevent others.
The graph tracks all these needs together.
A result can be a row or a service value.

## What the proposal adds

Current catalogs hold topology, storage facts, and policy data.
Rust passes use those facts to choose and build plans.
This proposal adds an explicit graph of implementation choices and obligations between them.

The companion YAML covers a small slice of the design.
Its provider names refer to proposed hooks into code.
This branch does not implement those hooks.
Likewise, `requires_all`, query binding, and evidence recording describe proposed loader and planner behavior.
Parsing this YAML alone does not validate the semantic rules.

The first implementation should prove the following sequence:

1. Import the existing catalog facts without copying their authority.
2. Resolve every example node against one immutable snapshot.
3. Match IN_PROJECT, AUTHORED, and HAS_DIFF with correct FK holders.
4. Carry path, role, redaction, and content dependencies through construction.
5. Build both the ClickHouse fragment and the Gitaly lookup dependency.
6. Explain accepted and rejected implementation choices with source evidence.
7. Run every existing logical, planned, emitted, and execution assertion unchanged.

Authorization for aggregate-only results needs its own explicit contract.
Redaction cannot repair a count that already included forbidden resources.
Do not treat a role floor as proof of every per-resource ability check.
The prototype must document which aggregate policies match current behavior and which remain unresolved.

Other open questions are rule termination, cost selection, failed-construction ownership, and cache keys for request-bound policy evidence.
These belong in the design before turning the interaction graph into an execution authority.

## Source map

- [Generic data model](../../../crates/query-data-model/src/generic/mod.rs) and [non-relational test](../../../crates/query-data-model/src/derive.rs).
- [Graph declarations](../../../crates/query-data-model/src/generic/catalog.rs) and [relational mappings](../../../crates/query-data-model/src/layout/relational/mapping.rs).
- [ClickHouse mapping](../../../crates/query-data-model/src/implementations/clickhouse/mapping/mod.rs) and [DuckDB mapping](../../../crates/query-data-model/src/implementations/duckdb/mapping/mod.rs).
- [GitLab policies](../../../crates/query-data-model/src/authz/gitlab.rs) and [trusted local policies](../../../crates/query-data-model/src/authz/trusted.rs).
- [Ontology settings](../../../config/ontology/schema.yaml), [IN_PROJECT](../../../config/ontology/edges/in_project.yaml), [AUTHORED](../../../config/ontology/edges/authored.yaml), [HAS_DIFF](../../../config/ontology/edges/has_diff.yaml), and [CALLS](../../../config/ontology/edges/calls.yaml).
- [Project](../../../config/ontology/nodes/core/project.yaml), [MergeRequest](../../../config/ontology/nodes/code_review/merge_request.yaml), [MergeRequestDiff](../../../config/ontology/nodes/code_review/merge_request_diff.yaml), [Vulnerability](../../../config/ontology/nodes/security/vulnerability.yaml), and [Definition](../../../config/ontology/nodes/source_code/definition.yaml).
- [Compiler pipeline](../../../crates/query-engine/compiler/src/config.rs), [FK planning](../../../crates/query-engine/compiler/src/passes/plan/fk.rs), and [security pass](../../../crates/query-engine/compiler/src/passes/security.rs).
- [Tag coverage derivation](../../../crates/ontology/src/loading/mod.rs) and [virtual content resolver](../../../crates/query-engine/shared/src/content.rs).
