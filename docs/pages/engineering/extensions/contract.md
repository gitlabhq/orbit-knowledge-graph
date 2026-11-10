---
title: Extensions contract (v1alpha1)
---

# Extensions contract, `v1alpha1`

Orbit is a platform. It knows nothing about any integration. A downstream
**provider** implements a contract and serves declarative **package** documents.
Orbit validates each package, merges it into its model, indexes the provider's
data, and serves queries over it. A new source needs no Orbit change.

The contract has two parts. Orbit owns both.

## Part 1: the provider API (gRPC)

`orbit.extensions.v1alpha1.Provider`. Orbit is the client; a provider is the
server. The proto is the source of truth at
`crates/extensions/proto/orbit/extensions/v1alpha1/provider.proto`.

| RPC | Request | Response |
|---|---|---|
| `GetManifest` | — | `provider`, `scopes_table`, `packages[] {name, version, digest}` |
| `GetPackage` | `name`, `version` | the package document (YAML) and its digest |
| `Live` | `package`, `operation`, `scope_paths`, `user_id`, `keys`, `deadline_unix_ms`, `max_rows` | a stream of `row` {key, properties}, then one `status` {key, status} per requested key |

`GetManifest` and `GetPackage` build the model; `Live` serves live properties at
query time (see "The live block" below).

**Auth.** Orbit signs an HS256 JWT per call: `iss` `orbit`, `aud`
`orbit-provider`, `sub` `orbit`, 60-second expiry, key from the provider's
`jwt_key_file` (raw bytes, at least 32). It is sent as gRPC metadata
`authorization: Bearer <jwt>`.

**Config.** Off by default. With no `extensions.providers`, Orbit loads no
package and behaves as before.

```yaml
engine:
  modules: [extensions, namespace_deletion]
extensions:
  providers:
    - name: example
      url: http://example-provider.example.svc:8081
      jwt_key_file: /etc/secrets/example-provider/key
      refresh_secs: 60
```

Every `refresh_secs`, Orbit re-reads the manifest. When a package digest
changes, Orbit finishes its in-flight work and exits; Kubernetes restarts it
with the new model. The new digest yields new table names, so the package
re-indexes. There are no migrations.

## Part 2: the data contract

The provider writes these ClickHouse datalake tables. Orbit reads the names from
the package and manifest; it never hardcodes them.

- One **source table** per entity: the declared columns plus `traversal_path` and
  the three ETL columns Orbit reads (`_siphon_replicated_at`,
  `_siphon_watermark`, `_siphon_deleted`), `ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)`,
  `ORDER BY (traversal_path, id)`.
- One **scopes table** (named in the manifest): `id Int64, traversal_path String`
  plus ETL columns. A row with `_siphon_deleted = true` removes the scope and
  purges its graph data. Scope paths are numeric, e.g. `1/9000000000042/`.

## The package document

`apiVersion: orbit.gitlab.com/extensions/v1alpha1`, `kind: Package`. The JSON
Schema is at `crates/extensions/schema/package.v1alpha1.json`; it is the
machine-readable contract a provider validates against.

```yaml
apiVersion: orbit.gitlab.com/extensions/v1alpha1
kind: Package
metadata: { name: example.widgets, version: 0.1.0 }
graph:
  entities:
    Widget:
      label: name
      properties:
        id:     { type: int64 }
        name:   { type: string }
        status: { type: string }
  relationships:
    PART_OF:
      variants:
        - { from: Gadget, to: Widget, fk: { column: widget_id } }
    LINKS:
      variants:
        - { from: Widget, to: Gadget, table: ex_widget_links }
authorization:
  entities:
    Widget: { scope: namespace }
storage:
  store: graph
  entities:
    Widget:
      columns: { id: Int64, name: String, status: LowCardinality(String) }
      sort_key: [traversal_path, id]
sources:
  source: data_lake
  entities:
    Widget: { tables: [ex_widgets], order_by: [traversal_path, id] }
```

**Property types.** `int64`, `string`, `bool`, `float`, `timestamp`, `date`.
Entity ids are `int64`. Storage columns are raw ClickHouse type strings.

**Relationships** are a name with a list of **variants**. Each variant carries
its own `from` and `to` entity and exactly one of `fk` or `table`, so Orbit never
joins on natural keys (the provider resolves natural keys to ids itself):

- `fk`: `{ from: A, to: B, fk: { column: b_id } }` — the column on `A`'s source
  table holds `B`'s id.
- `table`: `{ from: A, to: B, table: <datalake table> }` — an edge table with
  `traversal_path String, source_id Int64, target_id Int64` plus the ETL columns,
  `ORDER BY (traversal_path, source_id, target_id)`.

Several packages may add variants to the same relationship name (ADR 024: a
package can add variants to a platform relationship). A `(name, from, to)` triple
is declared once across all loaded packages; `from` and `to` may name entities of
another loaded package, never an unknown one.

**Load rules.**

- Each entity has a required `id` property of type `int64`.
- Storage declares every property, and declares no column that is not a
  property. Nothing is inferred.
- Every entity has an authorization guard and a source table.
- No entity is declared by two packages or by the core model.
- A relationship refers only to entities a loaded package or the core model
  declares.
- A package that fails a rule is rejected with every error; the other packages
  still load.

## The live block

A package may declare a `live` block. Live properties are read per query from a
provider operation, never stored and never put in SQL. With no `live` block a
package behaves exactly as an index-only package.

```yaml
live:
  entities:
    Widget:                          # may be declared by this or another package
      properties:
        colour: { type: string }     # live-only: never in storage, never in SQL
        weight: { type: float }
      operation:
        name: widgets.get            # opaque; sent as LiveRequest.operation
        keys: [serial, batch]        # stored properties; their string values,
                                     # in this order, form each LiveRequest.keys tuple
        max_keys: 200                # default 200; a larger key set fails the query
        timeout_ms: 1500             # default 1500; the per-operation deadline
```

**Load rules.**

- A live property must not collide with a stored property of its entity, nor with
  another package's live property on the same entity.
- Every `operation.keys` entry is a stored property of the entity.
- The target entity is declared by some loaded package; a live block may target an
  entity another package owns.

**In the model.** Each live property becomes a virtual field on the entity, so it
appears in `GetGraphSchema` and the ontology tools, and an agent can `RETURN` it.
The compiler drops it from the SQL but keeps selecting the operation's key columns.
A `WHERE` or `ORDER BY` on a live property is a compile error.

**At query time.** A query that returns a live property triggers one `Live` call
per `(package, operation)` with the distinct key set of the returned rows. Every
operation of a query runs at once; the query waits only for the slowest. Each call
gets the smaller of its `timeout_ms` and the query's total live budget (config
`query.live.total_budget_ms`, default 3000). A key whose status is not `OK`, or
that misses the deadline, yields null for its live properties and a per-row
`_live_status` annotation (`<operation>=<STATUS>`); one slow or failing operation
never fails the query. A key set larger than `max_keys` fails the query with a
clear error.

**Security.** Orbit sends only the scope paths and user id from the caller's query
JWT. The provider serves only data in those scopes and uses that user's own source
token. It signs each `Live` call with the provider JWT, exactly as `GetManifest`.

## How Orbit loads a package

Orbit translates each entity into an ontology node and each relationship into an
ontology edge, registers them under one `extensions` domain, and merges them
with the bundled core sources through `OntologyArchive::from_sources`. The
resulting model carries core and extension entities together, so the indexer and
the query compiler read both. Each entity's graph table is
`gl_ext_<package>_<entity>_<digest8>`, created outside the core schema version
and the migration ledger.
