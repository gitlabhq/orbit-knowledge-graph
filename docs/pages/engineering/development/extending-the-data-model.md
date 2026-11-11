# Extending the data model

The data model is being built in the [data model spike MR](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/3346).
This guide shows its packages and APIs, and it changes with the code.
[ADR 024](../adr/024_pluggable_data_model.md) records the design and explains how it works.

Each extension writes only a package, a surface or a backend:

| To add | Write | Leave untouched |
|---|---|---|
| A GitLab entity | Fragments in a `gitlab.*` package | The compiler |
| An indexed third-party source | A package with a `source` fragment | The compiler and the indexing engine |
| A live third-party entity | A package with a `live` store | The surfaces and the compiler phases |
| A backend | A `StorageBackend`, its plan language and its renderer | The surfaces and the shared phases |
| A tenant overlay | A package that loads for one tenant | Everything else |
| A query language | A surface that builds logical plans | The planner |

## Add a GitLab entity

An entity has one fragment of each kind it needs. These are the fragments for `MergeRequest` in `gitlab.code_review`.

The graph fragment declares every property, with its type and its default value. It also declares the relationship variants the package adds, with their cardinality. It says nothing about columns or tables.

```yaml
# packages/gitlab.code_review/graph.yaml
kind: graph
package: gitlab.code_review
entities:
  MergeRequest:
    identity: id
    label: title
    default_columns: [id, iid, title, state]
    properties:
      id: {type: int64}
      iid: {type: int64}
      title: {type: string, default: ""}
      state: {type: enum, values: {1: opened, 2: closed, 3: merged, 4: locked}, default: opened}
      author_id: {type: int64?}
      traversal_path: {type: string}
relationships:
  AUTHORED:
    variants: {User -> MergeRequest: {cardinality: one_to_many}}
  APPROVED:
    variants: {User -> MergeRequest: {cardinality: many_to_many}}
```

The authorization fragment declares the entity's scope and the permission a read needs. `enforce` holds the policy for each backend that stores the entity. `checks` name a provider that checks the returned rows:

```yaml
# packages/gitlab.code_review/authorization.yaml
kind: authorization
package: gitlab.code_review
entities:
  MergeRequest:
    scope: namespace
    permission: read
    enforce:
      clickhouse: {scope_key: traversal_path}
    checks:
      - {provider: rails, permission: read_merge_request}
```

The ClickHouse policy filters every `MergeRequest` read on the paths where the caller holds `read`. The `rails` check asks GitLab, in one batch, whether the caller can read each returned merge request, and it fails closed. Policies name graph properties and relationships, never columns.

The platform package declares the permissions and scope kinds that every package uses:

```yaml
# packages/gitlab/authorization.yaml
kind: authorization
package: gitlab
permissions: [read, read_code, read_merge_request, read_vulnerability]
scopes:
  namespace: {kind: hierarchy}
  user: {kind: owner, entity: User}
entities:
  User: {scope: global, permission: read}
```

An entity without its own scope key reaches one through a relationship. A `Commit` takes its project's path:

```yaml
  Commit:
    scope: namespace
    permission: read
    enforce:
      clickhouse: {scope_key: {via: IN_PROJECT}}
    checks:
      - {provider: rails, permission: read_code, object: Project, id: project_id}
```

The storage fragment lays it out in a store, and nothing in it is inferred. It names the table and lists every stored property as a column, with its storage type and whether it is immutable. Shared fields work on every backend. A `backend` block holds what only that backend understands, and that backend validates it.

```yaml
# packages/gitlab.code_review/storage/clickhouse.yaml
kind: storage
package: gitlab.code_review
store: graph
entities:
  MergeRequest:
    table: gl_merge_request
    columns:
      id: {immutable: true, backend: {type: Int64, codec: [T64, ZSTD(1)]}}
      iid: {immutable: true, backend: {type: Int64, codec: [Delta(8), LZ4]}}
      title: {backend: {type: String, codec: [ZSTD(1)]}}
      state: {backend: {type: LowCardinality(String)}}
      author_id: {backend: {type: Nullable(Int64)}}
      traversal_path: {backend: {type: String}}
    access: {filter: [state, author_id], text: [title], order: [id]}
    backend:
      primary_key: [traversal_path, id]
relationships:
  AUTHORED:
    User -> MergeRequest: {fk: {on: target, column: author_id}}
  APPROVED:
    User -> MergeRequest: {edge_table: gl_edge}
```

`AUTHORED` is a foreign key: the `author_id` column on the `MergeRequest` row holds the author. `APPROVED` is many-to-many, so its rows live in an edge table. Every variant names one of the two, so a variant with no binding fails the load.

An edge never outlives its endpoints. When a merge request is tombstoned, the indexing engine also tombstones its `APPROVED` rows in `gl_edge`, in the same flow, whichever pipeline wrote them. No fragment declares this, because the bindings already name every edge table that holds a merge request's edges.

An edge table declares its endpoint ID columns with their types, in the store's engine block:

```yaml
engine:
  edge_tables:
    gl_edge:
      columns:
        source_id: {backend: {type: Int64}}
        target_id: {backend: {type: Int64}}
        source_kind: {backend: {type: LowCardinality(String)}}
        target_kind: {backend: {type: LowCardinality(String)}}
        relationship_kind: {backend: {type: LowCardinality(String)}}
```

A variant binds to an edge table only when both endpoint identities match its ID column types. An integration whose entities have string IDs declares its own edge table with `String` columns.

`access` says how the columns are queried, in terms every backend understands. ClickHouse turns it into its skip indexes and sort key, so an integration author never writes ClickHouse details. The `backend` block overrides that only where a platform package needs to.

The entity's home store must list every property of the graph, or exclude it by name with `exclude: [...]`. Another store lists exactly the properties it holds. The loader rejects a property that its home store neither stores nor excludes. It also rejects a column without a storage type and a binding without a table. A forgotten column fails at load, not at query time.

The source fragment says where the indexer reads it from:

```yaml
# packages/gitlab.code_review/sources/siphon.yaml
kind: source
package: gitlab.code_review
source: siphon
entities:
  MergeRequest:
    tables: [merge_requests, siphon_merge_request_metrics, siphon_merge_request_reviewers, siphon_approvals]
    order_by: [traversal_path, id]
    query: merge_request.sql.j2
    columns: {state: state_id, project_id: target_project_id}
```

A relationship that a foreign key carries needs no source entry. The indexer writes `author_id` with the merge request row, and the storage binding is enough.

A relationship can declare typed attributes in the graph when the data belongs to the edge itself, such as the access level of a membership. Queries filter and select them like properties, and storage binds each one to an edge column.

An edge table can also copy endpoint properties into its sort key, so a filter on an endpoint prunes the edge read. That is a storage choice, so the graph never sees it:

```yaml
# packages/gitlab.source_code/storage/clickhouse.yaml
relationships:
  CALLS:
    Definition -> Definition:
      edge_table: gl_code_edge
      copies: {project_id: source.project_id, branch: source.branch}
```

A filter on `a.branch` in `(a)-[:CALLS]->(b)` then also filters the edge read on its `branch` column.

Code entities have their own writer, the code indexer, declared like any other source:

```yaml
# packages/gitlab.source_code/sources/code_indexer.yaml
kind: source
package: gitlab.source_code
source: code_indexer
entities: [Branch, Directory, File, Definition, ImportedSymbol]
relationships: [CONTAINS, DEFINES, CALLS, IMPORTS]
```

Every stored entity and every edge-table variant has exactly one declared writer.

Publish the new package version to the registry. The registry diffs its storage against the active version, the indexing engine backfills the tables in scope, and the version goes live.

## Add an indexed third-party source

An integration that lands rows in the data lake ships a package with graph, authorization, storage and source fragments. Jira issues use the graph store, like GitLab entities:

```yaml
# packages/integration.jira/sources/data_lake.yaml
kind: source
package: integration.jira
source: data_lake
entities:
  JiraIssue:
    tables: [jira_issues]
    order_by: [traversal_path, id]
    query: generated
```

Every row carries the `traversal_path` of the namespace that installed the integration, so its authorization enforces a scope filter like any other entity: `enforce: {clickhouse: {scope_key: traversal_path}}`.

## Add a live third-party entity

An entity can be fetched at query time instead of indexed. Its storage is a `live` store that holds no rows.

```yaml
# packages/integration.slack/graph.yaml
kind: graph
package: integration.slack
entities:
  SlackMessage:
    identity: id
    label: text
    properties:
      id: {type: string}
      channel: {type: string}
      author: {type: string}
      text: {type: string}
      sent_at: {type: timestamp}
```

```yaml
# packages/integration.slack/authorization.yaml
kind: authorization
package: integration.slack
entities:
  SlackMessage:
    scope: namespace
    permission: read
    enforce:
      live: {scope_key: {via: [IN_WORKSPACE, INSTALLED_IN]}}
    checks:
      - {provider: slack, kind: delegated}
```

```yaml
# packages/integration.slack/storage/live.yaml
kind: storage
package: integration.slack
store: slack
engine:
  kind: live
  connection: data_extraction
  path_column: traversal_path
entities:
  SlackMessage:
    columns:
      id: {immutable: true}
      channel: {}
      author: {}
      text: {}
      sent_at: {}
    backend:
      operation: search.messages
      filters: [channel, author, sent_at]
      text: text
```

The live catalog's plan language is API calls: an operation, the filters that operation can push down, a time window, paging and a limit. A filter the operation cannot push down is a construction error.

```cypher
MATCH (m:SlackMessage)
WHERE m.channel = 'orbit-dev' AND m.text CONTAINS 'deploy'
RETURN m.author, m.text LIMIT 20
```

```rust
let live = model.physical::<Live>("data_extraction")?;
let plan = live.build(|q| {
    let messages = q.call(live.operation("slack", "SlackMessage")?)?;
    let messages = q.filter(messages, channel.eq("orbit-dev").and(text.contains("deploy")))?;
    q.select(q.limit(messages, 20)?, [author, text])
})?;
let request = live.render(&plan);
```

The Data Extraction Service runs the request with the caller's own Slack sign-in, and tokens never leave the service. Each workspace maps to the namespace that installed it. The `live` policy follows each message to the namespace that installed its workspace. The live backend then sends the request only for workspaces in namespaces where the caller holds `read`. The `slack` provider answers `Credentials`, so Slack then applies its own visibility through the caller's sign-in.

## Add a backend

A backend implements `StorageBackend`, which parses its `backend` blocks and resolves each store:

```rust
pub trait StorageBackend: Sized {
    const KIND: &'static str;
    type Options: DeserializeOwned;
    type SourceOptions: DeserializeOwned;
    type RouteOptions: DeserializeOwned;
    type Store: Debug;

    fn resolve(store: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> Self::Store;
}
```

Its catalog implements the three catalog traits. The associated types keep its vocabulary local, and `Render` returns its native request:

```rust
pub trait Catalog: Sized {
    type Operation;
    type ValueType;
    type Error;

    fn identity(&self) -> ModelId;
}

pub trait Construct: Catalog {
    type Builder<'catalog>
    where
        Self: 'catalog;

    fn builder(&self) -> Self::Builder<'_>;
}

pub trait Render: Catalog {
    type Executable;

    fn render(&self, plan: &Plan<Self>) -> Result<Self::Executable, Self::Error>;
}
```

The builder plugs into shared tracking, which proves the structural facts of every call. The backend's own rules check the rest, such as which match modes an index supports.

Orbit Next is a backend whose catalog speaks in repositories and structural matches. Its data lives on S3: the trigram index, the commit manifests and the code graph. The code indexing web servers read those objects and answer structural queries. Its storage fragment binds `File` to a store on the code indexing connection:

```yaml
# packages/gitlab.source_code/storage/orbit_code.yaml
kind: storage
package: gitlab.source_code
store: code
engine:
  kind: orbit_code
  connection: code_indexing
entities:
  File:
    backend:
      search: {content: [trigram, structural], path: [trigram]}
```

A query for every call to `compile` in a project's Rust files plans into a code plan:

```cypher
MATCH (f:File)
WHERE f.project_id = 42 AND f.content MATCHES ast('compile($$$ARGS)', 'rust')
RETURN f.path, match.line, match.ARGS LIMIT 50
```

```rust
let code = model.physical::<OrbitCode>("code_indexing")?;
let plan = code.build(|q| {
    let files = q.repositories(code.index("code", "File")?, [project])?;
    let files = q.languages(files, [Language::Rust])?;
    let files = q.prefilter(files, Trigrams::of(["compile"]))?;
    let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)"))?;
    let hits = q.top(hits, 50)?;
    q.select(hits, [hits.field("path")?, hits.field("line")?, hits.capture("ARGS")?])
})?;
let request = code.render(&plan);
```

The trigram prefilter is a planning decision: the planner takes the pattern's literal atoms and narrows the candidate files before the structural pass. The rendered request travels over gRPC to the code indexing web servers. They narrow the files with the trigram index on S3 and run the pattern over the matching objects at the branch heads. They stream back paths, lines and captures. `File` declares `orbit_code: {scope_key: {via: IN_PROJECT}}` next to its ClickHouse policy, and the Orbit Next backend renders it as a repository restriction.

## Add a tenant overlay

A tenant overlay is a package that loads only into one tenant's model. Agent memory adds a `Memory` entity linked to the merge requests and projects it is about:

```yaml
# packages/tenant.agent_memory/graph.yaml
kind: graph
package: tenant.agent_memory
entities:
  Memory:
    identity: id
    label: summary
    properties:
      id: {type: string}
      summary: {type: string, default: ""}
      traversal_path: {type: string}
relationships:
  ABOUT:
    variants:
      Memory -> MergeRequest: {cardinality: many_to_many}
      Memory -> Project: {cardinality: many_to_many}
```

```yaml
# packages/tenant.agent_memory/authorization.yaml
kind: authorization
package: tenant.agent_memory
entities:
  Memory:
    scope: namespace
    permission: read
    enforce:
      clickhouse: {scope_key: traversal_path}
relationships:
  ABOUT:
    enforce:
      clickhouse: {scope_key: traversal_path}
```

```yaml
# packages/tenant.agent_memory/storage/clickhouse.yaml
kind: storage
package: tenant.agent_memory
store: graph
entities:
  Memory:
    table: gl_memory
    columns:
      id: {immutable: true, backend: {type: String}}
      summary: {backend: {type: String, codec: [ZSTD(3)]}}
      traversal_path: {backend: {type: String}}
    backend:
      primary_key: [traversal_path, id]
relationships:
  ABOUT:
    Memory -> MergeRequest: {edge_table: gl_edge}
    Memory -> Project: {edge_table: gl_edge}
```

A user-scoped entity names its owner relationship instead of a scope key:

```yaml
# packages/tenant.todos/authorization.yaml
kind: authorization
package: tenant.todos
entities:
  Todo:
    scope: user
    permission: read
    enforce:
      clickhouse: {owner: OWNS}
relationships:
  OWNS:
    enforce:
      clickhouse: {scope_key: traversal_path}
```

The graph must give `OWNS` one owner per todo. Storage decides whether the owner check reads a column or an edge.

Its entities appear only in that tenant's catalogs, and that tenant's plans and caches carry their own model ID.

## Add an authorization provider

A provider is a Rust module that answers one or more authorization queries. Each query type has its own response:

| Query | Asks | Response |
|---|---|---|
| `Identify` | who sent this request | `Caller` |
| `Scopes` | which scope keys the caller holds a permission in | `ScopeSet`: `All`, `Keys` or `Subtrees` |
| `Check` | which of these rows the caller may use with a permission | the allowed rows |
| `Credentials` | the caller's sign-in for a delegated service | `Credential` |

```rust
pub trait Provider<Q: Query>: Send + Sync {
    async fn call(&self, query: Q) -> Result<Q::Response, AuthError>;
}

pub trait Module: Sized {
    const KIND: &'static str;
    type Options: DeserializeOwned;
    fn build(options: Self::Options) -> Result<Self, AuthError>;
}
```

The GitLab modules wrap what runs today:

```rust
impl Provider<Identify<'_>> for GitLabSession { /* verify the session token Rails issues */ }
impl Provider<Scopes<'_>> for GitLabSession { /* paths at the role the permission maps to, as Subtrees */ }
impl Provider<Check<'_>> for RailsAbilities { /* the redaction call to Rails, in batches */ }
```

The deployment config binds each provider name that packages use to a module, and names the one `Identify` provider:

```yaml
authorization:
  identify: gitlab_session
  providers:
    gitlab_session:
      module: gitlab.session
      connection: rails
      permissions: {read: reporter, read_code: reporter, read_merge_request: reporter, read_vulnerability: developer}
    rails: {module: gitlab.abilities, connection: rails, batch: 1000}
    slack: {module: slack.sign_in, connection: data_extraction}
```

GitLab's access levels appear only in the `gitlab.session` options. Another deployment binds other modules to the same contract:

```yaml
authorization:
  identify: oidc
  providers:
    oidc: {module: oidc, issuer: "https://login.example.com", subject: sub}
    spicedb: {module: spicedb, endpoint: "spicedb:50051"}
    cedar: {module: cedar, policies: "s3://example-authz/policies.cedar"}
```

`spicedb` answers `Scopes` with `LookupResources` and `Check` with `CheckBulkPermissions`. `cedar` answers `Check` with `is_authorized` per row, over the columns the check lists in `attributes`. A row comes back only if every check on its entity allows it. Every provider fails closed.

Tests use seeded fakes for each query, like the existing redaction mock.

## Add a query language

A surface parses text and builds a logical plan with the logical builder:

```rust
let statement = surface::gql::parse(text, model.logical())?;
```

Name, type and connectivity errors are construction errors, so a surface has no validation pass. It records intent, such as hop bounds or a skip, as data, and makes no planning decisions.

## Test it

Plan tests assert three stages of the same query:

| Stage | Inspects |
|---|---|
| `logical` | The logical plan that the surface built |
| `planned` | The physical plan right after the planning phase |
| `emitted` | The physical plan after every phase, just before render |

```yaml
logical:
  expect:
    - (Select (Filter mr.state = 'opened' (Follow AUTHORED u -> mr)))
physical:
  clickhouse:
    planned:
      expect:
        - (Join ON mr.author_id = u.id (Scan Table(gl_user) AS u) (_))
```

## Reference

### Load checks

The loader collects every error, names its package, and fails once. A missing declaration is always an error, never a default:

- An unknown entity, property, relationship, variant, permission, scope, provider, field or template.
- An entity or variant declared twice, or a second authorization entry.
- An entity with no `id`, or no authorization entry.
- A property that its home store neither stores nor excludes, a stored column without a storage type, or a binding without a table. Nothing is inferred.
- A declaration above the package's trust tier. Only a platform package may declare permissions, scope kinds, a trusted store, `none`, or checks by any provider. An integration package may declare only `Credentials` checks. A tenant overlay declares no checks, and its entities must be namespace- or user-scoped.
- A backend that stores a non-global entity or a relationship but has no `enforce` entry for it.
- A scope kind the backend cannot render, or a policy that names a property or relationship the graph lacks.
- A `via` chain that does not reach an entity holding the scope key, with one target per step.
- An `enforce` entry of `none` with no `checks`, or an entity whose scope answer is not exact and that has no check.
- A provider name the deployment does not bind, or one bound to a module that does not answer the query the check needs.
- A user-scoped entity whose owner relationship does not reach `User`, or allows more than one owner per row.
- A relationship variant with no storage binding in the home store of its endpoints.
- An entity without a declared `identity`, or an identity property that does not exist.
- A foreign key column, `via` step or edge-table ID column whose type differs from the identity it points to. Nothing is cast.
- A stored entity or edge-table variant with no writer, or with two.
- A foreign key binding on a many-to-many variant, on the wrong side of a one-to-many variant, or on a column its table does not hold.
- An edge table copy that names no property of that endpoint, or a graph attribute with no edge column.
- Two backends, or a trusted and an untrusted store, on one connection.

### Composition rules

- A package cannot redeclare an entity that another package owns.
- A package cannot add a second authorization entry for an entity, property or relationship.
- A package can add variants to a platform relationship.
- A package can bind more entities to a store that another package declares.
