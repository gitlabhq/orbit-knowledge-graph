# Extending the data model

This guide shows the packages and APIs of the [data model spike MR](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/3346), and it changes with the code. The MR is a reference implementation and is not meant to merge. [ADR 024](../adr/024_pluggable_data_model.md) records the design and explains how it works.

Each extension writes only a package, a frontend or a backend:

| To add | Write | Leave untouched |
|---|---|---|
| A GitLab entity | Fragments in a `gitlab.*` package | The compiler |
| An indexed third-party source | A package with a `source` fragment | The compiler and the indexing engine |
| A live third-party entity | A package with a `live` store | The frontends and the compiler phases |
| A backend | A `StorageBackend`, its plan language and its renderer | The frontends and the shared phases |
| A namespace overlay | A package that loads for one top-level namespace | Everything else |
| A query language | A frontend that builds logical plans | The planner |

## Add a GitLab entity

An entity has one fragment of each kind it needs. The graph fragment declares every property and each relationship variant that the package adds. It names no columns or tables.

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

The authorization fragment declares the entity's scope and the permission that a read needs. `enforce` holds the policy for each backend that stores the entity. Each entry in `checks` names a provider, a permission and the provider's resource type, and that provider checks the returned rows:

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
      - {provider: rails, permission: read_merge_request, resource: merge_request}
    properties:
      open: [id, iid, title, state, author_id]
      hidden: [traversal_path]
relationships:
  AUTHORED:
    scope: namespace
    permission: read
    enforce: {clickhouse: {scope_key: traversal_path, of: target}}
  APPROVED:
    scope: namespace
    permission: read
    enforce: {clickhouse: {scope_key: traversal_path, of: target}}
```

The ClickHouse policy limits every `MergeRequest` read to the paths where the caller holds `read`. The `rails` check asks GitLab in one batch whether the caller can read each returned merge request, and it fails closed. Policies name graph properties and relationships, never columns.

`properties` puts every property in one access class. A query can read and filter `open` properties freely, and it never sees `hidden` ones. `of: target` means that the caller must hold the merge request's scope, wherever the user sits.

The platform package declares the permissions and the scopes that every package uses. Each scope names its kind and the provider that answers it:

```yaml
# packages/gitlab/authorization.yaml
kind: authorization
package: gitlab
permissions: [read, read_user, read_code, read_merge_request, read_vulnerability]
scopes:
  namespace: {kind: hierarchy, provider: gitlab_session}
  user: {kind: owner, entity: User, provider: gitlab_session}
  global: {kind: global, provider: gitlab_session}
entities:
  User:
    scope: global
    permission: read
    checks:
      - {provider: rails, permission: read_user, resource: user}
    properties:
      open: [id, username, name]
```

A check can ask about another entity. A `Commit` row holds its project's path as its scope key, and its Rails check asks about the commit's project:

```yaml
  Commit:
    scope: namespace
    permission: read
    enforce:
      clickhouse: {scope_key: traversal_path}
    checks:
      - {provider: rails, permission: read_code, resource: project, object: Project, id: project_id}
    properties:
      open: [id, sha, message, project_id]
      hidden: [traversal_path]
```

A code or live store can reach its scope key through a relationship with `via` instead, as `File` does in [Add a backend](#add-a-backend).

The storage fragment lays out the entity in a store. The loader infers nothing, so it catches a forgotten column before any query runs. Shared fields work on every backend. A `backend` block holds fields that only its backend understands and validates.

```yaml
# packages/gitlab.code_review/storage/clickhouse.yaml
kind: storage
package: gitlab.code_review
store: graph
entities:
  MergeRequest:
    complete: true
    table: gl_merge_request
    columns:
      id: {immutable: true, backend: {type: Int64, codec: [T64, ZSTD(1)]}}
      iid: {immutable: true, backend: {type: Int64, codec: [Delta(8), LZ4]}}
      title: {backend: {type: String, codec: [ZSTD(1)]}}
      state: {backend: {type: LowCardinality(String)}}
      author_id: {backend: {type: Nullable(Int64)}}
      traversal_path: {backend: {type: String}}
    accepts: [id, author_id]
    access: {filter: [state, author_id], text: [title], order: [id]}
    backend:
      primary_key: [traversal_path, id]
relationships:
  AUTHORED:
    User -> MergeRequest: {fk: {on: target, column: author_id}}
  APPROVED:
    User -> MergeRequest: {edge_table: gl_edge}
```

`access` says how queries use the columns, in terms that every backend understands. ClickHouse derives its skip indexes and sort key from it, so no package writes index or sort-key details. Column storage types stay explicit. The `backend` block overrides the derived indexes only where a platform package needs it to.

`accepts` names the columns that the store takes key sets on, when another stage hands it keys. Every store declares it. A store returns every column it lists, so listing `id` is what makes it return the identity.

In each deployment's model, every stored entity marks exactly one store `complete: true`, even when it has only one store. That store must list every graph property or exclude it by name with `exclude: [...]`. A query reads the entity on that store's connection. If the deployment does not serve that connection, the query fails with an error that names the entity and the connection. Only a structural match reads elsewhere, on the code connection that searches its property. Every other store is partial and lists exactly the properties it holds. A live or code store holds no table. It lists the properties it serves, and its backend checks its own fields.

`AUTHORED` is a foreign key: the `author_id` column on the `MergeRequest` row holds the author. `APPROVED` is many-to-many, so its rows live in an edge table. Every variant binds to one of the two, or the load fails.

An edge never outlives its endpoints. When the indexing engine tombstones a merge request, it also tombstones its `APPROVED` rows in `gl_edge` in the same flow, whichever pipeline wrote them. No fragment declares this, because the bindings already name every edge table that holds a merge request's edges.

An edge table declares its endpoint ID columns and their types in the store's engine block:

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

The source fragment says where the indexer reads the entity from:

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
relationships:
  APPROVED:
    User -> MergeRequest:
      tables: [siphon_approvals]
      columns: {source: user_id, target: merge_request_id}
```

A foreign key relationship such as `AUTHORED` needs only its storage binding, because the indexer writes `author_id` with the merge request row. An edge-table variant such as `APPROVED` needs its own source entry, because its rows come from their own table.

A relationship can declare a typed attribute in the graph for data that belongs to the edge, such as the access level of a membership. Queries filter and select attributes like properties, and storage binds each one to an edge column.

An edge table can also copy endpoint properties into its sort key, so a filter on an endpoint prunes the edge read. The copy is a storage choice, and the graph never sees it:

```yaml
# packages/gitlab.source_code/storage/clickhouse.yaml
relationships:
  CALLS:
    Definition -> Definition:
      edge_table: gl_code_edge
      copies: {project_id: source.project_id, branch: source.branch}
```

A filter on `a.branch` in `(a)-[:CALLS]->(b)` then also filters the edge read on its `branch` column.

In every store that Orbit writes, each entity and edge-table variant has exactly one declared writer. The code indexer writes the code entities in the graph store, and its source fragment looks like any other:

```yaml
# packages/gitlab.source_code/sources/code_indexer.yaml
kind: source
package: gitlab.source_code
source: code_indexer
entities: [Branch, Directory, File, Definition, ImportedSymbol]
relationships: [CONTAINS, DEFINES, CALLS, IMPORTS]
```

Publish the new package version to the registry. The registry diffs its storage against the active version. The indexing engine then backfills the tables in scope, and the version goes live.

## Add an indexed third-party source

An integration that lands rows in the data lake ships a package with graph, authorization, storage and source fragments. The Data Extraction Service writes the Jira rows to S3, and a DuckDB store reads them there:

```yaml
# packages/integration.jira/sources/des.yaml
kind: source
package: integration.jira
source: des
declare:
  connection: jira_lake
  version: _siphon_replicated_at
  watermark: _siphon_watermark
  deleted: _siphon_deleted
entities:
  JiraIssue:
    tables: [jira_issue]
    order_by: [traversal_path, id]
    query: generated
```

`declare` names the connection that holds the source's tables and its lifecycle columns: the row version, the watermark and the deleted flag.

```yaml
# packages/integration.jira/storage/duckdb.yaml
kind: storage
package: integration.jira
store: jira
engine:
  kind: duckdb
  connection: jira_lake
entities:
  JiraIssue:
    complete: true
    table: jira_issue
    columns:
      id: {immutable: true, backend: {type: VARCHAR}}
      key: {backend: {type: VARCHAR}}
      summary: {backend: {type: VARCHAR}}
      merge_request_id: {backend: {type: BIGINT}}
      traversal_path: {backend: {type: VARCHAR}}
    accepts: [id, merge_request_id]
    access: {filter: [id, key, merge_request_id], text: [summary], order: [id]}
```

The ETL of the Data Extraction Service writes the `traversal_path` of the namespace that installed the integration on every row. That path is the scope key, so the authorization enforces a scope filter like any other entity: `enforce: {duckdb: {scope_key: traversal_path}}`.
`JiraIssue` declares no check. Its node comes back only when its stage's security check proved the scope filter on the read, and otherwise the query engine redacts it.

The connection's `location` in deployment config sets the DuckDB mode, and the package is the same in both modes:

```yaml
connections:
  duckdb:
    jira_lake:
      location: {mode: parquet, bucket: lake, prefix: jira/}
      # or: {mode: ducklake, catalog: /var/lib/orbit/jira.ducklake, bucket: lake, prefix: jira/data/}
```

`parquet` reads each table from the Parquet files under its prefix. `ducklake` reads each table through the DuckLake catalog, and its data path is the bucket and prefix.

## Add a live third-party entity

An entity can be fetched at query time instead of indexed, so its `live` store holds no rows.

```yaml
# packages/integration.slack/graph.yaml
kind: graph
package: integration.slack
entities:
  SlackMessage:
    identity: id
    label: text
    default_columns: [id, channel, author, text, sent_at]
    properties:
      id: {type: string}
      workspace: {type: string}
      channel: {type: string}
      author: {type: string}
      text: {type: string}
      sent_at: {type: timestamp}
  SlackWorkspace:
    identity: id
    label: name
    default_columns: [id, name]
    properties:
      id: {type: string}
      name: {type: string}
      namespace_id: {type: int64}
      traversal_path: {type: string}
relationships:
  IN_WORKSPACE:
    variants: {SlackMessage -> SlackWorkspace: {cardinality: many_to_one}}
  INSTALLED_IN:
    variants: {SlackWorkspace -> Group: {cardinality: many_to_one}}
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
    properties:
      open: [id, channel, author, text, sent_at]
      exact: [workspace]
  SlackWorkspace:
    scope: namespace
    permission: read
    enforce:
      clickhouse: {scope_key: traversal_path}
    properties:
      open: [id, name]
      hidden: [namespace_id, traversal_path]
relationships:
  IN_WORKSPACE:
    scope: namespace
    permission: read
    enforce: {}
  INSTALLED_IN:
    scope: namespace
    permission: read
    enforce: {clickhouse: {scope_key: traversal_path, of: holder}}
```

```yaml
# packages/integration.slack/storage/live.yaml
kind: storage
package: integration.slack
store: slack
engine:
  kind: live
  connection: data_extraction
entities:
  SlackMessage:
    complete: true
    columns:
      id: {immutable: true}
      workspace: {}
      channel: {}
      author: {}
      text: {}
      sent_at: {}
    accepts: [workspace]
    access: {filter: [workspace, channel, author, sent_at], text: [text]}
    backend:
      operation: search.messages
relationships:
  IN_WORKSPACE:
    SlackMessage -> SlackWorkspace: {fk: {on: source, column: workspace}}
```

A live store keeps no edge rows, so `IN_WORKSPACE` declares no edge policy. The message's own policy decides whether the caller sees it. The workspaces live in the graph store, so `INSTALLED_IN` is a foreign key on a `SlackWorkspace` row:

```yaml
# packages/integration.slack/storage/clickhouse.yaml
kind: storage
package: integration.slack
store: graph
entities:
  SlackWorkspace:
    complete: true
    table: slack_workspace
    columns:
      id: {immutable: true, backend: {type: String}}
      name: {backend: {type: String}}
      namespace_id: {backend: {type: Int64}}
      traversal_path: {backend: {type: String}}
    accepts: [id]
    access: {order: [id]}
relationships:
  INSTALLED_IN:
    SlackWorkspace -> Group: {fk: {on: source, column: namespace_id}}
```

The integration's data lake source writes the workspaces from its installation records. `SlackMessage` lives in a live store, which the live source serves, so it declares no writer:

```yaml
# packages/integration.slack/sources/data_lake.yaml
kind: source
package: integration.slack
source: data_lake
entities:
  SlackWorkspace:
    tables: [slack_installations]
    order_by: [id]
    query: generated
    columns: {id: workspace_id, name: workspace_name, namespace_id: namespace_id, traversal_path: traversal_path}
```

The live catalog's plan language is API calls: an operation, the filters that operation can push down, a time window, paging and a limit. The `access` hints say which filters the operation pushes down, and any other filter is a construction error.

```cypher
MATCH (m:SlackMessage)
WHERE m.channel = 'orbit-dev' AND m.text CONTAINS 'deploy'
RETURN m.author, m.text LIMIT 20
```

```rust
let live = model.physical::<Live>(data_extraction)?;
let plan = live.build(|q| {
    let messages = q.call(live.operation(slack, message)?)?;
    let messages = q.filter(messages, channel.eq("orbit-dev").and(text.contains("deploy")))?;
    q.select(q.limit(messages, 20)?, [author, text])
})?;
let request = live.render(&plan);
```

The variables `data_extraction`, `slack`, `message`, `channel`, `text` and `author` are handles, which the frontend resolved from the query's names.

The `live` policy follows each message to the namespace that installed its workspace. A scope lookup on the graph connection resolves that chain first, because the complete store of `SlackWorkspace` is there. The live backend sends the request only for workspaces in namespaces where the caller holds `read`. The Data Extraction Service runs the request with the caller's own Slack sign-in, and tokens never leave the service. The `slack` provider answers `Credentials`, so Slack then applies its own visibility through that sign-in.

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

The builder uses shared tracking, which proves the structural facts of every call. The backend's own rules check the rest, such as which match modes an index supports.

Orbit Next is a backend whose catalog describes repositories and structural matches. Its data lives on S3: the trigram index, the commit manifests and the code graph. A storage fragment binds `File` to an Orbit Next store, and its `backend` block names the match modes that only Orbit Next understands:

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
    columns:
      project_id: {}
      branch: {}
      path: {}
      content: {}
    accepts: [project_id, branch]
    access: {text: [path, content]}
    backend:
      repository: {project: project_id, branch: branch}
      search: {content: [trigram, structural], path: [trigram]}
```

`access` marks `path` and `content` as searchable. `repository` names the columns that hold each row's project and branch.

An identity can be composite. A `Head` is the head of a branch, and its identity is its project and branch:

```yaml
# packages/gitlab.source_code/graph.yaml
entities:
  Head:
    identity: [project_id, branch]
    label: branch
    default_columns: [project_id, branch]
    properties:
      project_id: {type: int64}
      branch: {type: string}
relationships:
  HEAD_FILE:
    variants: {Head -> File: {cardinality: one_to_many}}
```

```yaml
# packages/gitlab.source_code/storage/orbit_code.yaml
entities:
  Head:
    complete: true
    columns:
      project_id: {immutable: true}
      branch: {immutable: true}
    accepts: [project_id, branch]
    backend:
      repository: {project: project_id, branch: branch}
relationships:
  HEAD_FILE:
    Head -> File: {fk: {on: target, column: [project_id, branch]}}
```

A key set on `Head` holds `(project_id, branch)` pairs, and the `HEAD_FILE` foreign key joins on both columns.

A store's connection serves only in the deployment shapes that list it. `config/deployments.yaml` names each shape and the connections it serves:

```yaml
# config/deployments.yaml
server:
  connections: [main]
local:
  connections: [local]
demo:
  connections: [main, code_indexing, data_extraction, jira_lake]
```

The runtime config selects one shape with `schema.deployment`. The loader keeps only the storage fragments on that shape's connections, and then it runs every check. The model ID follows that deployment's model.

A query for every call to `compile` in a project's Rust files plans into a code plan:

```cypher
MATCH (f:File)
WHERE f.project_id = 42 AND f.content MATCHES ast('compile($$$ARGS)', 'rust')
RETURN f.path, match.line, match.ARGS LIMIT 50
```

```rust
let code = model.physical::<OrbitCode>(code_indexing)?;
let plan = code.build(|q| {
    let files = q.repositories(code.index(code_store, file)?, [project])?;
    let files = q.languages(files, [Language::Rust])?;
    let files = q.prefilter(files, Trigrams::of(["compile"]))?;
    let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)"))?;
    let hits = q.top(hits, 50)?;
    q.select(hits, [hits.field(path)?, hits.field("line")?, hits.capture("ARGS")?])
})?;
let request = code.render(&plan);
```

The planner adds the trigram prefilter: it takes the pattern's literal atoms and narrows the candidate files before the structural pass. The request goes over gRPC to the code indexing web servers. They narrow the files with the trigram index on S3 and run the pattern over the matching objects at the branch heads. They stream back paths, lines and captures. As in the live example, `code_indexing`, `code_store`, `file` and `path` are handles that the frontend resolved. `File` declares `orbit_code: {scope_key: {via: IN_PROJECT}}` next to its ClickHouse policy, and the Orbit Next backend renders it as a repository restriction. `of: holder` on `IN_PROJECT` makes the project that holds the file the scope entity.

## Add a namespace overlay

A namespace overlay is a package that loads only into one top-level namespace's model. Its entities appear only in that namespace's catalogs, and that namespace's plans and caches carry their own model ID. The agent memory overlay adds a `Memory` entity:

```yaml
# packages/overlay.agent_memory/graph.yaml
kind: graph
package: overlay.agent_memory
entities:
  Memory:
    identity: id
    label: summary
    default_columns: [id, summary]
    properties:
      id: {type: string}
      summary: {type: string, default: ""}
      traversal_path: {type: string}
relationships:
  ABOUT:
    variants:
      Memory -> MergeRequest: {cardinality: many_to_many}
      Memory -> Project: {cardinality: many_to_many}
    attributes: {traversal_path: string}
```

```yaml
# packages/overlay.agent_memory/authorization.yaml
kind: authorization
package: overlay.agent_memory
entities:
  Memory:
    scope: namespace
    permission: read
    enforce:
      clickhouse: {scope_key: traversal_path}
    properties:
      open: [id, summary]
      hidden: [traversal_path]
relationships:
  ABOUT:
    scope: namespace
    permission: read
    enforce:
      clickhouse: {scope_key: traversal_path, of: own}
    attributes:
      hidden: [traversal_path]
```

```yaml
# packages/overlay.agent_memory/storage/clickhouse.yaml
kind: storage
package: overlay.agent_memory
store: graph
engine:
  edge_tables:
    gl_memory_edge:
      columns:
        source_id: {backend: {type: String}}
        target_id: {backend: {type: Int64}}
        target_kind: {backend: {type: LowCardinality(String)}}
        relationship_kind: {backend: {type: LowCardinality(String)}}
        traversal_path: {backend: {type: String}}
entities:
  Memory:
    complete: true
    table: gl_memory
    columns:
      id: {immutable: true, backend: {type: String}}
      summary: {backend: {type: String, codec: [ZSTD(3)]}}
      traversal_path: {backend: {type: String}}
    accepts: [id]
    access: {text: [summary], order: [traversal_path, id]}
relationships:
  ABOUT:
    Memory -> MergeRequest: {edge_table: gl_memory_edge}
    Memory -> Project: {edge_table: gl_memory_edge}
```

`Memory` has string IDs and points at entities with integer IDs, so its edges live in their own table. The agent memory API writes both:

```yaml
# packages/overlay.agent_memory/sources/agent_api.yaml
kind: source
package: overlay.agent_memory
source: agent_api
entities: [Memory]
relationships: [ABOUT]
```

A user-scoped entity names its owner relationship instead of a scope key:

```yaml
# packages/overlay.todos/authorization.yaml
kind: authorization
package: overlay.todos
entities:
  Todo:
    scope: user
    permission: read
    enforce:
      clickhouse: {owner: OWNS}
    properties:
      open: [id, title, state]
      hidden: [user_id]
relationships:
  OWNS:
    scope: user
    permission: read
    enforce:
      clickhouse: {scope_key: user_id, of: target}
```

The graph must give `OWNS` one owner per todo. Storage binds `OWNS` as a foreign key on the todo row, and the owner check reads that column. `OWNS` itself takes `scope_key: user_id` and `of: target`, so a caller sees the edge only when they may see the todo.

## Add an authorization provider

A provider is a Rust module that answers one or more authorization queries. Each query type has its own response:

| Query | Asks | Response |
|---|---|---|
| `Identify` | who sent this request | `Caller` |
| `Scopes` | which scope keys the caller holds a permission in | `ScopeSet`: `All`, `Keys` or `Subtrees` |
| `Check` | which of these rows the caller may use with a permission | the allowed keys of each request, in one batch |
| `Credentials` | the caller's sign-in for a delegated service | `Credential` |

```rust
pub trait Provider<Q: Query>: Send + Sync {
    async fn call(&self, query: Q) -> Result<Q::Response, AuthError>;
}

pub trait Module: Sized {
    const KIND: &'static str;
    type Options: DeserializeOwned;
    fn build(options: Self::Options) -> Result<Self, AuthError>;
    fn answers(self: Arc<Self>, needs: &[Need]) -> Result<Answers, AuthError>;
}
```

`answers` receives the scopes and permissions that the model needs. It returns the queries that the module answers, and whether its `Scopes` answers are exact.

The GitLab modules wrap the Rails session and redaction calls that already exist:

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
      permissions: {read: reporter, read_code: reporter, read_merge_request: reporter, read_vulnerability: security_manager}
    rails: {module: gitlab.abilities, connection: rails, batch: 1000}
    slack: {module: slack.sign_in, connection: data_extraction}
```

Binding the deployment fails for an entity with no row check whose scope provider does not answer exactly. The error names the entity, its scope kind, its scope and the provider.

GitLab access levels appear only in the `gitlab.session` options. A deployment that loads its own packages instead of the GitLab packages binds other modules to the same contract:

```yaml
authorization:
  identify: oidc
  providers:
    oidc: {module: oidc, issuer: "https://login.example.com", subject: sub}
    spicedb: {module: spicedb, endpoint: "spicedb:50051"}
    cedar: {module: cedar, policies: "s3://example-authz/policies.cedar"}
```

`spicedb` answers `Scopes` with `LookupResources` and `Check` with `CheckBulkPermissions`. `cedar` answers `Check` with `is_authorized` for each row, over the graph properties that the check lists in `attributes`. Storage maps those properties to columns, as it does for every policy. A row comes back only if every check on its entity allows it. Every provider fails closed.

Tests use seeded fakes for each query, like the existing redaction mock.

## Add a query language

A frontend parses text and builds a logical plan with the logical builder:

```rust
let statement = frontend::gql::parse(text, model.logical())?;
```

Name, type and connectivity errors are construction errors, so a frontend has no validation pass. It records intent, such as hop bounds or a skip, as data and makes no planning decisions.

## Test it

Plan tests assert three stages of the same query:

| Stage | Inspects |
|---|---|
| `logical` | The logical plan that the frontend built |
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

The loader collects every error with its package and fails once. A missing declaration is always a load error, never a default:

- An unknown entity, property, relationship, variant, permission, scope, provider, field or template.
- An entity or variant declared twice, or a second authorization entry.
- A field that its fragment's schema does not know, or a missing required backend field. A live store requires `operation`, and a code store requires `repository`.
- A reference that resolves to no declared fact, or to a fact of the wrong type.
- A declared fact that no backend reads.
- An entity with no `id`, `identity`, `default_columns` or authorization entry, or with an `identity` property that does not exist.
- A property that its complete store neither stores nor excludes. In a relational store, a column without a storage type or an entity without a table.
- A property or relationship attribute with no access class, or with two.
- A scope without a `provider`, or a row check without a `resource`.
- A global entity with an `enforce` entry, or with no checks and no `checks: open`.
- A relationship without a scope or a permission, or a relational edge policy without a `scope_key` and an `of:`.
- A declaration above the package's trust tier. Only a platform package may declare permissions, scope kinds, a trusted store, `none`, `checks: open` or checks by any provider. An integration package may declare only `Credentials` checks. A namespace overlay declares no checks, and its entities must be namespace- or user-scoped.
- A backend that stores a non-global entity but has no `enforce` entry for it, or a relational store that binds a relationship without one.
- A scope kind the backend cannot render, or a policy that names a property or relationship the graph lacks.
- A `via` chain that does not reach an entity that holds the scope key, or a step with more than one target.
- An `enforce` entry of `none` with no `checks`.
- A provider name the deployment does not bind, or one bound to a module that does not answer the query the check needs.
- A user-scoped entity whose owner relationship does not reach `User` or allows more than one owner per row. The owner relationship must be a foreign key on the entity's row.
- An unknown deployment shape.
- A stored entity with no `complete: true` store in the deployment's model, or with two.
- A store entry without `accepts:`, or an `accepts:` column that the store does not list.
- A relationship variant with no binding, or with two. A foreign key binds in the store whose table holds the column, and an edge table binds in the store that declares it.
- An edge table whose two endpoints are not both on its connection.
- A foreign key column, `via` step or edge-table ID column whose type differs from the identity it points to. The loader casts nothing.
- In a store that Orbit writes, an entity or edge-table variant with no writer, or with two.
- A writer that targets a code or live store.
- A foreign key binding on a many-to-many variant, on the wrong side of a one-to-many variant, or on a column its table does not hold.
- An edge table copy that names no property of that endpoint, or a graph attribute with no edge column.
- Two backends, or a trusted and an untrusted store, on one connection.

### Composition rules

- A package cannot redeclare an entity that another package owns, or add a second authorization entry for an entity, property or relationship.
- A package can add variants to a platform relationship, and bind more entities to a store that another package declares.
