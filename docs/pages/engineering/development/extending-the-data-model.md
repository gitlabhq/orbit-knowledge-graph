# Extending the data model

This guide shows the packages and APIs of the [data model spike MR](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/3346), and it changes with the code. The MR is a reference implementation and is not meant to merge. [ADR 024](../adr/024_pluggable_data_model.md) records the design and explains how it works.

Each extension writes only a package, a frontend or a backend:

| To add | Write | Leave untouched |
|---|---|---|
| A GitLab entity | Fragments in a `gitlab.*` package | The compiler |
| An integration's lake store | A package with a `duckdb` store and a `source` fragment | The compiler and the indexing engine |
| An integration's live store | A `live` store next to the lake store | The frontends and the compiler phases |
| A backend | A `StorageBackend`, its plan language and its renderer | The frontends and the shared phases |
| A namespace overlay | A package that loads for one top-level namespace | Everything else |
| A query language | A frontend that builds logical plans | The planner |

## Add a GitLab entity

An entity has one fragment of each kind it needs. The graph fragment declares every property and each relationship variant that the package adds. It names no columns or tables.

Relationship names are global across all loaded packages. A graph fragment that names an existing relationship adds variants to it, and that relationship keeps its one authorization entry. An integration therefore gives its relationships their own names, such as `MENTIONS_ISSUE`, which keeps them apart from platform names.

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

A store whose backend does not join reaches its scope key through a relationship with `via` instead. `File` in Orbit Next is an example, in [Add a backend](#add-a-backend).

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

`access` says how queries use the columns, in terms that every backend that joins understands. A store whose backend does not join declares its push-downs and search modes in its `backend:` block instead. For example, ClickHouse derives its skip indexes and sort key from it, so no package writes index or sort-key details. Column storage types stay explicit. The `backend` block overrides the derived indexes only where a platform package needs it to.

`accepts` names the columns that the store takes key sets on, when another stage hands it keys. Every store declares it, whatever its backend's capabilities. A store returns every column it lists, so listing `id` is what makes it return the identity.

In each deployment's model, every stored entity marks exactly one store `complete: true`, even when it has only one store. That store must list every graph property or exclude it by name with `exclude: [...]`. Every other store is partial and lists exactly the properties it holds. A partial store holds a subset of the columns, or it offers an operation that the complete store does not. `File`'s Orbit Next store is partial: it holds a few `File` columns and offers the structural search that the complete ClickHouse store lacks.
A store whose backend has no writer capability holds no table, for example a live or code store. It lists the properties it serves, and its backend checks its own fields.

A query's needs from a node are the properties it reads, its filter operators and its search modes. Placement reads the node from its complete store when that store serves every need. Otherwise it reads from the one partial store that serves every need. Otherwise it splits the node across the fewest stores that together serve every need, and their stages cross on the entity's identity. [Add an integration's live store](#add-an-integrations-live-store) shows a split node. If no combination of stores serves every need, the query fails, and the error names the unmet need. If the deployment does not serve the chosen store's connection, the query fails with an error that names the entity and the connection.

`AUTHORED` is a foreign key: the `author_id` column on the `MergeRequest` row holds the author. `APPROVED` is many-to-many, so its rows live in an edge table. Every variant binds to one of the two, or the load fails.

An edge never outlives its endpoints. When the indexing engine tombstones a merge request, it also tombstones its `APPROVED` rows in `gl_edge` in the same flow, whichever pipeline wrote them. No fragment declares this, because the bindings already name every edge table that holds a merge request's edges.

The fragment that declares a store sets its `engine:` block once. Its `backend:` block maps the edge fields in `edge_columns:` and declares each edge table with its typed columns:

```yaml
# packages/gitlab/storage/clickhouse.yaml
engine:
  kind: clickhouse
  connection: main
  path_column: traversal_path
  backend:
    edge_columns:
      source_id: source_id
      source_kind: source_kind
      target_id: target_id
      target_kind: target_kind
      relationship_kind: relationship_kind
    edge_tables:
      gl_edge:
        columns:
          source_id: {type: Int64}
          target_id: {type: Int64}
          source_kind: {type: LowCardinality(String)}
          target_kind: {type: LowCardinality(String)}
          relationship_kind: {type: LowCardinality(String)}
```

`edge_columns:` maps each of the five edge fields to its column, once for every edge table in the store. Every field is required, so no column name is a convention. A table whose columns differ belongs in a separate store.
An edge row stores each endpoint's kind as the entity's graph name, such as `MergeRequest`. The storage codec decodes it into a handle, and it fails on an unknown kind.

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
    reindex: [siphon_approvals]
    pipelines:
      APPROVED_siphon_approvals_MergeRequest:
        tables: [siphon_approvals]
        order_by: [traversal_path, merge_request_id, id]
        query: generated
        from: {field: user_id, kind: User}
        to: {field: merge_request_id, kind: MergeRequest}
```

A foreign key relationship such as `AUTHORED` needs only its storage binding, because the indexer writes `author_id` with the merge request row. An edge-table variant such as `APPROVED` needs its own pipeline, because its rows come from their own table. `from:` and `to:` name the field and the entity kind of each endpoint.

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

In a store whose backend has the writer capability, each entity and edge-table variant has exactly one declared writer. The code indexer writes the code entities in the graph store, and its source fragment looks like any other:

```yaml
# packages/gitlab.source_code/sources/code_indexer.yaml
kind: source
package: gitlab.source_code
source: code_indexer
entities: [Branch, Directory, File, Definition, ImportedSymbol]
relationships:
  CALLS: [Definition -> Definition, File -> Definition]
  CONTAINS: [Branch -> Directory, Branch -> File, Directory -> File]
  DEFINES: [File -> Definition]
  IMPORTS: [File -> ImportedSymbol]
```

Publish the new package version to the registry. The registry diffs its storage against the active version. The indexing engine then backfills the tables in scope, and the version goes live.

## Add an integration's lake store

An integration combines a lake store and a live store for the same entities. The lake store is the complete store: it holds the full history and every column.
The live store is a partial store that calls the service's API at query time. Placement reads from the lake unless a node needs what the lake does not declare. That is an operation such as full-text search, or a column that the ETL does not extract.

An integration ships a package with graph, authorization, storage and source fragments. The Data Extraction Service writes the Jira rows to S3, and a DuckDB store reads them there:

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
    exclude: [description]
    accepts: [id, merge_request_id]
    access: {filter: [id, key, merge_request_id], text: [summary], order: [id]}
```

The ETL of the Data Extraction Service writes the `traversal_path` of the namespace that installed the integration on every row. That path is the scope key, so the authorization enforces a scope filter like any other entity: `enforce: {duckdb: {scope_key: traversal_path}}`.
`JiraIssue` declares no row check. Its node comes back only when its stage's security check proved the scope filter on the read, and otherwise the query engine redacts it.

The connection's `location` in deployment config sets the DuckDB mode, and the package is the same in both modes:

```yaml
connections:
  duckdb:
    jira_lake:
      location: {mode: parquet, bucket: lake, prefix: jira/}
      # or: {mode: ducklake, catalog: /var/lib/orbit/jira.ducklake, bucket: lake, prefix: jira/data/}
```

`parquet` reads each table from the Parquet files under its prefix. `ducklake` reads each table through the DuckLake catalog, and its data path is the bucket and prefix.

## Add an integration's live store

A live store is the integration's partial store, on a backend that delegates and neither joins nor has writers. It holds no rows and declares no writer. Its `backend:` block names the API `operation`, the `filters` it pushes down and its full-text field, `text`.
Because the backend delegates, a `Credentials` check governs its calls, and the service runs each call with the caller's own sign-in. Lake reads of the same entity rely on their scope filter.

The Jira ETL does not extract `description`, so the lake store excludes it. Jira's live store adds it, with full-text search through the API:

```yaml
# packages/integration.jira/storage/live.yaml
kind: storage
package: integration.jira
store: jira_live
engine:
  kind: live
  connection: data_extraction
entities:
  JiraIssue:
    accepts: [id, key]
    columns:
      id: {}
      key: {}
      summary: {}
      description: {}
    backend:
      operation: search.issues
      filters: [id, key]
      text: description
```

Its authorization adds a `live` policy and the check: `live: {scope_key: {via: IN_JIRA_PROJECT}}` next to the `duckdb` policy, and `checks: [{provider: jira, kind: delegated}]`.

Slack keeps its history in a lake store and adds a live store for search. `MENTIONS_ISSUE` links a message to the Jira issue it names, so `integration.slack` requires `integration.jira`. `DISCUSSES` links a message to a merge request, so it also requires `gitlab.code_review`:

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
      issue_id: {type: string?}
      merge_request_id: {type: int64?}
      traversal_path: {type: string}
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
  MENTIONS_ISSUE:
    variants: {SlackMessage -> JiraIssue: {cardinality: many_to_one}}
  DISCUSSES:
    variants: {SlackMessage -> MergeRequest: {cardinality: many_to_one}}
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
      duckdb: {scope_key: traversal_path}
      live: {scope_key: {via: [IN_WORKSPACE, INSTALLED_IN]}}
    checks:
      - {provider: slack, kind: delegated}
    properties:
      open: [id, channel, author, text, sent_at, issue_id, merge_request_id]
      exact: [workspace]
      hidden: [traversal_path]
  SlackWorkspace:
    scope: namespace
    permission: read
    enforce:
      duckdb: {scope_key: traversal_path}
    properties:
      open: [id, name]
      hidden: [namespace_id, traversal_path]
relationships:
  IN_WORKSPACE:
    scope: namespace
    permission: read
    enforce: {duckdb: {scope_key: traversal_path, of: both}}
  INSTALLED_IN:
    scope: namespace
    permission: read
    enforce: {duckdb: {scope_key: traversal_path, of: holder}}
  MENTIONS_ISSUE:
    scope: namespace
    permission: read
    enforce: {duckdb: {scope_key: traversal_path, of: source}}
  DISCUSSES:
    scope: namespace
    permission: read
    enforce: {duckdb: {scope_key: traversal_path, of: source}}
```

The Slack ETL does not extract message text, so the lake store excludes `text`. The lake store is complete for both entities and binds every relationship:

```yaml
# packages/integration.slack/storage/duckdb.yaml
kind: storage
package: integration.slack
store: slack
engine:
  kind: duckdb
  connection: slack_lake
entities:
  SlackMessage:
    complete: true
    table: slack_message
    columns:
      id: {immutable: true, backend: {type: VARCHAR}}
      workspace: {backend: {type: VARCHAR}}
      channel: {backend: {type: VARCHAR}}
      author: {backend: {type: VARCHAR}}
      sent_at: {immutable: true, backend: {type: TIMESTAMP}}
      issue_id: {backend: {type: VARCHAR}}
      merge_request_id: {backend: {type: BIGINT}}
      traversal_path: {backend: {type: VARCHAR}}
    exclude: [text]
    accepts: [id, workspace, issue_id]
    access: {filter: [workspace, channel, author, sent_at], order: [id]}
  SlackWorkspace:
    complete: true
    table: slack_workspace
    columns:
      id: {immutable: true, backend: {type: VARCHAR}}
      name: {backend: {type: VARCHAR}}
      namespace_id: {backend: {type: BIGINT}}
      traversal_path: {backend: {type: VARCHAR}}
    accepts: [id]
    access: {order: [id]}
relationships:
  IN_WORKSPACE:
    SlackMessage -> SlackWorkspace: {fk: {on: source, column: workspace}}
  INSTALLED_IN:
    SlackWorkspace -> Group: {fk: {on: source, column: namespace_id}}
  MENTIONS_ISSUE:
    SlackMessage -> JiraIssue: {fk: {on: source, column: issue_id}}
  DISCUSSES:
    SlackMessage -> MergeRequest: {fk: {on: source, column: merge_request_id}}
```

The live store is partial. It lists the columns the API returns, `text` among them, and declares the full-text search:

```yaml
# packages/integration.slack/storage/live.yaml
kind: storage
package: integration.slack
store: slack_live
engine:
  kind: live
  connection: data_extraction
entities:
  SlackMessage:
    accepts: [workspace, issue_id]
    columns:
      id: {}
      workspace: {}
      channel: {}
      author: {}
      text: {}
      sent_at: {}
      issue_id: {}
    backend:
      operation: search.messages
      filters: [workspace, channel, author, sent_at, issue_id]
      text: text
```

The Data Extraction Service's `des` source writes the lake store. The live backend has no writer capability, so the live store declares no writer:

```yaml
# packages/integration.slack/sources/des.yaml
kind: source
package: integration.slack
source: des
declare:
  connection: slack_lake
  version: _siphon_replicated_at
  watermark: _siphon_watermark
  deleted: _siphon_deleted
entities:
  SlackMessage: {tables: [slack_message], order_by: [traversal_path, id], query: generated}
  SlackWorkspace: {tables: [slack_workspace], order_by: [traversal_path, id], query: generated}
```

The live catalog's plan language is API calls: an operation, the filters that operation can push down, a time window, paging and a limit. The store's `filters` say which filters the operation pushes down, and any other filter is a construction error.

```cypher
MATCH (m:SlackMessage)
WHERE m.channel = 'orbit-dev' AND m.text CONTAINS 'deploy'
RETURN m.author, m.text LIMIT 20
```

```rust
let Runs::On { connection, stores } = &stage.runs else { unreachable!() };
let store = stores.iter().find(|(entity, _)| *entity == message).map(|(_, store)| *store);
let live = model.physical::<Live>(*connection)?;
let plan = live.build(|q| {
    let messages = q.call(live.operation(store.expect("placed"), message)?)?;
    let found = messages.field(channel)?.eq("orbit-dev");
    let messages = q.filter(messages, found.and(messages.field(text)?.contains("deploy")))?;
    let fields = [messages.field(author)?, messages.field(text)?];
    q.select(q.limit(messages, 20)?, fields)
})?;
let request = live.render(&plan)?;
```

Placement put `message` on the live store, because the lake store does not hold `text`. `get_store(message, &needs)` returned that store, and the stage carries it in `Runs::On` with the store's `ConnectionId`.
The frontend resolves only the query's names, once, at parse. Everything else comes from those handles through the model, and tests resolve names with the explicit `Names` resolver, `model.names()`.

The `live` policy follows each message to the namespace that installed its workspace. A scope lookup on the Slack lake connection resolves that chain first, because the complete store of `SlackWorkspace` is there. The live backend sends the request only for workspaces in namespaces where the caller holds `read`. The Data Extraction Service runs the request with the caller's own Slack sign-in, and tokens never leave the service. The `slack` provider answers `Credentials`, so Slack then applies its own visibility through that sign-in.

One query can read the lake for one node and live for another:

```cypher
MATCH (i:JiraIssue)<-[:MENTIONS_ISSUE]-(m:SlackMessage)
WHERE i.key = 'ORBIT-42' AND m.text CONTAINS 'rollback'
RETURN i.summary, m.channel, m.text LIMIT 20
```

`JiraIssue` reads from the Jira lake store, which serves the filter on `key` and the `summary` column. `SlackMessage` reads from the Slack live store, because only it holds `text`.
The lake stage hands the issue IDs to the live stage, which takes them on `issue_id` from its `accepts:`. The combine stage combines both stages' rows on `MENTIONS_ISSUE`.

One node can also read from two stores:

```cypher
MATCH (m:SlackMessage)-[:DISCUSSES]->(mr:MergeRequest)
WHERE m.text CONTAINS 'rollback'
RETURN mr.iid, m.author
```

No single store serves every need of `m`. Only the live store holds `text`, and only the lake store holds `merge_request_id` for `DISCUSSES`. So placement splits `m` across both.
The live stage runs the full-text search and returns message IDs. The lake stage reads `merge_request_id` and `author` for those IDs, which it takes on `id` from its `accepts:`.
The combine stage joins the two parts on the identity of `m` and follows `DISCUSSES` to the merge request.

## Add a backend

A backend implements `StorageBackend`, which parses its `backend` blocks and resolves each store. It also declares its [capabilities](../adr/024_pluggable_data_model.md#4-catalogs-give-each-connection-a-typed-view): joins, writer, search and delegates.

```rust
pub trait StorageBackend: Sized + 'static {
    const KIND: &'static str;
    const CAPABILITIES: Capabilities;
    const POLICIES: &'static [PolicyKind];
    const FIELDS: &'static [Field];
    type Options: Block + DeserializeOwned + Default + WrittenBlock;
    type SourceOptions: Block + DeserializeOwned + Default + WrittenBlock;
    type ColumnOptions: Block + DeserializeOwned + Default + WrittenBlock;
    type Store: Debug + Send + Sync + 'static;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> Self::Store;
    fn serves(store: &Self::Store, entity: EntityId, property: PropertyId, operation: Operation) -> bool;
}

pub struct Capabilities {
    pub joins: bool,
    pub writer: bool,
    pub search: bool,
    pub delegates: bool,
}
```

`POLICIES` lists the policies the backend renders, and `FIELDS` lists the generic fragment fields it reads. `serves` tells placement which operations a store runs itself, beyond its capabilities and `accepts:`.

Its catalog is `Catalog<B>`. Its `build` method takes a closure over the backend's plan builder, and `Render` turns the plan into the native request:

```rust
pub trait Render {
    type Language: Language;
    type Request;
    type Error;

    fn render(&self, plan: &Plan<Self::Language>) -> Result<Self::Request, Self::Error>;
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
    accepts: [project_id, branch]
    columns:
      project_id: {}
      branch: {}
      path: {}
    backend:
      repository: {project: project_id, branch: branch}
      search: {content: [trigram, structural], path: [trigram]}
```

`search` names the search modes for each property: trigram and structural search on `content`, and trigram search on `path`. `repository` names the columns that hold each row's project and branch. Orbit Next searches by repository, so its stores require `repository`.

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
    accepts: [project_id, branch]
    columns:
      project_id: {}
      branch: {}
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
  connections: [main, code_indexing, data_extraction, jira_lake, slack_lake]
```

The runtime config selects one shape with `schema.deployment`. The loader keeps only the storage fragments on that shape's connections, and then it runs every check. The model ID follows that deployment's model.

A query for every call to `compile` in a project's Rust files plans into a code plan:

```cypher
MATCH (f:File)
WHERE f.project_id = 42 AND f.content MATCHES ast('compile($$$ARGS)', 'rust')
RETURN f.path, match.line, match.ARGS LIMIT 50
```

```rust
let Runs::On { connection, stores } = &stage.runs else { unreachable!() };
let store = stores.iter().find(|(entity, _)| *entity == file).map(|(_, store)| *store);
let code = model.physical::<OrbitCode>(*connection)?;
let plan = code.build(|q| {
    let index = code.index(store.expect("placed"), file)?;
    let files = q.repositories(index, [Literal::from(42)])?;
    let files = q.languages(files, [Language::Rust])?;
    let files = q.prefilter(files, Trigrams::of(["compile"]))?;
    let hits = q.structural(files, Pattern::ast_grep("compile($$$ARGS)")?)?;
    let hits = q.top(hits, 50)?;
    let fields = [hits.column(path)?, hits.line()?, hits.capture("ARGS")?];
    q.select(hits, fields)
})?;
let request = code.render(&plan)?;
```

The planner adds the trigram prefilter: it takes the pattern's literal atoms and narrows the candidate files before the structural pass. The request goes over gRPC to the code indexing web servers. They narrow the files with the trigram index on S3 and run the pattern over the matching objects at the branch heads. They stream back paths, lines and captures.
Placement put `file` on the code store, because the query applies a structural search to `content`, which only that store's backend serves. `get_store(file, &needs)` returned that store for `Runs::On`.
`File` declares `orbit_code: {scope_key: {via: IN_PROJECT}}` next to its ClickHouse policy, and the Orbit Next backend renders it as a repository restriction. `of: holder` on `IN_PROJECT` makes the project that holds the file the scope entity.

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
store: memory
engine:
  kind: clickhouse
  connection: main
  path_column: traversal_path
  backend:
    edge_columns:
      source_id: source_id
      source_kind: source_kind
      target_id: target_id
      target_kind: target_kind
      relationship_kind: relationship_kind
    edge_tables:
      gl_memory_edge:
        columns:
          source_id: {type: String}
          target_id: {type: Int64}
          source_kind: {type: LowCardinality(String)}
          target_kind: {type: LowCardinality(String)}
          relationship_kind: {type: LowCardinality(String)}
          traversal_path: {type: String}
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

`Memory` has string IDs and points at entities with integer IDs, so its edges live in their own table. That table sits in the overlay's own store on the graph connection. The agent memory API writes both:

```yaml
# packages/overlay.agent_memory/sources/agent_api.yaml
kind: source
package: overlay.agent_memory
source: agent_api
entities: [Memory]
relationships:
  ABOUT: [Memory -> MergeRequest, Memory -> Project]
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
impl Provider<Identify> for GitLabSession { /* verify the session token Rails issues */ }
impl Provider<Scopes> for GitLabSession { /* paths at the role the permission maps to, as Subtrees */ }
impl Provider<Check> for RailsAbilities { /* the redaction call to Rails, in batches */ }
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
    jira: {module: jira.sign_in, connection: data_extraction}
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
let statement = gql::parse(text, model.logical(&limits))?;
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
- A field that its fragment's schema does not know, or a missing required backend field. For example, the live backend requires `operation`, and a backend that searches by repository requires `repository`.
- A reference that resolves to no declared fact, or to a fact of the wrong type.
- A declared fact that no backend reads.
- An entity with no `id`, `identity`, `default_columns` or authorization entry, or with an `identity` property that does not exist.
- A property that its complete store neither stores nor excludes. In a store whose backend has the writer capability, a column without a storage type or an entity without a table.
- A property or relationship attribute with no access class, or with two.
- A scope without a `provider`, or a row check without a `resource`.
- A global entity with an `enforce` entry, or with no checks and no `checks: open`.
- A relationship without a scope or a permission. On a backend that joins, an edge policy without a `scope_key` and an `of:`.
- A declaration above the package's trust tier. Only a platform package may declare permissions, scope kinds, a trusted store, `none`, `checks: open` or checks by any provider. An integration package may declare only `Credentials` checks. A namespace overlay declares no checks, and its entities must be namespace- or user-scoped.
- A backend that stores a non-global entity but has no `enforce` entry for it. A store whose backend joins and that binds a relationship without one.
- A scope kind the backend cannot render, or a policy that names a property or relationship the graph lacks.
- A `via` chain that does not reach an entity that holds the scope key, or a step with more than one target.
- An `enforce` entry of `none` with no `checks`.
- A `Credentials` check on an entity that no delegating backend stores.
- A provider name the deployment does not bind, or one bound to a module that does not answer the query the check needs.
- A user-scoped entity whose owner relationship does not reach `User` or allows more than one owner per row. The owner relationship must be a foreign key on the entity's row.
- An unknown deployment shape.
- A stored entity with no `complete: true` store in the deployment's model, or with two.
- Two different minimal combinations of one entity's stores that serve the same needs, unless only one of them includes the complete store.
- A store entry without `accepts:`, or an `accepts:` column that the store does not list.
- A relationship variant with no binding, or with two. A foreign key binds in the store whose table holds the column, and an edge table binds in the store that declares it.
- An edge table whose two endpoints are not both on its connection.
- A foreign key column, `via` step or edge-table ID column whose type differs from the identity it points to. The loader casts nothing.
- In a store that Orbit writes, an entity or edge-table variant with no writer, or with two.
- A writer that targets a store whose backend has no writer capability.
- A foreign key binding on a many-to-many variant, on the wrong side of a one-to-many variant, or on a column its table does not hold.
- An edge table copy that names no property of that endpoint, or a graph attribute with no edge column.
- A store with edge tables whose `edge_columns:` misses an edge field, or an edge table whose columns do not match its store's `edge_columns:`.
- Two backends, or a trusted and an untrusted store, on one connection.

### Composition rules

- A package cannot redeclare an entity that another package owns, or add a second authorization entry for an entity, property or relationship.
- A package can add variants to a platform relationship, which keeps the platform's authorization entry. It can also bind more entities to a store that another package declares.
