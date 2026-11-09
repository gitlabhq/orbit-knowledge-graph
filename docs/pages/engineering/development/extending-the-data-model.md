# Extending the data model

The data model is being built on the [data model branch](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/tree/michaelusa/data-model-spike).
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

The graph fragment declares its properties:

```yaml
kind: graph
package: gitlab.code_review
entities:
  MergeRequest:
    label: title
    defaults: [id, iid, title, state, source_branch, target_branch]
    properties:
      id: {type: int64, immutable: true}
      title: {type: string}
      state: {type: enum, values: {1: opened, 2: closed, 3: merged, 4: locked}}
```

The authorization fragment declares its scope, its role floor and its redaction check:

```yaml
kind: authorization
package: gitlab.code_review
entities:
  MergeRequest:
    scope: namespace
    role: reporter
    redaction: {resource: merge_request, ability: read_merge_request}
```

The storage fragment lays it out in a store. Shared fields work on every backend. The `backend` block holds what only that backend understands, and that backend validates it.

```yaml
kind: storage
package: gitlab.code_review
store: graph
entities:
  MergeRequest:
    backend:
      primary_key: [traversal_path, id]
      columns:
        title: {default: "''", codec: [ZSTD(1)]}
```

The source fragment says where the indexer reads it from:

```yaml
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

A relationship that a foreign key carries needs no source entry. `AUTHORED` reaches `MergeRequest` through `author_id`, so the graph reference and the storage route are enough.

A relationship can declare typed attributes, which queries filter and select like properties:

```yaml
relationships:
  CALLS:
    variants: [Definition -> Definition]
    attributes: {project_id: int64, branch: string}
```

Publish the new package version to the registry. The registry diffs its storage against the active version, the indexing engine backfills the tables in scope, and the version goes live.

## Add an indexed third-party source

An integration that lands rows in the data lake ships a package with graph, authorization, storage and source fragments. Jira issues use the graph store, like GitLab entities:

```yaml
kind: source
package: integration.jira
source: data_lake
entities:
  JiraIssue:
    tables: [jira_issues]
    order_by: [traversal_path, id]
    query: generated
```

Every row carries the `traversal_path` of the namespace that installed the integration, so the loader gives `JiraIssue` a path guard like any other entity.

## Add a live third-party entity

An entity can be fetched at query time instead of indexed. Its storage is a `live` store that holds no rows.

```yaml
kind: graph
package: integration.slack
entities:
  SlackMessage:
    label: text
    properties:
      id: {type: string, immutable: true}
      channel: {type: string}
      author: {type: string}
      text: {type: string}
      sent_at: {type: timestamp}
```

```yaml
kind: authorization
package: integration.slack
entities:
  SlackMessage: {scope: namespace, role: reporter, delegated: true}
```

```yaml
kind: storage
package: integration.slack
store: slack
engine:
  kind: live
  connection: data_extraction
  path_column: traversal_path
entities:
  SlackMessage:
    backend:
      operation: search.messages
      filters: [channel, author, sent_at]
      text: text
```

The live catalog's plan language is API calls: an operation, the filters that operation can push down, a time window, paging and a limit. A filter the operation cannot push down is a construction error.

```plaintext
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

The Data Extraction Service runs the request with the caller's own Slack sign-in, and tokens never leave the service. Each workspace maps to the namespace that installed it. The security phase sends the request only for workspaces in namespaces the caller can read. `delegated: true` records that Slack then applies its own visibility through the caller's sign-in.

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

Orbit Next is a backend whose catalog speaks in repositories and structural matches. Its storage fragment binds `File` to a store on the code indexing connection:

```yaml
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

```plaintext
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

The trigram prefilter is a planning decision: the planner takes the pattern's literal atoms and narrows the candidate files before the structural pass. The rendered request travels over gRPC to the code indexing web servers, which run the pattern and stream back paths, lines and captures. The security phase turns the `File` guard into a repository restriction.

## Add a tenant overlay

A tenant overlay is a package that loads only into one tenant's model. Agent memory adds a `Memory` entity linked to the merge requests and projects it is about:

```yaml
kind: graph
package: tenant.agent_memory
entities:
  Memory:
    label: summary
    properties:
      id: {type: string, immutable: true}
      summary: {type: string}
relationships:
  ABOUT:
    variants: [Memory -> MergeRequest, Memory -> Project]
```

Its entities appear only in that tenant's catalogs, and that tenant's plans and caches carry their own model ID.

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

The loader collects every error, names its package, and fails once:

- An unknown entity, property, relationship, variant, role, scope, field or template.
- An entity or variant declared twice, or a second authorization entry.
- An entity with no `id`, or no authorization entry.
- A foreign key on neither endpoint, or one the graph does not mark as a reference.
- Two backends, or a trusted and an untrusted store, on one connection.
- A namespaced entity that no guard rule covers.

### Composition rules

- A package cannot redeclare an entity that another package owns.
- A package cannot add a second authorization entry for an entity, property or relationship.
- A package can add variants to a platform relationship.
- A package can bind more entities to a store that another package declares.
