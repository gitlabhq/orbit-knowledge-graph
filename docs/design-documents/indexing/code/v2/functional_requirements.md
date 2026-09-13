# Orbit Code Indexing Functional Requirements

This document defines what Orbit Code Indexing and its query API must do.
It describes required behavior, independent of the database and storage design.

All API and event fixtures in this document are pseudocode. They show required behavior, not the final API syntax or field names.
Example payloads show only the fields relevant to each scenario.

[[_TOC_]]

## Terms

| Term. | Meaning. |
| --- | --- |
| Top-level namespace | The root GitLab group or personal namespace that owns a project, such as `gitlab-org`. |
| Traversal path | A chain of stable IDs that identifies an authorized namespace or project scope, such as `1/9970/1234567890/`. |
| Git commit | An immutable version of a repository tree. |
| Git tag | A named Git reference that can select a commit for search. |
| Search snapshot | The internal search view that holds indexed data and selected commits fixed across pages. |
| Continuation token | An opaque value that the client returns unchanged to fetch the next page. Also called a cursor. |
| Blob | The bytes of one Git file version. |
| Code graph | Definitions and relationships extracted from source code, including calls, imports, and containment. |
| Incremental indexing | Update affected content and relationships while reusing unchanged indexed data. |
| Push to search | The time from a GitLab push to its changes becoming queryable in Orbit. |
| Gitaly | The Git service from which Orbit reads repository data. |

## High-Level Product Contract

Orbit's Code Indexing service must support both content matching and code graph queries, as described in the [motivation](motivation.md).

From a query perspective:

- Users must be able to **query definitions and relationships** through the same index that supports content search.
- Users must be able to **find exact code matches** in the repository versions they select and may read.
- Orbit must support **text**, **regex**, and **ast-grep structural search**.
- Orbit must support optional file content filters in graph queries.
- Users must be able to **combine content filters and code graph relationships** in one query.
- Users must be able to **search across branches, tags, and commits**, including a specific commit in a specific project.
- These scenarios must be available through the UI, Orbit CLI, and query API (Open-Cypher/GQL).

For indexing and access:

- Repository changes must use **incremental indexing** for both content and the code graph.
- Orbit must **minimize push-to-search delay** and **expose indexing progress**.
- Search and indexing must **impose minimal load on Gitaly**.
- GitLab **authorization** must apply throughout the corpus and every product surface.
- Each top-level namespace must have a **strict data boundary** within its Organization.

## Querying Behavior

Every query must validate the caller's access scope before selecting or fetching protected content or graph data.
Unless a branch, tag, or commit is selected, Orbit must query each project's indexed default branch.

### Code graph and combined queries

- Code graph and combined queries must support the same project and revision scopes as content search.
- These scopes include exact commits, branch and tag names, and patterns.
- Content matches, definitions, and relationships must retain their selected project-and-commit context.
- A relationship must not connect nodes from incompatible revisions or reveal a node outside the caller's permissions.
- A cross-project relationship must refer to endpoint revisions pinned in the same published search snapshot.

| Type | Scenario | Required result |
| --- | --- | --- |
| Definition search | Find definition D at commit Y in project 42. | Return its kind, source location, project ID, and commit identity from that exact tree. |
| Relationship traversal | Find callers, callees, imports, or containing definitions for D. | Return the matching relationships and authorized nodes from the selected revision. |
| Related files | Find files related to a matching definition. | Return files connected through the requested graph relationships, with the relationship that explains each result. |
| File content and graph | Find definitions in files whose content contains X. | Select matching files, then return their definitions. The text may appear anywhere in the file, outside the definition itself. |
| Content and graph | Find definitions whose source contains X. | Apply the content match within each definition's source range. A match elsewhere in the file must not qualify. |
| Content and graph | Find callers of definitions that match X. | Select definitions by content, then return their callers from the same revision. |
| Graph and content | Find content X in files selected by a graph query. | Search only those files, with the same project, revision, language, and access filters. |
| Graph coverage | Query a language with incomplete graph support. | State which graph capabilities are available. Do not present missing analysis as proof that no relationships exist. |

> [!IMPORTANT]
>
> - Graph results must distinguish resolved relationships from unresolved references.
> - Orbit must not invent a relationship when the source analysis cannot resolve it.

### Graph filtering by file content

- Orbit can optionally use file content search results to constrain graph queries in the same request.
- When callers supply a content filter, Orbit must apply it without requiring file IDs first.
- Supplied filters must support exact text, regex, and structural search with the project, revision, language, and path filters defined below.

| Condition | Required result |
| --- | --- |
| No file content filter is supplied. | Evaluate the graph query without requiring a content match. Apply its project, revision, and access scope. |
| A file content filter is supplied. | Apply it to the selected File nodes. Do not treat a supplied filter as optional during execution. |
| The filter selects File nodes by content containing X. | Return only files with a verified content match, even when content is not requested as an output field. |
| The filter selects a File node within a larger graph query. | Constrain that node to matching files. Return only graph rows that satisfy both the content filter and the requested relationships. |
| The query traverses from content-matched files. | Use only matching files as the starting set. Returned nodes must satisfy the requested traversal and permissions. |
| The filter has no matches in the fully searched scope. | Return zero graph results when the search is complete. Never ignore the content filter. |
| A supplied filter matches only some files in the scope. | Keep only matches and graph results reached through them. A matching file must not cause unrelated files to qualify. |
| The query counts or pages results with a content filter. | Apply content filtering before graph result sorting, limits, counts, and pagination. Evaluate the full selected scope, not just an unfiltered page. |
| The supplied filter cannot be fully evaluated. | Return an explicit error or incomplete status. Never substitute unfiltered graph results or claim a complete empty result. |

File matches and graph nodes must use the same authorized project-and-commit context within the search snapshot.
Filtering by whole-file content differs from filtering within a definition's source range. Both operations must be supported.

### Example: file content selects graph results

**Given**

The caller can read project 42 at commit A. These are its source files.

`src/irrigation.rs`

```rust
const CROP: &str = "tomato";
fn water_beds() {}
```

`src/tools.rs`

```rust
fn sharpen_shears() {}
```

**Query**

```json
{
  "project_id": 42,
  "commit": "A",
  "files": { "content": { "mode": "text", "pattern": "tomato" } },
  "graph": {
    "from": "matching_files",
    "relationship": "contains",
    "return": "function_definitions"
  }
}
```

**Example payload**

```json
{
  "status": "complete",
  "project_id": 42,
  "commit": "A",
  "function_definitions": [
    {
      "name": "water_beds",
      "file": "src/irrigation.rs",
      "range": { "start_line": 2, "end_line": 2 }
    }
  ]
}
```

The response excludes `sharpen_shears`. Only the file containing `water_beds` matches, even though the text is outside the definition.

**Query: absent text**

```json
{
  "project_id": 42,
  "commit": "A",
  "files": { "content": { "mode": "text", "pattern": "dragonfruit" } },
  "graph": {
    "from": "matching_files",
    "relationship": "contains",
    "return": "function_definitions"
  }
}
```

**Example payload**

```json
{
  "status": "complete",
  "project_id": 42,
  "commit": "A",
  "function_definitions": []
}
```

**Query: no content filter**

Omit the content filter and select definitions from all files in the authorized project and commit.

```json
{
  "project_id": 42,
  "commit": "A",
  "graph": {
    "from": "files",
    "relationship": "contains",
    "return": "function_definitions"
  }
}
```

**Example payload**

```json
{
  "status": "complete",
  "project_id": 42,
  "commit": "A",
  "function_definitions": [
    { "name": "water_beds", "file": "src/irrigation.rs", "range": { "start_line": 2, "end_line": 2 } },
    { "name": "sharpen_shears", "file": "src/tools.rs", "range": { "start_line": 1, "end_line": 1 } }
  ]
}
```

### Example: content and graph queries

**Given**

The caller can read project 42, `shop/api`, at commit A.

`src/checkout.rs`

```rust
fn checkout() {
    charge();
}

fn charge() {
    payment.unwrap();
}
```

**Query**

Find the caller of the definition that contains the structural match.

```json
{
  "project_path": "shop/api",
  "commit": "A",
  "content": {
    "mode": "structural",
    "language": "rust",
    "pattern": "$X.unwrap()"
  },
  "graph": {
    "from": "definitions containing the content matches",
    "relationship": "callers"
  }
}
```

**Example payload**

All locations below belong to project 42, `shop/api`, at commit A.

```json
{
  "project_id": 42,
  "project_path": "shop/api",
  "commit": "A",
  "content_matches": [
    {
      "file": "src/checkout.rs",
      "range": { "start_line": 6, "end_line": 6 },
      "containing_definition": "charge"
    }
  ],
  "relationships": [
    {
      "kind": "calls",
      "from": {
        "name": "checkout",
        "file": "src/checkout.rs",
        "range": { "start_line": 1, "end_line": 3 }
      },
      "to": {
        "name": "charge",
        "file": "src/checkout.rs",
        "range": { "start_line": 5, "end_line": 7 }
      }
    }
  ]
}
```

### Content search types

The search type determines how Orbit matches content. Filters determine which projects, revisions, and files it searches.

| Type | Scenario | Required result |
| --- | --- | --- |
| Exact text search | Find the literal text `payment.unwrap()`. | Return exact substring matches. Treat punctuation and regex characters as literal text. |
| Regex search | Find calls matching `payment[.][a-z_]+[(]`. | Return matches under the documented regex syntax, case rules, and multiline behavior. |
| Structural search | Find the ast-grep pattern `$X.unwrap()` in Rust. | Return syntax matches in Rust code. Require a supported language. Text similarity alone is insufficient. |

Invalid patterns and unsupported languages must return clear input errors.
Candidate filtering must not discard valid matches, including patterns with little or no fixed text.

### Filters across projects

- These filters must work across accessible projects and within a single project.
- They must compose with each content search type.
- Project and revision scope must also apply to code graph and combined queries.

| Filter | Scenario | Required result |
| --- | --- | --- |
| Authorized scope | Search without a project filter. | Search accessible projects only. A filter must never expand the caller's permissions. |
| Group or subgroup | Find X within a selected namespace. | Search its accessible descendant projects. Exclude projects outside that namespace. |
| Project list | Find X in projects selected by IDs or full project paths. | Search the intersection of the selected projects and the caller's permissions. |
| Default branch | Find X without a branch, tag, or commit selector. | Search each project's indexed default branch, even when default branch names differ. |
| Branch name | Find X on the branch `main` across projects. | Search the named branch in each selected project. Do not substitute a project's default branch. |
| Branch pattern | Find X on branches matching `release/*`. | Search every matching indexed branch in the selected projects. Preserve project, branch, and commit identity. |
| Tag name | Find X at tag `v1.0` across projects. | Search each selected project's commit for that tag. Preserve project, tag, and commit identity. |
| Tag pattern | Find X at tags matching `v1.*`. | Search every matching indexed tag in the selected projects. Apply path, language, and content filters to each resolved tree. |
| Indexed history | Find X across retained commits in selected projects. | Search the selected projects' indexed commit coverage. Keep each project-and-commit context separate. |
| Language | Find X in Rust files. | Search files identified as Rust within the selected project and revision scope. |
| File pattern | Find X in files matching `*.rs`. | Match the file pattern at any directory depth under the documented glob rules. |
| Directory pattern | Find X under `src/**`. | Search only matching repository-relative paths in each selected project and revision. |
| Combined filters | Find X in Rust files under `src/**` on `release/*` across selected projects. | Apply every filter together. A result must satisfy all filters and the caller's permissions. |

Branch and tag names are resolved separately for each project. The same name can therefore select different commits across projects.
> [!NOTE]
>
> - If a selected branch or tag is missing or not indexed, report that coverage explicitly. Never silently search another revision.
> - Default branch, branch name, branch pattern, tag name, tag pattern, indexed history, and exact commit are alternative revision selectors. Reject conflicting selectors.
> - Distinguish branch selectors from tag selectors, even when their names match.

### Queries scoped to one project

These requests must specify `project_id` or `project_path`, such as `42` or `shop/api`.
A project path must include its full namespace path. Both forms must resolve to the same authorized project.
If both fields are supplied, they must identify the same project. Reject conflicting values.
Resolve paths when the search starts, then pin stable project IDs in the search snapshot. Recheck current permissions on every page.
Branch names, tag names, their patterns, and file patterns also work across projects. This table defines their single-project use.

| Selector | Scenario | Required result |
| --- | --- | --- |
| Project ID or path | Find X in project 42 or `shop/api`. | Search only project 42's indexed default branch, subject to authorization. |
| Project ID or path, and branch | Find X on `release/1.0` in project 42. | Resolve that project's branch to a commit. Return results only from that tree. |
| Project ID or path, and tag | Find X at tag `v1.0` in project 42 or `shop/api`. | Resolve that project's tag to a commit. Return results only from that tree. |
| Project ID or path, and commit | Find X at commit Y in project 42. | Search the exact tree at Y in project 42. Reject an exact-commit request without a project ID or path. |
| Project ID or path, revision, and file pattern | Find X in Rust files at commit Y under `src/**` in project 42. | Apply path and language filters within that project's exact revision. |

Exact-commit selection must support content, code graph, and combined queries.
A commit hash alone must not select projects or grant access, even when several projects contain that commit.
Finding commits that contain X across projects remains a result-discovery query, as described below.

### Example: cross-project and single-project filters

**Given**

The caller can read both projects. Their selected revisions are indexed.

| Project ID | Project path | Indexed coverage |
| --- | --- | --- |
| 42 | `shop/api` | `release/1.0` at commit A. |
| 44 | `shop/web` | `release/1.0` at commit B. |

These are the only matching branches. Each tree contains one structural match under `src/**`.
Project 42 uses the `src/checkout.rs` source shown above. Project 44 contains this file.

`src/client.rs`

```rust
fn render() { response.unwrap(); }
```

**Queries**

Across projects, select by full path. A project ID list must also be supported.

```json
{
  "project_paths": ["shop/api", "shop/web"],
  "content": { "mode": "structural", "pattern": "$X.unwrap()" },
  "filters": {
    "branch_pattern": "release/*",
    "language": "rust",
    "file_pattern": "src/**"
  }
}
```

The same cross-project query can select projects by ID.

```json
{
  "project_ids": [42, 44],
  "content": { "mode": "structural", "pattern": "$X.unwrap()" },
  "filters": {
    "branch_pattern": "release/*",
    "language": "rust",
    "file_pattern": "src/**"
  }
}
```

Within one project, select the same commit by project ID or project path.

```json
[
  {
    "project_id": 42,
    "commit": "A",
    "content": { "mode": "structural", "pattern": "$X.unwrap()" },
    "filters": { "language": "rust", "file_pattern": "src/**" }
  },
  {
    "project_path": "shop/api",
    "commit": "A",
    "content": { "mode": "structural", "pattern": "$X.unwrap()" },
    "filters": { "language": "rust", "file_pattern": "src/**" }
  }
]
```

**Example payload**

The cross-project query returns matches with their branch and commit context.

```json
{
  "status": "complete",
  "matches": [
    {
      "project_id": 42,
      "project_path": "shop/api",
      "branch": "release/1.0",
      "commit": "A",
      "file": "src/checkout.rs",
      "range": { "start_line": 6, "end_line": 6 },
      "text": "payment.unwrap()"
    },
    {
      "project_id": 44,
      "project_path": "shop/web",
      "branch": "release/1.0",
      "commit": "B",
      "file": "src/client.rs",
      "range": { "start_line": 1, "end_line": 1 },
      "text": "response.unwrap()"
    }
  ]
}
```

Both single-project queries return the same payload against the same indexed data.

```json
{
  "status": "complete",
  "matches": [
    {
      "project_id": 42,
      "project_path": "shop/api",
      "commit": "A",
      "file": "src/checkout.rs",
      "range": { "start_line": 6, "end_line": 6 },
      "text": "payment.unwrap()"
    }
  ]
}
```

### Result selection

The result kind determines what Orbit returns. It does not change the search type or the selected scope.

| Result kind | Scenario | Required result |
| --- | --- | --- |
| Content matches | Find matching source locations. | Return each match with its authorized project, file, revision, and source range. |
| Projects | Find projects whose code contains X. | Return distinct matching projects with supporting source matches. |
| Branches | Find branches whose code contains X. | Return distinct project-and-branch pairs with resolved commits and supporting matches. |
| Tags | Find tags whose code contains X. | Return distinct project-and-tag pairs with resolved commits and supporting matches. |
| Commits | Find commits whose trees contain X, with indexed history selected. | Return distinct project-and-commit pairs within indexed coverage, with supporting matches. |

All result kinds must support the three content search types and the applicable filters above.
Shared stored content must preserve every authorized project, file, branch, tag, and commit association.

> [!NOTE]
>
> - Dependency search means matching content in source or manifest files. It does not imply package resolution or analysis of transitive dependencies.
> - Searching a commit tree also differs from finding the commit that introduced a change.

## Branches, Tags, Commits, and Coverage

Orbit must support selecting any branch, tag, or commit, including non-default branches and commits outside the default branch history.

- The configured indexing and retention policy determines which revisions are queryable, as described in [branch and commit indexing](commits_and_branches_indexing.md).
- Orbit must expose that coverage and identify requests outside it.
- A retention limit must never cause a different revision to be searched.

A search across projects, branches, or tags must pin every selected project-and-commit pair and its matching content and graph data.
Any relationship across projects must use those pinned revisions. Pagination must preserve the full selection.

Tag selection must support lightweight and annotated tags that resolve to commits. Reject tags that do not resolve to a commit with a clear error.
Tag queries search the resolved repository tree. They do not search tag messages.


| Scenario | Required result |
| --- | --- |
| Query a branch or tag that moved or was deleted after the search started. | Continue the original search at its resolved commit. New searches must use current revision coverage and report deleted references as missing. |
| Query a file that was renamed or deleted. | Return its path and content at the selected commit. Its absence at the latest commit must not remove retained history. |
| Query an unindexed, pending, or expired revision. | Return an explicit coverage status. Never return a complete empty result as if that revision was searched. |
| Query a repository with excluded files. | Report relevant exclusions, including file-size, encoding, or language limits. |
| Query many branches or tags that share a commit or blob. | Reuse indexed data while preserving the requested branch, tag, and commit associations. |

### Example: tag selection

**Given**

The caller can read both projects. These are the only tags matching `v1.*`, and their target commits are indexed.

| Project ID | Project path | Tag | Tag type | Commit. |
| --- | --- | --- | --- | --- |
| 42 | `shop/api` | `v1.0` | Annotated. | A |
| 44 | `shop/web` | `v1.0` | Lightweight. | B |

Both trees contain `payment.unwrap()` in `src/checkout.rs`. The tag name selects a different commit in each project.

**Queries**

Select one tag by project ID or project path. Then search a tag pattern across projects.

```json
[
  { "project_id": 42, "tag": "v1.0", "text": "unwrap" },
  { "project_path": "shop/api", "tag": "v1.0", "text": "unwrap" },
  { "project_paths": ["shop/api", "shop/web"], "tag_pattern": "v1.*", "text": "unwrap" }
]
```

**Example payload**

The first two queries return the same payload.

```json
{
  "status": "complete",
  "project_id": 42,
  "tag": "v1.0",
  "commit": "A",
  "files": ["src/checkout.rs"]
}
```

The tag-pattern query keeps both project-and-tag associations.

```json
{
  "status": "complete",
  "results": [
    { "project_id": 42, "tag": "v1.0", "commit": "A", "files": ["src/checkout.rs"] },
    { "project_id": 44, "tag": "v1.0", "commit": "B", "files": ["src/checkout.rs"] }
  ]
}
```

### Example: exact commit behavior

**Given**

The caller can read project 42, `shop/api`. Both commits are indexed.

| File | Commit A contains | Commit B contains. |
| --- | --- | --- |
| `src/checkout.rs` | `payment.unwrap()` | `payment?` |

**Queries**

```json
[
  { "project_id": 42, "commit": "A", "text": "unwrap" },
  { "project_path": "shop/api", "commit": "B", "text": "unwrap" },
  { "project_id": 42, "commit": "B", "text": "payment?" }
]
```

**Example payload**

The responses below follow the query order.

```json
[
  { "status": "complete", "project_id": 42, "commit": "A", "files": ["src/checkout.rs"] },
  { "status": "complete", "project_id": 42, "commit": "B", "files": [] },
  { "status": "complete", "project_id": 42, "commit": "B", "files": ["src/checkout.rs"] }
]
```

## Results and Pagination

| Scenario | Required result |
| --- | --- |
| Return a content match or definition. | Include project, file, commit, and source range. Include matching branch or tag references when those selectors were used. |
| Return a relationship. | Identify both endpoints, the relationship kind, and the revision context. |
| Count results. | State the counted unit. Label totals as exact, estimated, or a lower bound. Provide an estimated total when full counting is too costly. |
| Reach a time, candidate, or result limit. | Mark the response as incomplete, explain the limit, and state whether the caller can continue. |
| Fetch another page during indexing or compaction. | Continue against the same selected commits and indexed data, in the same result order. Updates must not cause skipped or repeated results. |
| Continue a search. | Accept an opaque continuation token. Recheck current permissions before reading data for each page. |
| Change a query while reusing its cursor. | Reject a cursor that does not match the query's filters, revision, or scope. |
| Resume after token expiry or loss of the saved search view. | Return a clear response that requires restarting the search. Never silently switch to newer data. |
| Run the same query through UI, API, or Orbit Remote CLI. | Preserve modes, filters, revision selectors, result kinds, counts, and coverage status. |

The API must return a continuation token when another page is available. Clients must return it unchanged with the same query.
Tokens must prevent tampering with the query or saved search view and must not reveal protected metadata.
Permission changes must not expand the original project and revision selection.
Users must not need to select or manage storage snapshots. The UI must provide a way to load more results.

### Example: paging during an update

**Given**

The caller can read project 42, `shop/api`. Its indexed `main` branch points to commit A.
The search has exactly two pages of matches. Permissions stay the same throughout this example.

**Query**

```json
{
  "project_id": 42,
  "branch": "main",
  "text": "payment"
}
```

**Example payload: first page**

```json
{
  "project_id": 42,
  "commit": "A",
  "next_cursor": "opaque-token-1"
}
```

**Action and next query**

Publish commit B on `main`. Then repeat the original query with the returned token.

```json
{
  "project_id": 42,
  "branch": "main",
  "text": "payment",
  "cursor": "opaque-token-1"
}
```

**Example payload**

The next page stays at commit A. The update must not cause repeated or skipped matches.

```json
{
  "project_id": 42,
  "commit": "A",
  "next_cursor": null
}
```

Repeat the original query without a token to start a new search at commit B.

```json
{
  "project_id": 42,
  "commit": "B"
}
```

An expired token or unavailable saved view requires a restart. These alternative error responses contain no page results.

```json
[
  { "status": "restart_required", "reason": "token_expired" },
  { "status": "restart_required", "reason": "view_unavailable" }
]
```

## Authorization and Namespace Isolation

GitLab Rails owns access decisions. The trusted caller must supply authenticated traversal-path grants for each query.
Orbit must validate and enforce those grants, plus any required resource-level checks.
A project filter, blob hash, graph node ID, or cursor must never grant access by itself.

| Scenario | Required result |
| --- | --- |
| Query with missing, invalid, or expired credentials. | Deny access before reading protected data. |
| Query with a subgroup-only grant. | Restrict candidate selection to authorized projects before matching or graph traversal. Shared storage blocks must not widen access. |
| Query content or graph data across several authorized top-level namespaces. | Apply each namespace's storage and access boundary before combining results. |
| Query metadata, counts, refs, or diagnostics. | Apply the same permissions as source and graph results. Do not reveal private names, paths, or counts. |
| Query shared content through an unauthorized project. | Deny access. Knowing the blob hash or reading an authorized copy must not expose other owners. |
| Lose access while paging. | Recheck permissions before reading the next page. Remove or reject access to the revoked scope. |
| Transfer a project to another top-level namespace. | Revoke old-scope access before publishing new ownership. Update graph ownership and affected relationships. Old cursors and caches must not restore access. |
| Delete a project or namespace. | Remove access to its content, graph, and metadata, including relationships to deleted nodes. Preserve unrelated projects and their graph records. |
| Use credentials scoped to namespace A against namespace B. | Storage must reject the access, even if a query or worker selects the wrong data. |

These boundaries must cover indexes, source bytes, metadata, caches, work queues, temporary files, backups, and cleanup.
Any shared content service must enforce project-level access before fetching or serving bytes, including cached bytes.
Permission changes must take effect within a defined maximum delay. Stored snapshots must not bypass current permissions.

### Example: a cursor cannot preserve revoked access

**Given**

The trusted caller initially supplies the grant below. It permits access to project 43, `group-a/team/api`, but not project 84, `group-b/api`.

```yaml
trusted_caller:
  traversal_paths: ["1/10/20/"]
```

**Query**

```json
{
  "project_id": 43,
  "text": "payment"
}
```

**Example payload: first page**

The authorized search returns a page with a continuation token.

```json
{
  "next_cursor": "opaque-token-1"
}
```

**Action and next queries**

Revoke access to project 43. Once revocation takes effect, the trusted caller supplies no grants for either request below.

```yaml
trusted_caller:
  traversal_paths: []
```

```json
[
  { "project_id": 43, "text": "payment", "cursor": "opaque-token-1" },
  { "project_path": "group-b/api", "text": "payment" }
]
```

**Example payload**

Both requests return the same denial payload. Neither request may read data from the revoked or unauthorized scope.

```json
{
  "status": "access_denied"
}
```

## Incremental Indexing and Push to Search

Repository changes must update affected content, definitions, and relationships upon event delivery from GitLab.

- They must not rebuild the whole repository index.
- Unchanged source can still need relationship updates when a changed definition affects its callers or imports.
- Orbit must recompute those affected relationships while reusing unchanged source bytes and valid parse results.
Resolved relationships may be reused only when their resolution context remains valid.

| Scenario | Required result |
| --- | --- |
| Check an unchanged repository or replay an applied event. | Reuse the published data. Do not fetch source bodies or create a replacement index. |
| Change one file. | Fetch missing content and update its index records plus any affected graph relationships. Reuse unrelated records. |
| Change an exported definition or import target. | Update affected relationships from unchanged dependent files. Include supported cross-project relationships and preserve unrelated records. |
| Rename or delete a file. | Update paths, revision membership, definitions, and affected relationships. Preserve retained historical results. |
| Create, move, or delete a branch or tag. | Update reference membership and fetch only missing content. A reference change must not trigger a full repository re-index. |
| Force-push a branch. | Reconcile the new tree against stored content. Rewritten ancestry alone must not trigger a full refetch or re-index. |
| Receive duplicate or out-of-order events. | Apply changes without duplication or rollback to an older published state. |
| Fail during indexing or publication. | Keep the last complete snapshot available. Retry safely without exposing a partially updated content index or graph. |
| Publish a pushed commit. | Make its content, graph, and revision metadata available consistently. Report the searchable commit separately from the latest observed commit. |
| Index many projects while queries and compaction run. | Maintain query correctness and indexing progress within resource budgets. Large projects must not block every other project. |
| Restart an Orbit worker. | Recover from durable state. Local cache loss must not require a full repository refetch. |

> [!NOTE]
> - Initial indexing and repair of lost or incompatible data may require rebuilding affected records.
>- Orbit must expose the reason and scope.
>- Routine pushes and transient source failures must not use that path.
>- Push-to-search reporting must cover discovery, preparation, queueing, publication, and reader visibility.

### Example: search results after a force push

**Given**

Project 42, `shop/api`, has commit A indexed and retained. A force push replaces it with commit B on `main`.
A and B need not share ancestry. Only `src/checkout.rs` changes.

| File | Commit A | Commit B |
| --- | --- | --- |
| `src/checkout.rs` | Exports `charge`. | Exports `charge_card`. |
| `src/config.rs` | Imports `charge` from `checkout`. | Unchanged. |
| `README.md` | Project documentation. | Unchanged. |

**Action**

```json
{
  "event": "force_push",
  "project_id": 42,
  "branch": "main",
  "previous_commit": "A",
  "commit": "B"
}
```

**Queries**

After commit B is indexed, query the configuration file's imports at both commits.

```json
[
  {
    "project_id": 42,
    "commit": "A",
    "graph": { "file": "src/config.rs", "relationship": "imports", "include_unresolved": true }
  },
  {
    "project_path": "shop/api",
    "commit": "B",
    "graph": { "file": "src/config.rs", "relationship": "imports", "include_unresolved": true }
  }
]
```

**Example payload**

Commit A retains its resolved import. At B, the same import is unresolved because the exported name changed.

```json
[
  {
    "status": "complete",
    "project_id": 42,
    "commit": "A",
    "relationships": [
      { "from": "src/config.rs", "kind": "imports", "to": { "file": "src/checkout.rs", "name": "charge" } }
    ],
    "unresolved_references": []
  },
  {
    "status": "complete",
    "project_id": 42,
    "commit": "B",
    "relationships": [],
    "unresolved_references": [
      { "file": "src/config.rs", "name": "charge" }
    ]
  }
]
```

## Minimal Load on Gitaly

Orbit must protect Gitaly capacity used by normal GitLab operations.
Adding Orbit workers must not multiply source load without a shared limit.

| Scenario | Required result |
| --- | --- |
| Search published content, graph data, branches, tags, or commits. | Make zero Gitaly requests, including cold-cache queries and later pages. |
| Compact stored indexes or warm query caches. | Use durable indexed data. Do not fetch repository content again. |
| Index a change. | Read only necessary Git metadata and missing source content. Reuse content already stored across runs. |
| Receive a burst of events for one project. | Combine redundant work while retaining the revisions required by the indexing policy. |
| Add workers or index a large monorepo. | Respect deployment-wide request, byte-rate, and concurrency budgets for each Gitaly instance. |
| Encounter source throttling, timeouts, or errors. | Back off with bounded retries. Preserve the last complete view and expose indexing delay. |
| Measure indexing cost. | Record RPC counts, bytes, retries, and concurrency. Separate metadata reads from source-body reads. |

### Example: queries do not depend on Gitaly

**Given**

The caller can read project 42, `shop/api`. Content and graph data for commit B are published.

```yaml
project_id: 42
published_commit: B
gitaly: unavailable
query_cache: empty
```

**Queries**

```json
[
  { "project_id": 42, "commit": "B", "text": "payment" },
  { "project_path": "shop/api", "commit": "B", "definition": "checkout" }
]
```

**Example payload**

Both queries report complete coverage at commit B. Each query must make zero Gitaly requests, measured separately from the response.

```json
{
  "status": "complete",
  "project_id": 42,
  "commit": "B"
}
```

## Edge Cases

Orbit must handle these events without waiting for another push.
These cases apply to content search, graph queries, and combined queries under the same authorization and retention rules.
Event delivery and recovery must meet the operating limits below. Recovery must respect Gitaly budgets and reuse stored content.

### Git and reference events

| Event or scenario | Required result |
| --- | --- |
| Push to the default branch. | Update the selected commit, content, and graph together. A query must not combine old content with new relationships. |
| Create or push to a non-default branch. | Discover and index the branch under the configured revision policy. Do not require a default-branch push. |
| Create, move, or delete a tag. | Update tag selection and coverage, including annotated tags. Reuse already indexed target commits and blobs. |
| Delete a branch or tag without a replacement commit. | Remove the reference from new searches. Preserve commits still covered by retention or other references. Do not interpret deletion as an empty commit. |
| Delete and recreate a reference with the same name. | Use the new reference target. A delayed delete or update from its previous lifetime must not remove or restore the wrong target. |
| Force-push to unrelated history or an older commit. | Reconcile the selected tree without requiring a fast-forward diff. A valid rollback must work even when its commit is older. |
| Change the default branch without pushing code. | New unqualified searches select the new default branch. Report pending coverage if needed. Do not silently keep searching the former default. |
| A branch and tag have the same name. | Keep their types and targets separate during event processing, storage, and queries. |
| One push changes several references. | Process every affected reference. Combining work must not lose a deletion, another branch, or history required by retention. |
| A merge, mirror update, import, or repository restore changes refs. | Apply the same revision and indexing rules as a push. Do not depend on a particular user interface or Git transport. |
| Delete the default branch while other branches remain. | Report the missing default branch for unqualified searches. Keep explicit searches of surviving branches available. |
| Rewrite the default branch or a non-default branch. | Apply the configured commit-retention policy consistently. The indexing path must not silently discard retained history. |
| Observe an unborn repository, an empty committed tree, or a source outage. | Distinguish these states. An empty committed tree removes stale matches at that revision. An outage must not erase published data. |
| An empty repository receives its first commit. | Discover the new content. A previous empty result or indexing checkpoint must not prevent indexing. |
| Delete the last branch while tags or retained commits remain. | Report the missing branch without removing searchable tag targets or retained history. |

### Project, namespace, and access changes

Lifecycle changes must take effect without a source-code change.

| Event or scenario | Required result |
| --- | --- |
| Create, fork, or import a project with existing history. | Discover it within an enabled scope without requiring a later push. Apply its own project identity, permissions, and revision coverage. |
| Rename a project or an ancestor group. | Resolve the new full project path to the same project ID. Refresh result paths without fetching unchanged source content again. |
| Reuse an old project path for another project. | New searches resolve its current project ID. Existing cursors must not switch to the new project or inherit its permissions. |
| Transfer a project within a top-level namespace. | Refresh traversal paths and group-filter membership. Remove access through old grants that no longer apply. |
| Transfer a project or group across top-level namespaces. | Apply the destination boundary to every affected project and graph relationship. Old tasks, caches, and cursors must not publish or serve old-scope data. |
| Change project or ancestor namespace visibility, membership, or repository access. | Apply current GitLab permissions to content, graph data, counts, and later pages. Do not wait for re-indexing to revoke access. |
| Archive or unarchive a project or group. | Keep retained content searchable when GitLab permits access. Archival alone must not act as repository deletion. Resume required updates after unarchiving. |
| Delete a project or namespace while indexing runs. | Remove query access and prevent unfinished work from restoring data. Complete stored-data cleanup under the deletion policy without removing other owners' content. |
| Enable Orbit for a namespace that already contains projects. | Discover existing projects and configured revisions. Report indexing progress without requiring new pushes. |
| Disable and later re-enable Orbit for a namespace. | Stop serving the disabled scope. On re-enablement, recheck ownership and permissions, then reconcile missed changes before claiming current coverage. |

### Event delivery and recovery

| Event or scenario | Required result |
| --- | --- |
| Receive duplicate or out-of-order events. | Apply each change safely and converge on the current reference state. Event arrival order and commit age must not determine which state wins. |
| Miss events during an outage or exceed event retention. | Detect and repair the gap without waiting for another push. Reconcile metadata and fetch only missing content. Expose recovery progress. |
| Run initial indexing while references keep changing. | Preserve changes received during the initial scan. Do not mark the project current until the scan and later changes are reconciled. |
| Receive a task before its commit is readable from the source. | Retry within the source budget and report pending coverage. Do not substitute the current branch tip or mark the task complete. |
| A required commit disappears from the source before indexing. | Report unavailable revision coverage. Do not claim complete history or substitute another commit. Continue recovery for revisions still available. |
| Stop a reference listing early or receive a source error. | Keep the last complete reference set and report incomplete coverage. Do not infer that omitted projects, branches, or tags were deleted. |
| A worker is busy or another worker owns the same indexing work. | Keep the newest required change pending or retry it. Contention must not discard the only signal for that change. |
| A worker loses ownership or finishes after a newer update. | Prevent it from replacing newer published state, even if the commit SHA is unchanged. Recover unfinished work without duplicate publication. |
| A queued task contains an old traversal path or project path. | Recheck current project identity, ownership, and eligibility before publication. Reject obsolete work that would restore deleted or unauthorized data. |
| Exhaust retries or encounter an invalid event. | Expose the failure and affected coverage. Support recovery without a new push. Do not report successful indexing. |
| Fail partway through content, graph, or metadata publication. | Keep the last complete view queryable, subject to current permissions. Resume safely without exposing a mixed revision. |

### Example: a delayed delete cannot remove a recreated tag

**Given**

Project 42 has indexed commits A and B. Tag `v1.0` originally points to A.

**Action**

The event versions below belong to this reference. Delivery occurs in the order shown.

```json
[
  { "project_id": 42, "tag": "v1.0", "event": "create", "version": 3, "commit": "B" },
  { "project_id": 42, "tag": "v1.0", "event": "delete", "version": 2 }
]
```

**Query**

```json
{
  "project_id": 42,
  "tag": "v1.0",
  "text": "payment"
}
```

**Example payload**

The delayed deletion must not remove the recreated tag or restore its former target.

```json
{
  "status": "complete",
  "project_id": 42,
  "tag": "v1.0",
  "commit": "B"
}
```

### Example: a default-branch change needs no push

**Given**

Project 42 has two indexed branches. `main` points to commit A, and `release/2.0` points to commit B.
The default branch is `main`.

**Action**

```json
{
  "event": "default_branch_changed",
  "project_id": 42,
  "default_branch": "release/2.0"
}
```

**Query**

After Orbit processes the change, start a search without a revision selector.

```json
{
  "project_id": 42,
  "text": "payment"
}
```

**Example payload**

No push or source-content fetch is needed because commit B is already indexed.

```json
{
  "status": "complete",
  "project_id": 42,
  "branch": "release/2.0",
  "commit": "B"
}
```

## Operating Limits

The production contract must define maximum revocation delay, push-to-search targets, and query limits.
It must also define Gitaly request, byte-rate, and concurrency budgets.
Define maximum delays for lifecycle-event processing, missed-event detection, recovery, and stored-data deletion. These must apply even when no new push occurs.
Each limit must have an owner, a configured value or service-level target, and a verification workload.
Orbit must expose when it exceeds a target or reaches a limit. These values must be set before production acceptance.
