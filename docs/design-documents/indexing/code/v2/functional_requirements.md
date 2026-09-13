# Orbit Code Indexing Functional Requirements

This document defines what Orbit Code Indexing and its query API must do.
It describes required behavior, independent of the database and storage design.

All API fixtures in this document are pseudocode. They show required behavior, not the final API syntax or field names.
Example payloads show only the fields relevant to each scenario.

## Table of Contents

- [Terms](#terms).
- [High-Level Product Contract](#high-level-product-contract).
- [Querying Behavior](#querying-behavior).
  - [Code graph and combined queries](#code-graph-and-combined-queries).
  - [Graph filtering by file content](#graph-filtering-by-file-content).
  - [Example: file content selects graph results](#example-file-content-selects-graph-results).
  - [Example: content and graph queries](#example-content-and-graph-queries).
  - [Content search types](#content-search-types).
  - [Filters across projects](#filters-across-projects).
  - [Queries scoped to one project](#queries-scoped-to-one-project).
  - [Example: cross-project and single-project filters](#example-cross-project-and-single-project-filters).
  - [Result selection](#result-selection).
- [Branches, Tags, Commits, and Coverage](#branches-tags-commits-and-coverage).
  - [Example: tag selection](#example-tag-selection).
  - [Example: exact commit behavior](#example-exact-commit-behavior).
- [Results and Pagination](#results-and-pagination).
  - [Example: paging during an update](#example-paging-during-an-update).
- [Authorization and Namespace Isolation](#authorization-and-namespace-isolation).
  - [Example: a cursor cannot preserve revoked access](#example-a-cursor-cannot-preserve-revoked-access).
- [Incremental Indexing and Push to Search](#incremental-indexing-and-push-to-search).
  - [Example: a force push reuses unchanged content](#example-a-force-push-reuses-unchanged-content).
- [Minimal Load on Gitaly](#minimal-load-on-gitaly).
  - [Example: queries do not depend on Gitaly](#example-queries-do-not-depend-on-gitaly).
- [Operating Limits](#operating-limits).

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
- Orbit must filter graph queries by file content search results.
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

Orbit must use file content search results to constrain graph queries in the same request.
Callers must not need to find or supply file IDs first.
This must support exact text, regex, and structural search with the project, revision, language, and path filters defined below.

| Scenario | Required result |
| --- | --- |
| Filter File nodes by content containing X. | Return only files with a verified content match, even when content is not requested as an output field. |
| Filter a File node within a larger graph query. | Constrain that node to matching files. Return only graph rows that satisfy both the content filter and the requested relationships. |
| Traverse from content-matched files to their definitions or relationships. | Use only matching files as the starting set. Returned nodes must satisfy the requested traversal and permissions. |
| Search for text absent from every file in the selected scope. | Return zero graph results when the search is complete. Never ignore the content filter. |
| Search a fixture with both matching and nonmatching files. | Keep only matches and graph results reached through them. A matching file must not cause unrelated files to qualify. |
| Count or page graph results with a content filter. | Apply content filtering before graph result sorting, limits, counts, and pagination. Evaluate the full selected scope, not just an unfiltered page. |
| Fail to evaluate the content filter. | Return an explicit error or incomplete status. Never substitute unfiltered graph results or claim a complete empty result. |

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

These filters must work across accessible projects and within a single project.
They must compose with each content search type. Project and revision scope must also apply to code graph and combined queries.

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
The configured indexing and retention policy determines which revisions are queryable, as described in [branch and commit indexing](commits_and_branches_indexing.md).
Orbit must expose that coverage and identify requests outside it. A retention limit must never cause a different revision to be searched.

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

Repository changes must update affected content, definitions, and relationships. They must not rebuild the whole repository index.
Unchanged source can still need relationship updates when a changed definition affects its callers or imports.
Orbit must recompute those affected relationships while reusing unchanged source bytes and valid parse results.
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

Initial indexing and repair of lost or incompatible data may require rebuilding affected records.
Orbit must expose the reason and scope. Routine pushes and transient source failures must not use that path.
Push-to-search reporting must cover discovery, preparation, queueing, publication, and reader visibility.

### Example: a force push reuses unchanged content

**Given**

Project 42, `shop/api`, has commit A indexed and retained. Only `src/checkout.rs` has different bytes in commit B.
A and B need not share ancestry. Blob 4 is not yet stored.

| File | Commit A | Commit B. |
| --- | --- | --- |
| `src/checkout.rs` | `blob-1` | `blob-4` |
| `src/config.rs` | `blob-2` | `blob-2` |
| `README.md` | `blob-3` | `blob-3` |

Commit B renames the exported definition `charge`. The unchanged configuration file still imports its old name.

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

**Example payload**

This payload shows the indexing outcome, including work counters and graph changes.

```json
{
  "project_id": 42,
  "content_commit": "B",
  "fetched_blobs": ["blob-4"],
  "parsed_blobs": ["blob-4"],
  "reused_parse_blobs": ["blob-2", "blob-3"],
  "full_repository_rebuild": false,
  "retained_commits": ["A"],
  "graph": {
    "commit": "B",
    "removed_relationships": [
      { "from": "src/config.rs", "kind": "imports", "to": "charge" }
    ],
    "unresolved_references": [
      { "file": "src/config.rs", "name": "charge" }
    ]
  }
}
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

## Operating Limits

The production contract must define maximum revocation delay, push-to-search targets, and query limits.
It must also define Gitaly request, byte-rate, and concurrency budgets.
Each limit must have an owner, a configured value or service-level target, and a verification workload.
Orbit must expose when it exceeds a target or reaches a limit. These values must be set before production acceptance.
