---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the GitLab Orbit graph to find GitLab data, code, and relationships.
title: Queries
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

GitLab Orbit queries use a read-only subset of openCypher 9. A query can fetch one
kind of object, traverse relationships between objects, count matching objects,
find a path, or ask for the neighbors of a node.

Queries run through GitLab authorization. The response contains only data the
current user can read in GitLab.

## Choose a query shape

| Use case | Query shape |
|----------|-------------|
| Fetch matching nodes of one entity type | `MATCH (n:Label {...}) RETURN n` |
| Follow relationships between known entity types | `MATCH (a:A)-[:REL]->(b:B) RETURN a, b` |
| Count, sum, average, or group graph results | An aggregate such as `count(n)` in `RETURN` |
| Find a path between two bounded endpoints | `MATCH p = ANY SHORTEST (a:A)-[:REL*1..3]->(b:B) RETURN p` |
| Ask what is connected to one bounded node | `MATCH (a:A {id: 1})--(n) RETURN a, n` |

## Example: fetch a merge request diff

Use the `diff` column on `MergeRequest` to fetch the full unified diff for a
merge request. Request virtual columns explicitly by name.

```gql orbit-query
MATCH (mr:MergeRequest {id: 12345})
RETURN mr{.iid, .title, .state, .diff}
LIMIT 1
```

Merge request diff content has a few different shapes:

| Entity | Column | What it returns |
|--------|--------|-----------------|
| `MergeRequest` | `diff` | Full unified diff for the merge request |
| `MergeRequestDiff` | `patch` | Full patch for one diff snapshot |
| `MergeRequestDiffFile` | `diff` | Per-file unified diff text |
| `File` | `content` | Raw source file text |
| `Definition` | `content` | Source text for one indexed definition |

The `content` column is for source code nodes. For merge request diff text, use
`diff` or `patch`, depending on the entity.

## Example: fetch diff snapshots and changed files

Use `HAS_DIFF` to move from a merge request to its diff snapshots, then
`HAS_FILE` to fetch the files in those snapshots.

```gql orbit-query
MATCH (mr:MergeRequest {id: 12345})-[:HAS_DIFF]->(snapshot:MergeRequestDiff)-[:HAS_FILE]->(file:MergeRequestDiffFile)
RETURN mr{.iid, .title, .state}, snapshot{.id, .state, .patch}, file{.new_path, .old_path, .too_large, .diff}
LIMIT 20
```

`MergeRequestDiffFile.diff` is `null` when `too_large` is `true`.

## Example: fetch source file content

Use `content` on source code entities. This example searches indexed files by
path and returns the raw file text.

```gql orbit-query
MATCH (file:File)
WHERE file.path ENDS WITH 'app/models/project.rb'
RETURN file{.path, .language, .content}
LIMIT 5
```

For the full syntax, available fields, and validation rules, see
[GitLab Orbit query language](query-language.md).
