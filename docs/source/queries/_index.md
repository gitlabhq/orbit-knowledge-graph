---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the Orbit knowledge graph to find GitLab data, code, and relationships.
title: Queries
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

Orbit queries are JSON objects that describe graph work. A query can fetch one
kind of object, traverse relationships between objects, count matching objects,
find a path, or ask for the neighbors of a node.

Queries run through GitLab authorization. The response contains only data the
current user can read in GitLab.

## Choose a query shape

| Use case | Query shape |
|----------|-------------|
| Fetch matching nodes of one entity type | Single-node `traversal` |
| Follow relationships between known entity types | Multi-node `traversal` |
| Count, sum, average, or group graph results | `aggregation` |
| Find a path between two bounded endpoints | `path_finding` |
| Ask what is connected to one bounded node | `neighbors` |

Single-node `traversal` is the search shape. Orbit does not have a separate
`search` query type.

## Example: fetch a merge request diff

Use the `diff` column on `MergeRequest` to fetch the full unified diff for a
merge request. Request virtual columns explicitly by name.

```json
{
  "query_type": "traversal",
  "node": {
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state", "diff"]
  },
  "limit": 1
}
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

## Example: fetch the latest diff snapshot and changed files

Use `HAS_LATEST_DIFF` to move from a merge request to its latest diff snapshot,
then `HAS_FILE` to fetch the files in that snapshot.

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "mr",
      "entity": "MergeRequest",
      "node_ids": [12345],
      "columns": ["iid", "title", "state"]
    },
    {
      "id": "snapshot",
      "entity": "MergeRequestDiff",
      "columns": ["id", "state", "patch"]
    },
    {
      "id": "file",
      "entity": "MergeRequestDiffFile",
      "columns": ["new_path", "old_path", "too_large", "diff"]
    }
  ],
  "relationships": [
    {"type": "HAS_LATEST_DIFF", "from": "mr", "to": "snapshot"},
    {"type": "HAS_FILE", "from": "snapshot", "to": "file"}
  ],
  "limit": 20
}
```

`MergeRequestDiffFile.diff` is `null` when `too_large` is `true`.

## Example: fetch source file content

Use `content` on source code entities. This example searches indexed files by
path and returns the raw file text.

```json
{
  "query_type": "traversal",
  "node": {
    "id": "file",
    "entity": "File",
    "filters": {
      "path": {"op": "ends_with", "value": "app/models/project.rb"}
    },
    "columns": ["path", "language", "content"]
  },
  "limit": 5
}
```

For the full syntax, available fields, and validation rules, see
[Orbit query language](query_language.md).
