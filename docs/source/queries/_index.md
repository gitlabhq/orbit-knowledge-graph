---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the GitLab Orbit graph to find GitLab data, code, and relationships.
title: GitLab Orbit queries
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
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

A query asks the graph one exact question and returns exact rows.
Use queries in scripts, CI/CD jobs, and custom tools.

A query is a JSON object. It names the entities to match, the relationships
to follow, and the properties to return. The response contains only data that
you can read in GitLab.

{{< cards >}}

- [Query language](query-language.md)
- [REST API](api.md)

{{< /cards >}}

## Run a query

1. Put the request in `request.json`:

   ```json orbit-query
   {
     "query": {
       "query_type": "traversal",
       "nodes": [{
         "id": "p",
         "entity": "Project",
         "filters": {"full_path": "gitlab-org/orbit/knowledge-graph"},
         "columns": ["name", "full_path"]
       }],
       "limit": 1
     }
   }
   ```

1. Send the request:

   ```shell
   glab orbit query --file request.json
   ```

   The output is similar to:

   ```plaintext
   @header
   query_type:traversal
   goon_version:4.0.4
   nodes:1
   edges:0
   @nodes
   Project(1):
   77960826 full_path=gitlab-org/orbit/knowledge-graph name="GitLab Orbit"
   @edges
   ```

The CLI returns compact text for AI agents by default.
For structured JSON, add `--response-format raw`.

## Choose a query shape

| Use case | Query shape |
|----------|-------------|
| Fetch matching nodes of one entity type | Single-node [`traversal`](query-language.md#traversal-examples) |
| Follow relationships between known entity types | Multi-node [`traversal`](query-language.md#traversal-examples) |
| Count, sum, average, or group graph results | [`aggregation`](query-language.md#aggregation) |
| Find a path between two bounded endpoints | [`path_finding`](query-language.md#path-finding) |
| Ask what is connected to one bounded node | [`neighbors`](query-language.md#neighbors) |

To search, use a single-node `traversal` with filters.
There is no separate `search` query type.

## Example: fetch a merge request diff

The `diff` column on `MergeRequest` returns the full unified diff.
Request columns such as `diff` by name. For other diff and source text columns,
see [properties resolved at query time](../schema.md#properties-resolved-at-query-time).

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state", "diff"]
  }],
  "limit": 1
}
```

## Example: fetch the changed files of a merge request

`HAS_DIFF` goes from a merge request to its diff snapshots.
`HAS_FILE` goes from a snapshot to its files.

```json orbit-query
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
      "columns": ["id", "state"]
    },
    {
      "id": "file",
      "entity": "MergeRequestDiffFile",
      "columns": ["new_path", "old_path", "too_large", "diff"]
    }
  ],
  "relationships": [
    {"type": "HAS_DIFF", "from": "mr", "to": "snapshot"},
    {"type": "HAS_FILE", "from": "snapshot", "to": "file"}
  ],
  "limit": 20
}
```

When `too_large` is `true`, `diff` is `null`.

## Example: fetch source file content

This query finds indexed files by path and returns the raw file text.

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "file",
    "entity": "File",
    "filters": {
      "path": {"ends_with": "app/models/project.rb"}
    },
    "columns": ["path", "language", "content"]
  }],
  "limit": 5
}
```

## Related topics

- [GitLab Orbit query language](query-language.md)
- [REST API](api.md)
- [Use cases](../use-cases.md)
- [What GitLab Orbit indexes](../schema.md)
