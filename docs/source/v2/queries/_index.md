---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the GitLab Orbit graph with GQL to find GitLab data, code, and relationships from scripts and custom tools.
title: GitLab Orbit queries
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Feature flag `knowledge_graph` removed.

{{< /history >}}

A query asks the graph one exact question and returns exact rows, with only the data that you can access.
Your agent writes queries for you. Write them yourself for scripts, scheduled reports, and custom tools.
Queries use the GitLab Orbit [query language](query-language.md).

{{< cards >}}

- [Query language](query-language.md)
- [REST API](api.md)

{{< /cards >}}

## Run a query

Pass the query text to the [GitLab Orbit CLI](../cli.md):

```shell
glab orbit query "MATCH (p:Project {full_path: '<my-group/my-project>'}) RETURN p.name, p.full_path LIMIT 1"
```

The CLI prints compact [TOON](https://github.com/toon-format/spec/blob/main/SPEC.md) text. For JSON, add `--response-format raw`.
From a script, use the [query endpoint](api.md#query-endpoint). Agents on the [MCP endpoint](../agents/mcp.md)
send the same text with the `query_graph` command.

## Choose a query shape

| Use case | Query shape |
|----------|-------------|
| Fetch nodes of one type | `MATCH (n:Label {property: value}) RETURN n` |
| Follow relationships between node types | `MATCH (a:Label)-[:RELATIONSHIP]->(b:Label) RETURN a, b` |
| Count, sum, or average results by group | `RETURN key, count(n) AS total ORDER BY total DESC` |
| Find a path between two bounded nodes | `MATCH path = ANY SHORTEST (a:Label {id: 1})-[:RELATIONSHIP*1..3]->(b:Label {id: 2}) RETURN path` |
| Find what is connected to one bounded node | `MATCH (a:Label {id: 1})--(n) RETURN n` |

To search, filter one node type with `WHERE`, for example with `CONTAINS` or `all_tokens`.
For the syntax, see the [query language](query-language.md).

The examples use numeric IDs. To find the ID of a merge request or a project, see
[anchor at least one node](query-language.md#anchor-at-least-one-node).

## Examples

### Fetch a merge request diff

The `diff` property of `MergeRequest` returns the full unified diff.
GitLab Orbit fetches it from the repository when the query asks for it.
For other properties like this, see [properties resolved at query time](../schema.md#properties-resolved-at-query-time).

```gql orbit-query
MATCH (mr:MergeRequest {id: 12345})
RETURN mr.iid, mr.title, mr.state, mr.diff
LIMIT 1
```

### Fetch the changed files of a merge request

`HAS_LATEST_DIFF` goes from a merge request to its current diff version.
`HAS_FILE` goes from a diff version to its files.

```gql orbit-query
MATCH (mr:MergeRequest {id: 12345})-[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)-[:HAS_FILE]->(f:MergeRequestDiffFile)
RETURN f.old_path, f.new_path, f.too_large, f.diff
LIMIT 20
```

When `too_large` is `true`, `diff` is `null`.

### Fetch source file content

This query finds indexed files by path and returns the file text.

```gql orbit-query
MATCH (f:File {project_id: 278964})
WHERE f.path ENDS WITH 'app/models/project.rb'
RETURN f.path, f.language, f.content
LIMIT 5
```

## Related topics

- [Query language](query-language.md)
- [REST API](api.md)
- [GitLab Orbit API reference](https://docs.gitlab.com/api/orbit/)
- [GraphQL API: `Query.orbit`](https://docs.gitlab.com/api/graphql/reference/#queryorbit)
- [Prompt examples](../prompt-examples.md)
- [Indexed data](../schema.md)
