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

A query asks the graph one exact question and returns exact rows, with only the data that you can access.
Your agent writes queries for you. Write them yourself for scripts, scheduled reports, and custom tools.
Queries use the GitLab Orbit [query language](query-language.md).

## Run a query

Pass the query text to the [GitLab Orbit CLI](../cli.md):

```shell
glab orbit query "MATCH (p:Project {full_path: '<my-group/my-project>'}) RETURN p.name, p.full_path LIMIT 1"
```

The CLI prints compact [TOON](https://github.com/toon-format/spec/blob/main/SPEC.md) text. For JSON, add `--response-format raw`.
Agents on the [Model Context Protocol (MCP) endpoint](../agents/mcp.md) send the same text with the `query_graph` tool.

## Use the GitLab Orbit API

From a script, a scheduled report, or a custom tool, send the same query text to the GitLab REST API.
Authenticate with a personal access token, or with a [service account](../security.md#service-accounts)
for automation without a personal account.
Results include only the data that the token owner can access.

- For the endpoints, parameters, and examples, see the [GitLab Orbit API](https://docs.gitlab.com/api/orbit/).
- For GitLab Orbit data in the GraphQL API, such as the groups where GitLab Orbit is turned on and the schema,
  see [`Query.orbit`](https://docs.gitlab.com/api/graphql/reference/queries/#orbit).

## Query shapes

| Use case | Query shape |
|----------|-------------|
| Fetch nodes of one type | `MATCH (n:Label {property: value}) RETURN n` |
| Follow relationships between node types | `MATCH (a:Label {id: 1})-[:RELATIONSHIP]->(b:Label) RETURN a, b` |
| Count, sum, or average results by group key | `RETURN key, count(n) AS total ORDER BY total DESC` |
| Find a path between two bounded nodes | `MATCH path = ANY SHORTEST (a:Label {id: 1})-[:RELATIONSHIP*1..3]->(b:Label {id: 2}) RETURN path` |
| Find what is connected to one bounded node | `MATCH (a:Label {id: 1})--(n) RETURN n` |

To search, filter one node type with `WHERE`, for example with `CONTAINS` or `all_tokens`.
For the syntax, see [filters](query-language.md#filters).

Every query needs at least one node bounded by an ID or a literal property filter.
The query shapes use numeric IDs. To find the ID of a merge request or a project, see
[bounded nodes](query-language.md#bounded-nodes).

To read source text or diffs, return a property that GitLab Orbit fetches from the repository at query time,
such as `File.content`, `Definition.content`, or `MergeRequest.diff`.
For the full list, see [properties resolved at query time](../schema.md#properties-resolved-at-query-time).

## Related topics

- [Prompt examples](../prompt-examples.md)
- [Indexed data](../schema.md)
