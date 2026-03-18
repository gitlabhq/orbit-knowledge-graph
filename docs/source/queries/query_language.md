---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
title: Query language fields
---

{{< details >}}

- Tier: Ultimate
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

Queries support the following fields:

| Field                                   | Required     | Type      |
|-----------------------------------------|--------------|-----------|
| [`query_type`](#query_type)             | {{< yes >}}  | `string`  |
| [`node`](#node) <sup>1</sup>            | {{< yes >}}  | `object`  |
| [`nodes`](#nodes) <sup>1</sup>          | {{< yes >}}  | `array`   |
| [`relationships`](#relationships)       | {{< no >}}   | `array`   |
| [`aggregations`](#aggregations)         | {{< no >}}   | `array`   |
| [`path`](#path)                         | {{< no >}}   | `object`  |
| [`neighbors`](#neighbors)               | {{< no >}}   | `object`  |
| [`limit`](#limit)                       | {{< no >}}   | `integer` |
| [`order_by`](#order_by)                 | {{< no >}}   | `object`  |
| [`aggregation_sort`](#aggregation_sort) | {{< no >}}   | `object`  |
| [`options`](#options)                   | {{< no >}}   | `object`  |

**Footnotes**:

1. You cannot specify both `node` and `nodes` in the same query.

## `query_type`

The type of query to run:

| Query type    | Description                                                   |
|---------------|---------------------------------------------------------------|
| `search`      | Find nodes that match filters.                                |
| `traversal`   | Start from one or more nodes and follow relationships.        |
| `aggregation` | Search nodes and group the results. |
| `path_finding`| Find paths between nodes.                                     |
| `neighbors`   | Find nodes directly connected to a starting node.             |

Example:

Find all users who have authored open merge requests and return up to 10 results.

```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User"},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"}
  ],
  "limit": 10
}
```

## `node`

A single node selector. Use with `search` and `neighbors` queries.

A node selector specifies which graph nodes to match:

| Field         | Required    | Type             | Description                                                                                                                        |
|---------------|-------------|------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `id`          | {{< yes >}} | `string`         | Variable identifier for this node. Use `id` to reference a node in the `relationships` and `aggregations` fields.                 |
| `entity`      | {{< no >}}  | `string`         | Node type to match. For example, `User`, `Project`, `MergeRequest`.                                                               |
| `columns`     | {{< no >}}  | `string`/`array` | Properties to return. Use `"*"` for all columns, or an array of property names. Default: `id`.                                    |
| `filters`     | {{< no >}}  | `object`         | Property conditions a node must satisfy.                                                                                           |
| `node_ids`    | {{< no >}}  | `array`          | Match only nodes with these IDs. Maximum 500 IDs.                                                                                  |
| `id_range`    | {{< no >}}  | `object`         | Match nodes within an inclusive ID range, using `start` and `end`.                                                                |
| `id_property` | {{< no >}}  | `string`         | The property to use as the node identifier. Default: `id`.                                                                        |

Example:

Search for users whose username starts with `admin` and return their username and email.

```json
{
  "query_type": "search",
  "node": {
    "id": "u",
    "entity": "User",
    "columns": ["username", "email"],
    "filters": {
      "username": {"op": "starts_with", "value": "admin"}
    }
  },
  "limit": 10
}
```

## `nodes`

An array of node selectors. Use with `traversal`, `aggregation`, and `path_finding` queries.

Each node selector uses the same fields as [`node`](#node).

Example:

Find all users who have authored merged merge requests and return up to 25 results.

```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User"},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"}
  ],
  "limit": 25
}
```

## `relationships`

An array of relationships that connect node types.

| Field       | Required    | Type             | Description                                                                                      |
|-------------|-------------|------------------|--------------------------------------------------------------------------------------------------|
| `type`      | {{< yes >}} | `string`/`array` | Relationship type or types to traverse. For example, `AUTHORED`. Validated against the ontology. |
| `from`      | {{< yes >}} | `string`         | The `id` of the source node selector.                                                            |
| `to`        | {{< yes >}} | `string`         | The `id` of the target node selector.                                                            |
| `direction` | {{< no >}}  | `string`         | `outgoing` (default), `incoming`, or `both`.                                                     |
| `min_hops`  | {{< no >}}  | `integer`        | Minimum hops to traverse. Range: `0`-`3`. Default: `1`.                                              |
| `max_hops`  | {{< no >}}  | `integer`        | Maximum hops to traverse. Range: `1`-`3`. Default: `1`.                                              |
| `filters`   | {{< no >}}  | `object`         | Conditions on relationship properties. Uses the same syntax as node filters.                     |

Example:

Retrieve merge requests and their authors. Return up to 25 results.

```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User"},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr", "direction": "outgoing"}
  ],
  "limit": 25
}
```

## `aggregations`

An array of aggregation specifications. Required when `query_type` is `aggregation`.

| Field      | Required    | Type      | Description                                                                        |
|------------|-------------|-----------|------------------------------------------------------------------------------------|
| `function` | {{< yes >}} | `string`  | The aggregation function. One of: `count`, `sum`, `avg`, `min`, `max`, `collect`.  |
| `target`   | {{< yes >}} | `string`  | The `id` of the node selector to aggregate over.                                   |
| `group_by` | {{< no >}}  | `string`  | The `id` of the node selector to group results by.                                 |
| `property` | {{< no >}}  | `string`  | The property to aggregate. Required for `sum`, `avg`, `min`, `max`, and `collect`. |
| `alias`    | {{< no >}}  | `string`  | A name for the aggregation result in the response.                                 |

Example:

Count the number of merge requests authored by each user and return up to 10 results in descending order.

```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "mr", "entity": "MergeRequest"},
    {"id": "u", "entity": "User"}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"}
  ],
  "aggregations": [
    {"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}
  ],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 10
}
```

## `path`

Path finding configuration. Required when `query_type` is `path_finding`.

| Field       | Required    | Type      | Description                                                           |
|-------------|-------------|-----------|-----------------------------------------------------------------------|
| `type`      | {{< yes >}} | `string`  | Path type. One of: `shortest`, `all_shortest`, `any`.                 |
| `from`      | {{< yes >}} | `string`  | The `id` of the start node selector.                                  |
| `to`        | {{< yes >}} | `string`  | The `id` of the end node selector.                                    |
| `max_depth` | {{< no >}}  | `integer` | Maximum path depth. Range: `1`-`3`.                                       |
| `rel_types` | {{< no >}}  | `array`   | Relationship types to traverse. If omitted, all types are considered. |

Supported path types:

- `shortest`: Find the single shortest path.
- `all_shortest`: Find all paths of minimum length.
- `any`: Find any valid path.

Example:

Find the shortest path between project ID 100 and project ID 200 by following only `CONTAINS` relationships.

```json
{
  "query_type": "path_finding",
  "nodes": [
    {"id": "start", "entity": "Project", "node_ids": [100]},
    {"id": "end", "entity": "Project", "node_ids": [200]}
  ],
  "path": {
    "type": "shortest",
    "from": "start",
    "to": "end",
    "max_depth": 3,
    "rel_types": ["CONTAINS"]
  }
}
```

## `neighbors`

Neighbors configuration. Required when `query_type` is `neighbors`.

| Field       | Required    | Type     | Description                                                           |
|-------------|-------------|----------|-----------------------------------------------------------------------|
| `node`      | {{< yes >}} | `string` | The `id` of the node selector to find neighbors for.                  |
| `direction` | {{< no >}}  | `string` | `outgoing`, `incoming`, or `both` (default).                          |
| `rel_types` | {{< no >}}  | `array`  | Relationship types to traverse. If omitted, all types are considered. |

The response includes each neighbor's ID, entity type, and the relationship that connects it to the source node.

Example:

Retrieve all nodes directly connected to user ID 100 by `AUTHORED` or `MEMBER_OF` relationships. Return up to 20 results.

```json
{
  "query_type": "neighbors",
  "node": {"id": "u", "entity": "User", "node_ids": [100]},
  "neighbors": {
    "node": "u",
    "direction": "both",
    "rel_types": ["AUTHORED", "MEMBER_OF"]
  },
  "limit": 20
}
```

## `limit`

The maximum number of results to return. Range is `1`-`1000`. Default is `30`.

Example:

Search for all projects in the graph and return the name and full path of up to 100 results.

```json
{
  "query_type": "search",
  "node": {
    "id": "p",
    "entity": "Project",
    "columns": ["name", "full_path"]
  },
  "limit": 100
}
```

## `order_by`

Result ordering for `search` and `traversal` queries.

| Field       | Required    | Type     | Description                               |
|-------------|-------------|----------|-------------------------------------------|
| `node`      | {{< yes >}} | `string` | The `id` of the node selector to sort by. |
| `property`  | {{< yes >}} | `string` | The property to sort by.                  |
| `direction` | {{< no >}}  | `string` | `ASC` or `DESC`. Default: `ASC`.          |

Example:

Retrieve merge requests and their authors. Return results sorted by `updated_at` in descending order.

```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User"},
    {"id": "mr", "entity": "MergeRequest"}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"}
  ],
  "order_by": {"node": "mr", "property": "updated_at", "direction": "DESC"},
  "limit": 25
}
```

## `aggregation_sort`

Result ordering for `aggregation` queries.

| Field       | Required    | Type      | Description                                                                     |
|-------------|-------------|-----------|---------------------------------------------------------------------------------|
| `agg_index` | {{< yes >}} | `integer` | The zero-based index of the aggregation in the `aggregations` array to sort by. |
| `direction` | {{< no >}}  | `string`  | `ASC` or `DESC`. Default: `ASC`.                                                |

Example:

Count vulnerabilities in each project and return results in descending order.

```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "v", "entity": "Vulnerability"},
    {"id": "p", "entity": "Project"}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "aggregations": [
    {"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}
  ],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 10
}
```

## `options`

Presentation preferences that do not affect query semantics.

| Field             | Required   | Type     | Description                                                                                                                                                                                          |
|-------------------|------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dynamic_columns` | {{< no >}} | `string` | Columns fetched for dynamically discovered entities in `path_finding` and `neighbors` queries. `"default"` returns each entity's default columns. `"*"` returns all columns. Default: `"default"`.  |

`dynamic_columns` has no effect on `search` and `traversal` queries, where column selection is controlled through the `columns` field.

Example:

Find the shortest path between user ID 1 and project ID 100 across all relationship types. Return all available columns for every node discovered along the path.

```json
{
  "query_type": "path_finding",
  "nodes": [
    {"id": "start", "entity": "User", "node_ids": [1]},
    {"id": "end", "entity": "Project", "node_ids": [100]}
  ],
  "path": {
    "type": "shortest",
    "from": "start",
    "to": "end",
    "max_depth": 3
  },
  "options": {"dynamic_columns": "*"}
}
```
