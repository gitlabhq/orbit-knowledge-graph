---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use the GitLab Orbit query language to search and traverse the knowledge graph.
title: GitLab Orbit query language
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

Use the GitLab Orbit query language when you need GitLab data as a graph instead of a
flat API response. A query is a JSON object. It names the entities to match,
the relationships to follow, and the properties to return.

## Request envelope

When submitting a query via the REST API or `glab orbit remote query`, wrap the
query object in a top-level `query` field:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "node": {
      "id": "mr",
      "entity": "MergeRequest",
      "node_ids": [12345],
      "columns": ["iid", "title", "state"]
    },
    "limit": 1
  },
  "response_format": "raw"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `query` | Yes | The query object documented below. |
| `response_format` | No | `"llm"` (default when omitted; compact [GOON](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/docs/design-documents/querying/graph_engine.md) text optimized for LLM consumption) or `"raw"` (structured JSON). Use `"raw"` when piping output into `jq`. |

The `orbit query` CLI (for local graphs) takes the raw query body **without**
the envelope.

## Query shape

Every query has a `query_type` and either `node` or `nodes`.

```json orbit-query
{
  "query_type": "traversal",
  "node": {
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state"]
  },
  "limit": 1
}
```

Use `node` for one node selector. Use `nodes` for an array of selectors. You
cannot use both in the same query.

## Query types

| Query type | Use it to |
|------------|-----------|
| `traversal` | Fetch matching nodes or follow relationships between nodes. |
| `aggregation` | Count, sum, average, group, or sort matching graph results. |
| `path_finding` | Find a bounded path between two node selectors. |
| `neighbors` | Return nodes connected to one bounded node. |

Single-node `traversal` is the search shape. There is no separate `search`
query type.

## Top-level fields

| Field | Type | Description |
|-------|------|-------------|
| `query_type` | `string` | One of `traversal`, `aggregation`, `path_finding`, or `neighbors`. |
| `node` | `object` | One node selector. Required for single-node `traversal` and `neighbors`. |
| `nodes` | `array` | Multiple node selectors. Required for multi-node `traversal`, `aggregation`, and `path_finding`. Maximum 5. |
| `relationships` | `array` | Relationship selectors for traversal or aggregation. Maximum 5. |
| `aggregations` | `array` | Aggregation definitions. Required for `aggregation`. Maximum 10. |
| `group_by` | `array` | Group keys for aggregation rows. Maximum 4. |
| `path` | `object` | Path finding configuration. Required for `path_finding`. |
| `neighbors` | `object` | Neighbor lookup configuration. Required for `neighbors`. |
| `limit` | `integer` | Maximum rows to return when no `cursor` is set. Default 30. Maximum 1000. Check `pagination.truncated` in the response: when true, more matching rows exist. |
| `cursor` | `object` | Keyset pagination: `{"page_size": N}` for the first page, then `{"page_size": N, "after": "<pagination.next_cursor>"}` until `next_cursor` is absent. Reaches every row regardless of dataset size. The token is bound to the exact query that issued it. |
| `order_by` | `object` | Sort rows by a node property. |
| `aggregation_sort` | `object` | Sort aggregation rows by output column. |
| `options` | `object` | Presentation and debug options. |

Pagination reads live data at request time; there is no snapshot. Each page
independently resolves the latest version of every row and filters out
soft-deleted rows, so version churn and tombstone cleanup between pages do not
skip or duplicate results. Rows inserted after the cursor position in sort
order appear on later pages; rows inserted or reordered behind it are not
revisited. Rows whose sort key is NULL sort last and paginate like any other
row. A row whose sort key changes between pages can appear twice or not
at all, the same as any keyset pagination without a snapshot.

## Node selectors

A node selector names one entity type in the ontology.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Local alias for the node. Relationships, aggregations, path, and neighbors refer to this alias. |
| `entity` | `string` | Ontology node type, such as `Project`, `User`, `MergeRequest`, `File`, or `Definition`. |
| `columns` | `string` or `array` | Properties to return. Use `"*"` for all non-restricted properties or an array of names. If omitted, Orbit returns the entity's default columns. |
| `filters` | `object` | Property filters. |
| `node_ids` | `array` | Exact IDs to match. Accepts integers or digit strings. Maximum 500. |
| `id_range` | `object` | Inclusive ID range with `start` and `end`. |
| `id_property` | `string` | Property used by `node_ids` and `id_range`. Default `id`. |

Use `node_ids` when you already know the graph ID. Use `filters` when you know a
natural property such as `username`, `full_path`, `state`, or `path`.

## Relationships

Relationships connect node selectors by alias.

```json
{
  "type": "AUTHORED",
  "from": "user",
  "to": "mr",
  "direction": "outgoing"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` or `array` | Relationship type or types. Use `"*"` only when you need any relationship and have a bounded query. |
| `from` | `string` | Alias of the start node selector. |
| `to` | `string` | Alias of the end node selector. |
| `direction` | `string` | `outgoing`, `incoming`, or `both`. Default `outgoing`. |
| `min_hops` | `integer` | Minimum hops. Default 1. Maximum 3. |
| `max_hops` | `integer` | Maximum hops. Default 1. Maximum 3. |
| `filters` | `object` | Relationship property filters. Maximum 5 filters. |

For example, merge requests point to projects with `IN_PROJECT`, and users point
to merge requests with `AUTHORED`.

## Filters

Filters can use simple equality:

```json
{
  "filters": {
    "state": "merged"
  }
}
```

Or they can use an operator:

```json
{
  "filters": {
    "created_at": {"op": "gte", "value": "2026-01-01"},
    "state": {"op": "in", "value": ["opened", "merged"]}
  }
}
```

| Operator | Use |
|----------|-----|
| `eq` | Equal to a scalar value. |
| `gt`, `gte`, `lt`, `lte` | Numeric, date, or timestamp comparison. |
| `in` | Value is in an array. Maximum 100 values. |
| `contains` | String contains a substring. |
| `starts_with` | String starts with a prefix. |
| `ends_with` | String ends with a suffix. |
| `is_null` | Value is null. Do not provide `value`. |
| `is_not_null` | Value is not null. Do not provide `value`. |
| `token_match` | Text index contains one token. |
| `all_tokens` | Text index contains all tokens. |
| `any_tokens` | Text index contains any token. |

Token operators work only on properties with text indexes.

### Text-indexed properties

The following properties support `token_match`, `all_tokens`, and `any_tokens`.
Using these operators on other properties falls back to a full string scan, which is slower.

<!-- The table below is generated from the ontology's `text(...)` storage indexes. -->
<!-- Do not edit it by hand: run `mise run docs:query-language` and commit. CI fails on drift. -->
<!-- BEGIN GENERATED: text-indexed-properties -->

| Entity | Text-indexed properties |
|--------|------------------------|
| `Branch` | `name` |
| `Definition` | `file_path`, `fqn`, `name` |
| `Deployment` | `ref` |
| `Directory` | `name`, `path` |
| `Environment` | `environment_type`, `name` |
| `File` | `name`, `path` |
| `Finding` | `description`, `name` |
| `Group` | `description`, `name` |
| `ImportedSymbol` | `file_path`, `import_path` |
| `Job` | `name`, `ref` |
| `Label` | `description`, `title` |
| `MergeRequest` | `description`, `source_branch`, `target_branch`, `title` |
| `MergeRequestDiffFile` | `new_path`, `old_path` |
| `Milestone` | `description`, `title` |
| `Note` | `note` |
| `Pipeline` | `ref` |
| `Project` | `description`, `name` |
| `Runner` | `name` |
| `Stage` | `name` |
| `User` | `name`, `username` |
| `Vulnerability` | `description`, `title` |
| `VulnerabilityIdentifier` | `external_id`, `external_type`, `name` |
| `VulnerabilityOccurrence` | `description`, `name` |
| `VulnerabilityScanner` | `external_id`, `name` |
| `WorkItem` | `description`, `title` |

<!-- END GENERATED: text-indexed-properties -->

## Columns and virtual columns

Most columns come from indexed graph tables in ClickHouse. Some columns are
virtual: Orbit fetches them from another service after the graph query returns.

Request virtual columns explicitly in `columns`. The `dynamic_columns` option
used by `path_finding` and `neighbors` excludes virtual columns because they
can require external service calls.

| Entity | Virtual column | What it returns |
|--------|----------------|-----------------|
| `MergeRequest` | `diff` | Full unified diff for the merge request. |
| `MergeRequestDiff` | `patch` | Full patch for one merge request diff snapshot. |
| `MergeRequestDiffFile` | `diff` | Per-file unified diff text. Returns `null` when `too_large` is `true`. |
| `File` | `content` | Raw source text of a file. |
| `Definition` | `content` | Source text for one indexed definition. |

The `content` column is for source code. For merge request diff text, use
`MergeRequest.diff`, `MergeRequestDiff.patch`, or `MergeRequestDiffFile.diff`.

## Traversal examples

Fetch one merge request with its full diff:

```json orbit-query
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

Fetch per-file diff content from diff snapshots:

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
      "columns": ["id", "state", "patch"]
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

`HAS_DIFF` returns every diff snapshot the merge request ever had
(`MergeRequestDiff.merge_request_id` FK). `HAS_LATEST_DIFF` returns
only the most recent snapshot (`MergeRequest.latest_merge_request_diff_id`
FK) — useful for "what does the merge request look like right now", but
not for historical questions. For "every merge request that ever touched
a file", traverse `HAS_DIFF` over all snapshots. Using `HAS_LATEST_DIFF`
for historical-coverage questions can substantially undercount on
long-lived files: an MR that touched the file in an earlier revision
but not in its final diff is invisible through `HAS_LATEST_DIFF`.

`MergeRequestDiffFile.old_path` is the preferred column for file
lookups; `new_path` differs from `old_path` only on renames. Filtering
and grouping by `old_path` keeps the same row identity across an MR's
history. See the ontology field descriptions on
[`merge_request_diff_file.yaml`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/config/ontology/nodes/code_review/merge_request_diff_file.yaml).

Fetch source file content:

```json orbit-query
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

Fetch the source text of a specific function or class definition. The `content`
column returns the raw source text of just that definition, not the full file.
Use `fqn` (fully-qualified name) for an exact match, or `name` with `contains`
for a broader search:

```json orbit-query
{
  "query_type": "traversal",
  "node": {
    "id": "d",
    "entity": "Definition",
    "filters": {
      "fqn": {"op": "eq", "value": "Gitlab::Auth::authenticate"}
    },
    "columns": ["name", "fqn", "file_path", "start_line", "end_line", "content"]
  },
  "limit": 5
}
```

Find merged merge requests in a project:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "project",
      "entity": "Project",
      "filters": {"full_path": "your-group/your-project"},
      "columns": ["name", "full_path"]
    },
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"},
      "columns": ["iid", "title", "state", "merged_at"]
    }
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "project"}
  ],
  "limit": 25
}
```

Find every pipeline that ran for one merge request. Always filter
`Pipeline.source = "merge_request_event"` to match what the merge request's
**Pipelines** tab shows:

```json orbit-query
{
  "query_type": "traversal",
  "node": {
    "id": "p",
    "entity": "Pipeline",
    "filters": {
      "merge_request_id": {"op": "eq", "value": 482908721},
      "source": {"op": "eq", "value": "merge_request_event"}
    },
    "columns": ["id", "status", "source", "sha", "ref", "created_at"]
  },
  "order_by": "-p.created_at",
  "limit": 100
}
```

`merge_request_id` is the merge request's internal numeric `id`, not the
project-scoped `iid`. Look it up first with a `MergeRequest` traversal that
filters by `iid` and `project_id`, then plug the `id` into the query above.

Both `Pipeline.merge_request_id` and the `MergeRequest --TRIGGERED-->
Pipeline` edge link an MR to every CI pipeline spawned in its context,
including the downstream child pipelines (`source = "parent_pipeline"`)
that the top-level MR pipelines trigger. Without the
`source = "merge_request_event"` filter, the result over-counts by a large
factor on any MR that uses parent-child pipeline fan-out, and does not
match what the MR **Pipelines** tab shows. Apply the same filter when traversing
`MergeRequest --TRIGGERED--> Pipeline` in a multi-node query.

`MergeRequest --HAS_HEAD_PIPELINE--> Pipeline` is a different edge. It
points to the single most recent pipeline running against the tip of the
merge request's source branch. Use it for "what is currently running", not
for pipeline history.

## Aggregation

Aggregation queries use `aggregations`.

| Field | Type | Description |
|-------|------|-------------|
| `function` | `string` | `count`, `sum`, `avg`, `min`, or `max`. |
| `target` | `string` | Node alias to aggregate. |
| `property` | `string` | Property to aggregate. Required for `sum`, `avg`, `min`, and `max`. |
| `alias` | `string` | Name of the output column. |

Property type support depends on the function:

| Function | Requires `property` | Supported property types |
|----------|---------------------|--------------------------|
| `count` | No | N/A |
| `sum` | Yes | Numeric only |
| `avg` | Yes | Numeric only |
| `min` | Yes | Numeric, string, boolean, `Date`, or `DateTime` |
| `max` | Yes | Numeric, string, boolean, `Date`, or `DateTime` |

`sum` and `avg` reject `DateTime` properties with a validation error. To
aggregate over dates, use `min` or `max`.

Use top-level `group_by` to group aggregation rows. It applies to every
aggregation in the query. Do not put grouping inside an individual aggregation.

Group keys support these shapes:

| Group key | Shape | Result value |
|-----------|-------|--------------|
| Node | `{"kind": "node", "node": "<node-id>", "alias": "<optional-name>"}` | A nested entity object in each row. |
| Property | `{"kind": "property", "node": "<node-id>", "property": "<property>", "alias": "<optional-name>"}` | A scalar bucket value in each row. |

If you omit `alias`, node groups use the node ID as the output key. Property
groups use the property name when it is unique in the `group_by` list, or
`<node>_<property>` when needed to avoid ambiguity. Duplicate group or aggregate
output names are rejected.

Property groups must reference a real ClickHouse-backed, filterable property
that the caller is allowed to use. Virtual fields and unfilterable fields are
rejected during validation.

Count merged merge requests per project:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "project",
      "entity": "Project",
      "filters": {"full_path": "your-group/your-project"}
    },
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"}
    }
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "project"}
  ],
  "group_by": [{"kind": "node", "node": "project"}],
  "aggregations": [
    {"function": "count", "target": "mr", "alias": "merged_mrs"}
  ],
  "aggregation_sort": {"column": "merged_mrs", "direction": "DESC"},
  "limit": 10
}
```

Count detected vulnerabilities by severity:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    }
  ],
  "group_by": [
    {"kind": "property", "node": "v", "property": "severity", "alias": "severity"}
  ],
  "aggregations": [
    {"function": "count", "target": "v", "alias": "vulnerability_count"}
  ],
  "aggregation_sort": {"column": "vulnerability_count", "direction": "DESC"},
  "limit": 10
}
```

Aggregation responses are table-shaped. `columns` describes computed aggregate
values, `group_columns` describes grouping keys, and `rows` carries group values
plus metric values. Node-grouped rows store the grouped entity under the group
key. Property-grouped rows store the scalar bucket under the group key.

`collect` is listed in the input type but currently rejected by validation.

## Path finding

Path finding queries use `path`.

| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` | `shortest`. |
| `from` | `string` | Alias of the start node selector. |
| `to` | `string` | Alias of the end node selector. |
| `max_depth` | `integer` | Maximum path length. Maximum 3. |
| `rel_types` | `array` | Relationship types to traverse. Required unless both endpoints use `node_ids`. |

Both endpoints must be bounded by `node_ids`, filters, or an `id_range` with a
span of 500 or less. If either endpoint uses filters or `id_range`, provide
`rel_types`.

```json orbit-query
{
  "query_type": "path_finding",
  "nodes": [
    {"id": "start", "entity": "Project", "node_ids": [278964]},
    {"id": "end", "entity": "User", "node_ids": [1]}
  ],
  "path": {
    "type": "shortest",
    "from": "start",
    "to": "end",
    "max_depth": 3,
    "rel_types": ["CREATOR", "AUTHORED", "IN_PROJECT"]
  },
  "limit": 5
}
```

## Neighbors

Neighbor queries use one `node` selector and a `neighbors` object. The center
node must be bounded by `node_ids`, filters, or a narrow `id_range`.

```json orbit-query
{
  "query_type": "neighbors",
  "node": {
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345]
  },
  "neighbors": {
    "node": "mr",
    "direction": "both",
    "rel_types": ["AUTHORED", "IN_PROJECT", "HAS_DIFF"]
  },
  "options": {
    "dynamic_columns": "default"
  },
  "limit": 25
}
```

Set `options.dynamic_columns` to `"*"` if you need all non-restricted
ClickHouse-backed columns for dynamically discovered neighbor or path nodes.
Virtual columns still require an explicit request in a traversal query.

## Validation limits

Orbit rejects broad or ambiguous queries before compiling SQL.

| Limit | Value |
|-------|-------|
| Nodes per query | 5 |
| Relationships per query | 5 |
| Aggregations per query | 10 |
| `node_ids` per selector | 500 |
| Values in an `in` filter | 100 |
| Columns per node selector | 50 |
| Relationship types per selector | 10 |
| Relationship hops | 3 |
| Path depth | 3 |
| Filters per node | 10 |
| Filters per relationship | 5 |

Traversal and aggregation queries must include at least one selective node:
`node_ids`, filters, or an `id_range` with a span of 100,000 or less.

Single-node traversal also requires selectivity. To inspect a broad entity, add
a filter, provide IDs, or use a narrow `id_range`.

## Options

| Option | Description |
|--------|-------------|
| `dynamic_columns` | For `path_finding` and `neighbors` hydration. Use `default` for each entity's default columns, or `"*"` for all non-restricted ClickHouse-backed columns. Default `default`. |
| `include_debug_sql` | Include compiled ClickHouse SQL in response metadata when the caller is allowed to see it. |
