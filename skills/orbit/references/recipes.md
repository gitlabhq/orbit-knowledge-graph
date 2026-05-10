# Orbit query recipes

Paste-ready request bodies for each `query_type`. All examples omit
`response_format`; the CLI defaults to `llm` (compact, agent-friendly). Pass
`--format raw` when piping into `jq`.

Every recipe assumes `glab auth login` has succeeded and the
`knowledge_graph` feature flag is on for your user. See
[`SKILL.md`](../SKILL.md) for prerequisites.

The shell pattern is always:

```bash
glab orbit remote query /tmp/q.json
# or:
cat /tmp/q.json | glab orbit remote query -
# or, for jq pipelines:
glab orbit remote query --format raw /tmp/q.json | jq '.'
```

For the full field reference see [`query_language.md`](query_language.md).

## Look up a GitLab project's numeric ID

Many filters (e.g. `project_id` on MergeRequest) need the numeric project ID.
Get it from the URL-encoded full path (`/` → `%2F`):

```bash
glab api "projects/gitlab-org%2Forbit%2Fknowledge-graph" | jq '.id'
```

`glab api` does not support `--jq`. Always pipe to `jq`.

## Look up a merge request by IID

The most common entry point: "tell me about MR !1216 in project X".
Requires both `iid` and `project_id` filters (IID is only unique within a project):

```json
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {"id": "mr", "entity": "MergeRequest", "columns": "*",
       "filters": {"iid": {"op": "eq", "value": 1216},
                   "project_id": {"op": "eq", "value": 77960826}}},
      {"id": "author", "entity": "User", "columns": ["username"]}
    ],
    "relationships": [
      {"type": "AUTHORED", "from": "author", "to": "mr"}
    ],
    "limit": 1
  }
}
```

## `traversal` (single-node) — find nodes matching filters

Find up to 5 projects whose `full_path` contains `gitlab-org/cli`:

```json
{
  "query": {
    "query_type": "traversal",
    "node": {
      "id": "p",
      "entity": "Project",
      "columns": ["full_path", "name", "visibility_level"],
      "filters": {
        "full_path": {"op": "contains", "value": "gitlab-org/cli"}
      }
    },
    "limit": 5
  }
}
```

## `traversal` (multi-node) — start from nodes, follow relationships

List opened merge requests and their authors. Requires at least two nodes and
one relationship:

```json
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {"id": "u",  "entity": "User"},
      {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
    ],
    "relationships": [
      {"type": "AUTHORED", "from": "u", "to": "mr"}
    ],
    "limit": 10
  }
}
```

## `order_by` — sort traversal results

Add `order_by` to any traversal. Fields are `node` (the node `id`), `property`,
and `direction` (`ASC` or `DESC`):

```json
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {"id": "u",  "entity": "User", "filters": {"username": {"op": "eq", "value": "alice"}}},
      {"id": "mr", "entity": "MergeRequest", "columns": ["title", "state", "created_at"]}
    ],
    "relationships": [
      {"type": "AUTHORED", "from": "u", "to": "mr"}
    ],
    "order_by": {"node": "mr", "property": "created_at", "direction": "DESC"},
    "limit": 10
  }
}
```

## `neighbors` — nodes directly connected to a starting node

Find the immediate outgoing neighbours of the `gitlab-org/cli` project:

```json
{
  "query": {
    "query_type": "neighbors",
    "node": {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": {"op": "eq", "value": "gitlab-org/cli"}}
    },
    "neighbors": {"node": "p", "direction": "outgoing"},
    "limit": 20
  }
}
```

## `aggregation` — group and count

Count open merge requests per project, highest first:

```json
{
  "query": {
    "query_type": "aggregation",
    "nodes": [
      {"id": "p",  "entity": "Project"},
      {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
    ],
    "relationships": [
      {"type": "IN_PROJECT", "from": "mr", "to": "p"}
    ],
    "group_by": [{"kind": "node", "node": "p"}],
    "aggregations": [
      {"function": "count", "target": "mr", "alias": "open_mrs"}
    ],
    "aggregation_sort": {"column": "open_mrs", "direction": "DESC"},
    "limit": 10
  }
}
```

Count detected vulnerabilities by severity:

```json
{
  "query": {
    "query_type": "aggregation",
    "nodes": [
      {"id": "v", "entity": "Vulnerability", "filters": {"state": "detected"}}
    ],
    "group_by": [
      {"kind": "property", "node": "v", "property": "severity", "alias": "severity"}
    ],
    "aggregations": [
      {"function": "count", "target": "v", "alias": "vuln_count"}
    ],
    "aggregation_sort": {"column": "vuln_count", "direction": "DESC"},
    "limit": 10
  }
}
```

## `path_finding` — shortest path between nodes

Shortest path between two projects (`max_depth` ≤ 3, server-enforced):

```json
{
  "query": {
    "query_type": "path_finding",
    "nodes": [
      {"id": "from", "entity": "Project", "filters": {"full_path": {"op": "eq", "value": "gitlab-org/cli"}}},
      {"id": "to",   "entity": "Project", "filters": {"full_path": {"op": "eq", "value": "gitlab-org/gitlab"}}}
    ],
    "path": {"type": "shortest", "from": "from", "to": "to", "max_depth": 3}
  }
}
```

## Filter operators

The `filters` object supports simple equality (`{"state": "opened"}`) or a
structured `PropertyFilter`:

```json
{"filters": {"<property>": {"op": "<operator>", "value": <value>}}}
```

| Operator                                   | Value type                   | Notes                             |
|--------------------------------------------|------------------------------|-----------------------------------|
| `eq`, `gt`, `lt`, `gte`, `lte`             | string / number / boolean    | comparison                        |
| `in`                                       | array (1–100 items)          | membership                        |
| `contains`, `starts_with`, `ends_with`     | string (≤ 1024 chars)        | string ops                        |
| `is_null`, `is_not_null`                   | *(omit `value`)*             | null checks                       |

## Pagination

Add a `cursor`. `offset + page_size` must not exceed `limit`. `page_size` max 100.

```json
{
  "query": {
    "query_type": "traversal",
    "node": {"id": "p", "entity": "Project"},
    "limit": 200,
    "cursor": {"offset": 0, "page_size": 50}
  }
}
```

Increment `offset` by `page_size` for subsequent pages.

## More examples

Production-grade query examples — more complex traversals and aggregations —
live in [`fixtures/queries/sdlc_queries.yaml`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/fixtures/queries/sdlc_queries.yaml)
in the `gitlab-org/orbit/knowledge-graph` repo. Treat those as the source of
truth for idiomatic queries.
