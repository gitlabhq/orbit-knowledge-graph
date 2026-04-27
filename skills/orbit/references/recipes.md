# Orbit query recipes

Paste-ready `glab api` invocations for each `query_type`. All use
`response_format: "llm"` — switch to `"raw"` when piping into `jq`.

Every recipe assumes `glab auth login` has succeeded and the `knowledge_graph`
feature flag is on for your user. See [`SKILL.md`](../SKILL.md) for prerequisites.

The shell pattern is always:

```bash
glab api --method POST orbit/query \
  --header "Content-Type: application/json" \
  --input /tmp/q.json
```

Or use the bundled wrapper (injects the header). Use the absolute path — the
skill can be installed anywhere, so relative `scripts/orbit-query` only works
from inside the skill directory:

```bash
# Adjust path to wherever the skill is installed:
~/.config/opencode/skills/orbit/scripts/orbit-query /tmp/q.json
```

For the full field reference see [`query_language.md`](query_language.md).

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
  },
  "response_format": "llm"
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
  },
  "response_format": "llm"
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
  },
  "response_format": "llm"
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
    "aggregations": [
      {"function": "count", "target": "mr", "group_by": "p", "alias": "open_mrs"}
    ],
    "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
    "limit": 10
  },
  "response_format": "llm"
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
  },
  "response_format": "llm"
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
  },
  "response_format": "llm"
}
```

Increment `offset` by `page_size` for subsequent pages.

## More examples

Production-grade query examples — more complex traversals and aggregations —
live in [`fixtures/queries/sdlc_queries.yaml`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/fixtures/queries/sdlc_queries.yaml)
in the `gitlab-org/orbit/knowledge-graph` repo. Treat those as the source of
truth for idiomatic queries.
