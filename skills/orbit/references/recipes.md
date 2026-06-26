# Orbit query recipes

Paste-ready request bodies for each `query_type`. All examples omit
`response_format`; the CLI defaults to `llm` (compact, agent-friendly). Pass
`--response-format raw` when piping into `jq`.

Every recipe assumes `glab auth login` has succeeded and the
`knowledge_graph` feature flag is on for your user. See
[`SKILL.md`](../SKILL.md) for prerequisites.

The shell pattern is always:

```shell
glab orbit remote query /tmp/q.json
# or:
cat /tmp/q.json | glab orbit remote query -
# or, for jq pipelines:
glab orbit remote query --response-format raw /tmp/q.json | jq '.'
```

Raw output is a single JSON object; rows live under `.result.nodes[]` and each
row's entity type is in its `.type` field (not `.entity` or `.node_type`):

```shell
glab orbit remote query --response-format raw /tmp/q.json \
  | jq -r '.result.nodes[] | select(.type=="MergeRequest") | .iid'
```

For the full field reference see [`query_language.md`](query_language.md).

## Look up a GitLab project's numeric ID

Many filters (e.g. `project_id` on MergeRequest) need the numeric project ID.
Query the `Project` entity by `full_path` and read back its `id`:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "node": {
      "id": "p",
      "entity": "Project",
      "columns": ["id", "full_path"],
      "filters": {"full_path": {"op": "eq", "value": "gitlab-org/orbit/knowledge-graph"}}
    },
    "limit": 1
  }
}
```

## Look up a merge request by IID

The most common entry point: "tell me about MR !1216 in project X".
Requires both `iid` and `project_id` filters (IID is only unique within a project):

```json orbit-query
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

```json orbit-query
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

## Pipelines that ran for one merge request

> **Always filter `Pipeline.source = "merge_request_event"` for this question.**
> The graph also stores the downstream child pipelines those top-level MR
> pipelines triggered (`source = "parent_pipeline"`). Both
> `Pipeline.merge_request_id` and the `MergeRequest --TRIGGERED--> Pipeline`
> edge return parents **and** children. Without the `source` filter you
> will over-count by a factor of 5-10× and the answer will not match the
> MR's **Pipelines** tab.
> `MergeRequest --HAS_HEAD_PIPELINE--> Pipeline` is unrelated: it points
> to the one current head pipeline, useful for "what is running now" but
> not for history.

The canonical query is single-node and filters `Pipeline.merge_request_id`
plus `source`. `merge_request_id` is the MR's internal numeric `id` (not the
project-scoped `iid`); look it up first with the
[MR-by-IID recipe](#look-up-a-merge-request-by-iid) and reuse the returned
`id`.

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "node": {
      "id": "p", "entity": "Pipeline",
      "filters": {
        "merge_request_id": {"op": "eq", "value": 482908721},
        "source": {"op": "eq", "value": "merge_request_event"}
      },
      "columns": ["id", "status", "source", "sha", "ref", "created_at"]
    },
    "order_by": {"node": "p", "property": "created_at", "direction": "DESC"},
    "limit": 100
  }
}
```

Apply the same filter when narrowing by status — for example, "failed
pipelines for this MR":

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "node": {
      "id": "p", "entity": "Pipeline",
      "filters": {
        "merge_request_id": {"op": "eq", "value": 482908721},
        "source": {"op": "eq", "value": "merge_request_event"},
        "status": {"op": "eq", "value": "failed"}
      },
      "columns": ["id", "status", "sha", "ref", "failure_reason", "duration", "created_at"]
    },
    "order_by": {"node": "p", "property": "created_at", "direction": "DESC"},
    "limit": 100
  }
}
```

Count by status with a single-node `aggregation` (keep the node count at
one — adding `MergeRequest` or `Project` as extra nodes can change the
underlying join shape and inflate the count):

```json orbit-query
{
  "query": {
    "query_type": "aggregation",
    "nodes": [
      {"id": "p", "entity": "Pipeline",
       "filters": {
         "merge_request_id": {"op": "eq", "value": 482908721},
         "source": {"op": "eq", "value": "merge_request_event"}
       }}
    ],
    "group_by": [{"kind": "property", "node": "p", "property": "status", "alias": "status"}],
    "aggregations": [{"function": "count", "target": "p", "alias": "pipeline_count"}],
    "aggregation_sort": {"column": "pipeline_count", "direction": "DESC"},
    "limit": 20
  }
}
```

If you only have the MR's `iid` and not its internal `id`, the equivalent
two-node form via `TRIGGERED` works — still with the `source` filter on the
Pipeline node:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {"id": "mr", "entity": "MergeRequest",
       "filters": {"iid": {"op": "eq", "value": 235291},
                   "project_id": {"op": "eq", "value": 278964}}},
      {"id": "p",  "entity": "Pipeline",
       "filters": {"source": {"op": "eq", "value": "merge_request_event"}},
       "columns": ["id", "status", "sha", "created_at"]}
    ],
    "relationships": [{"type": "TRIGGERED", "from": "mr", "to": "p"}],
    "order_by": {"node": "p", "property": "created_at", "direction": "DESC"},
    "limit": 100
  }
}
```

## MRs that touched a file (historical coverage)

> **Use `HAS_DIFF`, not `HAS_LATEST_DIFF`, for historical-coverage questions.**
> `HAS_LATEST_DIFF` only links a merge request to its **most recent** diff
> snapshot (via `MergeRequest.latest_merge_request_diff_id`). An MR that
> touched a file in an earlier revision but not in its final diff is
> invisible through `HAS_LATEST_DIFF`. For questions like "every MR that
> ever touched file X", traverse
> `MergeRequest --HAS_DIFF--> MergeRequestDiff --HAS_FILE-->
> MergeRequestDiffFile` (the one-to-N edge over all snapshots, joined
> via `MergeRequestDiff.merge_request_id`). Using `HAS_LATEST_DIFF` here
> can substantially undercount on long-lived files.

`MergeRequestDiffFile.old_path` is the preferred column for file
lookups (`new_path` differs from `old_path` only on renames). Filtering
and grouping by `old_path` keeps the same row identity across an MR's
history — see the canonical field descriptions on
[`merge_request_diff_file.yaml`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/config/ontology/nodes/code_review/merge_request_diff_file.yaml):

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {"id": "mr",   "entity": "MergeRequest",
       "columns": ["iid", "title", "state", "project_id"]},
      {"id": "diff", "entity": "MergeRequestDiff"},
      {"id": "f",    "entity": "MergeRequestDiffFile",
       "filters": {"old_path": {"op": "eq", "value": "app/services/base_service.rb"}}}
    ],
    "relationships": [
      {"type": "HAS_DIFF", "from": "mr",   "to": "diff"},
      {"type": "HAS_FILE", "from": "diff", "to": "f"}
    ],
    "limit": 100
  }
}
```

> **Known coverage gap.** `HAS_FILE` edges between `MergeRequestDiff` and
> `MergeRequestDiffFile` are sparsely populated in the current Orbit
> dataset. If this query returns far fewer files than expected for a
> given MR, report the result as "incomplete coverage" rather than as
> authoritative.

## Subclasses / descendants of a class

For "every subclass of `ApplicationRecord`" or "descendants of
`Boards::BaseService`", traverse `Definition` via the `EXTENDS` edge
(child → parent). `EXTENDS` is the single high-level inheritance edge in
the ontology — the indexer collapses language-specific kinds (class
extension, interface implementation, Go struct embedding) into it.
`Definition.fqn` is the fully qualified name (e.g.
`Boards::BaseService`); use it instead of bare `name` when the parent
class is namespaced:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {"id": "parent", "entity": "Definition",
       "filters": {"fqn": {"op": "eq", "value": "ApplicationRecord"}}},
      {"id": "child",  "entity": "Definition",
       "columns": ["name", "fqn", "file_path"]}
    ],
    "relationships": [
      {"type": "EXTENDS", "from": "child", "to": "parent",
       "max_hops": 3}
    ],
    "limit": 1000
  }
}
```

## `traversal` (multi-node) — start from nodes, follow relationships

List opened merge requests and their authors. Requires at least two nodes and
one relationship:

```json orbit-query
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

```json orbit-query
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

```json orbit-query
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

```json orbit-query
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

```json orbit-query
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

Shortest path from a group to a project (`max_depth` ≤ 3, server-enforced).
`rel_types` is required when either endpoint uses `filters` — omitting it
causes a server-side validation error.

> **Pitfall:** `path_finding` follows `rel_types` only in their **defined
> (schema) direction**, unlike `traversal` where `from`/`to` merely name
> endpoints. Pick endpoints and edge types that form a forward-directed chain
> (e.g. `Group --CONTAINS--> Project`, not `Project → … → Project` via a
> reverse hop the engine will not take).

```json orbit-query
{
  "query": {
    "query_type": "path_finding",
    "nodes": [
      {"id": "from", "entity": "Group",   "filters": {"id": {"op": "eq", "value": 9970}}},
      {"id": "to",   "entity": "Project", "filters": {"full_path": {"op": "eq", "value": "gitlab-org/gitlab"}}}
    ],
    "path": {"type": "shortest", "from": "from", "to": "to", "max_depth": 2, "rel_types": ["CONTAINS"]}
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

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "node": {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": {"op": "starts_with", "value": "gitlab-org/"}}
    },
    "limit": 200,
    "cursor": {"offset": 0, "page_size": 50}
  }
}
```

Increment `offset` by `page_size` for subsequent pages.

## More examples

Production-grade query examples — more complex traversals and aggregations —
live in the categorized corpus under [`fixtures/queries/corpus/`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/tree/main/fixtures/queries/corpus)
in the `gitlab-org/orbit/knowledge-graph` repo (`sdlc.yaml`, `aggregation.yaml`,
`code_graph.yaml`, and more). Treat those as the source of truth for idiomatic
queries.
