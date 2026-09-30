---
title: "GKG ADR 019: Entity context expansion for the Orbit context endpoint"
creation-date: "2026-09-30"
authors: [ "@dgruzd", "@aalgutifan" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-09-30

## Context

Agents often hold a token such as `MergeRequest[123]` or a URL and need the
entity plus its neighborhood: reviewers, linked issues, labels, milestone.
Today that takes one Rails lookup plus one graph query per related thing.
[#1262](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1262)
tracks a Rails `context` endpoint that resolves tokens and returns PostgreSQL
summaries. The prototype,
[`gitlab-org/gitlab!254189`](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/254189),
is closed and motivated this ADR. The design moves here before any further
implementation.

The endpoint must optionally attach graph data, which raises a transport
question. Two accepted ADRs constrain it:

- [ADR 008](008_workhorse_query_acceleration.md): Workhorse owns the gRPC
  stream to GKG, so a graph query never holds a Puma worker for its whole
  duration.
- [ADR 011](011_agent_command_surface.md): executing queries Rails-side buffers
  results in Puma and is not the path for agents.

Measurements on a local stack support the design:

- An unreadable center returns the same empty graph as a nonexistent ID, and
  edges to unreadable neighbors are dropped. Per-type queries with `node_ids`
  therefore leak nothing.
- The unchanged Workhorse `SendQuery.Inject` ran 2 to 40 concurrent streams
  with its redaction loop, one redaction callback per stream. Five queries took
  0.4 s concurrently against 0.87 s sequentially.
- A `neighbors` query over 20 merge requests returned 9.9 KB raw and 5.7 KB in
  GOON, in 130 to 300 ms.
- `neighbors` can return stale `HAS_HEAD_PIPELINE` and `HAS_LATEST_DIFF` edges,
  and its `LIMIT` counts duplicate version rows (for example 112 rows for 52
  unique edges). This is a separate Orbit defect; the expansion design does not
  depend on it.

The content of an expansion (which edges, which facets) is under active
iteration by the Orbit team. This ADR does not fix it.

## Decision

This ADR fixes the interface and the transport. It does not design the
`expand_*` queries.

### Request

`POST /api/v4/orbit/context`, replacing the prototype's `GET`:

```json
{"tokens": ["MergeRequest[529482712]",
            {"token": "gitlab-org/gitlab#609451", "expand": true}],
 "expand": true,
 "response_format": "raw"}
```

- `tokens` is always an array of 1 to 20 items. Each item is a string or
  `{token, expand}`. There is no object keyed by token: it loses order in some
  clients and collapses duplicates.
- Top-level `expand` defaults to `false`. A per-token value overrides it.
  Expansion adds latency and a GKG dependency, so it is opt-in.
- Accepted token forms: `Type[id]`, `gid://gitlab/<Type>/<id>`, instance URLs,
  `path!iid` and `path#iid`. Group-level work items are in scope. Other types
  return `unsupported_type`.

### Response, version 1.1.0

Relative to 1.0.0, `ref` becomes `token` (also in `linked_issues[]` and
`linked_merge_requests[]`). The response adds `resolved_token`,
`namespace_path` and a top-level `expansions` object.

```json
{"version": "1.1.0",
 "entities": [{"token": "gitlab-org/gitlab#609451",
               "resolved_token": "WorkItem[196910643]",
               "type": "WorkItem", "id": 196910643, "found": true,
               "summary": {"...": "..."}}],
 "expansions": {
   "WorkItem": {"status": "ok", "result": {"nodes": ["..."], "edges": ["..."]}}}}
```

- `expansions` is always present (`{}` if nothing was requested) and holds one
  deduplicated graph per entity type, not per entity. Edges identify the
  center through `from_id` and `to_id`.
- A type appears only if a found record asked for expansion.
- `expansions.<Type>.status` is `ok`, `no_expansion` (the type has no query) or
  `error` with `error.code`. It is the only outcome signal; entities carry no
  expansion status. An expansion failure never fails the summaries.

### Leak rules

- `resolved_token` appears only when `found` is `true`.
- For URL and `path#iid` tokens, `id` is `null` when `found` is `false`. For
  `Type[id]` and GID tokens, `id` only echoes what the caller sent. `type` is
  `null` for every unfound token.
- An unknown project or group, an unknown iid and an unreadable record all
  return one identical `not_found` with no extra fields.
- Expansion queries and payloads contain ids of found records only. Redaction
  is the leak control; this rule is defense in depth and avoids cross-tenant
  timing.

### Client rule: `result` or `request`

For each `expansions.<Type>`: if `result` is present, use it. Otherwise, if
`request` is present, `POST` it (`{method, path, body}`) and read `.result`
from the reply. A client that follows this rule works unchanged when the
server moves between transports below.

### Expansion queries

Orbit owns one named query per entity type, each taking `node_ids` (1 to 20
integers) and returning a normal graph response. The Orbit team decides what
they contain and may change it without changing this contract. Rails keeps a
static map from type to query name and treats an absent center as "no
neighbors", never as an error.

### Transport

- **19.6 ships option (b).** Rails returns `expansions.<Type>.request`, a
  ready-to-run `POST /api/v4/orbit/query/<name>` payload. The client posts it
  through Workhorse, so ADR 008 holds. Raw REST users pay two round trips.
- **End state is option (e), Workhorse fan-out.** Rails sends one send-data
  header with its body and a list of named queries. Workhorse opens one
  ordinary `ExecuteQuery` stream per query in parallel and runs the existing
  redaction loop on each. It then writes the body with `expansions.<Type>`
  filled with `result` or `error`. GKG sees only ordinary named queries. The client
  makes one call and the server emits `result` only.
- **(e) depends on the Workhorse maintainers accepting merge logic.** If they
  object, fall back to option (c): GKG batch multiplexing, in which one
  `named` request carries several sub-queries and GKG merges the results. The
  client rule makes the switch invisible to clients.

### Follow-up implementation requirements

These apply to the Rails implementation that replaces the closed prototype:

- Use `POST` with `tokens`/`token`, as above.
- Rename `ref` to `token` and bump `version` to 1.1.0.
- Declare the `read_orbit` permission on `context`. Current master routes use
  `read_orbit`; the prototype predates the rename and used
  `read_knowledge_graph`.
- Update the fine-grained token documentation and route configuration, which
  list `GET /orbit/context`.

## Consequences

- **Puma and Workhorse budget.** Under (b) Puma serves one short `context`
  request and then ordinary `query/:name` requests. Under (e) Puma serves one
  `context` request plus one redaction callback per expanded type, the same
  count as (c). Workhorse gains about 150 to 250 lines (typed errors, a fan-out
  loop, a JSON merge). Expect 3 to 5 Workhorse days plus 2 Rails days; these
  estimates are not measured.
- **Typed errors in Workhorse.** Today `SendQuery` reports failures as
  plain-text 502/504 bodies, and a deadline during the redaction callback is a
  502. Filling `expansions.<Type>.status` requires typed returns from the
  failure paths. The stream message cap of 10 is per stream and stays
  unchanged under (e).
- **Pins.** The raw (5.0.3) and GOON (4.0.3) output pins do not move. Each
  `result` keeps its own `format_version`. Under (e) there is no new GKG pin:
  Rails and Workhorse ship together and `version` 1.1.0 covers the body. Under
  (c) GKG would own a merged-body shape and add a `context_output_format` pin.
- **Billing.** Billing and analytics events are emitted per sub-query, which
  matches usage. The billing owner should confirm this. Quota checks run once
  per stream.
- **Rate limit.** `orbit_query` allows 60 requests per minute per user. Under
  (b), an expanding request costs 1 plus one per expanded type, for example 3
  for a merge request plus a work item. Under (e) it costs 1. The
  named-query route checks the limit before the enabled-namespace check, so
  403 responses also consume budget. We accept this for v1 and document it.
- **MCP tool deferred.** An explicit MCP tool and an agent command are out of
  scope. The Rails service and resolver take plain values so the command
  interceptor can reuse them later.
- **Follow-up work.** Rails: resolver, group-level work items, the requirements
  above, `expand` plumbing behind a feature flag. Orbit: the per-type named
  queries. Workhorse: fan-out under (e). CLI: `glab orbit remote context`.
  Independent: the `neighbors` dedup and tombstone fix.
- **Risk.** (e) needs an agreement outside this repository. Until then 19.6
  works through (b) alone, and (b) stands by itself if the follow-up slips.

## Alternatives considered

### (a) Execute in-process from Rails

Rails would call the GKG gRPC client directly. Accepted ADR 008 rejects this:
each query holds a Puma worker for the whole stream, and ADR 011 adds that
Rails-side execution buffers results. Nothing in production calls it, cold
latency exceeds any safe low-urgency deadline, and a failed redaction would
silently become deny-all. Rejected.

### (c) GKG batch multiplexing versus (e) Workhorse fan-out

| | (c) GKG batch | (e) Workhorse fan-out |
| --- | --- | --- |
| GKG changes | Executor refactor (shared stream), batch envelope, passthrough, pin | None beyond the named queries |
| Rails summaries sent through GKG | Yes, needs security sign-off | No |
| GKG version skew | Older GKG rejects the batch; needs a gate | None |
| Summaries if an expansion fails | Stream failure loses everything unless Workhorse adds a fallback | Kept; the failure is `expansions.<Type>.status: error` |
| Owner of the merged body | Two (Rails and GKG) | One (Rails and Workhorse) |
| Billing, quota | N events, 1 quota check | N events, N quota checks |
| Latency | Sequential unless redaction is serialized under a lock | Parallel by default |
| Workhorse code | About 15 lines to write the body unwrapped | About 150 to 250 lines |
| Main risk | Size of the GKG work | Maintainers may reject merge logic |

(c) also needs the redaction exchange serialized across sub-queries. It needs
a cap of 8 pipelines under the stream message limit and a size cap on echoed
Rails data. We prefer (e) and keep (c) as the fallback.

### Object keyed by token

`{"tokens": {"<token>": {"expand": true}}}` loses order in some clients and
collapses duplicate tokens. Array-only `tokens` is unambiguous.

### Client-side composition

Leave `context` with summaries only and let agents compose expansion queries.
This keeps every agent re-deriving which queries suit which type and keeps the
cost of one query per related thing. Option (b) is the compatible middle
ground: the server names the query and the client only runs it.

## References

- [#1262: Orbit context endpoint](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1262)
- [`gitlab-org/gitlab!254189`](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/254189) (closed prototype)
- [ADR 008: Workhorse query acceleration](008_workhorse_query_acceleration.md)
- [ADR 011: Agent command surface](011_agent_command_surface.md)
- [ADR 012: GOON format](012_goon_format.md)
