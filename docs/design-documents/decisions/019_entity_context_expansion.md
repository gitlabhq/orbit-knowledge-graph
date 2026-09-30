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

An agent that holds a token such as `MergeRequest[123]` or a URL needs the
entity and its neighborhood: reviewers, linked issues, labels, milestone. Today
that takes one Rails lookup plus one graph query per related thing.
[#1262](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1262)
tracks a Rails `context` endpoint that resolves tokens and returns PostgreSQL
summaries. The prototype,
[`gitlab-org/gitlab!254189`](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/254189),
is closed. It motivated this ADR, and the design now lives here.

The endpoint must optionally attach graph data. Two accepted ADRs limit how:

- [ADR 008](008_workhorse_query_acceleration.md): Workhorse owns the gRPC
  stream to GKG, so a graph query never holds a Puma worker for its whole
  duration.
- [ADR 011](011_agent_command_surface.md): executing queries Rails-side buffers
  results in Puma and is not the path for agents.

On a local stack, the unchanged Workhorse `SendQuery.Inject` ran 2 to 40
concurrent streams with its redaction loop. Five queries took 0.4 s
concurrently against 0.87 s sequentially. One `neighbors` query over 20 merge
requests returned 10 KB raw and 5.7 KB in GOON.

## Decision

This ADR fixes the interface and the transport. It does not design the
`expand_*` queries. The Orbit team iterates on those and owns their content.

**Interface.** `POST /api/v4/orbit/context` takes `tokens`, an array of 1 to 20
strings or `{token, expand}` objects. Top-level `expand` defaults to `false`,
and a per-token value overrides it. The response is version 1.1.0. It carries
the PostgreSQL summaries as before, plus `expansions`. That holds one
deduplicated graph per entity type, each with a `status`. `ok` adds a `result`,
`error` adds an `error` object, and `no_expansion` adds neither. An
expansion failure never fails the summaries.

**Transport: Workhorse fan-out.** Rails resolves and authorizes the tokens and
builds the response body. It hands Workhorse one send-data header holding that
body (or the LLM text) and a list of `{key, named query}`. Workhorse opens one
ordinary `ExecuteQuery` stream per key, in parallel. Each stream runs the
unchanged redaction loop. Workhorse then fills in
`expansions.<Type>.{status, result | error}` and writes the body. GKG only ever
sees ordinary named queries. The client makes one call. We aim for 19.6. `expand`
stays behind a feature flag until fan-out ships. Once Orbit ships
`expand_merge_request` and `expand_work_item`, clients can call them directly
through `/api/v4/orbit/query/:name`.

**Expansion queries.** Orbit owns one named query per entity type. It takes
`node_ids` (1 to 20 integers) and returns a normal graph response. One request
per type limits the content to what one named query can express. Fresh typed
facets, such as a head pipeline or diff files, need one of two things. Either
several queries per type (a contract change) or composite named queries (not
built yet).
Rails keeps a static map from type to query name. An absent center is never an
error.

**Leaks.** Rails never reveals an ID the caller cannot read. Every failure
after a lookup starts returns the same `not_found`. Only found, readable records
are expanded. The details block below lists every rule.

**Truncation.** `limit` is global across all centers in a type, and redacted
rows take up window slots. A center missing from a graph means "no readable
neighbors" only when `pagination.truncated` is `false`. Otherwise the client
must treat it as unknown.

**Other decisions.** Billing is per sub-query. The explicit MCP tool and agent
command are deferred.

<details>
<summary>Request and response fields</summary>

- `tokens` is always an array. There is no object keyed by token.
- `response_format` is `raw` or `llm`, and defaults to `raw`.
- Token forms: `Type[id]`, `gid://gitlab/<Type>/<id>`, instance URLs, `path!iid`
  and `path#iid`. Group-level work items are in scope.
- Supported types are `MergeRequest` and `WorkItem`. `Issue` is an alias that
  resolves, is reported, and expands as `WorkItem`, so its graph is under
  `expansions.WorkItem`. Other types return `unsupported_type`.
- Resolution applies both the per-resource ability (for example
  `read_work_item`) and the fine-grained token boundary. Group-level work items
  pass their group as the boundary.
- Relative to 1.0.0, `ref` becomes `token` (also in `linked_issues[]` and
  `linked_merge_requests[]`), and the prototype's `invalid_ref` becomes
  `invalid_token`. The response adds `resolved_token`,
  `expansions`, and `namespace_path` inside `summary`. `project_path` is `null`
  for group-level work items.
- `expansions` is always present (`{}` if nothing was requested). A type appears
  only if a found record asked for expansion. A center is identified by the
  edge endpoint pair (`from`, `from_id`) or (`to`, `to_id`), because IDs are per
  type.
- `expansions.<Type>.status` is `ok`, `no_expansion` (the type has no query) or
  `error` with `error.code`. In raw, it is the only outcome signal. Entities
  carry no expansion status.

```json
{"version": "1.1.0",
 "entities": [{"token": "gitlab-org/gitlab#609451",
               "resolved_token": "WorkItem[196910643]",
               "type": "WorkItem", "id": 196910643, "found": true,
               "summary": {"...": "..."}}],
 "expansions": {
   "WorkItem": {"status": "ok", "result": {"nodes": ["..."], "edges": ["..."]}}}}
```

With `response_format: llm`, the response is `text/plain`: the Rails summary
text, then one labeled section for every type that a found record asked for.
A section holds either the GOON graph or a one-line status, `error <code>` or
`no_expansion`. Raw results carry
`format_version`. GOON carries `goon_version`
([ADR 012](012_goon_format.md)).

</details>

<details>
<summary>Leak rules</summary>

- Error codes are `invalid_token`, `unsupported_type`, and `not_found`.
  `invalid_token` (unparseable token, other host) and `unsupported_type` are
  decided from the token text alone, before any lookup.
- Once a lookup starts, every failure returns the identical `not_found` with no
  extra fields. This covers an unknown project or group, an unknown iid, and an
  unreadable record. It also covers a namespace of the wrong kind, such as
  `group!5` or a project path in a `/groups/` URL.
- `resolved_token` appears only when `found` is `true`.
- For every iid form (URL, `path!iid`, `path#iid`), `id` is `null` when `found`
  is `false`. For `Type[id]` and GID tokens, `id` only echoes what the caller
  sent. `type` is `null` for every unfound token.
- `linked_issues[]`, `linked_merge_requests[]`, and reviewers include readable
  records only.
- Expansion queries and payloads contain IDs of found records only. This saves
  work and avoids cross-tenant timing on centers. It is defense in depth, not
  the leak control. Redaction is.

</details>

<details>
<summary>Workhorse mechanics</summary>

- Today `SendQuery` reports transport failures as plain-text 502 or 504 bodies,
  and a deadline during the redaction callback is a 502. GKG in-band errors are
  already JSON. Filling `expansions.<Type>.status` needs typed returns from the
  transport failure paths.
- `maxStreamMessages` is 10 per stream. Each stream needs a request, at most one
  redaction round, and a result, so the cap stays unchanged.
- Workhorse has a 30 s default deadline and a 120 s maximum. Per-key timeouts
  must stay below them.
- The Rails body travels base64 in the send-data header. Workhorse sets no
  explicit limit, so Go's default `MaxResponseHeaderBytes` of 10 MiB applies. This is unmeasured for 20 entities
  and should be measured before shipping.
- We estimate about 150 to 250 lines in Workhorse, 3 to 5 Workhorse days, and 2
  Rails days. These are not measured.

</details>

## Consequences

- Puma serves one `context` request plus at most one redaction callback per
  expanded type.
- Rate limit and billing are separate meters. `orbit_query` (60 per minute per
  user) counts once per `context` request, however many types expand. Billing
  and analytics events are emitted per sub-query, which matches usage. The
  billing owner should confirm this.
- Both the `context` route and the named-query route check the rate limit
  before the enabled-namespace check, so 403 responses also use budget. We
  accept this for v1.
- Workhorse gains merge logic and typed errors. Rails and Workhorse ship
  together once fan-out lands, so version 1.1.0 covers the whole body and no new
  GKG pin is needed. This ADR moves neither the raw nor the GOON pin.
- A confidential issue tracks authorization-safe pagination (`has_more`,
  `truncated`, and cursors computed after redaction). It does not block this
  ADR, because `expand` runs only named queries a caller can already run
  through `/orbit/query/:name`. The fix must keep `truncated`, because the truncation rule above relies
  on it.
- The Rails implementation that replaces the closed prototype must:
  - Use `POST` with `tokens`/`token`, and version 1.1.0. The prototype never
    merged, so 1.1.0 lets clients built against it detect the change.
  - Declare the `read_orbit` permission on `context`. The prototype used
    `read_knowledge_graph`.
  - Add `POST /orbit/context` to the fine-grained token documentation and route
    configuration.
- Other follow-up work: the Rails resolver and group-level work items, the
  per-type named queries in Orbit, and Workhorse fan-out. The CLI remote routing
  in !2523 must move from `GET` with `refs[]` to `POST` with `tokens` and the
  1.1.0 envelope. Whether `Type:ID` is an accepted token form is open.

## Alternatives considered

### In-process execution in Rails

Rails would call the GKG gRPC client directly. Accepted ADR 008 rules this out:
each query holds a Puma worker for the whole stream.

### GKG batch multiplexing

One `named` request would carry several sub-queries and GKG would merge the
results. It needs a GKG executor refactor, sends Rails summaries through GKG,
and gives the body two owners. We would pick it if the Workhorse change is
rejected.

### Client-side two-call composition

The client calls `context`, then the `expand_*` queries itself. That costs two
round trips. It stays available by calling the queries directly.

### Object keyed by token

`{"tokens": {"<token>": {...}}}` loses order in some clients and collapses
duplicate tokens. Array-only `tokens` is unambiguous.

## References

- [#1262: Orbit context endpoint](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1262)
- [`gitlab-org/gitlab!254189`](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/254189) (closed prototype)
- [ADR 008: Workhorse query acceleration](008_workhorse_query_acceleration.md)
- [ADR 011: Agent command surface](011_agent_command_surface.md)
- [ADR 012: GOON format](012_goon_format.md)
