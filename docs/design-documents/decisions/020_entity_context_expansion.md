---
title: "GKG ADR 020: Entity context expansion for the Orbit context endpoint"
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

The endpoint must optionally attach graph data, and Orbit bills per graph
query. GKG emits one billing event for each finished `ExecuteQuery` stream.
Rails and Workhorse emit none. The event has no field that groups several
queries. `correlation_id` is on it, but Fulfillment said it is used for logging
and tracing only ([ADR 007](007_monetization_engineering.md)). A `context`
request that ran several graph queries would therefore bill as several queries,
and the caller could not predict how many.

Two accepted ADRs shape where a graph query runs:

- [ADR 008](008_workhorse_query_acceleration.md): Workhorse owns the gRPC
  stream to GKG, so a graph query never holds a Puma worker for its whole
  duration.
- [ADR 011](011_agent_command_surface.md): moving query execution out of
  Workhorse loses streaming, because Rails buffers large results.

Rails can now run the stream itself, behind the `orbit_query_in_rails` feature
flag
([#1381](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1381)).
Its runner marks the request as IO-bound so it does not use up a Puma thread,
and it redacts in process. The flag is a derisk flag and is off by default.

## Decision

This ADR fixes the interface and the billing rule. It does not design the
`expand_*` queries. The Orbit team iterates on those and owns their content.

**One token per request.** `POST /api/v4/orbit/context` takes `tokens`, an
array that must hold exactly one string. Any other length is a validation
error. We keep an array so that lifting the limit later is not a breaking
change. Top-level `expand` defaults to `false`.

**One billable query at most.** Without `expand`, the request runs only Rails
summaries and no graph query. With `expand`, it runs exactly one named query
for the token's entity type. One `context` request is therefore at most one
billed query. It also counts once against the `orbit_query` rate limit (60 per
minute per user), which matches the billed count.

**Response.** The response is version 1.1.0. It carries the PostgreSQL
summaries as before, plus `expansion`. The value is `null` when no expansion
was requested, the token was not found, or the type has no expansion query.
Otherwise it is `{"result": ...}` with the deduplicated graph, or
`{"error": {"code": ...}}`. An expansion failure never fails the summaries.

**Transport.** The expansion is an ordinary named query. It runs on whatever
path `/api/v4/orbit/query/:name` uses, so the contract does not depend on the
transport. Rails resolves and authorizes the token, builds the summaries, and
adds the graph result to the body. We do not add Workhorse merge logic. That
means `expand` runs only on the Rails path, behind `orbit_query_in_rails`.
While that flag is off, `expand: true` is accepted and ignored, and
`expansion` is `null`. The trade-off is that `expand` rollout follows #1381.
In return, the merge and the error mapping stay in Ruby, and no Go change is
needed. If #1381 slips, the fallback is Workhorse merge logic for a single
stream. The contract stays the same.

**Expansion query.** Orbit owns one named query per entity type. It takes
`node_ids` and returns a normal graph response. Rails keeps a static map from
type to query name. Fresh typed facets, such as a head pipeline or diff files,
need a richer query and are the Orbit team's call. Once Orbit ships
`expand_merge_request` and `expand_work_item`, clients can also call them
directly through `/api/v4/orbit/query/:name`.

**Leaks.** Rails never reveals an ID the caller cannot read. Every failure
after a lookup starts returns the same `not_found`. Only a found, readable
record is expanded. The details block below lists every rule.

**Truncation.** `limit` applies to the whole graph, and redacted rows take up
window slots. A center missing from the graph means "no readable neighbors"
only when `pagination.truncated` is `false`. Otherwise the client must treat it
as unknown.

**Later: several tokens.** We want N sub-queries of one `context` request to
bill as one query. A candidate mechanism is a shared `billable_request_id`.
Rails would put it in the signed claims, and GKG would set it on every
sub-query's billing event so that billing can collapse them. This needs sign-off
from the billing owners (the Orbit team for emission, Fulfillment for CDot
mediation). It is not built. Until it is, the one-token limit stays. The
explicit MCP tool and agent command are deferred.

<details>
<summary>Request and response fields</summary>

- `tokens` is always an array of one string. There is no object keyed by token.
- `response_format` is `raw` or `llm`, and defaults to `raw`.
- Token forms: `Type[id]`, `gid://gitlab/<Type>/<id>`, instance URLs, `path!iid`
  and `path#iid`. Group-level work items are in scope.
- Supported types are `MergeRequest` and `WorkItem`. `Issue` is an alias that
  resolves, is reported, and expands as `WorkItem`. Other types return
  `unsupported_type`.
- Resolution applies both the per-resource ability (for example
  `read_work_item`) and the fine-grained token boundary. Group-level work items
  pass their group as the boundary.
- Relative to 1.0.0, `ref` becomes `token` (also in `linked_issues[]` and
  `linked_merge_requests[]`), and the prototype's `invalid_ref` becomes
  `invalid_token`. The response adds `resolved_token`, `expansion`, and
  `namespace_path` inside `summary`. `project_path` is `null` for group-level
  work items.
- `expansion` is always present. Edges identify their endpoints by the pair
  (`from`, `from_id`) or (`to`, `to_id`), because IDs are per type.
- Expansion `error.code` is a GKG or Rails error code, such as
  `quota_exhausted`.

```json
{"version": "1.1.0",
 "entities": [{"token": "gitlab-org/gitlab#609451",
               "resolved_token": "WorkItem[196910643]",
               "type": "WorkItem", "id": 196910643, "found": true,
               "summary": {"...": "..."}}],
 "expansion": {"result": {"nodes": ["..."], "edges": ["..."]}}}
```

With `response_format: llm`, the response is `text/plain`: the Rails summary
text, then one labeled graph section in TOON, or a one-line `error <code>`.
Raw results carry `format_version`. TOON output carries its own version
(`toon_output_format` in `config/versions.yaml`).

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
- The expansion query and payload contain the ID of a found record only. This
  saves work and avoids cross-tenant timing on centers. It is defense in depth,
  not the leak control. Redaction is.

</details>

## Consequences

- A `context` request bills at most one query and uses one rate-limit unit.
  The billing owner should confirm that one `context` with `expand` is one
  billed query. A failed query is not billed.
- An agent that holds several tokens makes several `context` calls, each rate
  limited and billed on its own.
- Both the `context` route and the named-query route check the rate limit
  before the enabled-namespace check, so 403 responses also use budget. We
  accept this for v1.
- `expand` waits for `orbit_query_in_rails` to roll out. Rails owns the merge.
  Version 1.1.0 covers the whole body, and no new GKG pin is needed. This ADR
  moves neither the raw nor the TOON pin.
- A confidential issue tracks authorization-safe pagination (`has_more`,
  `truncated`, and cursors computed after redaction). It does not block this
  ADR, because `expand` runs only named queries a caller can already run
  through `/orbit/query/:name`. The fix must keep `truncated`, because the
  truncation rule above relies on it.
- The Rails implementation that replaces the closed prototype must:
  - Use `POST` with `tokens`/`token`, and version 1.1.0. The prototype never
    merged, so 1.1.0 lets clients built against it detect the change.
  - Declare the `read_orbit` permission on `context`. The prototype used
    `read_knowledge_graph`.
  - Add `POST /orbit/context` to the fine-grained token documentation and route
    configuration.
- Other follow-up work: the Rails resolver and group-level work items, and the
  per-type named queries in Orbit. The CLI remote routing in !2523 must move
  from `GET` with `refs[]` to `POST` with `tokens` and the 1.1.0 envelope.
  Whether `Type:ID` is an accepted token form is open.

## Alternatives considered

### N tokens, billed as one query

A shared `billable_request_id` on every sub-query's billing event would let
billing collapse them. It is the team's preference for the multi-token future.
It is deferred because it needs GKG, Rails, and CDot changes and billing-owner
sign-off. `correlation_id` cannot do this today.

### One cross-type query

Not possible today. One `ExecuteQuery` carries one query string, and
`neighbors` takes one node selector, so one entity type. A `traversal` joins up
to five selectors but does not return independent neighborhoods. A true single
query needs GKG batch or DSL work in the billing-scoped server paths. With one
token it is not needed.

### Workhorse fan-out

Rails hands Workhorse a list of named queries. Workhorse runs the streams in
parallel and merges the results. It only pays off with several tokens, and it
adds Go merge logic and typed transport errors. Revisit it together with the
multi-token limit.

### Puma or Rails execution for several tokens

Running several streams from Rails changes where redaction and merging run. It
does not change the query count or billing, because GKG emits one event per
stream. For one token it is the chosen transport.

### GKG batch multiplexing

One `named` request would carry several sub-queries and GKG would merge the
results. It needs a GKG executor refactor and sends Rails summaries through
GKG. It is also a way to the single-event billing above.

### Client-side two-call composition

The client calls `context`, then the `expand_*` query itself. That costs two
round trips but gives the caller control of the query count. It stays available
by calling the queries directly.

### Object keyed by token

`{"tokens": {"<token>": {...}}}` loses order in some clients and collapses
duplicate tokens. Array-only `tokens` is unambiguous.

## References

- [#1262: Orbit context endpoint](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1262)
- [#1381: Rails query execution](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1381)
- [`gitlab-org/gitlab!254189`](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/254189) (closed prototype)
- [ADR 007: Monetization engineering](007_monetization_engineering.md)
- [ADR 008: Workhorse query acceleration](008_workhorse_query_acceleration.md)
- [ADR 011: Agent command surface](011_agent_command_surface.md)
