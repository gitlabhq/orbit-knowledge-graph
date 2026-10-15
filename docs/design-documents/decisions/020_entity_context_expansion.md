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
([#1381](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1381),
rollout in
[the GitLab rollout issue](https://gitlab.com/gitlab-org/gitlab/-/issues/633423)).
Its runner marks the request as IO-bound. Once `max_io_threads` is configured,
the request then does not hold a regular Puma thread. That setting defaults to
0, and the deployment tasks in #1381 are open. The runner also redacts in
process. The flag is a derisk flag and is off by default.

This ADR does not amend or supersede ADR 008 or ADR 011. `expand` depends on
the #1381 rollout, which replaces ADR 008's premise for the query path once it rolls out.
Both ADRs should be revisited when #1381 removes the Workhorse query path. ADR
011's buffering concern is acceptable here because the expansion is one bounded
named query.

## Decision

This ADR fixes the interface and the billing rule. It does not design the
`expand_*` queries. The Orbit team iterates on those and owns their content.

**One token per request.** `POST /api/v4/orbit/context` takes `tokens`, an
array that must hold exactly one string. The limit covers summaries-only
requests too, so it deliberately deviates from #1262, which proposed several
references. We keep an array so that lifting the limit later is not a breaking
change. Top-level `expand` defaults to `false`. A wrong `tokens` length, a
non-string element, or a non-boolean `expand` returns the usual 400 validation
error.

**At most one billable query.** Without `expand`, the request runs only Rails
summaries and no graph query. With `expand`, it runs at most one named query:
exactly one for a found, readable token whose type has an expansion query.
One `context` request is therefore at most one billed query. It counts once
against the `orbit_query` rate limit (60 per minute per user). GKG bills a
query that finishes without a GKG error, for any row count. That includes a
fully redacted result and a result that Rails drops afterwards. Errors inside
GKG are not billed. Summaries-only calls, expansions that fail before a query
runs (including the scope pre-check below), and 403 responses use rate-limit
budget but are not billed.

**Response.** The response is version 1.1.0. `entities` is an array, with one
entry for the one token. Each entry carries the PostgreSQL summary and an
`expansion` field. Because the expansion lives on the entity, several tokens
later add entries and need no new field. `expansion` is `null` when expansion
was not requested or the token was not found. Those two cases take precedence
over every error. For a found token whose type has no expansion query,
`expansion` is `{"error": {"code": "expansion_not_supported"}}`. Otherwise it
is `{"result": ...}` with the deduplicated graph, or `{"error": {"code": ...}}`. An expansion failure never fails the summaries.

**Transport.** The expansion is an ordinary named query, so the contract does
not depend on the transport. Rails resolves and authorizes the token, builds
the summary, and adds the graph result to the body. We do not add Workhorse
merge logic, so `expand` runs only on the Rails path, behind
`orbit_query_in_rails` (checked per user). While the flag is off for a user,
`expand: true` still returns the summaries, with
`expansion: {"error": {"code": "expansion_unavailable"}}`, which also covers
Orbit not being enabled for the user. The error carries a `fallback` naming the
query and its parameters, so the client can call it itself through
`/api/v4/orbit/query/:name`, which runs on Workhorse. The trade-off is that `expand` rollout follows #1381.
In return, the merge and the error mapping stay in Ruby, and no Go change is
needed. If #1381 slips, the fallback is Workhorse merge logic for a single
stream. The contract stays the same.

**Expansion query.** Orbit owns one named query per entity type, and each type
must map to exactly one named query, composite if needed. The per-type queries
must ship in GKG before or with the Rails endpoint. The billing rule
above depends on it. The query takes `node_ids` and returns a normal graph
response. Rails keeps a static map from type to query name and sets `limit`
as a constant, so clients cannot change it. Fresh typed facets, such as a head
pipeline or diff files, go into that one query, and their content is the Orbit
team's call. Once Orbit ships
`expand_merge_request` and `expand_work_item`, clients can also call them
directly through `/api/v4/orbit/query/:name`.

**Leaks.** Rails never reveals an ID the caller cannot read. Every failure
after a lookup starts returns the same `not_found`. Only a found, readable
record is expanded. The details block below lists every rule.

**Graph scope.** GKG answers only inside the user's graph scope: Reporter+
traversal paths with Orbit enabled. Rails can still read a record outside that
scope. Examples are a Guest, a project-only member, a non-member on a public
project, and a root group with Orbit off. Before it runs the expansion query, `context` checks the
record's path against the scope. Outside scope, `expansion` is
`{"error": {"code": "outside_graph_scope"}}`, no query runs, and nothing is
billed.

**Truncation.** The `limit` constant applies to the whole graph, and redacted rows take up
window slots. Inside graph scope, a center missing from the graph means "no
readable neighbors" only when `pagination.truncated` is `false`. Otherwise the
client must treat it as unknown. A record that is not indexed yet can still
return an empty graph, so an empty result is not proof that no neighbors exist.

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
- `response_format` is `raw` or `llm`, and defaults to `raw`. `gql` is out of
  scope and returns a validation error.
- Token forms: `Type[id]`, `gid://gitlab/<Type>/<id>`, instance URLs, `path!iid`
  and `path#iid`. Group-level work items are in scope.
- Supported types are `MergeRequest` and `WorkItem`. `Issue` is an alias that
  resolves, is reported, and expands as `WorkItem`. Other types return
  `unsupported_type`.
- Resolution applies both the per-resource ability (for example
  `read_work_item`) and the fine-grained token boundary. Group-level work items
  pass their group as the boundary. A fine-grained token therefore needs
  `read_merge_request` or `read_work_item` (and `read_pipeline` for pipeline
  status) in addition to `read_orbit`. This is stricter than the
  [security design](../security.md) rule that token scopes gate endpoints and do not
  filter results. Reconciling the two is a follow-up.
- Each `entities[]` entry has `token` (as sent), `resolved_token` (only when
  found), `type`, `id`, `found`, `summary` (when found), `error` (when not
  found) and `expansion`. `summary` holds `namespace_path`, and `project_path`,
  which is `null` for group-level work items. The remaining summary fields
  are defined by the Rails implementation and may only grow. The #1262
  proposal lists them: for a merge request, title, state, author, branches,
  latest pipeline status, reviewers and linked issues. For a work item, title,
  state, labels, assignees, milestone and linked merge requests.
  `summary.truncated` lists the summary fields cut at their cap, for example
  `["linked_issues"]`; the lists stay arrays. `linked_issues[]` and
  `linked_merge_requests[]` entries carry `type`, `id`, `token` and `relation`
  (`CLOSES`), so a later `related[]` list reuses the shape.
- `id` on an entity is a string, like `nodes[].id` in the graph.
- Edges in an expansion identify their endpoints by the pair (`from`,
  `from_id`) or (`to`, `to_id`), because IDs are per type.
- `expansion.error.code` is one of these:
  - `expansion_unavailable`: rollout, or Orbit not enabled for the user.
  - `expansion_not_supported`: the type has no expand query.
  - `outside_graph_scope`: the record is outside the user's graph scope.
  - A code that the Rails handler maps from the query path:
    `billing_namespace_required`, `quota_exhausted`, `timeout` or
    `execution_error`. `billing_namespace_required` carries the candidate groups
  and the preferences link, like `/orbit/query/:name`.
- Version 1.1.0 is a deliberate choice. The closed prototype described 1.0.0
  with `ref` and `invalid_ref`, which this contract renames to `token` and
  `invalid_token`. The prototype never merged, so 1.1.0 lets clients built
  against it detect the change.

A found token:

```json
{"version": "1.1.0",
 "entities": [
   {"token": "gitlab-org/gitlab#609451",
    "resolved_token": "WorkItem[196910643]",
    "type": "WorkItem", "id": "196910643", "found": true,
    "summary": {"linked_merge_requests": [
                  {"type": "MergeRequest", "id": "42", "token": "MergeRequest[42]",
                   "relation": "CLOSES", "...": "..."}],
                "truncated": [], "...": "..."},
    "expansion": {"result": {"nodes": ["..."], "edges": ["..."]}}}]}
```

An unfound token:

```json
{"version": "1.1.0",
 "entities": [
   {"token": "gitlab-org/gitlab#999999999",
    "type": null, "id": null, "found": false,
    "error": {"code": "not_found"}, "expansion": null}]}
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
  The billing owner should confirm that one `context` with `expand` is at most
  one billed query. Errors inside GKG are not billed. Out-of-scope records
  run no query.
- An agent that holds several tokens makes several `context` calls, each rate
  limited and billed on its own.
- Both the `context` route and the named-query route check the rate limit
  before the enabled-namespace check, so 403 responses also use budget. We
  accept this for v1.
- `expand` waits for `orbit_query_in_rails` to roll out, and until then it
  returns `expansion_unavailable`. Rails owns the merge.
  Version 1.1.0 covers the whole body, and no new GKG pin is needed. This ADR
  moves neither the raw nor the TOON pin.
- A confidential issue tracks authorization-safe pagination (`has_more`,
  `truncated`, and cursors computed after redaction). It does not block this
  ADR, because `expand` runs only named queries a caller can already run
  through `/orbit/query/:name`. The fix must keep `truncated`, because the
  truncation rule above relies on it.
- The Rails implementation that replaces the closed prototype must:
  - Use `POST` with `tokens` and the response in version 1.1.0.
  - Declare the `read_orbit` permission on `context`. The prototype used
    `read_knowledge_graph`.
  - Add `POST /orbit/context` to the fine-grained token documentation and route
    configuration.
- Other follow-up work: the Rails resolver and group-level work items, and the
  per-type named queries in Orbit. CLI remote routing for `context` (the closed
  !2523 used `GET` with `refs[]`) must use `POST` with `tokens` and the 1.1.0
  envelope.
  Whether `Type:ID` is an accepted token form is open.
- Future work, not decided here:
  - Epic tokens, an `expansion.query` echo, and more types (Note, Milestone,
    User, Project, Group) through the generic `expand_neighbors` query.
  - File and Definition tokens resolved from the graph, never Gitaly, from a
    blob URL (project, path, commit SHA, line range) with `expand_file` and
    `expand_definition` queries. The code token forms are reserved: the blob
    URL, `File[id]` and `Definition[id]`.
  - `as_of`, `limit`, `cursor` and `include` request fields.

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
