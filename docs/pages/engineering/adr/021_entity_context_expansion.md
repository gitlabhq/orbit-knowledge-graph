---
title: "GKG ADR 021: Entity context expansion for the Orbit context endpoint"
creation-date: "2026-09-30"
authors: [ "@dgruzd", "@aalgutifan" ]
toc_hide: true
---

## Status

Proposed

Amended by [ADR 022](022_queries_in_rails.md): Rails now runs the query stream, and Workhorse only proxies the request.

## Date

2026-09-30

## Context

An agent often holds a token for an entity, as a typed ID or a URL.
The agent needs the entity with its reviewers, linked issues, labels, and milestone.
Today that takes one Rails lookup and one graph query for each related item.
An issue tracks a Rails context endpoint that resolves tokens and returns PostgreSQL summaries.
A closed prototype of that endpoint motivated this decision, and the design now lives here.

The endpoint must optionally attach graph data, and Orbit bills for each graph query.
Orbit emits one billing event for each finished query stream, and Rails and Workhorse emit none.
The event has no field that groups several queries.
The event has a correlation ID, but Fulfillment uses it for logging and tracing only ([ADR 007](007_monetization_engineering.md)).
A context request that ran several graph queries would bill as several queries, and the caller could not predict the count.

Two accepted decisions shape where a graph query runs:

- [ADR 008](008_workhorse_query_acceleration.md) gives Workhorse the gRPC stream, so a graph query never holds a Puma worker for its full duration.
- [ADR 011](011_agent_command_surface.md) says that query execution outside Workhorse loses streaming, because Rails buffers large results.

Rails can now run the stream itself, behind a feature flag that is off by default.
The Rails runner redacts in the same process and marks the request as I/O-bound.
With I/O threads configured, the request does not hold a regular Puma thread, but that setting defaults to zero.

This decision does not amend or supersede ADR 008 or ADR 011.
Expansion depends on the Rails query rollout, which replaces the premise of ADR 008 for the query path.
We must revisit both decisions when the Workhorse query path goes away.
The buffering concern of ADR 011 is acceptable here, because the expansion is one bounded named query.

## Decision

We fix the interface and the billing rule.
We do not design the expansion queries.
The Orbit team iterates on those queries and owns their content.

**A context request takes one token and runs at most one billable graph query.**

### Request

The context endpoint takes a POST request with an array of tokens, and the array must hold exactly one string.
The limit also applies to requests for summaries only, a deliberate change from the original proposal of several references.
We keep an array, so a later change to allow more tokens is not a breaking change.
An expand flag defaults to false.
A wrong array length, an element that is not a string, or an expand flag that is not a boolean is a validation error.
The response format is raw by default, or the large language model (LLM) format.
The GQL format is a validation error.
The endpoint declares the Orbit read permission, and fine-grained tokens list the route.

A token is a typed ID, a global ID, an instance URL, or a short reference for a merge request, issue, or epic.
The endpoint supports merge requests and work items, including group-level work items.
Issue and epic are aliases for work item, because Orbit models epics as work items.
An epic resolves to its work item, and the summary shows the epic work item type.
An epic typed ID or global ID takes the epic ID, and not the work item ID.
Epic URLs follow the URL rules for host, scheme, port, and relative root, and an epic URL inside a project is invalid.
Other types return an unsupported type error.

Resolution applies the ability check for the resource and the fine-grained token boundary.
A group-level work item uses its group as the boundary.
A fine-grained token therefore needs Orbit read access and read access to the merge request or work item.
Pipeline status also needs read access to pipelines.
This rule is stricter than the security design, where token scopes gate endpoints and do not filter results, and we must reconcile the two later.

### Billing

Without expansion, the request runs Rails summaries only and no graph query.
With expansion, it runs one named query for a found, readable token whose type has an expansion query, and none otherwise.
A context request therefore bills at most one query, and it counts once against the per-user Orbit query rate limit.

Orbit bills a query that finishes without an Orbit error, for any row count, and does not bill an Orbit error.
That includes a result that redaction removes in full, and a result that Rails drops later.
These requests use rate-limit budget but are not billed:

- Requests for summaries only
- Expansions that fail before a query runs, including the scope check
- Requests that return a forbidden error

### Response

The response is version `1.1.0`.
It holds an array of entities, with one entry for the one token.
Each entry carries the token as sent, the type, the string ID, a found flag, and an expansion field.
A found entry adds the resolved token and the PostgreSQL summary, and an entry that was not found adds an error.
Several tokens later add entries and need no new field, because the expansion lives on the entity.

The summary holds the namespace path and the project path, which is null for a group-level work item.
Rails defines the other summary fields, and the set can only grow.
For a merge request, the original proposal lists title, state, author, branches, latest pipeline status, reviewers, and linked issues.
For a work item, it lists title, state, labels, assignees, milestone, and linked merge requests.
The summary names the fields that it cuts at their cap, and the lists stay arrays.
Each linked item carries a type, an ID, a token, and a relation, so a later list of related items can reuse the shape.

The expansion is null when the caller did not ask for it, or when the token was not found.
These two cases take precedence over every error.
Otherwise, the expansion holds a deduplicated graph result or an error, and an expansion failure never fails the summary.
Edges name their endpoints by type and ID, because IDs are unique only inside a type.

| Expansion error | When | Echoes the query |
|---|---|---|
| Expansion unavailable | Expansion is not enabled for the user yet | Yes |
| Unsupported type | The type has no expansion query | No |
| Outside graph scope | The record is outside the user's graph scope | Yes |
| Billing namespace required | The query path needs a billing namespace, and the error lists the candidate groups and the preferences link | Yes |
| Quota exhausted | The query path reports no quota | Yes |
| Timeout | The query timed out | Yes |
| Execution error | The query failed | Yes |

When the type has an expansion query, the expansion echoes the query name and parameters.
The parameters are the node IDs and the limit.
The echo appears on a result, and on every error where a query ran or would run.
The echo is the exact payload that Rails sends to Orbit, and the named query endpoint accepts it unchanged.
The caller can see the billed query, and can run it when expansion is unavailable.
The echo replaces a fallback field from an earlier draft, and it is additive, so the response version stays the same.

Version `1.1.0` is deliberate.
The closed prototype described version 1.0.0 with reference fields, which this contract renames to token fields.
The prototype never merged, and the new version lets clients built against it detect the change.

The LLM format returns plain text with the Rails summary, then one labeled graph section in Token-Oriented Object Notation (TOON), or a one-line error.
Raw results carry their format version, and TOON output carries its own version.

### Transport

The expansion is an ordinary named query, so the contract does not depend on the transport.
Rails resolves and authorizes the token, builds the summary, and adds the graph result to the body.
We do not add merge logic to Workhorse.
Expansion therefore runs only on the Rails query path, behind the feature flag, which Rails checks for each user.
While the flag is off, the client gets the summary with an expansion unavailable error.
The client can then run the echoed query through the named query endpoint, which runs on Workhorse.

The cost is that the expansion rollout follows the Rails query rollout.
In return, the merge and the error mapping stay in Ruby, and no Go change is necessary.
If the Rails query rollout slips, the fallback is Workhorse merge logic for a single stream, with the same contract.

### Expansion queries

Orbit owns one named query for each entity type, and each type maps to exactly one named query, composite if necessary.
The per-type queries must ship in Orbit before or with the Rails endpoint, because the billing rule depends on them.
A query takes node IDs and returns a standard graph response.
Rails keeps a static map from type to query name, and sets the limit as a constant that clients cannot change.
New typed facets, such as a head pipeline or diff files, go into that one query, and the Orbit team decides their content.
After the queries ship, clients can also call them directly through the named query endpoint.

### Leaks and graph scope

**Rails never reveals an ID that the caller cannot read.**

Rails detects an invalid token, such as text it cannot parse or another host, from the token text alone, before any lookup.
The same applies to an unsupported type.
After a lookup starts, every failure returns the same not-found error with no extra fields.
That covers an unknown project or group, an unknown internal ID, an unreadable record, and a namespace of the wrong kind.
An epic that is missing, confidential, unlicensed, unreadable, or in the wrong group also returns not found.

The resolved token appears only for a found record.
For a token that was not found, the type is null.
The ID is null too for an internal ID token.
For a typed ID or global ID, the ID only echoes what the caller sent.
Linked items and reviewers include readable records only.
Rails expands only a found, readable record, so the expansion payload holds only the ID of a found record.
That rule saves work and avoids cross-tenant timing on the center, but redaction is the leak control.

Orbit answers only inside the user's graph scope.
The scope is the traversal paths where the user has at least the Reporter role and Orbit is on.
Rails can still read a record outside that scope.
Examples are a Guest, a member of the project only, a non-member on a public project, and a top-level group with Orbit off.
Before the expansion query runs, Rails checks the record path against the scope.
Outside the scope, Rails returns an outside graph scope error, and no query runs.

The limit applies to the whole graph, and redacted rows use slots in the window.
Inside the scope, a missing center means "no readable neighbors" only when the result is not truncated, and is unknown otherwise.
A record that is not indexed yet can return an empty graph, so an empty result does not prove that no neighbors exist.

### Several tokens later

We want the sub-queries of one context request to bill as one query.
A candidate is a shared billable request ID that Rails puts in the signed claims.
Orbit would set the ID on the billing event of each sub-query, and billing could collapse them.
This needs sign-off from the billing owners, who are the Orbit team for emission and Fulfillment for billing mediation.
We have not built it, and the one-token limit stays until we do.
We also defer an explicit Model Context Protocol (MCP) tool and an agent command.

## Consequences

- A context request bills at most one query and uses one rate-limit unit.
  The billing owner must confirm that an expanded context request is at most one billed query.
- An agent with several tokens makes several context calls, and each has its own rate limit and bill.
- The context route and the named query route check the rate limit before the enabled-namespace check.
  Forbidden responses therefore use budget too, and we accept this for the first version.
- Expansion waits for the Rails query rollout, and until then returns an expansion unavailable error.
  Version `1.1.0` covers the whole body, so this decision needs no new Orbit version pin for raw or TOON output.
- A confidential issue tracks authorization-safe pagination, with page flags and cursors computed after redaction.
  That work does not block expansion, because expansion runs only named queries that the caller can already run.
  The fix must keep the truncation flag, because the truncation rule above relies on it.
- The command-line interface (CLI) remote routing for context must use the POST request with tokens and the new response.
  The closed CLI draft used a GET request with references.
  Whether a type and ID pair with a colon is a valid token form is still open.
- We do not decide these future items:
  - More types, such as notes, milestones, users, projects, and groups, through a generic neighbors query.
  - File and definition tokens that resolve from the graph and never from Gitaly, with their own expansion queries.
    The reserved code token forms are typed file and definition IDs, and a blob URL.
    The blob URL holds the project, path, commit SHA, and line range.
  - Request fields for a point in time, a limit, a cursor, and included facets.

## Why not the alternatives

### Several tokens, billed as one query

A shared billable request ID on the billing event of each sub-query would let billing collapse them.
The team prefers this for the multi-token future.
We defer it, because it needs changes in Orbit, Rails, and billing, and sign-off from the billing owners.
The correlation ID cannot do this today.

### One cross-type query

This option is not possible today.
A query stream carries one query, and a neighbors query takes one node selector, so one entity type.
A traversal joins a few selectors, but does not return independent neighborhoods.
A true single query needs batch or query language work in the billing-scoped server paths.
With one token, we do not need it.

### Workhorse fan-out

Rails hands Workhorse a list of named queries, and Workhorse runs the streams in parallel and merges the results.
This option pays off only with several tokens, and it adds Go merge logic and typed transport errors.
We can revisit it together with the multi-token limit.

### Rails execution for several tokens

Several streams from Rails change where redaction and merging run.
They do not change the query count or billing, because Orbit emits one event for each stream.
For one token, Rails execution is the chosen transport.

### Orbit batch multiplexing

One named request would carry several sub-queries, and Orbit would merge the results.
It needs an Orbit executor refactor and sends Rails summaries through Orbit.
It is also a path to the single-event billing above.

### Two calls from the client

The client calls the context endpoint, then runs the expansion query itself.
That costs two round trips but gives the caller control of the query count.
This option stays available, because clients can call the queries directly.

### An object keyed by token

An object keyed by token loses order in some clients and collapses duplicate tokens.
An array of tokens has no ambiguity.

## References

- [Add the Orbit context endpoint to describe a remote graph entity with type-specific detail](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1262)
- [Run Orbit query streams in Rails instead of Workhorse](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1381)
- [Rollout of the feature flag to run Orbit queries in Rails](https://gitlab.com/gitlab-org/gitlab/-/issues/633423)
- [Add Orbit entity context API (closed prototype)](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/254189)
- [Security design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/security.md)
- [ADR 007: Monetization engineering](007_monetization_engineering.md)
- [ADR 008: Workhorse query acceleration](008_workhorse_query_acceleration.md)
- [ADR 011: Agent command surface](011_agent_command_surface.md)
- [ADR 022: Run Orbit queries in Rails instead of Workhorse](022_queries_in_rails.md)
