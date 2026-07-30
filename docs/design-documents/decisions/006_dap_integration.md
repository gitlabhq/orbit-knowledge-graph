---
title: "GKG ADR 006: Orbit + Duo Agent Platform integration"
creation-date: "2026-03-26"
last-updated: "2026-07-08"
authors: [ "@michaelangeloio", "@dgruzd", "@jgdoyon1", "@michaelusa", "@bohdanpk", "@thomas-schmidt", "@shekharpatnaik", "@eduardobonet" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-03-26

## Context

Orbit (the GitLab Knowledge Graph, GKG) exposes agent-facing tools that let
LLMs query structured graph data instead of assembling answers from dozens of
REST calls. `query_graph` executes a query written in the condensed graph DSL.
`get_graph_schema` returns the ontology with progressive disclosure.
`list_commands` and `invoke_command` form the named-command surface
([ADR 011](011_agent_command_surface.md)). Giving agents these tools cuts
tool-call volume and produces better answers because the agent receives
relational data in one shot.

These tools are reachable through the Rails REST API (`/api/v4/orbit/*`) and
the Orbit MCP server (`/api/v4/orbit/mcp`). The Duo Agent Platform (DAP)
executes agents in the Duo Workflow Service (DWS), a separate service with no
direct connection to GKG. This document defines how Orbit tools reach DAP
agents.

Billing and usage tracking are covered separately in
[ADR 007](007_monetization_engineering.md).

### Consumption channels

Orbit has four consumption channels, each with its own auth and billing
treatment:

| Channel | Path | Auth | Billing |
|---------|------|------|---------|
| DAP (DWS) | User -> Rails -> DWS -> Workhorse -> Rails (Orbit MCP) -> GKG | User OAuth token (`ai_workflows` scope) | Zero-rated, bundled with the Duo seat |
| Frontend (dashboard) | User -> Rails -> GKG (gRPC) | User JWT (HS256) | Included in the license |
| Core features | Internal Rails service -> GKG | System JWT (HS256) | Infrastructure, not billed |
| External agents (MCP, `glab` CLI) | Agent -> Rails OAuth -> GKG | OAuth + user JWT | Metered, paid add-on |

DWS authenticates to Rails with an OAuth token carrying the `ai_workflows`
scope, created by Rails when the workflow starts. All GitLab API traffic from
DWS is proxied through Workhorse by the executor HTTP client; DWS makes no
direct HTTP calls to Rails. The Orbit REST API accepts the `ai_workflows`
scope through the same shared concern other DWS-called APIs use, so the
integration needs no new data-plane auth mechanism. The scope is broader than
Orbit, so every Orbit route also requires the `read_knowledge_graph`
permission; the scope only admits the token type.

## Decision

Orbit is a first-class MCP server in the Duo Agent Platform. Rails defines the
`orbit` server in its MCP configuration service the same way it defines the
built-in GitLab MCP server, Workhorse hosts the MCP session, and DWS consumes
the tools like any other MCP tools. There are no Orbit-specific tool classes
in DWS.

### Request flow

```plaintext
User (agentic chat, foundational agent, or catalog agent)
  |
  v
GitLab Rails
  |  creates an OAuth token (ai_workflows scope)
  |  starts the workflow in DWS over gRPC
  v
DWS (Python / LangGraph)
  |  fetches the MCP server config that Rails assembles per workflow
  |  the agent selects an Orbit tool; DWS emits a RunMCPTool action
  v
Workhorse
  |  holds the MCP session to the Orbit server
  |  forwards the tool call to POST /api/v4/orbit/mcp
  |  stamps the channel identity header
  v
GitLab Rails
  |  validates the OAuth token and the read_knowledge_graph permission
  |  proxies to GKG over gRPC and answers redaction callbacks
  |  fires usage events
  v
GKG (Rust)
  |  executes the query with redaction
  v
Result returns through Rails and Workhorse to DWS, then to the agent
```

The REST data endpoints (`POST /api/v4/orbit/query`,
`GET /api/v4/orbit/schema`) remain available for other consumers. The DAP
path does not use them for tool execution.

### Why DWS -> Rails -> GKG, not DWS -> GKG

1. Rails owns authorization. The bidirectional streaming redaction protocol
   requires Rails to authorize each resource in a result set, and DWS cannot
   do that.
2. GKG runs wherever the GitLab instance runs, including self-managed and
   Dedicated environments, and DWS deployment topologies vary as well. Rails
   is the one component that always sits next to GKG and holds the
   authorization context.
3. DWS already reaches every other GitLab capability through Rails. Orbit
   follows the same principle, delivered over MCP rather than as native
   tools.
4. DWS never needs its own gRPC client to GKG or a separate auth channel. The
   only gRPC clients to GKG live in Rails and Workhorse.

Workhorse is a deliberate part of this path, not incidental proxying. It
hosts the MCP client, resolves the internal endpoint path for the built-in
servers, forwards session identifiers, mints the channel identity header, and
accelerates query execution by streaming results from GKG directly
([ADR 008](008_workhorse_query_acceleration.md)).

### First-class MCP server

For the built-in `gitlab` and `orbit` servers, the Rails MCP configuration
service supplies auth headers (the user's OAuth token), the pre-approved tool
set, and a trusted flag. It does not supply a URL; Workhorse resolves the
internal endpoint path from the server name. Workhorse opens the MCP session,
lists the tools, filters them against the allowlist, prefixes each name with
the server name (`orbit_query_graph` and so on), and passes the definitions to
DWS as protobuf messages. When the agent calls a tool, DWS sends a
`RunMCPTool` action back through Workhorse, which proxies it to the server.

Because Rails marks the Orbit server trusted, and DWS derives tool trust from
that flag, Orbit tool descriptions carry no untrusted-source warning banner.
The name prefix is the only transformation.

The trusted tool set is `query_graph`, `get_graph_schema`, `list_commands`,
and `invoke_command`. A feature flag switches the visible surface between the
legacy query pair and the named-command pair, so the legacy tools can be
retired without changing the transport.

This shape has three properties the design depends on:

- No per-tool code in DWS. Tool loading is dynamic, and descriptions come
  from the MCP tool listing, so they stay in sync with the ontology.
- Rollout and rollback are flag-driven and need no DWS deploy.
- Custom agents in the AI Catalog can select Orbit tools like any other MCP
  tools.

### Gating

Enablement runs through a single Rails facade rather than scattered
feature-flag checks. The facade layers:

1. Platform kill switches: the `knowledge_graph` and
   `orbit_foundational_agent` feature flags. Either one off disables Orbit
   everywhere.
2. The MCP client flag, which gates whether Rails sends any MCP servers to
   DWS at all.
3. A per-user preference: a master killswitch plus one subsetting per surface
   (agentic chat, the standalone Orbit agent, other foundational agents, and
   custom catalog agents).

When a workflow starts, Rails classifies it into one of those surfaces and
consults the matching predicate. If the predicate passes, the `orbit` server
is added to the MCP payload. If not, the entry is absent and the agent never
learns Orbit exists for that run. Duo Code Review is excluded regardless of
the other settings: it is flat-rate and has not adopted Orbit deliberately.

### Surfaces

- Agentic chat and the standalone Orbit foundational agent receive the Orbit
  MCP tools directly.
- Other foundational agents, such as the data-analyst agent, receive the
  tools through the same injection, with guidance in their flow definitions.
- Custom catalog agents select Orbit tools per agent. The injected tool list
  is the intersection of the agent's selection and the pre-approved set.

Duo Developer takes a different route and does not use the MCP tools at all.
The flow version resolver swaps in an Orbit variant of the developer flow when
Orbit is enabled for the user. That variant is a single agent with shell
access in its execution environment, and its system prompt includes a shared
Orbit skill that teaches the agent to reach Orbit through the
pre-authenticated `glab orbit` CLI: discover the schema and query DSL first,
then run queries against the same REST API the other channels use. The CLI
also serves a local graph for code-structure questions about the current
branch, which the remote graph does not cover. Flows that already give the
agent a shell get Orbit this way for free; the MCP path exists for the
surfaces that do not.

### Caller identification

Every Orbit request is classified by source: frontend, DWS, MCP, REST, code
intelligence, or core. Browser requests are detected by a verified session,
DWS by the `ai_workflows` scope, and the MCP surface by its endpoint; other
authenticated callers default to REST. The classification travels to GKG as a
required source-type claim on the JWT, which matches the telemetry enum, so
per-channel usage, query mix, and latency can be answered from either side of
the gRPC boundary.

Scope-based detection is enough for metrics but is not tamper proof, since a
user could extract an `ai_workflows` token and replay it. For attribution
strong enough to support zero-rating, Workhorse stamps Orbit calls that
originate from DWS with a short-lived signed channel header that Rails
verifies. [ADR 007](007_monetization_engineering.md) covers that mechanism.

### GKG service changes

GKG needs no new endpoints for this integration.
[ADR 003](003_api_design.md) established Rails as the REST proxy, and that
holds: GKG serves gRPC only, and capabilities added for agents (schema and
DSL discovery, named-query execution) are gRPC methods. The JWT claims carry
the source type described above; traversal IDs and redaction behave the same
for every channel.

### Prompt engineering

Adding Orbit tools to an agent takes more than registering them. Each tool
description embeds the full query DSL grammar (TOON-encoded), which makes
descriptions large, on the order of a few thousand tokens. Dropped into an
already crowded tool list without guidance, they can confuse the agent about
when to prefer them and dilute the context window. The prompt has to cover
when to prefer Orbit, when to avoid it, and how to use it well.

#### Replication lag

Orbit data is not real time. The pipeline (PostgreSQL to Siphon CDC to NATS
to ClickHouse to the GKG indexer) introduces a short delay. GKG tracks
freshness with a watermark lag metric (seconds between the indexing watermark
and wall clock), and each namespace has a last-indexed timestamp.

Agents should default to Orbit for anything that is not brand new, expect an
empty result for entities the user just created, and fall back to the generic
REST tool when that happens. An empty result should never be reported to the
user as "does not exist". Empty-result tool responses are also a good place
to carry the fallback hint, so the guidance does not rely on the system
prompt alone. Full file contents, job logs, and diff content are not in the
graph; agents should use the standard tools for those.

#### Progressive disclosure

`get_graph_schema` supports a two-step pattern. The first call, with no
arguments, returns a compact listing of domains, node types, and edge types.
A second call with `expand_nodes` returns full property and relationship
detail for only the named types. Expanding everything is reserved for when
the user asks to describe the whole schema.

#### Query guidance

Prompt content for Orbit-aware agents should teach:

- When to use Orbit: questions that span entity types, aggregations,
  relationship traversals, cross-entity search, neighbor discovery, and path
  finding.
- When to use standard tools instead: just-created entities, full file
  contents, job logs or diffs, real-time pipeline or deployment state, or a
  resource the user referenced by URL.
- How to query: discover the schema first and expand only the relevant
  nodes, respect the server limits (3 hops, 1,000 results, 500 node IDs per
  selector) and filter early, and read results as GOON, the compact text
  format for LLM consumption ([ADR 012](012_goon_format.md)).

Guidance in flow definitions must stay consistent with the server limits and
with itself. A prompt that names the wrong depth ceiling, or encourages
expanding the whole schema, directly degrades agent behavior and is treated
as a defect.

#### Where prompts live

Orbit prompt content lives inline in the DWS flow configurations and a shared
skill partial, maintained alongside the agent definitions. That keeps prompts
under flow version pinning, keeps prompt changes reviewable in one place, and
avoids a runtime fetch on every workflow. The GKG repository documents the
intended guidance; the flow definitions are the runtime source of truth.

## Consequences

What this enables:

- Agents get relational answers in one tool call instead of chaining REST
  calls.
- New Orbit capabilities, such as named commands or schema changes, reach
  agents without DWS or Rails deploys, because tool descriptions are fetched
  at session start.
- Per-surface gating and per-user preferences allow gradual rollout and a
  quick kill.

What this requires:

- Latency expectations must account for the whole path, DWS to Workhorse to
  Rails to GKG and back, not just query execution. Simple lookups return
  quickly; multi-hop traversals and aggregations can be much slower and can
  reach the query timeout.
- The MCP JSON-RPC framing adds overhead compared with a direct REST call.
  Workhorse query acceleration recovers most of it on the expensive path.
- Tool behavior such as caching, retries, and error shaping is controlled at
  the MCP server and prompt level, not by custom DWS code.

## Alternatives considered

### Native DWS tools

Build Orbit tool classes in DWS that call the Orbit REST API directly,
registered through a dedicated agent privilege and listed in the Rails
built-in tool catalog. Rejected: it duplicates tool definitions in three
places (DWS classes, Rails catalog entries, GKG descriptions), needs explicit
description-fetch logic that MCP provides for free, and couples Orbit
iteration to DWS and Rails deploys.

### Direct DWS to GKG connection

Give DWS its own gRPC client to GKG. Rejected: Rails owns authorization and
the redaction protocol, and GKG is not reachable from cloud services in
self-managed and Dedicated topologies.

### GKG-served prompts

Serve agent prompt content from a REST endpoint so the GKG team could iterate
on prompts without touching flow definitions. Rejected in review: prompts in
flow definitions are version-pinned, reviewable, and traceable, and a runtime
prompt fetch would break pinning while adding a query to every workflow.

## References

- [ADR 003: Orbit API design](003_api_design.md), Rails as the REST proxy
- [ADR 007: Orbit monetization engineering](007_monetization_engineering.md),
  billing, quotas, and channel attribution
- [ADR 008: Workhorse query acceleration](008_workhorse_query_acceleration.md)
- [ADR 011: Agent command surface](011_agent_command_surface.md),
  `list_commands` and `invoke_command`
- [ADR 012: GOON format](012_goon_format.md)
- [Duo / Orbit prompt routing architecture](../duo_orbit_prompt_routing.md),
  the consumer-side map of the Rails routing seams
- [Security](../security.md), the redaction and authorization model
