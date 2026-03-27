# Orbit + DAP integration spec

**Date:** 2026-03-26
**Author:** @michaelangeloio
**Epic:** [gitlab-org&21075](https://gitlab.com/groups/gitlab-org/-/epics/21075)
**Status:** Draft
**Related:** [Orbit usage billing spec](orbit-usage-billing-spec.md)

---

## 1. Problem statement

Orbit (GitLab Knowledge Graph / GKG) has two tools -- `get_graph_schema` and `query_graph` -- that let LLMs query structured graph data instead of making dozens of individual REST API calls. In early testing, this cuts tool call volume and gives agents better answers because they get relational data in one shot.

Today these tools are only reachable via the Rails REST API (`/api/v4/orbit/*`) and the MCP server (`/api/v4/orbit/mcp`). DAP agents can't use them because there's no wiring between the Duo Workflow Service and Orbit.

This spec covers getting Orbit tools into DWS for agentic web chat. For billing and usage tracking, see the [usage billing spec](orbit-usage-billing-spec.md).

---

## 2. Consumption channels

Four ways to consume Orbit, each with different auth and billing:

| Channel | Path | Auth Mechanism | Billing Model | Status |
|---------|------|----------------|---------------|--------|
| **DAP (DWS)** | User -> Rails -> DWS -> Rails -> GKG | User OAuth token (`ai_workflows` scope) | Zero-rated / bundled with Duo seat | **This spec** |
| **Frontend (Dashboard)** | User -> Rails -> GKG (gRPC) | User JWT (HS256, existing) | N/A (included in license) | Implemented |
| **Core Feature** | Internal Rails service -> GKG | System JWT (HS256, existing) | N/A (infrastructure) | Implemented |
| **External Agents (MCP, glab cli)** | Agent -> Rails OAuth -> GKG | OAuth + User JWT (existing) | Metered / paid add-on | Implemented |

DWS already calls Rails APIs on every workflow. When a user starts agentic web chat, Rails creates an OAuth token with the `ai_workflows` scope and passes it to DWS via gRPC metadata. DWS uses this token to call Rails endpoints (directly via `DirectGitLabHttpClient` on .com, or proxied through Workhorse via `ExecutorGitLabHttpClient`).

The Orbit REST API (`/api/v4/orbit/*`) already exists and accepts standard user auth. DWS can call it using the same OAuth token it uses for every other Rails API call. No new auth mechanism is needed for the integration itself -- DWS is already authenticated.

For billing verification (proving a request came from DWS for zero-rating), see the [usage billing spec](orbit-usage-billing-spec.md).

---

## 3. Architecture

### 3.1 Request flow (DWS channel)

```
User (Web Chat)
  │
  ▼
GitLab Rails
  │  (1) Creates OAuth token (ai_workflows scope)
  │  (2) Opens gRPC stream to DWS with token in metadata
  ▼
DWS (Python/LangGraph)
  │  (3) Agent selects Orbit tool based on user query
  │  (4) DWS calls Rails REST API:
  │      POST /api/v4/orbit/query
  │      GET  /api/v4/orbit/schema
  │      GET  /api/v4/orbit/tools
  │      Auth: Bearer <user_oauth_token> (existing)
  ▼
GitLab Rails
  │  (5) Validates OAuth token (ai_workflows scope)
  │  (6) Proxies to GKG via gRPC (existing flow)
  │  (7) Fires usage event
  ▼
GKG Service (Rust/gRPC)
  │  (8) Executes query with redaction
  │  (9) Returns result
  ▼
Response bubbles back: GKG -> Rails -> DWS -> Agent -> User
```

### 3.2 Why DWS -> Rails -> GKG (not DWS -> GKG directly)

1. Rails owns authorization -- the bidirectional streaming redaction protocol requires Rails to authorize each resource. DWS can't do this.
2. GKG runs in a customer-controlled environment (SM/Dedicated). DWS is a cloud service. Rails is the bridge.
3. DWS already calls Rails for every other tool (`GitLabApiGet`, `GitLabGraphQL`, etc.). Orbit tools follow the same pattern.
4. Avoids adding a gRPC client to DWS and managing a separate auth channel.

---

## 4. Integration paths

There are two viable paths for getting Orbit tools into DWS. Both use the existing Orbit REST API (`/api/v4/orbit/*`) as the data layer. The choice affects how tools are discovered, registered, and executed.

### 4.1 Path A: first-class MCP server

Treat the Orbit MCP server as a first-class citizen in `McpConfigService`, the same way the GitLab MCP server (`gitlab_search`, `search`, `semantic_code_search`) is hardcoded today. The Orbit MCP server would be defined in code behind a feature flag -- no admin setup, no OAuth flow for users.

**How it works today for the GitLab MCP server:**
1. `McpConfigService` builds a config hash with the server URL, auth headers, and a tool allowlist
2. Workhorse receives the config, opens an MCP session to the server, calls `ListTools`
3. Workhorse filters tools against the allowlist, prefixes names with the server name
4. Tool definitions (name, description, input schema) are passed to DWS as `McpTool` protobufs
5. When the agent calls a tool, DWS sends a `RunMCPTool` action back to Workhorse, which proxies to the MCP server

**What we'd add for Orbit:**
- Add an `orbit` entry to `McpConfigService` (similar to the `gitlab` entry), gated behind the `knowledge_graph` feature flag
- The MCP server URL is `{gitlab_url}/api/v4/orbit/mcp` (already implemented in Rails)
- Auth uses the same user OAuth token already available in `McpConfigService`
- Tool allowlist: `query_graph`, `get_graph_schema`
- Include for all workflow definitions (not just `chat` like the GitLab MCP server)

**Advantages:**
- No DWS code changes at all -- MCP tool loading is already dynamic
- Tool descriptions come from GKG automatically (the MCP `ListTools` response includes them)
- Workhorse handles tool execution proxying
- Can be feature-flagged on/off without deploying DWS
- Users can also create custom agents with Orbit MCP tools via the catalog (once connected)
- Good for rapid internal testing and proof of concept

**Disadvantages:**
- MCP JSON-RPC overhead for what is ultimately a REST call
- Tool execution goes DWS -> Workhorse -> Rails MCP -> GKG (extra hop through Workhorse)
- Less control over tool behavior (no custom caching, retry, or error handling in DWS)
- MCP tool descriptions get prefixed with `orbit_` and an untrusted warning banner

### 4.2 Path B: native DWS tools

Create two new `DuoBaseTool` subclasses in DWS that call the Orbit REST API directly using the existing `GitLabHttpClient` (which already carries the user's OAuth token).

**Two tools:**

| Tool | REST endpoint | Purpose |
|------|---------------|---------|
| `orbit_query_graph` | `POST /api/v4/orbit/query` (response_format=llm) | Execute a graph query |
| `orbit_get_graph_schema` | `GET /api/v4/orbit/schema` (response_format=llm) | Get the ontology |

**Tool descriptions from the API:** Both tools fetch their LLM-facing descriptions from `GET /api/v4/orbit/tools` at workflow start. This endpoint returns the tool name, description (with embedded DSL schema), and parameter definitions. The descriptions are cached for the workflow's lifetime -- no cross-process cache needed since each workflow is short-lived.

**Registration:** Add an `"orbit"` entry to the DWS `ToolsRegistry` agent privileges. Rails controls enablement via `agent_privileges_names` in the `WorkflowConfig` GraphQL response -- when Orbit is enabled for the user's namespace, Rails includes `"orbit"` in the privileges list. DWS only registers the tools when the privilege is present. Same mechanism as every other tool.

**BuiltInTool catalog entries:** Add `orbit_query_graph` and `orbit_get_graph_schema` to the `BuiltInToolDefinitions` list in Rails (currently 89 tools). This lets users select Orbit tools when creating custom agents in the catalog.

**Advantages:**
- Direct REST calls -- simpler data flow (DWS -> Rails -> GKG, no Workhorse hop)
- Full control over tool behavior: caching, error handling, response parsing, retry logic
- Can add Orbit-specific logic (e.g., check if result is empty and suggest fallback in tool output)
- Tool descriptions fetched from `GET /api/v4/orbit/tools` stay in sync with the ontology
- Tools are selectable in the catalog for custom agent creation

**Disadvantages:**
- Requires DWS code changes (new tool classes, registry update)
- Requires Rails changes (BuiltInTool entries, privilege wiring)
- Tool descriptions need explicit fetch logic (MCP gets them for free via ListTools)

### 4.3 Recommendation

Both paths work. They're not mutually exclusive -- we can do MCP first for rapid internal testing (proof of concept behind a feature flag, no DWS deploy needed), then build native tools for production.

**Phase 1 (internal testing):** MCP path. Define Orbit as a first-class MCP server in `McpConfigService` behind the existing `knowledge_graph` feature flag. This lets us test Orbit tools in agentic chat on staging without any DWS code changes. Custom agents can also use the Orbit MCP server.

**Phase 2 (production):** Native DWS tools. Build the `DuoBaseTool` subclasses that call the REST API directly, add to `BuiltInToolDefinitions` for catalog selection. This gives us the control we need for production quality: proper error handling, description caching from `GET /api/v4/orbit/tools`, and no MCP overhead.

### 4.4 Fallback to standard tools

When Orbit returns empty results (replication lag -- entity not yet indexed), the agent should fall back to existing tools like `GitLabApiGet`. This is handled by the skill prompt (section 7), not by tool-level logic. The agent sees both Orbit tools and standard tools and picks the right one based on the prompt guidance.

`GitLabApiGet` already exists as a generic fallback for any Rails API endpoint.

---

## 5. GitLab Rails changes

### 5.1 Orbit API scope update

The Orbit REST API (`/api/v4/orbit/*`) needs to accept the `ai_workflows` OAuth scope so DWS can call it. The `AiWorkflowsAccess` concern is already used by dozens of other APIs that DWS calls -- adding it to the Orbit endpoints is a one-liner.

### 5.2 MCP config update (Path A)

Add Orbit as a first-class MCP server in `McpConfigService`, gated behind the existing `knowledge_graph` feature flag. When enabled, the service includes an `orbit` entry pointing at `/api/v4/orbit/mcp` with the user's OAuth token and a tool allowlist of `query_graph` and `get_graph_schema`.

Unlike the GitLab MCP server (which is only included for `workflow_definition == 'chat'`), the Orbit MCP entry should be included for all workflow definitions so it's available to foundational flows like `developer` too.

### 5.3 Caller identification for metrics

The Orbit API should detect whether the caller is DWS and tag the request accordingly in metrics. This gives us visibility into DAP-originated Orbit usage from day one, before the full usage billing implementation (see [billing spec](orbit-usage-billing-spec.md)).

For the initial integration, a simple check on the OAuth token's scope is sufficient:

```ruby
# In Orbit::Data API:
def caller_channel
  if current_token&.scopes&.include?('ai_workflows')
    :dws
  elsif current_token&.scopes&.include?('mcp_orbit')
    :mcp
  else
    :frontend
  end
end
```

The `ai_workflows` scope is exclusive to DWS -- users can't create PATs or standard OAuth tokens with it. This isn't tamper-proof (a user could extract the token and replay it), but it's good enough for metrics. The billing spec covers the HMAC-based verification needed for actual zero-rating.

Log the channel on every Orbit API request so we can answer:
- How many Orbit queries come from DWS vs. MCP vs. frontend?
- What query types does the DWS agent use most?
- What's the latency profile per channel?

**Files to modify:**
- `ee/lib/api/orbit/data.rb` -- detect caller channel, include in structured logging / internal events

### 5.4 WorkflowConfig update (Path B)

**Files to modify:**
- Rails GraphQL that returns `WorkflowConfig` -- include `"orbit"` in `agent_privileges_names` when Orbit is available for the user's namespace.

```ruby
# In WorkflowConfig resolver:
if Feature.enabled?(:knowledge_graph, user)
  privileges << "orbit"
end
```

DWS picks up the privilege automatically. No DWS config changes needed.

---

## 6. GKG service changes

### 6.1 Caller type in JWT claims (follow-up, not MVP)

For MVP, GKG doesn't need to know the caller type. The user's JWT carries the same traversal IDs regardless of whether the request came from the dashboard or DWS.

**Follow-up:** Add an optional `caller_type` claim for telemetry:

```rust
pub struct Claims {
    // ... existing fields ...
    pub caller_type: Option<CallerType>,  // new
}

pub enum CallerType {
    User,       // Frontend dashboard
    Dws,        // Duo Workflow Service / DAP
    Mcp,        // External MCP agent
    System,     // Internal Rails service
}
```

### 6.2 No REST API on GKG

ADR 003 established that Rails is the REST proxy. DWS calls Rails, not GKG. No new endpoints on the Rust service.

---

## 7. Prompt engineering

### 7.1 Why prompting matters here

Adding Orbit tools to an agent isn't just a tool registration problem. The tool descriptions alone are ~3k tokens each (they embed the full query DSL schema). Dropping them into an already-crowded tool list without guidance can confuse the agent about when to use them vs. standard tools, dilute the context window, and lead to worse performance overall. The team explicitly flagged this concern: "more tools create more noise, so you have to convince your agent to use the tools."

The prompt needs to cover three things:
1. **When to prefer Orbit** over standard tools (relational queries, aggregations, cross-entity lookups)
2. **When NOT to use Orbit** (just-created entities, full file contents, job logs, non-default branches)
3. **How to use Orbit well** (progressive disclosure pattern, multi-step queries, DSL constraints)

### 7.2 Replication lag handling

Orbit data is not real-time (yet). The pipeline (PostgreSQL -> Siphon CDC -> NATS -> ClickHouse -> GKG Indexer) introduces a short delay. Current estimates put lag at a few seconds under normal load, but this hasn't been validated at scale. Siphon has a 500MB/min throughput limit.

GKG tracks data freshness via a `gkg.indexer.sdlc.watermark.lag` metric (seconds between the indexing watermark and wall clock). Each namespace also has a `last_indexed_at` timestamp in ClickHouse.

**How the agent should handle this:**
- Default to Orbit for any entity that isn't brand new
- If the user references something they just created ("the issue I just filed", "the MR I just opened"), try Orbit first but expect it might return empty
- On empty results for a recently-referenced entity, fall back to `GitLabApiGet` (which hits the Rails REST API directly and is always up to date)
- Don't try to use Orbit for full file contents, job logs, or diff content -- those aren't in the graph. Use `GitLabApiGet` or `read_file` instead

### 7.3 Progressive disclosure pattern

The `get_graph_schema` tool is designed for a two-step discovery pattern:

1. **First call** (no arguments): returns a compact listing of domains, node types, and edge types. Costs minimal tokens.
2. **Second call** (`expand_nodes: ["User", "MergeRequest"]`): returns full property lists and relationship details for only the relevant types.

The agent should never call `expand_nodes: ["*"]` unless specifically asked to describe the entire schema. Expanding all nodes wastes tokens and provides information the agent doesn't need for a focused query.

### 7.4 Prompt content

The skill prompt should guide the agent on tool selection, replication lag, and query patterns:

```
You have access to the GitLab Knowledge Graph (Orbit), which contains
structured data about this instance's projects, issues, merge requests,
pipelines, vulnerabilities, users, and code.

WHEN TO USE ORBIT TOOLS:
- Questions involving multiple entity types ("MRs that fix vulnerabilities")
- Aggregations ("how many pipelines failed this week")
- Relationship traversals ("who authored the MR that fixed this bug")
- Cross-entity search ("find all critical vulnerabilities in group X")
- Neighbor discovery ("what's connected to this merge request")
- Path finding ("how are user X and vulnerability Y related")

WHEN TO USE STANDARD TOOLS INSTEAD:
- The entity was just created (seconds/minutes ago) -- it may not be
  indexed yet. Try Orbit first; if empty, fall back to GitLabApiGet.
- You need full file contents, job logs, or diff content -- not in the graph.
- You need real-time pipeline status or deployment state.
- The user references a specific resource by URL -- fetch it directly.

HOW TO USE ORBIT WELL:
1. Call get_graph_schema first to discover available entity types.
   Only expand the nodes relevant to the question.
2. Construct a query_graph call using the DSL. The DSL supports
   traversal (multi-hop), aggregation, search, pathfinding, and
   neighbor queries.
3. Query limits: max 3 hops, max 1000 results, max 500 node_ids
   per selector. Filter early to stay within limits.
4. Results come in GOON format (compact text optimized for LLMs).

REPLICATION LAG:
Orbit data is eventually consistent. If a query returns no results
for an entity the user just mentioned, fall back to GitLabApiGet.
Do not tell the user the data doesn't exist -- it may just not be
indexed yet.
```

### 7.5 Where prompts live (phased)

**Phase 1, option A -- custom agent prompt:**
When creating a custom Orbit agent in the catalog, the system prompt above is embedded directly in the agent definition. This is the simplest path -- no code changes in DWS, the prompt is just part of the catalog agent's configuration. Anyone creating or cloning the agent can iterate on the prompt.

**Phase 1, option B -- inline in DWS flow config:**
If we decide to have Orbit tools embedded in foundational LangGraph workflows (like `developer_next`), the prompt goes inline in the flow config YAML under the component's `prompts` section. This gives us tighter control over how Orbit integrates with multi-step flows but requires a DWS deploy to iterate.

**Phase 2 -- fetched from GKG API:**
The prompts move to a new REST endpoint (`GET /api/v4/orbit/skills` or `GET /api/v4/orbit/prompts`) served by Rails (proxying to GKG). This lets the GKG team iterate on prompts without touching the catalog or DWS code. The endpoint returns skill prompts keyed by agent context (e.g., `developer`, `security_analyst`, `general_chat`), so different agents can get tailored guidance.

Eventually this could feed into whatever prompt catalog DAP builds. The GKG repo remains the source of truth for Orbit-specific prompting since the prompts are tightly coupled to the ontology and query DSL.

---

## 8. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Replication lag causes empty Orbit results | Agent gives wrong answer | Skill prompt instructs fallback; Duo Evals measure fallback success rate |
| Tool description too large for LLM context | Agent can't use Orbit tools | TOON-encoded descriptions are ~3k tokens each; within budget |
| Orbit query latency degrades agent response time | Slower agent responses | Measure in Duo Evals; ClickHouse queries are typically <200ms |
| `ai_workflows` scope too broad for Orbit | Security concern | Orbit endpoints already require `read_knowledge_graph` permission; scope just allows the token type |
