---
title: "GKG ADR 003: Orbit API Design — Unified REST + GraphQL"
creation-date: "2026-02-26"
authors: [ "@michaelangeloio" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-02-26

## Context

The Orbit dashboard needs APIs to serve three consumers:

1. **Dashboard frontend** — Vue app at `/dashboard/orbit` rendering graphs, tables, schema viewers
2. **MCP agents** — AI tools calling `tools/call("query_graph")` and `tools/call("get_graph_schema")`
3. **`glab` CLI** — `glab orbit query "..." --format=llm|human`

The current GKG gRPC service has 5 RPCs:

| RPC | Type | Purpose |
|-----|------|---------|
| `ListTools` | Unary | Returns 2 tool definitions |
| `ExecuteTool` | Bidi streaming | Generic tool dispatch (routes to `query_graph` or `get_graph_schema`) |
| `ExecuteQuery` | Bidi streaming | Raw query execution |
| `GetOntology` | Unary | Full schema as protobuf |
| `GetClusterHealth` | Unary | Cluster health status |

### Problems with the current design

1. **`ExecuteTool` is unnecessary indirection.** There are only 2 tools. `ExecuteTool("query_graph", args)` is functionally identical to `ExecuteQuery` — same pipeline, different formatter. `ExecuteTool("get_graph_schema")` is functionally identical to `GetOntology` — same data, different serialization (TOON text vs protobuf).

2. **`GetOntology` and `get_graph_schema` are the same data.** Both return the graph schema. `GetOntology` returns structured protobuf (domains, nodes with properties/styles, edges with variants). `get_graph_schema` returns TOON text with optional `expand_nodes` for selective detail. The only difference is format and granularity.

3. **`ExecuteTool` and `ExecuteQuery` are the same pipeline.** Both go through: security → compile → ClickHouse → extract → authorize → redact → hydrate → format. The only difference is the formatter: `ContextEngineFormatter` (for tools, context-engineered output) vs `RawRowFormatter` (for raw queries, tabular JSON).

4. **No REST API exists.** The dashboard frontend has no way to call these operations without going through GraphQL or a Rails controller action. Agents have to go through MCP JSON-RPC. The `glab` CLI has no entry point at all.

## Decision

### Split by domain: GraphQL for settings, REST for GKG data

**GraphQL** for admin/settings operations (per Adam Hegyi's reviews on [!224831](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/224831)):

Adam's key feedback ([note 3115671286](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/224831#note_3115671286)):

> We can save quite a lot of time if we don't build a new API. We already have one for loading groups.
> The API supports the `search` parameter already! I believe we can add the Orbit enabled/disabled setting to the payload so we can also restore the checkbox status.

```graphql
query {
  groups(ownedOnly: true, topLevelOnly: true) {
    nodes { id, name, fullPath, webUrl }
  }
}
```

Adam's follow-up ([note 3118324266](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/224831#note_3118324266)):

> All of these settings need to be implemented within the `EE` namespace (since these are licensed fields). Also, updating groups setting can happen via the existing `groupUpdate` mutation, or we just simply add a mutation for Orbit (we might have more configurable options for the feature).

```graphql
mutation {
  orbitUpdate(input: { groupPath: "your-group-path", enabled: true }) {
    group { id, name, description, visibility }
    errors
  }
}
```

Implementation approach:

- Namespace KG enablement — leverages existing `groups` query with new `knowledgeGraphEnabled` field
- Dedicated `orbitUpdate` mutation (not `groupUpdate`) for Orbit-specific settings — room to add more configurable options later
- All under `EE` namespace per [EE features guide](https://docs.gitlab.com/development/ee_features/)

**REST** for GKG data operations:

- Query execution, schema retrieval, health, tools
- Dual-format responses (`?format=raw|llm`) for human and agent consumption
- Reusable by any consumer (frontend, CLI, agents, external integrations)

### Simplify gRPC from 5 RPCs to 4

```plaintext
Current (5 RPCs)                        →  Proposed (4 RPCs)
──────────────────────────                  ──────────────────────────
ListTools                               →  ListTools
ExecuteTool("query_graph", args)         →  ExecuteQuery(query, format: llm)
ExecuteQuery(query)                 →  ExecuteQuery(query, format: raw)
ExecuteTool("get_graph_schema", args)  →  GetGraphSchema(expand_nodes, format: llm)
GetOntology()                           →  GetGraphSchema(format: raw)
GetClusterHealth()                      →  GetClusterHealth
```

**Removed:** `ExecuteTool` (generic dispatch), `GetOntology` (subsumed by `GetGraphSchema`)

### Unified `ResponseFormat` across all data RPCs

The `format` parameter answers one question: **who is consuming this response?**

```protobuf
enum ResponseFormat {
  RESPONSE_FORMAT_RAW = 0;
  RESPONSE_FORMAT_LLM = 1;
}
```

| RPC | `format = raw` | `format = llm` |
|-----|----------------|----------------|
| `ExecuteQuery` | Tabular JSON rows + `QueryMetadata` | [GOON](https://gitlab.com/gitlab-org/gitlab/-/snippets/4929205) (Graph Object Output Notation) — deduplicated nodes/edges, 25-50% token savings |
| `GetGraphSchema` | Structured schema (domains, nodes, edges, properties, styles) | [TOON](https://github.com/toon-format/spec/blob/main/SPEC.md) text (`{name: "User", props: ["id:int", ...], out: [...]}`) |
| `GetClusterHealth` | Structured health (status, version, components) | [TOON](https://github.com/toon-format/spec/blob/main/SPEC.md) — compact key-value notation |
| `ListTools` | Tool definitions (name, description, parameters_json_schema) | Same (no LLM variant needed) |

The detailed response format specification — including the unified response envelope (`metadata`, `rows`, `graph`), GOON encoding, and the shared JSON Schema contract between the Rust backend and the Vue frontend — will be covered in a separate follow-up ADR. The design research for this is tracked in [snippet 5965027](https://gitlab.com/gitlab-org/gitlab/-/snippets/5965027) (Michael Usachenko's proposal) and [snippet 5965036](https://gitlab.com/gitlab-org/gitlab/-/snippets/5965036) (Angelo's extension with Kuzu-inspired uniform model).

---

## Access Control

All Orbit code lives in `ee/` and is gated by the `:knowledge_graph` feature flag. The `:orbit` entry already exists in `PREMIUM_FEATURES` (added in [!224832](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/224832)).

Billing, tier gating, instrumentation, and monetization are tracked separately in [GKG Monetization Engineering &21198](https://gitlab.com/groups/gitlab-org/-/work_items/21198).

---

## REST API Specification

### Base path: `/api/v4/orbit`

All endpoints require authentication (personal access token, session cookie, or OAuth token). All gated behind `:knowledge_graph` feature flag.

### Common parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `format` | string | `raw` | Response format: `raw` (structured JSON) or `llm` ([GOON](https://gitlab.com/gitlab-org/gitlab/-/snippets/4929205) for query results, [TOON](https://github.com/toon-format/spec/blob/main/SPEC.md) for schema) |
| `query_type` | string | `json` | Query language: `json` (structured DSL). Future: `cypher` |

### `POST /api/v4/orbit/query`

Execute a Knowledge Graph query.

**Request body (pseudo code):**

```json
{
  "query": {
    "query_type": "traversal",
    "node": {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": { "state": "merged" }
    },
    "limit": 10
  },
  "query_type": "json",
  "format": "raw"
}
```

**Response (`format=raw`) — pseudo code:**

```json
{
  "result": [
    { "_id": "123", "_type": "MergeRequest", "title": "Fix bug", "state": "merged" }
  ],
  "query_type": "traversal",
  "raw_query_strings": ["SELECT ... FROM merge_requests WHERE ..."],
  "row_count": 10
}
```

**Response (`format=llm`) — pseudo code:** [GOON format](https://gitlab.com/gitlab-org/gitlab/-/snippets/4929205) — deduplicated graph with 25-50% token savings

```json
{
  "result": "@goon{v:1,org:123}\nnodes:\n  MergeRequest[10]{id,iid,title,state,author_id}:\n    501,42,\"Fix auth bug\",merged,1\n    ...",
  "query_type": "traversal",
  "raw_query_strings": ["SELECT ..."],
  "row_count": 10
}
```

**gRPC mapping:** `ExecuteQuery(query, format, query_type)`
**Streaming:** Bidi (redaction exchange handled internally by Rails `GrpcClient`)
**Auth scoping:** JWT includes user's traversal IDs; results are redacted per user authorization

---

### `GET /api/v4/orbit/schema`

Retrieve the Knowledge Graph schema (ontology).

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `expand` | string (comma-separated) | _(none)_ | Node names to expand with full properties and relationships |
| `format` | string | `raw` | `raw` or `llm` |

**Request examples:**

```plaintext
GET /api/v4/orbit/schema
GET /api/v4/orbit/schema?expand=User,Project,MergeRequest
GET /api/v4/orbit/schema?format=llm
GET /api/v4/orbit/schema?expand=User&format=llm
```

**Response (`format=raw`, no expand) — pseudo code:**

```json
{
  "schema_version": "1.0",
  "domains": [
    { "name": "core", "description": "Core entities", "node_names": ["User", "Project", "Group"] }
  ],
  "nodes": [
    { "name": "User", "domain": "core", "description": "GitLab user", "primary_key": "id", "label_field": "username" }
  ],
  "edges": [
    { "name": "AUTHORED", "description": "User authored a resource", "variants": [
      { "source_type": "User", "target_type": "MergeRequest" },
      { "source_type": "User", "target_type": "Issue" }
    ]}
  ]
}
```

**Response (`format=raw`, `expand=User`) — pseudo code:**

```json
{
  "schema_version": "1.0",
  "domains": [ "..." ],
  "nodes": [
    {
      "name": "User",
      "domain": "core",
      "description": "GitLab user",
      "primary_key": "id",
      "label_field": "username",
      "properties": [
        { "name": "id", "data_type": "int", "nullable": false },
        { "name": "username", "data_type": "string", "nullable": false },
        { "name": "email", "data_type": "string", "nullable": true }
      ],
      "style": { "size": 24, "color": "#6366f1" },
      "outgoing_edges": ["AUTHORED", "OWNS", "MEMBER_OF"],
      "incoming_edges": ["ASSIGNED_TO"]
    },
    { "name": "Project", "domain": "core" }
  ],
  "edges": [ "..." ]
}
```

**Response (`format=llm`, `expand=User`) — pseudo code:**

```plaintext
domains: [
  {name: "core", nodes: [{name: "User", props: ["id:int", "username:string", "email:string?"], out: ["AUTHORED", "OWNS", "MEMBER_OF"], in: ["ASSIGNED_TO"]}, "Project", "Group"]},
  {name: "plan", nodes: ["WorkItem", "Issue", "Epic"]},
  ...
]
edges: [
  {name: "AUTHORED", from: ["User"], to: ["MergeRequest", "Issue"]},
  ...
]
```

**gRPC mapping:** `GetGraphSchema(expand_nodes, format)`
**Streaming:** No (unary RPC — reads from in-memory ontology, no ClickHouse, no redaction)

---

### `GET /api/v4/orbit/status`

Cluster health and component status.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `format` | string | `raw` | `raw` or `llm` |

**Response (`format=raw`) — pseudo code:**

```json
{
  "status": "healthy",
  "timestamp": "2026-02-26T12:00:00Z",
  "version": "0.5.0",
  "components": [
    { "name": "clickhouse", "status": "healthy", "replicas": 3, "metrics": { "query_latency_p99_ms": 120 } },
    { "name": "indexer", "status": "healthy", "replicas": 2 },
    { "name": "webserver", "status": "healthy", "replicas": 2 }
  ]
}
```

**Response (`format=llm`) — pseudo code:** [TOON](https://github.com/toon-format/spec/blob/main/SPEC.md) notation

```plaintext
{status: "healthy", version: "0.5.0", ts: "2026-02-26T12:00:00Z", components: [{name: "clickhouse", status: "healthy", replicas: 3, p99_ms: 120}, {name: "indexer", status: "healthy", replicas: 2}, {name: "webserver", status: "healthy", replicas: 2}]}
```

**gRPC mapping:** `GetClusterHealth(format)`
**Streaming:** No (unary)

---

### `GET /api/v4/orbit/tools`

List available Orbit operations. **Pure passthrough from the Rust service** — Rails does not maintain tool definitions, descriptions, or parameter schemas. The Rust service's `tools/registry.rs` is the single source of truth.

The `query_graph` tool description includes the full TOON-format schema (~15KB) as context for LLMs. The `get_graph_schema` tool description includes the `expand_nodes` parameter schema. All of this comes directly from the gRPC `ListTools` response — Rails adds only the REST endpoint mapping.

**Response — pseudo code:**

```json
{
  "tools": [
    {
      "name": "query_graph",
      "description": "Execute graph queries (traversal, neighbors, path finding, aggregation)\n\nSchema:\ndomains: [{name: \"core\", nodes: [\"User\", \"Project\", ...]}, ...]\n...",
      "parameters_json_schema": { "type": "object", "properties": { "query": {} } },
      "endpoint": "POST /api/v4/orbit/query"
    },
    {
      "name": "get_graph_schema",
      "description": "List Knowledge Graph schema (node/edge discovery with optional expansion)",
      "parameters_json_schema": { "type": "object", "properties": { "expand_nodes": { "type": "array", "items": { "type": "string" } } } },
      "endpoint": "GET /api/v4/orbit/schema"
    }
  ]
}
```

**gRPC mapping:** `ListTools()`
**Streaming:** No (unary)
**Rails maintenance:** None — tool metadata flows directly from Rust service. The only Rails-side addition is the `endpoint` field mapping tool names to REST paths.

---

## `glab` CLI Design

```shell
# Query execution
glab orbit query '{"query_type":"traversal","node":{"id":"mr","entity":"MergeRequest"},"limit":5}' --format=human
glab orbit query '{"query_type":"traversal","node":{"id":"mr","entity":"MergeRequest"},"limit":5}' --format=llm

# Schema
glab orbit schema                                   # full schema (compact)
glab orbit schema --expand User,Project             # detailed nodes
glab orbit schema --format=llm                      # TOON format for piping to AI

# Health
glab orbit status

# Tools
glab orbit tools
```

CLI format mapping:

- `--format=human` → `?format=raw` (rendered as tables in terminal)
- `--format=llm` → `?format=llm` (text output, suitable for piping)
- Default: `human`

---

## MCP Compatibility

### Tool routing in `CallTool`

The MCP `tools/call` handler routes tool names to dedicated GrpcClient methods. Tool names are hardcoded in Rails — there are only two tools, and each maps to a specific RPC with different parameter shapes.

Pseudo code:

```ruby
# ee/lib/api/mcp_orbit/handlers/call_tool.rb
def invoke
  tool_name = params[:name]
  arguments = params[:arguments] || {}

  result = case tool_name
           when 'query_graph'
             grpc_client.execute_query(
               query: arguments['query'].to_json,
               query_type: :json,
               format: :llm,
               user: current_user,
               organization_id: current_user.organization_id
             )
           when 'get_graph_schema'
             grpc_client.get_graph_schema(
               expand_nodes: arguments['expand_nodes'] || [],
               format: :llm,
               user: current_user
             )
           else
             raise ArgumentError, "Unknown tool: #{tool_name}"
           end

  format_success(result)
end
```

The `else` branch rejects unknown tool names, addressing the [AppSec feedback](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/224832#note_3116878754) on validating tool calls before dispatching. Since tool names are hardcoded, no `public_send` or dynamic dispatch is involved.

### `ListTools` handler

Unchanged — pure passthrough from `grpc_client.list_tools`.

**Critical principle: the Rust service owns all tool metadata.** Tool names, descriptions (including the full TOON schema context in `query_graph`'s description), and parameter JSON schemas are defined in `tools/registry.rs` in the knowledge-graph repo. Rails never duplicates this. The `ListTools` RPC returns the authoritative tool list, and Rails passes it through verbatim to both the MCP `tools/list` handler and the `GET /api/v4/orbit/tools` REST endpoint. The only Rails-side enrichment is adding the `endpoint` field that maps each tool name to its corresponding REST path.

---

## Proto Changes Required

### New proto definition

The proto definition below reflects the end state after [MR !411](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/411). Pseudo code — see the authoritative definition in [`gkg.proto`](../../../crates/gkg-server/proto/gkg.proto).

```protobuf
syntax = "proto3";
package gkg.v1;

enum ResponseFormat {
  RESPONSE_FORMAT_RAW = 0;
  RESPONSE_FORMAT_LLM = 1;
}

service KnowledgeGraphService {
  rpc ListTools(ListToolsRequest) returns (ListToolsResponse);
  rpc ExecuteQuery(stream ExecuteQueryMessage) returns (stream ExecuteQueryMessage);
  rpc GetGraphSchema(GetGraphSchemaRequest) returns (GetGraphSchemaResponse);
  rpc GetClusterHealth(GetClusterHealthRequest) returns (GetClusterHealthResponse);
}

// --- ExecuteQuery ---

message ExecuteQueryMessage {
  oneof content {
    ExecuteQueryRequest request = 1;
    RedactionExchange redaction = 2;
    ExecuteQueryResult result = 3;
    ExecuteQueryError error = 4;
  }
}

enum QueryType {
  QUERY_TYPE_JSON = 0;   // structured JSON DSL (current)
  // QUERY_TYPE_CYPHER = 1;  // future: openCypher query string
}

message ExecuteQueryRequest {
  string query = 1;
  ResponseFormat format = 2;
  QueryType query_type = 3;
}

message ExecuteQueryResult {
  oneof content {
    string result_json = 1;
    string formatted_text = 2;
  }
  QueryMetadata metadata = 3;
}

message QueryMetadata {
  string query_type = 1;
  repeated string raw_query_strings = 2;
  int32 row_count = 3;
  PaginationInfo pagination = 4;      // present when query included a cursor
}

message PaginationInfo {
  bool has_more = 1;
  int64 total_rows = 2;
}

message ExecuteQueryError {
  string message = 1;
  string code = 2;
}

// --- GetGraphSchema ---

message GetGraphSchemaRequest {
  repeated string expand_nodes = 1;
  ResponseFormat format = 2;
}

message GetGraphSchemaResponse {
  oneof content {
    StructuredSchema structured = 1;
    string formatted_text = 2;
  }
}

message StructuredSchema {
  string schema_version = 1;
  repeated SchemaDomain domains = 2;
  repeated SchemaNode nodes = 3;
  repeated SchemaEdge edges = 4;
}

message SchemaDomain {
  string name = 1;
  string description = 2;
  repeated string node_names = 3;
}

message SchemaNode {
  string name = 1;
  string domain = 2;
  string description = 3;
  string primary_key = 4;
  string label_field = 5;
  repeated SchemaProperty properties = 6;
  SchemaNodeStyle style = 7;
  repeated string outgoing_edges = 8;
  repeated string incoming_edges = 9;
}

message SchemaProperty {
  string name = 1;
  string data_type = 2;
  bool nullable = 3;
  repeated string enum_values = 4;
}

message SchemaEdge {
  string name = 1;
  string description = 2;
  repeated SchemaEdgeVariant variants = 3;
}

message SchemaEdgeVariant {
  string source_type = 1;
  string target_type = 2;
}

message SchemaNodeStyle {
  int32 size = 1;
  string color = 2;
}

// --- Redaction (shared, unchanged from ADR 001) ---

message RedactionExchange {
  oneof content {
    RedactionRequired required = 1;
    RedactionResponse response = 2;
  }
}

message RedactionRequired {
  string result_id = 1;
  repeated ResourceToAuthorize resources = 2;
}

message ResourceToAuthorize {
  string resource_type = 1;
  repeated int64 resource_ids = 2;
  repeated string abilities = 3;
}

message RedactionResponse {
  string result_id = 1;
  repeated ResourceAuthorization authorizations = 2;
}

message ResourceAuthorization {
  string resource_type = 1;
  map<int64, bool> authorized = 2;
}

// --- ListTools ---

message ListToolsRequest {}

message ListToolsResponse {
  repeated ToolDefinition tools = 1;
}

message ToolDefinition {
  string name = 1;
  string description = 2;
  string parameters_json_schema = 3;
}
```

### No backward compatibility

`ExecuteTool` and `GetOntology` are **removed** from the proto definition. The Rust service drops these handlers entirely. All consumers migrate to the new RPCs in a single coordinated deploy.

---

## Rails GrpcClient Changes

Pseudo code:

```ruby
# ee/lib/analytics/knowledge_graph/grpc_client.rb

# NEW method — replaces get_ontology
def get_graph_schema(user:, expand_nodes: [], format: :raw, timeout: DEFAULT_TIMEOUT)
  request = Gkg::V1::GetGraphSchemaRequest.new(
    expand_nodes: expand_nodes,
    format: format == :llm ? :RESPONSE_FORMAT_LLM : :RESPONSE_FORMAT_RAW
  )

  response = stub.get_graph_schema(request, metadata: auth_metadata(user), deadline: timeout_deadline(timeout))

  case response.content
  when :structured
    map_structured_schema(response.structured)
  when :formatted_text
    { formatted_text: response.formatted_text }
  end
end

# MODIFIED — add format and query_type parameters
def execute_query(query:, user:, organization_id: nil, format: :raw, query_type: :json, timeout: STREAMING_TIMEOUT)
  # ... existing bidi streaming logic ...
  # format and query_type are included in the initial ExecuteQueryRequest
end
```

---

## Grape API Implementation

Pseudo code:

```ruby
# ee/lib/api/orbit.rb
module API
  class Orbit < ::API::Base
    feature_category :knowledge_graph

    before do
      not_found! unless Feature.enabled?(:knowledge_graph, current_user)
      authenticate!
    end

    helpers do
      def response_format
        params[:format]&.to_sym == :llm ? :llm : :raw
      end

      def grpc_client
        Analytics::KnowledgeGraph::GrpcClient.new
      end
    end

    namespace :orbit do
      desc 'Execute a Knowledge Graph query'
      params do
        requires :query, type: Hash, desc: 'Query DSL object'
        optional :query_type, type: String, values: %w[json], default: 'json'
        optional :format, type: String, values: %w[raw llm], default: 'raw'
      end
      post :query do
        result = grpc_client.execute_query(
          query: params[:query].to_json,
          query_type: params[:query_type],
          user: current_user,
          organization_id: current_user.organization_id,
          format: response_format
        )
        present result
      rescue Analytics::KnowledgeGraph::GrpcClient::ExecutionError => e
        bad_request!(e.message)
      rescue Analytics::KnowledgeGraph::GrpcClient::ConnectionError => e
        service_unavailable!(e.message)
      end

      desc 'Retrieve Knowledge Graph schema'
      params do
        optional :expand, type: String, desc: 'Comma-separated node names to expand'
        optional :format, type: String, values: %w[raw llm], default: 'raw'
      end
      get :schema do
        expand_nodes = params[:expand]&.split(',')&.map(&:strip) || []
        result = grpc_client.get_graph_schema(
          user: current_user,
          expand_nodes: expand_nodes,
          format: response_format
        )
        present result
      end

      desc 'Cluster health and component status'
      params do
        optional :format, type: String, values: %w[raw llm], default: 'raw'
      end
      get :status do
        result = grpc_client.get_cluster_health(
          user: current_user,
          format: response_format
        )
        present result
      end

      desc 'List available Orbit operations'
      get :tools do
        result = grpc_client.list_tools(user: current_user)
        present result
      end
    end
  end
end
```

## References

- [Proto definition: `gkg.proto`](../../../crates/gkg-server/proto/gkg.proto)
- [ADR 001: gRPC Communication Protocol](001_grpc_communication.md)
- [ADR 002: Rust Core Runtime](002_rust_core_runtime.md)
- [MR !411: Proto rewrite for REST API alignment](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/411)
- [GOON Format Specification](https://gitlab.com/gitlab-org/gitlab/-/snippets/4929205) — Graph Object Output Notation for `format=llm` query results
- [TOON Specification](https://github.com/toon-format/spec/blob/main/SPEC.md) — Token-Oriented Object Notation for schema `format=llm`
- [Orbit GA Designs (Figma)](https://www.figma.com/design/GOrqDStp1E1SE0Ms7lVbXF/--588317--Orbit-GA-Designs?node-id=4066-5048)
