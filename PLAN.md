# Implementation Plan: GKG gRPC Server with JWT Authentication

## Overview

Add gRPC service to `gkg-server` that allows Rails to:
1. **List tools** - Get available Knowledge Graph tools
2. **Execute tools** - Run tool calls with bidirectional redaction flow

Rails remains the MCP host and handles:
- MCP protocol (JSON-RPC)
- User authentication
- Permission checks via `Authz::RedactionService`

GKG Server provides:
- Tool definitions
- Tool execution
- Redaction signals (which resources need permission checks)
- **Context engineering** - Applies redaction results and optimizes response for LLM consumption

---

## Bidirectional redaction flow

GKG handles redaction and response formatting. The flow:

```
1. Rails → GKG:  ExecuteTool(tool_name, arguments)
2. GKG → Rails:  RedactionRequired(result_id, resources_to_check)
3. Rails:        Checks permissions via Authz::RedactionService
4. Rails → GKG:  ApplyRedaction(result_id, authorized_resources)
5. GKG → Rails:  FinalResponse(redacted result)
```

Why this works:
- Authorization stays in Rails (where `Ability.allowed?` lives)
- Raw data stays in GKG (unauthorized content never hits Rails)
- GKG formats the response before returning it

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Rails VM                                    │
│                                                                          │
│  ┌──────────────┐    ┌─────────────────────┐    ┌─────────────────────┐ │
│  │  MCP Host    │───▶│ KnowledgeGraph::    │───▶│ Authz::Redaction    │ │
│  │  (Duo)       │    │ GrpcClient          │    │ Service             │ │
│  └──────────────┘    └─────────────────────┘    └─────────────────────┘ │
│                              │  ▲                         │              │
│                              │  │ 4. ApplyRedaction       │              │
│                              │  └─────────────────────────┘              │
└──────────────────────────────┼───────────────────────────────────────────┘
                               │ 1. ExecuteTool
                               │ 2. RedactionRequired (streaming)
                               │ 5. FinalResponse
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           GKE Cluster                                     │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                          gkg-server                                 │  │
│  │  ┌──────────────┐    ┌──────────────┐    ┌───────────────────────┐ │  │
│  │  │ gRPC Service │───▶│ Tool         │───▶│ Redaction Cache +     │ │  │
│  │  │ (tonic)      │    │ Executor     │    │ Context Engineering   │ │  │
│  │  └──────────────┘    └──────────────┘    └───────────────────────┘ │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Part 1: GKG Server (Rust) Changes

### 1.1 Add gRPC Dependencies

**File:** `crates/gkg-server/Cargo.toml`

```toml
[dependencies]
tonic = "0.12"
prost = "0.13"
prost-types = "0.13"
tower = "0.4"

[build-dependencies]
tonic-build = "0.12"
```

### 1.2 Define Protocol Buffers

**File:** `crates/gkg-server/proto/knowledge_graph.proto`

```protobuf
syntax = "proto3";

package gkg.v1;

// Main Knowledge Graph service
service KnowledgeGraphService {
  // List all available tools with their schemas
  rpc ListTools(ListToolsRequest) returns (ListToolsResponse);

  // Execute a tool with bidirectional streaming for redaction flow:
  // 1. Client sends ExecuteToolRequest
  // 2. Server sends RedactionRequired with resources to check
  // 3. Client sends RedactionResponse with authorized resources
  // 4. Server sends ToolResult with context-engineered response
  rpc ExecuteTool(stream ExecuteToolMessage) returns (stream ExecuteToolMessage);
}

// ============ List Tools ============

message ListToolsRequest {
  // Empty - user context comes from JWT
}

message ListToolsResponse {
  repeated ToolDefinition tools = 1;
}

message ToolDefinition {
  string name = 1;
  string description = 2;
  string parameters_json_schema = 3; // JSON Schema as string
}

// ============ Execute Tool (Bidirectional Streaming) ============

message ExecuteToolMessage {
  oneof message {
    // Client → Server: Initial tool execution request
    ExecuteToolRequest request = 1;

    // Server → Client: Resources that need permission checks
    RedactionRequired redaction_required = 2;

    // Client → Server: Results of permission checks
    RedactionResponse redaction_response = 3;

    // Server → Client: Final context-engineered result
    ToolResult result = 4;

    // Either direction: Error
    ToolError error = 5;
  }
}

// Client → Server: Initial request
message ExecuteToolRequest {
  string tool_name = 1;
  string arguments_json = 2;
}

// Server → Client: Resources needing authorization checks
message RedactionRequired {
  string result_id = 1;  // Correlation ID for this result
  repeated ResourceCheck resources = 2;
}

message ResourceCheck {
  string resource_type = 1;  // "issues", "merge_requests", "projects", etc.
  repeated int64 ids = 2;    // Resource IDs to check
}

// Client → Server: Authorization results from Rails
message RedactionResponse {
  string result_id = 1;  // Must match the result_id from RedactionRequired
  repeated ResourceAuthorization authorizations = 2;
}

message ResourceAuthorization {
  string resource_type = 1;
  map<int64, bool> authorized = 2;  // id → authorized (true/false)
}

// Server → Client: Final redacted and context-engineered result
message ToolResult {
  string result_json = 1;  // Redacted, context-engineered result
}

message ToolError {
  string code = 1;    // e.g., "tool_not_found", "invalid_arguments", "execution_error"
  string message = 2;
}
```

### Why bidirectional streaming?

Alternatives considered:

1. **Two separate RPCs** (ExecuteTool + ApplyRedaction): Requires server-side state management (result cache with TTL). More complex error handling for orphaned results.

2. **Single unary RPC with all data returning to Rails**: Rails would see unauthorized data before redaction.

3. **Bidirectional streaming** (chosen): Single logical operation with multiple exchanges. GKG holds result in memory during the exchange. Stream closes on error. No persistent state between requests.

### Puma thread blocking consideration

Bidirectional streaming holds a Puma thread for the full request duration. GitLab's Gitaly client has the same pattern for merge/rebase operations.

This is acceptable when requests complete in <5 seconds. The entire flow (tool execution + Authz check + redaction) should stay under 3 seconds for typical operations.

**Safeguards:**
- Set gRPC deadline to 10-15 seconds (matches Gitaly's `medium_timeout`)
- If requests consistently exceed this, move tool execution to Sidekiq workers
- Mocked tools will be instant; real graph queries may need profiling later

### 1.3 Build Script for Protobuf

**File:** `crates/gkg-server/build.rs`

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(&["proto/knowledge_graph.proto"], &["proto"])?;
    Ok(())
}
```

### 1.4 gRPC Module Structure

**Create new files:**

```
crates/gkg-server/src/
├── grpc/
│   ├── mod.rs           # Module exports
│   ├── server.rs        # tonic gRPC server setup
│   ├── service.rs       # KnowledgeGraphService implementation
│   └── auth.rs          # JWT interceptor for gRPC
├── tools/
│   ├── mod.rs           # Module exports
│   ├── registry.rs      # Tool registry (ported from old code)
│   ├── executor.rs      # Tool execution logic (mocked)
│   └── redaction.rs     # Redaction signal types
```

### 1.5 JWT Interceptor for gRPC

**File:** `crates/gkg-server/src/grpc/auth.rs`

```rust
use tonic::{Request, Status};
use crate::auth::{JwtValidator, Claims};

pub fn extract_claims<T>(
    request: &Request<T>,
    validator: &JwtValidator,
) -> Result<Claims, Status> {
    let token = request
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| Status::unauthenticated("Missing authorization header"))?;

    validator
        .validate(token)
        .map_err(|e| Status::unauthenticated(format!("Invalid token: {:?}", e)))
}
```

### 1.6 Tool Registry (Mock Implementation)

**File:** `crates/gkg-server/src/tools/registry.rs`

Port the tool definitions from the old code, returning 8 tools:
- `get_graph_schema`
- `find_nodes`
- `traverse_relationships`
- `explore_neighborhood`
- `find_paths`
- `aggregate_nodes`
- `view_merge_request_diffs`
- `get_repository_files`

### 1.7 Tool Executor (Mock Implementation)

**File:** `crates/gkg-server/src/tools/executor.rs`

```rust
pub struct ToolExecutor;

impl ToolExecutor {
    pub fn execute(
        &self,
        tool_name: &str,
        arguments: serde_json::Value,
        claims: &Claims,
    ) -> Result<ToolResult, ToolError> {
        // Mock implementation - returns sample data with redaction signals
        match tool_name {
            "find_nodes" => self.mock_find_nodes(arguments, claims),
            "get_graph_schema" => self.mock_get_schema(),
            // ... other tools
            _ => Err(ToolError::NotFound(tool_name.to_string())),
        }
    }
}

pub struct ToolResult {
    pub result_json: serde_json::Value,
    pub redactions: Vec<RedactionRequest>,
}

pub struct RedactionRequest {
    pub resource_type: String,  // "issues", "merge_requests", etc.
    pub ids: Vec<i64>,
}
```

### 1.8 gRPC Service Implementation

**File:** `crates/gkg-server/src/grpc/service.rs`

```rust
use std::pin::Pin;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use crate::auth::JwtValidator;
use crate::tools::{ToolRegistry, ToolExecutor, ContextEngine};

pub mod gkg_proto {
    tonic::include_proto!("gkg.v1");
}

use gkg_proto::knowledge_graph_service_server::KnowledgeGraphService;
use gkg_proto::*;

pub struct GkgService {
    validator: JwtValidator,
    executor: ToolExecutor,
    context_engine: ContextEngine,
}

type ExecuteToolStream = Pin<Box<dyn Stream<Item = Result<ExecuteToolMessage, Status>> + Send>>;

#[tonic::async_trait]
impl KnowledgeGraphService for GkgService {
    async fn list_tools(
        &self,
        request: Request<ListToolsRequest>,
    ) -> Result<Response<ListToolsResponse>, Status> {
        let _claims = extract_claims(&request, &self.validator)?;

        let tools = ToolRegistry::get_all_tools()
            .into_iter()
            .map(|t| ToolDefinition {
                name: t.name,
                description: t.description,
                parameters_json_schema: t.parameters.to_string(),
            })
            .collect();

        Ok(Response::new(ListToolsResponse { tools }))
    }

    type ExecuteToolStream = ExecuteToolStream;

    async fn execute_tool(
        &self,
        request: Request<Streaming<ExecuteToolMessage>>,
    ) -> Result<Response<Self::ExecuteToolStream>, Status> {
        // Extract claims from initial request metadata
        let claims = extract_claims(&request, &self.validator)?;
        let mut stream = request.into_inner();

        let (tx, rx) = mpsc::channel(4);
        let executor = self.executor.clone();
        let context_engine = self.context_engine.clone();

        tokio::spawn(async move {
            // 1. Wait for initial ExecuteToolRequest
            let first_msg = match stream.next().await {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                None => {
                    let _ = tx.send(Err(Status::invalid_argument("Empty stream"))).await;
                    return;
                }
            };

            let req = match first_msg.message {
                Some(execute_tool_message::Message::Request(r)) => r,
                _ => {
                    let _ = tx.send(Err(Status::invalid_argument("Expected ExecuteToolRequest"))).await;
                    return;
                }
            };

            // 2. Execute the tool (returns raw result + redaction requirements)
            let execution_result = match executor.execute(&req.tool_name, &req.arguments_json, &claims) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Ok(ExecuteToolMessage {
                        message: Some(execute_tool_message::Message::Error(ToolError {
                            code: e.code(),
                            message: e.message(),
                        })),
                    })).await;
                    return;
                }
            };

            // 3. If no redaction needed, return result directly
            if execution_result.resources_to_check.is_empty() {
                let final_result = context_engine.prepare_response(
                    execution_result.raw_result,
                    &[], // No redactions
                );
                let _ = tx.send(Ok(ExecuteToolMessage {
                    message: Some(execute_tool_message::Message::Result(ToolResult {
                        result_json: final_result.to_string(),
                    })),
                })).await;
                return;
            }

            // 4. Send RedactionRequired to client
            let result_id = Uuid::new_v4().to_string();
            let _ = tx.send(Ok(ExecuteToolMessage {
                message: Some(execute_tool_message::Message::RedactionRequired(RedactionRequired {
                    result_id: result_id.clone(),
                    resources: execution_result.resources_to_check.iter().map(|r| ResourceCheck {
                        resource_type: r.resource_type.clone(),
                        ids: r.ids.clone(),
                    }).collect(),
                })),
            })).await;

            // 5. Wait for RedactionResponse from client
            let redaction_msg = match stream.next().await {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                None => {
                    let _ = tx.send(Err(Status::cancelled("Client closed stream"))).await;
                    return;
                }
            };

            let redaction_response = match redaction_msg.message {
                Some(execute_tool_message::Message::RedactionResponse(r)) => r,
                _ => {
                    let _ = tx.send(Err(Status::invalid_argument("Expected RedactionResponse"))).await;
                    return;
                }
            };

            // Validate result_id matches
            if redaction_response.result_id != result_id {
                let _ = tx.send(Err(Status::invalid_argument("result_id mismatch"))).await;
                return;
            }

            // 6. Apply redaction and context engineering
            let final_result = context_engine.apply_redaction_and_prepare(
                execution_result.raw_result,
                &redaction_response.authorizations,
            );

            // 7. Send final result
            let _ = tx.send(Ok(ExecuteToolMessage {
                message: Some(execute_tool_message::Message::Result(ToolResult {
                    result_json: final_result.to_string(),
                })),
            })).await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
```

### 1.9 Context Engineering Module

**File:** `crates/gkg-server/src/tools/context_engine.rs`

```rust
use serde_json::Value;

/// Handles redaction and context optimization for LLM consumption
#[derive(Clone)]
pub struct ContextEngine;

impl ContextEngine {
    /// Prepare response when no redaction is needed
    pub fn prepare_response(&self, raw_result: Value, _redactions: &[]) -> Value {
        self.optimize_for_llm(raw_result)
    }

    /// Apply redaction based on authorization results, then optimize
    pub fn apply_redaction_and_prepare(
        &self,
        raw_result: Value,
        authorizations: &[ResourceAuthorization],
    ) -> Value {
        // Build set of unauthorized resource IDs
        let unauthorized: HashSet<(String, i64)> = authorizations
            .iter()
            .flat_map(|auth| {
                auth.authorized
                    .iter()
                    .filter(|(_, authorized)| !authorized)
                    .map(|(id, _)| (auth.resource_type.clone(), *id))
            })
            .collect();

        // Recursively filter unauthorized resources from result
        let redacted = self.filter_unauthorized(raw_result, &unauthorized);

        // Optimize for LLM consumption
        self.optimize_for_llm(redacted)
    }

    /// Remove unauthorized resources from the result tree
    fn filter_unauthorized(&self, value: Value, unauthorized: &HashSet<(String, i64)>) -> Value {
        match value {
            Value::Array(arr) => {
                Value::Array(arr.into_iter()
                    .filter(|item| !self.is_unauthorized(item, unauthorized))
                    .map(|item| self.filter_unauthorized(item, unauthorized))
                    .collect())
            }
            Value::Object(map) => {
                Value::Object(map.into_iter()
                    .map(|(k, v)| (k, self.filter_unauthorized(v, unauthorized)))
                    .collect())
            }
            other => other,
        }
    }

    /// Check if a value represents an unauthorized resource
    fn is_unauthorized(&self, value: &Value, unauthorized: &HashSet<(String, i64)>) -> bool {
        // Check for id + type pattern (e.g., {"id": 123, "type": "gl_issue"})
        if let (Some(id), Some(type_val)) = (value.get("id"), value.get("type")) {
            if let (Some(id), Some(type_str)) = (id.as_i64(), type_val.as_str()) {
                let resource_type = self.map_type_to_resource(type_str);
                return unauthorized.contains(&(resource_type, id));
            }
        }
        false
    }

    /// Map graph node types to Rails resource types
    fn map_type_to_resource(&self, node_type: &str) -> String {
        match node_type {
            "gl_issue" => "issues",
            "gl_mr" | "gl_merge_request" => "merge_requests",
            "gl_project" => "projects",
            "gl_milestone" => "milestones",
            "gl_snippet" => "snippets",
            _ => node_type,
        }.to_string()
    }

    /// Optimize result structure for LLM consumption
    fn optimize_for_llm(&self, value: Value) -> Value {
        // Add context hints, compress verbose structures, etc.
        // This is where domain-specific optimization happens
        value
    }
}
```

### 1.9 Update Main Entry Point

**File:** `crates/gkg-server/src/main.rs`

Add gRPC server alongside HTTP server (dual-protocol):

```rust
// Start both HTTP and gRPC servers
let http_server = webserver::Server::bind(http_addr, mode, validator.clone()).await?;
let grpc_server = grpc::Server::bind(grpc_addr, validator.clone()).await?;

tokio::select! {
    res = http_server.run() => res?,
    res = grpc_server.run() => res?,
    _ = shutdown::wait_for_signal() => {},
}
```

### 1.10 Configuration Updates

**File:** `crates/gkg-server/src/config.rs`

Add:
```rust
pub grpc_bind_address: SocketAddr,  // e.g., 0.0.0.0:50051
```

---

## Part 2: Rails Changes

### 2.1 Add gRPC Gem

**File:** `Gemfile`

```ruby
gem 'grpc', '~> 1.64'
```

### 2.2 Generate Ruby Protobuf Code

Create rake task or use `grpc_tools_ruby_protoc`:

```bash
grpc_tools_ruby_protoc \
  --ruby_out=ee/lib/ai/knowledge_graph/proto \
  --grpc_out=ee/lib/ai/knowledge_graph/proto \
  proto/knowledge_graph.proto
```

### 2.3 gRPC Client with Bidirectional Streaming

**File:** `ee/lib/ai/knowledge_graph/grpc_client.rb`

```ruby
# frozen_string_literal: true

require_relative 'proto/knowledge_graph_pb'
require_relative 'proto/knowledge_graph_services_pb'

module Ai
  module KnowledgeGraph
    class GrpcClient
      include ::Gitlab::Loggable

      ExecutionError = Class.new(StandardError)
      StreamError = Class.new(StandardError)

      def initialize(endpoint: nil)
        @endpoint = endpoint || configured_endpoint
        @stub = create_stub
      end

      def list_tools(user:)
        request = Gkg::V1::ListToolsRequest.new
        metadata = auth_metadata(user: user)

        response = @stub.list_tools(request, metadata: metadata)
        response.tools.map do |tool|
          {
            name: tool.name,
            description: tool.description,
            parameters: JSON.parse(tool.parameters_json_schema)
          }
        end
      end

      # Execute tool with bidirectional streaming for redaction flow
      # Yields a block that performs authorization checks
      def execute_tool(tool_name:, arguments:, user:, organization_id: nil, &authorization_block)
        metadata = auth_metadata(user: user, organization_id: organization_id)

        # Create bidirectional stream
        call = @stub.execute_tool(metadata: metadata)

        # Send initial request
        call.send_msg(Gkg::V1::ExecuteToolMessage.new(
          request: Gkg::V1::ExecuteToolRequest.new(
            tool_name: tool_name,
            arguments_json: arguments.to_json
          )
        ))

        # Process stream messages
        handle_execute_stream(call, &authorization_block)
      end

      private

      def handle_execute_stream(call)
        loop do
          msg = call.recv_msg
          break if msg.nil?

          case msg.message
          when :redaction_required
            # GKG is asking us to check permissions
            redaction_req = msg.redaction_required

            # Yield to caller to perform authorization
            auth_results = yield(redaction_req.resources)

            # Send authorization results back to GKG
            call.send_msg(Gkg::V1::ExecuteToolMessage.new(
              redaction_response: Gkg::V1::RedactionResponse.new(
                result_id: redaction_req.result_id,
                authorizations: build_authorizations(auth_results)
              )
            ))

          when :result
            # Final result from GKG (already redacted and context-engineered)
            return JSON.parse(msg.result.result_json)

          when :error
            raise ExecutionError.new("#{msg.error.code}: #{msg.error.message}")
          end
        end

        raise StreamError, "Stream ended without result"
      end

      def build_authorizations(auth_results)
        auth_results.map do |resource_type, id_authorizations|
          Gkg::V1::ResourceAuthorization.new(
            resource_type: resource_type,
            authorized: id_authorizations.transform_keys(&:to_i)
          )
        end
      end

      def create_stub
        credentials = if tls_enabled?
          GRPC::Core::ChannelCredentials.new(ca_cert)
        else
          :this_channel_is_insecure
        end

        Gkg::V1::KnowledgeGraphService::Stub.new(
          @endpoint,
          credentials,
          channel_args: channel_args
        )
      end

      def auth_metadata(user:, organization_id: nil)
        {
          'authorization' => JwtAuth.authorization_header(
            user: user,
            organization_id: organization_id
          )
        }
      end

      def channel_args
        {
          'grpc.keepalive_time_ms' => 20_000,
          'grpc.keepalive_permit_without_calls' => 1
        }
      end

      def configured_endpoint
        Gitlab.config.knowledge_graph.grpc_endpoint
      rescue GitlabSettings::MissingSetting
        ENV.fetch('KNOWLEDGE_GRAPH_GRPC_ENDPOINT', 'localhost:50051')
      end

      def tls_enabled?
        ENV['KNOWLEDGE_GRAPH_GRPC_TLS'] == 'true'
      end

      def ca_cert
        File.read(ENV.fetch('KNOWLEDGE_GRAPH_GRPC_CA_CERT', '/etc/ssl/certs/ca-certificates.crt'))
      end
    end
  end
end
```

### 2.4 Tool Execution Service with Bidirectional Redaction

**File:** `ee/app/services/ai/knowledge_graph/execute_tool_service.rb`

```ruby
# frozen_string_literal: true

module Ai
  module KnowledgeGraph
    class ExecuteToolService
      def initialize(user:, tool_name:, arguments:, organization_id: nil)
        @user = user
        @tool_name = tool_name
        @arguments = arguments
        @organization_id = organization_id
      end

      def execute
        # Execute tool with bidirectional streaming
        # The block is called when GKG requests authorization checks
        grpc_client.execute_tool(
          tool_name: @tool_name,
          arguments: @arguments,
          user: @user,
          organization_id: @organization_id
        ) do |resources_to_check|
          # This block is called by GrpcClient when GKG sends RedactionRequired
          perform_authorization_check(resources_to_check)
        end
      end

      private

      def perform_authorization_check(resources_to_check)
        # Convert proto resources to format expected by RedactionService
        resources_by_type = resources_to_check.each_with_object({}) do |resource, hash|
          hash[resource.resource_type] = resource.ids.to_a
        end

        # Call Rails authorization service
        Authz::RedactionService.new(
          user: @user,
          resources_by_type: resources_by_type,
          source: 'knowledge_graph'
        ).execute
      end

      def grpc_client
        @grpc_client ||= GrpcClient.new
      end
    end
  end
end
```

### Why block-based API?

The `execute_tool` method takes a block that performs authorization. This keeps authorization logic in the service (which knows about `Authz::RedactionService`) and streaming logic in the client. The client doesn't need to know about Rails authorization. Easy to mock in tests.

### 2.5 MCP Tool Provider Integration

**File:** Update existing MCP tool provider to use new service

```ruby
# In the MCP tool execution handler
def execute_knowledge_graph_tool(tool_name, arguments)
  Ai::KnowledgeGraph::ExecuteToolService.new(
    user: current_user,
    tool_name: tool_name,
    arguments: arguments,
    organization_id: current_organization&.id
  ).execute
end
```

---

## Part 3: Kubernetes Service Exposure

### 3.1 Update Helm Chart

**File:** `helm-dev/templates/gkg-server.yaml` (new or updated)

```yaml
apiVersion: v1
kind: Service
metadata:
  name: gkg-server-grpc
  annotations:
    cloud.google.com/load-balancer-type: "Internal"
spec:
  type: LoadBalancer
  ports:
    - name: grpc
      port: 50051
      targetPort: 50051
  selector:
    app: gkg-server
---
apiVersion: v1
kind: Service
metadata:
  name: gkg-server-http
spec:
  type: ClusterIP
  ports:
    - name: http
      port: 8080
      targetPort: 8080
  selector:
    app: gkg-server
```

### 3.2 Firewall Rules (GCP)

```
Name: allow-rails-to-gkg-grpc
Source: 10.128.0.0/9 (Rails VM CIDR)
Destination: Internal LB IP
Protocol: TCP
Port: 50051
Action: ALLOW
```

---

## Files to create/modify

### GKG Server (Rust) - New Files

| File | Description |
|------|-------------|
| `crates/gkg-server/proto/knowledge_graph.proto` | gRPC service definition (bidirectional streaming) |
| `crates/gkg-server/build.rs` | Protobuf build script |
| `crates/gkg-server/src/grpc/mod.rs` | gRPC module |
| `crates/gkg-server/src/grpc/server.rs` | tonic server setup |
| `crates/gkg-server/src/grpc/service.rs` | Bidirectional streaming service implementation |
| `crates/gkg-server/src/grpc/auth.rs` | JWT interceptor |
| `crates/gkg-server/src/tools/mod.rs` | Tools module |
| `crates/gkg-server/src/tools/registry.rs` | Tool definitions (8 tools) |
| `crates/gkg-server/src/tools/executor.rs` | Tool execution logic (mocked) |
| `crates/gkg-server/src/tools/context_engine.rs` | Redaction + context engineering |

### GKG Server (Rust) - Modified Files

| File | Change |
|------|--------|
| `crates/gkg-server/Cargo.toml` | Add tonic, prost deps |
| `crates/gkg-server/src/lib.rs` | Add grpc, tools modules |
| `crates/gkg-server/src/main.rs` | Start gRPC server |
| `crates/gkg-server/src/config.rs` | Add grpc_bind_address |
| `crates/gkg-server/src/cli/mod.rs` | Add gRPC CLI args |

### Rails - New Files

| File | Description |
|------|-------------|
| `ee/lib/ai/knowledge_graph/proto/knowledge_graph_pb.rb` | Generated protobuf |
| `ee/lib/ai/knowledge_graph/proto/knowledge_graph_services_pb.rb` | Generated gRPC service |
| `ee/lib/ai/knowledge_graph/grpc_client.rb` | Bidirectional streaming gRPC client |
| `ee/app/services/ai/knowledge_graph/execute_tool_service.rb` | Orchestrates tool execution + auth checks |

### Rails - Modified Files

| File | Change |
|------|--------|
| `Gemfile` | Add grpc gem |
| `config/gitlab.yml.example` | Add knowledge_graph.grpc_endpoint |

### Kubernetes

| File | Change |
|------|--------|
| `helm-dev/templates/gkg-server.yaml` | Add gRPC service |
| `helm-dev/values.yaml` | Add gRPC port config |

---

## Redaction contract (bidirectional flow)

### Step 1: GKG Executes Tool and Identifies Resources

GKG executes the tool and identifies resources that need authorization:

```
Tool execution result (held in GKG memory):
{
  "nodes": [
    {"id": 123, "type": "gl_issue", "title": "Bug fix"},
    {"id": 456, "type": "gl_issue", "title": "Feature request"},
    {"id": 789, "type": "gl_mr", "title": "Add feature"}
  ]
}
```

### Step 2: GKG Sends RedactionRequired

GKG sends resources to check (data stays in GKG):

```protobuf
RedactionRequired {
  result_id: "uuid-123",
  resources: [
    { resource_type: "issues", ids: [123, 456] },
    { resource_type: "merge_requests", ids: [789] }
  ]
}
```

### Step 3: Rails Checks Permissions

Rails calls `Authz::RedactionService`:

```ruby
Authz::RedactionService.new(
  user: current_user,
  resources_by_type: {
    'issues' => [123, 456],
    'merge_requests' => [789]
  },
  source: 'knowledge_graph'
).execute

# Returns:
{
  'issues' => { 123 => true, 456 => false },
  'merge_requests' => { 789 => true }
}
```

### Step 4: Rails Sends RedactionResponse

Rails sends authorization results back to GKG:

```protobuf
RedactionResponse {
  result_id: "uuid-123",
  authorizations: [
    { resource_type: "issues", authorized: { 123: true, 456: false } },
    { resource_type: "merge_requests", authorized: { 789: true } }
  ]
}
```

### Step 5: GKG Applies Redaction and Context Engineering

GKG filters unauthorized resources and optimizes for LLM:

```json
{
  "nodes": [
    {"id": 123, "type": "gl_issue", "title": "Bug fix"},
    {"id": 789, "type": "gl_mr", "title": "Add feature"}
  ],
  "_context": {
    "total_results": 3,
    "redacted_count": 1,
    "hint": "Some results were filtered due to access permissions"
  }
}
```

### Security properties

1. Unauthorized data never leaves GKG. Rails only sees resource IDs, not content.
2. Rails is the authorization authority. All permission checks go through `Ability.allowed?`.
3. GKG handles context engineering. It optimizes the final response for LLM consumption.
4. Correlation via result_id ensures responses match requests.

---

## Verification steps

### 1. Unit Tests (Rust)

```bash
cd crates/gkg-server
cargo test
```

### 2. Integration Test (gRPC)

```bash
# Start server
cargo run --bin gkg-server -- webserver --grpc-bind 0.0.0.0:50051

# Test with grpcurl
grpcurl -plaintext -d '{}' localhost:50051 gkg.v1.KnowledgeGraphService/ListTools
```

### 3. Rails Tests

```bash
# Run gRPC client specs
bundle exec rspec ee/spec/lib/ai/knowledge_graph/grpc_client_spec.rb

# Run service specs
bundle exec rspec ee/spec/services/ai/knowledge_graph/execute_tool_service_spec.rb
```

### 4. End-to-End Test

1. Deploy gkg-server to GKE with gRPC enabled
2. Configure Rails with gRPC endpoint
3. Make MCP tool call through Duo
4. Verify:
   - Tools are listed correctly
   - Tool execution works
   - Redaction removes unauthorized resources

---

## Implementation order

1. **Phase 1: Rust gRPC Setup**
   - Add dependencies to Cargo.toml
   - Create proto file
   - Add build.rs
   - Create grpc module structure

2. **Phase 2: Tool Registry**
   - Port tool definitions from old code
   - Create mock executor
   - Add redaction types

3. **Phase 3: gRPC Service**
   - Implement KnowledgeGraphService
   - Add JWT interceptor
   - Update main.rs for dual-protocol

4. **Phase 4: Rails Client**
   - Add grpc gem
   - Generate Ruby protobuf code
   - Create GrpcClient
   - Create ExecuteToolService

5. **Phase 5: Redaction Integration**
   - Create RedactionFilter
   - Integrate with Authz::RedactionService
   - Add tests

6. **Phase 6: Kubernetes**
   - Update Helm chart
   - Configure Internal LB
   - Set up firewall rules
