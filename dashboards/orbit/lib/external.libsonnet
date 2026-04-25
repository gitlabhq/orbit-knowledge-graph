// External-service metrics: HTTP autoinstrumentation, Siphon, NATS, Rails KG.
// These are emitted by other services so they can't live in the Rust catalog;
// kind / labels / description are spelled out here.

{
  GKG_HTTP: [
    { name: 'http_server_request_duration_seconds', kind: 'histogram', labels: ['http_route', 'http_response_status_code', 'http_request_method'], description: 'HTTP server request latency (autoinstrumentation).' },
    { name: 'http_server_active_requests', kind: 'gauge', labels: ['http_route', 'http_request_method'], description: 'In-flight HTTP server requests.' },
    { name: 'http_server_request_body_size_bytes', kind: 'histogram', labels: ['http_route'], description: 'HTTP request body size.' },
    { name: 'http_server_response_body_size_bytes', kind: 'histogram', labels: ['http_route'], description: 'HTTP response body size.' },
  ],

  GKG_GRPC: [
    { name: 'rpc_server_duration_seconds', kind: 'histogram', labels: ['rpc_service', 'rpc_method', 'rpc_grpc_status_code'], description: 'gRPC server request latency.' },
    { name: 'rpc_server_active_requests', kind: 'gauge', labels: ['rpc_service', 'rpc_method'], description: 'In-flight gRPC server requests.' },
    { name: 'rpc_server_requests_per_rpc', kind: 'histogram', labels: ['rpc_service', 'rpc_method'], description: 'Request messages per RPC call.' },
    { name: 'rpc_server_responses_per_rpc', kind: 'histogram', labels: ['rpc_service', 'rpc_method'], description: 'Response messages per RPC call.' },
  ],

  SIPHON_PRODUCERS: [
    { name: 'siphon_operations_total', kind: 'counter', labels: ['app_id', 'container'], description: 'Siphon producer ops (CDC events emitted).' },
  ],

  SIPHON_CONSUMERS: [
    { name: 'siphon_clickhouse_consumer_number_of_events', kind: 'counter', labels: ['product_app_id'], description: 'Events consumed off NATS and written to ClickHouse.' },
  ],

  NATS_METRICS: [
    { name: 'nats_varz_in_msgs', kind: 'counter', labels: [], description: 'NATS inbound messages (varz).' },
    { name: 'nats_varz_out_msgs', kind: 'counter', labels: [], description: 'NATS outbound messages (varz).' },
    { name: 'nats_varz_in_bytes', kind: 'counter', labels: [], description: 'NATS inbound bytes (varz).' },
    { name: 'nats_varz_out_bytes', kind: 'counter', labels: [], description: 'NATS outbound bytes (varz).' },
    { name: 'nats_varz_slow_consumers', kind: 'counter', labels: [], description: 'NATS slow-consumer count.' },
    { name: 'nats_stream_total_messages', kind: 'gauge', labels: ['stream_name'], description: 'Messages currently in each JetStream stream.' },
    { name: 'nats_stream_total_bytes', kind: 'gauge', labels: ['stream_name'], description: 'Bytes currently in each JetStream stream.' },
    { name: 'nats_consumer_num_pending', kind: 'gauge', labels: ['stream_name', 'consumer_name'], description: 'Messages pending per JetStream consumer.' },
    { name: 'nats_consumer_num_redelivered', kind: 'counter', labels: ['stream_name', 'consumer_name'], description: 'Redelivered messages per consumer.' },
    { name: 'nats_consumer_num_ack_pending', kind: 'gauge', labels: ['stream_name', 'consumer_name'], description: 'Messages awaiting ack per consumer.' },
  ],

  RAILS_KG_REQUEST: [
    { name: 'gitlab_knowledge_graph_grpc_duration_seconds', kind: 'histogram', labels: ['method', 'status'], description: 'Rails → GKG gRPC call latency.' },
    { name: 'gitlab_knowledge_graph_grpc_errors_total', kind: 'counter', labels: ['method', 'code'], description: 'Rails → GKG gRPC error count.' },
    { name: 'gitlab_knowledge_graph_redaction_duration_seconds', kind: 'histogram', labels: [], description: 'Rails-side redaction time.' },
    { name: 'gitlab_knowledge_graph_redaction_batch_size', kind: 'histogram', labels: [], description: 'Rails-side redaction batch size.' },
    { name: 'gitlab_knowledge_graph_redaction_filtered_count', kind: 'histogram', labels: [], description: 'Rows filtered by redaction per request.' },
    { name: 'gitlab_knowledge_graph_jwt_build_duration_seconds', kind: 'histogram', labels: [], description: 'JWT assembly latency on Rails side.' },
    { name: 'gitlab_knowledge_graph_auth_context_duration_seconds', kind: 'histogram', labels: [], description: 'Auth-context build latency on Rails side.' },
  ],

  RAILS_KG_TRAVERSAL: [
    { name: 'gitlab_knowledge_graph_traversal_ids_count', kind: 'histogram', labels: [], description: 'Traversal IDs per request (pre-compaction).' },
    { name: 'gitlab_knowledge_graph_compaction_ratio', kind: 'histogram', labels: [], description: 'Compaction ratio of traversal IDs.' },
    { name: 'gitlab_knowledge_graph_compaction_fallback_total', kind: 'counter', labels: [], description: 'Traversal compaction fell back to uncompressed form.' },
    { name: 'gitlab_knowledge_graph_traversal_ids_threshold_exceeded_total', kind: 'counter', labels: [], description: 'Requests with too many traversal IDs.' },
  ],
}
