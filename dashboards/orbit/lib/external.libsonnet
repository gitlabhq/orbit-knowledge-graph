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
    { name: 'siphon_operations_total', kind: 'counter', labels: ['app_id', 'phase', 'schema', 'table', 'operation'], description: 'Siphon producer ops (CDC events emitted).' },
    { name: 'siphon_snapshot_operations_total', kind: 'counter', labels: ['app_id', 'phase', 'schema', 'table', 'operation'], description: 'Initial-snapshot rows emitted.' },
    { name: 'siphon_buffer_size_total', kind: 'counter', labels: ['app_id', 'phase', 'schema', 'table'], description: 'Rows buffered before flush.' },
    { name: 'siphon_oversized_events_total', kind: 'counter', labels: ['app_id', 'schema', 'table', 'stream'], description: 'Events spilled to object storage because they exceed the NATS payload cap.' },
    { name: 'siphon_nats_publish_retries_total', kind: 'counter', labels: ['app_id', 'schema', 'table', 'stream'], description: 'NATS publish retries from the producer.' },
    { name: 'siphon_lr_filtered_skipped_total', kind: 'counter', labels: ['app_id', 'schema', 'table'], description: 'Logical-replication rows skipped by table filters.' },
    { name: 'siphon_snapshot_status_write_failures_total', kind: 'counter', labels: ['app_id', 'schema', 'table', 'status'], description: 'Failures writing snapshot progress markers.' },
    { name: 'siphon_snapshot_paused', kind: 'gauge', labels: ['app_id'], description: '1 while the initial snapshot is administratively paused.' },
    { name: 'siphon_serialization_duration_ms', kind: 'histogram', labels: ['app_id', 'schema', 'table', 'stream'], description: 'Row-batch serialization latency.' },
    { name: 'siphon_compression_duration_ms', kind: 'histogram', labels: ['app_id', 'schema', 'table', 'stream'], description: 'Package compression latency.' },
    { name: 'siphon_queueing_duration_ms', kind: 'histogram', labels: ['app_id', 'schema', 'table', 'stream'], description: 'Time spent publishing a package to NATS.' },
    { name: 'siphon_flush_buffer_duration_ms', kind: 'histogram', labels: ['app_id'], description: 'Buffer flush latency.' },
    { name: 'siphon_serialized_package_size_bytes', kind: 'histogram', labels: ['app_id', 'schema', 'table', 'stream'], description: 'Serialized package size.' },
    { name: 'siphon_compressed_package_size_bytes', kind: 'histogram', labels: ['app_id', 'schema', 'table', 'stream'], description: 'Compressed package size on the wire.' },
  ],

  SIPHON_CONSUMERS: [
    { name: 'siphon_clickhouse_consumer_number_of_events', kind: 'counter', labels: ['producer_app_id', 'app_id', 'stream'], description: 'Events consumed off NATS and written to ClickHouse.' },
    { name: 'siphon_clickhouse_consumer_number_of_batches', kind: 'counter', labels: ['producer_app_id', 'app_id', 'stream'], description: 'Batches written to ClickHouse.' },
    { name: 'siphon_data_lag_ms', kind: 'gauge', labels: ['producer_app_id', 'app_id', 'stream_identifier', 'schema', 'table', 'source'], description: 'Data lag; source=data is true end-to-end lag, source=heartbeat measures freshness only.' },
    { name: 'siphon_nats_transit_time_ms', kind: 'histogram', labels: ['producer_app_id', 'app_id', 'stream_identifier'], description: 'Time a message spent in NATS before the consumer fetched it.' },
    { name: 'siphon_clickhouse_consumer_ch_query_duration', kind: 'histogram', labels: ['producer_app_id', 'app_id', 'query_type', 'table_name'], description: 'ClickHouse query latency from the consumer.' },
    { name: 'siphon_clickhouse_consumer_ch_row_count', kind: 'counter', labels: ['producer_app_id', 'app_id', 'query_type', 'table_name'], description: 'Rows written to ClickHouse.' },
    { name: 'siphon_clickhouse_consumer_batch_retry_total', kind: 'counter', labels: ['producer_app_id', 'app_id', 'query_type', 'table_name'], description: 'Batch retries after ClickHouse errors.' },
    { name: 'siphon_clickhouse_consumer_adaptive_batch_size', kind: 'histogram', labels: ['producer_app_id', 'app_id', 'query_type', 'table_name'], description: 'Adaptive batch sizes in use.' },
    { name: 'siphon_clickhouse_consumer_refresh_package_events', kind: 'counter', labels: ['producer_app_id', 'app_id', 'source_stream', 'target_stream'], description: 'Events in reconciliation refresh packages.' },
    { name: 'siphon_decompression_duration_ms', kind: 'histogram', labels: ['app_id', 'stream_identifier'], description: 'Package decompression latency.' },
    { name: 'siphon_deserialization_duration_ms', kind: 'histogram', labels: ['app_id', 'stream_identifier'], description: 'Package deserialization latency.' },
  ],

  SIPHON_RECONCILER: [
    { name: 'siphon_reconciler_iteration_duration_seconds', kind: 'histogram', labels: ['app_id', 'identifier'], description: 'Full reconciliation table-iteration duration.' },
    { name: 'siphon_reconciler_inconsistent_rows_total', kind: 'counter', labels: ['app_id', 'identifier'], description: 'Inconsistent rows queued for refresh.' },
  ],

  SIPHON_RETENTION: [
    { name: 'siphon_retention_claim_attempts_total', kind: 'counter', labels: ['app_id', 'stream'], description: 'Retention leader-claim attempts.' },
    { name: 'siphon_retention_claim_errors_total', kind: 'counter', labels: ['app_id', 'stream', 'phase'], description: 'Retention claim errors by phase.' },
    { name: 'siphon_retention_maxage_updates_total', kind: 'counter', labels: ['app_id', 'stream'], description: 'Stream MaxAge updates applied.' },
    { name: 'siphon_retention_maxage_seconds', kind: 'gauge', labels: ['app_id', 'stream'], description: 'Current MaxAge applied to the stream.' },
    { name: 'siphon_retention_floor_below_first_seq_total', kind: 'counter', labels: ['app_id', 'stream'], description: 'Retention floor fell below the first stream sequence (data-loss guard).' },
  ],

  SIPHON_FAILOVER: [
    { name: 'siphon_failover_mode_active', kind: 'gauge', labels: ['app_id'], description: '1 while emergency failover mode is active (pages s2 on GitLab.com).' },
    { name: 'siphon_failover_writes_total', kind: 'counter', labels: ['app_id', 'schema', 'table'], description: 'Rows written to the failover store.' },
    { name: 'siphon_failover_drain_published_total', kind: 'counter', labels: ['app_id'], description: 'Failover rows drained back to NATS.' },
    { name: 'siphon_failover_drain_errors_total', kind: 'counter', labels: ['app_id'], description: 'Errors draining failover rows.' },
  ],

  SIPHON_OBJECT_STORAGE: [
    { name: 'siphon_object_storage_put_duration_ms', kind: 'histogram', labels: ['identifier'], description: 'Oversize-spill object PUT latency.' },
    { name: 'siphon_object_storage_get_duration_ms', kind: 'histogram', labels: ['identifier'], description: 'Oversize-spill object GET latency.' },
    { name: 'siphon_object_storage_put_failures_total', kind: 'counter', labels: ['identifier'], description: 'Oversize-spill PUT failures.' },
  ],

  NATS_METRICS: [
    { name: 'nats_varz_in_msgs', kind: 'counter', labels: [], description: 'NATS inbound messages (varz).' },
    { name: 'nats_varz_out_msgs', kind: 'counter', labels: [], description: 'NATS outbound messages (varz).' },
    { name: 'nats_varz_in_bytes', kind: 'counter', labels: [], description: 'NATS inbound bytes (varz).' },
    { name: 'nats_varz_out_bytes', kind: 'counter', labels: [], description: 'NATS outbound bytes (varz).' },
    { name: 'nats_varz_slow_consumers', kind: 'gauge', labels: [], description: 'NATS slow-consumer count (varz reports a current count, not a counter).' },
    { name: 'nats_varz_connections', kind: 'gauge', labels: [], description: 'Active client connections.' },
    { name: 'nats_varz_subscriptions', kind: 'gauge', labels: [], description: 'Active subscriptions.' },
    { name: 'nats_varz_cpu', kind: 'gauge', labels: [], description: 'Server CPU usage (varz).' },
    { name: 'nats_varz_mem', kind: 'gauge', labels: [], description: 'Server memory usage bytes (varz).' },
    { name: 'nats_server_total_streams', kind: 'gauge', labels: [], description: 'JetStream streams on the server.' },
    { name: 'nats_server_total_consumers', kind: 'gauge', labels: [], description: 'JetStream consumers on the server.' },
  ],

  // Queried with is_stream_leader="true" so replicated streams (R3) are not
  // summed once per replica.
  NATS_JETSTREAM_STREAMS: [
    { name: 'nats_stream_total_messages', kind: 'gauge', labels: ['stream_name'], description: 'Messages currently in each JetStream stream.' },
    { name: 'nats_stream_total_bytes', kind: 'gauge', labels: ['stream_name'], description: 'Bytes currently in each JetStream stream.' },
    { name: 'nats_stream_first_seq', kind: 'gauge', labels: ['stream_name'], description: 'First sequence retained in the stream.' },
    { name: 'nats_stream_last_seq', kind: 'gauge', labels: ['stream_name'], description: 'Last sequence written to the stream.' },
    { name: 'nats_stream_consumer_count', kind: 'gauge', labels: ['stream_name'], description: 'Consumers attached to the stream.' },
    { name: 'nats_consumer_num_pending', kind: 'gauge', labels: ['stream_name', 'consumer_name'], description: 'Messages pending per JetStream consumer.' },
    { name: 'nats_consumer_num_redelivered', kind: 'counter', labels: ['stream_name', 'consumer_name'], description: 'Redelivered messages per consumer.' },
    { name: 'nats_consumer_num_ack_pending', kind: 'gauge', labels: ['stream_name', 'consumer_name'], description: 'Messages awaiting ack per consumer.' },
    { name: 'nats_consumer_num_waiting', kind: 'gauge', labels: ['stream_name', 'consumer_name'], description: 'Pull requests waiting for messages.' },
    { name: 'nats_consumer_delivered_consumer_seq', kind: 'gauge', labels: ['stream_name', 'consumer_name'], description: 'Last delivered consumer sequence.' },
    { name: 'nats_consumer_last_delivery_seconds', kind: 'gauge', labels: ['stream_name', 'consumer_name'], description: 'Seconds since the last delivery to the consumer.' },
  ],

  NATS_JETSTREAM_CAPACITY: [
    { name: 'nats_varz_jetstream_stats_storage', kind: 'gauge', labels: [], description: 'JetStream file storage in use.' },
    { name: 'nats_varz_jetstream_stats_memory', kind: 'gauge', labels: [], description: 'JetStream memory storage in use.' },
    { name: 'nats_varz_jetstream_config_max_storage', kind: 'gauge', labels: [], description: 'Configured JetStream max file storage.' },
    { name: 'nats_varz_jetstream_config_max_memory', kind: 'gauge', labels: [], description: 'Configured JetStream max memory.' },
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
