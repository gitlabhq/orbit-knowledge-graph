use std::sync::Arc;

use bytes::Bytes;
use indexer::clickhouse::ArrowClickHouseClient;
use indexer::handler::{Handler, HandlerContext};
use indexer::modules::code::{CodeBackfillDispatchHandler, CodeBackfillDispatchHandlerConfig};
use indexer::nats::ProgressNotifier;
use indexer::testkit::{MockLockService, MockNatsServices, TestEnvelopeFactory};
use indexer::topic::CodeBackfillRequest;
use prost::Message;
use siphon_proto::replication_event::Column;
use siphon_proto::{LogicalReplicationEvents, ReplicationEvent, Value, value};

fn build_namespace_enabled_payload(root_namespace_id: i64) -> Bytes {
    let columns = vec![Column {
        column_index: 0,
        value: Some(Value {
            value: Some(value::Value::Int64Value(root_namespace_id)),
        }),
    }];

    let encoded = LogicalReplicationEvents {
        event: 1,
        table: "knowledge_graph_enabled_namespaces".into(),
        schema: "public".into(),
        application_identifier: "test".into(),
        columns: vec!["root_namespace_id".to_string()],
        events: vec![ReplicationEvent {
            operation: 2, // Insert
            columns,
        }],
        version_hash: 0,
    }
    .encode_to_vec();

    let compressed = zstd::encode_all(encoded.as_slice(), 0).expect("compression failed");
    Bytes::from(compressed)
}

fn dispatch_handler(datalake: ArrowClickHouseClient) -> CodeBackfillDispatchHandler {
    CodeBackfillDispatchHandler::new(datalake, CodeBackfillDispatchHandlerConfig::default())
}

fn dispatch_context(
    clickhouse: &integration_testkit::TestContext,
    nats: Arc<MockNatsServices>,
) -> HandlerContext {
    use indexer::clickhouse::ClickHouseDestination;
    use indexer::metrics::EngineMetrics;

    let destination = ClickHouseDestination::new(
        clickhouse.config.clone(),
        Arc::new(EngineMetrics::default()),
    )
    .expect("failed to create destination");

    HandlerContext::new(
        Arc::new(destination),
        nats,
        Arc::new(MockLockService::new()),
        ProgressNotifier::noop(),
    )
}

#[tokio::test]
async fn dispatches_backfill_requests_for_projects_in_namespace() {
    let root_namespace_id: i64 = 100;
    let project_a_id: i64 = 10;
    let project_b_id: i64 = 20;
    let traversal_path = "100/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    clickhouse
        .execute(&format!(
            "INSERT INTO namespace_traversal_paths (id, traversal_path, deleted) \
             VALUES ({root_namespace_id}, '{traversal_path}', false)"
        ))
        .await;

    clickhouse
        .execute(&format!(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path, deleted) VALUES \
             ({project_a_id}, '{traversal_path}{project_a_id}/', false), \
             ({project_b_id}, '{traversal_path}{project_b_id}/', false)"
        ))
        .await;

    let datalake = clickhouse.config.build_client();
    let handler = dispatch_handler(datalake);
    let nats = Arc::new(MockNatsServices::new());
    let context = dispatch_context(&clickhouse, Arc::clone(&nats));
    let envelope =
        TestEnvelopeFactory::with_bytes(build_namespace_enabled_payload(root_namespace_id));

    let result = handler.handle(context, envelope).await;
    assert!(result.is_ok(), "dispatch handler failed: {:?}", result);

    let published = nats.get_published();
    assert_eq!(
        published.len(),
        2,
        "expected 2 backfill requests, got {}",
        published.len()
    );

    let mut dispatched_project_ids: Vec<i64> = published
        .iter()
        .map(|(_, envelope)| {
            let request: CodeBackfillRequest =
                serde_json::from_slice(&envelope.payload).expect("failed to deserialize request");
            request.project_id
        })
        .collect();
    dispatched_project_ids.sort();

    assert_eq!(dispatched_project_ids, vec![project_a_id, project_b_id]);
}
