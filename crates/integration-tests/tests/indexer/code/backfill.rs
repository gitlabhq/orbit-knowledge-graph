use bytes::Bytes;
use indexer::handler::Handler;
use indexer::testkit::TestEnvelopeFactory;
use indexer::topic::CodeBackfillRequest;

use super::helpers::*;

#[tokio::test]
async fn indexes_project_with_backfill_event_id() {
    let project_id: i64 = 3;

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/App.java",
            "public class App {
            public void start() {}
        }",
        )],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.backfill_handler();
    let context = handler_context(&clickhouse);
    let payload = serde_json::to_vec(&CodeBackfillRequest {
        project_id,
        traversal_path: "/reconcile".to_string(),
    })
    .unwrap();
    let envelope = TestEnvelopeFactory::with_bytes(Bytes::from(payload));

    let result = handler.handle(context, envelope).await;
    assert!(
        result.is_ok(),
        "reconciliation handler failed: {:?}",
        result
    );

    assert_code_indexed(&clickhouse, project_id).await;
}
