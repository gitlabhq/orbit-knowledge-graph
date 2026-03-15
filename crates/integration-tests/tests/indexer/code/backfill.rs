use std::sync::Arc;

use indexer::handler::Handler;
use indexer::testkit::TestEnvelopeFactory;
use indexer::topic::{CODE_BACKFILL_SUBJECT_PREFIX, CodeBackfillRequest};
use indexer::types::Envelope;

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

    create_project_in_graph(&clickhouse, project_id, "/reconcile", "reconcile/repo").await;

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_handler();
    let context = handler_context(&clickhouse);

    let request = CodeBackfillRequest {
        project_id,
        traversal_path: "/reconcile".to_string(),
    };
    let mut envelope = Envelope::new(&request).unwrap();
    envelope.subject = Arc::from(format!("{CODE_BACKFILL_SUBJECT_PREFIX}.{project_id}"));

    let result = handler.handle(context, envelope).await;
    assert!(result.is_ok(), "backfill handler failed: {:?}", result);

    assert_code_indexed(&clickhouse, project_id).await;
}
