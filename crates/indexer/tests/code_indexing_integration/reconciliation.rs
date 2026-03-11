use bytes::Bytes;
use indexer::handler::Handler;
use indexer::testkit::TestEnvelopeFactory;
use indexer::topic::ProjectCodeIndexingRequest;

use super::helpers::*;

const SIPHON_SCHEMA_SQL: &str = include_str!("../fixtures/siphon.sql");
const GRAPH_SCHEMA_SQL: &str = include_str!("../../../../config/graph.sql");

#[tokio::test]
async fn indexes_project_from_datalake_push_event() {
    let project_id: i64 = 3;

    let clickhouse =
        integration_testkit::TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    let (gitaly_address, _container) = start_gitaly().await;

    let repo_path = hashed_repo_path(project_id);
    let commit_sha = create_test_repo(
        &_container,
        &repo_path,
        "src/App.java",
        "public class App {
            public void start() {}
        }",
    )
    .await;

    seed_project(&clickhouse, project_id, "/reconcile", "reconcile/repo").await;
    seed_push_event(&clickhouse, project_id, 42, "main", &commit_sha).await;

    let deps = CodeIndexingDeps::new(&gitaly_address, &clickhouse);
    let handler = deps.reconciliation_handler();
    let context = handler_context(&clickhouse);
    let payload = serde_json::to_vec(&ProjectCodeIndexingRequest { project_id }).unwrap();
    let envelope = TestEnvelopeFactory::with_bytes(Bytes::from(payload));

    let result = handler.handle(context, envelope).await;
    assert!(
        result.is_ok(),
        "reconciliation handler failed: {:?}",
        result
    );

    assert_code_indexed(&clickhouse, project_id).await;
}
