use bytes::Bytes;
use indexer::module::Handler;
use indexer::modules::code::PushEventHandler;
use indexer::testkit::TestEnvelopeFactory;
use prost::Message;
use siphon_proto::replication_event::Column;
use siphon_proto::{LogicalReplicationEvents, ReplicationEvent, Value, value};

use super::helpers::*;

const SIPHON_SCHEMA_SQL: &str = include_str!("../fixtures/siphon.sql");
const GRAPH_SCHEMA_SQL: &str = include_str!("../../../../fixtures/schema/graph.sql");

#[tokio::test]
async fn indexes_repository_from_gitaly() {
    let project_id: i64 = 1;

    let clickhouse =
        integration_testkit::TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    let (gitaly_address, _container) = start_gitaly().await;

    let repo_path = hashed_repo_path(project_id);
    let commit_sha = create_test_repo(
        &_container,
        &repo_path,
        "src/Main.java",
        "public class Main {
            public void save() { validate(); }
            public void validate() {}
        }",
    )
    .await;

    seed_project(&clickhouse, project_id, "/test", "test/repo").await;

    let deps = CodeIndexingDeps::new(&gitaly_address, &clickhouse);
    let handler = deps.push_event_handler();
    let context = handler_context(&clickhouse);
    let envelope = TestEnvelopeFactory::with_bytes(push_event_payload(project_id, &commit_sha, 1));

    let result = handler.handle(context, envelope).await;
    assert!(result.is_ok(), "handler failed: {:?}", result);

    assert_code_indexed(&clickhouse, project_id).await;
}

#[tokio::test]
async fn soft_deletes_stale_code_data_after_reindexing() {
    let project_id: i64 = 2;

    let clickhouse =
        integration_testkit::TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    let (gitaly_address, gitaly_container) = start_gitaly().await;
    let repo_path = hashed_repo_path(project_id);

    seed_project(&clickhouse, project_id, "/stale-test", "stale/test").await;
    let deps = CodeIndexingDeps::new(&gitaly_address, &clickhouse);
    let handler = deps.push_event_handler();

    let first_commit = create_test_repo(
        &gitaly_container,
        &repo_path,
        "src/Main.java",
        "public class Main {
            public void save() { validate(); }
            public void validate() {}
        }",
    )
    .await;
    index_push_event(&handler, &clickhouse, project_id, &first_commit, 1).await;

    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_active_definitions(
        &clickhouse,
        project_id,
        "src/Main.java",
        &["Main", "save", "validate"],
    )
    .await;
    assert_eq!(
        count_active_edges(&clickhouse, project_id, "DEFINES").await,
        2,
        "Main→save and Main→validate DEFINES edges should exist"
    );

    let second_commit = update_repo_file(
        &gitaly_container,
        &repo_path,
        "src/Main.java",
        "src/Other.java",
        "public class Other {
            public void run() {}
        }",
    )
    .await;
    index_push_event(&handler, &clickhouse, project_id, &second_commit, 2).await;

    assert_file_not_active(&clickhouse, project_id, "src/Main.java").await;
    assert_no_active_definitions(&clickhouse, project_id, "src/Main.java").await;

    assert_file_is_active(&clickhouse, project_id, "src/Other.java").await;
    assert_active_definitions(&clickhouse, project_id, "src/Other.java", &["Other", "run"]).await;

    assert_eq!(
        count_active_edges(&clickhouse, project_id, "DEFINES").await,
        1,
        "only Other→run DEFINES edge should remain active"
    );
}

async fn index_push_event(
    handler: &PushEventHandler,
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    commit_sha: &str,
    event_id: i64,
) {
    let context = handler_context(clickhouse);
    let envelope =
        TestEnvelopeFactory::with_bytes(push_event_payload(project_id, commit_sha, event_id));

    handler
        .handle(context, envelope)
        .await
        .unwrap_or_else(|e| panic!("indexing commit {commit_sha} (event {event_id}) failed: {e}"));
}

fn push_event_payload(project_id: i64, commit_sha: &str, event_id: i64) -> Bytes {
    let cols = [
        ("event_id", value::Value::Int64Value(event_id)),
        ("project_id", value::Value::Int64Value(project_id)),
        ("ref_type", value::Value::Int16Value(0)),
        ("action", value::Value::Int16Value(2)),
        ("ref", value::Value::StringValue("refs/heads/main".into())),
        ("commit_to", value::Value::StringValue(commit_sha.into())),
    ];

    let columns: Vec<Column> = cols
        .iter()
        .enumerate()
        .map(|(i, (_, v))| Column {
            column_index: i as u32,
            value: Some(Value {
                value: Some(v.clone()),
            }),
        })
        .collect();

    let encoded = LogicalReplicationEvents {
        event: 1,
        table: "push_event_payloads".into(),
        schema: "public".into(),
        application_identifier: "test".into(),
        columns: cols.iter().map(|(n, _)| n.to_string()).collect(),
        events: vec![ReplicationEvent {
            operation: 2,
            columns,
        }],
    }
    .encode_to_vec();

    let compressed = zstd::encode_all(encoded.as_slice(), 0).expect("compression failed");
    Bytes::from(compressed)
}

async fn assert_file_not_active(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    path: &str,
) {
    let active_rows = clickhouse
        .query(&format!(
            "SELECT id FROM gl_file FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false"
        ))
        .await;
    assert!(
        active_rows.first().is_none_or(|b| b.num_rows() == 0),
        "file '{path}' should not be active after soft-deletion"
    );
}

async fn assert_file_is_active(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    path: &str,
) {
    let result = clickhouse
        .query(&format!(
            "SELECT id FROM gl_file FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false"
        ))
        .await;
    assert!(
        result.first().is_some_and(|b| b.num_rows() > 0),
        "file '{path}' should be active"
    );
}

async fn query_active_definition_names(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    file_path: &str,
) -> Vec<String> {
    let result = clickhouse
        .query(&format!(
            "SELECT name FROM gl_definition FINAL \
             WHERE project_id = {project_id} AND file_path = '{file_path}' AND _deleted = false"
        ))
        .await;
    let Some(batch) = result.first() else {
        return Vec::new();
    };
    let names = batch
        .column_by_name("name")
        .expect("name column should exist")
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("name should be StringArray");
    (0..batch.num_rows())
        .map(|i| names.value(i).to_string())
        .collect()
}

async fn assert_active_definitions(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    file_path: &str,
    expected_names: &[&str],
) {
    let mut actual = query_active_definition_names(clickhouse, project_id, file_path).await;
    actual.sort();
    let mut expected: Vec<&str> = expected_names.to_vec();
    expected.sort();
    assert_eq!(
        actual, expected,
        "active definitions in '{file_path}' should be {expected:?}, got {actual:?}"
    );
}

async fn count_active_edges(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    relationship_kind: &str,
) -> usize {
    let result = clickhouse
        .query(&format!(
            "SELECT source_id FROM gl_edge FINAL \
             WHERE relationship_kind = '{relationship_kind}' AND _deleted = false \
             AND source_id IN (SELECT id FROM gl_definition FINAL WHERE project_id = {project_id} AND _deleted = false)"
        ))
        .await;
    result.first().map_or(0, |b| b.num_rows())
}

async fn assert_no_active_definitions(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    file_path: &str,
) {
    let active = query_active_definition_names(clickhouse, project_id, file_path).await;
    assert!(
        active.is_empty(),
        "definitions in '{file_path}' should not be active (soft-deleted), but found: {active:?}"
    );
}
