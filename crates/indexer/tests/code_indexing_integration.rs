mod common;

use std::sync::Arc;

use bytes::Bytes;
use gitaly_client::{GitalyClient, GitalyRepositoryConfig, RepositorySource};
use indexer::module::{Handler, HandlerContext};
use indexer::modules::code::{
    ClickHouseCodeWatermarkStore, ClickHouseProjectStore, ClickHouseStaleDataCleaner,
    CodeIndexingConfig, GitalyConfiguration, GitalyRepositoryService, PushEventHandler,
};
use indexer::testkit::{MockLockService, MockNatsServices, TestEnvelopeFactory};
use prost::Message;
use sha2::{Digest, Sha256};
use siphon_proto::replication_event::Column;
use siphon_proto::{LogicalReplicationEvents, ReplicationEvent, Value, value};
use testcontainers::GenericImage;
use testcontainers::core::{ContainerPort, ExecCommand, ImageExt, WaitFor};
use testcontainers::runners::AsyncRunner;

use common::{GRAPH_SCHEMA_SQL, IndexerTestExt, SIPHON_SCHEMA_SQL, TestContext};

const GITALY_IMAGE: &str = "registry.gitlab.com/gitlab-org/build/cng/gitaly";
const GITALY_TAG: &str = "17-7-stable";
const GITALY_TOKEN: &str = "secret_token";

#[tokio::test]
async fn indexes_repository_from_gitaly() {
    let project_id: i64 = 1;

    let clickhouse = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
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

    clickhouse
        .execute(&format!(
            "INSERT INTO gl_project (id, traversal_path, full_path, _version) \
             VALUES ({}, '/test', 'test/repo', 1)",
            project_id
        ))
        .await;

    let gitaly_config = GitalyConfiguration {
        address: gitaly_address,
        storage: "default".to_string(),
        token: Some(GITALY_TOKEN.to_string()),
    };
    let clickhouse_client = Arc::new(clickhouse.config.build_client());
    let handler = PushEventHandler::new(
        GitalyRepositoryService::create(gitaly_config),
        Arc::new(ClickHouseCodeWatermarkStore::new(Arc::clone(
            &clickhouse_client,
        ))),
        Arc::new(ClickHouseProjectStore::new(Arc::clone(&clickhouse_client))),
        Arc::new(ClickHouseStaleDataCleaner::new(clickhouse_client)),
        CodeIndexingConfig::default(),
    );

    let context = HandlerContext::new(
        Arc::new(clickhouse.create_destination()),
        Arc::new(MockNatsServices::new()),
        Arc::new(MockLockService::new()),
    );
    let envelope = TestEnvelopeFactory::with_bytes(push_event_payload(project_id, &commit_sha, 1));

    let result = handler.handle(context, envelope).await;
    assert!(result.is_ok(), "handler failed: {:?}", result);

    let files = clickhouse
        .query(&format!(
            "SELECT path FROM gl_file WHERE project_id = {}",
            project_id
        ))
        .await;
    assert!(
        files.first().is_some_and(|b| b.num_rows() > 0),
        "no files indexed"
    );

    let definitions = clickhouse
        .query(&format!(
            "SELECT name FROM gl_definition WHERE project_id = {}",
            project_id
        ))
        .await;
    assert!(
        definitions.first().is_some_and(|b| b.num_rows() > 0),
        "no definitions indexed"
    );

    let defines_edges = clickhouse
        .query(
            "SELECT source_id, target_id, relationship_kind FROM gl_edge \
             WHERE source_kind = 'File' AND target_kind = 'Definition' \
             AND relationship_kind = 'DEFINES'",
        )
        .await;
    assert!(
        defines_edges.first().is_some_and(|b| b.num_rows() > 0),
        "no DEFINES edges indexed"
    );

    let file_ids = clickhouse
        .query(&format!(
            "SELECT id FROM gl_file WHERE project_id = {}",
            project_id
        ))
        .await;
    let definition_ids = clickhouse
        .query(&format!(
            "SELECT id FROM gl_definition WHERE project_id = {}",
            project_id
        ))
        .await;

    assert!(
        file_ids.first().is_some_and(|b| b.num_rows() > 0),
        "file should have an id"
    );
    assert!(
        definition_ids.first().is_some_and(|b| b.num_rows() > 0),
        "definition should have an id"
    );

    let definition_defines_edges = clickhouse
        .query(
            "SELECT source_id, target_id, relationship_kind FROM gl_edge \
             WHERE source_kind = 'Definition' AND target_kind = 'Definition' \
             AND relationship_kind = 'DEFINES'",
        )
        .await;
    assert!(
        definition_defines_edges
            .first()
            .is_some_and(|b| b.num_rows() > 0),
        "no DEFINES edges (Definition → Definition) indexed"
    );

    let edge_paths = clickhouse
        .query("SELECT DISTINCT traversal_path FROM gl_edge")
        .await;
    assert!(
        edge_paths.first().is_some_and(|b| b.num_rows() > 0),
        "edges should have traversal_path"
    );
    let paths = edge_paths[0]
        .column_by_name("traversal_path")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(
        paths.value(0),
        "/test",
        "code edge traversal_path should match the project's traversal_path"
    );
}

/// Indexes a repo, replaces a file, re-indexes, and verifies that nodes and edges
/// from the removed file are soft-deleted while the new file's data remains visible.
#[tokio::test]
async fn soft_deletes_stale_code_data_after_reindexing() {
    let project_id: i64 = 2;

    let clickhouse = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    let (gitaly_address, gitaly_container) = start_gitaly().await;
    let repo_path = hashed_repo_path(project_id);

    seed_project(&clickhouse, project_id, "/stale-test", "stale/test").await;
    let handler = create_push_event_handler(&gitaly_address, &clickhouse);

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
    index_commit(&handler, &clickhouse, project_id, &first_commit, 1).await;

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
    index_commit(&handler, &clickhouse, project_id, &second_commit, 2).await;

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

// -- Test helpers ---------------------------------------------------------------------------------

fn create_push_event_handler(gitaly_address: &str, clickhouse: &TestContext) -> PushEventHandler {
    let gitaly_config = GitalyConfiguration {
        address: gitaly_address.to_string(),
        storage: "default".to_string(),
        token: Some(GITALY_TOKEN.to_string()),
    };
    let clickhouse_client = Arc::new(clickhouse.config.build_client());

    PushEventHandler::new(
        GitalyRepositoryService::create(gitaly_config),
        Arc::new(ClickHouseCodeWatermarkStore::new(Arc::clone(
            &clickhouse_client,
        ))),
        Arc::new(ClickHouseProjectStore::new(Arc::clone(&clickhouse_client))),
        Arc::new(ClickHouseStaleDataCleaner::new(clickhouse_client)),
        CodeIndexingConfig::default(),
    )
}

async fn seed_project(
    clickhouse: &TestContext,
    project_id: i64,
    traversal_path: &str,
    full_path: &str,
) {
    clickhouse
        .execute(&format!(
            "INSERT INTO gl_project (id, traversal_path, full_path, _version) \
             VALUES ({project_id}, '{traversal_path}', '{full_path}', 1)",
        ))
        .await;
}

async fn index_commit(
    handler: &PushEventHandler,
    clickhouse: &TestContext,
    project_id: i64,
    commit_sha: &str,
    event_id: i64,
) {
    let context = HandlerContext::new(
        Arc::new(clickhouse.create_destination()),
        Arc::new(MockNatsServices::new()),
        Arc::new(MockLockService::new()),
    );
    let envelope =
        TestEnvelopeFactory::with_bytes(push_event_payload(project_id, commit_sha, event_id));

    handler
        .handle(context, envelope)
        .await
        .unwrap_or_else(|e| panic!("indexing commit {commit_sha} (event {event_id}) failed: {e}"));
}

async fn assert_file_not_active(clickhouse: &TestContext, project_id: i64, path: &str) {
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

async fn assert_file_is_active(clickhouse: &TestContext, project_id: i64, path: &str) {
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
    clickhouse: &TestContext,
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
    clickhouse: &TestContext,
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
    clickhouse: &TestContext,
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

async fn assert_no_active_definitions(clickhouse: &TestContext, project_id: i64, file_path: &str) {
    let active = query_active_definition_names(clickhouse, project_id, file_path).await;
    assert!(
        active.is_empty(),
        "definitions in '{file_path}' should not be active (soft-deleted), but found: {active:?}"
    );
}

/// GitLab's hashed storage path: @hashed/xx/yy/sha256(project_id).git
/// See: https://docs.gitlab.com/ee/administration/repository_storage_paths.html
fn hashed_repo_path(project_id: i64) -> String {
    let hash = format!("{:x}", Sha256::digest(project_id.to_string()));
    format!("@hashed/{}/{}/{}.git", &hash[0..2], &hash[2..4], hash)
}

fn push_event_payload(project_id: i64, commit_sha: &str, event_id: i64) -> Bytes {
    // ref_type=0 is BRANCH, action=2 is PUSHED (see config.rs)
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

// -- Container setup ------------------------------------------------------------------------------

async fn start_gitaly() -> (String, testcontainers::ContainerAsync<GenericImage>) {
    // Start Gitaly with auth token configured. The startup command:
    // 1. Creates /home/git/repositories (Gitaly's storage root)
    // 2. Appends auth token to config (must match GITALY_TOKEN)
    // 3. Runs the normal entrypoint
    // GITALY_TESTING_NO_GIT_HOOKS=1 disables hooks that expect GitLab infrastructure.
    let container = GenericImage::new(GITALY_IMAGE, GITALY_TAG)
        .with_wait_for(WaitFor::message_on_stderr("Starting Gitaly"))
        .with_exposed_port(ContainerPort::Tcp(8075))
        .with_env_var("GITALY_TESTING_NO_GIT_HOOKS", "1")
        .with_cmd([
            "bash",
            "-c",
            "mkdir -p /home/git/repositories && \
             echo -e '[auth]\\ntoken = \"secret_token\"' >> /etc/gitaly/config.toml && \
             exec /scripts/process-wrapper",
        ])
        .start()
        .await
        .expect("failed to start Gitaly");

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(ContainerPort::Tcp(8075))
        .await
        .unwrap();
    let host = if host == "localhost" {
        "127.0.0.1".to_string()
    } else {
        host
    };
    let address = format!("tcp://{}:{}", host, port);

    // Wait for Gitaly to accept connections
    for _ in 0..30 {
        let cfg = GitalyRepositoryConfig {
            address: address.clone(),
            storage: "default".into(),
            relative_path: "x.git".into(),
            token: Some(GITALY_TOKEN.into()),
        };
        if let Ok(c) = GitalyClient::connect(cfg).await
            && c.exists().await.is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    (address, container)
}

/// Creates a bare repo in the Gitaly container with a single file and returns the commit SHA.
///
/// Uses manual object copying instead of `git push` because git-upload-pack isn't in PATH.
/// Git binary is at /usr/local/bin/gitaly-git-v2.47 (not in the container's PATH).
async fn create_test_repo(
    container: &testcontainers::ContainerAsync<GenericImage>,
    repo_path: &str,
    file_path: &str,
    file_content: &str,
) -> String {
    let script = format!(
        r#"
set -e
GIT=/usr/local/bin/gitaly-git-v2.47
mkdir -p $(dirname /home/git/repositories/{repo_path})
$GIT init -q --bare /home/git/repositories/{repo_path}
rm -rf /tmp/work && mkdir -p /tmp/work && cd /tmp/work
$GIT init -q && $GIT config user.email x@x && $GIT config user.name x
mkdir -p $(dirname {file_path})
cat > {file_path} << 'SRCEOF'
{file_content}
SRCEOF
$GIT add . && $GIT -c maintenance.auto=false commit -q -m init
cp -r .git/objects/* /home/git/repositories/{repo_path}/objects/
mkdir -p /home/git/repositories/{repo_path}/refs/heads
$GIT rev-parse HEAD > /home/git/repositories/{repo_path}/refs/heads/main
echo 'ref: refs/heads/main' > /home/git/repositories/{repo_path}/HEAD
$GIT rev-parse HEAD
"#
    );

    exec_git_script(container, &script).await
}

/// Removes `old_file` from the working tree, adds `new_file` with the given content,
/// commits, and pushes objects to the bare repo. Returns the new commit SHA.
async fn update_repo_file(
    container: &testcontainers::ContainerAsync<GenericImage>,
    repo_path: &str,
    old_file: &str,
    new_file: &str,
    new_content: &str,
) -> String {
    let script = format!(
        r#"
set -e
GIT=/usr/local/bin/gitaly-git-v2.47
cd /tmp/work
$GIT rm -q {old_file}
mkdir -p $(dirname {new_file})
cat > {new_file} << 'SRCEOF'
{new_content}
SRCEOF
$GIT add . && $GIT -c maintenance.auto=false commit -q -m "replace {old_file} with {new_file}"
cp -rf .git/objects/* /home/git/repositories/{repo_path}/objects/
$GIT rev-parse HEAD > /home/git/repositories/{repo_path}/refs/heads/main
$GIT rev-parse HEAD
"#
    );

    exec_git_script(container, &script).await
}

async fn exec_git_script(
    container: &testcontainers::ContainerAsync<GenericImage>,
    script: &str,
) -> String {
    let mut result = container
        .exec(ExecCommand::new(["bash", "-c", script]))
        .await
        .expect("exec failed");

    let stdout = result.stdout_to_vec().await.unwrap();
    String::from_utf8_lossy(&stdout).trim().to_string()
}
