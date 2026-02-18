mod common;

use std::sync::Arc;

use bytes::Bytes;
use gitaly_client::{GitalyClient, GitalyRepositoryConfig, RepositorySource};
use indexer::module::{Handler, HandlerContext};
use indexer::modules::code::{
    ClickHouseCodeWatermarkStore, ClickHouseProjectStore, CodeIndexingConfig, GitalyConfiguration,
    GitalyRepositoryService, PushEventHandler,
};
use indexer::testkit::{MockLockService, MockNatsServices, TestEnvelopeFactory};
use prost::Message;
use sha2::{Digest, Sha256};
use siphon_proto::replication_event::Column;
use siphon_proto::{LogicalReplicationEvents, ReplicationEvent, Value, value};
use testcontainers::GenericImage;
use testcontainers::core::{ContainerPort, ExecCommand, ImageExt, WaitFor};
use testcontainers::runners::AsyncRunner;

use common::TestContext;

const GITALY_IMAGE: &str = "registry.gitlab.com/gitlab-org/build/cng/gitaly";
const GITALY_TAG: &str = "17-7-stable";
const GITALY_TOKEN: &str = "secret_token";

#[tokio::test]
async fn indexes_repository_from_gitaly() {
    let project_id: i64 = 1;

    let clickhouse = TestContext::new().await;
    let (gitaly_address, _container) = start_gitaly().await;

    let repo_path = hashed_repo_path(project_id);
    let commit_sha = create_test_repo(&_container, &repo_path).await;

    // Seed project in ClickHouse (handler looks up project info before indexing)
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
    let handler = PushEventHandler::new(
        GitalyRepositoryService::create(gitaly_config),
        Arc::new(ClickHouseCodeWatermarkStore::new(Arc::new(
            clickhouse.config.build_client(),
        ))),
        Arc::new(ClickHouseProjectStore::new(Arc::new(
            clickhouse.config.build_client(),
        ))),
        CodeIndexingConfig::default(),
    );

    let context = HandlerContext::new(
        Arc::new(clickhouse.create_destination()),
        Arc::new(MockNatsServices::new()),
        Arc::new(MockLockService::new()),
    );
    let envelope = TestEnvelopeFactory::with_bytes(push_event_payload(project_id, &commit_sha));

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

    let file_defines_edges = clickhouse
        .query(
            "SELECT source_id, target_id, relationship_kind FROM gl_edge \
             WHERE source_kind = 'File' AND target_kind = 'Definition' \
             AND relationship_kind = 'FILE_DEFINES'",
        )
        .await;
    assert!(
        file_defines_edges.first().is_some_and(|b| b.num_rows() > 0),
        "no FILE_DEFINES edges indexed"
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

    let class_to_method_edges = clickhouse
        .query(
            "SELECT source_id, target_id, relationship_kind FROM gl_edge \
             WHERE source_kind = 'Definition' AND target_kind = 'Definition' \
             AND relationship_kind = 'CLASS_TO_METHOD'",
        )
        .await;
    assert!(
        class_to_method_edges
            .first()
            .is_some_and(|b| b.num_rows() > 0),
        "no CLASS_TO_METHOD edges indexed"
    );
}

/// GitLab's hashed storage path: @hashed/xx/yy/sha256(project_id).git
/// See: https://docs.gitlab.com/ee/administration/repository_storage_paths.html
fn hashed_repo_path(project_id: i64) -> String {
    let hash = format!("{:x}", Sha256::digest(project_id.to_string()));
    format!("@hashed/{}/{}/{}.git", &hash[0..2], &hash[2..4], hash)
}

fn push_event_payload(project_id: i64, commit_sha: &str) -> Bytes {
    // ref_type=0 is BRANCH, action=2 is PUSHED (see config.rs)
    let cols = [
        ("event_id", value::Value::Int64Value(1)),
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

/// Creates a test repository inside the Gitaly container and returns its HEAD commit SHA.
///
/// We can't use `git push` because git-upload-pack isn't in PATH. Instead:
/// 1. Create a bare repo at /home/git/repositories/{repo_path}
/// 2. Make commits in a temp working dir
/// 3. Copy .git/objects/* and write refs/heads/main manually
///
/// Git binary is at /usr/local/bin/gitaly-git-v2.47 (not in PATH).
/// The -q flags suppress output so only the final SHA is printed.
async fn create_test_repo(
    container: &testcontainers::ContainerAsync<GenericImage>,
    repo_path: &str,
) -> String {
    let script = format!(
        r#"
set -e
GIT=/usr/local/bin/gitaly-git-v2.47
mkdir -p $(dirname /home/git/repositories/{repo_path})
$GIT init -q --bare /home/git/repositories/{repo_path}
rm -rf /tmp/work && mkdir -p /tmp/work && cd /tmp/work
$GIT init -q && $GIT config user.email x@x && $GIT config user.name x
mkdir -p src && cat > src/Main.java << 'JAVA'
public class Main {{
    public void save() {{
        validate();
    }}
    public void validate() {{
    }}
}}
JAVA
$GIT add . && $GIT commit -q -m init
cp -r .git/objects/* /home/git/repositories/{repo_path}/objects/
mkdir -p /home/git/repositories/{repo_path}/refs/heads
$GIT rev-parse HEAD > /home/git/repositories/{repo_path}/refs/heads/main
echo 'ref: refs/heads/main' > /home/git/repositories/{repo_path}/HEAD
$GIT rev-parse HEAD
"#
    );

    let mut result = container
        .exec(ExecCommand::new(["bash", "-c", &script]))
        .await
        .expect("exec failed");

    let stdout = result.stdout_to_vec().await.unwrap();
    String::from_utf8_lossy(&stdout).trim().to_string()
}
