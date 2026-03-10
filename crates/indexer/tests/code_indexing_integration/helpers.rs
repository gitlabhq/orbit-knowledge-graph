#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use gitaly_client::{GitalyClient, GitalyError, GitalyRepositoryConfig, RepositorySource};
use gitlab_client::{GitalyConnectionInfo, RepositoryInfo};
use indexer::handler::HandlerContext;
use indexer::modules::code::{
    ClickHouseCodeWatermarkStore, ClickHouseProjectStore, ClickHousePushEventStore,
    ClickHouseStaleDataCleaner, CodeIndexingPipeline, ProjectCodeIndexingHandler,
    ProjectCodeIndexingHandlerConfig, PushEventHandler, PushEventHandlerConfig, RepositoryService,
    config::CodeTableNames, metrics::CodeMetrics,
};
use indexer::testkit::{MockLockService, MockNatsServices};
use integration_testkit::TestContext;
use sha2::{Digest, Sha256};
use testcontainers::GenericImage;
use testcontainers::core::{ContainerPort, ExecCommand, ImageExt, WaitFor};
use testcontainers::runners::AsyncRunner;

pub const GITALY_IMAGE: &str = "registry.gitlab.com/gitlab-org/build/cng/gitaly";
pub const GITALY_TAG: &str = "17-7-stable";
pub const GITALY_TOKEN: &str = "secret_token";

pub struct CodeIndexingDeps {
    pub pipeline: Arc<CodeIndexingPipeline>,
    pub repository_service: Arc<dyn RepositoryService>,
    pub watermark_store: Arc<ClickHouseCodeWatermarkStore>,
    pub project_store: Arc<ClickHouseProjectStore>,
    pub push_event_store: Arc<ClickHousePushEventStore>,
    pub metrics: CodeMetrics,
}

impl CodeIndexingDeps {
    pub fn new(gitaly_address: &str, clickhouse: &TestContext) -> Self {
        let repository_service = create_direct_gitaly_service(gitaly_address, GITALY_TOKEN);
        let graph_client = Arc::new(clickhouse.config.build_client());
        let watermark_store =
            Arc::new(ClickHouseCodeWatermarkStore::new(Arc::clone(&graph_client)));
        let project_store = Arc::new(ClickHouseProjectStore::new(Arc::clone(&graph_client)));
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let table_names =
            Arc::new(CodeTableNames::from_ontology(&ontology).expect("code tables must resolve"));

        let stale_data_cleaner =
            Arc::new(ClickHouseStaleDataCleaner::new(graph_client, &table_names));
        let push_event_store = Arc::new(ClickHousePushEventStore::new(
            clickhouse.config.build_client(),
        ));
        let metrics = CodeMetrics::new();

        let pipeline = Arc::new(CodeIndexingPipeline::new(
            Arc::clone(&repository_service),
            Arc::clone(&watermark_store) as _,
            stale_data_cleaner,
            metrics.clone(),
            table_names,
        ));

        Self {
            pipeline,
            repository_service,
            watermark_store,
            project_store,
            push_event_store,
            metrics,
        }
    }

    pub fn push_event_handler(&self) -> PushEventHandler {
        PushEventHandler::new(
            Arc::clone(&self.pipeline),
            Arc::clone(&self.repository_service),
            Arc::clone(&self.watermark_store) as _,
            Arc::clone(&self.project_store) as _,
            self.metrics.clone(),
            PushEventHandlerConfig::default(),
        )
    }

    pub fn reconciliation_handler(&self) -> ProjectCodeIndexingHandler {
        ProjectCodeIndexingHandler::new(
            Arc::clone(&self.pipeline),
            Arc::clone(&self.repository_service),
            Arc::clone(&self.watermark_store) as _,
            Arc::clone(&self.project_store) as _,
            Arc::clone(&self.push_event_store) as _,
            self.metrics.clone(),
            ProjectCodeIndexingHandlerConfig::default(),
        )
    }
}

struct DirectGitalyRepositoryService {
    address: String,
    storage: String,
    token: Option<String>,
}

#[async_trait]
impl RepositoryService for DirectGitalyRepositoryService {
    async fn repository_info(&self, project_id: i64) -> Result<RepositoryInfo, GitalyError> {
        let config = GitalyRepositoryConfig {
            address: self.address.clone(),
            storage: self.storage.clone(),
            relative_path: hashed_repo_path(project_id),
            token: self.token.clone(),
        };
        let client = GitalyClient::connect(config).await?;
        let raw_branch = client.find_default_branch_name().await?.ok_or_else(|| {
            GitalyError::Config(format!("no default branch for project {project_id}"))
        })?;
        let default_branch = raw_branch
            .strip_prefix("refs/heads/")
            .unwrap_or(&raw_branch)
            .to_string();

        Ok(RepositoryInfo {
            project_id,
            default_branch,
            gitaly_connection_info: GitalyConnectionInfo {
                address: self.address.clone(),
                token: self.token.clone(),
                storage: self.storage.clone(),
                path: hashed_repo_path(project_id),
            },
        })
    }

    async fn fetch_archive(
        &self,
        repository: &RepositoryInfo,
        target_dir: &Path,
        commit_id: &str,
    ) -> Result<PathBuf, GitalyError> {
        let config = GitalyRepositoryConfig {
            address: repository.gitaly_connection_info.address.clone(),
            storage: repository.gitaly_connection_info.storage.clone(),
            relative_path: repository.gitaly_connection_info.path.clone(),
            token: repository.gitaly_connection_info.token.clone(),
        };
        let client = GitalyClient::connect(config).await?;
        client.fetch_archive(target_dir, Some(commit_id)).await
    }
}

fn create_direct_gitaly_service(address: &str, token: &str) -> Arc<dyn RepositoryService> {
    Arc::new(DirectGitalyRepositoryService {
        address: address.to_string(),
        storage: "default".to_string(),
        token: Some(token.to_string()),
    })
}

pub fn handler_context(clickhouse: &TestContext) -> HandlerContext {
    use indexer::clickhouse::ClickHouseDestination;
    use indexer::metrics::EngineMetrics;

    let destination = ClickHouseDestination::new(
        clickhouse.config.clone(),
        Arc::new(EngineMetrics::default()),
    )
    .expect("failed to create destination");

    HandlerContext::new(
        Arc::new(destination),
        Arc::new(MockNatsServices::new()),
        Arc::new(MockLockService::new()),
    )
}

pub async fn seed_project(
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

pub async fn seed_push_event(
    clickhouse: &TestContext,
    project_id: i64,
    event_id: i64,
    branch: &str,
    commit_sha: &str,
) {
    clickhouse
        .execute(&format!(
            "INSERT INTO siphon_push_event_payloads \
             (commit_count, action, ref_type, commit_to, ref, event_id, project_id) \
             VALUES (1, 2, 0, '{commit_sha}', '{branch}', {event_id}, {project_id})",
        ))
        .await;
}

pub async fn assert_code_indexed(clickhouse: &TestContext, project_id: i64) {
    let files = clickhouse
        .query(&format!(
            "SELECT path FROM gl_file WHERE project_id = {project_id}"
        ))
        .await;
    assert!(
        files.first().is_some_and(|b| b.num_rows() > 0),
        "no files indexed"
    );

    let definitions = clickhouse
        .query(&format!(
            "SELECT name FROM gl_definition WHERE project_id = {project_id}"
        ))
        .await;
    assert!(
        definitions.first().is_some_and(|b| b.num_rows() > 0),
        "no definitions indexed"
    );

    let defines_edges = clickhouse
        .query(
            "SELECT source_id FROM gl_edge \
             WHERE source_kind = 'File' AND target_kind = 'Definition' \
             AND relationship_kind = 'DEFINES'",
        )
        .await;
    assert!(
        defines_edges.first().is_some_and(|b| b.num_rows() > 0),
        "no DEFINES edges indexed"
    );
}

pub fn hashed_repo_path(project_id: i64) -> String {
    let hash = format!("{:x}", Sha256::digest(project_id.to_string()));
    format!("@hashed/{}/{}/{}.git", &hash[0..2], &hash[2..4], hash)
}

pub async fn start_gitaly() -> (String, testcontainers::ContainerAsync<GenericImage>) {
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

pub async fn create_test_repo(
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

pub async fn update_repo_file(
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
