//! An overrun leaves no checkpoint, so the sweep re-dispatches the repository every tick;
//! landing writes one, which is what stops it.

use indexer::handler::Handler;
use indexer::topic::CodeIndexingTaskRequest;
use indexer::types::Envelope;
use integration_testkit::t;
use orbit_server_config::CodeIndexingPipelineConfig;

use super::helpers::{CodeIndexingDeps, MockGitlabServer, handler_context};

const PROJECT_ID: i64 = 4242;
const TRAVERSAL_PATH: &str = "1/4242/";
const BRANCH: &str = "main";

fn backfill_envelope() -> Envelope {
    Envelope::new(&CodeIndexingTaskRequest {
        task_id: 0,
        project_id: PROJECT_ID,
        branch: Some(BRANCH.to_string()),
        commit_sha: None,
        traversal_path: TRAVERSAL_PATH.to_string(),
        dispatch_id: uuid::Uuid::new_v4(),
        campaign_id: None,
    })
    .expect("envelope")
}

async fn checkpoint_exists(clickhouse: &integration_testkit::TestContext) -> bool {
    let rows = clickhouse
        .query(&format!(
            "SELECT last_task_id FROM {} FINAL \
             WHERE traversal_path = '{TRAVERSAL_PATH}' AND project_id = {PROJECT_ID} \
             AND branch = '{BRANCH}' AND _deleted = false",
            t("code_indexing_checkpoint")
        ))
        .await;
    rows.first().is_some_and(|b| b.num_rows() > 0)
}

async fn index_once(
    pipeline_config: CodeIndexingPipelineConfig,
    add_project: impl FnOnce(&MockGitlabServer),
) -> (Result<(), indexer::handler::HandlerError>, bool) {
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;
    let mock = MockGitlabServer::start().await;
    add_project(&mock);

    let deps = CodeIndexingDeps::new_with_pipeline_config(&mock, &clickhouse, pipeline_config);
    let handler = deps.code_indexing_task_handler();
    let outcome = handler.handle(handler_context(), backfill_envelope()).await;
    let _ = handler.flush().await;
    (outcome, checkpoint_exists(&clickhouse).await)
}

#[tokio::test]
async fn a_job_too_slow_for_its_budget_never_checkpoints() {
    // The slow archive takes 3s to start; a 1s budget overruns on any host.
    let (outcome, checkpointed) = index_once(
        CodeIndexingPipelineConfig {
            job_timeout_secs: 1,
            ..Default::default()
        },
        |mock| mock.add_project_with_slow_archive(PROJECT_ID, BRANCH),
    )
    .await;

    assert!(
        outcome.is_err(),
        "a job that overruns its work budget must fail"
    );
    assert!(
        !checkpointed,
        "no checkpoint may be written, which is why the sweep re-dispatches it forever"
    );
}

#[tokio::test]
async fn the_same_repository_lands_once_the_work_fits() {
    let (outcome, checkpointed) = index_once(
        CodeIndexingPipelineConfig {
            job_timeout_secs: 30,
            ..Default::default()
        },
        |mock| mock.add_project(PROJECT_ID, BRANCH, &[("src/main.rs", "pub fn x() {}")]),
    )
    .await;

    assert!(outcome.is_ok(), "work inside the budget must land");
    assert!(
        checkpointed,
        "a checkpoint is what stops the sweep from re-dispatching it"
    );
}

#[tokio::test]
async fn a_repository_over_the_byte_cap_is_checkpointed_instead_of_overrunning() {
    let oversized = "x".repeat(2_000_000);
    let (outcome, checkpointed) = index_once(
        CodeIndexingPipelineConfig {
            max_total_bytes: 1_000_000,
            ..Default::default()
        },
        |mock| mock.add_project(PROJECT_ID, BRANCH, &[("big.txt", &oversized)]),
    )
    .await;

    assert!(
        outcome.is_ok(),
        "crossing the byte cap ends the job as terminal-empty rather than failing it"
    );
    assert!(
        checkpointed,
        "the checkpoint is what stops the sweep re-dispatching a repository too big to extract"
    );
}
