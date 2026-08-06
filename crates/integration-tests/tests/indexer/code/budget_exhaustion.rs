//! A repository whose parse cannot finish inside the job budget times out and is never
//! checkpointed, so the sweep re-dispatches it every tick forever. Batches that flushed
//! before the abort still land, so it is left partially present and outside stale-data
//! management rather than absent.
//!
//! The repository is deliberately few-files-but-expensive-to-parse. `max_files` is applied
//! after extraction, so it can only rescue a repository whose *parse* is what overruns;
//! one that cannot even be extracted in time is unaffected by it.

use gkg_server_config::CodeIndexingPipelineConfig;
use indexer::handler::Handler;
use indexer::topic::CodeIndexingTaskRequest;
use indexer::types::Envelope;
use integration_testkit::t;

use super::helpers::{CodeIndexingDeps, MockGitlabServer, handler_context};

const PROJECT_ID: i64 = 4242;
const TRAVERSAL_PATH: &str = "1/4242/";
const BRANCH: &str = "main";
const FILE_COUNT: usize = 10_000;
const METHODS_PER_FILE: usize = 80;
const BUDGET_SECS: u64 = 10;

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

fn java_source(i: usize) -> String {
    let mut s = format!("package p{};\n\npublic class C{} {{\n", i % 40, i);
    for m in 0..METHODS_PER_FILE {
        s.push_str(&format!(
            "  public int m{m}(int a, int b) {{ int c = a + b; for (int i = 0; i < b; i++) {{ c += i * a; }} return c; }}\n"
        ));
    }
    s.push_str("}\n");
    s
}

fn big_repository() -> Vec<(String, String)> {
    (0..FILE_COUNT)
        .map(|i| (format!("src/p{}/C{}.java", i % 40, i), java_source(i)))
        .collect()
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

async fn index_once(pipeline_config: CodeIndexingPipelineConfig) -> (bool, bool) {
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let owned = big_repository();
    let borrowed: Vec<(&str, &str)> = owned
        .iter()
        .map(|(p, c)| (p.as_str(), c.as_str()))
        .collect();
    let mock = MockGitlabServer::start().await;
    mock.add_project(PROJECT_ID, BRANCH, &borrowed);

    let deps = CodeIndexingDeps::new_with_pipeline_config(&mock, &clickhouse, pipeline_config);
    let handler = deps.code_indexing_task_handler();
    let outcome = handler.handle(handler_context(), backfill_envelope()).await;
    let _ = handler.flush().await;
    (outcome.is_ok(), checkpoint_exists(&clickhouse).await)
}

#[tokio::test]
async fn a_repository_whose_parse_overruns_the_budget_never_checkpoints() {
    let (ok, checkpointed) = index_once(CodeIndexingPipelineConfig {
        job_timeout_secs: BUDGET_SECS,
        ..Default::default()
    })
    .await;

    assert!(
        !ok,
        "a repository whose parse exceeds the budget must fail the job"
    );
    assert!(
        !checkpointed,
        "no checkpoint may be written, which is why the sweep re-dispatches it forever"
    );
}

#[tokio::test]
async fn the_bounded_config_lets_the_same_repository_land() {
    let (ok, checkpointed) = index_once(CodeIndexingPipelineConfig {
        job_timeout_secs: BUDGET_SECS,
        max_files: 200,
        worker_threads: 4,
        ..Default::default()
    })
    .await;

    assert!(
        ok,
        "bounding per-repository parse work must bring the same repository inside the budget"
    );
    assert!(
        checkpointed,
        "a checkpoint is what stops the sweep from re-dispatching it"
    );
}

#[tokio::test]
async fn a_repository_over_the_byte_cap_is_checkpointed_instead_of_overrunning() {
    let (ok, checkpointed) = index_once(CodeIndexingPipelineConfig {
        job_timeout_secs: BUDGET_SECS,
        max_total_bytes: 1_000_000,
        ..Default::default()
    })
    .await;

    assert!(
        ok,
        "crossing the byte cap aborts extraction early, which is a terminal outcome rather than a failure"
    );
    assert!(
        checkpointed,
        "the checkpoint is what stops the sweep re-dispatching a repository too big to extract"
    );
}
