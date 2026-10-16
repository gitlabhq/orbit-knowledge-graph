//! The kill path under a fake cgroup, and the gated proof under a real memory
//! limit (`mise run test:integration:memory`), where the kernel would otherwise
//! kill the whole process and every job in it.

use indexer::handler::{Handler, HandlerError, PermanentAction};
use indexer::topic::CodeIndexingTaskRequest;
use indexer::types::Envelope;
use integration_testkit::{TestContext, t};
use orbit_server_config::CodeIndexingPipelineConfig;
use orbit_utils::traversal_path::TraversalPath;

use indexer::modules::code::metrics::CodeMetrics;
use indexer::modules::code::{Cgroup, MemoryWatchdog};

use super::helpers::{CodeIndexingDeps, MockGitlabServer, enable_incremental, handler_context};

const BIG: i64 = 4300;
const SMALL: i64 = 4301;
const BRANCH: &str = "main";

fn traversal_path(project_id: i64) -> String {
    format!("1/{project_id}/")
}

fn envelope(project_id: i64, attempt: u32) -> Envelope {
    let mut envelope = Envelope::new(&CodeIndexingTaskRequest {
        task_id: 0,
        project_id,
        branch: Some(BRANCH.to_string()),
        commit_sha: None,
        traversal_path: TraversalPath::new_unchecked(traversal_path(project_id)),
        dispatch_id: uuid::Uuid::new_v4(),
        campaign_id: None,
    })
    .expect("envelope");
    envelope.attempt = attempt;
    envelope
}

async fn indexed(clickhouse: &TestContext, project_id: i64) -> bool {
    let rows = clickhouse
        .query(&format!(
            "SELECT last_task_id FROM {} FINAL \
             WHERE traversal_path = '{}' AND project_id = {project_id} \
             AND branch = '{BRANCH}' AND indexed_at IS NOT NULL AND _deleted = false",
            t("code_indexing_checkpoint"),
            traversal_path(project_id)
        ))
        .await;
    rows.first().is_some_and(|b| b.num_rows() > 0)
}

fn big_repository(files: usize) -> Vec<(String, String)> {
    (0..files)
        .map(|file| {
            let body: String = (0..4000)
                .map(|i| format!("def f{file}_{i}(x):\n    return helper(x) + {i}\n\n"))
                .collect();
            (
                format!("pkg{}/m{file}.py", file % 50),
                format!("from .helper import helper\n\n{body}"),
            )
        })
        .collect()
}

fn fake_cgroup(dir: &std::path::Path, limit: u64, anon: u64) {
    std::fs::write(dir.join("memory.max"), format!("{limit}\n")).expect("memory.max");
    std::fs::write(dir.join("memory.stat"), format!("anon {anon}\nfile 0\n")).expect("memory.stat");
}

#[tokio::test]
async fn a_job_killed_under_pressure_is_retried_then_dead_lettered_and_a_small_job_lands() {
    enable_incremental(&traversal_path(BIG));
    let clickhouse = TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;
    let mock = MockGitlabServer::start().await;
    {
        let big = big_repository(300);
        let files: Vec<(&str, &str)> = big
            .iter()
            .map(|(path, content)| (path.as_str(), content.as_str()))
            .collect();
        mock.add_project(BIG, BRANCH, &files);
    }
    mock.add_project(
        SMALL,
        BRANCH,
        &[("src/main.py", "def small():\n    pass\n")],
    );
    let cgroup = tempfile::tempdir().expect("cgroup dir");
    fake_cgroup(cgroup.path(), 1000, 950);
    let watchdog = MemoryWatchdog::new(
        Cgroup::v2(cgroup.path().to_path_buf()),
        CodeMetrics::new(),
        90,
    );
    let deps = CodeIndexingDeps::new_with_memory_watchdog(
        &mock,
        &clickhouse,
        indexer::testkit::test_pipeline_configuration(),
        watchdog,
    );
    let handler = deps.code_indexing_task_handler();

    let first = handler.handle(handler_context(), envelope(BIG, 1)).await;
    assert!(
        matches!(&first, Err(HandlerError::Processing(message)) if message.contains("memory pressure")),
        "the first attempt is retried: {first:?}"
    );
    let second = handler.handle(handler_context(), envelope(BIG, 2)).await;
    assert!(
        matches!(
            &second,
            Err(HandlerError::Permanent { action: PermanentAction::DeadLetter, message })
                if message.contains("memory pressure")
        ),
        "the second attempt dead-letters: {second:?}"
    );

    fake_cgroup(cgroup.path(), 1000, 500);
    let small = handler.handle(handler_context(), envelope(SMALL, 1)).await;
    assert!(small.is_ok(), "{small:?}");
    handler.flush().await.expect("flush");
    assert!(indexed(&clickhouse, SMALL).await);
    assert!(!indexed(&clickhouse, BIG).await);
}

fn refs(files: &[(String, String)]) -> Vec<(&str, &str)> {
    files
        .iter()
        .map(|(path, content)| (path.as_str(), content.as_str()))
        .collect()
}

#[tokio::test]
async fn a_lane_waiter_is_never_the_victim_while_a_job_parses() {
    const PARSING: i64 = 4302;
    const WAITING: i64 = 4303;
    enable_incremental(&traversal_path(PARSING));
    let clickhouse = TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;
    let mock = MockGitlabServer::start().await;
    let parsing = big_repository(60);
    mock.add_project(PARSING, BRANCH, &refs(&parsing));
    let waiting = big_repository(70);
    mock.add_project_with_slow_archive_files(WAITING, BRANCH, &refs(&waiting));
    let cgroup = tempfile::tempdir().expect("cgroup dir");
    fake_cgroup(cgroup.path(), 1000, 500);
    let watchdog = MemoryWatchdog::new(
        Cgroup::v2(cgroup.path().to_path_buf()),
        CodeMetrics::new(),
        90,
    );
    let deps = CodeIndexingDeps::new_with_memory_watchdog(
        &mock,
        &clickhouse,
        CodeIndexingPipelineConfig {
            small_repo_max_files: 0,
            big_indexing_slots: Some(1),
            per_file_timeout_ms: 0,
            per_file_parse_timeout_ms: 0,
            per_file_walk_timeout_ms: 0,
            per_file_ssa_timeout_ms: 0,
            ..indexer::testkit::test_pipeline_configuration()
        },
        watchdog.clone(),
    );
    let first = deps.code_indexing_task_handler();
    let second = deps.code_indexing_task_handler();
    let parsing_job =
        tokio::spawn(async move { first.handle(handler_context(), envelope(PARSING, 1)).await });
    let waiting_job =
        tokio::spawn(async move { second.handle(handler_context(), envelope(WAITING, 1)).await });

    let started = std::time::Instant::now();
    while mock.archive_downloads(WAITING) == 0 || watchdog.candidates() == 0 {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(60),
            "the waiter never fetched"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    fake_cgroup(cgroup.path(), 1000, 950);
    let parsed = parsing_job.await.expect("join");
    fake_cgroup(cgroup.path(), 1000, 500);
    eprintln!("parsing job: {parsed:?}");
    assert!(
        matches!(&parsed, Err(HandlerError::Processing(message)) if message.contains("memory pressure")),
        "the job on the lane is the victim: {parsed:?}"
    );
    let waited = waiting_job.await.expect("join");
    assert!(
        waited.is_ok(),
        "the lane waiter lands once it gets the lane: {waited:?}"
    );
    deps.code_indexing_task_handler()
        .flush()
        .await
        .expect("flush");
    assert!(indexed(&clickhouse, WAITING).await);
    assert!(!indexed(&clickhouse, PARSING).await);
}

#[tokio::test]
#[ignore = "needs a cgroup limit: mise run test:integration:memory"]
async fn a_job_under_memory_pressure_dead_letters_alone_and_a_small_job_lands() {
    enable_incremental(&traversal_path(BIG));
    let clickhouse = TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;
    let mock = MockGitlabServer::start().await;
    {
        let big = big_repository(1200);
        let files: Vec<(&str, &str)> = big
            .iter()
            .map(|(path, content)| (path.as_str(), content.as_str()))
            .collect();
        mock.add_project(BIG, BRANCH, &files);
    }
    mock.add_project(
        SMALL,
        BRANCH,
        &[("src/main.py", "def small():\n    pass\n")],
    );
    let deps = CodeIndexingDeps::new_with_pipeline_config(
        &mock,
        &clickhouse,
        CodeIndexingPipelineConfig {
            per_file_timeout_ms: 0,
            per_file_parse_timeout_ms: 0,
            per_file_walk_timeout_ms: 0,
            per_file_ssa_timeout_ms: 0,
            ..indexer::testkit::test_pipeline_configuration()
        },
    );
    let handler = deps.code_indexing_task_handler();

    let (first, small) = tokio::join!(
        handler.handle(handler_context(), envelope(BIG, 1)),
        handler.handle(handler_context(), envelope(SMALL, 1)),
    );
    eprintln!("big job, attempt 1: {first:?}");
    eprintln!("small job: {small:?}");
    assert!(small.is_ok(), "the co-scheduled job lands: {small:?}");
    assert!(
        matches!(&first, Err(HandlerError::Processing(message)) if message.contains("memory pressure")),
        "the first attempt is retried: {first:?}"
    );

    let second = handler.handle(handler_context(), envelope(BIG, 2)).await;
    eprintln!("big job, attempt 2: {second:?}");
    assert!(
        matches!(
            &second,
            Err(HandlerError::Permanent { action: PermanentAction::DeadLetter, message })
                if message.contains("memory pressure")
        ),
        "the second attempt dead-letters: {second:?}"
    );

    handler.flush().await.expect("flush");
    assert!(indexed(&clickhouse, SMALL).await);
    assert!(!indexed(&clickhouse, BIG).await);
}
