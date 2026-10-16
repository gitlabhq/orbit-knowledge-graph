use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use indexer::handler::{Handler, HandlerContext, HandlerError, PermanentAction};
use indexer::modules::code::MemoryWatchdog;
use indexer::modules::code::metrics::CodeMetrics;
use indexer::nats::{MessageAcker, NatsError, ProgressNotifier};
use indexer::testkit::{MockLockService, MockNatsServices};
use indexer::types::Envelope;
use integration_testkit::TestContext;
use orbit_server_config::CodeIndexingPipelineConfig;
use orbit_server_config::resources::MemoryPressure;

use super::helpers::{
    CodeIndexingDeps, MockGitlabServer, checkpointed, code_indexing_backfill_envelope,
    enable_incremental, handler_context,
};

const BIG: i64 = 4300;
const SMALL: i64 = 4301;
const BRANCH: &str = "main";
const LIMIT: u64 = 1000;

fn traversal_path(project_id: i64) -> String {
    format!("1/{project_id}/")
}

fn envelope(project_id: i64, attempt: u32) -> Envelope {
    let mut envelope = code_indexing_backfill_envelope(project_id, 0, &traversal_path(project_id));
    envelope.attempt = attempt;
    envelope
}

async fn clickhouse() -> TestContext {
    TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await
}

async fn indexed(clickhouse: &TestContext, project_id: i64) -> bool {
    checkpointed(clickhouse, &traversal_path(project_id), project_id, BRANCH).await
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

fn add_project(mock: &MockGitlabServer, project_id: i64, files: &[(String, String)]) {
    let files: Vec<(&str, &str)> = files
        .iter()
        .map(|(path, content)| (path.as_str(), content.as_str()))
        .collect();
    mock.add_project(project_id, BRANCH, &files);
}

fn fake_pressure(used: &Arc<AtomicU64>) -> Arc<MemoryWatchdog> {
    let used = used.clone();
    MemoryWatchdog::new(
        Box::new(move || {
            Some(MemoryPressure {
                used: used.load(Ordering::Relaxed),
                limit: LIMIT,
            })
        }),
        CodeMetrics::new(),
        90,
    )
}

fn two_big_lanes() -> CodeIndexingPipelineConfig {
    CodeIndexingPipelineConfig {
        small_repo_max_files: 0,
        big_indexing_slots: Some(2),
        per_file_timeout_ms: 0,
        per_file_parse_timeout_ms: 0,
        per_file_walk_timeout_ms: 0,
        per_file_ssa_timeout_ms: 0,
        ..indexer::testkit::test_pipeline_configuration()
    }
}

fn retried<T>(result: &Result<T, HandlerError>) -> bool {
    matches!(result, Err(HandlerError::Processing(message)) if message.contains("memory pressure"))
}

fn dead_lettered<T>(result: &Result<T, HandlerError>) -> bool {
    matches!(
        result,
        Err(HandlerError::Permanent { action: PermanentAction::DeadLetter, message })
            if message.contains("memory pressure")
    )
}

fn backpressured<T>(result: &Result<T, HandlerError>) -> bool {
    matches!(result, Err(HandlerError::Backpressure(message)) if message.contains("memory pressure"))
}

#[tokio::test]
async fn a_job_killed_under_pressure_is_retried_then_dead_lettered_and_a_small_job_lands() {
    enable_incremental(&traversal_path(BIG));
    let clickhouse = clickhouse().await;
    let mock = MockGitlabServer::start().await;
    add_project(&mock, BIG, &big_repository(60));
    mock.add_project(
        SMALL,
        BRANCH,
        &[("src/main.py", "def small():\n    pass\n")],
    );
    let used = Arc::new(AtomicU64::new(950));
    let deps = CodeIndexingDeps::new_with_memory_watchdog(
        &mock,
        &clickhouse,
        indexer::testkit::test_pipeline_configuration(),
        fake_pressure(&used),
    );
    let handler = deps.code_indexing_task_handler();

    let first = handler.handle(handler_context(), envelope(BIG, 1)).await;
    assert!(retried(&first), "the first attempt is retried: {first:?}");
    used.store(960, Ordering::Relaxed);
    let second = handler.handle(handler_context(), envelope(BIG, 2)).await;
    assert!(
        dead_lettered(&second),
        "the retry parses again, memory rises, the second attempt dead-letters: {second:?}"
    );

    used.store(500, Ordering::Relaxed);
    let small = handler.handle(handler_context(), envelope(SMALL, 1)).await;
    assert!(small.is_ok(), "{small:?}");
    handler.flush().await.expect("flush");
    assert!(indexed(&clickhouse, SMALL).await);
    assert!(!indexed(&clickhouse, BIG).await);
}

#[derive(Default)]
struct ProgressAcks(AtomicUsize);

#[async_trait::async_trait]
impl MessageAcker for ProgressAcks {
    async fn ack(&self) -> Result<(), NatsError> {
        Ok(())
    }
    async fn ack_term(&self) -> Result<(), NatsError> {
        Ok(())
    }
    async fn ack_progress(&self) -> Result<(), NatsError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    async fn nack(&self, _: Option<std::time::Duration>) -> Result<(), NatsError> {
        Ok(())
    }
}

fn counted_context() -> (Arc<ProgressAcks>, HandlerContext) {
    let acks = Arc::new(ProgressAcks::default());
    let context = HandlerContext::new(
        Arc::new(MockNatsServices::new()),
        Arc::new(MockLockService::new()),
        ProgressNotifier::new(acks.clone()),
    );
    (acks, context)
}

async fn wait_for_acks(acks: &ProgressAcks, at_least: usize, what: &str) {
    let started = std::time::Instant::now();
    while acks.0.load(Ordering::Relaxed) < at_least {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(60),
            "{what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_lane_waiter_is_never_the_victim_while_a_job_parses() {
    const PARSING: i64 = 4302;
    const WAITING: i64 = 4303;
    enable_incremental(&traversal_path(PARSING));
    let clickhouse = clickhouse().await;
    let mock = MockGitlabServer::start().await;
    add_project(&mock, PARSING, &big_repository(60));
    add_project(&mock, WAITING, &big_repository(70));
    mock.slow_archive(WAITING);
    let used = Arc::new(AtomicU64::new(500));
    let deps = CodeIndexingDeps::new_with_memory_watchdog(
        &mock,
        &clickhouse,
        CodeIndexingPipelineConfig {
            big_indexing_slots: Some(1),
            ..two_big_lanes()
        },
        fake_pressure(&used),
    );
    let first = deps.code_indexing_task_handler();
    let second = deps.code_indexing_task_handler();
    let (acks, waiting_context) = counted_context();
    let parsing_job =
        tokio::spawn(async move { first.handle(handler_context(), envelope(PARSING, 1)).await });
    let waiting_job =
        tokio::spawn(async move { second.handle(waiting_context, envelope(WAITING, 1)).await });

    wait_for_acks(&acks, 1, "the waiter never reached the lane").await;
    assert!(
        !parsing_job.is_finished(),
        "the parsing job must still hold the lane when the pressure rises"
    );
    used.store(950, Ordering::Relaxed);
    let parsed = parsing_job.await.expect("join");
    used.store(500, Ordering::Relaxed);
    let waited = waiting_job.await.expect("join");
    assert!(
        retried(&parsed) && waited.is_ok(),
        "the victim must be the job on the lane, not the waiter: parsing {parsed:?}, waiting {waited:?}"
    );
    deps.code_indexing_task_handler()
        .flush()
        .await
        .expect("flush");
    assert!(indexed(&clickhouse, WAITING).await);
    assert!(!indexed(&clickhouse, PARSING).await);
}

#[tokio::test]
async fn a_kill_while_another_job_is_in_flight_does_not_spend_an_attempt() {
    const LARGER: i64 = 4304;
    const OTHER: i64 = 4305;
    enable_incremental(&traversal_path(LARGER));
    let clickhouse = clickhouse().await;
    let mock = MockGitlabServer::start().await;
    add_project(&mock, LARGER, &big_repository(70));
    add_project(&mock, OTHER, &big_repository(60));
    let used = Arc::new(AtomicU64::new(500));
    let deps = CodeIndexingDeps::new_with_memory_watchdog(
        &mock,
        &clickhouse,
        two_big_lanes(),
        fake_pressure(&used),
    );
    let larger = deps.code_indexing_task_handler();
    let other = deps.code_indexing_task_handler();
    let (larger_acks, larger_context) = counted_context();
    let (other_acks, other_context) = counted_context();
    let larger_job =
        tokio::spawn(async move { larger.handle(larger_context, envelope(LARGER, 2)).await });
    let other_job =
        tokio::spawn(async move { other.handle(other_context, envelope(OTHER, 1)).await });

    wait_for_acks(&larger_acks, 2, "the larger job never reached its lane").await;
    wait_for_acks(&other_acks, 2, "the other job never reached its lane").await;
    assert!(
        !larger_job.is_finished() && !other_job.is_finished(),
        "both jobs must be parsing when the pressure rises"
    );
    used.store(950, Ordering::Relaxed);
    let killed = larger_job.await.expect("join");
    used.store(500, Ordering::Relaxed);
    let survived = other_job.await.expect("join");
    assert!(
        backpressured(&killed),
        "a kill under shared memory is redelivered without spending attempt 2: {killed:?}"
    );
    assert!(survived.is_ok(), "{survived:?}");
    deps.code_indexing_task_handler()
        .flush()
        .await
        .expect("flush");
    assert!(indexed(&clickhouse, OTHER).await);
    assert!(!indexed(&clickhouse, LARGER).await);
}

#[tokio::test]
async fn a_file_larger_than_the_in_flight_budget_is_indexed_with_the_rest() {
    const OVERSIZE: i64 = 4306;
    enable_incremental(&traversal_path(OVERSIZE));
    let clickhouse = clickhouse().await;
    let mock = MockGitlabServer::start().await;
    let mut files = big_repository(50);
    files.push((
        "big.py".to_string(),
        format!("'''{}'''\n", "x".repeat(4_200_000)),
    ));
    add_project(&mock, OVERSIZE, &files);
    let deps = CodeIndexingDeps::new_with_pipeline_config(
        &mock,
        &clickhouse,
        indexer::testkit::test_pipeline_configuration(),
    );
    let handler = deps.code_indexing_task_handler();

    let outcome = handler
        .handle(handler_context(), envelope(OVERSIZE, 1))
        .await;
    assert!(outcome.is_ok(), "{outcome:?}");
    handler.flush().await.expect("flush");
    assert!(indexed(&clickhouse, OVERSIZE).await);
}
