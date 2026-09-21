use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use clickhouse_client::FromArrowColumn;
use integration_testkit::{GRAPH_SCHEMA_SQL, PERSISTENT_SCHEMA_SQL, TestContext};
use jobs::{JobKind, JobLedger, JobRef, JobRun, JobState, JobTransition};
use orbit_utils::traversal_path::TraversalPath;
use uuid::Uuid;

const NAMESPACE_DATA: JobKind = JobKind::new("test_namespace_data");
const CODE: JobKind = JobKind::new("test_code");

const ROOT_NAMESPACE: i64 = 100;
const ROOT_PATH: &str = "1/100/";

fn seconds_into_run(seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 21, 12, 0, 0).unwrap() + Duration::seconds(seconds)
}

fn transition(
    job: &JobRef,
    dispatch: Uuid,
    attempt: u32,
    state: JobState,
    at: DateTime<Utc>,
) -> JobTransition {
    JobTransition {
        job: job.clone(),
        dispatch_id: dispatch,
        attempt,
        state,
        reason: None,
        rows_read: 0,
        rows_written: 0,
        started_at: at,
        recorded_at: at,
    }
}

fn pipeline_job(plan_name: &str) -> JobRef {
    JobRef {
        campaign: None,
        namespace_id: ROOT_NAMESPACE,
        traversal_path: TraversalPath::from(ROOT_PATH),
        kind: NAMESPACE_DATA,
        key: plan_name.to_owned(),
    }
}

struct Scenario {
    context: TestContext,
}

impl Scenario {
    async fn new() -> Self {
        Self {
            context: TestContext::new(&[*GRAPH_SCHEMA_SQL, *PERSISTENT_SCHEMA_SQL]).await,
        }
    }

    fn ledger(&self) -> JobLedger {
        JobLedger::new(Arc::new(self.context.create_client()))
    }

    async fn record(&self, transition: JobTransition) {
        self.ledger().record(&transition).await.unwrap();
        self.context.flush_async_inserts().await;
    }

    async fn latest_runs(&self, path: &str) -> Vec<JobRun> {
        self.ledger()
            .latest_runs(&TraversalPath::from(path), &NAMESPACE_DATA)
            .await
            .unwrap()
    }

    async fn only_run(&self, path: &str) -> JobRun {
        let mut runs = self.latest_runs(path).await;
        assert_eq!(runs.len(), 1, "expected one run, got {runs:?}");
        runs.remove(0)
    }
}

#[tokio::test]
async fn latest_runs_is_empty_before_any_transition() {
    let scenario = Scenario::new().await;

    assert!(scenario.latest_runs(ROOT_PATH).await.is_empty());
}

#[tokio::test]
async fn a_running_job_has_no_completion_yet() {
    let scenario = Scenario::new().await;
    let job = pipeline_job("MergeRequest");

    scenario
        .record(transition(
            &job,
            Uuid::new_v4(),
            1,
            JobState::Running,
            seconds_into_run(0),
        ))
        .await;

    let run = scenario.only_run(ROOT_PATH).await;
    assert_eq!(run.key, "MergeRequest");
    assert_eq!(run.state, JobState::Running);
    assert_eq!(run.started_at, seconds_into_run(0));
    assert_eq!(run.completed_at, None);
}

#[tokio::test]
async fn a_succeeded_job_reports_its_rows_and_completion() {
    let scenario = Scenario::new().await;
    let job = pipeline_job("MergeRequest");
    let dispatch = Uuid::new_v4();

    scenario
        .record(transition(
            &job,
            dispatch,
            1,
            JobState::Running,
            seconds_into_run(0),
        ))
        .await;
    scenario
        .record(JobTransition {
            reason: None,
            rows_read: 307,
            rows_written: 465,
            started_at: seconds_into_run(0),
            ..transition(&job, dispatch, 1, JobState::Succeeded, seconds_into_run(5))
        })
        .await;

    let run = scenario.only_run(ROOT_PATH).await;
    assert_eq!(run.state, JobState::Succeeded);
    assert_eq!(run.started_at, seconds_into_run(0));
    assert_eq!(run.completed_at, Some(seconds_into_run(5)));
    assert_eq!((run.rows_read, run.rows_written), (307, 465));
    assert_eq!(run.reason, None);
}

#[tokio::test]
async fn a_failed_job_keeps_its_reason() {
    let scenario = Scenario::new().await;
    let job = pipeline_job("MergeRequest");
    let dispatch = Uuid::new_v4();

    scenario
        .record(transition(
            &job,
            dispatch,
            1,
            JobState::Running,
            seconds_into_run(0),
        ))
        .await;
    scenario
        .record(JobTransition {
            reason: Some("scan failure".to_owned()),
            rows_read: 0,
            rows_written: 0,
            ..transition(&job, dispatch, 1, JobState::Failed, seconds_into_run(2))
        })
        .await;

    let run = scenario.only_run(ROOT_PATH).await;
    assert_eq!(run.state, JobState::Failed);
    assert_eq!(run.reason.as_deref(), Some("scan failure"));
    assert_eq!(run.completed_at, Some(seconds_into_run(2)));
}

#[tokio::test]
async fn a_new_run_keeps_the_previous_completion_while_running() {
    let scenario = Scenario::new().await;
    let job = pipeline_job("MergeRequest");
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();

    scenario
        .record(transition(
            &job,
            first,
            1,
            JobState::Running,
            seconds_into_run(0),
        ))
        .await;
    scenario
        .record(transition(
            &job,
            first,
            1,
            JobState::Succeeded,
            seconds_into_run(5),
        ))
        .await;
    scenario
        .record(transition(
            &job,
            second,
            1,
            JobState::Running,
            seconds_into_run(60),
        ))
        .await;

    let run = scenario.only_run(ROOT_PATH).await;
    assert_eq!(run.state, JobState::Running);
    assert_eq!(run.started_at, seconds_into_run(60));
    assert_eq!(run.completed_at, Some(seconds_into_run(5)));
}

#[tokio::test]
async fn a_late_lower_ranked_row_cannot_regress_the_same_attempt() {
    let scenario = Scenario::new().await;
    let job = pipeline_job("MergeRequest");
    let dispatch = Uuid::new_v4();

    scenario
        .record(transition(
            &job,
            dispatch,
            1,
            JobState::Succeeded,
            seconds_into_run(0),
        ))
        .await;
    scenario
        .record(transition(
            &job,
            dispatch,
            1,
            JobState::Running,
            seconds_into_run(5),
        ))
        .await;

    assert_eq!(
        scenario.only_run(ROOT_PATH).await.state,
        JobState::Succeeded
    );
}

#[tokio::test]
async fn a_late_row_from_an_older_attempt_cannot_change_the_newer_attempt() {
    let scenario = Scenario::new().await;
    let job = pipeline_job("MergeRequest");
    let dispatch = Uuid::new_v4();

    scenario
        .record(transition(
            &job,
            dispatch,
            2,
            JobState::Succeeded,
            seconds_into_run(0),
        ))
        .await;
    scenario
        .record(transition(
            &job,
            dispatch,
            1,
            JobState::Failed,
            seconds_into_run(5),
        ))
        .await;

    assert_eq!(
        scenario.only_run(ROOT_PATH).await.state,
        JobState::Succeeded
    );
}

#[tokio::test]
async fn latest_runs_scopes_by_path_prefix_and_kind() {
    let scenario = Scenario::new().await;
    let in_scope = pipeline_job("MergeRequest");
    let other_kind = JobRef {
        kind: CODE,
        ..pipeline_job("MergeRequest")
    };
    let other_namespace = JobRef {
        namespace_id: 101,
        traversal_path: TraversalPath::from("1/101/"),
        ..pipeline_job("MergeRequest")
    };

    for job in [&in_scope, &other_kind, &other_namespace] {
        scenario
            .record(transition(
                job,
                Uuid::new_v4(),
                1,
                JobState::Succeeded,
                seconds_into_run(0),
            ))
            .await;
    }

    assert_eq!(scenario.latest_runs(ROOT_PATH).await.len(), 1);
    assert_eq!(scenario.latest_runs("1/100/1000/").await.len(), 0);
    assert_eq!(scenario.latest_runs("1/").await.len(), 2);
}

#[tokio::test]
async fn persistent_schema_renders_the_job_table_as_designed() {
    let sql = *PERSISTENT_SCHEMA_SQL;

    assert!(sql.contains("CREATE TABLE IF NOT EXISTS job ("));
    assert!(sql.contains(
        "PROJECTION by_namespace (SELECT * ORDER BY (namespace_id, kind, key, recorded_at))"
    ));
    assert!(sql.contains("TTL recorded_at + INTERVAL 30 DAY WHERE campaign_kind = ''"));
    assert!(sql.contains("_version UInt64"));
}

#[tokio::test]
async fn boot_adds_missing_columns_to_an_existing_unversioned_table() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    context
        .execute(
            "CREATE TABLE job (kind String, key String, _version UInt64) \
             ENGINE = ReplacingMergeTree(_version) ORDER BY (kind, key)",
        )
        .await;

    let ontology = ontology::Ontology::load_embedded().unwrap();
    let schema = orbit_migrations::schema::GraphSchema::from_ontology(&ontology);
    orbit_migrations::execute::create_unversioned_definitions(&context.create_client(), &schema)
        .await
        .unwrap();

    let batches = context
        .query(
            "SELECT name FROM system.columns \
             WHERE database = currentDatabase() AND table = 'job' ORDER BY position",
        )
        .await;
    let names = String::extract_column(&batches, 0).unwrap();
    for column in [
        "started_at",
        "campaign_kind",
        "namespace_id",
        "rows_read",
        "rows_written",
        "recorded_at",
        "_deleted",
    ] {
        assert!(
            names.iter().any(|name| name == column),
            "missing {column}: {names:?}"
        );
    }
}
