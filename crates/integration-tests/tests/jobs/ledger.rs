use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use clickhouse_client::FromArrowColumn;
use integration_testkit::{GRAPH_SCHEMA_SQL, PERSISTENT_SCHEMA_SQL, TestContext};
use jobs::{
    CampaignId, CampaignKind, CampaignSummary, JobFilter, JobKind, JobLedger, JobRef, JobState,
    JobTransition, PhaseSpec,
};
use orbit_utils::traversal_path::TraversalPath;
use uuid::Uuid;

const CODE: JobKind = JobKind::new("test_code");
const NAMESPACE_DATA: JobKind = JobKind::new("test_namespace_data");
const BACKFILL: CampaignKind = CampaignKind::new("test_backfill");

const ROOT_NAMESPACE: i64 = 42;
const ROOT_PATH: &str = "1/42/";

fn generation(offset_hours: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 21, 12, 0, 0).unwrap() + Duration::hours(offset_hours)
}

fn at(seconds: i64) -> DateTime<Utc> {
    generation(1) + Duration::seconds(seconds)
}

fn campaign_id(generation: DateTime<Utc>) -> CampaignId {
    CampaignId {
        kind: BACKFILL,
        subject: ROOT_NAMESPACE.to_string(),
        generation,
    }
}

fn phases() -> [PhaseSpec; 2] {
    [
        PhaseSpec {
            kind: CODE,
            required: true,
        },
        PhaseSpec {
            kind: NAMESPACE_DATA,
            required: true,
        },
    ]
}

fn code_job(campaign: Option<CampaignId>, key: &str) -> JobRef {
    JobRef {
        campaign,
        namespace_id: ROOT_NAMESPACE,
        traversal_path: TraversalPath::from(format!("{ROOT_PATH}{key}/")),
        kind: CODE,
        key: key.to_owned(),
    }
}

fn counts(summary: &CampaignSummary, kind: &JobKind, state: JobState) -> u64 {
    summary
        .phases
        .iter()
        .find(|phase| &phase.kind == kind)
        .map_or(0, |phase| phase.count(state))
}

struct Scenario {
    context: TestContext,
    campaign: CampaignId,
}

impl Scenario {
    async fn new() -> Self {
        Self {
            context: TestContext::new(&[*GRAPH_SCHEMA_SQL, *PERSISTENT_SCHEMA_SQL]).await,
            campaign: campaign_id(generation(0)),
        }
    }

    fn ledger(&self) -> JobLedger {
        JobLedger::new(Arc::new(self.context.create_client()))
    }

    async fn open_with_code_jobs(&self, keys: &[&str]) -> Vec<JobRef> {
        let ledger = self.ledger();
        ledger
            .open_campaign(&self.campaign, &phases())
            .await
            .unwrap();

        let jobs: Vec<JobRef> = keys
            .iter()
            .map(|key| code_job(Some(self.campaign.clone()), key))
            .collect();
        ledger
            .register(&jobs, JobState::Pending, None)
            .await
            .unwrap();
        jobs
    }

    async fn record(
        &self,
        job: &JobRef,
        dispatch: Uuid,
        attempt: u32,
        state: JobState,
        at: DateTime<Utc>,
    ) {
        let transition = JobTransition {
            job: job.clone(),
            dispatch_id: dispatch,
            attempt,
            state,
            reason: None,
            recorded_at: at,
        };
        self.ledger().record(&transition).await.unwrap();
    }

    async fn summary(&self) -> CampaignSummary {
        self.ledger()
            .latest_campaign(&BACKFILL, &self.campaign.subject)
            .await
            .unwrap()
            .expect("campaign should exist")
    }

    async fn current_state(&self, job: &JobRef) -> JobState {
        let mut filter = JobFilter::for_namespace(ROOT_NAMESPACE);
        filter.kind = Some(job.kind.clone());

        let snapshots = self.ledger().jobs(&filter).await.unwrap();
        snapshots
            .into_iter()
            .find(|snapshot| snapshot.job.key == job.key)
            .expect("job should have a snapshot")
            .state
    }
}

#[tokio::test]
async fn opening_a_campaign_and_registering_jobs_reports_pending_counts() {
    let scenario = Scenario::new().await;
    scenario.open_with_code_jobs(&["7", "8"]).await;

    let summary = scenario.summary().await;

    assert_eq!(summary.id, scenario.campaign);
    assert_eq!(summary.phases.len(), 2);
    assert_eq!(counts(&summary, &CODE, JobState::Pending), 2);
    assert_eq!(counts(&summary, &NAMESPACE_DATA, JobState::Pending), 0);
    assert!(!summary.is_complete());
    assert!(!summary.is_abandoned());
}

#[tokio::test]
async fn registering_the_same_jobs_twice_does_not_inflate_counts() {
    let scenario = Scenario::new().await;
    let jobs = scenario.open_with_code_jobs(&["7", "8"]).await;

    scenario
        .ledger()
        .register(&jobs, JobState::Pending, None)
        .await
        .unwrap();

    let summary = scenario.summary().await;
    assert_eq!(counts(&summary, &CODE, JobState::Pending), 2);
}

#[tokio::test]
async fn transitions_move_a_job_through_its_lifecycle() {
    let scenario = Scenario::new().await;
    let jobs = scenario.open_with_code_jobs(&["7", "8"]).await;
    let dispatch = Uuid::new_v4();

    scenario
        .record(&jobs[0], dispatch, 0, JobState::Queued, at(0))
        .await;
    scenario
        .record(&jobs[0], dispatch, 1, JobState::Running, at(1))
        .await;
    scenario
        .record(&jobs[0], dispatch, 1, JobState::Succeeded, at(2))
        .await;

    let summary = scenario.summary().await;
    assert_eq!(counts(&summary, &CODE, JobState::Pending), 1);
    assert_eq!(counts(&summary, &CODE, JobState::Succeeded), 1);
    assert_eq!(scenario.current_state(&jobs[0]).await, JobState::Succeeded);
}

#[tokio::test]
async fn a_late_lower_ranked_row_cannot_regress_the_same_attempt() {
    let scenario = Scenario::new().await;
    let jobs = scenario.open_with_code_jobs(&["7"]).await;
    let dispatch = Uuid::new_v4();

    scenario
        .record(&jobs[0], dispatch, 1, JobState::Succeeded, at(0))
        .await;
    scenario
        .record(&jobs[0], dispatch, 1, JobState::Running, at(5))
        .await;

    assert_eq!(scenario.current_state(&jobs[0]).await, JobState::Succeeded);
}

#[tokio::test]
async fn a_late_row_from_an_older_attempt_cannot_change_the_newer_attempt() {
    let scenario = Scenario::new().await;
    let jobs = scenario.open_with_code_jobs(&["7"]).await;
    let dispatch = Uuid::new_v4();

    scenario
        .record(&jobs[0], dispatch, 2, JobState::Succeeded, at(0))
        .await;
    scenario
        .record(&jobs[0], dispatch, 1, JobState::Failed, at(5))
        .await;

    assert_eq!(scenario.current_state(&jobs[0]).await, JobState::Succeeded);
}

#[tokio::test]
async fn a_newer_dispatch_supersedes_an_older_one() {
    let scenario = Scenario::new().await;
    let jobs = scenario.open_with_code_jobs(&["7"]).await;

    scenario
        .record(&jobs[0], Uuid::new_v4(), 1, JobState::Failed, at(0))
        .await;
    scenario
        .record(&jobs[0], Uuid::new_v4(), 1, JobState::Running, at(5))
        .await;

    assert_eq!(scenario.current_state(&jobs[0]).await, JobState::Running);
}

#[tokio::test]
async fn campaign_completes_when_discovery_is_closed_and_every_job_is_terminal() {
    let scenario = Scenario::new().await;
    let jobs = scenario.open_with_code_jobs(&["7", "8"]).await;
    let ledger = scenario.ledger();
    let [code, namespace_data] = phases();

    scenario
        .record(&jobs[0], Uuid::new_v4(), 1, JobState::Succeeded, at(0))
        .await;
    scenario
        .record(&jobs[1], Uuid::new_v4(), 1, JobState::Failed, at(0))
        .await;
    assert!(!scenario.summary().await.is_complete());

    ledger
        .close_discovery(&scenario.campaign, &code)
        .await
        .unwrap();
    assert!(!scenario.summary().await.is_complete());

    ledger
        .close_discovery(&scenario.campaign, &namespace_data)
        .await
        .unwrap();
    let summary = scenario.summary().await;
    assert!(summary.is_complete());
    assert!(summary.is_ready());
    assert_eq!(summary.count(JobState::Failed), 1);
    assert_eq!(summary.total(), 2);
}

#[tokio::test]
async fn abandoning_a_campaign_sticks_and_reopening_does_not_revive_it() {
    let scenario = Scenario::new().await;
    scenario.open_with_code_jobs(&["7"]).await;
    let ledger = scenario.ledger();

    ledger
        .abandon_campaign(&scenario.campaign, &phases())
        .await
        .unwrap();
    assert!(scenario.summary().await.is_abandoned());

    ledger
        .open_campaign(&scenario.campaign, &phases())
        .await
        .unwrap();
    let summary = scenario.summary().await;
    assert!(summary.is_abandoned());
    assert!(!summary.is_complete());
}

#[tokio::test]
async fn latest_campaign_returns_the_newest_generation_only() {
    let scenario = Scenario::new().await;
    scenario.open_with_code_jobs(&["7", "8", "9"]).await;
    let ledger = scenario.ledger();

    let next = campaign_id(generation(1));
    let next_jobs = [code_job(Some(next.clone()), "7")];
    ledger.open_campaign(&next, &phases()).await.unwrap();
    ledger
        .register(&next_jobs, JobState::Pending, None)
        .await
        .unwrap();

    let summary = scenario.summary().await;
    assert_eq!(summary.id, next);
    assert_eq!(counts(&summary, &CODE, JobState::Pending), 1);
}

#[tokio::test]
async fn latest_campaign_is_none_for_an_unknown_subject() {
    let scenario = Scenario::new().await;

    let summary = scenario
        .ledger()
        .latest_campaign(&BACKFILL, "unknown")
        .await
        .unwrap();

    assert!(summary.is_none());
}

#[tokio::test]
async fn pending_jobs_returns_only_pending_jobs_up_to_the_limit() {
    let scenario = Scenario::new().await;
    let jobs = scenario.open_with_code_jobs(&["1", "2", "3", "4"]).await;
    let ledger = scenario.ledger();

    scenario
        .record(&jobs[0], Uuid::new_v4(), 0, JobState::Queued, at(0))
        .await;
    scenario
        .record(&jobs[1], Uuid::new_v4(), 1, JobState::Succeeded, at(0))
        .await;

    let pending = ledger
        .pending_jobs(&scenario.campaign, ROOT_NAMESPACE, &CODE, 10)
        .await
        .unwrap();
    assert_eq!(pending, vec![jobs[2].clone(), jobs[3].clone()]);

    let limited = ledger
        .pending_jobs(&scenario.campaign, ROOT_NAMESPACE, &CODE, 1)
        .await
        .unwrap();
    assert_eq!(limited, vec![jobs[2].clone()]);
}

#[tokio::test]
async fn latest_success_at_scopes_by_namespace_and_path_prefix() {
    let scenario = Scenario::new().await;
    let jobs = scenario.open_with_code_jobs(&["7", "8"]).await;
    let ledger = scenario.ledger();
    let root = TraversalPath::from(ROOT_PATH);

    assert_eq!(
        ledger
            .latest_success_at(ROOT_NAMESPACE, &root)
            .await
            .unwrap(),
        None
    );

    scenario
        .record(&jobs[0], Uuid::new_v4(), 1, JobState::Succeeded, at(0))
        .await;
    scenario
        .record(&jobs[1], Uuid::new_v4(), 1, JobState::Succeeded, at(60))
        .await;
    let ordinary = code_job(None, "7");
    scenario
        .record(&ordinary, Uuid::new_v4(), 1, JobState::Succeeded, at(120))
        .await;

    let under_root = ledger
        .latest_success_at(ROOT_NAMESPACE, &root)
        .await
        .unwrap();
    assert_eq!(under_root, Some(at(120)));

    let under_project = ledger
        .latest_success_at(ROOT_NAMESPACE, &jobs[1].traversal_path)
        .await
        .unwrap();
    assert_eq!(under_project, Some(at(60)));

    let other_namespace = ledger.latest_success_at(99, &root).await.unwrap();
    assert_eq!(other_namespace, None);
}

#[tokio::test]
async fn jobs_filter_returns_one_snapshot_per_job_with_its_latest_transition() {
    let scenario = Scenario::new().await;
    let jobs = scenario.open_with_code_jobs(&["7", "8"]).await;
    let ledger = scenario.ledger();
    let dispatch = Uuid::new_v4();

    scenario
        .record(&jobs[0], dispatch, 1, JobState::Running, at(0))
        .await;
    let failure = JobTransition {
        job: jobs[0].clone(),
        dispatch_id: dispatch,
        attempt: 1,
        state: JobState::Failed,
        reason: Some("boom".into()),
        recorded_at: at(1),
    };
    ledger.record(&failure).await.unwrap();

    let mut filter = JobFilter::for_namespace(ROOT_NAMESPACE);
    filter.campaign = Some(scenario.campaign.clone());
    let snapshots = ledger.jobs(&filter).await.unwrap();
    assert_eq!(snapshots.len(), 2);

    let failed = snapshots.iter().find(|s| s.job.key == "7").unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.reason.as_deref(), Some("boom"));
    assert_eq!(failed.dispatch_id, dispatch);
    assert_eq!(failed.attempt, 1);
    assert_eq!(failed.job, jobs[0]);

    filter.states = vec![JobState::Pending];
    let pending = ledger.jobs(&filter).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].job.key, "8");
}

#[tokio::test]
async fn persistent_schema_renders_the_ledger_tables_as_designed() {
    let sql = *PERSISTENT_SCHEMA_SQL;

    assert!(sql.contains("CREATE TABLE IF NOT EXISTS campaign ("));
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
            "CREATE TABLE campaign (kind String, _version UInt64) \
             ENGINE = ReplacingMergeTree(_version) ORDER BY kind",
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
             WHERE database = currentDatabase() AND table = 'campaign' ORDER BY position",
        )
        .await;
    let names = String::extract_column(&batches, 0).unwrap();
    let expected = [
        "kind",
        "subject",
        "generation",
        "job_kind",
        "required",
        "state",
        "recorded_at",
        "_version",
        "_deleted",
    ];
    for column in expected {
        assert!(
            names.iter().any(|name| name == column),
            "missing {column}: {names:?}"
        );
    }
}
