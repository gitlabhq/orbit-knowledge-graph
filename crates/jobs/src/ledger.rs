use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use clickhouse_client::{ArrowClickHouseClient, ArrowQuery};
use orbit_utils::arrow::ArrowUtils;
use orbit_utils::traversal_path::TraversalPath;
use uuid::Uuid;

use crate::kind::{CampaignKind, InvalidKind, JobKind};
use crate::model::{
    CampaignId, CampaignSummary, InvalidState, JobFilter, JobRef, JobSnapshot, JobState,
    JobTransition, PhaseSpec, PhaseState, PhaseSummary,
};
use crate::rows::{CAMPAIGN_TABLE, JOB_TABLE, build_campaign_batch, build_job_batch};

const INSERT_CHUNK_ROWS: usize = 65_536;
const ASYNC_INSERT_OVERRIDES: &[(&str, &str)] =
    &[("async_insert", "1"), ("wait_for_async_insert", "1")];

const CAMPAIGN_MATCH_SQL: &str = "campaign_kind = {campaign_kind:String} \
     AND campaign_subject = {campaign_subject:String} \
     AND campaign_generation = fromUnixTimestamp64Micro({campaign_generation:Int64}, 'UTC')";

const LATEST_PHASES_SQL: &str = "\
SELECT job_kind, \
       any(generation) AS latest_generation, \
       argMax(required, _version) AS required, \
       argMax(state, _version) AS state \
FROM campaign \
WHERE kind = {kind:String} AND subject = {subject:String} \
  AND generation = ( \
    SELECT max(generation) FROM campaign WHERE kind = {kind:String} AND subject = {subject:String} \
  ) \
GROUP BY job_kind \
ORDER BY job_kind";

const LATEST_SUCCESS_SQL: &str = "\
SELECT max(recorded_at) AS recorded_at FROM job \
WHERE namespace_id = {namespace_id:Int64} \
  AND startsWith(traversal_path, {path:String}) \
  AND state = 'succeeded'";

const COUNT_BY_KIND_AND_STATE_SQL: &str =
    "SELECT kind, state, toInt64(count()) AS count FROM current_jobs GROUP BY kind, state";

const PENDING_JOBS_SQL: &str = "\
SELECT namespace_id, traversal_path, key FROM current_jobs \
WHERE state = 'pending' ORDER BY key LIMIT {limit:UInt64}";

const SNAPSHOT_COLUMNS_SQL: &str = "\
campaign_kind, campaign_subject, campaign_generation, namespace_id, traversal_path, \
kind, key, dispatch_id, attempt, state, reason, recorded_at";

#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error(transparent)]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("column {0} is missing or has an unexpected type")]
    Column(&'static str),
    #[error(transparent)]
    InvalidKind(#[from] InvalidKind),
    #[error(transparent)]
    InvalidState(#[from] InvalidState),
    #[error(transparent)]
    InvalidDispatchId(#[from] uuid::Error),
}

#[derive(Clone)]
pub struct JobLedger {
    client: Arc<ArrowClickHouseClient>,
}

impl JobLedger {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    pub async fn open_campaign(
        &self,
        campaign: &CampaignId,
        phases: &[PhaseSpec],
    ) -> Result<(), LedgerError> {
        self.write_phase_state(campaign, phases, PhaseState::Open)
            .await
    }

    pub async fn close_discovery(
        &self,
        campaign: &CampaignId,
        phase: &PhaseSpec,
    ) -> Result<(), LedgerError> {
        let phases = std::slice::from_ref(phase);
        self.write_phase_state(campaign, phases, PhaseState::DiscoveryClosed)
            .await
    }

    pub async fn abandon_campaign(
        &self,
        campaign: &CampaignId,
        phases: &[PhaseSpec],
    ) -> Result<(), LedgerError> {
        self.write_phase_state(campaign, phases, PhaseState::Abandoned)
            .await
    }

    pub async fn register(
        &self,
        jobs: &[JobRef],
        initial: JobState,
        reason: Option<&str>,
    ) -> Result<(), LedgerError> {
        let now = Utc::now();
        for chunk in jobs.chunks(INSERT_CHUNK_ROWS) {
            let transitions: Vec<JobTransition> = chunk
                .iter()
                .map(|job| registration_transition(job, initial, reason, now))
                .collect();
            self.record_many(&transitions).await?;
        }
        Ok(())
    }

    pub async fn record(&self, transition: &JobTransition) -> Result<(), LedgerError> {
        self.record_many(std::slice::from_ref(transition)).await
    }

    pub async fn record_many(&self, transitions: &[JobTransition]) -> Result<(), LedgerError> {
        for chunk in transitions.chunks(INSERT_CHUNK_ROWS) {
            self.insert_batch(JOB_TABLE, build_job_batch(chunk)?)
                .await?;
        }
        Ok(())
    }

    pub async fn pending_jobs(
        &self,
        campaign: &CampaignId,
        namespace_id: i64,
        kind: &JobKind,
        limit: usize,
    ) -> Result<Vec<JobRef>, LedgerError> {
        let predicates = format!(
            "{CAMPAIGN_MATCH_SQL} AND namespace_id = {{namespace_id:Int64}} AND kind = {{job_kind:String}}"
        );
        let sql = select_from_current_jobs(&predicates, PENDING_JOBS_SQL);
        let query = bind_campaign_params(self.client.query(&sql), campaign)
            .param("namespace_id", namespace_id)
            .param("job_kind", kind.as_str())
            .param("limit", limit as u64);
        let batches = query.fetch_arrow().await?;

        each_row(&batches)
            .map(|(batch, row)| {
                Ok(JobRef {
                    campaign: Some(campaign.clone()),
                    namespace_id: read_int64(batch, "namespace_id", row)?,
                    traversal_path: read_string(batch, "traversal_path", row)?.into(),
                    kind: kind.clone(),
                    key: read_string(batch, "key", row)?,
                })
            })
            .collect()
    }

    pub async fn latest_campaign(
        &self,
        kind: &CampaignKind,
        subject: &str,
    ) -> Result<Option<CampaignSummary>, LedgerError> {
        let query = self
            .client
            .query(LATEST_PHASES_SQL)
            .param("kind", kind.as_str())
            .param("subject", subject);
        let batches = query.fetch_arrow().await?;

        let Some((first, row)) = each_row(&batches).next() else {
            return Ok(None);
        };
        let id = CampaignId {
            kind: kind.clone(),
            subject: subject.to_owned(),
            generation: read_timestamp(first, "latest_generation", row)?,
        };

        let mut counts = self.count_jobs_by_kind_and_state(&id).await?;
        let phases = each_row(&batches)
            .map(|(batch, row)| read_phase_summary(batch, row, &mut counts))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Some(CampaignSummary { id, phases }))
    }

    pub async fn latest_success_at(
        &self,
        namespace_id: i64,
        traversal_path: &TraversalPath,
    ) -> Result<Option<DateTime<Utc>>, LedgerError> {
        let query = self
            .client
            .query(LATEST_SUCCESS_SQL)
            .param("namespace_id", namespace_id)
            .param("path", traversal_path.as_str());
        let batches = query.fetch_arrow().await?;

        let Some((batch, row)) = each_row(&batches).next() else {
            return Ok(None);
        };
        let latest = read_timestamp(batch, "recorded_at", row)?;
        Ok(Some(latest).filter(|at| *at != DateTime::<Utc>::UNIX_EPOCH))
    }

    pub async fn jobs(&self, filter: &JobFilter) -> Result<Vec<JobSnapshot>, LedgerError> {
        let sql = build_snapshot_sql(filter);
        let query = bind_filter_params(self.client.query(&sql), filter);
        let batches = query.fetch_arrow().await?;

        each_row(&batches)
            .map(|(batch, row)| read_snapshot(batch, row))
            .collect()
    }

    async fn write_phase_state(
        &self,
        campaign: &CampaignId,
        phases: &[PhaseSpec],
        state: PhaseState,
    ) -> Result<(), LedgerError> {
        let batch = build_campaign_batch(campaign, phases, state, Utc::now())?;
        self.insert_batch(CAMPAIGN_TABLE, batch).await
    }

    async fn insert_batch(&self, table: &str, batch: RecordBatch) -> Result<(), LedgerError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let sql = self
            .client
            .build_insert_sql_with_overrides(table, ASYNC_INSERT_OVERRIDES);
        self.client
            .insert_arrow_streaming_with_sql(table, &sql, vec![batch])
            .await?;
        Ok(())
    }

    async fn count_jobs_by_kind_and_state(
        &self,
        campaign: &CampaignId,
    ) -> Result<BTreeMap<String, BTreeMap<JobState, u64>>, LedgerError> {
        let sql = select_from_current_jobs(CAMPAIGN_MATCH_SQL, COUNT_BY_KIND_AND_STATE_SQL);
        let query = bind_campaign_params(self.client.query(&sql), campaign);
        let batches = query.fetch_arrow().await?;

        let mut by_kind: BTreeMap<String, BTreeMap<JobState, u64>> = BTreeMap::new();
        for (batch, row) in each_row(&batches) {
            let kind = read_string(batch, "kind", row)?;
            let state = JobState::parse(&read_string(batch, "state", row)?)?;
            let count = read_int64(batch, "count", row)?.unsigned_abs();
            by_kind.entry(kind).or_default().insert(state, count);
        }
        Ok(by_kind)
    }
}

fn registration_transition(
    job: &JobRef,
    initial: JobState,
    reason: Option<&str>,
    now: DateTime<Utc>,
) -> JobTransition {
    let recorded_at = job
        .campaign
        .as_ref()
        .map_or(now, |campaign| campaign.generation);
    JobTransition {
        job: job.clone(),
        dispatch_id: Uuid::nil(),
        attempt: 0,
        state: initial,
        reason: reason.map(str::to_owned),
        recorded_at,
    }
}

fn select_from_current_jobs(predicates: &str, select: &str) -> String {
    format!(
        "WITH per_dispatch AS ( \
           SELECT * FROM job WHERE {predicates} \
           ORDER BY attempt DESC, _version DESC LIMIT 1 BY kind, key, dispatch_id \
         ), current_jobs AS ( \
           SELECT * FROM per_dispatch \
           ORDER BY recorded_at DESC, _version DESC LIMIT 1 BY kind, key \
         ) {select}"
    )
}

fn build_snapshot_sql(filter: &JobFilter) -> String {
    let mut predicates = String::from("namespace_id = {namespace_id:Int64}");
    if filter.campaign.is_some() {
        predicates.push_str(" AND ");
        predicates.push_str(CAMPAIGN_MATCH_SQL);
    }
    if filter.kind.is_some() {
        predicates.push_str(" AND kind = {job_kind:String}");
    }

    let state_filter = if filter.states.is_empty() {
        ""
    } else {
        "WHERE state IN {states:Array(String)}"
    };
    let select = format!(
        "SELECT {SNAPSHOT_COLUMNS_SQL} FROM current_jobs {state_filter} \
         ORDER BY recorded_at DESC, kind, key LIMIT {{limit:UInt64}}"
    );
    select_from_current_jobs(&predicates, &select)
}

fn bind_filter_params(query: ArrowQuery, filter: &JobFilter) -> ArrowQuery {
    let mut query = query
        .param("namespace_id", filter.namespace_id)
        .param("limit", filter.limit as u64);
    if let Some(campaign) = &filter.campaign {
        query = bind_campaign_params(query, campaign);
    }
    if let Some(kind) = &filter.kind {
        query = query.param("job_kind", kind.as_str());
    }
    if !filter.states.is_empty() {
        let states: Vec<&str> = filter.states.iter().map(|state| state.as_str()).collect();
        query = query.param("states", states);
    }
    query
}

fn bind_campaign_params(query: ArrowQuery, campaign: &CampaignId) -> ArrowQuery {
    query
        .param("campaign_kind", campaign.kind.as_str())
        .param("campaign_subject", campaign.subject.as_str())
        .param(
            "campaign_generation",
            campaign.generation.timestamp_micros(),
        )
}

fn read_phase_summary(
    batch: &RecordBatch,
    row: usize,
    counts: &mut BTreeMap<String, BTreeMap<JobState, u64>>,
) -> Result<PhaseSummary, LedgerError> {
    let kind = read_string(batch, "job_kind", row)?;
    let state = PhaseState::parse(&read_string(batch, "state", row)?)?;
    Ok(PhaseSummary {
        counts_by_state: counts.remove(&kind).unwrap_or_default(),
        kind: JobKind::parse(&kind)?,
        required: read_bool(batch, "required", row)?,
        discovery_closed: state == PhaseState::DiscoveryClosed,
        abandoned: state == PhaseState::Abandoned,
    })
}

fn read_snapshot(batch: &RecordBatch, row: usize) -> Result<JobSnapshot, LedgerError> {
    let campaign_kind = read_string(batch, "campaign_kind", row)?;
    let campaign = if campaign_kind.is_empty() {
        None
    } else {
        Some(CampaignId {
            kind: CampaignKind::parse(&campaign_kind)?,
            subject: read_string(batch, "campaign_subject", row)?,
            generation: read_timestamp(batch, "campaign_generation", row)?,
        })
    };

    let job = JobRef {
        campaign,
        namespace_id: read_int64(batch, "namespace_id", row)?,
        traversal_path: read_string(batch, "traversal_path", row)?.into(),
        kind: JobKind::parse(&read_string(batch, "kind", row)?)?,
        key: read_string(batch, "key", row)?,
    };

    let reason = read_string(batch, "reason", row)?;
    Ok(JobSnapshot {
        job,
        dispatch_id: read_string(batch, "dispatch_id", row)?.parse()?,
        attempt: u32::try_from(read_int64(batch, "attempt", row)?).unwrap_or(u32::MAX),
        state: JobState::parse(&read_string(batch, "state", row)?)?,
        reason: Some(reason).filter(|reason| !reason.is_empty()),
        recorded_at: read_timestamp(batch, "recorded_at", row)?,
    })
}

fn each_row(batches: &[RecordBatch]) -> impl Iterator<Item = (&RecordBatch, usize)> {
    batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(move |row| (batch, row)))
}

fn read_string(batch: &RecordBatch, name: &'static str, row: usize) -> Result<String, LedgerError> {
    ArrowUtils::get_column_string(batch, name, row).ok_or(LedgerError::Column(name))
}

fn read_int64(batch: &RecordBatch, name: &'static str, row: usize) -> Result<i64, LedgerError> {
    ArrowUtils::get_column::<Int64Type>(batch, name, row).ok_or(LedgerError::Column(name))
}

fn read_bool(batch: &RecordBatch, name: &'static str, row: usize) -> Result<bool, LedgerError> {
    ArrowUtils::get_column_bool(batch, name, row).ok_or(LedgerError::Column(name))
}

fn read_timestamp(
    batch: &RecordBatch,
    name: &'static str,
    row: usize,
) -> Result<DateTime<Utc>, LedgerError> {
    ArrowUtils::get_column_timestamp(batch, name, row).ok_or(LedgerError::Column(name))
}
