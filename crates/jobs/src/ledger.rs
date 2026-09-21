use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use clickhouse_client::{ArrowClickHouseClient, ArrowQuery, FromArrowColumn};
use orbit_utils::traversal_path::TraversalPath;
use uuid::Uuid;

use crate::kind::{CampaignKind, InvalidKind, JobKind};
use crate::model::{
    CampaignId, CampaignSummary, InvalidState, JobFilter, JobRef, JobSnapshot, JobState,
    JobTransition, PhaseSpec, PhaseState, PhaseSummary,
};
use crate::rows::{CAMPAIGN_TABLE, CampaignRow, JOB_TABLE, JobRow, campaign_batch, job_batch};

const INSERT_CHUNK_ROWS: usize = 65_536;
const ASYNC_INSERT_OVERRIDES: &[(&str, &str)] =
    &[("async_insert", "1"), ("wait_for_async_insert", "1")];

const CAMPAIGN_PREDICATE: &str = "campaign_kind = {campaign_kind:String} \
     AND campaign_subject = {campaign_subject:String} \
     AND campaign_generation = fromUnixTimestamp64Micro({campaign_generation:Int64}, 'UTC')";

const LATEST_PHASES_SQL: &str = "\
SELECT job_kind, any(generation), argMax(required, _version), argMax(state, _version) \
FROM campaign \
WHERE kind = {kind:String} AND subject = {subject:String} \
  AND generation = ( \
    SELECT max(generation) FROM campaign WHERE kind = {kind:String} AND subject = {subject:String} \
  ) \
GROUP BY job_kind \
ORDER BY job_kind";

const LATEST_SUCCESS_SQL: &str = "\
SELECT max(recorded_at) FROM job \
WHERE namespace_id = {namespace_id:Int64} \
  AND startsWith(traversal_path, {path:String}) \
  AND state = 'succeeded'";

#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error(transparent)]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("column {0} has an unexpected type")]
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
        self.write_phases(campaign, phases, PhaseState::Open).await
    }

    pub async fn close_discovery(
        &self,
        campaign: &CampaignId,
        phase: &PhaseSpec,
    ) -> Result<(), LedgerError> {
        self.write_phases(
            campaign,
            std::slice::from_ref(phase),
            PhaseState::DiscoveryClosed,
        )
        .await
    }

    pub async fn abandon_campaign(
        &self,
        campaign: &CampaignId,
        phases: &[PhaseSpec],
    ) -> Result<(), LedgerError> {
        self.write_phases(campaign, phases, PhaseState::Abandoned)
            .await
    }

    pub async fn register(
        &self,
        jobs: &[JobRef],
        initial: JobState,
        reason: Option<&str>,
    ) -> Result<(), LedgerError> {
        let now = Utc::now();
        let rows: Vec<JobRow<'_>> = jobs
            .iter()
            .map(|job| JobRow {
                job,
                dispatch_id: Uuid::nil(),
                attempt: 0,
                state: initial,
                reason: reason.unwrap_or(""),
                recorded_at: job
                    .campaign
                    .as_ref()
                    .map_or(now, |campaign| campaign.generation),
            })
            .collect();
        self.write_jobs(&rows).await
    }

    pub async fn record(&self, transition: &JobTransition) -> Result<(), LedgerError> {
        self.record_many(std::slice::from_ref(transition)).await
    }

    pub async fn record_many(&self, transitions: &[JobTransition]) -> Result<(), LedgerError> {
        let rows: Vec<JobRow<'_>> = transitions
            .iter()
            .map(|transition| JobRow {
                job: &transition.job,
                dispatch_id: transition.dispatch_id,
                attempt: i64::from(transition.attempt),
                state: transition.state,
                reason: transition.reason.as_deref().unwrap_or(""),
                recorded_at: transition.recorded_at,
            })
            .collect();
        self.write_jobs(&rows).await
    }

    pub async fn pending_jobs(
        &self,
        campaign: &CampaignId,
        namespace_id: i64,
        kind: &JobKind,
        limit: usize,
    ) -> Result<Vec<JobRef>, LedgerError> {
        let predicates = format!(
            "{CAMPAIGN_PREDICATE} AND namespace_id = {{namespace_id:Int64}} AND kind = {{job_kind:String}}"
        );
        let sql = format!(
            "{} SELECT namespace_id, traversal_path, key FROM current_jobs \
             WHERE state = 'pending' ORDER BY key LIMIT {{limit:UInt64}}",
            current_jobs_cte(&predicates)
        );
        let batches = bind_campaign(self.client.query(&sql), campaign)
            .param("namespace_id", namespace_id)
            .param("job_kind", kind.as_str())
            .param("limit", limit as u64)
            .fetch_arrow()
            .await?;

        let namespace_ids: Vec<i64> = column(&batches, 0, "namespace_id")?;
        let paths: Vec<String> = column(&batches, 1, "traversal_path")?;
        let keys: Vec<String> = column(&batches, 2, "key")?;
        Ok(namespace_ids
            .into_iter()
            .zip(paths)
            .zip(keys)
            .map(|((namespace_id, path), key)| JobRef {
                campaign: Some(campaign.clone()),
                namespace_id,
                traversal_path: TraversalPath::from(path),
                kind: kind.clone(),
                key,
            })
            .collect())
    }

    pub async fn latest_campaign(
        &self,
        kind: &CampaignKind,
        subject: &str,
    ) -> Result<Option<CampaignSummary>, LedgerError> {
        let batches = self
            .client
            .query(LATEST_PHASES_SQL)
            .param("kind", kind.as_str())
            .param("subject", subject)
            .fetch_arrow()
            .await?;

        let job_kinds: Vec<String> = column(&batches, 0, "job_kind")?;
        let generations: Vec<DateTime<Utc>> = column(&batches, 1, "generation")?;
        let required: Vec<bool> = column(&batches, 2, "required")?;
        let states: Vec<String> = column(&batches, 3, "state")?;

        let Some(generation) = generations.first().copied() else {
            return Ok(None);
        };
        let id = CampaignId {
            kind: kind.clone(),
            subject: subject.to_owned(),
            generation,
        };
        let mut counts = self.counts_by_kind_and_state(&id).await?;

        let mut phases = Vec::with_capacity(job_kinds.len());
        for ((job_kind, required), state) in job_kinds.into_iter().zip(required).zip(states) {
            let phase_state = PhaseState::parse(&state)?;
            phases.push(PhaseSummary {
                counts_by_state: counts.remove(&job_kind).unwrap_or_default(),
                kind: JobKind::parse(&job_kind)?,
                required,
                discovery_closed: phase_state == PhaseState::DiscoveryClosed,
                abandoned: phase_state == PhaseState::Abandoned,
            });
        }
        Ok(Some(CampaignSummary { id, phases }))
    }

    pub async fn latest_success_at(
        &self,
        namespace_id: i64,
        traversal_path: &TraversalPath,
    ) -> Result<Option<DateTime<Utc>>, LedgerError> {
        let batches = self
            .client
            .query(LATEST_SUCCESS_SQL)
            .param("namespace_id", namespace_id)
            .param("path", traversal_path.as_str())
            .fetch_arrow()
            .await?;
        let latest: Vec<DateTime<Utc>> = column(&batches, 0, "recorded_at")?;
        Ok(latest
            .into_iter()
            .next()
            .filter(|at| *at != DateTime::<Utc>::UNIX_EPOCH))
    }

    pub async fn jobs(&self, filter: &JobFilter) -> Result<Vec<JobSnapshot>, LedgerError> {
        let mut predicates = String::from("namespace_id = {namespace_id:Int64}");
        if filter.campaign.is_some() {
            predicates.push_str(" AND ");
            predicates.push_str(CAMPAIGN_PREDICATE);
        }
        if filter.kind.is_some() {
            predicates.push_str(" AND kind = {job_kind:String}");
        }
        let state_filter = if filter.states.is_empty() {
            ""
        } else {
            "WHERE state IN {states:Array(String)}"
        };
        let sql = format!(
            "{} SELECT campaign_kind, campaign_subject, campaign_generation, namespace_id, \
             traversal_path, kind, key, dispatch_id, attempt, state, reason, recorded_at \
             FROM current_jobs {state_filter} ORDER BY recorded_at DESC, kind, key \
             LIMIT {{limit:UInt64}}",
            current_jobs_cte(&predicates)
        );

        let mut query = self
            .client
            .query(&sql)
            .param("namespace_id", filter.namespace_id)
            .param("limit", filter.limit as u64);
        if let Some(campaign) = &filter.campaign {
            query = bind_campaign(query, campaign);
        }
        if let Some(kind) = &filter.kind {
            query = query.param("job_kind", kind.as_str());
        }
        if !filter.states.is_empty() {
            let states: Vec<&str> = filter.states.iter().map(|state| state.as_str()).collect();
            query = query.param("states", states);
        }
        let batches = query.fetch_arrow().await?;
        snapshots_from(&batches)
    }

    async fn write_phases(
        &self,
        campaign: &CampaignId,
        phases: &[PhaseSpec],
        state: PhaseState,
    ) -> Result<(), LedgerError> {
        let recorded_at = Utc::now();
        let rows: Vec<CampaignRow<'_>> = phases
            .iter()
            .map(|phase| CampaignRow {
                campaign,
                phase,
                state,
                recorded_at,
            })
            .collect();
        self.insert(CAMPAIGN_TABLE, campaign_batch(&rows)?).await
    }

    async fn write_jobs(&self, rows: &[JobRow<'_>]) -> Result<(), LedgerError> {
        for chunk in rows.chunks(INSERT_CHUNK_ROWS) {
            self.insert(JOB_TABLE, job_batch(chunk)?).await?;
        }
        Ok(())
    }

    async fn insert(&self, table: &str, batch: RecordBatch) -> Result<(), LedgerError> {
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

    async fn counts_by_kind_and_state(
        &self,
        campaign: &CampaignId,
    ) -> Result<BTreeMap<String, BTreeMap<JobState, u64>>, LedgerError> {
        let sql = format!(
            "{} SELECT kind, state, toInt64(count()) FROM current_jobs GROUP BY kind, state",
            current_jobs_cte(CAMPAIGN_PREDICATE)
        );
        let batches = bind_campaign(self.client.query(&sql), campaign)
            .fetch_arrow()
            .await?;
        let kinds: Vec<String> = column(&batches, 0, "kind")?;
        let states: Vec<String> = column(&batches, 1, "state")?;
        let counts: Vec<i64> = column(&batches, 2, "count")?;

        let mut by_kind: BTreeMap<String, BTreeMap<JobState, u64>> = BTreeMap::new();
        for ((kind, state), count) in kinds.into_iter().zip(states).zip(counts) {
            by_kind
                .entry(kind)
                .or_default()
                .insert(JobState::parse(&state)?, count.unsigned_abs());
        }
        Ok(by_kind)
    }
}

fn current_jobs_cte(predicates: &str) -> String {
    format!(
        "WITH per_dispatch AS ( \
           SELECT * FROM job WHERE {predicates} \
           ORDER BY attempt DESC, _version DESC LIMIT 1 BY kind, key, dispatch_id \
         ), current_jobs AS ( \
           SELECT * FROM per_dispatch \
           ORDER BY recorded_at DESC, _version DESC LIMIT 1 BY kind, key \
         )"
    )
}

fn bind_campaign(query: ArrowQuery, campaign: &CampaignId) -> ArrowQuery {
    query
        .param("campaign_kind", campaign.kind.as_str())
        .param("campaign_subject", campaign.subject.as_str())
        .param(
            "campaign_generation",
            campaign.generation.timestamp_micros(),
        )
}

fn column<T: FromArrowColumn>(
    batches: &[RecordBatch],
    index: usize,
    name: &'static str,
) -> Result<Vec<T>, LedgerError> {
    T::extract_column(batches, index).map_err(|_| LedgerError::Column(name))
}

fn snapshots_from(batches: &[RecordBatch]) -> Result<Vec<JobSnapshot>, LedgerError> {
    let campaign_kinds: Vec<String> = column(batches, 0, "campaign_kind")?;
    let campaign_subjects: Vec<String> = column(batches, 1, "campaign_subject")?;
    let campaign_generations: Vec<DateTime<Utc>> = column(batches, 2, "campaign_generation")?;
    let namespace_ids: Vec<i64> = column(batches, 3, "namespace_id")?;
    let paths: Vec<String> = column(batches, 4, "traversal_path")?;
    let kinds: Vec<String> = column(batches, 5, "kind")?;
    let keys: Vec<String> = column(batches, 6, "key")?;
    let dispatch_ids: Vec<String> = column(batches, 7, "dispatch_id")?;
    let attempts: Vec<i64> = column(batches, 8, "attempt")?;
    let states: Vec<String> = column(batches, 9, "state")?;
    let reasons: Vec<String> = column(batches, 10, "reason")?;
    let recorded_ats: Vec<DateTime<Utc>> = column(batches, 11, "recorded_at")?;

    let mut snapshots = Vec::with_capacity(keys.len());
    for row in 0..keys.len() {
        let campaign = if campaign_kinds[row].is_empty() {
            None
        } else {
            Some(CampaignId {
                kind: CampaignKind::parse(&campaign_kinds[row])?,
                subject: campaign_subjects[row].clone(),
                generation: campaign_generations[row],
            })
        };
        snapshots.push(JobSnapshot {
            job: JobRef {
                campaign,
                namespace_id: namespace_ids[row],
                traversal_path: TraversalPath::from(paths[row].as_str()),
                kind: JobKind::parse(&kinds[row])?,
                key: keys[row].clone(),
            },
            dispatch_id: Uuid::parse_str(&dispatch_ids[row])?,
            attempt: u32::try_from(attempts[row]).unwrap_or(u32::MAX),
            state: JobState::parse(&states[row])?,
            reason: Some(reasons[row].clone()).filter(|reason| !reason.is_empty()),
            recorded_at: recorded_ats[row],
        });
    }
    Ok(snapshots)
}
