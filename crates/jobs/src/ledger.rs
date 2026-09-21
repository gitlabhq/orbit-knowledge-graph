use std::sync::Arc;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use clickhouse_client::ArrowClickHouseClient;
use orbit_utils::arrow::ArrowUtils;
use orbit_utils::traversal_path::TraversalPath;

use crate::kind::JobKind;
use crate::model::{InvalidState, JobRun, JobState, JobTransition};
use crate::rows::{JOB_TABLE, build_job_batch};

const ASYNC_INSERT_OVERRIDES: &[(&str, &str)] =
    &[("async_insert", "1"), ("wait_for_async_insert", "0")];

#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error(transparent)]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("column {0} is missing or has an unexpected type")]
    Column(&'static str),
    #[error(transparent)]
    InvalidState(#[from] InvalidState),
}

#[derive(Clone)]
pub struct JobLedger {
    client: Arc<ArrowClickHouseClient>,
}

impl JobLedger {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    pub async fn record(&self, transition: &JobTransition) -> Result<(), LedgerError> {
        let batch = build_job_batch(std::slice::from_ref(transition))?;
        let sql = self
            .client
            .build_insert_sql_with_overrides(JOB_TABLE, ASYNC_INSERT_OVERRIDES);
        self.client
            .insert_arrow_streaming_with_sql(JOB_TABLE, &sql, vec![batch])
            .await?;
        Ok(())
    }

    pub async fn latest_runs(
        &self,
        traversal_path: &TraversalPath,
        kind: &JobKind,
    ) -> Result<Vec<JobRun>, LedgerError> {
        let root_namespace_id = traversal_path.top_level_namespace_id();
        let sql = latest_runs_sql(root_namespace_id.is_some());

        let mut query = self
            .client
            .query(&sql)
            .param("path", traversal_path.as_str())
            .param("kind", kind.as_str());
        if let Some(namespace_id) = root_namespace_id {
            query = query.param("namespace_id", namespace_id);
        }
        let batches = query.fetch_arrow().await?;

        each_row(&batches)
            .map(|(batch, row)| read_job_run(batch, row))
            .collect()
    }
}

fn latest_runs_sql(scoped_to_root_namespace: bool) -> String {
    let mut scope =
        String::from("startsWith(traversal_path, {path:String}) AND kind = {kind:String}");
    if scoped_to_root_namespace {
        scope.push_str(" AND namespace_id = {namespace_id:Int64}");
    }
    format!(
        "WITH per_dispatch AS ( \
           SELECT * FROM job WHERE {scope} \
           ORDER BY attempt DESC, _version DESC \
           LIMIT 1 BY namespace_id, traversal_path, key, dispatch_id \
         ), current_jobs AS ( \
           SELECT * FROM per_dispatch \
           ORDER BY recorded_at DESC, _version DESC \
           LIMIT 1 BY namespace_id, traversal_path, key \
         ), history AS ( \
           SELECT namespace_id, traversal_path, key, \
                  maxIf(recorded_at, state IN ('succeeded', 'failed', 'skipped')) AS run_completed_at \
           FROM job WHERE {scope} \
           GROUP BY namespace_id, traversal_path, key \
         ) \
         SELECT current_jobs.namespace_id AS job_namespace_id, \
                current_jobs.traversal_path AS job_path, \
                current_jobs.key AS job_key, \
                current_jobs.state AS current_state, \
                current_jobs.reason AS current_reason, \
                current_jobs.rows_read AS read_rows, \
                current_jobs.rows_written AS written_rows, \
                current_jobs.started_at AS run_started_at, \
                history.run_completed_at AS run_completed_at \
         FROM current_jobs INNER JOIN history USING (namespace_id, traversal_path, key) \
         ORDER BY job_path, job_key"
    )
}

fn read_job_run(batch: &RecordBatch, row: usize) -> Result<JobRun, LedgerError> {
    let reason = read_string(batch, "current_reason", row)?;
    Ok(JobRun {
        namespace_id: read_int64(batch, "job_namespace_id", row)?,
        traversal_path: read_string(batch, "job_path", row)?.into(),
        key: read_string(batch, "job_key", row)?,
        state: JobState::parse(&read_string(batch, "current_state", row)?)?,
        reason: Some(reason).filter(|reason| !reason.is_empty()),
        rows_read: read_int64(batch, "read_rows", row)?.unsigned_abs(),
        rows_written: read_int64(batch, "written_rows", row)?.unsigned_abs(),
        started_at: read_timestamp(batch, "run_started_at", row)?,
        completed_at: read_optional_timestamp(batch, "run_completed_at", row)?,
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

fn read_timestamp(
    batch: &RecordBatch,
    name: &'static str,
    row: usize,
) -> Result<DateTime<Utc>, LedgerError> {
    ArrowUtils::get_column_timestamp(batch, name, row).ok_or(LedgerError::Column(name))
}

fn read_optional_timestamp(
    batch: &RecordBatch,
    name: &'static str,
    row: usize,
) -> Result<Option<DateTime<Utc>>, LedgerError> {
    let at = read_timestamp(batch, name, row)?;
    Ok(Some(at).filter(|at| *at != DateTime::<Utc>::UNIX_EPOCH))
}
