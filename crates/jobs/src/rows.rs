use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use orbit_utils::arrow::{BatchBuilder, ColumnSpec, ColumnType};

use crate::model::{JobRef, JobTransition};

pub(crate) const JOB_TABLE: &str = "job";

pub(crate) fn build_job_batch(transitions: &[JobTransition]) -> Result<RecordBatch, ArrowError> {
    let columns = [
        column("campaign_kind", ColumnType::Str),
        column("campaign_subject", ColumnType::Str),
        column("campaign_generation", ColumnType::TimestampMicros),
        column("namespace_id", ColumnType::Int),
        column("traversal_path", ColumnType::Str),
        column("kind", ColumnType::Str),
        column("key", ColumnType::Str),
        column("dispatch_id", ColumnType::Str),
        column("attempt", ColumnType::Int),
        column("state", ColumnType::Str),
        column("reason", ColumnType::Str),
        column("rows_read", ColumnType::Int),
        column("rows_written", ColumnType::Int),
        column("started_at", ColumnType::TimestampMicros),
        column("recorded_at", ColumnType::TimestampMicros),
        column("_version", ColumnType::UInt),
    ];

    BatchBuilder::new(&columns, transitions.len())?.build(transitions, |transition, row| {
        let job = &transition.job;
        row.col("campaign_kind")?
            .push_str(campaign_kind_or_empty(job))?;
        row.col("campaign_subject")?
            .push_str(campaign_subject_or_empty(job))?;
        row.col("campaign_generation")?
            .push_timestamp_micros(campaign_generation_or_epoch(job).timestamp_micros())?;
        row.col("namespace_id")?.push_int(job.namespace_id)?;
        row.col("traversal_path")?
            .push_str(job.traversal_path.as_str())?;
        row.col("kind")?.push_str(job.kind.as_str())?;
        row.col("key")?.push_str(&job.key)?;
        row.col("dispatch_id")?
            .push_str(transition.dispatch_id.to_string())?;
        row.col("attempt")?
            .push_int(i64::from(transition.attempt))?;
        row.col("state")?.push_str(transition.state.as_str())?;
        row.col("reason")?
            .push_str(transition.reason.as_deref().unwrap_or(""))?;
        row.col("rows_read")?
            .push_int(count_as_int64(transition.rows_read))?;
        row.col("rows_written")?
            .push_int(count_as_int64(transition.rows_written))?;
        row.col("started_at")?
            .push_timestamp_micros(transition.started_at.timestamp_micros())?;
        row.col("recorded_at")?
            .push_timestamp_micros(transition.recorded_at.timestamp_micros())?;
        row.col("_version")?.push_uint(transition.state.rank())?;
        Ok(())
    })
}

fn column(name: &str, col_type: ColumnType) -> ColumnSpec {
    ColumnSpec {
        name: name.to_owned(),
        col_type,
        nullable: false,
    }
}

fn count_as_int64(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

fn campaign_kind_or_empty(job: &JobRef) -> &str {
    job.campaign
        .as_ref()
        .map_or("", |campaign| campaign.kind.as_str())
}

fn campaign_subject_or_empty(job: &JobRef) -> &str {
    job.campaign
        .as_ref()
        .map_or("", |campaign| campaign.subject.as_str())
}

fn campaign_generation_or_epoch(job: &JobRef) -> DateTime<Utc> {
    job.campaign
        .as_ref()
        .map_or(DateTime::<Utc>::UNIX_EPOCH, |campaign| campaign.generation)
}
