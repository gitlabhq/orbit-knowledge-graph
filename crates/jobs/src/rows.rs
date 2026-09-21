use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use orbit_utils::arrow::{BatchBuilder, ColumnSpec, ColumnType};

use crate::model::{CampaignId, JobRef, JobTransition, PhaseSpec, PhaseState};

pub(crate) const CAMPAIGN_TABLE: &str = "campaign";
pub(crate) const JOB_TABLE: &str = "job";

pub(crate) fn build_campaign_batch(
    campaign: &CampaignId,
    phases: &[PhaseSpec],
    state: PhaseState,
    recorded_at: DateTime<Utc>,
) -> Result<RecordBatch, ArrowError> {
    let columns = [
        column("kind", ColumnType::Str),
        column("subject", ColumnType::Str),
        column("generation", ColumnType::TimestampMicros),
        column("job_kind", ColumnType::Str),
        column("required", ColumnType::Bool),
        column("state", ColumnType::Str),
        column("recorded_at", ColumnType::TimestampMicros),
        column("_version", ColumnType::UInt),
    ];

    BatchBuilder::new(&columns, phases.len())?.build(phases, |phase, row| {
        row.col("kind")?.push_str(campaign.kind.as_str())?;
        row.col("subject")?.push_str(&campaign.subject)?;
        row.col("generation")?
            .push_timestamp_micros(campaign.generation.timestamp_micros())?;
        row.col("job_kind")?.push_str(phase.kind.as_str())?;
        row.col("required")?.push_bool(phase.required)?;
        row.col("state")?.push_str(state.as_str())?;
        row.col("recorded_at")?
            .push_timestamp_micros(recorded_at.timestamp_micros())?;
        row.col("_version")?.push_uint(state.rank())?;
        Ok(())
    })
}

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
