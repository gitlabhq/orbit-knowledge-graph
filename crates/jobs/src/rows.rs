use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::model::{CampaignId, JobRef, JobState, PhaseSpec, PhaseState};

pub(crate) const CAMPAIGN_TABLE: &str = "campaign";
pub(crate) const JOB_TABLE: &str = "job";

pub(crate) struct CampaignRow<'a> {
    pub campaign: &'a CampaignId,
    pub phase: &'a PhaseSpec,
    pub state: PhaseState,
    pub recorded_at: DateTime<Utc>,
}

pub(crate) struct JobRow<'a> {
    pub job: &'a JobRef,
    pub dispatch_id: Uuid,
    pub attempt: i64,
    pub state: JobState,
    pub reason: &'a str,
    pub recorded_at: DateTime<Utc>,
}

pub(crate) fn campaign_batch(rows: &[CampaignRow<'_>]) -> Result<RecordBatch, ArrowError> {
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("kind", strings(rows, |r| r.campaign.kind.as_str())),
        ("subject", strings(rows, |r| r.campaign.subject.as_str())),
        ("generation", timestamps(rows, |r| r.campaign.generation)),
        ("job_kind", strings(rows, |r| r.phase.kind.as_str())),
        (
            "required",
            Arc::new(BooleanArray::from_iter(
                rows.iter().map(|r| Some(r.phase.required)),
            )),
        ),
        ("state", strings(rows, |r| r.state.as_str())),
        ("recorded_at", timestamps(rows, |r| r.recorded_at)),
        (
            "_version",
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|r| r.state.rank()),
            )),
        ),
    ];
    batch(columns)
}

pub(crate) fn job_batch(rows: &[JobRow<'_>]) -> Result<RecordBatch, ArrowError> {
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("campaign_kind", strings(rows, |r| campaign_kind(r.job))),
        (
            "campaign_subject",
            strings(rows, |r| campaign_subject(r.job)),
        ),
        (
            "campaign_generation",
            timestamps(rows, |r| campaign_generation(r.job)),
        ),
        (
            "namespace_id",
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.job.namespace_id),
            )),
        ),
        (
            "traversal_path",
            strings(rows, |r| r.job.traversal_path.as_str()),
        ),
        ("kind", strings(rows, |r| r.job.kind.as_str())),
        ("key", strings(rows, |r| r.job.key.as_str())),
        (
            "dispatch_id",
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.dispatch_id.to_string()),
            )),
        ),
        (
            "attempt",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.attempt))),
        ),
        ("state", strings(rows, |r| r.state.as_str())),
        ("reason", strings(rows, |r| r.reason)),
        ("recorded_at", timestamps(rows, |r| r.recorded_at)),
        (
            "_version",
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|r| r.state.rank()),
            )),
        ),
    ];
    batch(columns)
}

fn campaign_kind(job: &JobRef) -> &str {
    job.campaign
        .as_ref()
        .map_or("", |campaign| campaign.kind.as_str())
}

fn campaign_subject(job: &JobRef) -> &str {
    job.campaign
        .as_ref()
        .map_or("", |campaign| campaign.subject.as_str())
}

fn campaign_generation(job: &JobRef) -> DateTime<Utc> {
    job.campaign
        .as_ref()
        .map_or(DateTime::<Utc>::UNIX_EPOCH, |campaign| campaign.generation)
}

fn strings<'a, R>(rows: &'a [R], value: impl Fn(&'a R) -> &'a str) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(rows.iter().map(value)))
}

fn timestamps<R>(rows: &[R], value: impl Fn(&R) -> DateTime<Utc>) -> ArrayRef {
    let micros = rows.iter().map(|row| value(row).timestamp_micros());
    Arc::new(TimestampMicrosecondArray::from_iter_values(micros).with_timezone("UTC"))
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> Result<RecordBatch, ArrowError> {
    let fields = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), false))
        .collect::<Vec<_>>();
    let arrays = columns.into_iter().map(|(_, array)| array).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
}
