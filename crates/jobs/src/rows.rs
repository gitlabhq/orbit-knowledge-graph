use std::iter::repeat_n;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};

use crate::model::{CampaignId, JobRef, JobTransition, PhaseSpec, PhaseState};

pub(crate) const CAMPAIGN_TABLE: &str = "campaign";
pub(crate) const JOB_TABLE: &str = "job";

pub(crate) fn campaign_batch(
    campaign: &CampaignId,
    phases: &[PhaseSpec],
    state: PhaseState,
    recorded_at: DateTime<Utc>,
) -> Result<RecordBatch, ArrowError> {
    let count = phases.len();

    let kind = strings(repeat_n(campaign.kind.as_str(), count));
    let subject = strings(repeat_n(campaign.subject.as_str(), count));
    let generation = timestamps(repeat_n(campaign.generation, count));
    let job_kind = strings(phases.iter().map(|phase| phase.kind.as_str()));
    let required = booleans(phases.iter().map(|phase| phase.required));
    let phase_state = strings(repeat_n(state.as_str(), count));
    let recorded = timestamps(repeat_n(recorded_at, count));
    let version = unsigned(repeat_n(state.rank(), count));

    batch(vec![
        ("kind", kind),
        ("subject", subject),
        ("generation", generation),
        ("job_kind", job_kind),
        ("required", required),
        ("state", phase_state),
        ("recorded_at", recorded),
        ("_version", version),
    ])
}

pub(crate) fn job_batch(transitions: &[JobTransition]) -> Result<RecordBatch, ArrowError> {
    let jobs = || transitions.iter().map(|transition| &transition.job);
    let each = || transitions.iter();

    let campaign_kind = strings(jobs().map(campaign_kind));
    let campaign_subject = strings(jobs().map(campaign_subject));
    let campaign_generation = timestamps(jobs().map(campaign_generation));
    let namespace_id = integers(jobs().map(|job| job.namespace_id));
    let traversal_path = strings(jobs().map(|job| job.traversal_path.as_str()));
    let kind = strings(jobs().map(|job| job.kind.as_str()));
    let key = strings(jobs().map(|job| job.key.as_str()));
    let dispatch_id = strings(each().map(|t| t.dispatch_id.to_string()));
    let attempt = integers(each().map(|t| i64::from(t.attempt)));
    let state = strings(each().map(|t| t.state.as_str()));
    let reason = strings(each().map(|t| t.reason.as_deref().unwrap_or("")));
    let recorded_at = timestamps(each().map(|t| t.recorded_at));
    let version = unsigned(each().map(|t| t.state.rank()));

    batch(vec![
        ("campaign_kind", campaign_kind),
        ("campaign_subject", campaign_subject),
        ("campaign_generation", campaign_generation),
        ("namespace_id", namespace_id),
        ("traversal_path", traversal_path),
        ("kind", kind),
        ("key", key),
        ("dispatch_id", dispatch_id),
        ("attempt", attempt),
        ("state", state),
        ("reason", reason),
        ("recorded_at", recorded_at),
        ("_version", version),
    ])
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

fn strings<S: AsRef<str>>(values: impl Iterator<Item = S>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn integers(values: impl Iterator<Item = i64>) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(values))
}

fn unsigned(values: impl Iterator<Item = u64>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter_values(values))
}

fn booleans(values: impl Iterator<Item = bool>) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(values.map(Some)))
}

fn timestamps(values: impl Iterator<Item = DateTime<Utc>>) -> ArrayRef {
    let micros = values.map(|value| value.timestamp_micros());
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
