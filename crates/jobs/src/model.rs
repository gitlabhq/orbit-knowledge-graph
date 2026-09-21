use chrono::{DateTime, Utc};
use orbit_utils::traversal_path::TraversalPath;
use uuid::Uuid;

use crate::kind::{CampaignKind, JobKind};

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CampaignId {
    pub kind: CampaignKind,
    pub subject: String,
    pub generation: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct JobRef {
    pub campaign: Option<CampaignId>,
    pub namespace_id: i64,
    pub traversal_path: TraversalPath,
    pub kind: JobKind,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown job state {0:?}")]
pub struct InvalidState(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum JobState {
    Pending = 0,
    Queued = 1,
    Running = 2,
    Retrying = 3,
    Deferred = 4,
    Failed = 5,
    Skipped = 6,
    Succeeded = 7,
}

impl JobState {
    pub const ALL: [JobState; 8] = [
        JobState::Pending,
        JobState::Queued,
        JobState::Running,
        JobState::Retrying,
        JobState::Deferred,
        JobState::Failed,
        JobState::Skipped,
        JobState::Succeeded,
    ];

    pub fn rank(self) -> u64 {
        self as u64
    }

    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Pending => "pending",
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Retrying => "retrying",
            JobState::Deferred => "deferred",
            JobState::Failed => "failed",
            JobState::Skipped => "skipped",
            JobState::Succeeded => "succeeded",
        }
    }

    pub fn parse(name: &str) -> Result<Self, InvalidState> {
        JobState::ALL
            .into_iter()
            .find(|state| state.as_str() == name)
            .ok_or_else(|| InvalidState(name.to_owned()))
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobState::Failed | JobState::Skipped | JobState::Succeeded
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobTransition {
    pub job: JobRef,
    pub dispatch_id: Uuid,
    pub attempt: u32,
    pub state: JobState,
    pub reason: Option<String>,
    pub rows_read: u64,
    pub rows_written: u64,
    pub started_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobRun {
    pub namespace_id: i64,
    pub traversal_path: TraversalPath,
    pub key: String,
    pub state: JobState,
    pub reason: Option<String>,
    pub rows_read: u64,
    pub rows_written: u64,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}
