use std::collections::BTreeMap;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhaseSpec {
    pub kind: JobKind,
    pub required: bool,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PhaseState {
    Open = 1,
    DiscoveryClosed = 2,
    Abandoned = 3,
}

impl PhaseState {
    pub(crate) fn rank(self) -> u64 {
        self as u64
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            PhaseState::Open => "open",
            PhaseState::DiscoveryClosed => "discovery_closed",
            PhaseState::Abandoned => "abandoned",
        }
    }

    pub(crate) fn parse(name: &str) -> Result<Self, InvalidState> {
        [
            PhaseState::Open,
            PhaseState::DiscoveryClosed,
            PhaseState::Abandoned,
        ]
        .into_iter()
        .find(|state| state.as_str() == name)
        .ok_or_else(|| InvalidState(name.to_owned()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobTransition {
    pub job: JobRef,
    pub dispatch_id: Uuid,
    pub attempt: u32,
    pub state: JobState,
    pub reason: Option<String>,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhaseSummary {
    pub kind: JobKind,
    pub required: bool,
    pub discovery_closed: bool,
    pub abandoned: bool,
    pub counts_by_state: BTreeMap<JobState, u64>,
}

impl PhaseSummary {
    pub fn is_complete(&self) -> bool {
        self.discovery_closed
            && self
                .counts_by_state
                .iter()
                .all(|(state, count)| state.is_terminal() || *count == 0)
    }

    pub fn count(&self, state: JobState) -> u64 {
        self.counts_by_state.get(&state).copied().unwrap_or(0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CampaignSummary {
    pub id: CampaignId,
    pub phases: Vec<PhaseSummary>,
}

impl CampaignSummary {
    pub fn is_complete(&self) -> bool {
        self.phases.iter().all(PhaseSummary::is_complete)
    }

    pub fn is_ready(&self) -> bool {
        self.phases
            .iter()
            .filter(|phase| phase.required)
            .all(PhaseSummary::is_complete)
    }

    pub fn is_abandoned(&self) -> bool {
        self.phases.iter().any(|phase| phase.abandoned)
    }

    pub fn count(&self, state: JobState) -> u64 {
        self.phases.iter().map(|phase| phase.count(state)).sum()
    }

    pub fn total(&self) -> u64 {
        self.phases
            .iter()
            .flat_map(|phase| phase.counts_by_state.values())
            .sum()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobFilter {
    pub namespace_id: i64,
    pub campaign: Option<CampaignId>,
    pub kind: Option<JobKind>,
    pub states: Vec<JobState>,
    pub limit: usize,
}

impl JobFilter {
    pub fn for_namespace(namespace_id: i64) -> Self {
        Self {
            namespace_id,
            campaign: None,
            kind: None,
            states: Vec::new(),
            limit: 1_000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobSnapshot {
    pub job: JobRef,
    pub dispatch_id: Uuid,
    pub attempt: u32,
    pub state: JobState,
    pub reason: Option<String>,
    pub recorded_at: DateTime<Utc>,
}
