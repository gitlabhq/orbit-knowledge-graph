mod log;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::lane::{Lane, LaneId};
use crate::lease::Hold;
use crate::payload::{Key, Tenant};
use crate::task::TaskId;

pub(crate) use log::LogObserver;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineEvent {
    Task(TaskEvent),
    Lane(LaneEvent),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskEvent {
    pub at: SystemTime,
    pub task: TaskIdentity,
    pub step: TaskStep,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskIdentity {
    pub id: TaskId,
    pub tenant: Tenant,
    pub worker: String,
    pub key: Key,
    pub lane: Lane,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskStep {
    Submitted { already_pending: bool },
    Published,
    Delivered,
    LeaseAcquired(Hold),
    GivenBack { delay: Duration },
    Started { cleared_pending: bool },
    Renewed(Hold),
    RenewalLost,
    RenewalFailed(Failure),
    Finished { released_lease: bool },
    Abandoned(Failure),
    PortCallFailed(Failure),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneEvent {
    pub at: SystemTime,
    pub lane: LaneId,
    pub step: LaneStep,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaneStep {
    PullFailed(Failure),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub kind: &'static str,
    pub message: String,
}

pub trait Observer: Send + Sync + 'static {
    fn observe(&self, event: &EngineEvent);
}

pub(crate) struct Observers(Vec<Arc<dyn Observer>>);

impl TaskStep {
    pub fn name(&self) -> &'static str {
        match self {
            TaskStep::Submitted { .. } => "submitted",
            TaskStep::Published => "published",
            TaskStep::Delivered => "delivered",
            TaskStep::LeaseAcquired(_) => "lease_acquired",
            TaskStep::GivenBack { .. } => "given_back",
            TaskStep::Started { .. } => "started",
            TaskStep::Renewed(_) => "renewed",
            TaskStep::RenewalLost => "renewal_lost",
            TaskStep::RenewalFailed(_) => "renewal_failed",
            TaskStep::Finished { .. } => "finished",
            TaskStep::Abandoned(_) => "abandoned",
            TaskStep::PortCallFailed(_) => "port_call_failed",
        }
    }

    pub fn failure(&self) -> Option<&Failure> {
        match self {
            TaskStep::RenewalFailed(failure)
            | TaskStep::Abandoned(failure)
            | TaskStep::PortCallFailed(failure) => Some(failure),
            TaskStep::Submitted { .. }
            | TaskStep::Published
            | TaskStep::Delivered
            | TaskStep::LeaseAcquired(_)
            | TaskStep::GivenBack { .. }
            | TaskStep::Started { .. }
            | TaskStep::Renewed(_)
            | TaskStep::RenewalLost
            | TaskStep::Finished { .. } => None,
        }
    }
}

impl Failure {
    pub(crate) fn new(kind: &'static str, error: impl std::fmt::Display) -> Self {
        Self {
            kind,
            message: format!("{error:#}"),
        }
    }
}

impl Observers {
    pub(crate) fn new(observers: Vec<Arc<dyn Observer>>) -> Self {
        Self(observers)
    }
}

impl<O: Observer + ?Sized> Observer for Arc<O> {
    fn observe(&self, event: &EngineEvent) {
        O::observe(self, event);
    }
}

impl Observer for Observers {
    fn observe(&self, event: &EngineEvent) {
        for observer in &self.0 {
            observer.observe(event);
        }
    }
}
