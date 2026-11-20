use super::marker::TaskId;
use super::run::Hold;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolEvent {
    Submitted { task: TaskId, found_marker: bool },
    Published(TaskId),
    Delivered(TaskId),
    LeaseAcquired(Hold),
    Started { task: TaskId, cleared_marker: bool },
    Renewed(Hold),
    RenewalLost(TaskId),
    RenewalFailed(TaskId),
    Finished { task: TaskId, released: bool },
    Abandoned(TaskId),
}
