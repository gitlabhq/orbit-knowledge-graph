use std::time::SystemTime;

use crate::lease::Hold;
use crate::task::TaskId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProtocolEvent {
    Submitted { task: TaskId, found_pending: bool },
    Published(TaskId),
    Delivered(TaskId),
    LeaseAcquired(Hold),
    GivenBack(TaskId),
    Started { task: TaskId, cleared_pending: bool },
    Renewed(Hold),
    RenewalLost(TaskId),
    RenewalFailed(TaskId),
    Finished { task: TaskId, released: bool },
    Abandoned(TaskId),
}

pub(crate) trait ProtocolLog: Send + Sync {
    fn record(&self, at: SystemTime, event: ProtocolEvent);
}
