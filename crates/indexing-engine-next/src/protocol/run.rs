use std::time::SystemTime;

use super::lease::{Fence, Lease, LeaseEntry, LeaseRules, PodId, Revision};
use super::marker::TaskId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    pod: PodId,
    task: TaskId,
}

impl Claim {
    pub fn deliver(pod: PodId, task: TaskId) -> Self {
        Self { pod, task }
    }

    pub fn acquire_lease(
        &self,
        rules: &LeaseRules,
        entry: &LeaseEntry,
        local_now: SystemTime,
    ) -> Option<Lease> {
        rules
            .is_free(entry, local_now)
            .then(|| rules.lease_for(&self.pod, local_now))
    }

    pub fn hold(
        self,
        rules: &LeaseRules,
        lease: &Lease,
        written: Revision,
        local_now: SystemTime,
    ) -> Hold {
        Hold {
            pod: self.pod,
            task: self.task,
            fence: Fence(written.0),
            seen: written,
            expires_at: lease.expires_at,
            renew_at: local_now + rules.renew_every,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Renewal {
    Extend(Lease),
    Lost,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hold {
    pod: PodId,
    task: TaskId,
    fence: Fence,
    seen: Revision,
    expires_at: SystemTime,
    renew_at: SystemTime,
}

impl Hold {
    pub fn task(&self) -> TaskId {
        self.task
    }

    pub fn fence(&self) -> Fence {
        self.fence
    }

    pub fn seen_revision(&self) -> Revision {
        self.seen
    }

    pub fn expires_at(&self) -> SystemTime {
        self.expires_at
    }

    pub fn renew_at(&self) -> SystemTime {
        self.renew_at
    }

    pub fn start_clears_marker(&self, marker: Option<TaskId>) -> bool {
        marker == Some(self.task)
    }

    pub fn is_renew_due(&self, local_now: SystemTime) -> bool {
        local_now >= self.renew_at
    }

    pub fn renew(&self, rules: &LeaseRules, entry: &LeaseEntry, local_now: SystemTime) -> Renewal {
        if entry.is_held_by(&self.pod, self.seen) {
            Renewal::Extend(rules.lease_for(&self.pod, local_now))
        } else {
            Renewal::Lost
        }
    }

    pub fn renewed(
        &mut self,
        rules: &LeaseRules,
        lease: &Lease,
        written: Revision,
        local_now: SystemTime,
    ) {
        self.seen = written;
        self.expires_at = lease.expires_at;
        self.renew_at = local_now + rules.renew_every;
    }

    pub fn release(&self, entry: &LeaseEntry) -> Option<Revision> {
        entry
            .is_held_by(&self.pod, self.seen)
            .then_some(entry.revision)
    }
}
