use std::time::{Duration, SystemTime};

use super::{Fence, HolderId, Lease, LeaseEntry, Revision};
use crate::task::TaskId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseRules {
    pub ttl: Duration,
    pub renew_every: Duration,
    pub skew_margin: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    holder: HolderId,
    task: TaskId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hold {
    holder: HolderId,
    task: TaskId,
    fence: Fence,
    seen: Revision,
    expires_at: SystemTime,
    renew_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Renewal {
    Extend(Lease),
    Lost,
}

impl Claim {
    pub fn new(holder: HolderId, task: TaskId) -> Self {
        Self { holder, task }
    }

    pub fn lease_if_free(
        &self,
        rules: &LeaseRules,
        entry: &LeaseEntry,
        local_now: SystemTime,
    ) -> Option<Lease> {
        let free = match &entry.lease {
            None => true,
            Some(lease) => local_now >= lease.expires_at + rules.skew_margin,
        };
        let recovered = entry.generation.is_some();
        (recovered && free).then(|| rules.lease_for(&self.holder, local_now))
    }

    pub fn into_hold(
        self,
        rules: &LeaseRules,
        lease: &Lease,
        written: Fence,
        local_now: SystemTime,
    ) -> Hold {
        Hold {
            holder: self.holder,
            task: self.task,
            fence: written,
            seen: written.revision,
            expires_at: lease.expires_at,
            renew_at: local_now + rules.renew_every,
        }
    }
}

impl Hold {
    pub fn is_renew_due(&self, local_now: SystemTime) -> bool {
        local_now >= self.renew_at
    }

    pub fn build_renewal(
        &self,
        rules: &LeaseRules,
        entry: &LeaseEntry,
        local_now: SystemTime,
    ) -> Renewal {
        if entry.is_held_by(&self.holder, self.fence.generation, self.seen) {
            Renewal::Extend(rules.lease_for(&self.holder, local_now))
        } else {
            Renewal::Lost
        }
    }

    pub fn record_renewal(
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

    pub fn revision_to_release(&self, entry: &LeaseEntry) -> Option<Revision> {
        entry
            .is_held_by(&self.holder, self.fence.generation, self.seen)
            .then_some(entry.revision)
    }

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
}

impl LeaseRules {
    fn lease_for(&self, holder: &HolderId, local_now: SystemTime) -> Lease {
        Lease {
            holder: holder.clone(),
            expires_at: local_now + self.ttl,
        }
    }
}
