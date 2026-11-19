use std::time::{Duration, SystemTime};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HolderId(String);

impl HolderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fence {
    pub generation: Generation,
    pub revision: Revision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub holder: HolderId,
    pub expires_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseEntry {
    pub lease: Option<Lease>,
    pub revision: Revision,
    pub generation: Option<Generation>,
}

impl LeaseEntry {
    pub(super) fn is_held_by(&self, holder: &HolderId, revision: Revision) -> bool {
        self.revision == revision
            && self
                .lease
                .as_ref()
                .is_some_and(|lease| lease.holder == *holder)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseRules {
    pub ttl: Duration,
    pub renew_every: Duration,
    pub skew_margin: Duration,
}

impl LeaseRules {
    pub(super) fn lease_for(&self, holder: &HolderId, local_now: SystemTime) -> Lease {
        Lease {
            holder: holder.clone(),
            expires_at: local_now + self.ttl,
        }
    }

    pub(super) fn is_free(&self, entry: &LeaseEntry, local_now: SystemTime) -> bool {
        match &entry.lease {
            None => true,
            Some(lease) => local_now >= lease.expires_at + self.skew_margin,
        }
    }
}
