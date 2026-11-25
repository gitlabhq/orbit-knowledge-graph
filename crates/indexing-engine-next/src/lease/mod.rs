mod rules;
mod store;

use std::time::SystemTime;

pub use rules::{Claim, Hold, LeaseRules, Renewal};

pub(crate) use store::{Acquisition, LeaseStore, LeaseWrite, Leases};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub holder: HolderId,
    pub expires_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HolderId(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseEntry {
    pub lease: Option<Lease>,
    pub revision: Revision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(pub u64);

impl HolderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl LeaseEntry {
    fn is_held_by(&self, holder: &HolderId, revision: Revision) -> bool {
        self.revision == revision
            && self
                .lease
                .as_ref()
                .is_some_and(|lease| lease.holder == *holder)
    }
}
