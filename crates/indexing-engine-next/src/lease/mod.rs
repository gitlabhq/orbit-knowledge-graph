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
    pub generation: Option<Generation>,
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

impl HolderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl LeaseEntry {
    fn is_held_by(&self, holder: &HolderId, generation: Generation, revision: Revision) -> bool {
        self.generation == Some(generation)
            && self.revision == revision
            && self
                .lease
                .as_ref()
                .is_some_and(|lease| lease.holder == *holder)
    }
}
