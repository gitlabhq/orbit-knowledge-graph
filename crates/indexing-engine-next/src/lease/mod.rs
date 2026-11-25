mod rules;
mod store;

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

pub use rules::{Claim, Hold, LeaseRules, Renewal};

pub(crate) use store::{Acquisition, Leases};

use crate::versioned::{Revision, Versioned};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub holder: HolderId,
    pub expires_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HolderId(String);

impl HolderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Versioned<Lease> {
    fn is_held_by(&self, holder: &HolderId, revision: Revision) -> bool {
        self.revision == revision
            && self
                .value
                .as_ref()
                .is_some_and(|lease| lease.holder == *holder)
    }
}
