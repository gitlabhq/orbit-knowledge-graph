//! Campaign correlation for re-index dispatches.
//!
//! A campaign groups every dispatch produced by one "re-index everything"
//! decision — today, a schema migration. While a version is `migrating`, all
//! dispatched requests carry the same campaign id; in steady state it is `None`.
//! The id is a pure function of the migrating version, so it needs no storage:
//! any replica reconstructs it from the existing `migrating` row at boot.

use std::sync::RwLock;

/// Human-readable, version-scoped campaign id for a migration to `version`.
pub fn campaign_id_for_version(version: u32) -> String {
    format!("migration-v{version}")
}

/// In-memory holder for the currently active campaign id.
#[derive(Debug, Default)]
pub struct CampaignState {
    current: RwLock<Option<String>>,
}

impl CampaignState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn current(&self) -> Option<String> {
        self.current
            .read()
            .expect("campaign state lock poisoned")
            .clone()
    }

    pub fn set(&self, campaign_id: String) {
        *self.current.write().expect("campaign state lock poisoned") = Some(campaign_id);
    }

    pub fn clear(&self) {
        *self.current.write().expect("campaign state lock poisoned") = None;
    }

    /// Opens the campaign for a migrating version, or clears it in steady state.
    pub fn seed_from_migrating(&self, migrating_version: Option<u32>) {
        match migrating_version {
            Some(version) => self.set(campaign_id_for_version(version)),
            None => self.clear(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn campaign_id_is_human_readable_and_version_scoped() {
        assert_eq!(campaign_id_for_version(48), "migration-v48");
        assert_eq!(campaign_id_for_version(1), "migration-v1");
    }

    #[test]
    fn new_state_is_steady_state() {
        let state = CampaignState::new();
        assert_eq!(state.current(), None);
    }

    #[test]
    fn set_then_current_round_trips() {
        let state = CampaignState::new();
        state.set("migration-v48".to_string());
        assert_eq!(state.current(), Some("migration-v48".to_string()));
    }

    #[test]
    fn clear_returns_to_steady_state() {
        let state = CampaignState::new();
        state.set("migration-v48".to_string());
        state.clear();
        assert_eq!(state.current(), None);
    }

    #[test]
    fn seed_from_migrating_some_opens_campaign() {
        let state = CampaignState::new();
        state.seed_from_migrating(Some(48));
        assert_eq!(state.current(), Some("migration-v48".to_string()));
    }

    #[test]
    fn seed_from_migrating_none_clears_campaign() {
        let state = CampaignState::new();
        state.set("migration-v48".to_string());
        state.seed_from_migrating(None);
        assert_eq!(state.current(), None);
    }
}
