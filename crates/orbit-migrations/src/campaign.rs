use std::sync::RwLock;

pub fn campaign_id_for_version(version: u32) -> String {
    format!("migration-v{version}")
}

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn campaign_lifecycle() {
        let state = CampaignState::new();
        assert_eq!(state.current(), None);

        state.set(campaign_id_for_version(48));
        assert_eq!(state.current(), Some("migration-v48".to_string()));

        state.clear();
        assert_eq!(state.current(), None);
    }
}
