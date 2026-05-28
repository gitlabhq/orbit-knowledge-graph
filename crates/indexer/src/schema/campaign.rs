use std::sync::{Arc, RwLock};

use uuid::Uuid;

pub type CampaignState = Arc<RwLock<Option<Uuid>>>;

pub fn new_campaign_state() -> CampaignState {
    Arc::new(RwLock::new(None))
}
