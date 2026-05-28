use std::sync::{Arc, RwLock};

pub type CampaignState = Arc<RwLock<Option<String>>>;

pub fn new_campaign_state() -> CampaignState {
    Arc::new(RwLock::new(None))
}

/// Deterministic, human-readable campaign_id for a schema migration to
/// `version`.
///
/// A campaign is identified by the schema version being migrated to, so the
/// id is derived from that version rather than randomly generated. Any
/// process that knows the migrating version recomputes the same id without
/// storing or propagating a value, and it stays stable across orchestrator
/// restarts that re-mark the same migration.
///
/// The value is a readable label (e.g. `schema-migration-v48`) so it can be
/// queried directly in Snowflake without decoding an opaque id.
pub fn campaign_id_for_version(version: u32) -> String {
    format!("schema-migration-v{version}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn campaign_id_is_readable_and_stable() {
        assert_eq!(campaign_id_for_version(48), "schema-migration-v48");
    }

    #[test]
    fn campaign_id_differs_across_versions() {
        assert_ne!(campaign_id_for_version(47), campaign_id_for_version(48));
    }
}
