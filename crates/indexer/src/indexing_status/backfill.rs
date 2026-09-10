use chrono::{DateTime, Utc};
use orbit_utils::traversal_path::TraversalPath;
use serde::{Deserialize, Serialize};

use super::{Error, IndexingStatusStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialBackfillState {
    Running,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialBackfill {
    pub state: InitialBackfillState,
    pub completed_pipelines: u64,
    pub total_pipelines: u64,
    pub completed_projects: u64,
    pub last_progress_at: Option<DateTime<Utc>>,
}

impl IndexingStatusStore {
    pub async fn initial_backfill(
        &self,
        namespace: &TraversalPath,
    ) -> Result<Option<InitialBackfill>, Error> {
        self.read_key(&initial_backfill_key(namespace)?).await
    }

    pub async fn put_initial_backfill(
        &self,
        namespace: &TraversalPath,
        status: &InitialBackfill,
    ) -> Result<(), Error> {
        self.write_key(&initial_backfill_key(namespace)?, status)
            .await
    }
}

/// `"42/9970/12345/"` → `"backfill.42"`: one record per root namespace.
pub(super) fn initial_backfill_key(namespace: &TraversalPath) -> Result<String, Error> {
    let namespace_id = namespace
        .top_level_namespace_id()
        .ok_or(Error::EmptyTraversalPath)?;
    Ok(format!("backfill.{namespace_id}"))
}
