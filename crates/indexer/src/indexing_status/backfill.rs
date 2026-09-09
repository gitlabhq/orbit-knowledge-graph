use chrono::{DateTime, Utc};
use orbit_utils::traversal_path::TraversalPath;
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use super::{Error, IndexingStatusStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialBackfillState {
    Running,
    Retrying,
    Completed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceBackfill {
    pub target_schema_version: u32,
    pub generation: Uuid,
    pub state: InitialBackfillState,
    pub last_progress_at: Option<DateTime<Utc>>,
    pub completed_pipelines: u64,
    pub total_pipelines: u64,
    pub completed_projects: u64,
    pub error: Option<String>,
}

impl IndexingStatusStore {
    pub async fn namespace_backfill(
        &self,
        path: &TraversalPath,
    ) -> Result<Option<NamespaceBackfill>, Error> {
        self.read_key(&namespace_key(path)?).await
    }

    pub async fn begin_namespace_backfill(
        &self,
        path: &TraversalPath,
        schema_version: u32,
        total_pipelines: u64,
    ) -> Result<NamespaceBackfill, Error> {
        let initial = NamespaceBackfill {
            target_schema_version: schema_version,
            generation: Uuid::new_v4(),
            state: InitialBackfillState::Running,
            last_progress_at: None,
            completed_pipelines: 0,
            total_pipelines,
            completed_projects: 0,
            error: None,
        };

        self.update_key(&namespace_key(path)?, Some(initial.clone()), |current| {
            if current.state != InitialBackfillState::Completed
                && current.target_schema_version != schema_version
            {
                *current = initial.clone();
            }
        })
        .await?
        .ok_or(Error::ConcurrentUpdate)
    }

    pub async fn publish_namespace_backfill(
        &self,
        path: &TraversalPath,
        observation: &NamespaceBackfill,
    ) -> Result<(), Error> {
        self.update_key(
            &namespace_key(path)?,
            None,
            |current: &mut NamespaceBackfill| {
                if current.state == InitialBackfillState::Completed
                    || current.generation != observation.generation
                    || current.target_schema_version != observation.target_schema_version
                {
                    return;
                }

                current.completed_pipelines = observation.completed_pipelines;
                current.total_pipelines = observation.total_pipelines;
                current.completed_projects = observation.completed_projects;

                if observation.state == InitialBackfillState::Completed {
                    current.state = InitialBackfillState::Completed;
                    current.error = None;
                } else if observation.state == InitialBackfillState::Unknown {
                    current.state = InitialBackfillState::Unknown;
                    current.error = observation.error.clone();
                } else if current.state == InitialBackfillState::Unknown {
                    current.state = InitialBackfillState::Running;
                    current.error = None;
                }
            },
        )
        .await?;

        Ok(())
    }

    pub async fn record_progress(&self, path: &TraversalPath, schema_version: u32) {
        let now = Utc::now();

        self.record_backfill_activity(path, schema_version, |current| {
            current.last_progress_at = current.last_progress_at.max(Some(now));
            current.state = InitialBackfillState::Running;
            current.error = None;
        })
        .await;
    }

    pub(super) async fn record_backfill_error(&self, path: &TraversalPath, schema_version: u32) {
        self.record_backfill_activity(path, schema_version, |current| {
            current.state = InitialBackfillState::Retrying;
            current.error = Some("Some initial indexing work could not finish.".into());
        })
        .await;
    }

    async fn record_backfill_activity(
        &self,
        path: &TraversalPath,
        schema_version: u32,
        update: impl Fn(&mut NamespaceBackfill),
    ) {
        let Ok(key) = namespace_key(path) else {
            return;
        };

        let result = self
            .update_key(&key, None, |current: &mut NamespaceBackfill| {
                if current.state != InitialBackfillState::Completed
                    && current.target_schema_version == schema_version
                {
                    update(current);
                }
            })
            .await;

        if let Err(error) = result {
            warn!(%path, %error, "failed to record namespace backfill activity");
        }
    }
}

pub(super) fn namespace_key(path: &TraversalPath) -> Result<String, Error> {
    let namespace_id = path
        .top_level_namespace_id()
        .ok_or(Error::EmptyTraversalPath)?;
    Ok(format!("backfill.{namespace_id}"))
}
