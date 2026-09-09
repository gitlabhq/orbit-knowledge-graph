use std::collections::HashMap;
use std::sync::Arc;

use ontology::{EtlScope, Ontology};
use orbit_migrations::version::{read_active_version, read_migrating_version};
use orbit_utils::traversal_path::TraversalPath;

use crate::checkpoint::{CheckpointStore, ClickHouseCheckpointStore, namespace_position_key};
use crate::clickhouse::ArrowClickHouseClient;
use crate::indexing_status::{IndexingStatusStore, InitialBackfillState, NamespaceBackfill};
use crate::locking::{LockGuard, LockService};
use crate::orchestrator::scheduled::TaskError;
use crate::schema::migration::{MIGRATION_LOCK_KEY, MIGRATION_LOCK_TTL};

pub struct BackfillStatus {
    graph: ArrowClickHouseClient,
    store: Arc<IndexingStatusStore>,
    lock_service: Arc<dyn LockService>,
    pipeline_names: Vec<String>,
    schema_version: u32,
}

impl BackfillStatus {
    pub fn new(
        graph: ArrowClickHouseClient,
        store: Arc<IndexingStatusStore>,
        lock_service: Arc<dyn LockService>,
        ontology: &Ontology,
        schema_version: u32,
    ) -> Self {
        Self {
            graph,
            store,
            lock_service,
            pipeline_names: ontology
                .pipeline_descriptors()
                .into_iter()
                .filter(|pipeline| pipeline.scope == EtlScope::Namespaced)
                .map(|pipeline| pipeline.name)
                .collect(),
            schema_version,
        }
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub async fn begin(
        &self,
        path: &TraversalPath,
    ) -> Result<Option<NamespaceBackfill>, TaskError> {
        let previous = self
            .store
            .namespace_backfill(path)
            .await
            .map_err(TaskError::new)?;

        if previous.is_some_and(|status| status.state == InitialBackfillState::Completed) {
            return Ok(None);
        }

        let Some(guard) = self.target_guard().await? else {
            return Ok(None);
        };
        let result = self
            .store
            .begin_namespace_backfill(path, self.schema_version, self.pipeline_names.len() as u64)
            .await;
        guard.release().await.map_err(TaskError::new)?;

        let mut observation = result.map_err(TaskError::new)?;
        if observation.state == InitialBackfillState::Completed {
            return Ok(None);
        }

        let namespace_id = path
            .top_level_namespace_id()
            .ok_or_else(|| TaskError::new("namespace path is required"))?;
        let prefix = format!("{}.", namespace_position_key(namespace_id));
        let checkpoint_store = ClickHouseCheckpointStore::for_version(
            Arc::new(self.graph.clone()),
            self.schema_version,
        );
        let checkpoints: HashMap<_, _> = checkpoint_store
            .load_by_prefix(&prefix)
            .await
            .map_err(TaskError::new)?
            .into_iter()
            .collect();

        observation.completed_pipelines = self
            .pipeline_names
            .iter()
            .filter(|name| {
                checkpoints
                    .get(&format!("{prefix}{name}"))
                    .is_some_and(|checkpoint| {
                        checkpoint.cursor_values.is_none() || checkpoint.resume_floor.is_some()
                    })
            })
            .count() as u64;

        Ok(Some(observation))
    }

    pub async fn finish(
        &self,
        path: &TraversalPath,
        mut observation: NamespaceBackfill,
        completed_projects: usize,
        code_drained: bool,
    ) -> Result<(), TaskError> {
        let sdlc_complete = observation.completed_pipelines == observation.total_pipelines;

        observation.completed_projects = completed_projects as u64;
        observation.state = if sdlc_complete && code_drained {
            InitialBackfillState::Completed
        } else {
            InitialBackfillState::Running
        };

        self.publish(path, &observation).await
    }

    pub async fn unavailable(&self, path: &TraversalPath) -> Result<(), TaskError> {
        let Some(mut observation) = self
            .store
            .namespace_backfill(path)
            .await
            .map_err(TaskError::new)?
        else {
            return Ok(());
        };

        if observation.target_schema_version != self.schema_version {
            return Ok(());
        }

        observation.state = InitialBackfillState::Unknown;
        observation.error = Some("Initial indexing status is temporarily unavailable.".into());

        self.publish(path, &observation).await
    }

    async fn publish(
        &self,
        path: &TraversalPath,
        observation: &NamespaceBackfill,
    ) -> Result<(), TaskError> {
        let Some(guard) = self.target_guard().await? else {
            return Ok(());
        };
        let result = self
            .store
            .publish_namespace_backfill(path, observation)
            .await;
        guard.release().await.map_err(TaskError::new)?;
        result.map_err(TaskError::new)
    }

    async fn target_guard(&self) -> Result<Option<LockGuard>, TaskError> {
        let Some(guard) = LockGuard::acquire(
            self.lock_service.clone(),
            MIGRATION_LOCK_KEY,
            MIGRATION_LOCK_TTL,
        )
        .await
        .map_err(TaskError::new)?
        else {
            return Ok(None);
        };

        let migrating = read_migrating_version(&self.graph)
            .await
            .map_err(TaskError::new)?;
        let target = match migrating {
            Some(version) => Some(version),
            None => read_active_version(&self.graph)
                .await
                .map_err(TaskError::new)?,
        };

        if target != Some(self.schema_version) {
            guard.release().await.map_err(TaskError::new)?;
            return Ok(None);
        }

        guard
            .renew(MIGRATION_LOCK_TTL)
            .await
            .map_err(TaskError::new)?;
        Ok(Some(guard))
    }
}
