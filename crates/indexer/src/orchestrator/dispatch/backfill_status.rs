use std::sync::Arc;

use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use ontology::{EtlScope, Ontology};
use orbit_migrations::completion::count_completed_namespace_pipelines;
use orbit_migrations::scope::CODE_INDEXING_CHECKPOINT_TABLE;
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_utils::traversal_path::TraversalPath;
use tracing::warn;

use crate::clickhouse::ArrowClickHouseClient;
use crate::indexing_status::{IndexingStatusStore, InitialBackfill, InitialBackfillState};
use crate::orchestrator::scheduled::TaskError;

const LAST_CODE_CHECKPOINT_QUERY: &str = "\
SELECT maxOrNull(indexed_at) AS last_indexed_at \
FROM {table:Identifier} FINAL \
WHERE _deleted = false \
  AND startsWith(traversal_path, {traversal_path:String})";

pub struct InitialBackfillTracker {
    graph: ArrowClickHouseClient,
    store: Arc<IndexingStatusStore>,
    namespaced_pipelines: Vec<String>,
}

impl InitialBackfillTracker {
    pub fn new(
        graph: ArrowClickHouseClient,
        store: Arc<IndexingStatusStore>,
        ontology: &Ontology,
    ) -> Self {
        Self {
            graph,
            store,
            namespaced_pipelines: ontology
                .pipeline_descriptors()
                .into_iter()
                .filter(|pipeline| pipeline.scope == EtlScope::Namespaced)
                .map(|pipeline| pipeline.name)
                .collect(),
        }
    }

    pub async fn record(
        &self,
        namespace: &TraversalPath,
        checkpointed_projects: usize,
        all_projects_checkpointed: bool,
    ) {
        if let Err(error) = self
            .record_or_fail(namespace, checkpointed_projects, all_projects_checkpointed)
            .await
        {
            warn!(%namespace, %error, "could not record initial backfill status");
        }
    }

    async fn record_or_fail(
        &self,
        namespace: &TraversalPath,
        checkpointed_projects: usize,
        all_projects_checkpointed: bool,
    ) -> Result<(), TaskError> {
        let previous = self
            .store
            .initial_backfill(namespace)
            .await
            .map_err(TaskError::new)?;
        if previous.is_some_and(|status| status.state == InitialBackfillState::Completed) {
            return Ok(());
        }

        let namespace_id = namespace
            .top_level_namespace_id()
            .ok_or_else(|| TaskError::new("initial backfill status needs a root namespace"))?;
        let sdlc = count_completed_namespace_pipelines(
            &self.graph,
            *SCHEMA_VERSION,
            namespace_id,
            &self.namespaced_pipelines,
        )
        .await
        .map_err(TaskError::new)?;
        let last_code_checkpoint_at = self.last_code_checkpoint_at(namespace).await?;

        let total_pipelines = self.namespaced_pipelines.len() as u64;
        let state = if sdlc.pipelines == total_pipelines && all_projects_checkpointed {
            InitialBackfillState::Completed
        } else {
            InitialBackfillState::Running
        };

        self.store
            .put_initial_backfill(
                namespace,
                &InitialBackfill {
                    state,
                    completed_pipelines: sdlc.pipelines,
                    total_pipelines,
                    completed_projects: checkpointed_projects as u64,
                    last_progress_at: sdlc.last_checkpoint_at.max(last_code_checkpoint_at),
                },
            )
            .await
            .map_err(TaskError::new)
    }

    async fn last_code_checkpoint_at(
        &self,
        namespace: &TraversalPath,
    ) -> Result<Option<DateTime<Utc>>, TaskError> {
        let batches = self
            .graph
            .query(LAST_CODE_CHECKPOINT_QUERY)
            .param(
                "table",
                prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION),
            )
            .param("traversal_path", namespace.as_str())
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        Ok(DateTime::<Utc>::extract_column(&batches, 0)
            .map_err(TaskError::new)?
            .first()
            .copied())
    }
}
