//! Shared, trigger-agnostic code-backfill operations: namespace/project
//! enumeration, checkpoint filtering, and publishing code-indexing requests.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use rand::seq::SliceRandom;
use tracing::{debug, info};
use uuid::Uuid;

use super::DispatchOutcome;
use super::enabled_namespaces::resolved_enabled_namespaces_sql;
use crate::campaign::CampaignState;
use crate::clickhouse::ArrowClickHouseClient;
use crate::orchestrator::scheduled::{ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use crate::topic::CodeIndexingTaskRequest;
use crate::types::Envelope;
use clickhouse_client::FromArrowColumn;
use orbit_utils::traversal_path::TraversalPath;

pub const METRIC_NAME: &str = "dispatch.code_backfill";

const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";

/// Holding a full sweep (2.3M projects on gitlab.com) before the first publish OOMed the pod.
const PENDING_PUBLISH_WINDOW: usize = 50_000;

const CHECKPOINTED_PROJECT_IDS_QUERY: &str = r#"
SELECT DISTINCT project_id
FROM {table:Identifier} FINAL
WHERE _deleted = false
  AND startsWith(traversal_path, {traversal_path:String})
"#;

const NAMESPACE_PROJECTS_QUERY: &str = r#"
SELECT id AS project_id, traversal_path
FROM project_namespace_traversal_paths
WHERE deleted = false
  AND startsWith(traversal_path, {traversal_path:String})
"#;

pub struct PendingProject {
    pub project_id: i64,
    pub traversal_path: TraversalPath,
}

pub struct CodeBackfill {
    nats: Arc<dyn crate::nats::NatsServices>,
    graph: ArrowClickHouseClient,
    datalake: ArrowClickHouseClient,
    metrics: ScheduledTaskMetrics,
    campaign: Arc<CampaignState>,
}

impl CodeBackfill {
    pub fn new(
        nats: Arc<dyn crate::nats::NatsServices>,
        graph: ArrowClickHouseClient,
        datalake: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        campaign: Arc<CampaignState>,
    ) -> Self {
        Self {
            nats,
            graph,
            datalake,
            metrics,
            campaign,
        }
    }

    pub fn metrics(&self) -> &ScheduledTaskMetrics {
        &self.metrics
    }

    pub async fn dispatch_enabled(&self, dispatch_id: Uuid) -> Result<DispatchOutcome, TaskError> {
        let enabled = self.fetch_enabled_namespaces().await?;
        self.dispatch_for_namespaces(&enabled, dispatch_id).await
    }

    pub async fn dispatch_for_namespaces(
        &self,
        namespaces: &[(i64, TraversalPath)],
        dispatch_id: Uuid,
    ) -> Result<DispatchOutcome, TaskError> {
        let mut outcome = DispatchOutcome::default();
        let mut window: Vec<PendingProject> = Vec::new();
        for (namespace_id, traversal_path) in namespaces {
            let pending = self
                .fetch_pending_for_namespace(*namespace_id, traversal_path)
                .await?;
            if pending.is_empty() {
                outcome.drained_paths.push(traversal_path.clone());
                continue;
            }
            window.extend(pending);
            if window.len() >= PENDING_PUBLISH_WINDOW {
                self.publish_window(&mut window, dispatch_id, &mut outcome)
                    .await?;
            }
        }
        self.publish_window(&mut window, dispatch_id, &mut outcome)
            .await?;

        if outcome.dispatched > 0 || outcome.skipped > 0 {
            info!(
                dispatched = outcome.dispatched,
                skipped = outcome.skipped,
                "dispatched code backfill requests"
            );
        }

        Ok(outcome)
    }

    /// Scoped to the checkpoint table of the indexer's current schema version.
    async fn fetch_checkpointed_project_ids(
        &self,
        traversal_path: &TraversalPath,
    ) -> Result<HashSet<i64>, TaskError> {
        let table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let mut batches = self
            .graph
            .query(CHECKPOINTED_PROJECT_IDS_QUERY)
            .param("table", &table)
            .param("traversal_path", traversal_path.as_str())
            .fetch_arrow_streamed(None)
            .await
            .map_err(|error| {
                self.metrics.record_error(METRIC_NAME, "query");
                TaskError::new(error)
            })?;

        let mut ids = HashSet::new();
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|error| {
                self.metrics.record_error(METRIC_NAME, "query");
                TaskError::new(error)
            })?;
            ids.extend(
                i64::extract_column(std::slice::from_ref(&batch), 0).map_err(TaskError::new)?,
            );
        }
        Ok(ids)
    }

    pub async fn fetch_enabled_namespaces(&self) -> Result<Vec<(i64, TraversalPath)>, TaskError> {
        let batches = self
            .datalake
            .query(resolved_enabled_namespaces_sql())
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(METRIC_NAME, "query");
                TaskError::new(error)
            })?;

        let ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;
        Ok(ids
            .into_iter()
            .zip(paths.into_iter().map(TraversalPath::new_unchecked))
            .collect())
    }

    async fn fetch_pending_for_namespace(
        &self,
        namespace_id: i64,
        traversal_path: &TraversalPath,
    ) -> Result<Vec<PendingProject>, TaskError> {
        // Checkpoints first so each project batch is filtered on arrival, holding only the remainder.
        let checkpointed = self.fetch_checkpointed_project_ids(traversal_path).await?;
        let (projects, already_checkpointed) = self
            .fetch_pending_projects(traversal_path, &checkpointed)
            .await?;

        if projects.is_empty() {
            debug!(
                namespace_id,
                already_checkpointed, "no pending projects in namespace"
            );
            return Ok(Vec::new());
        }

        debug!(
            namespace_id,
            count = projects.len(),
            already_checkpointed,
            "fetched pending projects for code backfill"
        );

        Ok(projects)
    }

    /// Shuffled so FIFO consumption interleaves the namespaces in a window instead of draining one first.
    async fn publish_window(
        &self,
        window: &mut Vec<PendingProject>,
        dispatch_id: Uuid,
        outcome: &mut DispatchOutcome,
    ) -> Result<(), TaskError> {
        window.shuffle(&mut rand::rng());
        let campaign_id = self.campaign.current();
        let mut dispatched = 0u64;
        let mut skipped = 0u64;

        for project in window.iter() {
            let request = CodeIndexingTaskRequest {
                task_id: 0,
                project_id: project.project_id,
                branch: None,
                commit_sha: None,
                traversal_path: project.traversal_path.clone(),
                dispatch_id,
                campaign_id: campaign_id.clone(),
            };

            let subscription = request.publish_subscription();
            let envelope = Envelope::new(&request).map_err(|error| {
                self.metrics.record_error(METRIC_NAME, "publish");
                TaskError::new(error)
            })?;

            match self.nats.publish(&subscription, &envelope).await {
                Ok(()) => dispatched += 1,
                Err(crate::nats::NatsError::PublishDuplicate) => skipped += 1,
                Err(error) => {
                    self.metrics.record_error(METRIC_NAME, "publish");
                    return Err(TaskError::new(error));
                }
            }
        }

        // Recorded per window, so a later namespace's query error cannot drop the counts of what is already published.
        self.metrics
            .record_requests_published(METRIC_NAME, dispatched);
        self.metrics.record_requests_skipped(METRIC_NAME, skipped);
        outcome.dispatched += dispatched;
        outcome.skipped += skipped;
        window.clear();
        Ok(())
    }

    /// Returns the pending projects and how many rows the checkpoints filtered out.
    async fn fetch_pending_projects(
        &self,
        traversal_path: &TraversalPath,
        checkpointed: &HashSet<i64>,
    ) -> Result<(Vec<PendingProject>, usize), TaskError> {
        let query_start = Instant::now();
        let mut batches = self
            .datalake
            .query(NAMESPACE_PROJECTS_QUERY)
            .param("traversal_path", traversal_path.as_str())
            .fetch_arrow_streamed(None)
            .await
            .map_err(|error| {
                self.metrics.record_error(METRIC_NAME, "query");
                TaskError::new(error)
            })?;

        let mut pending: Vec<PendingProject> = Vec::new();
        let mut already_checkpointed = 0usize;
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|error| {
                self.metrics.record_error(METRIC_NAME, "query");
                TaskError::new(error)
            })?;
            let rows = std::slice::from_ref(&batch);
            let project_ids = i64::extract_column(rows, 0).map_err(TaskError::new)?;
            let traversal_paths = String::extract_column(rows, 1).map_err(TaskError::new)?;
            for (project_id, traversal_path) in project_ids.into_iter().zip(traversal_paths) {
                if checkpointed.contains(&project_id) {
                    already_checkpointed += 1;
                    continue;
                }
                pending.push(PendingProject {
                    project_id,
                    traversal_path: TraversalPath::new_unchecked(traversal_path),
                });
            }
        }
        self.metrics.record_query_duration(
            "namespace_pending_projects",
            query_start.elapsed().as_secs_f64(),
        );

        Ok((pending, already_checkpointed))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::testkit::MockNatsServices;

    fn test_metrics() -> ScheduledTaskMetrics {
        ScheduledTaskMetrics::with_meter(&crate::testkit::test_meter())
    }

    fn create_backfill(nats: Arc<MockNatsServices>) -> CodeBackfill {
        let empty = &std::collections::HashMap::new();
        let graph = ArrowClickHouseClient::new(
            "http://localhost:0",
            "default",
            "default",
            None,
            empty,
            empty,
        );
        let datalake = ArrowClickHouseClient::new(
            "http://localhost:0",
            "default",
            "default",
            None,
            empty,
            empty,
        );
        CodeBackfill::new(
            nats,
            graph,
            datalake,
            test_metrics(),
            Arc::new(CampaignState::new()),
        )
    }

    #[tokio::test]
    async fn published_window_interleaves_two_namespaces_and_is_drained() {
        let nats = Arc::new(MockNatsServices::new());
        let backfill = create_backfill(Arc::clone(&nats));

        let mut window: Vec<PendingProject> = (0..100)
            .map(|i| PendingProject {
                project_id: 10_000 + i,
                traversal_path: TraversalPath::new_unchecked("1/A/"),
            })
            .collect();
        window.extend((0..100).map(|i| PendingProject {
            project_id: 20_000 + i,
            traversal_path: TraversalPath::new_unchecked("1/B/"),
        }));

        let mut outcome = DispatchOutcome::default();
        backfill
            .publish_window(&mut window, Uuid::new_v4(), &mut outcome)
            .await
            .unwrap();
        assert_eq!(outcome.dispatched, 200);
        assert!(window.is_empty());

        let published = nats.get_published();
        let from_a = |idx: usize| {
            published[idx]
                .0
                .subject
                .strip_prefix("code.task.indexing.requested.1")
                .is_some()
        };
        let first_half_a = (0..100).filter(|&i| from_a(i)).count();
        let second_half_a = (100..200).filter(|&i| from_a(i)).count();
        assert!(
            (25..=75).contains(&first_half_a) && (25..=75).contains(&second_half_a),
            "expected both halves to contain projects from both namespaces; \
             got A in first half: {first_half_a}, A in second half: {second_half_a}"
        );
    }
}
