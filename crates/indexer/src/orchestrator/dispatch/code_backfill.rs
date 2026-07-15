//! Shared, trigger-agnostic code-backfill operations: namespace/project
//! enumeration, checkpoint filtering, and publishing code-indexing requests.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Instant;

use rand::seq::SliceRandom;
use tracing::{debug, info};
use uuid::Uuid;

use super::DispatchOutcome;
use crate::campaign::CampaignState;
use crate::clickhouse::ArrowClickHouseClient;
use crate::orchestrator::scheduled::{ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use crate::topic::CodeIndexingTaskRequest;
use crate::types::Envelope;
use clickhouse_client::FromArrowColumn;

pub const METRIC_NAME: &str = "dispatch.code_backfill";

const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";

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

static ENABLED_NAMESPACES_QUERY: LazyLock<String> = LazyLock::new(|| {
    let del = ontology::siphon_deleted_column();
    format!(
        "SELECT root_namespace_id, traversal_path \
         FROM siphon_knowledge_graph_enabled_namespaces \
         WHERE {del} = false AND traversal_path != ''"
    )
});

pub struct PendingProject {
    pub project_id: i64,
    pub traversal_path: String,
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

    /// Dispatches code indexing for every enabled namespace, publishing only
    /// un-checkpointed projects and reporting fully-checkpointed namespaces
    /// as drained.
    pub async fn dispatch_enabled(
        &self,
        dispatch_id: Uuid,
    ) -> Result<(DispatchOutcome, Vec<String>), TaskError> {
        let enabled = self.fetch_enabled_namespaces().await?;
        self.dispatch_for_namespaces(&enabled, dispatch_id).await
    }

    /// Returns the dispatch outcome and the traversal paths of namespaces
    /// with no pending projects this tick (every project checkpointed).
    pub async fn dispatch_for_namespaces(
        &self,
        namespaces: &[(i64, String)],
        dispatch_id: Uuid,
    ) -> Result<(DispatchOutcome, Vec<String>), TaskError> {
        let mut all_pending: Vec<PendingProject> = Vec::new();
        let mut drained_paths: Vec<String> = Vec::new();
        for (namespace_id, traversal_path) in namespaces {
            let pending = self
                .fetch_pending_for_namespace(*namespace_id, traversal_path)
                .await?;
            if pending.is_empty() {
                drained_paths.push(traversal_path.clone());
            }
            all_pending.extend(pending);
        }
        // Shuffle the flat project list so the NATS queue is interleaved
        // across namespaces; otherwise FIFO consumption processes one
        // namespace's entire batch before any other namespace gets a turn.
        all_pending.shuffle(&mut rand::rng());
        let outcome = self.publish_pending(&all_pending, dispatch_id).await?;

        if outcome.dispatched > 0 || outcome.skipped > 0 {
            self.metrics
                .record_requests_published(METRIC_NAME, outcome.dispatched);
            self.metrics
                .record_requests_skipped(METRIC_NAME, outcome.skipped);

            info!(
                dispatched = outcome.dispatched,
                skipped = outcome.skipped,
                "dispatched code backfill requests"
            );
        }

        Ok((outcome, drained_paths))
    }

    /// Returns the set of project IDs whose checkpoint row already exists
    /// under `traversal_path` for the indexer's current schema version.
    async fn fetch_checkpointed_project_ids(
        &self,
        traversal_path: &str,
    ) -> Result<HashSet<i64>, TaskError> {
        let table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let batches = self
            .graph
            .query(CHECKPOINTED_PROJECT_IDS_QUERY)
            .param("table", &table)
            .param("traversal_path", traversal_path)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(METRIC_NAME, "query");
                TaskError::new(error)
            })?;

        let ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        Ok(ids.into_iter().collect())
    }

    /// Returns (root_namespace_id, traversal_path) for every currently-enabled
    /// namespace. Reads `traversal_path` from the enabled namespaces table
    /// directly (gitlab-org/gitlab!232941); the prior implementation joined
    /// `namespace_traversal_paths` per namespace.
    pub async fn fetch_enabled_namespaces(&self) -> Result<Vec<(i64, String)>, TaskError> {
        let batches = self
            .datalake
            .query(&ENABLED_NAMESPACES_QUERY)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(METRIC_NAME, "query");
                TaskError::new(error)
            })?;

        let ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;
        Ok(ids.into_iter().zip(paths).collect())
    }

    async fn fetch_pending_for_namespace(
        &self,
        namespace_id: i64,
        traversal_path: &str,
    ) -> Result<Vec<PendingProject>, TaskError> {
        let projects = self.fetch_namespace_projects(traversal_path).await?;

        if projects.is_empty() {
            debug!(namespace_id, "no pending projects in namespace");
            return Ok(Vec::new());
        }

        // Skip projects that already have a checkpoint for the current
        // schema version. Filtering here keeps the publish loop bounded by
        // the un-indexed remainder rather than the full project set, which
        // matters at scale: a namespace with thousands of projects produces
        // O(remaining) NATS publishes per tick, not O(total).
        let checkpointed = self.fetch_checkpointed_project_ids(traversal_path).await?;
        let pending_count_before_filter = projects.len();
        let projects: Vec<PendingProject> = projects
            .into_iter()
            .filter(|p| !checkpointed.contains(&p.project_id))
            .collect();

        if projects.is_empty() {
            debug!(namespace_id, "all projects already checkpointed");
            return Ok(Vec::new());
        }

        info!(
            namespace_id,
            count = projects.len(),
            already_checkpointed = pending_count_before_filter - projects.len(),
            "fetched pending projects for code backfill"
        );

        Ok(projects)
    }

    pub async fn publish_pending(
        &self,
        projects: &[PendingProject],
        dispatch_id: Uuid,
    ) -> Result<DispatchOutcome, TaskError> {
        let mut outcome = DispatchOutcome {
            dispatched: 0,
            skipped: 0,
        };
        let campaign_id = self.campaign.current();

        for project in projects {
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
                Ok(()) => {
                    outcome.dispatched += 1;
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    outcome.skipped += 1;
                }
                Err(error) => {
                    self.metrics.record_error(METRIC_NAME, "publish");
                    return Err(TaskError::new(error));
                }
            }
        }

        Ok(outcome)
    }

    async fn fetch_namespace_projects(
        &self,
        traversal_path: &str,
    ) -> Result<Vec<PendingProject>, TaskError> {
        let query_start = Instant::now();
        let batches = self
            .datalake
            .query(NAMESPACE_PROJECTS_QUERY)
            .param("traversal_path", traversal_path)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(METRIC_NAME, "query");
                TaskError::new(error)
            })?;
        self.metrics.record_query_duration(
            "namespace_pending_projects",
            query_start.elapsed().as_secs_f64(),
        );

        let project_ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let traversal_paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;

        Ok(project_ids
            .into_iter()
            .zip(traversal_paths)
            .map(|(project_id, traversal_path)| PendingProject {
                project_id,
                traversal_path,
            })
            .collect())
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

    #[test]
    fn enabled_namespaces_query_filters_deleted_and_pulls_path() {
        assert!(ENABLED_NAMESPACES_QUERY.contains("_siphon_deleted = false"));
        assert!(ENABLED_NAMESPACES_QUERY.contains("traversal_path"));
        assert!(
            ENABLED_NAMESPACES_QUERY.contains("traversal_path != ''"),
            "must skip rows where the dictionary-backed default hasn't \
             populated yet — empty path would prefix-match every project"
        );
    }

    #[tokio::test]
    async fn shuffled_publish_interleaves_two_namespaces() {
        let nats = Arc::new(MockNatsServices::new());
        let backfill = create_backfill(Arc::clone(&nats));

        let mut projects: Vec<PendingProject> = (0..100)
            .map(|i| PendingProject {
                project_id: 10_000 + i,
                traversal_path: "1/A/".to_string(),
            })
            .collect();
        projects.extend((0..100).map(|i| PendingProject {
            project_id: 20_000 + i,
            traversal_path: "1/B/".to_string(),
        }));

        projects.shuffle(&mut rand::rng());
        let outcome = backfill
            .publish_pending(&projects, Uuid::new_v4())
            .await
            .unwrap();
        assert_eq!(outcome.dispatched, 200);

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
