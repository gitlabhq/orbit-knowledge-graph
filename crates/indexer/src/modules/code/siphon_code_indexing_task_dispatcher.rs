use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use siphon_proto::replication_event::Operation;
use tracing::{debug, info, warn};

use super::config::subjects;
use super::siphon_decoder::{ColumnExtractor, decode_logical_replication_events};
use crate::configuration::ScheduleConfiguration;
use crate::nats::NatsServices;
use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use crate::topic::CodeIndexingTaskRequest;
use crate::types::{Envelope, Subscription};

fn default_events_stream_name() -> String {
    "siphon_stream_main_db".to_string()
}

fn default_batch_size() -> usize {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiphonCodeIndexingTaskDispatcherConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,

    #[serde(default = "default_events_stream_name")]
    pub events_stream_name: String,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl Default for SiphonCodeIndexingTaskDispatcherConfig {
    fn default() -> Self {
        Self {
            schedule: ScheduleConfiguration::default(),
            events_stream_name: default_events_stream_name(),
            batch_size: default_batch_size(),
        }
    }
}

pub struct SiphonCodeIndexingTaskDispatcher {
    nats: Arc<dyn NatsServices>,
    metrics: ScheduledTaskMetrics,
    config: SiphonCodeIndexingTaskDispatcherConfig,
}

impl SiphonCodeIndexingTaskDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        metrics: ScheduledTaskMetrics,
        config: SiphonCodeIndexingTaskDispatcherConfig,
    ) -> Self {
        Self {
            nats,
            metrics,
            config,
        }
    }

    fn siphon_subscription(&self) -> Subscription {
        Subscription::new(
            self.config.events_stream_name.clone(),
            format!(
                "{}.{}",
                self.config.events_stream_name,
                subjects::CODE_INDEXING_TASKS
            ),
        )
        .manage_stream(false)
    }
}

#[async_trait]
impl ScheduledTask for SiphonCodeIndexingTaskDispatcher {
    fn name(&self) -> &str {
        "dispatch.code.task"
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();

        let result = self.dispatch_inner().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

impl SiphonCodeIndexingTaskDispatcher {
    async fn dispatch_inner(&self) -> Result<(), TaskError> {
        let subscription = self.siphon_subscription();
        let mut total_dispatched: u64 = 0;
        let mut total_skipped: u64 = 0;

        loop {
            let messages = self
                .nats
                .consume_pending(&subscription, self.config.batch_size)
                .await
                .map_err(|error| {
                    self.metrics.record_error(self.name(), "consume");
                    TaskError::new(error)
                })?;

            if messages.is_empty() {
                break;
            }

            for message in messages {
                let (dispatched, skipped) = self.dispatch_siphon_message(&message.envelope).await?;
                total_dispatched += dispatched;
                total_skipped += skipped;

                message.ack().await.map_err(|error| {
                    self.metrics.record_error(self.name(), "ack");
                    TaskError::new(error)
                })?;
            }
        }

        if total_dispatched > 0 || total_skipped > 0 {
            self.metrics
                .record_requests_published(self.name(), total_dispatched);
            self.metrics
                .record_requests_skipped(self.name(), total_skipped);

            info!(
                dispatched = total_dispatched,
                skipped = total_skipped,
                "dispatched code indexing task requests"
            );
        }

        Ok(())
    }

    async fn dispatch_siphon_message(
        &self,
        envelope: &crate::types::Envelope,
    ) -> Result<(u64, u64), TaskError> {
        let replication_events =
            decode_logical_replication_events(&envelope.payload).map_err(|error| {
                self.metrics.record_error(self.name(), "decode");
                TaskError::new(error)
            })?;

        let extractor = ColumnExtractor::new(&replication_events);
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;

        for event in &replication_events.events {
            if event.operation == Operation::InitialSnapshot as i32 {
                debug!("skipping initial snapshot event");
                continue;
            }

            let Some(task_id) = extractor.get_i64(event, "id") else {
                warn!("failed to extract task id, skipping");
                continue;
            };
            let Some(project_id) = extractor.get_i64(event, "project_id") else {
                warn!("failed to extract project_id, skipping");
                continue;
            };
            let Some(ref_name) = extractor.get_string(event, "ref") else {
                warn!(task_id, "failed to extract ref, skipping");
                continue;
            };
            let Some(commit_sha) = extractor.get_string(event, "commit_sha") else {
                warn!(task_id, "failed to extract commit_sha, skipping");
                continue;
            };
            let Some(traversal_path) = extractor.get_string(event, "traversal_path") else {
                warn!(task_id, "failed to extract traversal_path, skipping");
                continue;
            };

            let branch = ref_name
                .strip_prefix("refs/heads/")
                .unwrap_or(ref_name)
                .to_string();

            let request = CodeIndexingTaskRequest {
                task_id,
                project_id,
                branch,
                commit_sha: commit_sha.to_string(),
                traversal_path: traversal_path.to_string(),
            };

            let publish_subscription = request.publish_subscription();
            let request_envelope = Envelope::new(&request).map_err(|error| {
                self.metrics.record_error(self.name(), "publish");
                TaskError::new(error)
            })?;

            match self
                .nats
                .publish(&publish_subscription, &request_envelope)
                .await
            {
                Ok(()) => {
                    dispatched += 1;
                    debug!(task_id, project_id, "dispatched code indexing task request");
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    skipped += 1;
                    debug!(
                        task_id,
                        project_id, "skipped code indexing task request, already in-flight"
                    );
                }
                Err(error) => {
                    self.metrics.record_error(self.name(), "publish");
                    return Err(TaskError::new(error));
                }
            }
        }

        Ok((dispatched, skipped))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::modules::code::test_helpers::{
        build_replication_events, code_indexing_task_columns,
    };
    use crate::scheduler::ScheduledTaskMetrics;
    use crate::testkit::{MockNatsServices, TestEnvelopeFactory};
    use crate::topic::CodeIndexingTaskRequest;
    use siphon_proto::replication_event::Operation;

    fn test_metrics() -> ScheduledTaskMetrics {
        ScheduledTaskMetrics::with_meter(&crate::testkit::test_meter())
    }

    fn create_dispatcher(nats: Arc<MockNatsServices>) -> SiphonCodeIndexingTaskDispatcher {
        SiphonCodeIndexingTaskDispatcher::new(
            nats,
            test_metrics(),
            SiphonCodeIndexingTaskDispatcherConfig::default(),
        )
    }

    #[test]
    fn task_name() {
        let nats = Arc::new(MockNatsServices::new());
        let dispatcher = create_dispatcher(nats);

        assert_eq!(dispatcher.name(), "dispatch.code.task");
    }

    #[test]
    fn defaults_to_batch_size_100() {
        let config = SiphonCodeIndexingTaskDispatcherConfig::default();
        assert_eq!(config.batch_size, 100);
    }

    #[tokio::test]
    async fn dispatches_code_indexing_task_from_siphon_event() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(42, 123, "refs/heads/main", "abc123", "/org/project-123")
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        let dispatcher = create_dispatcher(Arc::clone(&nats));
        dispatcher.run().await.unwrap();

        let published = nats.get_published();
        assert_eq!(published.len(), 1);

        let request: CodeIndexingTaskRequest =
            serde_json::from_slice(&published[0].1.payload).unwrap();
        assert_eq!(request.task_id, 42);
        assert_eq!(request.project_id, 123);
        assert_eq!(request.branch, "main");
        assert_eq!(request.commit_sha, "abc123");
        assert_eq!(request.traversal_path, "/org/project-123");
    }

    #[tokio::test]
    async fn strips_refs_heads_prefix() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(
                1,
                100,
                "refs/heads/feature/test",
                "def456",
                "/org/project-100",
            )
            .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        let dispatcher = create_dispatcher(Arc::clone(&nats));
        dispatcher.run().await.unwrap();

        let published = nats.get_published();
        let request: CodeIndexingTaskRequest =
            serde_json::from_slice(&published[0].1.payload).unwrap();
        assert_eq!(request.branch, "feature/test");
    }

    #[tokio::test]
    async fn skips_initial_snapshot_events() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(1, 100, "main", "abc123", "/org/project-100")
                .with_operation(Operation::InitialSnapshot as i32)
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        let dispatcher = create_dispatcher(Arc::clone(&nats));
        dispatcher.run().await.unwrap();

        let published = nats.get_published();
        assert!(published.is_empty());
    }

    #[tokio::test]
    async fn no_messages_produces_no_dispatches() {
        let nats = Arc::new(MockNatsServices::new());
        let dispatcher = create_dispatcher(Arc::clone(&nats));

        dispatcher.run().await.unwrap();

        let published = nats.get_published();
        assert!(published.is_empty());
    }

    #[tokio::test]
    async fn publishes_to_correct_subject() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(1, 42, "refs/heads/main", "abc123", "/org/project-42")
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        let dispatcher = create_dispatcher(Arc::clone(&nats));
        dispatcher.run().await.unwrap();

        let published = nats.get_published();
        assert_eq!(
            published[0].0.subject.as_ref(),
            "code.task.indexing.requested.42.main"
        );
    }
}
