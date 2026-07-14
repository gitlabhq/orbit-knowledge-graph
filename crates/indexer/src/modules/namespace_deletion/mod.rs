mod handler;
mod lower;
mod metrics;
pub(crate) mod store;

pub use handler::NamespaceDeletionHandler;
pub use metrics::DeletionMetrics;
pub use store::{ClickHouseNamespaceDeletionStore, NamespaceDeletionStore};

use std::sync::Arc;

use crate::IndexerConfig;
use crate::clickhouse::ClickHouseConfigurationExt;
use crate::handler::{HandlerInitError, HandlerRegistry};
use crate::topic::{NAMESPACE_DELETION_TOPIC, NamespaceDeletionRequest};
use crate::types::Event;
use gkg_server_config::SubscriptionConfig;

const NAMESPACE_DELETION_CONCURRENCY_GROUP: &str = "code";

pub fn namespace_deletion_topic_policy() -> SubscriptionConfig {
    SubscriptionConfig {
        concurrency_group: Some(NAMESPACE_DELETION_CONCURRENCY_GROUP.to_string()),
        max_attempts: Some(1),
        retry_interval_secs: None,
        dead_letter_on_exhaustion: None,
        max_ack_pending: None,
    }
}

pub fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &ontology::Ontology,
) -> Result<(), HandlerInitError> {
    let graph_client = Arc::new(config.graph.build_client());
    let datalake_client = Arc::new(config.datalake.build_client());

    let store: Arc<dyn NamespaceDeletionStore> = Arc::new(ClickHouseNamespaceDeletionStore::new(
        datalake_client,
        graph_client,
        ontology,
    ));

    let policy = namespace_deletion_topic_policy()
        .with_optional_override(config.engine.topics.get(NAMESPACE_DELETION_TOPIC));
    let subscription = NamespaceDeletionRequest::subscription().with_config(&policy);

    let handler =
        NamespaceDeletionHandler::new(store, metrics::DeletionMetrics::new(), subscription);

    registry.register_handler(Box::new(handler));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::namespace_deletion_topic_policy;

    #[test]
    fn declared_deletion_policy_does_not_retry() {
        let policy = namespace_deletion_topic_policy();
        assert_eq!(policy.max_attempts, Some(1));
        assert_eq!(policy.retry_interval_secs, None);
        assert_eq!(policy.dead_letter_on_exhaustion, None);
        assert_eq!(policy.concurrency_group.as_deref(), Some("code"));
    }
}
