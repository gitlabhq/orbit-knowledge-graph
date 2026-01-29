mod extract;

use std::sync::Arc;

use chrono::Utc;
use etl_engine::clickhouse::ClickHouseError;
use etl_engine::nats::NatsBroker;
use etl_engine::types::{Envelope, Event};
use tracing::info;

use crate::config::AppConfig;
use crate::indexer::topic::{GlobalIndexingRequest, NamespaceIndexingRequest};
use extract::FromArrowColumn;

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, organization_id
FROM siphon_knowledge_graph_enabled_namespaces
INNER JOIN siphon_namespaces on siphon_knowledge_graph_enabled_namespaces.root_namespace_id = siphon_namespaces.id
WHERE _siphon_deleted = false
"#;

#[derive(Debug, thiserror::Error)]
pub enum DispatcherError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] etl_engine::nats::NatsError),

    #[error("ClickHouse query error: {0}")]
    ClickHouseQueryError(ClickHouseError),

    #[error("Invalid column type: expected {expected}")]
    InvalidColumnType { expected: &'static str },

    #[error("Failed to serialize message: {0}")]
    Serialization(#[from] etl_engine::types::SerializationError),

    #[error("Failed to publish message: {0}")]
    Publish(etl_engine::nats::NatsError),
}

struct Dispatcher {
    broker: Arc<NatsBroker>,
}

impl Dispatcher {
    async fn publish<E: Event>(&self, event: &E) -> Result<(), DispatcherError> {
        let envelope = Envelope::new(event)?;
        self.broker
            .publish(&E::topic(), &envelope)
            .await
            .map_err(DispatcherError::Publish)
    }
}

pub async fn run(config: &AppConfig) -> Result<(), DispatcherError> {
    let dispatcher = Dispatcher {
        broker: Arc::new(NatsBroker::connect(&config.nats).await?),
    };
    let datalake = config.datalake.build_client();

    let arrow_batches = datalake
        .query(ENABLED_NAMESPACE_QUERY)
        .fetch_arrow()
        .await
        .map_err(DispatcherError::ClickHouseQueryError)?;

    let namespace_ids = i64::extract_column(&arrow_batches, 0)?;
    let organization_ids = i64::extract_column(&arrow_batches, 1)?;
    let watermark = Utc::now();

    dispatcher
        .publish(&GlobalIndexingRequest { watermark })
        .await?;
    info!("Dispatched global indexing request");

    for (namespace_id, organization_id) in namespace_ids.iter().zip(organization_ids.iter()) {
        dispatcher
            .publish(&NamespaceIndexingRequest {
                organization: *organization_id,
                namespace: *namespace_id,
                watermark,
            })
            .await?;
    }
    info!(
        count = namespace_ids.len(),
        "Dispatched namespace indexing requests"
    );

    Ok(())
}
