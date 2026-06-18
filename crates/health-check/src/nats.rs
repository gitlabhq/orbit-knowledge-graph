use async_nats::jetstream::consumer::PullConsumer;
use gkg_server_config::NatsConfiguration;
use nats_client::NatsClient;
use tokio::sync::OnceCell;
use tracing::warn;

use crate::types::QueueDepth;

pub struct NatsDepthChecker {
    config: NatsConfiguration,
    stream_name: String,
    consumer_name: Option<String>,
    client: OnceCell<NatsClient>,
}

impl NatsDepthChecker {
    pub fn new(
        config: &NatsConfiguration,
        stream_name: String,
        consumer_name: Option<String>,
    ) -> Self {
        Self {
            config: config.clone(),
            stream_name,
            consumer_name,
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<&NatsClient, String> {
        self.client
            .get_or_try_init(|| async {
                NatsClient::connect(&self.config).await.map_err(|e| {
                    warn!(error = %e, "NATS queue-depth check failed to connect");
                    format!("failed to connect to NATS: {e}")
                })
            })
            .await
    }

    pub async fn check(&self) -> Result<QueueDepth, String> {
        let consumer_name = self.consumer_name.as_deref().ok_or_else(|| {
            "queue-depth endpoint is not configured: nats.consumer_name is unset".to_string()
        })?;

        let client = self.client().await?;

        let stream = client.get_stream(&self.stream_name).await.map_err(|e| {
            warn!(stream = %self.stream_name, error = %e, "NATS queue-depth check failed to get stream");
            format!("failed to get stream '{}': {e}", self.stream_name)
        })?;

        let mut consumer: PullConsumer = stream.get_consumer(consumer_name).await.map_err(|e| {
            warn!(stream = %self.stream_name, consumer = consumer_name, error = %e, "NATS queue-depth check failed to get consumer");
            format!("failed to get consumer '{consumer_name}': {e}")
        })?;

        let info = consumer.info().await.map_err(|e| {
            warn!(stream = %self.stream_name, consumer = consumer_name, error = %e, "NATS queue-depth check failed to get consumer info");
            format!("failed to get consumer info: {e}")
        })?;

        Ok(QueueDepth {
            code_pending: info.num_pending,
            code_in_flight: info.num_ack_pending as u64,
        })
    }
}
