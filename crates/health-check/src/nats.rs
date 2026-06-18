use async_nats::jetstream::consumer::PullConsumer;
use gkg_server_config::NatsConfiguration;
use nats_client::NatsClient;

use crate::types::QueueDepth;

pub struct NatsDepthChecker {
    client: NatsClient,
    stream_name: String,
    consumer_name: String,
}

impl NatsDepthChecker {
    pub async fn new(
        config: &NatsConfiguration,
        stream_name: String,
        consumer_name: String,
    ) -> Result<Self, nats_client::NatsError> {
        let client = NatsClient::connect(config).await?;
        Ok(Self {
            client,
            stream_name,
            consumer_name,
        })
    }

    pub async fn check(&self) -> Result<QueueDepth, String> {
        let stream = self
            .client
            .get_stream(&self.stream_name)
            .await
            .map_err(|e| format!("failed to get stream '{}': {e}", self.stream_name))?;

        let mut consumer: PullConsumer = stream
            .get_consumer(&self.consumer_name)
            .await
            .map_err(|e| format!("failed to get consumer '{}': {e}", self.consumer_name))?;

        let info = consumer
            .info()
            .await
            .map_err(|e| format!("failed to get consumer info: {e}"))?;

        Ok(QueueDepth {
            code_pending: info.num_pending,
            code_in_flight: info.num_ack_pending as u64,
        })
    }
}
