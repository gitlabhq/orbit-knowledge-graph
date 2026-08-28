use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::stream::Stream;
use nats_client::NatsClient;
use orbit_server_config::NatsConfiguration;
use tokio::sync::OnceCell;
use tracing::warn;

use crate::types::QueueDepth;

pub struct WorkQueueConfig {
    pub nats: NatsConfiguration,
    pub stream_name: String,
    pub code_consumer_name: Option<String>,
    pub sdlc_consumer_name: Option<String>,
}

pub struct NatsDepthChecker {
    config: NatsConfiguration,
    stream_name: String,
    code_consumer_name: Option<String>,
    sdlc_consumer_name: Option<String>,
    client: OnceCell<NatsClient>,
}

struct ConsumerDepth {
    pending: u64,
    in_flight: u64,
}

impl NatsDepthChecker {
    pub fn new(work_queue: WorkQueueConfig) -> Self {
        Self {
            config: work_queue.nats,
            stream_name: work_queue.stream_name,
            code_consumer_name: work_queue.code_consumer_name,
            sdlc_consumer_name: work_queue.sdlc_consumer_name,
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

    async fn consumer_depth(
        &self,
        stream: &Stream,
        consumer_name: &str,
    ) -> Result<ConsumerDepth, String> {
        let mut consumer: PullConsumer = stream.get_consumer(consumer_name).await.map_err(|e| {
            warn!(stream = %self.stream_name, consumer = consumer_name, error = %e, "NATS queue-depth check failed to get consumer");
            format!("failed to get consumer '{consumer_name}': {e}")
        })?;

        let info = consumer.info().await.map_err(|e| {
            warn!(stream = %self.stream_name, consumer = consumer_name, error = %e, "NATS queue-depth check failed to get consumer info");
            format!("failed to get consumer info for '{consumer_name}': {e}")
        })?;

        Ok(ConsumerDepth {
            pending: info.num_pending,
            in_flight: info.num_ack_pending as u64,
        })
    }

    pub async fn check(&self) -> Result<QueueDepth, String> {
        let (code_consumer_name, sdlc_consumer_name) = self
            .code_consumer_name
            .as_deref()
            .zip(self.sdlc_consumer_name.as_deref())
            .ok_or_else(|| {
                "queue-depth endpoint is not configured: nats.consumer_name is unset".to_string()
            })?;

        let client = self.client().await?;

        let stream = client.get_stream(&self.stream_name).await.map_err(|e| {
            warn!(stream = %self.stream_name, error = %e, "NATS queue-depth check failed to get stream");
            format!("failed to get stream '{}': {e}", self.stream_name)
        })?;

        let (code, sdlc) = tokio::try_join!(
            self.consumer_depth(&stream, code_consumer_name),
            self.consumer_depth(&stream, sdlc_consumer_name),
        )?;

        Ok(QueueDepth {
            code_pending: code.pending,
            code_in_flight: code.in_flight,
            sdlc_pending: sdlc.pending,
            sdlc_in_flight: sdlc.in_flight,
        })
    }
}
