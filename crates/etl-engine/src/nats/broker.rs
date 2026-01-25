//! NATS JetStream message broker.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const FETCH_RETRY_DELAY: Duration = Duration::from_millis(100);

use async_nats::jetstream::Context;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use async_nats::jetstream::stream::Stream;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::types::{Envelope, MessageId, Topic};

use super::configuration::NatsConfiguration;
use super::error::{NatsError, map_connect_error, map_subscribe_error};
use super::message::{NatsMessage, NatsSubscription};

/// NATS JetStream message broker.
///
/// See the [module docs](super) for examples.
///
/// Call [`shutdown`](Self::shutdown) for graceful termination of subscription tasks.
pub struct NatsBroker {
    jetstream: Context,
    config: NatsConfiguration,
    streams: RwLock<HashMap<Arc<str>, Stream>>,
    subscription_handles: Mutex<Vec<JoinHandle<()>>>,
    cancellation_token: CancellationToken,
}

impl NatsBroker {
    pub async fn connect(config: &NatsConfiguration) -> Result<Self, NatsError> {
        let connect_options = Self::build_connect_options(config);

        let url = format!("nats://{}", config.url);
        let client = async_nats::connect_with_options(&url, connect_options)
            .await
            .map_err(map_connect_error)?;

        let jetstream = async_nats::jetstream::new(client);

        Ok(Self {
            jetstream,
            config: config.clone(),
            streams: RwLock::new(HashMap::new()),
            subscription_handles: Mutex::new(Vec::new()),
            cancellation_token: CancellationToken::new(),
        })
    }

    pub async fn shutdown(self) {
        self.cancellation_token.cancel();
        let handles: Vec<_> = self.subscription_handles.lock().drain(..).collect();
        for handle in handles {
            let _ = handle.await;
        }
    }

    fn build_connect_options(config: &NatsConfiguration) -> async_nats::ConnectOptions {
        let mut options = async_nats::ConnectOptions::new()
            .connection_timeout(config.connection_timeout())
            .request_timeout(Some(config.request_timeout()));

        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            options = options.user_and_password(user.clone(), pass.clone());
        }

        options
    }

    async fn get_stream(&self, stream_name: &Arc<str>) -> Result<Stream, NatsError> {
        {
            let cache = self.streams.read().await;
            if let Some(stream) = cache.get(stream_name) {
                return Ok(stream.clone());
            }
        }

        let mut cache = self.streams.write().await;
        if let Some(stream) = cache.get(stream_name) {
            return Ok(stream.clone());
        }

        let stream = self
            .jetstream
            .get_stream(stream_name.as_ref())
            .await
            .map_err(|e| NatsError::StreamNotFound {
                stream: stream_name.to_string(),
                source: e,
            })?;

        cache.insert(stream_name.clone(), stream.clone());
        Ok(stream)
    }

    async fn get_or_create_consumer(
        &self,
        stream: &Stream,
        subject: &str,
    ) -> Result<PullConsumer, NatsError> {
        let max_deliver = self.config.max_deliver.map(|n| n as i64).unwrap_or(-1);

        let consumer_config = ConsumerConfig {
            filter_subject: subject.to_string(),
            ack_wait: self.config.ack_wait(),
            max_deliver,
            durable_name: self.config.consumer_name.clone(),
            ..Default::default()
        };

        match &self.config.consumer_name {
            Some(name) => stream
                .get_or_create_consumer(name, consumer_config)
                .await
                .map_err(map_subscribe_error),
            None => stream
                .create_consumer(consumer_config)
                .await
                .map_err(map_subscribe_error),
        }
    }

    fn convert_message(
        nats_message: async_nats::jetstream::message::Message,
    ) -> Result<NatsMessage, NatsError> {
        let message_info = nats_message
            .info()
            .map_err(|e| NatsError::Subscribe(format!("failed to get message info: {e}")))?;

        let message_id = format!(
            "{}.{}.{}",
            message_info.stream, message_info.stream_sequence, message_info.consumer_sequence
        );

        let attempt = message_info.delivered as u32;
        let timestamp = chrono::DateTime::from_timestamp(
            message_info.published.unix_timestamp(),
            message_info.published.nanosecond(),
        )
        .unwrap_or_else(chrono::Utc::now);

        let (message_data, acker) = nats_message.split();

        let envelope = Envelope {
            id: MessageId(Arc::from(message_id)),
            payload: message_data.payload,
            timestamp,
            attempt,
        };

        Ok(NatsMessage { envelope, acker })
    }

    pub async fn publish(&self, topic: &Topic, envelope: &Envelope) -> Result<(), NatsError> {
        self.jetstream
            .publish(topic.subject.to_string(), envelope.payload.clone())
            .await
            .map_err(|e| {
                NatsError::Publish(format!(
                    "failed to publish to '{}' (stream '{}'): {e}",
                    topic.subject, topic.stream
                ))
            })?
            .await
            .map_err(|e| {
                NatsError::Publish(format!(
                    "publish ack failed for '{}' (stream '{}'): {e}",
                    topic.subject, topic.stream
                ))
            })?;

        Ok(())
    }

    pub async fn subscribe(&self, topic: &Topic) -> Result<NatsSubscription, NatsError> {
        let stream = self.get_stream(&topic.stream).await?;
        let consumer = self.get_or_create_consumer(&stream, &topic.subject).await?;

        let (sender, receiver) = tokio::sync::mpsc::channel(self.config.subscription_buffer_size());

        let cancel_token = self.cancellation_token.clone();
        let batch_size = self.config.batch_size();

        let handle = tokio::spawn(async move {
            loop {
                if cancel_token.is_cancelled() {
                    break;
                }

                let batch = match consumer.fetch().max_messages(batch_size).messages().await {
                    Ok(batch) => batch,
                    Err(e) => {
                        let _ = sender.send(Err(map_subscribe_error(e))).await;
                        tokio::time::sleep(FETCH_RETRY_DELAY).await;
                        continue;
                    }
                };

                tokio::pin!(batch);

                while let Some(result) = batch.next().await {
                    if cancel_token.is_cancelled() {
                        break;
                    }

                    let converted = match result {
                        Ok(msg) => Self::convert_message(msg),
                        Err(e) => Err(map_subscribe_error(e)),
                    };

                    if sender.send(converted).await.is_err() {
                        return;
                    }
                }
            }
        });

        {
            let mut handles = self.subscription_handles.lock();
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }

        Ok(Box::pin(ReceiverStream::new(receiver)))
    }
}
