//! NATS JetStream message broker.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const FETCH_RETRY_DELAY: Duration = Duration::from_millis(100);

use async_nats::jetstream::Context;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use async_nats::jetstream::stream::Stream;
use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::message_broker::{
    BrokerError, Envelope, Message, MessageBroker, MessageId, Subscription,
};

use super::ack_handle::NatsAckHandle;
use super::configuration::NatsConfiguration;
use super::error::{map_connect_error, map_subscribe_error};

/// NATS JetStream message broker.
///
/// Topics are `stream:subject`. See the [module docs](super) for examples.
///
/// Call [`shutdown`](Self::shutdown) for graceful termination of subscription tasks.
pub struct NatsBroker {
    jetstream: Context,
    config: NatsConfiguration,
    streams: RwLock<HashMap<String, Stream>>,
    subscription_handles: Mutex<Vec<JoinHandle<()>>>,
    cancellation_token: CancellationToken,
}

impl NatsBroker {
    pub async fn connect(config: &NatsConfiguration) -> Result<Self, BrokerError> {
        let connect_options = Self::build_connect_options(config)?;

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

    /// Stops all subscription tasks and waits for them to finish.
    pub async fn shutdown(self) {
        self.cancellation_token.cancel();
        let handles: Vec<_> = self.subscription_handles.lock().drain(..).collect();
        for handle in handles {
            let _ = handle.await;
        }
    }

    fn build_connect_options(
        config: &NatsConfiguration,
    ) -> Result<async_nats::ConnectOptions, BrokerError> {
        let mut options = async_nats::ConnectOptions::new()
            .connection_timeout(config.connection_timeout())
            .request_timeout(Some(config.request_timeout()));

        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            options = options.user_and_password(user.clone(), pass.clone());
        }

        Ok(options)
    }

    /// Split "stream:subject" into parts. Errors if the format is not valid.
    fn parse_topic(topic: &str) -> Result<(&str, &str), BrokerError> {
        topic.split_once(':').ok_or_else(|| {
            BrokerError::Subscribe(format!(
                "invalid topic '{topic}': expected 'stream:subject'"
            ))
        })
    }

    async fn get_stream(&self, stream_name: &str) -> Result<Stream, BrokerError> {
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

        let stream = self.jetstream.get_stream(stream_name).await.map_err(|e| {
            BrokerError::Connection(format!("failed to get stream '{stream_name}': {e}"))
        })?;

        cache.insert(stream_name.to_string(), stream.clone());
        Ok(stream)
    }

    async fn get_or_create_consumer(
        &self,
        stream: &Stream,
        subject: &str,
    ) -> Result<PullConsumer, BrokerError> {
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
    ) -> Result<Message, BrokerError> {
        let message_info = nats_message
            .info()
            .map_err(|e| BrokerError::Subscribe(format!("failed to get message info: {e}")))?;

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

        let ack_handle = Box::new(NatsAckHandle::new(acker));
        Ok(Message::new(envelope, ack_handle))
    }
}

#[async_trait]
impl MessageBroker for NatsBroker {
    async fn publish(&self, topic: &str, envelope: Envelope) -> Result<(), BrokerError> {
        let (stream_name, subject) = Self::parse_topic(topic)?;

        self.jetstream
            .publish(subject.to_string(), envelope.payload)
            .await
            .map_err(|e| {
                BrokerError::Publish(format!(
                    "failed to publish to '{subject}' (stream '{stream_name}'): {e}"
                ))
            })?
            .await
            .map_err(|e| {
                BrokerError::Publish(format!(
                    "publish ack failed for '{subject}' (stream '{stream_name}'): {e}"
                ))
            })?;

        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<Subscription, BrokerError> {
        let (stream_name, subject) = Self::parse_topic(topic)?;

        let stream = self.get_stream(stream_name).await?;
        let consumer = self.get_or_create_consumer(&stream, subject).await?;

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

        // Track handle for shutdown(). Lock needed since subscribe() can be called
        // concurrently (engine subscribes to multiple topics in parallel).
        {
            let mut handles = self.subscription_handles.lock();
            handles.retain(|h| !h.is_finished()); // Prune finished to avoid unbounded growth
            handles.push(handle);
        }

        Ok(Box::pin(ReceiverStream::new(receiver)))
    }
}
