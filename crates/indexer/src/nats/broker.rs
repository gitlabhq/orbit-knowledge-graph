//! NATS JetStream message broker.
//!
//! Wraps [`nats_client::NatsClient`] and adds indexer-specific functionality:
//! subscriptions, publishing, dead-letter queues, and message conversion.

use std::sync::Arc;
use std::time::Duration;

const FETCH_RETRY_DELAY: Duration = Duration::from_millis(100);
const DEAD_LETTER_MAX_AGE: Duration = Duration::ZERO;

use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use bytes::Bytes;
use futures::StreamExt;
use nats_client::NatsClient;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use tracing::{debug, info, warn};

use crate::dead_letter::{
    DEAD_LETTER_STREAM, DEAD_LETTER_SUBJECT_PREFIX, DeadLetterEnvelope, dead_letter_subject,
};
use crate::metrics::EngineMetrics;
use crate::types::{Envelope, MessageId, Subscription};

use async_nats::jetstream::ErrorCode;
use async_nats::jetstream::context::{PublishError, PublishErrorKind};

use super::message::{NatsAcker, NatsMessage, NatsSubscription};
use gkg_server_config::NatsConfiguration;
use nats_client::NatsError;

fn map_subscribe_error<E: std::fmt::Display>(error: E) -> NatsError {
    NatsError::Subscribe(error.to_string())
}

pub struct NatsBroker {
    inner: Arc<NatsClient>,
    config: NatsConfiguration,
    subscription_handles: Mutex<Vec<JoinHandle<()>>>,
    cancellation_token: CancellationToken,
}

impl NatsBroker {
    pub async fn connect(config: &NatsConfiguration) -> Result<Self, NatsError> {
        let inner = NatsClient::connect(config).await?;
        Ok(Self {
            config: config.clone(),
            inner: Arc::new(inner),
            subscription_handles: Mutex::new(Vec::new()),
            cancellation_token: CancellationToken::new(),
        })
    }

    pub fn from_client(client: Arc<NatsClient>, config: &NatsConfiguration) -> Self {
        Self {
            inner: client,
            config: config.clone(),
            subscription_handles: Mutex::new(Vec::new()),
            cancellation_token: CancellationToken::new(),
        }
    }

    pub fn client(&self) -> &Arc<NatsClient> {
        &self.inner
    }

    pub async fn shutdown(self) {
        info!("broker shutdown initiated");
        self.cancellation_token.cancel();
        let handles: Vec<_> = self.subscription_handles.lock().drain(..).collect();
        for handle in handles {
            let _ = handle.await;
        }
    }

    pub fn nats_client(&self) -> &async_nats::Client {
        self.inner.nats_client()
    }

    pub async fn ensure_streams(&self, subscriptions: &[Subscription]) -> Result<(), NatsError> {
        self.ensure_managed_streams(subscriptions).await?;
        self.ensure_unmanaged_streams_exist(subscriptions).await?;
        Ok(())
    }

    pub(crate) async fn ensure_managed_streams(
        &self,
        subscriptions: &[Subscription],
    ) -> Result<(), NatsError> {
        if !self.config.auto_create_streams {
            return Ok(());
        }

        let mut managed_streams: std::collections::HashMap<&Arc<str>, Vec<String>> =
            std::collections::HashMap::new();

        for subscription in subscriptions {
            if subscription.manage_stream {
                managed_streams
                    .entry(&subscription.stream)
                    .or_default()
                    .push(subscription.subject.to_string());
            }
        }

        for (stream_name, subjects) in managed_streams {
            self.inner
                .create_or_update_stream(stream_name, subjects, None)
                .await?;
        }

        self.ensure_dead_letter_stream().await?;

        Ok(())
    }

    pub(crate) async fn ensure_unmanaged_streams_exist(
        &self,
        subscriptions: &[Subscription],
    ) -> Result<(), NatsError> {
        let unmanaged: Vec<&Arc<str>> = subscriptions
            .iter()
            .filter(|s| !s.manage_stream)
            .map(|s| &s.stream)
            .collect();

        for stream_name in unmanaged {
            self.inner.get_stream(stream_name).await?;
        }

        Ok(())
    }

    async fn ensure_dead_letter_stream(&self) -> Result<(), NatsError> {
        let subject = format!("{}.>", DEAD_LETTER_SUBJECT_PREFIX);
        self.inner
            .create_or_update_stream(DEAD_LETTER_STREAM, vec![subject], Some(DEAD_LETTER_MAX_AGE))
            .await?;
        Ok(())
    }

    pub async fn publish_dead_letter(
        &self,
        original_subscription: &Subscription,
        envelope: &Envelope,
        error: &str,
    ) -> Result<(), NatsError> {
        let dead_letter = DeadLetterEnvelope::new(original_subscription, envelope, error);

        let payload = serde_json::to_vec(&dead_letter)
            .map(Bytes::from)
            .map_err(|error| {
                NatsError::Publish(format!("failed to serialize dead letter: {error}"))
            })?;

        let subject = dead_letter_subject(original_subscription, envelope);
        let ack_future = self
            .inner
            .jetstream()
            .publish(subject.clone(), payload)
            .await
            .map_err(|error| {
                NatsError::Publish(format!(
                    "failed to publish dead letter to '{subject}': {error}"
                ))
            })?;

        ack_future.await.map_err(|error| {
            NatsError::Publish(format!(
                "dead letter publish ack failed for '{subject}': {error}"
            ))
        })?;

        Ok(())
    }

    pub async fn ensure_kv_bucket_exists(
        &self,
        bucket: &str,
        config: nats_client::KvBucketConfig,
    ) -> Result<(), NatsError> {
        self.inner.ensure_kv_bucket_exists(bucket, config).await
    }

    pub async fn kv_get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<nats_client::KvEntry>, NatsError> {
        self.inner.kv_get(bucket, key).await
    }

    pub async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: nats_client::KvPutOptions,
    ) -> Result<nats_client::KvPutResult, NatsError> {
        self.inner.kv_put(bucket, key, value, options).await
    }

    pub async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        self.inner.kv_delete(bucket, key).await
    }

    pub async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        self.inner.kv_keys(bucket).await
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
            subject: Arc::from(message_data.subject.as_str()),
            payload: message_data.payload,
            timestamp,
            attempt,
        };

        Ok(NatsMessage::new(envelope, NatsAcker(Arc::new(acker))))
    }

    pub async fn publish(
        &self,
        subscription: &Subscription,
        envelope: &Envelope,
    ) -> Result<(), NatsError> {
        let ack_future = self
            .inner
            .jetstream()
            .publish(subscription.subject.to_string(), envelope.payload.clone())
            .await
            .map_err(|e| {
                NatsError::Publish(format!(
                    "failed to publish to '{}' (stream '{}'): {e}",
                    subscription.subject, subscription.stream
                ))
            })?;

        match ack_future.await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == PublishErrorKind::Other && is_per_subject_limit_error(&e) => {
                Err(NatsError::PublishDuplicate)
            }
            Err(e) => Err(NatsError::Publish(format!(
                "publish ack failed for '{}' (stream '{}'): {e}",
                subscription.subject, subscription.stream
            ))),
        }
    }

    pub async fn subscribe(
        &self,
        subscription: &Subscription,
        metrics: Arc<EngineMetrics>,
    ) -> Result<NatsSubscription, NatsError> {
        let stream = self.inner.get_stream(&subscription.stream).await?;
        let consumer = self
            .get_or_create_consumer(&stream, &subscription.subject)
            .await?;

        let consumer_type = match &self.config.consumer_name {
            Some(name) => format!("durable({})", name),
            None => "ephemeral".to_string(),
        };
        let batch_size = self.config.batch_size();
        let fetch_expires = self.config.fetch_expires();
        info!(
            topic = %format!("{}.{}", subscription.stream, subscription.subject),
            consumer_type,
            batch_size,
            "subscription started"
        );

        let (sender, receiver) = tokio::sync::mpsc::channel(self.config.subscription_buffer_size());

        let cancel_token = self.cancellation_token.clone();

        let handle = tokio::spawn(async move {
            loop {
                if cancel_token.is_cancelled() {
                    break;
                }

                let fetch_start = std::time::Instant::now();
                let batch = match consumer.batch().max_messages(batch_size).expires(fetch_expires).messages().await {
                    Ok(batch) => batch,
                    Err(e) => {
                        warn!(error = %e, "fetch batch error");
                        metrics.record_nats_fetch_duration(
                            fetch_start.elapsed().as_secs_f64(),
                            "error",
                        );
                        let _ = sender.send(Err(map_subscribe_error(e))).await;
                        tokio::time::sleep(FETCH_RETRY_DELAY).await;
                        continue;
                    }
                };
                metrics.record_nats_fetch_duration(fetch_start.elapsed().as_secs_f64(), "success");

                tokio::pin!(batch);

                let mut batch_count: usize = 0;
                while let Some(result) = batch.next().await {
                    if cancel_token.is_cancelled() {
                        break;
                    }

                    batch_count += 1;
                    let converted = match result {
                        Ok(msg) => Self::convert_message(msg),
                        Err(e) => Err(map_subscribe_error(e)),
                    };

                    if sender.send(converted).await.is_err() {
                        return;
                    }
                }
                debug!(count = batch_count, "batch fetched");
            }
        });

        {
            let mut handles = self.subscription_handles.lock();
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }

        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    pub async fn consume_pending(
        &self,
        subscription: &Subscription,
        batch_size: usize,
    ) -> Result<Vec<NatsMessage>, NatsError> {
        let stream = self.inner.get_stream(&subscription.stream).await?;

        let durable_name = format!(
            "dispatch-{}",
            subscription
                .subject
                .replace('.', "-")
                .replace('*', "wildcard")
        );

        let consumer_config = ConsumerConfig {
            filter_subject: subscription.subject.to_string(),
            ack_wait: self.config.ack_wait(),
            max_deliver: -1,
            durable_name: Some(durable_name.clone()),
            ..Default::default()
        };

        let consumer = stream
            .get_or_create_consumer(&durable_name, consumer_config)
            .await
            .map_err(map_subscribe_error)?;

        let batch = consumer
            .fetch()
            .max_messages(batch_size)
            .expires(self.config.fetch_expires())
            .messages()
            .await
            .map_err(map_subscribe_error)?;

        tokio::pin!(batch);

        let mut messages = Vec::new();
        while let Some(result) = batch.next().await {
            let nats_message = result.map_err(map_subscribe_error)?;
            messages.push(Self::convert_message(nats_message)?);
        }

        Ok(messages)
    }

    async fn get_or_create_consumer(
        &self,
        stream: &async_nats::jetstream::stream::Stream,
        subject: &str,
    ) -> Result<PullConsumer, NatsError> {
        let max_deliver = self.config.max_deliver.map(|n| n as i64).unwrap_or(-1);

        let durable_name = self.config.consumer_name.as_ref().map(|base| {
            format!(
                "{base}-{}",
                subject.replace('.', "-").replace('*', "wildcard")
            )
        });

        let consumer_config = ConsumerConfig {
            filter_subject: subject.to_string(),
            ack_wait: self.config.ack_wait(),
            max_deliver,
            durable_name: durable_name.clone(),
            ..Default::default()
        };

        match &durable_name {
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
}

fn is_per_subject_limit_error(error: &PublishError) -> bool {
    use std::error::Error as _;
    let Some(source) = error.source() else {
        return false;
    };
    if let Some(api_error) = source.downcast_ref::<async_nats::jetstream::Error>() {
        return api_error.error_code() == ErrorCode::STREAM_STORE_FAILED;
    }
    false
}
