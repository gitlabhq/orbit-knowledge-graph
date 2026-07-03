use std::sync::Arc;
use std::time::Duration;

/// Flat 100ms hot-poll for the unbounded fetch supervisor loop.
const FETCH_RETRY: crate::engine::retry::RetryPolicy = crate::engine::retry::RetryPolicy {
    mode: crate::engine::retry::RetryMode::Local,
    backoff: crate::engine::retry::Backoff::Fixed(&[Duration::from_millis(100)]),
    max_attempts: u32::MAX, // unused by drive_forever; the loop is unbounded by design
    dead_letter: false,
};
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
use crate::engine::retry::{Loop, drive_forever};
use crate::metrics::EngineMetrics;
use crate::nats::versioning::NATS_VERSIONER;
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

        let mut managed_streams: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for subscription in subscriptions {
            if subscription.manage_stream {
                managed_streams
                    .entry(NATS_VERSIONER.stream(&subscription.stream))
                    .or_default()
                    .push(NATS_VERSIONER.subject(&subscription.subject));
            }
        }

        for (stream_name, subjects) in managed_streams {
            self.inner
                .create_or_update_stream(&stream_name, subjects, None)
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
        let subject = NATS_VERSIONER.subject(&format!("{}.>", DEAD_LETTER_SUBJECT_PREFIX));
        self.inner
            .create_or_update_stream(
                &NATS_VERSIONER.stream(DEAD_LETTER_STREAM),
                vec![subject],
                Some(DEAD_LETTER_MAX_AGE),
            )
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

        let subject = NATS_VERSIONER.subject(&dead_letter_subject(original_subscription, envelope));
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
        self.inner
            .ensure_kv_bucket_exists(&NATS_VERSIONER.bucket(bucket), config)
            .await
    }

    pub async fn kv_get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<nats_client::KvEntry>, NatsError> {
        self.inner.kv_get(&NATS_VERSIONER.bucket(bucket), key).await
    }

    pub async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: nats_client::KvPutOptions,
    ) -> Result<nats_client::KvPutResult, NatsError> {
        self.inner
            .kv_put(&NATS_VERSIONER.bucket(bucket), key, value, options)
            .await
    }

    pub async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        self.inner
            .kv_delete(&NATS_VERSIONER.bucket(bucket), key)
            .await
    }

    pub async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        self.inner.kv_keys(&NATS_VERSIONER.bucket(bucket)).await
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
        let (stream_name, subject) = NATS_VERSIONER.resolve_stream_and_subject(subscription);

        let ack_future = self
            .inner
            .jetstream()
            .publish(subject.clone(), envelope.payload.clone())
            .await
            .map_err(|e| {
                NatsError::Publish(format!(
                    "failed to publish to '{subject}' (stream '{stream_name}'): {e}",
                ))
            })?;

        match ack_future.await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == PublishErrorKind::Other && is_per_subject_limit_error(&e) => {
                Err(NatsError::PublishDuplicate)
            }
            Err(e) => Err(NatsError::Publish(format!(
                "publish ack failed for '{subject}' (stream '{stream_name}'): {e}",
            ))),
        }
    }

    pub async fn subscribe(
        &self,
        subscription: &Subscription,
        metrics: Arc<EngineMetrics>,
    ) -> Result<NatsSubscription, NatsError> {
        let (stream_name, subject) = NATS_VERSIONER.resolve_stream_and_subject(subscription);

        let stream = self.inner.get_stream(&stream_name).await?;
        let consumer = self
            .get_or_create_consumer(&stream, &subject, subscription.max_ack_pending)
            .await?;

        let consumer_type = match &self.config.consumer_name {
            Some(name) => format!("durable({})", name),
            None => "ephemeral".to_string(),
        };
        let batch_size = self.config.batch_size();
        let fetch_expires = self.config.fetch_expires();
        info!(
            topic = %format!("{stream_name}.{subject}"),
            consumer_type,
            batch_size,
            "subscription started"
        );

        let (sender, receiver) = tokio::sync::mpsc::channel(self.config.subscription_buffer_size());

        let cancel_token = self.cancellation_token.clone();

        let handle = tokio::spawn(async move {
            // Forward each fetch error downstream, back off, and run until cancel/consumer-close.
            drive_forever(&FETCH_RETRY, |_failures| {
                let consumer = &consumer;
                let sender = &sender;
                let metrics = &metrics;
                let cancel_token = &cancel_token;
                async move {
                    if cancel_token.is_cancelled() {
                        return Loop::Stop;
                    }

                    let fetch_start = std::time::Instant::now();
                    let batch = match consumer
                        .batch()
                        .max_messages(batch_size)
                        .expires(fetch_expires)
                        .messages()
                        .await
                    {
                        Ok(batch) => batch,
                        Err(e) => {
                            warn!(error = %e, "fetch batch error");
                            metrics.record_nats_fetch_duration(
                                fetch_start.elapsed().as_secs_f64(),
                                "error",
                            );
                            let _ = sender.send(Err(map_subscribe_error(e))).await;
                            return Loop::Backoff;
                        }
                    };
                    metrics
                        .record_nats_fetch_duration(fetch_start.elapsed().as_secs_f64(), "success");

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
                            return Loop::Stop;
                        }
                    }
                    debug!(count = batch_count, "batch fetched");
                    Loop::Continue
                }
            })
            .await;
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
        let (stream_name, filter_subject) = NATS_VERSIONER.resolve_stream_and_subject(subscription);

        let stream = self.inner.get_stream(&stream_name).await?;

        let durable_name = if subscription.manage_stream {
            format!("dispatch-{}", escape_subject_for_durable(&filter_subject))
        } else {
            format!(
                "dispatch-{}-{}",
                NATS_VERSIONER.tag(),
                escape_subject_for_durable(&filter_subject)
            )
        };

        let mut consumer_config = ConsumerConfig {
            filter_subject,
            ack_wait: self.config.ack_wait(),
            max_deliver: -1,
            durable_name: Some(durable_name.clone()),
            // ConsumerConfig::max_ack_pending uses 0 to mean "NATS server default" (currently 1000).
            max_ack_pending: max_ack_pending_to_i64(subscription.max_ack_pending),
            ..Default::default()
        };

        if !subscription.manage_stream {
            // Unmanaged (Siphon) streams: historical data comes from the datalake backfill,
            // not replay. If this consumer is auto-deleted after inactive_threshold and
            // recreated, events from the gap are skipped by design.
            consumer_config.deliver_policy = async_nats::jetstream::consumer::DeliverPolicy::New;
            consumer_config.inactive_threshold = self.config.consumer_inactive_threshold();
        }

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
        max_ack_pending: Option<u32>,
    ) -> Result<PullConsumer, NatsError> {
        let max_deliver = self.config.max_deliver.map(|n| n as i64).unwrap_or(-1);

        let durable_name = self
            .config
            .consumer_name
            .as_ref()
            .map(|base| format!("{base}-{}", escape_subject_for_durable(subject)));

        // A connected consumer's fetch loop keeps it active indefinitely; only
        // consumers of dead releases hit the threshold. Without it they persist
        // forever and their consumer_count keeps release GC from ever collecting
        // the stream. If reaped while the indexer is merely down, recreation
        // redelivers retained WorkQueue messages, so nothing is lost.
        let consumer_config = ConsumerConfig {
            filter_subject: subject.to_string(),
            ack_wait: self.config.ack_wait(),
            max_deliver,
            durable_name: durable_name.clone(),
            max_ack_pending: max_ack_pending_to_i64(max_ack_pending),
            inactive_threshold: self.config.consumer_inactive_threshold(),
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

#[async_trait::async_trait]
impl nats_client::KvServices for NatsBroker {
    async fn kv_get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<nats_client::KvEntry>, NatsError> {
        self.kv_get(bucket, key).await
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: nats_client::KvPutOptions,
    ) -> Result<nats_client::KvPutResult, NatsError> {
        self.kv_put(bucket, key, value, options).await
    }

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        self.kv_delete(bucket, key).await
    }

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        self.kv_keys(bucket).await
    }
}

fn max_ack_pending_to_i64(value: Option<u32>) -> i64 {
    value.map(i64::from).unwrap_or(0)
}

pub(crate) fn escape_subject_for_durable(subject: &str) -> String {
    subject
        .replace('.', "-")
        .replace('*', "wildcard")
        .replace('>', "deep")
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

#[cfg(test)]
mod tests {
    use super::{escape_subject_for_durable, max_ack_pending_to_i64};

    #[test]
    fn escapes_each_forbidden_character_distinctly() {
        assert_eq!(escape_subject_for_durable("a.b.c"), "a-b-c");
        assert_eq!(escape_subject_for_durable("a.*.c"), "a-wildcard-c");
        assert_eq!(escape_subject_for_durable("a.>"), "a-deep");
        assert_eq!(
            escape_subject_for_durable("sdlc.entity.indexing.requested.>"),
            "sdlc-entity-indexing-requested-deep",
        );
    }

    #[test]
    fn max_ack_pending_none_maps_to_server_default_sentinel() {
        assert_eq!(max_ack_pending_to_i64(None), 0);
        assert_eq!(max_ack_pending_to_i64(Some(0)), 0);
        assert_eq!(max_ack_pending_to_i64(Some(1)), 1);
        assert_eq!(max_ack_pending_to_i64(Some(2048)), 2048);
        assert_eq!(max_ack_pending_to_i64(Some(u32::MAX)), u32::MAX as i64);
    }

    #[test]
    fn escaped_output_is_a_legal_durable_name() {
        for subject in [
            "sdlc.entity.indexing.requested.>",
            "sdlc.namespace.indexing.requested.*.*",
            "code.task.indexing.requested.*.*",
            "sdlc.global.indexing.requested",
        ] {
            let escaped = escape_subject_for_durable(subject);
            assert!(
                !escaped.contains('.') && !escaped.contains('*') && !escaped.contains('>'),
                "escaped durable name '{escaped}' from '{subject}' still contains a forbidden char",
            );
        }
    }
}
