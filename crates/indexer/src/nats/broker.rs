//! NATS JetStream message broker.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const FETCH_RETRY_DELAY: Duration = Duration::from_millis(100);

use async_nats::jetstream::Context;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use async_nats::jetstream::kv::{CreateErrorKind, Store as KvStore, UpdateErrorKind};
use async_nats::jetstream::stream::Stream;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;
use tokio::sync::RwLock;
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

use super::error::{NatsError, map_connect_error, map_subscribe_error};
use super::kv_types::{KvBucketConfig, KvEntry, KvPutOptions, KvPutResult};
use super::message::{NatsAcker, NatsMessage, NatsSubscription};
use gkg_server_config::NatsConfiguration;

/// NATS JetStream message broker.
///
/// See the [module docs](super) for examples.
///
/// Call [`shutdown`](Self::shutdown) for graceful termination of subscription tasks.
pub struct NatsBroker {
    client: async_nats::Client,
    jetstream: Context,
    config: NatsConfiguration,
    streams: RwLock<HashMap<Arc<str>, Stream>>,
    kv_stores: RwLock<HashMap<String, KvStore>>,
    subscription_handles: Mutex<Vec<JoinHandle<()>>>,
    cancellation_token: CancellationToken,
    metrics: RwLock<Option<Arc<EngineMetrics>>>,
}

impl NatsBroker {
    pub async fn connect(config: &NatsConfiguration) -> Result<Self, NatsError> {
        config
            .validate_tls_config()
            .map_err(NatsError::Connection)?;

        let connect_options = Self::build_connect_options(config);

        let url = config.connection_url();
        let client = async_nats::connect_with_options(&url, connect_options)
            .await
            .map_err(map_connect_error)?;

        let jetstream = async_nats::jetstream::new(client.clone());

        Ok(Self {
            client,
            jetstream,
            config: config.clone(),
            streams: RwLock::new(HashMap::new()),
            kv_stores: RwLock::new(HashMap::new()),
            subscription_handles: Mutex::new(Vec::new()),
            cancellation_token: CancellationToken::new(),
            metrics: RwLock::new(None),
        })
    }

    /// Attaches metrics so broker operations can record NATS errors by
    /// operation/kind/transient. Called once by the engine at startup.
    pub async fn set_metrics(&self, metrics: Arc<EngineMetrics>) {
        *self.metrics.write().await = Some(metrics);
    }

    async fn record_error(&self, operation: &'static str, error: &NatsError) {
        if let Some(metrics) = self.metrics.read().await.as_ref() {
            metrics.record_nats_error(operation, error);
        }
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
        &self.client
    }

    /// Builds connect options. Must be called after `validate_tls_config()`.
    fn build_connect_options(config: &NatsConfiguration) -> async_nats::ConnectOptions {
        let mut options = async_nats::ConnectOptions::new()
            .connection_timeout(config.connection_timeout())
            .request_timeout(Some(config.request_timeout()));

        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            options = options.user_and_password(user.clone(), pass.clone());
        }

        if config.tls_enabled() {
            options = options.require_tls(true);
        }

        if let Some(ca_path) = &config.tls_ca_cert_path {
            options = options.add_root_certificates(PathBuf::from(ca_path));
        }

        if let (Some(cert), Some(key)) = (&config.tls_cert_path, &config.tls_key_path) {
            options = options.add_client_certificate(PathBuf::from(cert), PathBuf::from(key));
        }

        options
    }

    pub async fn ensure_streams(&self, subscriptions: &[Subscription]) -> Result<(), NatsError> {
        if !self.config.auto_create_streams {
            return Ok(());
        }

        let mut managed_streams: HashMap<&Arc<str>, Vec<String>> = HashMap::new();
        let mut unmanaged_streams: Vec<&Arc<str>> = Vec::new();

        for subscription in subscriptions {
            if subscription.manage_stream {
                managed_streams
                    .entry(&subscription.stream)
                    .or_default()
                    .push(subscription.subject.to_string());
            } else {
                unmanaged_streams.push(&subscription.stream);
            }
        }

        for (stream_name, subjects) in managed_streams {
            let subjects_clone = subjects.clone();
            self.retry_transient("stream_create", || {
                let subjects = subjects_clone.clone();
                async move { self.create_or_update_stream(stream_name, subjects).await }
            })
            .await?;
        }

        for stream_name in unmanaged_streams {
            self.retry_transient("stream_get", || async move {
                self.get_stream(stream_name).await
            })
            .await?;
        }

        self.retry_transient("stream_create", || async {
            self.ensure_dead_letter_stream().await
        })
        .await?;

        Ok(())
    }

    /// Runs an async NATS op with exponential backoff on transient errors.
    ///
    /// Cap is `startup_retry_max_attempts`; each attempt delay doubles up to
    /// `startup_retry_max_delay`. Non-transient errors abort immediately.
    /// Every attempt's error is counted via the `nats.errors` metric; only the
    /// final error is logged at ERROR, intermediate attempts at WARN.
    #[doc(hidden)]
    pub async fn retry_transient<F, Fut, T>(
        &self,
        operation: &'static str,
        mut op: F,
    ) -> Result<T, NatsError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, NatsError>>,
    {
        let max_attempts = self.config.startup_retry_max_attempts.max(1);
        let initial_delay = self.config.startup_retry_initial_delay();
        let max_delay = self.config.startup_retry_max_delay();
        let mut delay = initial_delay;

        for attempt in 1..=max_attempts {
            match op().await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    self.record_error(operation, &error).await;
                    let transient = error.is_transient();
                    let is_final = attempt == max_attempts || !transient;
                    if is_final {
                        tracing::error!(
                            operation,
                            attempt,
                            transient,
                            error = %error,
                            "NATS operation failed"
                        );
                        return Err(error);
                    }
                    let jittered = apply_jitter(delay);
                    warn!(
                        operation,
                        attempt,
                        max_attempts,
                        backoff_ms = jittered.as_millis() as u64,
                        error = %error,
                        "transient NATS error; retrying"
                    );
                    tokio::time::sleep(jittered).await;
                    delay = (delay * 2).min(max_delay);
                }
            }
        }
        // Unreachable: the loop always returns on the last attempt.
        unreachable!("retry loop exited without returning")
    }

    async fn ensure_dead_letter_stream(&self) -> Result<(), NatsError> {
        let stream_name: Arc<str> = Arc::from(DEAD_LETTER_STREAM);
        let subject = format!("{}.>", DEAD_LETTER_SUBJECT_PREFIX);
        self.create_or_update_stream(&stream_name, vec![subject])
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

        let subject = dead_letter_subject(original_subscription);
        self.jetstream
            .publish(subject.clone(), payload)
            .await
            .map_err(|error| {
                NatsError::Publish(format!(
                    "failed to publish dead letter to '{subject}': {error}"
                ))
            })?;

        Ok(())
    }

    pub async fn ensure_kv_bucket_exists(
        &self,
        bucket: &str,
        config: KvBucketConfig,
    ) -> Result<(), NatsError> {
        let kv_config = async_nats::jetstream::kv::Config {
            bucket: bucket.to_string(),
            limit_markers: config.limit_markers,
            ..Default::default()
        };

        let store = self
            .jetstream
            .create_key_value(kv_config)
            .await
            .map_err(|e| NatsError::KvBucket {
                bucket: bucket.to_string(),
                message: e.to_string(),
            })?;

        info!(bucket, "KV bucket ready");

        let mut cache = self.kv_stores.write().await;
        cache.insert(bucket.to_string(), store);
        Ok(())
    }

    async fn create_or_update_stream(
        &self,
        stream_name: &Arc<str>,
        subjects: Vec<String>,
    ) -> Result<Stream, NatsError> {
        let stream_config = async_nats::jetstream::stream::Config {
            name: stream_name.to_string(),
            subjects: subjects.clone(),
            num_replicas: self.config.stream_replicas,
            max_age: self.config.stream_max_age().unwrap_or_default(),
            max_bytes: self.config.stream_max_bytes.unwrap_or(-1),
            max_messages: self.config.stream_max_messages.unwrap_or(-1),
            max_messages_per_subject: 1,
            storage: async_nats::jetstream::stream::StorageType::File,
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            discard: async_nats::jetstream::stream::DiscardPolicy::New,
            discard_new_per_subject: true,
            ..Default::default()
        };

        self.jetstream
            .create_or_update_stream(stream_config)
            .await
            .map_err(|e| NatsError::StreamCreationFailed {
                stream: stream_name.to_string(),
                source: e,
            })?;

        let stream = self
            .jetstream
            .get_stream(stream_name.as_ref())
            .await
            .map_err(|e| NatsError::StreamNotFound {
                stream: stream_name.to_string(),
                source: e,
            })?;

        info!(stream = %stream_name, ?subjects, "stream created or updated");

        let mut cache = self.streams.write().await;
        cache.insert(stream_name.clone(), stream.clone());
        Ok(stream)
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
            .jetstream
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
        let stream = self.get_stream(&subscription.stream).await?;
        let consumer = self
            .get_or_create_consumer(&stream, &subscription.subject)
            .await?;

        let consumer_type = match &self.config.consumer_name {
            Some(name) => format!("durable({})", name),
            None => "ephemeral".to_string(),
        };
        let batch_size = self.config.batch_size();
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
                let batch = match consumer.fetch().max_messages(batch_size).messages().await {
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
        let stream = self.get_stream(&subscription.stream).await?;

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

    async fn get_or_create_kv_store(&self, bucket: &str) -> Result<KvStore, NatsError> {
        {
            let cache = self.kv_stores.read().await;
            if let Some(store) = cache.get(bucket) {
                return Ok(store.clone());
            }
        }

        let mut cache = self.kv_stores.write().await;
        if let Some(store) = cache.get(bucket) {
            return Ok(store.clone());
        }

        let store = match self.jetstream.get_key_value(bucket).await {
            Ok(store) => store,
            Err(_) => self
                .jetstream
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: bucket.to_string(),
                    ..Default::default()
                })
                .await
                .map_err(|e| NatsError::KvBucket {
                    bucket: bucket.to_string(),
                    message: e.to_string(),
                })?,
        };

        cache.insert(bucket.to_string(), store.clone());
        Ok(store)
    }

    pub async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        let store = self.get_or_create_kv_store(bucket).await?;

        match store.entry(key).await {
            Ok(Some(entry)) => Ok(Some(KvEntry {
                key: entry.key,
                value: entry.value,
                revision: entry.revision,
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(NatsError::KvGet {
                bucket: bucket.to_string(),
                key: key.to_string(),
                message: e.to_string(),
            }),
        }
    }

    pub async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        let store = self.get_or_create_kv_store(bucket).await?;

        if options.create_only {
            let result = if let Some(ttl) = options.ttl {
                store.create_with_ttl(key, value, ttl).await
            } else {
                store.create(key, value).await
            };

            return match result {
                Ok(revision) => Ok(KvPutResult::Success(revision)),
                Err(e) if e.kind() == CreateErrorKind::AlreadyExists => {
                    Ok(KvPutResult::AlreadyExists)
                }
                Err(e) => Err(NatsError::KvPut {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    message: e.to_string(),
                }),
            };
        }

        // Optimistic concurrency control using the expected revision
        if let Some(rev) = options.expected_revision {
            let result = store.update(key, value, rev).await;
            return match result {
                Ok(revision) => Ok(KvPutResult::Success(revision)),
                Err(e) if e.kind() == UpdateErrorKind::WrongLastRevision => {
                    Ok(KvPutResult::RevisionMismatch)
                }
                Err(e) => Err(NatsError::KvPut {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    message: e.to_string(),
                }),
            };
        }
        let result = store.put(key, value).await;
        match result {
            Ok(revision) => Ok(KvPutResult::Success(revision)),
            Err(e) => Err(NatsError::KvPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                message: e.to_string(),
            }),
        }
    }

    pub async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        let store = self.get_or_create_kv_store(bucket).await?;

        store.delete(key).await.map_err(|e| NatsError::KvDelete {
            bucket: bucket.to_string(),
            key: key.to_string(),
            message: e.to_string(),
        })
    }

    pub async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        let store = self.get_or_create_kv_store(bucket).await?;

        let keys = store.keys().await.map_err(|e| NatsError::KvKeys {
            bucket: bucket.to_string(),
            message: e.to_string(),
        })?;

        let result: Result<Vec<String>, _> = keys.try_collect().await;
        result.map_err(|e| NatsError::KvKeys {
            bucket: bucket.to_string(),
            message: e.to_string(),
        })
    }
}

/// Applies ±25% jitter to a retry delay using a cheap PRNG seeded from the
/// system clock. Keeps callers using the same broker from synchronising their
/// retry storms against the upstream.
fn apply_jitter(delay: Duration) -> Duration {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    // Fraction in [-0.25, 0.25) based on a nanosecond-scale source.
    let frac = (nanos as f64 / 1_000_000_000.0 - 0.5) * 0.5;
    let base = delay.as_millis() as f64;
    let jittered = (base * (1.0 + frac)).max(1.0);
    Duration::from_millis(jittered as u64)
}

/// async_nats has no typed variant for per-subject limit rejection; it falls through to PublishErrorKind::Other.
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
mod retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn transient_error() -> NatsError {
        NatsError::Subscribe("503, None".into())
    }

    fn terminal_error() -> NatsError {
        NatsError::Subscribe("permission denied".into())
    }

    fn fast_retry_config() -> NatsConfiguration {
        NatsConfiguration {
            url: "localhost:4222".into(),
            startup_retry_max_attempts: 4,
            startup_retry_initial_delay_ms: 1,
            startup_retry_max_delay_secs: 1,
            ..Default::default()
        }
    }

    // Extracted retry algorithm for pure tests. Mirrors the method body on
    // `NatsBroker::retry_transient` so the algorithm can be exercised
    // without a live NATS client (async_nats has no no-op constructor).
    async fn retry_loop<F, Fut, T>(
        config: &NatsConfiguration,
        mut op: F,
    ) -> (Result<T, NatsError>, u32)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, NatsError>>,
    {
        let max_attempts = config.startup_retry_max_attempts.max(1);
        let initial_delay = config.startup_retry_initial_delay();
        let max_delay = config.startup_retry_max_delay();
        let mut delay = initial_delay;
        for attempt in 1..=max_attempts {
            match op().await {
                Ok(v) => return (Ok(v), attempt),
                Err(e) => {
                    if attempt == max_attempts || !e.is_transient() {
                        return (Err(e), attempt);
                    }
                    tokio::time::sleep(apply_jitter(delay)).await;
                    delay = (delay * 2).min(max_delay);
                }
            }
        }
        unreachable!()
    }

    #[tokio::test]
    async fn retries_transient_until_success() {
        let config = fast_retry_config();
        let attempts = AtomicU32::new(0);
        let (result, count) = retry_loop::<_, _, ()>(&config, || {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(transient_error())
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts_on_persistent_transient() {
        let config = fast_retry_config();
        let (result, count) =
            retry_loop::<_, _, ()>(&config, || async { Err::<(), _>(transient_error()) }).await;
        assert!(result.is_err());
        assert_eq!(count, config.startup_retry_max_attempts);
    }

    #[tokio::test]
    async fn fails_fast_on_terminal_error() {
        let config = fast_retry_config();
        let (result, count) =
            retry_loop::<_, _, ()>(&config, || async { Err::<(), _>(terminal_error()) }).await;
        assert!(result.is_err());
        assert_eq!(count, 1);
    }
}
