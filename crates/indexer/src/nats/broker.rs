//! NATS JetStream message broker.

use std::collections::HashMap;
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

use super::configuration::NatsConfiguration;
use super::error::{NatsError, map_connect_error, map_subscribe_error};
use super::kv_types::{KvBucketConfig, KvEntry, KvPutOptions, KvPutResult};
use super::message::{NatsMessage, NatsSubscription};

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
}

impl NatsBroker {
    pub async fn connect(config: &NatsConfiguration) -> Result<Self, NatsError> {
        let connect_options = Self::build_connect_options(config);

        let url = format!("nats://{}", config.url);
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
        })
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

    fn build_connect_options(config: &NatsConfiguration) -> async_nats::ConnectOptions {
        let mut options = async_nats::ConnectOptions::new()
            .connection_timeout(config.connection_timeout())
            .request_timeout(Some(config.request_timeout()));

        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            options = options.user_and_password(user.clone(), pass.clone());
        }

        options
    }

    pub async fn ensure_streams(&self, subscriptions: &[Subscription]) -> Result<(), NatsError> {
        if !self.config.auto_create_streams {
            return Ok(());
        }

        let mut managed_streams: HashMap<&Arc<str>, Vec<String>> = HashMap::new();
        let mut sourced_streams: Vec<&Subscription> = Vec::new();
        let mut unmanaged_streams: Vec<&Arc<str>> = Vec::new();

        for subscription in subscriptions {
            if !subscription.sources.is_empty() {
                sourced_streams.push(subscription);
            } else if subscription.manage_stream {
                managed_streams
                    .entry(&subscription.stream)
                    .or_default()
                    .push(subscription.subject.to_string());
            } else {
                unmanaged_streams.push(&subscription.stream);
            }
        }

        for (stream_name, subjects) in managed_streams {
            self.create_or_update_stream(stream_name, subjects).await?;
        }

        for subscription in sourced_streams {
            self.create_sourced_stream(subscription).await?;
        }

        for stream_name in unmanaged_streams {
            self.get_stream(stream_name).await?;
        }

        self.ensure_dead_letter_stream().await?;

        Ok(())
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

    async fn create_sourced_stream(
        &self,
        subscription: &Subscription,
    ) -> Result<Stream, NatsError> {
        for source in &subscription.sources {
            if source.manage_stream {
                self.create_or_update_stream(&source.stream, vec![source.subject.to_string()])
                    .await?;
            } else {
                self.get_stream(&source.stream).await?;
            }
        }

        let stream_name = &subscription.stream;
        let nats_sources: Vec<async_nats::jetstream::stream::Source> = subscription
            .sources
            .iter()
            .map(|s| async_nats::jetstream::stream::Source {
                name: s.stream.to_string(),
                filter_subject: Some(s.subject.to_string()),
                ..Default::default()
            })
            .collect();

        let source_names: Vec<&str> = subscription
            .sources
            .iter()
            .map(|s| s.stream.as_ref())
            .collect();

        let stream_config = async_nats::jetstream::stream::Config {
            name: stream_name.to_string(),
            sources: Some(nats_sources),
            num_replicas: self.config.stream_replicas,
            max_age: self.config.stream_max_age().unwrap_or_default(),
            max_bytes: self.config.stream_max_bytes.unwrap_or(-1),
            max_messages: self.config.stream_max_messages.unwrap_or(-1),
            storage: async_nats::jetstream::stream::StorageType::File,
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
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

        info!(
            stream = %stream_name,
            sources = ?source_names,
            "sourced stream created or updated"
        );

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
                subject
                    .replace('.', "-")
                    .replace('*', "wildcard")
                    .replace('>', "all")
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

        Ok(NatsMessage {
            envelope,
            acker: Arc::new(acker),
        })
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
