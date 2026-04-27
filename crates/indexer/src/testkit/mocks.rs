//! Mock implementations for testing.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use nats_client::testkit::MockKvServices;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::destination::{BatchWriter, Destination, DestinationError};
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::locking::{LockError, LockService};
use crate::nats::{
    KvEntry, KvPutOptions, KvPutResult, NatsError, NatsMessage, NatsServices, NoopAcker,
};
use crate::types::{Envelope, MessageId, Subscription};
use gkg_server_config::HandlerConfiguration;

#[derive(Clone, Default)]
pub struct MockNatsServices {
    published: Arc<Mutex<Vec<(Subscription, Envelope)>>>,
    pending_messages: Arc<Mutex<Vec<Envelope>>>,
    kv: MockKvServices,
}

impl MockNatsServices {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_published(&self) -> Vec<(Subscription, Envelope)> {
        self.published.lock().clone()
    }

    pub fn get_kv(&self, bucket: &str, key: &str) -> Option<Bytes> {
        self.kv.get(bucket, key)
    }

    pub fn add_pending_message(&self, envelope: Envelope) {
        self.pending_messages.lock().push(envelope);
    }

    pub fn set_kv(&self, bucket: &str, key: &str, value: Bytes) {
        self.kv.set(bucket, key, value);
    }
}

#[async_trait]
impl NatsServices for MockNatsServices {
    async fn publish(
        &self,
        subscription: &Subscription,
        envelope: &Envelope,
    ) -> Result<(), NatsError> {
        self.published
            .lock()
            .push((subscription.clone(), envelope.clone()));
        Ok(())
    }

    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        nats_client::KvServices::kv_get(&self.kv, bucket, key).await
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        nats_client::KvServices::kv_put(&self.kv, bucket, key, value, options).await
    }

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        nats_client::KvServices::kv_delete(&self.kv, bucket, key).await
    }

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        nats_client::KvServices::kv_keys(&self.kv, bucket).await
    }

    async fn consume_pending(
        &self,
        _subscription: &Subscription,
        _batch_size: usize,
    ) -> Result<Vec<NatsMessage>, NatsError> {
        let envelopes: Vec<Envelope> = self.pending_messages.lock().drain(..).collect();
        let messages = envelopes
            .into_iter()
            .map(|envelope| NatsMessage::new(envelope, NoopAcker))
            .collect();
        Ok(messages)
    }
}

#[async_trait]
impl nats_client::KvServices for MockNatsServices {
    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        nats_client::KvServices::kv_get(&self.kv, bucket, key).await
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        nats_client::KvServices::kv_put(&self.kv, bucket, key, value, options).await
    }

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        nats_client::KvServices::kv_delete(&self.kv, bucket, key).await
    }

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        nats_client::KvServices::kv_keys(&self.kv, bucket).await
    }
}

/// Mock destination for testing.
pub struct MockDestination {
    batch_writes: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
}

impl MockDestination {
    pub fn new() -> Self {
        Self {
            batch_writes: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Default for MockDestination {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Destination for MockDestination {
    async fn new_batch_writer(
        &self,
        _table: &str,
    ) -> Result<Box<dyn BatchWriter>, DestinationError> {
        Ok(Box::new(MockBatchWriter {
            writes: self.batch_writes.clone(),
        }))
    }
}

pub struct MockBatchWriter {
    writes: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
}

#[async_trait]
impl BatchWriter for MockBatchWriter {
    async fn write_batch(&self, batch: &[RecordBatch]) -> Result<(), DestinationError> {
        self.writes.lock().push(batch.to_vec());
        Ok(())
    }
}

/// Mock handler for testing.
pub struct MockHandler {
    name: String,
    subscription: Subscription,
    delay: Option<Duration>,
    error: Option<HandlerError>,
    engine_config: HandlerConfiguration,
    invocations: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<Envelope>>>,
}

impl MockHandler {
    pub fn new(stream: &'static str, subject: &'static str) -> Self {
        Self {
            name: format!("mock-handler-{}:{}", stream, subject),
            subscription: Subscription::new(stream, subject),
            delay: None,
            error: None,
            engine_config: HandlerConfiguration::default(),
            invocations: Arc::new(AtomicUsize::new(0)),
            received: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn with_error(mut self, error: HandlerError) -> Self {
        self.error = Some(error);
        self
    }

    pub fn with_engine_config(mut self, config: HandlerConfiguration) -> Self {
        self.engine_config = config;
        self
    }
}

#[async_trait]
impl Handler for MockHandler {
    fn name(&self) -> &str {
        &self.name
    }

    fn subscription(&self) -> Subscription {
        self.subscription.clone()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.engine_config
    }

    async fn handle(
        &self,
        _context: HandlerContext,
        message: Envelope,
    ) -> Result<(), HandlerError> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        self.received.lock().push(message);

        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }

        if let Some(ref error) = self.error {
            return match error {
                HandlerError::Permanent { message, action } => Err(HandlerError::Permanent {
                    message: message.clone(),
                    action: *action,
                }),
                HandlerError::Deserialization(_) => {
                    Err(HandlerError::Processing(error.to_string()))
                }
                HandlerError::Processing(msg) => Err(HandlerError::Processing(msg.clone())),
            };
        }

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct MockLockService {
    held: Arc<Mutex<HashSet<String>>>,
}

impl MockLockService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_lock(&self, key: &str) {
        self.held.lock().insert(key.to_string());
    }

    pub fn is_held(&self, key: &str) -> bool {
        self.held.lock().contains(key)
    }
}

#[async_trait]
impl LockService for MockLockService {
    async fn try_acquire(&self, key: &str, _ttl: Duration) -> Result<bool, LockError> {
        let mut held = self.held.lock();
        if held.contains(key) {
            Ok(false)
        } else {
            held.insert(key.to_string());
            Ok(true)
        }
    }

    async fn release(&self, key: &str) -> Result<(), LockError> {
        self.held.lock().remove(key);
        Ok(())
    }
}

pub struct TestEnvelopeFactory;

impl TestEnvelopeFactory {
    pub fn simple(payload: &str) -> Envelope {
        Envelope {
            id: MessageId(Uuid::new_v4().to_string().into()),
            subject: Arc::from(""),
            payload: Bytes::from(payload.to_string()),
            timestamp: Utc::now(),
            attempt: 1,
        }
    }

    pub fn with_subject(subject: &str, payload: Bytes) -> Envelope {
        Envelope {
            id: MessageId(Uuid::new_v4().to_string().into()),
            subject: Arc::from(subject),
            payload,
            timestamp: Utc::now(),
            attempt: 1,
        }
    }

    pub fn with_attempt(payload: &str, attempt: u32) -> Envelope {
        Envelope {
            id: MessageId(Uuid::new_v4().to_string().into()),
            subject: Arc::from(""),
            payload: Bytes::from(payload.to_string()),
            timestamp: Utc::now(),
            attempt,
        }
    }

    pub fn batch(count: usize) -> Vec<Envelope> {
        (0..count)
            .map(|i| Self::simple(&format!("message-{}", i)))
            .collect()
    }

    pub fn with_bytes(payload: Bytes) -> Envelope {
        Envelope {
            id: MessageId(Uuid::new_v4().to_string().into()),
            subject: Arc::from(""),
            payload,
            timestamp: Utc::now(),
            attempt: 1,
        }
    }
}
