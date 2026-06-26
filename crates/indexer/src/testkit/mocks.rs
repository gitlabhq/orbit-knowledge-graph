//! Mock implementations for testing.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use nats_client::testkit::MockKvServices;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::locking::{LockError, LockService};
use crate::nats::{
    KvEntry, KvPutOptions, KvPutResult, NatsError, NatsMessage, NatsServices, NoopAcker,
};
use crate::types::{Envelope, MessageId, Subscription};

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

pub fn test_writer() -> Arc<crate::clickhouse::ClickHouseWriter> {
    Arc::new(crate::clickhouse::ClickHouseWriter::noop())
}

pub fn test_write_sink() -> Arc<crate::clickhouse::CodeWriteSink> {
    crate::clickhouse::CodeWriteSink::new(
        test_writer(),
        8,
        500_000,
        std::time::Duration::from_secs(60),
    )
}

/// Mock handler for testing.
pub struct MockHandler {
    name: String,
    subscription: Subscription,
    delay: Option<Duration>,
    error: Option<HandlerError>,
    panic_message: Option<String>,
    on_handle: Option<Arc<dyn Fn() + Send + Sync>>,
    invocations: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<Envelope>>>,
    requires_worker_pool: bool,
}

impl MockHandler {
    pub fn new(stream: &'static str, subject: &'static str) -> Self {
        Self {
            name: format!("mock-handler-{}:{}", stream, subject),
            subscription: Subscription::new(stream, subject),
            delay: None,
            error: None,
            panic_message: None,
            on_handle: None,
            invocations: Arc::new(AtomicUsize::new(0)),
            received: Arc::new(Mutex::new(Vec::new())),
            requires_worker_pool: true,
        }
    }

    pub fn with_requires_worker_pool(mut self, value: bool) -> Self {
        self.requires_worker_pool = value;
        self
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

    pub fn with_panic(mut self, message: &str) -> Self {
        self.panic_message = Some(message.to_string());
        self
    }

    pub fn with_on_handle(mut self, callback: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_handle = Some(Arc::new(callback));
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

    fn requires_worker_pool(&self) -> bool {
        self.requires_worker_pool
    }

    async fn handle(
        &self,
        _context: HandlerContext,
        message: Envelope,
    ) -> Result<(), HandlerError> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        self.received.lock().push(message);

        if let Some(ref msg) = self.panic_message {
            panic!("{msg}");
        }

        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }

        if let Some(ref callback) = self.on_handle {
            callback();
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
    renew_count: Arc<std::sync::atomic::AtomicUsize>,
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

    pub fn renew_count(&self) -> usize {
        self.renew_count.load(std::sync::atomic::Ordering::Relaxed)
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

    async fn renew(&self, _key: &str, _ttl: Duration) -> Result<(), LockError> {
        self.renew_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
