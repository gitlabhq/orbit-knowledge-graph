//! Mock implementations for testing.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::destination::{BatchWriter, Destination, DestinationError, StreamWriter};
use crate::entities::Entity;
use crate::message_broker::{
    AckHandle, BrokerError, Envelope, Message, MessageBroker, MessageId, Subscription,
};
use crate::module::{Handler, HandlerContext, HandlerError, Module};

type MessageSenders = Arc<Mutex<Vec<mpsc::Sender<Result<Message, BrokerError>>>>>;

/// Tracks ack/nack/dlq calls for verification in tests.
pub struct MockAckHandle {
    acked: Arc<AtomicBool>,
    nacked: Arc<AtomicBool>,
}

impl MockAckHandle {
    pub fn new() -> Self {
        Self {
            acked: Arc::new(AtomicBool::new(false)),
            nacked: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn was_acked(&self) -> bool {
        self.acked.load(Ordering::SeqCst)
    }

    pub fn was_nacked(&self) -> bool {
        self.nacked.load(Ordering::SeqCst)
    }
}

impl Default for MockAckHandle {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AckHandle for MockAckHandle {
    async fn ack(self: Box<Self>) -> Result<(), BrokerError> {
        self.acked.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn nack(self: Box<Self>) -> Result<(), BrokerError> {
        self.nacked.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// A shared ack handle that can be cloned for verification after message processing.
#[derive(Clone)]
pub struct SharedMockAckHandle {
    acked: Arc<AtomicBool>,
    nacked: Arc<AtomicBool>,
}

impl SharedMockAckHandle {
    pub fn new() -> Self {
        Self {
            acked: Arc::new(AtomicBool::new(false)),
            nacked: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn was_acked(&self) -> bool {
        self.acked.load(Ordering::SeqCst)
    }

    pub fn was_nacked(&self) -> bool {
        self.nacked.load(Ordering::SeqCst)
    }

    pub fn to_ack_handle(&self) -> Box<dyn AckHandle> {
        Box::new(MockAckHandle {
            acked: self.acked.clone(),
            nacked: self.nacked.clone(),
        })
    }
}

impl Default for SharedMockAckHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// A pre-built message with an envelope and shared ack handle for verification.
pub struct QueuedMessage {
    envelope: Envelope,
    ack_handle: SharedMockAckHandle,
}

impl QueuedMessage {
    /// Creates a new queued message with a shared ack handle.
    pub fn new(envelope: Envelope) -> Self {
        Self {
            envelope,
            ack_handle: SharedMockAckHandle::new(),
        }
    }

    /// Returns the shared ack handle for verification after processing.
    pub fn ack_handle(&self) -> &SharedMockAckHandle {
        &self.ack_handle
    }
}

/// A controllable mock broker for testing.
///
/// Uses channels internally so subscriptions stay open until the broker is dropped,
/// simulating real broker behavior where you listen until explicitly stopped.
#[derive(Clone, Default)]
pub struct MockMessageBroker {
    messages: Arc<Mutex<HashMap<String, Vec<Envelope>>>>,
    messages_with_handles: Arc<Mutex<HashMap<String, Vec<QueuedMessage>>>>,
    published: Arc<Mutex<Vec<(String, Envelope)>>>,
    subscription_error: Arc<Mutex<Option<BrokerError>>>,
    senders: MessageSenders,
}

impl MockMessageBroker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue messages to be delivered when subscribed.
    pub fn queue_messages(&self, topic: &str, messages: Vec<Envelope>) {
        self.messages
            .lock()
            .entry(topic.to_string())
            .or_default()
            .extend(messages);
    }

    /// Queue a message with a shared ack handle for verification.
    pub fn queue_message_with_handle(&self, topic: &str, message: QueuedMessage) {
        self.messages_with_handles
            .lock()
            .entry(topic.to_string())
            .or_default()
            .push(message);
    }

    /// Make subscribe() return an error.
    pub fn fail_subscription(&self, error: BrokerError) {
        *self.subscription_error.lock() = Some(error);
    }

    /// Get all published messages for assertions.
    pub fn get_published(&self) -> Vec<(String, Envelope)> {
        self.published.lock().clone()
    }
}

#[async_trait]
impl MessageBroker for MockMessageBroker {
    async fn publish(&self, topic: &str, envelope: Envelope) -> Result<(), BrokerError> {
        self.published.lock().push((topic.to_string(), envelope));
        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<Subscription, BrokerError> {
        if let Some(error) = self.subscription_error.lock().take() {
            return Err(error);
        }

        let messages = self.messages.lock().remove(topic).unwrap_or_default();
        let messages_with_handles = self
            .messages_with_handles
            .lock()
            .remove(topic)
            .unwrap_or_default();

        let total_messages = messages.len() + messages_with_handles.len();
        let (tx, rx) = mpsc::channel(total_messages.max(1));

        for envelope in messages {
            let _ = tx
                .send(Ok(Message::new(envelope, Box::new(MockAckHandle::new()))))
                .await;
        }

        for queued in messages_with_handles {
            let _ = tx
                .send(Ok(Message::new(
                    queued.envelope,
                    queued.ack_handle.to_ack_handle(),
                )))
                .await;
        }

        self.senders.lock().push(tx);
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

/// Records all writes for verification.
pub struct MockDestination {
    batch_writes: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
    stream_writes: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
}

impl MockDestination {
    pub fn new() -> Self {
        Self {
            batch_writes: Arc::new(Mutex::new(Vec::new())),
            stream_writes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_batch_writes(&self) -> Vec<Vec<RecordBatch>> {
        self.batch_writes.lock().clone()
    }

    pub fn get_stream_writes(&self) -> Vec<Vec<RecordBatch>> {
        self.stream_writes.lock().clone()
    }
}

impl Default for MockDestination {
    fn default() -> Self {
        Self::new()
    }
}

impl Destination for MockDestination {
    fn new_batch_writer(&self, _entity: &Entity) -> Box<dyn BatchWriter> {
        Box::new(MockBatchWriter {
            writes: self.batch_writes.clone(),
        })
    }

    fn new_stream_writer(&self, _entity: &Entity) -> Box<dyn StreamWriter> {
        Box::new(MockStreamWriter {
            buffer: Arc::new(Mutex::new(Vec::new())),
            writes: self.stream_writes.clone(),
        })
    }
}

/// Captures written RecordBatches for verification.
pub struct MockBatchWriter {
    writes: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
}

impl BatchWriter for MockBatchWriter {
    fn write_batch(&self, batch: &[RecordBatch]) -> Result<(), DestinationError> {
        self.writes.lock().push(batch.to_vec());
        Ok(())
    }
}

/// Captures buffered writes with flush/close.
pub struct MockStreamWriter {
    buffer: Arc<Mutex<Vec<RecordBatch>>>,
    writes: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
}

impl StreamWriter for MockStreamWriter {
    fn write(&self, batch: &[RecordBatch]) -> Result<(), DestinationError> {
        self.buffer.lock().extend(batch.iter().cloned());
        Ok(())
    }

    fn flush(&self) -> Result<(), DestinationError> {
        let buffered: Vec<_> = self.buffer.lock().drain(..).collect();
        if !buffered.is_empty() {
            self.writes.lock().push(buffered);
        }
        Ok(())
    }

    fn close(&self) -> Result<(), DestinationError> {
        self.flush()
    }
}

/// A configurable test handler.
pub struct MockHandler {
    topic: String,
    delay: Option<Duration>,
    error: Option<HandlerError>,
    invocations: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<Envelope>>>,
}

impl MockHandler {
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            delay: None,
            error: None,
            invocations: Arc::new(AtomicUsize::new(0)),
            received: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn with_error(mut self, error: HandlerError) -> Self {
        self.error = Some(error);
        self
    }

    pub fn invocation_count(&self) -> usize {
        self.invocations.load(Ordering::SeqCst)
    }

    pub fn get_received(&self) -> Vec<Envelope> {
        self.received.lock().clone()
    }

    pub fn invocations_arc(&self) -> Arc<AtomicUsize> {
        self.invocations.clone()
    }
}

#[async_trait]
impl Handler for MockHandler {
    fn topic(&self) -> &str {
        &self.topic
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
            return Err(HandlerError::Processing(error.to_string()));
        }

        Ok(())
    }
}

/// Builder pattern for test modules.
pub struct MockModule {
    name: String,
    handlers: Vec<Arc<dyn Handler>>,
    entities: Vec<Entity>,
}

impl MockModule {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            handlers: Vec::new(),
            entities: Vec::new(),
        }
    }

    pub fn with_handler<H: Handler + 'static>(mut self, handler: H) -> Self {
        self.handlers.push(Arc::new(handler));
        self
    }

    pub fn with_entity(mut self, entity: Entity) -> Self {
        self.entities.push(entity);
        self
    }
}

impl Module for MockModule {
    fn name(&self) -> &str {
        &self.name
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        self.handlers
            .iter()
            .map(|h| Box::new(HandlerWrapper(h.clone())) as Box<dyn Handler>)
            .collect()
    }

    fn entities(&self) -> Vec<Entity> {
        self.entities.clone()
    }
}

struct HandlerWrapper(Arc<dyn Handler>);

#[async_trait]
impl Handler for HandlerWrapper {
    fn topic(&self) -> &str {
        self.0.topic()
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        self.0.handle(context, message).await
    }
}

/// Factory for creating test envelopes.
pub struct TestEnvelopeFactory;

impl TestEnvelopeFactory {
    pub fn simple(payload: &str) -> Envelope {
        Envelope {
            id: MessageId(Uuid::new_v4().to_string().into()),
            payload: Bytes::from(payload.to_string()),
            timestamp: Utc::now(),
            attempt: 1,
        }
    }

    pub fn with_attempt(payload: &str, attempt: u32) -> Envelope {
        Envelope {
            id: MessageId(Uuid::new_v4().to_string().into()),
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
}
