//! Mock implementations for testing.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::destination::{BatchWriter, Destination, DestinationError};
use crate::entities::Entity;
use crate::metrics::MetricCollector;
use crate::module::{Handler, HandlerContext, HandlerError, Module};
use crate::nats::{NatsError, NatsServices};
use crate::types::{Envelope, MessageId, Topic};

/// Mock implementation of [`NatsServices`] for testing handlers.
///
/// Records all published messages for later verification.
///
/// # Example
///
/// ```ignore
/// let mock_nats = MockNatsServices::new();
/// let context = HandlerContext::new(destination, metrics, Arc::new(mock_nats.clone()));
///
/// handler.handle(context, envelope).await?;
///
/// let published = mock_nats.get_published();
/// assert_eq!(published.len(), 1);
/// ```
#[derive(Clone, Default)]
pub struct MockNatsServices {
    published: Arc<Mutex<Vec<(Topic, Envelope)>>>,
}

impl MockNatsServices {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_published(&self) -> Vec<(Topic, Envelope)> {
        self.published.lock().clone()
    }
}

#[async_trait]
impl NatsServices for MockNatsServices {
    async fn publish(&self, topic: &Topic, envelope: &Envelope) -> Result<(), NatsError> {
        self.published
            .lock()
            .push((topic.clone(), envelope.clone()));
        Ok(())
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
        _entity: &Entity,
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
    topic: Topic,
    delay: Option<Duration>,
    error: Option<HandlerError>,
    invocations: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<Envelope>>>,
}

impl MockHandler {
    pub fn new(stream: &'static str, subject: &'static str) -> Self {
        Self {
            name: format!("mock-handler-{}:{}", stream, subject),
            topic: Topic::new(stream, subject),
            delay: None,
            error: None,
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
}

#[async_trait]
impl Handler for MockHandler {
    fn name(&self) -> &str {
        &self.name
    }

    fn topic(&self) -> Topic {
        self.topic.clone()
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

/// Mock module for testing.
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
    fn name(&self) -> &str {
        self.0.name()
    }

    fn topic(&self) -> Topic {
        self.0.topic()
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        self.0.handle(context, message).await
    }
}

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

#[derive(Debug, Clone, PartialEq)]
pub enum RecordedMetric {
    Increment {
        name: String,
        tags: Vec<(String, String)>,
    },
    Gauge {
        name: String,
        value: f64,
        tags: Vec<(String, String)>,
    },
    Histogram {
        name: String,
        value: f64,
        tags: Vec<(String, String)>,
    },
}

#[derive(Default)]
pub struct MockMetricCollector {
    metrics: Mutex<Vec<RecordedMetric>>,
}

impl MockMetricCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_metrics(&self) -> Vec<RecordedMetric> {
        self.metrics.lock().clone()
    }

    pub fn count(&self, name: &str) -> usize {
        self.metrics
            .lock()
            .iter()
            .filter(|m| match m {
                RecordedMetric::Increment { name: n, .. } => n == name,
                RecordedMetric::Gauge { name: n, .. } => n == name,
                RecordedMetric::Histogram { name: n, .. } => n == name,
            })
            .count()
    }

    pub fn clear(&self) {
        self.metrics.lock().clear();
    }
}

impl MetricCollector for MockMetricCollector {
    fn increment(&self, name: &str, tags: &[(&str, &str)]) {
        self.metrics.lock().push(RecordedMetric::Increment {
            name: name.to_string(),
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        });
    }

    fn gauge(&self, name: &str, value: f64, tags: &[(&str, &str)]) {
        self.metrics.lock().push(RecordedMetric::Gauge {
            name: name.to_string(),
            value,
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        });
    }

    fn histogram(&self, name: &str, value: f64, tags: &[(&str, &str)]) {
        self.metrics.lock().push(RecordedMetric::Histogram {
            name: name.to_string(),
            value,
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        });
    }
}
