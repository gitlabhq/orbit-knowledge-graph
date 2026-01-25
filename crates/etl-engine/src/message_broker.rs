//! Implement [`MessageBroker`] to connect to Kafka, RabbitMQ, etc.
//!
//! Messages arrive as [`Envelope`]s (payload + metadata). The engine calls
//! ack/nack on the [`AckHandle`] based on handler success.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur during broker operations.
#[derive(Debug, Error)]
pub enum BrokerError {
    /// Failed to serialize or deserialize a message.
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Failed to publish a message to the broker.
    #[error("failed to publish message: {0}")]
    Publish(String),

    /// Failed to subscribe to a topic.
    #[error("failed to subscribe to topic: {0}")]
    Subscribe(String),

    /// Failed to acknowledge a message.
    #[error("failed to acknowledge message: {0}")]
    Ack(String),

    /// Failed to negatively acknowledge a message.
    #[error("failed to reject message: {0}")]
    Nack(String),

    /// Failed to connect to the broker.
    #[error("connection error: {0}")]
    Connection(String),
}

/// A unique identifier for a message.
///
/// Uses `Arc<str>` internally for cheap cloning.
#[derive(Clone)]
pub struct MessageId(pub Arc<str>);

impl MessageId {
    fn unique() -> MessageId {
        MessageId(Uuid::new_v4().to_string().into())
    }
}

/// A message envelope containing payload and metadata.
///
/// Envelopes wrap the raw message payload with tracking information
/// like unique IDs, timestamps, and retry counts.
///
/// # Creating an Envelope
///
/// ```ignore
/// use etl_engine::message_broker::{Envelope, Event};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct UserCreated {
///     user_id: String,
///     email: String,
/// }
///
/// impl Event for UserCreated {
///     fn topic() -> &'static str {
///         "user-events"
///     }
/// }
///
/// let event = UserCreated { user_id: "123".into(), email: "user@example.com".into() };
/// let envelope = Envelope::new(&event).unwrap();
/// ```
#[derive(Clone)]
pub struct Envelope {
    /// Unique identifier for this message.
    pub id: MessageId,

    /// The serialized message payload.
    pub payload: Bytes,

    /// When the message was created.
    pub timestamp: DateTime<Utc>,

    /// The current delivery attempt number (starts at 1).
    pub attempt: u32,
}

/// A typed event that can be published to a specific topic.
///
/// Implement this trait for your domain events to enable type-safe
/// serialization and topic routing.
///
/// # Example
///
/// ```ignore
/// use etl_engine::message_broker::Event;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct OrderPlaced {
///     order_id: String,
///     total: f64,
/// }
///
/// impl Event for OrderPlaced {
///     fn topic() -> &'static str {
///         "orders"
///     }
/// }
/// ```
pub trait Event: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Returns the topic this event should be published to.
    fn topic() -> &'static str;
}

impl Envelope {
    /// Creates a new envelope from a typed event.
    ///
    /// The event is serialized to JSON and wrapped with metadata.
    pub fn new<T: Event>(payload: &T) -> Result<Self, BrokerError> {
        let payload = Bytes::from(serde_json::to_vec(payload)?);

        Ok(Envelope {
            id: MessageId::unique(),
            payload,
            timestamp: Utc::now(),
            attempt: 1,
        })
    }

    /// Deserializes the payload into a typed event.
    pub fn to_event<T: Event>(&self) -> Result<T, BrokerError> {
        Ok(serde_json::from_slice(&self.payload)?)
    }

    /// Increments the attempt counter for retry tracking.
    pub fn retry(&mut self) -> &mut Self {
        self.attempt += 1;
        self
    }
}

/// A handle for acknowledging or rejecting a message.
///
/// Implementations of this trait control how messages are acknowledged
/// with the underlying broker. The engine calls these methods based on
/// handler success or failure.
#[async_trait]
pub trait AckHandle: Send {
    /// Acknowledges successful processing of the message.
    ///
    /// The broker will remove the message from the queue.
    async fn ack(self: Box<Self>) -> Result<(), BrokerError>;

    /// Negatively acknowledges the message.
    ///
    /// The broker will typically redeliver the message for retry.
    async fn nack(self: Box<Self>) -> Result<(), BrokerError>;
}

/// A received message with its acknowledgment handle.
///
/// Messages combine the envelope (payload + metadata) with the ack handle
/// that controls message lifecycle with the broker.
pub struct Message {
    /// The message envelope containing payload and metadata.
    pub envelope: Envelope,
    ack_handle: Box<dyn AckHandle>,
}

impl Message {
    /// Creates a new message with the given envelope and ack handle.
    pub fn new(envelope: Envelope, ack_handle: Box<dyn AckHandle>) -> Self {
        Message {
            envelope,
            ack_handle,
        }
    }

    /// Acknowledges successful processing.
    pub async fn ack(self) -> Result<(), BrokerError> {
        self.ack_handle.ack().await
    }

    /// Negatively acknowledges for redelivery.
    pub async fn nack(self) -> Result<(), BrokerError> {
        self.ack_handle.nack().await
    }
}

/// A stream of messages from a subscription.
pub type Subscription = Pin<Box<dyn Stream<Item = Result<Message, BrokerError>> + Send>>;

/// A message broker for publishing and subscribing to topics.
///
/// Implement this trait to integrate with your messaging infrastructure.
/// The engine uses this to receive messages for processing.
///
/// # Example
///
/// ```ignore
/// use etl_engine::message_broker::{MessageBroker, BrokerError, Envelope, Subscription};
/// use async_trait::async_trait;
///
/// struct InMemoryBroker { /* ... */ }
///
/// #[async_trait]
/// impl MessageBroker for InMemoryBroker {
///     async fn publish(&self, topic: &str, envelope: Envelope) -> Result<(), BrokerError> {
///         // Store message in memory
///         Ok(())
///     }
///
///     async fn subscribe(&self, topic: &str) -> Result<Subscription, BrokerError> {
///         // Return a stream of messages
///         todo!()
///     }
/// }
/// ```
#[async_trait]
pub trait MessageBroker: Send + Sync {
    /// Publishes a message to the specified topic.
    async fn publish(&self, topic: &str, envelope: Envelope) -> Result<(), BrokerError>;

    /// Subscribes to a topic and returns a stream of messages.
    async fn subscribe(&self, topic: &str) -> Result<Subscription, BrokerError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{SharedMockAckHandle, TestEnvelopeFactory};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestEvent {
        id: String,
        value: i32,
    }

    impl Event for TestEvent {
        fn topic() -> &'static str {
            "test-events"
        }
    }

    #[test]
    fn test_envelope_serialization() {
        let event = TestEvent {
            id: "123".into(),
            value: 42,
        };
        let envelope = Envelope::new(&event).unwrap();

        assert_eq!(envelope.attempt, 1);
        assert!(!envelope.id.0.is_empty());

        let deserialized: TestEvent = envelope.to_event().unwrap();
        assert_eq!(deserialized, event);
    }

    #[test]
    fn test_envelope_retry_and_unique_ids() {
        let mut first_envelope = TestEnvelopeFactory::simple("payload-a");
        let second_envelope = TestEnvelopeFactory::simple("payload-b");

        assert_ne!(first_envelope.id.0, second_envelope.id.0);

        first_envelope.retry();
        assert_eq!(first_envelope.attempt, 2);
    }

    #[tokio::test]
    async fn test_message_ack_and_nack() {
        // Test ack
        let ack_handle = SharedMockAckHandle::new();
        Message::new(
            TestEnvelopeFactory::simple("payload"),
            ack_handle.to_ack_handle(),
        )
        .ack()
        .await
        .unwrap();
        assert!(ack_handle.was_acked() && !ack_handle.was_nacked());

        // Test nack
        let nack_handle = SharedMockAckHandle::new();
        Message::new(
            TestEnvelopeFactory::simple("payload"),
            nack_handle.to_ack_handle(),
        )
        .nack()
        .await
        .unwrap();
        assert!(nack_handle.was_nacked() && !nack_handle.was_acked());
    }
}
