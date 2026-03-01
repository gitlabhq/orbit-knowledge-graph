//! Core types for message handling.
//!
//! These types are transport-agnostic and used throughout the ETL engine.

use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur during serialization/deserialization.
#[derive(Debug, Error)]
pub enum SerializationError {
    /// Failed to serialize or deserialize a message.
    #[error("serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// A NATS topic consisting of a stream and subject.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Topic {
    /// The NATS JetStream stream name.
    pub stream: Arc<str>,
    /// The subject filter within the stream.
    pub subject: Arc<str>,
}

impl Topic {
    /// Creates a new topic from stream and subject.
    pub fn new(stream: impl Into<Arc<str>>, subject: impl Into<Arc<str>>) -> Self {
        Self {
            stream: stream.into(),
            subject: subject.into(),
        }
    }
}

/// A unique identifier for a message.
///
/// Uses `Arc<str>` internally for cheap cloning.
#[derive(Clone)]
pub struct MessageId(pub Arc<str>);

impl MessageId {
    /// Creates a new unique message ID.
    pub fn unique() -> MessageId {
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
/// use etl_engine::types::{Envelope, Event, Topic};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct UserCreated {
///     user_id: String,
///     email: String,
/// }
///
/// impl Event for UserCreated {
///     fn topic() -> Topic {
///         Topic::new("users-stream", "user.created")
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
/// use etl_engine::types::{Event, Topic};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct OrderPlaced {
///     order_id: String,
///     total: f64,
/// }
///
/// impl Event for OrderPlaced {
///     fn topic() -> Topic {
///         Topic::new("orders-stream", "orders.placed")
///     }
/// }
/// ```
pub trait Event: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Returns the topic this event should be published to.
    fn topic() -> Topic;
}

impl Envelope {
    /// Creates a new envelope from a typed event.
    ///
    /// The event is serialized to JSON and wrapped with metadata.
    pub fn new<T: Event>(payload: &T) -> Result<Self, SerializationError> {
        let payload = Bytes::from(serde_json::to_vec(payload)?);

        Ok(Envelope {
            id: MessageId::unique(),
            payload,
            timestamp: Utc::now(),
            attempt: 1,
        })
    }

    /// Deserializes the payload into a typed event.
    pub fn to_event<T: Event>(&self) -> Result<T, SerializationError> {
        Ok(serde_json::from_slice(&self.payload)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestEvent {
        id: String,
        value: i32,
    }

    impl Event for TestEvent {
        fn topic() -> Topic {
            Topic::new("test-stream", "test-events")
        }
    }

    #[test]
    fn envelope_serialization() {
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
    fn message_ids_are_unique() {
        let first = MessageId::unique();
        let second = MessageId::unique();

        assert_ne!(first.0.as_ref(), second.0.as_ref());
    }
}
