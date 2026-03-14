use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{Envelope, Topic};

pub const DEAD_LETTER_STREAM: &str = "GKG_DEAD_LETTERS";
pub const DEAD_LETTER_SUBJECT_PREFIX: &str = "dlq";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEnvelope {
    pub original_subject: String,
    pub original_stream: String,
    pub original_payload: serde_json::Value,
    pub original_message_id: String,
    pub original_timestamp: DateTime<Utc>,
    pub failed_at: DateTime<Utc>,
    pub attempts: u32,
    pub last_error: String,
}

impl DeadLetterEnvelope {
    pub fn new(original_topic: &Topic, envelope: &Envelope, error: &str) -> Self {
        let original_payload = serde_json::from_slice(&envelope.payload).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&envelope.payload).into_owned())
        });

        Self {
            original_subject: original_topic.subject.to_string(),
            original_stream: original_topic.stream.to_string(),
            original_payload,
            original_message_id: envelope.id.0.to_string(),
            original_timestamp: envelope.timestamp,
            failed_at: Utc::now(),
            attempts: envelope.attempt,
            last_error: error.to_string(),
        }
    }
}

pub fn dead_letter_subject(topic: &Topic) -> String {
    format!(
        "{}.{}.{}",
        DEAD_LETTER_SUBJECT_PREFIX, topic.stream, topic.subject
    )
}

pub fn dead_letter_topic(topic: &Topic) -> Topic {
    Topic::owned(DEAD_LETTER_STREAM, dead_letter_subject(topic))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_letter_subject_formats_correctly() {
        let topic = Topic::external("siphon_db", "tables.merge_requests");
        assert_eq!(
            dead_letter_subject(&topic),
            "dlq.siphon_db.tables.merge_requests"
        );
    }

    #[test]
    fn dead_letter_topic_points_to_dlq_stream() {
        let topic = Topic::external("siphon_db", "tables.users");
        let dlq = dead_letter_topic(&topic);
        assert_eq!(&*dlq.stream, DEAD_LETTER_STREAM);
        assert_eq!(&*dlq.subject, "dlq.siphon_db.tables.users");
        assert!(dlq.owned);
    }

    #[test]
    fn envelope_serialization_round_trip() {
        use crate::types::MessageId;
        use bytes::Bytes;

        let topic = Topic::external("siphon_db", "tables.users");
        let payload = serde_json::to_vec(&serde_json::json!({"user_id": 42})).unwrap();
        let message_envelope = Envelope {
            id: MessageId::unique(),
            payload: Bytes::from(payload),
            timestamp: Utc::now(),
            attempt: 5,
        };
        let dead_letter = DeadLetterEnvelope::new(&topic, &message_envelope, "connection refused");

        let json = serde_json::to_string(&dead_letter).expect("serialize");
        let deserialized: DeadLetterEnvelope = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.original_subject, "tables.users");
        assert_eq!(deserialized.original_stream, "siphon_db");
        assert_eq!(
            deserialized.original_payload,
            serde_json::json!({"user_id": 42})
        );
        assert_eq!(deserialized.attempts, 5);
        assert_eq!(deserialized.last_error, "connection refused");
    }
}
