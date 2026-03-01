//! Dead letter queue for messages that exhaust retry attempts.
//!
//! When a handler fails more times than its configured `max_retry_attempts`,
//! the original message is published to the `gkg_dead_letters` stream with
//! metadata for diagnosis and selective replay.
//!
//! Subject format: `dlq.<original_stream>.<original_subject>`

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::Topic;

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
    pub handler_name: String,
    pub module_name: String,
}

/// Builds the DLQ subject for a given original topic.
///
/// Example: topic `siphon_db` / `tables.merge_requests`
/// becomes `dlq.siphon_db.tables.merge_requests`.
pub fn dead_letter_subject(topic: &Topic) -> String {
    format!(
        "{}.{}.{}",
        DEAD_LETTER_SUBJECT_PREFIX, topic.stream, topic.subject
    )
}

/// Returns a [`Topic`] pointing to the DLQ stream and subject for the given original topic.
pub fn dead_letter_topic(topic: &Topic) -> Topic {
    Topic::new(DEAD_LETTER_STREAM, dead_letter_subject(topic))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_letter_subject_formats_correctly() {
        let topic = Topic::new("siphon_db", "tables.merge_requests");
        assert_eq!(
            dead_letter_subject(&topic),
            "dlq.siphon_db.tables.merge_requests"
        );
    }

    #[test]
    fn dead_letter_topic_points_to_dlq_stream() {
        let topic = Topic::new("code_indexing", "repos.archive");
        let dlq = dead_letter_topic(&topic);
        assert_eq!(&*dlq.stream, DEAD_LETTER_STREAM);
        assert_eq!(&*dlq.subject, "dlq.code_indexing.repos.archive");
    }

    #[test]
    fn envelope_serialization_round_trip() {
        let envelope = DeadLetterEnvelope {
            original_subject: "tables.users".into(),
            original_stream: "siphon_db".into(),
            original_payload: serde_json::json!({"user_id": 42}),
            original_message_id: "msg-123".into(),
            original_timestamp: Utc::now(),
            failed_at: Utc::now(),
            attempts: 5,
            last_error: "connection refused".into(),
            handler_name: "user-handler".into(),
            module_name: "sdlc".into(),
        };

        let json = serde_json::to_string(&envelope).expect("serialize");
        let deserialized: DeadLetterEnvelope = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.original_subject, envelope.original_subject);
        assert_eq!(deserialized.original_stream, envelope.original_stream);
        assert_eq!(deserialized.original_payload, envelope.original_payload);
        assert_eq!(
            deserialized.original_message_id,
            envelope.original_message_id
        );
        assert_eq!(deserialized.attempts, envelope.attempts);
        assert_eq!(deserialized.last_error, envelope.last_error);
        assert_eq!(deserialized.handler_name, envelope.handler_name);
        assert_eq!(deserialized.module_name, envelope.module_name);
    }
}
