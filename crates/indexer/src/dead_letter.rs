use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{Envelope, Subscription};

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
    pub fn new(original_subscription: &Subscription, envelope: &Envelope, error: &str) -> Self {
        let original_payload = serde_json::from_slice(&envelope.payload).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&envelope.payload).into_owned())
        });

        Self {
            original_subject: original_subscription.subject.to_string(),
            original_stream: original_subscription.stream.to_string(),
            original_payload,
            original_message_id: envelope.id.0.to_string(),
            original_timestamp: envelope.timestamp,
            failed_at: Utc::now(),
            attempts: envelope.attempt,
            last_error: error.to_string(),
        }
    }
}

pub fn dead_letter_subject(subscription: &Subscription) -> String {
    format!(
        "{}.{}.{}",
        DEAD_LETTER_SUBJECT_PREFIX, subscription.stream, subscription.subject
    )
}

pub fn dead_letter_subscription(subscription: &Subscription) -> Subscription {
    Subscription::new(DEAD_LETTER_STREAM, dead_letter_subject(subscription))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_letter_subject_formats_correctly() {
        let subscription = Subscription::new("siphon_db", "tables.merge_requests");
        assert_eq!(
            dead_letter_subject(&subscription),
            "dlq.siphon_db.tables.merge_requests"
        );
    }

    #[test]
    fn dead_letter_subscription_points_to_dlq_stream() {
        let subscription = Subscription::new("siphon_db", "tables.users");
        let dlq = dead_letter_subscription(&subscription);
        assert_eq!(&*dlq.stream, DEAD_LETTER_STREAM);
        assert_eq!(&*dlq.subject, "dlq.siphon_db.tables.users");
        assert!(dlq.manage_stream);
    }
}
