use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::types::{Envelope, Subscription};

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
    pub fn new(
        resolved_stream: &str,
        original_subscription: &Subscription,
        envelope: &Envelope,
        error: &str,
    ) -> Self {
        let original_payload = serde_json::from_slice(&envelope.payload).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&envelope.payload).into_owned())
        });
        let original_subject = original_subject(original_subscription, envelope);

        Self {
            original_subject: original_subject.to_string(),
            original_stream: resolved_stream.to_string(),
            original_payload,
            original_message_id: envelope.id.0.to_string(),
            original_timestamp: envelope.timestamp,
            failed_at: Utc::now(),
            attempts: envelope.attempt,
            last_error: error.to_string(),
        }
    }
}

pub fn dead_letter_subject(
    resolved_stream: &str,
    subscription: &Subscription,
    envelope: &Envelope,
) -> String {
    dead_letter_subject_for_subject(resolved_stream, original_subject(subscription, envelope))
}

fn dead_letter_subject_for_subject(stream: &str, subject: &str) -> String {
    format!("{}.{}.{}", DEAD_LETTER_SUBJECT_PREFIX, stream, subject)
}

pub fn dead_letter_subscription(
    resolved_stream: &str,
    subscription: &Subscription,
) -> Subscription {
    Subscription::new(
        DEAD_LETTER_STREAM,
        dead_letter_subject_for_subject(resolved_stream, &subscription.subject),
    )
}

fn original_subject<'a>(subscription: &'a Subscription, envelope: &'a Envelope) -> &'a str {
    if envelope.subject.is_empty() {
        &subscription.subject
    } else {
        &envelope.subject
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;

    fn envelope_with_subject(subject: &str) -> Envelope {
        Envelope {
            id: crate::engine::types::MessageId(Arc::from("test-message")),
            subject: Arc::from(subject),
            payload: Bytes::from_static(br#"{"ok":true}"#),
            timestamp: Utc::now(),
            attempt: 5,
        }
    }

    #[test]
    fn dead_letter_subject_uses_delivered_message_subject() {
        let subscription = Subscription::new("GKG_INDEXER", "code.task.indexing.requested.*.*");
        let envelope = envelope_with_subject("code.task.indexing.requested.278964.bWFzdGVy");

        assert_eq!(
            dead_letter_subject("GKG_INDEXER_V69", &subscription, &envelope),
            "dlq.GKG_INDEXER_V69.code.task.indexing.requested.278964.bWFzdGVy"
        );
    }

    #[test]
    fn dead_letter_envelope_records_delivered_message_subject() {
        let subscription = Subscription::new("GKG_INDEXER", "code.task.indexing.requested.*.*");
        let envelope = envelope_with_subject("code.task.indexing.requested.278964.bWFzdGVy");
        let dead_letter =
            DeadLetterEnvelope::new("GKG_INDEXER_V69", &subscription, &envelope, "failed");

        assert_eq!(
            dead_letter.original_subject,
            "code.task.indexing.requested.278964.bWFzdGVy"
        );
        assert_eq!(dead_letter.original_stream, "GKG_INDEXER_V69");
    }

    #[test]
    fn dead_letter_subject_falls_back_to_subscription_for_local_envelopes() {
        let subscription = Subscription::new("siphon_db", "tables.merge_requests");
        let envelope = envelope_with_subject("");

        assert_eq!(
            dead_letter_subject("siphon_db", &subscription, &envelope),
            "dlq.siphon_db.tables.merge_requests"
        );
    }

    #[test]
    fn dead_letter_subscription_points_to_dlq_stream() {
        let subscription = Subscription::new("siphon_db", "tables.users");
        let dlq = dead_letter_subscription("siphon_db", &subscription);
        assert_eq!(&*dlq.stream, DEAD_LETTER_STREAM);
        assert_eq!(&*dlq.subject, "dlq.siphon_db.tables.users");
        assert!(dlq.manage_stream);
    }
}
