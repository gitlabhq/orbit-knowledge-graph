use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::types::{Envelope, Subscription};
use crate::nats::versioning::NATS_VERSIONER;

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
        let (resolved_stream, _) = NATS_VERSIONER.resolve_stream_and_subject(original_subscription);
        let original_payload = serde_json::from_slice(&envelope.payload).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&envelope.payload).into_owned())
        });
        let original_subject = original_subject(original_subscription, envelope);

        Self {
            original_subject: original_subject.to_string(),
            original_stream: resolved_stream,
            original_payload,
            original_message_id: envelope.id.0.to_string(),
            original_timestamp: envelope.timestamp,
            failed_at: Utc::now(),
            attempts: envelope.attempt,
            last_error: error.to_string(),
        }
    }
}

pub fn dead_letter_subject(subscription: &Subscription, envelope: &Envelope) -> String {
    let (resolved_stream, _) = NATS_VERSIONER.resolve_stream_and_subject(subscription);
    let base =
        dead_letter_subject_for_subject(&resolved_stream, original_subject(subscription, envelope));
    NATS_VERSIONER.subject(&base)
}

fn dead_letter_subject_for_subject(stream: &str, subject: &str) -> String {
    format!("{}.{}.{}", DEAD_LETTER_SUBJECT_PREFIX, stream, subject)
}

pub fn dead_letter_subscription(subscription: &Subscription) -> Subscription {
    let (resolved_stream, _) = NATS_VERSIONER.resolve_stream_and_subject(subscription);
    let base = dead_letter_subject_for_subject(&resolved_stream, &subscription.subject);
    Subscription::new(DEAD_LETTER_STREAM, NATS_VERSIONER.subject(&base))
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
    use crate::schema::version::SCHEMA_VERSION;
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
        let v = *SCHEMA_VERSION;
        let subscription = Subscription::new("GKG_INDEXER", "code.task.indexing.requested.*.*");
        let envelope = envelope_with_subject("code.task.indexing.requested.278964.bWFzdGVy");

        assert_eq!(
            dead_letter_subject(&subscription, &envelope),
            format!("v{v}.dlq.GKG_INDEXER_V{v}.code.task.indexing.requested.278964.bWFzdGVy")
        );
    }

    #[test]
    fn dead_letter_envelope_records_resolved_stream() {
        let v = *SCHEMA_VERSION;
        let subscription = Subscription::new("GKG_INDEXER", "code.task.indexing.requested.*.*");
        let envelope = envelope_with_subject("code.task.indexing.requested.278964.bWFzdGVy");
        let dead_letter = DeadLetterEnvelope::new(&subscription, &envelope, "failed");

        assert_eq!(
            dead_letter.original_subject,
            "code.task.indexing.requested.278964.bWFzdGVy"
        );
        assert_eq!(dead_letter.original_stream, format!("GKG_INDEXER_V{v}"));
    }

    #[test]
    fn dead_letter_subject_falls_back_to_subscription_for_unmanaged_streams() {
        let v = *SCHEMA_VERSION;
        let mut subscription = Subscription::new("siphon_db", "tables.merge_requests");
        subscription.manage_stream = false;
        let envelope = envelope_with_subject("");

        assert_eq!(
            dead_letter_subject(&subscription, &envelope),
            format!("v{v}.dlq.siphon_db.tables.merge_requests")
        );
    }

    #[test]
    fn dead_letter_subscription_points_to_dlq_stream() {
        let v = *SCHEMA_VERSION;
        let mut subscription = Subscription::new("siphon_db", "tables.users");
        subscription.manage_stream = false;
        let dlq = dead_letter_subscription(&subscription);
        assert_eq!(&*dlq.stream, DEAD_LETTER_STREAM);
        assert_eq!(&*dlq.subject, format!("v{v}.dlq.siphon_db.tables.users"));
        assert!(dlq.manage_stream);
    }
}
