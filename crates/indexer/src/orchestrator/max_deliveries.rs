//! Reconciles NATS JetStream messages that have exhausted `max_deliver`.
//!
//! GKG streams allow one message per subject (`discard_new_per_subject`), so
//! a message JetStream gives up on without ack/nack/term permanently blocks
//! that subject. This listens for JetStream's `MAX_DELIVERIES` advisory and
//! deletes the exhausted message so the existing sweep/backfill dispatchers
//! can retry it.

use async_nats::jetstream::ErrorCode;
use async_nats::jetstream::stream::{DeleteMessageErrorKind, RawMessageErrorKind};
use async_trait::async_trait;
use futures::StreamExt;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::orchestrator::{Trigger, TriggerError};
use crate::topic::INDEXER_STREAM;

/// Fixed by the NATS wire protocol, not configurable.
const ADVISORY_SUBJECT: &str = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>";

/// Shared across replicas so JetStream delivers each advisory to exactly one, with no extra locking needed.
const QUEUE_GROUP: &str = "gkg-max-deliveries-reconciler";

/// Backoff before retrying a failed subscribe attempt.
const RESUBSCRIBE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);

/// `io.nats.jetstream.advisory.v1.max_deliver` fields this trigger needs; `async-nats` 0.49 has no typed struct for it.
#[derive(Debug, Deserialize)]
struct MaxDeliverAdvisory {
    stream: String,
    consumer: String,
    stream_seq: u64,
    deliveries: u64,
}

#[derive(Clone)]
struct MaxDeliveriesMetrics {
    exhausted: Counter<u64>,
}

impl MaxDeliveriesMetrics {
    fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            exhausted: gkg_observability::indexer::nats::MAX_DELIVERIES_EXHAUSTED
                .build_counter_u64(&meter),
        }
    }

    fn record_exhaustion(&self, stream: &str, consumer: &str) {
        self.exhausted.add(
            1,
            &[
                KeyValue::new(
                    gkg_observability::indexer::nats::labels::STREAM,
                    stream.to_owned(),
                ),
                KeyValue::new(
                    gkg_observability::indexer::nats::labels::CONSUMER,
                    consumer.to_owned(),
                ),
            ],
        );
    }
}

pub struct MaxDeliveriesReconciler {
    nats_client: async_nats::Client,
    metrics: MaxDeliveriesMetrics,
}

impl MaxDeliveriesReconciler {
    pub fn new(nats_client: async_nats::Client) -> Self {
        Self {
            nats_client,
            metrics: MaxDeliveriesMetrics::new(),
        }
    }

    /// Only GKG's own versioned streams have this failure mode; e.g. Siphon's don't.
    fn is_managed_stream(stream: &str) -> bool {
        stream.starts_with(&format!("{INDEXER_STREAM}_V"))
    }

    async fn handle_advisory(
        &self,
        jetstream: &async_nats::jetstream::Context,
        message: async_nats::Message,
    ) {
        let advisory: MaxDeliverAdvisory = match serde_json::from_slice(&message.payload) {
            Ok(advisory) => advisory,
            Err(error) => {
                warn!(%error, "failed to decode max-deliveries advisory payload");
                return;
            }
        };

        if !Self::is_managed_stream(&advisory.stream) {
            return;
        }

        let stream = match jetstream.get_stream(&advisory.stream).await {
            Ok(stream) => stream,
            Err(error) => {
                warn!(
                    stream = %advisory.stream,
                    %error,
                    "failed to look up stream for exhausted max-deliveries advisory"
                );
                return;
            }
        };

        let subject = match stream.get_raw_message(advisory.stream_seq).await {
            Ok(raw) => Some(raw.subject.to_string()),
            Err(error) if error.kind() == RawMessageErrorKind::NoMessageFound => {
                // Already gone (e.g. a duplicate advisory, or someone else in the
                // queue group already deleted it): nothing left to reconcile.
                return;
            }
            Err(error) => {
                warn!(
                    stream = %advisory.stream,
                    stream_seq = advisory.stream_seq,
                    %error,
                    "failed to fetch exhausted message for logging; proceeding with deletion"
                );
                None
            }
        };

        match stream.delete_message(advisory.stream_seq).await {
            Ok(_) => {
                self.metrics
                    .record_exhaustion(&advisory.stream, &advisory.consumer);
                warn!(
                    stream = %advisory.stream,
                    consumer = %advisory.consumer,
                    subject = ?subject,
                    deliveries = advisory.deliveries,
                    "deleted permanently stuck message after max_deliver exhaustion"
                );
            }
            Err(error) => match error.kind() {
                DeleteMessageErrorKind::JetStream(inner)
                    if inner.error_code() == ErrorCode::NO_MESSAGE_FOUND =>
                {
                    // Deleted between the fetch and the delete (e.g. manual intervention).
                }
                _ => {
                    warn!(
                        stream = %advisory.stream,
                        stream_seq = advisory.stream_seq,
                        %error,
                        "failed to delete exhausted message"
                    );
                }
            },
        }
    }
}

#[async_trait]
impl Trigger for MaxDeliveriesReconciler {
    fn name(&self) -> &str {
        "orchestrator.max_deliveries_reconciler"
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> Result<(), TriggerError> {
        let jetstream = async_nats::jetstream::new(self.nats_client.clone());

        loop {
            if cancel.is_cancelled() {
                break;
            }

            let mut subscription = match self
                .nats_client
                .queue_subscribe(ADVISORY_SUBJECT, QUEUE_GROUP.to_string())
                .await
            {
                Ok(subscription) => subscription,
                Err(error) => {
                    warn!(%error, "failed to subscribe to max-deliveries advisories, retrying");
                    tokio::select! {
                        () = cancel.cancelled() => break,
                        () = tokio::time::sleep(RESUBSCRIBE_BACKOFF) => continue,
                    }
                }
            };

            loop {
                tokio::select! {
                    () = cancel.cancelled() => return Ok(()),
                    next = subscription.next() => {
                        match next {
                            Some(message) => self.handle_advisory(&jetstream, message).await,
                            None => break,
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_managed_stream_matches_versioned_indexer_streams() {
        assert!(MaxDeliveriesReconciler::is_managed_stream(
            "GKG_INDEXER_V72"
        ));
        assert!(MaxDeliveriesReconciler::is_managed_stream("GKG_INDEXER_V1"));
    }

    #[test]
    fn is_managed_stream_rejects_foreign_and_unversioned_streams() {
        assert!(!MaxDeliveriesReconciler::is_managed_stream(
            "siphon_stream_main_db"
        ));
        assert!(!MaxDeliveriesReconciler::is_managed_stream("GKG_INDEXER"));
        assert!(!MaxDeliveriesReconciler::is_managed_stream(
            "GKG_DEAD_LETTERS_V72"
        ));
    }

    #[test]
    fn advisory_payload_deserializes_expected_fields() {
        let payload = serde_json::json!({
            "type": "io.nats.jetstream.advisory.v1.max_deliver",
            "id": "abc",
            "timestamp": "2026-07-06T00:00:00Z",
            "stream": "GKG_INDEXER_V72",
            "consumer": "gkg-indexer-v72-code-task-indexing-requested-wildcard-wildcard",
            "stream_seq": 42,
            "deliveries": 5,
        });

        let advisory: MaxDeliverAdvisory = serde_json::from_value(payload).expect("deserialize");
        assert_eq!(advisory.stream, "GKG_INDEXER_V72");
        assert_eq!(
            advisory.consumer,
            "gkg-indexer-v72-code-task-indexing-requested-wildcard-wildcard"
        );
        assert_eq!(advisory.stream_seq, 42);
        assert_eq!(advisory.deliveries, 5);
    }
}
