//! NATS message types.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::AckKind;
use futures::stream::Stream as FuturesStream;
use tracing::warn;

use super::broker::NatsBroker;
use super::error::{NatsError, map_ack_error, map_nack_error};
use crate::types::{Envelope, Topic};

pub struct NatsMessage {
    /// The message envelope containing payload and metadata.
    pub envelope: Envelope,
    pub(crate) acker: Arc<async_nats::jetstream::message::Acker>,
}

impl NatsMessage {
    pub fn progress_notifier(&self) -> ProgressNotifier {
        ProgressNotifier {
            acker: Some(self.acker.clone()),
        }
    }

    pub async fn ack(self) -> Result<(), NatsError> {
        self.acker.ack().await.map_err(map_ack_error)
    }

    /// Tells NATS this message failed permanently, removing it from the stream.
    pub async fn term_ack(self) -> Result<(), NatsError> {
        self.acker
            .ack_with(AckKind::Term)
            .await
            .map_err(map_ack_error)
    }

    /// Negatively acknowledges the message for immediate redelivery.
    pub async fn nack(self) -> Result<(), NatsError> {
        self.acker
            .ack_with(AckKind::Nak(None))
            .await
            .map_err(map_nack_error)
    }

    /// Negatively acknowledges the message with a delay before redelivery.
    pub async fn nack_with_delay(self, delay: Duration) -> Result<(), NatsError> {
        self.acker
            .ack_with(AckKind::Nak(Some(delay)))
            .await
            .map_err(map_nack_error)
    }

    /// Publishes the message to the dead letter queue, then acks it.
    ///
    /// If the DLQ publish fails, the message is nacked for redelivery instead.
    pub async fn to_dlq(self, broker: &NatsBroker, topic: &Topic, error: &str) -> DlqResult {
        let message_id = self.envelope.id.0.clone();

        let dlq_result = broker
            .publish_dead_letter(topic, &self.envelope, error)
            .await;

        match dlq_result {
            Ok(()) => {
                if let Err(ack_error) = self.ack().await {
                    warn!(%ack_error, %message_id, "failed to ack exhausted message");
                }
                DlqResult::Published
            }
            Err(dlq_error) => {
                warn!(
                    %dlq_error,
                    %message_id,
                    "failed to publish to dead letter queue, nacking for redelivery"
                );
                if let Err(nack_error) = self.nack().await {
                    warn!(%nack_error, %message_id, "failed to nack message after DLQ failure");
                }
                DlqResult::Nacked
            }
        }
    }
}

pub enum DlqResult {
    Published,
    Nacked,
}

/// Tells NATS "I'm still working on this" so it doesn't redeliver the message.
///
/// Each call resets the `ack_wait` timer back to its full duration.
#[derive(Clone)]
pub struct ProgressNotifier {
    acker: Option<Arc<async_nats::jetstream::message::Acker>>,
}

impl ProgressNotifier {
    pub fn noop() -> Self {
        Self { acker: None }
    }

    /// Resets the ack wait timer, preventing redelivery while processing continues.
    pub async fn notify_in_progress(&self) {
        if let Some(acker) = &self.acker
            && let Err(error) = acker.ack_with(AckKind::Progress).await
        {
            warn!(%error, "failed to send in-progress ack");
        }
    }
}

/// A stream of messages from a NATS subscription.
pub type NatsSubscription =
    Pin<Box<dyn FuturesStream<Item = Result<NatsMessage, NatsError>> + Send>>;
