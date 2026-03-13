//! NATS message types.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::AckKind;
use futures::stream::Stream as FuturesStream;
use tracing::warn;

use super::error::{NatsError, map_ack_error, map_nack_error};
use crate::types::Envelope;

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
