//! NATS message types.

use std::pin::Pin;
use std::time::Duration;

use async_nats::jetstream::AckKind;
use futures::stream::Stream as FuturesStream;

use super::error::{NatsError, map_ack_error, map_nack_error};
use crate::types::Envelope;

pub struct NatsMessage {
    /// The message envelope containing payload and metadata.
    pub envelope: Envelope,
    pub(crate) acker: async_nats::jetstream::message::Acker,
}

impl NatsMessage {
    pub async fn ack(self) -> Result<(), NatsError> {
        self.acker.ack().await.map_err(map_ack_error)
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

/// A stream of messages from a NATS subscription.
pub type NatsSubscription =
    Pin<Box<dyn FuturesStream<Item = Result<NatsMessage, NatsError>> + Send>>;
