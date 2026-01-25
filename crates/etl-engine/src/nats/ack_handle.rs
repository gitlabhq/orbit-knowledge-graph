//! Ack/nack wrapper for NATS JetStream messages.

use async_nats::jetstream::AckKind;
use async_nats::jetstream::message::Acker;
use async_trait::async_trait;

use crate::message_broker::{AckHandle, BrokerError};

use super::error::{map_ack_error, map_nack_error};

pub struct NatsAckHandle {
    acker: Acker,
}

impl NatsAckHandle {
    pub(crate) fn new(acker: Acker) -> Self {
        Self { acker }
    }
}

#[async_trait]
impl AckHandle for NatsAckHandle {
    async fn ack(self: Box<Self>) -> Result<(), BrokerError> {
        self.acker.ack().await.map_err(map_ack_error)
    }

    async fn nack(self: Box<Self>) -> Result<(), BrokerError> {
        self.acker
            .ack_with(AckKind::Nak(None)) // Immediate redelivery
            .await
            .map_err(map_nack_error)
    }
}
