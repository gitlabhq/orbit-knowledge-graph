//! NATS message types.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::AckKind;
use async_trait::async_trait;
use futures::stream::Stream as FuturesStream;
use tracing::warn;

use super::broker::NatsBroker;
use crate::types::{Envelope, Subscription};
use nats_client::NatsError;

fn map_ack_error(error: async_nats::Error) -> NatsError {
    NatsError::Ack(error.to_string())
}

fn map_nack_error(error: async_nats::Error) -> NatsError {
    NatsError::Nack(error.to_string())
}

#[async_trait]
pub trait MessageAcker: Send + Sync {
    async fn ack(&self) -> Result<(), NatsError>;
    async fn ack_term(&self) -> Result<(), NatsError>;
    async fn ack_progress(&self) -> Result<(), NatsError>;
    async fn nack(&self, delay: Option<Duration>) -> Result<(), NatsError>;
}

pub(crate) struct NatsAcker(pub Arc<async_nats::jetstream::message::Acker>);

#[async_trait]
impl MessageAcker for NatsAcker {
    async fn ack(&self) -> Result<(), NatsError> {
        self.0.ack().await.map_err(map_ack_error)
    }

    async fn ack_term(&self) -> Result<(), NatsError> {
        self.0.ack_with(AckKind::Term).await.map_err(map_ack_error)
    }

    async fn ack_progress(&self) -> Result<(), NatsError> {
        self.0
            .ack_with(AckKind::Progress)
            .await
            .map_err(map_ack_error)
    }

    async fn nack(&self, delay: Option<Duration>) -> Result<(), NatsError> {
        self.0
            .ack_with(AckKind::Nak(delay))
            .await
            .map_err(map_nack_error)
    }
}

pub struct NoopAcker;

#[async_trait]
impl MessageAcker for NoopAcker {
    async fn ack(&self) -> Result<(), NatsError> {
        Ok(())
    }
    async fn ack_term(&self) -> Result<(), NatsError> {
        Ok(())
    }
    async fn ack_progress(&self) -> Result<(), NatsError> {
        Ok(())
    }
    async fn nack(&self, _delay: Option<Duration>) -> Result<(), NatsError> {
        Ok(())
    }
}

pub struct NatsMessage {
    /// The message envelope containing payload and metadata.
    pub envelope: Envelope,
    acker: Arc<dyn MessageAcker>,
}

impl NatsMessage {
    pub fn new(envelope: Envelope, acker: impl MessageAcker + 'static) -> Self {
        Self {
            envelope,
            acker: Arc::new(acker),
        }
    }

    pub fn progress_notifier(&self) -> ProgressNotifier {
        ProgressNotifier {
            acker: Some(self.acker.clone()),
            lock_renewal: None,
        }
    }

    pub async fn ack(self) -> Result<(), NatsError> {
        self.acker.ack().await
    }

    /// Tells NATS this message failed permanently, removing it from the stream.
    pub async fn term_ack(self) -> Result<(), NatsError> {
        self.acker.ack_term().await
    }

    /// Negatively acknowledges the message for immediate redelivery.
    pub async fn nack(self) -> Result<(), NatsError> {
        self.acker.nack(None).await
    }

    /// Negatively acknowledges the message with a delay before redelivery.
    pub async fn nack_with_delay(self, delay: Duration) -> Result<(), NatsError> {
        self.acker.nack(Some(delay)).await
    }

    /// Publishes the message to the dead letter queue, then acks it.
    ///
    /// If the DLQ publish fails, the message is nacked for redelivery instead.
    pub async fn to_dlq(
        self,
        broker: &NatsBroker,
        subscription: &Subscription,
        error: &str,
    ) -> DlqResult {
        let message_id = self.envelope.id.0.clone();

        let dlq_result = broker
            .publish_dead_letter(subscription, &self.envelope, error)
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

/// A held lock the notifier should keep alive alongside the NATS ack.
#[derive(Clone)]
struct LockRenewal {
    service: Arc<dyn crate::locking::LockService>,
    key: String,
    ttl: Duration,
}

/// Tells NATS "I'm still working on this" so it doesn't redeliver the message,
/// and (when a lock is attached) renews that lock's lease in the same beat — so
/// one "still alive" signal keeps both the `ack_wait` timer and the lock alive.
#[derive(Clone)]
pub struct ProgressNotifier {
    acker: Option<Arc<dyn MessageAcker>>,
    lock_renewal: Option<LockRenewal>,
}

impl ProgressNotifier {
    pub fn noop() -> Self {
        Self {
            acker: None,
            lock_renewal: None,
        }
    }

    /// Attach a held lock so every in-progress notification also renews its
    /// lease, on the same cadence as the ack and decoupled from `ack_wait`.
    pub fn with_lock_renewal(
        mut self,
        service: Arc<dyn crate::locking::LockService>,
        key: String,
        ttl: Duration,
    ) -> Self {
        self.lock_renewal = Some(LockRenewal { service, key, ttl });
        self
    }

    /// Resets the ack wait timer (preventing redelivery while processing
    /// continues) and renews the attached lock lease, if any.
    pub async fn notify_in_progress(&self) {
        if let Some(acker) = &self.acker
            && let Err(error) = acker.ack_progress().await
        {
            warn!(%error, "failed to send in-progress ack");
        }
        if let Some(renewal) = &self.lock_renewal {
            match renewal.service.renew(&renewal.key, renewal.ttl).await {
                Ok(true) => {}
                Ok(false) => warn!(key = %renewal.key, "lock lease lost during in-progress notify"),
                Err(error) => warn!(key = %renewal.key, %error, "lock lease renewal failed"),
            }
        }
    }
}

/// A stream of messages from a NATS subscription.
pub type NatsSubscription =
    Pin<Box<dyn FuturesStream<Item = Result<NatsMessage, NatsError>> + Send>>;
