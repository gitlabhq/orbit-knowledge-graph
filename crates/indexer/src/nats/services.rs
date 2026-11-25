use async_trait::async_trait;
use bytes::Bytes;
use nats_client::{KvEntry, KvPutOptions, KvPutResult};

use crate::types::{Envelope, Subscription};
use nats_client::NatsError;

use super::message::NatsMessage;

#[async_trait]
pub trait NatsServices: Send + Sync {
    async fn publish(
        &self,
        subscription: &Subscription,
        envelope: &Envelope,
    ) -> Result<(), NatsError>;

    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError>;

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError>;

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError>;

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError>;

    async fn consume_pending(
        &self,
        subscription: &Subscription,
        batch_size: usize,
    ) -> Result<Vec<NatsMessage>, NatsError>;
}

pub struct NatsServicesImpl {
    broker: std::sync::Arc<super::broker::NatsBroker>,
}

impl NatsServicesImpl {
    pub fn new(broker: std::sync::Arc<super::broker::NatsBroker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl NatsServices for NatsServicesImpl {
    async fn publish(
        &self,
        subscription: &Subscription,
        envelope: &Envelope,
    ) -> Result<(), NatsError> {
        self.broker.publish(subscription, envelope).await
    }

    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        self.broker.kv_get(bucket, key).await
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        self.broker.kv_put(bucket, key, value, options).await
    }

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        self.broker.kv_delete(bucket, key).await
    }

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        self.broker.kv_keys(bucket).await
    }

    async fn consume_pending(
        &self,
        subscription: &Subscription,
        batch_size: usize,
    ) -> Result<Vec<NatsMessage>, NatsError> {
        self.broker.consume_pending(subscription, batch_size).await
    }
}
