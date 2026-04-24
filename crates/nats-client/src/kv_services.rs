use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::NatsClient;
use crate::error::NatsError;
use crate::kv_types::{KvEntry, KvPutOptions, KvPutResult};

#[async_trait]
pub trait KvServices: Send + Sync {
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
}

pub struct KvServicesImpl {
    client: Arc<NatsClient>,
}

impl KvServicesImpl {
    pub fn new(client: Arc<NatsClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl KvServices for KvServicesImpl {
    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        self.client.kv_get(bucket, key).await
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        self.client.kv_put(bucket, key, value, options).await
    }

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        self.client.kv_delete(bucket, key).await
    }

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        self.client.kv_keys(bucket).await
    }
}
