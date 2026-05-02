use std::time::Duration;

use bytes::Bytes;
use circuit_breaker::CircuitBreaker;

use crate::NatsClient;
use crate::error::NatsError;
use crate::kv_types::{KvBucketConfig, KvEntry, KvPutOptions, KvPutResult};

pub struct CircuitBreakingNatsClient {
    client: NatsClient,
    breaker: CircuitBreaker,
}

impl CircuitBreakingNatsClient {
    pub fn new(client: NatsClient, breaker: CircuitBreaker) -> Self {
        Self { client, breaker }
    }

    pub fn client(&self) -> &NatsClient {
        &self.client
    }

    pub fn nats_client(&self) -> &async_nats::Client {
        self.client.nats_client()
    }

    pub fn jetstream(&self) -> &async_nats::jetstream::Context {
        self.client.jetstream()
    }

    pub fn config(&self) -> &gkg_server_config::NatsConfiguration {
        self.client.config()
    }

    pub async fn create_or_update_stream(
        &self,
        stream_name: &str,
        subjects: Vec<String>,
        max_age: Option<Duration>,
    ) -> Result<async_nats::jetstream::stream::Stream, NatsError> {
        self.breaker
            .call_with_filter(
                || {
                    self.client
                        .create_or_update_stream(stream_name, subjects.clone(), max_age)
                },
                NatsError::is_transient,
            )
            .await
            .map_err(NatsError::from_circuit_breaker)
    }

    pub async fn get_stream(
        &self,
        stream_name: &str,
    ) -> Result<async_nats::jetstream::stream::Stream, NatsError> {
        self.breaker
            .call_with_filter(
                || self.client.get_stream(stream_name),
                NatsError::is_transient,
            )
            .await
            .map_err(NatsError::from_circuit_breaker)
    }

    pub async fn ensure_kv_bucket_exists(
        &self,
        bucket: &str,
        config: KvBucketConfig,
    ) -> Result<(), NatsError> {
        self.breaker
            .call_with_filter(
                || self.client.ensure_kv_bucket_exists(bucket, config.clone()),
                NatsError::is_transient,
            )
            .await
            .map_err(NatsError::from_circuit_breaker)
    }

    pub async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        self.breaker
            .call_with_filter(|| self.client.kv_get(bucket, key), NatsError::is_transient)
            .await
            .map_err(NatsError::from_circuit_breaker)
    }

    pub async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        self.breaker
            .call_with_filter(
                || self.client.kv_put(bucket, key, value.clone(), options),
                NatsError::is_transient,
            )
            .await
            .map_err(NatsError::from_circuit_breaker)
    }

    pub async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        self.breaker
            .call_with_filter(
                || self.client.kv_delete(bucket, key),
                NatsError::is_transient,
            )
            .await
            .map_err(NatsError::from_circuit_breaker)
    }

    pub async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        self.breaker
            .call_with_filter(|| self.client.kv_keys(bucket), NatsError::is_transient)
            .await
            .map_err(NatsError::from_circuit_breaker)
    }
}

impl std::fmt::Debug for CircuitBreakingNatsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreakingNatsClient").finish()
    }
}
