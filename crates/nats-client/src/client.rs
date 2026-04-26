use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use async_nats::jetstream::Context;
use async_nats::jetstream::kv::{CreateErrorKind, Store as KvStore, UpdateErrorKind};
use async_nats::jetstream::stream::Stream;
use bytes::Bytes;
use futures::TryStreamExt;
use tokio::sync::RwLock;
use tracing::info;

use crate::error::{NatsError, map_connect_error};
use crate::kv_types::{KvBucketConfig, KvEntry, KvPutOptions, KvPutResult};
use gkg_server_config::NatsConfiguration;

pub struct NatsClient {
    client: async_nats::Client,
    jetstream: Context,
    config: NatsConfiguration,
    streams: RwLock<HashMap<String, Stream>>,
    kv_stores: RwLock<HashMap<String, KvStore>>,
}

impl NatsClient {
    pub async fn connect(config: &NatsConfiguration) -> Result<Self, NatsError> {
        config
            .validate_tls_config()
            .map_err(NatsError::Connection)?;

        let connect_options = Self::build_connect_options(config);

        let url = config.connection_url();
        let client = async_nats::connect_with_options(&url, connect_options)
            .await
            .map_err(map_connect_error)?;

        let jetstream = async_nats::jetstream::new(client.clone());

        Ok(Self {
            client,
            jetstream,
            config: config.clone(),
            streams: RwLock::new(HashMap::new()),
            kv_stores: RwLock::new(HashMap::new()),
        })
    }

    pub fn nats_client(&self) -> &async_nats::Client {
        &self.client
    }

    pub fn jetstream(&self) -> &Context {
        &self.jetstream
    }

    pub fn config(&self) -> &NatsConfiguration {
        &self.config
    }

    fn build_connect_options(config: &NatsConfiguration) -> async_nats::ConnectOptions {
        let mut options = async_nats::ConnectOptions::new()
            .connection_timeout(config.connection_timeout())
            .request_timeout(Some(config.request_timeout()));

        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            options = options.user_and_password(user.clone(), pass.clone());
        }

        if config.tls_enabled() {
            options = options.require_tls(true);
        }

        if let Some(ca_path) = &config.tls_ca_cert_path {
            options = options.add_root_certificates(PathBuf::from(ca_path));
        }

        if let (Some(cert), Some(key)) = (&config.tls_cert_path, &config.tls_key_path) {
            options = options.add_client_certificate(PathBuf::from(cert), PathBuf::from(key));
        }

        options
    }

    pub async fn create_or_update_stream(
        &self,
        stream_name: &str,
        subjects: Vec<String>,
        max_age: Option<Duration>,
    ) -> Result<Stream, NatsError> {
        let stream_config = async_nats::jetstream::stream::Config {
            name: stream_name.to_string(),
            subjects: subjects.clone(),
            num_replicas: self.config.stream_replicas,
            max_age: max_age.unwrap_or(self.config.stream_max_age().unwrap_or_default()),
            max_bytes: self.config.stream_max_bytes.unwrap_or(-1),
            max_messages: self.config.stream_max_messages.unwrap_or(-1),
            max_messages_per_subject: 1,
            storage: async_nats::jetstream::stream::StorageType::File,
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            discard: async_nats::jetstream::stream::DiscardPolicy::New,
            discard_new_per_subject: true,
            ..Default::default()
        };

        self.jetstream
            .create_or_update_stream(stream_config)
            .await
            .map_err(|e| NatsError::StreamCreationFailed {
                stream: stream_name.to_string(),
                source: e,
            })?;

        let stream = self.jetstream.get_stream(stream_name).await.map_err(|e| {
            NatsError::StreamNotFound {
                stream: stream_name.to_string(),
                source: e,
            }
        })?;

        info!(stream = %stream_name, ?subjects, "stream created or updated");

        let mut cache = self.streams.write().await;
        cache.insert(stream_name.to_string(), stream.clone());
        Ok(stream)
    }

    pub async fn get_stream(&self, stream_name: &str) -> Result<Stream, NatsError> {
        {
            let cache = self.streams.read().await;
            if let Some(stream) = cache.get(stream_name) {
                return Ok(stream.clone());
            }
        }

        let mut cache = self.streams.write().await;
        if let Some(stream) = cache.get(stream_name) {
            return Ok(stream.clone());
        }

        let stream = self.jetstream.get_stream(stream_name).await.map_err(|e| {
            NatsError::StreamNotFound {
                stream: stream_name.to_string(),
                source: e,
            }
        })?;

        cache.insert(stream_name.to_string(), stream.clone());
        Ok(stream)
    }

    pub async fn ensure_kv_bucket_exists(
        &self,
        bucket: &str,
        config: KvBucketConfig,
    ) -> Result<(), NatsError> {
        let kv_config = async_nats::jetstream::kv::Config {
            bucket: bucket.to_string(),
            limit_markers: config.limit_markers,
            ..Default::default()
        };

        let store = self
            .jetstream
            .create_or_update_key_value(kv_config)
            .await
            .map_err(|e| NatsError::KvBucket {
                bucket: bucket.to_string(),
                message: e.to_string(),
            })?;

        info!(bucket, "KV bucket ready");

        let mut cache = self.kv_stores.write().await;
        cache.insert(bucket.to_string(), store);
        Ok(())
    }

    async fn get_or_create_kv_store(&self, bucket: &str) -> Result<KvStore, NatsError> {
        {
            let cache = self.kv_stores.read().await;
            if let Some(store) = cache.get(bucket) {
                return Ok(store.clone());
            }
        }

        let mut cache = self.kv_stores.write().await;
        if let Some(store) = cache.get(bucket) {
            return Ok(store.clone());
        }

        let store = match self.jetstream.get_key_value(bucket).await {
            Ok(store) => store,
            Err(_) => self
                .jetstream
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: bucket.to_string(),
                    ..Default::default()
                })
                .await
                .map_err(|e| NatsError::KvBucket {
                    bucket: bucket.to_string(),
                    message: e.to_string(),
                })?,
        };

        cache.insert(bucket.to_string(), store.clone());
        Ok(store)
    }

    pub async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        let store = self.get_or_create_kv_store(bucket).await?;

        match store.entry(key).await {
            Ok(Some(entry)) => Ok(Some(KvEntry {
                key: entry.key,
                value: entry.value,
                revision: entry.revision,
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(NatsError::KvGet {
                bucket: bucket.to_string(),
                key: key.to_string(),
                message: e.to_string(),
            }),
        }
    }

    pub async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        let store = self.get_or_create_kv_store(bucket).await?;

        if options.create_only {
            let result = if let Some(ttl) = options.ttl {
                store.create_with_ttl(key, value, ttl).await
            } else {
                store.create(key, value).await
            };

            return match result {
                Ok(revision) => Ok(KvPutResult::Success(revision)),
                Err(e) if e.kind() == CreateErrorKind::AlreadyExists => {
                    Ok(KvPutResult::AlreadyExists)
                }
                Err(e) => Err(NatsError::KvPut {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    message: e.to_string(),
                }),
            };
        }

        if let Some(rev) = options.expected_revision {
            let result = store.update(key, value, rev).await;
            return match result {
                Ok(revision) => Ok(KvPutResult::Success(revision)),
                Err(e) if e.kind() == UpdateErrorKind::WrongLastRevision => {
                    Ok(KvPutResult::RevisionMismatch)
                }
                Err(e) => Err(NatsError::KvPut {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    message: e.to_string(),
                }),
            };
        }
        let result = store.put(key, value).await;
        match result {
            Ok(revision) => Ok(KvPutResult::Success(revision)),
            Err(e) => Err(NatsError::KvPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                message: e.to_string(),
            }),
        }
    }

    pub async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        let store = self.get_or_create_kv_store(bucket).await?;

        store.delete(key).await.map_err(|e| NatsError::KvDelete {
            bucket: bucket.to_string(),
            key: key.to_string(),
            message: e.to_string(),
        })
    }

    pub async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        let store = self.get_or_create_kv_store(bucket).await?;

        let keys = store.keys().await.map_err(|e| NatsError::KvKeys {
            bucket: bucket.to_string(),
            message: e.to_string(),
        })?;

        let result: Result<Vec<String>, _> = keys.try_collect().await;
        result.map_err(|e| NatsError::KvKeys {
            bucket: bucket.to_string(),
            message: e.to_string(),
        })
    }
}
