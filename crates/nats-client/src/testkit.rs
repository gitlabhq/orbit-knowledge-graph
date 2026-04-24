use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;

use crate::KvServices;
use crate::error::NatsError;
use crate::kv_types::{KvEntry, KvPutOptions, KvPutResult};

#[derive(Clone, Default)]
pub struct MockKvServices {
    stores: Arc<Mutex<HashMap<String, HashMap<String, MockKvEntry>>>>,
}

#[derive(Clone)]
struct MockKvEntry {
    value: Bytes,
    revision: u64,
}

impl MockKvServices {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, bucket: &str, key: &str) -> Option<Bytes> {
        let stores = self.stores.lock();
        stores
            .get(bucket)
            .and_then(|b| b.get(key))
            .map(|e| e.value.clone())
    }

    pub fn set(&self, bucket: &str, key: &str, value: Bytes) {
        let mut stores = self.stores.lock();
        let bucket_store = stores.entry(bucket.to_string()).or_default();
        let revision = bucket_store.get(key).map(|e| e.revision + 1).unwrap_or(1);
        bucket_store.insert(key.to_string(), MockKvEntry { value, revision });
    }
}

#[async_trait]
impl KvServices for MockKvServices {
    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        let stores = self.stores.lock();
        let entry = stores
            .get(bucket)
            .and_then(|b| b.get(key))
            .map(|e| KvEntry {
                key: key.to_string(),
                value: e.value.clone(),
                revision: e.revision,
            });
        Ok(entry)
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        let mut stores = self.stores.lock();
        let bucket_store = stores.entry(bucket.to_string()).or_default();

        let existing = bucket_store.get(key);

        if options.create_only && existing.is_some() {
            return Ok(KvPutResult::AlreadyExists);
        }

        if let Some(expected_rev) = options.expected_revision {
            match existing {
                Some(e) if e.revision != expected_rev => {
                    return Ok(KvPutResult::RevisionMismatch);
                }
                None => {
                    return Ok(KvPutResult::RevisionMismatch);
                }
                _ => {}
            }
        }

        let revision = existing.map(|e| e.revision + 1).unwrap_or(1);
        bucket_store.insert(key.to_string(), MockKvEntry { value, revision });

        Ok(KvPutResult::Success(revision))
    }

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        let mut stores = self.stores.lock();
        if let Some(bucket_store) = stores.get_mut(bucket) {
            bucket_store.remove(key);
        }
        Ok(())
    }

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        let stores = self.stores.lock();
        let keys = stores
            .get(bucket)
            .map(|b| b.keys().cloned().collect())
            .unwrap_or_default();
        Ok(keys)
    }
}
