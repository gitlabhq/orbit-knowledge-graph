use std::fmt::Write as _;
use std::marker::PhantomData;

use async_nats::jetstream::kv::{self, Operation, UpdateErrorKind};
use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::task::WorkerKey;
use crate::versioned::{DeleteOutcome, Revision, Versioned, VersionedStore, WriteOutcome};

pub(super) struct NatsVersionedStore<V> {
    bucket: kv::Store,
    prefix: &'static str,
    value: PhantomData<fn() -> V>,
}

impl<V> NatsVersionedStore<V> {
    pub(super) fn new(bucket: kv::Store, prefix: &'static str) -> Self {
        Self {
            bucket,
            prefix,
            value: PhantomData,
        }
    }

    fn name(&self, key: &WorkerKey) -> String {
        let mut name = format!("{}.{}.", self.prefix, key.worker);
        for byte in key.key.to_string().bytes() {
            let _ = write!(name, "{byte:02x}");
        }
        name
    }
}

#[async_trait]
impl<V> VersionedStore<V> for NatsVersionedStore<V>
where
    V: Serialize + DeserializeOwned + Send + Sync,
{
    async fn read(&self, key: &WorkerKey) -> anyhow::Result<Versioned<V>> {
        let Some(entry) = self.bucket.entry(self.name(key)).await? else {
            return Ok(Versioned {
                value: None,
                revision: Revision(0),
            });
        };
        let value = match entry.operation {
            Operation::Put => Some(serde_json::from_slice(&entry.value)?),
            Operation::Delete | Operation::Purge => None,
        };
        Ok(Versioned {
            value,
            revision: Revision(entry.revision),
        })
    }

    async fn write(
        &self,
        key: &WorkerKey,
        expected: Revision,
        value: &V,
    ) -> anyhow::Result<WriteOutcome> {
        let bytes = serde_json::to_vec(value)?;
        let updated = self
            .bucket
            .update(self.name(key), bytes.into(), expected.0)
            .await;
        match updated {
            Ok(revision) => Ok(WriteOutcome::Written(Revision(revision))),
            Err(error) if error.kind() == UpdateErrorKind::WrongLastRevision => {
                Ok(WriteOutcome::Conflict)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn delete(&self, key: &WorkerKey, expected: Revision) -> anyhow::Result<DeleteOutcome> {
        let deleted = self
            .bucket
            .delete_expect_revision(self.name(key), Some(expected.0))
            .await;
        match deleted {
            Ok(()) => Ok(DeleteOutcome::Deleted),
            Err(error) if error.kind() == UpdateErrorKind::WrongLastRevision => {
                Ok(DeleteOutcome::Conflict)
            }
            Err(error) => Err(error.into()),
        }
    }
}
