//! Message deduplication via NATS KV store.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use etl_engine::nats::{KvPutOptions, NatsServices};

use crate::error::MailboxError;

const DEDUP_BUCKET: &str = "mailbox-dedup";
const DEDUP_TTL: Duration = Duration::from_secs(24 * 60 * 60);

pub struct DeduplicationStore {
    nats: Arc<dyn NatsServices>,
}

impl DeduplicationStore {
    pub fn new(nats: Arc<dyn NatsServices>) -> Self {
        Self { nats }
    }

    pub async fn is_duplicate(&self, message_id: &str) -> Result<bool, MailboxError> {
        let key = format!("msg_{}", message_id);

        let exists = self
            .nats
            .kv_get(DEDUP_BUCKET, &key)
            .await
            .map_err(|e| MailboxError::processing(format!("failed to check dedup: {}", e)))?
            .is_some();

        Ok(exists)
    }

    pub async fn mark_processed(&self, message_id: &str) -> Result<(), MailboxError> {
        let key = format!("msg_{}", message_id);

        let options = KvPutOptions::with_ttl(DEDUP_TTL);

        self.nats
            .kv_put(DEDUP_BUCKET, &key, Bytes::new(), options)
            .await
            .map_err(|e| MailboxError::processing(format!("failed to mark processed: {}", e)))?;

        Ok(())
    }
}
