//! Small typed helpers over `NatsServices` for reading and writing JSON
//! snapshots in the `indexing_progress` bucket. Centralizes the
//! bucket/serialization/error boilerplate duplicated across
//! `ProgressWriter`, `CodeProgressWriter`, and the namespace deletion handler.

use bytes::Bytes;
use gkg_server_config::indexing_progress::INDEXING_PROGRESS_BUCKET;
use serde::{Serialize, de::DeserializeOwned};
use tracing::warn;

use crate::handler::HandlerError;
use crate::nats::{KvPutOptions, NatsServices};

/// Read-modify-write helper for a JSON snapshot. Loads `key` (or `T::default()`
/// if absent/corrupt), passes it to `mutate`, then writes it back. Centralizes
/// the read/mutate/serialize/put dance duplicated across the two writers.
pub(crate) async fn update_json<T, F>(
    nats: &dyn NatsServices,
    key: &str,
    mutate: F,
) -> Result<(), HandlerError>
where
    T: DeserializeOwned + Serialize + Default,
    F: FnOnce(&mut T),
{
    let mut value = read_json::<T>(nats, key).await.unwrap_or_default();
    mutate(&mut value);
    write_json(nats, key, &value).await
}

/// Reads a JSON snapshot from the progress bucket. Returns `None` on a miss,
/// a NATS error, or a deserialization failure. Callers that need to
/// distinguish those cases should use `NatsServices::kv_get` directly.
pub(crate) async fn read_json<T: DeserializeOwned>(
    nats: &dyn NatsServices,
    key: &str,
) -> Option<T> {
    let entry = nats
        .kv_get(INDEXING_PROGRESS_BUCKET, key)
        .await
        .ok()
        .flatten()?;
    serde_json::from_slice(&entry.value).ok()
}

/// Serializes `value` as JSON and writes it to the progress bucket. Returns
/// `HandlerError::Processing` on either serialization or NATS failure.
pub(crate) async fn write_json<T: Serialize>(
    nats: &dyn NatsServices,
    key: &str,
    value: &T,
) -> Result<(), HandlerError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|e| HandlerError::Processing(format!("serialize {key}: {e}")))?;
    nats.kv_put(
        INDEXING_PROGRESS_BUCKET,
        key,
        Bytes::from(bytes),
        KvPutOptions::default(),
    )
    .await
    .map_err(|e| HandlerError::Processing(format!("KV put {key}: {e}")))?;
    Ok(())
}

/// Deletes `key` from the progress bucket. Logs a warning on failure and
/// swallows the error — cleanup is best-effort and must not block the
/// caller's primary operation (e.g. namespace deletion).
pub(crate) async fn delete_best_effort(nats: &dyn NatsServices, label: &str, key: &str) {
    if let Err(e) = nats.kv_delete(INDEXING_PROGRESS_BUCKET, key).await {
        warn!(key = %key, error = %e, "failed to delete {label} KV key");
    }
}
