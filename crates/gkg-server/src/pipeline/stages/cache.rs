use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use indexer::nats::{KvBucketConfig, KvPutOptions, NatsBroker};
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::ExecutionOutput;

use super::execution::ClickHouseExecutor;

const QUERY_CACHE_BUCKET: &str = "gkg_query_cache";
const MAX_CACHE_VALUE_BYTES: usize = 1024 * 1024; // 1 MiB KV limit

/// Wraps `ClickHouseExecutor` with a NATS KV cache layer.
/// On hit, returns cached Arrow batches without touching ClickHouse.
/// On miss, delegates to ClickHouse and stores the result.
#[derive(Clone)]
pub struct CachedExecutor;

/// Ensures the KV bucket used for query caching exists.
pub async fn ensure_query_cache_bucket(
    broker: &NatsBroker,
) -> Result<(), indexer::nats::NatsError> {
    broker
        .ensure_kv_bucket_exists(QUERY_CACHE_BUCKET, KvBucketConfig::with_per_message_ttl())
        .await
}

impl PipelineStage for CachedExecutor {
    type Input = ();
    type Output = ExecutionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let broker = ctx.server_extensions.get::<Arc<NatsBroker>>().cloned();

        let Some(broker) = broker else {
            return ClickHouseExecutor.execute(ctx, obs).await;
        };

        let compiled = ctx.compiled()?;
        let config = &compiled.base.query_config;
        let cache_enabled = config.graph_query_cache_enabled.unwrap_or(false);
        if !cache_enabled {
            return ClickHouseExecutor.execute(ctx, obs).await;
        }

        let ttl_secs = config.graph_query_cache_ttl.unwrap_or(60);

        let security_ctx = ctx.security_context()?;
        let cache_key = compute_cache_key(
            &ctx.query_json,
            security_ctx.org_id,
            &security_ctx.traversal_paths,
        );

        let start = Instant::now();
        match broker.kv_get(QUERY_CACHE_BUCKET, &cache_key).await {
            Ok(Some(entry)) => match deserialize_batches(&entry.value) {
                Ok(batches) => {
                    let result_context = compiled.base.result_context.clone();
                    let elapsed = start.elapsed();
                    debug!(
                        cache_key,
                        elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                        batches = batches.len(),
                        "query cache hit"
                    );
                    obs.executed(elapsed, batches.len());
                    return Ok(ExecutionOutput {
                        batches,
                        result_context,
                    });
                }
                Err(e) => {
                    warn!(cache_key, error = %e, "failed to deserialize cached result, falling through");
                }
            },
            Ok(None) => {
                debug!(cache_key, "query cache miss");
            }
            Err(e) => {
                warn!(cache_key, error = %e, "query cache lookup failed, falling through");
            }
        }

        let output = ClickHouseExecutor.execute(ctx, obs).await?;

        match serialize_batches(&output.batches) {
            Err(e) => {
                warn!(cache_key, error = %e, "failed to serialize query result for cache, skipping store");
            }
            Ok(data) if data.len() <= MAX_CACHE_VALUE_BYTES => {
                let broker = Arc::clone(&broker);
                let key = cache_key.clone();
                tokio::spawn(async move {
                    let options = KvPutOptions::with_ttl(Duration::from_secs(u64::from(ttl_secs)));
                    if let Err(e) = broker
                        .kv_put(QUERY_CACHE_BUCKET, &key, data.into(), options)
                        .await
                    {
                        warn!(key, error = %e, "failed to store query result in cache");
                    } else {
                        debug!(key, "stored query result in cache");
                    }
                });
            }
            Ok(data) => {
                debug!(
                    cache_key,
                    size = data.len(),
                    limit = MAX_CACHE_VALUE_BYTES,
                    "skipped cache store: result too large"
                );
            }
        }

        Ok(output)
    }
}

/// Cache key from the query JSON and security context.
/// JSON is canonicalized via RFC 8785 (JCS) -- keys sorted, no whitespace.
/// The `cursor` field is stripped so all pages of the same query share one
/// cache entry (the full LIMIT window is cached, cursor slicing happens
/// after authorization in userspace).
/// Traversal paths are sorted to ensure deterministic keys regardless
/// of the order Rails sends them.
fn compute_cache_key(query_json: &str, org_id: i64, traversal_paths: &[String]) -> String {
    let canonical = serde_json::from_str::<serde_json::Value>(query_json)
        .ok()
        .map(|mut v| {
            if let Some(obj) = v.as_object_mut() {
                obj.remove("cursor");
            }
            v
        })
        .and_then(|v| serde_json_canonicalizer::to_string(&v).ok())
        .unwrap_or_else(|| query_json.to_string());

    let mut sorted_paths = traversal_paths.to_vec();
    sorted_paths.sort();

    let mut hasher = Sha256::new();
    hasher.update(org_id.to_le_bytes());
    hasher.update((canonical.len() as u64).to_le_bytes());
    hasher.update(canonical.as_bytes());
    for path in &sorted_paths {
        hasher.update((path.len() as u64).to_le_bytes());
        hasher.update(path.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn serialize_batches(batches: &[RecordBatch]) -> Result<Vec<u8>, PipelineError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).map_err(PipelineError::custom)?;
        for batch in batches {
            writer.write(batch).map_err(PipelineError::custom)?;
        }
        writer.finish().map_err(PipelineError::custom)?;
    }
    Ok(buf)
}

fn deserialize_batches(data: &[u8]) -> Result<Vec<RecordBatch>, PipelineError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None).map_err(PipelineError::custom)?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(PipelineError::custom)?);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn round_trip_serialize_deserialize() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        let data = serialize_batches(std::slice::from_ref(&batch)).unwrap();
        let restored = deserialize_batches(&data).unwrap();

        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].num_rows(), 3);
        assert_eq!(restored[0].num_columns(), 1);
    }

    #[test]
    fn empty_batches_round_trip() {
        let data = serialize_batches(&[]).unwrap();
        let restored = deserialize_batches(&data).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn cache_key_deterministic() {
        let paths = vec!["1/".to_string()];
        let k1 = compute_cache_key(r#"{"a":1}"#, 1, &paths);
        let k2 = compute_cache_key(r#"{"a":1}"#, 1, &paths);
        assert_eq!(k1, k2);
    }

    #[test]
    fn cache_key_differs_for_different_queries() {
        let paths = vec!["1/".to_string()];
        let k1 = compute_cache_key(r#"{"a":1}"#, 1, &paths);
        let k2 = compute_cache_key(r#"{"a":2}"#, 1, &paths);
        assert_ne!(k1, k2);
    }

    #[test]
    fn cache_key_normalizes_whitespace() {
        let paths = vec!["1/".to_string()];
        let k1 = compute_cache_key(r#"{"a": 1}"#, 1, &paths);
        let k2 = compute_cache_key(r#"{  "a" :  1  }"#, 1, &paths);
        assert_eq!(k1, k2);
    }

    #[test]
    fn cache_key_normalizes_key_order() {
        let paths = vec!["1/".to_string()];
        let k1 = compute_cache_key(r#"{"a":1,"b":2}"#, 1, &paths);
        let k2 = compute_cache_key(r#"{"b":2,"a":1}"#, 1, &paths);
        assert_eq!(k1, k2);
    }

    #[test]
    fn cache_key_differs_for_different_orgs() {
        let paths = vec!["1/".to_string()];
        let k1 = compute_cache_key(r#"{"a":1}"#, 1, &paths);
        let k2 = compute_cache_key(r#"{"a":1}"#, 2, &paths);
        assert_ne!(k1, k2);
    }

    #[test]
    fn cache_key_differs_for_different_traversal_paths() {
        let k1 = compute_cache_key(r#"{"a":1}"#, 1, &["1/".to_string()]);
        let k2 = compute_cache_key(r#"{"a":1}"#, 1, &["1/2/".to_string()]);
        assert_ne!(k1, k2);
    }

    #[test]
    fn cache_key_ignores_cursor_field() {
        let paths = vec!["1/".to_string()];
        let k1 = compute_cache_key(
            r#"{"query_type":"search","limit":100,"cursor":{"offset":0,"page_size":20}}"#,
            1,
            &paths,
        );
        let k2 = compute_cache_key(
            r#"{"query_type":"search","limit":100,"cursor":{"offset":40,"page_size":20}}"#,
            1,
            &paths,
        );
        let k3 = compute_cache_key(r#"{"query_type":"search","limit":100}"#, 1, &paths);
        assert_eq!(k1, k2, "different cursor offsets should produce same key");
        assert_eq!(k1, k3, "with and without cursor should produce same key");
    }

    #[test]
    fn cache_key_stable_regardless_of_path_order() {
        let k1 = compute_cache_key(r#"{"a":1}"#, 1, &["1/".into(), "1/2/".into()]);
        let k2 = compute_cache_key(r#"{"a":1}"#, 1, &["1/2/".into(), "1/".into()]);
        assert_eq!(k1, k2);
    }
}
