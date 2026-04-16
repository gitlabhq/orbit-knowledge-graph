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
        let cache_key = compute_cache_key(&compiled.base.sql, &compiled.base.render());

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

        if let Ok(data) = serialize_batches(&output.batches) {
            if data.len() <= MAX_CACHE_VALUE_BYTES {
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
            } else {
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

fn compute_cache_key(sql: &str, rendered: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    hasher.update(b"|");
    hasher.update(rendered.as_bytes());
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
        let k1 = compute_cache_key("SELECT 1", "SELECT 1");
        let k2 = compute_cache_key("SELECT 1", "SELECT 1");
        assert_eq!(k1, k2);
    }

    #[test]
    fn cache_key_differs_for_different_queries() {
        let k1 = compute_cache_key("SELECT 1", "SELECT 1");
        let k2 = compute_cache_key("SELECT 2", "SELECT 2");
        assert_ne!(k1, k2);
    }
}
