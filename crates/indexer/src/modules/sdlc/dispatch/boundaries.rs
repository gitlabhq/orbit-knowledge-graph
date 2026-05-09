use arrow::array::Array;
use bytes::Bytes;
use nats_client::KvPutOptions;

use crate::clickhouse::ArrowClickHouseClient;
use crate::nats::NatsServices;
use crate::scheduler::TaskError;
use crate::topic::IndexingScope;

pub const PARTITION_BOUNDARIES_BUCKET: &str = "partition_boundaries";

pub fn boundaries_key(entity_kind: &str, scope: &IndexingScope) -> String {
    match scope {
        IndexingScope::Global => format!("global.{entity_kind}"),
        IndexingScope::Namespace { namespace_id, .. } => {
            format!("ns.{namespace_id}.{entity_kind}")
        }
    }
}

pub async fn compute_boundaries(
    datalake: &ArrowClickHouseClient,
    source_table: &str,
    partition_column: &str,
    partition_count: u32,
    scope: &IndexingScope,
) -> Result<Vec<String>, TaskError> {
    if partition_count <= 1 {
        return Ok(vec![]);
    }

    let quantile_positions: Vec<String> = (1..partition_count)
        .map(|i| format!("{}", i as f64 / partition_count as f64))
        .collect();
    let quantile_list = quantile_positions.join(", ");

    let scope_filter = match scope {
        IndexingScope::Global => "1=1".to_string(),
        IndexingScope::Namespace { traversal_path, .. } => {
            format!("startsWith(traversal_path, '{traversal_path}')")
        }
    };

    let sql = format!(
        "SELECT quantilesExactExclusive({quantile_list})({partition_column}) \
         FROM {source_table} \
         WHERE {scope_filter}"
    );

    let batches = datalake
        .query(&sql)
        .fetch_arrow()
        .await
        .map_err(TaskError::new)?;

    let batch = match batches.into_iter().next() {
        Some(batch) if batch.num_rows() > 0 => batch,
        _ => return Ok(vec![]),
    };

    let column = batch.column(0);
    let list_array = column
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or_else(|| TaskError::new("expected ListArray from quantile query"))?;

    if list_array.is_empty() || list_array.is_null(0) {
        return Ok(vec![]);
    }

    let values = list_array.value(0);
    let float_array = values
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .ok_or_else(|| TaskError::new("expected Float64Array inside quantile result"))?;

    let boundaries: Vec<String> = float_array
        .iter()
        .filter_map(|v| v.map(|f| format!("{}", f.floor() as i64)))
        .collect();

    Ok(boundaries)
}

pub async fn load_boundaries(
    nats: &dyn NatsServices,
    key: &str,
) -> Result<Option<Vec<String>>, TaskError> {
    let entry = nats
        .kv_get(PARTITION_BOUNDARIES_BUCKET, key)
        .await
        .map_err(TaskError::new)?;

    match entry {
        Some(entry) => {
            let boundaries: Vec<String> =
                serde_json::from_slice(&entry.value).map_err(TaskError::new)?;
            Ok(Some(boundaries))
        }
        None => Ok(None),
    }
}

pub async fn save_boundaries(
    nats: &dyn NatsServices,
    key: &str,
    boundaries: &[String],
) -> Result<(), TaskError> {
    let json = serde_json::to_vec(boundaries).map_err(TaskError::new)?;
    nats.kv_put(
        PARTITION_BOUNDARIES_BUCKET,
        key,
        Bytes::from(json),
        KvPutOptions::default(),
    )
    .await
    .map_err(TaskError::new)?;
    Ok(())
}

pub async fn delete_boundaries(nats: &dyn NatsServices, key: &str) -> Result<(), TaskError> {
    nats.kv_delete(PARTITION_BOUNDARIES_BUCKET, key)
        .await
        .map_err(TaskError::new)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundaries_key_global() {
        let key = boundaries_key("User", &IndexingScope::Global);
        assert_eq!(key, "global.User");
    }

    #[test]
    fn boundaries_key_namespaced() {
        let scope = IndexingScope::Namespace {
            namespace_id: 100,
            traversal_path: "42/100/".to_string(),
        };
        let key = boundaries_key("MergeRequest", &scope);
        assert_eq!(key, "ns.100.MergeRequest");
    }

    #[tokio::test]
    async fn load_returns_none_when_key_missing() {
        let nats = crate::testkit::MockNatsServices::new();
        let result = load_boundaries(&nats, "nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn save_then_load_roundtrip() {
        let nats = crate::testkit::MockNatsServices::new();
        let boundaries = vec!["25000000".to_string(), "50000000".to_string()];

        save_boundaries(&nats, "global.MergeRequest", &boundaries)
            .await
            .unwrap();

        let loaded = load_boundaries(&nats, "global.MergeRequest").await.unwrap();
        assert_eq!(loaded, Some(boundaries));
    }

    #[tokio::test]
    async fn delete_removes_boundaries() {
        let nats = crate::testkit::MockNatsServices::new();
        let boundaries = vec!["10".to_string()];

        save_boundaries(&nats, "global.User", &boundaries)
            .await
            .unwrap();

        delete_boundaries(&nats, "global.User").await.unwrap();

        let loaded = load_boundaries(&nats, "global.User").await.unwrap();
        assert!(loaded.is_none());
    }
}
