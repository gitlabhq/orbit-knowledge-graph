use std::collections::HashMap;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use indexer::orchestrator::dispatch::enabled_namespaces::ENABLED_NAMESPACE_TABLE;
use tonic::Status;

use crate::status_query::{QueryCache, fetch_status_query_batches, map_column_extraction_error};

static ENABLED_AT_SQL: LazyLock<String> = LazyLock::new(|| {
    let version = ontology::siphon_version_column();
    let deleted = ontology::siphon_deleted_column();
    format!(
        "SELECT root_namespace_id, min(created_at) AS enabled_at \
           FROM (SELECT root_namespace_id, argMax(created_at, {version}) AS created_at \
                   FROM {ENABLED_NAMESPACE_TABLE} \
                  WHERE root_namespace_id IN {{roots:Array(Int64)}} \
                  GROUP BY root_namespace_id, id \
                 HAVING argMax({deleted}, {version}) = false) \
          GROUP BY root_namespace_id"
    )
});

pub async fn read_enabled_at(
    datalake: &ArrowClickHouseClient,
    roots: &[i64],
) -> Result<HashMap<i64, DateTime<Utc>>, Status> {
    if roots.is_empty() {
        return Ok(HashMap::new());
    }

    let batches = fetch_status_query_batches(
        datalake,
        &ENABLED_AT_SQL,
        "enabled namespaces",
        QueryCache::Skip,
        |query| query.param("roots", roots),
    )
    .await?;

    let roots = i64::extract_column(&batches, 0).map_err(map_column_extraction_error)?;
    let enabled_at =
        DateTime::<Utc>::extract_column(&batches, 1).map_err(map_column_extraction_error)?;
    Ok(roots.into_iter().zip(enabled_at).collect())
}
