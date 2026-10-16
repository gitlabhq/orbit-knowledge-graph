use std::collections::HashMap;

use clickhouse_client::ArrowClickHouseClient;
use indexer::indexing_status::{PLAN_CHECKPOINTS_SQL, PlanCheckpoints, fetch_plan_checkpoints};
use tonic::Status;

use crate::active_schema::SchemaSnapshot;
use crate::status_query::status_query;

pub async fn read_plan_checkpoints(
    client: &ArrowClickHouseClient,
    schema: &SchemaSnapshot,
    roots: &[i64],
) -> Result<HashMap<i64, PlanCheckpoints>, Status> {
    if roots.is_empty() {
        return Ok(HashMap::new());
    }

    let query = status_query(client, PLAN_CHECKPOINTS_SQL, "plan checkpoints")?;
    fetch_plan_checkpoints(query, schema.migration_version, roots)
        .await
        .map_err(|e| Status::internal(format!("plan checkpoints: {e}")))
}
