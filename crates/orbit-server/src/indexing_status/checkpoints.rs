use std::collections::HashMap;

use clickhouse_client::ArrowClickHouseClient;
use indexer::indexing_status::{
    PLAN_CHECKPOINTS_SQL, PlanCheckpoints, bind_plan_checkpoints, parse_plan_checkpoints,
};
use tonic::Status;

use crate::active_schema::SchemaSnapshot;
use crate::status_query::{fetch_status_query_batches, map_column_extraction_error};

pub async fn read_plan_checkpoints(
    client: &ArrowClickHouseClient,
    schema: &SchemaSnapshot,
    roots: &[i64],
) -> Result<HashMap<i64, PlanCheckpoints>, Status> {
    if roots.is_empty() {
        return Ok(HashMap::new());
    }

    let batches =
        fetch_status_query_batches(client, PLAN_CHECKPOINTS_SQL, "plan checkpoints", |query| {
            bind_plan_checkpoints(query, schema.migration_version, roots)
        })
        .await?;
    parse_plan_checkpoints(roots, &batches).map_err(map_column_extraction_error)
}
