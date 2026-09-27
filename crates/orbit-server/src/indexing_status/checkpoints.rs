use std::collections::HashMap;

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use indexer::checkpoint::NAMESPACE_KEY_PREFIX;
use orbit_migrations::execute::CHECKPOINT_TABLE;
use orbit_migrations::version::prefixed_table_name;
use tonic::Status;

use super::phase::Phase;
use crate::active_schema::SchemaSnapshot;
use crate::status_query::{QueryCache, fetch_status_query_batches, map_column_extraction_error};

// Not `FINAL`: until a merge, a completed row still counts after an overlapping run's late page write.
const PLAN_COMPLETIONS_SQL: &str = "\
SELECT root, plan, \
       toBool(isNotNull(maxIf(indexed_at, NOT _deleted AND _version >= tombstoned_at))) AS completed \
  FROM (SELECT *, \
               toInt64OrZero(splitByChar('.', key)[2]) AS root, \
               splitByChar('.', key)[3] AS plan, \
               maxIf(_version, _deleted) OVER (PARTITION BY key) AS tombstoned_at \
          FROM {table:Identifier} \
         WHERE startsWith(key, {namespace_prefix:String}) \
           AND length(splitByChar('.', key)) = 3 \
           AND root IN {roots:Array(Int64)}) \
 GROUP BY key, root, plan \
HAVING argMax(_deleted, _version) = false";

#[derive(Default)]
pub struct PlanCheckpoints {
    completed_by_plan: HashMap<String, bool>,
}

impl PlanCheckpoints {
    pub fn get_plan_phase(&self, plan: &str) -> Phase {
        match self.completed_by_plan.get(plan) {
            Some(true) => Phase::Ready,
            Some(false) => Phase::Syncing,
            None => Phase::NotStarted,
        }
    }
}

pub async fn read_plan_checkpoints(
    client: &ArrowClickHouseClient,
    schema: &SchemaSnapshot,
    roots: &[i64],
) -> Result<HashMap<i64, PlanCheckpoints>, Status> {
    if roots.is_empty() {
        return Ok(HashMap::new());
    }

    let table = prefixed_table_name(CHECKPOINT_TABLE, schema.migration_version);
    let batches = fetch_status_query_batches(
        client,
        PLAN_COMPLETIONS_SQL,
        "plan checkpoints",
        QueryCache::Skip,
        |query| {
            query
                .param("table", &table)
                .param("namespace_prefix", NAMESPACE_KEY_PREFIX)
                .param("roots", roots)
        },
    )
    .await?;

    let roots = i64::extract_column(&batches, 0).map_err(map_column_extraction_error)?;
    let plans = String::extract_column(&batches, 1).map_err(map_column_extraction_error)?;
    let completed = bool::extract_column(&batches, 2).map_err(map_column_extraction_error)?;

    let mut checkpoints: HashMap<i64, PlanCheckpoints> = HashMap::new();
    for ((root, plan), completed) in roots.into_iter().zip(plans).zip(completed) {
        checkpoints
            .entry(root)
            .or_default()
            .completed_by_plan
            .insert(plan, completed);
    }
    Ok(checkpoints)
}
