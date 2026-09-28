use std::collections::HashMap;

use chrono::{DateTime, TimeDelta, Utc};
use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use indexer::checkpoint::NAMESPACE_KEY_PREFIX;
use orbit_migrations::execute::CHECKPOINT_TABLE;
use orbit_migrations::version::prefixed_table_name;
use tonic::Status;

use super::phase::Phase;
use crate::active_schema::SchemaSnapshot;
use crate::status_query::{QueryCache, fetch_status_query_batches, map_column_extraction_error};

const MAX_SDLC_ATTEMPTS: i64 = 5;
// The hourly sweep retries a dead run, so wait for two sweeps.
const STALE_AFTER: TimeDelta = TimeDelta::hours(2);

// Not `FINAL`: until a merge, a completed row still counts after an overlapping run's late page write.
const PLAN_CHECKPOINTS_SQL: &str = "\
SELECT root, plan, \
       toBool(isNotNull(maxIf(indexed_at, NOT _deleted AND _version >= tombstoned_at))) AS completed, \
       argMax(attempts, _version) AS attempts, \
       max(_version) AS written_at \
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

struct PlanCheckpoint {
    completed: bool,
    attempts: i64,
    written_at: DateTime<Utc>,
}

#[derive(Default)]
pub struct PlanCheckpoints {
    by_plan: HashMap<String, PlanCheckpoint>,
}

impl PlanCheckpoints {
    pub fn get_plan_phase(&self, plan: &str, now: DateTime<Utc>) -> Phase {
        let Some(checkpoint) = self.by_plan.get(plan) else {
            return Phase::NotStarted;
        };

        if checkpoint.completed {
            Phase::Ready
        } else if checkpoint.attempts >= MAX_SDLC_ATTEMPTS
            || now - checkpoint.written_at > STALE_AFTER
        {
            Phase::Error
        } else {
            Phase::Syncing
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
        PLAN_CHECKPOINTS_SQL,
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
    let attempts = i64::extract_column(&batches, 3).map_err(map_column_extraction_error)?;
    let written_at =
        DateTime::<Utc>::extract_column(&batches, 4).map_err(map_column_extraction_error)?;

    let mut checkpoints: HashMap<i64, PlanCheckpoints> = HashMap::new();
    for ((((root, plan), completed), attempts), written_at) in roots
        .into_iter()
        .zip(plans)
        .zip(completed)
        .zip(attempts)
        .zip(written_at)
    {
        let checkpoint = PlanCheckpoint {
            completed,
            attempts,
            written_at,
        };
        checkpoints
            .entry(root)
            .or_default()
            .by_plan
            .insert(plan, checkpoint);
    }
    Ok(checkpoints)
}
