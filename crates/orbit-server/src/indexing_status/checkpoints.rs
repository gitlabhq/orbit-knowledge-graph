use std::collections::HashMap;

use chrono::{DateTime, TimeDelta, Utc};
use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use indexer::checkpoint::NAMESPACE_KEY_PREFIX;
use ontology::pipelines::PipelineDescriptor;
use orbit_migrations::execute::CHECKPOINT_TABLE;
use orbit_migrations::version::prefixed_table_name;
use tonic::Status;

use super::phase::Phase;
use crate::active_schema::SchemaSnapshot;
use crate::status_query::{
    fetch_status_query_batches, map_column_extraction_error, uncached_query_config,
};

const MAX_SDLC_ATTEMPTS: i64 = 5;
// The hourly sweep retries a dead run, so wait for two sweeps.
const STALE_AFTER: TimeDelta = TimeDelta::hours(2);

// Not `FINAL`: until a merge, a completed row still counts after an overlapping run's late page write.
const PLAN_CHECKPOINTS_SQL: &str = "\
SELECT root, plan, \
       maxIf(indexed_at, NOT _deleted AND _version >= tombstoned_at) AS last_completed_at, \
       argMax(attempts, _version) AS attempts, \
       argMax(watermark, _version) AS watermark, \
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
    indexed_at: Option<DateTime<Utc>>,
    attempts: i64,
    watermark: DateTime<Utc>,
    written_at: DateTime<Utc>,
}

#[derive(Default)]
pub struct PlanCheckpoints {
    by_plan: HashMap<String, PlanCheckpoint>,
}

impl PlanCheckpoints {
    /// A run that completed with nothing indexed because the source table does not exist.
    pub fn is_source_unavailable(&self, plan: &str) -> bool {
        self.by_plan.get(plan).is_some_and(|checkpoint| {
            checkpoint.indexed_at.is_some() && checkpoint.watermark == DateTime::<Utc>::UNIX_EPOCH
        })
    }

    pub fn get_plan_phase(&self, plan: &str, now: DateTime<Utc>) -> Phase {
        let Some(checkpoint) = self.by_plan.get(plan) else {
            return Phase::NotStarted;
        };

        if checkpoint.indexed_at.is_some() {
            Phase::Ready
        } else if checkpoint.attempts >= MAX_SDLC_ATTEMPTS
            || now - checkpoint.written_at > STALE_AFTER
        {
            Phase::Error
        } else {
            Phase::Syncing
        }
    }

    pub fn get_last_completed_at(&self, plans: &[&PipelineDescriptor]) -> Option<DateTime<Utc>> {
        let indexed_at: Option<Vec<DateTime<Utc>>> = plans
            .iter()
            .map(|plan| self.by_plan.get(&plan.name)?.indexed_at)
            .collect();

        indexed_at?.into_iter().min()
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
    let config = uncached_query_config();
    let batches = fetch_status_query_batches(
        client,
        PLAN_CHECKPOINTS_SQL,
        "plan checkpoints",
        &config,
        |query| {
            query
                .param("table", &table)
                .param("namespace_prefix", NAMESPACE_KEY_PREFIX)
                .param("roots", roots)
        },
    )
    .await?;

    let mut checkpoints: HashMap<i64, PlanCheckpoints> = roots
        .iter()
        .map(|root| (*root, PlanCheckpoints::default()))
        .collect();

    let roots = i64::extract_column(&batches, 0).map_err(map_column_extraction_error)?;
    let plans = String::extract_column(&batches, 1).map_err(map_column_extraction_error)?;
    let indexed_at = Option::<DateTime<Utc>>::extract_column(&batches, 2)
        .map_err(map_column_extraction_error)?;
    let attempts = i64::extract_column(&batches, 3).map_err(map_column_extraction_error)?;
    let watermarks =
        DateTime::<Utc>::extract_column(&batches, 4).map_err(map_column_extraction_error)?;
    let written_at =
        DateTime::<Utc>::extract_column(&batches, 5).map_err(map_column_extraction_error)?;

    for (((((root, plan), indexed_at), attempts), watermark), written_at) in roots
        .into_iter()
        .zip(plans)
        .zip(indexed_at)
        .zip(attempts)
        .zip(watermarks)
        .zip(written_at)
    {
        let checkpoint = PlanCheckpoint {
            indexed_at,
            attempts,
            watermark,
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
