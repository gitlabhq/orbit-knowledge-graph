use std::collections::HashMap;

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use indexer::checkpoint::namespace_position_key;
use orbit_migrations::execute::CHECKPOINT_TABLE;
use orbit_migrations::version::prefixed_table_name;
use tonic::Status;

use super::phase::Phase;
use crate::active_schema::SchemaSnapshot;
use crate::status_query::{
    bind_prefixes, extraction_error, fetch_status_rows, starts_with_any_sql,
};

#[derive(Default)]
pub struct PlanCheckpoints {
    completed_by_plan: HashMap<String, bool>,
}

impl PlanCheckpoints {
    pub fn phase_of(&self, plan: &str) -> Phase {
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
    let prefixes: Vec<String> = roots
        .iter()
        .map(|root| format!("{}.", namespace_position_key(*root)))
        .collect();
    let sql = plan_completions_sql(prefixes.len());
    let batches = fetch_status_rows(client, &sql, "plan checkpoints", |query| {
        bind_prefixes(query.param("table", &table), "prefix", &prefixes)
    })
    .await?;

    let roots = i64::extract_column(&batches, 0).map_err(extraction_error)?;
    let plans = String::extract_column(&batches, 1).map_err(extraction_error)?;
    let completed = bool::extract_column(&batches, 2).map_err(extraction_error)?;

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

// Not `FINAL`: a late page write from an overlapping run would hide the completion.
fn plan_completions_sql(prefix_count: usize) -> String {
    let key_in_roots = starts_with_any_sql("key", "prefix", prefix_count);
    format!(
        "SELECT toInt64(splitByChar('.', key)[2]) AS root, \
                splitByChar('.', key)[3] AS plan, \
                toBool(isNotNull(maxIf(indexed_at, NOT _deleted AND _version >= tombstoned_at))) AS completed \
           FROM (SELECT *, maxIf(_version, _deleted) OVER (PARTITION BY key) AS tombstoned_at \
                   FROM {{table:Identifier}} \
                  WHERE {key_in_roots} AND length(splitByChar('.', key)) = 3) \
          GROUP BY key \
         HAVING argMax(_deleted, _version) = false"
    )
}
