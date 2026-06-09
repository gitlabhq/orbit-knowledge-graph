use std::collections::HashMap;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use tracing::debug;

use crate::handler::HandlerError;

use super::datalake::DatalakeQuery;
use super::plan::input::PlanInput;

/// Buckets the probe aims for; width is derived from the id range to hit this at
/// any scale. A fixed width would collapse a narrow id range into one bucket and
/// silently disable partitioning.
const TARGET_BUCKET_COUNT: i64 = 10_000;

/// A half-open `[lower, upper)` slice of the leading sort-key prefix; `None` is an
/// open end (first/last partition).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::modules::sdlc) struct PartitionAssignment {
    pub index: u32,
    pub total: u32,
    pub key_columns: Vec<String>,
    pub lower_bound: Option<Vec<String>>,
    pub upper_bound: Option<Vec<String>>,
}

impl PartitionAssignment {
    pub(crate) const CHECKPOINT_PREFIX: &str = ".p";

    pub fn position_suffix(&self) -> String {
        format!("{}{}of{}", Self::CHECKPOINT_PREFIX, self.index, self.total)
    }
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct PartitionStrategy {
    pub count: u32,
    pub key_columns: Vec<String>,
    pub datalake_table: String,
    pub min_rows: u64,
}

impl PartitionStrategy {
    pub async fn compute_ranges(
        &self,
        datalake: &dyn DatalakeQuery,
        traversal_path: Option<&str>,
    ) -> Result<Vec<PartitionAssignment>, HandlerError> {
        compute_partition_ranges(
            datalake,
            &self.datalake_table,
            &self.key_columns,
            self.count,
            self.min_rows,
            traversal_path,
        )
        .await
    }
}

pub(in crate::modules::sdlc) fn build_strategies(
    inputs: &PlanInput,
    overrides: &HashMap<String, u32>,
    min_rows: u64,
) -> HashMap<String, PartitionStrategy> {
    inputs
        .node_plans
        .iter()
        .filter_map(|node| {
            let count = *overrides.get(&node.name)?;
            if count <= 1 {
                return None;
            }
            let key_columns = partition_key_columns(&node.extract.order_by)?;
            Some((
                node.name.clone(),
                PartitionStrategy {
                    count,
                    key_columns,
                    datalake_table: node.extract.base_table.clone(),
                    min_rows,
                },
            ))
        })
        .collect()
}

/// The `(…, id)` prefix to partition on; `None` when the trailing key isn't a
/// numeric column we can bucket (e.g. `traversal_path` alone).
fn partition_key_columns(order_by: &[String]) -> Option<Vec<String>> {
    let key: Vec<String> = order_by.iter().take(2).cloned().collect();
    match key.last().map(String::as_str) {
        Some("traversal_path") | None => None,
        Some(_) => Some(key),
    }
}

async fn compute_partition_ranges(
    datalake: &dyn DatalakeQuery,
    table: &str,
    key_columns: &[String],
    count: u32,
    min_rows: u64,
    traversal_path: Option<&str>,
) -> Result<Vec<PartitionAssignment>, HandlerError> {
    if count <= 1 || key_columns.is_empty() {
        return Ok(Vec::new());
    }

    let batches = datalake
        .query_batches(
            &probe_sql(table, key_columns, count, min_rows, traversal_path),
            probe_params(traversal_path),
            None,
        )
        .await
        .map_err(|err| HandlerError::Processing(format!("partition probe failed: {err}")))?;

    let cuts = parse_cut_tuples(&batches, key_columns.len());

    if cuts.len() < count as usize {
        debug!(
            ?cuts,
            count, "skipping partitioning: too few buckets to fill all partitions"
        );
        return Ok(Vec::new());
    }

    // cuts[0] is the namespace start; cuts[1..] are the internal partition edges.
    let internal_boundaries = &cuts[1..count as usize];
    Ok((0..count)
        .map(|i| PartitionAssignment {
            index: i,
            total: count,
            key_columns: key_columns.to_vec(),
            lower_bound: (i > 0).then(|| internal_boundaries[(i - 1) as usize].clone()),
            upper_bound: (i < count - 1).then(|| internal_boundaries[i as usize].clone()),
        })
        .collect())
}

fn probe_params(traversal_path: Option<&str>) -> Value {
    match traversal_path {
        Some(path) => serde_json::json!({ "traversal_path": path }),
        None => Value::Null,
    }
}

/// Carrying the leading keys lets a cut split *inside* a dominant path by id —
/// which a path-only cut cannot — and `argMin` avoids a `row_number()` scan.
fn probe_sql(
    table: &str,
    key_columns: &[String],
    count: u32,
    min_rows: u64,
    traversal_path: Option<&str>,
) -> String {
    let scope = match traversal_path {
        Some(_) => "startsWith(traversal_path, {traversal_path:String})",
        None => "1=1",
    };
    let (leading_keys, id_column) = key_columns.split_at(key_columns.len() - 1);
    let id_column = &id_column[0];

    let leading_columns = comma_terminated(leading_keys.iter().cloned());
    let leading_earliest = comma_terminated(
        leading_keys
            .iter()
            .map(|key| format!("argMin({key}, rows_through_bucket) AS {key}")),
    );
    let target = TARGET_BUCKET_COUNT;

    format!(
        "WITH span AS (\
           SELECT greatest(1, intDiv(max({id_column}) - min({id_column}), {target})) AS width \
           FROM {table} WHERE {scope}\
         ), bucket_counts AS (\
           SELECT {leading_columns}intDiv({id_column}, (SELECT width FROM span)) AS bucket, \
                  min({id_column}) AS bucket_min_id, count() AS rows \
           FROM {table} WHERE {scope} GROUP BY {leading_columns}bucket\
         ), cumulative AS (\
           SELECT {leading_columns}bucket_min_id, \
                  sum(rows) OVER (ORDER BY {leading_columns}bucket) AS rows_through_bucket, \
                  sum(rows) OVER () AS total_rows \
           FROM bucket_counts\
         ) \
         SELECT {leading_earliest}toString(argMin(bucket_min_id, rows_through_bucket)) AS id_lower \
         FROM cumulative \
         WHERE total_rows >= {min_rows} \
         GROUP BY least(intDiv((rows_through_bucket - 1) * {count}, total_rows), {count} - 1) AS quantile \
         ORDER BY quantile"
    )
}

fn comma_terminated(items: impl Iterator<Item = String>) -> String {
    let joined = items.collect::<Vec<_>>().join(", ");
    if joined.is_empty() {
        joined
    } else {
        format!("{joined}, ")
    }
}

fn parse_cut_tuples(batches: &[RecordBatch], key_len: usize) -> Vec<Vec<String>> {
    let Some(batch) = batches.first() else {
        return Vec::new();
    };
    let columns: Vec<&StringArray> = (0..key_len)
        .filter_map(|i| {
            batch
                .columns()
                .get(i)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        })
        .collect();
    if columns.len() != key_len {
        return Vec::new();
    }
    (0..batch.num_rows())
        .map(|row| {
            columns
                .iter()
                .map(|col| col.value(row).to_string())
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::datalake::{DatalakeError, RecordBatchStream};
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    /// Returns fixed cut-tuple rows (column-aligned with the partition key) and
    /// captures the probe SQL/params for assertion.
    struct ProbeDatalake {
        rows: Vec<Vec<String>>,
        key_len: usize,
        captured_sql: Mutex<String>,
        captured_params: Mutex<Value>,
    }

    impl ProbeDatalake {
        fn new(rows: Vec<Vec<&str>>) -> Self {
            let key_len = rows.first().map(Vec::len).unwrap_or(0);
            Self {
                rows: rows
                    .into_iter()
                    .map(|r| r.into_iter().map(String::from).collect())
                    .collect(),
                key_len,
                captured_sql: Mutex::new(String::new()),
                captured_params: Mutex::new(Value::Null),
            }
        }
    }

    fn build_probe_batch(rows: &[Vec<String>], key_len: usize) -> RecordBatch {
        let fields: Vec<Field> = (0..key_len)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, false))
            .collect();
        let columns: Vec<ArrayRef> = (0..key_len)
            .map(|c| {
                let vals: Vec<&str> = rows.iter().map(|r| r[c].as_str()).collect();
                Arc::new(StringArray::from(vals)) as ArrayRef
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[async_trait]
    impl DatalakeQuery for ProbeDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn query_batches(
            &self,
            sql: &str,
            params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            *self.captured_sql.lock().unwrap() = sql.to_string();
            *self.captured_params.lock().unwrap() = params;
            if self.rows.is_empty() {
                return Ok(vec![]);
            }
            Ok(vec![build_probe_batch(&self.rows, self.key_len)])
        }
    }

    fn keys(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn id_cuts_yield_open_ended_contiguous_partitions() {
        // Bucket 0's tuple is the namespace start (no lower bound); the 4 internal
        // boundaries are the remaining rows.
        let datalake = ProbeDatalake::new(vec![
            vec!["0"],
            vec!["100"],
            vec!["200"],
            vec!["300"],
            vec!["400"],
        ]);
        let ranges = compute_partition_ranges(&datalake, "t", &keys(&["id"]), 5, 0, None)
            .await
            .unwrap();

        assert_eq!(ranges.len(), 5);
        assert_eq!(ranges[0].lower_bound, None);
        assert_eq!(ranges[0].upper_bound, Some(keys(&["100"])));
        assert_eq!(ranges[1].lower_bound, Some(keys(&["100"])));
        assert_eq!(ranges[1].upper_bound, Some(keys(&["200"])));
        assert_eq!(ranges[4].lower_bound, Some(keys(&["400"])));
        assert_eq!(ranges[4].upper_bound, None);
        assert_eq!(ranges[4].total, 5);
    }

    #[tokio::test]
    async fn composite_cuts_split_within_a_dominant_path() {
        // A mega-path fills buckets 1..3: their cuts share traversal_path but
        // differ by id — the intra-path split a path-only cut cannot make.
        let datalake = ProbeDatalake::new(vec![
            vec!["1/9970/a/", "0"],
            vec!["1/9970/mega/", "6000000000"],
            vec!["1/9970/mega/", "9000000000"],
            vec!["1/9970/mega/", "12000000000"],
            vec!["1/9970/z/", "0"],
        ]);
        let ranges = compute_partition_ranges(
            &datalake,
            "t",
            &keys(&["traversal_path", "id"]),
            5,
            0,
            Some("1/9970/"),
        )
        .await
        .unwrap();

        assert_eq!(ranges.len(), 5);
        assert_eq!(ranges[0].lower_bound, None);
        assert_eq!(
            ranges[0].upper_bound,
            Some(keys(&["1/9970/mega/", "6000000000"]))
        );
        assert_eq!(
            ranges[2].lower_bound,
            Some(keys(&["1/9970/mega/", "9000000000"]))
        );
        assert_eq!(
            ranges[2].upper_bound,
            Some(keys(&["1/9970/mega/", "12000000000"]))
        );
        assert_eq!(ranges[4].lower_bound, Some(keys(&["1/9970/z/", "0"])));
        assert_eq!(ranges[4].upper_bound, None);
        assert_eq!(ranges[2].key_columns, keys(&["traversal_path", "id"]));
    }

    #[tokio::test]
    async fn skips_when_too_few_buckets() {
        let datalake = ProbeDatalake::new(vec![vec!["0"], vec!["100"]]);
        let ranges = compute_partition_ranges(&datalake, "t", &keys(&["id"]), 5, 0, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn count_of_one_returns_empty() {
        let datalake = ProbeDatalake::new(vec![vec!["0"]]);
        let ranges = compute_partition_ranges(&datalake, "t", &keys(&["id"]), 1, 0, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn empty_probe_returns_empty() {
        let datalake = ProbeDatalake::new(vec![]);
        let ranges = compute_partition_ranges(&datalake, "t", &keys(&["id"]), 4, 0, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn probe_sql_buckets_trailing_key_and_scopes_by_path() {
        let datalake = ProbeDatalake::new(vec![
            vec!["1/9970/a/", "0"],
            vec!["1/9970/b/", "0"],
            vec!["1/9970/c/", "0"],
            vec!["1/9970/d/", "0"],
        ]);
        let _ = compute_partition_ranges(
            &datalake,
            "t",
            &keys(&["traversal_path", "id"]),
            4,
            50_000_000,
            Some("1/9970/"),
        )
        .await
        .unwrap();

        let sql = datalake.captured_sql.lock().unwrap().clone();
        assert!(
            sql.contains("greatest(1, intDiv(max(id) - min(id), 10000))"),
            "expected adaptive bucket width from id span: {sql}"
        );
        assert!(
            sql.contains("WHERE total_rows >= 50000000"),
            "expected min-rows gate: {sql}"
        );
        assert!(
            sql.contains("intDiv(id, (SELECT width FROM span))"),
            "expected id bucketing by derived width: {sql}"
        );
        assert!(
            sql.contains("argMin(traversal_path, rows_through_bucket)"),
            "expected leading-key argMin: {sql}"
        );
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "expected traversal_path scope: {sql}"
        );
        assert!(
            sql.contains("GROUP BY least(intDiv((rows_through_bucket - 1) * 4"),
            "expected quintile bucketing: {sql}"
        );
        assert_eq!(
            datalake.captured_params.lock().unwrap().clone()["traversal_path"],
            "1/9970/"
        );
    }

    #[test]
    fn partition_key_columns_takes_leading_prefix_and_rejects_string_trailing() {
        assert_eq!(partition_key_columns(&keys(&["id"])), Some(keys(&["id"])));
        assert_eq!(
            partition_key_columns(&keys(&["traversal_path", "id"])),
            Some(keys(&["traversal_path", "id"]))
        );
        // partition_id is dropped; the (traversal_path, id) prefix is enough.
        assert_eq!(
            partition_key_columns(&keys(&["traversal_path", "id", "partition_id"])),
            Some(keys(&["traversal_path", "id"]))
        );
        // A trailing string key has nothing numeric to bucket.
        assert_eq!(partition_key_columns(&keys(&["traversal_path"])), None);
    }

    #[test]
    fn build_strategies_resolves_composite_key_for_overridden_entities() {
        use crate::modules::sdlc::plan::input::{
            ExtractColumn, ExtractPlan, ExtractSource, NodePlan,
        };
        use ontology::EtlScope;

        let inputs = PlanInput {
            node_plans: vec![NodePlan {
                name: "User".to_string(),
                scope: EtlScope::Global,
                columns: vec![],
                edges: vec![],
                extract: ExtractPlan {
                    destination_table: "gl_user".to_string(),
                    columns: vec![ExtractColumn::Bare("id".to_string())],
                    source: ExtractSource::Table("siphon_users".to_string()),
                    base_table: "siphon_users".to_string(),
                    watermark: "_siphon_replicated_at".to_string(),
                    deleted: "_siphon_deleted".to_string(),
                    order_by: vec!["id".to_string()],
                    namespaced: false,
                    traversal_path_filter: None,
                    additional_where: None,
                    enrichment: None,
                },
            }],
            standalone_edge_plans: vec![],
            derived_entity_plans: vec![],
        };

        let overrides = HashMap::from([("User".to_string(), 4)]);
        let strategies = build_strategies(&inputs, &overrides, 50_000_000);
        let user = strategies.get("User").expect("User should be partitioned");
        assert_eq!(user.count, 4);
        assert_eq!(user.key_columns, keys(&["id"]));
        assert_eq!(user.datalake_table, "siphon_users");
        assert_eq!(user.min_rows, 50_000_000);

        let no_overrides = HashMap::new();
        let strategies = build_strategies(&inputs, &no_overrides, 50_000_000);
        assert!(strategies.is_empty());
    }
}
