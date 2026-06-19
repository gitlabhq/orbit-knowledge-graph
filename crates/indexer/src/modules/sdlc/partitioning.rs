use std::collections::HashMap;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use tracing::{debug, warn};

use crate::handler::HandlerError;

use super::datalake::DatalakeQuery;
use super::plan::input::PlanInput;

/// The probe derives its bucket width from the id span to land near this count;
/// a fixed width would collapse a narrow id range into a single bucket.
const TARGET_BUCKET_COUNT: i64 = 10_000;

/// Half-open `[lower, upper)` slice of the leading sort-key prefix; `None` is an
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

impl PartitionStrategy {
    pub async fn compute_ranges(
        &self,
        datalake: &dyn DatalakeQuery,
        traversal_path: Option<&str>,
    ) -> Result<Vec<PartitionAssignment>, HandlerError> {
        if self.count <= 1 || self.key_columns.is_empty() {
            return Ok(Vec::new());
        }

        let batches = datalake
            .query_batches(
                &self.probe_sql(traversal_path),
                probe_params(traversal_path),
                None,
            )
            .await
            .map_err(|err| HandlerError::Processing(format!("partition probe failed: {err}")))?;
        let cuts = parse_cut_tuples(&batches, self.key_columns.len());

        if cuts.len() < self.count as usize {
            let probe_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            if probe_rows > 0 && self.min_rows > 0 {
                // Returned rows cleared the probe's `total_rows >= min_rows` gate, so too-few-cuts on a real scope is the silent single-threaded fallback (#869).
                warn!(
                    batches = batches.len(),
                    probe_rows,
                    cuts = cuts.len(),
                    count = self.count,
                    table = %self.datalake_table,
                    "partition probe cleared min_rows but yielded too few cuts; load will run single-threaded"
                );
            } else {
                debug!(
                    probe_rows,
                    count = self.count,
                    "not partitioning: too few cuts to fill all partitions"
                );
            }
            return Ok(Vec::new());
        }
        Ok(self.assignments_between(&cuts))
    }

    // cuts[0] is the scope start; cuts[1..count] are the internal partition edges.
    fn assignments_between(&self, cuts: &[Vec<String>]) -> Vec<PartitionAssignment> {
        let edges = &cuts[1..self.count as usize];
        (0..self.count)
            .map(|index| PartitionAssignment {
                index,
                total: self.count,
                key_columns: self.key_columns.clone(),
                lower_bound: (index > 0).then(|| edges[index as usize - 1].clone()),
                upper_bound: (index < self.count - 1).then(|| edges[index as usize].clone()),
            })
            .collect()
    }

    /// Buckets the trailing id key, accumulates row counts in sort-key order,
    /// and takes the first key tuple of each row-balanced quantile. Carrying the
    /// leading keys lets a cut split *inside* a dominant path by id — a
    /// path-only cut cannot.
    fn probe_sql(&self, traversal_path: Option<&str>) -> String {
        let table = &self.datalake_table;
        let count = self.count;
        let scope = match traversal_path {
            Some(_) => "startsWith(traversal_path, {traversal_path:String})",
            None => "1=1",
        };
        let (leading_keys, trailing_key) = self.key_columns.split_at(self.key_columns.len() - 1);
        let id = &trailing_key[0];
        let with_leading = |extras: Vec<String>| {
            leading_keys
                .iter()
                .cloned()
                .chain(extras)
                .collect::<Vec<_>>()
                .join(", ")
        };

        let bucket_key = with_leading(vec!["bucket".to_string()]);
        let bucket_select = with_leading(vec![
            format!("intDiv({id}, (SELECT width FROM span)) AS bucket"),
            format!("min({id}) AS bucket_min_id"),
            "count() AS rows".to_string(),
        ]);
        let cumulative_select = with_leading(vec![
            "bucket_min_id".to_string(),
            format!("sum(rows) OVER (ORDER BY {bucket_key}) AS rows_through_bucket"),
            "sum(rows) OVER () AS total_rows".to_string(),
        ]);
        let cut_select = leading_keys
            .iter()
            .map(|key| format!("argMin({key}, rows_through_bucket) AS {key}"))
            .chain(["toString(argMin(bucket_min_id, rows_through_bucket)) AS id_lower".to_string()])
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "WITH span AS (\n\
               SELECT greatest(1, intDiv(max({id}) - min({id}), {TARGET_BUCKET_COUNT})) AS width\n\
               FROM {table} WHERE {scope}\n\
             ),\n\
             bucket_counts AS (\n\
               SELECT {bucket_select}\n\
               FROM {table} WHERE {scope}\n\
               GROUP BY {bucket_key}\n\
             ),\n\
             cumulative AS (\n\
               SELECT {cumulative_select}\n\
               FROM bucket_counts\n\
             )\n\
             SELECT {cut_select}\n\
             FROM cumulative\n\
             WHERE total_rows >= {min_rows}\n\
             GROUP BY least(intDiv((rows_through_bucket - 1) * {count}, total_rows), {count} - 1) AS quantile\n\
             ORDER BY quantile",
            min_rows = self.min_rows,
        )
    }
}

fn probe_params(traversal_path: Option<&str>) -> Value {
    match traversal_path {
        Some(path) => serde_json::json!({ "traversal_path": path }),
        None => Value::Null,
    }
}

fn parse_cut_tuples(batches: &[RecordBatch], key_len: usize) -> Vec<Vec<String>> {
    batches
        .iter()
        .flat_map(|batch| parse_batch_cuts(batch, key_len))
        .collect()
}

fn parse_batch_cuts(batch: &RecordBatch, key_len: usize) -> Vec<Vec<String>> {
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

    struct ProbeDatalake {
        rows: Vec<Vec<String>>,
        key_len: usize,
        rows_per_batch: usize,
        captured_sql: Mutex<String>,
        captured_params: Mutex<Value>,
    }

    impl ProbeDatalake {
        fn new(rows: Vec<Vec<&str>>) -> Self {
            Self::chunked(rows, usize::MAX)
        }

        fn chunked(rows: Vec<Vec<&str>>, rows_per_batch: usize) -> Self {
            let key_len = rows.first().map(Vec::len).unwrap_or(0);
            Self {
                rows: rows
                    .into_iter()
                    .map(|r| r.into_iter().map(String::from).collect())
                    .collect(),
                key_len,
                rows_per_batch,
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
            Ok(self
                .rows
                .chunks(self.rows_per_batch.max(1))
                .map(|chunk| build_probe_batch(chunk, self.key_len))
                .collect())
        }
    }

    fn keys(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }

    fn strategy(key_columns: &[&str], count: u32, min_rows: u64) -> PartitionStrategy {
        PartitionStrategy {
            count,
            key_columns: keys(key_columns),
            datalake_table: "t".to_string(),
            min_rows,
        }
    }

    #[tokio::test]
    async fn id_cuts_yield_open_ended_contiguous_partitions() {
        let datalake = ProbeDatalake::new(vec![
            vec!["0"],
            vec!["100"],
            vec!["200"],
            vec!["300"],
            vec!["400"],
        ]);
        let ranges = strategy(&["id"], 5, 0)
            .compute_ranges(&datalake, None)
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
        let datalake = ProbeDatalake::new(vec![
            vec!["1/9970/a/", "0"],
            vec!["1/9970/mega/", "6000000000"],
            vec!["1/9970/mega/", "9000000000"],
            vec!["1/9970/mega/", "12000000000"],
            vec!["1/9970/z/", "0"],
        ]);
        let ranges = strategy(&["traversal_path", "id"], 5, 0)
            .compute_ranges(&datalake, Some("1/9970/"))
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
    async fn cuts_spanning_multiple_batches_still_partition() {
        let datalake = ProbeDatalake::chunked(
            vec![
                vec!["1/9970/a/", "0"],
                vec!["1/9970/b/", "100"],
                vec!["1/9970/c/", "200"],
                vec!["1/9970/d/", "300"],
                vec!["1/9970/e/", "400"],
            ],
            2,
        );
        let ranges = strategy(&["traversal_path", "id"], 5, 0)
            .compute_ranges(&datalake, Some("1/9970/"))
            .await
            .unwrap();

        assert_eq!(ranges.len(), 5);
    }

    #[tokio::test]
    async fn skips_when_too_few_buckets() {
        let datalake = ProbeDatalake::new(vec![vec!["0"], vec!["100"]]);
        let ranges = strategy(&["id"], 5, 0)
            .compute_ranges(&datalake, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn count_of_one_returns_empty() {
        let datalake = ProbeDatalake::new(vec![vec!["0"]]);
        let ranges = strategy(&["id"], 1, 0)
            .compute_ranges(&datalake, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn empty_probe_returns_empty() {
        let datalake = ProbeDatalake::new(vec![]);
        let ranges = strategy(&["id"], 4, 0)
            .compute_ranges(&datalake, None)
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
        let _ = strategy(&["traversal_path", "id"], 4, 50_000_000)
            .compute_ranges(&datalake, Some("1/9970/"))
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
            "expected quartile bucketing: {sql}"
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
        assert_eq!(
            partition_key_columns(&keys(&["traversal_path", "id", "partition_id"])),
            Some(keys(&["traversal_path", "id"]))
        );
        assert_eq!(partition_key_columns(&keys(&["traversal_path"])), None);
    }

    #[test]
    fn build_strategies_resolves_composite_key_for_overridden_entities() {
        use crate::modules::sdlc::plan::fragments::ExtractColumn;
        use crate::modules::sdlc::plan::input::{ExtractPlan, ExtractSource, NodePlan};
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
                    watermark: "_siphon_watermark".to_string(),
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
