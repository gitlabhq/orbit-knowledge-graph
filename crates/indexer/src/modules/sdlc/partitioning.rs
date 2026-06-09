use std::collections::HashMap;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use tracing::debug;

use crate::handler::HandlerError;

use super::datalake::DatalakeQuery;
use super::plan::input::PlanInput;

/// Granularity for bucketing the trailing (numeric) key during the cut probe.
/// Coarse enough to keep the probe to a single aggregate scan, fine enough that
/// a multi-million-row path still splits into balanced partitions.
const KEY_BUCKET_WIDTH: i64 = 100_000_000;

/// A contiguous half-open slice `[lower, upper)` of the table's leading sort-key
/// prefix. Bounds are tuples over `key_columns` (e.g. `(traversal_path, id)`), so
/// the extract prunes by the primary index instead of filtering on a non-leading
/// column. `None` bound = open end (first/last partition).
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
            traversal_path,
        )
        .await
    }
}

pub(in crate::modules::sdlc) fn build_strategies(
    inputs: &PlanInput,
    overrides: &HashMap<String, u32>,
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
                },
            ))
        })
        .collect()
}

/// Partition on the leading sort-key prefix `(…, id)` so each slice is a
/// contiguous granule range. The trailing column (`id`) is what we bucket;
/// any leading columns (`traversal_path`) pin the prefix. Returns `None` if the
/// trailing key isn't a numeric column we can bucket (e.g. `traversal_path`
/// alone), since the probe would have nothing meaningful to split on.
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
    traversal_path: Option<&str>,
) -> Result<Vec<PartitionAssignment>, HandlerError> {
    if count <= 1 || key_columns.is_empty() {
        return Ok(Vec::new());
    }

    let batches = datalake
        .query_batches(
            &probe_sql(table, key_columns, count, traversal_path),
            probe_params(traversal_path),
            None,
        )
        .await
        .map_err(|err| HandlerError::Processing(format!("partition probe failed: {err}")))?;

    let cuts = parse_cut_tuples(&batches, key_columns.len());

    // One row per filled quintile bucket. Fewer than `count` means the data is
    // too sparse to split into `count` balanced partitions — fall back to a
    // single unpartitioned pass.
    if cuts.len() < count as usize {
        debug!(
            ?cuts,
            count, "skipping partitioning: insufficient distinct cuts"
        );
        return Ok(Vec::new());
    }

    // cuts[0] is bucket 0's first tuple (the namespace start); the internal
    // boundaries are cuts[1..count]. Partition i spans [boundary_{i-1}, boundary_i),
    // with the first/last bound left open.
    let boundaries = &cuts[1..count as usize];
    Ok((0..count)
        .map(|i| PartitionAssignment {
            index: i,
            total: count,
            key_columns: key_columns.to_vec(),
            lower_bound: (i > 0).then(|| boundaries[(i - 1) as usize].clone()),
            upper_bound: (i < count - 1).then(|| boundaries[i as usize].clone()),
        })
        .collect())
}

fn probe_params(traversal_path: Option<&str>) -> Value {
    match traversal_path {
        Some(path) => serde_json::json!({ "traversal_path": path }),
        None => Value::Null,
    }
}

/// Row-balanced composite cut points: bucket the trailing key, accumulate row
/// counts in sort-key order, then take the first `(leading…, id)` tuple of each
/// quintile. A single aggregate scan (no `row_number` materialization), and it
/// splits *within* a dominant path by id — which a path-only cut cannot.
fn probe_sql(
    table: &str,
    key_columns: &[String],
    count: u32,
    traversal_path: Option<&str>,
) -> String {
    let scope = match traversal_path {
        Some(_) => "startsWith(traversal_path, {traversal_path:String})",
        None => "1=1",
    };
    let (group_cols, bucket_col) = key_columns.split_at(key_columns.len() - 1);
    let bucket_col = &bucket_col[0];

    let group_prefix = if group_cols.is_empty() {
        String::new()
    } else {
        format!("{}, ", group_cols.join(", "))
    };
    let argmin_prefix = if group_cols.is_empty() {
        String::new()
    } else {
        format!(
            "{}, ",
            group_cols
                .iter()
                .map(|c| format!("argMin({c}, _cu) AS {c}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let w = KEY_BUCKET_WIDTH;

    format!(
        "WITH g AS (\
           SELECT {group_prefix}intDiv({bucket_col}, {w}) AS _b, count() AS _c \
           FROM {table} WHERE {scope} GROUP BY {group_prefix}_b\
         ), cum AS (\
           SELECT {group_prefix}_b, \
                  sum(_c) OVER (ORDER BY {group_prefix}_b) AS _cu, \
                  sum(_c) OVER () AS _total \
           FROM g\
         ) \
         SELECT {argmin_prefix}toString(argMin(_b, _cu) * {w}) AS _idlo \
         FROM cum \
         GROUP BY least(intDiv((_cu - 1) * {count}, _total), {count} - 1) AS _bucket \
         ORDER BY _bucket"
    )
}

/// Each probe row is one cut tuple, column-aligned with `key_columns`
/// (leading group columns, then the bucketed id), all returned as strings.
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
        let ranges = compute_partition_ranges(&datalake, "t", &keys(&["id"]), 5, None)
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
        let ranges = compute_partition_ranges(&datalake, "t", &keys(&["id"]), 5, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn count_of_one_returns_empty() {
        let datalake = ProbeDatalake::new(vec![vec!["0"]]);
        let ranges = compute_partition_ranges(&datalake, "t", &keys(&["id"]), 1, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn empty_probe_returns_empty() {
        let datalake = ProbeDatalake::new(vec![]);
        let ranges = compute_partition_ranges(&datalake, "t", &keys(&["id"]), 4, None)
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
            Some("1/9970/"),
        )
        .await
        .unwrap();

        let sql = datalake.captured_sql.lock().unwrap().clone();
        assert!(
            sql.contains("intDiv(id, 100000000)"),
            "expected id bucketing: {sql}"
        );
        assert!(
            sql.contains("argMin(traversal_path, _cu)"),
            "expected leading-key argMin: {sql}"
        );
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "expected traversal_path scope: {sql}"
        );
        assert!(
            sql.contains("GROUP BY least(intDiv((_cu - 1) * 4"),
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
        let strategies = build_strategies(&inputs, &overrides);
        let user = strategies.get("User").expect("User should be partitioned");
        assert_eq!(user.count, 4);
        assert_eq!(user.key_columns, keys(&["id"]));
        assert_eq!(user.datalake_table, "siphon_users");

        let no_overrides = HashMap::new();
        let strategies = build_strategies(&inputs, &no_overrides);
        assert!(strategies.is_empty());
    }
}
