use std::collections::HashMap;

use arrow::array::{Array, Int64Array, ListArray};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use tracing::debug;

use crate::handler::HandlerError;

use super::datalake::DatalakeQuery;
use super::plan::input::PlanInput;

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct PartitionAssignment {
    pub index: u32,
    pub total: u32,
    pub column: String,
    pub lower_bound: String,
    pub upper_bound: String,
}

impl PartitionAssignment {
    pub fn position_suffix(&self) -> String {
        format!(".p{}of{}", self.index, self.total)
    }
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct PartitionStrategy {
    pub count: u32,
    pub column: String,
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
            &self.column,
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
            let column = partition_column(&node.extract.order_by)?.to_string();
            Some((
                node.name.clone(),
                PartitionStrategy {
                    count,
                    column,
                    datalake_table: node.extract.base_table.clone(),
                },
            ))
        })
        .collect()
}

fn partition_column(sort_key: &[String]) -> Option<&str> {
    sort_key
        .iter()
        .map(String::as_str)
        .find(|col| *col != "traversal_path")
}

async fn compute_partition_ranges(
    datalake: &dyn DatalakeQuery,
    table: &str,
    column: &str,
    count: u32,
    traversal_path: Option<&str>,
) -> Result<Vec<PartitionAssignment>, HandlerError> {
    if count <= 1 {
        return Ok(Vec::new());
    }

    let (scope, params) = match traversal_path {
        Some(path) => (
            "startsWith(traversal_path, {traversal_path:String})",
            serde_json::json!({ "traversal_path": path }),
        ),
        None => ("1=1", Value::Null),
    };

    let fractions = (1..count)
        .map(|i| format!("{}", i as f64 / count as f64))
        .collect::<Vec<_>>()
        .join(", ");
    // isFinite filter: empty scopes produce NaN quantiles, and toInt64(NaN) errors out.
    let sql = format!(
        "SELECT arraySort(arrayDistinct(arrayConcat( \
            [toInt64(min({column}))], \
            arrayMap(x -> toInt64(x), arrayFilter(x -> isFinite(x), \
                quantilesBFloat16({fractions})({column}))), \
            [toInt64(max({column})) + 1] \
         ))) AS cuts FROM {table} WHERE {scope}"
    );

    let batches = datalake
        .query_batches(&sql, params, None)
        .await
        .map_err(|err| HandlerError::Processing(format!("partition probe failed: {err}")))?;

    let cuts = parse_cuts(&batches);

    // Fewer than count+1 distinct cuts means the data can't fill the requested partitions.
    if cuts.len() < count as usize + 1 {
        debug!(
            ?cuts,
            count, "skipping partitioning: insufficient distinct cuts"
        );
        return Ok(Vec::new());
    }

    let total = (cuts.len() - 1) as u32;
    Ok(cuts
        .windows(2)
        .enumerate()
        .map(|(i, w)| PartitionAssignment {
            index: i as u32,
            total,
            column: column.to_string(),
            lower_bound: w[0].to_string(),
            upper_bound: w[1].to_string(),
        })
        .collect())
}

fn parse_cuts(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<ListArray>().cloned())
        .filter(|list| list.len() > 0 && !list.is_null(0))
        .and_then(|list| list.value(0).as_any().downcast_ref::<Int64Array>().cloned())
        .map(|arr| arr.iter().flatten().collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::datalake::{DatalakeError, RecordBatchStream};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    struct ProbeDatalake {
        cuts: Vec<i64>,
        captured_sql: Mutex<String>,
        captured_params: Mutex<Value>,
    }

    impl ProbeDatalake {
        fn new(cuts: Vec<i64>) -> Self {
            Self {
                cuts,
                captured_sql: Mutex::new(String::new()),
                captured_params: Mutex::new(Value::Null),
            }
        }
    }

    fn build_cuts_batch(cuts: &[i64]) -> RecordBatch {
        let inner_field = Arc::new(Field::new("item", DataType::Int64, true));
        let list_field = Field::new("cuts", DataType::List(inner_field.clone()), false);
        let schema = Arc::new(Schema::new(vec![list_field]));
        let values = Arc::new(Int64Array::from(cuts.to_vec()));
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, cuts.len() as i32].into());
        let list = ListArray::new(inner_field, offsets, values, None);
        RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap()
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
            Ok(vec![build_cuts_batch(&self.cuts)])
        }
    }

    #[tokio::test]
    async fn quantile_cuts_yield_disjoint_ranges() {
        let datalake = ProbeDatalake::new(vec![0, 25, 50, 75, 100]);
        let ranges = compute_partition_ranges(&datalake, "t", "id", 4, None)
            .await
            .unwrap();

        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].lower_bound, "0");
        assert_eq!(ranges[0].upper_bound, "25");
        assert_eq!(ranges[3].upper_bound, "100");
        assert_eq!(ranges[3].total, 4);
    }

    #[tokio::test]
    async fn clustered_data_produces_dense_partitions() {
        let datalake = ProbeDatalake::new(vec![1, 2, 3, 4, 10000, 100000]);
        let ranges = compute_partition_ranges(&datalake, "t", "id", 5, None)
            .await
            .unwrap();

        let bounds: Vec<(&str, &str)> = ranges
            .iter()
            .map(|r| (r.lower_bound.as_str(), r.upper_bound.as_str()))
            .collect();
        assert_eq!(
            bounds,
            vec![
                ("1", "2"),
                ("2", "3"),
                ("3", "4"),
                ("4", "10000"),
                ("10000", "100000"),
            ]
        );
    }

    #[tokio::test]
    async fn skips_when_too_few_distinct_cuts() {
        let datalake = ProbeDatalake::new(vec![5, 11]);
        let ranges = compute_partition_ranges(&datalake, "t", "id", 5, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn count_of_one_returns_empty() {
        let datalake = ProbeDatalake::new(vec![]);
        let ranges = compute_partition_ranges(&datalake, "t", "id", 1, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn empty_probe_returns_empty() {
        let datalake = ProbeDatalake::new(vec![]);
        let ranges = compute_partition_ranges(&datalake, "t", "id", 4, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn probe_sql_uses_quantiles_bfloat16_and_scopes_by_traversal_path() {
        let datalake = ProbeDatalake::new(vec![0, 25, 50, 75, 101]);
        let _ = compute_partition_ranges(&datalake, "t", "id", 4, Some("42/100/"))
            .await
            .unwrap();

        let sql = datalake.captured_sql.lock().unwrap().clone();
        assert!(
            sql.contains("quantilesBFloat16"),
            "expected quantilesBFloat16 in: {sql}"
        );
        assert!(
            sql.contains("startsWith(traversal_path"),
            "expected traversal_path scoping in: {sql}"
        );
        let params = datalake.captured_params.lock().unwrap().clone();
        assert_eq!(params["traversal_path"], "42/100/");
    }

    #[tokio::test]
    async fn probe_sql_filters_non_finite_quantiles() {
        let datalake = ProbeDatalake::new(vec![0, 25, 50, 75, 101]);
        let _ = compute_partition_ranges(&datalake, "t", "id", 4, Some("42/100/"))
            .await
            .unwrap();
        let sql = datalake.captured_sql.lock().unwrap().clone();
        assert!(
            sql.contains("arrayFilter(x -> isFinite(x)"),
            "expected isFinite filter around quantiles in: {sql}"
        );
    }

    #[test]
    fn build_strategies_skips_entities_without_overrides() {
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
        };

        let overrides = HashMap::from([("User".to_string(), 4)]);
        let strategies = build_strategies(&inputs, &overrides);
        let user = strategies.get("User").expect("User should be partitioned");
        assert_eq!(user.count, 4);
        assert_eq!(user.column, "id");
        assert_eq!(user.datalake_table, "siphon_users");

        let no_overrides = HashMap::new();
        let strategies = build_strategies(&inputs, &no_overrides);
        assert!(strategies.is_empty());
    }
}
