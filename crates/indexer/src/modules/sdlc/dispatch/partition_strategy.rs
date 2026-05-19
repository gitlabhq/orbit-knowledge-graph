use async_trait::async_trait;
use clickhouse_client::FromArrowColumn;

use crate::clickhouse::ArrowClickHouseClient;
use crate::scheduler::TaskError;
use crate::topic::{IndexingScope, PartitionBounds};
use ontology::{EtlConfig, EtlScope, Ontology};

pub struct PartitionPlan {
    pub column: String,
    pub boundaries: Vec<PartitionBounds>,
}

#[async_trait]
pub trait PartitionStrategy: Send + Sync {
    async fn compute_boundaries(
        &self,
        entity_name: &str,
        num_partitions: u32,
        scope: &IndexingScope,
    ) -> Result<PartitionPlan, TaskError>;
}

pub struct DatalakePartitionStrategy {
    datalake: ArrowClickHouseClient,
    ontology: Ontology,
}

impl DatalakePartitionStrategy {
    pub fn new(datalake: ArrowClickHouseClient, ontology: &Ontology) -> Self {
        Self {
            datalake,
            ontology: ontology.clone(),
        }
    }

    fn resolve_entity(&self, entity_name: &str) -> Result<(&str, &str), TaskError> {
        let node = self
            .ontology
            .get_node(entity_name)
            .ok_or_else(|| TaskError::new(format!("unknown entity: {entity_name}")))?;
        let etl = node
            .etl
            .as_ref()
            .ok_or_else(|| TaskError::new(format!("entity {entity_name} has no ETL config")))?;
        let source_table = match etl {
            EtlConfig::Table { source, .. } => source.as_str(),
            EtlConfig::Query { .. } => {
                return Err(TaskError::new(format!(
                    "cannot partition {entity_name} (Query-type ETL)"
                )));
            }
        };
        let partition_column = partition_column(etl.order_by(), etl.scope()).ok_or_else(|| {
            TaskError::new(format!("cannot derive partition column for {entity_name}"))
        })?;
        Ok((source_table, partition_column))
    }
}

#[async_trait]
impl PartitionStrategy for DatalakePartitionStrategy {
    async fn compute_boundaries(
        &self,
        entity_name: &str,
        num_partitions: u32,
        scope: &IndexingScope,
    ) -> Result<PartitionPlan, TaskError> {
        let (source_table, partition_column) = self.resolve_entity(entity_name)?;
        let column = partition_column.to_string();
        let sql = build_quantile_query(source_table, partition_column, num_partitions, scope);

        let batches = self
            .datalake
            .query(&sql)
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;

        let min_vals = String::extract_column(&batches, 0).map_err(TaskError::new)?;
        let max_vals =
            String::extract_column(&batches, num_partitions as usize).map_err(TaskError::new)?;

        let min_val = min_vals
            .first()
            .ok_or_else(|| TaskError::new("empty quantile result — source table may be empty"))?;
        let max_val = max_vals
            .first()
            .ok_or_else(|| TaskError::new("empty quantile result — source table may be empty"))?;

        let mut quantile_splits = Vec::with_capacity((num_partitions - 1) as usize);
        for col_idx in 1..num_partitions as usize {
            let vals = String::extract_column(&batches, col_idx).map_err(TaskError::new)?;
            let val = vals
                .first()
                .ok_or_else(|| TaskError::new(format!("missing quantile column {col_idx}")))?;
            quantile_splits.push(val.clone());
        }

        Ok(PartitionPlan {
            column,
            boundaries: boundaries_from_splits(min_val, &quantile_splits, max_val),
        })
    }
}

pub fn build_quantile_query(
    source_table: &str,
    partition_column: &str,
    num_partitions: u32,
    scope: &IndexingScope,
) -> String {
    let quantile_columns: Vec<String> = (1..num_partitions)
        .map(|i| {
            let level = i as f64 / num_partitions as f64;
            format!("toString(quantileTDigest({level})({partition_column})) as q{i}")
        })
        .collect();

    let select_parts = std::iter::once(format!("toString(min({partition_column})) as min_val"))
        .chain(quantile_columns)
        .chain(std::iter::once(format!(
            "toString(max({partition_column})) as max_val"
        )))
        .collect::<Vec<_>>()
        .join(", ");

    let scope_filter = match scope {
        IndexingScope::Global => String::new(),
        IndexingScope::Namespace { traversal_path, .. } => {
            format!("AND startsWith(traversal_path, '{traversal_path}')")
        }
    };

    format!(
        "SELECT {select_parts} \
         FROM {source_table} \
         WHERE _siphon_deleted = false {scope_filter}"
    )
}

pub fn boundaries_from_splits(
    min_val: &str,
    quantile_splits: &[String],
    max_val: &str,
) -> Vec<PartitionBounds> {
    let mut boundaries = Vec::with_capacity(quantile_splits.len() + 1);
    let mut lower = min_val.to_string();

    for split in quantile_splits {
        boundaries.push(PartitionBounds::Range {
            lower_bound: lower,
            upper_bound: split.clone(),
        });
        lower = split.clone();
    }

    boundaries.push(PartitionBounds::Range {
        lower_bound: lower,
        upper_bound: max_val.to_string(),
    });

    boundaries
}

pub fn partition_column(order_by: &[String], scope: EtlScope) -> Option<&str> {
    let skip = match scope {
        EtlScope::Namespaced => 1,
        EtlScope::Global => 0,
    };
    order_by.get(skip).map(String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundaries_from_three_splits() {
        let bounds =
            boundaries_from_splits("1", &["100".into(), "200".into(), "300".into()], "400");

        assert_eq!(bounds.len(), 4);
        assert_eq!(
            bounds[0],
            PartitionBounds::Range {
                lower_bound: "1".into(),
                upper_bound: "100".into()
            }
        );
        assert_eq!(
            bounds[1],
            PartitionBounds::Range {
                lower_bound: "100".into(),
                upper_bound: "200".into()
            }
        );
        assert_eq!(
            bounds[2],
            PartitionBounds::Range {
                lower_bound: "200".into(),
                upper_bound: "300".into()
            }
        );
        assert_eq!(
            bounds[3],
            PartitionBounds::Range {
                lower_bound: "300".into(),
                upper_bound: "400".into()
            }
        );
    }

    #[test]
    fn boundaries_from_single_split() {
        let bounds = boundaries_from_splits("1", &["50".into()], "100");

        assert_eq!(bounds.len(), 2);
        assert_eq!(
            bounds[0],
            PartitionBounds::Range {
                lower_bound: "1".into(),
                upper_bound: "50".into()
            }
        );
        assert_eq!(
            bounds[1],
            PartitionBounds::Range {
                lower_bound: "50".into(),
                upper_bound: "100".into()
            }
        );
    }

    #[test]
    fn quantile_query_global_two_partitions() {
        let sql = build_quantile_query("siphon_users", "id", 2, &IndexingScope::Global);

        assert!(sql.contains("min(id)"), "sql: {sql}");
        assert!(sql.contains("max(id)"), "sql: {sql}");
        assert!(sql.contains("quantileTDigest(0.5)(id)"), "sql: {sql}");
        assert!(sql.contains("FROM siphon_users"), "sql: {sql}");
        assert!(!sql.contains("traversal_path"), "sql: {sql}");
    }

    #[test]
    fn quantile_query_namespaced_four_partitions() {
        let scope = IndexingScope::Namespace {
            namespace_id: 100,
            traversal_path: "42/100/".to_string(),
        };
        let sql = build_quantile_query("siphon_p_merge_requests", "id", 4, &scope);

        assert!(sql.contains("quantileTDigest(0.25)(id)"), "sql: {sql}");
        assert!(sql.contains("quantileTDigest(0.5)(id)"), "sql: {sql}");
        assert!(sql.contains("quantileTDigest(0.75)(id)"), "sql: {sql}");
        assert!(
            sql.contains("startsWith(traversal_path, '42/100/')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn partition_column_namespaced_skips_traversal_path() {
        let order_by = vec!["traversal_path".into(), "id".into()];
        assert_eq!(
            partition_column(&order_by, EtlScope::Namespaced),
            Some("id")
        );
    }

    #[test]
    fn partition_column_global_uses_first() {
        let order_by = vec!["id".into()];
        assert_eq!(partition_column(&order_by, EtlScope::Global), Some("id"));
    }

    #[test]
    fn partition_column_none_when_no_non_scope_columns() {
        let order_by = vec!["traversal_path".into()];
        assert_eq!(partition_column(&order_by, EtlScope::Namespaced), None);
    }
}
