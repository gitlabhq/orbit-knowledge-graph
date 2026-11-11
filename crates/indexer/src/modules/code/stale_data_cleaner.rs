use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::try_join_all;
use orbit_log_fields::debug;
use thiserror::Error;

use super::config::CodeTableNames;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT, insert_overrides};
use crate::durability::WriteDurability;
use crate::lifetime::{IDS_PER_PARAM, Tombstone};
use clickhouse_client::FromArrowColumn;
use orbit_utils::traversal_path::TraversalPath;
use std::collections::HashMap;

#[async_trait]
pub trait StaleDataCleaner: Send + Sync {
    async fn delete_stale_data(
        &self,
        traversal_path: &TraversalPath,
        project_id: i64,
        branch: &str,
        watermark_time: DateTime<Utc>,
    ) -> Result<(), StaleDataCleanerError>;
}

#[derive(Debug, Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum StaleDataCleanerError {
    #[error(
        "failed to delete stale rows from {table} (traversal_path={traversal_path}, project_id={project_id}, branch={branch}): {reason}"
    )]
    Query {
        table: String,
        traversal_path: TraversalPath,
        project_id: i64,
        branch: String,
        reason: String,
    },
}

pub struct ClickHouseStaleDataCleaner {
    client: Arc<ArrowClickHouseClient>,
    node_queries: Vec<(String, String)>,
    edge_queries: Vec<(String, String)>,
    foreign_edges: HashMap<String, Vec<Tombstone>>,
}

impl ClickHouseStaleDataCleaner {
    pub fn new(client: Arc<ArrowClickHouseClient>, table_names: &CodeTableNames) -> Self {
        let node_tables = table_names.node_tables();
        let node_queries = node_tables
            .iter()
            .map(|table| {
                (
                    table.to_string(),
                    Self::build_node_delete_query(table, table_names.immutable_columns(table)),
                )
            })
            .collect();

        let edge_queries = table_names
            .edge_table_names()
            .iter()
            .map(|table| {
                (
                    table.to_string(),
                    Self::build_edge_delete_query(table, table_names),
                )
            })
            .collect();

        Self {
            client,
            foreign_edges: node_tables
                .iter()
                .map(|table| (table.to_string(), table_names.foreign_edges(table).to_vec()))
                .collect(),
            node_queries,
            edge_queries,
        }
    }

    fn build_node_delete_query(table: &str, carried_columns: &[String]) -> String {
        let carried = carried_columns
            .iter()
            .map(|column| format!("{column},\n                "))
            .collect::<String>();
        format!(
            r#"
            SELECT
                traversal_path,
                project_id,
                branch,
                id,
                {carried}{{watermark_time:DateTime64(6, 'UTC')}} - toIntervalMicrosecond(1) AS _version,
                true AS _deleted
            FROM {table} AS s FINAL
            WHERE traversal_path = {{traversal_path:String}}
              AND project_id = {{project_id:Int64}}
              AND branch = {{branch:String}}
              AND s._version < {{watermark_time:DateTime64(6, 'UTC')}}
            "#
        )
    }

    fn build_edge_delete_query(edge_table: &str, table_names: &CodeTableNames) -> String {
        let written = table_names.written_variants_sql();
        if table_names.branch_scoped(edge_table) {
            return format!(
                r#"
                SELECT
                    traversal_path,
                    project_id,
                    branch,
                    source_id,
                    source_kind,
                    relationship_kind,
                    target_id,
                    target_kind,
                    {{watermark_time:DateTime64(6, 'UTC')}} - toIntervalMicrosecond(1) AS _version,
                    true AS _deleted
                FROM {edge_table} AS s FINAL
                WHERE traversal_path = {{traversal_path:String}}
                  AND project_id = {{project_id:Int64}}
                  AND branch = {{branch:String}}
                  AND {written}
                  AND s._version < {{watermark_time:DateTime64(6, 'UTC')}}
                "#,
            );
        }

        // A table without project_id/branch is scoped by the code indexer's variants: a flat
        // literal needs no source-id subquery (a UNION wedges mutation replay,
        // #1221) and survives the source node being tombstoned first. Correct only
        // because the indexer writes one branch per project into such a table; a
        // second branch would tombstone the first's edges below the watermark.
        format!(
            r#"
            SELECT
                traversal_path,
                source_id,
                source_kind,
                relationship_kind,
                target_id,
                target_kind,
                {{watermark_time:DateTime64(6, 'UTC')}} - toIntervalMicrosecond(1) AS _version,
                true AS _deleted
            FROM {edge_table} AS s FINAL
            WHERE traversal_path = {{traversal_path:String}}
              AND {written}
              AND s._version < {{watermark_time:DateTime64(6, 'UTC')}} - toIntervalMicrosecond(1)
            "#
        )
    }

    async fn tombstone_stale_rows(
        &self,
        table: &str,
        query: &str,
        traversal_path: &TraversalPath,
        project_id: i64,
        branch: &str,
        formatted_watermark: &str,
    ) -> Result<(), StaleDataCleanerError> {
        let query_error = |reason: String| StaleDataCleanerError::Query {
            table: table.to_string(),
            traversal_path: traversal_path.clone(),
            project_id,
            branch: branch.to_string(),
            reason,
        };

        debug!(safe(TABLE) = table, "tombstoning stale rows");
        let stale = self
            .client
            .query(query)
            .param("traversal_path", traversal_path.as_str())
            .param("project_id", project_id)
            .param("branch", branch)
            .param("watermark_time", formatted_watermark)
            .fetch_arrow()
            .await
            .map_err(|e| query_error(e.to_string()))?;

        let stale: Vec<_> = stale.into_iter().filter(|b| b.num_rows() > 0).collect();
        if stale.is_empty() {
            return Ok(());
        }

        let edges = self.foreign_edges.get(table).map_or(&[][..], Vec::as_slice);
        let ids = match edges {
            [] => Vec::new(),
            _ => i64::extract_column(&stale, 3).map_err(|e| query_error(e.to_string()))?,
        };
        let sql = self
            .client
            .build_insert_sql_with_overrides(table, insert_overrides(WriteDurability::Durable));
        self.client
            .insert_arrow_streaming_with_sql(table, &sql, stale)
            .await
            .map_err(|e| query_error(e.to_string()))?;
        for edge in edges {
            for ids in ids.chunks(IDS_PER_PARAM) {
                self.client
                    .insert_query(&edge.sql)
                    .param("ids", ids)
                    .execute()
                    .await
                    .map_err(|e| query_error(e.to_string()))?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl StaleDataCleaner for ClickHouseStaleDataCleaner {
    async fn delete_stale_data(
        &self,
        traversal_path: &TraversalPath,
        project_id: i64,
        branch: &str,
        watermark_time: DateTime<Utc>,
    ) -> Result<(), StaleDataCleanerError> {
        let formatted_watermark = watermark_time.format(TIMESTAMP_FORMAT).to_string();

        for queries in [&self.node_queries, &self.edge_queries] {
            try_join_all(queries.iter().map(|(table, query)| {
                self.tombstone_stale_rows(
                    table,
                    query,
                    traversal_path,
                    project_id,
                    branch,
                    &formatted_watermark,
                )
            }))
            .await?;
        }

        debug!("stale data deletion complete");
        Ok(())
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;

    #[derive(Default)]
    pub struct MockStaleDataCleaner {
        #[allow(
            clippy::type_complexity,
            reason = "test-only call recorder; the tuple mirrors the trait method arguments"
        )]
        pub calls: Mutex<Vec<(TraversalPath, i64, String, DateTime<Utc>)>>,
    }

    #[async_trait]
    impl StaleDataCleaner for MockStaleDataCleaner {
        async fn delete_stale_data(
            &self,
            traversal_path: &TraversalPath,
            project_id: i64,
            branch: &str,
            watermark_time: DateTime<Utc>,
        ) -> Result<(), StaleDataCleanerError> {
            self.calls.lock().push((
                traversal_path.clone(),
                project_id,
                branch.to_string(),
                watermark_time,
            ));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_tombstones_carry_the_unchanging_columns() {
        let names = CodeTableNames::from_catalog(
            &query_data_model::Catalog::<query_data_model::ClickHouse>::embedded().unwrap(),
        )
        .expect("code tables");
        let carried = names.immutable_columns(&names.file);
        assert!(carried.iter().any(|c| c == "path"), "{carried:?}");
        let sql = ClickHouseStaleDataCleaner::build_node_delete_query(&names.file, carried);
        for column in carried {
            assert!(sql.contains(&format!("{column},")), "{sql}");
        }
        assert!(sql.contains("true AS _deleted"), "{sql}");
    }
}
