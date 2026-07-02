use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::try_join_all;
use thiserror::Error;
use tracing::debug;

use super::config::CodeTableNames;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};

#[async_trait]
pub trait StaleDataCleaner: Send + Sync {
    async fn delete_stale_data(
        &self,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
        watermark_time: DateTime<Utc>,
    ) -> Result<(), StaleDataCleanerError>;
}

#[derive(Debug, Error)]
pub enum StaleDataCleanerError {
    #[error(
        "failed to delete stale rows from {table} (traversal_path={traversal_path}, project_id={project_id}, branch={branch}): {reason}"
    )]
    Query {
        table: String,
        traversal_path: String,
        project_id: i64,
        branch: String,
        reason: String,
    },
}

pub struct ClickHouseStaleDataCleaner {
    client: Arc<ArrowClickHouseClient>,
    node_queries: Vec<(String, String)>,
    edge_queries: Vec<(String, String)>,
}

impl ClickHouseStaleDataCleaner {
    pub fn new(client: Arc<ArrowClickHouseClient>, table_names: &CodeTableNames) -> Self {
        let node_tables = table_names.node_tables();
        let node_queries = node_tables
            .iter()
            .map(|table| (table.to_string(), Self::build_node_delete_query(table)))
            .collect();

        let edge_queries = table_names
            .edge_table_names()
            .iter()
            .filter_map(|table| {
                let query = Self::build_edge_delete_query(table, &node_tables);
                if query.is_empty() {
                    None
                } else {
                    Some((table.to_string(), query))
                }
            })
            .collect();

        Self {
            client,
            node_queries,
            edge_queries,
        }
    }

    fn build_node_delete_query(table: &str) -> String {
        format!(
            r#"
            SELECT
                traversal_path,
                project_id,
                branch,
                id,
                {{watermark_time:DateTime64(6, 'UTC')}} - toIntervalMicrosecond(1) AS _version,
                true AS _deleted
            FROM {table} FINAL
            WHERE traversal_path = {{traversal_path:String}}
              AND project_id = {{project_id:Int64}}
              AND branch = {{branch:String}}
              AND _version < {{watermark_time:DateTime64(6, 'UTC')}}
            "#
        )
    }

    fn build_edge_delete_query(edge_table: &str, node_tables: &[&str]) -> String {
        // gl_code_edge has project_id + branch columns, so we can
        // filter directly without a subquery join.
        if edge_table.contains("code_edge") {
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
                FROM {edge_table} FINAL
                WHERE traversal_path = {{traversal_path:String}}
                  AND project_id = {{project_id:Int64}}
                  AND branch = {{branch:String}}
                  AND _version < {{watermark_time:DateTime64(6, 'UTC')}}
                "#,
            );
        }

        // Other edge tables (gl_edge) lack project_id/branch, so scope
        // via a source_id subquery from the node tables.
        let source_id_subqueries = node_tables
            .iter()
            .map(|t| {
                format!(
                    "SELECT id FROM {t} FINAL \
                     WHERE traversal_path = {{traversal_path:String}} \
                       AND project_id = {{project_id:Int64}} \
                       AND branch = {{branch:String}}"
                )
            })
            .collect::<Vec<_>>();

        if source_id_subqueries.is_empty() {
            return String::new();
        }

        let source_id_union = source_id_subqueries.join(" UNION ALL ");

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
            FROM {edge_table} FINAL
            WHERE traversal_path = {{traversal_path:String}}
              AND source_id IN ({source_id_union})
              AND _version < {{watermark_time:DateTime64(6, 'UTC')}}
            "#
        )
    }

    async fn tombstone_stale_rows(
        &self,
        table: &str,
        query: &str,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
        formatted_watermark: &str,
    ) -> Result<(), StaleDataCleanerError> {
        let query_error = |reason: String| StaleDataCleanerError::Query {
            table: table.to_string(),
            traversal_path: traversal_path.to_string(),
            project_id,
            branch: branch.to_string(),
            reason,
        };

        debug!(
            table,
            traversal_path, project_id, branch, "tombstoning stale rows"
        );
        let stale = self
            .client
            .query(query)
            .param("traversal_path", traversal_path)
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

        self.client
            .insert_arrow(table, &stale)
            .await
            .map_err(|e| query_error(e.to_string()))
    }
}

#[async_trait]
impl StaleDataCleaner for ClickHouseStaleDataCleaner {
    async fn delete_stale_data(
        &self,
        traversal_path: &str,
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

        debug!(project_id, branch, "stale data deletion complete");
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
        pub calls: Mutex<Vec<(String, i64, String, DateTime<Utc>)>>,
    }

    #[async_trait]
    impl StaleDataCleaner for MockStaleDataCleaner {
        async fn delete_stale_data(
            &self,
            traversal_path: &str,
            project_id: i64,
            branch: &str,
            watermark_time: DateTime<Utc>,
        ) -> Result<(), StaleDataCleanerError> {
            self.calls.lock().push((
                traversal_path.to_string(),
                project_id,
                branch.to_string(),
                watermark_time,
            ));
            Ok(())
        }
    }
}
