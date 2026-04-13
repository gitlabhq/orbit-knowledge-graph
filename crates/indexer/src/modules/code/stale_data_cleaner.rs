use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::try_join_all;
use thiserror::Error;
use tracing::debug;

use super::config::CodeTableNames;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};

const CODE_EDGE_SOURCE_KINDS: &[&str] = &[
    "Branch",
    "Directory",
    "File",
    "Definition",
    "ImportedSymbol",
];

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
    edge_table: String,
    edge_query: String,
}

impl ClickHouseStaleDataCleaner {
    pub fn new(client: Arc<ArrowClickHouseClient>, table_names: &CodeTableNames) -> Self {
        let node_queries = table_names
            .node_tables()
            .iter()
            .map(|table| (table.to_string(), Self::build_node_delete_query(table)))
            .collect();

        // TODO(multi-edge-tables): when gl_code_edge is declared, table_names.edge
        // will already point to the correct table via CodeTableNames::from_ontology.
        Self {
            client,
            node_queries,
            edge_table: table_names.edge.clone(),
            edge_query: Self::build_edge_delete_query(&table_names.edge),
        }
    }

    fn build_node_delete_query(table: &str) -> String {
        format!(
            r#"
            INSERT INTO {table} (traversal_path, project_id, branch, id, _deleted)
            SELECT
                traversal_path,
                project_id,
                branch,
                id,
                true AS _deleted
            FROM {table}
            WHERE traversal_path = {{traversal_path:String}}
              AND project_id = {{project_id:Int64}}
              AND branch = {{branch:String}}
            GROUP BY traversal_path, project_id, branch, id
            HAVING max(_version) != {{watermark_time:DateTime64(6, 'UTC')}}
            "#
        )
    }

    fn build_edge_delete_query(edge_table: &str) -> String {
        let source_kinds = CODE_EDGE_SOURCE_KINDS
            .iter()
            .map(|k| format!("'{k}'"))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            r#"
            INSERT INTO {edge_table}
                (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _deleted)
            SELECT
                traversal_path,
                source_id,
                source_kind,
                relationship_kind,
                target_id,
                target_kind,
                true AS _deleted
            FROM {edge_table}
            WHERE traversal_path = {{traversal_path:String}}
              AND source_kind IN ({source_kinds})
            GROUP BY traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind
            HAVING max(_version) != {{watermark_time:DateTime64(6, 'UTC')}}
            "#
        )
    }

    async fn delete_stale_nodes(
        &self,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
        formatted_watermark: &str,
    ) -> Result<(), StaleDataCleanerError> {
        let futures = self.node_queries.iter().map(|(table, query)| async move {
            debug!(table, project_id, branch, "deleting stale nodes");

            self.client
                .query(query)
                .param("traversal_path", traversal_path)
                .param("project_id", project_id)
                .param("branch", branch)
                .param("watermark_time", formatted_watermark)
                .execute()
                .await
                .map_err(|e| StaleDataCleanerError::Query {
                    table: table.to_string(),
                    traversal_path: traversal_path.to_string(),
                    project_id,
                    branch: branch.to_string(),
                    reason: e.to_string(),
                })
        });

        try_join_all(futures).await?;
        Ok(())
    }

    async fn delete_stale_edges(
        &self,
        traversal_path: &str,
        formatted_watermark: &str,
    ) -> Result<(), StaleDataCleanerError> {
        debug!(traversal_path, "deleting stale edges");

        self.client
            .query(&self.edge_query)
            .param("traversal_path", traversal_path)
            .param("watermark_time", formatted_watermark)
            .execute()
            .await
            .map_err(|e| StaleDataCleanerError::Query {
                table: self.edge_table.clone(),
                traversal_path: traversal_path.to_string(),
                project_id: 0,
                branch: String::new(),
                reason: e.to_string(),
            })
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

        self.delete_stale_nodes(traversal_path, project_id, branch, &formatted_watermark)
            .await?;

        self.delete_stale_edges(traversal_path, &formatted_watermark)
            .await?;

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
        #[allow(clippy::type_complexity)]
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
