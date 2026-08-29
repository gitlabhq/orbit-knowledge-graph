use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::try_join_all;
use thiserror::Error;
use tracing::debug;

use super::config::CodeTableNames;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use orbit_utils::traversal_path::TraversalPath;

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

#[derive(Debug, Error)]
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
            "DELETE FROM {table} \
             WHERE traversal_path = {{traversal_path:String}} \
             AND project_id = {{project_id:Int64}} \
             AND branch = {{branch:String}} \
             AND _version < {{watermark_time:DateTime64(6, 'UTC')}}"
        )
    }

    fn build_edge_delete_query(edge_table: &str, node_tables: &[&str]) -> String {
        if edge_table.contains("code_edge") {
            return format!(
                "DELETE FROM {edge_table} \
                 WHERE traversal_path = {{traversal_path:String}} \
                 AND project_id = {{project_id:Int64}} \
                 AND branch = {{branch:String}} \
                 AND _version < {{watermark_time:DateTime64(6, 'UTC')}}"
            );
        }

        // ClickHouse stores a lightweight-delete predicate verbatim as a mutation
        // command and re-parses it when replaying on each replica, where a UNION
        // fails with "UNION mode UNION_DEFAULT must be normalized" and retries
        // forever, so the per-table lookups are OR'd instead of unioned.
        let source_id_predicates = node_tables
            .iter()
            .map(|t| {
                format!(
                    "source_id IN (SELECT id FROM {t} FINAL \
                     WHERE traversal_path = {{traversal_path:String}} \
                       AND project_id = {{project_id:Int64}} \
                       AND branch = {{branch:String}})"
                )
            })
            .collect::<Vec<_>>();

        if source_id_predicates.is_empty() {
            return String::new();
        }

        let source_id_match = source_id_predicates.join(" OR ");

        format!(
            "DELETE FROM {edge_table} \
             WHERE traversal_path = {{traversal_path:String}} \
             AND ({source_id_match}) \
             AND _version < {{watermark_time:DateTime64(6, 'UTC')}}"
        )
    }

    async fn delete_stale_rows(
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

        debug!(
            table,
            %traversal_path, project_id, branch, "lightweight-deleting stale rows"
        );
        self.client
            .query(query)
            .param("traversal_path", traversal_path.as_str())
            .param("project_id", project_id)
            .param("branch", branch)
            .param("watermark_time", formatted_watermark)
            .execute()
            .await
            .map_err(|e| query_error(e.to_string()))
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
                self.delete_stale_rows(
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
mod tests {
    use super::*;

    const NODE_TABLES: [&str; 4] = [
        "v93_gl_directory",
        "v93_gl_file",
        "v93_gl_definition",
        "v93_gl_imported_symbol",
    ];

    #[test]
    fn edge_delete_predicate_never_contains_a_union() {
        let sql = ClickHouseStaleDataCleaner::build_edge_delete_query("v93_gl_edge", &NODE_TABLES);
        assert!(
            !sql.contains("UNION"),
            "ClickHouse stores a lightweight-delete predicate verbatim as a mutation and \
             re-parses it on replay, where a UNION fails with \"UNION mode UNION_DEFAULT \
             must be normalized\" and retries forever: {sql}"
        );
    }

    #[test]
    fn edge_delete_scopes_source_ids_to_every_node_table() {
        let sql = ClickHouseStaleDataCleaner::build_edge_delete_query("v93_gl_edge", &NODE_TABLES);
        for table in NODE_TABLES {
            assert!(
                sql.contains(&format!("source_id IN (SELECT id FROM {table} FINAL")),
                "{table} must still narrow the delete to this project's code nodes: {sql}"
            );
        }
    }

    #[test]
    fn edge_delete_isolates_the_source_id_alternation() {
        let sql = ClickHouseStaleDataCleaner::build_edge_delete_query("v93_gl_edge", &NODE_TABLES);
        let alternation = sql
            .split_once("AND (")
            .and_then(|(_, rest)| rest.rsplit_once(") AND _version <"))
            .map(|(inner, _)| inner)
            .expect("the source-id alternation must be parenthesised: {sql}");
        assert_eq!(
            alternation.matches("source_id IN (").count(),
            NODE_TABLES.len(),
            "every lookup must sit inside the parentheses. AND binds tighter than OR, so an \
             unparenthesised alternation reads as (path AND t1) OR t2 OR (t3 AND version) and \
             deletes rows from other namespaces and above the watermark: {sql}"
        );
    }

    #[test]
    fn edge_delete_keeps_project_branch_and_watermark_scoping() {
        let sql = ClickHouseStaleDataCleaner::build_edge_delete_query("v93_gl_edge", &NODE_TABLES);
        assert!(sql.starts_with("DELETE FROM v93_gl_edge"), "{sql}");
        assert!(
            sql.contains("traversal_path = {traversal_path:String}"),
            "{sql}"
        );
        assert!(sql.contains("project_id = {project_id:Int64}"), "{sql}");
        assert!(sql.contains("branch = {branch:String}"), "{sql}");
        assert!(
            sql.contains("_version < {watermark_time:DateTime64(6, 'UTC')}"),
            "{sql}"
        );
    }

    #[test]
    fn code_edge_delete_filters_directly_without_a_subquery() {
        let sql =
            ClickHouseStaleDataCleaner::build_edge_delete_query("v93_gl_code_edge", &NODE_TABLES);
        assert!(!sql.contains("SELECT"), "{sql}");
        assert!(sql.contains("project_id = {project_id:Int64}"), "{sql}");
    }

    #[test]
    fn edge_delete_is_skipped_when_no_node_tables_resolve() {
        assert!(ClickHouseStaleDataCleaner::build_edge_delete_query("v93_gl_edge", &[]).is_empty());
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
