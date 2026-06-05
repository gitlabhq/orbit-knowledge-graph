use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::UInt64Array;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::try_join_all;
use gkg_utils::arrow::ArrowUtils;
use thiserror::Error;
use tracing::debug;

use super::config::CodeTableNames;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};

/// Skip cleanup when a node table re-emitted fewer than this fraction of its
/// prior live rows. A healthy re-index re-emits ~all current entities; a run
/// that emits a small fraction is degraded (build/converter/sink under-emission)
/// and tombstoning its complement wipes live data permanently. Only the
/// catastrophic-wipe shape (near-zero emission against a populated table) trips
/// this; ordinary churn — even a large refactor — stays well above 10%.
const CLEANUP_MIN_REEMIT_RATIO: f64 = 0.10;

/// Tables with fewer prior live rows than this are too small to gate on, so a
/// brand-new or tiny repo is never blocked by the completeness guard.
const CLEANUP_PRIOR_LIVE_FLOOR: u64 = 50;

#[async_trait]
pub trait StaleDataCleaner: Send + Sync {
    /// Tombstone prior-version rows that the just-completed run did not
    /// re-emit, unless the run under-emitted (see [`CleanupOutcome`]).
    ///
    /// `written_rows` maps destination table name -> rows this run actually
    /// wrote to ClickHouse (from `BufferedClickHouseSink::flush`), which is the
    /// ground truth — the in-memory `stats` counter over-reports when the
    /// converter/sink drops rows.
    async fn delete_stale_data(
        &self,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
        watermark_time: DateTime<Utc>,
        written_rows: &HashMap<String, u64>,
    ) -> Result<CleanupOutcome, StaleDataCleanerError>;
}

/// Result of a cleanup attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupOutcome {
    /// The run re-emitted a healthy fraction of every node table; prior-version
    /// rows not re-emitted were tombstoned.
    Ran,
    /// Cleanup was refused because the run re-emitted far fewer rows for `table`
    /// than the prior live state. Tombstoning would have wiped live data, so
    /// nothing was deleted. The prior rows stay live (bounded ghost data) and a
    /// later complete run cleans them up.
    SkippedUnderEmit {
        table: String,
        prior_live: u64,
        written: u64,
    },
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

/// True when `written` is a small enough fraction of `prior_live` that running
/// cleanup would tombstone live data. Pure so the gate logic is unit-testable
/// without ClickHouse.
fn is_under_emit(prior_live: u64, written: u64, min_reemit_ratio: f64) -> bool {
    prior_live >= CLEANUP_PRIOR_LIVE_FLOOR
        && (written as f64) < min_reemit_ratio * (prior_live as f64)
}

pub struct ClickHouseStaleDataCleaner {
    client: Arc<ArrowClickHouseClient>,
    node_queries: Vec<(String, String)>,
    edge_queries: Vec<(String, String)>,
    min_reemit_ratio: f64,
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
            min_reemit_ratio: CLEANUP_MIN_REEMIT_RATIO,
        }
    }

    /// Override the completeness threshold (test/ops escape hatch).
    pub fn with_min_reemit_ratio(mut self, ratio: f64) -> Self {
        self.min_reemit_ratio = ratio;
        self
    }

    fn build_node_delete_query(table: &str) -> String {
        // `_version` is set explicitly to the tombstone watermark instead of
        // relying on the column DEFAULT now64(6). The default stamps every
        // tombstone at cleanup wall-clock time, which always outranks the
        // current run's live rows under ReplacingMergeTree FINAL and makes
        // retries write fresh zombie versions. A deterministic
        // `watermark + 1µs` wins only over genuinely older rows and is
        // idempotent across retries.
        format!(
            r#"
            INSERT INTO {table} (traversal_path, project_id, branch, id, _version, _deleted)
            SELECT
                traversal_path,
                project_id,
                branch,
                id,
                {{tombstone_version:DateTime64(6, 'UTC')}} AS _version,
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
                INSERT INTO {edge_table}
                    (traversal_path, project_id, branch, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted)
                SELECT
                    traversal_path,
                    project_id,
                    branch,
                    source_id,
                    source_kind,
                    relationship_kind,
                    target_id,
                    target_kind,
                    {{tombstone_version:DateTime64(6, 'UTC')}} AS _version,
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
            INSERT INTO {edge_table}
                (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted)
            SELECT
                traversal_path,
                source_id,
                source_kind,
                relationship_kind,
                target_id,
                target_kind,
                {{tombstone_version:DateTime64(6, 'UTC')}} AS _version,
                true AS _deleted
            FROM {edge_table} FINAL
            WHERE traversal_path = {{traversal_path:String}}
              AND source_id IN ({source_id_union})
              AND _version < {{watermark_time:DateTime64(6, 'UTC')}}
            "#
        )
    }

    /// Count distinct ids that were live (latest pre-run version not deleted)
    /// for `table` before this run. `_version < watermark` excludes the current
    /// run's own writes, so this is the prior baseline the run is compared to.
    async fn count_prior_live(
        &self,
        table: &str,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
        formatted_watermark: &str,
    ) -> Result<u64, StaleDataCleanerError> {
        let query = format!(
            "SELECT count() AS live FROM ( \
                SELECT id, argMax(_deleted, _version) AS deleted \
                FROM {table} \
                WHERE traversal_path = {{traversal_path:String}} \
                  AND project_id = {{project_id:Int64}} \
                  AND branch = {{branch:String}} \
                  AND _version < {{watermark_time:DateTime64(6, 'UTC')}} \
                GROUP BY id \
             ) WHERE deleted = false"
        );

        let map_err = |reason: String| StaleDataCleanerError::Query {
            table: table.to_string(),
            traversal_path: traversal_path.to_string(),
            project_id,
            branch: branch.to_string(),
            reason,
        };

        let batches = self
            .client
            .query(&query)
            .param("traversal_path", traversal_path)
            .param("project_id", project_id)
            .param("branch", branch)
            .param("watermark_time", formatted_watermark)
            .fetch_arrow()
            .await
            .map_err(|e| map_err(e.to_string()))?;

        let Some(batch) = batches.into_iter().next() else {
            return Ok(0);
        };
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        let col: &UInt64Array = ArrowUtils::get_column_by_index(&batch, 0)
            .ok_or_else(|| map_err("count() column missing or not UInt64".to_string()))?;
        Ok(col.value(0))
    }

    async fn delete_stale_nodes(
        &self,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
        formatted_watermark: &str,
        tombstone_version: &str,
    ) -> Result<(), StaleDataCleanerError> {
        let futures = self.node_queries.iter().map(|(table, query)| async move {
            debug!(table, project_id, branch, "deleting stale nodes");

            self.client
                .insert_query(query)
                .param("traversal_path", traversal_path)
                .param("project_id", project_id)
                .param("branch", branch)
                .param("watermark_time", formatted_watermark)
                .param("tombstone_version", tombstone_version)
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
        project_id: i64,
        branch: &str,
        formatted_watermark: &str,
        tombstone_version: &str,
    ) -> Result<(), StaleDataCleanerError> {
        let futures = self.edge_queries.iter().map(|(table, query)| async move {
            debug!(
                table,
                traversal_path, project_id, branch, "deleting stale edges"
            );

            self.client
                .insert_query(query)
                .param("traversal_path", traversal_path)
                .param("project_id", project_id)
                .param("branch", branch)
                .param("watermark_time", formatted_watermark)
                .param("tombstone_version", tombstone_version)
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
}

#[async_trait]
impl StaleDataCleaner for ClickHouseStaleDataCleaner {
    async fn delete_stale_data(
        &self,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
        watermark_time: DateTime<Utc>,
        written_rows: &HashMap<String, u64>,
    ) -> Result<CleanupOutcome, StaleDataCleanerError> {
        let formatted_watermark = watermark_time.format(TIMESTAMP_FORMAT).to_string();

        // Completeness guard. The cleaner deletes "every prior id this run did
        // not re-emit". On a degraded run (the converter/sink dropped most
        // rows) that is a mass wipe of live data. Refuse if any node table was
        // re-emitted far below its prior live count; the run still committed
        // whatever it wrote, so no data is lost — only the destructive delete
        // is withheld until a complete run.
        for (table, _query) in &self.node_queries {
            let prior_live = self
                .count_prior_live(
                    table,
                    traversal_path,
                    project_id,
                    branch,
                    &formatted_watermark,
                )
                .await?;
            let written = written_rows.get(table).copied().unwrap_or(0);
            if is_under_emit(prior_live, written, self.min_reemit_ratio) {
                return Ok(CleanupOutcome::SkippedUnderEmit {
                    table: table.clone(),
                    prior_live,
                    written,
                });
            }
        }

        let tombstone_version = (watermark_time + chrono::Duration::microseconds(1))
            .format(TIMESTAMP_FORMAT)
            .to_string();

        self.delete_stale_nodes(
            traversal_path,
            project_id,
            branch,
            &formatted_watermark,
            &tombstone_version,
        )
        .await?;

        self.delete_stale_edges(
            traversal_path,
            project_id,
            branch,
            &formatted_watermark,
            &tombstone_version,
        )
        .await?;

        debug!(project_id, branch, "stale data deletion complete");
        Ok(CleanupOutcome::Ran)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn under_emit_trips_on_near_zero_emission_against_populated_table() {
        // The 22:35 KG wipe shape: 941 written against 12,684 prior live.
        assert!(is_under_emit(12_684, 941, CLEANUP_MIN_REEMIT_RATIO));
        assert!(is_under_emit(45_469, 0, CLEANUP_MIN_REEMIT_RATIO));
    }

    #[test]
    fn under_emit_allows_healthy_reindex_and_ordinary_churn() {
        assert!(!is_under_emit(12_684, 12_600, CLEANUP_MIN_REEMIT_RATIO));
        // A large refactor halving the definitions still clears the 10% floor.
        assert!(!is_under_emit(12_000, 6_000, CLEANUP_MIN_REEMIT_RATIO));
    }

    #[test]
    fn under_emit_ignores_tiny_tables() {
        // Below the floor a zero-emission run is not treated as a wipe, so a
        // brand-new or tiny repo is never blocked.
        assert!(!is_under_emit(
            CLEANUP_PRIOR_LIVE_FLOOR - 1,
            0,
            CLEANUP_MIN_REEMIT_RATIO
        ));
    }

    #[test]
    fn node_tombstone_sets_version_explicitly() {
        let sql = ClickHouseStaleDataCleaner::build_node_delete_query("v99_gl_definition");
        assert!(
            sql.contains("id, _version, _deleted"),
            "tombstone INSERT must list _version so it does not fall back to DEFAULT now64(6): {sql}"
        );
        assert!(sql.contains("{tombstone_version:DateTime64(6, 'UTC')} AS _version"));
    }

    #[test]
    fn edge_tombstone_sets_version_explicitly() {
        let code_edge = ClickHouseStaleDataCleaner::build_edge_delete_query(
            "v99_gl_code_edge",
            &["v99_gl_definition"],
        );
        assert!(code_edge.contains("target_kind, _version, _deleted"));
        assert!(code_edge.contains("{tombstone_version:DateTime64(6, 'UTC')} AS _version"));

        let gl_edge = ClickHouseStaleDataCleaner::build_edge_delete_query(
            "v99_gl_edge",
            &["v99_gl_definition"],
        );
        assert!(gl_edge.contains("target_kind, _version, _deleted"));
        assert!(gl_edge.contains("{tombstone_version:DateTime64(6, 'UTC')} AS _version"));
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
        pub calls: Mutex<Vec<(String, i64, String, DateTime<Utc>, HashMap<String, u64>)>>,
        /// Outcome the mock returns; defaults to `Ran`.
        pub forced_outcome: Mutex<Option<CleanupOutcome>>,
    }

    #[async_trait]
    impl StaleDataCleaner for MockStaleDataCleaner {
        async fn delete_stale_data(
            &self,
            traversal_path: &str,
            project_id: i64,
            branch: &str,
            watermark_time: DateTime<Utc>,
            written_rows: &HashMap<String, u64>,
        ) -> Result<CleanupOutcome, StaleDataCleanerError> {
            self.calls.lock().push((
                traversal_path.to_string(),
                project_id,
                branch.to_string(),
                watermark_time,
                written_rows.clone(),
            ));
            Ok(self
                .forced_outcome
                .lock()
                .clone()
                .unwrap_or(CleanupOutcome::Ran))
        }
    }
}
