use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use croner::Cron;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::campaign::CampaignState;
use crate::clickhouse::ArrowClickHouseClient;
use crate::nats::NatsServices;
use crate::orchestrator::scheduled::ScheduledTaskMetrics;
use crate::orchestrator::scheduled::{ScheduledTask, TaskError};
use crate::topic::NamespaceIndexingRequest;
use crate::types::Envelope;
use clickhouse_client::FromArrowColumn;
use std::sync::LazyLock;

use gkg_server_config::{NamespaceDispatcherConfig, ScheduleConfiguration};

/// Enabled namespace ID + traversal path pairs from the datalake.
static ENABLED_NAMESPACE_QUERY: LazyLock<String> = LazyLock::new(|| {
    let del = ontology::siphon_deleted_column();
    format!(
        "SELECT root_namespace_id, traversal_path \
         FROM siphon_knowledge_graph_enabled_namespaces \
         WHERE {del} = false AND traversal_path != ''"
    )
});

/// Siphon source tables eligible for dirty-detection, derived from the
/// ontology. Only namespaced entities whose `source` table carries both the
/// watermark column and `traversal_path` are included — tables where
/// `SELECT DISTINCT traversal_path WHERE <watermark> > <cutoff>` is valid.
///
/// Entities whose watermark lives on a different JOINed table than `source`
/// (Group via `siphon_namespaces`, Project via `siphon_projects` — both lack
/// `traversal_path`) are excluded and ride on the periodic full sweep.
/// See #908 for a future JOIN-based dirty query for those entities.
static DIRTY_DETECTION_TABLES: LazyLock<Vec<DirtyDetectionTable>> = LazyLock::new(|| {
    let ontology = ontology::Ontology::load_embedded().expect("embedded ontology must be valid");
    let mut seen = HashSet::new();
    let mut tables = Vec::new();

    let mut push_etl = |etl: &ontology::EtlConfig| {
        if etl.scope() != ontology::EtlScope::Namespaced {
            return;
        }
        if let Some((table, col)) = dirty_detection_table_and_column(etl)
            && seen.insert(table.clone())
        {
            tables.push(DirtyDetectionTable {
                table,
                watermark_column: col,
            });
        }
    };

    for node in ontology.nodes() {
        if let Some(etl) = &node.etl {
            push_etl(etl);
        }
    }
    for derived in ontology.derived_entities() {
        push_etl(&derived.etl);
    }

    tables
});

/// Resolves which table to query and which column to filter on for dirty
/// detection. Returns `None` when the entity cannot be dirty-detected with a
/// simple single-table query (the watermark lives on a JOINed table whose
/// `traversal_path` and watermark don't coexist on `source`).
///
/// For `Table`-type ETLs, this is always `Some((source, watermark))`.
///
/// For `Query`-type ETLs, we compare the first table in `from` against
/// `source`. When they match (e.g. MergeRequest: `source=merge_requests`,
/// `from: merge_requests AS m`), the alias just disambiguates within the JOIN
/// and `source` carries both columns → include with unqualified watermark.
/// When they differ (e.g. Group: `source=namespace_traversal_paths`,
/// `from: siphon_namespaces namespace INNER JOIN …`), the watermark lives on
/// a different table that lacks `traversal_path` → exclude (covered by sweep).
fn dirty_detection_table_and_column(etl: &ontology::EtlConfig) -> Option<(String, String)> {
    match etl {
        ontology::EtlConfig::Table {
            source, watermark, ..
        } => Some((source.clone(), watermark.clone())),
        ontology::EtlConfig::Query {
            source,
            from,
            watermark,
            ..
        } => {
            let from_table = first_table_in_from(from);
            if from_table == *source {
                // Alias resolves to the source table itself — it carries both
                // `traversal_path` and the watermark column.
                Some((source.clone(), unqualified_column(watermark)))
            } else {
                // The watermark-bearing table (first in `from`) differs from
                // `source` and typically lacks `traversal_path` (e.g.
                // siphon_namespaces, siphon_projects). Covered by sweep.
                None
            }
        }
    }
}

/// Extracts the first table name from a `from` clause.
/// E.g. `"siphon_namespaces namespace INNER JOIN ..."` → `"siphon_namespaces"`.
fn first_table_in_from(from: &str) -> String {
    from.split_whitespace().next().unwrap_or(from).to_owned()
}

/// Strips a potential table-alias qualifier (e.g. `sn._siphon_watermark` →
/// `_siphon_watermark`). Query-type ETLs may prefix the watermark with the
/// table alias for disambiguation; dirty-detection queries the source table
/// directly and needs the bare column name.
fn unqualified_column(col: &str) -> String {
    col.rsplit_once('.')
        .map_or(col, |(_, bare)| bare)
        .to_owned()
}

struct DirtyDetectionTable {
    table: String,
    watermark_column: String,
}

pub struct NamespaceDispatcher {
    nats: Arc<dyn NatsServices>,
    datalake: ArrowClickHouseClient,
    metrics: ScheduledTaskMetrics,
    config: NamespaceDispatcherConfig,
    campaign: Arc<CampaignState>,
    /// Tracks the last sweep time to determine when a full sweep is due.
    /// Resets on process restart — bounded by the 1/min scheduler cadence lock.
    last_sweep: std::sync::Mutex<Option<chrono::DateTime<Utc>>>,
}

impl NamespaceDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        config: NamespaceDispatcherConfig,
        campaign: Arc<CampaignState>,
    ) -> Self {
        Self {
            nats,
            datalake,
            metrics,
            config,
            campaign,
            last_sweep: std::sync::Mutex::new(None),
        }
    }

    /// Returns true when it is time for a full sweep (dispatch all namespaces).
    fn is_sweep_due(&self) -> bool {
        let guard = self.last_sweep.lock().expect("lock not poisoned");
        let Some(last) = *guard else {
            return true;
        };
        let now = Utc::now();
        let Ok(cron) = std::str::FromStr::from_str(&self.config.sweep.cron) as Result<Cron, _>
        else {
            return true;
        };
        cron.find_next_occurrence(&last, false)
            .map(|next| now >= next)
            .unwrap_or(true)
    }

    fn mark_sweep_done(&self) {
        let mut guard = self.last_sweep.lock().expect("lock not poisoned");
        *guard = Some(Utc::now());
    }
}

#[async_trait]
impl ScheduledTask for NamespaceDispatcher {
    fn name(&self) -> &str {
        "dispatch.sdlc.namespace"
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();

        let result = self.dispatch_inner().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

impl NamespaceDispatcher {
    async fn dispatch_inner(&self) -> Result<(), TaskError> {
        let query_start = Instant::now();
        let arrow_batches = self
            .datalake
            .query(&ENABLED_NAMESPACE_QUERY)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                TaskError::new(error)
            })?;
        self.metrics
            .record_query_duration("enabled_namespaces", query_start.elapsed().as_secs_f64());

        let namespace_ids = i64::extract_column(&arrow_batches, 0).map_err(TaskError::new)?;
        let traversal_paths = String::extract_column(&arrow_batches, 1).map_err(TaskError::new)?;

        debug!(
            enabled_namespaces = namespace_ids.len(),
            "found enabled namespaces to dispatch indexing requests for"
        );

        let is_sweep = self.is_sweep_due();

        // Always compute the dirty set — even on sweep cycles it feeds the
        // silent-drop canary metric. On dirty-detection failure, fall back to
        // full dispatch (dirty_paths = None) for the whole cycle.
        let dirty_set = self.detect_dirty_namespaces().await;
        let dirty_paths = match (&dirty_set, is_sweep) {
            (_, true) => {
                debug!("full sweep cycle — dispatching all enabled namespaces");
                None
            }
            (Ok(dirty), false) => Some(dirty),
            (Err(_), false) => {
                debug!("dirty-detection failed — falling back to full dispatch");
                None
            }
        };

        let watermark = Utc::now();
        let campaign_id = self.campaign.current();
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;
        let mut dispatched_paths: HashSet<String> = HashSet::new();

        for (namespace_id, traversal_path) in namespace_ids.iter().zip(traversal_paths.iter()) {
            if !is_dispatchable_traversal_path(traversal_path) {
                warn!(
                    namespace_id = *namespace_id,
                    traversal_path = %traversal_path,
                    "skipping enabled namespace with invalid traversal_path"
                );
                continue;
            }

            let is_dirty = dirty_paths
                .as_ref()
                .is_none_or(|dirty| is_namespace_dirty(traversal_path, dirty));

            if !is_dirty {
                continue;
            }

            let request = NamespaceIndexingRequest {
                namespace: *namespace_id,
                traversal_path: traversal_path.clone(),
                watermark,
                dispatch_id: Uuid::new_v4(),
                campaign_id: campaign_id.clone(),
            };

            let subscription = request.publish_subscription();
            let envelope = Envelope::new(&request).map_err(|error| {
                self.metrics.record_error(self.name(), "publish");
                TaskError::new(error)
            })?;

            match self.nats.publish(&subscription, &envelope).await {
                Ok(()) => {
                    dispatched += 1;
                    dispatched_paths.insert(traversal_path.clone());
                    debug!(
                        namespace_id = *namespace_id,
                        traversal_path = %traversal_path,
                        "dispatched namespace indexing request"
                    );
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    skipped += 1;
                    debug!(
                        namespace_id = *namespace_id,
                        traversal_path = %traversal_path,
                        "skipped namespace indexing request, already in-flight"
                    );
                }
                Err(error) => {
                    self.metrics.record_error(self.name(), "publish");
                    return Err(TaskError::new(error));
                }
            }
        }

        if is_sweep {
            self.mark_sweep_done();
        }

        // Silent-drop canary: count namespaces that were actually dispatched
        // during a sweep but were NOT in the dirty set. Only meaningful when
        // the dirty set was successfully computed.
        let sweep_only = if is_sweep {
            if let Ok(dirty) = &dirty_set {
                let count = dispatched_paths
                    .iter()
                    .filter(|tp| !is_namespace_dirty(tp, dirty))
                    .count() as u64;
                if count > 0 {
                    self.metrics.record_sweep_only_dispatched(count);
                }
                count
            } else {
                0
            }
        } else {
            0
        };

        self.metrics
            .record_requests_published(self.name(), dispatched);
        self.metrics.record_requests_skipped(self.name(), skipped);

        info!(
            dispatched,
            skipped, sweep_only, is_sweep, "dispatched namespace indexing requests"
        );
        Ok(())
    }

    /// Queries each namespaced Siphon source table for recently-changed
    /// `traversal_path` values, returning the union of dirty paths.
    ///
    /// On any per-table query failure, returns `Err` so the caller falls back
    /// to full dispatch for the whole cycle.
    async fn detect_dirty_namespaces(&self) -> Result<HashSet<String>, TaskError> {
        let slack = ChronoDuration::seconds(self.config.sweep.slack_secs as i64);
        let cutoff = Utc::now() - self.config.schedule.interval_hint_chrono() - slack;
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();
        let mut dirty: HashSet<String> = HashSet::new();

        for table in DIRTY_DETECTION_TABLES.iter() {
            let query = format!(
                "SELECT DISTINCT traversal_path \
                 FROM {} \
                 WHERE {} > '{}'",
                table.table, table.watermark_column, cutoff_str
            );

            let query_start = Instant::now();
            let result = self.datalake.query(&query).fetch_arrow_with_summary().await;

            let duration = query_start.elapsed().as_secs_f64();

            match result {
                Ok((batches, summary)) => {
                    let paths = String::extract_column(&batches, 0).unwrap_or_default();
                    let read_rows = summary
                        .and_then(|s| s.read_rows())
                        .unwrap_or(paths.len() as u64);
                    self.metrics.record_dirty_detection_query(
                        &table.table,
                        duration,
                        read_rows as f64,
                    );
                    for path in &paths {
                        dirty.insert(path.clone());
                    }
                }
                Err(error) => {
                    warn!(
                        table = %table.table,
                        %error,
                        "dirty-detection query failed, falling back to full dispatch"
                    );
                    self.metrics.record_error(self.name(), "dirty_detection");
                    return Err(TaskError::new(error));
                }
            }
        }

        self.metrics.record_dirty_namespaces(dirty.len() as f64);
        debug!(dirty_namespaces = dirty.len(), "dirty-detection complete");
        Ok(dirty)
    }
}

/// A namespace is dirty if any dirty traversal_path starts with or equals the
/// namespace's traversal_path prefix. The descendant branch
/// (`d.starts_with(namespace_path)`) catches sub-group rows stored under a
/// deeper path than the enabled namespace.
fn is_namespace_dirty(namespace_path: &str, dirty: &HashSet<String>) -> bool {
    if dirty.contains(namespace_path) {
        return true;
    }
    dirty.iter().any(|d| d.starts_with(namespace_path))
}

fn is_dispatchable_traversal_path(path: &str) -> bool {
    gkg_utils::traversal_path::is_valid(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_namespace_query_excludes_empty_traversal_paths() {
        assert!(ENABLED_NAMESPACE_QUERY.contains("traversal_path != ''"));
    }

    #[test]
    fn dispatchable_traversal_paths_require_org_and_namespace_segments() {
        assert!(is_dispatchable_traversal_path("1/9/"));
        assert!(!is_dispatchable_traversal_path(""));
        assert!(!is_dispatchable_traversal_path("0/"));
        assert!(!is_dispatchable_traversal_path("1/"));
    }

    #[test]
    fn dirty_detection_tables_derived_from_ontology() {
        let tables = &*DIRTY_DETECTION_TABLES;
        assert!(
            !tables.is_empty(),
            "ontology must have at least one namespaced entity"
        );
        for table in tables {
            assert!(!table.table.is_empty());
            assert!(!table.watermark_column.is_empty());
        }
    }

    /// Regression guard: every table in the dirty-detection list must resolve
    /// its watermark column from the per-entity `EtlConfig::watermark()`.
    #[test]
    fn dirty_detection_resolves_correct_table_and_column_per_entity() {
        let ontology = ontology::Ontology::load_embedded().expect("embedded ontology must load");
        let detection_map: std::collections::HashMap<_, _> = DIRTY_DETECTION_TABLES
            .iter()
            .map(|t| (t.table.as_str(), t.watermark_column.as_str()))
            .collect();

        let check = |etl: &ontology::EtlConfig, entity_name: &str| {
            if etl.scope() != ontology::EtlScope::Namespaced {
                return;
            }
            let Some((expected_table, expected_col)) = dirty_detection_table_and_column(etl) else {
                return;
            };
            if let Some(&actual_col) = detection_map.get(expected_table.as_str()) {
                assert_eq!(
                    actual_col, expected_col,
                    "dirty-detection column mismatch for table '{}' (entity '{}')",
                    expected_table, entity_name,
                );
            }
        };

        for node in ontology.nodes() {
            if let Some(etl) = &node.etl {
                check(etl, &node.name);
            }
        }
        for derived in ontology.derived_entities() {
            check(&derived.etl, &derived.name);
        }
    }

    /// Group and Project: `from` first table differs from `source` (the
    /// watermark-bearing table lacks `traversal_path`) → excluded from
    /// dirty-detection, covered by the periodic full sweep.
    #[test]
    fn group_and_project_excluded_from_dirty_detection() {
        let ontology = ontology::Ontology::load_embedded().expect("embedded ontology must load");

        let group_etl = ontology
            .get_node("Group")
            .and_then(|n| n.etl.as_ref())
            .expect("Group must have ETL");
        assert!(dirty_detection_table_and_column(group_etl).is_none());

        let project_etl = ontology
            .get_node("Project")
            .and_then(|n| n.etl.as_ref())
            .expect("Project must have ETL");
        assert!(dirty_detection_table_and_column(project_etl).is_none());
    }

    /// MergeRequest and MergeRequestDiffFile: `from` first table == `source`
    /// (the alias just disambiguates within the JOIN) → included with
    /// unqualified watermark column.
    #[test]
    fn merge_request_entities_included_in_dirty_detection() {
        let ontology = ontology::Ontology::load_embedded().expect("embedded ontology must load");

        let mr_etl = ontology
            .get_node("MergeRequest")
            .and_then(|n| n.etl.as_ref())
            .expect("MergeRequest must have ETL");
        let (table, col) = dirty_detection_table_and_column(mr_etl)
            .expect("MergeRequest should be included in dirty-detection");
        assert_eq!(table, "merge_requests");
        assert_eq!(col, "_siphon_watermark");

        let mrd_etl = ontology
            .get_node("MergeRequestDiffFile")
            .and_then(|n| n.etl.as_ref())
            .expect("MergeRequestDiffFile must have ETL");
        let (table, col) = dirty_detection_table_and_column(mrd_etl)
            .expect("MergeRequestDiffFile should be included in dirty-detection");
        assert_eq!(table, "siphon_merge_request_diff_files");
        assert_eq!(col, "_siphon_watermark");
    }

    #[test]
    fn first_table_extraction_handles_alias_and_join() {
        assert_eq!(
            first_table_in_from("siphon_namespaces namespace INNER JOIN (...)"),
            "siphon_namespaces"
        );
        assert_eq!(
            first_table_in_from("merge_requests AS m LEFT JOIN (...)"),
            "merge_requests"
        );
        assert_eq!(first_table_in_from("siphon_notes"), "siphon_notes");
    }

    #[test]
    fn is_namespace_dirty_exact_match() {
        let dirty: HashSet<String> = ["1/100/".to_string()].into();
        assert!(is_namespace_dirty("1/100/", &dirty));
        assert!(!is_namespace_dirty("2/200/", &dirty));
    }

    #[test]
    fn is_namespace_dirty_descendant_match() {
        let dirty: HashSet<String> = ["1/100/200/".to_string()].into();
        assert!(is_namespace_dirty("1/100/", &dirty));
    }

    #[test]
    fn is_namespace_dirty_no_parent_match() {
        // A dirty path at a shallower level does NOT mark deeper namespaces
        // dirty — siphon rows carry the full leaf traversal_path, so a bare
        // org-level path is not expected in practice and would be over-broad.
        let dirty: HashSet<String> = ["1/".to_string()].into();
        assert!(!is_namespace_dirty("1/100/", &dirty));
    }
}
