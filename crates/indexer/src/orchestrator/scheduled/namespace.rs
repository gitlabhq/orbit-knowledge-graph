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

/// Source tables and their per-entity watermark columns, derived from the
/// ontology. Only namespaced entities with an ETL config contribute — these are
/// the tables whose changes imply a namespace needs re-indexing.
static DIRTY_DETECTION_TABLES: LazyLock<Vec<DirtyDetectionTable>> = LazyLock::new(|| {
    let ontology = ontology::Ontology::load_embedded().expect("embedded ontology must be valid");
    let mut seen = HashSet::new();
    let mut tables = Vec::new();

    for node in ontology.nodes() {
        let Some(etl) = &node.etl else { continue };
        if etl.scope() != ontology::EtlScope::Namespaced {
            continue;
        }
        let source = etl.source().to_owned();
        if !seen.insert(source.clone()) {
            continue;
        }
        tables.push(DirtyDetectionTable {
            source,
            watermark_column: unqualified_column(etl.watermark()),
        });
    }

    for derived in ontology.derived_entities() {
        let etl = &derived.etl;
        if etl.scope() != ontology::EtlScope::Namespaced {
            continue;
        }
        let source = etl.source().to_owned();
        if !seen.insert(source.clone()) {
            continue;
        }
        tables.push(DirtyDetectionTable {
            source,
            watermark_column: unqualified_column(etl.watermark()),
        });
    }

    tables
});

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
    source: String,
    watermark_column: String,
}

pub struct NamespaceDispatcher {
    nats: Arc<dyn NatsServices>,
    datalake: ArrowClickHouseClient,
    metrics: ScheduledTaskMetrics,
    config: NamespaceDispatcherConfig,
    campaign: Arc<CampaignState>,
    /// Tracks the last sweep time to determine when a full sweep is due.
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
        let dirty_paths = if is_sweep {
            debug!("full sweep cycle — dispatching all enabled namespaces");
            None
        } else {
            Some(self.detect_dirty_namespaces().await?)
        };

        let watermark = Utc::now();
        let campaign_id = self.campaign.current();
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;
        let mut sweep_only: u64 = 0;

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

        // For sweep cycles, count namespaces that were dispatched despite not
        // appearing in the dirty set — this is the silent-drop canary metric.
        if is_sweep && dispatched > 0 {
            if let Ok(dirty) = self.detect_dirty_namespaces().await {
                for tp in traversal_paths.iter() {
                    if is_dispatchable_traversal_path(tp) && !is_namespace_dirty(tp, &dirty) {
                        sweep_only += 1;
                    }
                }
            }
            if sweep_only > 0 {
                self.metrics.record_sweep_only_dispatched(sweep_only);
            }
        }

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
                table.source, table.watermark_column, cutoff_str
            );

            let query_start = Instant::now();
            let result = self.datalake.query(&query).fetch_arrow().await;

            let duration = query_start.elapsed().as_secs_f64();

            match result {
                Ok(batches) => {
                    let paths = String::extract_column(&batches, 0).unwrap_or_default();
                    self.metrics.record_dirty_detection_query(
                        &table.source,
                        duration,
                        paths.len() as f64,
                    );
                    for path in &paths {
                        dirty.insert(path.clone());
                    }
                }
                Err(error) => {
                    warn!(
                        table = %table.source,
                        %error,
                        "dirty-detection query failed, falling back to full dispatch for this table"
                    );
                    self.metrics.record_error(self.name(), "dirty_detection");
                }
            }
        }

        self.metrics.record_dirty_namespaces(dirty.len() as f64);
        debug!(dirty_namespaces = dirty.len(), "dirty-detection complete");
        Ok(dirty)
    }
}

/// A namespace is dirty if any dirty traversal_path starts with or equals
/// the namespace's traversal_path prefix, or if a dirty path is a parent
/// prefix of the namespace. In practice, datalake rows carry the same
/// `traversal_path` as the enabled namespace, so an exact set membership
/// check is the fast path; the prefix check handles sub-group inheritance.
fn is_namespace_dirty(namespace_path: &str, dirty: &HashSet<String>) -> bool {
    if dirty.contains(namespace_path) {
        return true;
    }
    dirty
        .iter()
        .any(|d| d.starts_with(namespace_path) || namespace_path.starts_with(d.as_str()))
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
            assert!(!table.source.is_empty());
            assert!(!table.watermark_column.is_empty());
        }
    }

    /// Regression guard: the dirty-detection watermark column must match
    /// the per-entity `EtlConfig::watermark()` (unqualified), not the global
    /// default helper. This catches re-introduction of the global helper.
    #[test]
    fn dirty_detection_column_matches_etl_config_watermark_per_entity() {
        let ontology = ontology::Ontology::load_embedded().expect("embedded ontology must load");
        let detection_tables: std::collections::HashMap<_, _> = DIRTY_DETECTION_TABLES
            .iter()
            .map(|t| (t.source.as_str(), t.watermark_column.as_str()))
            .collect();

        for node in ontology.nodes() {
            let Some(etl) = &node.etl else { continue };
            if etl.scope() != ontology::EtlScope::Namespaced {
                continue;
            }
            let source = etl.source();
            if let Some(&detection_col) = detection_tables.get(source) {
                assert_eq!(
                    detection_col,
                    unqualified_column(etl.watermark()),
                    "dirty-detection column for '{}' (entity '{}') must match EtlConfig::watermark()",
                    source,
                    node.name,
                );
            }
        }

        for derived in ontology.derived_entities() {
            let etl = &derived.etl;
            if etl.scope() != ontology::EtlScope::Namespaced {
                continue;
            }
            let source = etl.source();
            if let Some(&detection_col) = detection_tables.get(source) {
                assert_eq!(
                    detection_col,
                    unqualified_column(etl.watermark()),
                    "dirty-detection column for '{}' (derived '{}') must match EtlConfig::watermark()",
                    source,
                    derived.name,
                );
            }
        }
    }

    #[test]
    fn is_namespace_dirty_exact_match() {
        let dirty: HashSet<String> = ["1/100/".to_string()].into();
        assert!(is_namespace_dirty("1/100/", &dirty));
        assert!(!is_namespace_dirty("2/200/", &dirty));
    }

    #[test]
    fn is_namespace_dirty_prefix_match() {
        let dirty: HashSet<String> = ["1/100/200/".to_string()].into();
        assert!(is_namespace_dirty("1/100/", &dirty));
    }

    #[test]
    fn is_namespace_dirty_parent_prefix() {
        let dirty: HashSet<String> = ["1/".to_string()].into();
        assert!(is_namespace_dirty("1/100/", &dirty));
    }
}
