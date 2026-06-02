//! [`SnowplowIndexingObserver`] accumulates the per-dispatch stats emitted
//! through [`IndexingObserver`] and, on `finish`, emits a single
//! `gkg_indexing_completed` Snowplow event carrying the run's resource cost.
//!
//! One event per dispatch: SDLC dispatches carry an `orbit_sdlc_indexing`
//! context (per entity type), code dispatches an `orbit_code_indexing`
//! context. Both ride alongside `orbit_common`. Runs that error out are
//! skipped so partial costs don't pollute the cost-attribution dataset.

use std::sync::Arc;

use gkg_analytics::AnalyticsTracker;
use gkg_server_config::AnalyticsConfig;
use labkit_events::StructuredEvent;
use uuid::Uuid;

use super::context::{CodeInputs, SdlcInputs, TriggerType, build_code, build_common, build_sdlc};
use crate::observer::{IndexingMode, IndexingObserver, PipelineType};

const GKG_CATEGORY: &str = "gkg";
const ACTION_INDEXING_COMPLETED: &str = "gkg_indexing_completed";

pub struct SnowplowIndexingObserver {
    tracker: Arc<dyn AnalyticsTracker>,
    config: Arc<AnalyticsConfig>,

    pipeline_type: Option<PipelineType>,
    dispatch_id: Option<Uuid>,
    campaign_id: Option<String>,
    traversal_path: Option<String>,
    namespace_id: Option<i64>,
    indexing_mode: Option<IndexingMode>,

    entity_type: Option<String>,
    read_rows: u64,
    read_bytes: u64,

    project_id: Option<i64>,
    branch: Option<String>,
    commit_sha: Option<String>,
    files_discovered: u64,
    files_parsed: u64,
    files_skipped: u64,
    bytes_discovered: u64,
    directories_indexed: u64,
    definitions_indexed: u64,
    imported_symbols_indexed: u64,
    edges_indexed: u64,

    written_rows: u64,
    written_bytes: u64,
    duration_ms: u64,

    errored: bool,
    emitted: bool,
}

impl SnowplowIndexingObserver {
    pub fn new(tracker: Arc<dyn AnalyticsTracker>, config: Arc<AnalyticsConfig>) -> Self {
        Self {
            tracker,
            config,
            pipeline_type: None,
            dispatch_id: None,
            campaign_id: None,
            traversal_path: None,
            namespace_id: None,
            indexing_mode: None,
            entity_type: None,
            read_rows: 0,
            read_bytes: 0,
            project_id: None,
            branch: None,
            commit_sha: None,
            files_discovered: 0,
            files_parsed: 0,
            files_skipped: 0,
            bytes_discovered: 0,
            directories_indexed: 0,
            definitions_indexed: 0,
            imported_symbols_indexed: 0,
            edges_indexed: 0,
            written_rows: 0,
            written_bytes: 0,
            duration_ms: 0,
            errored: false,
            emitted: false,
        }
    }

    /// Resolve `(namespace_id, root_namespace_id)`. A traversal path yields
    /// both (leaf + top-level); otherwise we fall back to a directly-set
    /// namespace with no root (globally-scoped SDLC runs have neither).
    fn namespace_ids(&self) -> (Option<i64>, Option<i64>) {
        match &self.traversal_path {
            Some(path) => (
                gkg_utils::traversal_path::leaf_id(path),
                gkg_utils::traversal_path::top_level_namespace_id(path),
            ),
            None => (self.namespace_id, None),
        }
    }

    fn trigger_type(&self) -> TriggerType {
        if self.campaign_id.is_some() {
            TriggerType::Scheduled
        } else {
            TriggerType::Push
        }
    }

    fn emit(&self) {
        let Some(dispatch_id) = self.dispatch_id else {
            tracing::warn!("indexing analytics event dropped: dispatch_id was never set");
            return;
        };
        let (namespace_id, root_namespace_id) = self.namespace_ids();

        let common = match build_common(&self.config, root_namespace_id) {
            Ok(context) => context,
            Err(error) => {
                tracing::warn!(%error, "failed to build orbit_common context, skipping indexing event");
                return;
            }
        };

        let event = match self.pipeline_type {
            Some(PipelineType::Sdlc) => {
                let Some(entity_type) = self.entity_type.clone() else {
                    return;
                };
                let context = build_sdlc(SdlcInputs {
                    namespace_id,
                    root_namespace_id,
                    entity_type,
                    indexing_mode: self.indexing_mode.unwrap_or(IndexingMode::Full),
                    dispatch_id: dispatch_id.to_string(),
                    campaign_id: self.campaign_id.clone(),
                    read_rows: self.read_rows,
                    read_bytes: self.read_bytes,
                    written_rows: self.written_rows,
                    written_bytes: self.written_bytes,
                    duration_ms: self.duration_ms,
                });
                build_event(context.map(EventContext::Sdlc), common)
            }
            Some(PipelineType::Code) => {
                let Some(project_id) = self.project_id else {
                    return;
                };
                let context = build_code(CodeInputs {
                    project_id,
                    namespace_id,
                    root_namespace_id,
                    branch: self.branch.clone(),
                    commit_sha: self.commit_sha.clone(),
                    trigger_type: self.trigger_type(),
                    indexing_mode: self.indexing_mode.unwrap_or(IndexingMode::Full),
                    dispatch_id: dispatch_id.to_string(),
                    campaign_id: self.campaign_id.clone(),
                    files_discovered: self.files_discovered,
                    files_parsed: self.files_parsed,
                    files_skipped: self.files_skipped,
                    bytes_discovered: self.bytes_discovered,
                    directories_indexed: self.directories_indexed,
                    definitions_indexed: self.definitions_indexed,
                    imported_symbols_indexed: self.imported_symbols_indexed,
                    edges_indexed: self.edges_indexed,
                    written_rows: self.written_rows,
                    written_bytes: self.written_bytes,
                    duration_ms: self.duration_ms,
                });
                build_event(context.map(EventContext::Code), common)
            }
            None => return,
        };

        match event {
            Ok(event) => self.tracker.track(event),
            Err(error) => {
                tracing::warn!(%error, "failed to build indexing analytics event")
            }
        }
    }
}

enum EventContext {
    Sdlc(gkg_analytics::OrbitSdlcIndexingContext),
    Code(gkg_analytics::OrbitCodeIndexingContext),
}

fn build_event(
    context: Result<EventContext, labkit_events::Error>,
    common: gkg_analytics::OrbitCommonContext,
) -> Result<StructuredEvent, labkit_events::Error> {
    let builder = StructuredEvent::builder(GKG_CATEGORY, ACTION_INDEXING_COMPLETED).context(common);
    match context? {
        EventContext::Sdlc(context) => builder.context(context).build(),
        EventContext::Code(context) => builder.context(context).build(),
    }
}

impl IndexingObserver for SnowplowIndexingObserver {
    fn set_dispatch_id(&mut self, dispatch_id: Uuid) {
        self.dispatch_id = Some(dispatch_id);
    }

    fn set_campaign_id(&mut self, campaign_id: Option<String>) {
        self.campaign_id = campaign_id;
    }

    fn set_pipeline_type(&mut self, pipeline_type: PipelineType) {
        self.pipeline_type = Some(pipeline_type);
    }

    fn set_traversal_path(&mut self, traversal_path: &str) {
        self.traversal_path = Some(traversal_path.to_owned());
    }

    fn set_namespace(&mut self, namespace_id: i64) {
        self.namespace_id = Some(namespace_id);
    }

    fn set_entity_type(&mut self, entity_type: &str) {
        self.entity_type = Some(entity_type.to_owned());
    }

    fn set_project(&mut self, project_id: i64, branch: &str) {
        self.project_id = Some(project_id);
        self.branch = Some(branch.to_owned());
    }

    fn set_commit_sha(&mut self, commit_sha: Option<String>) {
        self.commit_sha = commit_sha;
    }

    fn set_indexing_mode(&mut self, mode: IndexingMode) {
        self.indexing_mode = Some(mode);
    }

    fn record_datalake_read(&mut self, rows: u64, bytes: u64) {
        self.read_rows += rows;
        self.read_bytes += bytes;
    }

    fn record_source_bytes(&mut self, bytes: u64) {
        self.bytes_discovered += bytes;
    }

    fn files_processed(&mut self, discovered: u64, parsed: u64, skipped: u64) {
        self.files_discovered += discovered;
        self.files_parsed += parsed;
        self.files_skipped += skipped;
    }

    fn nodes_indexed(&mut self, kind: &str, count: u64) {
        match kind {
            "directory" => self.directories_indexed += count,
            "definition" => self.definitions_indexed += count,
            "imported_symbol" => self.imported_symbols_indexed += count,
            "edge" => self.edges_indexed += count,
            _ => {}
        }
    }

    fn record_graph_write(&mut self, _entity: &str, rows: u64, bytes: u64) {
        self.written_rows += rows;
        self.written_bytes += bytes;
    }

    fn record_duration(&mut self, duration_ms: u64) {
        self.duration_ms = duration_ms;
    }

    fn record_error(&mut self, _error: &str) {
        self.errored = true;
    }

    fn finish(&mut self) {
        if self.emitted || self.errored {
            return;
        }
        self.emitted = true;
        self.emit();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use gkg_analytics::{
        InMemoryAnalyticsTracker, ORBIT_CODE_INDEXING_SCHEMA, ORBIT_COMMON_SCHEMA,
        ORBIT_SDLC_INDEXING_SCHEMA,
    };
    use gkg_server_config::{
        AnalyticsConfig, DeploymentConfig, DeploymentEnvironment, DeploymentKind,
    };
    use uuid::Uuid;

    use super::*;

    fn analytics_config() -> Arc<AnalyticsConfig> {
        Arc::new(AnalyticsConfig {
            enabled: true,
            collector_url: "https://collector.example".to_string(),
            deployment: DeploymentConfig {
                kind: DeploymentKind::Com,
                environment: DeploymentEnvironment::Production,
            },
        })
    }

    fn observer(tracker: &Arc<InMemoryAnalyticsTracker>) -> SnowplowIndexingObserver {
        SnowplowIndexingObserver::new(
            tracker.clone() as Arc<dyn AnalyticsTracker>,
            analytics_config(),
        )
    }

    fn validator(schema_name: &str) -> jsonschema::Validator {
        let schema = gkg_analytics::load_schema_json(schema_name);
        jsonschema::validator_for(&schema).expect("vendored schema compiles")
    }

    fn assert_valid(schema_name: &str, data: &serde_json::Value) {
        static CODE: LazyLock<jsonschema::Validator> =
            LazyLock::new(|| validator("orbit_code_indexing"));
        static SDLC: LazyLock<jsonschema::Validator> =
            LazyLock::new(|| validator("orbit_sdlc_indexing"));
        let validator = match schema_name {
            "orbit_code_indexing" => &*CODE,
            "orbit_sdlc_indexing" => &*SDLC,
            other => panic!("no validator for {other}"),
        };
        let errors: Vec<_> = validator
            .iter_errors(data)
            .map(|e| format!("  - {e}"))
            .collect();
        assert!(
            errors.is_empty(),
            "{schema_name} invalid:\n{}",
            errors.join("\n")
        );
    }

    #[test]
    fn code_run_emits_validated_event() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut obs = observer(&tracker);

        obs.set_dispatch_id(Uuid::nil());
        obs.set_campaign_id(None);
        obs.set_pipeline_type(PipelineType::Code);
        obs.set_project(99, "main");
        obs.set_commit_sha(Some("deadbeef".to_string()));
        obs.set_traversal_path("42/100/200/");
        obs.set_indexing_mode(IndexingMode::Full);
        obs.record_source_bytes(123_456);
        obs.files_processed(500, 480, 20);
        obs.nodes_indexed("directory", 30);
        obs.nodes_indexed("file", 480);
        obs.nodes_indexed("definition", 3000);
        obs.nodes_indexed("imported_symbol", 200);
        obs.nodes_indexed("edge", 5000);
        obs.record_graph_write("gl_file", 480, 1000);
        obs.record_graph_write("gl_definition", 3000, 8000);
        obs.record_duration(45_000);
        obs.finish();

        let events = tracker.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].category(), "gkg");
        assert_eq!(events[0].action(), "gkg_indexing_completed");
        assert_eq!(events[0].contexts()[0].schema, *ORBIT_COMMON_SCHEMA);
        assert_eq!(events[0].contexts()[1].schema, *ORBIT_CODE_INDEXING_SCHEMA);

        let data = &events[0].contexts()[1].data;
        assert_eq!(data["project_id"], 99);
        assert_eq!(data["namespace_id"], 200);
        assert_eq!(data["root_namespace_id"], 100);
        assert_eq!(data["trigger_type"], "push");
        assert_eq!(data["indexing_mode"], "full");
        assert_eq!(data["files_discovered"], 500);
        assert_eq!(data["files_parsed"], 480);
        assert_eq!(data["files_skipped"], 20);
        assert_eq!(data["bytes_discovered"], 123_456);
        assert_eq!(data["directories_indexed"], 30);
        assert_eq!(data["definitions_indexed"], 3000);
        assert_eq!(data["imported_symbols_indexed"], 200);
        assert_eq!(data["edges_indexed"], 5000);
        assert_eq!(data["written_rows"], 3480);
        assert_eq!(data["written_bytes"], 9000);
        assert_eq!(data["duration_ms"], 45_000);
        assert_valid("orbit_code_indexing", data);
    }

    #[test]
    fn campaign_correlated_code_run_is_scheduled() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut obs = observer(&tracker);
        obs.set_dispatch_id(Uuid::nil());
        obs.set_campaign_id(Some("namespace-backfill".to_string()));
        obs.set_pipeline_type(PipelineType::Code);
        obs.set_project(1, "main");
        obs.set_traversal_path("42/100/");
        obs.finish();

        let events = tracker.drain();
        assert_eq!(events[0].contexts()[1].data["trigger_type"], "scheduled");
    }

    #[test]
    fn sdlc_run_emits_validated_event() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut obs = observer(&tracker);

        obs.set_dispatch_id(Uuid::nil());
        obs.set_campaign_id(Some("migration-v48".to_string()));
        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.set_entity_type("MergeRequest");
        obs.set_traversal_path("42/100/");
        obs.set_indexing_mode(IndexingMode::Incremental);
        obs.record_datalake_read(1000, 50_000);
        obs.record_graph_write("gl_merge_request", 1000, 40_000);
        obs.record_duration(12_000);
        obs.finish();

        let events = tracker.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action(), "gkg_indexing_completed");
        assert_eq!(events[0].contexts()[1].schema, *ORBIT_SDLC_INDEXING_SCHEMA);

        let data = &events[0].contexts()[1].data;
        assert_eq!(data["entity_type"], "MergeRequest");
        assert_eq!(data["indexing_mode"], "incremental");
        assert_eq!(data["namespace_id"], 100);
        assert_eq!(data["root_namespace_id"], 100);
        assert_eq!(data["campaign_id"], "migration-v48");
        assert_eq!(data["read_rows"], 1000);
        assert_eq!(data["read_bytes"], 50_000);
        assert_eq!(data["written_rows"], 1000);
        assert_eq!(data["written_bytes"], 40_000);
        assert_eq!(data["duration_ms"], 12_000);
        assert_valid("orbit_sdlc_indexing", data);
    }

    #[test]
    fn global_sdlc_run_has_null_namespaces() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut obs = observer(&tracker);
        obs.set_dispatch_id(Uuid::nil());
        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.set_entity_type("User");
        obs.set_indexing_mode(IndexingMode::Full);
        obs.finish();

        let events = tracker.drain();
        let data = &events[0].contexts()[1].data;
        assert!(data["namespace_id"].is_null());
        assert!(data["root_namespace_id"].is_null());
        assert_valid("orbit_sdlc_indexing", data);
    }

    #[test]
    fn errored_run_emits_nothing() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut obs = observer(&tracker);
        obs.set_dispatch_id(Uuid::nil());
        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.set_entity_type("MergeRequest");
        obs.record_error("datalake query timeout");
        obs.finish();
        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn finish_is_idempotent() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut obs = observer(&tracker);
        obs.set_dispatch_id(Uuid::nil());
        obs.set_pipeline_type(PipelineType::Code);
        obs.set_project(1, "main");
        obs.finish();
        obs.finish();
        assert_eq!(tracker.count(), 1);
    }
}
