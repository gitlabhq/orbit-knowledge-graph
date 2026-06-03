//! Engine wiring for the system-notes edge handler.
//!
//! [`SystemNotesHandler`] is a standalone [`Handler`] registered through the
//! engine [`HandlerRegistry`], following the
//! `crates/indexer/src/modules/namespace_deletion/` precedent. It rides the
//! existing [`NamespaceIndexingRequest`] subscription (the dispatcher already
//! publishes one namespace message per namespace; NATS fans it out to every
//! subscriber, and this handler is one more subscriber) and keeps its own
//! checkpoint key so its watermark advances independently of the per-entity
//! handlers.
//!
//! Per ADR 013 the ETL itself (parse → resolve → emit) is custom Rust rather
//! than ontology-driven YAML, but it no longer drives its own paging loop:
//! the handler builds a [`Plan`] with a [`TransformStage::Rust`] stage and
//! hands it to the shared SDLC [`Pipeline`], exactly like the ontology
//! [`EntityHandler`](super::super::entity::EntityHandler). The pipeline owns
//! extraction (streaming + retry/halving), keyset paging, the read-ahead
//! window, the streaming `gl_edge` insert, and the checkpoint cadence; the
//! custom Rust lives only in [`SystemNotesTransform`], which the pipeline
//! invokes once per page through the [`PageTransform`] seam.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{Int64Type, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gkg_observability::indexer::sdlc as sdlc_metrics;
use gkg_utils::arrow::{ArrowUtils, AsRecordBatch, BatchBuilder, ColumnSpec, ColumnType};
use ontology::{DataType as OntDataType, Ontology};
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use serde_json::json;
use tracing::info;

use super::emit::EmittedEdge;
use super::extract::{
    ACTIONS_PARAM_NAME, CURSOR_ADVANCE_KEY, CURSOR_SORT_KEY, SYSTEM_NOTES_EXTRACT_SQL,
};
use super::resolve::{
    EntityRow, MERGE_REQUESTS_SQL, PROJECT_PATHS_SQL, ROUTES_SQL, ResolutionPlan, ResolvedIndex,
    RouteRow, WORK_ITEMS_SQL,
};
use super::vendored::icon_types::{HANDLED_CROSS_REFERENCE_ACTIONS, HANDLED_LIFECYCLE_ACTIONS};
use super::{DefaultProjectLookup, ExtractedNote, plan_for_batch, process_batch};
use crate::IndexerConfig;
use crate::checkpoint::{CheckpointStore, ClickHouseCheckpointStore, namespace_position_key};
use crate::clickhouse::ClickHouseConfigurationExt;
use crate::handler::{Handler, HandlerContext, HandlerError, HandlerInitError, HandlerRegistry};
use crate::modules::sdlc::datalake::{Datalake, DatalakeQuery};
use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::modules::sdlc::observer::SdlcOtelObserver;
use crate::modules::sdlc::pipeline::{PageTransform, Pipeline, PipelineContext, TableBatch};
use crate::modules::sdlc::plan::{
    InListFilter, Plan, TransformStage, TraversalPathFilter, WatermarkFilter,
};
use crate::observer::IndexingObserver;
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use crate::topic::{NAMESPACE_HANDLER_TOPIC, NamespaceIndexingRequest};
use crate::types::{Envelope, Event, SerializationError, Subscription};

/// Physical edge table all system-note edges land in. `MENTIONS`,
/// `REOPENED`, `CLOSED`, and `MERGED` all route to the default `gl_edge`
/// table (none appears in `schema.yaml::settings.edge_tables`). Resolved to
/// the schema-version-prefixed name at write time via
/// [`prefixed_table_name`], matching every other write path.
const EDGE_TABLE: &str = "gl_edge";

/// Entity label for the per-entity SDLC metrics and the checkpoint key
/// suffix. Matches the `entity_kind` convention used by the ADR 014 entity
/// handlers (`{scope}.SystemNote`).
const ENTITY: &str = "SystemNote";

/// Default page size when no handler config override is present. Kept in
/// step with the SDLC entity handlers' datalake batch size.
const DEFAULT_BATCH_LIMIT: u64 = 10_000;

/// The note's `created_at` is the watermark column (system notes are
/// immutable post-creation), qualified to the `sn` alias because the join
/// exposes `created_at` on both sides.
const WATERMARK_COLUMN: &str = "sn.created_at";

/// All Rails `system_note_metadata.action` values the parser handles,
/// bound into the extract query's `action` IN-list so the datalake
/// pre-filters before bodies cross the wire.
fn handled_actions() -> Vec<String> {
    HANDLED_CROSS_REFERENCE_ACTIONS
        .iter()
        .chain(HANDLED_LIFECYCLE_ACTIONS.iter())
        .map(|s| s.to_string())
        .collect()
}

/// Standalone system-notes edge handler. Owns the shared [`Pipeline`] and the
/// [`SystemNotesTransform`] it drives; `handle` builds a per-namespace [`Plan`]
/// and runs it.
pub struct SystemNotesHandler {
    pipeline: Arc<Pipeline>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: SdlcMetrics,
    subscription: Subscription,
    batch_limit: u64,
    transform: Arc<SystemNotesTransform>,
}

impl SystemNotesHandler {
    pub(in crate::modules::sdlc) fn new(
        pipeline: Arc<Pipeline>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: SdlcMetrics,
        subscription: Subscription,
        batch_limit: u64,
        edge_specs: Vec<ColumnSpec>,
    ) -> Self {
        Self {
            pipeline,
            checkpoint_store,
            metrics,
            subscription,
            batch_limit,
            transform: Arc::new(SystemNotesTransform::new(edge_specs)),
        }
    }

    /// Checkpoint key for a namespace, entity-scoped so namespace deletion's
    /// `startsWith(key, "ns.{id}.")` sweep covers it.
    fn checkpoint_key(namespace: i64) -> String {
        format!("{}.{ENTITY}", namespace_position_key(namespace))
    }
}

/// Register the system-notes handler on the SDLC namespace subscription.
/// Mirrors `namespace_deletion::register_handlers`; called from
/// `modules::sdlc::register_handlers`.
pub fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &Ontology,
) -> Result<(), HandlerInitError> {
    registry.register_handler(Box::new(build_handler(config, ontology)));
    info!("registered SDLC system-notes edge handler");
    Ok(())
}

/// Build the handler from config. Exposed (via the `system_notes` and `sdlc`
/// module re-exports) so integration tests can construct it.
pub fn build_handler(config: &IndexerConfig, ontology: &Ontology) -> SystemNotesHandler {
    let datalake_client = Arc::new(config.datalake.build_client());
    let graph_client = Arc::new(config.graph.build_client());

    let configured = config.engine.handlers.entity_handler.datalake_batch_size;
    let batch_limit = if configured == 0 {
        DEFAULT_BATCH_LIMIT
    } else {
        configured
    };

    let datalake: Arc<dyn DatalakeQuery> = Arc::new(Datalake::new(datalake_client, batch_limit));
    let checkpoint_store: Arc<dyn CheckpointStore> =
        Arc::new(ClickHouseCheckpointStore::new(graph_client));
    let metrics = SdlcMetrics::new();
    let pipeline = Arc::new(Pipeline::new(
        Arc::clone(&datalake),
        Arc::clone(&checkpoint_store),
        metrics.clone(),
        config.engine.datalake_retry.clone(),
    ));

    let mut subscription = NamespaceIndexingRequest::subscription();
    if let Some(topic_config) = config.engine.topics.get(NAMESPACE_HANDLER_TOPIC) {
        subscription = subscription.with_config(topic_config);
    }

    SystemNotesHandler::new(
        pipeline,
        checkpoint_store,
        metrics,
        subscription,
        batch_limit,
        edge_specs(ontology),
    )
}

#[async_trait]
impl Handler for SystemNotesHandler {
    fn name(&self) -> &str {
        "sdlc.system_notes"
    }

    fn subscription(&self) -> Subscription {
        self.subscription.clone()
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let request: NamespaceIndexingRequest =
            message.to_event().map_err(|error| match error {
                SerializationError::Json(err) => HandlerError::Deserialization(err),
            })?;

        let key = Self::checkpoint_key(request.namespace);

        // Only a completed checkpoint may advance the watermark; an in-progress
        // one stores a never-reached target that would skip unprocessed rows.
        // The shared pipeline loads the cursor itself; the watermark window is
        // a coarse range around it.
        let last_watermark = self
            .checkpoint_store
            .load(&key)
            .await
            .map_err(|e| HandlerError::Processing(e.to_string()))?
            .filter(|cp| cp.cursor_values.is_none())
            .map(|cp| cp.watermark)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);

        let plan = self.build_plan(request.watermark, &request.traversal_path);
        let base_query = plan
            .prepare()
            .with(WatermarkFilter {
                column: WATERMARK_COLUMN,
                last: last_watermark,
                current: request.watermark,
            })
            .with(TraversalPathFilter {
                path: &request.traversal_path,
                column: "sn.traversal_path",
            })
            .with(InListFilter {
                column: "snm.action",
                param: ACTIONS_PARAM_NAME,
                values: handled_actions(),
            });

        let observer: Arc<Mutex<dyn IndexingObserver>> =
            Arc::new(Mutex::new(SdlcOtelObserver::new(self.metrics.clone())));
        let pipeline_context = PipelineContext {
            destination: Arc::clone(&context.destination),
            progress: context.progress.clone(),
            observer,
        };

        self.transform.reset_drop_counts();
        let stats = self
            .pipeline
            .run_plan(
                &pipeline_context,
                &plan,
                base_query,
                &key,
                request.watermark,
            )
            .await?;

        info!(
            namespace = request.namespace,
            notes = stats.read_rows,
            edges = stats.written_rows,
            duration_ms = stats.duration_ms,
            "system_notes: materialized edges"
        );
        Ok(())
    }
}

impl SystemNotesHandler {
    /// Build the per-namespace [`Plan`]. The extract SQL, keyset sort key, and
    /// `gl_edge` Rust transform are fixed; only the watermark window and
    /// traversal path vary per message (applied as filters on `prepare()`).
    fn build_plan(&self, _watermark: DateTime<Utc>, _traversal_path: &str) -> Plan {
        Plan {
            name: ENTITY.to_string(),
            extract_template: SYSTEM_NOTES_EXTRACT_SQL.to_string(),
            watermark_column: WATERMARK_COLUMN.to_string(),
            sort_key: CURSOR_SORT_KEY.iter().map(|s| s.to_string()).collect(),
            advance_key: CURSOR_ADVANCE_KEY.iter().map(|s| s.to_string()).collect(),
            batch_size: self.batch_limit,
            stage: TransformStage::Rust {
                transform: Arc::clone(&self.transform) as Arc<dyn PageTransform>,
                output_tables: vec![prefixed_table_name(EDGE_TABLE, *SCHEMA_VERSION)],
            },
        }
    }
}

/// The page-wise custom-ETL transform: decode a page of extracted system
/// notes, resolve their GFM references against ClickHouse (routes + MR + WI
/// IN-list lookups, batched across the whole page), and emit `gl_edge` rows.
/// Invoked once per page by the shared [`Pipeline`] through [`PageTransform`].
pub struct SystemNotesTransform {
    unknown_action: Counter<u64>,
    unsupported_noteable: Counter<u64>,
    /// `gl_edge` column specs, derived once from the ontology at construction
    /// (see [`edge_specs`]) so the write schema can't drift from
    /// `config/graph.sql`.
    edge_specs: Vec<ColumnSpec>,
}

impl SystemNotesTransform {
    fn new(edge_specs: Vec<ColumnSpec>) -> Self {
        let meter = gkg_observability::meter();
        Self {
            unknown_action: sdlc_metrics::SYSTEM_NOTES_UNKNOWN_ACTION.build_counter_u64(&meter),
            unsupported_noteable: sdlc_metrics::SYSTEM_NOTES_UNSUPPORTED_NOTEABLE
                .build_counter_u64(&meter),
            edge_specs,
        }
    }

    /// Counters are monotonic and shared across pages/messages; nothing to
    /// reset, but the hook documents the per-run boundary and lets a future
    /// per-run gauge slot in without touching `handle`.
    fn reset_drop_counts(&self) {}

    /// Resolve a page's references against the datalake and emit edges.
    async fn resolve_and_emit(
        &self,
        datalake: &dyn DatalakeQuery,
        notes: &[ExtractedNote],
    ) -> Result<Vec<EmittedEdge>, HandlerError> {
        // Observability for the two drop paths in `process_batch`.
        //
        // `unknown_action` is defense-in-depth: the extract query already
        // pre-filters `action IN (handled set)`, so it normally never fires.
        // It only catches the IN-list and `Action::parse` drifting apart in
        // code (a bug), not upstream Rails drift — that lives in
        // `scripts/check-system-note-actions.sh`.
        //
        // `unsupported_noteable` *can* fire at runtime: the extract query does
        // not constrain `noteable_type`, so a note on a kind we don't map
        // (e.g. a new Rails STI type) reaches here and is dropped. A sustained
        // non-zero count signals a missing mapping.
        for n in notes {
            if super::parse::Action::parse(&n.action).is_none() {
                self.unknown_action.add(
                    1,
                    &[KeyValue::new(
                        sdlc_metrics::labels::ACTION,
                        n.action.clone(),
                    )],
                );
            } else if super::emit::NoteableKind::from_siphon(&n.noteable_type).is_none() {
                self.unsupported_noteable.add(
                    1,
                    &[KeyValue::new(
                        sdlc_metrics::labels::NOTEABLE_TYPE,
                        n.noteable_type.clone(),
                    )],
                );
            }
        }

        let default_projects = resolve_default_projects(datalake, notes).await?;
        let plan = plan_for_batch(notes, &default_projects);
        let index = resolve_plan(datalake, &plan).await?;

        let edges = process_batch(notes, &default_projects, |r, default_project| {
            index.resolve(r, default_project)
        });
        Ok(edges)
    }
}

#[async_trait]
impl PageTransform for SystemNotesTransform {
    async fn transform_page(
        &self,
        datalake: &dyn DatalakeQuery,
        page: &[RecordBatch],
    ) -> Result<Vec<TableBatch>, HandlerError> {
        let mut notes = Vec::new();
        for batch in page {
            decode_extracted_notes(batch, &mut notes);
        }
        if notes.is_empty() {
            return Ok(Vec::new());
        }

        let edges = self.resolve_and_emit(datalake, &notes).await?;
        if edges.is_empty() {
            return Ok(Vec::new());
        }

        let batch = edge_record_batch(&edges, &self.edge_specs)
            .map_err(|e| HandlerError::Processing(format!("edge batch: {e}")))?;
        // All system-note edges target the single `gl_edge` output table
        // (index 0 in the plan's `output_tables`).
        Ok(vec![TableBatch {
            transform_index: 0,
            batch,
        }])
    }
}

/// Resolve each note's owning `project_id` to a project path, keyed back to
/// `(noteable_type, noteable_id)` so [`plan_for_batch`] and [`process_batch`]
/// can substitute it for unqualified references.
async fn resolve_default_projects(
    datalake: &dyn DatalakeQuery,
    notes: &[ExtractedNote],
) -> Result<DefaultProjectLookup, HandlerError> {
    let project_ids: Vec<i64> = {
        let mut ids: Vec<i64> = notes.iter().filter_map(|n| n.project_id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };
    if project_ids.is_empty() {
        return Ok(DefaultProjectLookup::new());
    }

    // Reverse routes lookup: source_id (project_id) -> path. ROUTES_SQL filters
    // on `path IN`, so use a dedicated id-keyed query here.
    let params = json!({ "source_ids": project_ids });
    let batches = datalake
        .query_batches(PROJECT_PATHS_SQL, params, None)
        .await
        .map_err(|e| HandlerError::Processing(format!("system_notes routes: {e}")))?;

    let mut id_to_path: HashMap<i64, String> = HashMap::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            if let (Some(source_id), Some(path)) = (
                ArrowUtils::get_column::<Int64Type>(batch, "source_id", row),
                ArrowUtils::get_column_string(batch, "path", row),
            ) {
                id_to_path.insert(source_id, path);
            }
        }
    }

    let mut lookup = DefaultProjectLookup::new();
    for n in notes {
        if let Some(path) = n.project_id.and_then(|pid| id_to_path.get(&pid)) {
            lookup.insert((n.noteable_type.clone(), n.noteable_id), path.clone());
        }
    }
    Ok(lookup)
}

/// Fan the [`ResolutionPlan`] out to the routes + noteable lookups and build
/// the [`ResolvedIndex`] the emitter consults.
async fn resolve_plan(
    datalake: &dyn DatalakeQuery,
    plan: &ResolutionPlan,
) -> Result<ResolvedIndex, HandlerError> {
    if plan.paths.is_empty() {
        return Ok(ResolvedIndex::default());
    }

    let paths: Vec<&str> = plan.paths.iter().map(String::as_str).collect();
    // The routes lookup must complete first — it maps each project path to a
    // `project_id` that the MR/work-item `(project_id, iid)` lookups key on.
    let routes = query_routes(datalake, &paths).await?;

    let mr_pairs: Vec<(i64, i64)> = pairs_with_project_id(&plan.mr_pairs, &routes);
    let wi_pairs: Vec<(i64, i64)> = pairs_with_project_id(&plan.issue_pairs, &routes);

    // The MR and work-item lookups are independent, so run them concurrently
    // rather than serially.
    let (mr_entities, wi_entities) = tokio::try_join!(
        query_entities(datalake, MERGE_REQUESTS_SQL, &mr_pairs),
        query_entities(datalake, WORK_ITEMS_SQL, &wi_pairs),
    )?;

    Ok(ResolvedIndex::build(&routes, &mr_entities, &wi_entities))
}

async fn query_routes(
    datalake: &dyn DatalakeQuery,
    paths: &[&str],
) -> Result<Vec<RouteRow>, HandlerError> {
    let params = json!({ "paths": paths });
    let batches = datalake
        .query_batches(ROUTES_SQL, params, None)
        .await
        .map_err(|e| HandlerError::Processing(format!("system_notes routes: {e}")))?;

    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            if let (Some(source_id), Some(path), Some(tp)) = (
                ArrowUtils::get_column::<Int64Type>(batch, "source_id", row),
                ArrowUtils::get_column_string(batch, "path", row),
                ArrowUtils::get_column_string(batch, "traversal_path", row),
            ) {
                rows.push(RouteRow {
                    source_id,
                    path,
                    traversal_path: tp,
                });
            }
        }
    }
    Ok(rows)
}

async fn query_entities(
    datalake: &dyn DatalakeQuery,
    sql: &str,
    pairs: &[(i64, i64)],
) -> Result<Vec<EntityRow>, HandlerError> {
    if pairs.is_empty() {
        return Ok(Vec::new());
    }
    // Pass the (project_id, iid) pairs as two parallel Int64 arrays; the SQL
    // zips them back into the tuple IN-list server-side. A single
    // Array(Tuple(...)) param can't survive the JSON parameter channel (a tuple
    // serializes as a nested array, which ClickHouse rejects).
    let project_ids: Vec<i64> = pairs.iter().map(|(p, _)| *p).collect();
    let iids: Vec<i64> = pairs.iter().map(|(_, i)| *i).collect();
    let params = json!({
        "project_ids": project_ids,
        "iids": iids,
    });
    let batches = datalake
        .query_batches(sql, params, None)
        .await
        .map_err(|e| HandlerError::Processing(format!("system_notes entities: {e}")))?;

    let mut rows = Vec::new();
    for batch in &batches {
        let project_id_col = ArrowUtils::get_column_by_index::<Int64Array>(batch, 1).filter(|_| {
            // Second column is target_project_id (MR) or project_id (WI); both
            // decode positionally as `(id, project_id, iid)`.
            batch.num_columns() >= 2
        });
        for row in 0..batch.num_rows() {
            let project_id = project_id_col
                .filter(|arr| !arr.is_null(row))
                .map(|arr| arr.value(row));
            if let (Some(id), Some(project_id), Some(iid)) = (
                ArrowUtils::get_column::<Int64Type>(batch, "id", row),
                project_id,
                ArrowUtils::get_column::<Int64Type>(batch, "iid", row),
            ) {
                rows.push(EntityRow {
                    id,
                    project_id,
                    iid,
                });
            }
        }
    }
    Ok(rows)
}

/// Map parser `(project_path, iid)` pairs onto `(project_id, iid)` pairs for
/// the noteable lookups, using the resolved project routes. Pairs whose
/// project path didn't resolve to a project route are dropped.
fn pairs_with_project_id(
    path_pairs: &HashSet<(String, i64)>,
    routes: &[RouteRow],
) -> Vec<(i64, i64)> {
    // `ROUTES_SQL` already restricts to `source_type = 'Project'`.
    let path_to_id: HashMap<&str, i64> = routes
        .iter()
        .map(|r| (r.path.as_str(), r.source_id))
        .collect();
    let mut out: Vec<(i64, i64)> = path_pairs
        .iter()
        .filter_map(|(path, iid)| path_to_id.get(path.as_str()).map(|&pid| (pid, *iid)))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn decode_extracted_notes(batch: &RecordBatch, out: &mut Vec<ExtractedNote>) {
    for row in 0..batch.num_rows() {
        // `sn.id` stays in the SELECT (the cursor advances off it via the
        // result batch) but isn't carried on `ExtractedNote` — parse, resolve
        // and emit key off `noteable_id` / `created_at`, not the note id.
        let (Some(noteable_id), Some(noteable_type), Some(created_at), Some(tp), Some(action)) = (
            ArrowUtils::get_column::<Int64Type>(batch, "noteable_id", row),
            ArrowUtils::get_column_string(batch, "noteable_type", row),
            col_timestamp_micros(batch, "created_at", row),
            ArrowUtils::get_column_string(batch, "traversal_path", row),
            ArrowUtils::get_column_string(batch, "action", row),
        ) else {
            continue;
        };
        out.push(ExtractedNote {
            note: ArrowUtils::get_column_string(batch, "note", row).unwrap_or_default(),
            noteable_id,
            noteable_type,
            author_id: ArrowUtils::get_column::<Int64Type>(batch, "author_id", row),
            project_id: ArrowUtils::get_column::<Int64Type>(batch, "project_id", row),
            created_at,
            traversal_path: tp,
            action,
        });
    }
}

/// `ArrowUtils::get_column::<TimestampMicrosecondType>` returns the raw `i64`
/// micros; system-note rows carry `created_at` as `DateTime64(6)`, so convert
/// to a `DateTime<Utc>` here.
fn col_timestamp_micros(batch: &RecordBatch, name: &str, row: usize) -> Option<DateTime<Utc>> {
    let micros = ArrowUtils::get_column::<TimestampMicrosecondType>(batch, name, row)?;
    DateTime::<Utc>::from_timestamp_micros(micros)
}

/// `gl_edge` column specs derived from the ontology so the write schema tracks
/// `config/graph.sql` automatically. Mirrors the ontology-driven `edge_specs`
/// in `modules/code/arrow_converter.rs`, but scoped to the single default edge
/// table (`gl_edge`) that all system-note edges land in — the code path builds
/// the union across every edge table because it also writes `gl_code_edge`.
///
/// `EmittedEdge::write_row` addresses columns by name, so the only contract is
/// that every column the ontology declares for `gl_edge` is written.
/// `source_tags` / `target_tags` come from the denormalized columns (always
/// empty for system-note edges — endpoints carry their own tags from their
/// entity ETL); `_version` / `_deleted` are the infra columns.
fn edge_specs(ontology: &Ontology) -> Vec<ColumnSpec> {
    let table = ontology.edge_table();
    let Some(config) = ontology.edge_table_config(table) else {
        // The default edge table is always present in a loaded ontology; an
        // absent config means a malformed schema and is a hard bug.
        panic!("ontology is missing a config for the default edge table {table:?}");
    };

    let dict_fields: HashSet<String> = config
        .storage
        .columns
        .iter()
        .filter(|col| col.ch_type.starts_with("LowCardinality"))
        .map(|col| col.name.clone())
        .collect();

    let mut specs: Vec<ColumnSpec> = config
        .columns
        .iter()
        .map(|c| ColumnSpec {
            name: c.name.clone(),
            col_type: match c.data_type {
                OntDataType::Int => ColumnType::Int,
                OntDataType::Bool => ColumnType::Bool,
                OntDataType::DateTime => ColumnType::TimestampMicros,
                _ if dict_fields.contains(&c.name) => ColumnType::DictStr,
                _ => ColumnType::Str,
            },
            nullable: false,
        })
        .collect();

    for col in &config.storage.denormalized_columns {
        specs.push(ColumnSpec {
            name: col.name.clone(),
            col_type: ColumnType::StrList,
            nullable: false,
        });
    }

    specs.push(ColumnSpec {
        name: "_version".into(),
        col_type: ColumnType::TimestampMicros,
        nullable: false,
    });
    specs.push(ColumnSpec {
        name: "_deleted".into(),
        col_type: ColumnType::Bool,
        nullable: false,
    });
    specs
}

impl AsRecordBatch for EmittedEdge {
    fn write_row(&self, b: &mut BatchBuilder, _ctx: &()) -> Result<(), arrow::error::ArrowError> {
        b.col("traversal_path")?.push_str(&self.traversal_path)?;
        b.col("source_id")?.push_int(self.source_id)?;
        b.col("source_kind")?.push_str(self.source_kind)?;
        b.col("relationship_kind")?
            .push_str(self.relationship_kind)?;
        b.col("target_id")?.push_int(self.target_id)?;
        b.col("target_kind")?.push_str(self.target_kind)?;
        b.col("source_tags")?.push_str_list(&[])?;
        b.col("target_tags")?.push_str_list(&[])?;
        b.col("_version")?
            .push_timestamp_micros(self.version_micros)?;
        b.col("_deleted")?.push_bool(false)?;
        Ok(())
    }
}

fn edge_record_batch(
    edges: &[EmittedEdge],
    specs: &[ColumnSpec],
) -> Result<RecordBatch, arrow::error::ArrowError> {
    EmittedEdge::to_record_batch(edges, specs, &())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, ListArray};
    use chrono::TimeZone;

    /// `gl_edge` specs derived from the embedded ontology — the same path the
    /// production handler takes, so these tests would catch a drift between
    /// the ontology and `EmittedEdge::write_row`.
    fn test_specs() -> Vec<ColumnSpec> {
        let ontology = ontology::Ontology::load_embedded().expect("embedded ontology loads");
        edge_specs(&ontology)
    }

    #[test]
    fn checkpoint_key_uses_entity_scoped_namespace_prefix() {
        // Matches the ADR 014 checkpoint key convention so namespace deletion's
        // `startsWith(key, "ns.{id}.")` sweep covers it.
        assert_eq!(SystemNotesHandler::checkpoint_key(42), "ns.42.SystemNote");
    }

    #[test]
    fn handled_actions_cover_cross_reference_and_lifecycle() {
        let actions = handled_actions();
        assert!(actions.iter().any(|a| a == "cross_reference"));
        assert!(actions.iter().any(|a| a == "closed"));
        assert!(actions.iter().any(|a| a == "reopened"));
        assert!(actions.iter().any(|a| a == "merged"));
        // No unhandled action leaks into the IN-list.
        assert!(!actions.iter().any(|a| a == "description"));
    }

    fn edge(kind: &'static str, src: i64, tgt: i64) -> EmittedEdge {
        EmittedEdge {
            traversal_path: "1/100/".to_string(),
            relationship_kind: kind,
            source_id: src,
            source_kind: "MergeRequest",
            target_id: tgt,
            target_kind: "WorkItem",
            version_micros: Utc
                .with_ymd_and_hms(2026, 5, 1, 0, 0, 0)
                .unwrap()
                .timestamp_micros(),
        }
    }

    #[test]
    fn edge_record_batch_matches_gl_edge_columns() {
        let specs = test_specs();
        let batch = edge_record_batch(&[edge("MENTIONS", 10, 20)], &specs).unwrap();
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // The ontology-derived spec must cover every column
        // `EmittedEdge::write_row` writes, plus the infra columns. Order
        // follows the ontology column list (logical columns, then denormalized
        // tags, then `_version` / `_deleted`).
        assert_eq!(
            names,
            vec![
                "traversal_path",
                "relationship_kind",
                "source_id",
                "source_kind",
                "target_id",
                "target_kind",
                "source_tags",
                "target_tags",
                "_version",
                "_deleted",
            ]
        );
        assert_eq!(batch.num_rows(), 1);
    }

    fn dict_value(batch: &RecordBatch, name: &str, row: usize) -> String {
        use arrow::array::{DictionaryArray, StringArray};
        use arrow::datatypes::Int32Type;
        let idx = batch.schema().index_of(name).unwrap();
        let dict = batch
            .column(idx)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("LowCardinality column decodes as Int32 dictionary");
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        values.value(dict.keys().value(row) as usize).to_string()
    }

    #[test]
    fn edge_record_batch_writes_values_and_empty_tags() {
        let specs = test_specs();
        let batch = edge_record_batch(&[edge("MENTIONS", 10, 20)], &specs).unwrap();
        // relationship_kind / source_kind / target_kind are LowCardinality
        // (dictionary-encoded), matching the gl_edge column types.
        assert_eq!(dict_value(&batch, "relationship_kind", 0), "MENTIONS");
        assert_eq!(
            ArrowUtils::get_column::<Int64Type>(&batch, "source_id", 0),
            Some(10)
        );
        assert_eq!(
            ArrowUtils::get_column::<Int64Type>(&batch, "target_id", 0),
            Some(20)
        );
        assert_eq!(dict_value(&batch, "source_kind", 0), "MergeRequest");
        assert_eq!(dict_value(&batch, "target_kind", 0), "WorkItem");

        let deleted = batch
            .column(batch.schema().index_of("_deleted").unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!deleted.value(0), "_deleted is always false (append-only)");

        let tags = batch
            .column(batch.schema().index_of("source_tags").unwrap())
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(tags.value(0).len(), 0, "system-note edges carry no tags");
    }

    #[test]
    fn edge_record_batch_handles_multiple_rows() {
        let specs = test_specs();
        let batch =
            edge_record_batch(&[edge("MENTIONS", 1, 2), edge("REOPENED", 3, 4)], &specs).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
