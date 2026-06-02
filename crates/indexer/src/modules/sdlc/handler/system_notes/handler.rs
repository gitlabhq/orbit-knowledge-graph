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
//! than ontology-driven YAML; this file is only the thin I/O shell around
//! that pure core: extract a batch from the datalake, resolve references with
//! two batched IN-list queries, build `gl_edge` rows, write them, advance the
//! checkpoint.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

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
use tracing::{info, warn};

use super::emit::EmittedEdge;
use super::extract::{
    CURSOR_ADVANCE_KEY, CURSOR_PLACEHOLDER, CURSOR_SORT_KEY, SYSTEM_NOTES_EXTRACT_SQL,
};
use super::resolve::{
    EntityRow, MERGE_REQUESTS_SQL, PROJECT_PATHS_SQL, ROUTES_SQL, ResolutionPlan, ResolvedIndex,
    RouteRow, WORK_ITEMS_SQL,
};
use super::vendored::icon_types::{HANDLED_CROSS_REFERENCE_ACTIONS, HANDLED_LIFECYCLE_ACTIONS};
use super::{DefaultProjectLookup, ExtractedNote, plan_for_batch, process_batch};
use crate::IndexerConfig;
use crate::checkpoint::{Checkpoint, CheckpointStore, ClickHouseCheckpointStore};
use crate::clickhouse::{ClickHouseConfigurationExt, TIMESTAMP_FORMAT};
use crate::handler::{Handler, HandlerContext, HandlerError, HandlerInitError, HandlerRegistry};
use crate::modules::sdlc::datalake::{Datalake, DatalakeQuery};
use crate::modules::sdlc::plan::{Cursor, CursorFilter, Filter};
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
/// handlers (`ns.{id}.SystemNote`).
const ENTITY: &str = "SystemNote";

/// Default page size when no handler config override is present. Kept in
/// step with the SDLC entity handlers' datalake batch size.
const DEFAULT_BATCH_LIMIT: u64 = 10_000;

/// Hard cap on pages per namespace message so a pathological backlog can't
/// monopolise a worker. On hitting the cap the keyset cursor is persisted to
/// the checkpoint so the next message resumes from it rather than re-scanning.
const MAX_PAGES_PER_RUN: usize = 1_000;

/// All Rails `system_note_metadata.action` values the parser handles,
/// bound into the extract query's `{actions:Array(String)}` IN-list so the
/// datalake pre-filters before bodies cross the wire.
fn handled_actions() -> Vec<&'static str> {
    HANDLED_CROSS_REFERENCE_ACTIONS
        .iter()
        .chain(HANDLED_LIFECYCLE_ACTIONS.iter())
        .copied()
        .collect()
}

/// Standalone system-notes edge handler.
pub struct SystemNotesHandler {
    datalake: Arc<dyn DatalakeQuery>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    subscription: Subscription,
    batch_limit: u64,
    max_pages: usize,
    unknown_action: Counter<u64>,
    unsupported_noteable: Counter<u64>,
    /// `gl_edge` column specs, derived once from the ontology at
    /// construction (see [`edge_specs`]) so the write schema can't drift
    /// from `config/graph.sql`.
    edge_specs: Vec<ColumnSpec>,
}

impl SystemNotesHandler {
    pub(crate) fn new(
        datalake: Arc<dyn DatalakeQuery>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        subscription: Subscription,
        batch_limit: u64,
        edge_specs: Vec<ColumnSpec>,
    ) -> Self {
        let meter = gkg_observability::meter();
        Self {
            datalake,
            checkpoint_store,
            subscription,
            batch_limit,
            max_pages: MAX_PAGES_PER_RUN,
            unknown_action: sdlc_metrics::SYSTEM_NOTES_UNKNOWN_ACTION.build_counter_u64(&meter),
            unsupported_noteable: sdlc_metrics::SYSTEM_NOTES_UNSUPPORTED_NOTEABLE
                .build_counter_u64(&meter),
            edge_specs,
        }
    }

    /// Override the per-message page cap. Used by integration tests to
    /// exercise the cap-exit / resume path without seeding millions of rows.
    pub fn with_max_pages(mut self, max_pages: usize) -> Self {
        self.max_pages = max_pages.max(1);
        self
    }

    fn checkpoint_key(namespace: i64) -> String {
        format!("ns.{namespace}.{ENTITY}")
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

/// Build the handler from config. Exposed (via the `system_notes` and
/// `sdlc` module re-exports) so integration tests can construct it and tune
/// `max_pages` through [`SystemNotesHandler::with_max_pages`] without seeding
/// millions of rows to reach the production page cap.
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

    let mut subscription = NamespaceIndexingRequest::subscription();
    if let Some(topic_config) = config.engine.topics.get(NAMESPACE_HANDLER_TOPIC) {
        subscription = subscription.with_config(topic_config);
    }

    SystemNotesHandler::new(
        datalake,
        checkpoint_store,
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

        let started_at = Instant::now();
        let key = Self::checkpoint_key(request.namespace);

        let checkpoint = self
            .checkpoint_store
            .load(&key)
            .await
            .map_err(|e| HandlerError::Processing(e.to_string()))?;

        // The keyset cursor is the source of truth for position; the
        // watermark window is a coarse range around it. Reuses the shared
        // SDLC `Cursor` (the same keyset machinery as the entity pipeline in
        // `handler/entity.rs`): a completed checkpoint (`cursor_values: None`)
        // yields an empty (first-page) cursor and advances the window lower
        // bound to its watermark; an in-progress one (`cursor_values: Some`)
        // means a prior run stopped mid-window (page cap or crash), so we
        // re-open from epoch and let the cursor keyset-skip what was already
        // processed.
        let (last_watermark, mut cursor) = match &checkpoint {
            Some(cp) if cp.cursor_values.is_some() => {
                (DateTime::<Utc>::UNIX_EPOCH, Cursor::from_checkpoint(cp))
            }
            Some(cp) => (cp.watermark, Cursor::first_page()),
            None => (DateTime::<Utc>::UNIX_EPOCH, Cursor::first_page()),
        };

        let edge_table = prefixed_table_name(EDGE_TABLE, *SCHEMA_VERSION);
        let writer = context
            .destination
            .new_batch_writer(&edge_table)
            .await
            .map_err(|e| HandlerError::Processing(format!("edge writer for {edge_table}: {e}")))?;

        let mut total_edges = 0usize;
        let mut total_notes = 0usize;
        // `drained` flips to false only if we exhaust the page budget while
        // pages are still full. A short/empty page means the window is
        // fully processed, regardless of how many full pages preceded it —
        // so the "exactly N full pages then empty" boundary still drains.
        let mut drained = true;

        for page in 0..self.max_pages {
            let (notes, advanced) = self
                .extract_page(
                    &request.traversal_path,
                    last_watermark,
                    request.watermark,
                    &cursor,
                )
                .await?;

            if notes.is_empty() {
                break;
            }
            total_notes += notes.len();
            let page_was_full = notes.len() as u64 >= self.batch_limit;

            // Advance the cursor to the last row of this page (the rows are
            // `ORDER BY sn.created_at, sn.id`, so the shared `Cursor::advance`
            // reads the keyset off the final batch row).
            cursor = advanced;

            let edges = self.resolve_and_emit(&notes).await?;

            if !edges.is_empty() {
                let batch = edge_record_batch(&edges, &self.edge_specs)
                    .map_err(|e| HandlerError::Processing(format!("edge batch: {e}")))?;
                writer
                    .write_batch(&[batch])
                    .await
                    .map_err(|e| HandlerError::Processing(format!("write {edge_table}: {e}")))?;
                total_edges += edges.len();
            }

            if !page_was_full {
                // Reached the tail of the window.
                break;
            }

            if page + 1 == self.max_pages {
                // Last allowed iteration and the page was still full, so the
                // window may have more rows. Persist the cursor and stop;
                // the next message resumes from it.
                self.checkpoint_store
                    .save_progress(
                        &key,
                        &Checkpoint {
                            watermark: request.watermark,
                            cursor_values: cursor.to_checkpoint_values(),
                        },
                    )
                    .await
                    .map_err(|e| HandlerError::Processing(e.to_string()))?;
                drained = false;
                break;
            }

            // Mid-window full page: persist the cursor so a crash resumes
            // here rather than re-scanning the window from the start.
            self.checkpoint_store
                .save_progress(
                    &key,
                    &Checkpoint {
                        watermark: request.watermark,
                        cursor_values: cursor.to_checkpoint_values(),
                    },
                )
                .await
                .map_err(|e| HandlerError::Processing(e.to_string()))?;
        }

        if drained {
            // Whole window processed: advance the watermark and clear the
            // cursor so the next message starts a fresh incremental window.
            self.checkpoint_store
                .save_completed(&key, &request.watermark)
                .await
                .map_err(|e| HandlerError::Processing(e.to_string()))?;
        } else {
            // Hit the page cap with rows still in the window. The cursor was
            // persisted above; the next message resumes from it.
            warn!(
                namespace = request.namespace,
                "system_notes: hit MAX_PAGES_PER_RUN; cursor persisted, resuming next message"
            );
        }

        info!(
            namespace = request.namespace,
            notes = total_notes,
            edges = total_edges,
            duration_ms = started_at.elapsed().as_millis() as u64,
            "system_notes: materialized edges"
        );
        Ok(())
    }
}

impl SystemNotesHandler {
    /// Run one page of the extract query, decode the rows, and return the
    /// cursor advanced past the last row of the page. The keyset predicate
    /// is produced by the shared [`CursorFilter`] over [`CURSOR_SORT_KEY`]
    /// and substituted into the `{{cursor}}` placeholder, so paging reuses
    /// the SDLC pipeline's cursor SQL rather than a hand-written clause.
    async fn extract_page(
        &self,
        traversal_path: &str,
        last_watermark: DateTime<Utc>,
        watermark: DateTime<Utc>,
        cursor: &Cursor,
    ) -> Result<(Vec<ExtractedNote>, Cursor), HandlerError> {
        let sort_key: Vec<String> = CURSOR_SORT_KEY.iter().map(|s| s.to_string()).collect();
        let cursor_clause = CursorFilter {
            sort_key: &sort_key,
            values: cursor.values(),
        }
        .condition();
        let cursor_sql = if cursor_clause.is_empty() {
            String::new()
        } else {
            format!("AND ({cursor_clause})")
        };
        let sql = SYSTEM_NOTES_EXTRACT_SQL.replace(CURSOR_PLACEHOLDER, &cursor_sql);

        let mut params = json!({
            "traversal_path": traversal_path,
            "last_watermark": last_watermark.format(TIMESTAMP_FORMAT).to_string(),
            "watermark": watermark.format(TIMESTAMP_FORMAT).to_string(),
            "batch_limit": self.batch_limit,
        });
        params[super::extract::ACTIONS_PARAM_NAME] = json!(handled_actions());

        let batches = self
            .datalake
            .query_batches(&sql, params, Some(self.batch_limit))
            .await
            .map_err(|e| HandlerError::Processing(format!("system_notes extract: {e}")))?;

        let mut notes = Vec::new();
        for batch in &batches {
            decode_extracted_notes(batch, &mut notes);
        }

        // Advance the cursor off the last non-empty batch. The cursor sort
        // key is table-qualified (`sn.created_at`), but the SELECT aliases
        // those to bare `created_at` / `id`, so advance on the bare names.
        let advance_key: Vec<String> = CURSOR_ADVANCE_KEY.iter().map(|s| s.to_string()).collect();
        let advanced = match batches.iter().rev().find(|b| b.num_rows() > 0) {
            Some(batch) => Cursor::first_page().advance(batch, &advance_key)?,
            None => cursor.clone(),
        };
        Ok((notes, advanced))
    }

    /// Resolve the batch's references against the datalake and emit edges.
    async fn resolve_and_emit(
        &self,
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
        // `unsupported_noteable` *can* fire at runtime: the extract query
        // does not constrain `noteable_type`, so a note on a kind we don't
        // map (e.g. a new Rails STI type) reaches here and is dropped. A
        // sustained non-zero count signals a missing mapping.
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

        let default_projects = self.resolve_default_projects(notes).await?;
        let plan = plan_for_batch(notes, &default_projects);
        let index = self.resolve_plan(&plan).await?;

        let edges = process_batch(notes, &default_projects, |r, default_project| {
            index.resolve(r, default_project)
        });
        Ok(edges)
    }
}

impl SystemNotesHandler {
    /// Resolve each note's owning `project_id` to a project path, keyed back
    /// to `(noteable_type, noteable_id)` so [`plan_for_batch`] and
    /// [`process_batch`] can substitute it for unqualified references.
    async fn resolve_default_projects(
        &self,
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

        // Reverse routes lookup: source_id (project_id) -> path. ROUTES_SQL
        // filters on `path IN`, so use a dedicated id-keyed query here.
        let params = json!({ "source_ids": project_ids });
        let batches = self
            .datalake
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

    /// Fan the [`ResolutionPlan`] out to the routes + noteable lookups and
    /// build the [`ResolvedIndex`] the emitter consults.
    async fn resolve_plan(&self, plan: &ResolutionPlan) -> Result<ResolvedIndex, HandlerError> {
        if plan.paths.is_empty() {
            return Ok(ResolvedIndex::default());
        }

        let paths: Vec<&str> = plan.paths.iter().map(String::as_str).collect();
        let routes = self.query_routes(&paths).await?;

        let mr_pairs: Vec<(i64, i64)> = pairs_with_project_id(&plan.mr_pairs, &routes);
        let wi_pairs: Vec<(i64, i64)> = pairs_with_project_id(&plan.issue_pairs, &routes);

        let mr_entities = self.query_entities(MERGE_REQUESTS_SQL, &mr_pairs).await?;
        let wi_entities = self.query_entities(WORK_ITEMS_SQL, &wi_pairs).await?;

        Ok(ResolvedIndex::build(&routes, &mr_entities, &wi_entities))
    }

    async fn query_routes(&self, paths: &[&str]) -> Result<Vec<RouteRow>, HandlerError> {
        let params = json!({ "paths": paths });
        let batches = self
            .datalake
            .query_batches(ROUTES_SQL, params, None)
            .await
            .map_err(|e| HandlerError::Processing(format!("system_notes routes: {e}")))?;

        let mut rows = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                if let (Some(source_type), Some(source_id), Some(path), Some(tp)) = (
                    ArrowUtils::get_column_string(batch, "source_type", row),
                    ArrowUtils::get_column::<Int64Type>(batch, "source_id", row),
                    ArrowUtils::get_column_string(batch, "path", row),
                    ArrowUtils::get_column_string(batch, "traversal_path", row),
                ) {
                    rows.push(RouteRow {
                        source_type,
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
        &self,
        sql: &str,
        pairs: &[(i64, i64)],
    ) -> Result<Vec<EntityRow>, HandlerError> {
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        // Pass the (project_id, iid) pairs as two parallel Int64 arrays; the
        // SQL zips them back into the tuple IN-list server-side. A single
        // Array(Tuple(...)) param can't survive the JSON parameter channel
        // (a tuple serializes as a nested array, which ClickHouse rejects).
        let project_ids: Vec<i64> = pairs.iter().map(|(p, _)| *p).collect();
        let iids: Vec<i64> = pairs.iter().map(|(_, i)| *i).collect();
        let params = json!({
            "project_ids": project_ids,
            "iids": iids,
        });
        let batches = self
            .datalake
            .query_batches(sql, params, None)
            .await
            .map_err(|e| HandlerError::Processing(format!("system_notes entities: {e}")))?;

        let mut rows = Vec::new();
        for batch in &batches {
            let project_id_col =
                ArrowUtils::get_column_by_index::<Int64Array>(batch, 1).filter(|_| {
                    // Second column is target_project_id (MR) or project_id
                    // (WI); both decode positionally as `(id, project_id, iid)`.
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
}

/// Map parser `(project_path, iid)` pairs onto `(project_id, iid)` pairs for
/// the noteable lookups, using the resolved project routes. Pairs whose
/// project path didn't resolve to a project route are dropped.
fn pairs_with_project_id(
    path_pairs: &std::collections::HashSet<(String, i64)>,
    routes: &[RouteRow],
) -> Vec<(i64, i64)> {
    let path_to_id: HashMap<&str, i64> = routes
        .iter()
        .filter(|r| r.source_type == "Project")
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

/// `ArrowUtils::get_column::<TimestampMicrosecondType>` returns the raw
/// `i64` micros; system-note rows carry `created_at` as `DateTime64(6)`, so
/// convert to a `DateTime<Utc>` here.
fn col_timestamp_micros(batch: &RecordBatch, name: &str, row: usize) -> Option<DateTime<Utc>> {
    let micros = ArrowUtils::get_column::<TimestampMicrosecondType>(batch, name, row)?;
    DateTime::<Utc>::from_timestamp_micros(micros)
}

/// `gl_edge` column specs derived from the ontology so the write schema
/// tracks `config/graph.sql` automatically. Mirrors the ontology-driven
/// `edge_specs` in `modules/code/arrow_converter.rs`, but scoped to the
/// single default edge table (`gl_edge`) that all system-note edges land
/// in — the code path builds the union across every edge table because it
/// also writes `gl_code_edge`.
///
/// `EmittedEdge::write_row` addresses columns by name, so the only contract
/// is that every column the ontology declares for `gl_edge` is written.
/// `source_tags` / `target_tags` come from the denormalized columns (always
/// empty for system-note edges — endpoints carry their own tags from their
/// entity ETL); `_version` / `_deleted` are the infra columns.
fn edge_specs(ontology: &Ontology) -> Vec<ColumnSpec> {
    let table = ontology.edge_table();
    let Some(config) = ontology.edge_table_config(table) else {
        // The default edge table is always present in a loaded ontology;
        // an absent config means a malformed schema and is a hard bug.
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

    /// `gl_edge` specs derived from the embedded ontology — the same path
    /// the production handler takes, so these tests would catch a drift
    /// between the ontology and `EmittedEdge::write_row`.
    fn test_specs() -> Vec<ColumnSpec> {
        let ontology = ontology::Ontology::load_embedded().expect("embedded ontology loads");
        edge_specs(&ontology)
    }

    #[test]
    fn checkpoint_key_uses_entity_scoped_namespace_prefix() {
        // Matches the ADR 014 checkpoint key convention so namespace
        // deletion's `startsWith(key, "ns.{id}.")` sweep covers it.
        assert_eq!(SystemNotesHandler::checkpoint_key(42), "ns.42.SystemNote");
    }

    #[test]
    fn handled_actions_cover_cross_reference_and_lifecycle() {
        let actions = handled_actions();
        assert!(actions.contains(&"cross_reference"));
        assert!(actions.contains(&"closed"));
        assert!(actions.contains(&"reopened"));
        assert!(actions.contains(&"merged"));
        // No unhandled action leaks into the IN-list.
        assert!(!actions.contains(&"description"));
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
        // follows the ontology column list (logical columns, then
        // denormalized tags, then `_version` / `_deleted`).
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
