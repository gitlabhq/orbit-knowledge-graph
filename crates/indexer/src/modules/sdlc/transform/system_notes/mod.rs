//! SystemNotes `BlockTransform` implementation (ADR 013, ADR 015).
//!
//! Parses system-note bodies extracted by the ontology-driven query plan
//! (`config/ontology/derived/core/system_note.yaml`), resolves GFM reference
//! tokens against the datalake, and emits `gl_edge` rows. Rides the shared
//! SDLC pipeline — only the transform body is custom Rust; extraction,
//! paging, checkpointing, and writing are all inherited.
//!
//! ## Registration
//!
//! [`register`] adds the factory to the [`TransformRegistry`] during handler
//! setup, flipping `is_registered("system_notes")` → true so the dormant
//! `SystemNote` plan starts producing an `EntityHandler`.

pub(crate) mod emit;
pub(crate) mod parse;
pub(crate) mod resolve;
pub(crate) mod vendored;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;
use tracing::warn;

use crate::handler::HandlerError;
use crate::modules::sdlc::datalake::DatalakeQuery;
use crate::modules::sdlc::transform::{
    BlockTransform, TableBatch, TransformFactory, TransformRegistry,
};

use emit::{EmittedEdge, NoteRow, NoteableKind, build_edges};
use parse::{Action, Reference, extract as parse_body};
use resolve::{
    EntityRow, MERGE_REQUESTS_SQL, PROJECT_PATHS_SQL, ROUTES_SQL, ResolutionPlan, ResolvedIndex,
    ResolvedTarget, RouteRow, WORK_ITEMS_SQL,
};

/// Raw row pulled from the extract SQL. The transform parses, resolves, and
/// emits edges from these rows in batches.
struct ExtractedNote {
    note: String,
    noteable_id: i64,
    noteable_type: String,
    author_id: Option<i64>,
    project_id: Option<i64>,
    created_at: DateTime<Utc>,
    traversal_path: String,
    action: String,
}

/// Best-effort default-project lookup keyed on `project_id`.
type DefaultProjectLookup = HashMap<i64, String>;

pub(in crate::modules::sdlc) struct SystemNotesTransform {
    datalake: Arc<dyn DatalakeQuery>,
    outputs: Vec<String>,
}

impl SystemNotesTransform {
    fn new(datalake: Arc<dyn DatalakeQuery>) -> Self {
        Self {
            datalake,
            outputs: vec!["gl_edge".to_string()],
        }
    }
}

#[async_trait]
impl BlockTransform for SystemNotesTransform {
    fn name(&self) -> &str {
        "system_notes"
    }

    fn outputs(&self) -> &[String] {
        &self.outputs
    }

    async fn transform(&self, block: &RecordBatch) -> Result<Vec<TableBatch>, HandlerError> {
        let notes = extract_notes(block)?;
        if notes.is_empty() {
            return Ok(Vec::new());
        }

        let Some(root_prefix) = notes
            .iter()
            .find_map(|n| gkg_utils::traversal_path::root_prefix(&n.traversal_path))
        else {
            warn!("system_notes: block has no valid traversal_path root; skipping resolution");
            return Ok(Vec::new());
        };

        let edges = resolve_and_emit(&*self.datalake, &notes, &root_prefix).await?;
        if edges.is_empty() {
            return Ok(Vec::new());
        }

        let batch = edges_to_record_batch(&edges)?;
        Ok(vec![TableBatch {
            output_index: 0,
            batch,
        }])
    }
}

/// Register the `system_notes` transform factory in the registry. Called
/// from `register_handlers` during handler setup; the captured `datalake`
/// handle is available for second-hop lookups (ADR 015).
pub(in crate::modules::sdlc) fn register(
    registry: &mut TransformRegistry,
    datalake: Arc<dyn DatalakeQuery>,
) {
    let factory: TransformFactory =
        Box::new(move |_plan| Arc::new(SystemNotesTransform::new(Arc::clone(&datalake))));
    registry.register("system_notes", factory);
}

// ---------------------------------------------------------------------------
// Extract block → ExtractedNote
// ---------------------------------------------------------------------------

fn col_string<'a>(block: &'a RecordBatch, name: &str) -> Result<&'a StringArray, HandlerError> {
    block
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| HandlerError::Processing(format!("missing or wrong-type column: {name}")))
}

fn col_i64<'a>(block: &'a RecordBatch, name: &str) -> Result<&'a Int64Array, HandlerError> {
    block
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| HandlerError::Processing(format!("missing or wrong-type column: {name}")))
}

fn col_ts<'a>(
    block: &'a RecordBatch,
    name: &str,
) -> Result<&'a TimestampMicrosecondArray, HandlerError> {
    block
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
        .ok_or_else(|| HandlerError::Processing(format!("missing or wrong-type column: {name}")))
}

fn extract_notes(block: &RecordBatch) -> Result<Vec<ExtractedNote>, HandlerError> {
    let num_rows = block.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    let note_col = col_string(block, "note")?;
    let noteable_id_col = col_i64(block, "noteable_id")?;
    let noteable_type_col = col_string(block, "noteable_type")?;
    let author_id_col = col_i64(block, "author_id")?;
    let project_id_col = col_i64(block, "project_id")?;
    let traversal_path_col = col_string(block, "traversal_path")?;
    let action_col = col_string(block, "action")?;
    let created_at_col = col_ts(block, "created_at")?;

    let mut notes = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        let created_at = if created_at_col.is_null(i) {
            Utc::now()
        } else {
            let micros = created_at_col.value(i);
            Utc.timestamp_micros(micros)
                .single()
                .unwrap_or_else(Utc::now)
        };
        notes.push(ExtractedNote {
            note: note_col.value(i).to_string(),
            noteable_id: noteable_id_col.value(i),
            noteable_type: noteable_type_col.value(i).to_string(),
            author_id: if author_id_col.is_null(i) {
                None
            } else {
                Some(author_id_col.value(i))
            },
            project_id: if project_id_col.is_null(i) {
                None
            } else {
                Some(project_id_col.value(i))
            },
            created_at,
            traversal_path: traversal_path_col.value(i).to_string(),
            action: action_col.value(i).to_string(),
        });
    }
    Ok(notes)
}

// ---------------------------------------------------------------------------
// Resolve + emit
// ---------------------------------------------------------------------------

async fn resolve_and_emit(
    datalake: &dyn DatalakeQuery,
    notes: &[ExtractedNote],
    root_prefix: &str,
) -> Result<Vec<EmittedEdge>, HandlerError> {
    let default_projects = resolve_default_projects(datalake, notes, root_prefix).await?;
    let plan = plan_for_batch(notes, &default_projects);
    let index = resolve_plan(datalake, &plan, root_prefix).await?;

    let edges = process_batch(notes, &default_projects, |r, default_project| {
        index.resolve(r, default_project)
    });

    Ok(edges)
}

fn process_batch<R>(
    notes: &[ExtractedNote],
    default_projects: &DefaultProjectLookup,
    mut resolve: R,
) -> Vec<EmittedEdge>
where
    R: FnMut(&Reference, &str) -> Option<ResolvedTarget>,
{
    let mut rows = Vec::with_capacity(notes.len());
    for n in notes {
        let Some(action) = Action::parse(&n.action) else {
            warn!(action = %n.action, "system_notes: unknown action, dropping");
            continue;
        };
        let Some(noteable_kind) = NoteableKind::from_siphon(&n.noteable_type) else {
            warn!(
                noteable_type = %n.noteable_type,
                "system_notes: unsupported noteable_type, dropping"
            );
            continue;
        };
        let references = parse_body(action, &n.note);
        let default_project = default_projects
            .get(&n.project_id.unwrap_or(0))
            .cloned()
            .unwrap_or_default();
        rows.push(NoteRow {
            traversal_path: n.traversal_path.clone(),
            default_project,
            author_id: n.author_id,
            noteable_id: n.noteable_id,
            noteable_kind,
            action,
            created_at: n.created_at,
            references,
        });
    }

    build_edges(&rows, |r, default_project| resolve(r, default_project))
}

fn plan_for_batch(
    notes: &[ExtractedNote],
    default_projects: &DefaultProjectLookup,
) -> ResolutionPlan {
    let mut plan = ResolutionPlan::default();
    for n in notes {
        let Some(action) = Action::parse(&n.action) else {
            continue;
        };
        if NoteableKind::from_siphon(&n.noteable_type).is_none() {
            continue;
        }
        let default_project = default_projects
            .get(&n.project_id.unwrap_or(0))
            .map(String::as_str)
            .unwrap_or("");
        for r in parse_body(action, &n.note) {
            plan.add_ref(&r, default_project);
        }
    }
    plan
}

// ---------------------------------------------------------------------------
// Datalake lookups
// ---------------------------------------------------------------------------

async fn resolve_default_projects(
    datalake: &dyn DatalakeQuery,
    notes: &[ExtractedNote],
    root_prefix: &str,
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

    let params = json!({ "root_prefix": root_prefix, "source_ids": project_ids });
    let batches = datalake
        .query_batches(PROJECT_PATHS_SQL, params, None)
        .await
        .map_err(|e| HandlerError::Processing(format!("project paths query failed: {e}")))?;

    let mut lookup = DefaultProjectLookup::new();
    for batch in &batches {
        let source_id_col = col_i64(batch, "source_id")?;
        let path_col = col_string(batch, "path")?;
        for i in 0..batch.num_rows() {
            lookup.insert(source_id_col.value(i), path_col.value(i).to_string());
        }
    }
    Ok(lookup)
}

async fn resolve_plan(
    datalake: &dyn DatalakeQuery,
    plan: &ResolutionPlan,
    root_prefix: &str,
) -> Result<ResolvedIndex, HandlerError> {
    if plan.paths.is_empty() {
        return Ok(ResolvedIndex::default());
    }

    let paths: Vec<&str> = plan.paths.iter().map(String::as_str).collect();
    let routes = query_routes(datalake, &paths, root_prefix).await?;

    let mr_pairs: Vec<(i64, i64)> = pairs_with_project_id(&plan.mr_pairs, &routes);
    let wi_pairs: Vec<(i64, i64)> = pairs_with_project_id(&plan.issue_pairs, &routes);

    let (mr_entities, wi_entities) = tokio::try_join!(
        query_entities(datalake, MERGE_REQUESTS_SQL, &mr_pairs, root_prefix),
        query_entities(datalake, WORK_ITEMS_SQL, &wi_pairs, root_prefix),
    )?;

    Ok(ResolvedIndex::build(&routes, &mr_entities, &wi_entities))
}

fn pairs_with_project_id(
    pairs: &std::collections::HashSet<(String, i64)>,
    routes: &[RouteRow],
) -> Vec<(i64, i64)> {
    let path_to_id: HashMap<&str, i64> = routes
        .iter()
        .map(|r| (r.path.as_str(), r.source_id))
        .collect();
    pairs
        .iter()
        .filter_map(|(path, iid)| path_to_id.get(path.as_str()).map(|pid| (*pid, *iid)))
        .collect()
}

async fn query_routes(
    datalake: &dyn DatalakeQuery,
    paths: &[&str],
    root_prefix: &str,
) -> Result<Vec<RouteRow>, HandlerError> {
    let params = json!({ "root_prefix": root_prefix, "paths": paths });
    let batches = datalake
        .query_batches(ROUTES_SQL, params, None)
        .await
        .map_err(|e| HandlerError::Processing(format!("routes query failed: {e}")))?;

    let mut rows = Vec::new();
    for batch in &batches {
        let source_id_col = col_i64(batch, "source_id")?;
        let path_col = col_string(batch, "path")?;
        let tp_col = col_string(batch, "traversal_path")?;
        for i in 0..batch.num_rows() {
            rows.push(RouteRow {
                source_id: source_id_col.value(i),
                path: path_col.value(i).to_string(),
                traversal_path: tp_col.value(i).to_string(),
            });
        }
    }
    Ok(rows)
}

async fn query_entities(
    datalake: &dyn DatalakeQuery,
    sql: &str,
    pairs: &[(i64, i64)],
    root_prefix: &str,
) -> Result<Vec<EntityRow>, HandlerError> {
    if pairs.is_empty() {
        return Ok(Vec::new());
    }
    let project_ids: Vec<i64> = pairs.iter().map(|(p, _)| *p).collect();
    let iids: Vec<i64> = pairs.iter().map(|(_, i)| *i).collect();
    let params = json!({
        "root_prefix": root_prefix,
        "project_ids": project_ids,
        "iids": iids,
    });
    let batches = datalake
        .query_batches(sql, params, None)
        .await
        .map_err(|e| HandlerError::Processing(format!("entity query failed: {e}")))?;

    let mut rows = Vec::new();
    for batch in &batches {
        let id_col = col_i64(batch, "id")?;
        let iid_col = col_i64(batch, "iid")?;
        let project_col_name = if batch.schema().field_with_name("target_project_id").is_ok() {
            "target_project_id"
        } else {
            "project_id"
        };
        let project_col = col_i64(batch, project_col_name)?;
        for i in 0..batch.num_rows() {
            rows.push(EntityRow {
                id: id_col.value(i),
                project_id: project_col.value(i),
                iid: iid_col.value(i),
            });
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Edge → RecordBatch
// ---------------------------------------------------------------------------

fn edges_to_record_batch(edges: &[EmittedEdge]) -> Result<RecordBatch, HandlerError> {
    let len = edges.len();
    let mut traversal_paths = Vec::with_capacity(len);
    let mut relationship_kinds = Vec::with_capacity(len);
    let mut source_ids = Vec::with_capacity(len);
    let mut source_kinds = Vec::with_capacity(len);
    let mut target_ids = Vec::with_capacity(len);
    let mut target_kinds = Vec::with_capacity(len);
    let mut versions = Vec::with_capacity(len);
    let mut deleted = Vec::with_capacity(len);

    for e in edges {
        traversal_paths.push(e.traversal_path.as_str());
        relationship_kinds.push(e.relationship_kind);
        source_ids.push(e.source_id);
        source_kinds.push(e.source_kind);
        target_ids.push(e.target_id);
        target_kinds.push(e.target_kind);
        versions.push(e.version_micros);
        deleted.push(false);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("traversal_path", DataType::Utf8, false),
        Field::new("relationship_kind", DataType::Utf8, false),
        Field::new("source_id", DataType::Int64, false),
        Field::new("source_kind", DataType::Utf8, false),
        Field::new("target_id", DataType::Int64, false),
        Field::new("target_kind", DataType::Utf8, false),
        Field::new(
            "_version",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("_deleted", DataType::Boolean, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(traversal_paths)),
            Arc::new(StringArray::from(relationship_kinds)),
            Arc::new(Int64Array::from(source_ids)),
            Arc::new(StringArray::from(source_kinds)),
            Arc::new(Int64Array::from(target_ids)),
            Arc::new(StringArray::from(target_kinds)),
            Arc::new(TimestampMicrosecondArray::from(versions).with_timezone("UTC")),
            Arc::new(arrow::array::BooleanArray::from(deleted)),
        ],
    )
    .map_err(|e| HandlerError::Processing(format!("failed to build gl_edge batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_note(action: &str, body: &str, noteable_type: &str, noteable_id: i64) -> ExtractedNote {
        ExtractedNote {
            note: body.to_string(),
            noteable_id,
            noteable_type: noteable_type.to_string(),
            author_id: Some(7),
            project_id: Some(100),
            created_at: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            traversal_path: "1/100/".to_string(),
            action: action.to_string(),
        }
    }

    #[test]
    fn process_batch_emits_mentions_edge_for_cross_reference() {
        let notes = vec![make_note(
            "cross_reference",
            "mentioned in !456",
            "MergeRequest",
            100,
        )];
        let edges = process_batch(&notes, &DefaultProjectLookup::new(), |_r, _default| {
            Some(ResolvedTarget {
                id: 456,
                traversal_path: "1/100/".to_string(),
            })
        });
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].relationship_kind, "MENTIONS");
    }

    #[test]
    fn process_batch_passes_default_project_to_resolver() {
        let notes = vec![make_note(
            "cross_reference",
            "mentioned in !456",
            "MergeRequest",
            100,
        )];
        let mut defaults = DefaultProjectLookup::new();
        defaults.insert(100, "my/proj".to_string());

        let mut seen_default = None;
        let _edges = process_batch(&notes, &defaults, |_r, default| {
            seen_default = Some(default.to_string());
            Some(ResolvedTarget {
                id: 456,
                traversal_path: "1/100/".to_string(),
            })
        });
        assert_eq!(seen_default.as_deref(), Some("my/proj"));
    }

    #[test]
    fn process_batch_emits_user_closed_edge_for_lifecycle_action() {
        let notes = vec![make_note("closed", "closed", "Issue", 999)];
        let edges = process_batch(&notes, &DefaultProjectLookup::new(), |_, _| None);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].relationship_kind, "CLOSED");
        assert_eq!(edges[0].target_kind, "WorkItem");
        assert_eq!(edges[0].source_id, 7);
    }

    #[test]
    fn process_batch_drops_unknown_action_silently() {
        let notes = vec![make_note("designs_added", "", "MergeRequest", 1)];
        let edges = process_batch(&notes, &DefaultProjectLookup::new(), |_, _| None);
        assert!(edges.is_empty());
    }

    #[test]
    fn process_batch_drops_unsupported_noteable_type_silently() {
        let notes = vec![make_note("closed", "closed", "Snippet", 1)];
        let edges = process_batch(&notes, &DefaultProjectLookup::new(), |_, _| None);
        assert!(edges.is_empty());
    }

    #[test]
    fn plan_for_batch_collects_distinct_iid_pairs() {
        let notes = vec![
            make_note(
                "cross_reference",
                "mentioned in gitlab-org/gitlab!42",
                "MergeRequest",
                100,
            ),
            make_note(
                "cross_reference",
                "mentioned in gitlab-org/gitlab!42",
                "MergeRequest",
                101,
            ),
            make_note(
                "cross_reference",
                "mentioned in gitlab-org/gitlab#9",
                "MergeRequest",
                102,
            ),
        ];
        let defaults = DefaultProjectLookup::new();
        let plan = plan_for_batch(&notes, &defaults);
        assert_eq!(plan.mr_pairs.len(), 1, "MR pair !42 deduped");
        assert_eq!(plan.issue_pairs.len(), 1, "issue pair #9");
        assert_eq!(plan.paths.len(), 1, "project path gitlab-org/gitlab");
    }
}
