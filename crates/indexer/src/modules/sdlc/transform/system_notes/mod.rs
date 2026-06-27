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

use arrow::array::{
    Array, Int64Array, ListBuilder, StringArray, StringBuilder, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use tracing::warn;

use gkg_utils::arrow::ArrowUtils;

use crate::handler::HandlerError;
use crate::modules::sdlc::datalake::DatalakeQuery;
use crate::modules::sdlc::transform::{
    BlockTransform, TableBatch, TransformFactory, TransformRegistry,
};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

/// Registry key for this transform; also the `etl.transform` value in
/// `config/ontology/derived/core/system_note.yaml`.
pub(in crate::modules::sdlc) const TRANSFORM_NAME: &str = "system_notes";

use emit::{EmittedEdge, NoteRow, NoteableKind, build_edges};
use parse::{Action, Reference, extract as parse_body};
use resolve::{
    EntityRow, MERGE_REQUESTS_SQL, PROJECT_PATHS_SQL, ROUTES_SQL, ResolutionPlan, ResolvedIndex,
    ResolvedTarget, RouteRow, WORK_ITEMS_SQL, lookup_chunks, paths_per_routes_query,
};

struct ExtractedNote {
    note: String,
    noteable_id: i64,
    noteable_type: String,
    author_id: Option<i64>,
    project_id: Option<i64>,
    traversal_path: String,
    action: String,
}

type DefaultProjectLookup = HashMap<i64, String>;

pub(in crate::modules::sdlc) struct SystemNotesTransform {
    datalake: Arc<dyn DatalakeQuery>,
    outputs: Vec<String>,
    resolve_lookup_batch_size: usize,
}

impl SystemNotesTransform {
    fn new(
        datalake: Arc<dyn DatalakeQuery>,
        edge_table: String,
        resolve_lookup_batch_size: usize,
    ) -> Self {
        Self {
            datalake,
            outputs: vec![edge_table],
            resolve_lookup_batch_size,
        }
    }
}

#[async_trait]
impl BlockTransform for SystemNotesTransform {
    fn name(&self) -> &str {
        TRANSFORM_NAME
    }

    fn outputs(&self) -> &[String] {
        &self.outputs
    }

    async fn transform(&self, block: &RecordBatch) -> Result<Vec<TableBatch>, HandlerError> {
        let notes = batch_to_notes(block)?;
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

        let edges = resolve_and_emit(
            &*self.datalake,
            &notes,
            &root_prefix,
            self.resolve_lookup_batch_size,
        )
        .await?;
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

/// The captured `datalake` handle is available for second-hop lookups (ADR 015).
pub(in crate::modules::sdlc) fn register(
    registry: &mut TransformRegistry,
    datalake: Arc<dyn DatalakeQuery>,
    edge_table: &str,
    resolve_lookup_batch_size: usize,
) {
    // Every write path targets the current schema version's table-set, so the
    // hand-written transform must prefix its destination exactly like the
    // lowered DataFusion plans do (`lower.rs`); a bare `gl_edge` writes to the
    // wrong (unprefixed) table.
    let edge_table = prefixed_table_name(edge_table, *SCHEMA_VERSION);
    let factory: TransformFactory = Box::new(move |_plan| {
        Arc::new(SystemNotesTransform::new(
            Arc::clone(&datalake),
            edge_table.clone(),
            resolve_lookup_batch_size,
        ))
    });
    registry.register(TRANSFORM_NAME, factory);
}

fn col<'a, A: 'static>(block: &'a RecordBatch, name: &str) -> Result<&'a A, HandlerError> {
    ArrowUtils::get_column_by_name::<A>(block, name)
        .ok_or_else(|| HandlerError::Processing(format!("missing or wrong-type column: {name}")))
}

fn batch_to_notes(block: &RecordBatch) -> Result<Vec<ExtractedNote>, HandlerError> {
    let num_rows = block.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    let note_col = col::<StringArray>(block, "note")?;
    let noteable_id_col = col::<Int64Array>(block, "noteable_id")?;
    let noteable_type_col = col::<StringArray>(block, "noteable_type")?;
    let author_id_col = col::<Int64Array>(block, "author_id")?;
    let project_id_col = col::<Int64Array>(block, "project_id")?;
    let traversal_path_col = col::<StringArray>(block, "traversal_path")?;
    let action_col = col::<StringArray>(block, "action")?;

    let mut notes = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
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
            traversal_path: traversal_path_col.value(i).to_string(),
            action: action_col.value(i).to_string(),
        });
    }
    Ok(notes)
}

async fn resolve_and_emit(
    datalake: &dyn DatalakeQuery,
    notes: &[ExtractedNote],
    root_prefix: &str,
    resolve_lookup_batch_size: usize,
) -> Result<Vec<EmittedEdge>, HandlerError> {
    let default_projects =
        resolve_default_projects(datalake, notes, root_prefix, resolve_lookup_batch_size).await?;
    let plan = plan_for_batch(notes, &default_projects);
    let index = resolve_plan(datalake, &plan, root_prefix, resolve_lookup_batch_size).await?;

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

async fn resolve_default_projects(
    datalake: &dyn DatalakeQuery,
    notes: &[ExtractedNote],
    root_prefix: &str,
    resolve_lookup_batch_size: usize,
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

    let mut lookup = DefaultProjectLookup::new();
    for source_ids in lookup_chunks(&project_ids, resolve_lookup_batch_size) {
        let params = json!({ "root_prefix": root_prefix, "source_ids": source_ids });
        let batches = datalake
            .query_batches(&PROJECT_PATHS_SQL, params, None)
            .await
            .map_err(|e| HandlerError::Processing(format!("project paths query failed: {e}")))?;

        for batch in &batches {
            let source_id_col = col::<Int64Array>(batch, "source_id")?;
            let path_col = col::<StringArray>(batch, "path")?;
            for i in 0..batch.num_rows() {
                lookup.insert(source_id_col.value(i), path_col.value(i).to_string());
            }
        }
    }
    Ok(lookup)
}

async fn resolve_plan(
    datalake: &dyn DatalakeQuery,
    plan: &ResolutionPlan,
    root_prefix: &str,
    resolve_lookup_batch_size: usize,
) -> Result<ResolvedIndex, HandlerError> {
    if plan.paths.is_empty() {
        return Ok(ResolvedIndex::default());
    }

    let paths: Vec<&str> = plan.paths.iter().map(String::as_str).collect();
    let routes = query_routes(datalake, &paths, root_prefix).await?;

    let mr_pairs: Vec<(i64, i64)> = pairs_with_project_id(&plan.mr_pairs, &routes);
    let wi_pairs: Vec<(i64, i64)> = pairs_with_project_id(&plan.issue_pairs, &routes);

    let (mr_entities, wi_entities) = tokio::try_join!(
        query_entities(
            datalake,
            &MERGE_REQUESTS_SQL,
            &mr_pairs,
            root_prefix,
            resolve_lookup_batch_size
        ),
        query_entities(
            datalake,
            &WORK_ITEMS_SQL,
            &wi_pairs,
            root_prefix,
            resolve_lookup_batch_size
        ),
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
    let mut rows = Vec::new();

    for paths in lookup_chunks(paths, paths_per_routes_query(paths)) {
        let params = json!({ "root_prefix": root_prefix, "paths": paths });
        let batches = datalake
            .query_batches(&ROUTES_SQL, params, None)
            .await
            .map_err(|e| HandlerError::Processing(format!("routes query failed: {e}")))?;

        for batch in &batches {
            let source_id_col = col::<Int64Array>(batch, "source_id")?;
            let path_col = col::<StringArray>(batch, "path")?;
            let tp_col = col::<StringArray>(batch, "traversal_path")?;
            for i in 0..batch.num_rows() {
                rows.push(RouteRow {
                    source_id: source_id_col.value(i),
                    path: path_col.value(i).to_string(),
                    traversal_path: tp_col.value(i).to_string(),
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
    root_prefix: &str,
    resolve_lookup_batch_size: usize,
) -> Result<Vec<EntityRow>, HandlerError> {
    if pairs.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    for pairs in lookup_chunks(pairs, resolve_lookup_batch_size) {
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

        for batch in &batches {
            let id_col = col::<Int64Array>(batch, "id")?;
            let iid_col = col::<Int64Array>(batch, "iid")?;
            let project_col_name = if batch.schema().field_with_name("target_project_id").is_ok() {
                "target_project_id"
            } else {
                "project_id"
            };
            let project_col = col::<Int64Array>(batch, project_col_name)?;
            for i in 0..batch.num_rows() {
                rows.push(EntityRow {
                    id: id_col.value(i),
                    project_id: project_col.value(i),
                    iid: iid_col.value(i),
                });
            }
        }
    }
    Ok(rows)
}

fn edges_to_record_batch(edges: &[EmittedEdge]) -> Result<RecordBatch, HandlerError> {
    let len = edges.len();
    let mut traversal_paths = Vec::with_capacity(len);
    let mut relationship_kinds = Vec::with_capacity(len);
    let mut source_ids = Vec::with_capacity(len);
    let mut source_kinds = Vec::with_capacity(len);
    let mut target_ids = Vec::with_capacity(len);
    let mut target_kinds = Vec::with_capacity(len);
    let mut deleted = Vec::with_capacity(len);

    // Strict ClickHouse rejects an INSERT omitting a no-DEFAULT column, so emit the
    // denormalized tag columns as empty lists even though system notes project no tags.
    let mut source_tags = ListBuilder::new(StringBuilder::new());
    let mut target_tags = ListBuilder::new(StringBuilder::new());

    for e in edges {
        traversal_paths.push(e.traversal_path.as_str());
        relationship_kinds.push(e.relationship_kind);
        source_ids.push(e.source_id);
        source_kinds.push(e.source_kind);
        target_ids.push(e.target_id);
        target_kinds.push(e.target_kind);
        deleted.push(false);
        source_tags.append(true);
        target_tags.append(true);
    }

    let source_tags = source_tags.finish();
    let target_tags = target_tags.finish();
    let version_micros = Utc::now().timestamp_micros();
    let versions = TimestampMicrosecondArray::from(vec![version_micros; len]).with_timezone("UTC");

    let schema = Arc::new(Schema::new(vec![
        Field::new("traversal_path", DataType::Utf8, false),
        Field::new("relationship_kind", DataType::Utf8, false),
        Field::new("source_id", DataType::Int64, false),
        Field::new("source_kind", DataType::Utf8, false),
        Field::new("target_id", DataType::Int64, false),
        Field::new("target_kind", DataType::Utf8, false),
        Field::new("source_tags", source_tags.data_type().clone(), false),
        Field::new("target_tags", target_tags.data_type().clone(), false),
        Field::new("_version", versions.data_type().clone(), false),
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
            Arc::new(source_tags),
            Arc::new(target_tags),
            Arc::new(versions),
            Arc::new(arrow::array::BooleanArray::from(deleted)),
        ],
    )
    .map_err(|e| HandlerError::Processing(format!("failed to build gl_edge batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use arrow::error::ArrowError;
    use async_trait::async_trait;
    use futures::stream;

    use crate::modules::sdlc::datalake::{DatalakeError, RecordBatchStream};
    use crate::modules::sdlc::transform::system_notes::parse::RefKind;

    const TEST_RESOLVE_LOOKUP_BATCH_SIZE: usize = 1_000;
    const TEST_ENTITY_ID_PROJECT_FACTOR: i64 = 1_000_000;

    fn make_note(action: &str, body: &str, noteable_type: &str, noteable_id: i64) -> ExtractedNote {
        ExtractedNote {
            note: body.to_string(),
            noteable_id,
            noteable_type: noteable_type.to_string(),
            author_id: Some(7),
            project_id: Some(100),
            traversal_path: "1/100/".to_string(),
            action: action.to_string(),
        }
    }

    struct RecordingDatalake {
        queries: Mutex<Vec<serde_json::Value>>,
    }

    impl RecordingDatalake {
        fn new() -> Self {
            Self {
                queries: Mutex::new(Vec::new()),
            }
        }

        fn queries(&self) -> Vec<serde_json::Value> {
            self.queries.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DatalakeQuery for RecordingDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: serde_json::Value,
            _max_block_size: Option<u64>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(stream::empty()))
        }

        async fn query_batches(
            &self,
            sql: &str,
            params: serde_json::Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            self.queries.lock().unwrap().push(params.clone());
            if sql == *ROUTES_SQL {
                route_batch_from_params(&params).map(|batch| vec![batch])
            } else if sql == *PROJECT_PATHS_SQL {
                project_paths_batch_from_params(&params).map(|batch| vec![batch])
            } else if sql == *MERGE_REQUESTS_SQL {
                entity_batch_from_params(&params, "target_project_id").map(|batch| vec![batch])
            } else if sql == *WORK_ITEMS_SQL {
                entity_batch_from_params(&params, "project_id").map(|batch| vec![batch])
            } else {
                Ok(Vec::new())
            }
            .map_err(DatalakeError::ArrowDecode)
        }
    }

    fn json_i64_array(params: &serde_json::Value, key: &str) -> Vec<i64> {
        params[key]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_i64().unwrap())
            .collect()
    }

    fn json_str_array(params: &serde_json::Value, key: &str) -> Vec<String> {
        params[key]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap().to_string())
            .collect()
    }

    fn route_batch_from_params(params: &serde_json::Value) -> Result<RecordBatch, ArrowError> {
        let paths = json_str_array(params, "paths");
        let source_ids: Vec<i64> = paths
            .iter()
            .map(|path| path.rsplit('-').next().unwrap().parse().unwrap())
            .collect();
        let traversal_paths: Vec<String> = source_ids.iter().map(|id| format!("1/{id}/")).collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("source_id", DataType::Int64, false),
                Field::new("path", DataType::Utf8, false),
                Field::new("traversal_path", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(source_ids)),
                Arc::new(StringArray::from(paths)),
                Arc::new(StringArray::from(traversal_paths)),
            ],
        )
    }

    fn project_paths_batch_from_params(
        params: &serde_json::Value,
    ) -> Result<RecordBatch, ArrowError> {
        let source_ids = json_i64_array(params, "source_ids");
        let paths: Vec<String> = source_ids
            .iter()
            .map(|id| format!("group/project-{id}"))
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("source_id", DataType::Int64, false),
                Field::new("path", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(source_ids)),
                Arc::new(StringArray::from(paths)),
            ],
        )
    }

    fn entity_batch_from_params(
        params: &serde_json::Value,
        project_column: &str,
    ) -> Result<RecordBatch, ArrowError> {
        let project_ids = json_i64_array(params, "project_ids");
        let iids = json_i64_array(params, "iids");
        let ids: Vec<i64> = project_ids
            .iter()
            .zip(iids.iter())
            .map(|(project_id, iid)| project_id * TEST_ENTITY_ID_PROJECT_FACTOR + iid)
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new(project_column, DataType::Int64, false),
                Field::new("iid", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(project_ids)),
                Arc::new(Int64Array::from(iids)),
            ],
        )
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

    #[tokio::test]
    async fn resolve_default_projects_chunks_source_id_lookups() {
        let datalake = RecordingDatalake::new();
        let notes: Vec<_> = (0..TEST_RESOLVE_LOOKUP_BATCH_SIZE + 2)
            .map(|i| ExtractedNote {
                project_id: Some(i as i64),
                ..make_note("cross_reference", "mentioned in #1", "Issue", i as i64)
            })
            .collect();

        let lookup =
            resolve_default_projects(&datalake, &notes, "1/", TEST_RESOLVE_LOOKUP_BATCH_SIZE)
                .await
                .unwrap();

        let queries = datalake.queries();
        let chunk_sizes: Vec<_> = queries
            .iter()
            .map(|params| params["source_ids"].as_array().unwrap().len())
            .collect();
        assert_eq!(chunk_sizes, vec![TEST_RESOLVE_LOOKUP_BATCH_SIZE, 2]);
        assert_eq!(lookup.len(), TEST_RESOLVE_LOOKUP_BATCH_SIZE + 2);
        assert_eq!(lookup[&0], "group/project-0");
        assert_eq!(
            lookup[&((TEST_RESOLVE_LOOKUP_BATCH_SIZE + 1) as i64)],
            format!("group/project-{}", TEST_RESOLVE_LOOKUP_BATCH_SIZE + 1)
        );
    }

    #[tokio::test]
    async fn query_routes_splits_path_lookups_and_unions_rows() {
        let datalake = RecordingDatalake::new();
        let paths: Vec<_> = (0..3_000).map(|i| format!("group/project-{i}")).collect();
        let path_refs: Vec<_> = paths.iter().map(String::as_str).collect();

        let routes = query_routes(&datalake, &path_refs, "1/").await.unwrap();

        assert!(
            datalake.queries().len() > 1,
            "input must span multiple chunks"
        );
        assert_eq!(routes.len(), 3_000);
        assert_eq!(routes[0].source_id, 0);
        assert_eq!(routes[2_999].source_id, 2_999);
    }

    #[tokio::test]
    async fn query_entities_chunks_project_iid_pair_lookups_and_unions_rows() {
        let datalake = RecordingDatalake::new();
        let pairs: Vec<_> = (0..TEST_RESOLVE_LOOKUP_BATCH_SIZE + 2)
            .map(|i| (10_000 + i as i64, i as i64))
            .collect();

        let entities = query_entities(
            &datalake,
            &WORK_ITEMS_SQL,
            &pairs,
            "1/",
            TEST_RESOLVE_LOOKUP_BATCH_SIZE,
        )
        .await
        .unwrap();

        let queries = datalake.queries();
        let chunk_sizes: Vec<_> = queries
            .iter()
            .map(|params| {
                let project_ids = params["project_ids"].as_array().unwrap();
                assert_eq!(project_ids.len(), params["iids"].as_array().unwrap().len());
                project_ids.len()
            })
            .collect();
        assert_eq!(chunk_sizes, vec![TEST_RESOLVE_LOOKUP_BATCH_SIZE, 2]);
        assert_eq!(entities.len(), TEST_RESOLVE_LOOKUP_BATCH_SIZE + 2);
        assert_eq!(entities[0].project_id, 10_000);
        assert_eq!(entities[0].iid, 0);
        assert_eq!(
            entities[TEST_RESOLVE_LOOKUP_BATCH_SIZE + 1].project_id,
            10_000 + (TEST_RESOLVE_LOOKUP_BATCH_SIZE + 1) as i64
        );
    }

    #[tokio::test]
    async fn resolve_plan_unions_results_from_more_than_three_chunks() {
        let datalake = RecordingDatalake::new();
        let mut plan = ResolutionPlan::default();
        for i in 0..(TEST_RESOLVE_LOOKUP_BATCH_SIZE * 3 + 7) {
            let path = format!("group/project-{i}");
            plan.paths.insert(path.clone());
            plan.issue_pairs.insert((path, i as i64));
        }

        let index = resolve_plan(&datalake, &plan, "1/", TEST_RESOLVE_LOOKUP_BATCH_SIZE)
            .await
            .unwrap();

        for i in [
            0,
            TEST_RESOLVE_LOOKUP_BATCH_SIZE,
            TEST_RESOLVE_LOOKUP_BATCH_SIZE * 3 + 6,
        ] {
            let reference = Reference {
                kind: RefKind::Issue,
                project_path: Some(format!("group/project-{i}")),
                iid: Some(i as i64),
                commit_sha: None,
            };
            let resolved = index.resolve(&reference, "").unwrap();
            assert_eq!(
                resolved.id,
                i as i64 * TEST_ENTITY_ID_PROJECT_FACTOR + i as i64
            );
            assert_eq!(resolved.traversal_path, format!("1/{i}/"));
        }
    }

    #[tokio::test]
    async fn resolve_plan_empty_input_skips_lookups() {
        let datalake = RecordingDatalake::new();
        let index = resolve_plan(
            &datalake,
            &ResolutionPlan::default(),
            "1/",
            TEST_RESOLVE_LOOKUP_BATCH_SIZE,
        )
        .await
        .unwrap();
        let reference = Reference {
            kind: RefKind::Issue,
            project_path: Some("group/project-1".to_string()),
            iid: Some(1),
            commit_sha: None,
        };
        assert!(index.resolve(&reference, "").is_none());
        assert!(datalake.queries().is_empty());
    }

    #[tokio::test]
    async fn resolve_plan_resolves_boundary_references_across_chunks() {
        for count in [
            TEST_RESOLVE_LOOKUP_BATCH_SIZE,
            TEST_RESOLVE_LOOKUP_BATCH_SIZE + 1,
        ] {
            let datalake = RecordingDatalake::new();
            let mut plan = ResolutionPlan::default();
            for i in 0..count {
                let path = format!("group/project-{i}");
                plan.paths.insert(path.clone());
                plan.issue_pairs.insert((path, i as i64));
            }

            let index = resolve_plan(&datalake, &plan, "1/", TEST_RESOLVE_LOOKUP_BATCH_SIZE)
                .await
                .unwrap();
            let reference = Reference {
                kind: RefKind::Issue,
                project_path: Some(format!("group/project-{}", count - 1)),
                iid: Some((count - 1) as i64),
                commit_sha: None,
            };

            let resolved = index.resolve(&reference, "").unwrap();
            assert_eq!(resolved.traversal_path, format!("1/{}/", count - 1));
        }
    }
}
