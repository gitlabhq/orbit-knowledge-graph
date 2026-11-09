use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use code_graph::v2::SinkError;
use orbit_arrow::{ArrowUtils, AsRecordBatch, BatchBuilder};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use super::super::arrow_converter::{
    ConvertedGraphData, ConverterSpecs, IndexerEdgeRow, IndexerEnvelope, compute_branch_id,
    convert_branch_row,
};

pub(super) struct Output<'a> {
    envelope: &'a IndexerEnvelope,
    specs: &'a ConverterSpecs,
    emitted_branch: AtomicBool,
    pub(super) batch_bytes: usize,
}

fn text(batch: &RecordBatch, column: &str, row: usize) -> Result<String, SinkError> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| SinkError(format!("missing {column} at row {row}")))?;
    arrow::util::display::array_value_to_string(array.as_ref(), row).map_err(SinkError::from)
}

fn id(batch: &RecordBatch, column: &str, row: usize) -> Result<i64, SinkError> {
    ArrowUtils::get_column::<Int64Type>(batch, column, row)
        .ok_or_else(|| SinkError(format!("missing {column} at row {row}")))
}

impl<'a> Output<'a> {
    pub fn new(envelope: &'a IndexerEnvelope, specs: &'a ConverterSpecs) -> Self {
        Self {
            envelope,
            specs,
            emitted_branch: AtomicBool::new(false),
            batch_bytes: orbit_arrow::BATCH_BYTES,
        }
    }

    pub fn convert(
        &self,
        tables: Vec<(String, RecordBatch)>,
    ) -> Result<Vec<ConvertedGraphData>, SinkError> {
        let mut by_table: HashMap<_, Vec<_>> = HashMap::new();
        for (table, batch) in tables {
            by_table.entry(table).or_default().push(batch);
        }
        let mut take = |table: &str| {
            by_table
                .remove(table)
                .ok_or_else(|| SinkError(format!("missing {table}")))
        };
        let properties = &self.specs.tag_properties;
        let branch_id = compute_branch_id(self.envelope.project_id, &self.envelope.branch);
        let branch_tags: Vec<_> = properties
            .get("Branch")
            .into_iter()
            .flatten()
            .map(|(key, _)| format!("{key}:true"))
            .collect();
        let mut tags: HashMap<i64, Vec<String>> = HashMap::new();
        let mut structure = Vec::new();
        let mut entities = Vec::new();
        for kind in ["Directory", "File", "Definition", "ImportedSymbol"] {
            let table = self
                .specs
                .export
                .entity(kind)
                .ok_or_else(|| SinkError(format!("{kind} is not a local entity")))?;
            let batches = take(&table.table)?;
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    let id = id(batch, "id", row)?;
                    let row_tags = properties
                        .get(kind)
                        .into_iter()
                        .flatten()
                        .map(|(key, property)| {
                            text(batch, property, row).map(|value| format!("{key}:{value}"))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    tags.insert(id, row_tags);
                    if matches!(kind, "Directory" | "File") {
                        structure.push((id, kind, text(batch, "path", row)?));
                    }
                }
            }
            entities.push(batches);
        }
        let mut edges = BatchBuilder::new(&self.specs.edge, 0)?;
        let mut edge_batches = Vec::new();
        let empty_tags = Vec::new();
        let mut edge =
            |source_id, target_id, edge_kind: &str, source_kind: &str, target_kind: &str| {
                let source_tags = if source_kind == "Branch" {
                    &branch_tags
                } else {
                    tags.get(&source_id).unwrap_or(&empty_tags)
                };
                let target_tags = if target_kind == "Branch" {
                    &branch_tags
                } else {
                    tags.get(&target_id).unwrap_or(&empty_tags)
                };
                IndexerEdgeRow {
                    env: self.envelope,
                    source_id,
                    target_id,
                    edge_kind,
                    source_node_kind: source_kind,
                    target_node_kind: target_kind,
                    source_tags,
                    target_tags,
                }
                .write_row(&mut edges, &())?;
                edge_batches.extend(edges.take_if_over(self.batch_bytes)?);
                Ok::<_, arrow::error::ArrowError>(())
            };
        let branch = if self.emitted_branch.swap(true, Ordering::Relaxed) {
            BatchBuilder::new(&self.specs.branch, 0)?.finish()?
        } else {
            edge(
                branch_id,
                self.envelope.project_id,
                "IN_PROJECT",
                "Branch",
                "Project",
            )?;
            edge(
                self.envelope.project_id,
                branch_id,
                "CONTAINS",
                "Project",
                "Branch",
            )?;
            convert_branch_row(self.envelope, &self.specs.branch)?
        };
        for (id, kind, path) in structure {
            edge(id, branch_id, "ON_BRANCH", kind, "Branch")?;
            if path != "." && !path.contains('/') {
                edge(branch_id, id, "CONTAINS", "Branch", kind)?;
            }
        }
        for batch in take(&self.specs.export.edge_table)? {
            for row in 0..batch.num_rows() {
                let target = id(&batch, "target_id", row)?;
                let kind = text(&batch, "relationship_kind", row)?;
                edge(
                    id(&batch, "source_id", row)?,
                    target,
                    &kind,
                    &text(&batch, "source_kind", row)?,
                    &text(&batch, "target_kind", row)?,
                )?;
            }
        }
        edge_batches.push(edges.finish()?);
        let [directories, files, definitions, imported_symbols]: [Vec<RecordBatch>; 4] = entities
            .try_into()
            .map_err(|_| SinkError("invalid incremental entity set".into()))?;
        let parts = [
            &directories,
            &files,
            &definitions,
            &imported_symbols,
            &edge_batches,
        ]
        .iter()
        .map(|batches| batches.len())
        .max()
        .unwrap_or(1);
        let part = |batches: &[RecordBatch], index: usize| {
            batches
                .get(index)
                .cloned()
                .unwrap_or_else(|| RecordBatch::new_empty(batches[0].schema()))
        };
        Ok((0..parts)
            .map(|index| ConvertedGraphData {
                branch: if index == 0 {
                    branch.clone()
                } else {
                    RecordBatch::new_empty(branch.schema())
                },
                directories: part(&directories, index),
                files: part(&files, index),
                definitions: part(&definitions, index),
                imported_symbols: part(&imported_symbols, index),
                edges: part(&edge_batches, index),
            })
            .collect())
    }
}
