//! Convert a v2 `CodeGraph` directly to Arrow batches for ClickHouse.
//!
//! Uses the shared row types from code-graph with an `IndexerEnvelope`
//! that adds `traversal_path`, `_version`, and `_deleted` columns.
//! Column schemas are driven by the ontology — the source of truth
//! for what columns each entity table has.

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use code_graph::v2::linker::graph::{DefinitionRow, DirectoryRow, FileRow, ImportRow};
use gkg_utils::arrow::{AsRecordBatch, BatchBuilder, ColumnSpec, ColumnType, RowEnvelope};
use ontology::DataType as OntDataType;
use ontology::Ontology;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// ClickHouse row envelope. Adds `traversal_path`, `_version`, `_deleted`
/// around the core node columns.
pub struct IndexerEnvelope {
    pub traversal_path: String,
    pub project_id: i64,
    pub branch: String,
    pub commit_sha: String,
    pub version_micros: i64,
}

impl IndexerEnvelope {
    pub fn new(
        traversal_path: String,
        project_id: i64,
        branch: String,
        commit_sha: String,
        indexed_at: DateTime<Utc>,
    ) -> Self {
        Self {
            traversal_path,
            project_id,
            branch,
            commit_sha,
            version_micros: indexed_at.timestamp_micros(),
        }
    }
}

impl RowEnvelope for IndexerEnvelope {
    fn write_header(&self, b: &mut BatchBuilder, id: i64) -> Result<(), ArrowError> {
        b.col("id")?.push_int(id)?;
        b.col("traversal_path")?.push_str(&self.traversal_path)?;
        b.col("project_id")?.push_int(self.project_id)?;
        b.col("branch")?.push_str(&self.branch)?;
        b.col("commit_sha")?.push_str(&self.commit_sha)?;
        b.col("_version")?
            .push_timestamp_micros(self.version_micros)?;
        b.col("_deleted")?.push_bool(false)?;
        Ok(())
    }

    fn header_specs(&self) -> Vec<ColumnSpec> {
        // Not used — specs come from the ontology. Kept for trait compliance.
        vec![]
    }
}

/// All Arrow batches produced from a `CodeGraph`, ready for ClickHouse.
pub struct ConvertedGraphData {
    pub branch: RecordBatch,
    pub directories: RecordBatch,
    pub files: RecordBatch,
    pub definitions: RecordBatch,
    pub imported_symbols: RecordBatch,
    pub edges: RecordBatch,
}

/// Convert a v2 `CodeGraph` to Arrow batches with ClickHouse envelope columns.
/// Column schemas are derived from the ontology.
pub fn convert_code_graph(
    graph: &code_graph::v2::linker::CodeGraph,
    envelope: &IndexerEnvelope,
    ontology: &Ontology,
) -> Result<ConvertedGraphData, ArrowError> {
    let ids = graph.assign_ids(envelope.project_id, &envelope.branch);

    Ok(ConvertedGraphData {
        branch: convert_branch(envelope)?,
        directories: convert_entity(graph, &ids, envelope, ontology, "Directory", |g, ids| {
            g.directories()
                .map(|(idx, dir)| DirectoryRow {
                    dir,
                    id: ids[idx.index()],
                })
                .collect()
        })?,
        files: convert_entity(graph, &ids, envelope, ontology, "File", |g, ids| {
            g.files()
                .map(|(idx, file)| FileRow {
                    file,
                    id: ids[idx.index()],
                })
                .collect()
        })?,
        definitions: convert_entity(graph, &ids, envelope, ontology, "Definition", |g, ids| {
            g.definitions()
                .map(|(idx, file_path, def)| DefinitionRow {
                    file_path,
                    def,
                    pool: &g.strings,
                    id: ids[idx.index()],
                })
                .collect()
        })?,
        imported_symbols: convert_entity(
            graph,
            &ids,
            envelope,
            ontology,
            "ImportedSymbol",
            |g, ids| {
                g.imports_iter()
                    .map(|(idx, file_path, import)| ImportRow {
                        file_path,
                        import,
                        pool: &g.strings,
                        id: ids[idx.index()],
                    })
                    .collect()
            },
        )?,
        edges: convert_edges(graph, &ids, envelope, ontology)?,
    })
}

/// Ontology-driven specs for a node entity, plus ClickHouse
/// infrastructure columns (_version, _deleted) that aren't in
/// the ontology but are required by the ReplacingMergeTree schema.
/// Fields with a small set of distinct values — stored as dictionary-encoded
/// Arrow arrays (integer indices + unique value table) instead of plain strings.
const DICT_ENCODED_FIELDS: &[&str] = &[
    "definition_type",
    "language",
    "relationship_kind",
    "source_node_kind",
    "target_node_kind",
    "import_type",
];

fn entity_specs(ontology: &Ontology, entity_name: &str) -> Vec<ColumnSpec> {
    let node = ontology
        .get_node(entity_name)
        .unwrap_or_else(|| panic!("entity '{entity_name}' not in ontology"));
    let mut specs: Vec<ColumnSpec> = node
        .fields
        .iter()
        .filter(|f| !f.is_virtual())
        .map(|f| ColumnSpec {
            name: f.name.clone(),
            col_type: match f.data_type {
                OntDataType::Int => ColumnType::Int,
                OntDataType::Bool => ColumnType::Bool,
                OntDataType::DateTime => ColumnType::TimestampMicros,
                _ if DICT_ENCODED_FIELDS.contains(&f.name.as_str()) => ColumnType::DictStr,
                _ => ColumnType::Str,
            },
            nullable: f.nullable,
        })
        .collect();
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

/// Ontology-driven specs for edges, plus infrastructure columns.
fn edge_specs(ontology: &Ontology) -> Vec<ColumnSpec> {
    let mut specs: Vec<ColumnSpec> = ontology
        .edge_columns()
        .iter()
        .map(|c| ColumnSpec {
            name: c.name.clone(),
            col_type: match c.data_type {
                OntDataType::Int => ColumnType::Int,
                OntDataType::Bool => ColumnType::Bool,
                OntDataType::DateTime => ColumnType::TimestampMicros,
                _ if DICT_ENCODED_FIELDS.contains(&c.name.as_str()) => ColumnType::DictStr,
                _ => ColumnType::Str,
            },
            nullable: false,
        })
        .collect();
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

/// Generic entity converter. Gets specs from the ontology, builds rows
/// via the provided closure, and produces a RecordBatch.
fn convert_entity<'a, R: AsRecordBatch<IndexerEnvelope>>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    ontology: &Ontology,
    entity_name: &str,
    build_rows: impl FnOnce(&'a code_graph::v2::linker::CodeGraph, &[i64]) -> Vec<R>,
) -> Result<RecordBatch, ArrowError> {
    let specs = entity_specs(ontology, entity_name);
    let rows = build_rows(graph, ids);
    R::to_record_batch(&rows, &specs, env)
}

fn convert_branch(env: &IndexerEnvelope) -> Result<RecordBatch, ArrowError> {
    let branch_id = compute_branch_id(env.project_id, &env.branch);
    // Branch has a fixed schema (not driven by row types).
    let specs = vec![
        ColumnSpec {
            name: "id".into(),
            col_type: ColumnType::Int,
            nullable: false,
        },
        ColumnSpec {
            name: "traversal_path".into(),
            col_type: ColumnType::Str,
            nullable: false,
        },
        ColumnSpec {
            name: "project_id".into(),
            col_type: ColumnType::Int,
            nullable: false,
        },
        ColumnSpec {
            name: "name".into(),
            col_type: ColumnType::Str,
            nullable: false,
        },
        ColumnSpec {
            name: "is_default".into(),
            col_type: ColumnType::Bool,
            nullable: false,
        },
        ColumnSpec {
            name: "_version".into(),
            col_type: ColumnType::TimestampMicros,
            nullable: false,
        },
        ColumnSpec {
            name: "_deleted".into(),
            col_type: ColumnType::Bool,
            nullable: false,
        },
    ];

    struct BranchRow<'a> {
        id: i64,
        env: &'a IndexerEnvelope,
    }
    impl AsRecordBatch for BranchRow<'_> {
        fn write_row(&self, b: &mut BatchBuilder, _ctx: &()) -> Result<(), ArrowError> {
            b.col("id")?.push_int(self.id)?;
            b.col("traversal_path")?
                .push_str(&self.env.traversal_path)?;
            b.col("project_id")?.push_int(self.env.project_id)?;
            b.col("name")?.push_str(&self.env.branch)?;
            b.col("is_default")?.push_bool(true)?;
            b.col("_version")?
                .push_timestamp_micros(self.env.version_micros)?;
            b.col("_deleted")?.push_bool(false)?;
            Ok(())
        }
    }

    BranchRow::to_record_batch(&[BranchRow { id: branch_id, env }], &specs, &())
}

fn convert_edges(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    ontology: &Ontology,
) -> Result<RecordBatch, ArrowError> {
    let specs = edge_specs(ontology);

    struct IndexerEdgeRow<'a> {
        env: &'a IndexerEnvelope,
        source_id: i64,
        target_id: i64,
        edge_kind: &'a str,
        source_node_kind: &'a str,
        target_node_kind: &'a str,
    }

    impl AsRecordBatch for IndexerEdgeRow<'_> {
        fn write_row(&self, b: &mut BatchBuilder, _ctx: &()) -> Result<(), ArrowError> {
            b.col("traversal_path")?
                .push_str(&self.env.traversal_path)?;
            b.col("source_id")?.push_int(self.source_id)?;
            b.col("source_kind")?.push_str(self.source_node_kind)?;
            b.col("relationship_kind")?.push_str(self.edge_kind)?;
            b.col("target_id")?.push_int(self.target_id)?;
            b.col("target_kind")?.push_str(self.target_node_kind)?;
            b.col("_version")?
                .push_timestamp_micros(self.env.version_micros)?;
            b.col("_deleted")?.push_bool(false)?;
            Ok(())
        }
    }

    let branch_id = compute_branch_id(env.project_id, &env.branch);

    let mut edge_rows: Vec<IndexerEdgeRow<'_>> = Vec::new();

    // Branch --IN_PROJECT--> Project
    edge_rows.push(IndexerEdgeRow {
        env,
        source_id: branch_id,
        target_id: env.project_id,
        edge_kind: "IN_PROJECT",
        source_node_kind: "Branch",
        target_node_kind: "Project",
    });

    // Branch --CONTAINS--> root-level directories and files
    for (idx, dir) in graph.directories() {
        if dir.path != "." && !dir.path.contains('/') {
            edge_rows.push(IndexerEdgeRow {
                env,
                source_id: branch_id,
                target_id: ids[idx.index()],
                edge_kind: "CONTAINS",
                source_node_kind: "Branch",
                target_node_kind: "Directory",
            });
        }
    }
    for (idx, file) in graph.files() {
        if !file.path.contains('/') {
            edge_rows.push(IndexerEdgeRow {
                env,
                source_id: branch_id,
                target_id: ids[idx.index()],
                edge_kind: "CONTAINS",
                source_node_kind: "Branch",
                target_node_kind: "File",
            });
        }
    }

    // Graph edges (CONTAINS, DEFINES, CALLS, etc.)
    for ei in graph.graph.edge_indices() {
        let (src, tgt) = graph.graph.edge_endpoints(ei).unwrap();
        let edge = &graph.graph[ei];
        edge_rows.push(IndexerEdgeRow {
            env,
            source_id: ids[src.index()],
            target_id: ids[tgt.index()],
            edge_kind: edge.relationship.edge_kind.as_ref(),
            source_node_kind: edge.relationship.source_node.as_ref(),
            target_node_kind: edge.relationship.target_node.as_ref(),
        });
    }

    // Sort edges by low-cardinality columns so run-length encoding
    // (and dictionary encoding) on relationship_kind, source_kind,
    // target_kind produce long runs of identical values.
    edge_rows.sort_by(|a, b| {
        a.edge_kind
            .cmp(b.edge_kind)
            .then_with(|| a.source_node_kind.cmp(b.source_node_kind))
            .then_with(|| a.target_node_kind.cmp(b.target_node_kind))
    });

    IndexerEdgeRow::to_record_batch(&edge_rows, &specs, &())
}

fn compute_branch_id(project_id: i64, branch: &str) -> i64 {
    let mut hasher = rustc_hash::FxHasher::default();
    project_id.hash(&mut hasher);
    branch.hash(&mut hasher);
    // Mask clears the sign bit so the result is always a positive i64.
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

/// `GraphConverter` for the ClickHouse indexer. Wraps `convert_code_graph`.
pub struct IndexerConverter {
    pub envelope: IndexerEnvelope,
    pub ontology: Arc<Ontology>,
    pub table_names: Arc<super::config::CodeTableNames>,
}

impl code_graph::v2::GraphConverter for IndexerConverter {
    fn convert(
        &self,
        graph: code_graph::v2::linker::CodeGraph,
    ) -> Result<Vec<(String, RecordBatch)>, code_graph::v2::SinkError> {
        let data = convert_code_graph(&graph, &self.envelope, &self.ontology)
            .map_err(|e| code_graph::v2::SinkError(format!("ClickHouse graph conversion: {e}")))?;
        let mut result = vec![
            (self.table_names.branch.clone(), data.branch),
            (self.table_names.directory.clone(), data.directories),
            (self.table_names.file.clone(), data.files),
            (self.table_names.definition.clone(), data.definitions),
            (
                self.table_names.imported_symbol.clone(),
                data.imported_symbols,
            ),
        ];

        // Route edges to ontology-resolved tables by relationship_kind.
        if data.edges.num_rows() > 0 {
            use arrow::array::AsArray;
            use std::collections::HashMap;

            let rel_col = data
                .edges
                .column_by_name("relationship_kind")
                .ok_or_else(|| {
                    code_graph::v2::SinkError("edges batch missing relationship_kind column".into())
                })?;
            // The column may be dictionary-encoded (DictStr) or plain Utf8.
            // Cast to StringArray for uniform access.
            let rel_col_str = arrow::compute::cast(rel_col, &arrow::datatypes::DataType::Utf8)
                .map_err(|e| {
                    code_graph::v2::SinkError(format!("cast relationship_kind to string: {e}"))
                })?;
            let rel_array = rel_col_str.as_string::<i32>();

            let mut table_rows: HashMap<&str, Vec<u32>> = HashMap::new();
            for i in 0..data.edges.num_rows() {
                let rel_kind = rel_array.value(i);
                let table = self.table_names.edge_table_for(rel_kind);
                table_rows.entry(table).or_default().push(i as u32);
            }

            for (table, indices) in table_rows {
                let idx_array = arrow::array::UInt32Array::from(indices);
                let batch = arrow::compute::take_record_batch(&data.edges, &idx_array)
                    .map_err(|e| code_graph::v2::SinkError(format!("edge routing: {e}")))?;
                result.push((table.to_string(), batch));
            }
        }

        Ok(result)
    }
}

/// `BatchSink` for ClickHouse. Bridges the sync `write_batch` trait
/// method to async ClickHouse writes via a tokio runtime handle.
pub struct ClickHouseSink {
    destination: Arc<dyn crate::destination::Destination>,
    runtime: tokio::runtime::Handle,
}

impl ClickHouseSink {
    pub fn new(
        destination: Arc<dyn crate::destination::Destination>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            destination,
            runtime,
        }
    }
}

impl code_graph::v2::BatchSink for ClickHouseSink {
    fn write_batch(
        &self,
        table: &str,
        batch: &RecordBatch,
    ) -> Result<(), code_graph::v2::SinkError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.runtime.block_on(async {
            let writer = self
                .destination
                .new_batch_writer(table)
                .await
                .map_err(|e| code_graph::v2::SinkError(format!("writer for {table}: {e}")))?;
            writer
                .write_batch(std::slice::from_ref(batch))
                .await
                .map_err(|e| code_graph::v2::SinkError(format!("write to {table}: {e}")))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_branch_id_is_always_non_negative() {
        // Project/branch pairs whose unmasked FxHash output has the
        // high bit set previously produced negative i64 ids.
        let cases = [
            (1_i64, "main"),
            (42, "feature/x"),
            (7, "release/2025-04"),
            (999, "renovate/deps-update"),
            (i64::MAX, "main"),
        ];
        for (project_id, branch) in cases {
            let id = compute_branch_id(project_id, branch);
            assert!(
                id >= 0,
                "compute_branch_id({project_id}, {branch:?}) returned {id}"
            );
        }
    }
}
