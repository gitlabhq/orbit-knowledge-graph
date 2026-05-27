//! Convert a v2 `CodeGraph` directly to Arrow batches for ClickHouse.
//!
//! Uses the shared row types from code-graph with an `IndexerEnvelope`
//! that adds `traversal_path`, `_version`, and `_deleted` columns.
//! Column schemas are driven by the ontology — the source of truth
//! for what columns each entity table has.

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use code_graph::v2::linker::graph::{DefinitionRow, DirectoryRow, FileRow, GraphOutput, ImportRow};
use gkg_utils::arrow::{AsRecordBatch, BatchBuilder, ColumnSpec, ColumnType, RowEnvelope};
use ontology::DataType as OntDataType;
use ontology::Ontology;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
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
    specs: &ConverterSpecs,
) -> Result<ConvertedGraphData, ArrowError> {
    let ids = graph.assign_ids(envelope.project_id, &envelope.branch);
    match graph.output {
        GraphOutput::Complete => convert_repository_graph(graph, &ids, envelope, specs),
        GraphOutput::ParsedOnly => convert_semantic_graph(graph, &ids, envelope, specs),
    }
}

fn convert_repository_graph(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    envelope: &IndexerEnvelope,
    specs: &ConverterSpecs,
) -> Result<ConvertedGraphData, ArrowError> {
    Ok(ConvertedGraphData {
        branch: convert_branch_row(envelope)?,
        directories: convert_directories(graph, ids, envelope, &specs.directory)?,
        files: convert_files(graph, ids, envelope, &specs.file)?,
        definitions: convert_definitions(graph, ids, envelope, &specs.definition)?,
        imported_symbols: convert_imports(graph, ids, envelope, &specs.imported_symbol)?,
        edges: convert_repository_edges(graph, ids, envelope, specs)?,
    })
}

fn convert_semantic_graph(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    envelope: &IndexerEnvelope,
    specs: &ConverterSpecs,
) -> Result<ConvertedGraphData, ArrowError> {
    Ok(ConvertedGraphData {
        branch: convert_empty_branch()?,
        directories: convert_empty_directories(envelope, &specs.directory)?,
        files: convert_empty_files(envelope, &specs.file)?,
        definitions: convert_definitions(graph, ids, envelope, &specs.definition)?,
        imported_symbols: convert_imports(graph, ids, envelope, &specs.imported_symbol)?,
        edges: convert_semantic_edges(graph, ids, envelope, specs)?,
    })
}

/// Collect LowCardinality column names from ClickHouse storage metadata.
fn low_cardinality_columns(storage_columns: &[ontology::StorageColumn]) -> HashSet<String> {
    storage_columns
        .iter()
        .filter(|col| col.ch_type.starts_with("LowCardinality"))
        .map(|col| col.name.clone())
        .collect()
}

fn entity_specs(ontology: &Ontology, entity_name: &str) -> Vec<ColumnSpec> {
    let node = ontology
        .get_node(entity_name)
        .unwrap_or_else(|| panic!("entity '{entity_name}' not in ontology"));
    let dict_fields = low_cardinality_columns(&node.storage.columns);
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
                _ if dict_fields.contains(&f.name) => ColumnType::DictStr,
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
    let dict_fields: HashSet<String> = ontology
        .edge_tables()
        .iter()
        .filter_map(|t| ontology.edge_table_config(t))
        .flat_map(|c| &c.storage.columns)
        .filter(|col| col.ch_type.starts_with("LowCardinality"))
        .map(|col| col.name.clone())
        .collect();

    let mut specs: Vec<ColumnSpec> = ontology
        .edge_columns()
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

    let mut seen = std::collections::HashSet::new();
    for table_name in ontology.edge_tables() {
        if let Some(config) = ontology.edge_table_config(table_name) {
            for col in &config.storage.denormalized_columns {
                if seen.insert(col.name.clone()) {
                    specs.push(ColumnSpec {
                        name: col.name.clone(),
                        col_type: ColumnType::StrList,
                        nullable: false,
                    });
                }
            }
        }
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

/// Generic entity converter. Uses precomputed specs, builds rows
/// via the provided closure, and produces a RecordBatch.
fn convert_entity<'a, R: AsRecordBatch<IndexerEnvelope>>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    specs: &[ColumnSpec],
    build_rows: impl FnOnce(&'a code_graph::v2::linker::CodeGraph, &[i64]) -> Vec<R>,
) -> Result<RecordBatch, ArrowError> {
    let rows = build_rows(graph, ids);
    R::to_record_batch(&rows, specs, env)
}

fn convert_empty_entity<R: AsRecordBatch<IndexerEnvelope>>(
    env: &IndexerEnvelope,
    specs: &[ColumnSpec],
) -> Result<RecordBatch, ArrowError> {
    let rows: Vec<R> = Vec::new();
    R::to_record_batch(&rows, specs, env)
}

fn convert_directories(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    specs: &[ColumnSpec],
) -> Result<RecordBatch, ArrowError> {
    convert_entity(graph, ids, env, specs, |g, ids| {
        g.directories()
            .map(|(idx, dir)| DirectoryRow {
                dir,
                id: ids[idx.index()],
            })
            .collect()
    })
}

fn convert_empty_directories(
    env: &IndexerEnvelope,
    specs: &[ColumnSpec],
) -> Result<RecordBatch, ArrowError> {
    convert_empty_entity::<DirectoryRow<'_>>(env, specs)
}

fn convert_files(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    specs: &[ColumnSpec],
) -> Result<RecordBatch, ArrowError> {
    convert_entity(graph, ids, env, specs, |g, ids| {
        g.files()
            .map(|(idx, file)| FileRow {
                file,
                id: ids[idx.index()],
            })
            .collect()
    })
}

fn convert_empty_files(
    env: &IndexerEnvelope,
    specs: &[ColumnSpec],
) -> Result<RecordBatch, ArrowError> {
    convert_empty_entity::<FileRow<'_>>(env, specs)
}

fn convert_definitions(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    specs: &[ColumnSpec],
) -> Result<RecordBatch, ArrowError> {
    convert_entity(graph, ids, env, specs, |g, ids| {
        g.definitions()
            .map(|(idx, file_path, def)| DefinitionRow {
                file_path,
                def,
                pool: &g.strings,
                id: ids[idx.index()],
            })
            .collect()
    })
}

fn convert_imports(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    specs: &[ColumnSpec],
) -> Result<RecordBatch, ArrowError> {
    convert_entity(graph, ids, env, specs, |g, ids| {
        g.imports_iter()
            .map(|(idx, file_path, import)| ImportRow {
                file_path,
                import,
                pool: &g.strings,
                id: ids[idx.index()],
            })
            .collect()
    })
}

fn branch_specs() -> Vec<ColumnSpec> {
    vec![
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
    ]
}

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

fn convert_branch_row(env: &IndexerEnvelope) -> Result<RecordBatch, ArrowError> {
    let branch_id = compute_branch_id(env.project_id, &env.branch);
    BranchRow::to_record_batch(&[BranchRow { id: branch_id, env }], &branch_specs(), &())
}

fn convert_empty_branch() -> Result<RecordBatch, ArrowError> {
    let rows: Vec<BranchRow<'_>> = Vec::new();
    BranchRow::to_record_batch(&rows, &branch_specs(), &())
}

fn convert_repository_edges(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    specs: &ConverterSpecs,
) -> Result<RecordBatch, ArrowError> {
    let denorm_cols = &specs.denormalized_edge_columns;
    let branch_id = compute_branch_id(env.project_id, &env.branch);
    let mut edge_rows: Vec<IndexerEdgeRow<'_>> = Vec::new();

    edge_rows.push(IndexerEdgeRow {
        env,
        source_id: branch_id,
        target_id: env.project_id,
        edge_kind: "IN_PROJECT",
        source_node_kind: "Branch",
        target_node_kind: "Project",
        denormalized_column_names: denorm_cols,
    });

    edge_rows.push(IndexerEdgeRow {
        env,
        source_id: env.project_id,
        target_id: branch_id,
        edge_kind: "CONTAINS",
        source_node_kind: "Project",
        target_node_kind: "Branch",
        denormalized_column_names: denorm_cols,
    });

    edge_rows.extend(branch_contains_directory_rows(
        graph,
        ids,
        env,
        branch_id,
        denorm_cols,
    ));
    edge_rows.extend(branch_contains_file_rows(
        graph,
        ids,
        env,
        branch_id,
        denorm_cols,
    ));
    edge_rows.extend(repository_on_branch_rows(
        graph,
        ids,
        env,
        branch_id,
        denorm_cols,
    ));
    edge_rows.extend(graph_edge_rows(graph, ids, env, denorm_cols));

    edge_row_batch(edge_rows, &specs.edge)
}

fn convert_semantic_edges(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    specs: &ConverterSpecs,
) -> Result<RecordBatch, ArrowError> {
    let denorm_cols = &specs.denormalized_edge_columns;
    let edge_rows: Vec<_> = graph_edge_rows(graph, ids, env, denorm_cols)
        .into_iter()
        .filter(|row| row.edge_kind != "CONTAINS")
        .collect();

    edge_row_batch(edge_rows, &specs.edge)
}

fn denormalized_edge_columns(ontology: &Ontology) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut cols = Vec::new();
    for table_name in ontology.edge_tables() {
        if let Some(config) = ontology.edge_table_config(table_name) {
            for col in &config.storage.denormalized_columns {
                if seen.insert(col.name.clone()) {
                    cols.push(col.name.clone());
                }
            }
        }
    }
    cols
}

struct IndexerEdgeRow<'a> {
    env: &'a IndexerEnvelope,
    source_id: i64,
    target_id: i64,
    edge_kind: &'a str,
    source_node_kind: &'a str,
    target_node_kind: &'a str,
    denormalized_column_names: &'a [String],
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
        for col_name in self.denormalized_column_names {
            b.col(col_name)?.push_empty_str_list()?;
        }
        b.col("_version")?
            .push_timestamp_micros(self.env.version_micros)?;
        b.col("_deleted")?.push_bool(false)?;
        Ok(())
    }
}

fn branch_contains_directory_rows<'a>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &'a [i64],
    env: &'a IndexerEnvelope,
    branch_id: i64,
    denorm_cols: &'a [String],
) -> Vec<IndexerEdgeRow<'a>> {
    graph
        .directories()
        .filter(|(_, dir)| dir.path != "." && !dir.path.contains('/'))
        .map(|(idx, _)| IndexerEdgeRow {
            env,
            source_id: branch_id,
            target_id: ids[idx.index()],
            edge_kind: "CONTAINS",
            source_node_kind: "Branch",
            target_node_kind: "Directory",
            denormalized_column_names: denorm_cols,
        })
        .collect()
}

fn branch_contains_file_rows<'a>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &'a [i64],
    env: &'a IndexerEnvelope,
    branch_id: i64,
    denorm_cols: &'a [String],
) -> Vec<IndexerEdgeRow<'a>> {
    graph
        .files()
        .filter(|(_, file)| !file.path.contains('/'))
        .map(|(idx, _)| IndexerEdgeRow {
            env,
            source_id: branch_id,
            target_id: ids[idx.index()],
            edge_kind: "CONTAINS",
            source_node_kind: "Branch",
            target_node_kind: "File",
            denormalized_column_names: denorm_cols,
        })
        .collect()
}

fn repository_on_branch_rows<'a>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &'a [i64],
    env: &'a IndexerEnvelope,
    branch_id: i64,
    denorm_cols: &'a [String],
) -> Vec<IndexerEdgeRow<'a>> {
    let mut rows = Vec::new();

    rows.extend(graph.directories().map(|(idx, _)| IndexerEdgeRow {
        env,
        source_id: ids[idx.index()],
        target_id: branch_id,
        edge_kind: "ON_BRANCH",
        source_node_kind: "Directory",
        target_node_kind: "Branch",
        denormalized_column_names: denorm_cols,
    }));
    rows.extend(graph.files().map(|(idx, _)| IndexerEdgeRow {
        env,
        source_id: ids[idx.index()],
        target_id: branch_id,
        edge_kind: "ON_BRANCH",
        source_node_kind: "File",
        target_node_kind: "Branch",
        denormalized_column_names: denorm_cols,
    }));

    rows
}

fn graph_edge_rows<'a>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &'a [i64],
    env: &'a IndexerEnvelope,
    denorm_cols: &'a [String],
) -> Vec<IndexerEdgeRow<'a>> {
    let mut rows = Vec::new();
    for ei in graph.graph.edge_indices() {
        let (src, tgt) = graph.graph.edge_endpoints(ei).unwrap();
        let edge = &graph.graph[ei];
        rows.push(IndexerEdgeRow {
            env,
            source_id: ids[src.index()],
            target_id: ids[tgt.index()],
            edge_kind: edge.relationship.edge_kind.as_ref(),
            source_node_kind: edge.relationship.source_node.as_ref(),
            target_node_kind: edge.relationship.target_node.as_ref(),
            denormalized_column_names: denorm_cols,
        });
    }
    rows
}

fn edge_row_batch(
    mut edge_rows: Vec<IndexerEdgeRow<'_>>,
    specs: &[ColumnSpec],
) -> Result<RecordBatch, ArrowError> {
    // Sort edges by low-cardinality columns so run-length encoding
    // (and dictionary encoding) on relationship_kind, source_kind,
    // target_kind produce long runs of identical values.
    edge_rows.sort_by(|a, b| {
        a.edge_kind
            .cmp(b.edge_kind)
            .then_with(|| a.source_node_kind.cmp(b.source_node_kind))
            .then_with(|| a.target_node_kind.cmp(b.target_node_kind))
    });

    IndexerEdgeRow::to_record_batch(&edge_rows, specs, &())
}

fn compute_branch_id(project_id: i64, branch: &str) -> i64 {
    let mut hasher = rustc_hash::FxHasher::default();
    project_id.hash(&mut hasher);
    branch.hash(&mut hasher);
    // Mask clears the sign bit so the result is always a positive i64.
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

pub struct ConverterSpecs {
    directory: Arc<Vec<ColumnSpec>>,
    file: Arc<Vec<ColumnSpec>>,
    definition: Arc<Vec<ColumnSpec>>,
    imported_symbol: Arc<Vec<ColumnSpec>>,
    edge: Arc<Vec<ColumnSpec>>,
    denormalized_edge_columns: Arc<[String]>,
}

impl ConverterSpecs {
    pub fn from_ontology(ontology: &Ontology) -> Self {
        Self {
            directory: Arc::new(entity_specs(ontology, "Directory")),
            file: Arc::new(entity_specs(ontology, "File")),
            definition: Arc::new(entity_specs(ontology, "Definition")),
            imported_symbol: Arc::new(entity_specs(ontology, "ImportedSymbol")),
            edge: Arc::new(edge_specs(ontology)),
            denormalized_edge_columns: denormalized_edge_columns(ontology).into(),
        }
    }
}

/// `GraphConverter` for the ClickHouse indexer. Wraps `convert_code_graph`.
pub struct IndexerConverter {
    pub envelope: IndexerEnvelope,
    pub table_names: Arc<super::config::CodeTableNames>,
    specs: ConverterSpecs,
}

impl IndexerConverter {
    pub fn new(
        envelope: IndexerEnvelope,
        ontology: &Ontology,
        table_names: Arc<super::config::CodeTableNames>,
    ) -> Self {
        Self {
            envelope,
            table_names,
            specs: ConverterSpecs::from_ontology(ontology),
        }
    }
}

impl code_graph::v2::GraphConverter for IndexerConverter {
    fn convert(
        &self,
        graph: code_graph::v2::linker::CodeGraph,
    ) -> Result<Vec<(String, RecordBatch)>, code_graph::v2::SinkError> {
        let data = convert_code_graph(&graph, &self.envelope, &self.specs)
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
            // edge_row_batch sorts edges by edge_kind, so adjacent rows share
            // rel_kind: cache the last (rel_kind, table) to skip the lookup.
            let mut last: Option<(&str, &str)> = None;
            for i in 0..data.edges.num_rows() {
                let rel_kind = rel_array.value(i);
                let table = match last {
                    Some((prev_kind, prev_table)) if prev_kind == rel_kind => prev_table,
                    _ => {
                        let t = self.table_names.edge_table_for(rel_kind);
                        last = Some((rel_kind, t));
                        t
                    }
                };
                table_rows.entry(table).or_default().push(i as u32);
            }

            if table_rows.len() == 1 {
                let table = *table_rows.keys().next().unwrap();
                if table == self.table_names.default_edge_table() {
                    result.push((table.to_string(), data.edges));
                    return Ok(result);
                }
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

/// `BatchSink` for ClickHouse that buffers all batches per table in memory
/// during pipeline execution, then flushes all tables in parallel via
/// [`flush()`](BufferedClickHouseSink::flush).
///
/// This avoids sequential HTTP round-trips during the CPU-bound pipeline
/// phase and instead issues one concurrent write per distinct table at the
/// end. Benchmarking shows a ~3x wall-time improvement over the previous
/// streaming approach.
pub struct BufferedClickHouseSink {
    destination: Arc<dyn crate::destination::Destination>,
    buffers: RwLock<HashMap<String, Vec<RecordBatch>>>,
}

impl BufferedClickHouseSink {
    pub fn new(destination: Arc<dyn crate::destination::Destination>) -> Self {
        Self {
            destination,
            buffers: RwLock::new(HashMap::new()),
        }
    }

    /// Flush all buffered tables to ClickHouse in parallel. Each table
    /// gets a single `insert_arrow_streaming` call with all its batches,
    /// and all tables are written concurrently.
    pub async fn flush(&self) -> Result<(), code_graph::v2::SinkError> {
        let buffers = std::mem::take(&mut *self.buffers.write());
        let mut handles = Vec::new();

        for (table, batches) in buffers {
            if batches.is_empty() {
                continue;
            }
            let dest = self.destination.clone();
            handles.push(tokio::spawn(async move {
                let writer = dest
                    .new_batch_writer(&table)
                    .await
                    .map_err(|e| code_graph::v2::SinkError(format!("writer for {table}: {e}")))?;
                writer
                    .write_batch(&batches)
                    .await
                    .map_err(|e| code_graph::v2::SinkError(format!("write to {table}: {e}")))?;
                Ok::<(), code_graph::v2::SinkError>(())
            }));
        }

        let results = futures::future::join_all(handles).await;
        for result in results {
            result.map_err(|e| code_graph::v2::SinkError(format!("flush join: {e}")))??;
        }
        Ok(())
    }
}

impl code_graph::v2::BatchSink for BufferedClickHouseSink {
    fn write_batch(
        &self,
        table: &str,
        batch: &RecordBatch,
    ) -> Result<(), code_graph::v2::SinkError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.buffers
            .write()
            .entry(table.to_string())
            .or_default()
            .push(batch.clone());
        Ok(())
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
