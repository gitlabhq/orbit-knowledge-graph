use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use code_graph::v2::SinkError;
use code_graph::v2::linker::graph::{DefinitionRow, DirectoryRow, FileRow, GraphOutput, ImportRow};
use ontology::DataType as OntDataType;
use ontology::Ontology;
use ontology::constants::{RELATIONSHIP_KIND_COLUMN, SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN};
use orbit_arrow::{AsRecordBatch, BatchBuilder, ColumnSpec, ColumnType, RowEnvelope};
use orbit_utils::traversal_path::TraversalPath;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub struct IndexerEnvelope {
    pub traversal_path: TraversalPath,
    pub project_id: i64,
    pub branch: String,
    pub commit_sha: String,
    pub version_micros: i64,
}

impl IndexerEnvelope {
    pub fn new(
        traversal_path: TraversalPath,
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
        b.col("traversal_path")?
            .push_str(self.traversal_path.as_str())?;
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

pub struct ConvertedGraphData {
    pub branch: RecordBatch,
    pub directories: RecordBatch,
    pub files: RecordBatch,
    pub definitions: RecordBatch,
    pub imported_symbols: RecordBatch,
    pub edges: RecordBatch,
}

pub fn convert_code_graph(
    graph: &mut code_graph::v2::linker::CodeGraph,
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
    let branch = convert_branch_row(envelope, &specs.branch)?;
    let directories = convert_directories(graph, ids, envelope, &specs.directory)?;
    let files = convert_files(graph, ids, envelope, &specs.file)?;
    let definitions = convert_definitions(graph, ids, envelope, &specs.definition)?;
    let imported_symbols = convert_imports(graph, ids, envelope, &specs.imported_symbol)?;
    let edges = convert_repository_edges(graph, ids, envelope, specs)?;
    Ok(ConvertedGraphData {
        branch,
        directories,
        files,
        definitions,
        imported_symbols,
        edges,
    })
}

fn convert_semantic_graph(
    graph: &mut code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    envelope: &IndexerEnvelope,
    specs: &ConverterSpecs,
) -> Result<ConvertedGraphData, ArrowError> {
    let definitions = convert_definitions(graph, ids, envelope, &specs.definition)?;
    let imported_symbols = convert_imports(graph, ids, envelope, &specs.imported_symbol)?;
    let tag_cache = graph.build_node_tags(&specs.tag_properties);
    // The tag cache is the last reader of the definition and import payloads, so
    // from here the edge build needs only endpoints, edge weights and the cache.
    graph.release_node_payloads();
    let edges = convert_semantic_edges(graph, ids, envelope, specs, &tag_cache)?;
    Ok(ConvertedGraphData {
        branch: convert_empty_branch(&specs.branch)?,
        directories: convert_empty_directories(envelope, &specs.directory)?,
        files: convert_empty_files(envelope, &specs.file)?,
        definitions,
        imported_symbols,
        edges,
    })
}

fn low_cardinality_columns(storage_columns: &[ontology::StorageColumn]) -> HashSet<String> {
    storage_columns
        .iter()
        .filter(|col| col.ch_type.starts_with("LowCardinality"))
        .map(|col| col.name.clone())
        .collect()
}

#[expect(
    clippy::panic,
    reason = "callers pass either a built-in code-graph entity name or a name taken from this same ontology, so the lookup cannot miss"
)]
pub(super) fn entity_specs(ontology: &Ontology, entity_name: &str) -> Vec<ColumnSpec> {
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
            col_type: column_type_for(&f.name, f.data_type, &dict_fields),
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

/// Constant for every row in a batch, so the wire encoding collapses each to one
/// key. That is an indexer-side fact, not graph shape, so not in the ontology.
const ENVELOPE_CONSTANT_COLUMNS: [&str; 3] = ["traversal_path", "branch", "commit_sha"];

fn column_type_for(
    name: &str,
    data_type: OntDataType,
    dict_fields: &HashSet<String>,
) -> ColumnType {
    match data_type {
        OntDataType::Int => ColumnType::Int,
        OntDataType::Bool => ColumnType::Bool,
        OntDataType::DateTime => ColumnType::TimestampMicros,
        _ if dict_fields.contains(name) || ENVELOPE_CONSTANT_COLUMNS.contains(&name) => {
            ColumnType::DictStr
        }
        _ => ColumnType::Str,
    }
}

pub(super) fn edge_specs(ontology: &Ontology) -> Vec<ColumnSpec> {
    let dict_fields: HashSet<String> = ontology
        .edge_tables()
        .iter()
        .filter_map(|t| ontology.edge_table_config(t))
        .flat_map(|c| &c.storage.columns)
        .filter(|col| col.ch_type.starts_with("LowCardinality"))
        .map(|col| col.name.clone())
        .collect();

    // Build the union of logical columns across ALL edge tables so the
    // batch can hold columns from tables with extra fields (gl_code_edge
    // has project_id + branch that gl_edge does not).
    let mut seen_cols = std::collections::HashSet::new();
    let mut specs: Vec<ColumnSpec> = Vec::new();
    for table_name in ontology.edge_tables() {
        if let Some(config) = ontology.edge_table_config(table_name) {
            for c in &config.columns {
                if seen_cols.insert(c.name.clone()) {
                    specs.push(ColumnSpec {
                        name: c.name.clone(),
                        col_type: column_type_for(&c.name, c.data_type, &dict_fields),
                        nullable: false,
                    });
                }
            }
        }
    }

    let mut seen = std::collections::HashSet::new();
    for table_name in ontology.edge_tables() {
        if let Some(config) = ontology.edge_table_config(table_name) {
            for col in &config.storage.denormalized_columns {
                if seen.insert(col.name.clone()) {
                    // A handful of tag values repeat across every edge row. Only
                    // the wire encoding changes; the column stays `Array(String)`.
                    specs.push(ColumnSpec {
                        name: col.name.clone(),
                        col_type: ColumnType::DictStrList,
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

pub(super) fn convert_branch_row(
    env: &IndexerEnvelope,
    specs: &[ColumnSpec],
) -> Result<RecordBatch, ArrowError> {
    let branch_id = compute_branch_id(env.project_id, &env.branch);
    BranchRow::to_record_batch(&[BranchRow { id: branch_id, env }], specs, &())
}

fn convert_empty_branch(specs: &[ColumnSpec]) -> Result<RecordBatch, ArrowError> {
    let rows: Vec<BranchRow<'_>> = Vec::new();
    BranchRow::to_record_batch(&rows, specs, &())
}

fn convert_repository_edges(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    specs: &ConverterSpecs,
) -> Result<RecordBatch, ArrowError> {
    let branch_id = compute_branch_id(env.project_id, &env.branch);
    let tag_cache = graph.build_node_tags(&specs.tag_properties);
    let branch_tags: Vec<String> = specs
        .tag_properties
        .get("Branch")
        .map(|props| {
            props
                .iter()
                .map(|(tag_key, _)| format!("{tag_key}:true"))
                .collect()
        })
        .unwrap_or_default();
    let mut edge_rows: Vec<IndexerEdgeRow<'_>> = Vec::new();

    edge_rows.push(IndexerEdgeRow {
        env,
        source_id: branch_id,
        target_id: env.project_id,
        edge_kind: "IN_PROJECT",
        source_node_kind: "Branch",
        target_node_kind: "Project",
        source_tags: &branch_tags,
        target_tags: &[],
    });

    edge_rows.push(IndexerEdgeRow {
        env,
        source_id: env.project_id,
        target_id: branch_id,
        edge_kind: "CONTAINS",
        source_node_kind: "Project",
        target_node_kind: "Branch",
        source_tags: &[],
        target_tags: &branch_tags,
    });

    edge_rows.extend(branch_contains_directory_rows(
        graph,
        ids,
        env,
        branch_id,
        &branch_tags,
    ));
    edge_rows.extend(branch_contains_file_rows(
        graph,
        ids,
        env,
        branch_id,
        &branch_tags,
        &tag_cache,
    ));
    edge_rows.extend(repository_on_branch_rows(
        graph,
        ids,
        env,
        branch_id,
        &branch_tags,
        &tag_cache,
    ));

    let mut builder = BatchBuilder::new(&specs.edge, edge_rows.len() + graph.graph.edge_count())?;
    for row in &edge_rows {
        row.write_row(&mut builder, &())?;
    }
    for ei in graph.graph.edge_indices() {
        graph_edge_row(graph, ids, env, &tag_cache, ei).write_row(&mut builder, &())?;
    }
    builder.finish()
}

fn convert_semantic_edges(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    specs: &ConverterSpecs,
    tag_cache: &code_graph::v2::linker::graph::NodeTags,
) -> Result<RecordBatch, ArrowError> {
    let mut builder = BatchBuilder::new(&specs.edge, graph.graph.edge_count())?;
    for ei in graph.graph.edge_indices() {
        if graph.graph[ei].relationship.edge_kind.as_ref() == "CONTAINS" {
            continue;
        }
        graph_edge_row(graph, ids, env, tag_cache, ei).write_row(&mut builder, &())?;
    }
    builder.finish()
}

pub(super) struct IndexerEdgeRow<'a> {
    pub(super) env: &'a IndexerEnvelope,
    pub(super) source_id: i64,
    pub(super) target_id: i64,
    pub(super) edge_kind: &'a str,
    pub(super) source_node_kind: &'a str,
    pub(super) target_node_kind: &'a str,
    pub(super) source_tags: &'a [String],
    pub(super) target_tags: &'a [String],
}

impl AsRecordBatch for IndexerEdgeRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, _ctx: &()) -> Result<(), ArrowError> {
        b.col("traversal_path")?
            .push_str(&self.env.traversal_path)?;
        b.col("project_id")?.push_int(self.env.project_id)?;
        b.col("branch")?.push_str(&self.env.branch)?;
        b.col("source_id")?.push_int(self.source_id)?;
        b.col("source_kind")?.push_str(self.source_node_kind)?;
        b.col("relationship_kind")?.push_str(self.edge_kind)?;
        b.col("target_id")?.push_int(self.target_id)?;
        b.col("target_kind")?.push_str(self.target_node_kind)?;
        let src: Vec<&str> = self.source_tags.iter().map(|s| s.as_str()).collect();
        b.col("source_tags")?.push_str_list(&src)?;
        let tgt: Vec<&str> = self.target_tags.iter().map(|s| s.as_str()).collect();
        b.col("target_tags")?.push_str_list(&tgt)?;
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
    branch_tags: &'a [String],
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
            source_tags: branch_tags,
            target_tags: &[],
        })
        .collect()
}

fn branch_contains_file_rows<'a>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &'a [i64],
    env: &'a IndexerEnvelope,
    branch_id: i64,
    branch_tags: &'a [String],
    tag_cache: &'a code_graph::v2::linker::graph::NodeTags,
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
            source_tags: branch_tags,
            target_tags: tag_cache.get(idx),
        })
        .collect()
}

fn repository_on_branch_rows<'a>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &'a [i64],
    env: &'a IndexerEnvelope,
    branch_id: i64,
    branch_tags: &'a [String],
    tag_cache: &'a code_graph::v2::linker::graph::NodeTags,
) -> Vec<IndexerEdgeRow<'a>> {
    let mut rows = Vec::new();

    rows.extend(graph.directories().map(|(idx, _)| IndexerEdgeRow {
        env,
        source_id: ids[idx.index()],
        target_id: branch_id,
        edge_kind: "ON_BRANCH",
        source_node_kind: "Directory",
        target_node_kind: "Branch",
        source_tags: &[],
        target_tags: branch_tags,
    }));
    rows.extend(graph.files().map(|(idx, _)| IndexerEdgeRow {
        env,
        source_id: ids[idx.index()],
        target_id: branch_id,
        edge_kind: "ON_BRANCH",
        source_node_kind: "File",
        target_node_kind: "Branch",
        source_tags: tag_cache.get(idx),
        target_tags: branch_tags,
    }));

    rows
}

#[expect(
    clippy::unwrap_used,
    reason = "edge indices come from iterating this same graph, so endpoints exist"
)]
fn graph_edge_row<'a>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &'a [i64],
    env: &'a IndexerEnvelope,
    tag_cache: &'a code_graph::v2::linker::graph::NodeTags,
    ei: petgraph::graph::EdgeIndex,
) -> IndexerEdgeRow<'a> {
    let (src, tgt) = graph.graph.edge_endpoints(ei).unwrap();
    let rel = &graph.graph[ei].relationship;
    IndexerEdgeRow {
        env,
        source_id: ids[src.index()],
        target_id: ids[tgt.index()],
        edge_kind: rel.edge_kind.as_ref(),
        source_node_kind: rel.source_node.as_ref(),
        target_node_kind: rel.target_node.as_ref(),
        source_tags: tag_cache.get(src),
        target_tags: tag_cache.get(tgt),
    }
}

pub(super) fn compute_branch_id(project_id: i64, branch: &str) -> i64 {
    let mut hasher = rustc_hash::FxHasher::default();
    project_id.hash(&mut hasher);
    branch.hash(&mut hasher);
    // Mask clears the sign bit so the result is always a positive i64.
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

/// Per-node-kind list of `(tag_key, property_name)` pairs derived from
/// the ontology's denormalization declarations. Deduplicated because the
/// ontology expands one declaration per edge relationship, but the tag
/// values are the same regardless of which edge the node appears in.
type TagProperties = std::collections::HashMap<String, Vec<(String, String)>>;

pub(super) fn build_tag_properties(ontology: &Ontology) -> TagProperties {
    let mut map: TagProperties = std::collections::HashMap::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for dp in ontology.denormalized_properties() {
        let key = (dp.node_kind.clone(), dp.property_name.clone());
        if seen.insert(key) {
            map.entry(dp.node_kind.clone())
                .or_default()
                .push((dp.tag_key.clone(), dp.property_name.clone()));
        }
    }
    map
}

pub struct ConverterSpecs {
    branch: Vec<ColumnSpec>,
    directory: Vec<ColumnSpec>,
    file: Vec<ColumnSpec>,
    definition: Vec<ColumnSpec>,
    imported_symbol: Vec<ColumnSpec>,
    edge: Vec<ColumnSpec>,
    tag_properties: TagProperties,
}

impl ConverterSpecs {
    pub fn from_ontology(ontology: &Ontology) -> Self {
        Self {
            branch: entity_specs(ontology, "Branch"),
            directory: entity_specs(ontology, "Directory"),
            file: entity_specs(ontology, "File"),
            definition: entity_specs(ontology, "Definition"),
            imported_symbol: entity_specs(ontology, "ImportedSymbol"),
            edge: edge_specs(ontology),
            tag_properties: build_tag_properties(ontology),
        }
    }
}

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
        mut graph: code_graph::v2::linker::CodeGraph,
    ) -> Result<Vec<(String, RecordBatch)>, SinkError> {
        let data = convert_code_graph(&mut graph, &self.envelope, &self.specs)
            .map_err(|e| SinkError(format!("ClickHouse graph conversion: {e}")))?;
        route_graph(data, &self.table_names)
    }
}

type RouteKey = (usize, usize, usize);

pub(super) fn route_graph(
    data: ConvertedGraphData,
    table_names: &super::config::CodeTableNames,
) -> Result<Vec<(String, RecordBatch)>, SinkError> {
    let mut result = vec![
        (table_names.branch.clone(), data.branch),
        (table_names.directory.clone(), data.directories),
        (table_names.file.clone(), data.files),
        (table_names.definition.clone(), data.definitions),
        (table_names.imported_symbol.clone(), data.imported_symbols),
    ];

    if data.edges.num_rows() > 0 {
        use std::collections::HashMap;

        let routing = EdgeRouting::resolve(&data.edges, table_names)?;

        // Columns that only exist on gl_code_edge. Sub-batches going
        // to other edge tables (gl_edge) must have them stripped.
        let code_only_cols: &[&str] = &["project_id", "branch"];

        // Every semantic edge kind usually routes to one table, and `take`
        // would copy the whole batch while the original is still live.
        if let Some(table) = routing.single_table() {
            let batch = if table.contains("code_edge") {
                data.edges
            } else {
                drop_columns(&data.edges, code_only_cols)
            };
            result.push((table.to_string(), batch));
            return Ok(result);
        }

        let mut table_rows: HashMap<&str, Vec<u32>> = HashMap::new();
        for (i, table) in routing.tables_by_row().enumerate() {
            table_rows.entry(table).or_default().push(i as u32);
        }

        for (table, indices) in table_rows {
            let idx_array = arrow::array::UInt32Array::from(indices);
            let mut batch = arrow::compute::take_record_batch(&data.edges, &idx_array)
                .map_err(|e| SinkError(format!("edge routing: {e}")))?;
            if !table.contains("code_edge") {
                batch = drop_columns(&batch, code_only_cols);
            }
            result.push((table.to_string(), batch));
        }
    }

    Ok(result)
}

struct EdgeRouting<'a>(Vec<&'a str>);

type DictionaryStrings<'a> = (&'a [i32], &'a arrow::array::StringArray);

impl<'a> EdgeRouting<'a> {
    fn resolve(
        edges: &RecordBatch,
        table_names: &'a super::config::CodeTableNames,
    ) -> Result<Self, SinkError> {
        use arrow::array::{Array, AsArray};
        use arrow::datatypes::Int32Type;

        let strings = [
            RELATIONSHIP_KIND_COLUMN,
            SOURCE_KIND_COLUMN,
            TARGET_KIND_COLUMN,
        ]
        .map(|name| {
            let column = edges
                .column_by_name(name)
                .ok_or_else(|| SinkError(format!("edges batch missing {name} column")))?;
            if column.null_count() > 0 {
                return Err(SinkError(format!(
                    "{name} contains nulls, which edge routing cannot resolve"
                )));
            }
            let dictionary = column
                .as_dictionary_opt::<Int32Type>()
                .ok_or_else(|| SinkError(format!("{name} is not dictionary encoded")))?;
            let values = dictionary
                .values()
                .as_string_opt::<i32>()
                .ok_or_else(|| SinkError(format!("{name} dictionary values are not strings")))?;
            Ok((dictionary.keys().values().as_ref(), values))
        })
        .into_iter()
        .collect::<Result<Vec<_>, SinkError>>()?;

        let [relationship, source, target] = strings.as_slice() else {
            return Err(SinkError("edge route columns did not resolve".into()));
        };
        let mut routes: rustc_hash::FxHashMap<RouteKey, &'a str> = rustc_hash::FxHashMap::default();
        routes.reserve(relationship.1.len().min(edges.num_rows()));
        let mut tables = Vec::with_capacity(edges.num_rows());
        for row in 0..edges.num_rows() {
            let relationship_key = dictionary_key(relationship, row, "relationship_kind")?;
            let source_key = dictionary_key(source, row, "source_kind")?;
            let target_key = dictionary_key(target, row, "target_kind")?;
            let route_key = (relationship_key, source_key, target_key);
            let table = match routes.get(&route_key) {
                Some(table) => *table,
                None => {
                    let table = table_names
                        .edge_table_for(
                            relationship.1.value(relationship_key),
                            source.1.value(source_key),
                            target.1.value(target_key),
                        )
                        .map_err(|error| SinkError(error.to_string()))?;
                    routes.insert(route_key, table);
                    table
                }
            };
            tables.push(table);
        }
        Ok(Self(tables))
    }

    /// `Some` when every row routes to the same table.
    #[expect(clippy::expect_used, reason = "length is checked to be 1 just before")]
    fn single_table(&self) -> Option<&'a str> {
        let mut tables: Vec<&str> = Vec::new();
        for &table in &self.0 {
            if !tables.contains(&table) {
                tables.push(table);
            }
        }
        (tables.len() == 1).then(|| tables.pop().expect("length checked"))
    }

    fn tables_by_row(&self) -> Box<dyn Iterator<Item = &'a str> + '_> {
        Box::new(self.0.iter().copied())
    }
}

fn dictionary_key(
    dictionary: &DictionaryStrings<'_>,
    row: usize,
    column: &str,
) -> Result<usize, SinkError> {
    let key = usize::try_from(dictionary.0[row])
        .map_err(|_| SinkError(format!("{column} contains a negative dictionary key")))?;
    if key >= dictionary.1.offsets().len() - 1 {
        return Err(SinkError(format!(
            "{column} dictionary key {key} is out of bounds"
        )));
    }
    Ok(key)
}

#[expect(
    clippy::expect_used,
    reason = "indices are derived from the batch's own schema, so projection is in bounds"
)]
fn drop_columns(batch: &RecordBatch, drop: &[&str]) -> RecordBatch {
    let schema = batch.schema();
    let mut indices: Vec<usize> = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        if !drop.contains(&field.name().as_str()) {
            indices.push(i);
        }
    }
    batch.project(&indices).expect("column projection")
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
