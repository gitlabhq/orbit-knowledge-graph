use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use code_graph::v2::SinkError;
use code_graph::v2::linker::graph::{DefinitionRow, DirectoryRow, FileRow, GraphOutput, ImportRow};
use ontology::DataType as OntDataType;
use ontology::Ontology;
use orbit_utils::arrow::{AsRecordBatch, BatchBuilder, ColumnSpec, ColumnType, RowEnvelope};
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
    graph: &code_graph::v2::linker::CodeGraph,
    envelope: &IndexerEnvelope,
    specs: &ConverterSpecs,
) -> Result<ConvertedGraphData, ArrowError> {
    let ids = graph.assign_ids(envelope.project_id, &envelope.branch);
    if code_graph::v2::memprobe::enabled() {
        tracing::debug!(
            target: "codegraph_mem",
            stage = "convert_entity",
            entity = "node_ids",
            rows = ids.len(),
            bytes_arrow = ids.capacity() * std::mem::size_of::<i64>(),
            "entity converted"
        );
    }
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
    probe("directories", &directories);
    let files = convert_files(graph, ids, envelope, &specs.file)?;
    probe("files", &files);
    let definitions = convert_definitions(graph, ids, envelope, &specs.definition)?;
    probe("definitions", &definitions);
    let imported_symbols = convert_imports(graph, ids, envelope, &specs.imported_symbol)?;
    probe("imported_symbols", &imported_symbols);
    let edges = convert_repository_edges(graph, ids, envelope, specs)?;
    probe("edges", &edges);
    Ok(ConvertedGraphData {
        branch,
        directories,
        files,
        definitions,
        imported_symbols,
        edges,
    })
}

/// One event per entity as its `RecordBatch` lands, so the profiler's tracing
/// layer can stamp the heap reading at each step of the conversion ramp. Shares
/// the `codegraph_mem` target with the `code-graph` probes so a run's structure
/// timeline is one stream.
fn probe(entity: &str, batch: &dyn ProbeSized) {
    if !code_graph::v2::memprobe::enabled() {
        return;
    }
    tracing::debug!(
        target: "codegraph_mem",
        stage = "convert_entity",
        entity,
        rows = batch.rows(),
        bytes_arrow = batch.arrow_bytes(),
        "entity converted"
    );
    batch.probe_columns(entity);
}

/// Per-column Arrow bytes, so a wide batch can be attributed to the columns
/// that actually cost something rather than to the batch as a whole.
fn probe_batch_columns(entity: &str, batch: &RecordBatch) {
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        tracing::debug!(
            target: "codegraph_mem",
            stage = "convert_column",
            entity,
            column = field.name().as_str(),
            arrow_type = %field.data_type(),
            bytes_arrow = arrow::array::Array::get_array_memory_size(column.as_ref()),
            "column converted"
        );
    }
}

trait ProbeSized {
    fn rows(&self) -> usize;
    fn arrow_bytes(&self) -> usize;
    fn probe_columns(&self, _entity: &str) {}
}

impl ProbeSized for RecordBatch {
    fn rows(&self) -> usize {
        self.num_rows()
    }
    fn arrow_bytes(&self) -> usize {
        self.get_array_memory_size()
    }
    fn probe_columns(&self, entity: &str) {
        probe_batch_columns(entity, self);
    }
}

impl ProbeSized for arrow::array::ArrayRef {
    fn rows(&self) -> usize {
        arrow::array::Array::len(self.as_ref())
    }
    fn arrow_bytes(&self) -> usize {
        arrow::array::Array::get_array_memory_size(self.as_ref())
    }
}

fn convert_semantic_graph(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    envelope: &IndexerEnvelope,
    specs: &ConverterSpecs,
) -> Result<ConvertedGraphData, ArrowError> {
    let definitions = convert_definitions(graph, ids, envelope, &specs.definition)?;
    probe("definitions", &definitions);
    let imported_symbols = convert_imports(graph, ids, envelope, &specs.imported_symbol)?;
    probe("imported_symbols", &imported_symbols);
    let edges = convert_semantic_edges(graph, ids, envelope, specs)?;
    probe("edges", &edges);
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

/// Columns the [`IndexerEnvelope`] fills with the same value for every row in a
/// batch. Their ClickHouse type is unchanged; encoding them as a dictionary on
/// the wire replaces one copy of the string per row with one `i32` key, which on
/// a multi-million-row edge batch is the difference between tens of MiB and a
/// few. Whether the indexer writes a constant is an indexer-side fact, not a
/// graph-shape fact, so it does not belong in the ontology.
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

fn edge_specs(ontology: &Ontology) -> Vec<ColumnSpec> {
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
                    // Denormalized tag lists repeat a handful of values across
                    // every edge row, and on a large repository the two of them
                    // are over half the edge batch. The destination column stays
                    // `Array(String)`; only the wire encoding changes.
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

fn convert_branch_row(
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
    log_tag_cache("edges_by_table", &tag_cache);
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
) -> Result<RecordBatch, ArrowError> {
    let tag_cache = graph.build_node_tags(&specs.tag_properties);
    log_tag_cache("edges", &tag_cache);
    let mut builder = BatchBuilder::new(&specs.edge, graph.graph.edge_count())?;
    for ei in graph.graph.edge_indices() {
        if graph.graph[ei].relationship.edge_kind.as_ref() == "CONTAINS" {
            continue;
        }
        graph_edge_row(graph, ids, env, &tag_cache, ei).write_row(&mut builder, &())?;
    }
    builder.finish()
}

struct IndexerEdgeRow<'a> {
    env: &'a IndexerEnvelope,
    source_id: i64,
    target_id: i64,
    edge_kind: &'a str,
    source_node_kind: &'a str,
    target_node_kind: &'a str,
    source_tags: &'a [String],
    target_tags: &'a [String],
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
    tag_cache: &'a [Vec<String>],
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
            target_tags: &tag_cache[idx.index()],
        })
        .collect()
}

fn repository_on_branch_rows<'a>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &'a [i64],
    env: &'a IndexerEnvelope,
    branch_id: i64,
    branch_tags: &'a [String],
    tag_cache: &'a [Vec<String>],
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
        source_tags: &tag_cache[idx.index()],
        target_tags: branch_tags,
    }));

    rows
}

/// The tag cache lives for the whole of an edge batch build, which is where the
/// heap peaks, and nothing else reports it.
fn log_tag_cache(stage: &str, tag_cache: &[Vec<String>]) {
    if !code_graph::v2::memprobe::enabled() {
        return;
    }
    let bytes: usize = tag_cache
        .iter()
        .map(|tags| {
            tags.capacity() * size_of::<String>() + tags.iter().map(String::capacity).sum::<usize>()
        })
        .sum::<usize>()
        + std::mem::size_of_val(tag_cache);
    tracing::debug!(
        target: code_graph::v2::memprobe::TARGET,
        stage = "tag_cache",
        entity = stage,
        rows = tag_cache.len(),
        bytes_total = bytes,
        "tag cache"
    );
}

fn graph_edge_row<'a>(
    graph: &'a code_graph::v2::linker::CodeGraph,
    ids: &'a [i64],
    env: &'a IndexerEnvelope,
    tag_cache: &'a [Vec<String>],
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
        source_tags: &tag_cache[src.index()],
        target_tags: &tag_cache[tgt.index()],
    }
}

fn compute_branch_id(project_id: i64, branch: &str) -> i64 {
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

fn build_tag_properties(ontology: &Ontology) -> TagProperties {
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
        graph: code_graph::v2::linker::CodeGraph,
    ) -> Result<Vec<(String, RecordBatch)>, SinkError> {
        let data = convert_code_graph(&graph, &self.envelope, &self.specs)
            .map_err(|e| SinkError(format!("ClickHouse graph conversion: {e}")))?;
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

        if data.edges.num_rows() > 0 {
            use std::collections::HashMap;

            // Cloning an `ArrayRef` is an `Arc` bump, and it detaches the
            // routing borrow from `data.edges` so the single-table path can
            // move the batch out instead of copying it.
            let rel_col = data
                .edges
                .column_by_name("relationship_kind")
                .ok_or_else(|| SinkError("edges batch missing relationship_kind column".into()))?
                .clone();
            let routing = EdgeRouting::resolve(&rel_col, &self.table_names)?;

            // Columns that only exist on gl_code_edge. Sub-batches going
            // to other edge tables (gl_edge) must have them stripped.
            let code_only_cols: &[&str] = &["project_id", "branch"];

            // One destination means the batch already is that table's batch.
            // `take` would copy all of it while the original is still live, and
            // on a code repository every semantic edge kind routes to the same
            // table, so that copy set the whole run's peak.
            if let Some(table) = routing.single_table() {
                let batch = if table.contains("code_edge") {
                    data.edges
                } else {
                    drop_columns(&data.edges, code_only_cols)
                };
                probe(&format!("edge_single:{table}"), &batch);
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
                probe(&format!("edge_take:{table}"), &batch);
                if !table.contains("code_edge") {
                    batch = drop_columns(&batch, code_only_cols);
                }
                result.push((table.to_string(), batch));
            }
        }

        Ok(result)
    }
}

/// Destination table per edge row, resolved without materialising a string per
/// row. `relationship_kind` is `LowCardinality(String)` in every edge table, so
/// the batch carries a dictionary of a handful of kinds; mapping the dictionary
/// values once and indexing by key avoids both the Utf8 cast of the whole column
/// and the per-row hash lookup.
enum EdgeRouting<'a> {
    Dictionary {
        keys: &'a [i32],
        by_key: Vec<&'a str>,
    },
    /// The column was not dictionary-encoded after all.
    PerRow(Vec<&'a str>),
}

impl<'a> EdgeRouting<'a> {
    fn resolve(
        rel_col: &'a arrow::array::ArrayRef,
        table_names: &'a super::config::CodeTableNames,
    ) -> Result<Self, SinkError> {
        use arrow::array::{Array, AsArray};
        use arrow::datatypes::Int32Type;

        // The dictionary path reads the raw key buffer, whose contents at a null
        // position are arbitrary and would index `by_key` out of bounds.
        // `relationship_kind` is non-nullable in every edge table, so this is a
        // guard against a future schema change rather than a case to handle.
        if rel_col.null_count() > 0 {
            return Err(SinkError(
                "relationship_kind contains nulls, which edge routing cannot resolve".into(),
            ));
        }

        if let Some(dict) = rel_col.as_dictionary_opt::<Int32Type>() {
            let values = dict.values().as_string_opt::<i32>().ok_or_else(|| {
                SinkError("relationship_kind dictionary values are not strings".into())
            })?;
            let by_key = (0..values.len())
                .map(|i| table_names.edge_table_for(values.value(i)))
                .collect();
            return Ok(Self::Dictionary {
                keys: dict.keys().values(),
                by_key,
            });
        }

        let plain = rel_col.as_string_opt::<i32>().ok_or_else(|| {
            SinkError("relationship_kind is neither a dictionary nor a string column".into())
        })?;
        Ok(Self::PerRow(
            (0..plain.len())
                .map(|i| table_names.edge_table_for(plain.value(i)))
                .collect(),
        ))
    }

    /// `Some` when every row routes to the same table.
    fn single_table(&self) -> Option<&'a str> {
        let mut tables = match self {
            Self::Dictionary { keys, by_key } => {
                // A dictionary can carry values no row references, so the
                // distinct set has to come from the keys actually present.
                let mut seen: Vec<&str> = Vec::new();
                for &k in *keys {
                    let table = by_key[k as usize];
                    if !seen.contains(&table) {
                        seen.push(table);
                    }
                }
                seen
            }
            Self::PerRow(tables) => {
                let mut seen: Vec<&str> = Vec::new();
                for &table in tables {
                    if !seen.contains(&table) {
                        seen.push(table);
                    }
                }
                seen
            }
        };
        (tables.len() == 1).then(|| tables.pop().expect("length checked"))
    }

    fn tables_by_row(&self) -> Box<dyn Iterator<Item = &'a str> + '_> {
        match self {
            Self::Dictionary { keys, by_key } => {
                Box::new(keys.iter().map(move |&k| by_key[k as usize]))
            }
            Self::PerRow(tables) => Box::new(tables.iter().copied()),
        }
    }
}

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
