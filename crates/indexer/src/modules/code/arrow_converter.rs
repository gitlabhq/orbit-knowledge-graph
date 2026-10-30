use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use code_graph::v2::SinkError;
use code_graph::v2::linker::graph::{DirectoryRow, EdgeProperties, FileRow, GraphOutput};
use ontology::DataType as OntDataType;
use ontology::Ontology;
use ontology::constants::{RELATIONSHIP_KIND_COLUMN, SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN};
use orbit_arrow::{AsRecordBatch, BatchBuilder, ColumnSpec, ColumnType, RowEnvelope};
use orbit_utils::traversal_path::TraversalPath;
use std::collections::{HashMap, HashSet};
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
    let unwritten = graph.unwritten_nodes();
    // The tag cache is the last reader of the definition and import payloads, so
    // from here the edge build needs only endpoints, edge weights and the cache.
    graph.release_node_payloads();
    let edges = convert_semantic_edges(graph, ids, envelope, specs, &tag_cache, &unwritten)?;
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
        OntDataType::StringList => ColumnType::StrList,
        OntDataType::IntList => ColumnType::IntList,
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
                        nullable: config.storage.columns.iter().any(|column| {
                            column.name == c.name && column.ch_type.contains("Nullable(")
                        }),
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
        g.definition_rows(ids).collect()
    })
}

fn convert_imports(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
    specs: &[ColumnSpec],
) -> Result<RecordBatch, ArrowError> {
    convert_entity(graph, ids, env, specs, |g, ids| {
        g.import_rows(ids).collect()
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
        b.col("commit_sha")?.push_str(&self.env.commit_sha)?;
        b.col("indexed_at")?
            .push_timestamp_micros(self.env.version_micros)?;
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
        properties: EdgeProperties::default(),
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
        properties: EdgeProperties::default(),
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

    let mut builder = BatchBuilder::new(&specs.edge, edge_rows.len() + graph.graph.edge_count())?;
    for row in &edge_rows {
        row.write_row(&mut builder, &())?;
    }
    let unwritten = graph.unwritten_nodes();
    for ei in graph.graph.edge_indices() {
        if endpoint_unwritten(graph, &unwritten, ei) {
            continue;
        }
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
    unwritten: &rustc_hash::FxHashSet<petgraph::graph::NodeIndex>,
) -> Result<RecordBatch, ArrowError> {
    let mut builder = BatchBuilder::new(&specs.edge, graph.graph.edge_count())?;
    for ei in graph.graph.edge_indices() {
        if graph.graph[ei].relationship.edge_kind.as_ref() == "CONTAINS"
            || endpoint_unwritten(graph, unwritten, ei)
        {
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
    pub(super) properties: EdgeProperties,
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
        self.properties.write(b)
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
            properties: EdgeProperties::default(),
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
            properties: EdgeProperties::default(),
        })
        .collect()
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
        properties: graph.edge_properties(&graph.graph[ei]),
    }
}

fn endpoint_unwritten(
    graph: &code_graph::v2::linker::CodeGraph,
    unwritten: &rustc_hash::FxHashSet<petgraph::graph::NodeIndex>,
    ei: petgraph::graph::EdgeIndex,
) -> bool {
    !unwritten.is_empty()
        && graph
            .graph
            .edge_endpoints(ei)
            .is_some_and(|(s, t)| unwritten.contains(&s) || unwritten.contains(&t))
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
            imported_symbol: entity_specs(ontology, "Import"),
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
        result.extend(route_edges(data.edges, table_names)?);
    }

    Ok(result)
}

pub(super) fn route_edges(
    edges: RecordBatch,
    table_names: &super::config::CodeTableNames,
) -> Result<Vec<(String, RecordBatch)>, SinkError> {
    let routed = match EdgeRouting::resolve(&edges, table_names)? {
        EdgeRouting::Single(table) => vec![(table, edges)],
        EdgeRouting::Split(table_rows) => table_rows
            .into_iter()
            .map(|(table, indices)| {
                let idx_array = arrow::array::UInt32Array::from(indices);
                arrow::compute::take_record_batch(&edges, &idx_array)
                    .map(|batch| (table, batch))
                    .map_err(|e| SinkError(format!("edge routing: {e}")))
            })
            .collect::<Result<Vec<_>, SinkError>>()?,
    };
    routed
        .into_iter()
        .map(|(table, batch)| {
            let columns = table_names
                .edge_columns(table)
                .ok_or_else(|| SinkError(format!("edge table {table} has no column list")))?;
            let schema = batch.schema();
            let extra: Vec<&str> = schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .filter(|name| !columns.contains(*name))
                .collect();
            Ok((table.to_string(), drop_columns(&batch, &extra)))
        })
        .collect()
}

enum EdgeRouting<'a> {
    Single(&'a str),
    Split(HashMap<&'a str, Vec<u32>>),
}

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
        let mut routing = None;
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
            match &mut routing {
                None => routing = Some(Self::Single(table)),
                Some(Self::Single(first)) if *first != table => {
                    let first = *first;
                    let row = u32::try_from(row)
                        .map_err(|_| SinkError("edge batch exceeds u32 row indexing".into()))?;
                    let mut rows = HashMap::new();
                    rows.insert(first, (0..row).collect());
                    rows.insert(table, vec![row]);
                    routing = Some(Self::Split(rows));
                }
                Some(Self::Split(rows)) => {
                    let row = u32::try_from(row)
                        .map_err(|_| SinkError("edge batch exceeds u32 row indexing".into()))?;
                    rows.entry(table).or_default().push(row);
                }
                Some(Self::Single(_)) => {}
            }
        }
        routing.ok_or_else(|| SinkError("cannot route an empty edge batch".into()))
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
    fn declared_edge_properties_reach_the_code_edge_batch_with_their_defaults() {
        use arrow::array::{Array, Int64Array, ListArray};
        use code_graph::v2::linker::graph::EdgeValue;
        let ontology = Ontology::load_embedded_with_overlay(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/seeds/overlays/code_edge_properties"
        ))
        .unwrap();
        let specs = edge_specs(&ontology);
        let env = IndexerEnvelope::new(
            TraversalPath::new_unchecked("1/42/"),
            42,
            "main".to_string(),
            "abc".to_string(),
            Utc::now(),
        );
        let mut properties = EdgeProperties::default();
        properties.insert("lines", EdgeValue::Ints(vec![3, 7]));
        let mut builder = BatchBuilder::new(&specs, 1).unwrap();
        IndexerEdgeRow {
            env: &env,
            source_id: 1,
            target_id: 2,
            edge_kind: "CALLS",
            source_node_kind: "Definition",
            target_node_kind: "Definition",
            source_tags: &[],
            target_tags: &[],
            properties,
        }
        .write_row(&mut builder, &())
        .unwrap();
        let batch = builder.finish().unwrap();
        let weight = batch
            .column_by_name("weight")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("overlay property column");
        assert_eq!(weight.value(0), 0);
        let lines = batch
            .column_by_name("lines")
            .and_then(|c| c.as_any().downcast_ref::<ListArray>())
            .expect("lines column");
        let first = lines.value(0);
        let first = first.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(first.values(), &[3, 7]);
        let resolution = batch
            .column_by_name("resolution")
            .expect("resolution column");
        assert!(!resolution.is_null(0));
        assert_eq!(
            arrow::util::display::array_value_to_string(resolution.as_ref(), 0).unwrap(),
            ""
        );
    }

    #[test]
    fn edge_specs_carry_declared_edge_properties() {
        let ontology = Ontology::load_embedded_with_overlay(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/seeds/overlays/code_edge_properties"
        ))
        .unwrap();
        let specs = edge_specs(&ontology);
        let weight = specs
            .iter()
            .find(|spec| spec.name == "weight")
            .expect("declared property column");
        assert!(!weight.nullable);
    }

    #[test]
    fn code_edge_batches_carry_only_the_columns_of_their_destination_table() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("edges")).unwrap();
        std::fs::write(
            dir.path().join("edges/has_note.yaml"),
            "properties:\n  created_at:\n    type: datetime\n    source: created_at\nvariants:\n  - from_node: { type: File, id: id }\n    to_node: { type: Note, id: id }\n    description: \"File has note (test overlay).\"\n",
        )
        .unwrap();
        let ontology = Ontology::load_embedded_with_overlay(dir.path()).unwrap();
        let specs = edge_specs(&ontology);
        assert!(specs.iter().any(|spec| spec.name == "created_at"));
        let table_names = super::super::config::CodeTableNames::from_ontology(&ontology).unwrap();
        let code_table = table_names
            .edge_table_for("CONTAINS", "Project", "Branch")
            .unwrap()
            .to_string();
        let env = IndexerEnvelope::new(
            TraversalPath::new_unchecked("1/42/"),
            42,
            "main".to_string(),
            "abc".to_string(),
            Utc::now(),
        );
        let mut builder = BatchBuilder::new(&specs, 2).unwrap();
        for (source_id, target_id, kind, source_kind, target_kind) in [
            (42, 7, "CONTAINS", "Project", "Branch"),
            (8, 9, "HAS_NOTE", "File", "Note"),
        ] {
            IndexerEdgeRow {
                env: &env,
                source_id,
                target_id,
                edge_kind: kind,
                source_node_kind: source_kind,
                target_node_kind: target_kind,
                source_tags: &[],
                target_tags: &[],
                properties: EdgeProperties::default(),
            }
            .write_row(&mut builder, &())
            .unwrap();
        }
        let routed = route_edges(builder.finish().unwrap(), &table_names).unwrap();
        assert_eq!(routed.len(), 2);
        for (table, batch) in &routed {
            assert_eq!(batch.num_rows(), 1, "{table}");
            let schema = batch.schema();
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            if *table == code_table {
                assert!(names.contains(&"project_id"), "{table}: {names:?}");
                assert!(!names.contains(&"created_at"), "{table}: {names:?}");
            } else {
                assert!(!names.contains(&"project_id"), "{table}: {names:?}");
                assert!(!names.contains(&"branch"), "{table}: {names:?}");
                assert!(
                    !batch.column_by_name("created_at").unwrap().is_null(0),
                    "{table}"
                );
            }
        }
    }

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
