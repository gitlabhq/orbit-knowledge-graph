//! Convert a v2 `CodeGraph` directly to Arrow batches for ClickHouse.
//!
//! Uses the shared row types from code-graph with an `IndexerEnvelope`
//! that adds `traversal_path`, `_version`, and `_deleted` columns.

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use code_graph::v2::linker::graph::{DefinitionRow, DirectoryRow, FileRow, ImportRow};
use gkg_utils::arrow::{AsRecordBatch, BatchBuilder, ColumnSpec, ColumnType, RowEnvelope};
use std::hash::{Hash, Hasher};

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
                name: "branch".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
            ColumnSpec {
                name: "commit_sha".into(),
                col_type: ColumnType::Str,
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
pub fn convert_code_graph(
    graph: &code_graph::v2::linker::CodeGraph,
    envelope: &IndexerEnvelope,
) -> Result<ConvertedGraphData, ArrowError> {
    let ids = graph.assign_ids(envelope.project_id, &envelope.branch);

    Ok(ConvertedGraphData {
        branch: convert_branch(envelope)?,
        directories: convert_directories(graph, &ids, envelope)?,
        files: convert_files(graph, &ids, envelope)?,
        definitions: convert_definitions(graph, &ids, envelope)?,
        imported_symbols: convert_imported_symbols(graph, &ids, envelope)?,
        edges: convert_edges(graph, &ids, envelope)?,
    })
}

fn convert_branch(env: &IndexerEnvelope) -> Result<RecordBatch, ArrowError> {
    let branch_id = compute_branch_id(env.project_id, &env.branch);
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

fn convert_directories(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
) -> Result<RecordBatch, ArrowError> {
    let rows: Vec<_> = graph
        .directories()
        .map(|(idx, dir)| DirectoryRow {
            dir,
            id: ids[idx.index()],
        })
        .collect();
    DirectoryRow::to_batch(&rows, env)
}

fn convert_files(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
) -> Result<RecordBatch, ArrowError> {
    let rows: Vec<_> = graph
        .files()
        .map(|(idx, file)| FileRow {
            file,
            id: ids[idx.index()],
        })
        .collect();
    FileRow::to_batch(&rows, env)
}

fn convert_definitions(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
) -> Result<RecordBatch, ArrowError> {
    let rows: Vec<_> = graph
        .definitions()
        .map(|(idx, file_path, def)| DefinitionRow {
            file_path,
            def,
            pool: &graph.strings,
            id: ids[idx.index()],
        })
        .collect();
    DefinitionRow::to_batch(&rows, env)
}

fn convert_imported_symbols(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
) -> Result<RecordBatch, ArrowError> {
    let rows: Vec<_> = graph
        .imports_iter()
        .map(|(idx, file_path, import)| ImportRow {
            file_path,
            import,
            pool: &graph.strings,
            id: ids[idx.index()],
        })
        .collect();
    ImportRow::to_batch(&rows, env)
}

fn convert_edges(
    graph: &code_graph::v2::linker::CodeGraph,
    ids: &[i64],
    env: &IndexerEnvelope,
) -> Result<RecordBatch, ArrowError> {
    let specs = vec![
        ColumnSpec {
            name: "traversal_path".into(),
            col_type: ColumnType::Str,
            nullable: false,
        },
        ColumnSpec {
            name: "source_id".into(),
            col_type: ColumnType::Int,
            nullable: false,
        },
        ColumnSpec {
            name: "source_kind".into(),
            col_type: ColumnType::Str,
            nullable: false,
        },
        ColumnSpec {
            name: "relationship_kind".into(),
            col_type: ColumnType::Str,
            nullable: false,
        },
        ColumnSpec {
            name: "target_id".into(),
            col_type: ColumnType::Int,
            nullable: false,
        },
        ColumnSpec {
            name: "target_kind".into(),
            col_type: ColumnType::Str,
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

    let edge_rows: Vec<_> = graph
        .graph
        .edge_indices()
        .map(|ei| {
            let (src, tgt) = graph.graph.edge_endpoints(ei).unwrap();
            let edge = &graph.graph[ei];
            IndexerEdgeRow {
                env,
                source_id: ids[src.index()],
                target_id: ids[tgt.index()],
                edge_kind: edge.relationship.edge_kind.as_ref(),
                source_node_kind: edge.relationship.source_node.as_ref(),
                target_node_kind: edge.relationship.target_node.as_ref(),
            }
        })
        .collect();

    IndexerEdgeRow::to_record_batch(&edge_rows, &specs, &())
}

fn compute_branch_id(project_id: i64, branch: &str) -> i64 {
    let mut hasher = rustc_hash::FxHasher::default();
    project_id.hash(&mut hasher);
    branch.hash(&mut hasher);
    hasher.finish() as i64
}
