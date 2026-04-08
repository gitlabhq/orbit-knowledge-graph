use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use code_graph::linker::analysis::types::GraphData;

use crate::error::Result;

pub struct LocalGraphData {
    pub directories: RecordBatch,
    pub files: RecordBatch,
    pub definitions: RecordBatch,
    pub imported_symbols: RecordBatch,
    pub edges: RecordBatch,
}

/// Build a node RecordBatch with the standard envelope columns
/// (id, project_id, branch, ..., _version). The `columns!` macro
/// declares extra columns between `branch` and `_version`.
macro_rules! node_batch {
    ($nodes:expr, $project_id:expr, $branch:expr, |$n:ident| { $($col:tt)* }) => {{
        let nodes = $nodes;
        let cap = nodes.len();
        let branch = $branch;
        let mut _id = Int64Builder::with_capacity(cap);
        let mut _pid = Int64Builder::with_capacity(cap);
        let mut _br = StringBuilder::with_capacity(cap, cap * branch.len());
        let mut _ver = Int64Builder::with_capacity(cap);

        node_batch!(@builders cap, $($col)*);

        for $n in nodes {
            let Some(id) = $n.id else { continue };
            _id.append_value(id);
            _pid.append_value($project_id);
            _br.append_value(branch);
            _ver.append_value(0);
            node_batch!(@fill $n, $($col)*);
        }

        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
            $( node_batch!(@field $col), )*
            Field::new("_version", DataType::Int64, false),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(_id.finish()),
            Arc::new(_pid.finish()),
            Arc::new(_br.finish()),
            $( node_batch!(@array $col), )*
            Arc::new(_ver.finish()),
        ];
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(crate::error::DuckDbError::from)
    }};

    // Declare a builder for each column.
    (@builders $cap:ident, $( ($kind:ident $name:ident $($rest:tt)*) )*) => {
        $( node_batch!(@builder $cap, $kind $name); )*
    };
    (@builder $cap:ident, str $name:ident)     => { let mut $name = StringBuilder::with_capacity($cap, $cap * 32); };
    (@builder $cap:ident, int $name:ident)     => { let mut $name = Int64Builder::with_capacity($cap); };
    (@builder $cap:ident, opt_str $name:ident) => { let mut $name = StringBuilder::with_capacity($cap, $cap * 32); };

    // Fill builders for one node.
    (@fill $n:ident, $( ($kind:ident $name:ident => $expr:expr) )*) => {
        $( node_batch!(@fill_one $kind, $name, $expr); )*
    };
    (@fill_one str, $name:ident, $expr:expr)     => { $name.append_value($expr); };
    (@fill_one int, $name:ident, $expr:expr)     => { $name.append_value($expr); };
    (@fill_one opt_str, $name:ident, $expr:expr) => { match $expr { Some(v) => $name.append_value(v), None => $name.append_null() }; };

    // Schema field for each column.
    (@field (str $name:ident => $e:expr))     => { Field::new(stringify!($name), DataType::Utf8, false) };
    (@field (int $name:ident => $e:expr))     => { Field::new(stringify!($name), DataType::Int64, false) };
    (@field (opt_str $name:ident => $e:expr)) => { Field::new(stringify!($name), DataType::Utf8, true) };

    // Finished array for each column.
    (@array (str $name:ident => $e:expr))     => { Arc::new($name.finish()) as ArrayRef };
    (@array (int $name:ident => $e:expr))     => { Arc::new($name.finish()) as ArrayRef };
    (@array (opt_str $name:ident => $e:expr)) => { Arc::new($name.finish()) as ArrayRef };
}

#[allow(clippy::vec_init_then_push)]
pub fn convert_graph_data(
    graph_data: &GraphData,
    project_id: i64,
    branch: &str,
) -> Result<LocalGraphData> {
    Ok(LocalGraphData {
        directories: node_batch!(&graph_data.directory_nodes, project_id, branch, |n| {
            (str path => &n.path)
            (str name => &n.name)
        })?,
        files: node_batch!(&graph_data.file_nodes, project_id, branch, |n| {
            (str path => &n.path)
            (str name => &n.name)
            (str extension => &n.extension)
            (str language => &n.language)
        })?,
        definitions: node_batch!(&graph_data.definition_nodes, project_id, branch, |n| {
            (str file_path => n.file_path.as_ref())
            (str fqn => n.fqn.to_string())
            (str name => n.fqn.name())
            (str definition_type => n.definition_type.as_str())
            (int start_line => n.range.start.line as i64)
            (int end_line => n.range.end.line as i64)
            (int start_byte => n.range.byte_offset.0 as i64)
            (int end_byte => n.range.byte_offset.1 as i64)
        })?,
        imported_symbols: node_batch!(&graph_data.imported_symbol_nodes, project_id, branch, |n| {
            (str file_path => &n.location.file_path)
            (str import_type => n.import_type.as_str())
            (str import_path => &n.import_path)
            (opt_str identifier_name => n.identifier.as_ref().map(|i| &i.name))
            (opt_str identifier_alias => n.identifier.as_ref().and_then(|i| i.alias.as_ref()))
            (int start_line => n.location.start_line as i64)
            (int end_line => n.location.end_line as i64)
            (int start_byte => n.location.start_byte)
            (int end_byte => n.location.end_byte)
        })?,
        edges: convert_edges(graph_data)?,
    })
}

fn convert_edges(graph_data: &GraphData) -> Result<RecordBatch> {
    let rels = &graph_data.relationships;
    let cap = rels.len();
    let mut source_id = Int64Builder::with_capacity(cap);
    let mut source_kind = StringBuilder::with_capacity(cap, cap * 16);
    let mut rel_kind = StringBuilder::with_capacity(cap, cap * 16);
    let mut target_id = Int64Builder::with_capacity(cap);
    let mut target_kind = StringBuilder::with_capacity(cap, cap * 16);
    let mut ver = Int64Builder::with_capacity(cap);

    for rel in rels {
        let (src_kind_str, tgt_kind_str) = rel.kind.source_target_kinds();
        let src_id = lookup_node_id(graph_data, src_kind_str, rel.source_id);
        let tgt_id = lookup_node_id(graph_data, tgt_kind_str, rel.target_id);

        let (Some(s), Some(t)) = (src_id, tgt_id) else {
            continue;
        };

        source_id.append_value(s);
        source_kind.append_value(src_kind_str);
        rel_kind.append_value(rel.relationship_type.edge_kind());
        target_id.append_value(t);
        target_kind.append_value(tgt_kind_str);
        ver.append_value(0);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("source_id", DataType::Int64, false),
            Field::new("source_kind", DataType::Utf8, false),
            Field::new("relationship_kind", DataType::Utf8, false),
            Field::new("target_id", DataType::Int64, false),
            Field::new("target_kind", DataType::Utf8, false),
            Field::new("_version", DataType::Int64, false),
        ])),
        vec![
            Arc::new(source_id.finish()) as ArrayRef,
            Arc::new(source_kind.finish()),
            Arc::new(rel_kind.finish()),
            Arc::new(target_id.finish()),
            Arc::new(target_kind.finish()),
            Arc::new(ver.finish()),
        ],
    )?)
}

fn lookup_node_id(graph_data: &GraphData, kind: &str, index: Option<u32>) -> Option<i64> {
    let index = index? as usize;
    match kind {
        "Directory" => graph_data.directory_nodes.get(index).and_then(|n| n.id),
        "File" => graph_data.file_nodes.get(index).and_then(|n| n.id),
        "Definition" => graph_data.definition_nodes.get(index).and_then(|n| n.id),
        "ImportedSymbol" => graph_data
            .imported_symbol_nodes
            .get(index)
            .and_then(|n| n.id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph::linker::analysis::types::{DirectoryNode, FileNode};

    #[test]
    fn empty_graph_produces_zero_row_batches() {
        let graph = GraphData {
            directory_nodes: vec![],
            file_nodes: vec![],
            definition_nodes: vec![],
            imported_symbol_nodes: vec![],
            relationships: vec![],
        };

        let result = convert_graph_data(&graph, 1, "main").unwrap();
        assert_eq!(result.directories.num_rows(), 0);
        assert_eq!(result.files.num_rows(), 0);
        assert_eq!(result.definitions.num_rows(), 0);
        assert_eq!(result.imported_symbols.num_rows(), 0);
        assert_eq!(result.edges.num_rows(), 0);
    }

    #[test]
    fn nodes_without_ids_are_skipped() {
        let graph = GraphData {
            directory_nodes: vec![DirectoryNode {
                id: None,
                path: "src".into(),
                absolute_path: "/repo/src".into(),
                repository_name: "repo".into(),
                name: "src".into(),
            }],
            file_nodes: vec![],
            definition_nodes: vec![],
            imported_symbol_nodes: vec![],
            relationships: vec![],
        };

        let result = convert_graph_data(&graph, 1, "main").unwrap();
        assert_eq!(result.directories.num_rows(), 0);
    }

    #[test]
    fn directory_conversion() {
        let graph = GraphData {
            directory_nodes: vec![DirectoryNode {
                id: Some(42),
                path: "src".into(),
                absolute_path: "/repo/src".into(),
                repository_name: "repo".into(),
                name: "src".into(),
            }],
            file_nodes: vec![],
            definition_nodes: vec![],
            imported_symbol_nodes: vec![],
            relationships: vec![],
        };

        let result = convert_graph_data(&graph, 100, "main").unwrap();
        assert_eq!(result.directories.num_rows(), 1);
        assert_eq!(result.directories.num_columns(), 6);
    }

    #[test]
    fn file_conversion() {
        let graph = GraphData {
            directory_nodes: vec![],
            file_nodes: vec![FileNode {
                id: Some(10),
                path: "src/lib.rs".into(),
                absolute_path: "/repo/src/lib.rs".into(),
                language: "Rust".into(),
                repository_name: "repo".into(),
                extension: "rs".into(),
                name: "lib.rs".into(),
            }],
            definition_nodes: vec![],
            imported_symbol_nodes: vec![],
            relationships: vec![],
        };

        let result = convert_graph_data(&graph, 100, "main").unwrap();
        assert_eq!(result.files.num_rows(), 1);
        assert_eq!(result.files.num_columns(), 8);
    }
}
