use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use code_graph::analysis::types::{
    DefinitionNode, DirectoryNode, FileNode, GraphData, ImportedSymbolNode,
};

use crate::error::Result;

const LOCAL_TRAVERSAL_PATH: &str = "0/";

pub struct LocalGraphData {
    pub directories: RecordBatch,
    pub files: RecordBatch,
    pub definitions: RecordBatch,
    pub imported_symbols: RecordBatch,
    pub edges: RecordBatch,
}

pub fn convert_graph_data(
    graph_data: &GraphData,
    project_id: i64,
    branch: &str,
) -> Result<LocalGraphData> {
    Ok(LocalGraphData {
        directories: convert_directories(&graph_data.directory_nodes, project_id, branch)?,
        files: convert_files(&graph_data.file_nodes, project_id, branch)?,
        definitions: convert_definitions(&graph_data.definition_nodes, project_id, branch)?,
        imported_symbols: convert_imported_symbols(
            &graph_data.imported_symbol_nodes,
            project_id,
            branch,
        )?,
        edges: convert_edges(graph_data)?,
    })
}

fn convert_directories(
    nodes: &[DirectoryNode],
    project_id: i64,
    branch: &str,
) -> Result<RecordBatch> {
    let cap = nodes.len();
    let mut id = Int64Builder::with_capacity(cap);
    let mut tp = StringBuilder::with_capacity(cap, cap * 3);
    let mut pid = Int64Builder::with_capacity(cap);
    let mut br = StringBuilder::with_capacity(cap, cap * branch.len());
    let mut path = StringBuilder::with_capacity(cap, cap * 64);
    let mut name = StringBuilder::with_capacity(cap, cap * 32);
    let mut ver = Int64Builder::with_capacity(cap);

    for node in nodes {
        let Some(node_id) = node.id else { continue };
        id.append_value(node_id);
        tp.append_value(LOCAL_TRAVERSAL_PATH);
        pid.append_value(project_id);
        br.append_value(branch);
        path.append_value(&node.path);
        name.append_value(&node.name);
        ver.append_value(0);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("_version", DataType::Int64, false),
        ])),
        vec![
            Arc::new(id.finish()) as ArrayRef,
            Arc::new(tp.finish()),
            Arc::new(pid.finish()),
            Arc::new(br.finish()),
            Arc::new(path.finish()),
            Arc::new(name.finish()),
            Arc::new(ver.finish()),
        ],
    )?)
}

fn convert_files(nodes: &[FileNode], project_id: i64, branch: &str) -> Result<RecordBatch> {
    let cap = nodes.len();
    let mut id = Int64Builder::with_capacity(cap);
    let mut tp = StringBuilder::with_capacity(cap, cap * 3);
    let mut pid = Int64Builder::with_capacity(cap);
    let mut br = StringBuilder::with_capacity(cap, cap * branch.len());
    let mut path = StringBuilder::with_capacity(cap, cap * 64);
    let mut name = StringBuilder::with_capacity(cap, cap * 32);
    let mut ext = StringBuilder::with_capacity(cap, cap * 8);
    let mut lang = StringBuilder::with_capacity(cap, cap * 16);
    let mut ver = Int64Builder::with_capacity(cap);

    for node in nodes {
        let Some(node_id) = node.id else { continue };
        id.append_value(node_id);
        tp.append_value(LOCAL_TRAVERSAL_PATH);
        pid.append_value(project_id);
        br.append_value(branch);
        path.append_value(&node.path);
        name.append_value(&node.name);
        ext.append_value(&node.extension);
        lang.append_value(&node.language);
        ver.append_value(0);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("extension", DataType::Utf8, false),
            Field::new("language", DataType::Utf8, false),
            Field::new("_version", DataType::Int64, false),
        ])),
        vec![
            Arc::new(id.finish()) as ArrayRef,
            Arc::new(tp.finish()),
            Arc::new(pid.finish()),
            Arc::new(br.finish()),
            Arc::new(path.finish()),
            Arc::new(name.finish()),
            Arc::new(ext.finish()),
            Arc::new(lang.finish()),
            Arc::new(ver.finish()),
        ],
    )?)
}

fn convert_definitions(
    nodes: &[DefinitionNode],
    project_id: i64,
    branch: &str,
) -> Result<RecordBatch> {
    let cap = nodes.len();
    let mut id = Int64Builder::with_capacity(cap);
    let mut tp = StringBuilder::with_capacity(cap, cap * 3);
    let mut pid = Int64Builder::with_capacity(cap);
    let mut br = StringBuilder::with_capacity(cap, cap * branch.len());
    let mut file_path = StringBuilder::with_capacity(cap, cap * 64);
    let mut fqn = StringBuilder::with_capacity(cap, cap * 128);
    let mut name = StringBuilder::with_capacity(cap, cap * 32);
    let mut def_type = StringBuilder::with_capacity(cap, cap * 16);
    let mut start_line = Int64Builder::with_capacity(cap);
    let mut end_line = Int64Builder::with_capacity(cap);
    let mut start_byte = Int64Builder::with_capacity(cap);
    let mut end_byte = Int64Builder::with_capacity(cap);
    let mut ver = Int64Builder::with_capacity(cap);

    for node in nodes {
        let Some(node_id) = node.id else { continue };
        id.append_value(node_id);
        tp.append_value(LOCAL_TRAVERSAL_PATH);
        pid.append_value(project_id);
        br.append_value(branch);
        file_path.append_value(node.file_path.as_ref());
        fqn.append_value(node.fqn.to_string());
        name.append_value(node.fqn.name());
        def_type.append_value(node.definition_type.as_str());
        start_line.append_value(node.range.start.line as i64);
        end_line.append_value(node.range.end.line as i64);
        start_byte.append_value(node.range.byte_offset.0 as i64);
        end_byte.append_value(node.range.byte_offset.1 as i64);
        ver.append_value(0);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
            Field::new("file_path", DataType::Utf8, false),
            Field::new("fqn", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("definition_type", DataType::Utf8, false),
            Field::new("start_line", DataType::Int64, false),
            Field::new("end_line", DataType::Int64, false),
            Field::new("start_byte", DataType::Int64, false),
            Field::new("end_byte", DataType::Int64, false),
            Field::new("_version", DataType::Int64, false),
        ])),
        vec![
            Arc::new(id.finish()) as ArrayRef,
            Arc::new(tp.finish()),
            Arc::new(pid.finish()),
            Arc::new(br.finish()),
            Arc::new(file_path.finish()),
            Arc::new(fqn.finish()),
            Arc::new(name.finish()),
            Arc::new(def_type.finish()),
            Arc::new(start_line.finish()),
            Arc::new(end_line.finish()),
            Arc::new(start_byte.finish()),
            Arc::new(end_byte.finish()),
            Arc::new(ver.finish()),
        ],
    )?)
}

fn convert_imported_symbols(
    nodes: &[ImportedSymbolNode],
    project_id: i64,
    branch: &str,
) -> Result<RecordBatch> {
    let cap = nodes.len();
    let mut id = Int64Builder::with_capacity(cap);
    let mut tp = StringBuilder::with_capacity(cap, cap * 3);
    let mut pid = Int64Builder::with_capacity(cap);
    let mut br = StringBuilder::with_capacity(cap, cap * branch.len());
    let mut file_path = StringBuilder::with_capacity(cap, cap * 64);
    let mut import_type = StringBuilder::with_capacity(cap, cap * 16);
    let mut import_path = StringBuilder::with_capacity(cap, cap * 64);
    let mut ident_name = StringBuilder::with_capacity(cap, cap * 32);
    let mut ident_alias = StringBuilder::with_capacity(cap, cap * 32);
    let mut start_line = Int64Builder::with_capacity(cap);
    let mut end_line = Int64Builder::with_capacity(cap);
    let mut start_byte = Int64Builder::with_capacity(cap);
    let mut end_byte = Int64Builder::with_capacity(cap);
    let mut ver = Int64Builder::with_capacity(cap);

    for node in nodes {
        let Some(node_id) = node.id else { continue };
        id.append_value(node_id);
        tp.append_value(LOCAL_TRAVERSAL_PATH);
        pid.append_value(project_id);
        br.append_value(branch);
        file_path.append_value(&node.location.file_path);
        import_type.append_value(node.import_type.as_str());
        import_path.append_value(&node.import_path);
        match &node.identifier {
            Some(ident) => {
                ident_name.append_value(&ident.name);
                match &ident.alias {
                    Some(alias) => ident_alias.append_value(alias),
                    None => ident_alias.append_null(),
                }
            }
            None => {
                ident_name.append_null();
                ident_alias.append_null();
            }
        }
        start_line.append_value(node.location.start_line as i64);
        end_line.append_value(node.location.end_line as i64);
        start_byte.append_value(node.location.start_byte);
        end_byte.append_value(node.location.end_byte);
        ver.append_value(0);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
            Field::new("file_path", DataType::Utf8, false),
            Field::new("import_type", DataType::Utf8, false),
            Field::new("import_path", DataType::Utf8, false),
            Field::new("identifier_name", DataType::Utf8, true),
            Field::new("identifier_alias", DataType::Utf8, true),
            Field::new("start_line", DataType::Int64, false),
            Field::new("end_line", DataType::Int64, false),
            Field::new("start_byte", DataType::Int64, false),
            Field::new("end_byte", DataType::Int64, false),
            Field::new("_version", DataType::Int64, false),
        ])),
        vec![
            Arc::new(id.finish()) as ArrayRef,
            Arc::new(tp.finish()),
            Arc::new(pid.finish()),
            Arc::new(br.finish()),
            Arc::new(file_path.finish()),
            Arc::new(import_type.finish()),
            Arc::new(import_path.finish()),
            Arc::new(ident_name.finish()),
            Arc::new(ident_alias.finish()),
            Arc::new(start_line.finish()),
            Arc::new(end_line.finish()),
            Arc::new(start_byte.finish()),
            Arc::new(end_byte.finish()),
            Arc::new(ver.finish()),
        ],
    )?)
}

fn convert_edges(graph_data: &GraphData) -> Result<RecordBatch> {
    let rels = &graph_data.relationships;
    let cap = rels.len();
    let mut tp = StringBuilder::with_capacity(cap, cap * 3);
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

        tp.append_value(LOCAL_TRAVERSAL_PATH);
        source_id.append_value(s);
        source_kind.append_value(src_kind_str);
        rel_kind.append_value(rel.relationship_type.edge_kind());
        target_id.append_value(t);
        target_kind.append_value(tgt_kind_str);
        ver.append_value(0);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("source_id", DataType::Int64, false),
            Field::new("source_kind", DataType::Utf8, false),
            Field::new("relationship_kind", DataType::Utf8, false),
            Field::new("target_id", DataType::Int64, false),
            Field::new("target_kind", DataType::Utf8, false),
            Field::new("_version", DataType::Int64, false),
        ])),
        vec![
            Arc::new(tp.finish()) as ArrayRef,
            Arc::new(source_id.finish()),
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
        assert_eq!(result.directories.num_columns(), 7);
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
        assert_eq!(result.files.num_columns(), 9);
    }
}
