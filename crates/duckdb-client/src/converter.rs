use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use code_graph::analysis::types::{
    DefinitionNode, DirectoryNode, FileNode, GraphData, ImportedSymbolNode,
};

use crate::error::Result;

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

/// Common columns shared by all node tables: id, project_id, branch, _version.
struct BaseColumns {
    id: Int64Builder,
    project_id: Int64Builder,
    branch: StringBuilder,
    version: Int64Builder,
    project_id_val: i64,
    branch_val: String,
}

impl BaseColumns {
    fn new(capacity: usize, branch: &str) -> Self {
        Self {
            id: Int64Builder::with_capacity(capacity),
            project_id: Int64Builder::with_capacity(capacity),
            branch: StringBuilder::with_capacity(capacity, capacity * branch.len()),
            version: Int64Builder::with_capacity(capacity),
            project_id_val: 0,
            branch_val: branch.to_string(),
        }
    }

    fn with_project_id(mut self, project_id: i64) -> Self {
        self.project_id_val = project_id;
        self
    }

    fn append(&mut self, node_id: i64) {
        self.id.append_value(node_id);
        self.project_id.append_value(self.project_id_val);
        self.branch.append_value(&self.branch_val);
        self.version.append_value(0);
    }

    fn into_batch(mut self, extra: Vec<(&str, DataType, bool, ArrayRef)>) -> Result<RecordBatch> {
        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(self.id.finish()),
            Arc::new(self.project_id.finish()),
            Arc::new(self.branch.finish()),
        ];

        for (name, dtype, nullable, array) in extra {
            fields.push(Field::new(name, dtype, nullable));
            columns.push(array);
        }

        fields.push(Field::new("_version", DataType::Int64, false));
        columns.push(Arc::new(self.version.finish()));

        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }
}

fn convert_directories(
    nodes: &[DirectoryNode],
    project_id: i64,
    branch: &str,
) -> Result<RecordBatch> {
    let cap = nodes.len();
    let mut base = BaseColumns::new(cap, branch).with_project_id(project_id);
    let mut path = StringBuilder::with_capacity(cap, cap * 64);
    let mut name = StringBuilder::with_capacity(cap, cap * 32);

    for node in nodes {
        let Some(node_id) = node.id else { continue };
        base.append(node_id);
        path.append_value(&node.path);
        name.append_value(&node.name);
    }

    base.into_batch(vec![
        ("path", DataType::Utf8, false, Arc::new(path.finish())),
        ("name", DataType::Utf8, false, Arc::new(name.finish())),
    ])
}

fn convert_files(nodes: &[FileNode], project_id: i64, branch: &str) -> Result<RecordBatch> {
    let cap = nodes.len();
    let mut base = BaseColumns::new(cap, branch).with_project_id(project_id);
    let mut path = StringBuilder::with_capacity(cap, cap * 64);
    let mut name = StringBuilder::with_capacity(cap, cap * 32);
    let mut ext = StringBuilder::with_capacity(cap, cap * 8);
    let mut lang = StringBuilder::with_capacity(cap, cap * 16);

    for node in nodes {
        let Some(node_id) = node.id else { continue };
        base.append(node_id);
        path.append_value(&node.path);
        name.append_value(&node.name);
        ext.append_value(&node.extension);
        lang.append_value(&node.language);
    }

    base.into_batch(vec![
        ("path", DataType::Utf8, false, Arc::new(path.finish())),
        ("name", DataType::Utf8, false, Arc::new(name.finish())),
        ("extension", DataType::Utf8, false, Arc::new(ext.finish())),
        ("language", DataType::Utf8, false, Arc::new(lang.finish())),
    ])
}

fn convert_definitions(
    nodes: &[DefinitionNode],
    project_id: i64,
    branch: &str,
) -> Result<RecordBatch> {
    let cap = nodes.len();
    let mut base = BaseColumns::new(cap, branch).with_project_id(project_id);
    let mut file_path = StringBuilder::with_capacity(cap, cap * 64);
    let mut fqn = StringBuilder::with_capacity(cap, cap * 128);
    let mut name = StringBuilder::with_capacity(cap, cap * 32);
    let mut def_type = StringBuilder::with_capacity(cap, cap * 16);
    let mut start_line = Int64Builder::with_capacity(cap);
    let mut end_line = Int64Builder::with_capacity(cap);
    let mut start_byte = Int64Builder::with_capacity(cap);
    let mut end_byte = Int64Builder::with_capacity(cap);

    for node in nodes {
        let Some(node_id) = node.id else { continue };
        base.append(node_id);
        file_path.append_value(node.file_path.as_ref());
        fqn.append_value(node.fqn.to_string());
        name.append_value(node.fqn.name());
        def_type.append_value(node.definition_type.as_str());
        start_line.append_value(node.range.start.line as i64);
        end_line.append_value(node.range.end.line as i64);
        start_byte.append_value(node.range.byte_offset.0 as i64);
        end_byte.append_value(node.range.byte_offset.1 as i64);
    }

    base.into_batch(vec![
        (
            "file_path",
            DataType::Utf8,
            false,
            Arc::new(file_path.finish()),
        ),
        ("fqn", DataType::Utf8, false, Arc::new(fqn.finish())),
        ("name", DataType::Utf8, false, Arc::new(name.finish())),
        (
            "definition_type",
            DataType::Utf8,
            false,
            Arc::new(def_type.finish()),
        ),
        (
            "start_line",
            DataType::Int64,
            false,
            Arc::new(start_line.finish()),
        ),
        (
            "end_line",
            DataType::Int64,
            false,
            Arc::new(end_line.finish()),
        ),
        (
            "start_byte",
            DataType::Int64,
            false,
            Arc::new(start_byte.finish()),
        ),
        (
            "end_byte",
            DataType::Int64,
            false,
            Arc::new(end_byte.finish()),
        ),
    ])
}

fn convert_imported_symbols(
    nodes: &[ImportedSymbolNode],
    project_id: i64,
    branch: &str,
) -> Result<RecordBatch> {
    let cap = nodes.len();
    let mut base = BaseColumns::new(cap, branch).with_project_id(project_id);
    let mut file_path = StringBuilder::with_capacity(cap, cap * 64);
    let mut import_type = StringBuilder::with_capacity(cap, cap * 16);
    let mut import_path = StringBuilder::with_capacity(cap, cap * 64);
    let mut ident_name = StringBuilder::with_capacity(cap, cap * 32);
    let mut ident_alias = StringBuilder::with_capacity(cap, cap * 32);
    let mut start_line = Int64Builder::with_capacity(cap);
    let mut end_line = Int64Builder::with_capacity(cap);
    let mut start_byte = Int64Builder::with_capacity(cap);
    let mut end_byte = Int64Builder::with_capacity(cap);

    for node in nodes {
        let Some(node_id) = node.id else { continue };
        base.append(node_id);
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
    }

    base.into_batch(vec![
        (
            "file_path",
            DataType::Utf8,
            false,
            Arc::new(file_path.finish()),
        ),
        (
            "import_type",
            DataType::Utf8,
            false,
            Arc::new(import_type.finish()),
        ),
        (
            "import_path",
            DataType::Utf8,
            false,
            Arc::new(import_path.finish()),
        ),
        (
            "identifier_name",
            DataType::Utf8,
            true,
            Arc::new(ident_name.finish()),
        ),
        (
            "identifier_alias",
            DataType::Utf8,
            true,
            Arc::new(ident_alias.finish()),
        ),
        (
            "start_line",
            DataType::Int64,
            false,
            Arc::new(start_line.finish()),
        ),
        (
            "end_line",
            DataType::Int64,
            false,
            Arc::new(end_line.finish()),
        ),
        (
            "start_byte",
            DataType::Int64,
            false,
            Arc::new(start_byte.finish()),
        ),
        (
            "end_byte",
            DataType::Int64,
            false,
            Arc::new(end_byte.finish()),
        ),
    ])
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
