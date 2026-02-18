//! Arrow conversion for code indexer graph data.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use code_indexer::analysis::types::{
    DefinitionNode, DirectoryNode, FileNode, GraphData, ImportedSymbolNode,
};
use code_indexer::graph::RelationshipKind;

pub struct ArrowConverter {
    traversal_path: String,
    project_id: i64,
    branch: String,
}

impl ArrowConverter {
    pub fn new(traversal_path: String, project_id: i64, branch: String) -> Self {
        Self {
            traversal_path,
            project_id,
            branch,
        }
    }

    pub fn convert_all(&self, graph_data: &GraphData) -> Result<ConvertedGraphData, ArrowError> {
        Ok(ConvertedGraphData {
            directories: self.convert_directories(&graph_data.directory_nodes)?,
            files: self.convert_files(&graph_data.file_nodes)?,
            definitions: self.convert_definitions(&graph_data.definition_nodes)?,
            imported_symbols: self.convert_imported_symbols(&graph_data.imported_symbol_nodes)?,
            edges: self.convert_edges(graph_data)?,
        })
    }

    fn base_builders(&self, count: usize) -> BaseColumnBuilders {
        BaseColumnBuilders::new(&self.traversal_path, self.project_id, &self.branch, count)
    }

    pub fn convert_directories(&self, nodes: &[DirectoryNode]) -> Result<RecordBatch, ArrowError> {
        let mut base = self.base_builders(nodes.len());
        let mut id = Int64Builder::with_capacity(nodes.len());
        let mut path = StringBuilder::with_capacity(nodes.len(), nodes.len() * 64);
        let mut name = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);

        for node in nodes {
            let Some(node_id) = node.id else { continue };
            base.append_row();
            id.append_value(node_id);
            path.append_value(&node.path);
            name.append_value(&node.name);
        }

        base.build_batch_with_id(
            Arc::new(id.finish()) as ArrayRef,
            vec![
                (
                    "path",
                    DataType::Utf8,
                    false,
                    Arc::new(path.finish()) as ArrayRef,
                ),
                (
                    "name",
                    DataType::Utf8,
                    false,
                    Arc::new(name.finish()) as ArrayRef,
                ),
            ],
        )
    }

    pub fn convert_files(&self, nodes: &[FileNode]) -> Result<RecordBatch, ArrowError> {
        let mut base = self.base_builders(nodes.len());
        let mut id = Int64Builder::with_capacity(nodes.len());
        let mut path = StringBuilder::with_capacity(nodes.len(), nodes.len() * 64);
        let mut name = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);
        let mut extension = StringBuilder::with_capacity(nodes.len(), nodes.len() * 8);
        let mut language = StringBuilder::with_capacity(nodes.len(), nodes.len() * 16);

        for node in nodes {
            let Some(node_id) = node.id else { continue };
            base.append_row();
            id.append_value(node_id);
            path.append_value(&node.path);
            name.append_value(&node.name);
            extension.append_value(&node.extension);
            language.append_value(&node.language);
        }

        base.build_batch_with_id(
            Arc::new(id.finish()) as ArrayRef,
            vec![
                (
                    "path",
                    DataType::Utf8,
                    false,
                    Arc::new(path.finish()) as ArrayRef,
                ),
                (
                    "name",
                    DataType::Utf8,
                    false,
                    Arc::new(name.finish()) as ArrayRef,
                ),
                (
                    "extension",
                    DataType::Utf8,
                    false,
                    Arc::new(extension.finish()) as ArrayRef,
                ),
                (
                    "language",
                    DataType::Utf8,
                    false,
                    Arc::new(language.finish()) as ArrayRef,
                ),
            ],
        )
    }

    pub fn convert_definitions(&self, nodes: &[DefinitionNode]) -> Result<RecordBatch, ArrowError> {
        let mut base = self.base_builders(nodes.len());
        let mut id = Int64Builder::with_capacity(nodes.len());
        let mut file_path = StringBuilder::with_capacity(nodes.len(), nodes.len() * 64);
        let mut fqn = StringBuilder::with_capacity(nodes.len(), nodes.len() * 128);
        let mut name = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);
        let mut definition_type = StringBuilder::with_capacity(nodes.len(), nodes.len() * 16);
        let mut start_line = Int64Builder::with_capacity(nodes.len());
        let mut end_line = Int64Builder::with_capacity(nodes.len());
        let mut start_byte = Int64Builder::with_capacity(nodes.len());
        let mut end_byte = Int64Builder::with_capacity(nodes.len());

        for node in nodes {
            let Some(node_id) = node.id else { continue };
            base.append_row();
            id.append_value(node_id);
            file_path.append_value(node.file_path.as_ref());
            fqn.append_value(node.fqn.to_string());
            name.append_value(node.fqn.name());
            definition_type.append_value(node.definition_type.as_str());
            start_line.append_value(node.range.start.line as i64);
            end_line.append_value(node.range.end.line as i64);
            start_byte.append_value(node.range.byte_offset.0 as i64);
            end_byte.append_value(node.range.byte_offset.1 as i64);
        }

        base.build_batch_with_id(
            Arc::new(id.finish()) as ArrayRef,
            vec![
                (
                    "file_path",
                    DataType::Utf8,
                    false,
                    Arc::new(file_path.finish()) as ArrayRef,
                ),
                (
                    "fqn",
                    DataType::Utf8,
                    false,
                    Arc::new(fqn.finish()) as ArrayRef,
                ),
                (
                    "name",
                    DataType::Utf8,
                    false,
                    Arc::new(name.finish()) as ArrayRef,
                ),
                (
                    "definition_type",
                    DataType::Utf8,
                    false,
                    Arc::new(definition_type.finish()) as ArrayRef,
                ),
                (
                    "start_line",
                    DataType::Int64,
                    false,
                    Arc::new(start_line.finish()) as ArrayRef,
                ),
                (
                    "end_line",
                    DataType::Int64,
                    false,
                    Arc::new(end_line.finish()) as ArrayRef,
                ),
                (
                    "start_byte",
                    DataType::Int64,
                    false,
                    Arc::new(start_byte.finish()) as ArrayRef,
                ),
                (
                    "end_byte",
                    DataType::Int64,
                    false,
                    Arc::new(end_byte.finish()) as ArrayRef,
                ),
            ],
        )
    }

    pub fn convert_imported_symbols(
        &self,
        nodes: &[ImportedSymbolNode],
    ) -> Result<RecordBatch, ArrowError> {
        let mut base = self.base_builders(nodes.len());
        let mut id = Int64Builder::with_capacity(nodes.len());
        let mut file_path = StringBuilder::with_capacity(nodes.len(), nodes.len() * 64);
        let mut import_type = StringBuilder::with_capacity(nodes.len(), nodes.len() * 16);
        let mut import_path = StringBuilder::with_capacity(nodes.len(), nodes.len() * 64);
        let mut identifier_name = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);
        let mut identifier_alias = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);
        let mut start_line = Int64Builder::with_capacity(nodes.len());
        let mut end_line = Int64Builder::with_capacity(nodes.len());
        let mut start_byte = Int64Builder::with_capacity(nodes.len());
        let mut end_byte = Int64Builder::with_capacity(nodes.len());

        for node in nodes {
            let Some(node_id) = node.id else { continue };
            base.append_row();
            id.append_value(node_id);
            file_path.append_value(&node.location.file_path);
            import_type.append_value(node.import_type.as_str());
            import_path.append_value(&node.import_path);

            match &node.identifier {
                Some(ident) => {
                    identifier_name.append_value(&ident.name);
                    match &ident.alias {
                        Some(alias) => identifier_alias.append_value(alias),
                        None => identifier_alias.append_null(),
                    }
                }
                None => {
                    identifier_name.append_null();
                    identifier_alias.append_null();
                }
            }

            start_line.append_value(node.location.start_line as i64);
            end_line.append_value(node.location.end_line as i64);
            start_byte.append_value(node.location.start_byte);
            end_byte.append_value(node.location.end_byte);
        }

        base.build_batch_with_id(
            Arc::new(id.finish()) as ArrayRef,
            vec![
                (
                    "file_path",
                    DataType::Utf8,
                    false,
                    Arc::new(file_path.finish()) as ArrayRef,
                ),
                (
                    "import_type",
                    DataType::Utf8,
                    false,
                    Arc::new(import_type.finish()) as ArrayRef,
                ),
                (
                    "import_path",
                    DataType::Utf8,
                    false,
                    Arc::new(import_path.finish()) as ArrayRef,
                ),
                (
                    "identifier_name",
                    DataType::Utf8,
                    true,
                    Arc::new(identifier_name.finish()) as ArrayRef,
                ),
                (
                    "identifier_alias",
                    DataType::Utf8,
                    true,
                    Arc::new(identifier_alias.finish()) as ArrayRef,
                ),
                (
                    "start_line",
                    DataType::Int64,
                    false,
                    Arc::new(start_line.finish()) as ArrayRef,
                ),
                (
                    "end_line",
                    DataType::Int64,
                    false,
                    Arc::new(end_line.finish()) as ArrayRef,
                ),
                (
                    "start_byte",
                    DataType::Int64,
                    false,
                    Arc::new(start_byte.finish()) as ArrayRef,
                ),
                (
                    "end_byte",
                    DataType::Int64,
                    false,
                    Arc::new(end_byte.finish()) as ArrayRef,
                ),
            ],
        )
    }

    pub fn convert_edges(&self, graph_data: &GraphData) -> Result<RecordBatch, ArrowError> {
        let rels = &graph_data.relationships;
        let mut source_id = Int64Builder::with_capacity(rels.len());
        let mut source_kind = StringBuilder::with_capacity(rels.len(), rels.len() * 16);
        let mut relationship_kind = StringBuilder::with_capacity(rels.len(), rels.len() * 32);
        let mut target_id = Int64Builder::with_capacity(rels.len());
        let mut target_kind = StringBuilder::with_capacity(rels.len(), rels.len() * 16);

        for rel in rels {
            let (src_kind_str, tgt_kind_str) = relationship_kind_to_strings(&rel.kind);

            let source_node_id = self.lookup_node_id(graph_data, src_kind_str, rel.source_id);
            let target_node_id = self.lookup_node_id(graph_data, tgt_kind_str, rel.target_id);

            let (Some(src_id), Some(tgt_id)) = (source_node_id, target_node_id) else {
                continue;
            };

            source_id.append_value(src_id);
            source_kind.append_value(src_kind_str);
            relationship_kind.append_value(rel.relationship_type.as_str());
            target_id.append_value(tgt_id);
            target_kind.append_value(tgt_kind_str);
        }

        let schema = Schema::new(vec![
            Field::new("source_id", DataType::Int64, false),
            Field::new("source_kind", DataType::Utf8, false),
            Field::new("relationship_kind", DataType::Utf8, false),
            Field::new("target_id", DataType::Int64, false),
            Field::new("target_kind", DataType::Utf8, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(source_id.finish()) as ArrayRef,
                Arc::new(source_kind.finish()) as ArrayRef,
                Arc::new(relationship_kind.finish()) as ArrayRef,
                Arc::new(target_id.finish()) as ArrayRef,
                Arc::new(target_kind.finish()) as ArrayRef,
            ],
        )
    }

    fn lookup_node_id(
        &self,
        graph_data: &GraphData,
        node_kind: &str,
        index: Option<u32>,
    ) -> Option<i64> {
        let index = index? as usize;
        match node_kind {
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
}

struct BaseColumnBuilders {
    traversal_path: StringBuilder,
    project_id: Int64Builder,
    branch: StringBuilder,
    traversal_path_value: String,
    project_id_value: i64,
    branch_value: String,
}

impl BaseColumnBuilders {
    fn new(traversal_path: &str, project_id: i64, branch: &str, capacity: usize) -> Self {
        Self {
            traversal_path: StringBuilder::with_capacity(capacity, capacity * traversal_path.len()),
            project_id: Int64Builder::with_capacity(capacity),
            branch: StringBuilder::with_capacity(capacity, capacity * branch.len()),
            traversal_path_value: traversal_path.to_string(),
            project_id_value: project_id,
            branch_value: branch.to_string(),
        }
    }

    fn append_row(&mut self) {
        self.traversal_path.append_value(&self.traversal_path_value);
        self.project_id.append_value(self.project_id_value);
        self.branch.append_value(&self.branch_value);
    }

    fn build_batch_with_id(
        mut self,
        id_array: ArrayRef,
        extra_columns: Vec<(&str, DataType, bool, ArrayRef)>,
    ) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
        ];

        let mut columns: Vec<ArrayRef> = vec![
            id_array,
            Arc::new(self.traversal_path.finish()),
            Arc::new(self.project_id.finish()),
            Arc::new(self.branch.finish()),
        ];

        for (name, dtype, nullable, array) in extra_columns {
            fields.push(Field::new(name, dtype, nullable));
            columns.push(array);
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

pub struct ConvertedGraphData {
    pub directories: RecordBatch,
    pub files: RecordBatch,
    pub definitions: RecordBatch,
    pub imported_symbols: RecordBatch,
    pub edges: RecordBatch,
}

fn relationship_kind_to_strings(kind: &RelationshipKind) -> (&'static str, &'static str) {
    match kind {
        RelationshipKind::DirectoryToDirectory => ("Directory", "Directory"),
        RelationshipKind::DirectoryToFile => ("Directory", "File"),
        RelationshipKind::FileToDefinition => ("File", "Definition"),
        RelationshipKind::FileToImportedSymbol => ("File", "ImportedSymbol"),
        RelationshipKind::DefinitionToDefinition => ("Definition", "Definition"),
        RelationshipKind::DefinitionToImportedSymbol => ("Definition", "ImportedSymbol"),
        RelationshipKind::ImportedSymbolToImportedSymbol => ("ImportedSymbol", "ImportedSymbol"),
        RelationshipKind::ImportedSymbolToDefinition => ("ImportedSymbol", "Definition"),
        RelationshipKind::ImportedSymbolToFile => ("ImportedSymbol", "File"),
        RelationshipKind::Empty => ("Unknown", "Unknown"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_indexer::analysis::types::{DefinitionType, FqnType};
    use internment::ArcIntern;
    use parser_core::ruby::types::{RubyDefinitionType, RubyFqn, RubyFqnPart, RubyFqnPartType};
    use parser_core::utils::{Position, Range};
    use smallvec::SmallVec;
    use std::sync::Arc as StdArc;

    fn create_test_definition(
        class_name: &str,
        method_name: &str,
        file_path: &str,
        start_byte: usize,
        end_byte: usize,
    ) -> DefinitionNode {
        let range = Range::new(
            Position::new(1, 0),
            Position::new(10, 0),
            (start_byte, end_byte),
        );

        let fqn = RubyFqn {
            parts: StdArc::new(SmallVec::from_vec(vec![
                RubyFqnPart::new(RubyFqnPartType::Class, class_name.to_string(), range),
                RubyFqnPart::new(RubyFqnPartType::Method, method_name.to_string(), range),
            ])),
        };

        DefinitionNode::new(
            FqnType::Ruby(fqn),
            DefinitionType::Ruby(RubyDefinitionType::Method),
            range,
            ArcIntern::new(file_path.to_string()),
        )
    }

    fn create_test_file(path: &str) -> FileNode {
        FileNode {
            id: None,
            path: path.to_string(),
            absolute_path: format!("/repo/{}", path),
            language: "Ruby".to_string(),
            repository_name: "test-repo".to_string(),
            extension: "rb".to_string(),
            name: path.split('/').next_back().unwrap_or(path).to_string(),
        }
    }

    #[test]
    fn test_node_ids_are_unique() {
        let project_id = 123;
        let branch = "main";

        let mut graph_data = GraphData {
            directory_nodes: vec![],
            file_nodes: vec![create_test_file("src/user.rb")],
            definition_nodes: vec![
                create_test_definition("User", "save", "src/user.rb", 0, 100),
                create_test_definition("User", "validate", "src/user.rb", 100, 200),
            ],
            imported_symbol_nodes: vec![],
            relationships: vec![],
        };

        graph_data.assign_node_ids(project_id, branch);

        let file_id = graph_data.file_nodes[0].id.unwrap();
        let def1_id = graph_data.definition_nodes[0].id.unwrap();
        let def2_id = graph_data.definition_nodes[1].id.unwrap();

        println!("file_id: {}", file_id);
        println!("def1_id: {}", def1_id);
        println!("def2_id: {}", def2_id);
        assert_ne!(file_id, def1_id, "file and def1 should have different IDs");
        assert_ne!(file_id, def2_id, "file and def2 should have different IDs");
        assert_ne!(def1_id, def2_id, "def1 and def2 should have different IDs");
    }
}
