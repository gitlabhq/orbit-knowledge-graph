//! Arrow conversion for code indexer graph data.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Builder, Int64Builder, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use indexer::analysis::types::{
    ConsolidatedRelationship, DefinitionNode, DirectoryNode, FileNode, GraphData,
    ImportedSymbolNode,
};
use indexer::graph::RelationshipKind;

pub struct ArrowConverter {
    traversal_path: String,
    project_id: i64,
    branch: String,
    version: u64,
}

impl ArrowConverter {
    pub fn new(traversal_path: String, project_id: i64, branch: String, version: u64) -> Self {
        Self {
            traversal_path,
            project_id,
            branch,
            version,
        }
    }

    pub fn convert_all(&self, graph_data: &GraphData) -> Result<ConvertedGraphData, ArrowError> {
        Ok(ConvertedGraphData {
            directories: self.convert_directories(&graph_data.directory_nodes)?,
            files: self.convert_files(&graph_data.file_nodes)?,
            definitions: self.convert_definitions(&graph_data.definition_nodes)?,
            imported_symbols: self.convert_imported_symbols(&graph_data.imported_symbol_nodes)?,
            edges: self.convert_edges(&graph_data.relationships)?,
        })
    }

    fn base_builders(&self, count: usize) -> BaseColumnBuilders {
        BaseColumnBuilders::new(
            &self.traversal_path,
            self.project_id,
            &self.branch,
            self.version,
            count,
        )
    }

    pub fn convert_directories(&self, nodes: &[DirectoryNode]) -> Result<RecordBatch, ArrowError> {
        let mut base = self.base_builders(nodes.len());
        // Paths are typically longer than names (e.g., "src/auth/login.rs" vs "login.rs")
        let mut path = StringBuilder::with_capacity(nodes.len(), nodes.len() * 64);
        let mut name = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);

        for node in nodes {
            base.append_row();
            path.append_value(&node.path);
            name.append_value(&node.name);
        }

        base.build_batch(vec![
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
        ])
    }

    pub fn convert_files(&self, nodes: &[FileNode]) -> Result<RecordBatch, ArrowError> {
        let mut base = self.base_builders(nodes.len());
        let mut path = StringBuilder::with_capacity(nodes.len(), nodes.len() * 64);
        let mut name = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);
        let mut extension = StringBuilder::with_capacity(nodes.len(), nodes.len() * 8);
        let mut language = StringBuilder::with_capacity(nodes.len(), nodes.len() * 16);

        for node in nodes {
            base.append_row();
            path.append_value(&node.path);
            name.append_value(&node.name);
            extension.append_value(&node.extension);
            language.append_value(&node.language);
        }

        base.build_batch(vec![
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
        ])
    }

    pub fn convert_definitions(&self, nodes: &[DefinitionNode]) -> Result<RecordBatch, ArrowError> {
        let mut base = self.base_builders(nodes.len());
        let mut file_path = StringBuilder::with_capacity(nodes.len(), nodes.len() * 64);
        let mut fqn = StringBuilder::with_capacity(nodes.len(), nodes.len() * 128);
        let mut name = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);
        let mut definition_type = StringBuilder::with_capacity(nodes.len(), nodes.len() * 16);
        let mut start_line = Int32Builder::with_capacity(nodes.len());
        let mut end_line = Int32Builder::with_capacity(nodes.len());
        let mut start_byte = Int64Builder::with_capacity(nodes.len());
        let mut end_byte = Int64Builder::with_capacity(nodes.len());

        for node in nodes {
            base.append_row();
            file_path.append_value(node.file_path.as_ref());
            fqn.append_value(node.fqn.to_string());
            name.append_value(node.fqn.name());
            definition_type.append_value(node.definition_type.as_str());
            start_line.append_value(node.range.start.line as i32);
            end_line.append_value(node.range.end.line as i32);
            start_byte.append_value(node.range.byte_offset.0 as i64);
            end_byte.append_value(node.range.byte_offset.1 as i64);
        }

        base.build_batch(vec![
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
                DataType::Int32,
                false,
                Arc::new(start_line.finish()) as ArrayRef,
            ),
            (
                "end_line",
                DataType::Int32,
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
        ])
    }

    pub fn convert_imported_symbols(
        &self,
        nodes: &[ImportedSymbolNode],
    ) -> Result<RecordBatch, ArrowError> {
        let mut base = self.base_builders(nodes.len());
        let mut file_path = StringBuilder::with_capacity(nodes.len(), nodes.len() * 64);
        let mut import_type = StringBuilder::with_capacity(nodes.len(), nodes.len() * 16);
        let mut import_path = StringBuilder::with_capacity(nodes.len(), nodes.len() * 64);
        let mut identifier_name = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);
        let mut identifier_alias = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);
        let mut start_byte = Int64Builder::with_capacity(nodes.len());
        let mut end_byte = Int64Builder::with_capacity(nodes.len());

        for node in nodes {
            base.append_row();
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

            start_byte.append_value(node.location.start_byte);
            end_byte.append_value(node.location.end_byte);
        }

        base.build_batch(vec![
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
        ])
    }

    pub fn convert_edges(
        &self,
        rels: &[ConsolidatedRelationship],
    ) -> Result<RecordBatch, ArrowError> {
        let mut base = self.base_builders(rels.len());
        let mut source_kind = StringBuilder::with_capacity(rels.len(), rels.len() * 16);
        let mut target_kind = StringBuilder::with_capacity(rels.len(), rels.len() * 16);
        let mut relationship_type = StringBuilder::with_capacity(rels.len(), rels.len() * 16);
        let mut source_path = StringBuilder::with_capacity(rels.len(), rels.len() * 64);
        let mut target_path = StringBuilder::with_capacity(rels.len(), rels.len() * 64);

        for rel in rels {
            base.append_row();
            let (src_kind, tgt_kind) = relationship_kind_to_strings(&rel.kind);
            source_kind.append_value(src_kind);
            target_kind.append_value(tgt_kind);
            relationship_type.append_value(rel.relationship_type.as_str());
            source_path.append_value(rel.source_path.as_ref().map(|s| s.as_str()).unwrap_or(""));
            target_path.append_value(rel.target_path.as_ref().map(|s| s.as_str()).unwrap_or(""));
        }

        base.build_batch(vec![
            (
                "source_kind",
                DataType::Utf8,
                false,
                Arc::new(source_kind.finish()) as ArrayRef,
            ),
            (
                "target_kind",
                DataType::Utf8,
                false,
                Arc::new(target_kind.finish()) as ArrayRef,
            ),
            (
                "relationship_type",
                DataType::Utf8,
                false,
                Arc::new(relationship_type.finish()) as ArrayRef,
            ),
            (
                "source_path",
                DataType::Utf8,
                false,
                Arc::new(source_path.finish()) as ArrayRef,
            ),
            (
                "target_path",
                DataType::Utf8,
                false,
                Arc::new(target_path.finish()) as ArrayRef,
            ),
        ])
    }
}

struct BaseColumnBuilders {
    traversal_path: StringBuilder,
    project_id: Int64Builder,
    branch: StringBuilder,
    version: UInt64Builder,
    traversal_path_value: String,
    project_id_value: i64,
    branch_value: String,
    version_value: u64,
}

impl BaseColumnBuilders {
    fn new(
        traversal_path: &str,
        project_id: i64,
        branch: &str,
        version: u64,
        capacity: usize,
    ) -> Self {
        Self {
            traversal_path: StringBuilder::with_capacity(capacity, capacity * traversal_path.len()),
            project_id: Int64Builder::with_capacity(capacity),
            branch: StringBuilder::with_capacity(capacity, capacity * branch.len()),
            version: UInt64Builder::with_capacity(capacity),
            traversal_path_value: traversal_path.to_string(),
            project_id_value: project_id,
            branch_value: branch.to_string(),
            version_value: version,
        }
    }

    fn append_row(&mut self) {
        self.traversal_path.append_value(&self.traversal_path_value);
        self.project_id.append_value(self.project_id_value);
        self.branch.append_value(&self.branch_value);
        self.version.append_value(self.version_value);
    }

    fn build_batch(
        mut self,
        extra_columns: Vec<(&str, DataType, bool, ArrayRef)>,
    ) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
        ];

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(self.traversal_path.finish()),
            Arc::new(self.project_id.finish()),
            Arc::new(self.branch.finish()),
        ];

        for (name, dtype, nullable, array) in extra_columns {
            fields.push(Field::new(name, dtype, nullable));
            columns.push(array);
        }

        fields.push(Field::new("_version", DataType::UInt64, false));
        columns.push(Arc::new(self.version.finish()));

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
