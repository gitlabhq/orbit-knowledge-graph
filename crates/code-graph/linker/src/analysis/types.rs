use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

use internment::ArcIntern;
use rustc_hash::FxHasher;

use crate::graph::{RelationshipKind, RelationshipType};
use code_graph_types::{CanonicalFqn, DefKind, Position, Range};
use parser_core::{
    csharp::types::CSharpImportType, imports::ImportTypeInfo, java::types::JavaImportType,
    kotlin::types::KotlinImportType, python::types::PythonImportType, rust::types::RustImportType,
    typescript::types::TypeScriptImportType, utils::HasRange,
};
use serde::{Deserialize, Serialize};

/// Context for [`AsRecordBatch`](gkg_utils::arrow::AsRecordBatch)
/// implementations on code graph node types.
pub struct RowContext<'a> {
    pub project_id: i64,
    pub branch: &'a str,
    pub commit_sha: &'a str,
}

// TODO: Use a more robust id generator: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/60
fn compute_id(components: &[&str]) -> i64 {
    let mut hasher = FxHasher::default();
    components.hash(&mut hasher);
    hasher.finish() as i64
}

/// Consolidated relationship data for efficient storage
#[derive(Debug, Clone)]
pub struct ConsolidatedRelationship {
    pub kind: RelationshipKind,
    pub source_id: Option<u32>,
    pub target_id: Option<u32>,
    pub relationship_type: RelationshipType,
    pub source_path: Option<ArcIntern<String>>,
    pub target_path: Option<ArcIntern<String>>,
    pub source_range: ArcIntern<Range>,
    pub target_range: ArcIntern<Range>,
    /// Definition location for source node (used for ID lookup)
    pub source_definition_range: Option<ArcIntern<Range>>,
    /// Definition location for target node (used for ID lookup)
    pub target_definition_range: Option<ArcIntern<Range>>,
}

impl Default for ConsolidatedRelationship {
    fn default() -> Self {
        Self {
            kind: RelationshipKind::Empty,
            source_id: None,
            target_id: None,
            relationship_type: RelationshipType::Empty,
            source_path: None,
            target_path: None,
            source_range: ArcIntern::new(Range::empty()),
            target_range: ArcIntern::new(Range::empty()),
            source_definition_range: None,
            target_definition_range: None,
        }
    }
}

impl ConsolidatedRelationship {
    pub fn dir_to_dir(from_path: ArcIntern<String>, to_path: ArcIntern<String>) -> Self {
        Self {
            source_path: Some(from_path),
            target_path: Some(to_path),
            kind: RelationshipKind::DirectoryToDirectory,
            ..Default::default()
        }
    }

    pub fn dir_to_file(from_path: ArcIntern<String>, to_path: ArcIntern<String>) -> Self {
        Self {
            source_path: Some(from_path),
            target_path: Some(to_path),
            kind: RelationshipKind::DirectoryToFile,
            ..Default::default()
        }
    }

    pub fn import_to_import(from_path: ArcIntern<String>, to_path: ArcIntern<String>) -> Self {
        Self {
            source_path: Some(from_path),
            target_path: Some(to_path),
            kind: RelationshipKind::ImportedSymbolToImportedSymbol,
            ..Default::default()
        }
    }

    pub fn import_to_definition(from_path: ArcIntern<String>, to_path: ArcIntern<String>) -> Self {
        Self {
            source_path: Some(from_path),
            target_path: Some(to_path),
            kind: RelationshipKind::ImportedSymbolToDefinition,
            ..Default::default()
        }
    }

    pub fn import_to_file(from_path: ArcIntern<String>, to_path: ArcIntern<String>) -> Self {
        Self {
            source_path: Some(from_path),
            target_path: Some(to_path),
            kind: RelationshipKind::ImportedSymbolToFile,
            ..Default::default()
        }
    }

    pub fn definition_to_definition(
        from_path: ArcIntern<String>,
        to_path: ArcIntern<String>,
    ) -> Self {
        Self {
            source_path: Some(from_path),
            target_path: Some(to_path),
            kind: RelationshipKind::DefinitionToDefinition,
            ..Default::default()
        }
    }

    pub fn file_to_definition(from_path: ArcIntern<String>, to_path: ArcIntern<String>) -> Self {
        Self {
            source_path: Some(from_path),
            target_path: Some(to_path),
            kind: RelationshipKind::FileToDefinition,
            ..Default::default()
        }
    }

    pub fn file_to_imported_symbol(
        from_path: ArcIntern<String>,
        to_path: ArcIntern<String>,
    ) -> Self {
        Self {
            source_path: Some(from_path),
            target_path: Some(to_path),
            kind: RelationshipKind::FileToImportedSymbol,
            ..Default::default()
        }
    }

    pub fn definition_to_imported_symbol(
        from_path: ArcIntern<String>,
        to_path: ArcIntern<String>,
    ) -> Self {
        Self {
            source_path: Some(from_path),
            target_path: Some(to_path),
            kind: RelationshipKind::DefinitionToImportedSymbol,
            ..Default::default()
        }
    }
}

/// A fully resolved edge with concrete node IDs and ontology kind strings.
/// Produced by [`GraphData::resolve_edges`].
#[derive(Debug, Clone)]
pub struct ResolvedEdge {
    pub source_id: i64,
    pub source_kind: &'static str,
    pub relationship_kind: String,
    pub target_id: i64,
    pub target_kind: &'static str,
}

impl gkg_utils::arrow::AsRecordBatch<RowContext<'_>> for ResolvedEdge {
    fn write_row(
        &self,
        b: &mut gkg_utils::arrow::BatchBuilder,
        _ctx: &RowContext<'_>,
    ) -> Result<(), arrow::error::ArrowError> {
        b.col("source_id")?.push_int(self.source_id)?;
        b.col("source_kind")?.push_str(self.source_kind)?;
        b.col("relationship_kind")?
            .push_str(&self.relationship_kind)?;
        b.col("target_id")?.push_int(self.target_id)?;
        b.col("target_kind")?.push_str(self.target_kind)?;
        Ok(())
    }
}

pub fn rels_by_kind(
    relationships: &[ConsolidatedRelationship],
    kind: RelationshipKind,
) -> impl Iterator<Item = ConsolidatedRelationship> + '_ {
    relationships
        .iter()
        .filter(move |rel| rel.kind == kind)
        .cloned()
}

/// Structured graph data ready for writing to Parquet files
#[derive(Debug)]
pub struct GraphData {
    /// Directory nodes to be written to directories.parquet
    pub directory_nodes: Vec<DirectoryNode>,
    /// File nodes to be written to files.parquet
    pub file_nodes: Vec<FileNode>,
    /// Definition nodes to be written to definitions.parquet  
    pub definition_nodes: Vec<DefinitionNode>,
    /// Imported symbol nodes to be written to imported_symbols.parquet
    pub imported_symbol_nodes: Vec<ImportedSymbolNode>,
    /// Relationships to be written to parquet files based on their kind
    pub relationships: Vec<ConsolidatedRelationship>,
}

impl GraphData {
    /// Assign integer IDs to all nodes and populate relationship source_id/target_id fields.
    /// This replicates the logic from NodeIdGenerator and GraphMapper.
    pub fn assign_node_ids(&mut self, project_id: i64, branch: &str) {
        let mut directory_lookup: HashMap<String, u32> = HashMap::new();
        let mut file_lookup: HashMap<String, u32> = HashMap::new();
        let mut definition_lookup: HashMap<(String, usize, usize), u32> = HashMap::new();
        let mut imported_symbol_lookup: HashMap<(String, usize, usize), u32> = HashMap::new();

        for (index, node) in self.directory_nodes.iter_mut().enumerate() {
            node.assign_id(project_id, branch);
            directory_lookup.insert(node.path.clone(), index as u32);
        }

        for (index, node) in self.file_nodes.iter_mut().enumerate() {
            node.assign_id(project_id, branch);
            file_lookup.insert(node.path.clone(), index as u32);
        }

        for (index, node) in self.definition_nodes.iter_mut().enumerate() {
            node.assign_id(project_id, branch);
            let key = (
                node.file_path.as_ref().clone(),
                node.range.byte_offset.0,
                node.range.byte_offset.1,
            );
            definition_lookup.insert(key, index as u32);
        }

        for (index, node) in self.imported_symbol_nodes.iter_mut().enumerate() {
            node.assign_id(project_id, branch);
            let key = (
                node.location.file_path.clone(),
                node.location.start_byte as usize,
                node.location.end_byte as usize,
            );
            imported_symbol_lookup.insert(key, index as u32);
        }

        for rel in &mut self.relationships {
            let from_path = rel.source_path.as_ref().map(|p| p.as_ref().as_str());
            let to_path = rel.target_path.as_ref().map(|p| p.as_ref().as_str());

            let (Some(from_path), Some(to_path)) = (from_path, to_path) else {
                continue;
            };

            match rel.kind {
                RelationshipKind::DirectoryToDirectory => {
                    rel.source_id = directory_lookup.get(from_path).copied();
                    rel.target_id = directory_lookup.get(to_path).copied();
                }
                RelationshipKind::DirectoryToFile => {
                    rel.source_id = directory_lookup.get(from_path).copied();
                    rel.target_id = file_lookup.get(to_path).copied();
                }
                RelationshipKind::FileToDefinition => {
                    rel.source_id = file_lookup.get(from_path).copied();
                    let target_key = (
                        to_path.to_string(),
                        rel.target_range.byte_offset.0,
                        rel.target_range.byte_offset.1,
                    );
                    rel.target_id = definition_lookup.get(&target_key).copied();
                }
                RelationshipKind::FileToImportedSymbol => {
                    rel.source_id = file_lookup.get(from_path).copied();
                    let target_key = (
                        to_path.to_string(),
                        rel.target_range.byte_offset.0,
                        rel.target_range.byte_offset.1,
                    );
                    rel.target_id = imported_symbol_lookup.get(&target_key).copied();
                }
                RelationshipKind::DefinitionToDefinition => {
                    let (source_start, source_end) =
                        if let Some(def_range) = &rel.source_definition_range {
                            (def_range.byte_offset.0, def_range.byte_offset.1)
                        } else {
                            (
                                rel.source_range.byte_offset.0,
                                rel.source_range.byte_offset.1,
                            )
                        };
                    let (target_start, target_end) =
                        if let Some(def_range) = &rel.target_definition_range {
                            (def_range.byte_offset.0, def_range.byte_offset.1)
                        } else {
                            (
                                rel.target_range.byte_offset.0,
                                rel.target_range.byte_offset.1,
                            )
                        };

                    let source_key = (from_path.to_string(), source_start, source_end);
                    let target_key = (to_path.to_string(), target_start, target_end);
                    rel.source_id = definition_lookup.get(&source_key).copied();
                    rel.target_id = definition_lookup.get(&target_key).copied();
                }
                RelationshipKind::DefinitionToImportedSymbol => {
                    let (source_start, source_end) =
                        if let Some(def_range) = &rel.source_definition_range {
                            (def_range.byte_offset.0, def_range.byte_offset.1)
                        } else {
                            (
                                rel.source_range.byte_offset.0,
                                rel.source_range.byte_offset.1,
                            )
                        };

                    let source_key = (from_path.to_string(), source_start, source_end);
                    let target_key = (
                        to_path.to_string(),
                        rel.target_range.byte_offset.0,
                        rel.target_range.byte_offset.1,
                    );
                    rel.source_id = definition_lookup.get(&source_key).copied();
                    rel.target_id = imported_symbol_lookup.get(&target_key).copied();
                }
                RelationshipKind::ImportedSymbolToImportedSymbol => {
                    let source_key = (
                        from_path.to_string(),
                        rel.source_range.byte_offset.0,
                        rel.source_range.byte_offset.1,
                    );
                    let target_key = (
                        to_path.to_string(),
                        rel.target_range.byte_offset.0,
                        rel.target_range.byte_offset.1,
                    );
                    rel.source_id = imported_symbol_lookup.get(&source_key).copied();
                    rel.target_id = imported_symbol_lookup.get(&target_key).copied();
                }
                RelationshipKind::ImportedSymbolToDefinition => {
                    let source_key = (
                        from_path.to_string(),
                        rel.source_range.byte_offset.0,
                        rel.source_range.byte_offset.1,
                    );
                    let (target_start, target_end) =
                        if let Some(def_range) = &rel.target_definition_range {
                            (def_range.byte_offset.0, def_range.byte_offset.1)
                        } else {
                            (
                                rel.target_range.byte_offset.0,
                                rel.target_range.byte_offset.1,
                            )
                        };
                    let target_key = (to_path.to_string(), target_start, target_end);
                    rel.source_id = imported_symbol_lookup.get(&source_key).copied();
                    rel.target_id = definition_lookup.get(&target_key).copied();
                }
                RelationshipKind::ImportedSymbolToFile => {
                    let source_key = (
                        from_path.to_string(),
                        rel.source_range.byte_offset.0,
                        rel.source_range.byte_offset.1,
                    );
                    rel.source_id = imported_symbol_lookup.get(&source_key).copied();
                    rel.target_id = file_lookup.get(to_path).copied();
                }
                RelationshipKind::Empty => {}
            }
        }
    }

    /// Resolve all relationships into [`ResolvedEdge`]s with concrete
    /// node IDs. Must be called after [`assign_node_ids`](Self::assign_node_ids).
    /// Relationships with unresolved endpoints are silently dropped.
    pub fn resolve_edges(&self) -> Vec<ResolvedEdge> {
        self.relationships
            .iter()
            .filter_map(|rel| {
                let (src_kind, tgt_kind) = rel.kind.source_target_kinds();
                let src_id = self.lookup_node_id(src_kind, rel.source_id)?;
                let tgt_id = self.lookup_node_id(tgt_kind, rel.target_id)?;
                Some(ResolvedEdge {
                    source_id: src_id,
                    source_kind: src_kind,
                    relationship_kind: rel.relationship_type.edge_kind().to_string(),
                    target_id: tgt_id,
                    target_kind: tgt_kind,
                })
            })
            .collect()
    }

    fn lookup_node_id(&self, kind: &str, index: Option<u32>) -> Option<i64> {
        let index = index? as usize;
        match kind {
            "Directory" => self.directory_nodes.get(index).and_then(|n| n.id),
            "File" => self.file_nodes.get(index).and_then(|n| n.id),
            "Definition" => self.definition_nodes.get(index).and_then(|n| n.id),
            "ImportedSymbol" => self.imported_symbol_nodes.get(index).and_then(|n| n.id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryNode {
    #[serde(skip)]
    pub id: Option<i64>,
    pub path: String,
    pub absolute_path: String,
    pub repository_name: String,
    pub name: String,
}

impl DirectoryNode {
    pub fn assign_id(&mut self, project_id: i64, branch: &str) -> i64 {
        let id = compute_id(&[&project_id.to_string(), branch, "dir", &self.path]);
        self.id = Some(id);
        id
    }
}

impl gkg_utils::arrow::AsRecordBatch<RowContext<'_>> for DirectoryNode {
    fn should_include(&self) -> bool {
        self.id.is_some()
    }

    fn write_row(
        &self,
        b: &mut gkg_utils::arrow::BatchBuilder,
        ctx: &RowContext<'_>,
    ) -> Result<(), arrow::error::ArrowError> {
        let id = self
            .id
            .ok_or_else(|| gkg_utils::arrow::missing_id("DirectoryNode"))?;
        b.col("id")?.push_int(id)?;
        b.col("project_id")?.push_int(ctx.project_id)?;
        b.col("branch")?.push_str(ctx.branch)?;
        b.col("commit_sha")?.push_str(ctx.commit_sha)?;
        b.col("path")?.push_str(&self.path)?;
        b.col("name")?.push_str(&self.name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileNode {
    #[serde(skip)]
    pub id: Option<i64>,
    pub path: String,
    pub absolute_path: String,
    pub language: String,
    pub repository_name: String,
    pub extension: String,
    pub name: String,
}

impl FileNode {
    pub fn assign_id(&mut self, project_id: i64, branch: &str) -> i64 {
        let id = compute_id(&[&project_id.to_string(), branch, "file", &self.path]);
        self.id = Some(id);
        id
    }
}

impl gkg_utils::arrow::AsRecordBatch<RowContext<'_>> for FileNode {
    fn should_include(&self) -> bool {
        self.id.is_some()
    }

    fn write_row(
        &self,
        b: &mut gkg_utils::arrow::BatchBuilder,
        ctx: &RowContext<'_>,
    ) -> Result<(), arrow::error::ArrowError> {
        let id = self
            .id
            .ok_or_else(|| gkg_utils::arrow::missing_id("FileNode"))?;
        b.col("id")?.push_int(id)?;
        b.col("project_id")?.push_int(ctx.project_id)?;
        b.col("branch")?.push_str(ctx.branch)?;
        b.col("commit_sha")?.push_str(ctx.commit_sha)?;
        b.col("path")?.push_str(&self.path)?;
        b.col("name")?.push_str(&self.name)?;
        b.col("extension")?.push_str(&self.extension)?;
        b.col("language")?.push_str(&self.language)?;
        Ok(())
    }
}

/// Represents a definition node in the graph
#[derive(Debug, Clone)]
pub struct DefinitionNode {
    pub id: Option<i64>,
    pub fqn: CanonicalFqn,
    pub definition_type: String,
    pub kind: DefKind,
    pub range: Range,
    pub file_path: ArcIntern<String>,
}

impl HasRange for DefinitionNode {
    fn range(&self) -> Range {
        self.range
    }
}

impl DefinitionNode {
    pub fn new(
        fqn: CanonicalFqn,
        definition_type: String,
        kind: DefKind,
        range: Range,
        file_path: ArcIntern<String>,
    ) -> Self {
        Self {
            id: None,
            fqn,
            definition_type,
            kind,
            range,
            file_path,
        }
    }

    pub fn assign_id(&mut self, project_id: i64, branch: &str) -> i64 {
        let fqn_str = self.fqn.to_string();
        let id = compute_id(&[
            &project_id.to_string(),
            branch,
            &self.definition_type,
            &fqn_str,
        ]);
        self.id = Some(id);
        id
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        self.fqn.name()
    }
}

impl gkg_utils::arrow::AsRecordBatch<RowContext<'_>> for DefinitionNode {
    fn should_include(&self) -> bool {
        self.id.is_some()
    }

    fn write_row(
        &self,
        b: &mut gkg_utils::arrow::BatchBuilder,
        ctx: &RowContext<'_>,
    ) -> Result<(), arrow::error::ArrowError> {
        let id = self
            .id
            .ok_or_else(|| gkg_utils::arrow::missing_id("DefinitionNode"))?;
        b.col("id")?.push_int(id)?;
        b.col("project_id")?.push_int(ctx.project_id)?;
        b.col("branch")?.push_str(ctx.branch)?;
        b.col("commit_sha")?.push_str(ctx.commit_sha)?;
        b.col("file_path")?.push_str(self.file_path.as_ref())?;
        b.col("fqn")?.push_str(self.fqn.to_string())?;
        b.col("name")?.push_str(self.fqn.name())?;
        b.col("definition_type")?
            .push_str(self.definition_type.as_str())?;
        b.col("start_line")?
            .push_int(self.range.start.line as i64)?;
        b.col("end_line")?.push_int(self.range.end.line as i64)?;
        b.col("start_byte")?
            .push_int(self.range.byte_offset.0 as i64)?;
        b.col("end_byte")?
            .push_int(self.range.byte_offset.1 as i64)?;
        Ok(())
    }
}

/// Represents a single location where an imported symbol is found
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct ImportedSymbolLocation {
    /// File path where this symbol was imported
    pub file_path: String,
    /// Start byte position in the file
    pub start_byte: i64,
    /// End byte position in the file  
    pub end_byte: i64,
    /// Start line number
    pub start_line: i32,
    /// End line number
    pub end_line: i32,
    /// Start column
    pub start_col: i32,
    /// End column
    pub end_col: i32,
}

impl ImportedSymbolLocation {
    pub fn range(&self) -> Range {
        let start_pos = Position::new(self.start_line as usize, self.start_col as usize);
        let end_pos = Position::new(self.end_line as usize, self.end_col as usize);
        Range::new(
            start_pos,
            end_pos,
            (self.start_byte as usize, self.end_byte as usize),
        )
    }
}

/// Per-language import type. Will be replaced by a canonical string
/// once ImportedSymbolNode is converted to canonical types.
#[derive(Debug, Clone)]
pub enum ImportType {
    Java(JavaImportType),
    Kotlin(KotlinImportType),
    Python(PythonImportType),
    CSharp(CSharpImportType),
    TypeScript(TypeScriptImportType),
    Rust(RustImportType),
}

impl ImportType {
    pub fn as_str(&self) -> &str {
        match self {
            ImportType::Java(t) => t.as_str(),
            ImportType::Kotlin(t) => t.as_str(),
            ImportType::Python(t) => t.as_str(),
            ImportType::CSharp(t) => t.as_str(),
            ImportType::TypeScript(t) => t.as_str(),
            ImportType::Rust(t) => t.as_str(),
        }
    }
}

/// Represents an identifier associated with an imported symbol
#[derive(Debug, Clone)]
pub struct ImportIdentifier {
    /// Original name, e.g. "foo" in `from module import foo as bar`
    pub name: String,
    /// Alias, e.g. "bar" in `from module import foo as bar`
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ImportedSymbolNode {
    pub id: Option<i64>,
    pub import_type: ImportType,
    pub import_path: String,
    pub identifier: Option<ImportIdentifier>,
    pub location: ImportedSymbolLocation,
}

impl ImportedSymbolNode {
    pub fn new(
        import_type: ImportType,
        import_path: String,
        identifier: Option<ImportIdentifier>,
        location: ImportedSymbolLocation,
    ) -> Self {
        Self {
            id: None,
            import_type,
            import_path,
            identifier,
            location,
        }
    }

    pub fn assign_id(&mut self, project_id: i64, branch: &str) -> i64 {
        let identifier_name = self.identifier.as_ref().map(|i| i.name.as_str());
        let identifier_alias = self.identifier.as_ref().and_then(|i| i.alias.as_deref());
        let id = compute_id(&[
            &project_id.to_string(),
            branch,
            &self.location.file_path,
            &self.import_path,
            identifier_name.unwrap_or(""),
            identifier_alias.unwrap_or(""),
        ]);
        self.id = Some(id);
        id
    }
}

impl gkg_utils::arrow::AsRecordBatch<RowContext<'_>> for ImportedSymbolNode {
    fn should_include(&self) -> bool {
        self.id.is_some()
    }

    fn write_row(
        &self,
        b: &mut gkg_utils::arrow::BatchBuilder,
        ctx: &RowContext<'_>,
    ) -> Result<(), arrow::error::ArrowError> {
        let id = self
            .id
            .ok_or_else(|| gkg_utils::arrow::missing_id("ImportedSymbolNode"))?;
        b.col("id")?.push_int(id)?;
        b.col("project_id")?.push_int(ctx.project_id)?;
        b.col("branch")?.push_str(ctx.branch)?;
        b.col("commit_sha")?.push_str(ctx.commit_sha)?;
        b.col("file_path")?.push_str(&self.location.file_path)?;
        b.col("import_type")?.push_str(self.import_type.as_str())?;
        b.col("import_path")?.push_str(&self.import_path)?;
        b.col("identifier_name")?
            .push_opt_str(self.identifier.as_ref().map(|i| &i.name))?;
        b.col("identifier_alias")?
            .push_opt_str(self.identifier.as_ref().and_then(|i| i.alias.as_ref()))?;
        b.col("start_line")?
            .push_int(self.location.start_line as i64)?;
        b.col("end_line")?.push_int(self.location.end_line as i64)?;
        b.col("start_byte")?.push_int(self.location.start_byte)?;
        b.col("end_byte")?.push_int(self.location.end_byte)?;
        Ok(())
    }
}

/// Optimized file tree structure for fast lookups
#[derive(Debug, Clone)]
pub struct OptimizedFileTree {
    /// File paths
    normalized_files: HashMap<String, String>, // Normalized file path -> Original file path
    /// Precomputed root directories
    root_dirs: HashSet<PathBuf>,
    /// Directory structure for efficient path operations
    dirs: HashSet<PathBuf>,
}

impl OptimizedFileTree {
    pub fn new<'a>(files: impl Iterator<Item = &'a String>) -> Self {
        let mut dirs = HashSet::new();
        let mut normalized_files = HashMap::new();

        // Precompute normalized files and directory structure
        for file_path in files {
            normalized_files.insert(file_path.to_lowercase(), file_path.clone());

            let path = Path::new(&file_path);
            if let Some(parent) = path.parent() {
                dirs.insert(parent.to_path_buf());
            }
        }

        // Precompute root directories
        let root_dirs = Self::compute_root_dirs(&normalized_files, &dirs);

        Self {
            normalized_files,
            root_dirs,
            dirs,
        }
    }

    fn compute_root_dirs(
        files: &HashMap<String, String>,
        dirs: &HashSet<PathBuf>,
    ) -> HashSet<PathBuf> {
        let mut root_dirs = HashSet::new();

        // Find the most common root directory (shortest path)
        if let Some(common_root) = dirs.iter().min_by_key(|p| p.as_os_str().len()) {
            root_dirs.insert(common_root.clone());
        }

        // Look for directories that might be package roots (contain __init__.py)
        for (file_path, norm_file_path) in files {
            if norm_file_path.ends_with("__init__.py") {
                let path = Path::new(file_path);
                if let Some(package_dir) = path.parent()
                    && let Some(package_parent) = package_dir.parent()
                {
                    root_dirs.insert(package_parent.to_path_buf());
                }
            }
        }

        root_dirs
    }

    /// Get the original file path if it exists (case-insensitive)
    pub fn get_denormalized_file(&self, norm_file_path: &str) -> Option<&String> {
        self.normalized_files.get(norm_file_path)
    }

    /// Get root directories
    pub fn get_root_dirs(&self) -> &HashSet<PathBuf> {
        &self.root_dirs
    }

    /// Get all directories
    pub fn get_dirs(&self) -> &HashSet<PathBuf> {
        &self.dirs
    }
}
