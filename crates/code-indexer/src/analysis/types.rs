use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use internment::ArcIntern;

use crate::graph::{RelationshipKind, RelationshipType};
use parser_core::{
    csharp::types::{CSharpDefinitionType, CSharpFqn, CSharpImportType},
    definitions::DefinitionTypeInfo,
    imports::ImportTypeInfo,
    java::ast::java_fqn_to_string,
    java::types::{JavaDefinitionType, JavaFqn, JavaImportType},
    kotlin::ast::kotlin_fqn_to_string,
    kotlin::types::{KotlinDefinitionType, KotlinFqn, KotlinImportType},
    python::fqn::python_fqn_to_string,
    python::types::{PythonDefinitionType, PythonFqn, PythonImportType},
    ruby::{
        fqn::ruby_fqn_to_string,
        types::{RubyDefinitionType, RubyFqn},
    },
    rust::fqn::rust_fqn_to_string,
    rust::types::{RustDefinitionType, RustFqn, RustImportType},
    typescript::ast::typescript_fqn_to_string,
    typescript::types::{TypeScriptDefinitionType, TypeScriptFqn, TypeScriptImportType},
    utils::{HasRange, Position, Range},
};
use serde::{Deserialize, Serialize};

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
    pub fn assign_node_ids(&mut self) {
        // Build lookup maps: (file_path, start_byte, end_byte) -> index
        let mut directory_ids: HashMap<String, u32> = HashMap::new();
        let mut file_ids: HashMap<String, u32> = HashMap::new();
        let mut definition_ids: HashMap<(String, usize, usize), u32> = HashMap::new();
        let mut imported_symbol_ids: HashMap<(String, usize, usize), u32> = HashMap::new();

        // Assign directory IDs (using path as key)
        for (idx, dir_node) in self.directory_nodes.iter().enumerate() {
            directory_ids.insert(dir_node.path.clone(), idx as u32);
        }

        // Assign file IDs (using path as key)
        for (idx, file_node) in self.file_nodes.iter().enumerate() {
            file_ids.insert(file_node.path.clone(), idx as u32);
        }

        // Assign definition IDs (using file_path + byte range as key)
        for (idx, def_node) in self.definition_nodes.iter().enumerate() {
            let key = (
                def_node.file_path.as_ref().clone(),
                def_node.range.byte_offset.0,
                def_node.range.byte_offset.1,
            );
            definition_ids.insert(key, idx as u32);
        }

        // Assign imported symbol IDs (using file_path + byte range as key)
        for (idx, import_node) in self.imported_symbol_nodes.iter().enumerate() {
            let key = (
                import_node.location.file_path.clone(),
                import_node.location.start_byte as usize,
                import_node.location.end_byte as usize,
            );
            imported_symbol_ids.insert(key, idx as u32);
        }

        // Now assign source_id and target_id on relationships
        for rel in &mut self.relationships {
            let from_path = rel.source_path.as_ref().map(|p| p.as_ref().as_str());
            let to_path = rel.target_path.as_ref().map(|p| p.as_ref().as_str());

            let (Some(from_path), Some(to_path)) = (from_path, to_path) else {
                continue;
            };

            match rel.kind {
                RelationshipKind::DirectoryToDirectory => {
                    rel.source_id = directory_ids.get(from_path).copied();
                    rel.target_id = directory_ids.get(to_path).copied();
                }
                RelationshipKind::DirectoryToFile => {
                    rel.source_id = directory_ids.get(from_path).copied();
                    rel.target_id = file_ids.get(to_path).copied();
                }
                RelationshipKind::FileToDefinition => {
                    rel.source_id = file_ids.get(from_path).copied();
                    let target_key = (
                        to_path.to_string(),
                        rel.target_range.byte_offset.0,
                        rel.target_range.byte_offset.1,
                    );
                    rel.target_id = definition_ids.get(&target_key).copied();
                }
                RelationshipKind::FileToImportedSymbol => {
                    rel.source_id = file_ids.get(from_path).copied();
                    let target_key = (
                        to_path.to_string(),
                        rel.target_range.byte_offset.0,
                        rel.target_range.byte_offset.1,
                    );
                    rel.target_id = imported_symbol_ids.get(&target_key).copied();
                }
                RelationshipKind::DefinitionToDefinition => {
                    // Use definition_range if available, otherwise fall back to source/target range
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
                    rel.source_id = definition_ids.get(&source_key).copied();
                    rel.target_id = definition_ids.get(&target_key).copied();
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
                    rel.source_id = definition_ids.get(&source_key).copied();
                    rel.target_id = imported_symbol_ids.get(&target_key).copied();
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
                    rel.source_id = imported_symbol_ids.get(&source_key).copied();
                    rel.target_id = imported_symbol_ids.get(&target_key).copied();
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
                    rel.source_id = imported_symbol_ids.get(&source_key).copied();
                    rel.target_id = definition_ids.get(&target_key).copied();
                }
                RelationshipKind::ImportedSymbolToFile => {
                    let source_key = (
                        from_path.to_string(),
                        rel.source_range.byte_offset.0,
                        rel.source_range.byte_offset.1,
                    );
                    rel.source_id = imported_symbol_ids.get(&source_key).copied();
                    rel.target_id = file_ids.get(to_path).copied();
                }
                RelationshipKind::Empty => {}
            }
        }
    }
}

/// Represents a directory node in the graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryNode {
    /// Relative path from repository root
    pub path: String,
    /// Absolute path on filesystem
    pub absolute_path: String,
    /// Repository name
    pub repository_name: String,
    /// Directory name (last component of path)
    pub name: String,
}

/// Represents a file node in the graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileNode {
    /// Relative path from repository root
    pub path: String,
    /// Absolute path on filesystem
    pub absolute_path: String,
    /// Programming language detected
    pub language: String,
    /// Repository name
    pub repository_name: String,
    /// File extension
    pub extension: String,
    /// File name (last component of path)
    pub name: String,
}

/// Represents a language-specific definition type (e.g. class, module, method, etc.)
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum DefinitionType {
    Ruby(RubyDefinitionType),
    Python(PythonDefinitionType),
    Kotlin(KotlinDefinitionType),
    Java(JavaDefinitionType),
    CSharp(CSharpDefinitionType),
    TypeScript(TypeScriptDefinitionType),
    Rust(RustDefinitionType),
    Unsupported(),
}

impl DefinitionType {
    pub fn as_str(&self) -> &str {
        match self {
            DefinitionType::Ruby(ruby_type) => ruby_type.as_str(),
            DefinitionType::Python(python_type) => python_type.as_str(),
            DefinitionType::Kotlin(kotlin_type) => kotlin_type.as_str(),
            DefinitionType::Java(java_type) => java_type.as_str(),
            DefinitionType::CSharp(csharp_type) => csharp_type.as_str(),
            DefinitionType::TypeScript(typescript_type) => typescript_type.as_str(),
            DefinitionType::Rust(rust_type) => rust_type.as_str(),
            DefinitionType::Unsupported() => "unsupported",
        }
    }
}

/// Represents a language-specific FQN type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FqnType {
    Ruby(RubyFqn),
    Python(PythonFqn),
    Kotlin(KotlinFqn),
    Java(JavaFqn),
    CSharp(CSharpFqn),
    TypeScript(TypeScriptFqn),
    Rust(RustFqn),
}

impl std::fmt::Display for FqnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FqnType::Ruby(ruby_type) => write!(f, "{}", ruby_fqn_to_string(ruby_type)),
            FqnType::Python(python_type) => write!(f, "{}", python_fqn_to_string(python_type)),
            FqnType::Kotlin(kotlin_type) => write!(f, "{}", kotlin_fqn_to_string(kotlin_type)),
            FqnType::Java(java_type) => write!(f, "{}", java_fqn_to_string(java_type)),
            FqnType::CSharp(csharp_type) => write!(
                f,
                "{}",
                csharp_type
                    .iter()
                    .map(|part| part.node_name.as_str())
                    .collect::<Vec<_>>()
                    .join(".")
            ),
            FqnType::TypeScript(typescript_type) => {
                write!(f, "{}", typescript_fqn_to_string(typescript_type))
            }
            FqnType::Rust(rust_type) => write!(f, "{}", rust_fqn_to_string(rust_type)),
        }
    }
}

impl FqnType {
    #[inline(always)]
    pub fn name(&self) -> &str {
        match self {
            FqnType::Ruby(ruby_type) => ruby_type.parts.last().unwrap().node_name(),
            FqnType::Python(python_type) => python_type.parts.last().unwrap().node_name(),
            FqnType::Kotlin(kotlin_type) => kotlin_type.last().unwrap().node_name(),
            FqnType::Java(java_type) => java_type.last().unwrap().node_name(),
            FqnType::CSharp(csharp_type) => csharp_type.last().unwrap().node_name(),
            FqnType::TypeScript(typescript_type) => typescript_type.last().unwrap().node_name(),
            FqnType::Rust(rust_type) => rust_type.parts.last().unwrap().node_name(),
        }
    }
}
/// Represents a definition node in the graph
#[derive(Debug, Clone)]
pub struct DefinitionNode {
    /// Fully qualified name (unique identifier)
    pub fqn: FqnType,
    /// Type of definition
    pub definition_type: DefinitionType,
    // Lines, cols, byte offsets
    pub range: Range,
    // File location of the definition
    pub file_path: ArcIntern<String>,
}

impl HasRange for DefinitionNode {
    fn range(&self) -> Range {
        self.range
    }
}

impl DefinitionNode {
    /// Create a new DefinitionNode
    pub fn new(
        fqn: FqnType,
        definition_type: DefinitionType,
        range: Range,
        file_path: ArcIntern<String>,
    ) -> Self {
        Self {
            fqn,
            definition_type,
            range,
            file_path,
        }
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        self.fqn.name()
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

/// Represents a language-specific import type
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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
            ImportType::Java(java_type) => java_type.as_str(),
            ImportType::Kotlin(kotlin_type) => kotlin_type.as_str(),
            ImportType::Python(python_type) => python_type.as_str(),
            ImportType::CSharp(csharp_type) => csharp_type.as_str(),
            ImportType::TypeScript(typescript_type) => typescript_type.as_str(),
            ImportType::Rust(rust_type) => rust_type.as_str(),
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

/// Represents an imported symbol node in the graph
#[derive(Debug, Clone)]
pub struct ImportedSymbolNode {
    /// Language-specific type of import (regular, from, aliased, wildcard, etc.)
    pub import_type: ImportType,
    /// The import path as specified in the source code
    /// e.g., "./my_module", "react", "../utils"
    pub import_path: String,
    /// Information about the imported identifier(s)
    /// None for side-effect imports like `import "./styles.css"`
    pub identifier: Option<ImportIdentifier>,
    /// Location of the enclosing import statement
    pub location: ImportedSymbolLocation,
}

impl ImportedSymbolNode {
    /// Create a new ImportedSymbolNode
    pub fn new(
        import_type: ImportType,
        import_path: String,
        identifier: Option<ImportIdentifier>,
        location: ImportedSymbolLocation,
    ) -> Self {
        Self {
            import_type,
            import_path,
            identifier,
            location,
        }
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
