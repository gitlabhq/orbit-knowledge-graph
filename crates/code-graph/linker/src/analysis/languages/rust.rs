use crate::analysis::canonical_helpers::fqn_parts_to_canonical;
use crate::analysis::types::{
    ConsolidatedRelationship, DefinitionNode, ImportIdentifier, ImportType, ImportedSymbolLocation,
    ImportedSymbolNode,
};
use crate::graph::RelationshipType;
use crate::parse_types::FileProcessingResult;
use code_graph_types::{Language, Range, ToCanonical};
use internment::ArcIntern;
use parser_core::definitions::DefinitionTypeInfo;
use parser_core::rust::{fqn::rust_fqn_to_string, imports::RustImportedSymbolInfo, types::RustFqn};
use smallvec::SmallVec;
use std::collections::HashMap;

// Handles Rust-specific analysis operations
pub struct RustAnalyzer;

impl Default for RustAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl RustAnalyzer {
    /// Create a new Rust analyzer
    pub fn new() -> Self {
        Self
    }

    /// Process definitions from a file result and update the definitions map
    pub fn process_definitions(
        &self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        definition_map: &mut HashMap<(String, String), DefinitionNode>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(defs) = file_result.definitions.iter_rust() {
            for definition in defs {
                let fqn = fqn_parts_to_canonical(&definition.fqn.parts, Language::Rust);
                let path = ArcIntern::new(relative_file_path.to_string());
                let definition_node = DefinitionNode::new(
                    fqn.clone(),
                    definition.definition_type.as_str().to_string(),
                    definition.definition_type.to_def_kind(),
                    definition.range,
                    path.clone(),
                );

                let key = (fqn.to_string(), relative_file_path.to_string());

                if definition_map.contains_key(&key) {
                    log::warn!(
                        "Duplicate definition found for Rust: {} in file {}",
                        definition.name,
                        relative_file_path
                    );
                    continue;
                }

                definition_map.insert(key, definition_node);
                let mut relationship =
                    ConsolidatedRelationship::file_to_definition(path.clone(), path.clone());
                relationship.relationship_type = RelationshipType::FileDefines;
                relationship.source_range = ArcIntern::new(Range::empty());
                relationship.target_range = ArcIntern::new(definition.range);
                relationships.push(relationship);
            }
        }
    }

    /// Process imports from a file result and update the imports map
    pub fn process_imports(
        &self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        imported_symbol_map: &mut HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(imports) = file_result.imported_symbols.as_ref()
            && let Some(rust_imports) = imports.iter_rust()
        {
            for import in rust_imports {
                if let Ok(Some((location, import_fqn))) =
                    self.create_import_location(import, relative_file_path)
                {
                    let import_fqn_string = rust_fqn_to_string(&import_fqn);
                    let imported_symbol_node = ImportedSymbolNode::new(
                        ImportType::Rust(import.import_type),
                        import.import_path.clone(),
                        Some(self.create_import_identifier(import)),
                        location.clone(),
                    );

                    let key = (import_fqn_string, relative_file_path.to_string());
                    imported_symbol_map
                        .entry(key)
                        .or_default()
                        .push(imported_symbol_node.clone());

                    let mut relationship = ConsolidatedRelationship::file_to_imported_symbol(
                        ArcIntern::new(relative_file_path.to_string()),
                        ArcIntern::new(location.file_path.clone()),
                    );
                    relationship.relationship_type = RelationshipType::FileImports;
                    relationship.source_range = ArcIntern::new(Range::empty());
                    relationship.target_range = ArcIntern::new(location.range());
                    relationships.push(relationship);
                }
            }
        }
    }

    /// Add definition relationships for Rust
    pub fn add_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), DefinitionNode>,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        // Handle definition-to-definition relationships
        self.add_rust_definition_relationships(definition_map, relationships);

        // Handle definition-to-imported-symbol relationships (scoped imports)
        self.add_rust_definition_import_relationships(
            definition_map,
            imported_symbol_map,
            relationships,
        );
    }

    /// Create import location from Rust import info
    fn create_import_location(
        &self,
        import: &RustImportedSymbolInfo,
        file_path: &str,
    ) -> Result<Option<(ImportedSymbolLocation, RustFqn)>, String> {
        let location = ImportedSymbolLocation {
            file_path: file_path.to_string(),
            start_line: import.range.start.line as i32,
            start_col: import.range.start.column as i32,
            end_line: import.range.end.line as i32,
            end_col: import.range.end.column as i32,
            start_byte: import.range.byte_offset.0 as i64,
            end_byte: import.range.byte_offset.1 as i64,
        };

        // For Rust imports, we need to construct an FQN from the import information
        let import_fqn = if let Some(scope) = &import.scope {
            scope.clone()
        } else {
            // Create a simple FQN from the import path
            RustFqn::new(SmallVec::new())
        };

        Ok(Some((location, import_fqn)))
    }

    /// Create import identifier from Rust import info
    fn create_import_identifier(&self, import: &RustImportedSymbolInfo) -> ImportIdentifier {
        if let Some(identifier) = &import.identifier {
            ImportIdentifier {
                name: identifier.name.clone(),
                alias: identifier.alias.clone(),
            }
        } else {
            ImportIdentifier {
                name: import.import_path.clone(),
                alias: None,
            }
        }
    }

    /// Add Rust-specific definition-to-imported-symbol relationships (scoped imports)
    fn add_rust_definition_import_relationships(
        &self,
        definition_map: &HashMap<(String, String), DefinitionNode>,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        // Iterate through all definitions to find imports scoped within them
        for ((definition_fqn_string, file_path), definition_node) in definition_map {
            // Look for imports that have this definition's FQN as their scope
            if let Some(imported_symbol_nodes) =
                imported_symbol_map.get(&(definition_fqn_string.clone(), file_path.clone()))
            {
                for imported_symbol in imported_symbol_nodes {
                    // FIXME: add source location for Rust
                    let mut relationship = ConsolidatedRelationship::definition_to_imported_symbol(
                        ArcIntern::new(file_path.clone()),
                        definition_node.file_path.clone(),
                    );
                    relationship.relationship_type = RelationshipType::DefinesImportedSymbol;
                    relationship.source_range = ArcIntern::new(Range::empty());
                    relationship.target_range = ArcIntern::new(imported_symbol.location.range());
                    relationships.push(relationship);
                }
            }
        }
    }

    /// Add Rust-specific definition relationships
    fn add_rust_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), DefinitionNode>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        let rust_definitions: Vec<_> = definition_map.values().collect();

        for node in &rust_definitions {
            self.create_nested_relationships(node, &rust_definitions, relationships);
        }
    }

    fn create_nested_relationships(
        &self,
        node: &DefinitionNode,
        all_definitions: &[&DefinitionNode],
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        let Some(parent_fqn) = node.fqn.parent() else {
            return;
        };
        let parent_fqn_string = parent_fqn.to_string();

        if let Some(parent_node) = all_definitions
            .iter()
            .find(|def| def.fqn.to_string() == parent_fqn_string)
        {
            if let Some(rel_type) = crate::analysis::canonical_helpers::determine_relationship_type(
                parent_node.kind,
                node.kind,
            ) {
                let mut relationship = ConsolidatedRelationship::definition_to_definition(
                    parent_node.file_path.clone(),
                    node.file_path.clone(),
                );
                relationship.relationship_type = rel_type;
                relationship.source_range = ArcIntern::new(parent_node.range);
                relationship.target_range = ArcIntern::new(node.range);
                relationships.push(relationship);
            }
        }
    }
}

impl crate::analysis::analyzer_trait::LanguageAnalyzer for RustAnalyzer {
    fn process_definitions(
        &mut self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        definition_map: &mut HashMap<(String, String), DefinitionNode>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        RustAnalyzer::process_definitions(
            self,
            file_result,
            relative_file_path,
            definition_map,
            relationships,
        );
    }

    fn process_imports(
        &mut self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        imported_symbol_map: &mut HashMap<
            (String, String),
            Vec<crate::analysis::types::ImportedSymbolNode>,
        >,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        RustAnalyzer::process_imports(
            self,
            file_result,
            relative_file_path,
            imported_symbol_map,
            relationships,
        );
    }

    fn add_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), DefinitionNode>,
        imported_symbol_map: &HashMap<
            (String, String),
            Vec<crate::analysis::types::ImportedSymbolNode>,
        >,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        RustAnalyzer::add_definition_relationships(
            self,
            definition_map,
            imported_symbol_map,
            relationships,
        );
    }
}
