use std::collections::HashMap;

use crate::graph::RelationshipType;
use crate::{
    analysis::types::{
        ConsolidatedRelationship, DefinitionNode, DefinitionType, FqnType, ImportIdentifier,
        ImportType, ImportedSymbolLocation, ImportedSymbolNode,
    },
    parsing::processor::FileProcessingResult,
};
use internment::ArcIntern;
use parser_core::{
    go::types::{GoDefinitionType, GoFqn, GoImportedSymbolInfo, GoReferenceInfo, GoReferenceType},
    utils::Range,
};

pub struct GoAnalyzer;

impl Default for GoAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl GoAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Process definitions from a file result and update the definitions map
    pub fn process_definitions(
        &self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        definition_map: &mut HashMap<(String, String), (DefinitionNode, FqnType)>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(defs) = file_result.definitions.iter_go() {
            for definition in defs {
                let fqn = FqnType::Go(definition.fqn.clone());
                let path = ArcIntern::new(relative_file_path.to_string());
                let definition_node = DefinitionNode::new(
                    fqn.clone(),
                    DefinitionType::Go(definition.definition_type),
                    definition.range,
                    path.clone(),
                );

                let key = (fqn.to_string(), relative_file_path.to_string());

                if definition_map.contains_key(&key) {
                    log::warn!(
                        "Duplicate definition found for Go: {} in file {}",
                        definition.name,
                        relative_file_path
                    );
                    continue;
                }

                definition_map.insert(
                    key,
                    (definition_node.clone(), FqnType::Go(definition.fqn.clone())),
                );

                let mut relationship =
                    ConsolidatedRelationship::file_to_definition(path.clone(), path.clone());
                relationship.relationship_type = RelationshipType::FileDefines;
                relationship.source_range = ArcIntern::new(Range::empty());
                relationship.target_range = ArcIntern::new(definition.range);
                relationships.push(relationship);
            }
        }
    }

    /// Process imported symbols from a file result and update the import map
    pub fn process_imports(
        &self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        imported_symbol_map: &mut HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(imports) = file_result.imported_symbols.as_ref()
            && let Some(go_imports) = imports.iter_go()
        {
            for import in go_imports {
                let location = self.create_import_location(import, relative_file_path);
                let identifier = self.create_import_identifier(import);

                let imported_symbol_node = ImportedSymbolNode::new(
                    ImportType::Go(import.import_type),
                    import.import_path.clone(),
                    identifier,
                    location.clone(),
                );

                let import_key = import
                    .identifier
                    .as_ref()
                    .map(|id| id.name.clone())
                    .unwrap_or_else(|| import.import_path.clone());

                let key = (import_key, relative_file_path.to_string());
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

    /// Process references from Go files and create call-graph relationships
    pub fn process_references(
        &self,
        references: &[GoReferenceInfo],
        relative_file_path: &str,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        for reference in references {
            // Try to find any definition whose short name matches the referenced symbol
            let matching_def = definition_map.iter().find(|((fqn_str, file_path), _)| {
                // Match the last segment of the FQN against the reference name
                let last_segment = fqn_str.split('.').next_back().unwrap_or(fqn_str);
                last_segment == reference.name && file_path == relative_file_path
            });

            if let Some((_, (def_node, _))) = matching_def {
                let mut rel = ConsolidatedRelationship::definition_to_definition(
                    ArcIntern::new(relative_file_path.to_string()),
                    def_node.file_path.clone(),
                );
                let rel_type = match reference.reference_type {
                    GoReferenceType::MethodCall
                    | GoReferenceType::FunctionCall
                    | GoReferenceType::StructInstantiation => RelationshipType::Calls,
                };
                rel.relationship_type = rel_type;
                rel.source_range = ArcIntern::new(reference.range);
                rel.target_range = ArcIntern::new(def_node.range);
                rel.target_definition_range = Some(ArcIntern::new(def_node.range));
                relationships.push(rel);
            }
        }
    }

    /// Add definition-to-definition relationships (struct contains method, etc.)
    pub fn add_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        let go_definitions: Vec<_> = definition_map
            .iter()
            .filter_map(|((_, _), (node, fqn_type))| {
                if let FqnType::Go(fqn) = fqn_type {
                    Some((node, fqn))
                } else {
                    None
                }
            })
            .collect();

        for (node, fqn) in &go_definitions {
            self.create_go_nested_relationships(node, fqn, &go_definitions, relationships);
        }
    }

    fn create_go_nested_relationships(
        &self,
        node: &DefinitionNode,
        fqn: &GoFqn,
        all_definitions: &[(&DefinitionNode, &GoFqn)],
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        // Methods have a receiver type — link method to its struct/interface parent
        if let Some(receiver) = &fqn.receiver {
            // Build the FQN for the parent (struct/interface)
            let parent_fqn_str = match &fqn.package {
                Some(pkg) => format!("{}.{}", pkg, receiver),
                None => receiver.clone(),
            };

            if let Some((parent_node, _)) = all_definitions
                .iter()
                .find(|(def_node, _)| def_node.fqn.to_string() == parent_fqn_str)
            {
                let relationship_type = match parent_node.definition_type {
                    DefinitionType::Go(GoDefinitionType::Struct) => {
                        Some(RelationshipType::ClassToMethod)
                    }
                    DefinitionType::Go(GoDefinitionType::Interface) => {
                        Some(RelationshipType::InterfaceToMethod)
                    }
                    _ => Some(RelationshipType::ClassToMethod),
                };

                if let Some(rel_type) = relationship_type {
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

    fn create_import_location(
        &self,
        import: &GoImportedSymbolInfo,
        file_path: &str,
    ) -> ImportedSymbolLocation {
        ImportedSymbolLocation {
            file_path: file_path.to_string(),
            start_line: import.range.start.line as i32,
            start_col: import.range.start.column as i32,
            end_line: import.range.end.line as i32,
            end_col: import.range.end.column as i32,
            start_byte: import.range.byte_offset.0 as i64,
            end_byte: import.range.byte_offset.1 as i64,
        }
    }

    fn create_import_identifier(&self, import: &GoImportedSymbolInfo) -> Option<ImportIdentifier> {
        import.identifier.as_ref().map(|id| ImportIdentifier {
            name: id.name.clone(),
            alias: id.alias.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsing::processor::FileProcessor;

    #[test]
    fn test_go_analyzer_processes_definitions() {
        let code = r#"
package main

import "fmt"

func Hello() string {
    fmt.Println("Hello")
    return "hello"
}
"#;
        let processor = FileProcessor::new("main.go".to_string(), code);
        let result = processor.process();
        assert!(result.is_success(), "Go file should process successfully");

        if let crate::parsing::processor::ProcessingResult::Success(file_result) = result {
            let mut definition_map = HashMap::new();
            let mut relationships = Vec::new();
            let analyzer = GoAnalyzer::new();
            analyzer.process_definitions(
                &file_result,
                "main.go",
                &mut definition_map,
                &mut relationships,
            );
            assert!(
                !definition_map.is_empty(),
                "Should have at least one definition"
            );
        }
    }

    #[test]
    fn test_go_analyzer_processes_imports() {
        let code = r#"
package main

import (
    "fmt"
    "net/http"
)

func main() {}
"#;
        let processor = FileProcessor::new("main.go".to_string(), code);
        let result = processor.process();
        assert!(result.is_success());

        if let crate::parsing::processor::ProcessingResult::Success(file_result) = result {
            let mut imported_symbol_map = HashMap::new();
            let mut relationships = Vec::new();
            let mut definition_map = HashMap::new();
            let analyzer = GoAnalyzer::new();
            analyzer.process_definitions(
                &file_result,
                "main.go",
                &mut definition_map,
                &mut relationships,
            );
            analyzer.process_imports(
                &file_result,
                "main.go",
                &mut imported_symbol_map,
                &mut relationships,
            );
            assert!(!imported_symbol_map.is_empty(), "Should have imports");
        }
    }
}
