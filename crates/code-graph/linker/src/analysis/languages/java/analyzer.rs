use std::collections::HashMap;

use crate::analysis::canonical_helpers::fqn_parts_to_canonical;
use crate::analysis::{
    languages::java::{expression_resolver::ExpressionResolver, utils::full_import_path},
    types::{
        ConsolidatedRelationship, DefinitionNode, ImportIdentifier, ImportedSymbolLocation,
        ImportedSymbolNode,
    },
};
use crate::graph::RelationshipType;
use crate::parse_types::{FileProcessingResult, References};
use code_graph_types::{Language, Range, ToCanonical};
use internment::ArcIntern;
use parser_core::definitions::DefinitionTypeInfo;
use parser_core::imports::ImportTypeInfo;
use parser_core::java::types::{
    JavaDefinitionType, JavaFqn, JavaFqnPartType, JavaImportedSymbolInfo,
};

#[derive(Default)]
pub struct JavaAnalyzer {
    expression_resolver: ExpressionResolver,
}

impl JavaAnalyzer {
    pub fn new() -> Self {
        Self {
            expression_resolver: ExpressionResolver::new(),
        }
    }

    pub fn process_definitions(
        &mut self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        definition_map: &mut HashMap<(String, String), DefinitionNode>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(defs) = file_result.definitions.iter_java() {
            for definition in defs {
                if matches!(definition.definition_type, JavaDefinitionType::Package) {
                    self.expression_resolver
                        .add_file(definition.name.clone(), relative_file_path.to_string());
                    continue;
                }

                let fqn = fqn_parts_to_canonical(&definition.fqn, Language::Java);
                let path = ArcIntern::new(relative_file_path.to_string());
                let definition_node = DefinitionNode::new(
                    fqn.clone(),
                    definition.definition_type.as_str().to_string(),
                    definition.definition_type.to_def_kind(),
                    definition.range,
                    path.clone(),
                );

                self.expression_resolver.add_definition(
                    relative_file_path.to_string(),
                    definition.clone(),
                    definition_node.clone(),
                );

                // We don't want to index local variables, parameters, or fields
                if definition.definition_type == JavaDefinitionType::LocalVariable
                    || definition.definition_type == JavaDefinitionType::Parameter
                    || definition.definition_type == JavaDefinitionType::Field
                {
                    continue;
                }

                // Only add file definition relationship for top-level definitions
                if self.is_top_level_definition(&definition.fqn) {
                    let mut relationship =
                        ConsolidatedRelationship::file_to_definition(path.clone(), path.clone());
                    relationship.relationship_type = RelationshipType::FileDefines;
                    relationship.source_range = ArcIntern::new(Range::empty());
                    relationship.target_range = ArcIntern::new(definition.range);
                    relationships.push(relationship);
                }

                definition_map.insert(
                    (fqn.to_string(), relative_file_path.to_string()),
                    definition_node,
                );
            }
        }
    }

    /// Process imported symbols from a file result and update the import map
    pub fn process_imports(
        &mut self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        imported_symbol_map: &mut HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(imported_symbols) = &file_result.imported_symbols
            && let Some(imports) = imported_symbols.iter_java()
        {
            for imported_symbol in imports {
                let location =
                    self.create_imported_symbol_location(imported_symbol, relative_file_path);
                let identifier = self.create_imported_symbol_identifier(imported_symbol);

                let imported_symbol_node = ImportedSymbolNode::new(
                    imported_symbol.import_type.as_str().to_string(),
                    imported_symbol.import_path.clone(),
                    identifier,
                    location.clone(),
                );

                let (_, full_import_path) = full_import_path(&imported_symbol_node);
                imported_symbol_map.insert(
                    (full_import_path, relative_file_path.to_string()),
                    vec![imported_symbol_node.clone()],
                );

                let mut relationship = ConsolidatedRelationship::file_to_imported_symbol(
                    ArcIntern::new(relative_file_path.to_string()),
                    ArcIntern::new(location.file_path.clone()),
                );
                relationship.relationship_type = RelationshipType::FileImports;
                relationship.source_range = ArcIntern::new(Range::empty());
                relationship.target_range = ArcIntern::new(location.range());
                relationships.push(relationship);

                self.expression_resolver
                    .add_import(relative_file_path.to_string(), &imported_symbol_node);
            }
        }
    }

    /// Process Java references (calls and creations) and create definition relationships
    pub fn process_references(
        &mut self,
        references: &References,
        file_path: &str,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        self.expression_resolver
            .resolve_references(file_path, references, relationships);
    }

    /// Create definition-to-definition relationships using definitions map
    pub fn add_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), DefinitionNode>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        for ((_child_fqn_string, child_file_path), child_def) in definition_map {
            if let Some(parent_fqn) = child_def.fqn.parent()
                && let Some(parent_def) =
                    definition_map.get(&(parent_fqn.to_string(), child_file_path.to_string()))
                && let Some(relationship_type) =
                    crate::analysis::canonical_helpers::determine_relationship_type(
                        parent_def.kind,
                        child_def.kind,
                    )
            {
                let mut relationship = ConsolidatedRelationship::definition_to_definition(
                    parent_def.file_path.clone(),
                    child_def.file_path.clone(),
                );
                relationship.relationship_type = relationship_type;
                relationship.source_range = ArcIntern::new(parent_def.range);
                relationship.target_range = ArcIntern::new(child_def.range);
                relationships.push(relationship);
            }
        }
    }

    fn is_top_level_definition(&self, fqn: &JavaFqn) -> bool {
        fqn.len() == 1 || (fqn.len() == 2 && fqn[0].node_type == JavaFqnPartType::Package)
    }

    /// Create an imported symbol location from an imported symbol info
    fn create_imported_symbol_location(
        &self,
        imported_symbol: &JavaImportedSymbolInfo,
        file_path: &str,
    ) -> ImportedSymbolLocation {
        ImportedSymbolLocation {
            file_path: file_path.to_string(),
            start_byte: imported_symbol.range.byte_offset.0 as i64,
            end_byte: imported_symbol.range.byte_offset.1 as i64,
            start_line: imported_symbol.range.start.line as i32,
            end_line: imported_symbol.range.end.line as i32,
            start_col: imported_symbol.range.start.column as i32,
            end_col: imported_symbol.range.end.column as i32,
        }
    }

    fn create_imported_symbol_identifier(
        &self,
        imported_symbol: &JavaImportedSymbolInfo,
    ) -> Option<ImportIdentifier> {
        if let Some(identifier) = &imported_symbol.identifier {
            return Some(ImportIdentifier {
                name: identifier.name.clone(),
                alias: identifier.alias.clone(),
            });
        }

        None
    }
}

impl crate::analysis::analyzer_trait::LanguageAnalyzer for JavaAnalyzer {
    fn process_definitions(
        &mut self,
        file_result: &crate::parse_types::FileProcessingResult,
        relative_file_path: &str,
        definition_map: &mut std::collections::HashMap<
            (String, String),
            crate::analysis::types::DefinitionNode,
        >,
        relationships: &mut Vec<crate::analysis::types::ConsolidatedRelationship>,
    ) {
        JavaAnalyzer::process_definitions(
            self,
            file_result,
            relative_file_path,
            definition_map,
            relationships,
        );
    }

    fn process_imports(
        &mut self,
        file_result: &crate::parse_types::FileProcessingResult,
        relative_file_path: &str,
        imported_symbol_map: &mut std::collections::HashMap<
            (String, String),
            Vec<crate::analysis::types::ImportedSymbolNode>,
        >,
        relationships: &mut Vec<crate::analysis::types::ConsolidatedRelationship>,
    ) {
        JavaAnalyzer::process_imports(
            self,
            file_result,
            relative_file_path,
            imported_symbol_map,
            relationships,
        );
    }

    fn add_definition_relationships(
        &self,
        definition_map: &std::collections::HashMap<
            (String, String),
            crate::analysis::types::DefinitionNode,
        >,
        _imported_symbol_map: &std::collections::HashMap<
            (String, String),
            Vec<crate::analysis::types::ImportedSymbolNode>,
        >,
        relationships: &mut Vec<crate::analysis::types::ConsolidatedRelationship>,
    ) {
        JavaAnalyzer::add_definition_relationships(self, definition_map, relationships);
    }

    fn process_references(
        &mut self,
        references: &Option<crate::parse_types::References>,
        relative_path: &str,
        _definition_map: &std::collections::HashMap<
            (String, String),
            crate::analysis::types::DefinitionNode,
        >,
        _imported_symbol_map: &std::collections::HashMap<
            (String, String),
            Vec<crate::analysis::types::ImportedSymbolNode>,
        >,
        relationships: &mut Vec<crate::analysis::types::ConsolidatedRelationship>,
        _isis: &std::collections::HashMap<
            crate::analysis::types::ImportedSymbolLocation,
            Vec<crate::analysis::types::ImportedSymbolNode>,
        >,
        _isd: &std::collections::HashMap<
            crate::analysis::types::ImportedSymbolLocation,
            Vec<crate::analysis::types::DefinitionNode>,
        >,
        _isf: &std::collections::HashMap<
            crate::analysis::types::ImportedSymbolLocation,
            Vec<String>,
        >,
    ) {
        if let Some(refs) = references {
            JavaAnalyzer::process_references(self, refs, relative_path, relationships);
        }
    }
}
