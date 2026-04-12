use crate::analysis::canonical_helpers::fqn_parts_to_canonical;
use crate::analysis::types::{
    ConsolidatedRelationship, DefinitionNode, ImportIdentifier, ImportType,
    ImportedSymbolLocation, ImportedSymbolNode,
};
use crate::graph::RelationshipType;
use crate::parse_types::FileProcessingResult;
use code_graph_types::{Language, Range, ToCanonical};
use internment::ArcIntern;
use parser_core::csharp::types::{CSharpFqn, CSharpFqnPartType, CSharpImportType};
use parser_core::definitions::DefinitionTypeInfo;
use parser_core::imports::ImportedSymbolInfo;
use std::collections::HashMap;

#[derive(Default)]
pub struct CSharpAnalyzer;

impl CSharpAnalyzer {
    pub fn new() -> Self {
        Self
    }

    pub fn process_definitions(
        &self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        definition_map: &mut HashMap<(String, String), DefinitionNode>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(defs) = file_result.definitions.iter_csharp() {
            for definition in defs {
                let fqn = fqn_parts_to_canonical(&definition.fqn, Language::CSharp);
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
                        "Duplicate definition found for CSharp: {} in file {}",
                        definition.name,
                        relative_file_path
                    );
                    continue;
                }

                if self.is_top_level_definition(&definition.fqn) {
                    let mut relationship =
                        ConsolidatedRelationship::file_to_definition(path.clone(), path.clone());
                    relationship.relationship_type = RelationshipType::FileDefines;
                    relationship.source_range = ArcIntern::new(Range::empty());
                    relationship.target_range = ArcIntern::new(definition.range);
                    relationships.push(relationship);
                }

                definition_map.insert(key, definition_node);
            }
        }
    }

    pub fn process_imports(
        &self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        imported_symbol_map: &mut HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(imported_symbols) = &file_result.imported_symbols
            && let Some(imports) = imported_symbols.iter_csharp()
        {
            for imported_symbol in imports {
                let location =
                    self.create_imported_symbol_location(imported_symbol, relative_file_path);
                let identifier = self.create_imported_symbol_identifier(imported_symbol);

                let imported_symbol_node = ImportedSymbolNode::new(
                    ImportType::CSharp(imported_symbol.import_type),
                    imported_symbol.import_path.clone(),
                    identifier,
                    location.clone(),
                );

                imported_symbol_map.insert(
                    (
                        imported_symbol.import_path.clone(),
                        relative_file_path.to_string(),
                    ),
                    vec![imported_symbol_node],
                );

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
                    crate::analysis::canonical_helpers::determine_relationship_type(parent_def.kind, child_def.kind)
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

    fn is_top_level_definition(&self, fqn: &CSharpFqn) -> bool {
        fqn.len() == 1 || (fqn.len() == 2 && fqn[0].node_type == CSharpFqnPartType::Namespace)
    }

    fn create_imported_symbol_location(
        &self,
        imported_symbol: &ImportedSymbolInfo<CSharpImportType, CSharpFqn>,
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
        imported_symbol: &ImportedSymbolInfo<CSharpImportType, CSharpFqn>,
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
