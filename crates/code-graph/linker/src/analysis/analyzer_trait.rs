use crate::analysis::types::{
    ConsolidatedRelationship, DefinitionNode, ImportedSymbolLocation, ImportedSymbolNode,
};
use crate::parse_types::{FileProcessingResult, References};
use std::collections::HashMap;

/// Common interface for all language analyzers.
pub trait LanguageAnalyzer {
    fn process_definitions(
        &mut self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        definition_map: &mut HashMap<(String, String), DefinitionNode>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    );

    fn process_imports(
        &mut self,
        _file_result: &FileProcessingResult,
        _relative_file_path: &str,
        _imported_symbol_map: &mut HashMap<(String, String), Vec<ImportedSymbolNode>>,
        _relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
    }

    fn process_references(
        &mut self,
        _references: &Option<References>,
        _relative_path: &str,
        _definition_map: &HashMap<(String, String), DefinitionNode>,
        _imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        _relationships: &mut Vec<ConsolidatedRelationship>,
        _imported_symbol_to_imported_symbols: &HashMap<
            ImportedSymbolLocation,
            Vec<ImportedSymbolNode>,
        >,
        _imported_symbol_to_definitions: &HashMap<ImportedSymbolLocation, Vec<DefinitionNode>>,
        _imported_symbol_to_files: &HashMap<ImportedSymbolLocation, Vec<String>>,
    ) {
    }

    fn add_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), DefinitionNode>,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    );
}
