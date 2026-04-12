use crate::analysis::types::{ConsolidatedRelationship, DefinitionNode, ImportedSymbolNode};
use crate::parse_types::FileProcessingResult;
use std::collections::HashMap;

/// Common interface for all language analyzers.
///
/// Every analyzer can extract definitions and build definition relationships.
/// Import processing and reference resolution have default no-op implementations
/// since not all languages support them.
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
        // Default: no import processing
    }

    fn add_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), DefinitionNode>,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    );
}
