use crate::analysis::canonical_helpers::fqn_parts_to_canonical;
use crate::analysis::types::{
    ConsolidatedRelationship, DefinitionNode, ImportIdentifier, ImportType,
    ImportedSymbolLocation, ImportedSymbolNode,
};
use crate::graph::{RelationshipKind, RelationshipType};
use crate::parse_types::{FileProcessingResult, References};
use code_graph_types::{Language, Range, ToCanonical};
use internment::ArcIntern;
use parser_core::definitions::DefinitionTypeInfo;
use parser_core::typescript::{
    ast::typescript_fqn_to_string,
    swc::references::types::{TypeScriptReferenceTarget, TypeScriptTargetResolution},
    types::{TypeScriptDefinitionType, TypeScriptImportedSymbolInfo},
};
use std::collections::HashMap;

// Handles Python-specific analysis operations
pub struct TypeScriptAnalyzer;

impl Default for TypeScriptAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeScriptAnalyzer {
    /// Create a new TypeScript analyzer
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
        if let Some(defs) = file_result.definitions.iter_typescript() {
            for definition in defs {
                if definition.definition_type == TypeScriptDefinitionType::Namespace {
                    continue;
                }
                let fqn = fqn_parts_to_canonical(&definition.fqn, Language::TypeScript);
                let path = ArcIntern::new(relative_file_path.to_string());
                let definition_node = DefinitionNode::new(
                    fqn.clone(),
                    definition.definition_type.as_str().to_string(),
                    definition.definition_type.to_def_kind(),
                    definition.range,
                    path.clone(),
                );

                if definition.fqn.len() == 1 {
                    let relationship = ConsolidatedRelationship {
                        source_path: Some(path.clone()),
                        target_path: Some(path.clone()),
                        kind: RelationshipKind::FileToDefinition,
                        relationship_type: RelationshipType::FileDefines,
                        source_range: ArcIntern::new(Range::empty()),
                        target_range: ArcIntern::new(definition.range),
                        ..Default::default()
                    };
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
        &self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        imported_symbol_map: &mut HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(imported_symbols) = &file_result.imported_symbols
            && let Some(imports) = imported_symbols.iter_typescript()
        {
            for imported_symbol in imports {
                let location =
                    self.create_imported_symbol_location(imported_symbol, relative_file_path);
                let identifier = self.create_imported_symbol_identifier(imported_symbol);
                let scope_fqn_string = if let Some(ref scope) = imported_symbol.scope {
                    typescript_fqn_to_string(scope)
                } else {
                    "".to_string()
                };
                let imported_symbol_node = ImportedSymbolNode::new(
                    ImportType::TypeScript(imported_symbol.import_type),
                    imported_symbol.import_path.clone(),
                    identifier,
                    location.clone(),
                );

                if let Some(imported_symbol_nodes) = imported_symbol_map
                    .get_mut(&(scope_fqn_string.clone(), relative_file_path.to_string()))
                {
                    imported_symbol_nodes.push(imported_symbol_node);
                } else {
                    imported_symbol_map.insert(
                        (scope_fqn_string.clone(), relative_file_path.to_string()),
                        vec![imported_symbol_node],
                    );
                }

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

    pub fn process_references(
        &self,
        file_references: &Option<References>,
        relative_file_path: &str,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(analyzer_references) = file_references {
            let iter_refs = analyzer_references.iter_typescript();
            if let Some(iter_refs) = iter_refs {
                for reference in iter_refs {
                    let target_defn = match &reference.target {
                        TypeScriptReferenceTarget::Resolved(target) => target,
                        _ => continue,
                    };
                    let target_defn = match &**target_defn {
                        TypeScriptTargetResolution::Definition(definition) => definition,
                        _ => continue,
                    };

                    let Some(scope) = &reference.scope else {
                        let path = ArcIntern::new(relative_file_path.to_string());
                        let mut relationship = ConsolidatedRelationship::file_to_definition(
                            path.clone(),
                            path.clone(),
                        );
                        relationship.relationship_type = RelationshipType::Calls;
                        relationship.source_range = ArcIntern::new(Range::empty());
                        relationship.target_range = ArcIntern::new(target_defn.range);
                        relationships.push(relationship);
                        continue;
                    };

                    let Some(scope_range) = scope.last().map(|part| part.range) else {
                        continue;
                    };

                    let relationship = ConsolidatedRelationship {
                        source_path: Some(ArcIntern::new(relative_file_path.to_string())),
                        target_path: Some(ArcIntern::new(relative_file_path.to_string())),
                        kind: RelationshipKind::DefinitionToDefinition,
                        relationship_type: RelationshipType::Calls,
                        source_range: ArcIntern::new(reference.range), // Call site location for source_start_line etc
                        target_range: ArcIntern::new(target_defn.range),
                        source_definition_range: Some(ArcIntern::new(scope_range)), // Source definition range for ID lookup
                        target_definition_range: Some(ArcIntern::new(target_defn.range)),
                        ..Default::default()
                    };

                    relationships.push(relationship);
                }
            }
        }
    }

    /// Create definition-to-definition and definition-to-imported-symbol relationships using definitions map
    pub fn add_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), DefinitionNode>,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        for ((child_fqn_string, child_file_path), child_def) in definition_map {
            if let Some(imported_symbol_nodes) =
                imported_symbol_map.get(&(child_fqn_string.clone(), child_file_path.to_string()))
            {
                for imported_symbol in imported_symbol_nodes {
                    let relationship = ConsolidatedRelationship {
                        source_path: Some(ArcIntern::new(child_file_path.to_string())),
                        target_path: Some(ArcIntern::new(
                            imported_symbol.location.file_path.clone(),
                        )),
                        kind: RelationshipKind::DefinitionToImportedSymbol,
                        relationship_type: RelationshipType::DefinesImportedSymbol,
                        source_range: ArcIntern::new(child_def.range),
                        target_range: ArcIntern::new(imported_symbol.location.range()),
                        ..Default::default()
                    };
                    relationships.push(relationship);
                }
            }

            if let Some(parent_fqn) = child_def.fqn.parent()
                && let Some(parent_def) =
                    definition_map.get(&(parent_fqn.to_string(), child_file_path.to_string()))
                && let Some(relationship_type) =
                    crate::analysis::canonical_helpers::determine_relationship_type(
                        parent_def.kind,
                        child_def.kind,
                    )
            {
                let relationship = ConsolidatedRelationship {
                    source_path: Some(parent_def.file_path.clone()),
                    target_path: Some(child_def.file_path.clone()),
                    kind: RelationshipKind::DefinitionToDefinition,
                    relationship_type,
                    source_range: ArcIntern::new(parent_def.range),
                    target_range: ArcIntern::new(child_def.range),
                    ..Default::default()
                };
                relationships.push(relationship);
            }
        }
    }

    fn create_imported_symbol_location(
        &self,
        imported_symbol: &TypeScriptImportedSymbolInfo,
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
        imported_symbol: &TypeScriptImportedSymbolInfo,
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
