use crate::graph::RelationshipType;
use internment::ArcIntern;
use parser_core::utils::Range;
use parser_core::{
    csharp::types::{CSharpDefinitionType, CSharpFqn, CSharpFqnPartType, CSharpImportType},
    imports::ImportedSymbolInfo,
};
use std::collections::HashMap;

use crate::analysis::types::{
    ConsolidatedRelationship, DefinitionNode, DefinitionType, FqnType, ImportIdentifier,
    ImportType, ImportedSymbolLocation, ImportedSymbolNode,
};
use crate::parse_types::FileProcessingResult;

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
        definition_map: &mut HashMap<(String, String), (DefinitionNode, FqnType)>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(defs) = file_result.definitions.iter_csharp() {
            for definition in defs {
                let fqn = FqnType::CSharp(definition.fqn.clone());
                let path = ArcIntern::new(relative_file_path.to_string());
                let definition_node = DefinitionNode::new(
                    fqn.clone(),
                    DefinitionType::CSharp(definition.definition_type),
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

                definition_map.insert(
                    key,
                    (
                        definition_node.clone(),
                        FqnType::CSharp(definition.fqn.clone()),
                    ),
                );
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
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        for ((_child_fqn_string, child_file_path), (child_def, child_fqn)) in definition_map {
            if let Some(parent_fqn_string) = self.get_parent_fqn_string(child_fqn)
                && let Some((parent_def, _)) =
                    definition_map.get(&(parent_fqn_string.clone(), child_file_path.to_string()))
                && let Some(relationship_type) = self.get_definition_relationship_type(
                    &parent_def.definition_type,
                    &child_def.definition_type,
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

    fn get_parent_fqn_string(&self, fqn: &FqnType) -> Option<String> {
        match fqn {
            FqnType::CSharp(csharp_fqn) => {
                if csharp_fqn.len() <= 1 {
                    return None;
                }

                let parent_parts: Vec<String> = csharp_fqn[..csharp_fqn.len() - 1]
                    .iter()
                    .map(|part| part.node_name.clone())
                    .collect();

                if parent_parts.is_empty() {
                    None
                } else {
                    Some(parent_parts.join("."))
                }
            }
            _ => None,
        }
    }

    fn get_definition_relationship_type(
        &self,
        parent_type: &DefinitionType,
        child_type: &DefinitionType,
    ) -> Option<RelationshipType> {
        let parent_type = self.simplify_definition_type(parent_type)?;
        let child_type = self.simplify_definition_type(child_type)?;

        match (parent_type, child_type) {
            (
                DefinitionType::CSharp(CSharpDefinitionType::Class),
                DefinitionType::CSharp(CSharpDefinitionType::Class),
            ) => Some(RelationshipType::ClassToClass),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Class),
                DefinitionType::CSharp(CSharpDefinitionType::Interface),
            ) => Some(RelationshipType::ClassToInterface),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Class),
                DefinitionType::CSharp(CSharpDefinitionType::InstanceMethod),
            ) => Some(RelationshipType::ClassToMethod),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Class),
                DefinitionType::CSharp(CSharpDefinitionType::StaticMethod),
            ) => Some(RelationshipType::ClassToMethod),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Class),
                DefinitionType::CSharp(CSharpDefinitionType::Property),
            ) => Some(RelationshipType::ClassToProperty),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Class),
                DefinitionType::CSharp(CSharpDefinitionType::Constructor),
            ) => Some(RelationshipType::ClassToConstructor),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Class),
                DefinitionType::CSharp(CSharpDefinitionType::Enum),
            ) => Some(RelationshipType::ClassToClass),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Class),
                DefinitionType::CSharp(CSharpDefinitionType::Lambda),
            ) => Some(RelationshipType::ClassToLambda),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Interface),
                DefinitionType::CSharp(CSharpDefinitionType::Interface),
            ) => Some(RelationshipType::InterfaceToInterface),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Interface),
                DefinitionType::CSharp(CSharpDefinitionType::Class),
            ) => Some(RelationshipType::InterfaceToClass),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Interface),
                DefinitionType::CSharp(CSharpDefinitionType::InstanceMethod),
            ) => Some(RelationshipType::InterfaceToMethod),
            (
                DefinitionType::CSharp(CSharpDefinitionType::Interface),
                DefinitionType::CSharp(CSharpDefinitionType::Property),
            ) => Some(RelationshipType::InterfaceToProperty),
            (
                DefinitionType::CSharp(CSharpDefinitionType::InstanceMethod),
                DefinitionType::CSharp(CSharpDefinitionType::InstanceMethod),
            ) => Some(RelationshipType::MethodToMethod),
            (
                DefinitionType::CSharp(CSharpDefinitionType::StaticMethod),
                DefinitionType::CSharp(CSharpDefinitionType::StaticMethod),
            ) => Some(RelationshipType::MethodToMethod),
            (
                DefinitionType::CSharp(CSharpDefinitionType::InstanceMethod),
                DefinitionType::CSharp(CSharpDefinitionType::Lambda),
            ) => Some(RelationshipType::MethodToLambda),
            (
                DefinitionType::CSharp(CSharpDefinitionType::StaticMethod),
                DefinitionType::CSharp(CSharpDefinitionType::Lambda),
            ) => Some(RelationshipType::MethodToLambda),
            _ => None,
        }
    }

    fn simplify_definition_type(&self, definition_type: &DefinitionType) -> Option<DefinitionType> {
        match definition_type {
            DefinitionType::CSharp(CSharpDefinitionType::Class) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Class))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Struct) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Class))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Record) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Class))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Enum) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Class))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Interface) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Interface))
            }
            DefinitionType::CSharp(CSharpDefinitionType::InstanceMethod) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::InstanceMethod))
            }
            DefinitionType::CSharp(CSharpDefinitionType::StaticMethod) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::StaticMethod))
            }
            DefinitionType::CSharp(CSharpDefinitionType::ExtensionMethod) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::StaticMethod))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Property) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Property))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Constructor) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Constructor))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Lambda) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Lambda))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Field) => None,
            DefinitionType::CSharp(CSharpDefinitionType::Delegate) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Class))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Finalizer) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::InstanceMethod))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Operator) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::StaticMethod))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Indexer) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Property))
            }
            DefinitionType::CSharp(CSharpDefinitionType::Event) => None,
            DefinitionType::CSharp(CSharpDefinitionType::AnonymousType) => {
                Some(DefinitionType::CSharp(CSharpDefinitionType::Class))
            }
            _ => None,
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
