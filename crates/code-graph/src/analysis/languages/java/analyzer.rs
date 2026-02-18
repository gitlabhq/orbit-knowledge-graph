use std::collections::HashMap;

use crate::graph::RelationshipType;
use parser_core::java::types::{
    JavaDefinitionType, JavaFqn, JavaFqnPartType, JavaImportedSymbolInfo,
};

use crate::{
    analysis::{
        languages::java::{expression_resolver::ExpressionResolver, utils::full_import_path},
        types::{
            ConsolidatedRelationship, DefinitionNode, DefinitionType, FqnType, ImportIdentifier,
            ImportType, ImportedSymbolLocation, ImportedSymbolNode,
        },
    },
    parsing::processor::{FileProcessingResult, References},
};
use internment::ArcIntern;
use parser_core::utils::Range;

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
        definition_map: &mut HashMap<(String, String), (DefinitionNode, FqnType)>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(defs) = file_result.definitions.iter_java() {
            for definition in defs {
                if matches!(definition.definition_type, JavaDefinitionType::Package) {
                    self.expression_resolver
                        .add_file(definition.name.clone(), relative_file_path.to_string());
                    continue;
                }

                let fqn = FqnType::Java(definition.fqn.clone());
                let path = ArcIntern::new(relative_file_path.to_string());
                let definition_node = DefinitionNode::new(
                    fqn.clone(),
                    DefinitionType::Java(definition.definition_type),
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
                    (definition_node, fqn),
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
                    ImportType::Java(imported_symbol.import_type),
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
                // FIXME: https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/177
                // Definition Heirarchy relationships should have their own struct
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
            FqnType::Java(java_fqn) => {
                if java_fqn.len() <= 1 {
                    return None;
                }

                let parent_parts: Vec<String> = java_fqn[..java_fqn.len() - 1]
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
        use JavaDefinitionType::*;

        let parent_type = self.simplify_definition_type(parent_type)?;
        let child_type = self.simplify_definition_type(child_type)?;

        match (parent_type, child_type) {
            // Class relationships
            (DefinitionType::Java(Class), DefinitionType::Java(Class)) => {
                Some(RelationshipType::ClassToClass)
            }
            (DefinitionType::Java(Class), DefinitionType::Java(Interface)) => {
                Some(RelationshipType::ClassToInterface)
            }
            (DefinitionType::Java(Class), DefinitionType::Java(EnumConstant)) => {
                Some(RelationshipType::ClassToEnumEntry)
            }
            (DefinitionType::Java(Class), DefinitionType::Java(Method)) => {
                Some(RelationshipType::ClassToMethod)
            }
            (DefinitionType::Java(Class), DefinitionType::Java(Lambda)) => {
                Some(RelationshipType::ClassToLambda)
            }
            // Interface relationships
            (DefinitionType::Java(Interface), DefinitionType::Java(Interface)) => {
                Some(RelationshipType::InterfaceToInterface)
            }
            (DefinitionType::Java(Interface), DefinitionType::Java(Class)) => {
                Some(RelationshipType::InterfaceToClass)
            }
            (DefinitionType::Java(Interface), DefinitionType::Java(Method)) => {
                Some(RelationshipType::InterfaceToMethod)
            }
            (DefinitionType::Java(Interface), DefinitionType::Java(Lambda)) => {
                Some(RelationshipType::InterfaceToLambda)
            }
            // Method relationships
            (DefinitionType::Java(Method), DefinitionType::Java(Method)) => {
                Some(RelationshipType::MethodToMethod)
            }
            (DefinitionType::Java(Method), DefinitionType::Java(Class)) => {
                Some(RelationshipType::MethodToClass)
            }
            (DefinitionType::Java(Method), DefinitionType::Java(Interface)) => {
                Some(RelationshipType::MethodToInterface)
            }
            (DefinitionType::Java(Method), DefinitionType::Java(Lambda)) => {
                Some(RelationshipType::MethodToLambda)
            }
            // Lambda relationships
            (DefinitionType::Java(Lambda), DefinitionType::Java(Lambda)) => {
                Some(RelationshipType::LambdaToLambda)
            }
            (DefinitionType::Java(Lambda), DefinitionType::Java(Class)) => {
                Some(RelationshipType::LambdaToClass)
            }
            (DefinitionType::Java(Lambda), DefinitionType::Java(Method)) => {
                Some(RelationshipType::LambdaToMethod)
            }
            (DefinitionType::Java(Lambda), DefinitionType::Java(Interface)) => {
                Some(RelationshipType::LambdaToInterface)
            }
            _ => None,
        }
    }

    fn simplify_definition_type(&self, definition_type: &DefinitionType) -> Option<DefinitionType> {
        use JavaDefinitionType::*;

        match definition_type {
            DefinitionType::Java(Class) => Some(DefinitionType::Java(Class)),
            DefinitionType::Java(Enum) => Some(DefinitionType::Java(Class)),
            DefinitionType::Java(AnnotationDeclaration) => Some(DefinitionType::Java(Class)),
            DefinitionType::Java(Record) => Some(DefinitionType::Java(Class)),
            DefinitionType::Java(Interface) => Some(DefinitionType::Java(Interface)),
            DefinitionType::Java(EnumConstant) => Some(DefinitionType::Java(EnumConstant)),
            DefinitionType::Java(Method) => Some(DefinitionType::Java(Method)),
            DefinitionType::Java(Constructor) => Some(DefinitionType::Java(Method)),
            DefinitionType::Java(Lambda) => Some(DefinitionType::Java(Lambda)),
            _ => None,
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
