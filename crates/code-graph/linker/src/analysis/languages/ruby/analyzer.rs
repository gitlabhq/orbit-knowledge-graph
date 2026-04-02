//! Main Ruby analyzer orchestrating the semantic analysis process.
//!
//! This module implements the central [`RubyAnalyzer`] that coordinates the two-phase
//! Ruby code analysis process, transforming parsed structural data into a semantic
//! Knowledge Graph with accurate cross-references.

use crate::analysis::types::{ConsolidatedRelationship, DefinitionNode, DefinitionType, FqnType};
use crate::graph::RelationshipType;
use crate::parse_types::{FileProcessingResult, References};
use internment::ArcIntern;
use parser_core::utils::Range;
use parser_core::{
    references::ReferenceInfo,
    ruby::{
        references::types::{RubyExpressionMetadata, RubyReferenceType, RubyTargetResolution},
        types::{RubyDefinitionType, RubyFqn},
    },
};
use std::collections::HashMap;

// Import the new Ruby-specific analyzers
use super::ExpressionResolver;

pub type RubyReference =
    ReferenceInfo<RubyTargetResolution, RubyReferenceType, RubyExpressionMetadata, RubyFqn>;

pub struct RubyAnalyzer {
    expression_resolver: Option<ExpressionResolver>,
    stats: AnalyzerStats,
}

#[derive(Debug, Default)]
pub struct AnalyzerStats {
    pub definitions_processed: usize,
    pub references_processed: usize,
    pub relationships_created: usize,
}

impl Default for RubyAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl RubyAnalyzer {
    pub fn new() -> Self {
        Self {
            expression_resolver: Some(ExpressionResolver::new()),
            stats: AnalyzerStats::default(),
        }
    }

    pub fn get_stats(&self) -> &AnalyzerStats {
        &self.stats
    }

    pub fn process_definitions(
        &mut self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        definition_map: &mut HashMap<(String, String), (DefinitionNode, FqnType)>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) -> Result<(), String> {
        if let Some(defs) = file_result.definitions.iter_ruby() {
            for definition in defs {
                // Process all definition types including modules for better scope resolution
                // Modules provide namespace context that's important for method resolution
                let fqn = FqnType::Ruby(definition.fqn.clone());
                let path = ArcIntern::new(relative_file_path.to_string());
                let definition_node = DefinitionNode::new(
                    fqn.clone(),
                    DefinitionType::Ruby(definition.definition_type),
                    definition.range,
                    path.clone(),
                );

                let key = (fqn.to_string(), relative_file_path.to_string());

                if definition_map.contains_key(&key) {
                    log::warn!(
                        "Duplicate definition found for Ruby: {} in file {}",
                        definition.name,
                        relative_file_path
                    );
                    continue;
                }

                definition_map.insert(
                    key,
                    (
                        definition_node.clone(),
                        FqnType::Ruby(definition.fqn.clone()),
                    ),
                );
                let mut relationship =
                    ConsolidatedRelationship::file_to_definition(path.clone(), path.clone());
                relationship.relationship_type = RelationshipType::FileDefines;
                relationship.source_range = ArcIntern::new(Range::empty());
                relationship.target_range = ArcIntern::new(definition.range);
                relationships.push(relationship);

                // Add definition to expression resolver if available
                if let Some(ref mut resolver) = self.expression_resolver {
                    resolver.add_definition(
                        fqn.to_string(),
                        definition_node,
                        &FqnType::Ruby(definition.fqn.clone()),
                    );
                }

                self.stats.definitions_processed += 1;
            }
        }

        Ok(())
    }

    /// Create definition-to-definition relationships using definitions map
    pub fn add_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        for ((_child_fqn_string, child_file_path), (child_def, child_fqn)) in definition_map {
            // Find parent definition by using FQN parts directly
            if let Some(parent_fqn_string) = self.get_parent_fqn_from_parts(child_fqn)
                && let Some((parent_def, _)) =
                    definition_map.get(&(parent_fqn_string.clone(), child_file_path.clone()))
            {
                // Determine relationship type based on parent and child types
                if let Some(relationship_type) = self.get_definition_relationship_type(
                    &parent_def.definition_type,
                    &child_def.definition_type,
                ) {
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
    }

    /// Processes Ruby references and creates call relationships in the Knowledge Graph.
    pub fn process_references(
        &mut self,
        references: &References,
        file_path: &str,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(ref mut resolver) = self.expression_resolver {
            let initial_count = relationships.len();

            resolver.process_references(references, file_path, relationships);

            let new_relationships = relationships.len() - initial_count;

            self.stats.references_processed += new_relationships;
            self.stats.relationships_created += new_relationships;
        }
    }

    /// Extract parent FQN from a RubyFqn by working with parts directly (more efficient)
    fn get_parent_fqn_from_parts(&self, fqn: &FqnType) -> Option<String> {
        match fqn {
            FqnType::Ruby(ruby_fqn) => {
                if ruby_fqn.parts.len() <= 1 {
                    // No parent if FQN has only one part or is empty
                    return None;
                }

                // Take all parts except the last one to create parent FQN
                let parent_parts: Vec<String> = ruby_fqn.parts[..ruby_fqn.parts.len() - 1]
                    .iter()
                    .map(|part| part.node_name.to_string())
                    .collect();

                if parent_parts.is_empty() {
                    None
                } else {
                    Some(parent_parts.join("::"))
                }
            }
            _ => None,
        }
    }

    /// Determine the relationship type between parent and child definitions using proper types
    fn get_definition_relationship_type(
        &self,
        parent_type: &DefinitionType,
        child_type: &DefinitionType,
    ) -> Option<RelationshipType> {
        use RubyDefinitionType::*;

        match (parent_type, child_type) {
            // Class relationships
            (DefinitionType::Ruby(Class), DefinitionType::Ruby(Method)) => {
                Some(RelationshipType::ClassToMethod)
            }
            (DefinitionType::Ruby(Class), DefinitionType::Ruby(SingletonMethod)) => {
                Some(RelationshipType::ClassToSingletonMethod)
            }
            (DefinitionType::Ruby(Class), DefinitionType::Ruby(Class)) => {
                Some(RelationshipType::ClassToClass)
            }
            (DefinitionType::Ruby(Class), DefinitionType::Ruby(Lambda)) => {
                Some(RelationshipType::ClassToLambda)
            }
            (DefinitionType::Ruby(Class), DefinitionType::Ruby(Proc)) => {
                Some(RelationshipType::ClassToProc)
            }
            // Module relationships
            (DefinitionType::Ruby(Module), DefinitionType::Ruby(Method)) => {
                Some(RelationshipType::ModuleToMethod)
            }
            (DefinitionType::Ruby(Module), DefinitionType::Ruby(SingletonMethod)) => {
                Some(RelationshipType::ModuleToSingletonMethod)
            }
            (DefinitionType::Ruby(Module), DefinitionType::Ruby(Class)) => {
                Some(RelationshipType::ModuleToClass)
            }
            (DefinitionType::Ruby(Module), DefinitionType::Ruby(Module)) => {
                Some(RelationshipType::ModuleToModule)
            }
            _ => None, // Unknown or unsupported relationship
        }
    }

    /// Get the Ruby-specific scope relationship between definitions
    pub fn get_ruby_scope_relationship(
        parent_type: &RubyDefinitionType,
        child_type: &RubyDefinitionType,
    ) -> Option<String> {
        use RubyDefinitionType::*;

        match (parent_type, child_type) {
            // Namespace relationships
            (Module, _) => Some("NAMESPACE".to_string()),
            (Class, Method | SingletonMethod) => Some("SCOPE".to_string()),
            // Block relationships - Note: Block is not a RubyDefinitionType, so this pattern won't work
            (Method | SingletonMethod, _) => Some("BLOCK_SCOPE".to_string()),
            _ => None,
        }
    }
}
