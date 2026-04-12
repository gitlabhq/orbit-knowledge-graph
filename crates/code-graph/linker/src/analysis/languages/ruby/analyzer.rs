//! Main Ruby analyzer orchestrating the semantic analysis process.
//!
//! This module implements the central [`RubyAnalyzer`] that coordinates the two-phase
//! Ruby code analysis process, transforming parsed structural data into a semantic
//! Knowledge Graph with accurate cross-references.

use crate::analysis::canonical_helpers::fqn_parts_to_canonical;
use crate::analysis::types::{ConsolidatedRelationship, DefinitionNode};
use crate::graph::RelationshipType;
use crate::parse_types::{FileProcessingResult, References};
use code_graph_types::{Language, Range, ToCanonical};
use internment::ArcIntern;
use parser_core::definitions::DefinitionTypeInfo;
use parser_core::{
    references::ReferenceInfo,
    ruby::{
        references::types::{RubyExpressionMetadata, RubyReferenceType, RubyTargetResolution},
        types::RubyFqn,
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
        definition_map: &mut HashMap<(String, String), DefinitionNode>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) -> Result<(), String> {
        if let Some(defs) = file_result.definitions.iter_ruby() {
            for definition in defs {
                // Process all definition types including modules for better scope resolution
                // Modules provide namespace context that's important for method resolution
                let fqn = fqn_parts_to_canonical(&definition.fqn.parts, Language::Ruby);
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
                        "Duplicate definition found for Ruby: {} in file {}",
                        definition.name,
                        relative_file_path
                    );
                    continue;
                }

                definition_map.insert(key, definition_node.clone());
                let mut relationship =
                    ConsolidatedRelationship::file_to_definition(path.clone(), path.clone());
                relationship.relationship_type = RelationshipType::FileDefines;
                relationship.source_range = ArcIntern::new(Range::empty());
                relationship.target_range = ArcIntern::new(definition.range);
                relationships.push(relationship);

                // Add definition to expression resolver if available
                if let Some(ref mut resolver) = self.expression_resolver {
                    resolver.add_definition(fqn.to_string(), definition_node);
                }

                self.stats.definitions_processed += 1;
            }
        }

        Ok(())
    }

    /// Create definition-to-definition relationships using definitions map
    pub fn add_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), DefinitionNode>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        for ((_, child_file_path), child_def) in definition_map {
            if let Some(parent_fqn) = child_def.fqn.parent()
                && let Some(parent_def) =
                    definition_map.get(&(parent_fqn.to_string(), child_file_path.clone()))
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
}
