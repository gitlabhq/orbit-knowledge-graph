//! High-performance Ruby expression resolver with type inference capabilities.
//!
//! This module implements the core logic for resolving Ruby expressions to their target
//! definitions, following the **Expression-Oriented Type Inference** strategy. It processes
//! symbol chains from the parser and creates accurate call relationships by mimicking
//! Ruby's own method lookup rules.
//!
//! ## Key Features
//!
//! - **Sequential Symbol Resolution**: Processes expression chains symbol-by-symbol
//! - **Scope-Aware Type Tracking**: Maintains variable types across method scopes  
//! - **Ruby Method Lookup**: Follows Ruby's inheritance and module inclusion rules
//! - **Performance Optimizations**: Caching and batch processing
//! - **Memory Efficiency**: Pre-allocated data structures and string interning
//!
//! ## Resolution Process
//!
//! The resolver processes Ruby expressions in a multi-step pipeline:
//!
//! 1. **Reference Grouping**: Groups references by scope for cache locality
//! 2. **Assignment Processing**: Processes assignments first to populate type map
//! 3. **Call Resolution**: Resolves method calls using populated type information
//! 4. **Relationship Creation**: Creates call relationships for successful resolutions
//!
//! ## Type Inference Heuristics
//!
//! The resolver uses several heuristics to infer return types:
//! - **Constructor Calls**: `.new` on class returns instance of that class
//! - **Framework Conventions**: Rails patterns like `.find`, `.first` return model instances
//! - **Future Enhancement**: YARD documentation parsing for explicit type annotations

use super::{
    scope_resolver::ScopeResolver,
    type_map::{InferredType, ScopeId, VariableId},
};
use crate::analysis::types::{ConsolidatedRelationship, DefinitionNode, DefinitionType};
use crate::graph::RelationshipType;
use crate::parse_types::{References, RubyReference};
use internment::ArcIntern;
use parser_core::ruby::types::RubyDefinitionType;
use parser_core::ruby::{
    fqn::ruby_fqn_to_string,
    references::{
        expressions::{RubyExpressionSymbol, RubySymbolType},
        types::RubyReferenceType,
    },
};
use rustc_hash::FxHashMap;
use std::sync::Arc;

/// Result of resolving a single expression symbol with type inference.
///
/// This struct captures the complete outcome of symbol resolution, including
/// the original symbol information, any matched definition, and the inferred
/// type for use in subsequent symbol resolution in a chain.
#[derive(Debug, Clone)]
pub struct SymbolResolution {
    /// The original symbol that was resolved, preserving source location information.
    pub symbol: RubyExpressionSymbol,

    /// The definition this symbol resolved to, if any.
    ///
    /// Will be `Some` for successful resolutions where the symbol matched a known
    /// definition in the project. `None` indicates the symbol could not be resolved,
    /// which may occur for:
    /// - Framework methods not present in the analyzed codebase
    /// - Dynamic method definitions that cannot be statically analyzed
    /// - Symbols outside the current project scope
    pub resolved_definition: Option<Arc<DefinitionNode>>,

    /// The inferred type of this symbol for use in subsequent resolution steps.
    ///
    /// This type becomes the receiver context for the next symbol in an expression chain.
    /// For example, in `user.profile.update`, after resolving `user` to type `User`,
    /// the `profile` symbol is resolved in the context of the `User` type.
    pub inferred_type: InferredType,
}

/// Context information for resolving expressions within a specific scope.
///
/// Provides the necessary context for accurate symbol resolution, including
/// the current scope for variable lookup and type information for method resolution.
#[derive(Debug, Clone)]
pub struct ResolutionContext {
    /// The current scope identifier for variable and method lookup.
    ///
    /// Used to lookup local variables in the type map and determine the
    /// appropriate class context for implicit method calls (calls on `self`).
    pub current_scope: ScopeId,

    /// The current type context for method resolution.
    ///
    /// Represents the receiver type for the current symbol being resolved.
    /// For example, in `user.save`, when resolving `save`, the current_type
    /// would be `"User"` (the type of the `user` variable).
    pub current_type: Option<String>,

    /// File path for location tracking in created relationships.
    ///
    /// Used when creating definition relationships to track the source file
    /// of the method call.
    pub file_path: String,
}

/// Ruby expression resolver with parallel processing support
pub struct ExpressionResolver {
    /// Scope resolver implementing Ruby's method lookup and variable resolution rules.
    scope_resolver: ScopeResolver,

    /// Performance and accuracy statistics for monitoring resolver effectiveness.
    ///
    /// Includes metrics like resolution success rates, cache hit rates, and processing
    /// counts to help optimize performance and identify resolution issues.
    stats: ResolutionStats,
}

impl Default for ExpressionResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpressionResolver {
    /// Create a new expression resolver with estimated capacity
    pub fn new() -> Self {
        Self {
            scope_resolver: ScopeResolver::new(),
            stats: ResolutionStats::new(),
        }
    }

    /// Processes all Ruby references and creates call relationships in the Knowledge Graph.
    ///
    /// This is the main entry point for reference resolution, implementing the core logic
    /// of Phase 2 analysis. It processes Ruby expressions extracted by the parser and
    /// resolves them to their target definitions using sophisticated type inference.
    ///
    /// # Processing Pipeline
    ///
    /// 1. **Reference Extraction**: Extracts Ruby references from the provided collection
    /// 2. **Scope Grouping**: Groups references by scope for better cache locality and type consistency
    /// 3. **Sequential Processing**: Processes each scope's references to maintain type map consistency
    /// 4. **Assignment-First Strategy**: Processes assignments before calls to populate type information
    /// 5. **Relationship Creation**: Creates call relationships for successfully resolved references
    ///
    /// # Parameters
    ///
    /// * `references` - Collection of Ruby references from the parser containing expression metadata
    /// * `file_path` - File path for tracking relationship locations
    /// * `definition_relationships` - Mutable collection where created relationships are stored
    ///
    /// # Processing Order
    ///
    /// The method processes references in a specific order to maximize resolution accuracy:
    ///
    /// 1. **Assignments First**: `user = User.new` - Populates type map with variable types
    /// 2. **Calls Second**: `user.save` - Uses populated type map for accurate resolution
    /// 3. **Other References**: Constants, instance variables, etc.
    ///
    /// This ordering ensures that when resolving `user.save`, the type of `user` is already
    /// known from processing the earlier assignment.
    ///
    ///
    /// # Thread Safety
    ///
    /// This method is not thread-safe and should not be called concurrently on the same
    /// resolver instance.
    pub fn process_references(
        &mut self,
        references: &References,
        file_path: &str,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(ruby_refs) = references.iter_ruby() {
            let references_vec: Vec<_> = ruby_refs.collect();

            // Group references by scope for better cache locality
            let mut refs_by_scope: FxHashMap<String, Vec<&RubyReference>> = FxHashMap::default();

            for reference in &references_vec {
                if let Some(scope) = &reference.scope {
                    let scope_str = ruby_fqn_to_string(scope);
                    refs_by_scope.entry(scope_str).or_default().push(reference);
                }
            }

            // Process each scope's references sequentially to maintain type map consistency
            for (scope_str, scope_refs) in refs_by_scope {
                let scope_id = ScopeId::new(scope_str);
                self.process_scope_references(scope_refs, &scope_id, file_path, relationships);
            }

            self.stats.total_references_processed += references_vec.len();
        }
    }

    /// Process references within a single scope
    fn process_scope_references(
        &mut self,
        references: Vec<&RubyReference>,
        scope_id: &ScopeId,
        file_path: &str,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        // Pre-allocate collections for this scope
        let mut batch_updates = Vec::with_capacity(references.len());
        let mut resolved_relationships = Vec::with_capacity(references.len() * 2); // Estimate 2 relationships per reference

        // Set up scope hierarchy if not already done
        self.setup_scope_hierarchy(scope_id);

        // Process assignments first to populate the type map
        for reference in references
            .iter()
            .filter(|r| r.reference_type == RubyReferenceType::Assignment)
        {
            self.process_assignment_reference(
                reference,
                scope_id,
                file_path,
                &mut batch_updates,
                &mut resolved_relationships,
            );
        }

        // Apply assignment updates to type map before processing calls
        if !batch_updates.is_empty() {
            self.scope_resolver
                .type_map_mut()
                .batch_insert(batch_updates);
        }

        // Now process calls - they can now see the variable types from assignments
        for reference in references
            .iter()
            .filter(|r| r.reference_type == RubyReferenceType::Call)
        {
            self.process_call_reference(
                reference,
                scope_id,
                file_path,
                &mut resolved_relationships,
            );
        }

        // Handle other reference types
        for _reference in references.iter().filter(|r| {
            !matches!(
                r.reference_type,
                RubyReferenceType::Assignment | RubyReferenceType::Call
            )
        }) {
            self.stats.unhandled_references += 1;
        }

        // Add all resolved relationships
        relationships.extend(resolved_relationships);
    }

    /// Set up scope hierarchy for proper variable resolution
    fn setup_scope_hierarchy(&mut self, scope_id: &ScopeId) {
        let scope_str = scope_id.as_str();

        // Extract parent scope from FQN
        // For method scopes like "User#save", parent is "User"
        // For class scopes like "User", parent is top-level (no parent)
        if let Some(hash_pos) = scope_str.find('#') {
            // Instance method - parent is the class
            let class_name = &scope_str[..hash_pos];
            let parent_scope = ScopeId::new(class_name.to_string());

            self.scope_resolver
                .type_map_mut()
                .register_scope_hierarchy(scope_id.clone(), parent_scope);
        } else if let Some(double_colon_pos) = scope_str.rfind("::") {
            // Singleton method or nested class - parent is the containing scope
            let parent_name = &scope_str[..double_colon_pos];
            let parent_scope = ScopeId::new(parent_name.to_string());

            self.scope_resolver
                .type_map_mut()
                .register_scope_hierarchy(scope_id.clone(), parent_scope);
        }
        // Top-level scopes (classes/modules) have no parent
    }

    /// Process an assignment reference (e.g., `user = User.new`)
    fn process_assignment_reference(
        &mut self,
        reference: &RubyReference,
        scope_id: &ScopeId,
        file_path: &str,
        batch_updates: &mut Vec<(ScopeId, VariableId, InferredType)>,
        resolved_relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(metadata) = reference.metadata.as_deref()
            && let Some(assignment_target) = &metadata.assignment_target
        {
            // Resolve the right-hand side of the assignment
            let mut context = ResolutionContext {
                current_scope: scope_id.clone(),
                current_type: None,
                file_path: file_path.to_string(),
            };

            let final_type =
                self.resolve_symbol_chain(&metadata.symbols, &mut context, resolved_relationships);

            // Store the inferred type for the assigned variable
            let variable_id = VariableId::new(assignment_target.name.to_string());
            batch_updates.push((scope_id.clone(), variable_id, final_type));

            self.stats.assignments_processed += 1;
        }
    }

    /// Process a call reference (e.g., `user.save`)
    fn process_call_reference(
        &mut self,
        reference: &RubyReference,
        scope_id: &ScopeId,
        file_path: &str,
        resolved_relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(metadata) = reference.metadata.as_deref() {
            let mut context = ResolutionContext {
                current_scope: scope_id.clone(),
                current_type: None,
                file_path: file_path.to_string(),
            };

            self.resolve_symbol_chain(&metadata.symbols, &mut context, resolved_relationships);
            self.stats.calls_processed += 1;
        }
    }

    /// Resolve a chain of symbols sequentially with type inference
    fn resolve_symbol_chain(
        &mut self,
        symbols: &[RubyExpressionSymbol],
        context: &mut ResolutionContext,
        resolved_relationships: &mut Vec<ConsolidatedRelationship>,
    ) -> InferredType {
        let mut current_type = None;
        let mut created_relationships: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for symbol in symbols.iter() {
            let resolution = self.resolve_single_symbol(symbol, context, current_type.as_deref());

            // Create call relationship if we found a definition
            if let Some(ref definition) = resolution.resolved_definition {
                // Skip framework methods to reduce noise
                let is_framework = self.is_framework_method(&symbol.name, current_type.as_deref());

                if !is_framework {
                    // Only create call relationship if the calling method definition exists and is a real method
                    if let Some(calling_definition) = self
                        .scope_resolver
                        .definition_map()
                        .get_definition(context.current_scope.as_str())
                    {
                        // Additional validation: only create relationships for actual method definitions
                        if matches!(calling_definition.kind, code_graph_types::DefKind::Method) {
                            // Create unique key to prevent duplicate relationships
                            let relationship_key =
                                format!("{}->{}", context.current_scope.as_str(), definition.fqn);

                            // Only create relationship if we haven't already created it
                            if !created_relationships.contains(&relationship_key) {
                                let mut call_relationship =
                                    ConsolidatedRelationship::definition_to_definition(
                                        calling_definition.file_path.clone(),
                                        definition.file_path.clone(),
                                    );
                                call_relationship.relationship_type = RelationshipType::Calls;
                                call_relationship.source_range = ArcIntern::new(symbol.range);
                                call_relationship.target_range = ArcIntern::new(definition.range);
                                call_relationship.source_definition_range =
                                    Some(ArcIntern::new(calling_definition.range));
                                call_relationship.target_definition_range =
                                    Some(ArcIntern::new(definition.range));
                                resolved_relationships.push(call_relationship);
                                created_relationships.insert(relationship_key);
                            }
                        }
                    }
                }

                // Track method call resolution statistics only for method calls
                if matches!(
                    symbol.symbol_type,
                    RubySymbolType::MethodCall | RubySymbolType::SafeMethodCall
                ) {
                    self.stats.successful_resolutions += 1;
                }
            } else if matches!(
                symbol.symbol_type,
                RubySymbolType::MethodCall | RubySymbolType::SafeMethodCall
            ) {
                self.stats.failed_resolutions += 1;
            }

            // Update type context for next symbol
            current_type = resolution
                .inferred_type
                .as_concrete()
                .map(|s| s.to_string());
        }

        current_type
            .map(InferredType::new_concrete)
            .unwrap_or(InferredType::Unknown)
    }

    /// Check if a method call is a framework method that should be filtered out
    /// TODO: find better ways to do this
    fn is_framework_method(&self, method_name: &str, receiver_type: Option<&str>) -> bool {
        // Only filter very common Rails/ActiveRecord methods that add significant noise
        let common_framework_methods = [
            "present?",
            "blank?",
            "nil?",
            "respond_to?",
            "send",
            "instance_eval",
            "class_eval",
            "define_method",
            "attr_reader",
            "attr_writer",
            "attr_accessor",
            "validates",
            "belongs_to",
            "has_many",
            "has_one",
            "before_action",
            "after_action",
            "render",
            "redirect_to",
            "params",
            "request",
            "response",
            "session",
        ];

        // Special case for ActiveRecord::Base methods - these are definitely framework
        if let Some(receiver) = receiver_type
            && (receiver == "ActiveRecord::Base" || receiver.ends_with("::Base"))
        {
            return true;
        }

        // Only filter if it's a very common framework method
        common_framework_methods.contains(&method_name)
    }

    /// Resolve a single symbol in the given context
    fn resolve_single_symbol(
        &mut self,
        symbol: &RubyExpressionSymbol,
        context: &ResolutionContext,
        receiver_type: Option<&str>,
    ) -> SymbolResolution {
        // Resolve using scope resolver
        let resolved_definition = self
            .scope_resolver
            .resolve_symbol(
                &symbol.name,
                &symbol.symbol_type,
                &context.current_scope,
                receiver_type,
            )
            .cloned();

        let inferred_type =
            self.infer_symbol_type(symbol, receiver_type, resolved_definition.as_ref());

        SymbolResolution {
            symbol: symbol.clone(),
            resolved_definition,
            inferred_type,
        }
    }

    /// Infer the type of a symbol based on context and resolution
    fn infer_symbol_type(
        &self,
        symbol: &RubyExpressionSymbol,
        receiver_type: Option<&str>,
        resolved_definition: Option<&Arc<DefinitionNode>>,
    ) -> InferredType {
        match symbol.symbol_type {
            RubySymbolType::Constant => {
                // Constants typically refer to classes/modules
                if let Some(definition) = resolved_definition {
                    if matches!(
                        definition.kind,
                        code_graph_types::DefKind::Class | code_graph_types::DefKind::Module
                    ) {
                        InferredType::new_concrete(definition.fqn.to_string())
                    } else {
                        InferredType::Unknown
                    }
                } else {
                    // Fallback: assume constant name is a class/module name
                    InferredType::new_concrete(symbol.name.to_string())
                }
            }
            RubySymbolType::MethodCall | RubySymbolType::SafeMethodCall => {
                if let Some(receiver) = receiver_type {
                    self.scope_resolver
                        .infer_return_type(&symbol.name, receiver)
                } else if resolved_definition.is_some() {
                    // Method call without explicit receiver - could return any type
                    // For now, we can't infer return type without more context
                    InferredType::Unknown
                } else {
                    InferredType::Unknown
                }
            }
            RubySymbolType::InstanceVariable => {
                // Instance variables: only use concrete type information, no heuristics
                if let Some(definition) = resolved_definition {
                    if matches!(
                        definition.kind,
                        code_graph_types::DefKind::Class | code_graph_types::DefKind::Module
                    ) {
                        InferredType::new_concrete(definition.fqn.to_string())
                    } else {
                        InferredType::Unknown
                    }
                } else {
                    InferredType::Unknown
                }
            }
            RubySymbolType::Identifier => {
                // Could be variable or method call - check if we resolved to a definition
                if let Some(definition) = resolved_definition {
                    // If we found a definition and it's a class/module, the identifier represents that type
                    if matches!(
                        definition.kind,
                        code_graph_types::DefKind::Class | code_graph_types::DefKind::Module
                    ) {
                        InferredType::new_concrete(definition.fqn.to_string())
                    } else if let Some(receiver) = receiver_type {
                        // It's a method call
                        self.scope_resolver
                            .infer_return_type(&symbol.name, receiver)
                    } else {
                        InferredType::Unknown
                    }
                } else {
                    // No definition found - likely a variable, we can't infer type
                    InferredType::Unknown
                }
            }
            _ => InferredType::Unknown,
        }
    }

    /// Add definitions to the resolver (delegated to scope resolver)
    pub fn add_definition(&mut self, fqn: String, node: DefinitionNode) {
        self.scope_resolver.add_definition(fqn, node);
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> &ResolutionStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = ResolutionStats::new();
    }
}

/// Performance and accuracy statistics
#[derive(Debug, Clone)]
pub struct ResolutionStats {
    pub total_references_processed: usize,
    pub assignments_processed: usize,
    pub calls_processed: usize,
    pub successful_resolutions: usize,
    pub failed_resolutions: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub unhandled_references: usize,
}

impl Default for ResolutionStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ResolutionStats {
    pub fn new() -> Self {
        Self {
            total_references_processed: 0,
            assignments_processed: 0,
            calls_processed: 0,
            successful_resolutions: 0,
            failed_resolutions: 0,
            cache_hits: 0,
            cache_misses: 0,
            unhandled_references: 0,
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.successful_resolutions + self.failed_resolutions == 0 {
            0.0
        } else {
            self.successful_resolutions as f64
                / (self.successful_resolutions + self.failed_resolutions) as f64
        }
    }

    pub fn cache_hit_rate(&self) -> f64 {
        if self.cache_hits + self.cache_misses == 0 {
            0.0
        } else {
            self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::types::{DefinitionType, FqnType};
    use parser_core::ruby::types::{RubyDefinitionType, RubyFqn};
    use parser_core::utils::{Position, Range};

    #[test]
    fn test_expression_resolver_basic() {
        let mut resolver = ExpressionResolver::new();

        // Add a test definition
        let fqn = crate::analysis::canonical_helpers::fqn_parts_to_canonical(
            &[
                parser_core::ruby::types::RubyFqnPart::new(
                    parser_core::ruby::types::RubyFqnPartType::Class,
                    "User".to_string(),
                    Range::new(Position::new(1, 0), Position::new(1, 4), (0, 4)),
                ),
                parser_core::ruby::types::RubyFqnPart::new(
                    parser_core::ruby::types::RubyFqnPartType::Method,
                    "save".to_string(),
                    Range::new(Position::new(2, 0), Position::new(2, 4), (20, 24)),
                ),
            ],
            code_graph_types::Language::Ruby,
        );
        let node = DefinitionNode::new(
            fqn,
            "Method".to_string(),
            code_graph_types::DefKind::Method,
            Range::new(Position::new(1, 0), Position::new(1, 10), (0, 10)),
            ArcIntern::new("user.rb".to_string()),
        );

        let ruby_fqn = RubyFqn {
            parts: std::sync::Arc::new(smallvec::SmallVec::from_vec(vec![
                parser_core::ruby::types::RubyFqnPart::new(
                    parser_core::ruby::types::RubyFqnPartType::Class,
                    "User".to_string(),
                    Range::new(
                        parser_core::utils::Position { line: 1, column: 0 },
                        parser_core::utils::Position { line: 1, column: 4 },
                        (0, 4),
                    ),
                ),
                parser_core::ruby::types::RubyFqnPart::new(
                    parser_core::ruby::types::RubyFqnPartType::Method,
                    "save".to_string(),
                    Range::new(
                        parser_core::utils::Position { line: 2, column: 0 },
                        parser_core::utils::Position { line: 2, column: 4 },
                        (20, 24),
                    ),
                ),
            ])),
        };

        resolver.add_definition("User#save".to_string(), node);

        // Test resolution
        let scope = ScopeId::new("TestClass#test_method".to_string());
        let symbol = RubyExpressionSymbol {
            symbol_type: RubySymbolType::MethodCall,
            name: "save".into(),
            range: Range::new(
                parser_core::utils::Position { line: 1, column: 0 },
                parser_core::utils::Position { line: 1, column: 4 },
                (0, 4),
            ),
        };

        let context = ResolutionContext {
            current_scope: scope.clone(),
            current_type: Some("User".to_string()),
            file_path: "test.rb".to_string(),
        };

        let resolution = resolver.resolve_single_symbol(&symbol, &context, Some("User"));
        assert!(resolution.resolved_definition.is_some());
    }

    #[test]
    fn test_resolution_stats() {
        let mut stats = ResolutionStats::new();
        stats.successful_resolutions = 80;
        stats.failed_resolutions = 20;

        assert_eq!(stats.success_rate(), 0.8);
    }
}
