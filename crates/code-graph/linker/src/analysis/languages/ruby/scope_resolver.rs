//! Ruby scope resolution and method lookup implementation.
//!
//! This module implements Ruby's scope resolution and method lookup rules,
//! including inheritance chains, module inclusions, and lexical scoping.
//!
//! ## Ruby Method Lookup Order
//!
//! The resolver follows Ruby's method lookup order:
//! 1. **Singleton methods** on the class itself
//! 2. **Instance methods** on the class itself  
//! 3. **Included modules** in reverse order of inclusion
//! 4. **Superclass methods** following the same pattern recursively
//! 5. **BasicObject** as the ultimate ancestor
//!

use super::type_map::{InferredType, ScopeId, TypeMap, VariableId};
use crate::analysis::types::DefinitionNode;
use parser_core::ruby::{
    references::expressions::RubySymbolType,
    types::{RubyDefinitionType, RubyFqn},
};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DefinitionMap {
    /// Primary definition storage mapping fully-qualified names to definition nodes.
    /// This is the authoritative source for all definitions in the project.
    ///
    /// Example entries:
    /// - `"User"` -> Class definition
    /// - `"User#save"` -> Instance method definition  
    /// - `"User::find_by_email"` -> Singleton method definition
    definitions: FxHashMap<Arc<str>, Arc<DefinitionNode>>,

    /// Instance method index mapping class FQNs to their instance method names.
    ///
    /// This index enables fast lookup of all instance methods available on a class,
    /// without having to scan the entire definitions map. Uses SmallVec since most
    /// classes have a modest number of methods (typically < 8 commonly used ones).
    ///
    /// Example: `"User"` -> `["save", "update", "destroy", "email", "profile"]`
    instance_methods: FxHashMap<Arc<str>, SmallVec<[Arc<str>; 8]>>,

    /// Singleton method index mapping class FQNs to their class method names.
    ///
    /// Similar to instance_methods but for class/singleton methods (defined with `self.`
    /// or in the class's singleton class). These methods are called directly on the class.
    ///
    /// Example: `"User"` -> `["find", "create", "find_by_email", "all"]`
    singleton_methods: FxHashMap<Arc<str>, SmallVec<[Arc<str>; 8]>>,

    /// Inheritance chain mapping child classes to their parent class FQNs.
    ///
    /// Used to traverse the inheritance hierarchy during method lookup. Only stores
    /// direct parent relationships; full ancestry is computed by following the chain.
    ///
    /// Example: `"User"` -> `"ApplicationRecord"`, `"ApplicationRecord"` -> `"ActiveRecord::Base"`
    inheritance_chain: FxHashMap<Arc<str>, Arc<str>>,

    /// Module inclusion mapping classes to their included module FQNs.
    ///
    /// Tracks modules mixed into classes via `include`, `prepend`, or `extend`.
    /// The order matters for Ruby's method resolution, with later inclusions taking precedence.
    ///
    /// Example: `"User"` -> `["Authenticatable", "Trackable", "Validatable"]`
    included_modules: FxHashMap<Arc<str>, SmallVec<[Arc<str>; 4]>>,

    /// Constant lookup index for lexical scope resolution.
    ///
    /// **Note**: This is currently a placeholder for future implementation of Ruby's
    /// complex constant lookup rules, which involve lexical scoping and autoloading.
    #[allow(dead_code)]
    constants: FxHashMap<Arc<str>, SmallVec<[Arc<str>; 4]>>,
}

impl Default for DefinitionMap {
    fn default() -> Self {
        Self::new()
    }
}

impl DefinitionMap {
    pub fn new() -> Self {
        Self {
            definitions: FxHashMap::with_hasher(Default::default()),
            instance_methods: FxHashMap::with_hasher(Default::default()),
            singleton_methods: FxHashMap::with_hasher(Default::default()),
            inheritance_chain: FxHashMap::with_hasher(Default::default()),
            included_modules: FxHashMap::with_hasher(Default::default()),
            constants: FxHashMap::with_hasher(Default::default()),
        }
    }

    /// Add a definition with optimized indexing
    pub fn add_definition(&mut self, fqn: String, node: DefinitionNode) {
        let fqn_arc: Arc<str> = fqn.into();
        let node_arc = Arc::new(node);

        self.definitions.insert(fqn_arc.clone(), node_arc.clone());

        match node_arc.kind {
            code_graph_types::DefKind::Method => {
                let is_singleton = node_arc.definition_type == "SingletonMethod";
                self.index_method_from_canonical(&node_arc.fqn, node_arc.name(), is_singleton);
            }
            code_graph_types::DefKind::Class => {
                self.index_class_from_canonical(&node_arc.fqn);
            }
            _ => {}
        }
    }

    fn index_method_from_canonical(
        &mut self,
        fqn: &code_graph_types::CanonicalFqn,
        method_name: &str,
        is_singleton: bool,
    ) {
        if let Some(parent) = fqn.parent() {
            let class_fqn = parent.to_string();
            let class_fqn_arc: Arc<str> = class_fqn.into();
            let method_name_arc: Arc<str> = method_name.into();

            let methods_map = if is_singleton {
                &mut self.singleton_methods
            } else {
                &mut self.instance_methods
            };

            methods_map
                .entry(class_fqn_arc)
                .or_insert_with(SmallVec::new)
                .push(method_name_arc);
        }
    }

    fn index_class_from_canonical(&mut self, fqn: &code_graph_types::CanonicalFqn) {
        if let Some(parent) = fqn.parent() {
            let class_fqn: Arc<str> = fqn.to_string().into();
            let parent_fqn_arc: Arc<str> = parent.to_string().into();
            self.inheritance_chain.insert(class_fqn, parent_fqn_arc);
        }
    }

    /// Look up a definition by exact FQN
    pub fn get_definition(&self, fqn: &str) -> Option<&Arc<DefinitionNode>> {
        self.definitions.get(fqn)
    }

    /// Find an instance method on a class following inheritance chain
    pub fn find_instance_method(
        &self,
        class_fqn: &str,
        method_name: &str,
    ) -> Option<&Arc<DefinitionNode>> {
        self.find_method_in_hierarchy(class_fqn, method_name, false)
    }

    /// Find a singleton method on a class
    pub fn find_singleton_method(
        &self,
        class_fqn: &str,
        method_name: &str,
    ) -> Option<&Arc<DefinitionNode>> {
        self.find_method_in_hierarchy(class_fqn, method_name, true)
    }

    /// Find method in inheritance hierarchy with optimized traversal
    fn find_method_in_hierarchy(
        &self,
        class_fqn: &str,
        method_name: &str,
        is_singleton: bool,
    ) -> Option<&Arc<DefinitionNode>> {
        let methods_map = if is_singleton {
            &self.singleton_methods
        } else {
            &self.instance_methods
        };

        let mut current_class = Some(class_fqn);
        let mut visited = SmallVec::<[&str; 8]>::new(); // Prevent infinite loops

        while let Some(class) = current_class {
            if visited.contains(&class) {
                break; // Circular inheritance
            }
            visited.push(class);

            // Check current class
            if let Some(methods) = methods_map.get(class)
                && methods.iter().any(|m| m.as_ref() == method_name)
            {
                let method_fqn = if is_singleton {
                    format!("{class}::{method_name}")
                } else {
                    format!("{class}#{method_name}")
                };
                return self.definitions.get(method_fqn.as_str());
            }

            // Check included modules (modules come before parent class in Ruby)
            if let Some(modules) = self.included_modules.get(class) {
                for module_fqn in modules {
                    if let Some(methods) = methods_map.get(module_fqn.as_ref())
                        && methods.iter().any(|m| m.as_ref() == method_name)
                    {
                        let method_fqn = format!("{module_fqn}#{method_name}");
                        if let Some(def) = self.definitions.get(method_fqn.as_str()) {
                            return Some(def);
                        }
                    }
                }
            }

            // Move to parent class
            current_class = self.inheritance_chain.get(class).map(|s| s.as_ref());
        }

        None
    }

    /// Get all classes that have a specific method (for global search)
    pub fn find_classes_with_method(&self, method_name: &str, is_singleton: bool) -> Vec<&str> {
        let methods_map = if is_singleton {
            &self.singleton_methods
        } else {
            &self.instance_methods
        };

        methods_map
            .par_iter() // Parallel iteration for large codebases
            .filter_map(|(class_fqn, methods)| {
                if methods.iter().any(|m| m.as_ref() == method_name) {
                    Some(class_fqn.as_ref())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Batch lookup optimization for processing multiple references
    pub fn batch_find_methods(
        &self,
        requests: &[(String, String, bool)],
    ) -> Vec<Option<Arc<DefinitionNode>>> {
        requests
            .par_iter() // Parallel processing
            .map(|(class_fqn, method_name, is_singleton)| {
                self.find_method_in_hierarchy(class_fqn, method_name, *is_singleton)
                    .cloned()
            })
            .collect()
    }

    // Helper methods
    fn get_class_fqn_from_method(&self, ruby_fqn: &RubyFqn) -> Option<String> {
        if ruby_fqn.parts.len() >= 2 {
            let class_parts: Vec<String> = ruby_fqn.parts[..ruby_fqn.parts.len() - 1]
                .iter()
                .map(|part| part.node_name.to_string())
                .collect();
            Some(class_parts.join("::"))
        } else {
            None
        }
    }

    fn get_parent_class_fqn(&self, ruby_fqn: &RubyFqn) -> Option<String> {
        if ruby_fqn.parts.len() > 1 {
            let parent_parts: Vec<String> = ruby_fqn.parts[..ruby_fqn.parts.len() - 1]
                .iter()
                .map(|part| part.node_name.to_string())
                .collect();
            Some(parent_parts.join("::"))
        } else {
            None
        }
    }

    fn ruby_fqn_to_string(&self, ruby_fqn: &RubyFqn) -> String {
        ruby_fqn
            .parts
            .iter()
            .map(|part| part.node_name.as_str())
            .collect::<Vec<_>>()
            .join("::")
    }
}

pub struct ScopeResolver {
    /// Definition map containing all project definitions and their relationships.
    definition_map: DefinitionMap,

    /// Type map tracking inferred variable types across different scopes.
    type_map: TypeMap,
}

impl Default for ScopeResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl ScopeResolver {
    pub fn new() -> Self {
        Self {
            definition_map: DefinitionMap::new(),
            type_map: TypeMap::new(),
        }
    }

    /// Add a definition to the resolver
    pub fn add_definition(&mut self, fqn: String, node: DefinitionNode) {
        self.definition_map.add_definition(fqn, node);
    }

    /// Set a variable's type in a scope
    pub fn set_variable_type(
        &mut self,
        scope: ScopeId,
        variable: VariableId,
        inferred_type: InferredType,
    ) {
        self.type_map.insert(scope, variable, inferred_type);
    }

    /// Resolve a symbol in a given context following Ruby's lookup rules
    pub fn resolve_symbol(
        &self,
        symbol_name: &str,
        symbol_type: &RubySymbolType,
        current_scope: &ScopeId,
        receiver_type: Option<&str>,
    ) -> Option<&Arc<DefinitionNode>> {
        match symbol_type {
            RubySymbolType::Constant => self.resolve_constant(symbol_name, current_scope),
            RubySymbolType::Identifier => {
                self.resolve_identifier(symbol_name, current_scope, receiver_type)
            }
            RubySymbolType::MethodCall | RubySymbolType::SafeMethodCall => {
                if receiver_type.is_some() {
                    // Explicit receiver - use normal method call resolution
                    self.resolve_method_call(symbol_name, receiver_type)
                } else {
                    // No receiver - this is an implicit self call, resolve in current scope
                    self.resolve_method_on_current_scope(symbol_name, current_scope)
                }
            }
            RubySymbolType::InstanceVariable => {
                self.resolve_instance_variable(symbol_name, current_scope)
            }
            RubySymbolType::ClassVariable => {
                self.resolve_class_variable(symbol_name, current_scope)
            }
            RubySymbolType::GlobalVariable => self.resolve_global_variable(symbol_name),
        }
    }

    /// Resolve a constant following Ruby's constant lookup rules
    fn resolve_constant(
        &self,
        constant_name: &str,
        _current_scope: &ScopeId,
    ) -> Option<&Arc<DefinitionNode>> {
        // Direct lookup for now - can be enhanced with lexical scope traversal
        self.definition_map.get_definition(constant_name)
    }

    /// Resolve an identifier (local variable or method call)
    fn resolve_identifier(
        &self,
        identifier: &str,
        scope: &ScopeId,
        receiver_type: Option<&str>,
    ) -> Option<&Arc<DefinitionNode>> {
        // Variable lookup working correctly

        // First check if it's a local variable with known type
        let variable_id = VariableId::new(identifier.to_string());
        if let Some(inferred_type) = self.type_map.lookup(scope, &variable_id)
            && let Some(type_name) = inferred_type.as_concrete()
        {
            return self.definition_map.get_definition(type_name);
        }

        // If not a variable, try as method call on implicit receiver
        if let Some(receiver) = receiver_type {
            self.resolve_method_call(identifier, Some(receiver))
        } else {
            // Try as method on current scope's class
            self.resolve_method_on_current_scope(identifier, scope)
        }
    }

    /// Resolve a method call on a specific receiver type.
    ///
    /// Following ruby-lsp's approach: when receiver_type is None, this represents an
    /// implicit self call which should be resolved in the current scope context.
    fn resolve_method_call(
        &self,
        method_name: &str,
        receiver_type: Option<&str>,
    ) -> Option<&Arc<DefinitionNode>> {
        if let Some(receiver) = receiver_type {
            // Explicit receiver - try instance method first, then singleton method
            self.definition_map
                .find_instance_method(receiver, method_name)
                .or_else(|| {
                    self.definition_map
                        .find_singleton_method(receiver, method_name)
                })
        } else {
            // No explicit receiver - this is an implicit self call
            // We need the current scope to resolve this properly, but resolve_method_call
            // doesn't have access to scope. This is a design limitation.
            // For now, return None - the caller should handle implicit self calls differently
            None
        }
    }

    /// Resolve method on current scope (implicit self)
    fn resolve_method_on_current_scope(
        &self,
        method_name: &str,
        scope: &ScopeId,
    ) -> Option<&Arc<DefinitionNode>> {
        // Extract class name from scope FQN
        let scope_str = scope.as_str();
        if let Some(class_end) = scope_str.find('#') {
            // Instance method scope - look for instance methods
            let class_name = &scope_str[..class_end];
            self.definition_map
                .find_instance_method(class_name, method_name)
        } else if scope_str.contains("::") {
            // Singleton method scope (e.g., "NotificationService::notify") - look for singleton methods
            let class_name = scope_str.split("::").next().unwrap_or(scope_str);
            self.definition_map
                .find_singleton_method(class_name, method_name)
        } else {
            // Scope is a class itself
            self.definition_map
                .find_singleton_method(scope_str, method_name)
        }
    }

    /// Resolve instance variable using tracked type information and method scope context
    fn resolve_instance_variable(
        &self,
        var_name: &str,
        scope: &ScopeId,
    ) -> Option<&Arc<DefinitionNode>> {
        // Instance variables need type inference - check if type is known from assignments
        let variable_id = VariableId::new(var_name.to_string());
        if let Some(inferred_type) = self.type_map.lookup(scope, &variable_id)
            && let Some(type_name) = inferred_type.as_concrete()
        {
            return self.definition_map.get_definition(type_name);
        }

        // Smart heuristic: extract class name from variable name
        // @user -> User, @notification_service -> NotificationService, etc.
        // TODO: figure out how to do this better
        let inferred_class_name = self.infer_class_from_instance_variable(var_name);
        if let Some(class_name) = inferred_class_name
            && let Some(definition) = self.definition_map.get_definition(&class_name)
        {
            return Some(definition);
        }

        None
    }

    /// Resolve class variable (simplified)
    fn resolve_class_variable(
        &self,
        _var_name: &str,
        _scope: &ScopeId,
    ) -> Option<&Arc<DefinitionNode>> {
        // Class variables don't have explicit definitions in most cases
        None
    }

    /// Resolve global variable (simplified)
    fn resolve_global_variable(&self, _var_name: &str) -> Option<&Arc<DefinitionNode>> {
        // Global variables rarely have explicit definitions
        None
    }

    /// Get the type map for external access
    pub fn type_map(&self) -> &TypeMap {
        &self.type_map
    }

    /// Get the type map for external mutation
    pub fn type_map_mut(&mut self) -> &mut TypeMap {
        &mut self.type_map
    }

    /// Get the definition map for external access
    pub fn definition_map(&self) -> &DefinitionMap {
        &self.definition_map
    }

    /// Infer return type of a method call.
    ///
    /// For now, we only handle the most basic case of constructor methods.
    /// This avoids heuristic-based string matching while keeping the API for future enhancement
    /// with proper type annotation parsing (YARD docs, RBI files, etc.).
    pub fn infer_return_type(&self, method_name: &str, receiver_type: &str) -> InferredType {
        match method_name {
            // Only handle the most basic case where we can be 100% certain
            "new" => InferredType::new_concrete(receiver_type.to_string()),

            // For all other cases, we don't make assumptions and accept the limitation
            // TODO: This could be enhanced with:
            // - YARD documentation parsing (@return annotations)
            // - RBI file type signatures (for Sorbet)
            // - Explicit return type annotations
            _ => InferredType::Unknown,
        }
    }
    /// TODO: verify accuracy of this
    /// Infer class name from instance variable name using Ruby naming conventions.
    ///
    /// This follows the same approach as ruby-lsp's TypeInferrer.guess_type method:
    /// https://github.com/Shopify/ruby-lsp/blob/main/lib/ruby_lsp/type_inferrer.rb#L116-129
    ///
    /// The algorithm:
    /// 1. Remove @ and @@ prefixes
    /// 2. Split on underscores  
    /// 3. Capitalize each part and join them
    /// 4. Look up the resulting class name
    ///
    /// Examples:
    /// - @user -> User
    /// - @notification_service -> NotificationService  
    /// - @user_profile -> UserProfile
    fn infer_class_from_instance_variable(&self, var_name: &str) -> Option<String> {
        if !var_name.starts_with('@') {
            return None;
        }

        // Following ruby-lsp's approach: remove @ prefix and convert snake_case to PascalCase
        let var_name_without_at = var_name
            .strip_prefix("@@")
            .or_else(|| var_name.strip_prefix("@"))
            .unwrap_or(var_name);

        // Convert snake_case to PascalCase by splitting on underscores and capitalizing each part
        let class_name = var_name_without_at
            .split('_')
            .map(|part| {
                if part.is_empty() {
                    String::new()
                } else {
                    let mut chars = part.chars();
                    match chars.next() {
                        None => String::new(),
                        Some(first) => {
                            format!("{}{}", first.to_uppercase(), chars.as_str().to_lowercase())
                        }
                    }
                }
            })
            .collect::<Vec<String>>()
            .join("");

        if class_name.is_empty() {
            None
        } else {
            Some(class_name)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph_types::DefKind;
    use internment::ArcIntern;
    use parser_core::utils::{Position, Range};

    #[test]
    fn test_definition_map_method_lookup() {
        let mut def_map = DefinitionMap::new();

        // Create a test definition
        let node = DefinitionNode::new(
            FqnType::Ruby(RubyFqn {
                parts: std::sync::Arc::new(smallvec::SmallVec::from_vec(vec![
                    parser_core::ruby::types::RubyFqnPart::new(
                        parser_core::ruby::types::RubyFqnPartType::Class,
                        "User".to_string(),
                        parser_core::utils::Range::new(
                            parser_core::utils::Position { line: 1, column: 0 },
                            parser_core::utils::Position {
                                line: 1,
                                column: 10,
                            },
                            (0, 10),
                        ),
                    ),
                    parser_core::ruby::types::RubyFqnPart::new(
                        parser_core::ruby::types::RubyFqnPartType::Method,
                        "save".to_string(),
                        parser_core::utils::Range::new(
                            parser_core::utils::Position { line: 2, column: 0 },
                            parser_core::utils::Position { line: 2, column: 4 },
                            (20, 24),
                        ),
                    ),
                ])),
            }),
            DefinitionType::Ruby(RubyDefinitionType::Method),
            Range::new(Position::new(1, 0), Position::new(1, 10), (0, 10)),
            ArcIntern::new("user.rb".to_string()),
        );

        // Create a mock RubyFqn
        let ruby_fqn = RubyFqn {
            parts: std::sync::Arc::new(smallvec::SmallVec::from_vec(vec![
                parser_core::ruby::types::RubyFqnPart::new(
                    parser_core::ruby::types::RubyFqnPartType::Class,
                    "User".to_string(),
                    parser_core::utils::Range::new(
                        parser_core::utils::Position { line: 1, column: 0 },
                        parser_core::utils::Position { line: 1, column: 4 },
                        (0, 4),
                    ),
                ),
                parser_core::ruby::types::RubyFqnPart::new(
                    parser_core::ruby::types::RubyFqnPartType::Method,
                    "save".to_string(),
                    parser_core::utils::Range::new(
                        parser_core::utils::Position { line: 2, column: 0 },
                        parser_core::utils::Position { line: 2, column: 4 },
                        (20, 24),
                    ),
                ),
            ])),
        };

        def_map.add_definition("User#save".to_string(), node, &FqnType::Ruby(ruby_fqn));

        // Test lookup
        let result = def_map.find_instance_method("User", "save");
        assert!(result.is_some());
        assert_eq!(result.unwrap().name(), "save");
    }
}
