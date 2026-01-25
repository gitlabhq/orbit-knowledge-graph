//! Scope-aware type tracking for Ruby variable type inference.
//!
//! This module provides the type mapping infrastructure necessary for tracking variable types
//! across Ruby scopes. It implements the "memory" component of the Expression-Oriented Type
//! Inference strategy, allowing the resolver to remember variable types from assignments and
//! use them for method resolution.
//!
//! ## Type Inference Strategy
//!
//! 1. **Assignment Tracking**: Records variable types from assignment expressions
//! 2. **Scope Isolation**: Each method/class scope maintains separate type information
//! 3. **Type Propagation**: Inferred types flow through expression chains
//! 4. **Fallback Handling**: Graceful degradation when types cannot be determined
//!
//! ## Ruby-Specific Considerations
//!
//! - **Dynamic Nature**: Accepts that some types cannot be statically determined
//! - **Duck Typing**: Focuses on practical resolution rather than strict type checking
//! - **Self References**: Special handling for `self` in different class contexts
//! - **Variable Scoping**: Respects Ruby's variable scoping rules (local, instance, class, global)

use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::sync::Arc;

/// Compact scope identifier using string interning for memory efficiency.
///
/// Represents a Ruby scope (method, class, module) using an interned string to minimize
/// memory usage when the same scope is referenced multiple times. Scopes are identified
/// by their fully-qualified names (e.g., "User#save", "NotificationService::notify").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScopeId(Arc<str>);

impl ScopeId {
    pub fn new(scope_fqn: String) -> Self {
        Self(scope_fqn.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Efficient variable identifier using string interning for memory optimization.
///
/// Represents a Ruby variable name using an interned string. This reduces memory usage
/// when the same variable name appears multiple times across different scopes or in
/// complex expressions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VariableId(Arc<str>);

impl VariableId {
    pub fn new(variable_name: String) -> Self {
        Self(variable_name.into())
    }

    /// Returns the variable name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub enum InferredType {
    /// Known concrete type (class name).
    ///
    /// Represents a variable with a definitively known type, typically from direct assignment
    /// like `user = User.new`. This is the most useful case for method resolution.
    ///
    /// # Examples
    /// - `user = User.new` -> `Concrete("User")`
    /// - `profile = Profile.find(1)` -> `Concrete("Profile")` (with proper heuristics)
    Concrete(Arc<str>),

    /// Multiple possible types from control flow branches.
    ///
    /// Represents a variable that could be one of several types depending on execution path.
    /// This occurs in Ruby code with conditional assignments or complex control flow.
    ///
    /// # Examples  
    /// - `obj = condition ? User.new : Admin.new` -> `Union(["User", "Admin"])`
    /// - Variable assigned in different branches of an if/else statement
    ///
    /// Uses `SmallVec` optimized for the common case of 2 possible types.
    Union(SmallVec<[Arc<str>; 2]>),

    /// Unknown or indeterminate type.
    ///
    /// Used when the type cannot be reliably determined through static analysis. This is
    /// common in Ruby due to dynamic method calls, complex metaprogramming, or methods
    /// with indeterminate return types.
    ///
    /// # Examples
    /// - Variables from method parameters without type annotations
    /// - Return values from methods with complex or dynamic behavior
    /// - Variables assigned through dynamic dispatch (`obj.send(:method_name)`)
    Unknown,

    /// Self reference within a class or module context.
    ///
    /// Represents `self` or implicit method calls within a class/module. The type is the
    /// FQN of the containing class/module, allowing resolution of methods on `self`.
    ///
    /// # Examples
    /// - `self` within a `User` class -> `SelfType("User")`
    /// - Implicit method calls like `save` (equivalent to `self.save`)
    SelfType(Arc<str>),
}

impl InferredType {
    pub fn new_concrete(type_name: String) -> Self {
        Self::Concrete(type_name.into())
    }

    pub fn new_self_type(class_name: String) -> Self {
        Self::SelfType(class_name.into())
    }

    pub fn as_concrete(&self) -> Option<&str> {
        match self {
            Self::Concrete(type_name) => Some(type_name),
            Self::SelfType(class_name) => Some(class_name),
            _ => None,
        }
    }

    pub fn merge(self, other: InferredType) -> InferredType {
        match (self, other) {
            (Self::Concrete(a), Self::Concrete(b)) if a == b => Self::Concrete(a),
            (Self::Concrete(a), Self::Concrete(b)) => {
                let mut union = SmallVec::new();
                union.push(a);
                union.push(b);
                Self::Union(union)
            }
            (Self::Union(mut union), Self::Concrete(type_name)) => {
                if !union.contains(&type_name) {
                    union.push(type_name);
                }
                Self::Union(union)
            }
            (Self::Concrete(type_name), Self::Union(mut union)) => {
                if !union.contains(&type_name) {
                    union.insert(0, type_name);
                }
                Self::Union(union)
            }
            (Self::Union(mut a), Self::Union(b)) => {
                for type_name in b {
                    if !a.contains(&type_name) {
                        a.push(type_name);
                    }
                }
                Self::Union(a)
            }
            (_, Self::Unknown) | (Self::Unknown, _) => Self::Unknown,
            (a, _) => a, // Prefer first type in ambiguous cases
        }
    }
}

/// The `TypeMap` serves as the "memory" of the type inference system, tracking variable types
/// across different Ruby scopes. It implements efficient storage and lookup mechanisms that
/// respect Ruby's lexical scoping rules while providing the performance needed for large
/// codebases.
///
/// ## Scope Hierarchy
///
/// The type map maintains a hierarchy of scopes to support Ruby's lexical scoping:
/// ```text
/// TopLevel
/// └── User (class scope)
///     ├── User#initialize (method scope)
///     └── User#save (method scope)
/// ```
///
/// When looking up a variable, the map searches from the current scope up through parent
/// scopes until a definition is found, mirroring Ruby's variable resolution behavior.
pub struct TypeMap {
    /// Main storage mapping (scope, variable) pairs to their inferred types.
    ///
    /// Structure: `(ScopeId, VariableId)` allows the same variable name to have
    /// different types in different scopes.
    types: FxHashMap<(ScopeId, VariableId), InferredType>,

    /// Scope hierarchy mapping child scopes to their parent scopes.
    ///
    /// Enables traversal up the scope chain during variable lookup. Implements Ruby's
    /// lexical scoping behavior. For example, a method scope's parent would be its
    /// containing class scope.
    ///
    /// Example mapping:
    /// - `"User#save"` -> `"User"`
    /// - `"User"` -> `"TopLevel"`
    scope_hierarchy: FxHashMap<ScopeId, ScopeId>,
}

impl Default for TypeMap {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeMap {
    pub fn new() -> Self {
        Self {
            types: FxHashMap::with_hasher(Default::default()),
            scope_hierarchy: FxHashMap::with_hasher(Default::default()),
        }
    }

    /// Insert or update a variable's type in a specific scope
    pub fn insert(&mut self, scope: ScopeId, variable: VariableId, inferred_type: InferredType) {
        let key = (scope, variable);

        match self.types.get_mut(&key) {
            Some(existing_type) => {
                // Merge with existing type for better accuracy in control flow
                *existing_type =
                    std::mem::replace(existing_type, InferredType::Unknown).merge(inferred_type);
            }
            None => {
                self.types.insert(key, inferred_type);
            }
        }
    }

    /// Look up a variable's type following Ruby's scope resolution rules
    pub fn lookup(&self, scope: &ScopeId, variable: &VariableId) -> Option<&InferredType> {
        // Start with current scope
        let mut current_scope = Some(scope);

        while let Some(scope_id) = current_scope {
            if let Some(inferred_type) = self.types.get(&(scope_id.clone(), variable.clone())) {
                return Some(inferred_type);
            }

            // Move to parent scope
            current_scope = self.scope_hierarchy.get(scope_id);
        }

        None
    }

    /// Register a scope hierarchy relationship
    pub fn register_scope_hierarchy(&mut self, child_scope: ScopeId, parent_scope: ScopeId) {
        self.scope_hierarchy.insert(child_scope, parent_scope);
    }

    /// Get all variables in a specific scope (for debugging/analysis)
    pub fn get_scope_variables(&self, scope: &ScopeId) -> Vec<(&VariableId, &InferredType)> {
        self.types
            .iter()
            .filter_map(|((s, v), t)| if s == scope { Some((v, t)) } else { None })
            .collect()
    }

    /// Parallel batch update for processing multiple assignments efficiently
    pub fn batch_insert(&mut self, updates: Vec<(ScopeId, VariableId, InferredType)>) {
        // Reserve additional capacity if needed
        let additional_capacity = updates.len();
        if self.types.len() + additional_capacity > self.types.capacity() {
            self.types.reserve(additional_capacity);
        }

        for (scope, variable, inferred_type) in updates {
            self.insert(scope, variable, inferred_type);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_map_basic_operations() {
        let mut type_map = TypeMap::new();

        let scope = ScopeId::new("MyClass#my_method".to_string());
        let variable = VariableId::new("user".to_string());
        let inferred_type = InferredType::new_concrete("User".to_string());

        type_map.insert(scope.clone(), variable.clone(), inferred_type);

        let result = type_map.lookup(&scope, &variable);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_concrete(), Some("User"));
    }

    #[test]
    fn test_scope_hierarchy_lookup() {
        let mut type_map = TypeMap::new();

        let parent_scope = ScopeId::new("MyClass".to_string());
        let child_scope = ScopeId::new("MyClass#my_method".to_string());
        let variable = VariableId::new("class_var".to_string());

        // Register hierarchy
        type_map.register_scope_hierarchy(child_scope.clone(), parent_scope.clone());

        // Insert variable in parent scope
        type_map.insert(
            parent_scope,
            variable.clone(),
            InferredType::new_concrete("String".to_string()),
        );

        // Should find variable when looking from child scope
        let result = type_map.lookup(&child_scope, &variable);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_concrete(), Some("String"));
    }

    #[test]
    fn test_type_merging() {
        let type1 = InferredType::new_concrete("User".to_string());
        let type2 = InferredType::new_concrete("Admin".to_string());

        let merged = type1.merge(type2);

        match merged {
            InferredType::Union(types) => {
                assert_eq!(types.len(), 2);
                assert!(types.iter().any(|t| t.as_ref() == "User"));
                assert!(types.iter().any(|t| t.as_ref() == "Admin"));
            }
            _ => panic!("Expected Union type"),
        }
    }
}
