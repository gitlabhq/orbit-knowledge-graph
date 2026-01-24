//! Generic definition structures for representing code definitions across languages
//!
//! This module provides generic data structures for representing definitions found in source code.
//! It is designed to be language-agnostic while allowing for language-specific extensions through
//! generics and trait implementations.

use std::collections::HashMap;

use crate::fqn::Fqn;
use crate::{AnalysisResult, Range};

/// Generic Definition Information structure
///
/// This represents a definition found in source code with its metadata.
/// It is generic over both the definition type and the FQN type to allow
/// language-specific implementations while maintaining a common structure.
///
/// # Examples
///
/// ```rust
/// use parser_core::definitions::DefinitionInfo;
/// use parser_core::fqn::Fqn;
///
/// // For a language-specific definition type
/// #[derive(Debug, Clone, PartialEq)]
/// enum RubyDefinitionType {
///     Class,
///     Module,
///     Method,
/// }
///
/// // Ruby-specific FQN type
/// type RubyFqn = Fqn<String>;
///
/// // Ruby-specific definition with Ruby FQN
/// type RubyDefinitionInfo = DefinitionInfo<RubyDefinitionType, RubyFqn>;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DefinitionInfo<DefinitionType, FqnType = Fqn<String>, Metadata = ()> {
    /// The type of definition (class, method, etc.) - language-specific
    pub definition_type: DefinitionType,
    /// The name of the definition
    pub name: String,
    /// The fully qualified name of the definition (language-specific)
    pub fqn: FqnType,
    /// The match information from the rule engine
    pub range: Range,
    /// The metadata for the definition
    pub metadata: Option<Metadata>,
}

impl<DefinitionType, FqnType, Metadata> DefinitionInfo<DefinitionType, FqnType, Metadata> {
    /// Create a new definition info with the specified parameters
    pub fn new(definition_type: DefinitionType, name: String, fqn: FqnType, range: Range) -> Self {
        Self {
            definition_type,
            name,
            fqn,
            range,
            metadata: None,
        }
    }

    pub fn new_with_metadata(
        definition_type: DefinitionType,
        name: String,
        fqn: FqnType,
        range: Range,
        metadata: Metadata,
    ) -> Self {
        Self {
            definition_type,
            name,
            fqn,
            range,
            metadata: Some(metadata),
        }
    }
}

/// Trait for definition types that can be converted to string representations
/// This enables generic processing of definitions across different languages
pub trait DefinitionTypeInfo {
    /// Get the string representation of this definition type
    fn as_str(&self) -> &str;
}

/// Trait for common definition lookup operations
///
/// This trait provides common operations for working with collections of definitions,
/// allowing for easy filtering and searching of definitions by type or name.
pub trait DefinitionLookup<FqnType, DefinitionType, ImportType> {
    /// Get definitions of a specific type
    fn definitions_of_type(
        &self,
        def_type: &DefinitionType,
    ) -> Vec<&DefinitionInfo<DefinitionType, FqnType>>;

    /// Get definitions by name (case-sensitive)
    fn definitions_by_name(&self, name: &str) -> Vec<&DefinitionInfo<DefinitionType, FqnType>>;

    /// Count definitions by type
    fn count_definitions_by_type(&self) -> HashMap<DefinitionType, usize>
    where
        DefinitionType: Clone + std::hash::Hash + Eq;

    /// Get all definition names
    fn definition_names(&self) -> Vec<&str>;

    /// Get all definitions (all definitions now have FQNs)
    fn all_definitions(&self) -> Vec<&DefinitionInfo<DefinitionType, FqnType>>;
}

impl<FqnType, DefinitionType, ImportType, ReferenceType>
    DefinitionLookup<FqnType, DefinitionType, ImportType>
    for AnalysisResult<FqnType, DefinitionType, ImportType, ReferenceType>
where
    DefinitionType: PartialEq + Clone + std::hash::Hash + Eq,
{
    fn definitions_of_type(
        &self,
        def_type: &DefinitionType,
    ) -> Vec<&DefinitionInfo<DefinitionType, FqnType>> {
        self.definitions
            .iter()
            .filter(|def| &def.definition_type == def_type)
            .collect()
    }

    fn definitions_by_name(&self, name: &str) -> Vec<&DefinitionInfo<DefinitionType, FqnType>> {
        self.definitions
            .iter()
            .filter(|def| def.name == name)
            .collect()
    }

    fn count_definitions_by_type(&self) -> HashMap<DefinitionType, usize> {
        let mut counts = HashMap::new();
        for def in &self.definitions {
            *counts.entry(def.definition_type.clone()).or_insert(0) += 1;
        }
        counts
    }

    fn definition_names(&self) -> Vec<&str> {
        self.definitions
            .iter()
            .map(|def| def.name.as_str())
            .collect()
    }

    fn all_definitions(&self) -> Vec<&DefinitionInfo<DefinitionType, FqnType>> {
        self.definitions.iter().collect()
    }
}
