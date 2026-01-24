//! Generic analyzer structures for representing code analysis results across languages
//!
//! This module provides generic data structures for analyzing source code and returning
//! results with definitions. It is designed to be language-agnostic while allowing for
//! language-specific extensions through generics.

use std::collections::HashMap;

use crate::definitions::DefinitionInfo;
use crate::imports::ImportedSymbolInfo;

/// Generic analyzer for extracting definitions from source code
///
/// This analyzer is generic over the definition type and FQN type to allow
/// language-specific implementations while maintaining a common interface.
pub struct Analyzer<FqnType, DefinitionType = (), ImportType = ()> {
    _phantom: std::marker::PhantomData<(FqnType, DefinitionType, ImportType)>,
}

/// Generic result of analyzing source code
///
/// Contains all definitions found during analysis with methods for filtering
/// and accessing the results.
pub struct AnalysisResult<FqnType, DefinitionType = (), ImportType = (), ReferenceType = ()> {
    /// All definitions found in the code
    pub definitions: Vec<DefinitionInfo<DefinitionType, FqnType>>,
    /// All imported symbols found in the code
    pub imports: Vec<ImportedSymbolInfo<ImportType, FqnType>>,
    /// All references found in the code
    pub references: Vec<ReferenceType>,
}

impl<FqnType, DefinitionType, ImportType> Analyzer<DefinitionType, FqnType, ImportType> {
    /// Create a new analyzer
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<FqnType, DefinitionType, ImportType> Default
    for Analyzer<FqnType, DefinitionType, ImportType>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<FqnType, DefinitionType, ImportType, ReferenceType>
    AnalysisResult<FqnType, DefinitionType, ImportType, ReferenceType>
where
    DefinitionType: Clone + PartialEq + Eq + std::hash::Hash,
    ImportType: Clone + PartialEq + Eq + std::hash::Hash,
{
    /// Create a new analysis result
    pub fn new(
        definitions: Vec<DefinitionInfo<DefinitionType, FqnType>>,
        imports: Vec<ImportedSymbolInfo<ImportType, FqnType>>,
        references: Vec<ReferenceType>,
    ) -> Self {
        Self {
            definitions,
            imports,
            references,
        }
    }

    /// Get FQN strings for all definitions that have them
    /// Takes a function to convert FQN to string representation
    pub fn definition_fqn_strings<ToString>(&self, fqn_to_string: ToString) -> Vec<String>
    where
        ToString: Fn(&FqnType) -> String,
    {
        self.definitions
            .iter()
            .map(|def| fqn_to_string(&def.fqn))
            .collect()
    }

    pub fn count_imports_by_type(&self) -> HashMap<ImportType, usize> {
        let mut counts = HashMap::new();
        for import in &self.imports {
            *counts.entry(import.import_type.clone()).or_insert(0) += 1;
        }
        counts
    }
}
