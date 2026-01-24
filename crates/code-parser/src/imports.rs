//! Generic structures for representing imported symbols across languages
//!
//! This module provides generic data structures for representing imports found in source code.

use std::collections::HashMap;

use crate::AnalysisResult;
use crate::fqn::Fqn;
use crate::utils::Range;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImportIdentifier {
    /// Original name, e.g. "foo" in `from module import foo as bar`
    pub name: String,
    /// Alias, e.g. "bar" in `from module import foo as bar`
    pub alias: Option<String>,
}

/// Generic imported symbol information structure
///
/// This represents an imported symbol found in source code with its metadata.
/// It is generic over both the import type and the FQN type to allow
/// language-specific implementations while maintaing a common structure.
///
/// # Examples
///
/// ```rust
/// use parser_core::imports::ImportedSymbolInfo;
/// use parser_core::python::types::PythonFqn;
///
/// // For a language-specific import type
/// #[derive(Debug, Clone, PartialEq)]
/// enum PythonImportType {
///     Import,                    // import module
///     AliasedImport,             // import module as alias
///     FromImport,                // from module import symbol
///     AliasedFromImport,         // from module import symbol as alias
///     WildcardImport,            // from module import *
///     RelativeWildcardImport,    // from . import *
///     RelativeImport,            // from .. import symbol
///     AliasedRelativeImport,     // from .. import symbol as alias
///     FutureImport,              // from __future__ import symbol
///     AliasedFutureImport,       // from __future__ import symbol as alias
/// }
///
/// // Python-specific import with Python FQN
/// type PythonImportedSymbolInfo = ImportedSymbolInfo<PythonImportType, PythonFqn>;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImportedSymbolInfo<ImportType, FqnType = Fqn<String>> {
    /// The type of import (regular, from, aliased, wildcard, etc.) - language-specific
    pub import_type: ImportType,
    /// The import path as specified in the source code
    /// e.g., "./my_module", "react", "../utils"
    pub import_path: String,
    /// Information about the imported identifier(s)
    /// None for side-effect imports like `import "./styles.css"`
    pub identifier: Option<ImportIdentifier>,
    /// Location of the enclosing import statement
    pub range: Range,
    /// Scope in which the import was made - language-specific
    pub scope: Option<FqnType>,
}

impl<ImportType, FqnType> ImportedSymbolInfo<ImportType, FqnType> {
    /// Create a new imported symbol info with the specified parameters
    pub fn new(
        import_type: ImportType,
        import_path: String,
        identifier: Option<ImportIdentifier>,
        range: Range,
        scope: Option<FqnType>,
    ) -> Self {
        Self {
            import_type,
            import_path,
            identifier,
            range,
            scope,
        }
    }
}

/// Trait for import types that can be converted to string representations
pub trait ImportTypeInfo {
    /// Get the string representation of this import type
    fn as_str(&self) -> &str;
}

/// Trait for common import lookup operations
///
/// This trait provides common operations for working with collections of imported symbols,
/// allowing for easy filtering and searching of imports by type, name, or source.
pub trait ImportLookup<FqnType, DefinitionType, ImportType, ReferenceType> {
    /// Get imports of a specific type
    fn imports_of_type(
        &self,
        import_type: &ImportType,
    ) -> Vec<&ImportedSymbolInfo<ImportType, FqnType>>;

    /// Count imports by type
    fn count_imports_by_type(&self) -> HashMap<ImportType, usize>
    where
        ImportType: Clone + std::hash::Hash + Eq;
}

impl<FqnType, DefinitionType, ImportType, ReferenceType>
    ImportLookup<FqnType, DefinitionType, ImportType, ReferenceType>
    for AnalysisResult<FqnType, DefinitionType, ImportType, ReferenceType>
where
    ImportType: PartialEq + Clone + std::hash::Hash + Eq,
{
    fn imports_of_type(
        &self,
        import_type: &ImportType,
    ) -> Vec<&ImportedSymbolInfo<ImportType, FqnType>> {
        self.imports
            .iter()
            .filter(|import| &import.import_type == import_type)
            .collect()
    }

    fn count_imports_by_type(&self) -> HashMap<ImportType, usize> {
        let mut counts = HashMap::new();
        for import in &self.imports {
            *counts.entry(import.import_type.clone()).or_insert(0) += 1;
        }
        counts
    }
}
