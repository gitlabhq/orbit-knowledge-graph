//! Generic structures for representing references across languages

use crate::{fqn::Fqn, utils::Range};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReferenceTarget<TargetResolutionType> {
    // Single, unambiguous target
    Resolved(Box<TargetResolutionType>),
    // Multiple possible targets (ambiguous reference)
    Ambiguous(Vec<TargetResolutionType>),
    // No target found
    Unresolved(),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TargetResolution<DefinitionInfoType, ImportedSymbolInfoType, PartialResolutionType> {
    // A definition (from the same file)
    Definition(DefinitionInfoType),
    // An imported symbol
    ImportedSymbol(ImportedSymbolInfoType),
    // An expression to be resolved later (e.g. in the indexer)
    PartialResolution(PartialResolutionType),
}

/// Generic reference data structure
///
/// Here is an example of how you'd implement this for Python:
///
/// ```rust
/// use parser_core::references::{ReferenceInfo, ReferenceTarget, TargetResolution};
/// use parser_core::python::types::{PythonFqn, PythonDefinitionInfo, PythonImportedSymbolInfo};
///
/// // Types of references (for now, we only care about function calls)
/// #[derive(Debug, Clone, PartialEq)]
/// enum PythonReferenceType {
///     Call // func()
/// }
///
/// // Symbol used in an expression
/// #[derive(Debug, Clone)]
/// struct AnnotatedSymbol<SymbolType, TargetResolution, Metadata = ()> {
///     pub symbol: String,
///     pub symbol_type: SymbolType,
///     pub target: Option<ReferenceTarget<TargetResolution>>,
///     pub metadata: Option<Metadata> // To include call args, or index keys
/// }
///
/// #[derive(Debug, Clone, PartialEq)]
/// enum SymbolType {
///     Identifier, // obj
///     Attribute,  // obj.attr
///     Method,     // obj.method()
///     Index,      // obj[key]
///     Call        // obj()
/// }
///
/// // A partial resolution (a chain of connected symbols, i.e. an expression)
/// #[derive(Debug, Clone)]
/// struct PythonExpression(Vec<AnnotatedSymbol<SymbolType, PythonTargetResolution>>);
///
/// // A resolved (or partially resolved) reference target
/// type PythonTargetResolution = TargetResolution<PythonDefinitionInfo, PythonImportedSymbolInfo, PythonExpression>;
///
/// // ~~Finally, the Python-specific reference~~
/// type PythonReferenceInfo = ReferenceInfo<PythonTargetResolution, PythonReferenceType>;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReferenceInfo<TargetResolutionType, ReferenceType, Metadata = (), FqnType = Fqn<String>>
{
    // Name of the reference (e.g. "foo.bar" in `foo.bar()`)
    pub name: String,
    // Location of the reference
    pub range: Range,
    // What's being referenced (language-specific resolution info)
    pub target: ReferenceTarget<TargetResolutionType>,
    // Language-specific reference type
    pub reference_type: ReferenceType,
    // Language-specific metadata (e.g. argument types)
    pub metadata: Option<Box<Metadata>>,
    // The scope of the reference
    pub scope: Option<FqnType>,
}

impl<TargetResolutionType, ReferenceType, Metadata, FqnType>
    ReferenceInfo<TargetResolutionType, ReferenceType, Metadata, FqnType>
{
    pub fn new(
        name: String,
        range: Range,
        target: ReferenceTarget<TargetResolutionType>,
        reference_type: ReferenceType,
        metadata: Option<Metadata>,
        scope: Option<FqnType>,
    ) -> Self {
        Self {
            name,
            range,
            target,
            reference_type,
            metadata: metadata.map(Box::new),
            scope,
        }
    }
}
