use code_graph_types::{
    CanonicalDefinition, CanonicalImport, CanonicalReference, CanonicalResult, DefKind, Fqn,
    Language, Range as CgRange, ReferenceStatus,
};

/// Converts existing per-language parser output into a `CanonicalResult`.
///
/// Each language's parser produces typed output (e.g. `RubyDefinitionInfo`,
/// `PythonImportedSymbolInfo`). This trait wraps that output into the
/// canonical types that the v2 linker consumes.
///
/// Implementations live here rather than in the per-language modules to
/// avoid modifying existing parser code.
pub trait FileResultConverter {
    /// The per-language parse output (e.g. AnalysisResult, DslParseOutput).
    type Input;

    fn convert(input: &Self::Input, file_path: &str, language: Language) -> CanonicalResult;
}

/// Convert a parser-core `Range` to a code-graph-types `Range`.
pub(crate) fn convert_range(r: crate::utils::Range) -> CgRange {
    CgRange::new(
        code_graph_types::Position::new(r.start.line, r.start.column),
        code_graph_types::Position::new(r.end.line, r.end.column),
        r.byte_offset,
    )
}

/// Build an `Fqn` from a slice of FQNPart-like items that have `node_name()`.
pub(crate) fn fqn_from_parts<T>(parts: &[T], separator: &'static str) -> Fqn
where
    T: AsRef<str>,
{
    let strs: Vec<&str> = parts.iter().map(|p| p.as_ref()).collect();
    Fqn::from_parts(&strs, separator)
}
