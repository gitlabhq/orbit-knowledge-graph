use parser_core::utils::Range;
use std::collections::HashMap;

/// Owned per-file data that survives past OXC's allocator scope.
/// Extracted at the end of Pass 1 for use in Pass 2 cross-file resolution.
#[derive(Debug, Clone, Default)]
pub struct JsModuleInfo {
    /// What this file exports (exported name -> binding info)
    pub exports: HashMap<String, ExportedBinding>,
    /// What this file imports (each import entry)
    pub imports: Vec<OwnedImportEntry>,
    /// Star re-export source specifiers (`export * from './foo'`)
    pub star_export_sources: Vec<String>,
    /// CJS exports detected from `module.exports` / `exports.*` patterns
    pub cjs_exports: Vec<CjsExport>,
    /// Whether this file has ESM syntax (import/export)
    pub has_module_syntax: bool,
    /// Definition FQNs with their ranges (for cross-file edge targets)
    pub definition_fqns: HashMap<String, Range>,
}

#[derive(Debug, Clone)]
pub struct ExportedBinding {
    pub local_fqn: String,
    pub range: Range,
    pub is_type: bool,
    pub is_default: bool,
}

#[derive(Debug, Clone)]
pub struct OwnedImportEntry {
    pub specifier: String,
    pub imported_name: ImportedName,
    pub local_name: String,
    pub is_type: bool,
    pub range: Range,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportedName {
    Named(String),
    Default,
    Namespace,
}

/// CommonJS export pattern
#[derive(Debug, Clone)]
pub enum CjsExport {
    /// `module.exports = <expr>` -- whole-module export
    Default { range: Range },
    /// `exports.foo = <expr>` or `module.exports.foo = <expr>`
    Named { name: String, range: Range },
}

/// Confidence level for call graph edges
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CallConfidence {
    /// Direct call, import binding, `this.method()` in same class
    Known,
    /// Resolved via explicit type annotation (`: Foo`)
    Annotated,
    /// Resolved via `new Foo()` initializer or return type annotation
    Inferred,
    /// Resolved via inheritance chain or known globals
    Guessed,
}

impl CallConfidence {
    pub fn as_str(&self) -> &'static str {
        match self {
            CallConfidence::Known => "known",
            CallConfidence::Annotated => "annotated",
            CallConfidence::Inferred => "inferred",
            CallConfidence::Guessed => "guessed",
        }
    }
}
