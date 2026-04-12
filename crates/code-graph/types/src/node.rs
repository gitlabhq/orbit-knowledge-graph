use crate::fqn::Fqn;
use crate::lang::Language;
use crate::range::Range;

/// Canonical definition categories used by the linker for
/// language-agnostic relationship determination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DefKind {
    Class,
    Interface,
    Module,
    Function,
    Method,
    Constructor,
    Lambda,
    Property,
    EnumEntry,
    Other,
}

impl DefKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Class => "class",
            Self::Interface => "interface",
            Self::Module => "module",
            Self::Function => "function",
            Self::Method => "method",
            Self::Constructor => "constructor",
            Self::Lambda => "lambda",
            Self::Property => "property",
            Self::EnumEntry => "enum_entry",
            Self::Other => "other",
        }
    }
}

impl std::fmt::Display for DefKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Edge kinds for the code graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeKind {
    Contains,
    Defines,
    Imports,
    Calls,
}

impl EdgeKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Contains => "CONTAINS",
            Self::Defines => "DEFINES",
            Self::Imports => "IMPORTS",
            Self::Calls => "CALLS",
        }
    }
}

impl std::fmt::Display for EdgeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Resolution status of a reference at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReferenceStatus {
    Resolved,
    Ambiguous,
    Unresolved,
}

/// Trait for converting language-specific definition type enums to DefKind.
/// Implemented by each parser language module.
pub trait ToCanonical {
    fn to_def_kind(&self) -> DefKind;
}

/// A parsed definition, language-agnostic.
#[derive(Debug, Clone)]
pub struct CanonicalDefinition {
    /// The language-specific type string (e.g. "DecoratedAsyncMethod", "SingletonMethod").
    /// Preserved for output fidelity — the Arrow column `definition_type` gets this value.
    pub definition_type: &'static str,
    /// The canonical category for relationship logic.
    pub kind: DefKind,
    pub name: String,
    pub fqn: Fqn,
    pub range: Range,
    /// Whether this is a top-level definition (not nested inside another definition).
    /// Replaces the old `fqn[0].node_type == Package/Namespace` checks.
    pub is_top_level: bool,
}

/// A parsed import, language-agnostic.
#[derive(Debug, Clone)]
pub struct CanonicalImport {
    /// The language-specific import type string (e.g. "RequireRelative", "WildcardImport").
    pub import_type: &'static str,
    pub path: String,
    pub name: Option<String>,
    pub alias: Option<String>,
    pub scope_fqn: Option<Fqn>,
    pub range: Range,
}

/// A parsed reference (call site / usage), language-agnostic.
#[derive(Debug, Clone)]
pub struct CanonicalReference {
    /// The language-specific reference type string (e.g. "Call", "PropertyAccess").
    pub reference_type: &'static str,
    pub name: String,
    pub range: Range,
    pub scope_fqn: Option<Fqn>,
    pub status: ReferenceStatus,
    /// FQN of the resolved target, if any.
    pub target_fqn: Option<Fqn>,
}

/// The complete output of parsing a single file. This is the boundary
/// type between the parser and the linker — the parser produces this,
/// the linker consumes it. Nothing language-specific crosses this boundary.
#[derive(Debug, Clone)]
pub struct CanonicalResult {
    pub file_path: String,
    pub extension: String,
    pub file_size: u64,
    pub language: Language,
    pub definitions: Vec<CanonicalDefinition>,
    pub imports: Vec<CanonicalImport>,
    pub references: Vec<CanonicalReference>,
}
