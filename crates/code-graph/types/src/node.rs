use crate::fqn::Fqn;
use crate::range::Range;
use code_graph_config::Language;
use strum::{AsRefStr, Display, EnumIter, EnumString};

/// Canonical definition categories used by the linker for
/// language-agnostic relationship determination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, EnumString, AsRefStr, Display)]
#[strum(serialize_all = "snake_case")]
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
    pub const fn as_upper_str(&self) -> &'static str {
        match self {
            Self::Class => "CLASS",
            Self::Interface => "INTERFACE",
            Self::Module => "MODULE",
            Self::Function => "FUNCTION",
            Self::Method => "METHOD",
            Self::Constructor => "CONSTRUCTOR",
            Self::Lambda => "LAMBDA",
            Self::Property => "PROPERTY",
            Self::EnumEntry => "ENUM_ENTRY",
            Self::Other => "OTHER",
        }
    }
}

/// Resolution status of a reference at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, EnumString, AsRefStr, Display)]
#[strum(serialize_all = "snake_case")]
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

/// A directory in the code graph.
#[derive(Debug, Clone)]
pub struct CanonicalDirectory {
    pub path: String,
    pub name: String,
}

/// A file in the code graph.
#[derive(Debug, Clone)]
pub struct CanonicalFile {
    pub path: String,
    pub name: String,
    pub extension: String,
    pub language: Language,
    pub size: u64,
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
