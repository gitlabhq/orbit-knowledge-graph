use super::fqn::Fqn;
use super::range::Range;
use crate::v2::config::Language;
use strum::{AsRefStr, Display, EnumIter, EnumString, IntoStaticStr};

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

    /// When a value resolves to a definition of this kind, the resolver uses its
    /// FQN as a type name for member lookup.
    pub const fn is_type_container(&self) -> bool {
        matches!(self, Self::Class | Self::Interface | Self::Module)
    }

    pub const fn ontology_kind(&self) -> Kind {
        match self {
            Self::Class => Kind::Class,
            Self::Interface => Kind::Interface,
            Self::Module => Kind::Module,
            Self::Function | Self::Lambda => Kind::Function,
            Self::Method => Kind::Method,
            Self::Constructor => Kind::Constructor,
            Self::Property => Kind::Field,
            Self::EnumEntry => Kind::EnumMember,
            Self::Other => Kind::Variable,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, IntoStaticStr)]
pub enum Kind {
    Class,
    Struct,
    Interface,
    Trait,
    Enum,
    TypeAlias,
    Function,
    Method,
    Constructor,
    Test,
    Field,
    Constant,
    Variable,
    EnumMember,
    Module,
    Macro,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    pub const fn category(&self) -> &'static str {
        match self {
            Self::Class
            | Self::Struct
            | Self::Interface
            | Self::Trait
            | Self::Enum
            | Self::TypeAlias => "Type",
            Self::Function | Self::Method | Self::Constructor | Self::Test => "Callable",
            Self::Field | Self::Constant | Self::Variable | Self::EnumMember => "Value",
            Self::Module => "Module",
            Self::Macro => "Macro",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalDefinition {
    pub definition_type: &'static str,
    pub kind: DefKind,
    pub name: String,
    pub fqn: Fqn,
    pub range: Range,
    pub is_top_level: bool,
    /// `None` for most definitions keeps the common case small.
    pub metadata: Option<Box<DefinitionMetadata>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DefinitionMetadata {
    pub super_types: Vec<String>,
    /// `None` for void/untyped.
    pub return_type: Option<String>,
    pub type_annotation: Option<String>,
    /// Receiver type for extension functions (Kotlin).
    pub receiver_type: Option<String>,
    pub decorators: Vec<String>,
    /// If this is a companion object, the FQN of the owning class.
    pub companion_of: Option<String>,
    pub is_exported: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ImportBindingKind {
    #[default]
    Named,
    Primary,
    Namespace,
    SideEffect,
}

/// Whether a binding enters scope through a declarative import form or a runtime load primitive.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ImportMode {
    #[default]
    Declarative,
    Runtime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalImport {
    pub import_type: &'static str,
    pub binding_kind: ImportBindingKind,
    pub mode: ImportMode,
    pub path: String,
    pub name: Option<String>,
    pub alias: Option<String>,
    pub scope_fqn: Option<Fqn>,
    pub range: Range,
    pub is_type_only: bool,
    /// The resolver uses this to drive wildcard import lookup instead of matching
    /// on `import_type` strings.
    pub wildcard: bool,
}

/// Chains are read left-to-right. The resolver resolves the base then applies
/// each subsequent step, threading the resolved type through.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExpressionStep {
    /// Bare identifier — the base of the chain.
    Ident(smol_str::SmolStr),
    Field(smol_str::SmolStr),
    Call(smol_str::SmolStr),
    New(smol_str::SmolStr),
    This,
    Super,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingKind {
    Assignment,
    Parameter,
    Deletion,
    ForTarget,
    WithAlias,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalDirectory {
    pub path: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalFile {
    pub path: String,
    pub name: String,
    pub extension: String,
    pub language: Option<Language>,
    pub size: u64,
    /// Why the file did not index cleanly (skip/fault), or `None`. Strictly an
    /// enum so `gl_file.reason` can never hold an unbounded string.
    pub reason: crate::v2::error::FileReason,
}

impl CanonicalFile {
    pub fn language_name(&self) -> &'static str {
        self.language.map_or("unknown", |lang| lang.names()[0])
    }
}
