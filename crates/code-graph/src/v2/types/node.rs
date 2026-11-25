use super::fqn::Fqn;
use super::range::Range;
use crate::v2::config::Language;
use orbit_utils::markdown::FrontMatterValues;
use strum::{AsRefStr, Display, EnumIter, EnumString};

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

    pub const fn is_type_container(&self) -> bool {
        matches!(self, Self::Class | Self::Interface | Self::Module)
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
    pub metadata: Option<Box<DefinitionMetadata>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DefinitionMetadata {
    pub super_types: Vec<String>,
    pub return_type: Option<String>,
    pub type_annotation: Option<String>,
    pub receiver_type: Option<String>,
    pub decorators: Vec<String>,
    pub companion_of: Option<String>,
    pub is_exported: bool,
    pub front_matter: Option<Box<FrontMatterValues>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ImportBindingKind {
    #[default]
    Named,
    Primary,
    Namespace,
    SideEffect,
}

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
    pub wildcard: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExpressionStep {
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
    pub reason: crate::v2::error::FileReason,
}

impl CanonicalFile {
    pub fn language_name(&self) -> &'static str {
        self.language.map_or("unknown", |lang| lang.names()[0])
    }
}
