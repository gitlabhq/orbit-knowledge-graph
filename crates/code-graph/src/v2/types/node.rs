use super::fqn::Fqn;
use super::range::Range;
use crate::v2::config::Language;
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

    /// Whether this kind produces a type scope that can have members.
    /// Used by the resolver for type-flow: when a value resolves to a
    /// definition of this kind, the resolver uses its FQN as a type name
    /// for member lookup.
    pub const fn is_type_container(&self) -> bool {
        matches!(self, Self::Class | Self::Interface | Self::Module)
    }
}

/// A parsed definition, language-agnostic.
///
/// All strings are `IStr` (8 bytes, interned). Common names like "get",
/// "set", "toString" are shared across thousands of definitions.
/// Metadata is inlined — no `Box` indirection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalDefinition {
    /// Language-specific type string preserved for output fidelity.
    pub definition_type: &'static str,
    /// Canonical category for containment and relationship logic.
    pub kind: DefKind,
    pub name: String,
    pub fqn: Fqn,
    pub range: Range,
    /// Whether this is a top-level (not nested inside another definition).
    pub is_top_level: bool,
    /// Boxed metadata — `None` for most definitions keeps the common case small.
    pub metadata: Option<Box<DefinitionMetadata>>,
}

/// Resolution-relevant metadata extracted by the parser.
///
/// Flat struct with optional fields — each parser fills in what its
/// language provides. The resolver reads whichever fields are present
/// without knowing the source language.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DefinitionMetadata {
    /// Super types (class parents, implemented interfaces).
    pub super_types: Vec<String>,
    /// Return type of a method/function. `None` for void/untyped.
    pub return_type: Option<String>,
    /// Type annotation on a field/parameter/variable.
    pub type_annotation: Option<String>,
    /// Receiver type for extension functions (Kotlin).
    pub receiver_type: Option<String>,
    /// Decorators/annotations (Python's `@classmethod`, etc.).
    pub decorators: Vec<String>,
    /// If this is a companion object, the FQN of the owning class.
    pub companion_of: Option<String>,
    /// Whether the definition is exported from the file/module.
    pub is_exported: bool,
}

/// Language-agnostic import binding categories.
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

/// A parsed import, language-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalImport {
    /// Language-specific import type (e.g. "RequireRelative", "FromImport").
    pub import_type: &'static str,
    pub binding_kind: ImportBindingKind,
    pub mode: ImportMode,
    pub path: String,
    pub name: Option<String>,
    pub alias: Option<String>,
    pub scope_fqn: Option<Fqn>,
    pub range: Range,
    /// Whether this import exists only in the type space (`import type`, etc.).
    pub is_type_only: bool,
    /// Whether this is a wildcard import (e.g. `from foo import *`,
    /// `import java.util.*`). Set by the parser; the resolver uses
    /// this to drive wildcard import lookup instead of matching on
    /// `import_type` strings.
    pub wildcard: bool,
}

/// A single step in a linearized expression chain.
///
/// Chains are read left-to-right. The resolver resolves the base
/// then applies each subsequent step, threading the resolved type through.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExpressionStep {
    /// Bare identifier — the base of the chain.
    Ident(smol_str::SmolStr),
    /// Field/attribute access.
    Field(smol_str::SmolStr),
    /// Method/function call.
    Call(smol_str::SmolStr),
    /// Constructor invocation (`new Foo()`).
    New(smol_str::SmolStr),
    /// `this` or `self` reference.
    This,
    /// `super` reference.
    Super,
}

// ── Bindings ────────────────────────────────────────────────────

/// What kind of variable binding this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingKind {
    Assignment,
    Parameter,
    Deletion,
    ForTarget,
    WithAlias,
}

// ── Structural types ────────────────────────────────────────────

/// A directory in the code graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalDirectory {
    pub path: String,
    pub name: String,
}

/// A file in the code graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalFile {
    pub path: String,
    pub name: String,
    pub extension: String,
    pub language: Option<Language>,
    pub size: u64,
}

impl CanonicalFile {
    pub fn language_name(&self) -> &'static str {
        self.language.map_or("unknown", |lang| lang.names()[0])
    }
}
