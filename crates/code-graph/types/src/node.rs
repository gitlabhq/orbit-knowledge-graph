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

    /// Whether this kind produces a type scope that can have members.
    /// Used by the resolver for type-flow: when a value resolves to a
    /// definition of this kind, the resolver uses its FQN as a type name
    /// for member lookup.
    pub const fn is_type_container(&self) -> bool {
        matches!(self, Self::Class | Self::Interface | Self::Module)
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

/// A parsed definition, language-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalDefinition {
    /// Language-specific type string preserved for output fidelity.
    /// e.g. "DecoratedAsyncMethod", "SingletonMethod".
    /// The Arrow column `definition_type` gets this value.
    pub definition_type: &'static str,
    /// Canonical category for containment and relationship logic.
    pub kind: DefKind,
    pub name: String,
    pub fqn: Fqn,
    pub range: Range,
    /// Whether this is a top-level (not nested inside another definition).
    pub is_top_level: bool,
    /// Language-neutral metadata for resolution. Boxed because most
    /// definitions carry none, keeping the common case small.
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
}

/// A parsed import, language-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalImport {
    /// Language-specific import type (e.g. "RequireRelative", "FromImport").
    pub import_type: &'static str,
    pub path: String,
    pub name: Option<String>,
    pub alias: Option<String>,
    pub scope_fqn: Option<Fqn>,
    pub range: Range,
    /// Whether this is a wildcard import (e.g. `from foo import *`,
    /// `import java.util.*`). Set by the parser; the resolver uses
    /// this to drive wildcard import lookup instead of matching on
    /// `import_type` strings.
    pub wildcard: bool,
}

/// A parsed reference (call site / usage), language-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalReference {
    /// Language-specific reference type (e.g. "Call", "PropertyAccess").
    pub reference_type: &'static str,
    pub name: String,
    pub range: Range,
    pub scope_fqn: Option<Fqn>,
    pub status: ReferenceStatus,
    /// FQN of the resolved target, if any.
    pub target_fqn: Option<Fqn>,
    /// Linearized expression chain for member access resolution.
    /// e.g. `obj.getService().process()` becomes
    ///   `[Ident("obj"), Field("getService"), Call, Field("process"), Call]`
    pub expression: Option<Vec<ExpressionStep>>,
}

/// A single step in a linearized expression chain.
///
/// Chains are read left-to-right. The resolver resolves the base
/// then applies each subsequent step, threading the resolved type through.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExpressionStep {
    /// Bare identifier — the base of the chain.
    Ident(String),
    /// Field/attribute access.
    Field(String),
    /// Method/function call.
    Call(String),
    /// Constructor invocation (`new Foo()`).
    New(String),
    /// `this` or `self` reference.
    This,
    /// `super` reference.
    Super,
    /// Array/index access.
    Index,
    /// Method reference (`Foo::bar`).
    MethodRef(String),
}

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
    pub language: Language,
    pub size: u64,
}

/// The complete output of parsing a single file.
///
/// Boundary type between parser and linker — nothing language-specific
/// crosses this boundary. Bindings are not extracted at parse time;
/// the SSA walker discovers them from the AST using `BindingRule`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalResult {
    pub file_path: String,
    pub extension: String,
    pub file_size: u64,
    pub language: Language,
    pub definitions: Vec<CanonicalDefinition>,
    pub imports: Vec<CanonicalImport>,
    pub references: Vec<CanonicalReference>,
}
