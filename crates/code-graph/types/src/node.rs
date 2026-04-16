use crate::IStr;
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
///
/// String fields use `IStr` (8 bytes, interned). Common names like "get",
/// "set", "toString" are shared across thousands of definitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalDefinition {
    /// Language-specific type string preserved for output fidelity.
    pub definition_type: &'static str,
    /// Canonical category for containment and relationship logic.
    pub kind: DefKind,
    pub name: IStr,
    pub fqn: Fqn,
    pub range: Range,
    /// Whether this is a top-level (not nested inside another definition).
    pub is_top_level: bool,
    pub metadata: Option<Box<DefinitionMetadata>>,
}

/// Resolution-relevant metadata extracted by the parser.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DefinitionMetadata {
    /// Super types (class parents, implemented interfaces).
    pub super_types: Vec<IStr>,
    /// Return type of a method/function.
    pub return_type: Option<IStr>,
    /// Type annotation on a field/parameter/variable.
    pub type_annotation: Option<IStr>,
    /// Receiver type for extension functions (Kotlin).
    pub receiver_type: Option<IStr>,
    /// Decorators/annotations (Python's `@classmethod`, etc.).
    pub decorators: Vec<IStr>,
    /// If this is a companion object, the FQN of the owning class.
    pub companion_of: Option<IStr>,
}

/// A parsed import, language-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalImport {
    /// Language-specific import type (e.g. "RequireRelative", "FromImport").
    pub import_type: &'static str,
    pub path: IStr,
    pub name: Option<IStr>,
    pub alias: Option<IStr>,
    pub scope_fqn: Option<Fqn>,
    pub range: Range,
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

/// A variable binding extracted from the AST.
///
/// Represents `x = expr`, `def f(param)`, `del x`, `for x in ...`, etc.
/// The walker writes these to SSA blocks without touching the AST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalBinding {
    pub name: String,
    pub kind: BindingKind,
    pub range: Range,
    /// Type annotation from the AST (e.g. `int x` → "int", `x: str` → "str").
    pub type_annotation: Option<String>,
    /// Callee name extracted from the RHS (e.g. `get_builder()` → "get_builder",
    /// `foo` → "foo"). None for complex/non-name expressions.
    pub rhs_name: Option<String>,
    /// Whether this is an instance attribute binding (e.g. `self.db = ...`).
    pub instance_attr: bool,
}

// ── Control flow ────────────────────────────────────────────────

/// A branch or loop extracted from the AST.
///
/// The walker uses these to build SSA blocks (Braun et al.) without
/// re-walking the tree-sitter AST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalControlFlow {
    pub kind: ControlFlowKind,
    /// AST node kind (e.g. "if_statement", "for_statement") for
    /// disambiguation when parent and child share the same byte start.
    pub node_kind: String,
    /// Byte range of the entire control-flow node.
    pub byte_range: (usize, usize),
    /// Arms/body of the control-flow node.
    pub children: Vec<ControlFlowChild>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFlowKind {
    Branch {
        /// Whether the branch has a catch-all arm (else, default, finally).
        has_catch_all: bool,
    },
    Loop,
}

/// One arm of a branch or the body of a loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlFlowChild {
    pub byte_range: (usize, usize),
    /// True if this child is the condition expression (walked in the
    /// pre-branch block, not in a branch arm).
    pub is_condition: bool,
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
    pub language: Language,
    pub size: u64,
}

/// The complete output of parsing a single file.
///
/// Boundary type between parser and linker — nothing language-specific
/// crosses this boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalResult {
    pub file_path: String,
    pub extension: String,
    pub file_size: u64,
    pub language: Language,
    pub definitions: Vec<CanonicalDefinition>,
    pub imports: Vec<CanonicalImport>,
    pub references: Vec<CanonicalReference>,
    pub bindings: Vec<CanonicalBinding>,
    pub control_flow: Vec<CanonicalControlFlow>,
}
