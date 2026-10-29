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
    Struct,
    Interface,
    Trait,
    Enum,
    TypeAlias,
    Module,
    Function,
    Method,
    Constructor,
    Test,
    Lambda,
    Property,
    Field,
    Constant,
    Variable,
    EnumEntry,
    Macro,
}

impl DefKind {
    pub const fn as_upper_str(&self) -> &'static str {
        match self {
            Self::Class => "CLASS",
            Self::Struct => "STRUCT",
            Self::Interface => "INTERFACE",
            Self::Trait => "TRAIT",
            Self::Enum => "ENUM",
            Self::TypeAlias => "TYPE_ALIAS",
            Self::Module => "MODULE",
            Self::Function => "FUNCTION",
            Self::Method => "METHOD",
            Self::Constructor => "CONSTRUCTOR",
            Self::Test => "TEST",
            Self::Lambda => "LAMBDA",
            Self::Property => "PROPERTY",
            Self::Field => "FIELD",
            Self::Constant => "CONSTANT",
            Self::Variable => "VARIABLE",
            Self::EnumEntry => "ENUM_ENTRY",
            Self::Macro => "MACRO",
        }
    }

    /// When a value resolves to a definition of this kind, the resolver uses its
    /// FQN as a type name for member lookup.
    pub const fn is_type_container(&self) -> bool {
        matches!(
            self,
            Self::Class | Self::Struct | Self::Interface | Self::Trait | Self::Enum | Self::Module
        )
    }

    pub const fn is_referable(&self) -> bool {
        !matches!(self, Self::Test)
    }

    pub const fn is_interface_like(&self) -> bool {
        matches!(self, Self::Interface | Self::Trait)
    }

    pub const fn kind(&self) -> Kind {
        match self {
            Self::Class => Kind::Class,
            Self::Struct => Kind::Struct,
            Self::Interface => Kind::Interface,
            Self::Trait => Kind::Trait,
            Self::Enum => Kind::Enum,
            Self::TypeAlias => Kind::TypeAlias,
            Self::Module => Kind::Module,
            Self::Function | Self::Lambda => Kind::Function,
            Self::Method => Kind::Method,
            Self::Constructor => Kind::Constructor,
            Self::Test => Kind::Test,
            Self::Property | Self::Field => Kind::Field,
            Self::Constant => Kind::Constant,
            Self::Variable => Kind::Variable,
            Self::EnumEntry => Kind::EnumMember,
            Self::Macro => Kind::Macro,
        }
    }

    pub const fn category(&self) -> Category {
        self.kind().category()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, AsRefStr, IntoStaticStr, Display)]
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

    pub const fn category(&self) -> Category {
        match self {
            Self::Class
            | Self::Struct
            | Self::Interface
            | Self::Trait
            | Self::Enum
            | Self::TypeAlias => Category::Type,
            Self::Function | Self::Method | Self::Constructor | Self::Test => Category::Callable,
            Self::Field | Self::Constant | Self::Variable | Self::EnumMember => Category::Value,
            Self::Module => Category::Module,
            Self::Macro => Category::Macro,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, AsRefStr, IntoStaticStr, Display)]
pub enum Category {
    Type,
    Callable,
    Value,
    Module,
    Macro,
}

impl Category {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, EnumIter, AsRefStr, IntoStaticStr, Display,
)]
#[strum(serialize_all = "snake_case")]
pub enum DefScope {
    #[default]
    TopLevel,
    Member,
    Local,
}

impl DefScope {
    pub const fn outside_parent(kind: DefKind) -> Self {
        match kind {
            DefKind::Method
            | DefKind::Constructor
            | DefKind::Property
            | DefKind::Field
            | DefKind::EnumEntry => Self::Member,
            _ => Self::TopLevel,
        }
    }

    pub const fn inside(parent: DefKind) -> Self {
        match parent {
            DefKind::Test => Self::Member,
            _ => match parent.category() {
                Category::Callable | Category::Macro => Self::Local,
                Category::Type | Category::Module | Category::Value => Self::Member,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, AsRefStr, IntoStaticStr, Display)]
#[strum(serialize_all = "snake_case")]
pub enum Visibility {
    Public,
    Protected,
    Private,
    Internal,
    Package,
    File,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, AsRefStr, IntoStaticStr, Display)]
#[strum(serialize_all = "snake_case")]
pub enum Modifier {
    Abstract,
    Async,
    Extern,
    Final,
    Generator,
    Inline,
    Override,
    Readonly,
    Sealed,
    Static,
    Unsafe,
    Virtual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Modifiers(u16);

impl Modifiers {
    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn with(self, modifier: Modifier) -> Self {
        Self(self.0 | (1 << modifier as u16))
    }

    pub fn insert(&mut self, modifier: Modifier) {
        *self = self.with(modifier);
    }

    pub fn extend(&mut self, other: Modifiers) {
        self.0 |= other.0;
    }

    pub const fn contains(self, modifier: Modifier) -> bool {
        self.0 & (1 << modifier as u16) != 0
    }

    pub fn names(self) -> impl Iterator<Item = &'static str> {
        use strum::IntoEnumIterator;
        Modifier::iter()
            .filter(move |m| self.contains(*m))
            .map(<&'static str>::from)
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
    pub signature: Option<String>,
    pub parameters: Vec<String>,
    pub declared_return_type: Option<String>,
    pub declared_type: Option<String>,
    pub doc_comment: Option<String>,
    pub modifiers: Modifiers,
    pub visibility: Option<Visibility>,
    pub generated_by: Option<String>,
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
    pub flags: crate::v2::config::FileFlags,
}

impl CanonicalFile {
    pub fn language_name(&self) -> &'static str {
        self.language.map_or("unknown", |lang| lang.names()[0])
    }
}
