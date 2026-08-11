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

    /// When a value resolves to a definition of this kind, the resolver uses its
    /// FQN as a type name for member lookup.
    pub const fn is_type_container(&self) -> bool {
        matches!(self, Self::Class | Self::Interface | Self::Module)
    }
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
