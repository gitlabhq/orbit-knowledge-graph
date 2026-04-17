mod edge;
mod fqn;
mod node;
mod range;
mod scope;
pub mod ssa;

pub use edge::{EdgeKind, NodeKind, Relationship, containment_edge_kind, containment_relationship};
pub use fqn::Fqn;
pub use node::{
    BindingKind, CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalImport, DefKind,
    DefinitionMetadata, ExpressionStep,
};
pub use range::{Position, Range};
pub use scope::{HasRange, ScopeIndex};

/// Interned string. Pointer-sized (8 bytes), O(1) clone/hash/eq.
/// Use for strings that appear repeatedly: definition names, FQN
/// segments, import paths, type names.
pub type IStr = internment::Intern<str>;
