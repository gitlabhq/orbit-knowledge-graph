mod edge;
mod fqn;
mod node;
mod range;
mod scope;

pub use edge::{containment_edge_kind, containment_relationship, EdgeKind, NodeKind, Relationship};
pub use fqn::Fqn;
pub use node::{
    CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalImport, CanonicalReference,
    CanonicalResult, DefKind, DefinitionMetadata, ExpressionStep, ReferenceStatus, ToCanonical,
};
pub use range::{Position, Range};
pub use scope::{HasRange, ScopeIndex};

/// Parses a source file into canonical types, optionally retaining the
/// raw AST for downstream resolution.
///
/// The associated type `Ast` determines what (if anything) the parser
/// preserves beyond the `CanonicalResult`. Languages that don't need
/// AST-level resolution set `Ast = ()`. Languages whose resolvers walk
/// expression trees set `Ast` to the concrete tree-sitter root type.
pub trait CanonicalParser: Send + Sync {
    type Ast: Send;

    fn parse_file(
        &self,
        source: &[u8],
        file_path: &str,
    ) -> anyhow::Result<(CanonicalResult, Self::Ast)>;
}
