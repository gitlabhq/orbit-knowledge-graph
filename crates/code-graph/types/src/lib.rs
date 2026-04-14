mod edge;
mod fqn;
mod node;
mod range;
mod scope;

pub use edge::{EdgeKind, NodeKind, Relationship, containment_edge_kind, containment_relationship};
pub use fqn::Fqn;
pub use node::{
    CanonicalBinding, CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalImport,
    CanonicalReference, CanonicalResult, DefKind, DefinitionMetadata, ExpressionStep,
    ReferenceStatus,
};
pub use range::{Position, Range};
pub use scope::{HasRange, ScopeIndex};

/// Parses a source file into canonical types, retaining the raw AST
/// for downstream SSA-based resolution.
///
/// `Ast` determines what the parser preserves beyond the `CanonicalResult`.
/// For tree-sitter languages this is `Root<StrDoc<SupportLang>>`.
/// Custom pipelines (e.g. Prism) provide their own AST type that
/// implements `AsAst`.
pub trait CanonicalParser: Send + Sync {
    type Ast: Send;

    fn parse_file(
        &self,
        source: &[u8],
        file_path: &str,
    ) -> anyhow::Result<(CanonicalResult, Self::Ast)>;
}
