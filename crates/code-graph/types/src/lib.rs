mod edge;
mod fqn;
mod node;
mod range;
mod scope;
pub mod ssa;

pub use edge::{EdgeKind, NodeKind, Relationship, containment_edge_kind, containment_relationship};
pub use fqn::Fqn;
pub use node::{
    BindingKind, CanonicalBinding, CanonicalControlFlow, CanonicalDefinition, CanonicalDirectory,
    CanonicalFile, CanonicalImport, CanonicalReference, CanonicalResult, ControlFlowChild,
    ControlFlowKind, DefKind, DefinitionMetadata, ExpressionStep, ReferenceStatus,
};
pub use range::{Position, Range};
pub use scope::{HasRange, ScopeIndex};

/// Interned string. Pointer-sized (8 bytes), O(1) clone/hash/eq.
/// Use for strings that appear repeatedly: definition names, FQN
/// segments, import paths, type names.
pub type IStr = internment::Intern<str>;

/// Parses a source file into canonical types, retaining the raw AST
/// for downstream SSA-based resolution.
///
/// `Ast` determines what the parser preserves beyond the `CanonicalResult`.
/// For tree-sitter languages this is `Root<StrDoc<SupportLang>>`.
/// Custom pipelines (e.g. Prism) provide their own AST type that
/// implements `HasRoot`.
pub trait CanonicalParser: Send + Sync {
    type Ast: Send;

    fn parse_file(
        &self,
        source: &[u8],
        file_path: &str,
    ) -> anyhow::Result<(CanonicalResult, Self::Ast)>;

    /// Parse for defs+imports only (skip refs/bindings/cf). No AST returned.
    fn parse_defs_only(&self, source: &[u8], file_path: &str) -> anyhow::Result<CanonicalResult> {
        let (result, _ast) = self.parse_file(source, file_path)?;
        Ok(result)
    }
}
