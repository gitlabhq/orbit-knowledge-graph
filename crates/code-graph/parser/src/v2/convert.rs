use code_graph_types::CanonicalResult;

/// Parses a source file into canonical types, optionally retaining the
/// raw AST for downstream resolution.
///
/// The associated type `Ast` determines what (if anything) the parser
/// preserves beyond the `CanonicalResult`. Languages that don't need
/// AST-level resolution set `Ast = ()`. Languages whose resolvers walk
/// expression trees set `Ast` to the concrete tree-sitter root type.
pub trait CanonicalParser {
    type Ast: Send;

    fn parse_file(
        &self,
        source: &[u8],
        file_path: &str,
    ) -> crate::Result<(CanonicalResult, Self::Ast)>;
}
