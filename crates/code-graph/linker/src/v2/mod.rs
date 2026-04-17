pub mod graph;
pub mod imports;
pub mod rules;
pub mod ssa;
pub mod state;
pub mod stats;
pub mod walker;

pub use graph::{CodeGraph, GraphEdge, GraphNode};
pub use imports::ResolveSettings;
pub use rules::{HasRules, NoRules, ResolutionRules};
pub use ssa::SsaResolver;
pub use state::{
    BlockId, GraphDef, GraphDefMeta, GraphImport, ReachingDefs, StrId, StringPool, Value,
};
pub use stats::{FileTimingEntry, ResolveStats, SsaStats, print_long_tail_analysis};

/// Trait for AST types that can provide a tree-sitter root for walking.
pub trait HasRoot {
    fn as_root(
        &self,
    ) -> Option<
        treesitter_visit::Node<
            '_,
            treesitter_visit::tree_sitter::StrDoc<treesitter_visit::SupportLang>,
        >,
    >;
}

impl HasRoot
    for treesitter_visit::Root<treesitter_visit::tree_sitter::StrDoc<treesitter_visit::SupportLang>>
{
    fn as_root(
        &self,
    ) -> Option<
        treesitter_visit::Node<
            '_,
            treesitter_visit::tree_sitter::StrDoc<treesitter_visit::SupportLang>,
        >,
    > {
        Some(self.root())
    }
}
