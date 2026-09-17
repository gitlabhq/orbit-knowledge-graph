use crate::intern::Lang;
use crate::pattern::{EdgeCtx, Rewrite, apply_rewrites_with_edges};
use crate::tree::{Edge, Tree};

pub fn apply_display(trees: &mut [Tree], edges: &[Edge], lang: &Lang, rules: &[Rewrite]) {
    for (fi, tree) in trees.iter_mut().enumerate() {
        let ctx = EdgeCtx {
            tree_index: fi as u32,
            edges,
        };
        apply_rewrites_with_edges(tree, lang, rules, true, &ctx);
    }
}
