use rustc_hash::FxHashSet;

use crate::canonical::Canonical as C;
use crate::intern::Lang;
use crate::tree::{Edge, EdgeKind, Node, Tree};

pub fn pre_display(trees: &mut [Tree], cross_edges: &[Edge], lang: &Lang) {
    let resolved: FxHashSet<(usize, u32)> = cross_edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Imports)
        .map(|e| (e.from.tree as usize, e.from.node))
        .collect();

    let true_sym = lang.syms.intern("true");
    let false_sym = lang.syms.intern("false");

    for (fi, tree) in trees.iter_mut().enumerate() {
        let indices: Vec<(u32, bool, bool)> = tree
            .root()
            .descendants()
            .filter(|n| n.is(C::Import) || n.is(C::ImportType))
            .map(|n| {
                (
                    n.index(),
                    resolved.contains(&(fi, n.index())),
                    n.is(C::ImportType),
                )
            })
            .collect();

        for (idx, has_target, type_only) in indices {
            let parent = tree.to_id(idx);
            tree.append(
                parent,
                Node {
                    kind: C::DisplayHasTarget.into(),
                    named: true,
                    synth: true,
                    sym: if has_target { true_sym } else { false_sym },
                    ..Default::default()
                },
            );
            tree.append(
                parent,
                Node {
                    kind: C::DisplayIsTypeOnly.into(),
                    named: true,
                    synth: true,
                    sym: if type_only { true_sym } else { false_sym },
                    ..Default::default()
                },
            );
        }
    }
}
