use indextree::NodeId;
use smallvec::{SmallVec, smallvec};

use crate::intern::Lang;
use crate::tree::{Node, Tree};

use super::matching::matches;
use super::types::{Cap, EdgeCtx, Out, Pat, Rewrite, Text};

#[allow(clippy::too_many_arguments)]
pub(crate) fn materialize(
    t: &Tree,
    lang: &Lang,
    p: &Pat,
    caps: &[Cap],
    filters: &[Vec<u16>],
    parent: NodeId,
    span: (u32, u32),
    out: &mut Tree,
    edge_ctx: Option<&EdgeCtx>,
) {
    match p {
        Pat::Cap {
            slot,
            field,
            rekind,
            ..
        } => {
            let Some(src) = caps[*slot as usize].first().copied() else {
                return;
            };
            let copy = out.clone_subtree_from(t, src, Some(parent));
            if *field != 0 {
                out.arena[copy].get_mut().field = *field;
            }
            if let Some(k) = rekind {
                out.arena[copy].get_mut().kind = *k;
                out.arena[copy].get_mut().field = 0;
            }
        }
        Pat::Var {
            slot,
            rekind,
            leaf_only,
            guard,
            named_only,
            ..
        } => {
            if caps[*slot as usize].is_empty() {
                return;
            }
            let elems = caps[*slot as usize].as_slice();
            let filter = filters
                .get(*slot as usize)
                .map(|f| f.as_slice())
                .unwrap_or(&[]);
            let mut scratch: Vec<Cap> = vec![SmallVec::new(); caps.len()];
            for &e in elems {
                let en = t.node(e);
                if !filter.is_empty() && !filter.contains(&en.kind) {
                    continue;
                }
                if *named_only && !en.named {
                    continue;
                }
                if let Some(g) = guard
                    && !matches(t, lang, e, g, &mut scratch)
                {
                    continue;
                }
                if *leaf_only {
                    if en.sym == 0 {
                        continue;
                    }
                    out.append(
                        parent,
                        Node {
                            kind: rekind.unwrap_or(en.kind),
                            field: 0,
                            named: true,
                            synth: true,
                            ..*en
                        },
                    );
                } else {
                    let copy = out.clone_subtree_from(t, e, Some(parent));
                    if let Some(k) = rekind {
                        out.arena[copy].get_mut().kind = *k;
                    }
                }
            }
        }
        Pat::Node {
            kind,
            field,
            text,
            kids,
            optional,
        } => {
            if *optional {
                let text_empty =
                    std::matches!(text, Text::From(slot, _) if caps[*slot as usize].is_empty());
                let kids_empty = kids.is_empty()
                    || kids.iter().all(|k| match k {
                        Pat::Cap { slot, .. } | Pat::Var { slot, .. } => {
                            caps[*slot as usize].is_empty()
                        }
                        _ => false,
                    });
                if text_empty
                    || (kids_empty
                        && std::matches!(text, Text::Any | Text::Prefix(_) | Text::Regex(_)))
                {
                    return;
                }
            }
            let sym = match text {
                Text::Any | Text::Prefix(_) | Text::Regex(_) => 0,
                Text::Lit(s) => *s,
                Text::From(slot, tf) => {
                    let Some(src) = caps[*slot as usize].first().copied() else {
                        return;
                    };
                    tf.apply_sym(t, lang, src, edge_ctx)
                }
            };
            let pos_src = match text {
                Text::From(slot, _) if !caps[*slot as usize].is_empty() => {
                    caps[*slot as usize].first().copied().unwrap()
                }
                _ => caps[0].first().copied().unwrap_or(t.root),
            };
            let src = t.node(pos_src);
            let at = out.append(
                parent,
                Node {
                    kind: *kind,
                    field: *field,
                    named: true,
                    synth: true,
                    sym,
                    start: span.0,
                    end: span.1,
                    start_row: src.start_row,
                    start_col: src.start_col,
                    end_row: src.end_row,
                    end_col: src.end_col,
                },
            );
            for k in kids {
                materialize(t, lang, k, caps, filters, at, span, out, edge_ctx);
            }
        }
        Pat::Spread { slot, inject } => {
            let Some(src) = caps[*slot as usize].first().copied() else {
                return;
            };
            let copy = out.clone_subtree_from(t, src, Some(parent));
            for kid in inject {
                materialize(t, lang, kid, caps, filters, copy, span, out, edge_ctx);
            }
        }
        Pat::Not(_) | Pat::Desc(_) => {}
    }
}

fn build_template(
    t: &mut Tree,
    lang: &Lang,
    pat: &Pat,
    caps: &[Cap],
    filters: &[Vec<u16>],
    span: (u32, u32),
    edge_ctx: Option<&EdgeCtx>,
) -> Vec<NodeId> {
    let mut staging = Tree::new(Node::default());
    materialize(
        t,
        lang,
        pat,
        caps,
        filters,
        staging.root,
        span,
        &mut staging,
        edge_ctx,
    );
    staging
        .root
        .children(&staging.arena)
        .map(|c| t.clone_subtree_from(&staging, c, None))
        .collect()
}

pub fn apply_rewrites(t: &mut Tree, lang: &Lang, rules: &[Rewrite]) {
    apply_rewrites_inner(t, lang, rules, false, None);
}

pub fn apply_rewrites_preorder(t: &mut Tree, lang: &Lang, rules: &[Rewrite]) {
    apply_rewrites_inner(t, lang, rules, true, None);
}

pub fn apply_rewrites_with_edges(
    t: &mut Tree,
    lang: &Lang,
    rules: &[Rewrite],
    preorder: bool,
    edge_ctx: &EdgeCtx,
) {
    apply_rewrites_inner(t, lang, rules, preorder, Some(edge_ctx));
}

fn apply_rewrites_inner(
    t: &mut Tree,
    lang: &Lang,
    rules: &[Rewrite],
    preorder: bool,
    edge_ctx: Option<&EdgeCtx>,
) {
    let max_slots = rules.iter().map(|r| r.nslots).max().unwrap_or(1);
    let mut caps: Vec<Cap> = (0..max_slots).map(|_| SmallVec::new()).collect();

    let root_kinds: Vec<u16> = rules
        .iter()
        .map(|r| match &r.pat {
            Pat::Node { kind, .. } => *kind,
            _ => 0,
        })
        .collect();

    let candidates = if preorder {
        t.preorder()
    } else {
        t.postorder()
    };

    for target in candidates {
        if target.is_removed(&t.arena) {
            continue;
        }

        let target_kind = t.node(target).kind;
        for (ri, r) in rules.iter().enumerate() {
            if root_kinds[ri] != 0 && root_kinds[ri] != target_kind {
                continue;
            }
            for c in &mut caps[..r.nslots] {
                *c = SmallVec::new();
            }
            if !matches(t, lang, target, &r.pat, &mut caps) {
                continue;
            }
            caps[0] = smallvec![target];
            let guards_ok = r.guards.iter().all(|&(a, b, eq)| {
                let sym_a = caps[a as usize]
                    .first()
                    .copied()
                    .map_or(0, |id| t.node(id).sym);
                let sym_b = caps[b as usize]
                    .first()
                    .copied()
                    .map_or(0, |id| t.node(id).sym);
                (sym_a == sym_b) == eq
            });
            if !guards_ok {
                continue;
            }

            let root_node = t.node(target);
            let span = (root_node.start, root_node.end);

            match &r.out {
                Out::Tag(entries) => {
                    let raw = Tree::to_raw(target);
                    for entry in entries {
                        let src = caps[entry.slot as usize].first().copied().unwrap_or(target);
                        let val = if entry.val.is_node_tf() {
                            entry.val.apply_sym(t, lang, src, edge_ctx)
                        } else {
                            let base_sym = t.node(src).sym;
                            if base_sym == 0 {
                                entry.val.apply_sym(t, lang, src, edge_ctx)
                            } else {
                                let s = lang.syms.resolve(base_sym);
                                lang.syms.intern(&entry.val.apply_to_str(s))
                            }
                        };
                        t.set_tag(raw, entry.key, val);
                    }
                }
                Out::Replace(p, tag_entries, tag_on) => {
                    let built = build_template(t, lang, p, &caps, &r.filters, span, edge_ctx);
                    let first = built.first().copied();
                    if let (Some((pat, kind, nslots)), Some(new_root)) = (&r.unique, first) {
                        let key = t.cursor(Tree::to_raw(new_root)).child_sym_of_kind(*kind);
                        let mut ucaps: Vec<Cap> = (0..*nslots).map(|_| SmallVec::new()).collect();
                        let taken = target.parent(&t.arena).is_some_and(|parent| {
                            parent.children(&t.arena).filter(|&c| c != target).any(|c| {
                                matches(t, lang, c, pat, &mut ucaps)
                                    && t.cursor(Tree::to_raw(c)).child_sym_of_kind(*kind) == key
                            })
                        });
                        if taken {
                            built
                                .into_iter()
                                .for_each(|n| n.remove_subtree(&mut t.arena));
                            break;
                        }
                    }
                    let tags: Vec<(u32, u32)> = tag_entries
                        .iter()
                        .flatten()
                        .map(|entry| {
                            let src = caps[entry.slot as usize].first().copied().unwrap_or(target);
                            let val = if entry.val.is_node_tf() {
                                entry.val.apply_sym(t, lang, src, edge_ctx)
                            } else {
                                let base_sym = t.node(src).sym;
                                if base_sym == 0 {
                                    entry.val.apply_sym(t, lang, src, edge_ctx)
                                } else {
                                    let s = lang.syms.resolve(base_sym);
                                    lang.syms.intern(&entry.val.apply_to_str(s))
                                }
                            };
                            (entry.key, val)
                        })
                        .collect();
                    t.replace(target, built);
                    if let Some(new_root) = first {
                        let tagged = tag_on
                            .and_then(|k| {
                                new_root
                                    .descendants(&t.arena)
                                    .find(|&n| t.node(n).kind == k)
                            })
                            .unwrap_or(new_root);
                        for (key, val) in tags {
                            t.set_tag(Tree::to_raw(tagged), key, val);
                        }
                    }
                    break;
                }
                Out::Append(ps) => {
                    for pat in ps {
                        let built = build_template(t, lang, pat, &caps, &r.filters, span, edge_ctx);
                        for id in built {
                            target.append(id, &mut t.arena);
                        }
                    }
                }
            }
        }
    }
}

impl Tree {
    pub(crate) fn clone_subtree_from(
        &mut self,
        source: &Tree,
        id: NodeId,
        parent: Option<NodeId>,
    ) -> NodeId {
        let children: Vec<NodeId> = id.children(&source.arena).collect();
        let copy = self.arena.new_node(*source.node(id));
        if let Some(tags) = source.tags.get(&Tree::to_raw(id)) {
            self.tags.insert(Tree::to_raw(copy), tags.clone());
        }
        if let Some(parent) = parent {
            parent.append(copy, &mut self.arena);
        }
        for child in children {
            self.clone_subtree_from(source, child, Some(copy));
        }
        copy
    }
}
