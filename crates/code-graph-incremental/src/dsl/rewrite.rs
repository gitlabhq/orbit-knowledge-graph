use indextree::NodeId;
use smallvec::{SmallVec, smallvec};

use crate::intern::Lang;
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{Compact, Node, Storage, Tree};

use super::matching::matches;
use super::types::{Cap, EdgeCtx, Out, Pat, Rewrite, Text};

/// How captured subtrees enter a template. The target is about to be removed,
/// so its descendants can be moved in place of copied, except when the built
/// tree may be discarded (`unique:`) or the target survives (`append:`).
pub(crate) struct Placement {
    pub(crate) mv: bool,
    pub(crate) target: NodeId,
    moved: rustc_hash::FxHashSet<NodeId>,
}

impl Placement {
    pub(crate) fn new(mv: bool, target: NodeId) -> Self {
        Self {
            mv,
            target,
            moved: Default::default(),
        }
    }

    /// A capture used twice, or the target itself, is copied on the later use.
    fn place(&mut self, t: &mut Tree, src: NodeId, parent: NodeId) -> NodeId {
        if self.mv && src != self.target && self.moved.insert(src) {
            src.detach(&mut t.storage.0);
            parent.append(src, &mut t.storage.0);
            src
        } else {
            t.clone_within(src, parent)
        }
    }
}

/// Everything a match hands to template construction that stays fixed while
/// the template is built.
pub(crate) struct Fill<'a> {
    pub lang: &'a Lang,
    pub caps: &'a [Cap],
    pub filters: &'a [Vec<u16>],
    pub span: (u32, u32),
    pub edge_ctx: Option<&'a EdgeCtx<'a>>,
}

pub(crate) fn materialize(t: &mut Tree, fill: &Fill, p: &Pat, parent: NodeId, pl: &mut Placement) {
    let Fill {
        lang,
        caps,
        filters,
        span,
        edge_ctx,
    } = *fill;
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
            let copy = pl.place(t, src, parent);
            if *field != 0 {
                t.storage.0[copy].get_mut().field = *field;
            }
            if let Some(k) = rekind {
                let sym = t.sym_of(copy, lang);
                let n = t.storage.0[copy].get_mut();
                n.kind = *k;
                n.field = 0;
                n.sym = sym;
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
            let mut scratch: Vec<Cap> = if guard.is_some() {
                vec![SmallVec::new(); caps.len()]
            } else {
                Vec::new()
            };
            for &e in elems {
                let en = *t.node(e);
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
                    let sym = t.sym_of(e, lang);
                    if sym == 0 {
                        continue;
                    }
                    t.append(
                        parent,
                        Node {
                            kind: rekind.unwrap_or(en.kind),
                            field: 0,
                            named: true,
                            synth: true,
                            sym,
                            ..en
                        },
                    );
                } else {
                    let copy = pl.place(t, e, parent);
                    if let Some(k) = rekind {
                        let sym = t.sym_of(copy, lang);
                        let n = t.storage.0[copy].get_mut();
                        n.kind = *k;
                        n.sym = sym;
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
                Text::From(slot, _) => caps[*slot as usize].first(),
                _ => caps[0].first(),
            }
            .copied()
            .unwrap_or(t.root);
            let src = *t.node(pos_src);
            let at = t.append(
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
                materialize(t, fill, k, at, pl);
            }
        }
        Pat::Spread { slot, inject } => {
            let Some(src) = caps[*slot as usize].first().copied() else {
                return;
            };
            let copy = pl.place(t, src, parent);
            for kid in inject {
                materialize(t, fill, kid, copy, pl);
            }
        }
        Pat::Not(_) | Pat::Desc(_) => {}
    }
}

fn build_template(t: &mut Tree, fill: &Fill, pat: &Pat, mut pl: Placement) -> Vec<NodeId> {
    let holder = t.storage.0.new_node(Node::default());
    materialize(t, fill, pat, holder, &mut pl);
    let built: Vec<NodeId> = holder.children(&t.storage.0).collect();
    for &b in &built {
        b.detach(&mut t.storage.0);
    }
    holder.remove(&mut t.storage.0);
    built
}

pub fn apply_rewrites(
    t: &mut Tree,
    lang: &Lang,
    rules: &[Rewrite],
    sentinels: &[&Sentinel],
) -> Result<(), Killed> {
    apply_rewrites_inner(t, lang, rules, false, None, sentinels, apply_structure)
}

pub fn apply_rewrites_with_edges(
    t: &mut Tree,
    lang: &Lang,
    rules: &[Rewrite],
    preorder: bool,
    edge_ctx: &EdgeCtx,
    sentinels: &[&Sentinel],
) -> Result<(), Killed> {
    apply_rewrites_inner(
        t,
        lang,
        rules,
        preorder,
        Some(edge_ctx),
        sentinels,
        apply_structure,
    )
}

pub fn apply_display(
    t: Tree<Compact>,
    lang: &Lang,
    rules: &[Rewrite],
    edge_ctx: &EdgeCtx,
) -> Tree<Compact> {
    if rules.iter().all(|rule| matches!(rule.out, Out::Tag(..))) {
        let mut tree = t;
        let _ = apply_rewrites_inner(
            &mut tree,
            lang,
            rules,
            true,
            Some(edge_ctx),
            &[],
            |_, _, _, _, _, _| false,
        );
        tree
    } else {
        let mut tree: Tree = t.into();
        let _ = apply_rewrites_with_edges(&mut tree, lang, rules, true, edge_ctx, &[]);
        tree.into()
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_rewrites_inner<S: Storage<Node = Node>>(
    t: &mut Tree<S>,
    lang: &Lang,
    rules: &[Rewrite],
    preorder: bool,
    edge_ctx: Option<&EdgeCtx>,
    sentinels: &[&Sentinel],
    structure: impl Fn(&mut Tree<S>, &Lang, S::Id, &Rewrite, &[Cap<S::Id>], Option<&EdgeCtx>) -> bool,
) -> Result<(), Killed> {
    let max_slots = rules.iter().map(|r| r.nslots).max().unwrap_or(1);
    let mut caps: Vec<Cap<S::Id>> = (0..max_slots).map(|_| SmallVec::new()).collect();

    let mut by_kind = rustc_hash::FxHashMap::<u16, Vec<usize>>::default();
    let mut generic = Vec::new();
    for (index, rule) in rules.iter().enumerate() {
        match rule.pat {
            Pat::Node { kind, .. } if kind != 0 => by_kind.entry(kind).or_default().push(index),
            _ => generic.push(index),
        }
    }
    for candidates in by_kind.values_mut() {
        candidates.extend(&generic);
        candidates.sort_unstable();
    }

    let mut candidates: Vec<_> = std::iter::once(t.root())
        .chain(t.root().descendants())
        .map(|c| t.storage.id(c.index()))
        .collect();
    if !preorder {
        candidates.reverse();
    }

    for target in candidates {
        sentinels.iter().try_for_each(|s| s.check())?;
        if t.storage.is_removed(target) {
            continue;
        }

        let target_kind = t.storage.node(S::index(target)).kind;
        for &index in by_kind.get(&target_kind).unwrap_or(&generic) {
            let r = &rules[index];
            for c in &mut caps[..r.nslots] {
                c.clear();
            }
            if !matches(t, lang, target, &r.pat, &mut caps) {
                continue;
            }
            caps[0] = smallvec![target];
            let guards_ok = r.guards.iter().all(|&(a, b, eq)| {
                let sym_a = caps[a as usize]
                    .first()
                    .copied()
                    .map_or(0, |id| t.sym_at(S::index(id), lang));
                let sym_b = caps[b as usize]
                    .first()
                    .copied()
                    .map_or(0, |id| t.sym_at(S::index(id), lang));
                (sym_a == sym_b) == eq
            });
            if !guards_ok {
                continue;
            }

            match &r.out {
                Out::Tag(entries, tag_on) => {
                    let raw = S::index(tag_target(t, target, *tag_on));
                    for entry in entries {
                        let src = caps[entry.slot as usize].first().copied().unwrap_or(target);
                        let val = if entry.val.is_node_tf() {
                            entry.val.apply_sym(t, lang, src, edge_ctx)
                        } else {
                            let base_sym = t.sym_at(S::index(src), lang);
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
                _ => {
                    if structure(t, lang, target, r, &caps, edge_ctx) {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

fn apply_structure(
    t: &mut Tree,
    lang: &Lang,
    target: NodeId,
    r: &Rewrite,
    caps: &[Cap],
    edge_ctx: Option<&EdgeCtx>,
) -> bool {
    let root_node = t.node(target);
    let fill = Fill {
        lang,
        caps,
        filters: &r.filters,
        span: (root_node.start, root_node.end),
        edge_ctx,
    };
    match &r.out {
        Out::Replace(p, tag_entries, tag_on) => {
            let pl = Placement::new(r.unique.is_none(), target);
            let built = build_template(t, &fill, p, pl);
            let first = built.first().copied();
            if let (Some((pat, kind, nslots)), Some(new_root)) = (&r.unique, first) {
                let key = t.cursor(Tree::to_raw(new_root)).child_sym_of_kind(*kind);
                let mut ucaps: Vec<Cap> = (0..*nslots).map(|_| SmallVec::new()).collect();
                let taken = target.parent(&t.storage.0).is_some_and(|parent| {
                    parent
                        .children(&t.storage.0)
                        .filter(|&c| c != target)
                        .any(|c| {
                            matches(t, lang, c, pat, &mut ucaps)
                                && t.cursor(Tree::to_raw(c)).child_sym_of_kind(*kind) == key
                        })
                });
                if taken {
                    built
                        .into_iter()
                        .for_each(|n| n.remove_subtree(&mut t.storage.0));
                    return true;
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
                        let base_sym = t.sym_of(src, lang);
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
                let tagged = tag_target(t, new_root, *tag_on);
                for (key, val) in tags {
                    t.set_tag(Tree::to_raw(tagged), key, val);
                }
            }
            return true;
        }
        Out::Append(ps) => {
            for pat in ps {
                let pl = Placement::new(false, target);
                let built = build_template(t, &fill, pat, pl);
                for id in built {
                    target.append(id, &mut t.storage.0);
                }
            }
        }
        Out::Tag(..) => {}
    }
    false
}

impl Tree {
    pub(crate) fn clone_within(&mut self, id: NodeId, parent: NodeId) -> NodeId {
        let children: Vec<NodeId> = id.children(&self.storage.0).collect();
        let copy = self.storage.0.new_node(*self.node(id));
        if let Some(tags) = self.tags.get(&Tree::to_raw(id)) {
            let tags = tags.clone();
            self.tags.insert(Tree::to_raw(copy), tags);
        }
        parent.append(copy, &mut self.storage.0);
        for child in children {
            self.clone_within(child, copy);
        }
        copy
    }
}

/// The node `tag_on:` selects: the first descendant of that kind, else `root`.
fn tag_target<S: Storage<Node = Node>>(t: &Tree<S>, root: S::Id, tag_on: Option<u16>) -> S::Id {
    tag_on
        .and_then(|k| {
            std::iter::once(t.cursor(S::index(root)))
                .chain(t.cursor(S::index(root)).descendants())
                .find(|n| n.kind() == k)
        })
        .map(|n| t.storage.id(n.index()))
        .unwrap_or(root)
}
