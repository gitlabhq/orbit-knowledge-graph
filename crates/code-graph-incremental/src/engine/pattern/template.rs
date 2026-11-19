use indextree::NodeId;

use super::{Cap, Tpl, TplText};
use crate::intern::Lang;
use crate::tree::{Mutable, Node, Tree};

pub fn build<T>(
    tree: &mut Tree<Mutable>,
    lang: &Lang,
    parent: NodeId,
    tpl: &Tpl<T>,
    caps: &[Cap<NodeId>],
    mv: Option<NodeId>,
    text: &mut dyn FnMut(usize, &T) -> u32,
) -> Vec<NodeId> {
    let span = caps
        .first()
        .and_then(|root| root.first())
        .map_or_else(Node::default, |&root| *tree.node(root));
    let movable = mv.map_or_else(Vec::new, |target| movable(tree, tpl, caps, target));
    let last = tree.storage.0[parent].last_child();
    let mut b = Builder {
        tree,
        lang,
        caps,
        text,
        span,
        movable,
    };
    b.tpl(tpl, parent);
    let arena = &b.tree.storage.0;
    let first = match last {
        Some(last) => arena[last].next_sibling(),
        None => arena[parent].first_child(),
    };
    std::iter::successors(first, |&id| arena[id].next_sibling()).collect()
}

struct Builder<'a, T> {
    tree: &'a mut Tree,
    lang: &'a Lang,
    caps: &'a [Cap<NodeId>],
    text: &'a mut dyn FnMut(usize, &T) -> u32,
    span: Node,
    movable: Vec<NodeId>,
}

impl<T> Builder<'_, T> {
    fn tpl(&mut self, tpl: &Tpl<T>, parent: NodeId) {
        let caps = self.caps;
        match tpl {
            Tpl::Cap {
                slot,
                field,
                rekind,
            } => {
                let Some(&src) = caps[*slot as usize].first() else {
                    return;
                };
                let at = self.place(src, parent);
                if *field != 0 {
                    self.tree.node_mut(at).field = *field;
                }
                if let Some(kind) = rekind {
                    self.rekind(at, *kind);
                    self.tree.node_mut(at).field = 0;
                }
            }
            Tpl::Var {
                slot,
                test,
                rekind,
                leaf,
            } => {
                for &src in &caps[*slot as usize] {
                    let n = *self.tree.node(src);
                    if !test.admits(&n) {
                        continue;
                    }
                    if *leaf {
                        let sym = self.tree.sym_of(src, self.lang);
                        if sym != 0 {
                            self.tree.append(
                                parent,
                                Node {
                                    kind: rekind.unwrap_or(n.kind),
                                    field: 0,
                                    named: true,
                                    synth: true,
                                    sym,
                                    ..n
                                },
                            );
                        }
                        continue;
                    }
                    let at = self.place(src, parent);
                    if let Some(kind) = rekind {
                        self.rekind(at, *kind);
                    }
                }
            }
            Tpl::Spread { slot, kids } => {
                let Some(&src) = caps[*slot as usize].first() else {
                    return;
                };
                let at = self.place(src, parent);
                for kid in kids {
                    self.tpl(kid, at);
                }
            }
            Tpl::Node {
                kind,
                field,
                text,
                kids,
                optional,
                labelled,
            } => {
                let empty = |slot: &u16| caps[*slot as usize].is_empty();
                if *optional {
                    let kids_empty = kids.iter().all(|kid| match kid {
                        Tpl::Cap { slot, .. } | Tpl::Var { slot, .. } => empty(slot),
                        _ => false,
                    });
                    match text {
                        TplText::Expr { slot, .. } if empty(slot) => return,
                        TplText::None if kids_empty => return,
                        _ => {}
                    }
                }
                let sym = match text {
                    TplText::None => 0,
                    TplText::Lit(sym) => *sym,
                    TplText::Expr { slot, .. } if empty(slot) => return,
                    TplText::Expr { index, expr, .. } => (self.text)(*index, expr),
                };
                let s = self.span;
                let at = self.tree.append(
                    parent,
                    Node {
                        kind: *kind,
                        field: *field,
                        named: true,
                        synth: true,
                        sym,
                        lifted: false,
                        labelled: *labelled,
                        ..s
                    },
                );
                for kid in kids {
                    self.tpl(kid, at);
                }
            }
        }
    }

    fn rekind(&mut self, id: NodeId, kind: u16) {
        let sym = self.tree.sym_of(id, self.lang);
        let n = self.tree.node_mut(id);
        n.kind = kind;
        n.sym = sym;
    }

    fn place(&mut self, src: NodeId, parent: NodeId) -> NodeId {
        if self.movable.binary_search(&src).is_err() {
            return copy(self.tree, src, parent);
        }
        let arena = &mut self.tree.storage.0;
        src.detach(arena);
        parent.append(src, arena);
        src
    }
}

/// Moving a capture that holds, or sits inside, another used capture would tear that one apart.
fn movable<T>(tree: &Tree, tpl: &Tpl<T>, caps: &[Cap<NodeId>], target: NodeId) -> Vec<NodeId> {
    let mut used = Vec::new();
    uses(tree, tpl, caps, &mut used);
    used.sort_unstable();
    let mut blocked = vec![false; used.len()];
    for i in 1..used.len() {
        if used[i] == used[i - 1] {
            blocked[i] = true;
            blocked[i - 1] = true;
        }
    }
    let arena = &tree.storage.0;
    for i in 0..used.len() {
        let mut inside = false;
        let mut up = arena[used[i]].parent();
        while let Some(a) = up {
            if let Ok(j) = used.binary_search(&a) {
                blocked[i] = true;
                blocked[j] = true;
            }
            if a == target {
                inside = true;
                break;
            }
            up = arena[a].parent();
        }
        blocked[i] |= !inside;
    }
    used.into_iter()
        .zip(blocked)
        .filter_map(|(id, blocked)| (!blocked).then_some(id))
        .collect()
}

fn uses<T>(tree: &Tree, tpl: &Tpl<T>, caps: &[Cap<NodeId>], out: &mut Vec<NodeId>) {
    match tpl {
        Tpl::Cap { slot, .. } => out.extend(caps[*slot as usize].first()),
        Tpl::Var {
            slot,
            test,
            leaf: false,
            ..
        } => out.extend(
            caps[*slot as usize]
                .iter()
                .filter(|&&id| test.admits(tree.node(id))),
        ),
        Tpl::Var { .. } => {}
        Tpl::Spread { slot, kids } => {
            out.extend(caps[*slot as usize].first());
            kids.iter().for_each(|kid| uses(tree, kid, caps, out));
        }
        Tpl::Node { kids, .. } => kids.iter().for_each(|kid| uses(tree, kid, caps, out)),
    }
}

fn copy(tree: &mut Tree, src: NodeId, parent: NodeId) -> NodeId {
    let root = tree.append(parent, *tree.node(src));
    copy_tags(tree, src, root);
    let mut pending = vec![(src, root)];
    while let Some((from, to)) = pending.pop() {
        let mut child = tree.storage.0[from].first_child();
        while let Some(c) = child {
            child = tree.storage.0[c].next_sibling();
            if c == root {
                continue;
            }
            let copy = tree.append(to, *tree.node(c));
            copy_tags(tree, c, copy);
            pending.push((c, copy));
        }
    }
    root
}

fn copy_tags(tree: &mut Tree, from: NodeId, to: NodeId) {
    if let Some(tags) = tree.tags.get(&Tree::to_raw(from)) {
        let tags = tags.clone();
        tree.tags.insert(Tree::to_raw(to), tags);
    }
}
