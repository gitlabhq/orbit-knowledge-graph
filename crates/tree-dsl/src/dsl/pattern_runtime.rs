use indextree::NodeId;

use crate::lang::Lang;
use crate::tree::{Node, Tree};

use super::pattern_types::*;

pub(crate) fn matches(t: &Tree, id: NodeId, p: &Pat, caps: &mut [Cap]) -> bool {
    let n = t.node(id);
    let field_ok = |f: u16| f == 0 || f == n.field;
    match p {
        Pat::Var { .. } | Pat::Spread { .. } => false,
        Pat::Not(_) | Pat::Desc(_) => false,
        Pat::Cap {
            slot,
            field,
            kind,
            guard,
            named_only,
            ..
        } => {
            if !field_ok(*field) || kind.is_some_and(|k| k != n.kind) {
                return false;
            }
            if *named_only && !n.named {
                return false;
            }
            if let Some(g) = guard {
                if !matches(t, id, g, caps) {
                    return false;
                }
            }
            caps[*slot as usize] = Cap::One(id);
            true
        }
        Pat::Node {
            kind,
            field,
            text,
            kids,
            ..
        } => {
            if n.kind != *kind || !field_ok(*field) {
                return false;
            }
            if let Text::Lit(s) = text {
                if n.sym != *s {
                    return false;
                }
            }
            let children: Vec<NodeId> = id.children(&t.arena).collect();
            let mut ci = 0;
            for (k, kid) in kids.iter().enumerate() {
                match kid {
                    Pat::Var { slot, guard, .. } => {
                        let start = ci;
                        while ci < children.len()
                            && !kids
                                .get(k + 1)
                                .is_some_and(|nx| matches(t, children[ci], nx, caps))
                        {
                            ci += 1;
                        }
                        let range: Vec<NodeId> = children[start..ci].to_vec();
                        if let Some(g) = guard {
                            let any_match = range.iter().any(|&e| matches(t, e, g, caps));
                            caps[*slot as usize] = if any_match {
                                Cap::Many(range)
                            } else {
                                Cap::Empty
                            };
                        } else {
                            caps[*slot as usize] = Cap::Many(range);
                        }
                    }
                    Pat::Not(inner) => {
                        for &child in &children[ci..] {
                            if matches(t, child, inner, caps) {
                                return false;
                            }
                        }
                    }
                    Pat::Desc(inner) => {
                        let mut found = false;
                        for desc in id.descendants(&t.arena).skip(1) {
                            if matches(t, desc, inner, caps) {
                                found = true;
                                break;
                            }
                        }
                        if !found {
                            return false;
                        }
                    }
                    _ => {
                        let saved = ci;
                        while ci < children.len() && !matches(t, children[ci], kid, caps) {
                            ci += 1;
                        }
                        if ci >= children.len() {
                            if is_optional(kid) {
                                mark_empty(kid, caps);
                                ci = saved;
                                continue;
                            }
                            return false;
                        }
                        ci += 1;
                    }
                }
            }
            true
        }
    }
}

fn is_optional(p: &Pat) -> bool {
    std::matches!(
        p,
        Pat::Cap { optional: true, .. } | Pat::Node { optional: true, .. }
    )
}

fn mark_empty(p: &Pat, caps: &mut [Cap]) {
    if let Pat::Cap { slot, .. } = p {
        caps[*slot as usize] = Cap::Empty;
    }
}

pub(crate) fn materialize(
    t: &Tree,
    lang: &mut Lang,
    p: &Pat,
    caps: &[Cap],
    filters: &[Vec<u16>],
    parent: NodeId,
    span: (u32, u32),
    out: &mut Tree,
) {
    match p {
        Pat::Cap {
            slot,
            field,
            rekind,
            ..
        } => {
            let Some(src) = caps[*slot as usize].one() else {
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
            let elems = caps[*slot as usize].many();
            let filter = filters
                .get(*slot as usize)
                .map(|f| f.as_slice())
                .unwrap_or(&[]);
            let mut scratch: Vec<Cap> = vec![Cap::Empty; caps.len()];
            for &e in elems {
                let en = t.node(e);
                if !filter.is_empty() && !filter.contains(&en.kind) {
                    continue;
                }
                if *named_only && !en.named {
                    continue;
                }
                if let Some(g) = guard {
                    if !matches(t, e, g, &mut scratch) {
                        continue;
                    }
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
                            sym: en.sym,
                            start: en.start,
                            end: en.end,
                            start_row: en.start_row,
                            start_col: en.start_col,
                            end_row: en.end_row,
                            end_col: en.end_col,
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
                if text_empty || (kids_empty && std::matches!(text, Text::Any)) {
                    return;
                }
            }
            let sym = match text {
                Text::Any => 0,
                Text::Lit(s) => *s,
                Text::From(slot, tf) => {
                    let Some(src) = caps[*slot as usize].one() else {
                        return;
                    };
                    tf.apply_sym(t, lang, src)
                }
            };
            let pos_src = match text {
                Text::From(slot, _) if !caps[*slot as usize].is_empty() => {
                    caps[*slot as usize].one().unwrap()
                }
                _ => caps[0].one().unwrap_or(t.root),
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
                materialize(t, lang, k, caps, filters, at, span, out);
            }
        }
        Pat::Spread { slot, inject } => {
            let Some(src) = caps[*slot as usize].one() else {
                return;
            };
            let copy = out.clone_subtree_from(t, src, Some(parent));
            for kid in inject {
                materialize(t, lang, kid, caps, filters, copy, span, out);
            }
        }
        Pat::Not(_) | Pat::Desc(_) => {}
    }
}

fn import_subtree(from: &Tree, id: NodeId, to: &mut Tree) -> NodeId {
    let children: Vec<NodeId> = id.children(&from.arena).collect();
    let new_id = to.arena.new_node(*from.node(id));
    for child in children {
        let imported = import_subtree(from, child, to);
        new_id.append(imported, &mut to.arena);
    }
    new_id
}

pub fn apply_rewrites(t: &mut Tree, lang: &mut Lang, rules: &[Rewrite]) {
    let max_slots = rules.iter().map(|r| r.nslots).max().unwrap_or(1);
    let mut caps: Vec<Cap> = (0..max_slots).map(|_| Cap::Empty).collect();

    let candidates = t.postorder();

    for target in candidates {
        if target.is_removed(&t.arena) {
            continue;
        }

        for r in rules {
            for c in &mut caps[..r.nslots] {
                *c = Cap::Empty;
            }
            if !matches(t, target, &r.pat, &mut caps) {
                continue;
            }
            caps[0] = Cap::One(target);
            if !r.guards.is_empty() {
                let mut guard_ok = true;
                for &(a, b, eq) in &r.guards {
                    let sym_a = caps[a as usize].one().map_or(0, |id| t.node(id).sym);
                    let sym_b = caps[b as usize].one().map_or(0, |id| t.node(id).sym);
                    if (sym_a == sym_b) != eq {
                        guard_ok = false;
                        break;
                    }
                }
                if !guard_ok {
                    continue;
                }
            }

            let root_node = t.node(target);
            let span = (root_node.start, root_node.end);
            let Out::Replace(tpl) = &r.out;

            let mut staging = Tree::new(Node::default());
            materialize(
                t,
                lang,
                tpl,
                &caps,
                &r.filters,
                staging.root,
                span,
                &mut staging,
            );

            let replacement_roots: Vec<NodeId> = staging.root.children(&staging.arena).collect();
            let mut moved: Vec<NodeId> = Vec::with_capacity(replacement_roots.len());
            for child in replacement_roots {
                let imported = import_subtree(&staging, child, t);
                moved.push(imported);
            }

            t.replace(target, moved);
            break;
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
        if let Some(parent) = parent {
            parent.append(copy, &mut self.arena);
        }
        for child in children {
            self.clone_subtree_from(source, child, Some(copy));
        }
        copy
    }
}
