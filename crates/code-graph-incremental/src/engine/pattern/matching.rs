use super::{Cap, KindFilter, Pat, Text};
use crate::intern::Lang;
use crate::tree::{Node, Storage, Tree};

/// `$ROOT` is pinned to `anchor`, or to `id` without one, and bound to it on a match.
pub fn matches<S: Storage<Node = Node>>(
    t: &Tree<S>,
    lang: &Lang,
    id: S::Id,
    anchor: Option<S::Id>,
    p: &Pat,
    caps: &mut [Cap<S::Id>],
) -> bool {
    caps.iter_mut().for_each(Cap::clear);
    let root = anchor.unwrap_or(id);
    let m = Matcher {
        t,
        lang,
        root: S::index(root),
    };
    if !m.at(S::index(id), p, caps) {
        return false;
    }
    caps[0].clear();
    caps[0].push(root);
    true
}

pub fn anchor(p: &Pat) -> Option<(u8, u16)> {
    let (depth, kinds) = anchor_kinds(p)?;
    Some((depth, kinds.single()?))
}

pub fn anchor_kinds(p: &Pat) -> Option<(u8, &KindFilter)> {
    fn root(p: &Pat, depth: u8) -> Option<(u8, &KindFilter)> {
        match p {
            Pat::Cap {
                slot: 0,
                test,
                guard,
                ..
            } => Some((depth, own(guard.as_deref()).unwrap_or(&test.kinds))),
            Pat::Cap { guard: Some(g), .. } => root(g, depth),
            Pat::Node { kids, .. } => kids.iter().find_map(|kid| root(kid, depth + 1)),
            _ => None,
        }
    }
    fn own(p: Option<&Pat>) -> Option<&KindFilter> {
        match p? {
            Pat::Node { kind, .. } => Some(kind),
            Pat::Cap { test, guard, .. } => own(guard.as_deref()).or(Some(&test.kinds)),
            _ => None,
        }
    }
    root(p, 0).or_else(|| Some((0, own(Some(p))?)))
}

struct Matcher<'a, S: Storage> {
    t: &'a Tree<S>,
    lang: &'a Lang,
    root: u32,
}

impl<S: Storage<Node = Node>> Matcher<'_, S> {
    fn at(&self, raw: u32, p: &Pat, caps: &mut [Cap<S::Id>]) -> bool {
        let n = self.t.storage.node(raw);
        match p {
            Pat::Node {
                kind,
                field,
                text,
                tags,
                flags,
                unflags,
                kids,
                ..
            } => {
                let set = [n.lifted, n.labelled, n.named, n.synth];
                let flagged = (0..4).all(|bit| {
                    (*flags & (1 << bit) == 0 || set[bit])
                        && (*unflags & (1 << bit) == 0 || !set[bit])
                });
                flagged
                    && kind.admits(n.kind)
                    && (*field == 0 || *field == n.field)
                    && self.text(raw, n, text)
                    && self.tagged(raw, tags)
                    && self.kids(raw, kids, caps)
            }
            Pat::Cap {
                slot,
                field,
                test,
                guard,
                ..
            } => {
                if (*slot == 0 && raw != self.root)
                    || (*field != 0 && *field != n.field)
                    || !test.admits(n)
                    || guard.as_ref().is_some_and(|g| !self.at(raw, g, caps))
                {
                    return false;
                }
                let cap = &mut caps[*slot as usize];
                cap.clear();
                cap.push(self.t.storage.id(raw));
                true
            }
            Pat::Var { .. } | Pat::Not(_) | Pat::Desc(..) => false,
        }
    }

    fn tagged(&self, raw: u32, tags: &[(u32, bool)]) -> bool {
        if tags.is_empty() {
            return true;
        }
        let own = self.t.tags.get(&raw);
        tags.iter().all(|&(key, present)| {
            own.is_some_and(|own| own.iter().any(|t| t.key == key)) == present
        })
    }

    fn text(&self, raw: u32, n: &Node, text: &Text) -> bool {
        let syms = &self.lang.syms;
        match text {
            Text::Any => true,
            Text::Syms(set) if n.sym != 0 => set.contains(&n.sym),
            Text::Syms(set) => {
                let text = self.t.text_at(raw, self.lang);
                set.iter().any(|&s| syms.resolve(s) == text)
            }
            Text::Prefix(prefix) => self.t.text_at(raw, self.lang).starts_with(&**prefix),
            Text::Regex(re) => re.is_match(self.t.text_at(raw, self.lang)),
        }
    }

    fn kids(&self, raw: u32, kids: &[Pat], caps: &mut [Cap<S::Id>]) -> bool {
        let storage = &self.t.storage;
        let mut next = storage.first_child(raw);
        for (k, kid) in kids.iter().enumerate() {
            match kid {
                Pat::Var { slot, field, test } => {
                    let stop = kids[k + 1..]
                        .iter()
                        .find(|p| matches!(p, Pat::Node { .. } | Pat::Cap { .. }));
                    caps[*slot as usize].clear();
                    while let Some(child) = next {
                        if stop.is_some_and(|s| self.at(child, s, caps)) {
                            break;
                        }
                        let n = storage.node(child);
                        if (*field == 0 || *field == n.field) && test.admits(n) {
                            caps[*slot as usize].push(storage.id(child));
                        }
                        next = storage.next_sibling(child);
                    }
                }
                Pat::Not(inner) => {
                    let mut child = storage.first_child(raw);
                    while let Some(c) = child {
                        if self.at(c, inner, caps) {
                            return false;
                        }
                        child = storage.next_sibling(c);
                    }
                    clear(inner, caps);
                }
                Pat::Desc(inner, stop) => {
                    let mut walk = self
                        .t
                        .cursor(raw)
                        .descendants_pruned(|d| stop.admits(d.kind()));
                    if !walk.any(|d| self.at(d.index(), inner, caps)) {
                        return false;
                    }
                }
                Pat::Node { optional, .. } | Pat::Cap { optional, .. } => {
                    let from = next;
                    let mut found = false;
                    while let Some(child) = next {
                        next = storage.next_sibling(child);
                        if self.at(child, kid, caps) {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        if !optional {
                            return false;
                        }
                        clear(kid, caps);
                        next = from;
                    }
                }
            }
        }
        true
    }
}

fn clear<I>(p: &Pat, caps: &mut [Cap<I>]) {
    match p {
        Pat::Cap { slot, guard, .. } => {
            caps[*slot as usize].clear();
            if let Some(g) = guard {
                clear(g, caps);
            }
        }
        Pat::Var { slot, .. } => caps[*slot as usize].clear(),
        Pat::Node { kids, .. } => kids.iter().for_each(|kid| clear(kid, caps)),
        Pat::Not(inner) | Pat::Desc(inner, _) => clear(inner, caps),
    }
}
