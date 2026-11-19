use smallvec::{SmallVec, smallvec};

use crate::intern::Lang;
use crate::tree::{Node, Storage, Tree};

use crate::dsl::types::{Cap, Pat, Text};

pub(crate) fn matches<S: Storage<Node = Node>>(
    t: &Tree<S>,
    lang: &Lang,
    id: S::Id,
    p: &Pat,
    caps: &mut [Cap<S::Id>],
) -> bool {
    let raw = S::index(id);
    let n = t.storage.node(raw);
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
            if let Some(g) = guard
                && !matches(t, lang, id, g, caps)
            {
                return false;
            }
            caps[*slot as usize] = smallvec![id];
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
            match text {
                Text::Lit(s)
                    if n.sym != *s
                        && (n.sym != 0 || t.text_at(raw, lang) != lang.syms.resolve(*s)) =>
                {
                    return false;
                }
                Text::Prefix(p) if !t.text_at(raw, lang).starts_with(lang.syms.resolve(*p)) => {
                    return false;
                }
                Text::Regex(re) if !re.is_match(t.text_at(raw, lang)) => return false,
                _ => {}
            }
            let children = || {
                std::iter::successors(t.storage.first_child(raw), |&c| t.storage.next_sibling(c))
                    .map(|c| t.storage.id(c))
            };
            let mut rest = children().peekable();
            for (k, kid) in kids.iter().enumerate() {
                match kid {
                    Pat::Var { slot, guard, .. } => {
                        let mut range = SmallVec::new();
                        while let Some(&child) = rest.peek() {
                            if kids
                                .get(k + 1)
                                .is_some_and(|nx| matches(t, lang, child, nx, caps))
                            {
                                break;
                            }
                            range.push(child);
                            rest.next();
                        }
                        let keep = guard
                            .as_ref()
                            .is_none_or(|g| range.iter().any(|&e| matches(t, lang, e, g, caps)));
                        caps[*slot as usize] = if keep { range } else { SmallVec::new() };
                    }
                    Pat::Not(inner) => {
                        if children().any(|child| matches(t, lang, child, inner, caps)) {
                            return false;
                        }
                    }
                    Pat::Desc(inner) => {
                        let mut descendants = t.cursor(raw).descendants();
                        if !descendants
                            .any(|desc| matches(t, lang, t.storage.id(desc.index()), inner, caps))
                        {
                            return false;
                        }
                    }
                    _ => {
                        let saved = rest.clone();
                        if !rest.any(|child| matches(t, lang, child, kid, caps)) {
                            if is_optional(kid) {
                                mark_empty(kid, caps);
                                rest = saved;
                                continue;
                            }
                            return false;
                        }
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

fn mark_empty<I>(p: &Pat, caps: &mut [Cap<I>]) {
    if let Pat::Cap { slot, .. } = p {
        caps[*slot as usize] = SmallVec::new();
    }
}
