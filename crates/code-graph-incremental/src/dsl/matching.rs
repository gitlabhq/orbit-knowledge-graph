use smallvec::{SmallVec, smallvec};

use crate::intern::Lang;
use crate::tree::{Node, Storage, Tree};

use super::types::{Cap, Pat, Text};

pub(crate) fn matches<S: Storage<Node>>(
    t: &Tree<S>,
    lang: &Lang,
    id: S::Id,
    p: &Pat,
    caps: &mut [Cap<S::Id>],
) -> bool {
    let raw = S::index(id);
    let n = S::node(&t.arena, raw);
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
            let children: Vec<_> = t
                .cursor(raw)
                .children()
                .map(|c| S::id(&t.arena, c.index()))
                .collect();
            let mut ci = 0;
            for (k, kid) in kids.iter().enumerate() {
                match kid {
                    Pat::Var { slot, guard, .. } => {
                        let start = ci;
                        while ci < children.len()
                            && !kids
                                .get(k + 1)
                                .is_some_and(|nx| matches(t, lang, children[ci], nx, caps))
                        {
                            ci += 1;
                        }
                        let range = &children[start..ci];
                        let keep = guard
                            .as_ref()
                            .is_none_or(|g| range.iter().any(|&e| matches(t, lang, e, g, caps)));
                        caps[*slot as usize] = if keep {
                            SmallVec::from_slice(range)
                        } else {
                            SmallVec::new()
                        };
                    }
                    Pat::Not(inner) => {
                        for &child in &children {
                            if matches(t, lang, child, inner, caps) {
                                return false;
                            }
                        }
                    }
                    Pat::Desc(inner) => {
                        let mut found = false;
                        for desc in t.cursor(raw).descendants() {
                            if matches(t, lang, S::id(&t.arena, desc.index()), inner, caps) {
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
                        while ci < children.len() && !matches(t, lang, children[ci], kid, caps) {
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

fn mark_empty<I>(p: &Pat, caps: &mut [Cap<I>]) {
    if let Pat::Cap { slot, .. } = p {
        caps[*slot as usize] = SmallVec::new();
    }
}
