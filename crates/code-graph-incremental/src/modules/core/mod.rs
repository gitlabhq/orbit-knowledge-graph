//! The rules' common vocabulary: set operations, tree navigation, the graph
//! and declared indexes, and the writes a rule makes. Literal arguments
//! (edge kinds, index, row shape and template ids) arrive as numbers.

use crate::engine::{
    Cx, Distinct, Function, Loc, Module, SETS, Store, Val, Vals, WHOLE, Write, one,
};
use crate::modules::function;
use crate::tree::{Edge, EdgeKind};

pub struct Core;

#[derive(Default)]
pub struct State;

impl Module for Core {
    const NAME: &'static str = "core";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }
}

static FUNCTIONS: &[Function<Core>] = &[
    function!(Inline, "minus", [set, set], |_, a| {
        let drop: Distinct = a[1].iter().copied().collect();
        set(a[0].iter().copied().filter(|v| !drop.contains(v)))
    }),
    function!(Inline, "unique", [set], |_, a| match a[0].as_slice() {
        [v] => one(*v),
        _ => Vals::new(),
    }),
    function!(Inline, "instances", [set], |_, a| {
        set(a[0].iter().copied().filter(|v| matches!(v, Val::Type(_))))
    }),
    function!(Inline, "nodes", [set], |_, a| set(
        nodes(&a[0]).map(Val::Node)
    )),
    function!(Inline, "last", [set], |_, a| {
        a[0].last().map_or_else(Vals::new, |&v| one(v))
    }),
    function!(Inline, "aligned", [set, set], |cx, a| {
        let mut out = Distinct::default();
        for at in nodes(&a[1]) {
            let node = cx.jump(at);
            let Some(parent) = node.parent() else {
                continue;
            };
            let ordinal = parent
                .children()
                .filter(|sibling| sibling.kind() == node.kind())
                .position(|sibling| sibling.index() == node.index());
            if let Some(counterpart) = ordinal.and_then(|n| nodes(&a[0]).nth(n)) {
                out.push(Val::Node(counterpart));
            }
        }
        out.into_vals()
    }),
    function!(Inline, "file", [set], |_, a| {
        set(nodes(&a[0]).map(|at| Val::Node(Loc { fi: at.fi, node: 0 })))
    }),
    function!(Inline, "scope", [set], |cx, a| {
        let mut out = Distinct::default();
        for at in nodes(&a[0]) {
            out.push(Val::Node(cx.scope_above(at).unwrap_or(at)));
        }
        out.into_vals()
    }),
    function!(Inline, "enclose", [set], |cx, a| {
        let mut out = Distinct::default();
        for at in nodes(&a[0]) {
            let mut next = cx.scope_above(at);
            while let Some(scope) = next {
                out.push(Val::Node(scope));
                next = cx.scope_above(scope);
            }
        }
        out.into_vals()
    }),
    function!(Inline, "ancestors", [set], |cx, a| {
        nodes(&a[0])
            .flat_map(|at| cx.jump(at).ancestors().map(|c| Val::Node(c.into())))
            .collect()
    }),
    function!(Inline, "follow", [set, edge, edge_key], |cx, a| {
        let (kind, key) = (edge_kind(&a[1]), number(&a[2]) as usize);
        let mut out = Distinct::default();
        for at in nodes(&a[0]) {
            match key {
                0 => cx
                    .targets(kind, at)
                    .iter()
                    .for_each(|&to| out.push(Val::Node(to))),
                key => cx
                    .by_attribute(key - 1, kind, at)
                    .iter()
                    .for_each(|&(_, to)| out.push(Val::Node(to))),
            }
        }
        out.into_vals()
    }),
    function!(Inline, "select", [region, pattern, set], |cx, a| select(
        cx, a
    )),
    function!(Effect, "edge", [edge, set, set, rest], |cx, a| {
        let kind = edge_kind(&a[0]);
        let attrs: Vec<(usize, u32)> = cx
            .lang()
            .edge_attributes()
            .iter()
            .zip(&a[3..])
            .enumerate()
            .filter_map(|(slot, (attribute, vals))| {
                let value = match (attribute.node, vals.first()) {
                    (true, Some(v)) => v.loc()?.node,
                    (false, Some(&Val::Sym(sym))) => sym,
                    _ => return None,
                };
                Some((slot, value))
            })
            .collect();
        for from in nodes(&a[1]) {
            for to in nodes(&a[2]) {
                let edge = Edge::new(from.fi, from.node, to.fi, to.node, kind);
                cx.emit(
                    attrs
                        .iter()
                        .fold(edge, |edge, &(slot, value)| edge.with_attr(slot, value)),
                );
            }
        }
        Vals::new()
    }),
    function!(Effect, "set_tag", [one, one, set, set], |cx, a| {
        let (key, on) = (number(&a[0]), number(&a[1]) as u16);
        if let Some(val) = first_sym(cx, &a[3]) {
            for at in nodes(&a[2]) {
                cx.write_tree(Write::Tag {
                    at: at.node,
                    key,
                    on,
                    val,
                });
            }
        }
        Vals::new()
    }),
    function!(Effect, "parse", [set, set], |cx, a| {
        if let Some(frontend) = first_sym(cx, &a[1]) {
            for at in nodes(&a[0]) {
                cx.write_tree(Write::Parse {
                    at: at.node,
                    frontend,
                });
            }
        }
        Vals::new()
    }),
    function!(Effect, "build", [one, set, rest], |cx, a| {
        if let Some(at) = a[1].first().and_then(|v| v.loc()) {
            let syms = a[2..].iter().map(|v| first_sym(cx, v)).collect();
            cx.write_tree(Write::Build {
                template: number(&a[0]) as u16,
                at: at.node,
                syms,
            });
        }
        Vals::new()
    }),
];

/// The matches of index `a[2]` for keys `a[3]`: owned by a node of `a[0]`,
/// under one, or in the trees of a set the walk reads, as region `a[1]` says.
fn select<S: Store>(cx: &mut Cx<'_, '_, Core, S>, a: &[Vals]) -> Vals {
    let (region, id) = (number(&a[1]), number(&a[2]));
    let mut out = Distinct::default();
    if let Some(set) = region.checked_sub(SETS) {
        for &k in &a[3] {
            let Some(key) = cx.text(k) else { continue };
            let found = cx.family(set as usize, id).and_then(|f| f.get(&key));
            cx.profile_lookup(id, key, found.is_some_and(|f| !f.is_empty()));
            for &found in found.into_iter().flatten() {
                out.push(Val::Node(found));
            }
        }
        return out.into_vals();
    }
    for at in nodes(&a[0]) {
        let anchor = if region == UNDER { WHOLE } else { at.node };
        let Some(piece) = cx.piece(at.fi, id) else {
            return out.into_vals();
        };
        for &k in &a[3] {
            let Some(key) = cx.text(k) else { continue };
            let found = piece.get(&(anchor, key));
            cx.profile_lookup(id, key, found.is_some_and(|f| !f.is_empty()));
            for &node in found.into_iter().flatten() {
                let inside = anchor != WHOLE
                    || at.node == 0
                    || cx
                        .jump(Loc { fi: at.fi, node })
                        .ancestors()
                        .any(|a| a.index() == at.node);
                if inside {
                    out.push(Val::Node(Loc { fi: at.fi, node }));
                }
            }
        }
    }
    out.into_vals()
}

const UNDER: u32 = 1;

fn nodes(vals: &Vals) -> impl Iterator<Item = Loc> + '_ {
    vals.iter().filter_map(|v| v.loc())
}

fn set(vals: impl IntoIterator<Item = Val>) -> Vals {
    vals.into_iter().collect::<Distinct>().into_vals()
}

fn number(vals: &Vals) -> u32 {
    match vals.first() {
        Some(&Val::Sym(n)) => n,
        Some(&Val::Kind(k)) => u32::from(k),
        _ => 0,
    }
}

fn edge_kind(vals: &Vals) -> EdgeKind {
    EdgeKind(number(vals) as u16)
}

/// The first symbol of a set: a symbol, or a node's own text.
fn first_sym<S: Store>(cx: &Cx<'_, '_, Core, S>, vals: &Vals) -> Option<u32> {
    vals.iter().find_map(|&v| match v {
        Val::Sym(_) | Val::Node(_) => cx.text(v),
        _ => None,
    })
}
