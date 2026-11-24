//! The rules' common vocabulary: set operations, tree navigation, the graph
//! and declared indexes, and the writes a rule makes. Literal arguments
//! (edge kinds, index, row shape and template ids) arrive as numbers.

use crate::engine::{Cx, FILES, Function, Loc, Module, Store, Val, Vals, WHOLE, Write, one};
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
        set(a[0].iter().copied().filter(|v| !a[1].contains(v)))
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
    function!(Inline, "project", [], |cx, _| {
        cx.project()
            .map_or_else(Vals::new, |root| one(Val::Node(root.into())))
    }),
    function!(Inline, "file", [set], |_, a| {
        set(nodes(&a[0]).map(|at| Val::Node(Loc { fi: at.fi, node: 0 })))
    }),
    function!(Inline, "scope", [set], |cx, a| {
        let mut out = Vals::new();
        for at in nodes(&a[0]) {
            push(&mut out, Val::Node(cx.scope_above(at).unwrap_or(at)));
        }
        out
    }),
    function!(Inline, "enclose", [set], |cx, a| {
        let mut out = Vals::new();
        for at in nodes(&a[0]) {
            let mut next = cx.scope_above(at);
            while let Some(scope) = next {
                push(&mut out, Val::Node(scope));
                next = cx.scope_above(scope);
            }
        }
        out
    }),
    function!(Inline, "ancestors", [set], |cx, a| {
        set(nodes(&a[0]).flat_map(|at| cx.jump(at).ancestors().map(|c| Val::Node(c.into()))))
    }),
    function!(Inline, "follow", [set, edge, edge_key], |cx, a| {
        let (kind, key) = (edge_kind(&a[1]), number(&a[2]) as usize);
        let mut out = Vals::new();
        for at in nodes(&a[0]) {
            match key {
                0 => cx
                    .targets(kind, at)
                    .iter()
                    .for_each(|&to| push(&mut out, Val::Node(to))),
                key => cx
                    .by_attribute(key - 1, kind, at)
                    .iter()
                    .for_each(|&(_, to)| push(&mut out, Val::Node(to))),
            }
        }
        out
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

/// Matches of a declared index under each node of the first set, by each
/// key of the third: in the node's own scope, or anywhere under it.
/// The matches of index `a[2]` for keys `a[3]`: owned by a node of `a[0]`,
/// under one, or in any file the walk reads, as region `a[1]` says.
fn select<S: Store>(cx: &mut Cx<'_, '_, Core, S>, a: &[Vals]) -> Vals {
    let (region, id) = (number(&a[1]), number(&a[2]));
    let mut out = Vals::new();
    if region == FILES {
        for key in a[3].iter().filter_map(|&k| cx.text(k)) {
            for &found in cx
                .family(id)
                .and_then(|f| f.get(&key))
                .into_iter()
                .flatten()
            {
                push(&mut out, Val::Node(found));
            }
        }
        return out;
    }
    for at in nodes(&a[0]) {
        let anchor = if region == UNDER { WHOLE } else { at.node };
        let Some(piece) = cx.piece(at.fi, id) else {
            return out;
        };
        for key in a[3].iter().filter_map(|&k| cx.text(k)) {
            for &node in piece.get(&(anchor, key)).into_iter().flatten() {
                let inside = anchor != WHOLE
                    || at.node == 0
                    || cx
                        .jump(Loc { fi: at.fi, node })
                        .ancestors()
                        .any(|a| a.index() == at.node);
                if inside {
                    push(&mut out, Val::Node(Loc { fi: at.fi, node }));
                }
            }
        }
    }
    out
}

const UNDER: u32 = 1;

fn nodes(vals: &Vals) -> impl Iterator<Item = Loc> + '_ {
    vals.iter().filter_map(|v| v.loc())
}

fn push(out: &mut Vals, v: Val) {
    if !out.contains(&v) {
        out.push(v);
    }
}

fn set(vals: impl IntoIterator<Item = Val>) -> Vals {
    let mut out = Vals::new();
    vals.into_iter().for_each(|v| push(&mut out, v));
    out
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
