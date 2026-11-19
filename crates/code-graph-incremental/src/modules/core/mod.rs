mod lookup;
pub mod ops;

use smallvec::SmallVec;

use crate::canonical::Canonical as C;
use crate::engine::{Arg, Cursor, Cx, Function, Kind, Loc, Module, Val, Vals, of, truthy, yes};
use crate::modules::ssa::function;
use crate::tree::{EdgeKind, Role};

fn cursor<'t>(cx: &Cx<'t>, val: Val) -> Option<Cursor<'t>> {
    val.loc().map(|loc| cx.jump(loc))
}

fn one<'t>(cx: &Cx<'t>, args: &[Vals], at: usize) -> Option<Cursor<'t>> {
    args[at].first().and_then(|&v| cursor(cx, v))
}

fn kind(args: &[Vals], at: usize) -> u16 {
    match args[at].first() {
        Some(Val::Kind(kind)) => *kind,
        _ => 0,
    }
}

fn keep<'t>(c: Option<Cursor<'t>>, pred: impl FnOnce(Cursor<'t>) -> bool) -> Vals {
    of(c.filter(|&c| pred(c)))
}

pub(crate) fn role(member: Cursor<'_>) -> Role {
    member
        .parent()
        .filter(|p| p.is(C::Callee))
        .map_or(Role::Value, Cursor::role)
}

pub static MODULE: Module = Module {
    name: "core",
    functions: &[
        function!(Pure, "named", [one], |cx, a| keep(one(cx, a, 0), |c| c
            .sym_opt()
            .is_some())),
        function!(Pure, "child", [one, one], |cx, a| {
            let k = kind(a, 1);
            of(one(cx, a, 0).and_then(|c| c.children().find(|ch| ch.kind() == k)))
        }),
        function!(Pure, "child_all", [one, one], |cx, a| {
            let k = kind(a, 1);
            of(one(cx, a, 0)
                .into_iter()
                .flat_map(|c| c.children())
                .filter(|ch| ch.kind() == k))
        }),
        function!(Pure, "without", [one, one], |cx, a| {
            let k = kind(a, 1);
            of(one(cx, a, 0)
                .into_iter()
                .flat_map(|c| c.children())
                .filter(|ch| ch.kind() != k))
        }),
        function!(Pure, "lacks", [one, one], |cx, a| {
            let k = kind(a, 1);
            keep(one(cx, a, 0), |c| !c.children().any(|ch| ch.kind() == k))
        }),
        function!(Pure, "tagged", [one, one], |cx, a| {
            let Some(&Val::Sym(tag)) = a[1].first() else {
                return Vals::new();
            };
            keep(one(cx, a, 0), |c| c.has_tag(tag))
        }),
        Function {
            name: "not",
            sig: &[Arg::Set],
            kind: Kind::Pure,
            call: |_, a| yes(!truthy(&a[0])),
        },
        function!(Pure, "is_class", [one], |cx, a| keep(one(cx, a, 0), |c| c
            .is_class())),
        function!(Pure, "is_contract", [one], |cx, a| {
            keep(one(cx, a, 0), |c| c.is_dispatch_contract())
        }),
        function!(Pure, "impl_name", [one], |cx, a| {
            keep(one(cx, a, 0), |c| {
                c.parent().is_some_and(|p| p.has(C::ImplBlock))
            })
        }),
        function!(Pure, "chain_head", [one], |cx, a| {
            of(one(cx, a, 0)
                .filter(|c| c.value_child(C::Member).is_some())
                .map(|c| c.chain_root()))
        }),
        function!(Pure, "member_of", [one], |cx, a| of(
            one(cx, a, 0).and_then(|c| c.child(C::Member))
        )),
        function!(Pure, "body", [one], |cx, a| {
            let Some(c) = one(cx, a, 0).filter(|c| c.has_tag(cx.tags.scoped)) else {
                return Vals::new();
            };
            let initializer = c.initializer().filter(|_| c.is_class()).map(Cursor::index);
            of(c.children()
                .filter(|node| Some(node.index()) != initializer))
        }),
        function!(Flow, "enclosing", [], |cx, _| of([Loc {
            fi: cx.fi,
            node: cx.enclosing()
        }])),
        function!(Flow, "enclosing_def", [], |cx, _| {
            let fi = cx.fi;
            of(cx.defs.last().map(|&node| Loc { fi, node }))
        }),
        function!(Flow, "class", [one], |cx, a| {
            let fi = cx.fi;
            let class = one(cx, a, 0).and_then(|c| cx.enclosing_class(c.index()));
            of(class.map(|node| Loc { fi, node }))
        }),
        function!(Flow, "name", [one], |cx, a| match one(cx, a, 0) {
            Some(c) => {
                let found = cx.lookup(c.sym());
                crate::modules::ssa::to_vals(&found)
            }
            None => Vals::new(),
        }),
        function!(Flow, "type", [one], |cx, a| match one(cx, a, 0) {
            Some(c) if cx.family.is_some() => of(cx.names(c)),
            Some(c) => crate::modules::ssa::to_vals(&ops::chain(cx, c)),
            None => Vals::new(),
        }),
        function!(Flow, "member", [one, one], |cx, a| {
            let Some(m) = one(cx, a, 1) else {
                return Vals::new();
            };
            match (cx.family.is_some(), cursor(cx, a[0][0])) {
                (true, Some(receiver)) => {
                    of(cx.method_up(receiver, m.sym(), m.fi() as usize, role(m)))
                }
                (true, None) => Vals::new(),
                (false, _) => cx.member(a[0][0], m),
            }
        }),
        function!(
            Flow,
            "contract_member",
            [one, one],
            |cx, a| match (one(cx, a, 0), one(cx, a, 1)) {
                (Some(receiver), Some(m)) => of(cx.member_targets(m, receiver, role(m))),
                _ => Vals::new(),
            }
        ),
        Function {
            name: "site_class",
            sig: &[Arg::One, Arg::Set, Arg::Set],
            kind: Kind::Pure,
            call: |cx, a| {
                let loc = |at: usize| a[at].first().and_then(|v| v.loc());
                let Some(site) = loc(0) else {
                    return Vals::new();
                };
                of(cx.site_class(site, loc(1), loc(2)))
            },
        },
        function!(Pure, "cross", [one, one], |cx, a| {
            let site = one(cx, a, 1).map(|s| s.fi());
            keep(one(cx, a, 0), |c| Some(c.fi()) != site)
        }),
        function!(Pure, "owner", [one], |cx, a| of(one(cx, a, 0)
            .and_then(|c| c.parent())
            .filter(|p| p.is(C::Def)))),
        function!(Pure, "root_name", [one], |cx, a| {
            of(one(cx, a, 0)
                .map(|c| c.reference().chain_root())
                .filter(|r| r.sym_opt().is_some()))
        }),
        function!(
            Flow,
            "unbound",
            [one, one],
            |cx, a| match (one(cx, a, 0), a[1].first()) {
                (Some(_), Some(&Val::Sym(sym))) => cx.unbound(sym),
                _ => Vals::new(),
            }
        ),
        function!(
            Flow,
            "unbound_object",
            [one, one],
            |cx, a| match (one(cx, a, 0), one(cx, a, 1)) {
                (Some(from), Some(object)) => cx.unbound_object(from, object),
                _ => Vals::new(),
            }
        ),
        function!(
            Effect,
            "inherit",
            [one, one],
            |cx, a| match (one(cx, a, 0), one(cx, a, 1)) {
                (Some(class), Some(parent)) => yes(cx.inherited(class, parent)),
                _ => Vals::new(),
            }
        ),
        function!(
            Effect,
            "destructure",
            [one, one],
            |cx, a| match (one(cx, a, 0), one(cx, a, 1)) {
                (Some(d), Some(from)) => yes(cx.destructure_calls(d, from)),
                _ => Vals::new(),
            }
        ),
        function!(Flow, "unmatched", [one, one], |cx, a| {
            let Some(m) = one(cx, a, 1) else {
                return Vals::new();
            };
            cx.unmatched(a[0][0], m)
        }),
        function!(Flow, "members", [one, one, one], |cx, a| {
            let (Some(owner), Some(&Val::Sym(name)), Some(site)) =
                (one(cx, a, 0), a[1].first(), one(cx, a, 2))
            else {
                return Vals::new();
            };
            let found = ops::members(cx, owner, name, site.role());
            of(found)
        }),
        function!(Flow, "supers", [one], |cx, a| {
            let fi = cx.fi;
            let Some(def) = one(cx, a, 0) else {
                return Vals::new();
            };
            let supers = cx.supers_of(def.index());
            of(supers.into_iter().map(|node| Loc { fi, node }))
        }),
        function!(Flow, "reference", [one], |cx, a| match one(cx, a, 0) {
            Some(c) => cx.reference(c),
            None => Vals::new(),
        }),
        function!(Flow, "receiver_calls", [one], |cx, a| {
            let receiver = one(cx, a, 0)
                .and_then(|m| m.child(C::Dispatch))
                .and_then(|d| d.child(C::Object));
            let Some(receiver) = receiver else {
                return Vals::new();
            };
            let found = cx.lookup(receiver.sym());
            let calls = found
                .iter()
                .filter(|v| matches!(v, crate::modules::ssa::Value::Call(_)));
            crate::modules::ssa::to_vals(calls)
        }),
        function!(Flow, "typed_calls", [one], |cx, a| {
            let typed = matches!(cx.value(a[0][0]), Some(crate::modules::ssa::Value::Call(n)) if cx.binding_type(n).is_some());
            match typed {
                true => a[0].clone(),
                false => Vals::new(),
            }
        }),
        function!(Flow, "named_member", [one, one], |cx, _| cx.named_member()),
        function!(Flow, "standalone", [one], |cx, a| keep(
            one(cx, a, 0),
            |c| cx.standalone(c)
        )),
        function!(Flow, "callee_targets", [one], |cx, a| match one(cx, a, 0) {
            Some(call) => cx.callee_targets(call),
            None => Vals::new(),
        }),
    ],
};

/// The edges a reference to `target` makes: a call to a callable def, an
/// import to an imported name, a type flow to a call, and a constructor call
/// to a type.
pub(crate) fn refs(cx: &mut Cx, target: Val) -> SmallVec<[(EdgeKind, Loc); 2]> {
    let mut out = SmallVec::new();
    match target {
        Val::Type(loc) => {
            let class = cx.jump(loc);
            match class.child_sym(C::Callable) {
                Some(name) => out.extend(
                    ops::members(cx, class, name, Role::Callable)
                        .into_iter()
                        .map(|m| (EdgeKind::Calls, Loc::from(m))),
                ),
                None => out.push((EdgeKind::Calls, loc)),
            }
        }
        Val::Node(loc) => {
            let c = cx.jump(loc);
            let kind = match c.kind() {
                k if k == C::Def && c.has_tag(cx.tags.callable) => EdgeKind::Calls,
                k if k == C::Def => return out,
                k if k == C::Name => EdgeKind::Imports,
                _ => EdgeKind::TypeFlow,
            };
            out.push((kind, loc));
        }
        _ => {}
    }
    out
}
