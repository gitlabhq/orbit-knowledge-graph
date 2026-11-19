mod members;
mod values;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::engine::{
    Cursor, Cx, Derive, Function, KeyPart, Loc, Module, Store, Val, Vals, of, one, yes,
};
use crate::modules::{arg, function, node};
use crate::tree::{Compact, Mutable, Role};

pub struct Core;

#[derive(Default)]
pub struct State {
    defs: FxHashMap<u32, Vec<u32>>,
    supers: FxHashMap<u32, Vec<u32>>,
    shadowed: FxHashSet<u32>,
}

#[derive(Clone, Copy)]
enum Ref {
    Def(u32),
    Import(u32),
    Type(u32),
    Call(u32),
}

fn role_code(role: Role) -> u32 {
    match role {
        Role::Value => 0,
        Role::Callable => 1,
        Role::Type => 2,
    }
}

fn role_of(code: u32) -> Role {
    match code {
        1 => Role::Callable,
        2 => Role::Type,
        _ => Role::Value,
    }
}

fn site_role<S: Store>(site: Cursor<'_, S>) -> Role {
    match site.is(C::Callee) || site.is(C::Call) {
        true => site.role(),
        false => site
            .parent()
            .filter(|p| p.is(C::Callee))
            .map_or(Role::Value, Cursor::role),
    }
}

fn sym<S: Store>(c: Cursor<'_, S>) -> u32 {
    c.sym()
}

fn class_of<S: Store>(c: Cursor<'_, S>) -> Option<Cursor<'_, S>> {
    Some(c)
        .filter(|c| c.is_class())
        .or_else(|| c.enclosing_def(crate::tree::CLASS_LIKE))
}

const KEY_SYM: KeyPart = KeyPart::Of(
    0,
    Derive {
        compact: sym::<Compact>,
        mutable: sym::<Mutable>,
    },
);

impl<'t, S: Store> Cx<'_, 't, Core, S> {
    fn view(&self, val: Val) -> Option<Ref> {
        match val {
            Val::Type(loc) => Some(Ref::Type(loc.node)),
            Val::Node(loc) => Some(match self.jump(loc) {
                n if n.is(C::Def) => Ref::Def(loc.node),
                n if n.is(C::Name) => Ref::Import(loc.node),
                _ => Ref::Call(loc.node),
            }),
            _ => None,
        }
    }

    fn node(&self, node: u32) -> Val {
        Val::Node(self.loc(node))
    }

    fn read(&mut self, c: Cursor<'_, S>) -> Vals {
        self.call1(crate::fn_id!("core", "read"), Val::Node(c.into()))
    }

    fn ivar(&mut self, c: Cursor<'_, S>) -> Vals {
        self.call1(crate::fn_id!("core", "ivar"), Val::Node(c.into()))
    }

    fn keep(&self, val: Val, pred: impl FnOnce(Ref, &Self) -> bool) -> Vals {
        match self.view(val) {
            Some(r) if pred(r, self) => one(val),
            _ => Vals::new(),
        }
    }

    fn enclosing_class(&mut self) -> Option<Cursor<'t, S>> {
        let enclosing = self.enclosing();
        class_of(self.at(enclosing))
    }

    fn define(&mut self, def: Cursor<'_, S>) {
        let Some(name) = def.child_sym(C::DefName) else {
            return;
        };
        let known = self
            .state()
            .defs
            .get(&name)
            .is_some_and(|d| d.contains(&def.index()));
        if !known {
            self.write().defs.entry(name).or_default().push(def.index());
        }
    }

    fn inherit(&mut self, def: u32, supers: &[Val]) {
        let supers: Vec<u32> = supers
            .iter()
            .filter_map(|&v| match self.view(v) {
                Some(Ref::Def(d)) => Some(d),
                _ => None,
            })
            .collect();
        let known = self.write().supers.entry(def).or_default();
        for s in supers {
            if !known.contains(&s) {
                known.push(s);
            }
        }
    }

    fn shadow(&mut self, call: Cursor<'t, S>) {
        let Some(callee) = call.child(C::Callee) else {
            return;
        };
        let root = callee
            .child(C::Member)
            .and_then(|m| m.child(C::Object))
            .map_or(callee, |o| o.chain_root());
        if callee.has(C::Ivar) || root.is(C::Call) || root.value_child(C::Call).is_some() {
            return;
        }
        let found = self.read(root);
        let named = |v: &Val| !matches!(self.view(*v), Some(Ref::Call(_)) | None);
        if !found.iter().all(named) {
            self.write().shadowed.insert(call.index());
        }
    }

    fn field(&mut self, attr: u32) -> Vals {
        let Some(class) = self.enclosing_class() else {
            return Vals::new();
        };
        let binding = class.descendants().find(|n| {
            n.is(C::Binding) && n.child_sym(C::Ivar) == Some(attr) && n.typed().is_some()
        });
        of(binding.map(|b| self.at(values::producer(b))))
    }
}

impl Module for Core {
    const NAME: &'static str = "core";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }

    fn start<'t, S: Store>(cx: &mut Cx<'_, 't, Self, S>, root: Cursor<'t, S>) {
        for def in root
            .children()
            .filter(|d| d.is(C::Def) && !d.has(C::Declaration))
        {
            cx.define(def);
        }
    }
}

static FUNCTIONS: &[Function<Core>] = &[
    function!(required Flow, "read", [one]),
    function!(Flow, "enclosing", [], |cx, _| {
        let node = cx.enclosing();
        one(cx.node(node))
    }),
    function!(Flow, "enclosing_def", [], |cx, _| {
        let def = cx.enclosing_def();
        def.map_or_else(Vals::new, |d| one(cx.node(d)))
    }),
    function!(Pure, "class", [one], |cx, a| of(
        arg(cx, a, 0).and_then(class_of)
    )),
    function!(Flow, "name", [one], @uses ["core"."read"], |cx, a| {
        let mut found = cx.read(node(cx, a, 0));
        found.retain(|v| *v != Val::Opaque);
        found
    }),
    function!(Flow, "ivar", [one], @key [KEY_SYM], |cx, a| {
        let attr = node(cx, a, 0).sym();
        cx.field(attr)
    }),
    function!(Flow, "lookup", [one, one, set], @uses ["core"."declared"], |cx, a| {
        let (Some(name), Some(site)) = (a[0].first().and_then(|&v| cx.text(v)), arg(cx, a, 1)) else {
            return Vals::new();
        };
        cx.lookup(name, site_role(site), &a[2])
    }),
    function!(Flow, "meaning", [one, one], @uses ["core"."declared"], |cx, a| {
        let site = node(cx, a, 1);
        cx.meaning(a[0][0], site)
    }),
    function!(Flow, "unmatched", [one, one], @uses ["core"."declared"], |cx, a| {
        match arg(cx, a, 1) {
            Some(m) => cx.unmatched(a[0][0], m),
            None => Vals::new(),
        }
    }),
    function!(Flow, "callable", [one], @uses ["core"."declared"], |cx, a| cx.callable(a[0][0])),
    function!(Pure, "declared", [one, one, one], |cx, a| {
        let (owner, Some(&Val::Sym(name)), Some(&Val::Sym(role))) =
            (node(cx, a, 0), a[1].first(), a[2].first())
        else {
            return Vals::new();
        };
        let defs = crate::tree::members_named(owner)
            .filter(|&(n, _)| n == name)
            .map(|(_, def)| owner.jump(owner.fi(), def));
        let found = crate::tree::find_member_in(owner, defs, role_of(role), &cx.tags());
        of(found.map(|node| Loc::new(owner.fi() as usize, node)))
    }),
    function!(Flow, "supers", [one], |cx, a| {
        let def = node(cx, a, 0).index();
        let supers = cx.state().supers.get(&def).cloned().unwrap_or_default();
        supers.into_iter().map(|d| cx.node(d)).collect()
    }),
    function!(Pure, "sole", [set, one], |cx, a| {
        let exclude = node(cx, a, 1).index();
        match a[0].as_slice() {
            [v] if matches!(cx.view(*v), Some(Ref::Def(t) | Ref::Import(t)) if t != exclude) => {
                a[0].clone()
            }
            _ => Vals::new(),
        }
    }),
    function!(Pure, "type_node", [one], |cx, a| match cx.view(a[0][0]) {
        Some(Ref::Call(n)) => of(values::declared_type(cx.at(n))),
        _ => Vals::new(),
    }),
    function!(Pure, "tail", [one], |cx, a| of(
        arg(cx, a, 0).map(Cursor::tail_expr)
    )),
    function!(Pure, "imported", [one], |cx, a| cx
        .keep(a[0][0], |r, _| matches!(r, Ref::Import(_)))),
    function!(Pure, "produced", [one], |cx, a| cx
        .keep(a[0][0], |r, _| matches!(r, Ref::Call(_)))),
    function!(Pure, "is_def", [one], |cx, a| cx
        .keep(a[0][0], |r, _| matches!(r, Ref::Def(_)))),
    function!(Pure, "is_class", [one], |cx, a| cx
        .keep(a[0][0], |r, cx| {
            matches!(r, Ref::Def(n) if cx.at(n).is_class())
        })),
    function!(Pure, "is_contract", [one], |cx, a| cx
        .keep(a[0][0], |r, cx| {
            matches!(r, Ref::Def(n) if cx.at(n).is_dispatch_contract())
        })),
    function!(Pure, "initialized", [one], |cx, a| {
        cx.keep(a[0][0], |r, cx| {
        matches!(r, Ref::Def(n) if cx.at(n).initializer().and_then(Cursor::rhs_callee).is_some())
    })
    }),
    function!(Pure, "typed_calls", [one], |cx, a| cx
        .keep(a[0][0], |r, cx| {
            matches!(r, Ref::Call(n) if values::declared_type(cx.at(n)).is_some())
        })),
    function!(Pure, "callable_import", [one], |cx, a| {
        let callable = cx.tags().callable;
        cx.keep(a[0][0], |r, cx| {
            matches!(r, Ref::Import(n) if cx.at(n).parent().is_some_and(|i| i.has_tag(callable)))
        })
    }),
    function!(Pure, "unlifted", [one], |cx, a| match arg(cx, a, 0) {
        Some(c) if !c.lifted() => a[0].clone(),
        _ => Vals::new(),
    }),
    function!(Flow, "value_of", [one], @uses ["core"."read", "core"."ivar"], |cx, a| {
        cx.value_of(node(cx, a, 0))
    }),
    function!(Flow, "valued", [one], @uses ["core"."read", "core"."ivar"], |cx, a| {
        match cx.valued(node(cx, a, 0)) {
            true => a[0].clone(),
            false => Vals::new(),
        }
    }),
    function!(Flow, "receiver_calls", [one], @uses ["core"."read"], |cx, a| {
        cx.receiver_calls(node(cx, a, 0))
    }),
    function!(Flow, "targets", [one], @uses ["core"."read", "core"."declared"], |cx, a| {
        cx.targets(node(cx, a, 0))
    }),
    function!(Flow, "unresolved", [one], @uses ["core"."read", "core"."declared"], |cx, a| {
        let call = node(cx, a, 0);
        match cx.unresolved(call) {
            true => one(Val::Node(call.into())),
            false => Vals::new(),
        }
    }),
    function!(Flow, "resolution", [one, set], |cx, a| {
        let target = node(cx, a, 0);
        let shadowed = cx.state().shadowed.contains(&target.index());
        match shadowed && !target.is(C::Def) && !target.is(C::Name) {
            true => one(Val::Sym(cx.tags().non_callable)),
            false => Vals::new(),
        }
    }),
    function!(Effect, "define", [one], |cx, a| {
        cx.define(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "inherit", [one, set], |cx, a| {
        cx.inherit(node(cx, a, 0).index(), &a[1]);
        yes(true)
    }),
    function!(Effect, "shadow", [one], @uses ["core"."read"], |cx, a| {
        cx.shadow(node(cx, a, 0));
        yes(true)
    }),
];
