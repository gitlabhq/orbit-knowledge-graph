mod chain;
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

fn use_role<S: Store>(member: Cursor<'_, S>) -> Role {
    member
        .parent()
        .filter(|p| p.is(C::Callee))
        .map_or(Role::Value, Cursor::role)
}

fn sym<S: Store>(c: Cursor<'_, S>) -> u32 {
    c.sym()
}

fn member_role<S: Store>(c: Cursor<'_, S>) -> u32 {
    role_code(use_role(c))
}

fn site_role<S: Store>(c: Cursor<'_, S>) -> u32 {
    role_code(c.role())
}

fn object_name<S: Store>(member: Cursor<'_, S>) -> u32 {
    member.child(C::Object).map_or(0, |o| o.reference().sym())
}

macro_rules! derive {
    ($f:ident) => {
        Derive {
            compact: $f::<Compact>,
            mutable: $f::<Mutable>,
        }
    };
}

const KEY_NAME: KeyPart = KeyPart::Of(1, derive!(sym));
const KEY_ROLE: KeyPart = KeyPart::Of(1, derive!(member_role));

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

    fn type_of(&mut self, c: Cursor<'_, S>) -> Vals {
        self.call1(crate::fn_id!("core", "type"), Val::Node(c.into()))
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
}

fn class_of<S: Store>(c: Cursor<'_, S>) -> Option<Cursor<'_, S>> {
    Some(c)
        .filter(|c| c.is_class())
        .or_else(|| c.enclosing_def(crate::tree::CLASS_LIKE))
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
    function!(Flow, "class", [one], |cx, a| of(
        arg(cx, a, 0).and_then(class_of)
    )),
    function!(Flow, "name", [one], @uses ["core"."read"], |cx, a| {
        let mut found = cx.read(node(cx, a, 0));
        found.retain(|v| *v != Val::Opaque);
        found
    }),
    function!(
        Flow, "type", [one],
        @uses ["core"."read", "core"."type", "core"."named_member", "core"."ivar", "core"."declared"],
        |cx, a| {
            let mut found = cx.chain(node(cx, a, 0));
            found.retain(|v| *v != Val::Opaque);
            found
        }
    ),
    function!(
        Flow, "named_member", [one, one],
        @key [KeyPart::Of(0, derive!(object_name)), KeyPart::Of(0, derive!(sym)), KeyPart::Of(1, derive!(site_role))],
        @uses ["core"."type", "core"."declared"],
        |cx, a| {
            let member = node(cx, a, 0);
            cx.member_of_object(node(cx, a, 1), member)
        }
    ),
    function!(
        Flow, "member", [one, one],
        @key [KeyPart::Arg(0), KEY_NAME, KEY_ROLE],
        @uses ["core"."type", "core"."member", "core"."declared"],
        |cx, a| match arg(cx, a, 1) {
            Some(m) => cx.member(a[0][0], m),
            None => Vals::new(),
        }
    ),
    function!(
        Flow, "unmatched", [one, one],
        @key [KeyPart::Arg(0), KEY_NAME, KEY_ROLE],
        @uses ["core"."declared"],
        |cx, a| match arg(cx, a, 1) {
            Some(m) => cx.unmatched(a[0][0], m),
            None => Vals::new(),
        }
    ),
    function!(Flow, "members", [one, one, one], @uses ["core"."declared"], |cx, a| {
        let (Some(owner), Some(name), Some(site)) = (arg(cx, a, 0), a[1].first(), arg(cx, a, 2))
        else {
            return Vals::new();
        };
        let Some(name) = cx.text(*name) else {
            return Vals::new();
        };
        of(cx.members(owner, name, site.role()))
    }),
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
    function!(Pure, "ivar", [one, one], |cx, a| {
        let (class, Some(&Val::Sym(attr))) = (node(cx, a, 0), a[1].first()) else {
            return Vals::new();
        };
        let binding = class.descendants().find(|n| {
            n.is(C::Binding) && n.child_sym(C::Ivar) == Some(attr) && n.typed().is_some()
        });
        of(binding.map(|b| cx.at(values::producer(b))))
    }),
    function!(Flow, "supers", [one], |cx, a| {
        let def = node(cx, a, 0).index();
        let supers = cx.state().supers.get(&def).cloned().unwrap_or_default();
        supers.into_iter().map(|d| cx.node(d)).collect()
    }),
    function!(Flow, "callable", [one], @uses ["core"."declared"], |cx, a| cx.callable(a[0][0])),
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
    function!(Flow, "source", [one], @uses ["core"."type"], |cx, a| match arg(cx, a, 0) {
        Some(c) => of([cx.source(c)]),
        None => Vals::new(),
    }),
    function!(Pure, "sink", [one], @uses ["core"."sink"], |cx, a| {
        match values::sink_step(node(cx, a, 0)) {
            Ok(next) => cx.call1(crate::fn_id!("core", "sink"), Val::Node(next.into())),
            Err(binding) => of(binding),
        }
    }),
    function!(Flow, "value_of", [one], @uses ["core"."read", "core"."type", "core"."ivar", "core"."declared"], |cx, a| {
        cx.value_of(node(cx, a, 0))
    }),
    function!(Flow, "valued", [one], @uses ["core"."read", "core"."ivar"], |cx, a| {
        let arm = node(cx, a, 0);
        match cx.valued(arm) {
            true => a[0].clone(),
            false => Vals::new(),
        }
    }),
    function!(Pure, "chain_head", [one], |cx, a| {
        let head = arg(cx, a, 0).filter(|c| c.value_child(C::Member).is_some());
        of(head.map(|c| c.chain_root()))
    }),
    function!(Pure, "binding_site", [one], |cx, a| of(
        values::binding_site(node(cx, a, 0))
    )),
    function!(Pure, "unlifted", [one], |cx, a| match arg(cx, a, 0) {
        Some(c) if !c.lifted() => a[0].clone(),
        _ => Vals::new(),
    }),
    function!(Flow, "reference", [one], @uses ["core"."type"], |cx, a| cx.reference(node(cx, a, 0))),
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
        match cx.state().shadowed.contains(&target.index())
            && !target.is(C::Def)
            && !target.is(C::Name)
        {
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
