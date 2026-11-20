mod members;
mod values;

use rustc_hash::FxHashMap;

use crate::canonical::Canonical as C;
use crate::engine::{
    Cursor, Cx, Derive, Function, IndexDecl, KeyPart, Loc, Module, Store, Val, Vals, of, one,
};
use crate::modules::{function, node};
use crate::tree::{Compact, Mutable, Tree};

pub struct Core;

#[derive(Default)]
pub struct State;

type DefsByName = FxHashMap<u32, Vec<Loc>>;

const NON_CALLABLE: &str = "non_callable";

#[derive(Clone, Copy)]
enum Ref {
    Def(Loc),
    Import,
    Type(Loc),
    Call,
}

fn defs_by_name(
    _: &crate::intern::Lang,
    trees: &[Tree<Compact>],
    _: &[crate::tree::Edge],
) -> Box<dyn std::any::Any + Send + Sync> {
    let mut defs = DefsByName::default();
    for (fi, tree) in trees.iter().enumerate() {
        for def in tree.root().descendants().filter(|d| d.is(C::Def)) {
            if let Some(name) = def.child_sym(C::DefName) {
                defs.entry(name)
                    .or_default()
                    .push(Loc::new(fi, def.index()));
            }
        }
    }
    Box::new(defs)
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
            Val::Type(loc) => Some(Ref::Type(loc)),
            Val::Node(loc) => Some(match self.jump(loc) {
                n if n.is(C::Def) => Ref::Def(loc),
                n if n.is(C::Name) => Ref::Import,
                _ => Ref::Call,
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

    fn enclosing_class(&mut self) -> Option<Cursor<'t, S>> {
        let enclosing = self.enclosing();
        class_of(self.at(enclosing))
    }

    fn supers(&mut self, def: Loc) -> Vec<Loc> {
        let found = self.call(crate::fn_id!("core", "supertypes"), &[one(Val::Node(def))]);
        let mut supers: Vec<Loc> = found
            .iter()
            .filter_map(|&v| match self.view(v) {
                Some(Ref::Def(d)) => Some(d),
                _ => None,
            })
            .collect();
        let extends = self.lang().edge_kind("extends");
        for &s in extends.map_or(&[][..], |kind| self.targets(kind, def)) {
            if !supers.contains(&s) {
                supers.push(s);
            }
        }
        supers
    }

    fn named_defs(&mut self, name: u32) -> Vec<Loc> {
        self.index::<DefsByName>("defs")
            .and_then(|index| index.get(&name).cloned())
            .unwrap_or_default()
    }

    /// A call whose callee root reads a local value, so a name-only target
    /// is not what it calls.
    fn shadowed(&mut self, call: Cursor<'t, S>) -> bool {
        let Some(callee) = call.child(C::Callee) else {
            return false;
        };
        let root = callee
            .child(C::Member)
            .and_then(|m| m.child(C::Object))
            .map_or(callee, |o| o.chain_root());
        if callee.has(C::Ivar) || root.is(C::Call) || root.value_child(C::Call).is_some() {
            return false;
        }
        let found = self.read(root);
        let named = |v: &Val| !matches!(self.view(*v), Some(Ref::Call) | None);
        !found.iter().all(named)
    }

    fn field(&mut self, attr: u32) -> Vals {
        let Some(class) = self.enclosing_class() else {
            return Vals::new();
        };
        let binding = class.descendants().find(|n| {
            n.is(C::Binding) && n.child_sym(C::Ivar) == Some(attr) && n.typed().is_some()
        });
        of(binding.map(|b| b.jump(b.fi(), values::producer(b))))
    }
}

impl Module for Core {
    const NAME: &'static str = "core";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }

    fn indexes() -> &'static [IndexDecl] {
        &[IndexDecl {
            name: "defs",
            build: defs_by_name,
        }]
    }
}

static FUNCTIONS: &[Function<Core>] = &[
    function!(required Flow, "read", [one]),
    function!(required Flow, "supertypes", [one]),
    function!(Flow, "enclosing", [], |cx, _| {
        let node = cx.enclosing();
        one(cx.node(node))
    }),
    function!(Flow, "enclosing_def", [], |cx, _| {
        let def = cx.enclosing_def();
        def.map_or_else(Vals::new, |d| one(cx.node(d)))
    }),
    function!(Flow, "name", [one], @uses ["core"."read"], |cx, a| {
        let mut found = cx.read(node(cx, a, 0));
        found.retain(|v| *v != Val::Opaque);
        found
    }),
    function!(Flow, "ivar", [one], @key [KEY_SYM], |cx, a| {
        let attr = node(cx, a, 0).sym();
        cx.field(attr)
    }),
    function!(Flow, "supers", [one], |cx, a| {
        let def = Loc::from(node(cx, a, 0));
        of(cx.supers(def))
    }),
    function!(
        Pure,
        "sym",
        [one],
        |cx, a| match a[0].first().and_then(|&v| cx.text(v)) {
            Some(sym) => one(Val::Sym(sym)),
            None => Vals::new(),
        }
    ),
    function!(Flow, "lub", [set], @uses ["core"."supertypes"], |cx, a| {
        match a[0].contains(&Val::Opaque) {
            true => Vals::new(),
            false => cx.lub(&a[0]),
        }
    }),
    function!(Pure, "inferred_return", [one], |cx, a| {
        match crate::tree::infer_return_type(node(cx, a, 0)) {
            Some(sym) => one(Val::Sym(sym)),
            None => Vals::new(),
        }
    }),
    function!(Flow, "resolution", [one, set], @uses ["core"."read"], |cx, a| {
        let target = node(cx, a, 0);
        let shadowed = target.is(C::Call) && cx.shadowed(target);
        match shadowed && !target.is(C::Def) && !target.is(C::Name) {
            true => one(Val::Sym(cx.lang().syms.intern(NON_CALLABLE))),
            false => Vals::new(),
        }
    }),
];
