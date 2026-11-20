mod members;
mod values;

use rustc_hash::FxHashMap;

use crate::canonical::Canonical as C;
use crate::engine::{
    Cursor, Cx, Derive, Function, IndexDecl, KeyPart, Loc, Module, Store, Val, Vals, of, one,
};
use crate::modules::{arg, function, node};
use crate::tree::{Compact, Mutable, Role, Tree};

pub struct Core;

/// The file language's role kind lists, read once when a run starts; they
/// do not change within it.
#[derive(Default)]
pub struct State {
    roles: std::rc::Rc<Roles>,
}

type DefsByName = FxHashMap<u32, Vec<Loc>>;

const NON_CALLABLE: &str = "non_callable";

#[derive(Default)]
pub(super) struct Roles {
    callable: Vals,
    value: Vals,
    ty: Vals,
}

impl Roles {
    fn has<S: Store>(&self, def: Cursor<'_, S>, role: Role) -> bool {
        let kinds = match role {
            Role::Callable => &self.callable,
            Role::Value => &self.value,
            Role::Type => &self.ty,
        };
        def.children().any(|c| kinds.contains(&Val::Kind(c.kind())))
    }

    fn both<S: Store>(&self, def: Cursor<'_, S>) -> bool {
        self.has(def, Role::Value) && self.has(def, Role::Callable)
    }
}

#[derive(Clone, Copy)]
enum Ref {
    Def(Loc),
    Import(Loc),
    Type(Loc),
    Call(Loc),
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
            Val::Type(loc) => Some(Ref::Type(loc)),
            Val::Node(loc) => Some(match self.jump(loc) {
                n if n.is(C::Def) => Ref::Def(loc),
                n if n.is(C::Name) => Ref::Import(loc),
                _ => Ref::Call(loc),
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
        let named = |v: &Val| !matches!(self.view(*v), Some(Ref::Call(_)) | None);
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

    fn start<'t, S: Store>(cx: &mut Cx<'_, 't, Self, S>, root: Cursor<'t, S>) {
        let at = one(Val::Node(root.into()));
        let roles = Roles {
            callable: cx.call(
                crate::fn_id!("core", "callable_kinds"),
                std::slice::from_ref(&at),
            ),
            value: cx.call(
                crate::fn_id!("core", "value_kinds"),
                std::slice::from_ref(&at),
            ),
            ty: cx.call(
                crate::fn_id!("core", "type_kinds"),
                std::slice::from_ref(&at),
            ),
        };
        cx.write().roles = std::rc::Rc::new(roles);
    }
}

static FUNCTIONS: &[Function<Core>] = &[
    function!(required Flow, "read", [one]),
    function!(required Flow, "supertypes", [one]),
    function!(required Flow, "callable_kinds", [one]),
    function!(required Flow, "value_kinds", [one]),
    function!(required Flow, "type_kinds", [one]),
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
        let roles = std::rc::Rc::clone(&cx.state().roles);
        cx.lookup(name, site_role(site), &roles, &a[2])
    }),
    function!(Flow, "meaning", [one, one], @uses ["core"."declared"], |cx, a| {
        let site = node(cx, a, 1);
        let roles = std::rc::Rc::clone(&cx.state().roles);
        cx.meaning(a[0][0], site, &roles)
    }),
    function!(Flow, "unmatched", [one, one], @uses ["core"."declared"], |cx, a| {
        match arg(cx, a, 1) {
            Some(m) => {
                let roles = std::rc::Rc::clone(&cx.state().roles);
                cx.unmatched(a[0][0], m, &roles)
            }
            None => Vals::new(),
        }
    }),
    function!(Flow, "callable", [one], @uses ["core"."declared"], |cx, a| {
        let roles = std::rc::Rc::clone(&cx.state().roles);
        cx.callable(a[0][0], &roles)
    }),
    function!(Flow, "declared", [one, one, one], |cx, a| {
        let (owner, Some(&Val::Sym(name)), Some(&Val::Sym(role))) =
            (node(cx, a, 0), a[1].first(), a[2].first())
        else {
            return Vals::new();
        };
        let defs = crate::tree::members_named(owner)
            .filter(|&(n, _)| n == name)
            .map(|(_, def)| owner.jump(owner.fi(), def));
        let roles = std::rc::Rc::clone(&cx.state().roles);
        let role = role_of(role);
        let found =
            crate::tree::find_member_in(owner, defs, |d| roles.has(d, role), |d| roles.both(d));
        of(found.map(|node| Loc::new(owner.fi() as usize, node)))
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
    function!(Pure, "role", [one], |cx, a| {
        let role = role_code(site_role(node(cx, a, 0)));
        one(Val::Sym(role))
    }),
    function!(Pure, "known", [set], |_, a| match a[0].is_empty() {
        true => one(Val::Opaque),
        false => a[0].clone(),
    }),
    function!(Flow, "lub", [set], @uses ["core"."declared"], |cx, a| {
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
    function!(Pure, "sourced", [one, set, one], |cx, a| {
        let source = arg(cx, a, 0)
            .and_then(|name| name.parent())
            .and_then(|import| import.child_sym(C::SourcePath));
        let listed = source.is_some_and(|s| a[1].iter().any(|&v| cx.text(v) == Some(s)));
        match listed == crate::engine::truthy(&a[2]) {
            true => a[0].clone(),
            false => Vals::new(),
        }
    }),
    function!(Pure, "sole", [set, one], |cx, a| {
        let exclude = Loc::from(node(cx, a, 1));
        match a[0].as_slice() {
            [v] if matches!(cx.view(*v), Some(Ref::Def(t) | Ref::Import(t)) if t != exclude) => {
                a[0].clone()
            }
            _ => Vals::new(),
        }
    }),
    function!(Pure, "type_node", [one], |cx, a| match cx.view(a[0][0]) {
        Some(Ref::Call(n)) => of(values::declared_type(cx.jump(n))),
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
            matches!(r, Ref::Def(n) if cx.jump(n).is_class())
        })),
    function!(Pure, "is_contract", [one], |cx, a| cx
        .keep(a[0][0], |r, cx| {
            matches!(r, Ref::Def(n) if cx.jump(n).is_dispatch_contract())
        })),
    function!(Pure, "initialized", [one], |cx, a| {
        cx.keep(a[0][0], |r, cx| {
        matches!(r, Ref::Def(n) if cx.jump(n).initializer().and_then(Cursor::rhs_callee).is_some())
    })
    }),
    function!(Pure, "typed_calls", [one], |cx, a| cx
        .keep(a[0][0], |r, cx| {
            matches!(r, Ref::Call(n) if values::declared_type(cx.jump(n)).is_some())
        })),
    function!(Pure, "callable_import", [one, set], |cx, a| {
        let Some(callable) = a[1].first().and_then(|&v| cx.text(v)) else {
            return Vals::new();
        };
        cx.keep(a[0][0], |r, cx| {
            matches!(r, Ref::Import(n) if cx.jump(n).parent().is_some_and(|i| i.has_tag(callable)))
        })
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
    function!(Flow, "targets", [one, one], @uses ["core"."read", "core"."declared"], |cx, a| {
        let roles = std::rc::Rc::clone(&cx.state().roles);
        cx.call_targets(node(cx, a, 0), crate::engine::truthy(&a[1]), &roles)
    }),
    function!(Flow, "unresolved", [one, one], @uses ["core"."read", "core"."declared"], |cx, a| {
        let call = node(cx, a, 0);
        let roles = std::rc::Rc::clone(&cx.state().roles);
        match cx.unresolved(call, crate::engine::truthy(&a[1]), &roles) {
            true => one(Val::Node(call.into())),
            false => Vals::new(),
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
