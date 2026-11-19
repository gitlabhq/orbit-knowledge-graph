mod names;
pub mod ops;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Function, Module, Val, Vals, of, truthy, yes};
use crate::modules::{arg, function, node};
use crate::resolve::CLASS_LIKE;
use crate::tree::{CallResolution, EdgeKind, Role};
use ops::Members;

pub struct Core;

/// What the link scope knows about types: defs by name, each def's
/// supertypes, the calls whose callee a local shadows, and member indexes.
pub struct State {
    defs: FxHashMap<u32, Vec<u32>>,
    supers: FxHashMap<u32, Vec<u32>>,
    shadowed: FxHashSet<u32>,
    ivars: FxHashMap<u32, FxHashMap<u32, u32>>,
    members: Members,
}

impl Default for State {
    fn default() -> Self {
        Self {
            defs: FxHashMap::default(),
            supers: FxHashMap::default(),
            shadowed: FxHashSet::default(),
            ivars: FxHashMap::default(),
            members: Members::new(1),
        }
    }
}

enum Ref {
    Def(u32),
    Import(u32),
    Type(u32),
    Call(u32),
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

fn enclosing_class(c: Cursor<'_>) -> Option<Cursor<'_>> {
    Some(c)
        .filter(|c| c.is_class())
        .or_else(|| c.enclosing_def(CLASS_LIKE))
}

fn binding_type(v: Cursor<'_>) -> Option<Cursor<'_>> {
    match v.is(C::Binding) {
        true => v.typed(),
        false => v.child(C::Callee).filter(|_| v.is(C::Call)),
    }
}

fn producer_of(binding: Cursor) -> u32 {
    let call = binding.child(C::Rhs).and_then(Cursor::rhs_call);
    let member_call = call
        .filter(|_| !binding.has(C::SsaTyped))
        .filter(|c| c.child(C::Callee).is_some_and(|k| k.has(C::Member)));
    member_call.map_or(binding.index(), |c| c.index())
}

fn field_value(d: Cursor<'_>) -> Option<Val> {
    let binding = d.child(C::Binding).filter(|b| b.typed().is_some())?;
    let stored = d.has(C::FieldDef) || d.has(C::Property);
    stored.then(|| {
        Val::Node(crate::engine::Loc {
            fi: d.fi(),
            node: producer_of(binding),
        })
    })
}

impl<'t> Cx<'_, 't, Core> {
    fn view(&self, val: Val) -> Option<Ref> {
        match val {
            Val::Type(loc) => Some(Ref::Type(loc.node)),
            Val::Node(loc) => Some(match self.jump(loc).kind() {
                k if k == C::Def => Ref::Def(loc.node),
                k if k == C::Name => Ref::Import(loc.node),
                _ => Ref::Call(loc.node),
            }),
            _ => None,
        }
    }

    fn is_name(&self, val: Val) -> bool {
        matches!(
            self.view(val),
            Some(Ref::Def(_) | Ref::Import(_) | Ref::Type(_))
        )
    }

    fn ssa(&mut self, id: crate::engine::FnId, c: Cursor) -> Vals {
        self.call1(id, Val::Node(c.into()))
    }

    fn enclosing_class(&mut self) -> Option<Cursor<'t>> {
        let enclosing = self.enclosing();
        enclosing_class(self.at(enclosing))
    }

    fn ivar(&mut self, attr: u32) -> Option<Cursor<'t>> {
        let class = self.enclosing_class()?;
        let at = move |node| class.jump(class.fi(), node);
        let index = self.state().ivars.entry(class.index()).or_insert_with(|| {
            let mut index = FxHashMap::default();
            for n in class.descendants() {
                if n.is(C::Binding)
                    && let Some(iv) = n.child_sym(C::Ivar)
                    && n.typed().is_some()
                {
                    index.entry(iv).or_insert(n.index());
                }
            }
            index
        });
        index.get(&attr).map(|&n| at(n))
    }

    fn define(&mut self, def: Cursor<'_>) {
        let Some(name) = def.child_sym(C::DefName) else {
            return;
        };
        if !self
            .state()
            .defs
            .get(&name)
            .is_some_and(|d| d.contains(&def.index()))
        {
            self.write().defs.entry(name).or_default().push(def.index());
        }
    }

    fn shadow(&mut self, c: Cursor<'t>) {
        let Some(callee) = c.child(C::Callee) else {
            return;
        };
        let root = callee
            .child(C::Member)
            .and_then(|member| member.child(C::Object))
            .map_or(callee, |object| object.chain_root());
        if callee.has(C::Ivar) || root.is(C::Call) || root.value_child(C::Call).is_some() {
            return;
        }
        let found = self.ssa(crate::fn_id!("ssa", "read"), root);
        if !found.iter().all(|&v| self.is_name(v)) {
            // No epoch bump: only `finish` reads this set.
            self.state().shadowed.insert(c.index());
        }
    }

    fn callable(&mut self, target: Val) -> Vals {
        match self.view(target) {
            Some(Ref::Type(n)) => {
                let class = self.at(n);
                match class.child_sym(C::Callable) {
                    Some(name) => of(ops::members(self, class, name, Role::Callable)),
                    None => of([class]),
                }
            }
            Some(Ref::Def(n)) if self.at(n).has_tag(self.tags().callable) => of([self.at(n)]),
            _ => Vals::new(),
        }
    }
}

impl Module for Core {
    const NAME: &'static str = "core";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }

    fn finish(cx: &mut Cx<'_, '_, Self>) {
        let (tree, wildcard) = (
            cx.tree(),
            cx.env().lang.syms.lookup(crate::constants::WILDCARD),
        );
        let config = &cx.env().rules_for(&tree.label).config.link;
        let provided = |site: Option<u32>, to: u32| {
            let Some(name) = site.and_then(|site| tree.cursor(site).child_sym(C::Callee)) else {
                return false;
            };
            let source = tree
                .cursor(to)
                .parent()
                .and_then(|import| import.child_sym(C::SourcePath));
            config.builtins.contains(&name)
                || source.is_some_and(|source| {
                    config
                        .providers
                        .iter()
                        .any(|(module, symbols)| *module == source && symbols.contains(&name))
                })
        };
        let shadowed = std::mem::take(&mut cx.state().shadowed);
        for edge in cx.edges_since(0) {
            if edge.kind == EdgeKind::TypeFlow && shadowed.contains(&edge.to_node) {
                edge.call_resolution = CallResolution::NonCallable;
            }
            let target = tree.cursor(edge.to_node);
            if edge.kind == EdgeKind::Imports
                && (target.sym() == wildcard || target.child_sym(C::SsaHint) == Some(wildcard))
                && provided(edge.site, edge.to_node)
            {
                edge.call_resolution = CallResolution::Reference;
            }
        }
    }
}

static FUNCTIONS: &[Function<Core>] = &[
    function!(Pure, "named", [one], |cx, a| keep(arg(cx, a, 0), |c| c
        .sym_opt()
        .is_some())),
    function!(Pure, "child", [one, one], |cx, a| {
        let k = kind(a, 1);
        of(arg(cx, a, 0).and_then(|c| c.children().find(|ch| ch.kind() == k)))
    }),
    function!(Pure, "child_all", [one, one], |cx, a| {
        let k = kind(a, 1);
        of(arg(cx, a, 0)
            .into_iter()
            .flat_map(|c| c.children())
            .filter(|ch| ch.kind() == k))
    }),
    function!(Pure, "without", [one, one], |cx, a| {
        let k = kind(a, 1);
        of(arg(cx, a, 0)
            .into_iter()
            .flat_map(|c| c.children())
            .filter(|ch| ch.kind() != k))
    }),
    function!(Pure, "lacks", [one, one], |cx, a| {
        let k = kind(a, 1);
        keep(arg(cx, a, 0), |c| !c.children().any(|ch| ch.kind() == k))
    }),
    function!(Pure, "tagged", [one, one], |cx, a| {
        let Some(&Val::Sym(tag)) = a[1].first() else {
            return Vals::new();
        };
        keep(arg(cx, a, 0), |c| c.has_tag(tag))
    }),
    function!(Pure, "not", [set], |_, a| yes(!truthy(&a[0]))),
    function!(Pure, "if_any", [set, set], |_, a| match a[1].is_empty() {
        true => Vals::new(),
        false => a[0].clone(),
    }),
    function!(Pure, "is_class", [one], |cx, a| keep(arg(cx, a, 0), |c| c
        .is(C::Def)
        && c.is_class())),
    function!(Pure, "is_contract", [one], |cx, a| {
        keep(arg(cx, a, 0), |c| c.is(C::Def) && c.is_dispatch_contract())
    }),
    function!(Pure, "impl_name", [one], |cx, a| {
        keep(arg(cx, a, 0), |c| {
            c.parent().is_some_and(|p| p.has(C::ImplBlock))
        })
    }),
    function!(Pure, "chain_head", [one], |cx, a| {
        of(arg(cx, a, 0)
            .filter(|c| c.value_child(C::Member).is_some())
            .map(|c| c.chain_root()))
    }),
    function!(Pure, "member_of", [one], |cx, a| of(
        arg(cx, a, 0).and_then(|c| c.child(C::Member))
    )),
    function!(Pure, "body", [one], |cx, a| {
        let Some(c) = arg(cx, a, 0).filter(|c| c.has_tag(cx.tags().scoped)) else {
            return Vals::new();
        };
        let initializer = c.initializer().filter(|_| c.is_class()).map(Cursor::index);
        of(c.children()
            .filter(|node| Some(node.index()) != initializer))
    }),
    function!(Pure, "bare_rhs", [one], |cx, a| of(
        arg(cx, a, 0).and_then(Cursor::bare_rhs)
    )),
    function!(Pure, "imported", [one], |cx, a| match cx.view(a[0][0]) {
        Some(Ref::Import(_)) => a[0].clone(),
        _ => Vals::new(),
    }),
    function!(Pure, "produced", [one], |cx, a| match cx.view(a[0][0]) {
        Some(Ref::Call(_)) => a[0].clone(),
        _ => Vals::new(),
    }),
    function!(Pure, "initialized", [one], |cx, a| match cx.view(a[0][0]) {
        Some(Ref::Def(n))
            if cx
                .at(n)
                .initializer()
                .and_then(Cursor::rhs_callee)
                .is_some() =>
        {
            a[0].clone()
        }
        _ => Vals::new(),
    }),
    function!(Flow, "callable", [one], |cx, a| cx.callable(a[0][0])),
    function!(Flow, "enclosing", [], |cx, _| {
        let node = cx.enclosing();
        of([cx.at(node)])
    }),
    function!(Flow, "enclosing_def", [], |cx, _| {
        let def = cx.enclosing_def();
        of(def.map(|node| cx.at(node)))
    }),
    function!(Flow, "class", [one], |cx, a| of(
        arg(cx, a, 0).and_then(enclosing_class)
    )),
    function!(Flow, "ivar", [one], |cx, a| {
        let found = cx.ivar(node(cx, a, 0).sym());
        of(found.map(|binding| cx.at(producer_of(binding))))
    }),
    function!(Flow, "name", [one], |cx, a| {
        let mut found = cx.ssa(crate::fn_id!("ssa", "read"), node(cx, a, 0));
        found.retain(|v| *v != Val::Opaque);
        found
    }),
    function!(Flow, "type", [one], |cx, a| {
        let mut found = ops::chain(cx, node(cx, a, 0));
        found.retain(|v| *v != Val::Opaque);
        found
    }),
    function!(Flow, "member", [one, one], |cx, a| {
        let Some(m) = arg(cx, a, 1) else {
            return Vals::new();
        };
        cx.member(a[0][0], m)
    }),
    function!(Flow, "unmatched", [one, one], |cx, a| {
        let Some(m) = arg(cx, a, 1) else {
            return Vals::new();
        };
        cx.unmatched(a[0][0], m)
    }),
    function!(Flow, "members", [one, one, one], |cx, a| {
        let (Some(owner), Some(&Val::Sym(name)), Some(site)) =
            (arg(cx, a, 0), a[1].first(), arg(cx, a, 2))
        else {
            return Vals::new();
        };
        of(ops::members(cx, owner, name, site.role()))
    }),
    function!(Flow, "supers", [one], |cx, a| {
        let def = node(cx, a, 0).index();
        let supers = cx.state().supers.get(&def).cloned().unwrap_or_default();
        of(supers.into_iter().map(|node| cx.at(node)))
    }),
    function!(Flow, "reference", [one], |cx, a| cx
        .reference(node(cx, a, 0))),
    function!(Flow, "receiver_calls", [one], |cx, a| {
        let receiver = arg(cx, a, 0)
            .and_then(|m| m.child(C::Dispatch))
            .and_then(|d| d.child(C::Object));
        let Some(receiver) = receiver else {
            return Vals::new();
        };
        let mut found = cx.ssa(crate::fn_id!("ssa", "read"), receiver);
        found.retain(|v| matches!(cx.view(*v), Some(Ref::Call(_))));
        found
    }),
    function!(Flow, "typed_calls", [one], |cx, a| match cx.view(a[0][0]) {
        Some(Ref::Call(n)) if binding_type(cx.at(n)).is_some() => a[0].clone(),
        _ => Vals::new(),
    }),
    function!(
        Flow,
        "named_member",
        [one, one, unkeyed, unkeyed, unkeyed],
        |cx, a| {
            let c = node(cx, a, 2);
            cx.member_of_object(c, node(cx, a, 3), node(cx, a, 4))
        }
    ),
    function!(Flow, "standalone", [one], |cx, a| {
        let member = node(cx, a, 0);
        let in_chain = member.parent().is_some_and(|parent| {
            parent.is(C::Callee)
                || parent.is(C::Object) && parent.parent().is_some_and(|owner| owner.is(C::Member))
        });
        match in_chain || !cx.ssa(crate::fn_id!("ssa", "field"), member).is_empty() {
            true => Vals::new(),
            false => of([member]),
        }
    }),
    function!(Flow, "callee_targets", [one], |cx, a| {
        let mut found = cx.callee_targets(node(cx, a, 0));
        found.retain(|v| *v != Val::Opaque);
        found
    }),
    function!(Effect, "define", [one], |cx, a| {
        cx.define(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "inherit", [one, set], |cx, a| {
        let def = node(cx, a, 0).index();
        let supers = a[1]
            .iter()
            .filter_map(|v| match cx.view(*v) {
                Some(Ref::Def(d)) => Some(d),
                _ => None,
            })
            .collect();
        cx.write().supers.insert(def, supers);
        yes(true)
    }),
    function!(Effect, "shadow", [one], |cx, a| {
        cx.shadow(node(cx, a, 0));
        yes(true)
    }),
];
