mod chain;
pub mod ops;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Function, KeyPart, Module, Val, Vals, of, one, yes};
use crate::modules::{arg, function, node};
use crate::tree::CLASS_LIKE;
use crate::tree::Role;

pub struct Core;

const NON_CALLABLE: &str = "non_callable";
const REFERENCE: &str = "reference";

#[derive(Default)]
pub struct State {
    defs: FxHashMap<u32, Vec<u32>>,
    supers: FxHashMap<u32, Vec<u32>>,
    shadowed: FxHashSet<u32>,
}

enum Ref {
    Def(u32),
    Import(u32),
    Type(u32),
    Call(u32),
}

fn keep<'t>(c: Option<Cursor<'t>>, pred: impl FnOnce(Cursor<'t>) -> bool) -> Vals {
    of(c.filter(|&c| pred(c)))
}

pub(crate) fn role_code(role: Role) -> u32 {
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

fn member_role(member: Cursor) -> u32 {
    role_code(role(member))
}

fn sym(c: Cursor) -> u32 {
    c.sym()
}

fn object_name(member: Cursor) -> u32 {
    member
        .child(C::Object)
        .map_or(0, |object| object.reference().sym())
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
    stored.then(|| Val::Node(producer_of(binding).into_loc(d)))
}

trait IntoLoc {
    fn into_loc(self, at: Cursor) -> crate::engine::Loc;
}

impl IntoLoc for u32 {
    fn into_loc(self, at: Cursor) -> crate::engine::Loc {
        crate::engine::Loc {
            fi: at.fi(),
            node: self,
        }
    }
}

/// The branch whose value `c` gives: the branch of an arm, or the branch
/// an arm ends in through nodes that stand for their last child.
fn sink_step(c: Cursor<'_>) -> Result<Cursor<'_>, Option<Cursor<'_>>> {
    if c.is(C::SsaArm) {
        let tail = c.tail_expr();
        if tail.is(C::SsaBranch) || tail.is(C::SsaReturn) {
            return Err(None);
        }
        return c.parent().filter(|p| p.is(C::SsaBranch)).ok_or(None);
    }
    if !c.is(C::SsaBranch) {
        return Err(None);
    }
    let first = c.parent().ok_or(None)?;
    if first.is(C::Rhs) {
        let direct = first
            .child(C::SsaBranch)
            .is_some_and(|b| b.index() == c.index());
        return Err(first.parent().filter(|b| direct && b.is(C::Binding)));
    }
    let (mut up, mut below) = (first, c);
    while !up.is(C::SsaArm) {
        let tail = up.last_named().filter(|t| up.stands_for(*t));
        if tail.is_none_or(|t| t.index() != below.index()) {
            return Err(None);
        }
        (below, up) = (up, up.parent().ok_or(None)?);
    }
    match up.tail_expr().index() == c.index() {
        true => up.parent().filter(|p| p.is(C::SsaBranch)).ok_or(None),
        false => Err(None),
    }
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

    fn read(&mut self, c: Cursor) -> Vals {
        self.call1(crate::fn_id!("core", "read"), Val::Node(c.into()))
    }

    fn slot(&mut self, c: Cursor) -> bool {
        !self
            .call1(crate::fn_id!("core", "slot"), Val::Node(c.into()))
            .is_empty()
    }

    fn type_of(&mut self, c: Cursor) -> Vals {
        self.call1(crate::fn_id!("core", "type"), Val::Node(c.into()))
    }

    fn enclosing_class(&mut self) -> Option<Cursor<'t>> {
        let enclosing = self.enclosing();
        enclosing_class(self.at(enclosing))
    }

    fn ivar(&mut self, c: Cursor<'t>) -> Vals {
        let Some(class) = self.enclosing_class() else {
            return Vals::new();
        };
        let args = [one(Val::Node(class.into())), one(Val::Sym(c.sym()))];
        self.call(crate::fn_id!("core", "ivar_binding"), &args)
    }

    fn value_of(&mut self, t: Cursor<'t>) -> Vals {
        if C::try_from_u16(t.kind()).is_some_and(C::is_opaque_value) {
            return one(Val::Opaque);
        }
        if t.is(C::Call) {
            return of([t]);
        }
        if t.is(C::Ivar) {
            let found = self.ivar(t);
            return match found.is_empty() {
                true => one(Val::Opaque),
                false => found,
            };
        }
        Vals::new()
    }

    fn record(&mut self, t: Cursor<'t>) -> Option<Cursor<'t>> {
        if let Some(record) = t.is(C::Obj).then_some(t).or_else(|| t.child(C::Obj)) {
            return Some(record);
        }
        let class = t
            .child(C::Callee)
            .is_some_and(|callee| self.is_class_ref(callee));
        t.child(C::Args).filter(|_| class)
    }

    fn assigned(&mut self, binding: Cursor<'t>) -> Vals {
        let tail = binding.child(C::Rhs).map(Cursor::tail_expr);
        let reads_import = binding.bare_rhs().is_some_and(|rhs| {
            let found = self.read(rhs);
            found
                .iter()
                .any(|v| matches!(self.view(*v), Some(Ref::Import(_))))
        });
        let unslotted_member = tail.is_some_and(|t| t.is(C::Member) && !self.slot(t));
        if binding.has(C::SsaTyped) || reads_import || unslotted_member {
            return of([binding]);
        }
        match tail {
            Some(t) => self.value_of(t),
            None => one(Val::Opaque),
        }
    }

    fn arm_value(&mut self, arm: Cursor<'t>) -> Vals {
        let mut found = self.value_of(arm.tail_expr());
        found.retain(|v| *v != Val::Opaque);
        found
    }

    fn arm_target(&mut self, arm: Cursor<'t>) -> Vals {
        let tail = arm.tail_expr();
        let valued = !self.arm_value(arm).is_empty()
            || self.value_of(tail).is_empty() && self.read(tail).iter().any(|v| *v != Val::Opaque);
        match valued {
            true => self.call1(crate::fn_id!("core", "sink"), Val::Node(arm.into())),
            false => Vals::new(),
        }
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
        let found = self.read(root);
        if !found.iter().all(|&v| self.is_name(v)) {
            self.write().shadowed.insert(c.index());
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

    fn resolution(&mut self, target: Cursor<'t>, site: Option<Cursor<'t>>) -> Vals {
        let syms = &self.env().lang.syms;
        if !target.is(C::Def) && !target.is(C::Name) {
            return match self.state().shadowed.contains(&target.index()) {
                true => one(Val::Sym(syms.intern(NON_CALLABLE))),
                false => Vals::new(),
            };
        }
        let wildcard = syms.lookup(crate::constants::WILDCARD);
        let wild = target.sym() == wildcard || target.child_sym(C::SsaHint) == Some(wildcard);
        let callee = site.and_then(|site| site.child_sym(C::Callee));
        let (Some(name), true) = (callee, wild) else {
            return Vals::new();
        };
        let link = &self.env().rules_for(&self.tree().label).config.link;
        let source = target
            .parent()
            .and_then(|import| import.child_sym(C::SourcePath));
        let provided = link.builtins.contains(&name)
            || source.is_some_and(|source| {
                link.providers
                    .iter()
                    .any(|(module, symbols)| *module == source && symbols.contains(&name))
            });
        match provided {
            true => one(Val::Sym(syms.intern(REFERENCE))),
            false => Vals::new(),
        }
    }
}

impl Module for Core {
    const NAME: &'static str = "core";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }

    fn start<'t>(cx: &mut Cx<'_, 't, Self>, root: Cursor<'t>) {
        for def in root
            .children()
            .filter(|d| d.is(C::Def) && d.child(C::Declaration).is_none())
        {
            cx.define(def);
        }
    }
}

static FUNCTIONS: &[Function<Core>] = &[
    function!(required Flow, "read", [one]),
    function!(required Flow, "slot", [one]),
    function!(Pure, "tail", [one], |cx, a| of(
        arg(cx, a, 0).map(Cursor::tail_expr)
    )),
    function!(Pure, "sink", [one], @uses ["core"."sink"], |cx, a| match sink_step(node(cx, a, 0)) {
        Ok(next) => cx.call1(crate::fn_id!("core", "sink"), Val::Node(next.into())),
        Err(binding) => of(binding),
    }),
    function!(Pure, "chain_head", [one], |cx, a| of(arg(cx, a, 0)
        .filter(|c| c.value_child(C::Member).is_some())
        .map(|c| c.chain_root()))),
    function!(Pure, "binding_site", [one], |cx, a| {
        let site = node(cx, a, 0)
            .ancestors()
            .take_while(|up| !up.is(C::Def))
            .filter(|up| up.is(C::Binding))
            .find(|binding| {
                binding.child(C::Rhs).is_some_and(|rhs| {
                    rhs.child(C::SsaBranch).is_none() && rhs.value_child(C::Member).is_some()
                })
            });
        of(site)
    }),
    function!(Pure, "tagged", [one, one], |cx, a| {
        let Some(&Val::Sym(tag)) = a[1].first() else {
            return Vals::new();
        };
        keep(arg(cx, a, 0), |c| c.has_tag(tag))
    }),
    function!(Pure, "plain_declaration", [one], |cx, a| keep(
        arg(cx, a, 0),
        |c| { c.has(C::Declaration) && c.child_sym(C::Declaration).is_none() }
    )),
    function!(Pure, "initializer", [one], |cx, a| keep(
        arg(cx, a, 0),
        |c| {
            c.parent().is_some_and(|class| {
                class.is_class() && class.initializer().is_some_and(|i| i.index() == c.index())
            })
        }
    )),
    function!(Pure, "outermost", [one], |cx, a| keep(arg(cx, a, 0), |c| {
        !c.ancestors().any(|up| up.is(C::SsaLoop))
    })),
    function!(Pure, "unlifted", [one], |cx, a| keep(arg(cx, a, 0), |c| !c
        .lifted())),
    function!(Pure, "is_class", [one], |cx, a| keep(arg(cx, a, 0), |c| c
        .is(C::Def)
        && c.is_class())),
    function!(Pure, "is_contract", [one], |cx, a| keep(
        arg(cx, a, 0),
        |c| c.is(C::Def) && c.is_dispatch_contract()
    )),
    function!(Pure, "is_def", [one], |cx, a| keep(arg(cx, a, 0), |c| c
        .is(C::Def))),
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
    function!(Pure, "typed_calls", [one], |cx, a| match cx.view(a[0][0]) {
        Some(Ref::Call(n)) if binding_type(cx.at(n)).is_some() => a[0].clone(),
        _ => Vals::new(),
    }),
    function!(Pure, "callable_import", [one], |cx, a| {
        let callable = cx.tags().callable;
        keep(arg(cx, a, 0), |c| {
            c.parent().is_some_and(|import| import.has_tag(callable))
        })
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
        let tags = cx.tags();
        let found = crate::tree::find_member_in(owner, defs, role_of(role), &tags);
        of(found.map(|node| owner.jump(owner.fi(), node)))
    }),
    function!(Pure, "ivar_binding", [one, one], |cx, a| {
        let (class, Some(&Val::Sym(attr))) = (node(cx, a, 0), a[1].first()) else {
            return Vals::new();
        };
        let binding = class.descendants().find(|n| {
            n.is(C::Binding) && n.child_sym(C::Ivar) == Some(attr) && n.typed().is_some()
        });
        of(binding.map(|b| cx.at(producer_of(b))))
    }),
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
    function!(Flow, "source", [one], @uses ["core"."type"], |cx, a| {
        let t = node(cx, a, 0);
        of([cx.record(t).unwrap_or(t)])
    }),
    function!(Flow, "assigned", [one], @uses ["core"."read", "core"."slot", "core"."ivar_binding"], |cx, a| {
        cx.assigned(node(cx, a, 0))
    }),
    function!(Flow, "arm_value", [one], @uses ["core"."ivar_binding"], |cx, a| {
        cx.arm_value(node(cx, a, 0))
    }),
    function!(Flow, "arm_target", [one], @uses ["core"."read", "core"."sink", "core"."ivar_binding"], |cx, a| {
        cx.arm_target(node(cx, a, 0))
    }),
    function!(Flow, "callable", [one], @uses ["core"."declared"], |cx, a| cx.callable(a[0][0])),
    function!(Flow, "name", [one], @uses ["core"."read"], |cx, a| {
        let mut found = cx.read(node(cx, a, 0));
        found.retain(|v| *v != Val::Opaque);
        found
    }),
    function!(Flow, "type", [one], @uses ["core"."read", "core"."type", "core"."named_member", "core"."ivar_binding", "core"."declared"], |cx, a| {
        let mut found = ops::chain(cx, node(cx, a, 0));
        found.retain(|v| *v != Val::Opaque);
        found
    }),
    function!(Flow, "member", [one, one], @key [KeyPart::Arg(0), KeyPart::Of(1, sym), KeyPart::Of(1, member_role)], @uses ["core"."type", "core"."member", "core"."declared"], |cx, a| {
        let Some(m) = arg(cx, a, 1) else {
            return Vals::new();
        };
        cx.member(a[0][0], m)
    }),
    function!(Flow, "unmatched", [one, one], @key [KeyPart::Arg(0), KeyPart::Of(1, sym), KeyPart::Of(1, member_role)], @uses ["core"."declared"], |cx, a| {
        let Some(m) = arg(cx, a, 1) else {
            return Vals::new();
        };
        cx.unmatched(a[0][0], m)
    }),
    function!(Flow, "members", [one, one, one], @uses ["core"."declared"], |cx, a| {
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
    function!(Flow, "reference", [one], @uses ["core"."type"], |cx, a| cx.reference(node(cx, a, 0))),
    function!(Flow, "receiver_calls", [one], @uses ["core"."read"], |cx, a| {
        let receiver = arg(cx, a, 0)
            .and_then(|m| m.child(C::Dispatch))
            .and_then(|d| d.child(C::Object));
        let Some(receiver) = receiver else {
            return Vals::new();
        };
        let mut found = cx.read(receiver);
        found.retain(|v| matches!(cx.view(*v), Some(Ref::Call(_))));
        found
    }),
    function!(
        Flow,
        "named_member",
        [one, one],
        @key [KeyPart::Of(0, object_name), KeyPart::Of(0, sym)],
        @uses ["core"."type", "core"."declared"],
        |cx, a| {
            let member = node(cx, a, 0);
            let object = member.child(C::Object).expect("a chain member has an object");
            cx.member_of_object(node(cx, a, 1), member, object)
        }
    ),
    function!(Flow, "targets", [one], @uses ["core"."read", "core"."declared"], |cx, a| cx.targets(node(cx, a, 0))),
    function!(Flow, "unresolved", [one], @uses ["core"."read", "core"."declared"], |cx, a| {
        let call = node(cx, a, 0);
        match cx.unresolved(call) {
            true => of([call]),
            false => Vals::new(),
        }
    }),
    function!(Flow, "resolution", [one, set], |cx, a| {
        let target = node(cx, a, 0);
        cx.resolution(target, arg(cx, a, 1))
    }),
    function!(Effect, "define", [one], |cx, a| {
        cx.define(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "inherit", [one, set], |cx, a| {
        let def = node(cx, a, 0).index();
        let supers: Vec<u32> = a[1]
            .iter()
            .filter_map(|v| match cx.view(*v) {
                Some(Ref::Def(d)) => Some(d),
                _ => None,
            })
            .collect();
        let entry = cx.write().supers.entry(def).or_default();
        for s in supers {
            if !entry.contains(&s) {
                entry.push(s);
            }
        }
        yes(true)
    }),
    function!(Effect, "shadow", [one], @uses ["core"."read"], |cx, a| {
        cx.shadow(node(cx, a, 0));
        yes(true)
    }),
];
