mod names;
pub mod ops;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Function, KeyPart, Module, Val, Vals, of, one, truthy, yes};
use crate::modules::{arg, function, node};
use crate::resolve::CLASS_LIKE;
use crate::tree::Role;

pub struct Core;

const NON_CALLABLE: &str = "non_callable";
const REFERENCE: &str = "reference";

/// What the link scope knows about types: defs by name, each def's
/// supertypes, and the calls whose callee a local shadows.
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

fn kind(args: &[Vals], at: usize) -> u16 {
    match args[at].first() {
        Some(Val::Kind(kind)) => *kind,
        _ => 0,
    }
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

    fn read(&mut self, c: Cursor) -> Vals {
        self.call1(crate::fn_id!("core", "read"), Val::Node(c.into()))
    }

    fn type_of(&mut self, c: Cursor) -> Vals {
        self.call1(crate::fn_id!("core", "type"), Val::Node(c.into()))
    }

    fn enclosing_class(&mut self) -> Option<Cursor<'t>> {
        let enclosing = self.enclosing();
        enclosing_class(self.at(enclosing))
    }

    /// The producer of the typed binding of instance variable `c` in the
    /// enclosing class.
    fn ivar(&mut self, c: Cursor<'t>) -> Vals {
        let Some(class) = self.enclosing_class() else {
            return Vals::new();
        };
        let args = [one(Val::Node(class.into())), one(Val::Sym(c.sym()))];
        self.call(crate::fn_id!("core", "ivar_binding"), &args)
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
}

fn loop_head(c: Cursor<'_>) -> usize {
    c.children()
        .position(|node| node.is(C::Binding) && node.has(C::Declaration) && !node.lifted())
        .unwrap_or(0)
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
    function!(Pure, "if_true", [set, set], |_, a| match truthy(&a[1]) {
        true => a[0].clone(),
        false => Vals::new(),
    }),
    function!(Pure, "or", [set, set], |_, a| match a[0].is_empty() {
        true => a[1].clone(),
        false => a[0].clone(),
    }),
    function!(Pure, "opaque", [set], |_, a| match a[0].is_empty() {
        true => Vals::new(),
        false => one(Val::Opaque),
    }),
    function!(Pure, "not_opaque", [set], |_, a| {
        a[0].iter().copied().filter(|v| *v != Val::Opaque).collect()
    }),
    function!(Pure, "is", [one, one], |cx, a| {
        let k = kind(a, 1);
        keep(arg(cx, a, 0), |c| c.kind() == k)
    }),
    function!(Pure, "is_def", [one], |cx, a| match a[0][0] {
        Val::Node(loc) if cx.jump(loc).is(C::Def) => a[0].clone(),
        _ => Vals::new(),
    }),
    function!(Pure, "is_declaration", [one], |cx, a| keep(
        arg(cx, a, 0),
        |c| { c.has(C::Declaration) && c.child_sym(C::Declaration).is_none() }
    )),
    function!(Pure, "parent", [one], |cx, a| of(
        arg(cx, a, 0).and_then(|c| c.parent())
    )),
    function!(Pure, "children", [one], |cx, a| of(arg(cx, a, 0)
        .into_iter()
        .flat_map(|c| c.children()))),
    function!(Pure, "tail", [one], |cx, a| of(
        arg(cx, a, 0).map(Cursor::tail_expr)
    )),
    function!(Pure, "rhs_tail", [one], |cx, a| of(arg(cx, a, 0)
        .and_then(|c| c.child(C::Rhs))
        .map(Cursor::tail_expr))),
    function!(Pure, "rhs_order", [one], |cx, a| {
        let c = node(cx, a, 0);
        let Some(rhs) = c.child(C::Rhs) else {
            return Vals::new();
        };
        let calls = c
            .children()
            .take_while(|child| !child.is(C::Rhs))
            .filter(|child| child.is(C::Call));
        match rhs.child(C::SsaBranch) {
            Some(branch) => {
                let effects = rhs.children().filter(|child| child.is(C::SideEffect));
                of(std::iter::once(branch).chain(effects).chain(calls))
            }
            None => of(rhs.children().chain(calls)),
        }
    }),
    function!(
        Pure,
        "sink",
        [one], @uses ["core"."sink"],
        |cx, a| match sink_step(node(cx, a, 0)) {
            Ok(next) => cx.call1(crate::fn_id!("core", "sink"), Val::Node(next.into())),
            Err(binding) => of(binding),
        }
    ),
    function!(Pure, "loop_elements", [one], |cx, a| {
        let c = node(cx, a, 0);
        let iterable = c
            .parent()
            .filter(|parent| parent.is(C::SsaLoop))
            .and_then(|parent| {
                parent
                    .children()
                    .take_while(|node| node.index() != c.index())
                    .find(|node| node.is(C::Arr))
            });
        of(iterable
            .into_iter()
            .flat_map(|array| array.children())
            .filter(|element| element.is(C::Call)))
    }),
    function!(Pure, "loop_head", [one], |cx, a| {
        let c = node(cx, a, 0);
        of(c.children().take(loop_head(c)))
    }),
    function!(Pure, "loop_body", [one], |cx, a| {
        let c = node(cx, a, 0);
        of(c.children().skip(loop_head(c)))
    }),
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
    function!(Pure, "value_call", [one], |cx, a| of(
        arg(cx, a, 0).and_then(|c| c.value_child(C::Call))
    )),
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
    function!(Flow, "callable", [one], @uses ["core"."declared"], |cx, a| cx.callable(a[0][0])),
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
    function!(Pure, "ivar_binding", [one, one], |cx, a| {
        let (class, Some(&Val::Sym(attr))) = (node(cx, a, 0), a[1].first()) else {
            return Vals::new();
        };
        let binding = class.descendants().find(|n| {
            n.is(C::Binding) && n.child_sym(C::Ivar) == Some(attr) && n.typed().is_some()
        });
        of(binding.map(|b| cx.at(producer_of(b))))
    }),
    function!(Flow, "value_of", [one], @uses ["core"."ivar_binding"], |cx, a| {
        let t = node(cx, a, 0);
        match t {
            t if C::try_from_u16(t.kind()).is_some_and(C::is_opaque_value) => one(Val::Opaque),
            t if t.is(C::Call) => of([t]),
            t if t.is(C::Ivar) => {
                let found = cx.ivar(t);
                if found.is_empty() {
                    one(Val::Opaque)
                } else {
                    found
                }
            }
            _ => Vals::new(),
        }
    }),
    function!(Flow, "record", [one], @uses ["core"."type"], |cx, a| {
        let t = node(cx, a, 0);
        if let Some(record) = t.is(C::Obj).then_some(t).or_else(|| t.child(C::Obj)) {
            return of([record]);
        }
        let class = t
            .child(C::Callee)
            .is_some_and(|callee| cx.is_class_ref(callee));
        of(t.child(C::Args).filter(|_| class))
    }),
    function!(required Flow, "read", [one]),
    function!(required Flow, "slot", [one]),
    function!(required Flow, "wildcards", []),
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
    function!(Flow, "reference", [one], @uses ["core"."type"], |cx, a| cx
        .reference(node(cx, a, 0))),
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
    function!(Flow, "typed_calls", [one], |cx, a| match cx.view(a[0][0]) {
        Some(Ref::Call(n)) if binding_type(cx.at(n)).is_some() => a[0].clone(),
        _ => Vals::new(),
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
    function!(Flow, "standalone", [one], @uses ["core"."slot"], |cx, a| {
        let member = node(cx, a, 0);
        let in_chain = member.parent().is_some_and(|parent| {
            parent.is(C::Callee)
                || parent.is(C::Object) && parent.parent().is_some_and(|owner| owner.is(C::Member))
        });
        let slot = cx.call1(crate::fn_id!("core", "slot"), Val::Node(member.into()));
        match in_chain || !slot.is_empty() {
            true => Vals::new(),
            false => of([member]),
        }
    }),
    function!(Flow, "callee_targets", [one], @uses ["core"."read", "core"."wildcards", "core"."declared"], |cx, a| {
        let mut found = cx.callee_targets(node(cx, a, 0));
        found.retain(|v| *v != Val::Opaque);
        found
    }),
    function!(Flow, "resolution", [one, set], |cx, a| {
        let target = node(cx, a, 0);
        let syms = &cx.env().lang.syms;
        if !target.is(C::Def) && !target.is(C::Name) {
            return match cx.state().shadowed.contains(&target.index()) {
                true => one(Val::Sym(syms.intern(NON_CALLABLE))),
                false => Vals::new(),
            };
        }
        let wildcard = syms.lookup(crate::constants::WILDCARD);
        let wild = target.sym() == wildcard || target.child_sym(C::SsaHint) == Some(wildcard);
        let callee = arg(cx, a, 1).and_then(|site| site.child_sym(C::Callee));
        let (Some(name), true) = (callee, wild) else {
            return Vals::new();
        };
        let link = &cx.env().rules_for(&cx.tree().label).config.link;
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
    function!(Effect, "shadow", [one], @uses ["core"."read"], |cx, a| {
        cx.shadow(node(cx, a, 0));
        yes(true)
    }),
];
