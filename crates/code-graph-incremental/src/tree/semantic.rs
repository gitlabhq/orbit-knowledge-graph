use std::ops::ControlFlow;

use super::{Cursor, Edge, EdgeKind, Node, Step, Storage};
use crate::canonical::Canonical as C;
use crate::resolver::CLASS_LIKE;

impl<'a, S: Storage<Node = Node>> Cursor<'a, S> {
    pub fn kind(self) -> u16 {
        self.node().kind
    }
    pub fn sym(self) -> u32 {
        self.node().sym
    }
    pub fn sym_opt(self) -> Option<u32> {
        Some(self.sym()).filter(|&sym| sym != 0)
    }
    pub fn is(self, kind: C) -> bool {
        self.kind() == kind
    }
    pub fn named(self) -> bool {
        self.node().named
    }
    pub fn field(self) -> u16 {
        self.node().field
    }
    pub fn start(self) -> u32 {
        self.node().start
    }
    pub fn end(self) -> u32 {
        self.node().end
    }
    pub fn start_row(self) -> u32 {
        self.node().start_row
    }
    pub fn start_col(self) -> u32 {
        self.node().start_col
    }
    pub fn end_row(self) -> u32 {
        self.node().end_row
    }
    pub fn end_col(self) -> u32 {
        self.node().end_col
    }
    pub fn child_sym_of_kind(self, kind: u16) -> Option<u32> {
        self.children()
            .find(|node| node.kind() == kind)
            .and_then(|node| node.sym_opt())
    }
    pub fn children_of(self, kind: C) -> impl Iterator<Item = Self> + 'a {
        self.children()
            .filter(move |node| node.is(kind) && node.sym_opt().is_some())
    }
    pub fn last_named(self) -> Option<Self> {
        self.children().filter(|node| node.named()).last()
    }
    pub fn calls(self) -> impl Iterator<Item = Self> + 'a {
        self.descendants().filter(|node| node.is(C::Call))
    }
    pub fn member_calls(self) -> impl Iterator<Item = (Self, Self)> + 'a {
        self.calls()
            .filter_map(|call| Some((call, call.member()?)))
            .filter(|(_, member)| member.sym_opt().is_some())
    }
    pub fn is_class(self) -> bool {
        self.children()
            .any(|child| CLASS_LIKE.iter().any(|&kind| child.kind() == kind))
    }
    pub fn is_dispatch_contract(self) -> bool {
        self.has(C::Trait) || self.has(C::Interface)
    }
    pub fn def_kind(self) -> Option<C> {
        self.children()
            .find_map(|child| C::from_repr(child.kind()).filter(|kind| kind.is_def_type()))
    }
    pub fn has_role(self, role: Role) -> bool {
        self.def_kind().is_some_and(|kind| match role {
            Role::Value => kind.is_value(),
            Role::Callable => kind.is_callable(),
        })
    }
    pub fn binds_ivar(self) -> bool {
        self.child(C::Binding)
            .is_some_and(|binding| binding.has(C::Ivar))
    }
    pub fn fills_role(self, role: Role, callable: u32) -> bool {
        self.has_role(role) || (role == Role::Callable && self.has_tag(callable))
    }
    pub fn role(self) -> Role {
        let call = match self.is(C::Callee) {
            true => self.parent(),
            false => Some(self).filter(|node| node.is(C::Call)),
        };
        match call {
            Some(call) if !call.has(C::Property) => Role::Callable,
            _ => Role::Value,
        }
    }
    pub fn is_member_of(self, owner: Self) -> bool {
        let mut node = self;
        loop {
            let Some(up) = node.ancestors().find(|up| up.is(C::Def) || up.is(C::Scope)) else {
                return false;
            };
            if up.index() == owner.index() {
                return true;
            }
            let header = node.index() == self.index() && self.binds_ivar();
            if up.is(C::Scope) || !(up.has(C::ImplBlock) || header) {
                return false;
            }
            node = up;
        }
    }
    pub fn reference(self) -> Self {
        self.child(C::Call)
            .filter(|call| call.has(C::Property))
            .and_then(|call| call.child(C::Callee))
            .unwrap_or(self)
    }
    pub fn member(self) -> Option<Self> {
        self.child(C::Callee)?.child(C::Member)
    }
    pub fn chain_root(self) -> Self {
        std::iter::successors(Some(self), |node| node.child(C::Member)?.child(C::Object))
            .last()
            .unwrap_or(self)
    }
    pub fn tail_expr(self) -> Self {
        let stop = |node: &Self| {
            node.is(C::Call)
                || node.is(C::SsaBranch)
                || node.is(C::SsaReturn)
                || node.is(C::Member)
                || node.is(C::Obj)
        };
        std::iter::successors(Some(self), |node| {
            (!stop(node)).then(|| node.last_named()).flatten()
        })
        .last()
        .unwrap_or(self)
    }
    pub fn rhs_callee(self) -> Option<u32> {
        self.child(C::Rhs)?.child(C::Call)?.child_sym(C::Callee)
    }
    pub fn bare_rhs(self) -> Option<Self> {
        self.child(C::Rhs)
            .filter(|rhs| rhs.children().next().is_none())
    }
    pub fn initializer(self) -> Option<Self> {
        self.child(C::Binding)
            .filter(|binding| binding.sym_opt().is_none())
    }
    pub fn typed(self) -> Option<Self> {
        self.child(C::SsaTyped).or_else(|| {
            let rhs = self.child(C::Rhs)?;
            rhs.child(C::Call)
                .and_then(|call| call.child(C::Callee))
                .or_else(|| rhs.child(C::Member))
        })
    }
    pub fn enclosing_def(self, kinds: &'a [C]) -> Option<Self> {
        self.enclosing(move |node| {
            node.is(C::Def)
                && node
                    .children()
                    .any(|child| kinds.iter().any(|&kind| child.kind() == kind))
        })
    }
    pub fn follow(self, edge: &Edge) -> Self {
        self.jump(edge.to_tree, edge.to_node)
    }
    pub fn edge_to(self, to: Self, kind: EdgeKind) -> Edge {
        Edge::new(self.fi(), self.index(), to.fi(), to.index(), kind)
    }
    pub fn child(self, kind: C) -> Option<Self> {
        self.children().find(|node| node.is(kind))
    }
    pub fn child_sym(self, kind: C) -> Option<u32> {
        self.child(kind).and_then(|node| node.sym_opt())
    }
    pub fn has(self, kind: C) -> bool {
        self.child(kind).is_some()
    }
    pub fn names(self) -> impl Iterator<Item = Self> + 'a {
        self.children_of(C::Name)
    }
}

pub fn infer_return_type<S: Storage<Node = Node>>(def: Cursor<'_, S>) -> Option<u32> {
    if let Some(sym) = def.child_sym(C::SsaReturnType) {
        return Some(sym);
    }
    let mut bindings = rustc_hash::FxHashMap::default();
    def.walk().run(|node, walk| {
        if node.is(C::Def) && node.index() != def.index() {
            walk.skip_subtree();
            return ControlFlow::Continue(());
        }
        if node.is(C::Binding) && node.sym_opt().is_some() {
            if let Some(callee) = node.rhs_callee() {
                bindings.insert(node.sym(), callee);
            }
            walk.skip_subtree();
            return ControlFlow::Continue(());
        }
        if node.is(C::SsaReturn) {
            let value = node
                .children()
                .find(|child| child.is(C::Call) || child.sym_opt().is_some())
                .and_then(|child| {
                    if child.is(C::Call) {
                        child.child_sym(C::Callee)
                    } else {
                        Some(bindings.get(&child.sym()).copied().unwrap_or(child.sym()))
                    }
                });
            walk.skip_subtree();
            if let Some(value) = value {
                return ControlFlow::Break(value);
            }
        }
        ControlFlow::Continue(())
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Value,
    Callable,
}

#[derive(Clone, Copy, Debug)]
pub struct Slots {
    first: u32,
    value: Option<(u32, bool)>,
    callable: Option<(u32, bool)>,
}

impl Slots {
    fn new(first: u32) -> Self {
        Self {
            first,
            value: None,
            callable: None,
        }
    }
    pub fn slot(self, role: Role) -> Option<u32> {
        match role {
            Role::Value => self.value,
            Role::Callable => self.callable,
        }
        .map(|(def, _)| def)
    }
    pub fn pick(self, role: Role) -> u32 {
        self.slot(role).unwrap_or(self.first)
    }
    fn fill(&mut self, def: u32, kind: Option<C>) {
        let value = kind.is_some_and(C::is_value);
        let callable = kind.is_some_and(C::is_callable);
        let exact = value != callable;
        for (slot, has) in [(&mut self.value, value), (&mut self.callable, callable)] {
            if has && slot.is_none_or(|(_, was_exact)| exact && !was_exact) {
                *slot = Some((def, exact));
            }
        }
    }
}

pub type MemberIndex = rustc_hash::FxHashMap<u32, Slots>;

pub fn member_index<S: Storage<Node = Node>>(owner: Cursor<'_, S>) -> MemberIndex {
    let nested = |node: Cursor<'_, S>| node.is_class() && !node.has(C::ImplBlock);
    let mut index = MemberIndex::default();
    let mut locals: Vec<(u32, bool)> = Vec::new();
    for node in owner.descendants_pruned(nested) {
        while locals.last().is_some_and(|&(end, _)| node.start() >= end) {
            locals.pop();
        }
        if node.is(C::Scope) {
            locals.push((node.end(), false));
            continue;
        }
        if !node.is(C::Def) {
            continue;
        }
        let member = match locals.as_slice() {
            [] => true,
            [(_, true)] => node.binds_ivar(),
            _ => false,
        };
        if let Some(name) = node.child_sym(C::DefName) {
            let slots = index
                .entry(name)
                .or_insert_with(|| Slots::new(node.index()));
            if member {
                slots.fill(node.index(), node.def_kind());
            }
        }
        if !node.has(C::ImplBlock) && !nested(node) {
            locals.push((node.end(), true));
        }
    }
    index
}

pub fn by_role<T: Copy>(candidates: Vec<T>, has_role: impl Fn(T) -> bool) -> Vec<T> {
    match candidates.iter().any(|&node| has_role(node)) {
        true => candidates
            .into_iter()
            .filter(|&node| has_role(node))
            .collect(),
        false => candidates,
    }
}

pub fn find_method_in<'a, S: Storage<Node = Node>>(
    class: Cursor<'a, S>,
    name: u32,
    role: Role,
    callable: u32,
) -> Option<Cursor<'a, S>> {
    let mut fallback = None;
    class
        .descend(|node| {
            if node.is(C::Def)
                && node.index() != class.index()
                && node.child_sym(C::DefName) == Some(name)
            {
                if node.fills_role(role, callable) {
                    return Step::Out(node);
                }
                fallback.get_or_insert(node);
            }
            if node.is_class() && !node.has(C::ImplBlock) {
                return Step::Over;
            }
            Step::Into
        })
        .or(fallback)
}

pub fn members_by_level<N, I, T>(
    start: Vec<N>,
    successors: impl Fn(N) -> I,
    find: impl Fn(N) -> Option<T>,
) -> Vec<T>
where
    N: Copy + Eq + std::hash::Hash,
    I: IntoIterator<Item = N>,
    T: PartialEq,
{
    let mut seen: rustc_hash::FxHashSet<N> = start.iter().copied().collect();
    let mut level = start;
    while !level.is_empty() {
        let mut found = Vec::new();
        for value in level.iter().filter_map(|&node| find(node)) {
            if !found.contains(&value) {
                found.push(value);
            }
        }
        if !found.is_empty() {
            return found;
        }
        level = level
            .iter()
            .flat_map(|&node| successors(node))
            .filter(|node| seen.insert(*node))
            .collect();
    }
    Vec::new()
}

pub fn reachable<N, I>(start: N, successors: impl Fn(N) -> I) -> impl Iterator<Item = N>
where
    N: Copy + Eq,
    I: IntoIterator<Item = N>,
{
    let mut seen: smallvec::SmallVec<[N; 8]> = smallvec::smallvec![start];
    let mut stack: smallvec::SmallVec<[N; 8]> = smallvec::smallvec![start];
    std::iter::from_fn(move || {
        let node = stack.pop()?;
        stack.extend(successors(node).into_iter().filter(|next| {
            if seen.contains(next) {
                false
            } else {
                seen.push(*next);
                true
            }
        }));
        Some(node)
    })
}
