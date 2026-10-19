use std::ops::ControlFlow;

use super::{Cursor, Edge, EdgeKind, Node, Storage};
use crate::canonical::Canonical as C;
use crate::resolver::CLASS_LIKE;
use crate::tags::ReservedTags;

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
    pub fn lifted(self) -> bool {
        self.node().lifted
    }
    pub fn labelled(self) -> bool {
        self.node().labelled
    }
    pub fn is_value(self) -> bool {
        C::try_from_u16(self.kind()).is_some_and(C::is_value)
    }
    pub fn stands_for(self, child: Self) -> bool {
        !(self.labelled() && child.lifted())
    }
    pub fn value_child(self, kind: C) -> Option<Self> {
        self.children()
            .find(|node| node.is(kind) && self.stands_for(*node))
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
    pub fn role(self) -> Role {
        let call = match self.is(C::Callee) {
            true => self.parent(),
            false => Some(self).filter(|node| node.is(C::Call)),
        };
        match call {
            Some(call) if call.has(C::Type) => Role::Type,
            Some(call) if !call.has(C::Property) => Role::Callable,
            _ => Role::Value,
        }
    }
    pub fn is_member_of(self, owner: Self) -> bool {
        let mut header = self.child(C::Binding).is_some_and(|b| b.has(C::Ivar));
        let mut node = self;
        while let Some(up) = node.ancestors().find(|up| up.is(C::Def) || up.is(C::Scope)) {
            if up.index() == owner.index() {
                return true;
            }
            let header = std::mem::take(&mut header);
            if up.is(C::Scope) || !(up.has(C::ImplBlock) || header) {
                return false;
            }
            node = up;
        }
        false
    }
    pub fn reference(self) -> Self {
        self.value_child(C::Call)
            .filter(|call| call.has(C::Property))
            .and_then(|call| call.child(C::Callee))
            .unwrap_or(self)
    }
    pub fn member(self) -> Option<Self> {
        self.child(C::Callee)?.child(C::Member)
    }
    pub fn chain_root(self) -> Self {
        std::iter::successors(Some(self), |node| {
            node.value_child(C::Member)?.child(C::Object)
        })
        .last()
        .unwrap_or(self)
    }
    pub fn tail_expr(self) -> Self {
        let stop = |node: &Self| node.is_value() || node.is(C::SsaBranch) || node.is(C::SsaReturn);
        std::iter::successors(Some(self), |node| {
            (!stop(node))
                .then(|| node.last_named())
                .flatten()
                .filter(|tail| node.stands_for(*tail))
        })
        .last()
        .unwrap_or(self)
    }
    pub fn rhs_callee(self) -> Option<u32> {
        self.child(C::Rhs)?
            .value_child(C::Call)?
            .child_sym(C::Callee)
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
            rhs.value_child(C::Call)
                .and_then(|call| call.child(C::Callee))
                .or_else(|| rhs.value_child(C::Member))
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

pub fn is_name_text(text: &str) -> bool {
    !text.is_empty()
        && !text
            .chars()
            .any(|c| c.is_whitespace() || "()[]{}<>*&,;'\"=+/%^|~".contains(c))
}

pub fn infer_return_type<S: Storage<Node = Node>>(def: Cursor<'_, S>) -> Option<u32> {
    if let Some(sym) = def.child_sym(C::SsaReturnType) {
        return Some(sym);
    }
    let mut bindings = rustc_hash::FxHashMap::default();
    def.walk().run(|node, walk| {
        if node.is(C::Def) && node.index() != def.index() || node.is(C::Lambda) {
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
                .filter(|child| node.stands_for(*child))
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
    Type,
}

pub fn members_named<'a, S: Storage<Node = Node>>(
    owner: Cursor<'a, S>,
) -> impl Iterator<Item = (u32, u32)> + 'a {
    let nested = |node: Cursor<'a, S>| node.is_class() && !node.has(C::ImplBlock);
    owner
        .descendants_pruned(nested)
        .filter(|def| def.is(C::Def))
        .filter_map(|def| Some((def.child_sym(C::DefName)?, def.index())))
}

pub fn find_member_in<'a, S: Storage<Node = Node>>(
    owner: Cursor<'a, S>,
    defs: impl Iterator<Item = Cursor<'a, S>>,
    role: Role,
    tags: &ReservedTags,
) -> Option<u32> {
    let tag = match role {
        Role::Value => tags.value,
        Role::Callable => tags.callable,
        Role::Type => tags.declares_type,
    };
    let both = |def: Cursor<'a, S>| def.has_tag(tags.value) && def.has_tag(tags.callable);
    let mut defs = defs.filter(|def| def.has_tag(tag) && def.is_member_of(owner));
    let first = defs.next()?;
    let best = match both(first) {
        true => defs.find(|def| !both(*def)).unwrap_or(first),
        false => first,
    };
    Some(best.index())
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
