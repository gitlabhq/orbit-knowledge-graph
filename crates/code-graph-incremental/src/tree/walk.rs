use std::ops::ControlFlow;

use crate::canonical::Canonical as C;
use crate::resolver::CLASS_LIKE;

use super::types::{Edge, EdgeKind, Tree};
use super::{Mutable, Node, Storage};

pub enum Step<R> {
    Into,
    Over,
    Out(R),
}

pub struct Walk<'a, S: Storage<Node> = Mutable> {
    cur: Cursor<'a, S>,
    next: Option<Cursor<'a, S>>,
    last: Option<Cursor<'a, S>>,
}

impl<'a, S: Storage<Node>> Iterator for Walk<'a, S> {
    type Item = Cursor<'a, S>;
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.next?;
        self.next = node.children().next().or_else(|| self.after(node));
        self.last = Some(node);
        Some(node)
    }
}

impl<'a, S: Storage<Node>> Walk<'a, S> {
    fn after(&self, mut node: Cursor<'a, S>) -> Option<Cursor<'a, S>> {
        while node.id != self.cur.id {
            if let Some(id) = S::next_sibling(&node.tree().arena, node.id) {
                return Some(node.at(id));
            }
            node = node.parent()?;
        }
        None
    }

    pub fn skip_subtree(&mut self) {
        if let Some(node) = self.last.take() {
            self.next = self.after(node);
        }
    }

    /// Eager primitive: visit each node with `&mut Walk` for skip control.
    /// Return `Break(v)` to halt early, `Continue(())` to keep going.
    pub fn run<B>(
        mut self,
        mut f: impl FnMut(Cursor<'a, S>, &mut Self) -> ControlFlow<B>,
    ) -> Option<B> {
        while let Some(n) = self.next() {
            if let ControlFlow::Break(v) = f(n, &mut self) {
                return Some(v);
            }
        }
        None
    }
}

pub fn members_by_level<N, I, T>(
    start: Vec<N>,
    succ: impl Fn(N) -> I,
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
        let mut found: Vec<T> = Vec::new();
        for t in level.iter().filter_map(|&n| find(n)) {
            if !found.contains(&t) {
                found.push(t);
            }
        }
        if !found.is_empty() {
            return found;
        }
        level = level
            .iter()
            .flat_map(|&n| succ(n))
            .filter(|m| seen.insert(*m))
            .collect();
    }
    Vec::new()
}

pub fn reachable<N, I>(start: N, succ: impl Fn(N) -> I) -> impl Iterator<Item = N>
where
    N: Copy + Eq,
    I: IntoIterator<Item = N>,
{
    let mut seen: smallvec::SmallVec<[N; 8]> = smallvec::smallvec![start];
    let mut stack: smallvec::SmallVec<[N; 8]> = smallvec::smallvec![start];
    std::iter::from_fn(move || {
        let n = stack.pop()?;
        stack.extend(succ(n).into_iter().filter(|m| {
            if seen.contains(m) {
                false
            } else {
                seen.push(*m);
                true
            }
        }));
        Some(n)
    })
}

pub struct Cursor<'a, S: Storage<N> = Mutable, N: Clone = Node> {
    trees: &'a [Tree<S, N>],
    fi: u32,
    id: u32,
}

impl<S: Storage<N>, N: Clone> Copy for Cursor<'_, S, N> {}
impl<S: Storage<N>, N: Clone> Clone for Cursor<'_, S, N> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, S: Storage<Node>> Cursor<'a, S> {
    pub fn new(trees: &'a [Tree<S>], fi: u32, id: u32) -> Self {
        Self { trees, fi, id }
    }

    #[inline]
    fn tree(self) -> &'a Tree<S> {
        &self.trees[self.fi as usize]
    }

    fn node(self) -> &'a Node {
        S::node(&self.tree().arena, self.id)
    }

    fn at(self, id: u32) -> Self {
        Self { id, ..self }
    }

    #[inline]
    pub fn index(self) -> u32 {
        self.id
    }

    #[inline]
    pub fn fi(self) -> u32 {
        self.fi
    }

    #[inline]
    pub fn tag(self, key: u32) -> Option<u32> {
        self.tree().get_tag(self.id, key)
    }

    #[inline]
    pub fn has_tag(self, key: u32) -> bool {
        self.tree().get_tag(self.id, key).is_some()
    }

    #[inline]
    pub fn kind(self) -> u16 {
        self.node().kind
    }

    #[inline]
    pub fn sym(self) -> u32 {
        self.node().sym
    }

    #[inline]
    pub fn sym_opt(self) -> Option<u32> {
        Some(self.sym()).filter(|&s| s != 0)
    }

    #[inline]
    pub fn is(self, ck: C) -> bool {
        self.kind() == ck
    }

    pub fn child_sym_of_kind(self, kind: u16) -> Option<u32> {
        self.children()
            .find(|c| c.kind() == kind)
            .and_then(|c| c.sym_opt())
    }

    #[inline]
    pub fn named(self) -> bool {
        self.node().named
    }

    #[inline]
    pub fn field(self) -> u16 {
        self.node().field
    }

    #[inline]
    pub fn start(self) -> u32 {
        self.node().start
    }

    #[inline]
    pub fn end(self) -> u32 {
        self.node().end
    }

    #[inline]
    pub fn start_row(self) -> u32 {
        self.node().start_row
    }

    #[inline]
    pub fn start_col(self) -> u32 {
        self.node().start_col
    }

    #[inline]
    pub fn end_row(self) -> u32 {
        self.node().end_row
    }

    #[inline]
    pub fn end_col(self) -> u32 {
        self.node().end_col
    }

    pub fn size(self) -> u32 {
        self.descendants().count() as u32 + 1
    }

    pub fn parent(self) -> Option<Self> {
        S::parent(&self.tree().arena, self.id).map(|id| self.at(id))
    }

    pub fn children(self) -> impl Iterator<Item = Self> + 'a {
        std::iter::successors(S::first_child(&self.tree().arena, self.id), move |&id| {
            S::next_sibling(&self.tree().arena, id)
        })
        .map(move |id| self.at(id))
    }

    pub fn children_rev(self) -> impl Iterator<Item = Self> + 'a {
        std::iter::successors(S::last_child(&self.tree().arena, self.id), move |&id| {
            S::previous_sibling(&self.tree().arena, id)
        })
        .map(move |id| self.at(id))
    }

    pub fn children_of(self, ck: C) -> impl Iterator<Item = Self> + 'a {
        self.children()
            .filter(move |c| c.is(ck) && c.sym_opt().is_some())
    }

    pub fn last_named(self) -> Option<Self> {
        self.children().filter(|c| c.named()).last()
    }

    pub fn descendants(self) -> impl Iterator<Item = Self> + 'a {
        self.walk()
    }

    pub fn ancestors(self) -> impl Iterator<Item = Self> + 'a {
        std::iter::successors(self.parent(), |node| node.parent())
    }

    pub fn walk(self) -> Walk<'a, S> {
        Walk {
            cur: self,
            next: self.children().next(),
            last: None,
        }
    }

    pub fn calls(self) -> impl Iterator<Item = Self> + 'a {
        self.descendants().filter(|d| d.is(C::Call))
    }

    pub fn member_calls(self) -> impl Iterator<Item = (Self, Self)> + 'a {
        self.calls()
            .filter_map(|c| Some((c, c.member()?)))
            .filter(|(_, m)| m.sym_opt().is_some())
    }

    pub fn is_class(self) -> bool {
        self.children()
            .any(|child| CLASS_LIKE.iter().any(|&kind| child.kind() == kind))
    }

    pub fn reference(self) -> Self {
        self.child(C::Call)
            .filter(|c| c.has(C::Property))
            .and_then(|c| c.child(C::Callee))
            .unwrap_or(self)
    }

    pub fn member(self) -> Option<Self> {
        self.child(C::Callee)?.child(C::Member)
    }

    pub fn chain_root(self) -> Self {
        let inner = |r: &Self| r.child(C::Member)?.child(C::Object);
        std::iter::successors(Some(self), inner)
            .last()
            .unwrap_or(self)
    }

    pub fn tail_expr(self) -> Self {
        let stop = |c: &Self| {
            c.is(C::Call)
                || c.is(C::SsaBranch)
                || c.is(C::SsaReturn)
                || c.is(C::Member)
                || c.is(C::Obj)
        };
        let next = |c: &Self| (!stop(c)).then(|| c.last_named()).flatten();
        std::iter::successors(Some(self), next)
            .last()
            .unwrap_or(self)
    }

    pub fn rhs_callee(self) -> Option<u32> {
        self.child(C::Rhs)?.child(C::Call)?.child_sym(C::Callee)
    }

    pub fn initializer(self) -> Option<Self> {
        self.child(C::Binding)
            .filter(|binding| binding.sym_opt().is_none())
    }

    pub fn typed(self) -> Option<Self> {
        self.child(C::SsaTyped).or_else(|| {
            let rhs = self.child(C::Rhs)?;
            let callee = rhs.child(C::Call).and_then(|c| c.child(C::Callee));
            callee.or_else(|| rhs.child(C::Member))
        })
    }

    pub fn enclosing_def(self, kinds: &'a [C]) -> Option<Self> {
        self.enclosing(move |a| {
            a.is(C::Def) && a.children().any(|c| kinds.iter().any(|&k| c.kind() == k))
        })
    }

    pub fn jump(self, fi: u32, id: u32) -> Self {
        Self {
            trees: self.trees,
            fi,
            id,
        }
    }

    pub fn follow(self, edge: &Edge) -> Self {
        self.jump(edge.to_tree, edge.to_node)
    }

    pub fn edge_to(self, to: Self, kind: EdgeKind) -> Edge {
        Edge::new(self.fi, self.id, to.fi, to.id, kind)
    }

    pub fn descend<R>(self, mut visitor: impl FnMut(Self) -> Step<R>) -> Option<R> {
        self.walk().run(|n, w| match visitor(n) {
            Step::Out(r) => ControlFlow::Break(r),
            Step::Over => {
                w.skip_subtree();
                ControlFlow::Continue(())
            }
            Step::Into => ControlFlow::Continue(()),
        })
    }

    pub fn for_each(self, mut f: impl FnMut(Self, &mut Walk<'a, S>)) {
        self.walk().run(|n, w| {
            f(n, w);
            ControlFlow::<()>::Continue(())
        });
    }

    pub fn fold_tree<A>(self, mut init: A, mut f: impl FnMut(&mut A, Self, &mut Walk<'a, S>)) -> A {
        self.walk().run(|n, w| {
            f(&mut init, n, w);
            ControlFlow::<()>::Continue(())
        });
        init
    }

    pub fn descendants_pruned(
        self,
        prune: impl Fn(Self) -> bool + 'a,
    ) -> impl Iterator<Item = Self> + 'a {
        let mut w = self.walk();
        std::iter::from_fn(move || {
            let n = w.next()?;
            if prune(n) {
                w.skip_subtree();
            }
            Some(n)
        })
    }

    pub fn ascend<R>(self, mut visitor: impl FnMut(Self) -> Step<R>) -> Option<R> {
        for anc in self.ancestors() {
            match visitor(anc) {
                Step::Out(r) => return Some(r),
                Step::Into | Step::Over => {}
            }
        }
        None
    }

    pub fn child(self, ck: C) -> Option<Self> {
        self.children().find(|n| n.is(ck))
    }

    pub fn child_sym(self, ck: C) -> Option<u32> {
        self.child(ck).map(|n| n.sym()).filter(|&s| s != 0)
    }

    pub fn has(self, ck: C) -> bool {
        self.children().any(|n| n.is(ck))
    }

    pub fn names(self) -> impl Iterator<Item = Self> + 'a {
        self.children_of(C::Name)
    }

    pub fn any_desc(self, pred: impl Fn(Self) -> bool) -> bool {
        self.descendants().any(pred)
    }

    pub fn enclosing(self, pred: impl Fn(Self) -> bool) -> Option<Self> {
        self.ascend(|n| if pred(n) { Step::Out(n) } else { Step::Into })
    }
}

pub fn infer_return_type<S: Storage<Node>>(def: Cursor<'_, S>) -> Option<u32> {
    if let Some(s) = def.child_sym(C::SsaReturnType) {
        return Some(s);
    }
    let mut binds: rustc_hash::FxHashMap<u32, u32> = rustc_hash::FxHashMap::default();
    def.walk().run(|n, w| {
        if n.is(C::Def) && n.index() != def.index() {
            w.skip_subtree();
            return ControlFlow::Continue(());
        }
        if n.is(C::Binding) && n.sym_opt().is_some() {
            if let Some(callee) = n.rhs_callee() {
                binds.insert(n.sym(), callee);
            }
            w.skip_subtree();
            return ControlFlow::Continue(());
        }
        if n.is(C::SsaReturn) {
            let r = return_sym(n, &binds);
            w.skip_subtree();
            if let Some(v) = r {
                return ControlFlow::Break(v);
            }
        }
        ControlFlow::Continue(())
    })
}

fn return_sym<S: Storage<Node>>(
    ret: Cursor<'_, S>,
    binds: &rustc_hash::FxHashMap<u32, u32>,
) -> Option<u32> {
    let ch = ret
        .children()
        .find(|c| c.is(C::Call) || c.sym_opt().is_some())?;
    if ch.is(C::Call) {
        ch.child_sym(C::Callee)
    } else {
        Some(binds.get(&ch.sym()).copied().unwrap_or(ch.sym()))
    }
}

pub fn find_method_in<'a, S: Storage<Node>>(
    class: Cursor<'a, S>,
    name: u32,
) -> Option<Cursor<'a, S>> {
    class.descend(|n| {
        if n.is(C::Def) && n.index() != class.index() && n.child_sym(C::DefName) == Some(name) {
            return Step::Out(n);
        }
        if n.is_class() && !n.has(C::ImplBlock) {
            return Step::Over;
        }
        Step::Into
    })
}

impl<S: Storage<Node>> Tree<S> {
    #[inline]
    pub fn cursor(&self, id: u32) -> Cursor<'_, S> {
        Cursor::new(std::slice::from_ref(self), 0, id)
    }

    #[inline]
    pub fn root(&self) -> Cursor<'_, S> {
        self.cursor(S::index(self.root))
    }

    pub fn len(&self) -> u32 {
        self.root().size()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
