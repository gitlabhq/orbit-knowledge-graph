use std::ops::ControlFlow;

use crate::canonical::Canonical as C;

use super::types::{Edge, EdgeKind, Node, Tree};

pub enum Step<R> {
    Into,
    Over,
    Out(R),
}

pub struct Walk<'a> {
    cur: Cursor<'a>,
    iter: std::iter::Skip<indextree::Traverse<'a, Node>>,
    last: Option<indextree::NodeId>,
}

impl<'a> Iterator for Walk<'a> {
    type Item = Cursor<'a>;
    fn next(&mut self) -> Option<Cursor<'a>> {
        loop {
            if let indextree::NodeEdge::Start(id) = self.iter.next()? {
                self.last = Some(id);
                return Some(self.cur.at(id));
            }
        }
    }
}

impl<'a> Walk<'a> {
    pub fn skip_subtree(&mut self) {
        if let Some(id) = self.last.take() {
            self.iter
                .by_ref()
                .find(|e| *e == indextree::NodeEdge::End(id));
        }
    }

    /// Eager primitive: visit each node with `&mut Walk` for skip control.
    /// Return `Break(v)` to halt early, `Continue(())` to keep going.
    pub fn run<B>(
        mut self,
        mut f: impl FnMut(Cursor<'a>, &mut Self) -> ControlFlow<B>,
    ) -> Option<B> {
        while let Some(n) = self.next() {
            if let ControlFlow::Break(v) = f(n, &mut self) {
                return Some(v);
            }
        }
        None
    }
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

#[derive(Clone, Copy)]
pub struct Cursor<'a> {
    trees: &'a [Tree],
    fi: u32,
    id: u32,
}

impl<'a> Cursor<'a> {
    pub fn new(trees: &'a [Tree], fi: u32, id: u32) -> Self {
        Self { trees, fi, id }
    }

    #[inline]
    fn tree(self) -> &'a Tree {
        &self.trees[self.fi as usize]
    }

    fn nid(self) -> indextree::NodeId {
        self.tree().to_id(self.id)
    }

    fn at(self, nid: indextree::NodeId) -> Self {
        Self {
            id: Tree::to_raw(nid),
            ..self
        }
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
        self.tree().node(self.nid()).kind
    }

    #[inline]
    pub fn sym(self) -> u32 {
        self.tree().node(self.nid()).sym
    }

    #[inline]
    pub fn sym_opt(self) -> Option<u32> {
        Some(self.sym()).filter(|&s| s != 0)
    }

    #[inline]
    pub fn is(self, ck: C) -> bool {
        self.kind() == ck
    }

    #[inline]
    pub fn named(self) -> bool {
        self.tree().node(self.nid()).named
    }

    #[inline]
    pub fn field(self) -> u16 {
        self.tree().node(self.nid()).field
    }

    #[inline]
    pub fn start(self) -> u32 {
        self.tree().node(self.nid()).start
    }

    #[inline]
    pub fn end(self) -> u32 {
        self.tree().node(self.nid()).end
    }

    #[inline]
    pub fn start_row(self) -> u32 {
        self.tree().node(self.nid()).start_row
    }

    #[inline]
    pub fn start_col(self) -> u32 {
        self.tree().node(self.nid()).start_col
    }

    #[inline]
    pub fn end_row(self) -> u32 {
        self.tree().node(self.nid()).end_row
    }

    #[inline]
    pub fn end_col(self) -> u32 {
        self.tree().node(self.nid()).end_col
    }

    pub fn size(self) -> u32 {
        self.nid().descendants(&self.tree().arena).count() as u32
    }

    pub fn is_synth(self) -> bool {
        self.tree().node(self.nid()).synth
    }

    pub fn parent(self) -> Option<Self> {
        self.nid().parent(&self.tree().arena).map(|p| self.at(p))
    }

    pub fn children(self) -> impl Iterator<Item = Self> + 'a {
        self.nid()
            .children(&self.tree().arena)
            .map(move |id| self.at(id))
    }

    pub fn children_rev(self) -> impl Iterator<Item = Self> + 'a {
        let ids: Vec<_> = self.nid().children(&self.tree().arena).collect();
        ids.into_iter().rev().map(move |id| self.at(id))
    }

    pub fn children_of(self, ck: C) -> impl Iterator<Item = Self> + 'a {
        self.children()
            .filter(move |c| c.is(ck) && c.sym_opt().is_some())
    }

    pub fn last_named(self) -> Option<Self> {
        self.children().filter(|c| c.named()).last()
    }

    pub fn descendants(self) -> impl Iterator<Item = Self> + 'a {
        self.nid()
            .descendants(&self.tree().arena)
            .skip(1)
            .map(move |id| self.at(id))
    }

    pub fn ancestors(self) -> impl Iterator<Item = Self> + 'a {
        self.nid()
            .ancestors(&self.tree().arena)
            .skip(1)
            .map(move |id| self.at(id))
    }

    pub fn walk(self) -> Walk<'a> {
        Walk {
            cur: self,
            iter: self.nid().traverse(&self.tree().arena).skip(1),
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

    pub fn member(self) -> Option<Self> {
        self.child(C::Callee)?.child(C::Member)
    }

    pub fn object_ivar(self) -> Option<Self> {
        self.child(C::Object)?.child(C::Ivar)
    }

    pub fn rhs_callee(self) -> Option<u32> {
        self.child(C::Rhs)?.child(C::Call)?.child_sym(C::Callee)
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

    pub fn for_each(self, mut f: impl FnMut(Self, &mut Walk<'a>)) {
        self.walk().run(|n, w| {
            f(n, w);
            ControlFlow::<()>::Continue(())
        });
    }

    pub fn fold_tree<A>(self, mut init: A, mut f: impl FnMut(&mut A, Self, &mut Walk<'a>)) -> A {
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
        self.children()
            .filter(|c| c.is(C::Name) && c.sym_opt().is_some())
    }

    pub fn find_desc(self, pred: impl Fn(Self) -> bool) -> Option<Self> {
        self.descend(|n| if pred(n) { Step::Out(n) } else { Step::Into })
    }

    pub fn any_desc(self, pred: impl Fn(Self) -> bool) -> bool {
        self.find_desc(pred).is_some()
    }

    pub fn enclosing(self, pred: impl Fn(Self) -> bool) -> Option<Self> {
        self.ascend(|n| if pred(n) { Step::Out(n) } else { Step::Into })
    }
}

pub fn infer_return_type(def: Cursor) -> Option<u32> {
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

fn return_sym(ret: Cursor, binds: &rustc_hash::FxHashMap<u32, u32>) -> Option<u32> {
    let ch = ret
        .children()
        .find(|c| c.is(C::Call) || c.sym_opt().is_some())?;
    if ch.is(C::Call) {
        ch.child_sym(C::Callee)
    } else {
        Some(binds.get(&ch.sym()).copied().unwrap_or(ch.sym()))
    }
}

pub fn find_method_in<'a>(class: Cursor<'a>, name: u32) -> Option<Cursor<'a>> {
    class.descend(|n| {
        if n.is(C::Def) && n.index() != class.index() && n.child_sym(C::DefName) == Some(name) {
            return Step::Out(n);
        }
        Step::Into
    })
}

impl Tree {
    #[inline]
    pub fn cursor(&self, id: u32) -> Cursor<'_> {
        Cursor::new(std::slice::from_ref(self), 0, id)
    }

    #[inline]
    pub fn root(&self) -> Cursor<'_> {
        self.cursor(Tree::to_raw(self.root))
    }
}
