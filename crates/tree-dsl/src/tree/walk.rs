use crate::canonical::Canonical;

use super::access::TreeAccess;
use super::types::{Edge, EdgeKind, NONE};

pub enum Step<R> {
    Into,
    Over,
    Out(R),
}

pub struct Cursor<'a, T: TreeAccess = super::types::Tree> {
    trees: &'a [T],
    fi: u32,
    idx: u32,
}

impl<T: TreeAccess> Clone for Cursor<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: TreeAccess> Copy for Cursor<'_, T> {}

impl<'a, T: TreeAccess> Cursor<'a, T> {
    pub fn new(trees: &'a [T], fi: u32, idx: u32) -> Self {
        Self { trees, fi, idx }
    }

    #[inline]
    fn tree(&self) -> &'a T {
        &self.trees[self.fi as usize]
    }

    #[inline]
    pub fn trees_ref(&self) -> &'a [T] {
        self.trees
    }

    #[inline]
    pub fn index(&self) -> u32 {
        self.idx
    }

    #[inline]
    pub fn fi(&self) -> u32 {
        self.fi
    }

    #[inline]
    pub fn kind(&self) -> u16 {
        self.tree().kind(self.idx)
    }

    #[inline]
    pub fn sym(&self) -> u32 {
        self.tree().sym(self.idx)
    }

    #[inline]
    pub fn is(&self, ck: Canonical) -> bool {
        self.kind() == ck
    }

    #[inline]
    pub fn named(&self) -> bool {
        self.tree().named(self.idx)
    }

    #[inline]
    pub fn size(&self) -> u32 {
        self.tree().size(self.idx)
    }

    #[inline]
    pub fn field(&self) -> u16 {
        self.tree().field(self.idx)
    }

    #[inline]
    pub fn start(&self) -> u32 {
        self.tree().start(self.idx)
    }

    #[inline]
    pub fn end(&self) -> u32 {
        self.tree().end(self.idx)
    }

    #[inline]
    pub fn start_row(&self) -> u32 {
        self.tree().start_row(self.idx)
    }

    #[inline]
    pub fn start_col(&self) -> u32 {
        self.tree().start_col(self.idx)
    }

    #[inline]
    pub fn end_row(&self) -> u32 {
        self.tree().end_row(self.idx)
    }

    #[inline]
    pub fn end_col(&self) -> u32 {
        self.tree().end_col(self.idx)
    }

    #[inline]
    pub fn is_dead(&self) -> bool {
        self.tree().dead(self.idx)
    }

    pub fn parent(&self) -> Option<Cursor<'a, T>> {
        let p = self.tree().parent(self.idx);
        (p != NONE).then(|| Cursor {
            trees: self.trees,
            fi: self.fi,
            idx: p,
        })
    }

    pub fn children(&self) -> impl Iterator<Item = Cursor<'a, T>> {
        let trees = self.trees;
        let fi = self.fi;
        let t = self.tree();
        let end = t.hop(self.idx);
        let mut c = t.live(self.idx + 1, end);
        std::iter::from_fn(move || {
            if c >= end {
                return None;
            }
            let r = c;
            c = trees[fi as usize].live(trees[fi as usize].hop(c), end);
            Some(Cursor { trees, fi, idx: r })
        })
    }

    pub fn descendants(&self) -> impl Iterator<Item = Cursor<'a, T>> {
        let trees = self.trees;
        let fi = self.fi;
        let t = self.tree();
        let end = t.hop(self.idx);
        let mut c = t.live(self.idx + 1, end);
        std::iter::from_fn(move || {
            if c >= end {
                return None;
            }
            let r = c;
            c = trees[fi as usize].live(c + 1, end);
            Some(Cursor { trees, fi, idx: r })
        })
    }

    pub fn ancestors(&self) -> impl Iterator<Item = Cursor<'a, T>> {
        let trees = self.trees;
        let fi = self.fi;
        let tree = self.tree();
        let mut cur = tree.parent(self.idx);
        std::iter::from_fn(move || {
            if cur == NONE {
                return None;
            }
            let r = cur;
            cur = trees[fi as usize].parent(r);
            Some(Cursor { trees, fi, idx: r })
        })
    }

    pub fn jump(&self, fi: u32, idx: u32) -> Cursor<'a, T> {
        Cursor {
            trees: self.trees,
            fi,
            idx,
        }
    }

    pub fn follow(&self, edge: &Edge) -> Cursor<'a, T> {
        Cursor {
            trees: self.trees,
            fi: edge.to.tree,
            idx: edge.to.node,
        }
    }

    pub fn edge_to(&self, to: Cursor<'a, T>, kind: EdgeKind) -> Edge {
        Edge::new(self.fi as usize, self.idx, to.fi as usize, to.idx, kind)
    }

    pub fn descend<R>(&self, mut visitor: impl FnMut(Cursor<'a, T>) -> Step<R>) -> Option<R> {
        let tree = self.tree();
        let trees = self.trees;
        let fi = self.fi;
        let end = tree.hop(self.idx);
        let mut c = tree.live(self.idx + 1, end);
        while c < end {
            let cursor = Cursor { trees, fi, idx: c };
            match visitor(cursor) {
                Step::Out(r) => return Some(r),
                Step::Over => {
                    c = tree.live(tree.hop(c), end);
                }
                Step::Into => {
                    c = tree.live(c + 1, end);
                }
            }
        }
        None
    }

    pub fn ascend<R>(&self, mut visitor: impl FnMut(Cursor<'a, T>) -> Step<R>) -> Option<R> {
        let trees = self.trees;
        let fi = self.fi;
        let mut cur = self.tree().parent(self.idx);
        while cur != NONE {
            let cursor = Cursor { trees, fi, idx: cur };
            match visitor(cursor) {
                Step::Out(r) => return Some(r),
                Step::Into | Step::Over => {
                    cur = trees[fi as usize].parent(cur);
                }
            }
        }
        None
    }

    pub fn child(&self, ck: Canonical) -> Option<Cursor<'a, T>> {
        self.tree()
            .child_by_kind(self.idx, ck as u16)
            .map(|idx| Cursor {
                trees: self.trees,
                fi: self.fi,
                idx,
            })
    }

    pub fn child_sym(&self, ck: Canonical) -> Option<u32> {
        self.tree()
            .child_by_kind(self.idx, ck as u16)
            .map(|i| self.tree().sym(i))
            .filter(|&s| s != 0)
    }

    pub fn has(&self, ck: Canonical) -> bool {
        self.tree().child_by_kind(self.idx, ck as u16).is_some()
    }

    pub fn names(&self) -> impl Iterator<Item = Cursor<'a, T>> {
        use crate::canonical::Canonical as C;
        self.children().filter(|c| c.is(C::Name) && c.sym() != 0)
    }

    pub fn find_desc(&self, pred: impl Fn(Cursor<'a, T>) -> bool) -> Option<Cursor<'a, T>> {
        self.descend(|n| if pred(n) { Step::Out(n) } else { Step::Into })
    }

    pub fn any_desc(&self, pred: impl Fn(Cursor<'a, T>) -> bool) -> bool {
        self.find_desc(pred).is_some()
    }

    pub fn enclosing(&self, pred: impl Fn(Cursor<'a, T>) -> bool) -> Option<Cursor<'a, T>> {
        self.ascend(|n| if pred(n) { Step::Out(n) } else { Step::Into })
    }
}

use crate::canonical::Canonical as C;

pub fn infer_return_type<T: TreeAccess>(def: Cursor<T>) -> Option<u32> {
    def.child_sym(C::SsaReturnType).or_else(|| {
        let mut binds: Vec<(u32, u32)> = Vec::new();
        let mut result = None;
        def.descend(|n| -> Step<u32> {
            if n.is(C::Def) && n.index() != def.index() {
                return Step::Over;
            }
            if n.is(C::Binding) && n.sym() != 0 {
                if let Some(callee) = n
                    .child(C::Rhs)
                    .and_then(|r| r.child(C::Call))
                    .and_then(|c| c.child_sym(C::Callee))
                {
                    binds.push((n.sym(), callee));
                }
                return Step::Over;
            }
            if n.is(C::SsaReturn) && result.is_none() {
                for ch in n.children() {
                    if ch.is(C::Call) {
                        if let Some(s) = ch.child_sym(C::Callee) {
                            result = Some(s);
                        }
                        break;
                    }
                    if ch.sym() != 0 {
                        result = Some(
                            binds
                                .iter()
                                .find(|(l, _)| *l == ch.sym())
                                .map(|(_, c)| *c)
                                .unwrap_or(ch.sym()),
                        );
                        break;
                    }
                }
                return Step::Over;
            }
            Step::Into
        });
        result
    })
}

pub fn find_method_in<'a, T: TreeAccess>(class: Cursor<'a, T>, name: u32) -> Option<Cursor<'a, T>> {
    class.descend(|n| {
        if crate::canonical::is_def_type_kind(n.kind()) {
            if let Some(p) = n.parent() {
                if p.index() != class.index() && p.child_sym(C::DefName) == Some(name) {
                    return Step::Out(p);
                }
            }
        }
        Step::Into
    })
}

impl super::types::Tree {
    #[inline]
    pub fn cursor(&self, i: u32) -> Cursor<'_, super::types::Tree> {
        Cursor {
            trees: std::slice::from_ref(self),
            fi: 0,
            idx: i,
        }
    }

    #[inline]
    pub fn root(&self) -> Cursor<'_, super::types::Tree> {
        self.cursor(0)
    }
}

impl super::locked::LockedTree {
    #[inline]
    pub fn cursor(&self, i: u32) -> Cursor<'_, super::locked::LockedTree> {
        Cursor {
            trees: std::slice::from_ref(self),
            fi: 0,
            idx: i,
        }
    }

    #[inline]
    pub fn root(&self) -> Cursor<'_, super::locked::LockedTree> {
        self.cursor(0)
    }
}
