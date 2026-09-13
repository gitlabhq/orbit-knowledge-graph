use crate::canonical::Canonical;

use super::types::{NONE, Tree, live};

/// Control flow for `descend` and `ascend` traversals.
pub enum Step<R> {
    /// Continue into children (descend) or continue up (ascend).
    Into,
    /// Skip this subtree, continue with next sibling. In ascend, treated as Into.
    Over,
    /// Halt traversal and return a value.
    Out(R),
}

/// Read-only handle into a tree node. Copy, 16 bytes.
#[derive(Clone, Copy)]
pub struct NR<'a> {
    pub(crate) tree: &'a Tree,
    pub(crate) idx: u32,
}

impl<'a> NR<'a> {
    #[inline]
    pub fn index(&self) -> u32 {
        self.idx
    }

    #[inline]
    pub fn kind(&self) -> u16 {
        self.tree.nodes[self.idx as usize].kind
    }

    #[inline]
    pub fn sym(&self) -> u32 {
        self.tree.nodes[self.idx as usize].sym
    }

    #[inline]
    pub fn is(&self, ck: Canonical) -> bool {
        self.kind() == ck
    }

    #[inline]
    pub fn size(&self) -> u32 {
        self.tree.nodes[self.idx as usize].size
    }

    #[inline]
    pub fn field(&self) -> u16 {
        self.tree.nodes[self.idx as usize].field
    }

    #[inline]
    pub fn start(&self) -> u32 {
        self.tree.nodes[self.idx as usize].start
    }

    #[inline]
    pub fn end(&self) -> u32 {
        self.tree.nodes[self.idx as usize].end
    }

    #[inline]
    pub fn is_dead(&self) -> bool {
        self.tree.nodes[self.idx as usize].dead
    }

    // ── Navigation ──

    pub fn parent(&self) -> Option<NR<'a>> {
        let p = self.tree.nodes[self.idx as usize].parent;
        (p != NONE).then(|| NR {
            tree: self.tree,
            idx: p,
        })
    }

    pub fn children(&self) -> impl Iterator<Item = NR<'a>> {
        let tree = self.tree;
        self.tree
            .children(self.idx)
            .map(move |i| NR { tree, idx: i })
    }

    pub fn descendants(&self) -> impl Iterator<Item = NR<'a>> {
        let tree = self.tree;
        self.tree
            .descendants(self.idx)
            .map(move |i| NR { tree, idx: i })
    }

    pub fn ancestors(&self) -> impl Iterator<Item = NR<'a>> {
        let tree = self.tree;
        let mut cur = self.tree.nodes[self.idx as usize].parent;
        std::iter::from_fn(move || {
            if cur == NONE {
                return None;
            }
            let r = cur;
            cur = tree.nodes[r as usize].parent;
            Some(NR { tree, idx: r })
        })
    }

    // ── Traversal primitives ──

    /// Depth-first pre-order descent with subtree control.
    pub fn descend<R>(&self, mut visitor: impl FnMut(NR<'a>) -> Step<R>) -> Option<R> {
        let tree = self.tree;
        let end = tree.hop(self.idx);
        let mut c = live(tree, self.idx + 1, end);
        while c < end {
            let node = NR { tree, idx: c };
            match visitor(node) {
                Step::Out(r) => return Some(r),
                Step::Over => {
                    c = live(tree, tree.hop(c), end);
                }
                Step::Into => {
                    c = live(tree, c + 1, end);
                }
            }
        }
        None
    }

    /// Walk the parent chain with early exit.
    pub fn ascend<R>(&self, mut visitor: impl FnMut(NR<'a>) -> Step<R>) -> Option<R> {
        let tree = self.tree;
        let mut cur = tree.nodes[self.idx as usize].parent;
        while cur != NONE {
            let node = NR { tree, idx: cur };
            match visitor(node) {
                Step::Out(r) => return Some(r),
                Step::Into | Step::Over => {
                    cur = tree.nodes[cur as usize].parent;
                }
            }
        }
        None
    }

    // ── Child queries ──

    /// First child of canonical kind.
    pub fn child(&self, ck: Canonical) -> Option<NR<'a>> {
        self.children().find(|n| n.is(ck))
    }

    /// Sym of first child of canonical kind (nonzero only).
    pub fn child_sym(&self, ck: Canonical) -> Option<u32> {
        self.child(ck).map(|n| n.sym()).filter(|&s| s != 0)
    }

    /// Does any direct child have this canonical kind?
    pub fn has(&self, ck: Canonical) -> bool {
        self.children().any(|n| n.is(ck))
    }

    // ── Descendant queries ──

    /// First descendant matching predicate.
    pub fn find_desc(&self, pred: impl Fn(NR<'a>) -> bool) -> Option<NR<'a>> {
        self.descend(|n| if pred(n) { Step::Out(n) } else { Step::Into })
    }

    /// True if any descendant matches predicate.
    pub fn any_desc(&self, pred: impl Fn(NR<'a>) -> bool) -> bool {
        self.find_desc(pred).is_some()
    }

    // ── Ancestor queries ──

    /// First ancestor matching predicate.
    pub fn enclosing(&self, pred: impl Fn(NR<'a>) -> bool) -> Option<NR<'a>> {
        self.ascend(|n| if pred(n) { Step::Out(n) } else { Step::Into })
    }
}

// ── Tree entry points ──

impl Tree {
    /// Get a NodeRef handle for the given index.
    #[inline]
    pub fn nr(&self, i: u32) -> NR<'_> {
        NR { tree: self, idx: i }
    }

    /// NodeRef for the root node.
    #[inline]
    pub fn root(&self) -> NR<'_> {
        self.nr(0)
    }

    /// Number of nodes (including dead).
    #[inline]
    pub fn len(&self) -> u32 {
        self.nodes.len() as u32
    }
}
