use crate::canonical::Canonical;

use super::types::{Edge, EdgeKind, NONE, Tree, live};

/// Control flow for `descend` and `ascend` traversals.
pub enum Step<R> {
    /// Continue into children (descend) or continue up (ascend).
    Into,
    /// Skip this subtree, continue with next sibling. In ascend, treated as Into.
    Over,
    /// Halt traversal and return a value.
    Out(R),
}

/// Read-only position in a forest of trees. Copy, 24 bytes.
///
/// Single-tree: `tree.cursor(i)`. Cross-tree: `Cursor::new(trees, fi, node)`.
/// All navigation returns another Cursor. Edges are created via `edge_to`
/// and stored by the caller.
#[derive(Clone, Copy)]
pub struct Cursor<'a> {
    trees: &'a [Tree],
    fi: u32,
    idx: u32,
}

impl<'a> Cursor<'a> {
    /// Cross-tree constructor.
    pub fn new(trees: &'a [Tree], fi: u32, idx: u32) -> Self {
        Self { trees, fi, idx }
    }

    #[inline]
    fn tree(&self) -> &'a Tree {
        &self.trees[self.fi as usize]
    }

    /// Access the underlying tree slice (for raw node access in edge filtering).
    #[inline]
    pub fn trees_ref(&self) -> &'a [Tree] {
        self.trees
    }

    // ── Properties ──

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
        self.tree().nodes[self.idx as usize].kind
    }

    #[inline]
    pub fn sym(&self) -> u32 {
        self.tree().nodes[self.idx as usize].sym
    }

    #[inline]
    pub fn is(&self, ck: Canonical) -> bool {
        self.kind() == ck
    }

    #[inline]
    pub fn size(&self) -> u32 {
        self.tree().nodes[self.idx as usize].size
    }

    #[inline]
    pub fn field(&self) -> u16 {
        self.tree().nodes[self.idx as usize].field
    }

    #[inline]
    pub fn start(&self) -> u32 {
        self.tree().nodes[self.idx as usize].start
    }

    #[inline]
    pub fn end(&self) -> u32 {
        self.tree().nodes[self.idx as usize].end
    }

    #[inline]
    pub fn is_dead(&self) -> bool {
        self.tree().nodes[self.idx as usize].dead
    }

    // ── Navigation ──

    pub fn parent(&self) -> Option<Cursor<'a>> {
        let p = self.tree().nodes[self.idx as usize].parent;
        (p != NONE).then(|| Cursor {
            trees: self.trees,
            fi: self.fi,
            idx: p,
        })
    }

    pub fn children(&self) -> impl Iterator<Item = Cursor<'a>> {
        let trees = self.trees;
        let fi = self.fi;
        self.tree()
            .children(self.idx)
            .map(move |i| Cursor { trees, fi, idx: i })
    }

    pub fn descendants(&self) -> impl Iterator<Item = Cursor<'a>> {
        let trees = self.trees;
        let fi = self.fi;
        self.tree()
            .descendants(self.idx)
            .map(move |i| Cursor { trees, fi, idx: i })
    }

    pub fn ancestors(&self) -> impl Iterator<Item = Cursor<'a>> {
        let trees = self.trees;
        let fi = self.fi;
        let tree = self.tree();
        let mut cur = tree.nodes[self.idx as usize].parent;
        std::iter::from_fn(move || {
            if cur == NONE {
                return None;
            }
            let r = cur;
            cur = tree.nodes[r as usize].parent;
            Some(Cursor { trees, fi, idx: r })
        })
    }

    // ── Cross-tree ──

    /// Jump to a node in a different tree.
    pub fn jump(&self, fi: u32, idx: u32) -> Cursor<'a> {
        Cursor {
            trees: self.trees,
            fi,
            idx,
        }
    }

    /// Follow an edge to its target position.
    pub fn follow(&self, edge: &Edge) -> Cursor<'a> {
        Cursor {
            trees: self.trees,
            fi: edge.to.tree,
            idx: edge.to.node,
        }
    }

    /// Create an Edge value from self to target. Caller stores it.
    pub fn edge_to(&self, to: Cursor<'a>, kind: EdgeKind) -> Edge {
        Edge::new(self.fi as usize, self.idx, to.fi as usize, to.idx, kind)
    }

    // ── Traversal primitives ──

    /// Depth-first pre-order descent with subtree control.
    pub fn descend<R>(&self, mut visitor: impl FnMut(Cursor<'a>) -> Step<R>) -> Option<R> {
        let tree = self.tree();
        let trees = self.trees;
        let fi = self.fi;
        let end = tree.hop(self.idx);
        let mut c = live(tree, self.idx + 1, end);
        while c < end {
            let cursor = Cursor { trees, fi, idx: c };
            match visitor(cursor) {
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
    pub fn ascend<R>(&self, mut visitor: impl FnMut(Cursor<'a>) -> Step<R>) -> Option<R> {
        let tree = self.tree();
        let trees = self.trees;
        let fi = self.fi;
        let mut cur = tree.nodes[self.idx as usize].parent;
        while cur != NONE {
            let cursor = Cursor {
                trees,
                fi,
                idx: cur,
            };
            match visitor(cursor) {
                Step::Out(r) => return Some(r),
                Step::Into | Step::Over => {
                    cur = tree.nodes[cur as usize].parent;
                }
            }
        }
        None
    }

    // ── Child queries ──

    pub fn child(&self, ck: Canonical) -> Option<Cursor<'a>> {
        self.children().find(|n| n.is(ck))
    }

    pub fn child_sym(&self, ck: Canonical) -> Option<u32> {
        self.child(ck).map(|n| n.sym()).filter(|&s| s != 0)
    }

    pub fn has(&self, ck: Canonical) -> bool {
        self.children().any(|n| n.is(ck))
    }

    /// Children that are __name nodes with nonzero sym.
    pub fn names(&self) -> impl Iterator<Item = Cursor<'a>> {
        self.children().filter(|c| c.is(C::Name) && c.sym() != 0)
    }

    // ── Descendant queries ──

    pub fn find_desc(&self, pred: impl Fn(Cursor<'a>) -> bool) -> Option<Cursor<'a>> {
        self.descend(|n| if pred(n) { Step::Out(n) } else { Step::Into })
    }

    pub fn any_desc(&self, pred: impl Fn(Cursor<'a>) -> bool) -> bool {
        self.find_desc(pred).is_some()
    }

    // ── Ancestor queries ──

    pub fn enclosing(&self, pred: impl Fn(Cursor<'a>) -> bool) -> Option<Cursor<'a>> {
        self.ascend(|n| if pred(n) { Step::Out(n) } else { Step::Into })
    }
}

// ── Shared tree queries ──

use crate::canonical::Canonical as C;

/// Infer return type from annotation or body scan. Skips nested defs.
pub fn infer_return_type(def: Cursor) -> Option<u32> {
    def.child_sym(C::ReturnType).or_else(|| {
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
            if n.is(C::Return) && result.is_none() {
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

/// Find a method by name in a class. Searches DefType descendants.
pub fn find_method_in<'a>(class: Cursor<'a>, name: u32) -> Option<Cursor<'a>> {
    class.descend(|n| {
        if n.is(C::DefType) {
            if let Some(p) = n.parent() {
                if p.index() != class.index() && p.child_sym(C::DefName) == Some(name) {
                    return Step::Out(p);
                }
            }
        }
        Step::Into
    })
}

// ── Tree entry points ──

impl Tree {
    /// Single-tree cursor for the given index.
    #[inline]
    pub fn cursor(&self, i: u32) -> Cursor<'_> {
        Cursor {
            trees: std::slice::from_ref(self),
            fi: 0,
            idx: i,
        }
    }

    /// Cursor for the root node.
    #[inline]
    pub fn root(&self) -> Cursor<'_> {
        self.cursor(0)
    }

    /// Number of nodes (including dead).
    #[inline]
    pub fn len(&self) -> u32 {
        self.nodes.len() as u32
    }
}
