use crate::canonical::Canonical;

use super::types::{Edge, EdgeKind, Tree};

/// Control flow for `descend` and `ascend` traversals.
pub enum Step<R> {
    /// Continue into children (descend) or continue up (ascend).
    Into,
    /// Skip this subtree, continue with next sibling.
    Over,
    /// Halt traversal and return a value.
    Out(R),
}

/// Read-only position in a tree or forest of trees.
///
/// Single-tree: `tree.cursor(id)`. Cross-tree: `Cursor::new(trees, fi, node)`.
/// All navigation returns another Cursor. Edges are created via `edge_to`.
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
    pub fn kind(self) -> u16 {
        self.tree().node(self.nid()).kind
    }

    #[inline]
    pub fn sym(self) -> u32 {
        self.tree().node(self.nid()).sym
    }

    #[inline]
    pub fn is(self, ck: Canonical) -> bool {
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

    pub fn jump(self, fi: u32, id: u32) -> Self {
        Self {
            trees: self.trees,
            fi,
            id,
        }
    }

    pub fn follow(self, edge: &Edge) -> Self {
        self.jump(edge.to.tree, edge.to.node)
    }

    pub fn edge_to(self, to: Self, kind: EdgeKind) -> Edge {
        Edge::new(self.fi as usize, self.id, to.fi as usize, to.id, kind)
    }

    pub fn descend<R>(self, mut visitor: impl FnMut(Self) -> Step<R>) -> Option<R> {
        let tree = self.tree();
        let mut iter = self.nid().traverse(&tree.arena).skip(1);
        while let Some(edge) = iter.next() {
            let indextree::NodeEdge::Start(id) = edge else {
                continue;
            };
            match visitor(self.at(id)) {
                Step::Out(r) => return Some(r),
                Step::Over => {
                    let mut depth = 1u32;
                    for edge in iter.by_ref() {
                        match edge {
                            indextree::NodeEdge::Start(_) => depth += 1,
                            indextree::NodeEdge::End(_) if depth == 1 => break,
                            indextree::NodeEdge::End(_) => depth -= 1,
                        }
                    }
                }
                Step::Into => {}
            }
        }
        None
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

    pub fn child(self, ck: Canonical) -> Option<Self> {
        self.children().find(|n| n.is(ck))
    }

    pub fn child_sym(self, ck: Canonical) -> Option<u32> {
        self.child(ck).map(|n| n.sym()).filter(|&s| s != 0)
    }

    pub fn has(self, ck: Canonical) -> bool {
        self.children().any(|n| n.is(ck))
    }

    pub fn names(self) -> impl Iterator<Item = Self> + 'a {
        self.children().filter(|c| c.is(C::Name) && c.sym() != 0)
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

    pub(crate) fn trees_ref(self) -> &'a [Tree] {
        self.trees
    }
}

use crate::canonical::Canonical as C;

/// Infer return type from annotation or body scan. Skips nested defs.
pub fn infer_return_type(def: Cursor) -> Option<u32> {
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

pub fn find_method_in<'a>(class: Cursor<'a>, name: u32) -> Option<Cursor<'a>> {
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
