//! Core AST node types for tree-sitter traversal.

use crate::Language;
use crate::source::SgNode;
use crate::source::{Content, Doc};
use std::borrow::Cow;

pub type KindId = u16;

/// Represents a position in the source code.
/// The line and column are zero-based, character offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    line: usize,
    byte_column: usize,
    byte_offset: usize,
}

impl Position {
    pub fn new(line: usize, byte_column: usize, byte_offset: usize) -> Self {
        Self {
            line,
            byte_column,
            byte_offset,
        }
    }

    pub fn line(&self) -> usize {
        self.line
    }

    /// Returns the column in terms of characters.
    /// Note: node does not have to be a node of matching position.
    pub fn column<D: Doc>(&self, node: &Node<'_, D>) -> usize {
        let source = node.get_doc().get_source();
        source.get_char_column(self.byte_column, self.byte_offset)
    }

    pub fn byte_point(&self) -> (usize, usize) {
        (self.line, self.byte_column)
    }
}

/// Represents a parsed tree and owns the source string.
#[derive(Clone)]
pub struct Root<D: Doc> {
    pub(crate) doc: D,
}

impl<D: Doc> Root<D> {
    pub fn doc(doc: D) -> Self {
        Self { doc }
    }

    pub fn lang(&self) -> &D::Lang {
        self.doc.get_lang()
    }

    /// The root node represents the entire source
    pub fn root(&self) -> Node<'_, D> {
        Node {
            inner: self.doc.root_node(),
            root: self,
        }
    }

    /// Adopt the tree_sitter as the descendant of the root and return the wrapped sg Node.
    pub fn adopt<'r>(&'r self, inner: D::Node<'r>) -> Node<'r, D> {
        debug_assert!(self.check_lineage(&inner));
        Node { inner, root: self }
    }

    fn check_lineage(&self, inner: &D::Node<'_>) -> bool {
        let mut node = inner.clone();
        while let Some(n) = node.parent() {
            node = n;
        }
        node.node_id() == self.doc.root_node().node_id()
    }
}

/// A node in the AST tree.
/// 'r represents root lifetime
#[derive(Clone)]
pub struct Node<'r, D: Doc> {
    pub(crate) inner: D::Node<'r>,
    pub(crate) root: &'r Root<D>,
}

/// APIs for Node inspection
impl<'r, D: Doc> Node<'r, D> {
    pub fn get_doc(&self) -> &'r D {
        &self.root.doc
    }

    pub fn node_id(&self) -> usize {
        self.inner.node_id()
    }

    pub fn is_leaf(&self) -> bool {
        self.inner.is_leaf()
    }

    pub fn is_named_leaf(&self) -> bool {
        self.inner.is_named_leaf()
    }

    pub fn is_error(&self) -> bool {
        self.inner.is_error()
    }

    pub fn kind(&self) -> Cow<'_, str> {
        self.inner.kind()
    }

    pub fn kind_id(&self) -> KindId {
        self.inner.kind_id()
    }

    pub fn is_named(&self) -> bool {
        self.inner.is_named()
    }

    pub fn is_missing(&self) -> bool {
        self.inner.is_missing()
    }

    /// byte offsets of start and end.
    pub fn range(&self) -> std::ops::Range<usize> {
        self.inner.range()
    }

    /// Nodes' start position in terms of zero-based rows and columns.
    pub fn start_pos(&self) -> Position {
        self.inner.start_pos()
    }

    /// Nodes' end position in terms of rows and columns.
    pub fn end_pos(&self) -> Position {
        self.inner.end_pos()
    }

    pub fn text(&self) -> Cow<'r, str> {
        self.root.doc.get_node_text(&self.inner)
    }

    pub fn lang(&self) -> &'r D::Lang {
        self.root.lang()
    }

    /// the underlying tree-sitter Node
    pub fn get_inner_node(&self) -> D::Node<'r> {
        self.inner.clone()
    }

    pub fn root(&self) -> &'r Root<D> {
        self.root
    }
}

/// Tree traversal API
impl<'r, D: Doc> Node<'r, D> {
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        let inner = self.inner.parent()?;
        Some(Node {
            inner,
            root: self.root,
        })
    }

    pub fn children(&self) -> impl ExactSizeIterator<Item = Node<'r, D>> + '_ {
        self.inner.children().map(|inner| Node {
            inner,
            root: self.root,
        })
    }

    #[must_use]
    pub fn child(&self, nth: usize) -> Option<Self> {
        let inner = self.inner.child(nth)?;
        Some(Node {
            inner,
            root: self.root,
        })
    }

    pub fn field(&self, name: &str) -> Option<Self> {
        let inner = self.inner.field(name)?;
        Some(Node {
            inner,
            root: self.root,
        })
    }

    pub fn child_by_field_id(&self, field_id: u16) -> Option<Self> {
        let inner = self.inner.child_by_field_id(field_id)?;
        Some(Node {
            inner,
            root: self.root,
        })
    }

    pub fn field_children(&self, name: &str) -> impl Iterator<Item = Node<'r, D>> + '_ {
        let field_id = self.lang().field_to_id(name);
        self.inner.field_children(field_id).map(|inner| Node {
            inner,
            root: self.root,
        })
    }

    /// Returns all ancestors nodes of `self`.
    pub fn ancestors(&self) -> impl Iterator<Item = Node<'r, D>> + '_ {
        let root = self.root.doc.root_node();
        self.inner.ancestors(root).map(|inner| Node {
            inner,
            root: self.root,
        })
    }

    #[must_use]
    pub fn next(&self) -> Option<Self> {
        let inner = self.inner.next()?;
        Some(Node {
            inner,
            root: self.root,
        })
    }

    /// Returns all sibling nodes next to `self`.
    pub fn next_all(&self) -> impl Iterator<Item = Node<'r, D>> + '_ {
        self.inner.next_all().map(|inner| Node {
            inner,
            root: self.root,
        })
    }

    #[must_use]
    pub fn prev(&self) -> Option<Node<'r, D>> {
        let inner = self.inner.prev()?;
        Some(Node {
            inner,
            root: self.root,
        })
    }

    pub fn prev_all(&self) -> impl Iterator<Item = Node<'r, D>> + '_ {
        self.inner.prev_all().map(|inner| Node {
            inner,
            root: self.root,
        })
    }

    pub fn dfs<'s>(&'s self) -> impl Iterator<Item = Node<'r, D>> + 's {
        self.inner.dfs().map(|inner| Node {
            inner,
            root: self.root,
        })
    }

    /// Navigate a chain of named fields, returning `None` if any is missing.
    ///
    /// `node.field_chain(&["function", "object"])` is equivalent to
    /// `node.field("function")?.field("object")`.
    #[must_use]
    pub fn field_chain(&self, fields: &[&str]) -> Option<Self> {
        let mut current = self.clone();
        for &f in fields {
            current = current.field(f)?;
        }
        Some(current)
    }

    /// Find the first descendant (DFS, left-to-right) matching a predicate.
    /// Does not test `self`, only its descendants.
    #[must_use]
    pub fn find_descendant(&self, predicate: impl Fn(&Self) -> bool) -> Option<Self> {
        self.find_descendant_inner(&predicate)
    }

    fn find_descendant_inner(&self, predicate: &dyn Fn(&Self) -> bool) -> Option<Self> {
        for child in self.children() {
            if predicate(&child) {
                return Some(child);
            }
            if let Some(found) = child.find_descendant_inner(predicate) {
                return Some(found);
            }
        }
        None
    }

    /// Find the first node along `axis` whose kind satisfies `criterion`.
    ///
    /// ```ignore
    /// use treesitter_visit::{Axis::*, Match::*};
    /// node.find(Child, Kind("identifier"))
    /// node.find(Ancestor, AnyKind(&["class_definition", "module"]))
    /// node.find(Descendant, Kind("type_identifier"))
    /// node.find(Field("name"), Kind("identifier"))
    /// ```
    #[must_use]
    pub fn find(&self, axis: Axis<'_>, criterion: Match<'_>) -> Option<Self> {
        match axis {
            Axis::Child => self.children().find(|c| criterion.test(c)),
            Axis::Parent => self.parent().filter(|p| criterion.test(p)),
            Axis::Ancestor => self.parent_chain().find(|a| criterion.test(a)),
            Axis::Descendant => self.find_descendant(|n| criterion.test(n)),
            Axis::Field(f) => self.field(f).filter(|n| criterion.test(n)),
        }
    }

    /// Check if any node along `axis` satisfies `criterion`.
    pub fn has(&self, axis: Axis<'_>, criterion: Match<'_>) -> bool {
        self.find(axis, criterion).is_some()
    }

    /// All direct children whose kind satisfies `criterion`.
    pub fn children_matching<'a>(
        &'a self,
        criterion: Match<'a>,
    ) -> impl Iterator<Item = Self> + 'a {
        self.children().filter(move |c| criterion.test(c))
    }

    /// Find the first direct child whose kind equals `kind`.
    #[must_use]
    pub fn child_of_kind(&self, kind: &str) -> Option<Self> {
        self.find(Axis::Child, Match::Kind(kind))
    }

    /// Check whether any direct child has the given kind.
    pub fn has_child_of_kind(&self, kind: &str) -> bool {
        self.has(Axis::Child, Match::Kind(kind))
    }

    /// Lazy iterator from the immediate parent up to the root.
    pub fn parent_chain(&self) -> impl Iterator<Item = Node<'r, D>> {
        let mut current = self.parent();
        std::iter::from_fn(move || {
            let node = current.take()?;
            current = node.parent();
            Some(node)
        })
    }
}

// ── Composable traversal primitives ─────────────────────────────

/// Which direction to traverse from a node.
#[derive(Clone, Copy)]
pub enum Axis<'a> {
    /// Direct children only.
    Child,
    /// Immediate parent.
    Parent,
    /// Walk up from parent to root.
    Ancestor,
    /// DFS through all descendants.
    Descendant,
    /// Named field on the node.
    Field(&'a str),
}

/// What to match on a node during traversal.
#[derive(Clone, Copy)]
pub enum Match<'a> {
    /// Exact node kind.
    Kind(&'a str),
    /// Any of these node kinds.
    AnyKind(&'a [&'a str]),
}

impl Match<'_> {
    /// Test whether a node satisfies this criterion.
    pub fn test<D: Doc>(&self, node: &Node<'_, D>) -> bool {
        match self {
            Match::Kind(k) => node.kind().as_ref() == *k,
            Match::AnyKind(ks) => {
                let kind = node.kind();
                ks.iter().any(|k| *k == kind.as_ref())
            }
        }
    }
}
