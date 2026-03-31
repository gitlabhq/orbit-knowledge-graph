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
}
