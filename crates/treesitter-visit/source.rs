//! Document and content abstractions for tree-sitter parsing.

use crate::Position;
use crate::node::KindId;
use std::borrow::Cow;
use std::ops::Range;

/// Trait for document nodes that can be traversed.
/// NOTE: Some method names are the same as tree-sitter's methods.
pub trait SgNode<'r>: Clone {
    fn parent(&self) -> Option<Self>;
    fn children(&self) -> impl ExactSizeIterator<Item = Self>;
    fn kind(&self) -> Cow<'_, str>;
    fn kind_id(&self) -> KindId;
    fn node_id(&self) -> usize;
    fn range(&self) -> Range<usize>;
    fn start_pos(&self) -> Position;
    fn end_pos(&self) -> Position;

    fn ancestors(&self, _root: Self) -> impl Iterator<Item = Self> {
        let mut ancestors = vec![];
        let mut current = self.clone();
        while let Some(parent) = current.parent() {
            ancestors.push(parent.clone());
            current = parent;
        }
        ancestors.reverse();
        ancestors.into_iter()
    }

    fn dfs(&self) -> impl Iterator<Item = Self> {
        let mut stack = vec![self.clone()];
        std::iter::from_fn(move || {
            if let Some(node) = stack.pop() {
                let children: Vec<_> = node.children().collect();
                stack.extend(children.into_iter().rev());
                Some(node)
            } else {
                None
            }
        })
    }

    fn child(&self, nth: usize) -> Option<Self> {
        self.children().nth(nth)
    }

    fn next(&self) -> Option<Self> {
        let parent = self.parent()?;
        let mut children = parent.children();
        while let Some(child) = children.next() {
            if child.node_id() == self.node_id() {
                return children.next();
            }
        }
        None
    }

    fn prev(&self) -> Option<Self> {
        let parent = self.parent()?;
        let children = parent.children();
        let mut prev = None;
        for child in children {
            if child.node_id() == self.node_id() {
                return prev;
            }
            prev = Some(child);
        }
        None
    }

    fn next_all(&self) -> impl Iterator<Item = Self> {
        let mut next = self.next();
        std::iter::from_fn(move || {
            let n = next.clone()?;
            next = n.next();
            Some(n)
        })
    }

    fn prev_all(&self) -> impl Iterator<Item = Self> {
        let mut prev = self.prev();
        std::iter::from_fn(move || {
            let n = prev.clone()?;
            prev = n.prev();
            Some(n)
        })
    }

    fn is_named(&self) -> bool {
        true
    }

    fn is_named_leaf(&self) -> bool {
        self.is_leaf()
    }

    fn is_leaf(&self) -> bool {
        self.children().len() == 0
    }

    fn is_missing(&self) -> bool {
        false
    }

    fn is_error(&self) -> bool {
        false
    }

    fn field(&self, name: &str) -> Option<Self>;
    fn field_children(&self, field_id: Option<u16>) -> impl Iterator<Item = Self>;
    fn child_by_field_id(&self, field_id: u16) -> Option<Self>;
}

/// Trait for documents that can be parsed by tree-sitter.
pub trait Doc: Clone + 'static {
    type Source: Content;
    type Lang: crate::Language;
    type Node<'r>: SgNode<'r>;

    fn get_lang(&self) -> &Self::Lang;
    fn get_source(&self) -> &Self::Source;
    fn root_node(&self) -> Self::Node<'_>;
    fn get_node_text<'a>(&'a self, node: &Self::Node<'a>) -> Cow<'a, str>;
}

/// Trait for source content encoding.
pub trait Content: Sized {
    type Underlying: Clone + PartialEq;

    fn get_range(&self, range: Range<usize>) -> &[Self::Underlying];

    /// Get the character column at the given position
    fn get_char_column(&self, column: usize, offset: usize) -> usize;
}

impl Content for String {
    type Underlying = u8;

    fn get_range(&self, range: Range<usize>) -> &[Self::Underlying] {
        &self.as_bytes()[range]
    }

    /// This is an O(n) operation for UTF-8 column calculation.
    fn get_char_column(&self, _col: usize, offset: usize) -> usize {
        let src = self.as_bytes();
        let mut col = 0;
        for &b in src[..offset].iter().rev() {
            if b == b'\n' {
                break;
            }
            // https://en.wikipedia.org/wiki/UTF-8#Description
            if b & 0b1100_0000 != 0b1000_0000 {
                col += 1;
            }
        }
        col
    }
}
