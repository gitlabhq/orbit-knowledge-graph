//! Tree-sitter specific implementations.

pub mod traversal;

use crate::Language;
use crate::node::{KindId, Position, Root};
use crate::source::{Doc, SgNode};
use std::borrow::Cow;
use std::num::NonZero;
use thiserror::Error;

pub use traversal::TsPre;
pub use tree_sitter::Language as TSLanguage;
use tree_sitter::{Node, Parser, Tree};
pub use tree_sitter::{Point as TSPoint, Range as TSRange};

/// Represents tree-sitter related error
#[derive(Debug, Error)]
pub enum TSParseError {
    #[error("incompatible `Language` is assigned to a `Parser`.")]
    Language(#[from] tree_sitter::LanguageError),
    #[error("general error when tree-sitter fails to parse.")]
    TreeUnavailable,
}

#[inline]
fn parse_lang(
    parse_fn: impl Fn(&mut Parser) -> Option<Tree>,
    ts_lang: TSLanguage,
) -> Result<Tree, TSParseError> {
    let mut parser = Parser::new();
    parser.set_language(&ts_lang)?;
    if let Some(tree) = parse_fn(&mut parser) {
        Ok(tree)
    } else {
        Err(TSParseError::TreeUnavailable)
    }
}

/// A document backed by a String with a tree-sitter parse tree.
#[derive(Clone)]
pub struct StrDoc<L: LanguageExt> {
    pub src: String,
    pub lang: L,
    pub tree: Tree,
}

impl<L: LanguageExt> StrDoc<L> {
    pub fn try_new(src: &str, lang: L) -> Result<Self, String> {
        let src = src.to_string();
        let ts_lang = lang.get_ts_language();
        let tree =
            parse_lang(|p| p.parse(src.as_bytes(), None), ts_lang).map_err(|e| e.to_string())?;
        Ok(Self { src, lang, tree })
    }

    pub fn new(src: &str, lang: L) -> Self {
        Self::try_new(src, lang).expect("Parser tree error")
    }
}

impl<L: LanguageExt> Doc for StrDoc<L> {
    type Source = String;
    type Lang = L;
    type Node<'r> = Node<'r>;

    fn get_lang(&self) -> &Self::Lang {
        &self.lang
    }

    fn get_source(&self) -> &Self::Source {
        &self.src
    }

    fn root_node(&self) -> Node<'_> {
        self.tree.root_node()
    }

    fn get_node_text<'a>(&'a self, node: &Self::Node<'a>) -> Cow<'a, str> {
        Cow::Borrowed(
            node.utf8_text(self.src.as_bytes())
                .expect("invalid source text encoding"),
        )
    }
}

struct NodeWalker<'tree> {
    cursor: tree_sitter::TreeCursor<'tree>,
    count: usize,
}

impl<'tree> Iterator for NodeWalker<'tree> {
    type Item = Node<'tree>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            return None;
        }
        let ret = Some(self.cursor.node());
        self.cursor.goto_next_sibling();
        self.count -= 1;
        ret
    }
}

impl ExactSizeIterator for NodeWalker<'_> {
    fn len(&self) -> usize {
        self.count
    }
}

impl<'r> SgNode<'r> for Node<'r> {
    fn parent(&self) -> Option<Self> {
        Node::parent(self)
    }

    fn ancestors(&self, root: Self) -> impl Iterator<Item = Self> {
        let mut ancestor = Some(root);
        let self_id = self.id();
        std::iter::from_fn(move || {
            let inner = ancestor.take()?;
            if inner.id() == self_id {
                return None;
            }
            ancestor = inner.child_with_descendant(*self);
            Some(inner)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    }

    fn dfs(&self) -> impl Iterator<Item = Self> {
        TsPre::new(self)
    }

    fn child(&self, nth: usize) -> Option<Self> {
        Node::child(self, nth as u32)
    }

    fn children(&self) -> impl ExactSizeIterator<Item = Self> {
        let mut cursor = self.walk();
        cursor.goto_first_child();
        NodeWalker {
            cursor,
            count: self.child_count(),
        }
    }

    fn child_by_field_id(&self, field_id: u16) -> Option<Self> {
        Node::child_by_field_id(self, field_id)
    }

    fn next(&self) -> Option<Self> {
        self.next_sibling()
    }

    fn prev(&self) -> Option<Self> {
        self.prev_sibling()
    }

    fn next_all(&self) -> impl Iterator<Item = Self> {
        let node = self.parent().unwrap_or(*self);
        let mut cursor = node.walk();
        cursor.goto_first_child_for_byte(self.start_byte());
        std::iter::from_fn(move || {
            if cursor.goto_next_sibling() {
                Some(cursor.node())
            } else {
                None
            }
        })
    }

    fn prev_all(&self) -> impl Iterator<Item = Self> {
        let node = self.parent().unwrap_or(*self);
        let mut cursor = node.walk();
        cursor.goto_first_child_for_byte(self.start_byte());
        std::iter::from_fn(move || {
            if cursor.goto_previous_sibling() {
                Some(cursor.node())
            } else {
                None
            }
        })
    }

    fn is_named(&self) -> bool {
        Node::is_named(self)
    }

    fn is_named_leaf(&self) -> bool {
        self.named_child_count() == 0
    }

    fn is_leaf(&self) -> bool {
        self.child_count() == 0
    }

    fn kind(&self) -> Cow<'_, str> {
        Cow::Borrowed(Node::kind(self))
    }

    fn kind_id(&self) -> KindId {
        Node::kind_id(self)
    }

    fn node_id(&self) -> usize {
        self.id()
    }

    fn range(&self) -> std::ops::Range<usize> {
        self.start_byte()..self.end_byte()
    }

    fn start_pos(&self) -> Position {
        let pos = self.start_position();
        let byte = self.start_byte();
        Position::new(pos.row, pos.column, byte)
    }

    fn end_pos(&self) -> Position {
        let pos = self.end_position();
        let byte = self.end_byte();
        Position::new(pos.row, pos.column, byte)
    }

    fn is_missing(&self) -> bool {
        Node::is_missing(self)
    }

    fn is_error(&self) -> bool {
        Node::is_error(self)
    }

    fn field(&self, name: &str) -> Option<Self> {
        self.child_by_field_name(name)
    }

    fn field_children(&self, field_id: Option<u16>) -> impl Iterator<Item = Self> {
        let field_id = field_id.and_then(NonZero::new);
        let mut cursor = self.walk();
        cursor.goto_first_child();
        let mut done = field_id.is_none();

        std::iter::from_fn(move || {
            if done {
                return None;
            }
            while cursor.field_id() != field_id {
                if !cursor.goto_next_sibling() {
                    return None;
                }
            }
            let ret = cursor.node();
            if !cursor.goto_next_sibling() {
                done = true;
            }
            Some(ret)
        })
    }
}

/// Tree-sitter specific language trait
pub trait LanguageExt: Language {
    /// Create a [`Root`] instance for the language
    fn ast_grep<S: AsRef<str>>(&self, source: S) -> crate::Root<StrDoc<Self>> {
        crate::Root::new(source, self.clone())
    }

    /// tree sitter language to parse the source
    fn get_ts_language(&self) -> TSLanguage;
}

impl<L: LanguageExt> crate::Root<StrDoc<L>> {
    pub fn new<S: AsRef<str>>(src: S, lang: L) -> Self {
        Self::try_new(src, lang).expect("should parse")
    }

    pub fn try_new<S: AsRef<str>>(src: S, lang: L) -> Result<Self, String> {
        let doc = StrDoc::try_new(src.as_ref(), lang)?;
        Ok(Root { doc })
    }

    pub fn source(&self) -> &str {
        self.doc.get_source().as_str()
    }

    pub fn generate(self) -> String {
        self.doc.src
    }
}
