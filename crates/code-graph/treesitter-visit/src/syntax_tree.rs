//! Arena-backed mutable syntax tree.
//!
//! Built from a tree-sitter parse, mutated by rewrite passes, then walked
//! through the standard [`Doc`]/[`SgNode`] traits.

use crate::node::Position;
use crate::source::{Doc, SgNode};
use crate::{KindId, SupportLang};
use smallvec::SmallVec;
use smol_str::SmolStr;
use std::borrow::Cow;
use std::ops::Range;

pub type NodeId = u32;
const NO_PARENT: u32 = u32::MAX;

#[derive(Clone)]
pub struct SyntaxNode {
    kind: SmolStr,
    start_byte: u32,
    end_byte: u32,
    parent: u32,
    children: SmallVec<[NodeId; 6]>,
    fields: SmallVec<[(&'static str, NodeId); 4]>,
    virtual_text: Option<SmolStr>,
    is_named: bool,
}

#[derive(Clone)]
pub struct SyntaxTree {
    source: String,
    lang: SupportLang,
    nodes: Vec<SyntaxNode>,
    root: NodeId,
}

impl SyntaxTree {
    /// Single-pass iterative conversion using one cursor for the entire DFS.
    pub fn from_tree_sitter(source: &str, ts_tree: &tree_sitter::Tree, lang: SupportLang) -> Self {
        let ts_root = ts_tree.root_node();
        let mut nodes: Vec<SyntaxNode> = Vec::with_capacity(ts_root.descendant_count());
        let mut parent_stack: Vec<NodeId> = Vec::with_capacity(32);
        let mut cursor = ts_root.walk();
        let mut done = false;

        loop {
            let ts = cursor.node();
            let id = nodes.len() as NodeId;
            let parent = parent_stack.last().copied().unwrap_or(NO_PARENT);

            nodes.push(SyntaxNode {
                kind: SmolStr::new(ts.kind()),
                start_byte: ts.start_byte() as u32,
                end_byte: ts.end_byte() as u32,
                parent,
                children: SmallVec::new(),
                fields: SmallVec::new(),
                virtual_text: None,
                is_named: ts.is_named(),
            });

            if parent != NO_PARENT {
                nodes[parent as usize].children.push(id);
                if let Some(field_name) = cursor.field_name() {
                    nodes[parent as usize].fields.push((field_name, id));
                }
            }

            if cursor.goto_first_child() {
                parent_stack.push(id);
                continue;
            }
            if cursor.goto_next_sibling() {
                continue;
            }
            loop {
                if !cursor.goto_parent() {
                    done = true;
                    break;
                }
                parent_stack.pop();
                if cursor.goto_next_sibling() {
                    break;
                }
            }
            if done {
                break;
            }
        }

        SyntaxTree {
            source: source.to_string(),
            lang,
            nodes,
            root: 0,
        }
    }

    // ── Read ────────────────────────────────────────────────────

    #[inline]
    pub fn node(&self, id: NodeId) -> &SyntaxNode {
        &self.nodes[id as usize]
    }
    pub fn root_id(&self) -> NodeId {
        self.root
    }
    pub fn text(&self, id: NodeId) -> &str {
        let n = self.node(id);
        match n.virtual_text {
            Some(ref vt) => vt.as_str(),
            None => &self.source[n.start_byte as usize..n.end_byte as usize],
        }
    }
    pub fn kind(&self, id: NodeId) -> &str {
        self.node(id).kind.as_str()
    }
    pub fn field(&self, id: NodeId, name: &str) -> Option<NodeId> {
        self.node(id)
            .fields
            .iter()
            .find(|(n, _)| *n == name)
            .map(|&(_, c)| c)
    }
    pub fn field_text(&self, id: NodeId, name: &str) -> Option<&str> {
        self.field(id, name).map(|c| self.text(c))
    }
    pub fn children(&self, id: NodeId) -> &[NodeId] {
        &self.node(id).children
    }
    pub fn parent(&self, id: NodeId) -> Option<NodeId> {
        let p = self.node(id).parent;
        (p != NO_PARENT).then_some(p)
    }

    // ── Query ───────────────────────────────────────────────────

    pub fn nodes_of_kind<'a>(&'a self, kind: &'a str) -> impl Iterator<Item = NodeId> + 'a {
        (0..self.nodes.len() as NodeId).filter(move |&id| self.kind(id) == kind)
    }
    pub fn children_of_kind<'a>(
        &'a self,
        id: NodeId,
        kind: &'a str,
    ) -> impl Iterator<Item = NodeId> + 'a {
        self.node(id)
            .children
            .iter()
            .copied()
            .filter(move |&c| self.kind(c) == kind)
    }
    pub fn has_child_of_kind(&self, id: NodeId, kind: &str) -> bool {
        self.children_of_kind(id, kind).next().is_some()
    }
    pub fn has_child_text(&self, id: NodeId, text: &str) -> bool {
        self.node(id).children.iter().any(|&c| self.text(c) == text)
    }
    pub fn descendant_text(&self, id: NodeId, kind: &str, text: &str) -> bool {
        self.descendants(id)
            .any(|d| self.kind(d) == kind && self.text(d) == text)
    }
    fn descendants(&self, id: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        let mut stack = vec![id];
        std::iter::from_fn(move || {
            let node = stack.pop()?;
            stack.extend(self.node(node).children.iter().rev().copied());
            Some(node)
        })
    }
    pub fn next_sibling(&self, id: NodeId) -> Option<NodeId> {
        let sibs = &self.node(self.parent(id)?).children;
        let pos = sibs.iter().position(|&c| c == id)?;
        sibs.get(pos + 1).copied()
    }
    pub fn next_sibling_of_kind(&self, id: NodeId, kind: &str) -> Option<NodeId> {
        let sibs = &self.node(self.parent(id)?).children;
        let pos = sibs.iter().position(|&c| c == id)?;
        sibs[pos + 1..]
            .iter()
            .copied()
            .find(|&s| self.kind(s) == kind)
    }

    // ── Mutation ────────────────────────────────────────────────

    pub fn insert_child(&mut self, parent: NodeId, kind: &str, text: &str) -> NodeId {
        let (sb, eb) = (self.node(parent).start_byte, self.node(parent).end_byte);
        let id = self.nodes.len() as NodeId;
        self.nodes.push(SyntaxNode {
            kind: SmolStr::new(kind),
            start_byte: sb,
            end_byte: eb,
            parent,
            children: SmallVec::new(),
            fields: SmallVec::new(),
            virtual_text: Some(SmolStr::new(text)),
            is_named: true,
        });
        self.nodes[parent as usize].children.push(id);
        id
    }
    pub fn insert_field_child(
        &mut self,
        parent: NodeId,
        field_name: &'static str,
        kind: &str,
        text: &str,
    ) -> NodeId {
        let id = self.insert_child(parent, kind, text);
        self.nodes[parent as usize].fields.push((field_name, id));
        id
    }
    pub fn set_kind(&mut self, id: NodeId, kind: &str) {
        self.nodes[id as usize].kind = SmolStr::new(kind);
    }
    pub fn set_text(&mut self, id: NodeId, text: &str) {
        self.nodes[id as usize].virtual_text = Some(SmolStr::new(text));
    }
    pub fn remove(&mut self, id: NodeId) {
        let Some(parent) = self.parent(id) else {
            return;
        };
        let p = &mut self.nodes[parent as usize];
        p.children.retain(|c| *c != id);
        p.fields.retain(|(_, c)| *c != id);
    }
}

// ── SgNode / Doc ────────────────────────────────────────────────

fn byte_to_row_col(src: &[u8], byte: usize) -> (usize, usize) {
    let mut row = 0;
    let mut col = 0;
    for &b in &src[..byte.min(src.len())] {
        if b == b'\n' {
            row += 1;
            col = 0;
        } else {
            col += 1;
        }
    }
    (row, col)
}

#[derive(Clone, Copy)]
pub struct SyntaxNodeRef<'a> {
    tree: &'a SyntaxTree,
    id: NodeId,
}

impl<'a> SyntaxNodeRef<'a> {
    fn at(self, id: NodeId) -> Self {
        Self {
            tree: self.tree,
            id,
        }
    }
}

impl<'a> SgNode<'a> for SyntaxNodeRef<'a> {
    fn parent(&self) -> Option<Self> {
        self.tree.parent(self.id).map(|id| self.at(id))
    }
    fn children(&self) -> impl ExactSizeIterator<Item = Self> {
        let tree = self.tree;
        self.tree
            .node(self.id)
            .children
            .iter()
            .map(move |&id| SyntaxNodeRef { tree, id })
    }
    fn kind(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.tree.kind(self.id))
    }
    fn kind_id(&self) -> KindId {
        0
    }
    fn node_id(&self) -> usize {
        self.id as usize
    }
    fn range(&self) -> Range<usize> {
        let n = self.tree.node(self.id);
        n.start_byte as usize..n.end_byte as usize
    }
    fn start_pos(&self) -> Position {
        let n = self.tree.node(self.id);
        let (row, col) = byte_to_row_col(self.tree.source.as_bytes(), n.start_byte as usize);
        Position::new(row, col, n.start_byte as usize)
    }
    fn end_pos(&self) -> Position {
        let n = self.tree.node(self.id);
        let (row, col) = byte_to_row_col(self.tree.source.as_bytes(), n.end_byte as usize);
        Position::new(row, col, n.end_byte as usize)
    }
    fn is_named(&self) -> bool {
        self.tree.node(self.id).is_named
    }
    fn is_leaf(&self) -> bool {
        self.tree.node(self.id).children.is_empty()
    }
    fn is_named_leaf(&self) -> bool {
        self.is_named() && self.is_leaf()
    }
    fn field(&self, name: &str) -> Option<Self> {
        self.tree.field(self.id, name).map(|id| self.at(id))
    }
    fn field_children(&self, _field_id: Option<u16>) -> impl Iterator<Item = Self> {
        std::iter::empty()
    }
    fn child_by_field_id(&self, _field_id: u16) -> Option<Self> {
        None
    }
    fn next(&self) -> Option<Self> {
        self.tree.next_sibling(self.id).map(|id| self.at(id))
    }
    fn prev(&self) -> Option<Self> {
        let sibs = &self.tree.node(self.tree.parent(self.id)?).children;
        let pos = sibs.iter().position(|&c| c == self.id)?;
        (pos > 0).then(|| self.at(sibs[pos - 1]))
    }
}

impl Doc for SyntaxTree {
    type Source = String;
    type Lang = SupportLang;
    type Node<'r> = SyntaxNodeRef<'r>;

    fn get_lang(&self) -> &Self::Lang {
        &self.lang
    }
    fn get_source(&self) -> &Self::Source {
        &self.source
    }
    fn root_node(&self) -> SyntaxNodeRef<'_> {
        SyntaxNodeRef {
            tree: self,
            id: self.root,
        }
    }
    fn get_node_text<'a>(&'a self, node: &SyntaxNodeRef<'a>) -> Cow<'a, str> {
        Cow::Borrowed(self.text(node.id))
    }
    fn node_kind<'a>(&'a self, node: &SyntaxNodeRef<'a>) -> Cow<'a, str> {
        Cow::Borrowed(self.kind(node.id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LanguageExt, Root};

    fn py_tree(src: &str) -> SyntaxTree {
        let ts = SupportLang::Python.ast_grep(src);
        SyntaxTree::from_tree_sitter(src, &ts.doc.tree, SupportLang::Python)
    }

    #[test]
    fn round_trip_preserves_structure() {
        let tree = py_tree("def foo(x):\n    return x + 1");
        let root = Root::doc(tree);
        let module = root.root();
        assert_eq!(module.kind().as_ref(), "module");
        let func = module.children().next().unwrap();
        assert_eq!(func.kind().as_ref(), "function_definition");
        assert_eq!(func.field("name").unwrap().text().as_ref(), "foo");
    }

    #[test]
    fn insert_child_is_visible() {
        let mut tree = py_tree("class Foo: pass");
        let cls = tree
            .children_of_kind(tree.root_id(), "class_definition")
            .next()
            .unwrap();
        tree.insert_child(cls, "__property", "bar");
        let root = Root::doc(tree);
        let prop = root
            .root()
            .children()
            .next()
            .unwrap()
            .children()
            .last()
            .unwrap();
        assert_eq!(prop.kind().as_ref(), "__property");
        assert_eq!(prop.text().as_ref(), "bar");
    }

    #[test]
    fn set_kind_changes_kind() {
        let mut tree = py_tree("class Foo: pass");
        let cls = tree
            .children_of_kind(tree.root_id(), "class_definition")
            .next()
            .unwrap();
        tree.set_kind(cls, "__enum");
        assert_eq!(tree.kind(cls), "__enum");
    }

    #[test]
    fn set_text_overrides() {
        let mut tree = py_tree("x = 1");
        let id = tree.nodes_of_kind("identifier").next().unwrap();
        assert_eq!(tree.text(id), "x");
        tree.set_text(id, "replaced");
        assert_eq!(tree.text(id), "replaced");
    }

    #[test]
    fn remove_hides_node() {
        let mut tree = py_tree("x = 1");
        let root = tree.root_id();
        let before = tree.children(root).len();
        tree.remove(tree.children(root)[0]);
        assert_eq!(tree.children(root).len(), before - 1);
    }

    #[test]
    fn extract_works() {
        use crate::extract::field;
        let tree = py_tree("def foo(x): return x");
        let root = Root::doc(tree);
        let func = root.root().children().next().unwrap();
        assert_eq!(field("name").apply(&func), Some("foo".to_string()));
    }
}
