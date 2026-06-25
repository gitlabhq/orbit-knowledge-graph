//! Arena-backed mutable syntax tree.
//!
//! [`SyntaxTree`] is built from a tree-sitter parse via [`SyntaxTree::from_tree_sitter`],
//! then mutated by language-specific rewrite passes (insert virtual nodes, change kinds,
//! override text). The DSL engine walks the rewritten tree through the standard
//! [`Doc`]/[`SgNode`] traits — no hooks needed for structural normalization.

use crate::node::Position;
use crate::source::{Doc, SgNode};
use crate::{KindId, Language, SupportLang};
use smallvec::SmallVec;
use smol_str::SmolStr;
use std::borrow::Cow;
use std::ops::Range;

// ── Node ID ─────────────────────────────────────────────────────

/// Index into [`SyntaxTree::nodes`].
pub type NodeId = u32;

const NO_PARENT: u32 = u32::MAX;

// ── SyntaxNode ──────────────────────────────────────────────────

/// A single node in the arena. Real nodes point into source; virtual
/// nodes carry their own text.
pub struct SyntaxNode {
    kind: SmolStr,
    is_named: bool,
    start_byte: u32,
    end_byte: u32,
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
    parent: u32,
    children: SmallVec<[NodeId; 6]>,
    /// Field-name → child index. Only populated for children that have
    /// a grammar field name (e.g. `name`, `body`, `arguments`).
    fields: SmallVec<[(&'static str, NodeId); 4]>,
    /// Virtual nodes store their text here. Real nodes slice from source.
    virtual_text: Option<SmolStr>,
}

// ── SyntaxTree ──────────────────────────────────────────────────

/// Arena-backed syntax tree that can be mutated before the DSL engine walks it.
pub struct SyntaxTree {
    source: String,
    lang: SupportLang,
    nodes: Vec<SyntaxNode>,
    root: NodeId,
}

impl SyntaxTree {
    // ── Construction from tree-sitter ────────────────────────────

    /// Single-pass iterative conversion from a tree-sitter tree.
    /// Uses one cursor for the entire DFS with no recursion and no
    /// per-node cursor allocation.
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
            let start = ts.start_position();
            let end = ts.end_position();

            nodes.push(SyntaxNode {
                kind: SmolStr::new(ts.kind()),
                is_named: ts.is_named(),
                start_byte: ts.start_byte() as u32,
                end_byte: ts.end_byte() as u32,
                start_row: start.row as u32,
                start_col: start.column as u32,
                end_row: end.row as u32,
                end_col: end.column as u32,
                parent,
                children: SmallVec::new(),
                fields: SmallVec::new(),
                virtual_text: None,
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

    // ── Read API ────────────────────────────────────────────────

    #[inline]
    pub fn node(&self, id: NodeId) -> &SyntaxNode {
        &self.nodes[id as usize]
    }

    pub fn root_id(&self) -> NodeId {
        self.root
    }

    pub fn text(&self, id: NodeId) -> &str {
        let n = self.node(id);
        if let Some(ref vt) = n.virtual_text {
            vt.as_str()
        } else {
            &self.source[n.start_byte as usize..n.end_byte as usize]
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
            .map(|(_, child)| *child)
    }

    pub fn field_text(&self, id: NodeId, name: &str) -> Option<&str> {
        self.field(id, name).map(|child| self.text(child))
    }

    pub fn children(&self, id: NodeId) -> &[NodeId] {
        &self.node(id).children
    }

    pub fn parent(&self, id: NodeId) -> Option<NodeId> {
        let p = self.node(id).parent;
        (p != NO_PARENT).then_some(p)
    }

    // ── Query helpers ───────────────────────────────────────────

    /// Collect all node IDs with the given kind. Caller must collect
    /// into a Vec before mutating the tree.
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
        let parent = self.parent(id)?;
        let siblings = &self.node(parent).children;
        let pos = siblings.iter().position(|&c| c == id)?;
        siblings.get(pos + 1).copied()
    }

    pub fn next_sibling_of_kind(&self, id: NodeId, kind: &str) -> Option<NodeId> {
        let parent = self.parent(id)?;
        let siblings = &self.node(parent).children;
        let pos = siblings.iter().position(|&c| c == id)?;
        siblings[pos + 1..]
            .iter()
            .copied()
            .find(|&s| self.kind(s) == kind)
    }

    // ── Mutation API ────────────────────────────────────────────

    /// Insert a virtual child at the end of `parent`'s children.
    /// Returns the new node's ID.
    pub fn insert_child(&mut self, parent: NodeId, kind: &str, text: &str) -> NodeId {
        let p = self.node(parent);
        let (sb, eb, sr, sc, er, ec) = (
            p.start_byte,
            p.end_byte,
            p.start_row,
            p.start_col,
            p.end_row,
            p.end_col,
        );

        let id = self.nodes.len() as NodeId;
        self.nodes.push(SyntaxNode {
            kind: SmolStr::new(kind),
            is_named: true,
            start_byte: sb,
            end_byte: eb,
            start_row: sr,
            start_col: sc,
            end_row: er,
            end_col: ec,
            parent,
            children: SmallVec::new(),
            fields: SmallVec::new(),
            virtual_text: Some(SmolStr::new(text)),
        });
        self.nodes[parent as usize].children.push(id);
        id
    }

    /// Insert a virtual child with a field name.
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

    /// Change the kind of a node.
    pub fn set_kind(&mut self, id: NodeId, kind: &str) {
        self.nodes[id as usize].kind = SmolStr::new(kind);
    }

    /// Override the text of a node (makes it virtual-text backed).
    pub fn set_text(&mut self, id: NodeId, text: &str) {
        self.nodes[id as usize].virtual_text = Some(SmolStr::new(text));
    }

    /// Remove a node from its parent's child list. The node stays
    /// in the arena but becomes unreachable from the tree walk.
    pub fn remove(&mut self, id: NodeId) {
        let Some(parent) = self.parent(id) else {
            return;
        };
        let p = &mut self.nodes[parent as usize];
        p.children.retain(|c| *c != id);
        p.fields.retain(|(_, c)| *c != id);
    }
}

// ── SgNode implementation ───────────────────────────────────────

/// Lightweight reference into a [`SyntaxTree`].
#[derive(Clone, Copy)]
pub struct SyntaxNodeRef<'a> {
    tree: &'a SyntaxTree,
    id: NodeId,
}

impl<'a> SgNode<'a> for SyntaxNodeRef<'a> {
    fn parent(&self) -> Option<Self> {
        self.tree.parent(self.id).map(|id| Self {
            tree: self.tree,
            id,
        })
    }

    fn children(&self) -> impl ExactSizeIterator<Item = Self> {
        let children = &self.tree.node(self.id).children;
        SyntaxNodeIter {
            tree: self.tree,
            ids: children.as_slice(),
            pos: 0,
        }
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
        Position::new(
            n.start_row as usize,
            n.start_col as usize,
            n.start_byte as usize,
        )
    }

    fn end_pos(&self) -> Position {
        let n = self.tree.node(self.id);
        Position::new(n.end_row as usize, n.end_col as usize, n.end_byte as usize)
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
        self.tree.field(self.id, name).map(|id| Self {
            tree: self.tree,
            id,
        })
    }

    fn field_children(&self, _field_id: Option<u16>) -> impl Iterator<Item = Self> {
        std::iter::empty()
    }

    fn child_by_field_id(&self, _field_id: u16) -> Option<Self> {
        None
    }

    fn next(&self) -> Option<Self> {
        self.tree.next_sibling(self.id).map(|id| Self {
            tree: self.tree,
            id,
        })
    }

    fn prev(&self) -> Option<Self> {
        let parent = self.tree.parent(self.id)?;
        let siblings = &self.tree.node(parent).children;
        let pos = siblings.iter().position(|&c| c == self.id)?;
        (pos > 0).then(|| Self {
            tree: self.tree,
            id: siblings[pos - 1],
        })
    }
}

/// ExactSizeIterator over arena children.
struct SyntaxNodeIter<'a> {
    tree: &'a SyntaxTree,
    ids: &'a [NodeId],
    pos: usize,
}

impl<'a> Iterator for SyntaxNodeIter<'a> {
    type Item = SyntaxNodeRef<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let &id = self.ids.get(self.pos)?;
        self.pos += 1;
        Some(SyntaxNodeRef {
            tree: self.tree,
            id,
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.ids.len() - self.pos;
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for SyntaxNodeIter<'_> {
    fn len(&self) -> usize {
        self.ids.len() - self.pos
    }
}

// ── Doc implementation ──────────────────────────────────────────

impl Clone for SyntaxTree {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            lang: self.lang,
            nodes: self
                .nodes
                .iter()
                .map(|n| SyntaxNode {
                    kind: n.kind.clone(),
                    is_named: n.is_named,
                    start_byte: n.start_byte,
                    end_byte: n.end_byte,
                    start_row: n.start_row,
                    start_col: n.start_col,
                    end_row: n.end_row,
                    end_col: n.end_col,
                    parent: n.parent,
                    children: n.children.clone(),
                    fields: n.fields.clone(),
                    virtual_text: n.virtual_text.clone(),
                })
                .collect(),
            root: self.root,
        }
    }
}

/// Language adapter: delegates to the stored [`SupportLang`].
#[derive(Clone, Copy)]
pub struct SyntaxTreeLang(pub SupportLang);

impl Language for SyntaxTreeLang {
    fn kind_to_id(&self, kind: &str) -> u16 {
        self.0.kind_to_id(kind)
    }
    fn field_to_id(&self, field: &str) -> Option<u16> {
        self.0.field_to_id(field)
    }
}

impl Doc for SyntaxTree {
    type Source = String;
    type Lang = SyntaxTreeLang;
    type Node<'r> = SyntaxNodeRef<'r>;

    fn get_lang(&self) -> &Self::Lang {
        // Safety: SyntaxTreeLang is a transparent newtype over SupportLang.
        // This avoids storing a separate SyntaxTreeLang field.
        unsafe { &*(std::ptr::from_ref(&self.lang) as *const SyntaxTreeLang) }
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

    #[test]
    fn round_trip_preserves_structure() {
        let src = "def foo(x):\n    return x + 1";
        let lang = SupportLang::Python;
        let ts_root = lang.ast_grep(src);
        let tree = SyntaxTree::from_tree_sitter(src, &ts_root.doc.tree, lang);
        let root = Root::doc(tree);
        let module = root.root();
        assert_eq!(module.kind().as_ref(), "module");
        let func = module.children().next().unwrap();
        assert_eq!(func.kind().as_ref(), "function_definition");
        let name = func.field("name").unwrap();
        assert_eq!(name.text().as_ref(), "foo");
    }

    #[test]
    fn insert_child_is_visible() {
        let src = "class Foo: pass";
        let lang = SupportLang::Python;
        let ts_root = lang.ast_grep(src);
        let mut tree = SyntaxTree::from_tree_sitter(src, &ts_root.doc.tree, lang);

        let cls = tree
            .children_of_kind(tree.root_id(), "class_definition")
            .next()
            .unwrap();
        tree.insert_child(cls, "__property", "bar");

        let root = Root::doc(tree);
        let cls_node = root.root().children().next().unwrap();
        let prop = cls_node.children().last().unwrap();
        assert_eq!(prop.kind().as_ref(), "__property");
        assert_eq!(prop.text().as_ref(), "bar");
    }

    #[test]
    fn set_kind_changes_kind() {
        let src = "class Foo: pass";
        let lang = SupportLang::Python;
        let ts_root = lang.ast_grep(src);
        let mut tree = SyntaxTree::from_tree_sitter(src, &ts_root.doc.tree, lang);

        let cls = tree
            .children_of_kind(tree.root_id(), "class_definition")
            .next()
            .unwrap();
        tree.set_kind(cls, "__enum_definition");

        assert_eq!(tree.kind(cls), "__enum_definition");
        let root = Root::doc(tree);
        let node = root.root().children().next().unwrap();
        assert_eq!(node.kind().as_ref(), "__enum_definition");
    }

    #[test]
    fn set_text_overrides() {
        let src = "x = 1";
        let lang = SupportLang::Python;
        let ts_root = lang.ast_grep(src);
        let mut tree = SyntaxTree::from_tree_sitter(src, &ts_root.doc.tree, lang);

        let expr = tree.nodes_of_kind("identifier").next().unwrap();
        assert_eq!(tree.text(expr), "x");
        tree.set_text(expr, "replaced");
        assert_eq!(tree.text(expr), "replaced");
    }

    #[test]
    fn remove_hides_node() {
        let src = "x = 1";
        let lang = SupportLang::Python;
        let ts_root = lang.ast_grep(src);
        let mut tree = SyntaxTree::from_tree_sitter(src, &ts_root.doc.tree, lang);

        let root = tree.root_id();
        let child_count_before = tree.children(root).len();
        let first_child = tree.children(root)[0];
        tree.remove(first_child);
        assert_eq!(tree.children(root).len(), child_count_before - 1);
    }

    #[test]
    fn extract_works_on_syntax_tree() {
        use crate::Root;
        use crate::extract::field;

        let src = "def foo(x): return x";
        let lang = SupportLang::Python;
        let ts_root = lang.ast_grep(src);
        let tree = SyntaxTree::from_tree_sitter(src, &ts_root.doc.tree, lang);
        let root = Root::doc(tree);
        let func = root.root().children().next().unwrap();
        assert_eq!(field("name").apply(&func), Some("foo".to_string()));
    }
}
