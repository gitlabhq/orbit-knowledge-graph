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
    pub start_byte: u32,
    pub end_byte: u32,
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
    pub fn n(&self, id: NodeId) -> &SyntaxNode {
        &self.nodes[id as usize]
    }
    pub fn root_id(&self) -> NodeId {
        self.root
    }

    pub fn text(&self, id: NodeId) -> &str {
        let n = self.n(id);
        n.virtual_text
            .as_deref()
            .unwrap_or(&self.source[n.start_byte as usize..n.end_byte as usize])
    }
    pub fn kind(&self, id: NodeId) -> &str {
        self.n(id).kind.as_str()
    }
    pub fn children(&self, id: NodeId) -> &[NodeId] {
        &self.n(id).children
    }
    pub fn parent(&self, id: NodeId) -> Option<NodeId> {
        let p = self.n(id).parent;
        (p != NO_PARENT).then_some(p)
    }
    pub fn field(&self, id: NodeId, name: &str) -> Option<NodeId> {
        self.n(id)
            .fields
            .iter()
            .find(|(n, _)| *n == name)
            .map(|&(_, c)| c)
    }
    pub fn field_text(&self, id: NodeId, name: &str) -> Option<&str> {
        self.field(id, name).map(|c| self.text(c))
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
        self.n(id)
            .children
            .iter()
            .copied()
            .filter(move |&c| self.kind(c) == kind)
    }
    pub fn has_child_of_kind(&self, id: NodeId, kind: &str) -> bool {
        self.children_of_kind(id, kind).next().is_some()
    }
    pub fn has_child_text(&self, id: NodeId, text: &str) -> bool {
        self.n(id).children.iter().any(|&c| self.text(c) == text)
    }
    pub fn descendant_text(&self, id: NodeId, kind: &str, text: &str) -> bool {
        let mut stack = vec![id];
        while let Some(node) = stack.pop() {
            if self.kind(node) == kind && self.text(node) == text {
                return true;
            }
            stack.extend(self.n(node).children.iter().rev());
        }
        false
    }
    pub fn next_sibling(&self, id: NodeId) -> Option<NodeId> {
        let sibs = &self.n(self.parent(id)?).children;
        sibs.get(sibs.iter().position(|&c| c == id)? + 1).copied()
    }
    pub fn next_sibling_of_kind(&self, id: NodeId, kind: &str) -> Option<NodeId> {
        let sibs = &self.n(self.parent(id)?).children;
        let pos = sibs.iter().position(|&c| c == id)?;
        sibs[pos + 1..]
            .iter()
            .copied()
            .find(|&s| self.kind(s) == kind)
    }

    // ── Mutation ────────────────────────────────────────────────

    pub fn insert_child(&mut self, parent: NodeId, kind: &str, text: &str) -> NodeId {
        let (sb, eb) = (self.n(parent).start_byte, self.n(parent).end_byte);
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

    // ── Declarative rewrites ──────────────────────────────────────

    fn matching_ids(&self, kind: &str, pred: Option<&crate::predicate::Pred>) -> Vec<NodeId> {
        let pred = match pred {
            None => return self.nodes_of_kind(kind).collect(),
            Some(p) => p,
        };
        let root = crate::Root::doc(self.clone());
        self.nodes_of_kind(kind)
            .filter(|&id| {
                pred.test(&root.adopt(SyntaxNodeRef {
                    tree: root.inner(),
                    id,
                }))
            })
            .collect()
    }

    pub fn apply_rewrites(&mut self, rules: &[Rule]) {
        for rule in rules {
            if let Act::Custom(f) = &rule.act {
                f(self);
                continue;
            }
            let ids = self.matching_ids(rule.kind, rule.cond.as_ref());
            let mut inserts: Vec<(NodeId, &str, String)> = Vec::new();
            let mut renames: Vec<(NodeId, &str)> = Vec::new();
            let mut texts: Vec<(NodeId, String)> = Vec::new();

            match &rule.act {
                Act::SetKind(k) => {
                    for id in ids {
                        renames.push((id, k));
                    }
                }
                Act::SetText(tx) => {
                    for id in ids {
                        let t = tx(self.text(id));
                        if t != self.text(id) && !t.is_empty() {
                            texts.push((id, t));
                        }
                    }
                }
                Act::Insert(extract, target) => {
                    let root = crate::Root::doc(self.clone());
                    for id in ids {
                        let node = root.adopt(SyntaxNodeRef {
                            tree: root.inner(),
                            id,
                        });
                        let mut results = extract.apply_all(&node);
                        if rule.skip > 0 {
                            results = results.into_iter().skip(rule.skip).collect();
                        }
                        if rule.limit > 0 {
                            results.truncate(rule.limit);
                        }
                        for text in results {
                            inserts.push((id, target, text));
                        }
                    }
                }
                Act::MoveChildren { source, target, tx } => {
                    for id in ids {
                        let vals: Vec<String> = self
                            .children_of_kind(id, source)
                            .map(|c| tx(self.text(c)))
                            .filter(|s| !s.is_empty())
                            .collect();
                        let dest = self
                            .children(id)
                            .iter()
                            .copied()
                            .rev()
                            .find(|&c| self.kind(c) != *source && self.kind(c) != "comment");
                        if let Some(d) = dest {
                            for v in vals {
                                inserts.push((d, target, v));
                            }
                        }
                    }
                }
                Act::SetFieldText {
                    field_name,
                    extract,
                } => {
                    let root = crate::Root::doc(self.clone());
                    for id in ids {
                        let node = root.adopt(SyntaxNodeRef {
                            tree: root.inner(),
                            id,
                        });
                        if let Some(text) = extract.apply(&node) {
                            if let Some(field_id) = self.field(id, field_name) {
                                texts.push((field_id, text));
                            }
                        }
                    }
                }
                Act::Custom(_) => unreachable!(),
            }
            for (id, kind) in renames {
                self.set_kind(id, kind);
            }
            for (parent, kind, text) in inserts {
                self.insert_child(parent, kind, &text);
            }
            for (id, text) in texts {
                self.set_text(id, &text);
            }
        }
    }
}

// ── Rewrite rules ───────────────────────────────────────────────

#[derive(Clone)]
pub struct Rule {
    pub kind: &'static str,
    pub cond: Option<crate::predicate::Pred>,
    pub act: Act,
    pub skip: usize,
    pub limit: usize,
}

#[derive(Clone)]
pub enum Act {
    SetKind(&'static str),
    SetText(fn(&str) -> String),
    Insert(crate::extract::Extract, &'static str),
    /// Move children of `source` kind (transformed) as virtual children of the
    /// last non-source, non-comment child. For decorators.
    MoveChildren {
        source: &'static str,
        target: &'static str,
        tx: fn(&str) -> String,
    },
    /// Navigate to a field on the matched node and set its text using an Extract
    /// applied to the same node. For Ruby send rewrite.
    SetFieldText {
        field_name: &'static str,
        extract: crate::extract::Extract,
    },
    Custom(fn(&mut SyntaxTree)),
}

impl Rule {
    fn new(kind: &'static str, act: Act) -> Self {
        Self {
            kind,
            cond: None,
            act,
            skip: 0,
            limit: 0,
        }
    }
    pub fn when(mut self, c: crate::predicate::Pred) -> Self {
        self.cond = Some(c);
        self
    }
    pub fn skip(mut self, n: usize) -> Self {
        self.skip = n;
        self
    }
    pub fn first(mut self) -> Self {
        self.limit = 1;
        self
    }
}

pub fn rename(source: &'static str, target: &'static str) -> Rule {
    Rule::new(source, Act::SetKind(target))
}
pub fn insert(
    source: &'static str,
    extract: crate::extract::Extract,
    target: &'static str,
) -> Rule {
    Rule::new(source, Act::Insert(extract, target))
}
pub fn set_text(kind: &'static str, tx: fn(&str) -> String) -> Rule {
    Rule::new(kind, Act::SetText(tx))
}
pub fn move_children(
    parent: &'static str,
    source: &'static str,
    target: &'static str,
    tx: fn(&str) -> String,
) -> Rule {
    Rule::new(parent, Act::MoveChildren { source, target, tx })
}

pub fn set_field_text(
    kind: &'static str,
    field_name: &'static str,
    extract: crate::extract::Extract,
) -> Rule {
    Rule::new(
        kind,
        Act::SetFieldText {
            field_name,
            extract,
        },
    )
}

pub fn custom(f: fn(&mut SyntaxTree)) -> Rule {
    Rule::new("", Act::Custom(f))
}

// Transforms (used by Extract.strip_prefix / .trim_start_char and SetText rules)
pub fn trim_backslash(s: &str) -> String {
    s.trim_start_matches('\\').to_string()
}
pub fn strip_at(s: &str) -> String {
    s.trim_start_matches('@').trim().to_string()
}

// ── SgNode / Doc ────────────────────────────────────────────────

fn byte_to_row_col(src: &[u8], byte: usize) -> (usize, usize) {
    let (mut row, mut col) = (0, 0);
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
    fn n(&self) -> &'a SyntaxNode {
        self.tree.n(self.id)
    }
}

impl<'a> SgNode<'a> for SyntaxNodeRef<'a> {
    fn parent(&self) -> Option<Self> {
        self.tree.parent(self.id).map(|id| self.at(id))
    }
    fn children(&self) -> impl ExactSizeIterator<Item = Self> {
        let tree = self.tree;
        self.n()
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
        self.n().start_byte as usize..self.n().end_byte as usize
    }
    fn start_pos(&self) -> Position {
        let n = self.n();
        let (r, c) = byte_to_row_col(self.tree.source.as_bytes(), n.start_byte as usize);
        Position::new(r, c, n.start_byte as usize)
    }
    fn end_pos(&self) -> Position {
        let n = self.n();
        let (r, c) = byte_to_row_col(self.tree.source.as_bytes(), n.end_byte as usize);
        Position::new(r, c, n.end_byte as usize)
    }
    fn is_named(&self) -> bool {
        self.n().is_named
    }
    fn is_leaf(&self) -> bool {
        self.n().children.is_empty()
    }
    fn field(&self, name: &str) -> Option<Self> {
        self.tree.field(self.id, name).map(|id| self.at(id))
    }
    fn next(&self) -> Option<Self> {
        self.tree.next_sibling(self.id).map(|id| self.at(id))
    }
    fn prev(&self) -> Option<Self> {
        let sibs = &self.tree.n(self.tree.parent(self.id)?).children;
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

    fn py(src: &str) -> SyntaxTree {
        let ts = SupportLang::Python.ast_grep(src);
        SyntaxTree::from_tree_sitter(src, &ts.doc.tree, SupportLang::Python)
    }

    #[test]
    fn round_trip() {
        let root = Root::doc(py("def foo(x):\n    return x + 1"));
        let func = root.root().children().next().unwrap();
        assert_eq!(func.kind().as_ref(), "function_definition");
        assert_eq!(func.field("name").unwrap().text().as_ref(), "foo");
    }

    #[test]
    fn insert_child() {
        let mut tree = py("class Foo: pass");
        let cls = tree
            .children_of_kind(tree.root_id(), "class_definition")
            .next()
            .unwrap();
        tree.insert_child(cls, "__prop", "bar");
        let root = Root::doc(tree);
        let last = root
            .root()
            .children()
            .next()
            .unwrap()
            .children()
            .last()
            .unwrap();
        assert_eq!(
            (last.kind().as_ref(), last.text().as_ref()),
            ("__prop", "bar")
        );
    }

    #[test]
    fn set_kind() {
        let mut tree = py("class Foo: pass");
        let cls = tree
            .children_of_kind(tree.root_id(), "class_definition")
            .next()
            .unwrap();
        tree.set_kind(cls, "__enum");
        assert_eq!(tree.kind(cls), "__enum");
    }

    #[test]
    fn set_text() {
        let mut tree = py("x = 1");
        let id = tree.nodes_of_kind("identifier").next().unwrap();
        tree.set_text(id, "replaced");
        assert_eq!(tree.text(id), "replaced");
    }

    #[test]
    fn remove() {
        let mut tree = py("x = 1");
        let root = tree.root_id();
        let before = tree.children(root).len();
        tree.remove(tree.children(root)[0]);
        assert_eq!(tree.children(root).len(), before - 1);
    }

    #[test]
    fn extract_works() {
        use crate::extract::field;
        let root = Root::doc(py("def foo(x): return x"));
        let func = root.root().children().next().unwrap();
        assert_eq!(field("name").apply(&func), Some("foo".to_string()));
    }
}
