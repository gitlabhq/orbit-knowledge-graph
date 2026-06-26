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

    pub fn apply_rewrites(&mut self, rules: &[RewriteRule]) {
        for rule in rules {
            match rule {
                RewriteRule::Rename {
                    source,
                    condition,
                    target,
                } => {
                    let ids: Vec<_> = self.nodes_of_kind(source).collect();
                    for id in ids {
                        if condition.test(self, id) {
                            self.set_kind(id, target);
                        }
                    }
                }
                RewriteRule::Collect {
                    parent_kind,
                    field,
                    child_kinds,
                    shallow,
                    target_kind,
                    transform,
                } => {
                    let parents: Vec<_> = self.nodes_of_kind(parent_kind).collect();
                    let mut inserts: Vec<(NodeId, String)> = Vec::new();
                    for parent in parents {
                        let container = if field.is_empty() {
                            parent
                        } else {
                            match self.field(parent, field) {
                                Some(c) => c,
                                None => continue,
                            }
                        };
                        if *shallow {
                            self.collect_shallow_into(
                                container,
                                child_kinds,
                                &mut inserts,
                                parent,
                                *transform,
                            );
                        } else {
                            for &child in self.children(container) {
                                if child_kinds.contains(&self.kind(child)) {
                                    let text = apply_transform(self.text(child), *transform);
                                    if !text.is_empty() {
                                        inserts.push((parent, text));
                                    }
                                }
                            }
                        }
                    }
                    for (parent, text) in inserts {
                        self.insert_child(parent, target_kind, &text);
                    }
                }
                RewriteRule::ExpandSymbols {
                    method_names,
                    target_kind,
                    skip,
                    limit,
                    include_strings,
                } => {
                    let calls: Vec<_> = self.nodes_of_kind("call").collect();
                    let mut inserts: Vec<(NodeId, String)> = Vec::new();
                    for call in calls {
                        let method = self.field_text(call, "method").unwrap_or_default();
                        if !method_names.contains(&method) {
                            continue;
                        }
                        let Some(args) = self.field(call, "arguments") else {
                            continue;
                        };
                        let mut matched = 0usize;
                        let mut skipped = 0usize;
                        for &arg in self.children(args) {
                            let name = match self.kind(arg) {
                                "simple_symbol" => self
                                    .text(arg)
                                    .strip_prefix(':')
                                    .filter(|s| !s.is_empty())
                                    .map(|s| s.to_string()),
                                "string" if *include_strings => self
                                    .children_of_kind(arg, "string_content")
                                    .next()
                                    .map(|c| self.text(c).to_string())
                                    .filter(|s| !s.is_empty()),
                                _ => None,
                            };
                            let Some(name) = name else { continue };
                            if skipped < *skip {
                                skipped += 1;
                                continue;
                            }
                            inserts.push((call, name));
                            matched += 1;
                            if *limit > 0 && matched >= *limit {
                                break;
                            }
                        }
                    }
                    for (call, text) in inserts {
                        self.insert_child(call, target_kind, &text);
                    }
                }
                RewriteRule::ExtractImport {
                    source_kind,
                    target_kind,
                    path_child,
                    transform,
                } => {
                    let nodes: Vec<_> = self.nodes_of_kind(source_kind).collect();
                    let mut inserts: Vec<(NodeId, String)> = Vec::new();
                    for node in nodes {
                        if let Some(pc) = self.children_of_kind(node, path_child).next() {
                            let text = apply_transform(self.text(pc), *transform);
                            if !text.is_empty() {
                                inserts.push((node, text));
                            }
                        }
                    }
                    for (node, text) in inserts {
                        self.set_kind(node, target_kind);
                        self.insert_child(node, "__import_path", &text);
                    }
                }
                RewriteRule::RewriteText { kind, transform } => {
                    let ids: Vec<_> = self.nodes_of_kind(kind).collect();
                    let mut changes: Vec<(NodeId, String)> = Vec::new();
                    for id in ids {
                        let text = apply_transform(self.text(id), *transform);
                        if text != self.text(id) && !text.is_empty() {
                            changes.push((id, text));
                        }
                    }
                    for (id, text) in changes {
                        self.set_text(id, &text);
                    }
                }
            }
        }
    }

    fn collect_shallow_into(
        &self,
        node: NodeId,
        kinds: &[&str],
        out: &mut Vec<(NodeId, String)>,
        target: NodeId,
        transform: fn(&str) -> String,
    ) {
        for &child in self.children(node) {
            if kinds.contains(&self.kind(child)) {
                let text = apply_transform(self.text(child), transform);
                if !text.is_empty() {
                    out.push((target, text));
                }
            } else {
                self.collect_shallow_into(child, kinds, out, target, transform);
            }
        }
    }
}

fn apply_transform(s: &str, f: fn(&str) -> String) -> String {
    f(s)
}

pub fn identity(s: &str) -> String {
    s.to_string()
}
pub fn strip_colon(s: &str) -> String {
    s.strip_prefix(':').unwrap_or(s).to_string()
}
pub fn trim_quotes(s: &str) -> String {
    s.trim_matches(|c: char| c == '"' || c == '\'').to_string()
}
pub fn trim_backslash(s: &str) -> String {
    s.trim_start_matches('\\').to_string()
}
pub fn strip_star(s: &str) -> String {
    s.strip_prefix('*').unwrap_or(s).to_string()
}

// ── Rewrite rules ───────────────────────────────────────────────

#[derive(Clone)]
pub enum RewriteCondition {
    Always,
    HasChildOfKind(&'static str),
    HasChildText(&'static str),
    DescendantText(&'static str, &'static str),
}

impl RewriteCondition {
    fn test(&self, tree: &SyntaxTree, id: NodeId) -> bool {
        match self {
            Self::Always => true,
            Self::HasChildOfKind(k) => tree.has_child_of_kind(id, k),
            Self::HasChildText(t) => tree.has_child_text(id, t),
            Self::DescendantText(k, t) => tree.descendant_text(id, k, t),
        }
    }
}

#[derive(Clone)]
pub enum RewriteRule {
    Rename {
        source: &'static str,
        condition: RewriteCondition,
        target: &'static str,
    },
    Collect {
        parent_kind: &'static str,
        field: &'static str,
        child_kinds: &'static [&'static str],
        shallow: bool,
        target_kind: &'static str,
        transform: fn(&str) -> String,
    },
    ExpandSymbols {
        method_names: &'static [&'static str],
        target_kind: &'static str,
        skip: usize,
        limit: usize,
        include_strings: bool,
    },
    ExtractImport {
        source_kind: &'static str,
        target_kind: &'static str,
        path_child: &'static str,
        transform: fn(&str) -> String,
    },
    RewriteText {
        kind: &'static str,
        transform: fn(&str) -> String,
    },
}

// ── Builders ────────────────────────────────────────────────────

pub struct RenameBuilder {
    source: &'static str,
    condition: RewriteCondition,
}
pub struct CollectBuilder {
    parent: &'static str,
    field: &'static str,
    kinds: &'static [&'static str],
    shallow: bool,
    transform: fn(&str) -> String,
}
pub struct ExpandBuilder {
    methods: &'static [&'static str],
    skip: usize,
    limit: usize,
    include_strings: bool,
}

pub fn rename(source: &'static str) -> RenameBuilder {
    RenameBuilder {
        source,
        condition: RewriteCondition::Always,
    }
}
pub fn collect(parent: &'static str, field: &'static str) -> CollectBuilder {
    CollectBuilder {
        parent,
        field,
        kinds: &[],
        shallow: false,
        transform: identity,
    }
}
pub fn collect_self(parent: &'static str) -> CollectBuilder {
    CollectBuilder {
        parent,
        field: "",
        kinds: &[],
        shallow: false,
        transform: identity,
    }
}
pub fn expand(methods: &'static [&'static str]) -> ExpandBuilder {
    ExpandBuilder {
        methods,
        skip: 0,
        limit: 0,
        include_strings: false,
    }
}
pub fn extract_import(source: &'static str, path_child: &'static str) -> RewriteRule {
    RewriteRule::ExtractImport {
        source_kind: source,
        target_kind: source,
        path_child,
        transform: identity,
    }
}
pub fn rewrite_text(kind: &'static str, transform: fn(&str) -> String) -> RewriteRule {
    RewriteRule::RewriteText { kind, transform }
}

impl RenameBuilder {
    pub fn when(mut self, cond: RewriteCondition) -> Self {
        self.condition = cond;
        self
    }
    pub fn to(self, target: &'static str) -> RewriteRule {
        RewriteRule::Rename {
            source: self.source,
            condition: self.condition,
            target,
        }
    }
}

impl CollectBuilder {
    pub fn kinds(mut self, k: &'static [&'static str]) -> Self {
        self.kinds = k;
        self
    }
    pub fn shallow(mut self) -> Self {
        self.shallow = true;
        self
    }
    pub fn transform(mut self, f: fn(&str) -> String) -> Self {
        self.transform = f;
        self
    }
    pub fn as_child(self, target: &'static str) -> RewriteRule {
        RewriteRule::Collect {
            parent_kind: self.parent,
            field: self.field,
            child_kinds: self.kinds,
            shallow: self.shallow,
            target_kind: target,
            transform: self.transform,
        }
    }
}

impl ExpandBuilder {
    pub fn skip(mut self, n: usize) -> Self {
        self.skip = n;
        self
    }
    pub fn first(mut self) -> Self {
        self.limit = 1;
        self
    }
    pub fn with_strings(mut self) -> Self {
        self.include_strings = true;
        self
    }
    pub fn as_child(self, target: &'static str) -> RewriteRule {
        RewriteRule::ExpandSymbols {
            method_names: self.methods,
            target_kind: target,
            skip: self.skip,
            limit: self.limit,
            include_strings: self.include_strings,
        }
    }
}

pub fn has_child(kind: &'static str) -> RewriteCondition {
    RewriteCondition::HasChildOfKind(kind)
}
pub fn child_text(text: &'static str) -> RewriteCondition {
    RewriteCondition::HasChildText(text)
}
pub fn descendant_text(kind: &'static str, text: &'static str) -> RewriteCondition {
    RewriteCondition::DescendantText(kind, text)
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
