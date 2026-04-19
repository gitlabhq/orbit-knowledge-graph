//! Composable node extraction pipelines for tree-sitter ASTs.
//!
//! An [`Extract`] is a chain of [`Step`]s that navigate through a CST,
//! followed by an [`Emit`] that produces a string from the final node.
//!
//! Every step is just `(Axis, Match)` — the same two enums used by
//! `Node::find`. A step is either **required** (pipeline fails on miss)
//! or **optional** (stays at current node on miss).
//!
//! ```ignore
//! use treesitter_visit::extract::*;
//!
//! field("name")
//! field("function").field("object")
//! field("receiver")
//!     .child_of_kind("parameter_declaration")
//!     .field("type")
//!     .inner("pointer_type", "type_identifier")
//! ```

use crate::node::{Axis, Match, Node};
use crate::source::Doc;
use smallvec::SmallVec;

// ── Core types ──────────────────────────────────────────────────

/// A single navigation step: `(Axis, Match)`, required or optional.
#[derive(Clone)]
pub enum Step {
    /// Must succeed or pipeline returns None.
    Nav(Axis<'static>, Match<'static>),
    /// Stay at current node on failure.
    Try(Axis<'static>, Match<'static>),
    /// Fail pipeline if current node doesn't match (no navigation).
    Where(Match<'static>),
}

/// How to produce a string from the final node.
#[derive(Clone)]
pub enum Emit {
    Text,
    None,
    /// Try `field("name")`, then first child matching these kinds.
    Name(&'static [&'static str]),
    /// Collect text of all children matching this criterion.
    Children(Match<'static>),
}

pub const IDENT_KINDS: &[&str] = &[
    "identifier",
    "type_identifier",
    "simple_identifier",
    "scoped_identifier",
    "name",
    "field_identifier",
    "property_identifier",
];

/// A pipeline: navigation steps + terminal extraction.
#[derive(Clone)]
pub struct Extract {
    steps: SmallVec<[Step; 4]>,
    emit: Emit,
}

// ── Constructors ────────────────────────────────────────────────

pub fn field(name: &'static str) -> Extract {
    Extract::from_step(Step::Nav(Axis::Field(name), Match::Any))
}

pub fn field_chain(fields: &'static [&'static str]) -> Extract {
    let mut e = Extract::terminal(Emit::Text);
    for &f in fields {
        e = e.field(f);
    }
    e
}

pub fn child_of_kind(kind: &'static str) -> Extract {
    Extract::from_step(Step::Nav(Axis::Child, Match::Kind(kind)))
}

pub fn descendant(kind: &'static str) -> Extract {
    Extract::from_step(Step::Nav(Axis::Descendant, Match::Kind(kind)))
}

pub fn text() -> Extract {
    Extract::terminal(Emit::Text)
}

pub fn no_extract() -> Extract {
    Extract::terminal(Emit::None)
}

pub fn default_name() -> Extract {
    Extract::terminal(Emit::Name(IDENT_KINDS))
}

pub fn name_or_ident(ident_kinds: &'static [&'static str]) -> Extract {
    Extract::terminal(Emit::Name(ident_kinds))
}

// ── Chaining ────────────────────────────────────────────────────

impl Extract {
    fn from_step(step: Step) -> Self {
        Self {
            steps: SmallVec::from_elem(step, 1),
            emit: Emit::Text,
        }
    }

    /// Start a pipeline with a single required navigation step.
    pub fn one(axis: Axis<'static>, m: Match<'static>) -> Self {
        Self::from_step(Step::Nav(axis, m))
    }

    fn terminal(emit: Emit) -> Self {
        Self {
            steps: SmallVec::new(),
            emit,
        }
    }

    fn push(mut self, step: Step) -> Self {
        self.steps.push(step);
        self
    }

    // Required steps
    pub fn field(self, name: &'static str) -> Self {
        self.push(Step::Nav(Axis::Field(name), Match::Any))
    }
    pub fn child_of_kind(self, kind: &'static str) -> Self {
        self.push(Step::Nav(Axis::Child, Match::Kind(kind)))
    }
    pub fn descendant(self, kind: &'static str) -> Self {
        self.push(Step::Nav(Axis::Descendant, Match::Kind(kind)))
    }
    pub fn parent(self) -> Self {
        self.push(Step::Nav(Axis::Parent, Match::Any))
    }
    pub fn first_named(self) -> Self {
        self.push(Step::Nav(Axis::Child, Match::Named))
    }
    pub fn prev_sibling(self, kind: &'static str) -> Self {
        self.push(Step::Nav(Axis::PrevSibling, Match::Kind(kind)))
    }
    pub fn next_sibling(self, kind: &'static str) -> Self {
        self.push(Step::Nav(Axis::NextSibling, Match::Kind(kind)))
    }
    pub fn ancestor(self, kind: &'static str) -> Self {
        self.push(Step::Nav(Axis::Ancestor, Match::Kind(kind)))
    }
    pub fn nav(self, axis: Axis<'static>, m: Match<'static>) -> Self {
        self.push(Step::Nav(axis, m))
    }

    // Optional steps (stay at current node on failure)
    pub fn try_field(self, name: &'static str) -> Self {
        self.push(Step::Try(Axis::Field(name), Match::Any))
    }
    pub fn try_child(self, kind: &'static str) -> Self {
        self.push(Step::Try(Axis::Child, Match::Kind(kind)))
    }
    pub fn try_descendant(self, kind: &'static str) -> Self {
        self.push(Step::Try(Axis::Descendant, Match::Kind(kind)))
    }
    pub fn try_nav(self, axis: Axis<'static>, m: Match<'static>) -> Self {
        self.push(Step::Try(axis, m))
    }

    // Filter (validate current node without navigating)
    pub fn where_(self, m: Match<'static>) -> Self {
        self.push(Step::Where(m))
    }

    // Emit control
    pub fn or_default_name(mut self) -> Self {
        self.emit = Emit::Name(IDENT_KINDS);
        self
    }
    pub fn or_ident(mut self, kinds: &'static [&'static str]) -> Self {
        self.emit = Emit::Name(kinds);
        self
    }
    pub fn suppress(mut self) -> Self {
        self.emit = Emit::None;
        self
    }

    /// Collect text of all children matching this criterion.
    /// Use with `apply_all()` instead of `apply()`.
    pub fn collect(mut self, m: Match<'static>) -> Self {
        self.emit = Emit::Children(m);
        self
    }

    // Composition
    pub fn inner(self, container: &'static str, target: &'static str) -> Self {
        self.try_child(container).try_descendant(target)
    }
    pub fn then(mut self, next: Extract) -> Self {
        self.steps.extend(next.steps);
        self.emit = next.emit;
        self
    }
}

// ── Execution ───────────────────────────────────────────────────

impl Extract {
    pub fn apply<D: Doc>(&self, node: &Node<'_, D>) -> Option<String> {
        let target = self.navigate(node)?;
        emit(&self.emit, &target)
    }

    /// Navigate + extract, then transform the result with access to the
    /// origin node. The origin node gives full tree context — walk
    /// ancestors for scope, siblings for decorators, anything.
    pub fn apply_with<D: Doc>(
        &self,
        node: &Node<'_, D>,
        transform: impl Fn(String, &Node<'_, D>) -> String,
    ) -> Option<String> {
        let target = self.navigate(node)?;
        let raw = emit(&self.emit, &target)?;
        Some(transform(raw, node))
    }

    /// Navigate, then collect all children matching the `Emit::Children`
    /// criterion. Returns empty vec on navigation failure or non-Children emit.
    pub fn apply_all<D: Doc>(&self, node: &Node<'_, D>) -> Vec<String> {
        let Some(target) = self.navigate(node) else {
            return vec![];
        };
        emit_all(&self.emit, &target)
    }

    /// Like `apply_all`, but transform each collected string with tree context.
    pub fn apply_all_with<D: Doc>(
        &self,
        node: &Node<'_, D>,
        transform: impl Fn(String, &Node<'_, D>) -> String,
    ) -> Vec<String> {
        let Some(target) = self.navigate(node) else {
            return vec![];
        };
        emit_all(&self.emit, &target)
            .into_iter()
            .map(|s| transform(s, node))
            .collect()
    }

    pub fn navigate<'r, D: Doc>(&self, node: &Node<'r, D>) -> Option<Node<'r, D>> {
        let mut cur = node.clone();
        for step in &self.steps {
            match step {
                Step::Nav(axis, m) => cur = cur.find(*axis, *m)?,
                Step::Try(axis, m) => {
                    if let Some(next) = cur.find(*axis, *m) {
                        cur = next;
                    }
                }
                Step::Where(m) => {
                    if !m.test(&cur) {
                        return None;
                    }
                }
            }
        }
        Some(cur)
    }
}

fn emit<D: Doc>(mode: &Emit, node: &Node<'_, D>) -> Option<String> {
    match mode {
        Emit::Text => Some(node.text().to_string()),
        Emit::None => None,
        Emit::Name(ident_kinds) => {
            if let Some(n) = node.field("name") {
                return Some(n.text().to_string());
            }
            for child in node.children() {
                if child.is_named() {
                    let k = child.kind();
                    if ident_kinds.iter().any(|ik| *ik == k.as_ref()) {
                        return Some(child.text().to_string());
                    }
                }
            }
            None
        }
        Emit::Children(_) => {
            // Single-value fallback: return first match
            emit_all(mode, node).into_iter().next()
        }
    }
}

fn emit_all<D: Doc>(mode: &Emit, node: &Node<'_, D>) -> Vec<String> {
    match mode {
        Emit::Children(m) => node
            .children()
            .filter(|c| m.test(c))
            .map(|c| c.text().to_string())
            .collect(),
        // For non-Children emit, produce 0 or 1 element
        other => emit(other, node).into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LanguageExt, SupportLang};

    #[test]
    fn test_field() {
        let root = SupportLang::Python.ast_grep("def bar(): pass");
        let func = root.root().children().next().unwrap();
        assert_eq!(field("name").apply(&func), Some("bar".to_string()));
    }

    #[test]
    fn test_default_name() {
        let root = SupportLang::Python.ast_grep("def foo(): pass");
        let func = root.root().children().next().unwrap();
        assert_eq!(default_name().apply(&func), Some("foo".to_string()));
    }

    #[test]
    fn test_chained() {
        let root = SupportLang::Python.ast_grep("class Foo:\n    def bar(self): pass");
        let cls = root.root().children().next().unwrap();
        assert_eq!(
            field("body")
                .descendant("function_definition")
                .field("name")
                .apply(&cls),
            Some("bar".to_string()),
        );
    }

    #[test]
    fn test_inner_succeeds() {
        let root = SupportLang::Java.ast_grep("class Foo { List<UserService> field; }");
        let cls = root.root().children().next().unwrap();
        assert_eq!(
            descendant("generic_type")
                .inner("type_arguments", "type_identifier")
                .apply(&cls),
            Some("UserService".to_string()),
        );
    }

    #[test]
    fn test_inner_falls_through() {
        let root = SupportLang::Java.ast_grep("class Foo { String field; }");
        let cls = root.root().children().next().unwrap();
        assert_eq!(
            descendant("type_identifier")
                .inner("type_arguments", "type_identifier")
                .apply(&cls),
            Some("String".to_string()),
        );
    }

    #[test]
    fn test_then() {
        let root = SupportLang::Python.ast_grep("class Foo:\n    def bar(self): pass");
        let cls = root.root().children().next().unwrap();
        assert_eq!(
            field("body")
                .then(descendant("function_definition").field("name"))
                .apply(&cls),
            Some("bar".to_string()),
        );
    }

    #[test]
    fn test_navigate() {
        let root = SupportLang::Python.ast_grep("def foo(): pass");
        let func = root.root().children().next().unwrap();
        let node = field("name").navigate(&func).unwrap();
        assert_eq!(node.text().as_ref(), "foo");
    }

    #[test]
    fn test_apply_with_computes_fqn() {
        let code = "class Outer:\n    class Inner:\n        def method(self): pass";
        let root = SupportLang::Python.ast_grep(code);
        let method = root
            .root()
            .find(Axis::Descendant, Match::Kind("function_definition"))
            .unwrap();

        // Extract the method name, then compute FQN from ancestors
        let fqn = field("name").apply_with(&method, |name, origin| {
            let mut scope = Vec::new();
            for ancestor in origin.parent_chain() {
                if Match::AnyKind(&["class_definition", "function_definition"]).test(&ancestor)
                    && let Some(n) = ancestor.field("name")
                {
                    scope.push(n.text().to_string());
                }
            }
            scope.reverse();
            scope.push(name);
            scope.join(".")
        });

        assert_eq!(fqn, Some("Outer.Inner.method".to_string()));
    }

    #[test]
    fn test_apply_all_collects_children() {
        let code = "class Foo:\n    def a(self): pass\n    def b(self): pass\n    x = 1";
        let root = SupportLang::Python.ast_grep(code);
        let cls = root.root().children().next().unwrap();

        // Collect all function_definition names from the class body
        let methods = field("body")
            .collect(Match::Kind("function_definition"))
            .apply_all(&cls);
        // text() of function_definition nodes includes full "def a(self): pass"
        assert_eq!(methods.len(), 2);
        assert!(methods[0].contains("a"));
        assert!(methods[1].contains("b"));
    }

    #[test]
    fn test_apply_all_with_transforms() {
        let code = "class Foo:\n    def a(self): pass\n    def b(self): pass";
        let root = SupportLang::Python.ast_grep(code);
        let cls = root.root().children().next().unwrap();

        let fqns = field("body")
            .collect(Match::Kind("function_definition"))
            .apply_all_with(&cls, |method_text, origin| {
                let cls_name = origin.field("name").unwrap().text().to_string();
                // Just extract function name from the full text
                let fn_name = method_text
                    .split('(')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .strip_prefix("def ")
                    .unwrap_or("")
                    .trim();
                format!("{cls_name}.{fn_name}")
            });
        assert_eq!(fqns, vec!["Foo.a", "Foo.b"]);
    }
}
