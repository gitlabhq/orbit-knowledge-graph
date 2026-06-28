//! Composable node extraction pipelines for tree-sitter ASTs.
//!
//! An [`Extract`] is a chain of [`Step`]s that navigate through a CST,
//! followed by an [`Emit`] that produces one or more strings from the final
//! node, optionally rewritten by a chain of [`TextTransform`]s.
//!
//! A step navigates one of three ways: a required `(Axis, Match)` nav, an
//! optional `Try` nav, or a `Where`/`WherePred` filter that tests the current
//! node without moving. Filtering reuses the same [`Match`] and [`Pred`]
//! vocabulary as `Node::find` and the predicate module — there is no second
//! matching language.
//!
//! Set-producing emits are unified under [`Emit::Each`]: run an inner pipeline
//! per child and flatten. "Collect children", "collect a field from each
//! child", and arbitrary-depth nesting are all `each(inner)` with a deeper
//! `inner`; the `collect*` builders are thin sugar over it.
//!
//! ```ignore
//! use treesitter_visit::extract::*;
//!
//! field("name")
//! field("function").field("object")
//! // each child of `superclasses` that is an identifier, as a separate string:
//! field("superclasses").collect(Match::Kind("identifier"))
//! ```

use crate::node::{Axis, Match, Node};
use crate::source::Doc;
use smallvec::SmallVec;

#[derive(Clone)]
pub enum Step {
    /// Must succeed or pipeline returns None.
    Nav(Axis<'static>, Match<'static>),
    /// Stay at current node on failure.
    Try(Axis<'static>, Match<'static>),
    /// Fail pipeline if current node doesn't match (no navigation).
    Where(Match<'static>),
    /// Fail pipeline if current node doesn't satisfy the predicate (no navigation).
    WherePred(Box<crate::predicate::Pred>),
    /// Navigate to the n-th match along axis. Negative n counts from end (-1 = last).
    Nth(Axis<'static>, Match<'static>, isize),
}

#[derive(Clone)]
pub enum Emit {
    Text,
    None,
    /// Try `field("name")`, then first child matching these kinds.
    Name(&'static [&'static str]),
    /// For each direct child, run the inner pipeline and flatten the results.
    /// Subsumes "collect children", "collect a field from each child", and
    /// arbitrary-depth nested collection (the inner pipeline navigates deeper).
    Each(Box<Extract>),
    /// For each outermost descendant matching `m`, run the inner pipeline.
    /// DFS stops recursing into a subtree once a node matches `m`.
    EachDescendant(Match<'static>, Box<Extract>),
    Const(&'static str),
    /// Evaluate each part against the origin node, drop empty results, and join
    /// survivors with `sep`. Assembles one string from several navigated values;
    /// use `constant(...)` parts for literals.
    Join(&'static str, Vec<Extract>),
    /// Try the first pipeline; if it yields nothing, fall back to the second.
    OrElse(Box<Extract>, Box<Extract>),
}

#[derive(Clone)]
pub enum TextTransform {
    StripPrefix(&'static str),
    TrimStartChar(char),
    TrimMatches(&'static [char]),
    /// Strip the first matching prefix from a list.
    StripAnyPrefix(&'static [&'static str]),
    /// Split on separator, take last segment.
    SplitLast(&'static str),
    /// Split on separator, take everything before the last segment.
    SplitInit(&'static str),
    /// Take everything before the first occurrence of `sep` (e.g. strip a
    /// generic-argument suffix: `List<int>` → `List`). No-op when absent.
    TakeBefore(&'static str),
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

#[derive(Clone)]
pub struct Extract {
    steps: SmallVec<[Step; 4]>,
    emit: Emit,
    transforms: SmallVec<[TextTransform; 1]>,
}

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

pub fn child_of_text(text: &'static str) -> Extract {
    Extract::from_step(Step::Nav(Axis::Child, Match::Text(text)))
}

pub fn descendant(kind: &'static str) -> Extract {
    Extract::from_step(Step::Nav(Axis::Descendant, Match::Kind(kind)))
}

pub fn text() -> Extract {
    Extract::terminal(Emit::Text)
}

pub fn constant(s: &'static str) -> Extract {
    Extract::terminal(Emit::Const(s))
}

/// Assemble one string from several sub-pipelines evaluated against the same
/// origin node. Empty parts are dropped before joining with `sep`. Use
/// `constant(...)` parts for literal text.
pub fn join(sep: &'static str, parts: Vec<Extract>) -> Extract {
    Extract::terminal(Emit::Join(sep, parts))
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

impl Extract {
    fn from_step(step: Step) -> Self {
        Self {
            steps: SmallVec::from_elem(step, 1),
            emit: Emit::Text,
            transforms: SmallVec::new(),
        }
    }

    pub fn one(axis: Axis<'static>, m: Match<'static>) -> Self {
        Self::from_step(Step::Nav(axis, m))
    }

    pub fn terminal(emit: Emit) -> Self {
        Self {
            steps: SmallVec::new(),
            emit,
            transforms: SmallVec::new(),
        }
    }

    fn push(mut self, step: Step) -> Self {
        self.steps.push(step);
        self
    }

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
    /// Navigate to the n-th match. Negative n counts from end (-1 = last).
    pub fn nth(self, axis: Axis<'static>, m: Match<'static>, n: isize) -> Self {
        self.push(Step::Nth(axis, m, n))
    }

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

    pub fn where_(self, m: Match<'static>) -> Self {
        self.push(Step::Where(m))
    }

    /// Filter the current node with a boolean predicate (no navigation).
    pub fn where_pred(self, p: crate::predicate::Pred) -> Self {
        self.push(Step::WherePred(Box::new(p)))
    }

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

    /// For each direct child, run `inner` (starting at that child) and flatten
    /// the produced strings. `inner` typically opens with `where_(m)` to filter.
    /// Nesting is just a deeper `inner` (e.g. another `each`).
    pub fn each(mut self, inner: Extract) -> Self {
        self.emit = Emit::Each(Box::new(inner));
        self
    }

    /// Collect text of all children matching this criterion.
    pub fn collect(self, m: Match<'static>) -> Self {
        self.each(text().where_(m))
    }

    /// For each outermost descendant matching `m`, run `inner` (starting there).
    /// DFS stops recursing into a subtree once a node matches `m`.
    pub fn each_descendant(mut self, m: Match<'static>, inner: Extract) -> Self {
        self.emit = Emit::EachDescendant(m, Box::new(inner));
        self
    }

    /// Collect text of outermost descendants matching this criterion.
    pub fn collect_shallow(self, m: Match<'static>) -> Self {
        self.each_descendant(m, text())
    }

    /// Collect children matching a criterion, then extract a named field from each.
    pub fn collect_field(self, m: Match<'static>, field_name: &'static str) -> Self {
        self.each(text().where_(m).field(field_name))
    }

    /// For each child matching `outer`, collect THEIR children matching `inner`.
    pub fn collect_nested(self, outer: Match<'static>, inner: Match<'static>) -> Self {
        self.each(
            Extract::terminal(Emit::Text)
                .where_(outer)
                .each(text().where_(inner)),
        )
    }

    /// Strip a prefix from emitted text.
    pub fn strip_prefix(mut self, prefix: &'static str) -> Self {
        self.transforms.push(TextTransform::StripPrefix(prefix));
        self
    }

    /// Trim leading occurrences of a character.
    pub fn trim_start_char(mut self, ch: char) -> Self {
        self.transforms.push(TextTransform::TrimStartChar(ch));
        self
    }

    /// Trim matching characters from both ends.
    pub fn trim_matches(mut self, chars: &'static [char]) -> Self {
        self.transforms.push(TextTransform::TrimMatches(chars));
        self
    }

    /// Strip the first matching prefix from a list.
    pub fn strip_any_prefix(mut self, prefixes: &'static [&'static str]) -> Self {
        self.transforms
            .push(TextTransform::StripAnyPrefix(prefixes));
        self
    }

    /// Split on separator, keep the last segment.
    pub fn split_last(mut self, sep: &'static str) -> Self {
        self.transforms.push(TextTransform::SplitLast(sep));
        self
    }

    /// Split on separator, keep everything before the last segment.
    pub fn split_init(mut self, sep: &'static str) -> Self {
        self.transforms.push(TextTransform::SplitInit(sep));
        self
    }

    /// Keep everything before the first occurrence of `sep`.
    pub fn take_before(mut self, sep: &'static str) -> Self {
        self.transforms.push(TextTransform::TakeBefore(sep));
        self
    }

    /// Try this pipeline; if it produces nothing, fall back to `alt`.
    pub fn or_else(self, alt: Extract) -> Self {
        Extract::terminal(Emit::OrElse(Box::new(self), Box::new(alt)))
    }

    pub fn inner(self, container: &'static str, target: &'static str) -> Self {
        self.try_child(container).try_descendant(target)
    }
    pub fn then(mut self, next: Extract) -> Self {
        self.steps.extend(next.steps);
        self.emit = next.emit;
        self.transforms = next.transforms;
        self
    }
}

impl Extract {
    fn apply_tx(&self, mut s: String) -> String {
        for t in &self.transforms {
            match t {
                TextTransform::StripPrefix(p) => {
                    if let Some(rest) = s.strip_prefix(p) {
                        s = rest.trim().to_string();
                    }
                }
                TextTransform::TrimStartChar(ch) => {
                    s = s.trim_start_matches(*ch).to_string();
                }
                TextTransform::TrimMatches(chars) => {
                    s = s.trim_matches(chars as &[char]).to_string();
                }
                TextTransform::StripAnyPrefix(prefixes) => {
                    for p in *prefixes {
                        if let Some(rest) = s.strip_prefix(p) {
                            s = rest.trim().to_string();
                            break;
                        }
                    }
                }
                TextTransform::SplitLast(sep) => {
                    if let Some((_, last)) = s.rsplit_once(sep) {
                        s = last.to_string();
                    }
                }
                TextTransform::SplitInit(sep) => {
                    if let Some((init, _)) = s.rsplit_once(sep) {
                        s = init.to_string();
                    } else {
                        s = String::new();
                    }
                }
                TextTransform::TakeBefore(sep) => {
                    if let Some((head, _)) = s.split_once(sep) {
                        s = head.to_string();
                    }
                }
            }
        }
        s
    }

    pub fn apply<D: Doc>(&self, node: &Node<'_, D>) -> Option<String> {
        let target = self.navigate(node)?;
        let s = self.apply_tx(emit(&self.emit, &target)?);
        if s.is_empty() { None } else { Some(s) }
    }

    /// The transform receives the *origin* node (not the navigated target),
    /// so it can walk ancestors for scope or siblings for decorators.
    pub fn apply_with<D: Doc>(
        &self,
        node: &Node<'_, D>,
        transform: impl Fn(String, &Node<'_, D>) -> String,
    ) -> Option<String> {
        let target = self.navigate(node)?;
        let raw = emit(&self.emit, &target)?;
        Some(transform(self.apply_tx(raw), node))
    }

    pub fn apply_all<D: Doc>(&self, node: &Node<'_, D>) -> Vec<String> {
        let Some(target) = self.navigate(node) else {
            return vec![];
        };
        emit_all(&self.emit, &target)
            .into_iter()
            .map(|s| self.apply_tx(s))
            .filter(|s| !s.is_empty())
            .collect()
    }

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
            .map(|s| self.apply_tx(s))
            .filter(|s| !s.is_empty())
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
                Step::Nth(axis, m, n) => cur = cur.nth(*axis, *m, *n)?,
                Step::Where(m) => {
                    if !m.test(&cur) {
                        return None;
                    }
                }
                Step::WherePred(p) => {
                    if !p.test(&cur) {
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
        Emit::Each(_) | Emit::EachDescendant(..) => emit_all(mode, node).into_iter().next(),
        Emit::Const(s) => Some(s.to_string()),
        Emit::Join(sep, parts) => {
            let pieces: Vec<String> = parts.iter().filter_map(|p| p.apply(node)).collect();
            (!pieces.is_empty()).then(|| pieces.join(sep))
        }
        Emit::OrElse(a, b) => a.apply(node).or_else(|| b.apply(node)),
    }
}

fn emit_all<D: Doc>(mode: &Emit, node: &Node<'_, D>) -> Vec<String> {
    match mode {
        Emit::Each(inner) => node.children().flat_map(|c| inner.apply_all(&c)).collect(),
        Emit::EachDescendant(m, inner) => {
            let mut hits = Vec::new();
            collect_shallow_rec(node, m, &mut hits);
            hits.iter().flat_map(|c| inner.apply_all(c)).collect()
        }
        other => emit(other, node).into_iter().collect(),
    }
}

fn collect_shallow_rec<'r, D: Doc>(
    node: &Node<'r, D>,
    m: &Match<'_>,
    results: &mut Vec<Node<'r, D>>,
) {
    for child in node.children() {
        if m.test(&child) {
            results.push(child);
        } else {
            collect_shallow_rec(&child, m, results);
        }
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
    fn each_collect_variants() {
        let root = SupportLang::Python.ast_grep("class Foo(Bar, Baz): pass");
        let cls = root.root().children().next().unwrap();
        assert_eq!(
            field("superclasses")
                .collect(Match::AnyKind(&["identifier"]))
                .apply_all(&cls),
            vec!["Bar", "Baz"],
        );
        assert_eq!(
            field("superclasses")
                .collect_shallow(Match::Kind("identifier"))
                .apply_all(&cls),
            vec!["Bar", "Baz"],
        );
    }

    #[test]
    fn join_splices_multiple_sources() {
        let root = SupportLang::Python.ast_grep("class Foo(Bar): pass");
        let cls = root.root().children().next().unwrap();
        assert_eq!(
            join(".", vec![constant("pkg"), field("name")]).apply(&cls),
            Some("pkg.Foo".to_string()),
        );
        // empty parts drop out, leaving no dangling separator
        assert_eq!(
            join(".", vec![field("name"), field("nonexistent")]).apply(&cls),
            Some("Foo".to_string()),
        );
    }

    #[test]
    fn or_else_falls_back() {
        let root = SupportLang::Python.ast_grep("def foo(): pass");
        let func = root.root().children().next().unwrap();
        assert_eq!(
            field("nonexistent").or_else(field("name")).apply(&func),
            Some("foo".to_string()),
        );
        assert_eq!(
            field("name").or_else(constant("fallback")).apply(&func),
            Some("foo".to_string()),
        );
    }

    #[test]
    fn take_before_strips_suffix() {
        let root = SupportLang::Python.ast_grep("def foo(): pass");
        let func = root.root().children().next().unwrap();
        assert_eq!(
            field("name")
                .then(constant("List<int>"))
                .take_before("<")
                .apply(&func),
            Some("List".to_string()),
        );
        // no separator: unchanged
        assert_eq!(
            constant("Plain").take_before("<").apply(&func),
            Some("Plain".to_string()),
        );
    }

    #[test]
    fn test_apply_all_collects_children() {
        let code = "class Foo:\n    def a(self): pass\n    def b(self): pass\n    x = 1";
        let root = SupportLang::Python.ast_grep(code);
        let cls = root.root().children().next().unwrap();

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

#[cfg(test)]
mod each_collect {
    use super::*;
    use crate::node::Match;
    use crate::{LanguageExt, SupportLang};

    #[test]
    fn java_record_params() {
        let root = SupportLang::Java.ast_grep("public record Point(int x, int y) {}");
        let rec = root
            .root()
            .find(Axis::Descendant, Match::Kind("record_declaration"))
            .unwrap();
        let names = field("parameters")
            .collect_field(Match::Kind("formal_parameter"), "name")
            .apply_all(&rec);
        assert_eq!(names, vec!["x", "y"], "got {names:?}");
    }
}
