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

#[derive(Clone)]
pub enum Step {
    /// Must succeed or pipeline returns None.
    Nav(Axis<'static>, Match<'static>),
    /// Stay at current node on failure.
    Try(Axis<'static>, Match<'static>),
    /// Fail pipeline if current node doesn't match (no navigation).
    Where(Match<'static>),
    /// Navigate to the n-th match along axis. Negative n counts from end (-1 = last).
    Nth(Axis<'static>, Match<'static>, isize),
    /// C-family declarator descent, unwrapping wrappers to the declared name.
    /// See [`descend_declarator_name`].
    DeclaratorName,
}

/// C-family declarator wrapper kinds a pointer/parenthesized return inserts
/// between a definition and its name, so a fixed hop count lands on the wrong
/// node. `reference_declarator` and `parenthesized_declarator` carry no
/// `declarator` field (tree-sitter-cpp: `seq(choice('&','&&'), $._declarator)`),
/// unlike the other three.
const C_FAMILY_DECLARATOR_WRAPPERS: &[&str] = &[
    "function_declarator",
    "pointer_declarator",
    "array_declarator",
    "reference_declarator",
    "parenthesized_declarator",
];

/// Descend C/C++ declarator wrappers to the declared-name node, following the
/// `declarator` field or, when absent, the first named child.
///
/// Comments are skipped: they are named nodes that can precede the inner
/// declarator (`int (/*x*/ foo)()`).
fn descend_declarator_name<'r, D: Doc>(node: &Node<'r, D>) -> Option<Node<'r, D>> {
    let mut cur = node.clone();
    while C_FAMILY_DECLARATOR_WRAPPERS.contains(&cur.kind().as_ref()) {
        cur = match cur.field("declarator") {
            Some(inner) => inner,
            None => cur
                .children()
                .find(|c| c.is_named() && c.kind().as_ref() != "comment")?,
        };
    }
    Some(cur)
}

/// Text for a declared-name node: mostly verbatim (keeping any class qualifier
/// that feeds an out-of-line member's FQN), but with a trailing conversion
/// operator normalized to `operator <type>` so its parameter list does not leak
/// into the name.
///
/// The `operator_cast` is normalized at any qualifier depth because
/// `qualified_identifier` is right-recursive: `S::I::operator int()` nests as
/// `qualified_identifier(S, qualified_identifier(I, operator_cast))`.
fn declared_name_text<D: Doc>(node: &Node<'_, D>) -> Option<String> {
    if node.kind().as_ref() == "operator_cast" {
        return operator_cast_text(node);
    }
    if node.kind().as_ref() == "qualified_identifier"
        && let Some(op_cast) = trailing_operator_cast(node)
        && let Some(op) = operator_cast_text(&op_cast)
    {
        let text = node.text();
        let qualifier = &text[..op_cast.range().start - node.range().start];
        return Some(format!("{qualifier}{op}"));
    }
    Some(node.text().to_string())
}

/// The `operator_cast` at the end of a (possibly nested) `qualified_identifier`,
/// or `None` if the final component is a plain name.
fn trailing_operator_cast<'r, D: Doc>(node: &Node<'r, D>) -> Option<Node<'r, D>> {
    let mut cur = node.clone();
    while cur.kind().as_ref() == "qualified_identifier" {
        cur = cur.children().filter(|c| c.is_named()).last()?;
    }
    (cur.kind().as_ref() == "operator_cast").then_some(cur)
}

/// `operator_cast` → `operator <type>`, keeping pointer/reference/cv decorations
/// (`operator const char*`) but dropping the function declarator (`() const`).
/// The type is the text up to the operator's own `parameter_list`, so a type
/// that embeds its own (`operator std::function<void(int)>`) is preserved.
fn operator_cast_text<D: Doc>(node: &Node<'_, D>) -> Option<String> {
    let params = function_parameter_list(node)?;
    let text = node.text();
    let end = params.range().start - node.range().start;
    Some(text[..end].trim_end().to_string())
}

/// The `parameter_list` of the `abstract_function_declarator` reached by
/// descending an `operator_cast`'s abstract declarator wrappers.
fn function_parameter_list<'r, D: Doc>(node: &Node<'r, D>) -> Option<Node<'r, D>> {
    let mut cur = descend_abstract_declarator(node)?;
    while cur.kind().as_ref() != "abstract_function_declarator" {
        cur = descend_abstract_declarator(&cur)?;
    }
    cur.child_of_kind("parameter_list")
}

fn descend_abstract_declarator<'r, D: Doc>(node: &Node<'r, D>) -> Option<Node<'r, D>> {
    node.field("declarator")
        .or_else(|| node.children().find(|c| c.is_named()))
}

#[derive(Clone)]
pub enum Emit {
    Text,
    None,
    /// Try `field("name")`, then first child matching these kinds.
    Name(&'static [&'static str]),
    Children(Match<'static>),
    Const(&'static str),
    /// Emit a C/C++ declared name via [`declared_name_text`]. Set by
    /// [`Extract::declarator_name`].
    DeclaredName,
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
        }
    }

    pub fn one(axis: Axis<'static>, m: Match<'static>) -> Self {
        Self::from_step(Step::Nav(axis, m))
    }

    pub fn terminal(emit: Emit) -> Self {
        Self {
            steps: SmallVec::new(),
            emit,
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

    /// Descend C/C++ declarator wrappers to the declared name and emit it via
    /// [`Emit::DeclaredName`]. Pair with `field("declarator")`.
    pub fn declarator_name(mut self) -> Self {
        self.emit = Emit::DeclaredName;
        self.push(Step::DeclaratorName)
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

    /// Use with `apply_all()` instead of `apply()`.
    pub fn collect(mut self, m: Match<'static>) -> Self {
        self.emit = Emit::Children(m);
        self
    }

    pub fn inner(self, container: &'static str, target: &'static str) -> Self {
        self.try_child(container).try_descendant(target)
    }
    pub fn then(mut self, next: Extract) -> Self {
        self.steps.extend(next.steps);
        self.emit = next.emit;
        self
    }
}

impl Extract {
    pub fn apply<D: Doc>(&self, node: &Node<'_, D>) -> Option<String> {
        let target = self.navigate(node)?;
        emit(&self.emit, &target)
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
        Some(transform(raw, node))
    }

    /// Returns empty vec on navigation failure or non-Children emit.
    pub fn apply_all<D: Doc>(&self, node: &Node<'_, D>) -> Vec<String> {
        let Some(target) = self.navigate(node) else {
            return vec![];
        };
        emit_all(&self.emit, &target)
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
                Step::DeclaratorName => cur = descend_declarator_name(&cur)?,
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
        Emit::Children(_) => emit_all(mode, node).into_iter().next(),
        Emit::Const(s) => Some(s.to_string()),
        Emit::DeclaredName => declared_name_text(node),
    }
}

fn emit_all<D: Doc>(mode: &Emit, node: &Node<'_, D>) -> Vec<String> {
    match mode {
        Emit::Children(m) => node
            .children()
            .filter(|c| m.test(c))
            .map(|c| c.text().to_string())
            .collect(),
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

        let methods = field("body")
            .collect(Match::Kind("function_definition"))
            .apply_all(&cls);
        // text() of function_definition nodes includes full "def a(self): pass"
        assert_eq!(methods.len(), 2);
        assert!(methods[0].contains("a"));
        assert!(methods[1].contains("b"));
    }

    #[cfg(feature = "tree-sitter-c")]
    fn c_fn_name(code: &str) -> Option<String> {
        let root = SupportLang::C.ast_grep(code);
        let func = root
            .root()
            .find(Axis::Descendant, Match::Kind("function_definition"))
            .unwrap();
        field("declarator").declarator_name().apply(&func)
    }

    #[cfg(feature = "tree-sitter-cpp")]
    fn cpp_fn_name(code: &str) -> Option<String> {
        let root = SupportLang::Cpp.ast_grep(code);
        let func = root
            .root()
            .find(Axis::Descendant, Match::Kind("function_definition"))
            .unwrap();
        field("declarator").declarator_name().apply(&func)
    }

    #[cfg(feature = "tree-sitter-c")]
    fn c_typedef_name(code: &str) -> Option<String> {
        let root = SupportLang::C.ast_grep(code);
        let td = root
            .root()
            .find(Axis::Descendant, Match::Kind("type_definition"))
            .unwrap();
        field("declarator").declarator_name().apply(&td)
    }

    #[cfg(feature = "tree-sitter-c")]
    #[test]
    fn declarator_name_c_shapes() {
        assert_eq!(
            c_fn_name("int foo(int a) { return a; }"),
            Some("foo".into())
        );
        assert_eq!(
            c_fn_name("static inline int foo(int a) { return a; }"),
            Some("foo".into())
        );
        assert_eq!(
            c_fn_name("void *foo(int a) { return 0; }"),
            Some("foo".into())
        );
        assert_eq!(
            c_fn_name("void **foo(int a) { return 0; }"),
            Some("foo".into())
        );
        assert_eq!(
            c_fn_name("int (foo)(int a) { return a; }"),
            Some("foo".into())
        );
        assert_eq!(
            c_fn_name("int (*foo(int a))(int) { return 0; }"),
            Some("foo".into())
        );
        assert_eq!(
            c_fn_name("int arr[3]; int bar() { return 0; }"),
            Some("bar".into())
        );
        assert_eq!(
            c_fn_name("int (/*x*/ foo)(int a) { return a; }"),
            Some("foo".into())
        );
    }

    #[cfg(feature = "tree-sitter-c")]
    #[test]
    fn declarator_name_c_typedefs() {
        assert_eq!(
            c_typedef_name("typedef int *my_ptr;"),
            Some("my_ptr".into())
        );
        assert_eq!(
            c_typedef_name("typedef int (*fn_t)(int);"),
            Some("fn_t".into())
        );
        assert_eq!(
            c_typedef_name("typedef char buf_t[64];"),
            Some("buf_t".into())
        );
        assert_eq!(
            c_typedef_name("typedef struct { int x; } Point;"),
            Some("Point".into())
        );
    }

    #[cfg(feature = "tree-sitter-cpp")]
    #[test]
    fn declarator_name_cpp_shapes() {
        assert_eq!(
            cpp_fn_name("void *foo(int a) { return 0; }"),
            Some("foo".into())
        );
        assert_eq!(
            cpp_fn_name("int Foo::bar(int a) { return a; }"),
            Some("Foo::bar".into())
        );
        assert_eq!(
            cpp_fn_name("int* Foo::bar(int a) { return 0; }"),
            Some("Foo::bar".into())
        );
        assert_eq!(
            cpp_fn_name("bool Foo::operator==(const Foo& o) const { return true; }"),
            Some("Foo::operator==".into())
        );
        assert_eq!(cpp_fn_name("Foo::~Foo() {}"), Some("Foo::~Foo".into()));
        assert_eq!(
            cpp_fn_name("class Foo { public: int *bar(int a) { return 0; } };"),
            Some("bar".into())
        );
        assert_eq!(
            cpp_fn_name("template<typename T> T* Foo::get() { return 0; }"),
            Some("Foo::get".into())
        );
        assert_eq!(
            cpp_fn_name("int A::B::foo(int a) { return a; }"),
            Some("A::B::foo".into())
        );
        assert_eq!(
            cpp_fn_name("int& foo(int a) { return a; }"),
            Some("foo".into())
        );
        assert_eq!(cpp_fn_name("int&& foo() { return 0; }"), Some("foo".into()));
    }

    #[cfg(feature = "tree-sitter-cpp")]
    #[test]
    fn declarator_name_cpp_conversion_operators() {
        assert_eq!(
            cpp_fn_name("class Foo { public: operator bool() const { return true; } };"),
            Some("operator bool".into())
        );
        assert_eq!(
            cpp_fn_name("Ptr::operator int() const { return 0; }"),
            Some("Ptr::operator int".into())
        );
        assert_eq!(
            cpp_fn_name("S::I::operator int() const { return 0; }"),
            Some("S::I::operator int".into())
        );
        assert_eq!(
            cpp_fn_name("class C { operator const char*() const { return 0; } };"),
            Some("operator const char*".into())
        );
        assert_eq!(
            cpp_fn_name("class C { operator int*() { return 0; } };"),
            Some("operator int*".into())
        );
        assert_eq!(
            cpp_fn_name("class C { operator int&() { return x; } };"),
            Some("operator int&".into())
        );
        assert_eq!(
            cpp_fn_name("class C { operator std::string() const { return {}; } };"),
            Some("operator std::string".into())
        );
        assert_eq!(
            cpp_fn_name("class C { operator std::function<void(int)>() { return {}; } };"),
            Some("operator std::function<void(int)>".into())
        );
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
