//! Shared C/C++ declarator-name extraction.
//!
//! A definition's declarator can be wrapped by pointer/array/parenthesized
//! layers that a fixed field hop can't skip, so both languages descend to the
//! bare declared name. C++ additionally normalizes conversion operators. These
//! live here (not in `treesitter-visit`) because they encode tree-sitter-c/cpp
//! grammar shapes, which the traversal wrapper must stay agnostic of.

use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

/// Declarator wrapper kinds a pointer/parenthesized/array declarator inserts
/// between a definition and its name, so a fixed hop count lands on the wrong
/// node. `reference_declarator` and `parenthesized_declarator` carry no
/// `declarator` field (tree-sitter-cpp: `seq(choice('&','&&'), $._declarator)`),
/// unlike the other three.
const DECLARATOR_WRAPPERS: &[&str] = &[
    "function_declarator",
    "pointer_declarator",
    "array_declarator",
    "reference_declarator",
    "parenthesized_declarator",
];

/// Descend declarator wrappers to the declared-name node, following the
/// `declarator` field or, when absent, the first named child.
///
/// Comments are skipped: they are named nodes that can precede the inner
/// declarator (`int (/*x*/ foo)()`).
fn descend_declarator_name<'r>(node: &N<'r>) -> Option<N<'r>> {
    let mut cur = node.clone();
    while DECLARATOR_WRAPPERS.contains(&cur.kind().as_ref()) {
        cur = match cur.field("declarator") {
            Some(inner) => inner,
            None => cur
                .children()
                .find(|c| c.is_named() && c.kind().as_ref() != "comment")?,
        };
    }
    Some(cur)
}

/// The declared-name node for a `function_definition`/`type_definition`: its
/// `declarator` field, descended through any wrappers.
fn declared_name_node<'r>(def: &N<'r>) -> Option<N<'r>> {
    descend_declarator_name(&def.field("declarator")?)
}

/// C declared name: the bare declarator text, verbatim.
pub fn c_declarator_name(def: &N<'_>) -> Option<String> {
    Some(declared_name_node(def)?.text().to_string())
}

/// C++ declared name: verbatim (keeping any class qualifier that feeds an
/// out-of-line member's FQN), but with a trailing conversion operator
/// normalized to `operator <type>`.
pub fn cpp_declarator_name(def: &N<'_>) -> Option<String> {
    let node = declared_name_node(def)?;
    declared_name_text(&node)
}

/// Text for a C++ declared-name node: verbatim, but with a trailing conversion
/// operator normalized to `operator <type>` so its parameter list does not leak
/// into the name.
///
/// The `operator_cast` is normalized at any qualifier depth because
/// `qualified_identifier` is right-recursive: `S::I::operator int()` nests as
/// `qualified_identifier(S, qualified_identifier(I, operator_cast))`.
fn declared_name_text(node: &N<'_>) -> Option<String> {
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
fn trailing_operator_cast<'r>(node: &N<'r>) -> Option<N<'r>> {
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
fn operator_cast_text(node: &N<'_>) -> Option<String> {
    let params = function_parameter_list(node)?;
    let text = node.text();
    let end = params.range().start - node.range().start;
    Some(text[..end].trim_end().to_string())
}

/// The `parameter_list` of the `abstract_function_declarator` reached by
/// descending an `operator_cast`'s abstract declarator wrappers.
fn function_parameter_list<'r>(node: &N<'r>) -> Option<N<'r>> {
    let mut cur = descend_abstract_declarator(node)?;
    while cur.kind().as_ref() != "abstract_function_declarator" {
        cur = descend_abstract_declarator(&cur)?;
    }
    cur.child_of_kind("parameter_list")
}

fn descend_abstract_declarator<'r>(node: &N<'r>) -> Option<N<'r>> {
    node.field("declarator")
        .or_else(|| node.children().find(|c| c.is_named()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use treesitter_visit::tree_sitter::LanguageExt;
    use treesitter_visit::{Axis, Match};

    fn c_fn_name(code: &str) -> Option<String> {
        let root = SupportLang::C.ast_grep(code);
        let func = root
            .root()
            .find(Axis::Descendant, Match::Kind("function_definition"))
            .unwrap();
        c_declarator_name(&func)
    }

    fn c_typedef_name(code: &str) -> Option<String> {
        let root = SupportLang::C.ast_grep(code);
        let td = root
            .root()
            .find(Axis::Descendant, Match::Kind("type_definition"))
            .unwrap();
        c_declarator_name(&td)
    }

    fn cpp_fn_name(code: &str) -> Option<String> {
        let root = SupportLang::Cpp.ast_grep(code);
        let func = root
            .root()
            .find(Axis::Descendant, Match::Kind("function_definition"))
            .unwrap();
        cpp_declarator_name(&func)
    }

    #[test]
    fn c_shapes() {
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

    #[test]
    fn c_typedefs() {
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

    #[test]
    fn cpp_shapes() {
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

    #[test]
    fn cpp_conversion_operators() {
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
}
