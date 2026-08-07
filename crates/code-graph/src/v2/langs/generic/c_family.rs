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

/// Declarator wrapper kinds that wrap an inner declarator between a definition
/// and its name, so a fixed hop count lands on the wrong node. `reference_`,
/// `parenthesized_` and `attributed_declarator` carry no `declarator` field
/// (e.g. tree-sitter-cpp: `reference_declarator` is
/// `seq(choice('&','&&'), $._declarator)`), unlike the other three; the descent
/// falls back to their first named child.
const DECLARATOR_WRAPPERS: &[&str] = &[
    "function_declarator",
    "pointer_declarator",
    "array_declarator",
    "reference_declarator",
    "parenthesized_declarator",
    "attributed_declarator",
];

/// Named children that can precede the inner declarator in a wrapper but are not
/// the declared name, so the first-named-child fallback must skip them: a
/// `comment` (`int (/*x*/ foo)()`) or an MSVC `ms_call_modifier`
/// (`void (__cdecl foo)(int)`).
const NON_NAME_CHILDREN: &[&str] = &["comment", "ms_call_modifier"];

/// Descend declarator wrappers to the declared-name node, following the
/// `declarator` field or, when absent, the first named child that is a name.
fn descend_declarator_name<'r>(node: &N<'r>) -> Option<N<'r>> {
    let mut cur = node.clone();
    while DECLARATOR_WRAPPERS.contains(&cur.kind().as_ref()) {
        cur = match cur.field("declarator") {
            Some(inner) => inner,
            None => cur
                .children()
                .find(|c| c.is_named() && !NON_NAME_CHILDREN.contains(&c.kind().as_ref()))?,
        };
    }
    Some(cur)
}

/// The declared-name node for a `function_definition`/`type_definition`: its
/// `declarator` field, descended through any wrappers.
fn declared_name_node<'r>(def: &N<'r>) -> Option<N<'r>> {
    descend_declarator_name(&def.field("declarator")?)
}

pub fn c_declarator_name(def: &N<'_>) -> Option<String> {
    Some(declared_name_node(def)?.text().to_string())
}

/// C++ declared name: verbatim (keeping any class qualifier that feeds an
/// out-of-line member's FQN), but with a trailing conversion operator
/// normalized to `operator <type>` so its parameter list does not leak into the
/// name.
///
/// The `operator_cast` is normalized at any qualifier depth because
/// `qualified_identifier` is right-recursive: `S::I::operator int()` nests as
/// `qualified_identifier(S, qualified_identifier(I, operator_cast))`.
pub fn cpp_declarator_name(def: &N<'_>) -> Option<String> {
    let node = declared_name_node(def)?;
    if node.kind().as_ref() == "operator_cast" {
        return operator_cast_text(&node);
    }
    if node.kind().as_ref() == "qualified_identifier"
        && let Some(op_cast) = trailing_operator_cast(&node)
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
/// The type ends at the last non-comment token before the operator's own
/// `parameter_list`, so a type that embeds its own params
/// (`operator std::function<void(int)>`) is preserved while a comment between
/// the type and the `()` (`operator int& /*c*/ ()`) does not leak into the name.
fn operator_cast_text(node: &N<'_>) -> Option<String> {
    let params = function_parameter_list(node)?;
    let base = node.range().start;
    let type_end = node
        .dfs()
        .filter(|n| n.kind().as_ref() != "comment" && n.range().end <= params.range().start)
        .map(|n| n.range().end)
        .max()
        .unwrap_or(params.range().start);
    let text = node.text();
    Some(text[..type_end - base].trim_end().to_string())
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

/// One step down an `operator_cast`'s abstract declarator. `abstract_reference_declarator`
/// carries no `declarator` field, so the fallback takes its first name-bearing
/// child and must skip the same `NON_NAME_CHILDREN` the wrapper descent does
/// (e.g. `operator int& /*c*/ ()`).
fn descend_abstract_declarator<'r>(node: &N<'r>) -> Option<N<'r>> {
    node.field("declarator").or_else(|| {
        node.children()
            .find(|c| c.is_named() && !NON_NAME_CHILDREN.contains(&c.kind().as_ref()))
    })
}
