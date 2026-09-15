//! This module contains tree-sitter-c/cpp grammar rules that do not belong in
//! the language-neutral `treesitter-visit` wrapper.

use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

/// `reference_declarator`, `parenthesized_declarator`, and
/// `attributed_declarator` have no `declarator` field. Their first named child
/// is the inner declarator.
const DECLARATOR_WRAPPERS: &[&str] = &[
    "function_declarator",
    "pointer_declarator",
    "array_declarator",
    "reference_declarator",
    "parenthesized_declarator",
    "attributed_declarator",
];

/// These named children can precede the inner declarator, so the fallback must
/// skip them.
const NON_NAME_CHILDREN: &[&str] = &["comment", "ms_call_modifier"];

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

fn declared_name_node<'r>(def: &N<'r>) -> Option<N<'r>> {
    descend_declarator_name(&def.field("declarator")?)
}

pub fn c_declarator_name(def: &N<'_>) -> Option<String> {
    Some(declared_name_node(def)?.text().to_string())
}

/// Keep class qualifiers because they identify out-of-line members in the FQN.
/// Normalize conversion operators at any depth because `qualified_identifier`
/// is right-recursive.
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

fn trailing_operator_cast<'r>(node: &N<'r>) -> Option<N<'r>> {
    let mut cur = node.clone();
    while cur.kind().as_ref() == "qualified_identifier" {
        cur = cur.children().filter(|c| c.is_named()).last()?;
    }
    (cur.kind().as_ref() == "operator_cast").then_some(cur)
}

/// Use the last token before the operator's parameter list. This preserves type
/// parameters such as `std::function<void(int)>` and excludes trailing comments.
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

fn function_parameter_list<'r>(node: &N<'r>) -> Option<N<'r>> {
    let mut cur = descend_abstract_declarator(node)?;
    while cur.kind().as_ref() != "abstract_function_declarator" {
        cur = descend_abstract_declarator(&cur)?;
    }
    cur.child_of_kind("parameter_list")
}

/// `abstract_reference_declarator` has no `declarator` field, so use its first
/// name-bearing child.
fn descend_abstract_declarator<'r>(node: &N<'r>) -> Option<N<'r>> {
    node.field("declarator").or_else(|| {
        node.children()
            .find(|c| c.is_named() && !NON_NAME_CHILDREN.contains(&c.kind().as_ref()))
    })
}
