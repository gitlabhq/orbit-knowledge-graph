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

/// An unnamed parameter such as `void f(int *)` ends in an abstract declarator,
/// which names nothing, so it binds nothing rather than binding `*`.
pub fn binding_declarator_name(binding: &N<'_>) -> Option<String> {
    let node = declared_name_node(binding)?;
    (!node.kind().as_ref().starts_with("abstract_")).then(|| node.text().to_string())
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

pub fn c_parameters(def: &N<'_>) -> Vec<String> {
    let mut cur = def.field("declarator");
    while let Some(node) = cur {
        if node.kind().as_ref() == "function_declarator" {
            return node
                .field("parameters")
                .map(|list| {
                    list.children()
                        .filter(|c| c.is_named() && c.kind().as_ref() != "comment")
                        .map(|c| c.text().to_string())
                        .collect()
                })
                .unwrap_or_default();
        }
        cur = node.field("declarator");
    }
    Vec::new()
}

pub fn c_linkage(def: &N<'_>) -> Option<crate::v2::types::Visibility> {
    if def.kind().as_ref() != "function_definition" {
        return None;
    }
    let is_static = def
        .children()
        .any(|c| c.kind().as_ref() == "storage_class_specifier" && c.text() == "static");
    Some(if is_static {
        crate::v2::types::Visibility::File
    } else {
        crate::v2::types::Visibility::Public
    })
}

pub fn cpp_access(def: &N<'_>, _name: &str) -> Option<crate::v2::types::Visibility> {
    use crate::v2::types::Visibility;
    let Some(list) = def
        .parent()
        .filter(|p| p.kind().as_ref() == "field_declaration_list")
    else {
        return c_linkage(def);
    };
    for sibling in def.prev_all() {
        if sibling.kind().as_ref() == "access_specifier" {
            return match sibling.text().trim() {
                "public" => Some(Visibility::Public),
                "protected" => Some(Visibility::Protected),
                "private" => Some(Visibility::Private),
                _ => None,
            };
        }
    }
    let owner = list.parent()?;
    Some(if owner.kind().as_ref() == "class_specifier" {
        Visibility::Private
    } else {
        Visibility::Public
    })
}

pub const C_DOC_MARKERS: &[&str] = &["///", "//!", "//", "/**", "/*!", "/*", "*/", "*"];

pub const C_TOKENS: &[(&str, crate::v2::dsl::extractors::Token)] = &[
    (
        "static",
        crate::v2::dsl::extractors::Token::Modifier(crate::v2::types::Modifier::Static),
    ),
    (
        "extern",
        crate::v2::dsl::extractors::Token::Modifier(crate::v2::types::Modifier::Extern),
    ),
    (
        "inline",
        crate::v2::dsl::extractors::Token::Modifier(crate::v2::types::Modifier::Inline),
    ),
    (
        "virtual",
        crate::v2::dsl::extractors::Token::Modifier(crate::v2::types::Modifier::Virtual),
    ),
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::config::Language;

    fn binding_names(lang: Language, code: &str, kind: &str) -> Vec<Option<String>> {
        let ast = lang.parse_ast(code, None).unwrap();
        ast.root()
            .dfs()
            .filter(|n| n.kind().as_ref() == kind)
            .map(|n| binding_declarator_name(&n))
            .collect()
    }

    #[test]
    fn c_parameters_bind_the_bare_identifier() {
        assert_eq!(
            binding_names(
                Language::C,
                "void f(int n, int *p, char **argv, void (*cb)(void)) {}",
                "parameter_declaration",
            ),
            // A nested declarator carries its own parameter list, so an abstract
            // parameter inside it contributes an extra entry here.
            [Some("n"), Some("p"), Some("argv"), Some("cb"), None].map(|s| s.map(String::from)),
        );
    }

    #[test]
    fn c_initialized_locals_bind_the_bare_identifier() {
        assert_eq!(
            binding_names(
                Language::C,
                "void f(void) { int *x = 0; int (*fp)(int) = 0; }",
                "init_declarator",
            ),
            [Some("x"), Some("fp")].map(|s| s.map(String::from)),
        );
    }

    #[test]
    fn cpp_reference_bindings_bind_the_bare_identifier() {
        assert_eq!(
            binding_names(
                Language::Cpp,
                "void f(int &r, const int &&rv) { int &alias = r; }",
                "parameter_declaration",
            ),
            [Some("r"), Some("rv")].map(|s| s.map(String::from)),
        );
        assert_eq!(
            binding_names(
                Language::Cpp,
                "void f(int &r) { int &alias = r; }",
                "init_declarator"
            ),
            [Some("alias".to_string())],
        );
    }

    #[test]
    fn unnamed_parameters_bind_nothing() {
        assert_eq!(
            binding_names(
                Language::C,
                "void f(int *, char **);",
                "parameter_declaration"
            ),
            [None, None],
        );
    }
}
