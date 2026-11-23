use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

const DECLARATOR_WRAPPERS: &[&str] = &[
    "function_declarator",
    "pointer_declarator",
    "array_declarator",
    "reference_declarator",
    "parenthesized_declarator",
    "attributed_declarator",
];

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

pub fn binding_declarator_name(binding: &N<'_>) -> Option<String> {
    let node = declared_name_node(binding)?;
    (!node.kind().as_ref().starts_with("abstract_")).then(|| node.text().to_string())
}

pub fn cpp_declarator_name(def: &N<'_>) -> Option<String> {
    let node = declared_name_node(def)?;
    if node.kind().as_ref() == "operator_cast" {
        return operator_cast_text(&node);
    }
    let last = trailing_name(&node)?;
    let base = match last.kind().as_ref() {
        "operator_cast" => operator_cast_text(&last),
        "template_function" => last.field("name").map(|name| name.text().to_string()),
        _ => None,
    };
    let Some(base) = base else {
        return Some(node.text().to_string());
    };
    let text = node.text();
    let qualifier = &text[..last.range().start - node.range().start];
    Some(format!("{qualifier}{base}"))
}

fn trailing_name<'r>(node: &N<'r>) -> Option<N<'r>> {
    let mut cur = node.clone();
    while cur.kind().as_ref() == "qualified_identifier" {
        cur = cur.children().filter(|c| c.is_named()).last()?;
    }
    Some(cur)
}

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

fn descend_abstract_declarator<'r>(node: &N<'r>) -> Option<N<'r>> {
    node.field("declarator").or_else(|| {
        node.children()
            .find(|c| c.is_named() && !NON_NAME_CHILDREN.contains(&c.kind().as_ref()))
    })
}

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
