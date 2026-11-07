use super::operator::Operator;
use super::pattern::Expression as Tree;

pub(super) fn leaf(label: Operator, text: impl Into<String>) -> Tree {
    Tree::node(label, text, vec![])
}

pub(super) fn filter(predicates: Vec<String>, input: Tree) -> Tree {
    if predicates.is_empty() {
        return input;
    }
    if input.label == Operator::Filter {
        let mut input = input;
        input.items.extend(predicates);
        input
    } else {
        Tree::node(Operator::Filter, predicates.join(", "), vec![input])
    }
}

pub(super) fn membership_keys(keys: &[String]) -> String {
    match keys {
        [key] => key.clone(),
        _ => format!("({})", keys.join(", ")),
    }
}

pub(super) fn scan(table: &str, alias: &str, final_: bool) -> Tree {
    let scan = leaf(Operator::Scan, format!("Table({table}) AS {alias}"));
    if final_ {
        Tree::node(Operator::Deduplicate, "Final", vec![scan])
    } else {
        scan
    }
}

pub(super) fn text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

pub(super) fn literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => text_literal(value),
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values.iter().map(literal).collect::<Vec<_>>().join(", ")
        ),
        value => value.to_string(),
    }
}

pub(super) fn sort(input: Tree, keys: impl Iterator<Item = (String, bool)>) -> Tree {
    let keys: Vec<_> = keys
        .map(|(value, descending)| format!("{value}{}", if descending { " DESC" } else { "" }))
        .collect();
    if keys.is_empty() {
        input
    } else {
        Tree::node(Operator::Sort, keys.join(", "), vec![input])
    }
}

#[test]
fn exact_assertions_preserve_nested_filter_order() {
    let scan = scan("gl_project", "p", false);
    let tree = filter(
        vec!["p.star_count = 2".into()],
        filter(vec!["p.id = 1".into()], scan),
    );
    let assertions: super::Assertions = orbit_utils::yaml::from_str(
        "exact: (Filter p.id = 1, p.star_count = 2 (Scan Table(gl_project) AS p))",
    )
    .unwrap();
    assertions.check(&tree, "planned").unwrap();
}
