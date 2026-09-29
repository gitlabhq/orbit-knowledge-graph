use serde_json::{Map, json};
use shared::PaginationMeta;

use super::{literal, node_literal, render};

fn cells(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

#[test]
fn renders_a_padded_table_with_a_row_count() {
    let table = render(
        &cells(&["u", "g"]),
        &[
            cells(&["(:User {id: 1})", "(:Group {id: 22})"]),
            cells(&["NULL", "(:Group {id: 3})"]),
        ],
        None,
    );
    assert_eq!(
        table,
        "+-------------------------------------+\n\
         | u               | g                 |\n\
         +-------------------------------------+\n\
         | (:User {id: 1}) | (:Group {id: 22}) |\n\
         | NULL            | (:Group {id: 3})  |\n\
         +-------------------------------------+\n\
         \n\
         2 rows\n"
    );
}

#[test]
fn empty_results_keep_the_header_and_report_more_pages() {
    let page = PaginationMeta {
        has_more: true,
        truncated: true,
        next_cursor: Some("abc".into()),
    };
    assert_eq!(
        render(&cells(&["n"]), &[], Some(&page)),
        "+---+\n| n |\n+---+\n+---+\n\n0 rows, more available\nnext_cursor: \"abc\"\n"
    );
    assert!(render(&cells(&["n"]), &[cells(&["1"])], None).ends_with("\n1 row\n"));
}

#[test]
fn values_use_cypher_shell_literals() {
    let mut properties = Map::new();
    for (key, value) in [
        ("username", json!("zoë \"z\"")),
        ("confidential", json!(false)),
        ("score", json!(2.0)),
        ("milestone", json!(null)),
        ("labels", json!(["bug", "p1"])),
    ] {
        properties.insert(key.into(), value);
    }
    assert_eq!(
        node_literal("User", 7, &properties),
        "(:User {id: 7, username: \"zoë \\\"z\\\"\", confidential: FALSE, labels: [\"bug\", \"p1\"], score: 2.0})"
    );
    assert_eq!(literal(&json!(null), "x"), "NULL");
    assert_eq!(literal(&json!(true), "x"), "TRUE");
    assert!(literal(&json!("x".repeat(250)), "description").ends_with("...\""));
}
