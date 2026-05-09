use semver::Version;
use serde_json::{Value, json};

use super::encode::encode;
use super::fixtures::*;

fn version() -> Version {
    Version::new(1, 0, 0)
}

fn enc(response: &crate::graph::GraphResponse) -> String {
    encode(response, &version())
}

// ---------------------------------------------------------------------------
// Header + section structure
// ---------------------------------------------------------------------------

#[test]
fn header_emits_query_type_and_counts() {
    let r = traversal_response();
    let out = enc(&r);
    assert!(out.starts_with("@header\n"));
    assert!(out.contains("query_type:traversal"));
    assert!(out.contains("format_version:1.0.0"));
    assert!(out.contains("nodes:3"));
    assert!(out.contains("edges:2"));
}

#[test]
fn empty_response_emits_only_header() {
    let r = response("traversal", vec![], vec![]);
    let out = enc(&r);
    assert!(out.contains("@header\n"));
    assert!(out.contains("nodes:0"));
    assert!(out.contains("edges:0"));
    assert!(!out.contains("@nodes"));
    assert!(!out.contains("@edges"));
}

#[test]
fn pagination_offset_emits_has_more_and_total() {
    let mut r = response("traversal", vec![node("User", 1, &[])], vec![]);
    r.pagination = Some(pagination(true, 50));
    let out = enc(&r);
    assert!(out.contains("has_more:true"));
    assert!(out.contains("total_rows:50"));
}

#[test]
fn pagination_no_more_omits_has_more() {
    let mut r = response("traversal", vec![node("User", 1, &[])], vec![]);
    r.pagination = Some(pagination(false, 1));
    let out = enc(&r);
    assert!(!out.contains("has_more"));
    assert!(out.contains("total_rows:1"));
}

#[test]
fn no_hints_section_is_emitted() {
    let out = enc(&traversal_response());
    assert!(
        !out.contains("@hints"),
        "@hints must be dropped: got\n{out}"
    );
}

// ---------------------------------------------------------------------------
// Node rendering
// ---------------------------------------------------------------------------

#[test]
fn nodes_grouped_by_type_with_count_header() {
    let out = enc(&traversal_response());
    assert!(out.contains("MergeRequest(1):"));
    assert!(out.contains("Project(1):"));
    assert!(out.contains("User(1):"));
}

#[test]
fn node_row_starts_with_id_and_lists_properties() {
    let out = enc(&traversal_response());
    // Property order is column_priority then alphabetical; both `name` and
    // `username` are priority 0 so `name` < `username` alphabetically.
    assert!(
        out.contains("5252563 name=\"Jordan NG\" username=jordan_ng"),
        "unexpected node row: {out}"
    );
}

#[test]
fn null_and_empty_properties_are_skipped() {
    let r = response(
        "traversal",
        vec![node(
            "User",
            1,
            &[
                ("username", json!("a")),
                ("name", json!(null)),
                ("email", json!("")),
            ],
        )],
        vec![],
    );
    let out = enc(&r);
    // "username=a" contains the substring "name=", so check word-boundaried.
    assert!(!out.contains(" name="), "null name leaked: {out}");
    assert!(!out.contains(" email="), "empty email leaked: {out}");
    assert!(out.contains("username=a"), "username missing: {out}");
}

// ---------------------------------------------------------------------------
// Quoting + escaping
// ---------------------------------------------------------------------------

#[test]
fn values_with_spaces_are_quoted() {
    let r = response(
        "traversal",
        vec![node("User", 1, &[("name", json!("Jordan NG"))])],
        vec![],
    );
    assert!(enc(&r).contains("name=\"Jordan NG\""));
}

#[test]
fn embedded_double_quote_is_escaped() {
    let r = response(
        "traversal",
        vec![node("MR", 1, &[("title", json!(r#"a "quoted" b"#))])],
        vec![],
    );
    let out = enc(&r);
    assert!(out.contains(r#"title="a \"quoted\" b""#));
}

#[test]
fn embedded_backslash_is_escaped() {
    let r = response(
        "traversal",
        vec![node("MR", 1, &[("title", json!("a\\b"))])],
        vec![],
    );
    let out = enc(&r);
    assert!(out.contains(r#"title="a\\b""#));
}

#[test]
fn newlines_become_escape_sequences_not_raw_breaks() {
    let r = response(
        "traversal",
        vec![node("MR", 1, &[("title", json!("Fix bug\nUpdate tests"))])],
        vec![],
    );
    let out = enc(&r);
    assert!(out.contains("\\n"), "newline must escape: {out}");
    assert!(!out.contains("Fix bug\nUpdate tests"));
}

#[test]
fn cr_and_tab_are_escape_sequences() {
    let r = response(
        "traversal",
        vec![node("MR", 1, &[("title", json!("a\rb\tc"))])],
        vec![],
    );
    let out = enc(&r);
    assert!(out.contains("\\r"));
    assert!(out.contains("\\t"));
}

#[test]
fn other_control_chars_are_dropped() {
    let r = response(
        "traversal",
        vec![node("MR", 1, &[("title", json!("a\u{0001}b"))])],
        vec![],
    );
    let out = enc(&r);
    assert!(!out.contains('\u{0001}'));
}

// ---------------------------------------------------------------------------
// Datetime normalization
// ---------------------------------------------------------------------------

#[test]
fn space_separated_datetime_is_converted_to_t_form() {
    let r = response(
        "traversal",
        vec![node(
            "MR",
            1,
            &[("created_at", json!("2026-05-08 22:55:58.467450"))],
        )],
        vec![],
    );
    let out = enc(&r);
    assert!(out.contains("created_at=2026-05-08T22:55:58.467450"));
    assert!(!out.contains("2026-05-08 22:55:58"));
}

#[test]
fn t_separated_datetime_passes_through_bare() {
    let r = response(
        "traversal",
        vec![node(
            "MR",
            1,
            &[("created_at", json!("2026-05-08T22:55:58Z"))],
        )],
        vec![],
    );
    assert!(enc(&r).contains("created_at=2026-05-08T22:55:58Z"));
}

// ---------------------------------------------------------------------------
// Truncation
// ---------------------------------------------------------------------------

#[test]
fn long_text_keys_truncate_at_200_and_emit_breadcrumb() {
    let body: String = "x".repeat(500);
    let r = response(
        "traversal",
        vec![node("Note", 1, &[("body", Value::String(body))])],
        vec![],
    );
    let out = enc(&r);
    assert!(out.contains("body_len=500"));
    assert!(out.matches('x').count() < 500);
    assert!(out.contains("..."));
}

#[test]
fn unknown_long_key_truncates_at_hard_limit() {
    let value: String = "y".repeat(2000);
    let r = response(
        "traversal",
        vec![node("MR", 1, &[("ref_name", Value::String(value))])],
        vec![],
    );
    let out = enc(&r);
    assert!(out.contains("ref_name_len=2000"));
    assert!(out.matches('y').count() <= 1000);
}

// ---------------------------------------------------------------------------
// Numeric handling
// ---------------------------------------------------------------------------

#[test]
fn nan_and_infinity_are_dropped() {
    let r = response(
        "traversal",
        vec![node(
            "MR",
            1,
            &[
                ("nan_val", json!(f64::NAN)),
                ("inf_val", json!(f64::INFINITY)),
            ],
        )],
        vec![],
    );
    let out = enc(&r);
    assert!(!out.contains("nan"));
    assert!(!out.contains("inf"));
}

#[test]
fn integer_node_id_renders_bare() {
    let r = response(
        "traversal",
        vec![node("User", 9007199254740993, &[])],
        vec![],
    );
    assert!(enc(&r).contains("9007199254740993"));
}

#[test]
fn boolean_renders_lowercase() {
    let r = response(
        "traversal",
        vec![node(
            "MR",
            1,
            &[("draft", json!(true)), ("merged", json!(false))],
        )],
        vec![],
    );
    let out = enc(&r);
    assert!(out.contains("draft=true"));
    assert!(out.contains("merged=false"));
}

// ---------------------------------------------------------------------------
// Edges + dedup + ordering
// ---------------------------------------------------------------------------

#[test]
fn edges_grouped_by_type_with_arrow_notation() {
    let out = enc(&traversal_response());
    assert!(out.contains("AUTHORED(1):"));
    assert!(out.contains("User:5252563 --> MergeRequest:482927048"));
    assert!(out.contains("IN_PROJECT(1):"));
    assert!(out.contains("MergeRequest:482927048 --> Project:80212187"));
}

#[test]
fn duplicate_edges_dedup() {
    let dup = edge("AUTHORED", "User", 1, "MR", 42);
    let r = response(
        "traversal",
        vec![node("MR", 42, &[])],
        vec![dup.clone(), dup.clone(), dup],
    );
    let out = enc(&r);
    assert_eq!(
        out.matches("User:1 --> MR:42").count(),
        1,
        "edges must dedup: {out}"
    );
}

#[test]
fn shuffled_edges_produce_identical_output() {
    let a = response(
        "traversal",
        vec![node("MR", 1, &[]), node("MR", 2, &[])],
        vec![
            edge("AUTHORED", "User", 9, "MR", 1),
            edge("AUTHORED", "User", 9, "MR", 2),
        ],
    );
    let b = response(
        "traversal",
        vec![node("MR", 2, &[]), node("MR", 1, &[])],
        vec![
            edge("AUTHORED", "User", 9, "MR", 2),
            edge("AUTHORED", "User", 9, "MR", 1),
        ],
    );
    assert_eq!(enc(&a), enc(&b));
}

// ---------------------------------------------------------------------------
// Path-finding
// ---------------------------------------------------------------------------

#[test]
fn path_finding_emits_paths_section_not_edges() {
    let r = response(
        "path_finding",
        vec![
            node("User", 1, &[]),
            node("MR", 42, &[]),
            node("Project", 100, &[]),
        ],
        vec![
            path_edge("AUTHORED", "User", 1, "MR", 42, 0, 0),
            path_edge("IN_PROJECT", "MR", 42, "Project", 100, 0, 1),
        ],
    );
    let out = enc(&r);
    assert!(out.contains("@paths"));
    assert!(!out.contains("@edges"));
    assert!(out.contains("path=0: User:1 --AUTHORED--> MR:42 --IN_PROJECT--> Project:100"));
}

#[test]
fn path_step_tie_is_stable_across_input_order() {
    let nodes = vec![
        node("User", 1, &[]),
        node("MR", 10, &[]),
        node("MR", 20, &[]),
    ];
    let e1 = path_edge("AUTHORED", "User", 1, "MR", 10, 0, 0);
    let e2 = path_edge("REVIEWER", "User", 1, "MR", 20, 0, 0);
    let a = response("path_finding", nodes.clone(), vec![e1.clone(), e2.clone()]);
    let b = response("path_finding", nodes, vec![e2, e1]);
    assert_eq!(enc(&a), enc(&b));
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

#[test]
fn aggregation_function_appears_in_header() {
    let mut r = response(
        "aggregation",
        vec![node(
            "User",
            1,
            &[("username", json!("a")), ("c", json!(5))],
        )],
        vec![],
    );
    r.columns = Some(vec![aggregation_column("c", "count", None)]);
    let out = enc(&r);
    assert!(out.contains("aggregations:c(count)"));
}

#[test]
fn aggregation_preserves_server_order() {
    let mut r = response(
        "aggregation",
        vec![
            node("User", 1, &[("username", json!("a")), ("c", json!(100))]),
            node("User", 2, &[("username", json!("b")), ("c", json!(50))]),
        ],
        vec![],
    );
    r.columns = Some(vec![aggregation_column("c", "count", None)]);
    let out = enc(&r);
    let pos_100 = out.find("c=100").expect("c=100 missing");
    let pos_50 = out.find("c=50").expect("c=50 missing");
    assert!(pos_100 < pos_50, "server order not preserved:\n{out}");
}

#[test]
fn ungrouped_aggregation_inlines_values_in_header() {
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![aggregation_column("total", "count", Some(json!(42)))]);
    let out = enc(&r);
    assert!(out.contains("aggregations:total(count)"));
    assert!(out.contains("values:total=42"));
}

// ---------------------------------------------------------------------------
// traversal_path is excluded structurally (the GraphFormatter filter handles
// reserved keys; we trust GraphResponse here). We only assert the encoder
// doesn't accidentally surface internal property names that ARE in
// GraphResponse like `depth`.
// ---------------------------------------------------------------------------

#[test]
fn edge_depth_does_not_leak_into_node_rows() {
    let out = enc(&traversal_response());
    assert!(!out.contains(" depth="));
}
