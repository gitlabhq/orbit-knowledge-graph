use semver::Version;
use serde_json::{Value, json};

use super::encode::encode;
use super::fixtures::*;
use crate::graph::{ColumnDescriptor, GroupColumnDescriptor};

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
    assert!(out.contains("goon_version:1.0.0"));
    assert!(
        !out.contains("format_version:"),
        "field is named goon_version to disambiguate from the source format_version"
    );
    assert!(out.contains("nodes:3"));
    assert!(out.contains("edges:2"));
}

#[test]
fn empty_response_emits_section_markers() {
    let r = response("traversal", vec![], vec![]);
    let out = enc(&r);
    assert!(out.contains("@header\n"));
    assert!(out.contains("nodes:0"));
    assert!(out.contains("edges:0"));
    assert!(
        out.contains("@nodes\n"),
        "section markers always present so parsers stay uniform"
    );
    assert!(out.contains("@edges\n"));
}

#[test]
fn pagination_offset_emits_has_more_and_total() {
    let mut r = response("traversal", vec![node("User", 1, &[])], vec![]);
    r.pagination = Some(pagination(true, 50, true));
    let out = enc(&r);
    assert!(out.contains("has_more:true"));
    assert!(out.contains("total_rows:50"));
}

#[test]
fn pagination_no_more_omits_has_more() {
    let mut r = response("traversal", vec![node("User", 1, &[])], vec![]);
    r.pagination = Some(pagination(false, 1, false));
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
fn newline_does_not_double_escape_backslash() {
    // Production regression (MR description with markdown). A literal newline
    // byte must serialize as `\n`, not `\\n` — the latter is "literal
    // backslash followed by n" and breaks any inverse decoder.
    let r = response(
        "traversal",
        vec![node(
            "MR",
            1,
            &[("description", json!("line one\nline two"))],
        )],
        vec![],
    );
    let out = enc(&r);
    assert!(
        out.contains(r#"description="line one\nline two""#),
        "newline must be \\n, not \\\\n: {out}"
    );
    assert!(
        !out.contains("\\\\n"),
        "backslash must not be doubled when newline is escaped: {out}"
    );
}

#[test]
fn literal_backslash_with_control_does_not_collide() {
    // Different sources must not collapse to the same output: a real
    // newline encodes as `\n`, a literal backslash-n encodes as `\\n`.
    let r1 = response(
        "traversal",
        vec![node("MR", 1, &[("title", json!("a\nb"))])],
        vec![],
    );
    let r2 = response(
        "traversal",
        vec![node("MR", 1, &[("title", json!(r"a\nb"))])],
        vec![],
    );
    assert_ne!(
        enc(&r1),
        enc(&r2),
        "real newline and literal `\\n` must encode differently"
    );
}

#[test]
fn string_that_looks_like_native_token_is_quoted() {
    // A JSON string `"true"` must not render as the bare token `true`,
    // which would be indistinguishable from a real boolean.
    let r = response(
        "traversal",
        vec![node(
            "X",
            1,
            &[
                ("a", json!("true")),
                ("b", json!("false")),
                ("c", json!("null")),
                ("d", json!(true)),
            ],
        )],
        vec![],
    );
    let out = enc(&r);
    assert!(
        out.contains(r#"a="true""#),
        "string \"true\" must be quoted: {out}"
    );
    assert!(out.contains(r#"b="false""#));
    assert!(out.contains(r#"c="null""#));
    assert!(out.contains("d=true"), "real bool stays bare: {out}");
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
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![aggregation_column("c", "count")]);
    r.group_columns = Some(vec![property_group("severity", "v", "severity")]);
    r.rows = Some(vec![agg_row(&[
        ("severity", json!("critical")),
        ("c", json!(5)),
    ])]);
    let out = enc(&r);
    assert!(out.contains("aggregations:c(count)"));
    assert!(out.contains("group_by:severity(property)"));
}

#[test]
fn aggregation_descriptor_carries_target_node_alias() {
    // ColumnDescriptor.target identifies which node the aggregate ranges
    // over. Dropping it makes `count` ambiguous when a query has multiple
    // aggregatable nodes.
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![ColumnDescriptor {
        name: "user_count".into(),
        function: "count".into(),
        target: Some("u".into()),
        property: None,
    }]);
    r.group_columns = Some(vec![]);
    r.rows = Some(vec![agg_row(&[("user_count", json!(42))])]);
    assert!(enc(&r).contains("aggregations:user_count(count:u)"));
}

#[test]
fn aggregation_descriptor_carries_target_dot_property_for_max_over_property() {
    // For `max(target=v, property=updated_at)` the consumer needs to
    // know it's the max of v.updated_at, not just "max".
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![ColumnDescriptor {
        name: "latest_update".into(),
        function: "max".into(),
        target: Some("v".into()),
        property: Some("updated_at".into()),
    }]);
    r.group_columns = Some(vec![]);
    r.rows = Some(vec![agg_row(&[(
        "latest_update",
        json!("2026-05-08T22:55:58Z"),
    )])]);
    assert!(
        enc(&r).contains("aggregations:latest_update(max:v.updated_at)"),
        "max-over-property must surface both target and property: {}",
        enc(&r)
    );
}

#[test]
fn property_group_with_alias_surfaces_underlying_property() {
    // `{kind:property, node:v, property:severity, alias:severity_bucket}`
    // — without surfacing `severity` the reader sees `severity_bucket`
    // and can't tell which ontology property drives the dimension.
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![aggregation_column("count", "count")]);
    r.group_columns = Some(vec![GroupColumnDescriptor {
        name: "severity_bucket".into(),
        kind: "property".into(),
        node: "v".into(),
        property: Some("severity".into()),
        entity: None,
    }]);
    r.rows = Some(vec![agg_row(&[
        ("severity_bucket", json!("critical")),
        ("count", json!(5)),
    ])]);
    assert!(
        enc(&r).contains("group_by:severity_bucket(property:severity)"),
        "aliased property group must surface the underlying property: {}",
        enc(&r)
    );
}

#[test]
fn property_group_omits_property_when_alias_matches() {
    // No alias drift — keep the line terse rather than repeating.
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![aggregation_column("count", "count")]);
    r.group_columns = Some(vec![property_group("severity", "v", "severity")]);
    r.rows = Some(vec![agg_row(&[
        ("severity", json!("critical")),
        ("count", json!(5)),
    ])]);
    let out = enc(&r);
    assert!(out.contains("group_by:severity(property)"));
    assert!(
        !out.contains("property:severity"),
        "no need to repeat when alias matches: {out}"
    );
}

#[test]
fn null_group_bucket_renders_as_bare_null_not_dropped() {
    // ClickHouse can return NULL for a property-kind group key when rows
    // have no value for that column. The bucket itself is a meaningful
    // result ("rows with no severity assigned"); dropping the cell would
    // make the row look like `count=5` and lose the dimension entirely.
    // The string "null" is quoted (`"null"`) so the bare `null` token is
    // unambiguous.
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![aggregation_column("count", "count")]);
    r.group_columns = Some(vec![property_group("severity", "v", "severity")]);
    r.rows = Some(vec![
        agg_row(&[("severity", Value::Null), ("count", json!(5))]),
        agg_row(&[("severity", json!("critical")), ("count", json!(2))]),
    ]);
    let out = enc(&r);
    assert!(
        out.contains("severity=null count=5"),
        "null bucket must surface as `null`, not be dropped: {out}"
    );
    assert!(out.contains("severity=critical count=2"));
}

#[test]
fn null_metric_value_renders_as_bare_null_not_dropped() {
    // `min(updated_at)` over an empty bucket comes back as null. The
    // header still declares the column; the row should keep it.
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![aggregation_column("first_seen", "min")]);
    r.group_columns = Some(vec![property_group("status", "s", "status")]);
    r.rows = Some(vec![agg_row(&[
        ("status", json!("never_run")),
        ("first_seen", Value::Null),
    ])]);
    let out = enc(&r);
    assert!(
        out.contains("status=never_run first_seen=null"),
        "null metric must surface, not be dropped: {out}"
    );
}

#[test]
fn variable_length_traversal_edges_carry_depth() {
    // Variable-length traversal (`max_hops>1`) tags each hit with the hop
    // it was found at. Without surfacing it, depth-1 and depth-3 results
    // are indistinguishable in the output.
    let mut deep = edge("MEMBER_OF", "User", 1, "Group", 200);
    deep.depth = Some(2);
    let mut shallow = edge("MEMBER_OF", "User", 1, "Group", 100);
    shallow.depth = Some(1);
    let r = response(
        "traversal",
        vec![
            node("User", 1, &[]),
            node("Group", 100, &[]),
            node("Group", 200, &[]),
        ],
        vec![shallow, deep],
    );
    let out = enc(&r);
    assert!(
        out.contains("User:1 --> Group:100 depth=1"),
        "depth-1 edge must surface depth: {out}"
    );
    assert!(
        out.contains("User:1 --> Group:200 depth=2"),
        "depth-2 edge must surface depth: {out}"
    );
}

#[test]
fn fixed_traversal_edges_omit_depth_field() {
    // Single-hop traversal: depth is None. We must not write `depth=`.
    let r = response(
        "traversal",
        vec![node("User", 1, &[]), node("Group", 100, &[])],
        vec![edge("MEMBER_OF", "User", 1, "Group", 100)],
    );
    let out = enc(&r);
    assert!(
        !out.contains("depth="),
        "fixed-hop edges must not emit depth: {out}"
    );
}

#[test]
fn aggregation_property_grouping_emits_rows_section() {
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![aggregation_column("vulnerability_count", "count")]);
    r.group_columns = Some(vec![property_group("severity", "v", "severity")]);
    r.rows = Some(vec![
        agg_row(&[
            ("severity", json!("critical")),
            ("vulnerability_count", json!(120)),
        ]),
        agg_row(&[
            ("severity", json!("high")),
            ("vulnerability_count", json!(2350)),
        ]),
    ]);
    let out = enc(&r);
    assert!(
        out.contains("rows:2"),
        "header must declare row count: {out}"
    );
    assert!(out.contains("@rows\n"), "must emit @rows section: {out}");
    assert!(out.contains("severity=critical vulnerability_count=120"));
    assert!(out.contains("severity=high vulnerability_count=2350"));
}

#[test]
fn aggregation_preserves_server_row_order() {
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![aggregation_column("c", "count")]);
    r.group_columns = Some(vec![property_group("severity", "v", "severity")]);
    r.rows = Some(vec![
        agg_row(&[("severity", json!("high")), ("c", json!(100))]),
        agg_row(&[("severity", json!("low")), ("c", json!(50))]),
    ]);
    let out = enc(&r);
    let pos_100 = out.find("c=100").expect("c=100 missing");
    let pos_50 = out.find("c=50").expect("c=50 missing");
    assert!(pos_100 < pos_50, "server row order not preserved:\n{out}");
}

#[test]
fn aggregation_node_grouping_lifts_unique_nodes_into_at_nodes() {
    // Each row carries the grouped node inline; the encoder must dedup and
    // surface it once in @nodes so rows can stay one line as `Entity:id`.
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![aggregation_column("count", "count")]);
    r.group_columns = Some(vec![
        node_group("project", "p", "Project"),
        property_group("severity", "v", "severity"),
    ]);
    r.rows = Some(vec![
        agg_row(&[
            (
                "project",
                node_group_cell("Project", 278964, &[("name", json!("GitLab"))]),
            ),
            ("severity", json!("critical")),
            ("count", json!(12)),
        ]),
        agg_row(&[
            (
                "project",
                node_group_cell("Project", 278964, &[("name", json!("GitLab"))]),
            ),
            ("severity", json!("high")),
            ("count", json!(45)),
        ]),
    ]);
    let out = enc(&r);
    assert!(out.contains("group_by:project(node:Project),severity(property)"));
    assert!(
        out.contains("nodes:1"),
        "node-kind group must dedup to 1: {out}"
    );
    assert!(out.contains("Project(1):"));
    assert!(
        out.matches("278964 name=GitLab").count() == 1,
        "Project should appear in @nodes exactly once: {out}"
    );
    assert!(out.contains("project=Project:278964 severity=critical count=12"));
    assert!(out.contains("project=Project:278964 severity=high count=45"));
}

#[test]
fn ungrouped_aggregation_emits_single_row() {
    let mut r = response("aggregation", vec![], vec![]);
    r.columns = Some(vec![aggregation_column("total", "count")]);
    r.group_columns = Some(vec![]);
    r.rows = Some(vec![agg_row(&[("total", json!(42))])]);
    let out = enc(&r);
    assert!(out.contains("aggregations:total(count)"));
    assert!(out.contains("rows:1"));
    assert!(
        !out.contains("group_by:"),
        "ungrouped aggregation must not declare group_by: {out}"
    );
    assert!(out.contains("@rows\n"));
    assert!(
        out.contains("\ntotal=42\n"),
        "single-row aggregation must inline metric value: {out}"
    );
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
