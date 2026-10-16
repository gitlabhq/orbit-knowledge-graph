use serde_json::{Value, json};

use super::run_duckdb_dir;

fn run_suite(query: Value, expect: &str) {
    let suite = tempfile::tempdir().unwrap();
    let presets = tempfile::tempdir().unwrap();
    let definitions: Vec<_> = ["alpha", "beta", "gamma", "delta"].iter().enumerate().map(|(index, name)| json!({
        "id": index + 1, "project_id": 1000, "traversal_path": "1/100/1000/", "branch": "main",
        "commit_sha": "abc123", "file_path": "src/test.rs", "fqn": name, "name": name,
        "definition_type": "Function", "start_line": 1, "end_line": 2, "start_byte": 0,
        "end_byte": 10, "start_char": 0, "end_char": 5,
    })).collect();
    let edges: Vec<_> = [(1, 2), (2, 3), (2, 4)]
        .iter()
        .map(|(from, to)| {
            json!({
                "traversal_path": "1/100/1000/", "source_id": from, "source_kind": "Definition",
                "relationship_kind": "CALLS", "target_id": to, "target_kind": "Definition",
            })
        })
        .collect();
    std::fs::write(
        presets.path().join("seed.yaml"),
        json!({"local": {
            "gl_definition": definitions, "gl_edge": edges,
        }})
        .to_string(),
    )
    .unwrap();
    let expect: Value = orbit_utils::yaml::from_str(expect).unwrap();
    std::fs::write(
        suite.path().join("contract.yaml"),
        json!({
            "config": {"duckdb": true}, "query": query, "expect": expect,
        })
        .to_string(),
    )
    .unwrap();
    run_duckdb_dir(
        suite.path().to_str().unwrap(),
        presets.path().to_str().unwrap(),
        "local",
    );
}

fn rejects(query: Value, expect: &str, message: &str) {
    assert_panics(|| run_suite(query, expect), message);
}

fn assert_panics(assertion: impl FnOnce() + std::panic::UnwindSafe, message: &str) {
    let failure = std::panic::catch_unwind(assertion).expect_err("wrong expectation must fail");
    let actual = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap();
    assert!(
        actual.contains(message),
        "expected '{message}', got '{actual}'"
    );
}

#[test]
fn compound_filters_check_every_operator() {
    let query = json!({"gql": "MATCH (d:Definition {id: 1}) RETURN d.name"});
    run_suite(
        query.clone(),
        "node_count: 1\nnodes: {Definition: {ids: [1], filters: {name: {starts_with: al, ends_with: ha}}}}",
    );
    rejects(
        query,
        "node_count: 1\nnodes: {Definition: {ids: [1], filters: {name: {starts_with: al, ends_with: zz}}}}",
        "failed filter assertion",
    );
}

#[test]
fn typed_edges_check_endpoint_types_and_keep_pair_support() {
    let query = json!({"gql": "MATCH (d:Definition {id: 1})-[:CALLS]->(t:Definition) RETURN d.name, t.name"});
    run_suite(
        query.clone(),
        "node_count: 2\nnodes: {Definition: {ids: [1, 2]}}\nedge_exists: {CALLS: [[1, 2], {from: Definition, from_id: 1, to: Definition, to_id: 2}]}\nedge_absent: {CALLS: [{from: File, from_id: 1, to: Definition, to_id: 2}]}",
    );
    rejects(
        query.clone(),
        "node_count: 2\nedge_exists: {CALLS: [{from: File, from_id: 1, to: Definition, to_id: 2}]}",
        "expected CALLS edge",
    );
    rejects(
        query,
        "node_count: 2\nedge_exists: {CALLS: [[1, 2]]}\nedge_absent: {CALLS: [{from: Definition, from_id: 1, to: Definition, to_id: 2}]}",
        "unexpected CALLS edge",
    );
}

#[test]
fn compile_errors_require_all_fragments_and_frontend_configuration() {
    let query = json!({"gql": "CREATE (d:Definition)"});
    run_suite(
        query.clone(),
        "compile_error_contains: {gql: ['Orbit query syntax', 'line 1, column 1']}\ncompile_error_not_contains: [CREATE]",
    );
    rejects(
        query.clone(),
        "compile_error: true\ncompile_error_contains: ['Orbit query syntax', impossible-message]",
        "does not contain 'impossible-message'",
    );
    rejects(
        query.clone(),
        "compile_error: {json: error}",
        "compile_error missing frontend 'gql'",
    );
    rejects(
        query,
        "compile_error: true\ncompile_error_not_contains: {json: []}",
        "expectation missing frontend 'gql'",
    );
}

#[test]
fn exact_null_and_string_properties_do_not_accept_missing_or_other_types() {
    let query = json!({"gql": "MATCH (d:Definition {id: 1}) RETURN d.name"});
    rejects(
        query.clone(),
        "nodes: {Definition: {ids: [1], rows: [{id: 1, missing: null}]}}",
        "property 'missing' mismatch",
    );
    rejects(
        query,
        "nodes: {Definition: {ids: [1], rows: [{id: 1, name: 1}]}}",
        "property 'name' expected 1",
    );
}

#[test]
fn pages_check_exhaustion_and_global_node_order_on_both_frontends() {
    let query = json!({
        "json": json!({"query_type": "traversal", "nodes": [{"id": "d", "entity": "Definition", "columns": ["name"]}], "order_by": "d.id", "cursor": {"page_size": 2}}).to_string(),
        "gql": "MATCH (d:Definition) RETURN d.name ORDER BY d.id PAGE 2",
    });
    let pages = "pages:\n  - {node_count: 2, nodes: {Definition: {order: [1, 2]}}}\n  - {node_count: 2, nodes: {Definition: {order: [3, 4]}}}";
    run_suite(
        query.clone(),
        &format!(
            "{pages}\nall_pages: {{node_order: {{Definition: [1, 2, 3, 4]}}, no_duplicate_ids: true, matches_unpaginated: true}}"
        ),
    );
    rejects(
        query.clone(),
        &format!("{pages}\nall_pages: {{node_order: {{Definition: [1, 2, 4, 3]}}}}"),
        "collected Definition order mismatch",
    );
    rejects(
        query,
        "pages: [{node_count: 2, nodes: {Definition: {order: [1, 2]}}}]",
        "page sequence must end at exhaustion",
    );
}

#[test]
fn page_uniqueness_checks_group_nodes_and_typed_edges() {
    let grouped = json!({"gql": "MATCH (d:Definition {id: 2})-[:CALLS]->(t:Definition) RETURN d, t, count(t) AS total PAGE 1"});
    let pages = "pages:\n  - {groups: {d: {entity: Definition, ids: [2], count: 1}, t: {entity: Definition, ids: [3]}}}\n  - {groups: {d: {entity: Definition, ids: [2], count: 1}, t: {entity: Definition, ids: [4]}}}";
    run_suite(
        grouped.clone(),
        &format!("{pages}\nall_pages: {{matches_unpaginated: true}}"),
    );
    rejects(
        grouped,
        &format!("{pages}\nall_pages: {{no_duplicate_ids: true}}"),
        "duplicate d:Definition IDs",
    );

    let traversal = json!({"gql": "MATCH (a:Definition {id: 1})-[:CALLS]->(b:Definition)-[:CALLS]->(c:Definition) RETURN a.name, b.name, c.name PAGE 1"});
    let pages = "pages:\n  - {node_count: 3, nodes: {Definition: {ids: [1, 2, 3]}}, edges: {CALLS: [[1, 2], [2, 3]]}}\n  - {node_count: 3, nodes: {Definition: {ids: [1, 2, 4]}}, edges: {CALLS: [[1, 2], [2, 4]]}}";
    run_suite(
        traversal.clone(),
        &format!("{pages}\nall_pages: {{matches_unpaginated: true, edge_count: 3}}"),
    );
    rejects(
        traversal,
        &format!("{pages}\nall_pages: {{no_duplicate_edges: true}}"),
        "duplicate typed edges",
    );
}

#[test]
fn repeated_aggregation_runs_check_row_values() {
    let query = json!({"gql": "MATCH (d:Definition) RETURN d.definition_type, count(d) AS total"});
    run_suite(
        query,
        "repeat_count: 3\nrow_count: 1\nrow_values: [{total: 4}]",
    );
}

#[test]
fn repeated_responses_reject_changes_that_individual_expectations_allow() {
    use crate::visitor::tests::sample_grouped_aggregation_response;
    use query_engine::compiler::input::parse_input;

    let input = parse_input(r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User"}],"group_by":["u"],"aggregations":[{"count":"u","as":"c"}]}"#).unwrap();
    let expect = orbit_utils::yaml::from_str("groups: {u: {entity: User, count: 2}}").unwrap();
    let baseline = serde_json::to_value(sample_grouped_aggregation_response()).unwrap();
    super::assert_response(&input, baseline.clone(), &expect, "baseline");
    super::assert_repeated_response(&baseline, &baseline, 2, "repeat");

    for mutate in [
        (|response: &mut Value| response["rows"].as_array_mut().unwrap().reverse())
            as fn(&mut Value),
        |response: &mut Value| response["rows"][0]["c"] = json!(3),
        |response: &mut Value| response["rows"][0]["u"]["id"] = json!("3"),
        |response: &mut Value| {
            response["rows"][0]["u"]["properties"]["username"] = json!("changed")
        },
    ] {
        let mut response = baseline.clone();
        mutate(&mut response);
        super::assert_response(&input, response.clone(), &expect, "rerun");
        assert_panics(
            || super::assert_repeated_response(&baseline, &response, 2, "repeat"),
            "run 2 returned different ordered results",
        );
    }
}

#[test]
fn repeated_nodes_preserve_order_within_each_entity_type() {
    use crate::visitor::tests::sample_search_response;

    let mut baseline = serde_json::to_value(sample_search_response()).unwrap();
    baseline["nodes"] = json!([
        {"type": "User", "id": "1", "name": "Alice"},
        {"type": "Group", "id": "100", "name": "Public"},
        {"type": "User", "id": "2", "name": "Bob"},
    ]);
    let mut interleaved = baseline.clone();
    interleaved["nodes"].as_array_mut().unwrap().swap(0, 1);
    super::assert_repeated_response(&baseline, &interleaved, 2, "repeat");

    let mut reordered = baseline.clone();
    reordered["nodes"].as_array_mut().unwrap().swap(0, 2);
    assert_panics(
        || super::assert_repeated_response(&baseline, &reordered, 2, "repeat"),
        "different ordered results",
    );
    interleaved["nodes"][1]["name"] = json!("Changed");
    assert_panics(
        || super::assert_repeated_response(&baseline, &interleaved, 2, "repeat"),
        "different ordered results",
    );
}

#[test]
fn page_expectations_reject_fields_that_are_not_executed() {
    let query = json!({"gql": "MATCH (d:Definition) RETURN d.name PAGE 2"});
    for field in [
        "repeat_count: 2",
        "sql_contains: [SELECT]",
        "sql_not_contains: [DROP]",
        "compile_only: true",
        "compile_error: true",
        "compile_error_contains: [error]",
        "compile_error_not_contains: [error]",
        "hydration: none",
        "indexes_used: [{table: gl_definition, index: name}]",
        "pages: [{}]",
        "all_pages: {page_count: 1}",
    ] {
        rejects(
            query.clone(),
            &format!("pages: [{{{field}}}]"),
            "page 1 supports only result assertions",
        );
    }
}

#[test]
fn compile_only_rejects_runtime_expectations() {
    let query = json!({"gql": "MATCH (d:Definition) RETURN d.name"});
    run_suite(query.clone(), "compile_only: true\nsql_contains: [SELECT]");
    for field in [
        "node_count: 4",
        "repeat_count: 2",
        "pages: [{node_count: 4}]",
        "all_pages: {page_count: 1}",
        "indexes_used: [{table: gl_definition, index: name}]",
    ] {
        rejects(
            query.clone(),
            &format!("compile_only: true\n{field}"),
            "compile_only cannot be combined with runtime assertions",
        );
    }
}

#[test]
fn empty_aggregation_requires_rows_and_column_metadata() {
    use crate::visitor::tests::sample_grouped_aggregation_response;
    use query_engine::compiler::input::parse_input;

    let input = parse_input(r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User"}],"group_by":["u"],"aggregations":[{"count":"u","as":"c"}]}"#).unwrap();
    let expect = orbit_utils::yaml::from_str("empty_aggregation: true").unwrap();
    let mut baseline = serde_json::to_value(sample_grouped_aggregation_response()).unwrap();
    baseline["rows"] = json!([]);
    super::assert_response(&input, baseline.clone(), &expect, "empty aggregation");
    for field in ["rows", "columns", "group_columns"] {
        let mut response = baseline.clone();
        response.as_object_mut().unwrap().remove(field);
        assert_panics(
            || super::assert_response(&input, response, &expect, "empty aggregation"),
            &format!("aggregation response missing {field}"),
        );
    }
}

#[test]
fn traversal_rejects_aggregation_and_path_assertions() {
    let query = json!({"gql": "MATCH (d:Definition) RETURN d.name"});
    rejects(
        query.clone(),
        "node_count: 4\nrow_count: 0",
        "aggregation assertions require an aggregation query",
    );
    rejects(
        query,
        "node_count: 4\npath_count: 0",
        "path assertions require a path_finding query",
    );
}

#[test]
fn pagination_coverage_compares_typed_node_identities_without_excerpt_values() {
    use crate::visitor::tests::sample_search_response;
    use query_engine::compiler::input::parse_input;

    let input =
        parse_input(r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User"}]}"#).unwrap();
    let mut full = serde_json::to_value(sample_search_response()).unwrap();
    full["pagination"] = json!({"has_more": false, "truncated": false});
    let mut page = full.clone();
    page["nodes"][0]["username"] = json!("page-specific excerpt");
    let mut collected = super::PageCollection::default();
    collected.push(&super::validate_response(&input, page, "page"));
    collected.assert_matches(
        &super::validate_response(&input, full.clone(), "full"),
        "coverage",
    );

    full["nodes"][0]["type"] = json!("Group");
    assert_panics(
        || collected.assert_matches(&super::validate_response(&input, full, "full"), "coverage"),
        "paginated node set differs from unpaginated result",
    );
}

#[test]
fn numeric_metric_assertions_reject_numeric_strings() {
    use crate::visitor::ResponseView;
    use crate::visitor::tests::{sample_aggregation_response, sample_grouped_aggregation_response};
    use query_engine::compiler::input::parse_input;

    for assertion in [
        (|view: &ResponseView| view.assert_row_value_i64(0, "c", 2)) as fn(&ResponseView),
        |view: &ResponseView| view.assert_aggregation_value_i64("c", 2),
    ] {
        let check = |metric: Value| {
            let input = parse_input(r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User"}],"aggregations":[{"count":"u","as":"c"}]}"#).unwrap();
            let mut response = sample_aggregation_response();
            response.rows.as_mut().unwrap()[0].insert("c".into(), metric);
            assertion(&ResponseView::for_query(&input, response));
        };
        check(json!(2));
        assert_panics(|| check(json!("2")), "column 'c' is not an integer");
    }

    let check_group = |metric: Value| {
        let input = parse_input(r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User"}],"group_by":["u"],"aggregations":[{"count":"u","as":"c"}]}"#).unwrap();
        let mut response = sample_grouped_aggregation_response();
        response.rows.as_mut().unwrap()[0].insert("c".into(), metric);
        ResponseView::for_query(&input, response)
            .assert_group_row_value_i64("u", "User", 1, "c", 2);
    };
    check_group(json!(2));
    assert_panics(|| check_group(json!("2")), "column 'c' is not an integer");

    let check_average = |metric: Value| {
        let input = parse_input(r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User"}],"group_by":["u"],"aggregations":[{"avg":"u.id","as":"c"}]}"#).unwrap();
        let mut response = sample_grouped_aggregation_response();
        response.rows.as_mut().unwrap()[0].insert("c".into(), metric);
        ResponseView::for_query(&input, response)
            .assert_group_row_value_f64("u", "User", 1, "c", 2.5);
    };
    check_average(json!(2.5));
    assert_panics(|| check_average(json!("2.5")), "column 'c' is not a number");
}

#[test]
fn full_result_comparison_rejects_missing_groups_even_when_page_counts_match() {
    let mut query = json!({
        "query_type": "aggregation",
        "nodes": [{"id": "d", "entity": "Definition"}],
        "group_by": ["d.name"],
        "aggregations": [{"count": "d", "as": "total"}],
        "cursor": {"page_size": 3},
    });
    let compiled = query_engine::compiler::compile_local(
        &query.to_string(),
        query_engine::compiler::Frontend::JsonDsl,
        &std::sync::Arc::new(crate::load_unprefixed_ontology()),
    )
    .unwrap();
    let hash = compiled.pagination.query_hash;
    query["cursor"]["after"] = Value::String(query_engine::compiler::passes::cursor::encode(
        hash,
        &[Some("alpha".into())],
    ));
    let query = json!({"json": query.to_string()});
    let pages =
        "pages: [{row_count: 3, row_values: [{d_name: beta}, {d_name: delta}, {d_name: gamma}]}]";
    run_suite(
        query.clone(),
        &format!("{pages}\nall_pages: {{page_count: 1, no_duplicate_ids: true}}"),
    );
    rejects(
        query,
        &format!("{pages}\nall_pages: {{matches_unpaginated: true}}"),
        "paginated grouped rows differ from unpaginated result",
    );
}

#[test]
fn path_pages_compare_complete_signatures_against_unpaginated_output() {
    let query = json!({"json": json!({
        "query_type": "path_finding",
        "nodes": [{"id": "source", "entity": "Definition", "node_ids": [1]}, {"id": "destination", "entity": "Definition", "node_ids": [3, 4]}],
        "path": {"type": "shortest", "from": "source", "to": "destination", "max_depth": 3, "rel_types": ["CALLS"]},
        "cursor": {"page_size": 1},
    }).to_string()});
    run_suite(
        query,
        "pages:\n  - {node_count: 3, path_count: 1, path_destinations: {Definition: [3]}}\n  - {node_count: 3, path_count: 1, path_destinations: {Definition: [4]}}\nall_pages: {matches_unpaginated: true, edge_count: 3}",
    );
}
