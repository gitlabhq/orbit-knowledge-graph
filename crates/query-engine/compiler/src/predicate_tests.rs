use std::sync::{Arc, LazyLock};

use crate::input::{BooleanExpression, FilterOp};
use crate::{Frontend, compile, compile_local, validate_normalize_gql};

static ONTOLOGY: LazyLock<Arc<ontology::Ontology>> =
    LazyLock::new(|| Arc::new(ontology::Ontology::load_embedded().unwrap()));

fn remote(query: &str) -> crate::CompiledQueryContext {
    compile(
        query,
        Frontend::Gql,
        &ONTOLOGY,
        &crate::testkit::non_admin_ctx(),
    )
    .unwrap_or_else(|error| panic!("{query}: {error}"))
}

#[test]
fn boolean_precedence_groups_and_id_promotion() {
    let query = "MATCH (n:Definition) WHERE n.id IN [1, 2] AND NOT n.name IN ['skip', 'omit'] AND (n.name = 'left' OR n.id IN [3, 4] AND NOT n.name = 'right') RETURN n.name";
    let input = validate_normalize_gql(query, &ONTOLOGY).unwrap();
    assert_eq!(input.nodes[0].node_ids, [1, 2]);
    assert!(input.nodes[0].filters.is_empty());
    assert!(matches!(&input.predicates[0], BooleanExpression::Not(_)));
    let BooleanExpression::Or(arms) = &input.predicates[1] else {
        panic!("OR group missing")
    };
    assert!(matches!(&arms[1], BooleanExpression::And(_)));
    assert_eq!(
        input
            .predicate_leaves()
            .filter(|leaf| leaf.filter.op == Some(FilterOp::In))
            .count(),
        2
    );
    let sql = remote(query).base.render();
    assert!(sql.contains("NOT") && sql.contains(" OR "), "{sql}");
    assert!(
        sql.contains("n.id IN [1, 2]") || sql.contains("n.id IN (1, 2)"),
        "{sql}"
    );
}

#[test]
fn boolean_comparisons_compile_for_both_backends() {
    for predicate in [
        "NOT n.name IN ['skip', 'omit']",
        "NOT (n.name CONTAINS 'skip' OR n.name STARTS WITH 'omit')",
        "n.name ENDS WITH 'tail' OR NOT n.name IS NULL",
        "NOT NOT n.name IS NOT NULL",
        "NOT(NOT(n.name IS NULL))",
        "n.id < 10 OR n.id >= 20 AND NOT n.id <> 30",
        "NOT n.id IN []",
        "n.name = 'one' OR (n.name = 'two' AND NOT n.name = 'three')",
    ] {
        let query = format!("MATCH (n:Definition {{id: 1}}) WHERE {predicate} RETURN n.name");
        let remote = remote(&query);
        let local = compile_local(&query, Frontend::Gql, &ONTOLOGY).unwrap();
        assert!(!remote.base.sql.is_empty());
        assert!(!local.base.sql.is_empty());
    }
}

#[test]
fn boolean_membership_preserves_null_and_empty_list_semantics() {
    let nonempty = remote("MATCH (n:MergeRequest {id: 1}) WHERE NOT n.merged_at IN [datetime('2025-01-01T00:00:00Z'), datetime('2025-02-01T00:00:00Z')] RETURN n.id").base.render();
    assert!(
        nonempty.contains("if((n.merged_at IS NULL), NULL,"),
        "{nonempty}"
    );
    let empty = remote("MATCH (n:MergeRequest {id: 1}) WHERE NOT n.merged_at IN [] RETURN n.id")
        .base
        .render();
    assert!(empty.contains("(NOT false)"), "{empty}");
    assert!(!empty.contains("merged_at IS NULL"), "{empty}");
}

#[test]
fn boolean_cross_alias_predicates_keep_nodes_and_named_edges_visible() {
    for query in [
        "MATCH (a:Definition {id: 1})-[call:CALLS]->(b:Definition) WHERE NOT b.name IN ['skip'] OR call.source_id = 3 RETURN b.name",
        "MATCH (a:Definition {id: 1})-[call:CALLS]->(b:Definition) WHERE NOT call.branch STARTS WITH 'main' OR call.target_id IN [3, 4] RETURN b.name",
        "MATCH (u:User {id: 1})-[authored:AUTHORED]->(m:MergeRequest) WHERE u.username = 'alice' OR NOT m.state = 3 OR authored.target_id = 7 RETURN m.title",
        "MATCH (a:Definition {id: 1})-[:CALLS]->(b:Definition) WHERE a.name = b.name OR NOT b.name = 'skip' RETURN b.name",
        "MATCH (a:Definition)-[first:CALLS]->(b:Definition)-[second:CALLS]->(c:Definition {id: 7}) WHERE first.source_id = 3 OR second.target_id = 4 RETURN a.name",
    ] {
        let sql = remote(query).base.render();
        assert!(sql.contains(" OR "), "{sql}");
        assert!(sql.contains("startsWith("), "{sql}");
        if query.contains(":Definition") && !query.contains("call.branch") {
            compile_local(query, Frontend::Gql, &ONTOLOGY).unwrap();
        }
    }
}

#[test]
fn boolean_aggregation_keeps_filter_dependencies() {
    for query in [
        "MATCH (m:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1}) WHERE m.state = 3 OR NOT p.name = 'skip' RETURN count(m)",
        "MATCH (a:Definition {id: 1})-[call:CALLS]->(b:Definition) WHERE NOT b.name = 'skip' OR call.target_id = 3 RETURN count(b)",
    ] {
        let sql = remote(query).base.render();
        assert!(
            sql.contains(" OR ") && sql.to_lowercase().contains("count"),
            "{sql}"
        );
        assert!(sql.contains("gl_"), "{sql}");
        if query.contains(":Definition") {
            compile_local(query, Frontend::Gql, &ONTOLOGY).unwrap();
        }
    }
}

#[test]
fn boolean_normalization_and_parameterization() {
    let query = "MATCH (m:MergeRequest {id: 1}) WHERE NOT m.state IN [1, 3] OR m.title = 'x\\\' OR 1=1 --' RETURN m.id";
    let compiled = remote(query);
    assert!(!compiled.base.sql.contains("OR 1=1 --"));
    let sql = compiled.base.render();
    assert!(sql.contains("opened") && sql.contains("merged"), "{sql}");
}

#[test]
fn boolean_validation_checks_every_branch() {
    for (predicate, expected) in [
        ("n.name = 'okay' OR NOT n.missing = 'bad'", "does not exist"),
        ("NOT n.id IN ['bad']", "not an integer"),
        ("n.name = 'okay' OR NOT n.name CONTAINS 'x'", "at least 3"),
        ("NOT n.id CONTAINS 'bad'", "string operators"),
        ("NOT token_match(n.id, 'word')", "text index"),
        ("NOT n.traversal_path STARTS WITH '2/'", "authorized"),
        ("NOT n.traversal_path CONTAINS '1/'", "only eq"),
        ("NOT n.name = n.id", "type mismatch"),
    ] {
        let query = format!("MATCH (n:Definition {{id: 1}}) WHERE {predicate} RETURN n.id");
        let error = compile(
            &query,
            Frontend::Gql,
            &ONTOLOGY,
            &crate::testkit::non_admin_ctx(),
        )
        .expect_err("must reject");
        assert!(error.to_string().contains(expected), "{query}: {error}");
    }
    let error = compile(
        "MATCH (u:User {id: 1}) WHERE u.username = 'alice' OR NOT u.is_admin = true RETURN u.id",
        Frontend::Gql,
        &ONTOLOGY,
        &crate::testkit::non_admin_ctx(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("administrator"), "{error}");
}

#[test]
fn boolean_selectivity_does_not_promote_disjunctive_or_negated_ids() {
    let input = validate_normalize_gql(
        "MATCH (n:Definition) WHERE n.id IN [1] OR n.id IN [2] RETURN n.id",
        &ONTOLOGY,
    )
    .unwrap();
    assert!(input.nodes[0].node_ids.is_empty());
    assert!(input.nodes[0].id_range.is_none());
    for predicate in ["NOT n.id IN [1]", "n.id IN [1] OR NOT n.id IN [2]"] {
        let query = format!("MATCH (n:Definition) WHERE {predicate} RETURN n.id");
        let error = validate_normalize_gql(&query, &ONTOLOGY).unwrap_err();
        assert!(
            error.to_string().contains("avoid full edge table scans"),
            "{error}"
        );
    }
}

#[test]
fn boolean_expression_bounds_and_unsupported_shapes_reject() {
    let query = format!(
        "MATCH (n:Definition {{id: 1}}) WHERE {}n.name = 'test' RETURN n.id",
        "NOT ".repeat(32)
    );
    remote(&query);
    for predicate in [
        format!("{}n.name = 'test'", "NOT ".repeat(33)),
        format!("{}n.name = 'test'{}", "(".repeat(33), ")".repeat(33)),
    ] {
        let query = format!("MATCH (n:Definition {{id: 1}}) WHERE {predicate} RETURN n.id");
        let error = validate_normalize_gql(&query, &ONTOLOGY).unwrap_err();
        assert!(error.to_string().contains("nesting"), "{error}");
    }
    let query = "MATCH (a:Definition {id: 1})-[calls:CALLS*1..2]->(b:Definition) WHERE NOT calls.source_id = 1 RETURN b.id";
    let error = validate_normalize_gql(query, &ONTOLOGY).unwrap_err();
    assert!(error.to_string().contains("relationship-list"), "{error}");
    let predicate = std::iter::repeat_n("NOT n.name = 'test'", 257)
        .collect::<Vec<_>>()
        .join(" AND ");
    let query = format!("MATCH (n:Definition {{id: 1}}) WHERE {predicate} RETURN n.id");
    assert!(
        validate_normalize_gql(&query, &ONTOLOGY)
            .unwrap_err()
            .to_string()
            .contains("256 leaves")
    );
}

#[test]
fn positive_conjuncts_keep_pushdown_and_id_ranges() {
    let query = "MATCH (a:Definition)-[:CALLS]->(b:Definition) WHERE a.id IN [1, 2] AND a.id >= 1 AND a.id <= 10 AND b.name = 'keep' AND NOT b.name IN ['skip'] RETURN b.id";
    let input = validate_normalize_gql(query, &ONTOLOGY).unwrap();
    assert_eq!(input.nodes[0].node_ids, [1, 2]);
    let range = input.nodes[0].id_range.as_ref().unwrap();
    assert_eq!((range.start, range.end), (1, 10));
    assert_eq!(input.nodes[1].filters["name"][0].value_str(), Some("keep"));
    let sql = remote(query).base.render();
    assert!(
        sql.contains("gl_definition AS b FINAL WHERE") && sql.contains("b.name = 'keep'"),
        "{sql}"
    );
    let sql = remote("MATCH (u:User {id: 1})-[authored:AUTHORED]->(m:MergeRequest) WHERE authored.target_id = 7 AND NOT m.state = 3 RETURN m.title").base.render();
    assert!(
        sql.contains("gl_edge AS e0") && sql.contains("e0.target_id = 7"),
        "{sql}"
    );
}

#[test]
fn boolean_predicates_keep_authorization_and_cursor_requirements() {
    use crate::types::{AccessLevel, AuthorizedPath, SecurityContext};
    let query = "MATCH (v:Vulnerability {id: 1}) WHERE v.id = 1 OR NOT v.id = 2 RETURN v.id PAGE 2";
    let sql = remote(query).base.render();
    assert!(
        sql.contains("false") && sql.contains("ORDER BY") && sql.contains("LIMIT 3"),
        "{sql}"
    );
    let query =
        "MATCH (v:Vulnerability {id: 1}) WHERE NOT v.traversal_path STARTS WITH '1/' RETURN v.id";
    let error = compile(
        query,
        Frontend::Gql,
        &ONTOLOGY,
        &crate::testkit::non_admin_ctx(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("authorized"), "{error}");
    let context = SecurityContext::new_with_roles(
        1,
        vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
    )
    .unwrap();
    compile(query, Frontend::Gql, &ONTOLOGY, &context).unwrap();
}

#[test]
fn boolean_tokens_and_edge_validation() {
    for function in ["token_match", "all_tokens", "any_tokens"] {
        let query = format!(
            "MATCH (n:Definition {{id: 1}}) WHERE NOT {function}(n.name, 'word') RETURN n.id"
        );
        let compiled = remote(&query);
        assert!(compiled.base.render().contains("NOT"));
        assert!(compile_local(&query, Frontend::Gql, &ONTOLOGY).is_err());
    }
    for (predicate, expected) in [
        ("NOT call._deleted = true", "private edge"),
        (
            "call.source_id = 1 OR NOT call.missing = 1",
            "unknown edge column",
        ),
        ("NOT call.target_id = 'bad'", "not an integer"),
        ("NOT call.traversal_path STARTS WITH '2/'", "authorized"),
    ] {
        let query = format!(
            "MATCH (a:Definition {{id: 1}})-[call:CALLS]->(b:Definition) WHERE {predicate} RETURN b.id"
        );
        let error = compile(
            &query,
            Frontend::Gql,
            &ONTOLOGY,
            &crate::testkit::non_admin_ctx(),
        )
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn empty_membership_remains_a_filter_and_multiple_matches_conjoin_groups() {
    for query in [
        "MATCH (n:Definition) WHERE n.id IN [] RETURN n.id",
        "MATCH (n:Definition) WHERE n.id IN [] AND n.id >= 1 AND n.id <= 3 RETURN n.id",
        "MATCH (n:Definition) WHERE n.id IN [1, 2] AND n.id IN [] RETURN n.id",
    ] {
        assert!(remote(query).base.render().contains("false"));
    }
    let query = "MATCH (a:Definition {id: 1}) WHERE a.name = 'one' OR a.name = 'two' MATCH (a)-[:CALLS]->(b:Definition) WHERE b.name = 'three' OR NOT b.name = 'four' RETURN b.name";
    let input = validate_normalize_gql(query, &ONTOLOGY).unwrap();
    assert_eq!(input.predicates.len(), 2);
    let sql = remote(query).base.render();
    assert!(
        sql.contains("((a.name = 'one') OR (a.name = 'two'))"),
        "{sql}"
    );
    assert!(
        sql.contains("((b.name = 'three') OR (NOT (b.name = 'four')))"),
        "{sql}"
    );
}

#[test]
fn empty_denormalized_membership_keeps_the_false_filter() {
    for projection in ["count(m)", "m.id"] {
        let query = format!(
            "MATCH (u:User {{id: 1}})-[:REVIEWER]->(m:MergeRequest) WHERE m.state IN [] RETURN {projection}"
        );
        let sql = remote(&query).base.render();
        assert!(sql.contains("gl_merge_request AS m FINAL"), "{sql}");
        assert!(sql.contains("false"), "{sql}");
        assert!(
            sql.contains("(false AND") || sql.contains("AND false)"),
            "{sql}"
        );
    }
}

#[test]
fn empty_denormalized_membership_keeps_neighbors_center_filter() {
    for pattern in [
        "(m:MergeRequest {id: 2})-[:IN_PROJECT]->(n)",
        "(m:MergeRequest {id: 2})<-[:REVIEWER]-(n)",
        "(m:MergeRequest {id: 2})-[:IN_PROJECT]-(n)",
    ] {
        let sql = remote(&format!("MATCH {pattern} WHERE m.state IN [] RETURN n"))
            .base
            .render();
        assert!(sql.contains("gl_merge_request AS m FINAL"), "{sql}");
        assert!(
            sql.contains("(false AND") || sql.contains("AND false)"),
            "{sql}"
        );
    }
}

#[test]
fn traversal_path_property_comparisons_fail_as_client_errors() {
    for predicate in [
        "a.traversal_path = b.name",
        "NOT a.traversal_path = b.name",
        "a.name = b.traversal_path OR a.id = 1",
        "NOT a.traversal_path = a.name",
        "a.traversal_path = a.name",
    ] {
        let query = format!(
            "MATCH (a:Definition {{id: 1}})-[:CALLS]->(b:Definition) WHERE {predicate} RETURN b.id"
        );
        let error = compile(
            &query,
            Frontend::Gql,
            &ONTOLOGY,
            &crate::testkit::non_admin_ctx(),
        )
        .unwrap_err();
        assert!(matches!(error, crate::QueryError::Validation(_)), "{error}");
        assert!(
            error
                .to_string()
                .contains("property comparisons cannot reference traversal_path"),
            "{error}"
        );
    }
}

#[test]
fn same_node_disjunction_narrows_edges_before_dedup() {
    for projection in ["b.id", "count(b)"] {
        let query = format!(
            "MATCH (a:Definition)-[:CALLS]->(b:Definition) WHERE a.id = 1 OR a.id = 2 RETURN {projection}"
        );
        let input = validate_normalize_gql(&query, &ONTOLOGY).unwrap();
        assert!(input.nodes[0].node_ids.is_empty());
        let sql = remote(&query).base.render();
        let frontier_end = sql.find(") SELECT ").expect("frontier CTE");
        assert!(
            sql[..frontier_end].contains("((a.id = 1) OR (a.id = 2))"),
            "{sql}"
        );
        assert!(
            sql.contains("e0.source_id IN (SELECT id FROM _filter_a)"),
            "{sql}"
        );
        if projection == "count(b)" {
            let membership = sql
                .rfind("e0.source_id IN (SELECT id FROM _filter_a)")
                .unwrap();
            let dedup = sql[membership..].find("LIMIT 1 BY");
            assert!(dedup.is_some(), "frontier must precede edge dedup: {sql}");
        }
    }
    let query =
        "MATCH (a:Definition)-[:CALLS]->(b:Definition) WHERE a.id = 1 OR b.id = 2 RETURN b.id";
    assert!(
        validate_normalize_gql(query, &ONTOLOGY)
            .unwrap_err()
            .to_string()
            .contains("avoid full edge table scans")
    );
    for (query, frontier, predicate) in [
        (
            "MATCH (a:Definition)-[:CALLS]->(b:Definition)-[:CALLS]->(c:Definition) WHERE a.name = 'first' OR a.name = 'second' RETURN c.id",
            "_filter_a",
            "((a.name = 'first') OR (a.name = 'second'))",
        ),
        (
            "MATCH (m:MergeRequest)-[:IN_PROJECT]->(p:Project) WHERE p.id = 1 OR p.id = 2 RETURN m.id",
            "_candidate_p",
            "((p.id = 1) OR (p.id = 2))",
        ),
    ] {
        let sql = remote(query).base.render();
        let frontier_end = sql.find(") SELECT ").expect("frontier CTE");
        assert!(sql[..frontier_end].contains(predicate), "{sql}");
        assert!(
            sql.contains(&format!("IN (SELECT id FROM {frontier})")),
            "{sql}"
        );
        assert!(
            sql.matches(predicate).count() >= 2,
            "latest node scan must recheck the predicate: {sql}"
        );
    }
}
