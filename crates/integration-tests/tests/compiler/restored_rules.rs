use compiler::{Frontend, QueryError, compile, compile_local};

use super::setup::{embedded_local_model, embedded_model, test_ctx};

fn rejected(query: &str, frontend: Frontend) -> String {
    match compile(query, frontend, &embedded_model(), &test_ctx()) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("{query} must be rejected"),
    }
}

fn assert_rejected(query: &str, frontend: Frontend, expected: &str) {
    let message = rejected(query, frontend);
    assert!(message.contains(expected), "{query}: {message}");
}

const CONSTANT: &str = "every condition must compare a property of a matched node or relationship with a value or another property";

#[test]
fn property_compared_with_itself_is_rejected() {
    assert_rejected(
        "MATCH (u:User {id: 1})-[:AUTHORED]->(mr:MergeRequest) WHERE mr.iid = mr.iid RETURN mr",
        Frontend::Gql,
        CONSTANT,
    );
}

#[test]
fn empty_in_list_is_rejected() {
    for query in [
        "MATCH (mr:MergeRequest) WHERE mr.state IN [] RETURN mr",
        "MATCH (mr:MergeRequest) WHERE mr.id IN [] RETURN mr",
    ] {
        assert_rejected(query, Frontend::Gql, CONSTANT);
    }
}

#[test]
fn nested_id_lists_keep_the_in_list_cap() {
    let ids: Vec<String> = (1..=150).map(|id| id.to_string()).collect();
    let ids = ids.join(", ");
    assert_rejected(
        &format!("MATCH (p:Project) WHERE p.name = 'x' AND NOT p.id IN [{ids}] RETURN p"),
        Frontend::Gql,
        "an IN list is 150; the maximum is 100",
    );
    compile(
        &format!("MATCH (p:Project) WHERE p.id IN [{ids}] RETURN p"),
        Frontend::Gql,
        &embedded_model(),
        &test_ctx(),
    )
    .expect("a top-level id list pins up to 500 ids");
}

#[test]
fn text_operators_on_relationship_columns_are_rejected() {
    assert_rejected(
        "MATCH (u:User {id: 1})-[r:AUTHORED]->(mr:MergeRequest) WHERE r.source_kind STARTS WITH 'Use' RETURN mr",
        Frontend::Gql,
        "does not support substring matching",
    );
    let json = serde_json::json!({
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "mr", "entity": "MergeRequest"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr",
            "filters": {"target_kind": {"token_match": "MergeRequest"}}}]
    });
    assert_rejected(
        &json.to_string(),
        Frontend::JsonDsl,
        "does not support substring matching",
    );
}

#[test]
fn measure_inputs_must_be_stored() {
    let query = "MATCH (d:Definition) WHERE d.id = 1 RETURN max(d.content) AS longest";
    let error = compile_local(query, Frontend::Gql, &embedded_local_model())
        .expect_err("the local store does not hold Definition.content");
    assert!(
        matches!(&error, QueryError::AllowlistRejected(message)
            if message == "field \"content\" on Definition is not available in this data model"),
        "{error}"
    );
}

#[test]
fn duplicate_json_node_ids_are_rejected() {
    let json = serde_json::json!({
        "query_type": "aggregation",
        "nodes": [
            {"id": "p", "entity": "Project", "node_ids": [1]},
            {"id": "mr", "entity": "MergeRequest"},
            {"id": "mr", "entity": "User", "filters": {"username": "x"}}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
        "aggregations": [{"count": "mr"}]
    });
    assert_rejected(&json.to_string(), Frontend::JsonDsl, "mr is bound twice");
}

#[test]
fn json_fields_a_query_type_does_not_use_are_checked() {
    let cases = [
        (
            serde_json::json!({
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "Group", "node_ids": [1]},
                    {"id": "b", "entity": "Project", "node_ids": [2]},
                    {"id": "c", "entity": "User"}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2, "rel_types": ["CONTAINS"]}
            }),
            "node \"c\" is not referenced by any relationship or path",
        ),
        (
            serde_json::json!({
                "query_type": "aggregation",
                "nodes": [{"id": "g", "entity": "Group", "node_ids": [1]}],
                "aggregations": [{"count": "g"}],
                "path": {"type": "shortest", "from": "g", "to": "g", "max_depth": 1, "rel_types": ["CONTAINS"]}
            }),
            "aggregation over shortest paths is not supported",
        ),
        (
            serde_json::json!({
                "query_type": "aggregation",
                "nodes": [
                    {"id": "p", "entity": "Project", "node_ids": [1], "columns": ["no_such_field"]},
                    {"id": "mr", "entity": "MergeRequest"}
                ],
                "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
                "aggregations": [{"count": "mr"}]
            }),
            "no_such_field",
        ),
        (
            serde_json::json!({
                "query_type": "neighbors",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "x"}],
                "neighbors": {"direction": "both"}
            }),
            "unknown variable x",
        ),
        (
            serde_json::json!({
                "query_type": "aggregation",
                "nodes": [{"id": "mr", "entity": "MergeRequest", "node_ids": [1]}],
                "aggregations": [{"count": "mr"}],
                "order_by": "mr.no_such_field"
            }),
            "unknown property MergeRequest.no_such_field",
        ),
    ];
    for (query, expected) in cases {
        assert_rejected(&query.to_string(), Frontend::JsonDsl, expected);
    }
}
