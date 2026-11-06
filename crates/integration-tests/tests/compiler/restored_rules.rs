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
