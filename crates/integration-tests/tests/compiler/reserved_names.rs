use compiler::{Frontend, QueryError, compile};

use super::setup::{embedded_model, test_ctx};

fn assert_reserved(query: &str, frontend: Frontend, identifier: &str) {
    let expected = format!("identifier {identifier:?} conflicts with reserved prefix \"_orbit_\"");
    match compile(query, frontend, &embedded_model(), &test_ctx()) {
        Err(QueryError::Validation(message)) => assert_eq!(message, expected, "{query}"),
        other => panic!("{query} must be rejected, got {other:?}"),
    }
}

#[test]
fn gql_rejects_reserved_names_in_every_position() {
    let cases = [
        (
            "MATCH (_orbit_p:Project {id: 1}) RETURN _orbit_p",
            "_orbit_p",
        ),
        (
            "MATCH (u:User {id: 1})-[_orbit_r:AUTHORED]->(mr:MergeRequest) RETURN u, mr",
            "_orbit_r",
        ),
        (
            "MATCH (u:User {id: 1})-[r:AUTHORED]->(mr:MergeRequest) RETURN u, type(r) AS _orbit_e0_kind, mr",
            "_orbit_e0_kind",
        ),
        (
            "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project) WHERE p.id = 1 RETURN p, max(mr.iid) AS _orbit_p_id",
            "_orbit_p_id",
        ),
        (
            "MATCH (mr:MergeRequest) WHERE mr.id IN [1, 2] RETURN mr, mr.iid AS _orbit_mr_id, count(mr) AS n",
            "_orbit_mr_id",
        ),
        (
            "MATCH (center:User)-[r]-(_orbit_n) WHERE center.id IN [1] RETURN center, _orbit_n",
            "_orbit_n",
        ),
    ];
    for (query, identifier) in cases {
        assert_reserved(query, Frontend::Gql, identifier);
    }
}

#[test]
fn json_rejects_reserved_names_in_every_position() {
    let cases = [
        (
            serde_json::json!({
                "query_type": "traversal",
                "nodes": [
                    {"id": "project", "entity": "Project", "node_ids": [1]},
                    {"id": "_orbit", "entity": "MergeRequest"}
                ],
                "relationships": [{"type": "IN_PROJECT", "from": "_orbit", "to": "project"}]
            }),
            "_orbit",
        ),
        (
            serde_json::json!({
                "query_type": "aggregation",
                "nodes": [
                    {"id": "mr", "entity": "MergeRequest"},
                    {"id": "p", "entity": "Project", "node_ids": [1]}
                ],
                "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
                "group_by": ["p"],
                "aggregations": [{"max": "mr.iid", "as": "_orbit_p_id"}]
            }),
            "_orbit_p_id",
        ),
        (
            serde_json::json!({
                "query_type": "aggregation",
                "nodes": [{"id": "mr", "entity": "MergeRequest", "node_ids": [1, 2]}],
                "group_by": ["mr", {"key": "mr.iid", "as": "_orbit_mr_id"}],
                "aggregations": [{"count": "mr", "as": "n"}]
            }),
            "_orbit_mr_id",
        ),
    ];
    for (query, identifier) in cases {
        assert_reserved(&query.to_string(), Frontend::JsonDsl, identifier);
    }
}
