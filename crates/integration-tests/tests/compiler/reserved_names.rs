use compiler::{QueryError, compile};

use super::setup::{embedded_model, test_ctx};

fn assert_reserved(query: &str, identifier: &str) {
    let expected = format!("identifier {identifier:?} conflicts with reserved prefix \"_orbit_\"");
    match compile(
        query,
        &embedded_model(),
        &test_ctx(),
        query_data_model::testkit::limits(),
    ) {
        Err(QueryError::Validation(message)) => assert_eq!(message, expected, "{query}"),
        other => panic!("{query} must be rejected, got {other:?}"),
    }
}

#[test]
fn reserved_names_are_rejected_in_every_position() {
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
        assert_reserved(query, identifier);
    }
}
