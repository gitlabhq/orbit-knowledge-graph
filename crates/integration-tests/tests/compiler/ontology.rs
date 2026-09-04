//! Compiler fixtures against the embedded ontology, each paired with its
//! openCypher twin through the helpers in `setup.rs`. JSON-shape error tests
//! keep their schema-message assertions and gain the frontend's own rejection
//! beside them. The hydration fixtures build the internal `Hydration` query
//! type directly and have no twin.

use std::sync::Arc;

use super::setup::{
    admin_ctx, compile_both, embedded_ontology, reject_both, reject_opencypher, test_ctx,
};
use compiler::{
    AuthorizedPath, ColumnSelection, HydrationPlan, Input, InputNode, QueryError, QueryType,
    compile, compile_input,
};
use orbit_utils::traversal_path::TraversalPath;

#[test]
fn valid_column_in_order_by() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
        "limit": 10,
        "order_by": "u.username"
    }"#;
    compile_both(
        json,
        "MATCH (u:User {id: 1}) RETURN u.username ORDER BY u.username LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
}

#[test]
fn invalid_column_in_order_by() {
    let (json_err, cypher_err) = reject_both(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
            "limit": 10,
            "order_by": "u.nonexistent_column"
        }"#,
        "MATCH (u:User {id: 1}) RETURN u.username ORDER BY u.nonexistent_column LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    assert!(json_err.to_string().contains("does not exist"));
    assert!(
        cypher_err.to_string().contains("does not exist"),
        "{cypher_err}"
    );
}

#[test]
fn valid_column_in_filter() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "columns": ["username"], "filters": {"username": "admin"}}],
        "limit": 10
    }"#;
    compile_both(
        json,
        "MATCH (u:User {username: 'admin'}) RETURN u.username LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
}

#[test]
fn invalid_column_in_filter() {
    let (json_err, cypher_err) = reject_both(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "columns": ["username"], "filters": {"nonexistent_column": "value"}}],
            "limit": 10
        }"#,
        "MATCH (u:User {nonexistent_column: 'value'}) RETURN u.username LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    assert!(json_err.to_string().contains("nonexistent_column"));
    assert!(
        cypher_err.to_string().contains("nonexistent_column"),
        "{cypher_err}"
    );
}

#[test]
fn valid_column_in_aggregation() {
    compile_both(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]}],
            "aggregations": [{"count": "p.name", "as": "name_count"}],
            "limit": 10
        }"#,
        "MATCH (p:Project {id: 1}) RETURN count(p.name) AS name_count LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
}

#[test]
fn invalid_column_in_aggregation() {
    let (json_err, cypher_err) = reject_both(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]}],
            "aggregations": [{"sum": "p.invalid_property", "as": "total"}],
            "limit": 10
        }"#,
        "MATCH (p:Project {id: 1}) RETURN sum(p.invalid_property) AS total LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    assert!(json_err.to_string().contains("does not exist"));
    assert!(
        cypher_err.to_string().contains("does not exist"),
        "{cypher_err}"
    );
}

#[test]
fn invalid_entity_type_rejected() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "n", "entity": "NonexistentType", "node_ids": [1], "columns": ["name"]}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("NonexistentType"), "got: {msg}");
    // The enrichment must surface valid candidates, not strip or truncate them.
    assert!(msg.contains("Valid values:"), "got: {msg}");
    assert!(msg.contains("Branch"), "got: {msg}");
    assert!(msg.contains("WorkItem"), "got: {msg}");
    assert!(!msg.contains("more —"), "got: {msg}");

    let err = reject_opencypher(
        "MATCH (n:NonexistentType {id: 1}) RETURN n.name LIMIT 10",
        &embedded_ontology(),
    );
    assert!(matches!(err, QueryError::AllowlistRejected(_)), "{err:?}");
    let msg = err.to_string();
    assert!(msg.contains("NonexistentType"), "got: {msg}");
    assert!(msg.contains("Valid values:"), "got: {msg}");
    assert!(
        msg.contains("Branch") && msg.contains("WorkItem"),
        "got: {msg}"
    );
}

#[test]
fn invalid_filter_key_lists_valid_candidates() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"project_full_path": "x"}}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("project_full_path"), "got: {msg}");
    assert!(msg.contains("Valid values:"), "got: {msg}");
    assert!(msg.contains("username"), "got: {msg}");
    assert!(!msg.contains("more —"), "got: {msg}");
    // The opaque "or N other candidates" truncation must not leak through.
    assert!(!msg.contains("other candidates"), "got: {msg}");

    let err = reject_opencypher(
        "MATCH (u:User {project_full_path: 'x'}) RETURN u.username LIMIT 10",
        &embedded_ontology(),
    );
    let msg = err.to_string();
    assert!(msg.contains("project_full_path"), "got: {msg}");
    assert!(
        msg.contains("Valid values:") && msg.contains("username"),
        "got: {msg}"
    );
}

#[test]
fn invalid_group_by_property_lists_valid_fields() {
    let (json_err, cypher_err) = reject_both(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
            "group_by": ["p.reviewer_count"],
            "aggregations": [{"count": "p", "as": "c"}],
            "limit": 10
        }"#,
        "MATCH (p:Project {id: 1}) RETURN p.reviewer_count, count(p) AS c LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    for err in [json_err, cypher_err] {
        let msg = err.to_string();
        assert!(msg.contains("reviewer_count"), "got: {msg}");
        assert!(msg.contains("does not exist"), "got: {msg}");
        assert!(msg.contains("Valid fields"), "got: {msg}");
        assert!(msg.contains("name"), "got: {msg}");
    }
}

#[test]
fn malformed_group_by_entry_shows_expected_shapes() {
    let err = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
            "group_by": [{"node": "p", "property": "name"}],
            "aggregations": [{"count": "p", "as": "c"}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("/group_by/0"), "got: {msg}");
    assert!(
        msg.contains("\"<node-id>.<property>\"") && msg.contains("\"truncate\""),
        "got: {msg}"
    );
}

#[test]
fn bare_string_group_by_dotted_garbage_shows_expected_shapes() {
    let err = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
            "group_by": ["p.name.x"],
            "aggregations": [{"count": "p", "as": "c"}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("/group_by/0"), "got: {msg}");
    assert!(
        msg.contains("\"<node-id>\"") && msg.contains("\"<node-id>.<property>\""),
        "got: {msg}"
    );

    let err = reject_opencypher(
        "MATCH (p:Project {id: 1}) RETURN p.name.x, count(p) AS c LIMIT 10",
        &embedded_ontology(),
    );
    assert!(matches!(err, QueryError::Syntax(_)), "{err:?}");
    assert!(err.to_string().contains("Nested property access"), "{err}");
}

#[test]
fn bare_string_group_by_unknown_node_names_the_reference() {
    let err = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
            "group_by": ["name"],
            "aggregations": [{"count": "p", "as": "c"}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("undefined node \"name\""), "got: {msg}");

    let err = reject_opencypher(
        "MATCH (p:Project {id: 1}) RETURN name, count(p) AS c LIMIT 10",
        &embedded_ontology(),
    );
    assert!(matches!(err, QueryError::ReferenceError(_)), "{err:?}");
    assert!(err.to_string().contains("`name` is not bound"), "{err}");
}

#[test]
fn invalid_column_lists_valid_candidates() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "columns": ["bogus_col"]}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("bogus_col"), "got: {msg}");
    assert!(msg.contains("/nodes/0/columns"), "got: {msg}");
    assert!(msg.contains("Valid values"), "got: {msg}");
    assert!(msg.contains("username"), "got: {msg}");
    // The opaque oneOf fallthrough must not leak through.
    assert!(!msg.contains("under any of the schemas"), "got: {msg}");

    let err = reject_opencypher(
        "MATCH (u:User) RETURN u.bogus_col LIMIT 10",
        &embedded_ontology(),
    );
    assert!(matches!(err, QueryError::AllowlistRejected(_)), "{err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("bogus_col") && msg.contains("username"),
        "got: {msg}"
    );
}

#[test]
fn invalid_relationship_type_lists_valid_candidates() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "BOGUS_REL", "from": "u", "to": "n"}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("BOGUS_REL"), "got: {msg}");
    assert!(msg.contains("/relationships/0/type"), "got: {msg}");
    assert!(msg.contains("Valid values"), "got: {msg}");
    assert!(msg.contains("AUTHORED"), "got: {msg}");
    assert!(!msg.contains("under any of the schemas"), "got: {msg}");

    let err = reject_opencypher(
        "MATCH (u:User {id: 1})-[:BOGUS_REL]->(n:Note) RETURN u LIMIT 10",
        &embedded_ontology(),
    );
    assert!(matches!(err, QueryError::AllowlistRejected(_)), "{err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("BOGUS_REL") && msg.contains("AUTHORED"),
        "got: {msg}"
    );
}

#[test]
fn full_pipeline() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "n", "entity": "Note", "columns": ["confidential"], "filters": {"confidential": true}},
            {"id": "u", "entity": "User", "columns": ["username"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "limit": 25,
        "order_by": "-n.created_at"
    }"#;

    let result = compile_both(
        json,
        "MATCH (n:Note {confidential: true})<-[:AUTHORED]-(u:User)
         RETURN n.confidential, u.username
         ORDER BY n.created_at DESC
         LIMIT 25",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    // AUTHORED is FK-elided via author_id — no edge table scan.
    assert!(rendered.contains("gl_note"));
    assert!(rendered.contains("gl_user"));
    assert!(rendered.contains("LIMIT 26"));
}

#[test]
fn package_built_by_pipeline_traversal() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "pkg", "entity": "Package", "columns": ["name", "version", "package_type"], "filters": {"package_type": "npm"}},
            {"id": "pl", "entity": "Pipeline", "columns": ["id", "status"]}
        ],
        "relationships": [{"type": "BUILT_BY", "from": "pkg", "to": "pl"}],
        "limit": 25
    }"#;

    let result = compile_both(
        json,
        "MATCH (pkg:Package {package_type: 'npm'})-[:BUILT_BY]->(pl:Pipeline)
         RETURN pkg.name, pkg.version, pkg.package_type, pl.id, pl.status
         LIMIT 25",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("gl_package"));
    assert!(rendered.contains("gl_edge"));
    assert!(rendered.contains("'BUILT_BY'"));
    assert!(rendered.contains("(e0.source_kind = 'Package')"));
    assert!(rendered.contains("(e0.target_kind = 'Pipeline')"));
    assert!(rendered.contains("LIMIT 26"));
}

#[test]
fn basic_search_query() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{
            "id": "u",
            "entity": "User",
            "columns": ["username"],
            "filters": { "username": {"eq": "admin"} }
        }],
        "limit": 10
    }"#;

    let result = compile_both(
        json,
        "MATCH (u:User {username: 'admin'}) RETURN u.username LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(
        rendered.contains(" FINAL"),
        "search should use FINAL for dedup"
    );
    assert!(
        rendered.contains("_deleted"),
        "search should filter deleted rows"
    );
    assert!(rendered.contains("username"));
    assert!(rendered.contains("LIMIT 11"));
    assert!(
        !rendered.contains("JOIN"),
        "search queries should not have joins"
    );
}

#[test]
fn complex_search_query() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{
            "id": "u",
            "entity": "User",
            "columns": ["username", "state", "created_at"],
            "filters": {
                "username": {"starts_with": "admin"},
                "state": {"in": ["active", "blocked"]},
                "created_at": {"gte": "2024-01-01"}
            }
        }],
        "limit": 50,
        "order_by": "-u.created_at"
    }"#;

    let result = compile_both(
        json,
        "MATCH (u:User)
         WHERE u.username STARTS WITH 'admin' AND u.state IN ['active', 'blocked'] AND u.created_at >= '2024-01-01'
         RETURN u.username, u.state, u.created_at
         ORDER BY u.created_at DESC
         LIMIT 50",
        &embedded_ontology(),
        &test_ctx(),
    );
    // Uses ClickHouse `IN [...]` array syntax which sqlparser can't parse.
    let rendered = result.base.render();

    assert!(rendered.contains(" FINAL"));
    assert!(rendered.contains("_deleted"));
    assert!(rendered.contains("username"));
    assert!(rendered.contains("state"));
    assert!(rendered.contains("created_at"));
    assert!(rendered.contains("ORDER BY"));
    assert!(rendered.contains("DESC"));
    assert!(rendered.contains("LIMIT 51"));
    assert!(
        !rendered.contains("JOIN"),
        "search queries should not have joins"
    );
}

#[test]
fn search_with_specific_columns() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{ "id": "u", "entity": "User", "node_ids": [1], "columns": ["username", "state"] }],
        "limit": 10
    }"#;

    let result = compile_both(
        json,
        "MATCH (u:User {id: 1}) RETURN u.username, u.state LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_u_id"));
    assert!(rendered.contains("_gkg_u_type"));
    assert!(rendered.contains("u_username"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn search_with_wildcard_columns() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{ "id": "u", "entity": "User", "node_ids": [1], "columns": "*" }],
        "limit": 10
    }"#;

    let result = compile_both(
        json,
        "MATCH (u:User {id: 1}) RETURN * LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_u_id"));
    assert!(rendered.contains("_gkg_u_type"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

const USER_CONTAINS_PROJECT_JSON: &str = r#"{
    "query_type": "traversal",
    "nodes": [
        {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
        {"id": "p", "entity": "Project", "columns": ["name"]}
    ],
    "relationships": [{"type": "CONTAINS", "from": "u", "to": "p"}],
    "limit": 10
}"#;

const USER_CONTAINS_PROJECT_STATEMENT: &str =
    "MATCH (u:User {id: 1})-[:CONTAINS]->(p:Project) RETURN u.username, p.name LIMIT 10";

#[test]
fn traversal_with_columns() {
    let result = compile_both(
        USER_CONTAINS_PROJECT_JSON,
        USER_CONTAINS_PROJECT_STATEMENT,
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_u_id"));
    assert!(rendered.contains("_gkg_u_type"));
    assert!(rendered.contains("_gkg_p_id"));
    assert!(rendered.contains("_gkg_p_type"));
}

#[test]
fn aggregation_includes_mandatory_columns_for_group_by_node() {
    // `mr.columns` has no surface: mr is counted, not grouped, and the
    // compiler ignores columns on such a node, so the SQL matches.
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "mr", "entity": "MergeRequest", "columns": ["title"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "group_by": ["u"],
        "aggregations": [{"count": "mr", "as": "mr_count"}],
        "limit": 10
    }"#;

    let result = compile_both(
        json,
        "MATCH (u:User {id: 1})-[:AUTHORED]->(mr:MergeRequest)
         RETURN u, u.username, count(mr) AS mr_count
         LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_u_id"));
    assert!(rendered.contains("_gkg_u_type"));
    assert!(!rendered.contains("_gkg_mr_id"));
    assert!(!rendered.contains("_gkg_mr_type"));
    assert!(rendered.contains("COUNT()") || rendered.contains("countIf"));
    assert!(rendered.contains("GROUP BY"));
}

#[test]
fn path_finding_uses_gkg_path_not_node_columns() {
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "Project", "node_ids": [100], "columns": ["name"]},
            {"id": "end", "entity": "Project", "node_ids": [200], "columns": ["name"]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                 "rel_types": ["CONTAINS"]}
    }"#;

    let result = compile_both(
        json,
        "MATCH p = shortestPath((start:Project {id: 100})-[:CONTAINS*..3]->(`end`:Project {id: 200}))
         RETURN p, start.name, `end`.name",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_path"));
    assert!(result.base.result_context.query_type == Some(QueryType::PathFinding));
}

#[test]
fn result_context_populated() {
    let result = compile_both(
        USER_CONTAINS_PROJECT_JSON,
        USER_CONTAINS_PROJECT_STATEMENT,
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert_eq!(result.base.result_context.len(), 2);

    let user = result.base.result_context.get("u").unwrap();
    assert_eq!(user.entity_type, "User");
    assert_eq!(user.id_column, "_gkg_u_id");
    assert_eq!(user.type_column, "_gkg_u_type");

    let project = result.base.result_context.get("p").unwrap();
    assert_eq!(project.entity_type, "Project");
    assert_eq!(project.id_column, "_gkg_p_id");
    assert_eq!(project.type_column, "_gkg_p_type");

    assert!(rendered.contains("_gkg_u_id"));
    assert!(rendered.contains("_gkg_u_type"));
    assert!(rendered.contains("_gkg_p_id"));
    assert!(rendered.contains("_gkg_p_type"));
}

#[test]
fn multi_hop_traversal_generates_union_subquery() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p", "hops": [1, 3]}],
        "limit": 25
    }"#;

    let result = compile_both(
        json,
        "MATCH (u:User {id: 1})-[:MEMBER_OF*1..3]->(p:Project) RETURN u.username, p.name LIMIT 25",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("UNION ALL"));
    assert!(rendered.contains("hop_e0_type"));
    assert!(rendered.contains("depth"));
}

#[test]
fn multi_hop_with_floor_filter() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p", "hops": [2, 3]}],
        "limit": 10
    }"#;

    let result = compile_both(
        json,
        "MATCH (u:User {id: 1})-[:MEMBER_OF*2..3]->(p:Project) RETURN u.username, p.name LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("depth"));
}

#[test]
fn single_hop_does_not_generate_recursive_cte() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "n", "entity": "Note", "columns": ["confidential"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n", "hops": [1, 1]}],
        "limit": 25
    }"#;

    let result = compile_both(
        json,
        "MATCH (u:User {id: 1})-[:AUTHORED*1..1]->(n:Note) RETURN u.username, n.confidential LIMIT 25",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(
        !rendered.contains("WITH RECURSIVE"),
        "single hop should not generate recursive CTE"
    );
}

#[test]
fn multi_hop_aggregation() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p", "hops": [1, 2]}],
        "group_by": ["u"],
        "aggregations": [{"count": "p", "as": "project_count"}],
        "limit": 10
    }"#;

    let result = compile_both(
        json,
        "MATCH (u:User {id: 1})-[:MEMBER_OF*1..2]->(p:Project)
         RETURN u, u.username, count(p) AS project_count
         LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("UNION ALL"));
    assert!(rendered.contains("e0"));
    assert!(rendered.contains("COUNT()") || rendered.contains("countIf"));
}

#[test]
fn definition_uses_project_id_for_redaction() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "d", "entity": "Definition", "node_ids": [1], "columns": ["name", "project_id"]}],
        "limit": 10
    }"#;

    let result = compile_both(
        json,
        "MATCH (d:Definition {id: 1}) RETURN d.name, d.project_id LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_d_id"));
    assert!(rendered.contains("_gkg_d_type"));
    assert!(
        rendered.contains("d.project_id") && rendered.contains("_gkg_d_id"),
        "Definition should use project_id for redaction"
    );
}

#[test]
fn project_still_uses_id_for_redaction() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]}],
        "limit": 10
    }"#;

    let result = compile_both(
        json,
        "MATCH (p:Project {id: 1}) RETURN p.name LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_p_id"));
    assert!(
        rendered.contains("p.id AS _gkg_p_id"),
        "Project should use id for redaction"
    );
}

#[test]
fn cursor_pagination_validation() {
    use compiler::passes::cursor::{canonical_hash, encode};
    use compiler::{CompiledQueryContext, compile_from_input};

    let ontology = embedded_ontology();
    let ctx = test_ctx();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
        "cursor": {"page_size": 20}
    }"#;
    let result = compile(json, &ontology, &ctx);
    assert!(result.is_ok(), "valid cursor should compile: {result:?}");
    let first_page = result.unwrap();
    let rendered = first_page.base.render();
    assert!(
        rendered.contains("LIMIT 21"),
        "cursor fetches page_size + 1 probe row: {rendered}"
    );
    assert!(
        rendered.contains("_gkg_cursor_0"),
        "cursor queries select hidden key readback columns: {rendered}"
    );

    let hash = canonical_hash(&serde_json::from_str(json).unwrap());
    let paged: serde_json::Value = {
        let mut v: serde_json::Value = serde_json::from_str(json).unwrap();
        v["cursor"]["after"] = encode(hash, &[Some("7".into())]).into();
        v
    };
    let result = compile(&paged.to_string(), &ontology, &ctx);
    assert!(
        result.is_ok(),
        "after token from same query should compile: {result:?}"
    );
    let second_page = result.unwrap();
    let rendered = second_page.base.render();
    assert!(
        rendered.contains("u.id >"),
        "after token should lower to a seek predicate: {rendered}"
    );

    let mut foreign: serde_json::Value = serde_json::from_str(json).unwrap();
    foreign["cursor"]["after"] = encode(hash ^ 1, &[Some("7".into())]).into();
    let err = compile(&foreign.to_string(), &ontology, &ctx).unwrap_err();
    assert!(
        matches!(err, QueryError::PaginationError(_)),
        "token minted for a different query should be a pagination error, got: {err}"
    );

    let mut garbled: serde_json::Value = serde_json::from_str(json).unwrap();
    garbled["cursor"]["after"] = "not-base64!".into();
    let err = compile(&garbled.to_string(), &ontology, &ctx).unwrap_err();
    assert!(
        matches!(err, QueryError::PaginationError(_)),
        "malformed token should be a pagination error, got: {err}"
    );

    // The openCypher frontend takes the cursor beside the statement and binds
    // `after` to its own query hash; the compiled pages match the JSON ones.
    let statement = "MATCH (u:User {id: 1}) RETURN u.username";
    let cypher = |page_size: u32,
                  after: Option<String>|
     -> compiler::Result<CompiledQueryContext> {
        let mut input = opencypher::lower(statement, &opencypher::Parameters::new(), &ontology)?;
        opencypher::attach_cursor(&mut input, page_size, after)?;
        compile_from_input(input, &ontology, &ctx)
    };
    let cypher_first = cypher(20, None).unwrap();
    assert_eq!(cypher_first.base, first_page.base);
    let cypher_hash = cypher_first.input.compiler.query_hash;
    let cypher_second = cypher(20, Some(encode(cypher_hash, &[Some("7".into())]))).unwrap();
    assert_eq!(cypher_second.base, second_page.base);
    let err = cypher(20, Some(encode(hash, &[Some("7".into())]))).unwrap_err();
    assert!(
        matches!(err, QueryError::PaginationError(_)),
        "a JSON-issued token must not transfer to the openCypher frontend, got: {err}"
    );
    let err = cypher(20, Some("not-base64!".into())).unwrap_err();
    assert!(matches!(err, QueryError::PaginationError(_)), "{err}");
    for page_size in [0, 1001] {
        let err = cypher(page_size, None).unwrap_err();
        assert!(
            matches!(err, QueryError::Validation(_)),
            "page_size {page_size}: {err}"
        );
    }

    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
        "cursor": {"offset": 0, "page_size": 10}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "offset cursors are gone in schema v3");

    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
        "cursor": {}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "cursor missing page_size should fail");

    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
        "cursor": {"page_size": 0}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "page_size = 0 should fail");

    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
        "cursor": {"page_size": 1001}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "page_size above 1000 should fail");

    let result = compile_both(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}]
    }"#,
        "MATCH (u:User {id: 1}) RETURN u",
        &ontology,
        &ctx,
    );
    let rendered = result.base.render();
    assert!(
        rendered.contains("LIMIT 31"),
        "default limit 30 fetches one probe row: {rendered}"
    );
    assert!(
        !result.base.sql.contains("use_query_cache"),
        "queries no longer force the query cache: {}",
        result.base.sql
    );
}

const OPENED_MR_AUTHOR_JSON: &str = r#"{
    "query_type": "traversal",
    "nodes": [
        {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}},
        {"id": "u", "entity": "User"}
    ],
    "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
    "limit": 10
}"#;

const OPENED_MR_AUTHOR_STATEMENT: &str =
    "MATCH (mr:MergeRequest {state: 'opened'})<-[:AUTHORED]-(u:User) RETURN mr LIMIT 10";

#[test]
fn render_traversal_inlines_all_params() {
    let rendered = compile_both(
        OPENED_MR_AUTHOR_JSON,
        OPENED_MR_AUTHOR_STATEMENT,
        &embedded_ontology(),
        &test_ctx(),
    )
    .base
    .render();

    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );
    assert!(
        rendered.contains("'opened'") || rendered.contains("'state:opened'"),
        "rendered SQL should contain the state filter value"
    );
    assert!(rendered.contains("'AUTHORED'"));
}

#[test]
fn render_in_filter_inlines_array() {
    let rendered = compile_both(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "filters": {
            "user_type": {"in": ["project_bot", "service_account"]}
        }}],
        "limit": 10
    }"#,
        "MATCH (u:User) WHERE u.user_type IN ['project_bot', 'service_account'] RETURN u LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    )
    .base
    .render();

    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );
    assert!(rendered.contains("'project_bot'") && rendered.contains("'service_account'"));
}

#[test]
fn render_node_ids_inlines_array() {
    let rendered = compile_both(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [100, 200, 300]}],
        "limit": 10
    }"#,
        "MATCH (u:User) WHERE u.id IN [100, 200, 300] RETURN u LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    )
    .base
    .render();

    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );
    assert!(rendered.contains("100") && rendered.contains("200") && rendered.contains("300"));
}

#[test]
fn debug_json_round_trip() {
    let compiled = compile_both(
        OPENED_MR_AUTHOR_JSON,
        OPENED_MR_AUTHOR_STATEMENT,
        &embedded_ontology(),
        &test_ctx(),
    );

    let rendered = compiled.base.render();
    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );

    let debug_json = serde_json::json!({
        "base": compiled.base.sql,
        "base_rendered": rendered,
        "hydration": serde_json::json!([]),
    });
    let parsed: serde_json::Value = serde_json::from_str(&debug_json.to_string()).unwrap();
    assert!(
        parsed["base"].as_str().unwrap().contains("{p"),
        "base should have placeholders"
    );
    assert!(
        !parsed["base_rendered"].as_str().unwrap().contains("{p"),
        "rendered should not"
    );
    assert!(parsed["hydration"].is_array());
}

#[test]
fn hydration_query_type_generates_union_all() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![
            InputNode {
                id: "hydrate".into(),
                entity: Some("Note".into()),
                table: Some("gl_note".into()),
                columns: Some(ColumnSelection::List(vec![
                    "id".into(),
                    "noteable_type".into(),
                ])),
                node_ids: vec![1, 2, 3],
                ..InputNode::default()
            },
            InputNode {
                id: "hydrate".into(),
                entity: Some("Project".into()),
                table: Some("gl_project".into()),
                columns: Some(ColumnSelection::List(vec!["id".into(), "name".into()])),
                node_ids: vec![10, 20],
                ..InputNode::default()
            },
        ],
        limit: 10,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    // Hydration SQL uses ClickHouse array literals (`IN [1,2,3]`) which
    // sqlparser doesn't support yet, so we check the raw SQL string.
    let raw = &result.base.render();

    assert!(raw.contains("UNION ALL"));
    assert!(raw.contains("toJSONString"));
    assert!(raw.contains("gl_note"));
    assert!(raw.contains("gl_project"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn hydration_widens_paths_to_segment_budget() {
    let deep = |count: usize| -> Vec<TraversalPath> {
        (0..count)
            .map(|i| TraversalPath::new_unchecked(format!("1/{i:0>40}/{:0>40}/", i + 10000)))
            .collect()
    };
    let node = |table: &str, entity: &str, paths: Vec<TraversalPath>| InputNode {
        id: "hydrate".into(),
        entity: Some(entity.into()),
        table: Some(table.into()),
        columns: Some(ColumnSelection::List(vec!["id".into()])),
        node_ids: vec![1],
        traversal_paths: paths,
        ..InputNode::default()
    };
    let compile_hydration = |nodes: Vec<InputNode>, budget: Option<usize>| {
        let input = Input {
            query_type: QueryType::Hydration,
            nodes,
            limit: 10,
            hydration_dynamic: true,
            path_segment_budget: budget,
            ..Input::default()
        };
        compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap()
    };
    let bound_paths = |result: &compiler::CompiledQueryContext| -> Vec<TraversalPath> {
        result
            .base
            .params
            .values()
            .filter_map(|p| match &p.value {
                serde_json::Value::Array(items) => Some(items),
                _ => None,
            })
            .flat_map(|items| items.iter().filter_map(|v| v.as_str()))
            .map(TraversalPath::new_unchecked)
            .collect()
    };

    let exact = deep(500);
    let result = compile_hydration(
        vec![
            node("gl_note", "Note", exact.clone()),
            node("gl_project", "Project", exact.clone()),
        ],
        Some(2000),
    );
    let array_params = result
        .base
        .params
        .values()
        .filter(|p| matches!(p.value, serde_json::Value::Array(_)))
        .count();
    assert_eq!(array_params, 1, "arms share one path array param");
    let mut kept = bound_paths(&result);
    kept.sort_unstable();
    let mut expected = exact;
    expected.sort_unstable();
    assert_eq!(kept, expected, "under budget keeps exact leaf paths");

    let over = deep(900);
    let result = compile_hydration(vec![node("gl_note", "Note", over.clone())], Some(2000));
    let widened = bound_paths(&result);
    assert!(widened.iter().all(|w| !over.contains(w)));
    for path in &over {
        assert!(
            widened
                .iter()
                .any(|w| path.as_str().starts_with(w.as_str())),
            "{path} lost its ancestor prefix"
        );
    }

    let result = compile_hydration(vec![node("gl_note", "Note", over.clone())], None);
    assert_eq!(
        bound_paths(&result).len(),
        over.len(),
        "no budget, no widening"
    );
}

#[test]
fn hydration_single_entity_no_union_all() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            table: Some("gl_user".into()),
            columns: Some(ColumnSelection::List(vec!["id".into(), "username".into()])),
            node_ids: vec![42],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(!rendered.contains("UNION ALL"));
    assert!(rendered.contains("toJSONString"));
    assert!(rendered.contains("gl_user"));
}

#[test]
fn hydration_uses_parameterized_ids() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("Note".into()),
            table: Some("gl_note".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "confidential".into(),
                "created_at".into(),
            ])),
            node_ids: vec![7777, 8888, 9999],
            ..InputNode::default()
        }],
        limit: 3,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let parameterized = &result.base.sql;

    assert!(
        parameterized.contains("Array(Int64)"),
        "IDs should be parameterized"
    );
    assert!(
        !parameterized.contains("7777"),
        "literal IDs should not appear in parameterized SQL"
    );

    let rendered = result.base.render();
    assert!(rendered.contains("7777") && rendered.contains("8888") && rendered.contains("9999"));
}

#[test]
fn hydration_skips_security_context() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("Note".into()),
            table: Some("gl_note".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "confidential".into(),
            ])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(
        !rendered.contains("arrayExists"),
        "hydration should skip security filters"
    );
    assert!(
        !rendered.contains("startsWith"),
        "hydration should not have startsWith"
    );
}

#[test]
fn hydration_id_only_columns_produces_map_with_id() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            table: Some("gl_user".into()),
            columns: Some(ColumnSelection::List(vec!["id".into()])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("map(") && rendered.contains("'id'"),
        "PK should be included in map when requested"
    );
}

#[test]
fn hydration_empty_columns_produces_empty_json() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            table: Some("gl_user".into()),
            columns: Some(ColumnSelection::List(vec![])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        !rendered.contains("map("),
        "empty props should use literal '{{}}', not map()"
    );
}

#[test]
fn hydration_id_column_included_in_map() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            table: Some("gl_user".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "username".into(),
                "state".into(),
            ])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("'username'") && rendered.contains("'state'"));
    let map_section = rendered
        .split("map(")
        .nth(1)
        .and_then(|s| s.split(')').next())
        .unwrap_or("");
    assert!(
        map_section.contains("'id'"),
        "map should contain 'id' key when requested"
    );
}

/// Both frontends reject with the same compiler message.
fn reject_both_with(json: &str, statement: &str, ctx: &compiler::SecurityContext, expected: &str) {
    let (json_err, cypher_err) = reject_both(json, statement, &embedded_ontology(), ctx);
    for err in [json_err, cypher_err] {
        assert!(
            err.to_string().contains(expected),
            "expected {expected:?}, got: {err}"
        );
    }
}

#[test]
fn like_rejects_short_contains_pattern() {
    reject_both_with(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"username": {"contains": "ab"}}}],
            "limit": 10
        }"#,
        "MATCH (u:User) WHERE u.username CONTAINS 'ab' RETURN u LIMIT 10",
        &test_ctx(),
        "search pattern must be at least 3",
    );
}

#[test]
fn like_rejects_single_char_starts_with() {
    reject_both_with(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"username": {"starts_with": "a"}}}],
            "limit": 10
        }"#,
        "MATCH (u:User) WHERE u.username STARTS WITH 'a' RETURN u LIMIT 10",
        &test_ctx(),
        "search pattern must be at least 3",
    );
}

#[test]
fn like_rejects_empty_ends_with() {
    reject_both_with(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"username": {"ends_with": ""}}}],
            "limit": 10
        }"#,
        "MATCH (u:User) WHERE u.username ENDS WITH '' RETURN u LIMIT 10",
        &test_ctx(),
        "search pattern must be at least 3",
    );
}

#[test]
fn like_rejects_contains_on_email() {
    reject_both_with(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"email": {"contains": "example"}}}],
            "limit": 10
        }"#,
        "MATCH (u:User) WHERE u.email CONTAINS 'example' RETURN u LIMIT 10",
        &test_ctx(),
        "LIKE operators",
    );
}

#[test]
fn like_rejects_starts_with_on_email() {
    reject_both_with(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"email": {"starts_with": "alice"}}}],
            "limit": 10
        }"#,
        "MATCH (u:User) WHERE u.email STARTS WITH 'alice' RETURN u LIMIT 10",
        &test_ctx(),
        "LIKE operators",
    );
}

#[test]
fn like_equality_on_email_compiles_for_admin() {
    // `like_allowed: false` blocks LIKE operators but not equality. Admin context
    // is used because User.email is also gated by `admin_only`, which the
    // RestrictPass enforces ahead of like_allowed.
    compile_both(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"email": "alice@example.com"}}],
            "limit": 10
        }"#,
        "MATCH (u:User {email: 'alice@example.com'}) RETURN u LIMIT 10",
        &embedded_ontology(),
        &admin_ctx(),
    );
}

#[test]
fn equality_on_email_rejected_for_non_admin() {
    let (json_err, cypher_err) = reject_both(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"email": "alice@example.com"}}],
            "limit": 10
        }"#,
        "MATCH (u:User {email: 'alice@example.com'}) RETURN u LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
    for err in [json_err, cypher_err] {
        let msg = err.to_string();
        assert!(
            msg.contains("email") && msg.contains("administrator"),
            "expected admin-only rejection on User.email, got: {msg}"
        );
    }
}

#[test]
fn filterable_allows_traversal_path_starts_with_inside_scope() {
    compile_both(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "g", "entity": "Group",
                     "filters": {"traversal_path": {"starts_with": "1/100/"}}}],
            "limit": 10
        }"#,
        "MATCH (g:Group) WHERE g.traversal_path STARTS WITH '1/100/' RETURN g LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
}

#[test]
fn filterable_allows_traversal_path_root_starts_with_inside_scope() {
    compile_both(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "g", "entity": "Group",
                     "filters": {"traversal_path": {"starts_with": "1/"}}}],
            "limit": 10
        }"#,
        "MATCH (g:Group) WHERE g.traversal_path STARTS WITH '1/' RETURN g LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
}

#[test]
fn filterable_allows_traversal_path_equality_inside_scope() {
    compile_both(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "p", "entity": "Project",
                     "filters": {"traversal_path": "1/100/1000/"}}],
            "limit": 10
        }"#,
        "MATCH (p:Project {traversal_path: '1/100/1000/'}) RETURN p LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
}

#[test]
fn filterable_rejects_traversal_path_outside_scope() {
    reject_both_with(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "mr", "entity": "MergeRequest",
                     "filters": {"traversal_path": "2/"}}],
            "limit": 10
        }"#,
        "MATCH (mr:MergeRequest {traversal_path: '2/'}) RETURN mr LIMIT 10",
        &test_ctx(),
        "authorized traversal_path scope",
    );
}

#[test]
fn filterable_rejects_traversal_path_above_scope() {
    reject_both_with(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "p", "entity": "Project",
                     "filters": {"traversal_path": "1/"}}],
            "limit": 10
        }"#,
        "MATCH (p:Project {traversal_path: '1/'}) RETURN p LIMIT 10",
        &compiler::SecurityContext::new(1, vec!["1/100/".into()]).unwrap(),
        "authorized traversal_path scope",
    );
}

#[test]
fn filterable_rejects_traversal_path_without_trailing_slash() {
    reject_both_with(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "g", "entity": "Group",
                     "filters": {"traversal_path": {"starts_with": "1/100"}}}],
            "limit": 10
        }"#,
        "MATCH (g:Group) WHERE g.traversal_path STARTS WITH '1/100' RETURN g LIMIT 10",
        &test_ctx(),
        "invalid traversal_path format",
    );
}

#[test]
fn filterable_rejects_traversal_path_contains_operator() {
    reject_both_with(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "p", "entity": "Project",
                     "filters": {"traversal_path": {"contains": "100"}}}],
            "limit": 10
        }"#,
        "MATCH (p:Project) WHERE p.traversal_path CONTAINS '100' RETURN p LIMIT 10",
        &test_ctx(),
        "only eq, in, and starts_with",
    );
}

#[test]
fn filterable_rejects_traversal_path_below_entity_role_floor() {
    reject_both_with(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "v", "entity": "Vulnerability",
                     "filters": {"traversal_path": "1/100/1000/"}}],
            "limit": 10
        }"#,
        "MATCH (v:Vulnerability {traversal_path: '1/100/1000/'}) RETURN v LIMIT 10",
        &compiler::SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/100/", 20)])
            .unwrap(),
        "authorized traversal_path scope",
    );
}

#[test]
fn filterable_allows_traversal_path_in_columns() {
    compile_both(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "g", "entity": "Group",
                     "columns": ["name", "traversal_path"],
                     "node_ids": [100]}],
            "limit": 10
        }"#,
        "MATCH (g:Group {id: 100}) RETURN g.name, g.traversal_path LIMIT 10",
        &embedded_ontology(),
        &test_ctx(),
    );
}

// Bug 1 regression guard: single-aggregate queries with a sort-key filter must
// keep the filter inside the FINAL scan so ClickHouse uses the primary-key index
// to skip granules. Without this, the latest-row scan reads the full authorized
// table before aggregation.
#[test]
fn aggregation_count_pushes_project_id_into_dedup_subquery() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [{"id": "d", "entity": "Definition",
                   "filters": {"project_id": {"eq": 278964}}}],
        "aggregations": [{"count": "d", "as": "total"}]
    }"#;
    let result = compile_both(
        json,
        "MATCH (d:Definition {project_id: 278964}) RETURN count(d) AS total",
        &embedded_ontology(),
        &admin_ctx(),
    );
    let rendered = result.base.render();

    assert!(
        rendered.contains("COUNT()") || rendered.contains("countIf"),
        "should contain COUNT() or countIf: {rendered}"
    );
    let inner = rendered
        .split(" FINAL")
        .nth(1)
        .expect("rendered SQL should contain FINAL");
    assert!(
        inner.contains("project_id"),
        "project_id must appear inside the FINAL scan: {rendered}"
    );
}

#[test]
fn pinned_traversal_narrows_joined_node_via_nf_cte() {
    // Bug 2: when one node has node_ids pinned and joins to another via an
    // edge, the joined-side node table must be narrowed to ids reachable
    // from the pinned source. Without the fix, the joined Definition table
    // dedups the full authorized scope (~tens of millions of rows on
    // production data) before the JOIN.
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "f", "entity": "File", "node_ids": ["12345"], "columns": ["path"]},
            {"id": "d", "entity": "Definition", "columns": ["name"]}
        ],
        "relationships": [{"type": "DEFINES", "from": "f", "to": "d"}],
        "limit": 50
    }"#;
    let result = compile_both(
        json,
        "MATCH (f:File {id: '12345'})-[:DEFINES]->(d:Definition) RETURN f.path, d.name LIMIT 50",
        &embedded_ontology(),
        &admin_ctx(),
    );
    let rendered = result.base.render();

    assert!(
        rendered.contains("gl_code_edge"),
        "DEFINES should scan gl_code_edge: {rendered}"
    );
    assert!(
        rendered.contains("12345"),
        "pinned File node_id must appear in WHERE clause: {rendered}"
    );
    assert!(
        rendered.contains("e0.source_id"),
        "edge-centric filter must reference source_id: {rendered}"
    );
}

#[test]
fn calls_traversal_compiles_against_embedded_ontology() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "caller", "entity": "Definition", "node_ids": [1], "columns": ["name"]},
            {"id": "callee", "entity": "Definition", "columns": ["name"]}
        ],
        "relationships": [{"type": "CALLS", "from": "caller", "to": "callee"}],
        "limit": 25
    }"#;

    let result = compile_both(
        json,
        "MATCH (caller:Definition {id: 1})-[:CALLS]->(callee:Definition) RETURN caller.name, callee.name LIMIT 25",
        &embedded_ontology(),
        &admin_ctx(),
    );
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_code_edge"),
        "CALLS should scan gl_code_edge: {rendered}"
    );
    assert!(
        rendered.contains("'CALLS'"),
        "CALLS relationship_kind should appear in SQL: {rendered}"
    );
}

#[test]
fn aggregation_count_in_clause_pushes_project_id() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [{"id": "d", "entity": "Definition",
                   "filters": {"project_id": {"in": [69095239, 278964, 74646916]}}}],
        "aggregations": [{"count": "d", "as": "total"}]
    }"#;
    let result = compile_both(
        json,
        "MATCH (d:Definition) WHERE d.project_id IN [69095239, 278964, 74646916] RETURN count(d) AS total",
        &embedded_ontology(),
        &admin_ctx(),
    );
    let rendered = result.base.render();

    let inner = rendered
        .split(" FINAL")
        .nth(1)
        .expect("rendered SQL should contain FINAL");
    assert!(
        inner.contains("project_id"),
        "project_id IN must appear inside FINAL scan: {rendered}"
    );
}

#[test]
fn extends_traversal_compiles_against_embedded_ontology() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "child", "entity": "Definition", "node_ids": [1], "columns": ["name"]},
            {"id": "parent", "entity": "Definition", "columns": ["name"]}
        ],
        "relationships": [{"type": "EXTENDS", "from": "child", "to": "parent"}],
        "limit": 25
    }"#;

    let result = compile_both(
        json,
        "MATCH (child:Definition {id: 1})-[:EXTENDS]->(parent:Definition) RETURN child.name, parent.name LIMIT 25",
        &embedded_ontology(),
        &admin_ctx(),
    );
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_code_edge"),
        "EXTENDS should scan gl_code_edge: {rendered}"
    );
    assert!(
        rendered.contains("'EXTENDS'"),
        "EXTENDS relationship_kind should appear in SQL: {rendered}"
    );
}

#[test]
fn calls_to_imported_symbol_variant_compiles() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "caller", "entity": "Definition", "node_ids": [1], "columns": ["name"]},
            {"id": "sym", "entity": "ImportedSymbol", "columns": ["identifier_name"]}
        ],
        "relationships": [{"type": "CALLS", "from": "caller", "to": "sym"}],
        "limit": 10
    }"#;

    compile_both(
        json,
        "MATCH (caller:Definition {id: 1})-[:CALLS]->(sym:ImportedSymbol) RETURN caller.name, sym.identifier_name LIMIT 10",
        &embedded_ontology(),
        &admin_ctx(),
    );
}

#[test]
fn calls_aggregation_compiles() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "caller", "entity": "Definition", "node_ids": [1]},
            {"id": "callee", "entity": "Definition"}
        ],
        "relationships": [{"type": "CALLS", "from": "caller", "to": "callee"}],
        "group_by": ["callee"],
        "aggregations": [{"count": "caller", "as": "callers"}],
        "limit": 1
    }"#;

    compile_both(
        json,
        "MATCH (caller:Definition {id: 1})-[:CALLS]->(callee:Definition) RETURN callee, count(caller) AS callers LIMIT 1",
        &embedded_ontology(),
        &admin_ctx(),
    );
}

#[test]
fn code_graph_edge_union_routes_to_code_table() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "a", "entity": "Definition", "node_ids": [1]},
            {"id": "b", "entity": "Definition"}
        ],
        "relationships": [
            {"type": ["CALLS", "EXTENDS", "DEFINES"], "from": "a", "to": "b"}
        ],
        "limit": 25
    }"#;

    let result = compile_both(
        json,
        "MATCH (a:Definition {id: 1})-[:CALLS|EXTENDS|DEFINES]->(b:Definition) RETURN a LIMIT 25",
        &embedded_ontology(),
        &admin_ctx(),
    );
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_code_edge"),
        "code-graph edges should scan gl_code_edge: {rendered}"
    );
    // Match `gl_edge` only when it is a standalone identifier so the assertion
    // does not get fooled by `gl_code_edge` (which contains the substring
    // `_edge`) or future suffixed table names. `gl_edge` followed by an
    // alphanumeric or underscore is a different identifier and must not flag.
    let mentions_sdlc_edge = rendered.match_indices("gl_edge").any(|(idx, _)| {
        let after = rendered.as_bytes().get(idx + "gl_edge".len()).copied();
        let before = idx
            .checked_sub(1)
            .and_then(|i| rendered.as_bytes().get(i).copied());
        let next_is_ident = matches!(after, Some(b) if b.is_ascii_alphanumeric() || b == b'_');
        let prev_is_ident = matches!(before, Some(b) if b.is_ascii_alphanumeric() || b == b'_');
        !next_is_ident && !prev_is_ident
    });
    assert!(
        !mentions_sdlc_edge,
        "code-graph edges should not touch SDLC gl_edge: {rendered}"
    );
}
