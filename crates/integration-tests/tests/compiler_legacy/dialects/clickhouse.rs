use crate::compiler_legacy::setup::{
    admin_ctx, compile_pair, embedded_ontology, test_ctx, test_ontology,
};
use crate::compiler_legacy::utils::has_param_value;
use compiler::gql::{PreparedStatement, prepare};
use compiler::input::DynamicColumnMode;
use compiler::{Frontend, QueryError, compile};
use ontology::introspection::{
    IntrospectionScope::{All, Local},
    build_compact_schema_response,
};

#[test]
fn compile_to_ast_works() {
    let orbit_query = "MATCH (u:User {id: 1}) RETURN u.username LIMIT 10";
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
        "limit": 10
    }"#;

    let compiled = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    assert!(compiled.base.sql.contains("LIMIT 11"));
    assert!(compiled.base.sql.contains("SELECT"));
}

#[test]
fn bool_filter_value_is_preserved() {
    let orbit_query = "MATCH (n:Note {confidential: true}) RETURN n.confidential LIMIT 5";
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{
            "id": "n",
            "entity": "Note",
            "columns": ["confidential"],
            "filters": { "confidential": true }
        }],
        "limit": 5
    }"#;

    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    assert!(has_param_value(
        &result.base.params,
        &serde_json::Value::Bool(true)
    ));
}

#[test]
fn group_by_truncate_minute_without_selectivity_rejected() {
    let orbit_query =
        "MATCH (u:Note) RETURN date_trunc('minute', u.created_at), count(u) AS n LIMIT 10";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note"}
        ],
        "aggregations": [{"count": "u", "as": "n"}],
        "group_by": [{"key": "u.created_at", "truncate": "minute"}],
        "limit": 10
    }"#;
    let err = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("requires either node_ids") && msg.contains("minute"),
        "expected cardinality-guard rejection; got: {msg}"
    );
}

#[test]
fn group_by_truncate_on_non_date_property_rejected() {
    let orbit_query =
        "MATCH (u:Note {id: 1}) RETURN date_trunc('month', u.confidential), count(u) AS n LIMIT 10";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "node_ids": [1]}
        ],
        "aggregations": [{"count": "u", "as": "n"}],
        "group_by": [{"key": "u.confidential", "truncate": "month"}],
        "limit": 10
    }"#;
    let err = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("requires a Date or DateTime property"),
        "expected data-type rejection; got: {msg}"
    );
}

#[test]
fn filter_operators() {
    let orbit_query = "MATCH (u:User) WHERE u.created_at >= '2024-01-01' AND u.state IN ['active', 'blocked'] AND u.username CONTAINS 'admin' RETURN u.username, u.state, u.created_at LIMIT 30";
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{
            "id": "u",
            "entity": "User",
            "columns": ["username", "state", "created_at"],
            "filters": {
                "created_at": {"gte": "2024-01-01"},
                "state": {"in": ["active", "blocked"]},
                "username": {"contains": "admin"}
            }
        }],
        "limit": 30
    }"#;

    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("LIMIT 1 BY"));
    assert!(rendered.contains("_deleted"));
    assert!(rendered.contains(">="));
    assert!(rendered.contains("IN"));
    assert!(rendered.contains("multiSearchAny(lower("));
}

#[test]
fn invalid_json_rejected() {
    assert!(
        compile(
            "not valid json",
            Frontend::JsonDsl,
            &test_ontology(),
            &test_ctx()
        )
        .is_err()
    );
}

#[test]
fn missing_required_fields_rejected() {
    assert!(
        compile(
            r#"{"query_type": "traversal"}"#,
            Frontend::JsonDsl,
            &test_ontology(),
            &test_ctx()
        )
        .is_err()
    );
}

#[test]
fn sql_injection_in_node_id() {
    let err = compile(
        r#"{"query_type": "traversal", "nodes": [{"id": "n; DROP TABLE users; --"}]}"#,
        Frontend::JsonDsl,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn sql_injection_in_relationship() {
    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "a"}, {"id": "b"}],
        "relationships": [{"type": "REL", "from": "a' OR '1'='1", "to": "b"}]
    }"#,
        Frontend::JsonDsl,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn empty_node_id_rejected() {
    assert!(
        compile(
            r#"{"query_type": "traversal", "nodes": [{"id": ""}]}"#,
            Frontend::JsonDsl,
            &test_ontology(),
            &test_ctx(),
        )
        .is_err()
    );
}

#[test]
fn id_starting_with_number_rejected() {
    let err = compile(
        r#"{"query_type": "traversal", "nodes": [{"id": "123abc"}]}"#,
        Frontend::JsonDsl,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn sql_injection_in_filter_property() {
    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "filters": {"foo; DROP TABLE--": "value"}}]
    }"#,
        Frontend::JsonDsl,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn valid_identifiers_produce_renderable_sql() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "user_node", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "_private", "entity": "Note", "columns": ["confidential"]},
            {"id": "CamelCase", "entity": "Project", "node_ids": [1], "columns": ["name"]},
            {"id": "node123", "entity": "Group", "columns": ["name"]}
        ],
        "relationships": [
            {"type": "AUTHORED", "from": "user_node", "to": "_private"},
            {"type": "CONTAINS", "from": "CamelCase", "to": "_private"},
            {"type": "MEMBER_OF", "from": "user_node", "to": "node123"}
        ]
    }"#;
    let result = compile(json, Frontend::JsonDsl, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(!rendered.contains("{p"));
    assert!(rendered.contains("_orbit_user_node_id"));
    assert!(rendered.contains("_orbit__private_id"));
    assert!(rendered.contains("_orbit_CamelCase_id"));
    assert!(rendered.contains("_orbit_node123_id"));
}

#[test]
fn cross_namespace_related_to_edge_scans_only_the_source_namespace() {
    let orbit_query = "MATCH (p:Project)<-[:IN_PROJECT]-(wi:WorkItem)-[:RELATED_TO]->(rel:WorkItem) WHERE p.id = 1 RETURN p, wi.id, rel.id, rel.title LIMIT 100";
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "p", "entity": "Project", "filters": {"id": {"eq": 1}}},
            {"id": "wi", "entity": "WorkItem", "columns": ["id"]},
            {"id": "rel", "entity": "WorkItem", "columns": ["id", "title"]}
        ],
        "relationships": [
            {"type": "IN_PROJECT", "from": "wi", "to": "p"},
            {"type": "RELATED_TO", "from": "wi", "to": "rel"}
        ],
        "limit": 100
    }"#;
    let ontology = embedded_ontology();
    let compiled = compile_pair(json, orbit_query, &ontology, &admin_ctx()).unwrap();
    let sql = compiled.base.render();

    let related_scan = sql.split("FROM gl_edge AS _orbit_edge_1").nth(1).unwrap();
    let related_scope = related_scan.split(" ON ").next().unwrap();
    assert!(
        related_scope.contains("FROM gl_project AS _orbit_scope WHERE")
            && !sql.contains("startsWith(rel.traversal_path, (SELECT"),
        "the RELATED_TO rows carry the source's path, the target may live anywhere: {sql}"
    );

    let compiler::HydrationPlan::Static(templates) = &compiled.hydration else {
        panic!("expected static hydration");
    };
    let rel = templates.iter().find(|t| t.node_alias == "rel").unwrap();
    assert!(rel.injected_columns.is_empty());
    assert_eq!(rel.destination_table, "gl_work_item");
}

#[test]
fn orbit_query_id_list_and_range_promote_independently() {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1,2,3,5],"id_range":{"start":1,"end":3},"filters":{"state":"active"}}]}"#;
    let ontology = test_ontology();
    let context = test_ctx();
    for list in ["u.id IN [1, 2, 3, 5]", "u.id IN ['1', '2', '3', '5']"] {
        for bounds in [["u.id >= 1", "u.id <= 3"], ["u.id > 0", "u.id < 4"]] {
            let predicates = [list, bounds[0], bounds[1]];
            for order in [
                [0, 1, 2],
                [0, 2, 1],
                [1, 0, 2],
                [1, 2, 0],
                [2, 0, 1],
                [2, 1, 0],
            ] {
                let conditions = order.map(|i| predicates[i]).join(" AND ");
                let query =
                    format!("MATCH (u:User) WHERE {conditions} AND u.state = 'active' RETURN u");
                let input = compiler::passes::frontend::gql::parse(&query).unwrap();
                let node = &input.nodes[0];
                assert_eq!(node.node_ids, [1, 2, 3, 5], "{query}");
                let range = node.id_range.as_ref().unwrap();
                assert_eq!((range.start, range.end), (1, 3), "{query}");
                assert!(!node.filters.contains_key("id"), "{query}");
                compile_pair(json, &query, &ontology, &context).unwrap();
            }
        }
    }
}

#[test]
fn orbit_query_m23_literals_and_comments() {
    let cases = [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[16]}]}"#,
            "/* MATCH nothing */ match(u:User {id: 0x10}) return u; // done",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[8]}]}"#,
            "MATCH (u:User {id: 0o10}) RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[-9223372036854775808]}]}"#,
            "MATCH (u:User {id: -9223372036854775808}) RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"username":"a'b\\c\n😀"}}]}"#,
            r#"MATCH (`u`:User {username: 'a\'b\\c\n\uD83D\uDE00'}) RETURN u"#,
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"created_at":{"gte":"2024-01-01"}}}]}"#,
            "MATCH (u:User) WHERE u.created_at >= date('2024-01-01') RETURN u",
        ),
    ];
    for (json, orbit_query) in cases {
        compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    }
}

#[test]
fn orbit_query_rejects_inline_filters_on_variable_length_relationships() {
    for query in [
        "MATCH (a:Group {id: 1})-[:CONTAINS*2 {source_id: 1}]->(b:Group) RETURN a, b",
        "MATCH (a:Group {id: 1})-[:CONTAINS*1..2 {source_id: 1}]->(b:Group) RETURN a, b",
        "MATCH (a:Group {id: 1})<-[r:CONTAINS*2..3 {target_id: 1}]-(b:Group) RETURN a, b",
    ] {
        let error = compiler::passes::frontend::gql::parse(query).expect_err(query);
        assert!(
            error
                .to_string()
                .contains("property filters on variable-length relationships are unsupported"),
            "{query}: {error}"
        );
    }
}

#[test]
fn orbit_query_rejects_aggregation_over_shortest_paths() {
    let cases = [
        (
            r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User","id_range":{"start":1,"end":10000}},{"id":"p","entity":"Project"}],"path":{"type":"shortest","from":"u","to":"p","max_depth":3,"rel_types":["*"]},"group_by":["p"],"aggregations":[{"count":"u","as":"hit"}],"limit":10}"#,
            "MATCH path = ANY SHORTEST (u:User)-[*1..3]->(p:Project) WHERE u.id >= 1 AND u.id <= 10000 RETURN p, count(u) AS hit LIMIT 10",
        ),
        (
            r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User","node_ids":[1],"id_range":{"start":2,"end":2}},{"id":"p","entity":"Project","node_ids":[2],"columns":["id"]}],"path":{"type":"shortest","from":"u","to":"p","max_depth":3,"rel_types":["*"]},"group_by":["p"],"aggregations":[{"count":"u","as":"hit"}],"limit":1}"#,
            "MATCH path = ANY SHORTEST (u:User {id: 1})-[*1..3]->(p:Project {id: 2}) WHERE u.id >= 2 AND u.id <= 2 RETURN p{.id}, count(u) AS hit LIMIT 1",
        ),
    ];
    let ontology = embedded_ontology();
    let context = test_ctx();
    for (json, gql) in cases {
        for (source, frontend) in [(json, Frontend::JsonDsl), (gql, Frontend::Gql)] {
            let error = compile(source, frontend, &ontology, &context).expect_err(source);
            assert!(
                matches!(&error, QueryError::Validation(message)
                    if message == "aggregation over shortest paths is not supported"),
                "{source}: {error}"
            );
        }
    }
    for query in [
        "MATCH path = ANY SHORTEST (u:User {id: 1})-[*1..3]->(p:Project) RETURN path, count(u)",
        "MATCH path = ANY SHORTEST (u:User {id: 1})-[*1..3]->(p:Project) RETURN properties(path), count(u)",
    ] {
        let error = compiler::passes::frontend::gql::parse(query).expect_err(query);
        assert!(
            error
                .to_string()
                .contains("aggregation cannot return the path variable"),
            "{query}: {error}"
        );
    }
}

#[test]
fn orbit_query_rejects_neighbors_center_all_properties() {
    for query in [
        "MATCH (center:User {id: 1})--(n) RETURN properties(center), n",
        "MATCH (center:File {id: 1})-->(n) RETURN properties(center)",
        "MATCH (center:User {id: 1})<--(n) RETURN n, properties(center)",
    ] {
        let error = compiler::passes::frontend::gql::parse(query).expect_err(query);
        assert!(
            error
                .to_string()
                .contains("dynamic graph results cannot be renamed or projected as properties"),
            "{query}: {error}"
        );
    }
    let json = r#"{"query_type":"neighbors","nodes":[{"id":"center","entity":"User","node_ids":[1]}],"neighbors":{"direction":"both"}}"#;
    let query = "MATCH (center:User {id: 1})--(n) RETURN center, n";
    compile_pair(json, query, &embedded_ontology(), &test_ctx()).unwrap();
    let json = r#"{"query_type":"neighbors","nodes":[{"id":"center","entity":"User","node_ids":[1],"columns":["username","state"]}],"neighbors":{"direction":"both"}}"#;
    let query = "MATCH (center:User {id: 1})--(n) RETURN center.username, center.state, n";
    compile_pair(json, query, &embedded_ontology(), &test_ctx()).unwrap();
}

#[test]
fn orbit_query_neighbors_accepts_the_endpoint_on_either_side() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    for (direction, rel_types, endpoint_first, center_first) in [
        (
            "incoming",
            r#""rel_types":["AUTHORED"],"#,
            "MATCH (n)-[:AUTHORED]->(center:WorkItem {id: 1}) RETURN center, n",
            "MATCH (center:WorkItem {id: 1})<-[:AUTHORED]-(n) RETURN center, n",
        ),
        (
            "outgoing",
            "",
            "MATCH (n)<--(center:WorkItem {id: 1}) RETURN center, n",
            "MATCH (center:WorkItem {id: 1})-->(n) RETURN center, n",
        ),
        (
            "both",
            "",
            "MATCH (n)--(center:WorkItem {id: 1}) RETURN center, n",
            "MATCH (center:WorkItem {id: 1})--(n) RETURN center, n",
        ),
    ] {
        let json = format!(
            r#"{{"query_type":"neighbors","nodes":[{{"id":"center","entity":"WorkItem","node_ids":[1]}}],"neighbors":{{{rel_types}"direction":"{direction}"}}}}"#
        );
        for query in [endpoint_first, center_first] {
            compile_pair(&json, query, &ontology, &ctx)
                .unwrap_or_else(|error| panic!("{query}: {error}"));
        }
    }
}

#[test]
fn orbit_query_rejects_more_than_one_sort_key() {
    for query in [
        "MATCH (mr:MergeRequest {project_id: 1}) RETURN mr.iid ORDER BY mr.updated_at DESC, mr.iid DESC",
        "MATCH (mr:MergeRequest {project_id: 1}) RETURN mr.iid ORDER BY mr.updated_at,mr.iid LIMIT 5",
    ] {
        let error =
            compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).expect_err(query);
        assert!(
            matches!(error, QueryError::Validation(ref message) if message.contains("ORDER BY accepts one sort key")),
            "{query}: {error}"
        );
    }
}

#[test]
fn orbit_query_virtual_filter_equality_hydration_parity() {
    let ontology = embedded_ontology();
    for (json, query) in [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","node_ids":[1],"filters":{"content":"abc"}}]}"#,
            "MATCH (f:File {id: 1}) WHERE f.content = 'abc' RETURN f",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","node_ids":[1],"filters":{"content":{"eq":"abc"}}}]}"#,
            "MATCH (f:File {id: 1, content: 'abc'}) RETURN f",
        ),
    ] {
        let compiled = compile_pair(json, query, &ontology, &test_ctx()).unwrap();
        let compiler::HydrationPlan::Static(templates) = compiled.hydration else {
            panic!("expected static hydration for {query}");
        };
        let filters = &templates
            .iter()
            .find(|t| t.node_alias == "f")
            .unwrap()
            .virtual_filters;
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].0, "content");
        assert_eq!(filters[0].1.op, Some(compiler::input::FilterOp::Eq));
        assert_eq!(filters[0].1.value, Some(serde_json::Value::from("abc")));
    }
}

#[test]
fn orbit_query_consecutive_match_clauses_compile_like_one_pattern() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    let single = compile(
        "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1}), (u:User)-[:AUTHORED]->(mr) WHERE mr.state = 'merged' AND u.username = 'a' RETURN mr.iid, u.username LIMIT 5",
        Frontend::Gql,
        &ontology,
        &ctx,
    )
    .unwrap();
    for query in [
        "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1}) MATCH (u:User)-[:AUTHORED]->(mr) WHERE mr.state = 'merged' AND u.username = 'a' RETURN mr.iid, u.username LIMIT 5",
        "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1}) WHERE mr.state = 'merged' MATCH (u:User)-[:AUTHORED]->(mr) WHERE u.username = 'a' RETURN mr.iid, u.username LIMIT 5",
    ] {
        let compiled = compile(query, Frontend::Gql, &ontology, &ctx).unwrap();
        assert_eq!(single.base.render(), compiled.base.render(), "{query}");
        assert_eq!(single.hydration, compiled.hydration, "{query}");
    }
    for (query, expected) in [
        (
            "MATCH (u:User {id: 1}) MATCH (p:Project {id: 2}) RETURN u, p",
            "all declared nodes must belong to one connected pattern",
        ),
        (
            "MATCH p = ANY SHORTEST (a:User {id: 1})-[*1..3]->(b:Project {id: 2}) MATCH (b)-[:IN_GROUP]->(g:Group) RETURN p",
            "a shortest path must be the only MATCH pattern",
        ),
        (
            "MATCH (u:User {id: 1}) OPTIONAL MATCH (u)-[:AUTHORED]->(mr:MergeRequest) RETURN u",
            "Orbit query syntax",
        ),
    ] {
        let error = compile(query, Frontend::Gql, &ontology, &ctx).expect_err(query);
        assert!(
            matches!(error, QueryError::Validation(ref message) if message.contains(expected)),
            "{query}: {error}"
        );
    }
}

#[test]
fn orbit_query_rejects_relationships_the_ontology_cannot_match() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    for (query, expected) in [
        (
            "MATCH (mr:MergeRequest {project_id: 1})-[:AUTHORED]->(u:User) RETURN mr, u",
            "AUTHORED goes from User to MergeRequest; reverse the arrow: (u)-[:AUTHORED]->(mr)",
        ),
        (
            "MATCH (u:User)<-[a:AUTHORED]-(mr:MergeRequest {project_id: 1}) RETURN count(mr)",
            "AUTHORED goes from User to MergeRequest; reverse the arrow: (u)-[:AUTHORED]->(mr)",
        ),
        (
            "MATCH (mr:MergeRequest {project_id: 1})-[:AUTHORED]->(p:Project) RETURN mr, p",
            "AUTHORED does not connect MergeRequest and Project in either direction",
        ),
        (
            "MATCH (mr:MergeRequest {project_id: 1})-[:AUTHORED|CLOSES]->(u:User) RETURN mr, u",
            "AUTHORED goes from User to MergeRequest; reverse the arrow: (u)-[:AUTHORED]->(mr); CLOSES does not connect MergeRequest and User",
        ),
        (
            "MATCH (mr:MergeRequest {project_id: 1})-[:AUTHORED|CLOSES]->(p:Project) RETURN mr, p",
            "AUTHORED|CLOSES does not connect MergeRequest and Project in either direction",
        ),
        (
            "MATCH (p:Project {id: 1})<-[IN_PROJECT]-(mr:MergeRequest) RETURN count(mr)",
            "[IN_PROJECT] declares a variable, not a relationship type; write [:IN_PROJECT]",
        ),
        (
            "MATCH (mr:MergeRequest {id: 1})-[IN_MILESTONE]->(m) RETURN m",
            "[IN_MILESTONE] declares a variable, not a relationship type; write [:IN_MILESTONE]",
        ),
    ] {
        let error = compile(query, Frontend::Gql, &ontology, &ctx).expect_err(query);
        assert!(
            matches!(error, QueryError::Validation(ref message) if message.contains(expected)),
            "{query}: {error}"
        );
    }
    for query in [
        "MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {project_id: 1}) RETURN mr, u",
        "MATCH (mr:MergeRequest {project_id: 1})<-[:AUTHORED]-(u:User) RETURN count(mr)",
        "MATCH (mr:MergeRequest {project_id: 1})-[:AUTHORED|IN_PROJECT]->(p:Project) RETURN mr, p",
        "MATCH (g:Group {id: 1})-[:CONTAINS*1..2]->(p:Project) RETURN p",
        "MATCH (p:Project {id: 1})<-[r]-(mr:MergeRequest) RETURN count(mr)",
        "MATCH (p:Project {id: 1})<-[R]-(mr:MergeRequest) RETURN count(mr)",
        "MATCH (mr:MergeRequest {id: 1})-[:IN_MILESTONE]->(m) RETURN m",
    ] {
        compile(query, Frontend::Gql, &ontology, &ctx)
            .unwrap_or_else(|error| panic!("{query}: {error}"));
    }
    let reversed = r#"{"query_type":"traversal","nodes":[{"id":"mr","entity":"MergeRequest","node_ids":[1]},{"id":"u","entity":"User"}],"relationships":[{"type":"AUTHORED","from":"mr","to":"u"}]}"#;
    compile(reversed, Frontend::JsonDsl, &ontology, &ctx).unwrap();
}

#[test]
fn orbit_query_rejects_unsupported_syntax_and_shapes() {
    let queries = [
        "CREATE (u:User)",
        "MATCH (u:User) DELETE u",
        "MATCH (u:User) SET u.username = 'x' RETURN u",
        "MATCH (u:User) RETURN u; MATCH (p:Project) RETURN p",
        "MATCH (u:User) RETURN u UNION MATCH (p:Project) RETURN p",
        "OPTIONAL MATCH (u:User) RETURN u",
        "MATCH (u:User) WITH u RETURN u",
        "UNWIND [1] AS u RETURN u",
        "MATCH (u:User), (p:Project) RETURN u, p",
        "MATCH () RETURN *",
        "MATCH (u:User|Project) RETURN u",
        "MATCH (u IS User) RETURN u",
        "MATCH (u:User) WHERE u.id = 1 OR u.id = 2 RETURN u",
        "MATCH (u:User) WHERE u.created_at = DATE '2024-01-01' RETURN u",
        "MATCH (u:User) RETURN DISTINCT u",
        "MATCH (u:User) RETURN count(*)",
        "MATCH (u:User {id: 1}) RETURN sum(DISTINCT u.id) AS n",
        "MATCH (u:User) RETURN collect(u.username)",
        "MATCH (u:User {id: 1}) RETURN u SKIP 1 SKIP 1",
        "MATCH (u:User {id: 1}) RETURN u SKIP -1",
        "MATCH (u:User {id: 1}) RETURN u SKIP 10001",
        "MATCH (u:User) RETURN u DEBUG DEBUG",
        "MATCH (u:User) RETURN u DEBUG LIMIT 1",
        "MATCH (u:User) RETURN u LIMIT 1 ORDER BY u.id",
        "MATCH (u:User {id: 1}) RETURN u PAGE 1",
        "MATCH (u:User {id: 1}) RETURN u LIMIT 1 AFTER 'token'",
        "MATCH (u:User) RETURN u; DEBUG",
        "MATCH (u:User) RETURN count(u) AS n AS other",
        "MATCH (u:User) RETURN u ORDER BY u.id, u.username",
        "MATCH (u:User {id: 1}) RETURN u.username AS a, u.name AS a",
        "MATCH (u:User {id: 1}) RETURN u.username ORDER BY missing",
        "MATCH (u:User) RETURN u{.username}, u.state",
        "MATCH (u:User) RETURN u.username, u{.state}",
        "MATCH (u:User) RETURN date_trunc('month', u.created_at)",
        "MATCH (u:User {id: 1})-[:MEMBER_OF*]->(p:Project) RETURN u",
        "MATCH (u:User {id: 1})-[:MEMBER_OF*1..]->(p:Project) RETURN u",
        "MATCH (u:User {id: 1})-[:MEMBER_OF*0..3]->(p:Project) RETURN u",
        "MATCH (u:User {id: 1})-[:MEMBER_OF*3..1]->(p:Project) RETURN u",
        "MATCH (u:User {id: 1})-[:MEMBER_OF*1..4]->(p:Project) RETURN u",
        "MATCH (u:User {id: 1})-->(n) WHERE n.id = 2 RETURN n",
        "MATCH (u:User {id: 1})-->(n) RETURN count(n)",
        "MATCH (u:User {id: 1})-[r:MEMBER_OF*1..2]->(p:Project) WHERE r.target_id = 2 RETURN u",
        "MATCH p = ANY SHORTEST (u:User {id: 1})<-[:MEMBER_OF*1..3]-(p:Project {id: 2}) RETURN p",
        "MATCH p = shortestPath((u:User {id: 1})-[:MEMBER_OF*1..3]->(g:Group)) RETURN p",
        "MATCH p = ALL SHORTEST (u:User {id: 1})-[:MEMBER_OF*1..3]->(g:Group) RETURN p",
        "MATCH p = SHORTEST 2 (u:User {id: 1})-[:MEMBER_OF*1..3]->(g:Group) RETURN p",
        "MATCH p = (u:User {id: 1})-[:MEMBER_OF*1..3]->(g:Group) RETURN p",
        "MATCH (u:User {id: 1})-->(u) RETURN u",
        "MATCH (u:User {id: 1, id: 2}) RETURN u",
        "MATCH (u:User) WHERE missing.username = 'abc' RETURN u",
        "MATCH (u:User) RETURN missing",
        "MATCH (u:User) WHERE u.username = $missing RETURN u",
        "MATCH (`u; DROP TABLE gl_user`:User) RETURN *",
        "MATCH (u:User {username: '\\q'}) RETURN u",
        "MATCH (u:User {username: '\\uD800'}) RETURN u",
    ];
    for query in queries {
        let error = compiler::compile(
            query,
            compiler::Frontend::Gql,
            &test_ontology(),
            &test_ctx(),
        )
        .expect_err(query);
        assert!(error.is_client_safe(), "{query}: {error}");
    }
}

#[test]
fn orbit_query_nesting_guard_sees_through_escaped_names() {
    let hidden = format!(
        "MATCH (u:User) WHERE u.`'` = 1 AND {}u.id = 1{} AND u.`'` = 2 RETURN u",
        "(".repeat(2_000),
        ")".repeat(2_000)
    );
    let error = compiler::passes::frontend::gql::parse(&hidden).unwrap_err();
    assert!(error.to_string().contains("nesting is too deep"), "{error}");

    let json = r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"limit":5}"#;
    let orbit_query = "MATCH (`u`:`User` {`id`: 1}) RETURN `u` LIMIT 5";
    compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
}

#[test]
fn orbit_query_bounds_input_before_recursive_parsing() {
    let nested = format!(
        "MATCH (u:User) WHERE {}u.id = 1{} RETURN u",
        "(".repeat(40),
        ")".repeat(40)
    );
    let oversized = format!(
        "MATCH (u:User {{username: '{}'}}) RETURN u",
        "x".repeat(40_000)
    );
    for query in [nested, oversized] {
        assert!(compiler::passes::frontend::gql::parse(&query).is_err());
    }
    let query = "MATCH (u:User) WHERE u.id IN ['invalid'] AND u.id >= 1 AND u.id <= 3 RETURN u";
    assert!(
        compiler::compile(
            query,
            compiler::Frontend::Gql,
            &test_ontology(),
            &test_ctx()
        )
        .is_err()
    );
}

#[test]
fn orbit_query_caps_reject_during_lowering() {
    let nodes: Vec<String> = (0..4_000).map(|i| format!("(a{i})")).collect();
    let properties: Vec<String> = (0..51).map(|i| format!("u.c{i}")).collect();
    for (query, first_rejected, message) in [
        (
            format!("MATCH {} RETURN a0", nodes.join(",")),
            "(a5)",
            "a query declares at most 5 nodes",
        ),
        (
            format!("MATCH (u:User) RETURN {}", properties.join(", ")),
            "u.c50",
            "a node projects at most 50 properties",
        ),
    ] {
        let column = query.find(first_rejected).unwrap() + 1;
        let expected = format!("line 1, column {column}: {message}");
        let error = compiler::passes::frontend::gql::parse(&query).unwrap_err();
        assert!(
            matches!(&error, QueryError::Validation(m) if *m == expected),
            "{error:?}"
        );
    }
}

#[test]
fn native_predicate_trees_are_bounded_before_planning() {
    use compiler::input::{BooleanExpression, Condition, FilterOp, InputFilter, PredicateTarget};
    let model = query_data_model::ClickHouseDataModel::derive(
        crate::compiler_legacy::setup::embedded_ontology(),
    )
    .unwrap();
    let validator = compiler::passes::validate::Validator::new(&model);
    let mut input =
        compiler::passes::frontend::gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
    let leaf = |target| {
        BooleanExpression::Leaf(Condition {
            target,
            property: "id".into(),
            filter: InputFilter {
                op: Some(FilterOp::In),
                value: Some(serde_json::json!([1])),
                ..Default::default()
            },
        })
    };
    let mut deep = leaf(PredicateTarget::Node("u".into()));
    for _ in 0..1024 {
        deep = BooleanExpression::Not(Box::new(deep));
    }
    input.predicates = vec![deep];
    let error = validator.check_shape(&input).unwrap_err();
    assert!(
        error.to_string().contains("must not nest deeper than 32"),
        "{error}"
    );
    input.predicates = vec![BooleanExpression::Not(Box::new(leaf(
        PredicateTarget::Relationship(7),
    )))];
    let error = validator.check_shape(&input).unwrap_err();
    assert!(
        error.to_string().contains("undefined relationship"),
        "{error}"
    );
}

#[test]
fn native_conditions_need_a_column() {
    use compiler::input::{BooleanExpression, InputFilter};
    let base = compiler::passes::frontend::gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
    let mut empty_group = base.clone();
    empty_group.predicates = vec![BooleanExpression::Not(Box::new(BooleanExpression::And(
        vec![],
    )))];
    let mut empty_property = base;
    empty_property.nodes[0].filters.insert(
        String::new(),
        vec![InputFilter {
            value: Some(serde_json::json!(1)),
            ..Default::default()
        }],
    );
    for input in [empty_group, empty_property] {
        let error = compiler::compile_input(
            input,
            Default::default(),
            &crate::compiler_legacy::setup::embedded_ontology(),
            &test_ctx(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("every condition must compare a property"),
            "{error}"
        );
    }
}

#[test]
fn orbit_query_schema_caps_reject_with_the_json_category() {
    let cases = [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"limit":0}"#,
            "MATCH (u:User {id: 1}) RETURN u LIMIT 0",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"limit":1001}"#,
            "MATCH (u:User {id: 1}) RETURN u LIMIT 1001",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]},{"id":"p","entity":"Project"}],"relationships":[{"type":"MEMBER_OF","from":"u","to":"p","hops":[1,4]}]}"#,
            "MATCH (u:User {id: 1})-[:MEMBER_OF*1..4]->(p:Project) RETURN u, p",
        ),
        (
            r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"group_by":["ghost"],"aggregations":[{"count":"u"}]}"#,
            "MATCH (u:User {id: 1}) RETURN ghost, count(u)",
        ),
    ];
    for (json, orbit_query) in cases {
        compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap_err();
    }

    let long_identifier = "a".repeat(65);
    let long_string = "x".repeat(1025);
    let bounded = [
        (
            format!(
                r#"{{"query_type":"traversal","nodes":[{{"id":"{long_identifier}","entity":"User","node_ids":[1]}}]}}"#
            ),
            format!("MATCH ({long_identifier}:User {{id: 1}}) RETURN {long_identifier}"),
        ),
        (
            format!(
                r#"{{"query_type":"traversal","nodes":[{{"id":"u","entity":"User","filters":{{"username":"{long_string}"}}}}]}}"#
            ),
            format!("MATCH (u:User {{username: '{long_string}'}}) RETURN u"),
        ),
    ];
    for (json, orbit_query) in &bounded {
        compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap_err();
    }
    compile_pair(
        r#"{"query_type":"traversal","nodes":[{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","entity":"User","node_ids":[1]}]}"#,
        "MATCH (aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:User {id: 1}) RETURN aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
}

#[test]
fn orbit_query_path_depth_caps_match_json() {
    let ontology = test_ontology();
    let ctx = test_ctx();
    for depth in [0, 1, 3, 4, u32::MAX] {
        let json = format!(
            r#"{{"query_type":"path_finding","nodes":[{{"id":"a","entity":"Project","node_ids":[1]}},{{"id":"b","entity":"Project","node_ids":[2]}}],"path":{{"type":"shortest","from":"a","to":"b","max_depth":{depth},"rel_types":["CONTAINS"]}}}}"#
        );
        let query = format!(
            "MATCH p = ANY SHORTEST (a:Project {{id: 1}})-[:CONTAINS*1..{depth}]->(b:Project {{id: 2}}) RETURN p"
        );
        let result = compile_pair(&json, &query, &ontology, &ctx);
        if (1..=3).contains(&depth) {
            result.unwrap();
        } else {
            assert!(matches!(result.unwrap_err(), QueryError::Validation(_)));
        }
    }
}

#[test]
fn orbit_query_neighbors_relationship_type_caps_match_json() {
    let ontology = test_ontology();
    let ctx = test_ctx();
    for (direction, left, right) in [
        ("outgoing", "-", "->"),
        ("incoming", "<-", "-"),
        ("both", "-", "-"),
    ] {
        for count in [1, 10, 11] {
            let kinds = vec!["AUTHORED"; count];
            let json = serde_json::json!({
                "query_type": "neighbors",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "neighbors": {"direction": direction, "rel_types": kinds},
            })
            .to_string();
            let query = format!(
                "MATCH (u:User {{id: 1}}){left}[:{}]{right}(n) RETURN n",
                kinds.join("|")
            );
            let result = compile_pair(&json, &query, &ontology, &ctx);
            if count <= 10 {
                result.unwrap();
            } else {
                assert!(matches!(result.unwrap_err(), QueryError::Validation(_)));
            }
        }
    }
}

#[test]
fn orbit_query_structural_caps_reject_with_the_json_category() {
    let chain = |count: usize| {
        let nodes: Vec<String> = (0..count)
            .map(|i| format!(r#"{{"id":"n{i}","entity":"Group"}}"#))
            .collect();
        let edges: Vec<String> = (1..count)
            .map(|i| format!(r#"{{"type":"CONTAINS","from":"n{}","to":"n{i}"}}"#, i - 1))
            .collect();
        let json = format!(
            r#"{{"query_type":"traversal","nodes":[{{"id":"n0","entity":"Group","node_ids":[1]}},{}],"relationships":[{}]}}"#,
            nodes[1..].join(","),
            edges.join(",")
        );
        let pattern: Vec<String> = (1..count).map(|i| format!("(n{i}:Group)")).collect();
        let text = format!(
            "MATCH (n0:Group {{id: 1}})-[:CONTAINS]->{} RETURN n0",
            pattern.join("-[:CONTAINS]->")
        );
        (json, text)
    };
    let ids = |count: usize| {
        let list: Vec<String> = (1..=count).map(|i| i.to_string()).collect();
        (
            format!(
                r#"{{"query_type":"traversal","nodes":[{{"id":"u","entity":"User","node_ids":[{}]}}]}}"#,
                list.join(",")
            ),
            format!(
                "MATCH (u:User) WHERE u.id IN [{}] RETURN u",
                list.join(", ")
            ),
        )
    };
    let repeated_predicates = |count: usize| {
        let json_entries: Vec<String> = (0..count)
            .map(|i| format!(r#"{{"contains":"abc{i}"}}"#))
            .collect();
        let text_entries: Vec<String> = (0..count)
            .map(|i| format!("u.username CONTAINS 'abc{i}'"))
            .collect();
        (
            format!(
                r#"{{"query_type":"traversal","nodes":[{{"id":"u","entity":"User","filters":{{"username":[{}]}}}}]}}"#,
                json_entries.join(",")
            ),
            format!(
                "MATCH (u:User) WHERE {} RETURN u",
                text_entries.join(" AND ")
            ),
        )
    };

    let rel_types = |count: usize| {
        let kinds = &[
            "MEMBER_OF",
            "AUTHORED",
            "CONTAINS",
            "IN_PROJECT",
            "REVIEWER",
            "ASSIGNED",
            "HAS_HEAD_PIPELINE",
            "ON_BRANCH",
            "DEFINES",
            "CALLS",
            "EXTENDS",
        ][..count];
        let quoted: Vec<String> = kinds.iter().map(|k| format!("\"{k}\"")).collect();
        (
            format!(
                r#"{{"query_type":"traversal","nodes":[{{"id":"a","entity":"User","node_ids":[1]}},{{"id":"b","entity":"Project"}}],"relationships":[{{"type":[{}],"from":"a","to":"b"}}]}}"#,
                quoted.join(",")
            ),
            format!(
                "MATCH (a:User {{id: 1}})-[:{}]->(b:Project) RETURN a, b",
                kinds.join("|")
            ),
        )
    };

    for (json, text) in [chain(5), ids(500), repeated_predicates(10), rel_types(10)] {
        compile_pair(&json, &text, &embedded_ontology(), &test_ctx()).unwrap();
    }
    for (json, text) in [chain(6), ids(501), repeated_predicates(11), rel_types(11)] {
        let error = compile_pair(&json, &text, &embedded_ontology(), &test_ctx()).unwrap_err();
        assert!(
            matches!(error, QueryError::Validation(_)),
            "{text}: {error}"
        );
    }
}

#[test]
fn orbit_query_traversal_aliases_are_ignored_and_sortable() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    let base = "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1})";
    for (plain, aliased) in [
        (
            "RETURN mr.iid, p ORDER BY mr.iid LIMIT 5",
            "RETURN mr.iid AS number, p AS project ORDER BY number LIMIT 5",
        ),
        (
            "RETURN mr.iid, p ORDER BY mr.iid LIMIT 5",
            "RETURN mr.iid AS number, p AS project ORDER BY mr.iid LIMIT 5",
        ),
        (
            "RETURN mr.iid, p{.full_path} ORDER BY mr.iid DESC LIMIT 5",
            "RETURN mr.iid AS number, p{.full_path} AS project ORDER BY number DESC LIMIT 5",
        ),
        (
            "RETURN mr.iid ORDER BY mr.iid SKIP 5 LIMIT 5",
            "RETURN mr.iid AS number ORDER BY number SKIP 5 LIMIT 5",
        ),
    ] {
        let plain = compile(&format!("{base} {plain}"), Frontend::Gql, &ontology, &ctx).unwrap();
        let query = format!("{base} {aliased}");
        let aliased = compile(&query, Frontend::Gql, &ontology, &ctx).unwrap();
        assert_eq!(plain.base.render(), aliased.base.render(), "{query}");
        assert_eq!(plain.hydration, aliased.hydration, "{query}");
    }

    let plain = compile(
        "MATCH (center:WorkItem {id: 1})-->(n) RETURN center, n",
        Frontend::Gql,
        &ontology,
        &ctx,
    )
    .unwrap();
    let aliased = compile(
        "MATCH (center:WorkItem {id: 1})-->(n) RETURN center AS item, n",
        Frontend::Gql,
        &ontology,
        &ctx,
    )
    .unwrap();
    assert_eq!(plain.base.render(), aliased.base.render());

    for (query, expected) in [
        (
            "RETURN mr.iid, p AS project ORDER BY project",
            "not a node alias",
        ),
        ("RETURN mr.iid AS x, p AS x", "duplicate alias"),
        ("RETURN mr.iid AS x, p.full_path AS x", "duplicate alias"),
        ("RETURN mr.iid ORDER BY missing", "or a property alias"),
    ] {
        let query = format!("{base} {query}");
        let error = compile(&query, Frontend::Gql, &ontology, &ctx).expect_err(&query);
        assert!(
            matches!(error, QueryError::Validation(ref message) if message.contains(expected)),
            "{query}: {error}"
        );
    }
}

#[test]
fn orbit_query_debug_and_dynamic_properties_match_json_options() {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"options":{"include_debug_sql":true}}"#;
    let compiled = compile_pair(
        json,
        "MATCH (u:User {id: 1}) RETURN u DEBUG",
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert!(compiled.input.options.include_debug_sql);

    let json = r#"{"query_type":"neighbors","nodes":[{"id":"g","entity":"Group","node_ids":[100]}],"neighbors":{"direction":"both"},"options":{"dynamic_columns":"*"}}"#;
    let compiled = compile_pair(
        json,
        "MATCH (g:Group {id: 100})--(n) RETURN g, properties(n)",
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert_eq!(
        compiled.input.options.dynamic_columns,
        DynamicColumnMode::All
    );

    let json = r#"{"query_type":"path_finding","nodes":[{"id":"start","entity":"Project","node_ids":[100]},{"id":"end","entity":"Project","node_ids":[200]}],"path":{"type":"shortest","from":"start","to":"end","max_depth":3,"rel_types":["CONTAINS"]},"options":{"dynamic_columns":"*"}}"#;
    let compiled = compile_pair(
        json,
        "MATCH p = ANY SHORTEST (start:Project {id: 100})-[:CONTAINS*1..3]->(`end`:Project {id: 200}) RETURN properties(p)",
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert_eq!(
        compiled.input.options.dynamic_columns,
        DynamicColumnMode::All
    );
}

#[test]
fn orbit_query_optional_clauses_preserve_options() {
    for order in ["", " ORDER BY u.id"] {
        for (limit, rows, skip) in [
            ("", None, None),
            (" LIMIT 7", Some(7), None),
            (" SKIP 7", None, Some(7)),
            (" SKIP 7 LIMIT 3", Some(3), Some(7)),
        ] {
            for debug in ["", " DEBUG"] {
                let query = format!("MATCH (u:User {{id: 1}}) RETURN u{order}{limit}{debug}");
                let compiled =
                    compile(&query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
                let input = compiled.input;
                assert_eq!(
                    input.options.include_debug_sql,
                    !debug.is_empty(),
                    "{query}"
                );
                assert_eq!(input.order_by.is_some(), !order.is_empty(), "{query}");
                assert_eq!(input.skip, skip, "{query}");
                if let Some(rows) = rows {
                    assert_eq!(input.limit, rows, "{query}");
                }
            }
        }
    }
}

#[test]
fn orbit_query_debug_in_literals_and_comments_stays_data() {
    for (literal, value) in [
        (r#"'DEBUG'"#, "DEBUG"),
        (r#"'x\' RETURN u DEBUG //'"#, "x' RETURN u DEBUG //"),
        (r#"'\u0027 RETURN u DEBUG //'"#, "' RETURN u DEBUG //"),
        (
            r#""'; DROP TABLE gl_user; --""#,
            "'; DROP TABLE gl_user; --",
        ),
    ] {
        for comment in ["", " /* DEBUG */", " // DEBUG"] {
            let query = format!("MATCH (u:User {{username: {literal}}}) RETURN u{comment}");
            let compiled = compile(&query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
            assert!(!compiled.input.options.include_debug_sql, "{query}");
            assert!(
                has_param_value(&compiled.base.params, &serde_json::json!(value)),
                "{query}"
            );
            assert!(!compiled.base.sql.contains(value), "{query}");
        }
    }
}

#[test]
fn orbit_query_reserved_skip_words_need_backticks() {
    assert!(
        compile(
            "MATCH (offset:User {id: 1}) RETURN offset",
            Frontend::Gql,
            &test_ontology(),
            &test_ctx()
        )
        .is_err()
    );
    compile(
        "MATCH (`offset`:User {id: 1}) RETURN `offset`",
        Frontend::Gql,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
}

#[test]
fn gql_prepares_queries_and_scoped_schema() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    assert!(matches!(
        prepare("MATCH (n:User {id: 1}) RETURN n", &ontology, &ctx, All),
        Ok(PreparedStatement::Query(_))
    ));
    for (scope, call, node) in [
        (All, "CALL db.schema()", None),
        (Local, "CALL db.schema()", None),
        (All, "CALL db.schema('MergeRequest')", Some("MergeRequest")),
        (Local, "CALL db.schema('File')", Some("File")),
    ] {
        let PreparedStatement::Schema(response) = prepare(call, &ontology, &ctx, scope).unwrap()
        else {
            panic!("expected schema: {call}");
        };
        if let Some(name) = node {
            assert_eq!(response.domains.len(), 1);
            assert_eq!(response.domains[0].nodes.len(), 1);
            let node = serde_json::to_value(&response.domains[0].nodes[0]).unwrap();
            assert_eq!(node["name"], name);
            assert!(!node["props"].as_array().unwrap().is_empty());
            assert!(response.edges.is_empty());
        } else {
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                serde_json::to_value(build_compact_schema_response(&ontology, scope, None))
                    .unwrap(),
            );
        }
    }
    for (scope, call) in [
        (All, "CALL db.schema('Missing')"),
        (All, "CALL db.schema('*')"),
        (All, "CALL db.schema('File', 'User')"),
        (All, "CALL db.schema() YIELD name"),
        (Local, "CALL db.schema('MergeRequest')"),
    ] {
        assert!(
            prepare(call, &ontology, &ctx, scope)
                .unwrap_err()
                .is_client_safe()
        );
    }
    assert!(compile("CALL db.schema()", Frontend::Gql, &ontology, &ctx).is_err());
}

#[test]
fn cte_statements_carry_the_ctes_they_read() {
    let compiled = compile(
        "MATCH (g:Group)-[:CONTAINS]->(p:Project)<-[:IN_PROJECT]-(mr:MergeRequest)-[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)-[:HAS_FILE]->(f:MergeRequestDiffFile) WHERE g.full_path = 'g701' RETURN p, count(f) AS c LIMIT 10",
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let statements = compiler::passes::codegen::clickhouse::cte_statements(&compiled.ast).unwrap();
    assert!(
        statements
            .iter()
            .any(|(_, sql, _)| sql.starts_with("WITH ")),
        "expected a CTE body that reads another CTE"
    );
    for (own, sql, _) in &statements {
        for (position, _) in sql.match_indices("_orbit_relation_") {
            let digits: String = sql[position + "_orbit_relation_".len()..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            let name = format!("_orbit_relation_{digits}");
            assert!(
                &name == own || sql.contains(&format!("{name} AS ")),
                "{name} is read but not defined in: {sql}"
            );
        }
    }
}
