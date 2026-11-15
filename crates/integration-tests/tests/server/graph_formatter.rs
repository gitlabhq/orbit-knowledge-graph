use std::collections::HashSet;
use std::sync::Arc;

use crate::common::compile_model;
use crate::common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, TestContext, derive_clickhouse_data_model,
    load_ontology, run_redaction, test_security_context,
};
use integration_testkit::{run_subtests_shared, t};
use orbit_server::pipeline::HydrationStage;
use orbit_server::redaction::QueryResult;
use query_engine::compiler::SecurityContext;
use query_engine::formatters::{GraphFormatter, ResultFormatter};
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap};
use query_engine::shared::{PipelineOutput, RedactionOutput};
use serde_json::Value;

static RESPONSE_SCHEMA: std::sync::LazyLock<jsonschema::Validator> =
    std::sync::LazyLock::new(|| {
        let schema: Value = serde_json::from_str(include_str!(concat!(
            env!("SCHEMA_DIR"),
            "/query_response.json"
        )))
        .unwrap();
        jsonschema::validator_for(&schema).unwrap()
    });

fn assert_valid(value: &Value) {
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(value).collect();
    assert!(errors.is_empty(), "Schema validation failed: {errors:?}");
}

fn find_node<'a>(nodes: &'a [Value], entity_type: &str, id: i64) -> &'a Value {
    let id_str = id.to_string();
    nodes
        .iter()
        .find(|n| n["type"] == entity_type && n["id"].as_str() == Some(&id_str))
        .unwrap_or_else(|| panic!("node {entity_type}:{id} not found in {nodes:?}"))
}

fn node_ids(nodes: &[Value], entity_type: &str) -> HashSet<i64> {
    nodes
        .iter()
        .filter(|n| n["type"] == entity_type)
        .filter_map(|n| n["id"].as_str().and_then(|s| s.parse().ok()))
        .collect()
}

fn aggregation_rows(value: &Value) -> &Vec<Value> {
    value["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("aggregation response has no rows: {value:?}"))
}

fn first_aggregation_row(value: &Value) -> &Value {
    aggregation_rows(value)
        .first()
        .unwrap_or_else(|| panic!("aggregation response has no rows: {value:?}"))
}

fn aggregation_nodes(value: &Value, group_key: &str) -> Vec<Value> {
    aggregation_rows(value)
        .iter()
        .filter_map(|row| {
            let group = row.get(group_key)?.as_object()?;
            let mut node = serde_json::Map::new();
            node.insert("type".to_string(), group.get("type")?.clone());
            node.insert("id".to_string(), group.get("id")?.clone());

            if let Some(properties) = group.get("properties").and_then(Value::as_object) {
                for (key, value) in properties {
                    node.insert(key.clone(), value.clone());
                }
            }

            if let Some(row_obj) = row.as_object() {
                for (key, value) in row_obj {
                    if key != group_key {
                        node.insert(key.clone(), value.clone());
                    }
                }
            }

            Some(Value::Object(node))
        })
        .collect()
}

// Topology:
//
//   User 1 (alice)   →MEMBER_OF→ Group 100 (Public)     →CONTAINS→ Project 1000
//   User 1 (alice)   →MEMBER_OF→ Group 101 (Private)    →CONTAINS→ Project 1001
//   User 2 (bob)     →MEMBER_OF→ Group 101 (Private)
//   User 3 (charlie) →MEMBER_OF→ Group 102 (Internal)   →CONTAINS→ Project 1002
//   User 4 (diana)   →MEMBER_OF→ Group 102 (Internal)
//   User 5 (unicode) →MEMBER_OF→ Group 100 (Public)
//
//   Group 100 →MEMBER_OF→ Group 200 (Depth2)           -- group hierarchy for multi-hop
//   Group 200 →MEMBER_OF→ Group 300 (Depth3)           -- 3-hop chain: User→100→200→300
//
//   User 1 →AUTHORED→ MR 2000   →HAS_NOTE→ Note 3000, 3002, 3003
//   User 2 →AUTHORED→ MR 2001   →HAS_NOTE→ Note 3001 (giant string)
//   User 3 →AUTHORED→ MR 2002
//
async fn seed(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type) VALUES
         (1, 'alice', 'Alice Admin', 'active', 'human'),
         (2, 'bob', 'Bob Builder', 'active', 'human'),
         (3, 'charlie', 'Charlie Private', 'active', 'human'),
         (4, 'diana', 'Diana Dev', 'blocked', 'project_bot'),
         (5, '用户_émoji_🎉', 'Unicode Name ñ', 'active', 'human')",
        t("gl_user")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/'),
         (102, 'Internal Group', 'internal', '1/102/'),
         (200, 'Depth2 Group', 'public', '1/200/'),
         (300, 'Depth3 Group', 'public', '1/300/')",
        t("gl_group")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/102/1002/')",
        t("gl_project")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, author_id, traversal_path) VALUES
         (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', 1, '1/100/1000/'),
         (2001, 2, 'Fix bug B', 'merged', 'fix-b', 'main', 2, '1/101/1001/'),
         (2002, 3, 'Update C', 'closed', 'update-c', 'main', 3, '1/102/1002/')",
        t("gl_merge_request")
    ))
    .await;

    let giant = "A".repeat(10_000);
    ctx.execute(&format!(
        "INSERT INTO {} (id, note, noteable_type, noteable_id, author_id, traversal_path, internal, confidential) VALUES
         (3000, 'Normal note', 'MergeRequest', 2000, 1, '1/100/1000/', true, true),
         (3001, '{giant}', 'MergeRequest', 2001, 2, '1/101/1001/', false, NULL),
         (3002, 'SQL injection attempt: \\'; DROP TABLE gl_user; --', 'MergeRequest', 2000, 1, '1/100/1000/', false, NULL),
         (3003, 'Backslash\\\\quote\\\"newline\\n\\ttab', 'MergeRequest', 2000, 1, '1/100/1000/', false, NULL)",
        t("gl_note")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group', ['state:active', 'user_type:human'], ['visibility_level:public']),
         ('1/101/', 1, 'User', 'MEMBER_OF', 101, 'Group', ['state:active', 'user_type:human'], ['visibility_level:private']),
         ('1/101/', 2, 'User', 'MEMBER_OF', 101, 'Group', ['state:active', 'user_type:human'], ['visibility_level:private']),
         ('1/102/', 3, 'User', 'MEMBER_OF', 102, 'Group', ['state:active', 'user_type:human'], ['visibility_level:internal']),
         ('1/102/', 4, 'User', 'MEMBER_OF', 102, 'Group', ['state:blocked', 'user_type:project_bot'], ['visibility_level:internal']),
         ('1/100/', 5, 'User', 'MEMBER_OF', 100, 'Group', ['state:active', 'user_type:human'], ['visibility_level:public']),
         ('1/200/', 100, 'Group', 'MEMBER_OF', 200, 'Group', ['visibility_level:public'], ['visibility_level:public']),
         ('1/300/', 200, 'Group', 'MEMBER_OF', 300, 'Group', ['visibility_level:public'], ['visibility_level:public']),
         ('1/100/', 100, 'Group', 'CONTAINS', 1000, 'Project', [], []),
         ('1/101/', 101, 'Group', 'CONTAINS', 1001, 'Project', [], []),
         ('1/102/', 102, 'Group', 'CONTAINS', 1002, 'Project', [], []),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 2000, 'MergeRequest', [], []),
         ('1/101/1001/', 2, 'User', 'AUTHORED', 2001, 'MergeRequest', [], []),
         ('1/102/1002/', 3, 'User', 'AUTHORED', 2002, 'MergeRequest', [], []),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3000, 'Note', [], []),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3002, 'Note', [], []),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3003, 'Note', [], []),
         ('1/101/1001/', 2001, 'MergeRequest', 'HAS_NOTE', 3001, 'Note', [], [])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
}

async fn run_pipeline(ctx: &TestContext, json: &str, svc: &MockRedactionService) -> Value {
    let value =
        GraphFormatter.format(&pipeline_output(ctx, json, svc, test_security_context()).await);
    assert_valid(&value);
    value
}

pub(super) async fn pipeline_output(
    ctx: &TestContext,
    json: &str,
    svc: &MockRedactionService,
    security_ctx: SecurityContext,
) -> PipelineOutput {
    let ontology = load_ontology();
    let data_model = derive_clickhouse_data_model(&ontology);
    let client = Arc::new(ctx.create_client());
    let compiled = Arc::new(
        compile_model(
            json,
            query_engine::compiler::Frontend::JsonDsl,
            &data_model,
            &security_ctx,
        )
        .unwrap(),
    );

    let batches = ctx.query_parameterized(&compiled.base).await;
    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context).unwrap();
    let redacted_count = run_redaction(&mut result, svc);

    let mut server_extensions = TypeMap::default();
    server_extensions.insert(client);
    server_extensions.insert(data_model);
    let mut pipeline_ctx = QueryPipelineContext {
        frontend: query_engine::compiler::Frontend::JsonDsl,
        query_json: String::new(),
        compiled: Some(Arc::clone(&compiled)),
        ontology: Arc::clone(&ontology),
        security_context: Some(security_ctx),
        server_extensions,
        phases: TypeMap::default(),
    };
    pipeline_ctx.phases.insert(RedactionOutput {
        query_result: result,
        redacted_count,
    });
    let mut obs = NoOpObserver;

    let hydration_output = HydrationStage
        .execute(&mut pipeline_ctx, &mut obs)
        .await
        .expect("pipeline should succeed");

    let mut query_result = hydration_output.query_result;
    let pagination = Some(query_engine::shared::paginate(
        &mut query_result,
        &compiled.input,
    ));

    PipelineOutput {
        row_count: query_result.authorized_count(),
        redacted_count: hydration_output.redacted_count,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: vec![compiled.base.sql.clone()],
        compiled: Arc::clone(&compiled),
        query_result,
        result_context: hydration_output.result_context,
        execution_log: vec![],
        pagination,
    }
}

fn allow_all() -> MockRedactionService {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2, 3, 4, 5]);
    svc.allow("group", &[100, 101, 102, 200, 300]);
    svc.allow("project", &[1000, 1001, 1002]);
    svc.allow("merge_request", &[2000, 2001, 2002]);
    svc.allow("note", &[3000, 3001, 3002, 3003, 9000]);
    svc
}

async fn traversal_single_hop_exact(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "traversal");

    let nodes = value["nodes"].as_array().unwrap();
    let user_ids = node_ids(nodes, "User");
    let group_ids = node_ids(nodes, "Group");
    assert_eq!(user_ids, HashSet::from([1, 2, 3, 4, 5]));
    assert_eq!(group_ids, HashSet::from([100, 101, 102]));

    let alice = find_node(nodes, "User", 1);
    assert_eq!(alice["username"].as_str().unwrap(), "alice");
    assert!(alice["id"].is_string());
    let bob = find_node(nodes, "User", 2);
    assert_eq!(bob["username"].as_str().unwrap(), "bob");
    let group100 = find_node(nodes, "Group", 100);
    assert_eq!(group100["name"].as_str().unwrap(), "Public Group");
    let group101 = find_node(nodes, "Group", 101);
    assert_eq!(group101["name"].as_str().unwrap(), "Private Group");
    let group102 = find_node(nodes, "Group", 102);
    assert_eq!(group102["name"].as_str().unwrap(), "Internal Group");

    let edges = value["edges"].as_array().unwrap();
    assert!(edges.iter().all(|e| e["type"] == "MEMBER_OF"));

    let e_alice_pub = edges
        .iter()
        .find(|e| e["from_id"] == "1" && e["to_id"] == "100")
        .unwrap();
    assert_eq!(e_alice_pub["from"], "User");
    assert_eq!(e_alice_pub["to"], "Group");
    assert_eq!(e_alice_pub["type"], "MEMBER_OF");

    let e_alice_priv = edges
        .iter()
        .find(|e| e["from_id"] == "1" && e["to_id"] == "101")
        .unwrap();
    assert_eq!(e_alice_priv["from"], "User");
    assert_eq!(e_alice_priv["to"], "Group");

    assert!(
        edges
            .iter()
            .any(|e| e["from_id"] == "2" && e["to_id"] == "101"),
        "bob->group101 edge"
    );
    assert!(
        edges
            .iter()
            .any(|e| e["from_id"] == "5" && e["to_id"] == "100"),
        "unicode->group100 edge"
    );

    for edge in edges {
        assert!(edge["from_id"].is_string());
        assert!(edge["to_id"].is_string());
        assert!(
            edge.get("path_id").is_none(),
            "traversal edges should not have path_id"
        );
        assert!(
            edge.get("step").is_none(),
            "traversal edges should not have step"
        );
        assert!(
            edge.get("depth").is_none(),
            "traversal edges should not have depth"
        );
    }
}

async fn neighbors_outgoing_exact(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
            "neighbors": {"direction": "outgoing"}
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "neighbors");

    let nodes = value["nodes"].as_array().unwrap();
    let center = find_node(nodes, "User", 1);
    assert!(center["id"].is_string());

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    for edge in edges {
        assert_eq!(edge["from"], "User");
        assert_eq!(edge["from_id"], "1");
        assert!(edge["to"].is_string());
        assert!(edge["to_id"].is_string());
        assert!(edge["type"].is_string());
        assert!(
            edge.get("path_id").is_none(),
            "neighbor edges should not have path_id"
        );
        assert!(
            edge.get("step").is_none(),
            "neighbor edges should not have step"
        );
    }

    let edge_types: HashSet<&str> = edges.iter().filter_map(|e| e["type"].as_str()).collect();
    assert!(edge_types.contains("MEMBER_OF"));
    assert!(edge_types.contains("AUTHORED"));

    let member_targets: HashSet<i64> = edges
        .iter()
        .filter(|e| e["type"] == "MEMBER_OF")
        .filter_map(|e| e["to_id"].as_str().and_then(|s| s.parse().ok()))
        .collect();
    assert!(
        member_targets.contains(&100),
        "alice is MEMBER_OF group 100"
    );
    assert!(
        member_targets.contains(&101),
        "alice is MEMBER_OF group 101"
    );

    let authored_targets: HashSet<i64> = edges
        .iter()
        .filter(|e| e["type"] == "AUTHORED")
        .filter_map(|e| e["to_id"].as_str().and_then(|s| s.parse().ok()))
        .collect();
    assert!(authored_targets.contains(&2000), "alice AUTHORED MR 2000");
}

async fn neighbors_incoming_exact(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "g", "entity": "Group", "node_ids": [101]}],
            "neighbors": {"direction": "incoming"}
        }"#,
        &allow_all(),
    )
    .await;

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    for edge in edges {
        assert_eq!(edge["to"], "Group");
        assert_eq!(edge["to_id"], "101");
        assert_eq!(edge["type"], "MEMBER_OF");
        assert!(edge["from_id"].is_string());
        assert!(edge.get("path_id").is_none());
    }

    let nodes = value["nodes"].as_array().unwrap();
    let center = find_node(nodes, "Group", 101);
    assert!(center["id"].is_string());

    let neighbor_ids = node_ids(nodes, "User");
    assert!(neighbor_ids.contains(&1), "alice is MEMBER_OF group 101");
    assert!(neighbor_ids.contains(&2), "bob is MEMBER_OF group 101");
    assert!(!neighbor_ids.contains(&3), "charlie is NOT in group 101");
    assert!(!neighbor_ids.contains(&4), "diana is NOT in group 101");
    assert!(
        !neighbor_ids.contains(&5),
        "unicode user is NOT in group 101"
    );

    let from_ids: HashSet<i64> = edges
        .iter()
        .filter_map(|e| e["from_id"].as_str().and_then(|s| s.parse().ok()))
        .collect();
    assert!(from_ids.contains(&1), "alice edge from_id");
    assert!(from_ids.contains(&2), "bob edge from_id");
}

async fn neighbors_both_exact(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "g", "entity": "Group", "node_ids": [100]}],
            "neighbors": {"direction": "both"}
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let center = find_node(nodes, "Group", 100);
    assert!(center["id"].is_string());

    let user_ids = node_ids(nodes, "User");
    assert!(user_ids.contains(&1), "alice is MEMBER_OF group 100");
    assert!(user_ids.contains(&5), "unicode user is MEMBER_OF group 100");

    let project_ids = node_ids(nodes, "Project");
    assert!(
        project_ids.contains(&1000),
        "group 100 CONTAINS project 1000"
    );

    let edges = value["edges"].as_array().unwrap();
    let edge_types: HashSet<&str> = edges.iter().filter_map(|e| e["type"].as_str()).collect();
    assert!(
        edge_types.contains("MEMBER_OF"),
        "should have MEMBER_OF edges"
    );
    assert!(
        edge_types.contains("CONTAINS"),
        "should have CONTAINS edges"
    );

    assert!(edges.iter().any(|e| {
        e["from"] == "User"
            && e["from_id"] == "1"
            && e["to"] == "Group"
            && e["to_id"] == "100"
            && e["type"] == "MEMBER_OF"
    }));
    assert!(edges.iter().any(|e| {
        e["from"] == "User"
            && e["from_id"] == "5"
            && e["to"] == "Group"
            && e["to_id"] == "100"
            && e["type"] == "MEMBER_OF"
    }));
    assert!(edges.iter().any(|e| {
        e["from"] == "Group"
            && e["from_id"] == "100"
            && e["to"] == "Project"
            && e["to_id"] == "1000"
            && e["type"] == "CONTAINS"
    }));

    for edge in edges {
        assert!(edge["from_id"].is_string());
        assert!(edge["to_id"].is_string());
        assert!(edge.get("path_id").is_none());
    }
}

async fn ungrouped_count_emits_aggregates(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}}],
            "aggregations": [{"count": "g", "as": "total"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "aggregation");
    assert!(value["edges"].as_array().unwrap().is_empty());
    assert!(
        value["nodes"].as_array().unwrap().is_empty(),
        "ungrouped aggregation should have no nodes"
    );

    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0]["name"], "total");
    assert_eq!(columns[0]["function"], "count");
    assert_eq!(columns[0]["target"], "g");
    let row = first_aggregation_row(&value);
    assert_eq!(
        row["total"].as_i64().unwrap(),
        5,
        "should count all 5 groups (100, 101, 102, 200, 300)"
    );
}

async fn ungrouped_multiple_functions_emits_aggregates(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}}],
            "aggregations": [
                {"count": "g", "as": "total"},
                {"min": "g.id", "as": "min_id"},
                {"max": "g.id", "as": "max_id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    assert!(value["nodes"].as_array().unwrap().is_empty());

    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0]["name"], "total");
    assert_eq!(columns[0]["function"], "count");
    assert_eq!(columns[1]["name"], "min_id");
    assert_eq!(columns[1]["function"], "min");
    assert_eq!(columns[1]["property"], "id");
    assert_eq!(columns[2]["name"], "max_id");
    assert_eq!(columns[2]["function"], "max");
    assert_eq!(columns[2]["property"], "id");
    let row = first_aggregation_row(&value);
    assert_eq!(row["total"].as_i64().unwrap(), 5);
    assert_eq!(row["min_id"].as_i64().unwrap(), 100);
    assert_eq!(row["max_id"].as_i64().unwrap(), 300);
}

async fn grouped_aggregation_uses_node_group_rows(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": ["u"],
            "aggregations": [{"count": "g", "as": "group_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = aggregation_nodes(&value, "u");
    assert!(
        !nodes.is_empty(),
        "grouped aggregation should have node group cells"
    );
    assert!(
        nodes.iter().all(|n| n["type"] == "User"),
        "grouped aggregation should only have User node group cells"
    );

    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0]["name"], "group_count");
    assert_eq!(columns[0]["function"], "count");
    assert_eq!(columns[0]["target"], "g");
    assert!(
        columns[0].get("value").is_none() || columns[0]["value"].is_null(),
        "grouped columns should not carry values"
    );
}

async fn ungrouped_count_with_redaction(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("group", &[100, 101]);

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}}],
            "aggregations": [{"count": "g", "as": "total"}],
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    assert!(value["nodes"].as_array().unwrap().is_empty());

    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0]["name"], "total");
    assert_eq!(columns[0]["function"], "count");
    let row = first_aggregation_row(&value);
    assert_eq!(row["total"].as_i64().unwrap(), 5);
}

async fn traversal_incoming_direction(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User", "columns": ["username"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "g", "to": "u", "direction": "incoming"}],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let group_ids = node_ids(nodes, "Group");
    let user_ids = node_ids(nodes, "User");
    assert!(!group_ids.is_empty());
    assert!(!user_ids.is_empty());
    assert!(group_ids.contains(&100));
    assert!(user_ids.contains(&1));

    let g100 = find_node(nodes, "Group", 100);
    assert_eq!(g100["name"].as_str().unwrap(), "Public Group");
    let alice = find_node(nodes, "User", 1);
    assert_eq!(alice["username"].as_str().unwrap(), "alice");

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    assert!(edges.iter().all(|e| e["type"] == "MEMBER_OF"));

    for edge in edges {
        assert!(edge["from_id"].is_string());
        assert!(edge["to_id"].is_string());
        assert!(edge.get("path_id").is_none());
    }
}

async fn neighbors_with_rel_types_filter(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
            "neighbors": {"direction": "outgoing", "rel_types": ["AUTHORED"]}
        }"#,
        &allow_all(),
    )
    .await;

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    assert!(
        edges.iter().all(|e| e["type"] == "AUTHORED"),
        "only AUTHORED edges should appear"
    );

    for edge in edges {
        assert_eq!(edge["from"], "User");
        assert_eq!(edge["from_id"], "1");
        assert!(edge["to_id"].is_string());
        assert!(edge.get("path_id").is_none());
    }

    let nodes = value["nodes"].as_array().unwrap();
    let center = find_node(nodes, "User", 1);
    assert!(center["id"].is_string());

    let neighbor_types: HashSet<&str> = nodes
        .iter()
        .filter(|n| n["type"] != "User")
        .filter_map(|n| n["type"].as_str())
        .collect();
    assert!(neighbor_types.contains("MergeRequest"));
    assert!(
        !neighbor_types.contains("Group"),
        "MEMBER_OF edges should be filtered out"
    );

    let mr_ids = node_ids(nodes, "MergeRequest");
    assert!(mr_ids.contains(&2000), "alice authored MR 2000");
}

// When the query omits `alias`, the column `name` in the response MUST equal
// the function name (e.g. "count", "sum", "avg"). Regression guard for the
// v2 compiler bug where the default was "agg_result" instead.
async fn no_alias_grouped_count_uses_derived_name(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": ["u"],
            "aggregations": [{"count": "g"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(
        columns[0]["name"], "count_g",
        "default output name must derive from the expression"
    );
    assert_eq!(columns[0]["function"], "count");

    let nodes = aggregation_nodes(&value, "u");
    let alice = find_node(&nodes, "User", 1);
    assert!(
        alice.get("count_g").is_some(),
        "aggregate value must appear under key 'count_g' in the node group row"
    );
    assert_eq!(alice["count_g"].as_i64().unwrap(), 2);
}

async fn no_alias_ungrouped_count_uses_derived_name(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}}],
            "aggregations": [{"count": "g"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(
        columns[0]["name"], "count_g",
        "ungrouped default output name must derive from the expression"
    );
    assert_eq!(columns[0]["function"], "count");
    let row = first_aggregation_row(&value);
    assert_eq!(row["count_g"].as_i64().unwrap(), 5);
}

async fn no_alias_multi_agg_each_uses_own_derived_name(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": ["g"],
            "aggregations": [
                {"count": "u"},
                {"min": "u.id"},
                {"max": "u.id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0]["name"], "count_u");
    assert_eq!(columns[1]["name"], "min_u_id");
    assert_eq!(columns[2]["name"], "max_u_id");

    let nodes = aggregation_nodes(&value, "g");
    let g100 = find_node(&nodes, "Group", 100);
    assert!(
        g100.get("count_u").is_some(),
        "count_u key must exist on node"
    );
    assert!(
        g100.get("min_u_id").is_some(),
        "min_u_id key must exist on node"
    );
    assert!(
        g100.get("max_u_id").is_some(),
        "max_u_id key must exist on node"
    );
}

#[tokio::test]
async fn graph_formatter_e2e() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    run_subtests_shared!(
        &ctx,
        traversal_single_hop_exact,
        traversal_incoming_direction,
        ungrouped_count_emits_aggregates,
        ungrouped_multiple_functions_emits_aggregates,
        grouped_aggregation_uses_node_group_rows,
        ungrouped_count_with_redaction,
        no_alias_grouped_count_uses_derived_name,
        no_alias_ungrouped_count_uses_derived_name,
        no_alias_multi_agg_each_uses_own_derived_name,
        neighbors_outgoing_exact,
        neighbors_incoming_exact,
        neighbors_both_exact,
        neighbors_with_rel_types_filter,
    );
}

#[test]
fn schema_rejects_response_missing_format_version() {
    let value = serde_json::json!({
        "query_type": "traversal",
        "nodes": [],
        "edges": []
    });
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(&value).collect();
    assert!(
        !errors.is_empty(),
        "schema must reject responses missing format_version"
    );
}

#[test]
fn schema_rejects_invalid_format_version_string() {
    let value = serde_json::json!({
        "format_version": "not-a-version",
        "query_type": "traversal",
        "nodes": [],
        "edges": []
    });
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(&value).collect();
    assert!(
        !errors.is_empty(),
        "schema must reject non-semver format_version"
    );
}

#[test]
fn schema_accepts_valid_format_version() {
    let value = serde_json::json!({
        "format_version": "1.0.0",
        "query_type": "traversal",
        "nodes": [],
        "edges": []
    });
    assert_valid(&value);
}
