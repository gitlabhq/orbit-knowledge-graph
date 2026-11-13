//! A query across connections through the server's own stages: routing against the served
//! remotes, the DAG stage, redaction, hydration, and the graph, TOON and GQL formatters.

use std::sync::Arc;

use orbit_server::pipeline::{HydrationStage, RoutingStage};
use orbit_server::redaction::QueryResult;
use query_data_model::testkit::limits;
use query_data_model::{Catalog, ClickHouse};
use query_engine::formatters::{GqlFormatter, GraphFormatter, ResultFormatter, ToonFormatter};
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap};
use query_engine::shared::{DagStage, PipelineOutput, RedactionOutput};
use serde_json::json;

use super::{caller, row};
use crate::authorization_variants::{deployment, registry};
use crate::dag_fakes::{SeededGraph, dag_executors, dag_model};
use crate::mock_redaction::MockRedactionService;

const DISCUSSED: &str = "MATCH (mr:MergeRequest {state: 'opened'})<-[:DISCUSSES]-(m:SlackMessage) RETURN mr.iid, mr.title, m.author, m.text ORDER BY mr.iid LIMIT 10";

/// The server's DAG stage over the seeded fakes, checking rows through the providers the `dag`
/// deployment binds: its `rails` checks allow project 42 only.
fn served_stage(model: &Arc<query_data_model::Model>, graph: Arc<SeededGraph>) -> DagStage {
    let (_, connections) = crate::dag_fakes::dag_connections(model);
    let bound =
        orbit_server::authorization::Deployment::bind(&deployment("dag"), model, &registry())
            .expect("the dag deployment binds the DAG model");
    DagStage {
        executors: dag_executors(&connections, graph, "alice"),
        peers: connections
            .peers()
            .iter()
            .filter(|peer| !matches!(peer, compiler::dag::Peer::ClickHouse(_)))
            .cloned()
            .collect(),
        checks: bound.checks().clone(),
    }
}

fn context(model: &Arc<query_data_model::Model>, query: &str) -> QueryPipelineContext {
    let catalog = Arc::new(Catalog::<ClickHouse>::from_model(model).unwrap());
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(catalog);
    server_extensions.insert(*limits());
    let session = query_engine::pipeline::SecurityContext::new_with_roles(
        1,
        vec![query_engine::pipeline::AuthorizedPath::new("1/9970/", 20)],
    )
    .unwrap();
    server_extensions.insert(crate::query_scenario::claims_for(&session));
    QueryPipelineContext {
        query_json: query.to_string(),
        compiled: None,
        security_context: None,
        grants: Some(caller("1/9970/")),
        server_extensions,
        phases: TypeMap::default(),
    }
}

/// A code stage's `read_code` check runs through the deployment's `rails` provider in the
/// server's own DAG stage, so rows of a project it denies never come back.
#[tokio::test]
async fn the_server_checks_remote_rows_through_the_deployments_providers() {
    let model = dag_model();
    let dag = served_stage(&model, Arc::new(SeededGraph(Vec::new())));
    let query = "MATCH (f:File) WHERE f.project_id IN [42, 77] AND f.content MATCHES ast('compile($$$ARGS)', 'rust') RETURN f.project_id, f.path LIMIT 20";
    let mut ctx = context(&model, query);
    let mut obs = NoOpObserver;
    RoutingStage {
        peers: dag.peers.clone(),
    }
    .execute(&mut ctx, &mut obs)
    .await
    .expect("the query routes against the served remotes");
    let executed = dag.execute(&mut ctx, &mut obs).await.expect("the DAG runs");
    let rows = super::json_rows(&executed.batches);
    let projects: Vec<&serde_json::Value> = rows.iter().map(|row| &row["f_project_id"]).collect();
    assert_eq!(projects, [&json!(42), &json!(42)]);
}

async fn served_output() -> PipelineOutput {
    let model = dag_model();
    let graph = Arc::new(SeededGraph(vec![
        row(
            json!({"mr_iid": 1, "mr_title": "Add feature A", "mr_id": 2000, "_orbit_mr_id": 2000, "_orbit_mr_type": "MergeRequest"}),
        ),
        row(
            json!({"mr_iid": 2, "mr_title": "Fix bug B", "mr_id": 2001, "_orbit_mr_id": 2001, "_orbit_mr_type": "MergeRequest"}),
        ),
    ]));
    let dag = served_stage(&model, graph);
    let mut ctx = context(&model, DISCUSSED);
    let mut obs = NoOpObserver;
    RoutingStage {
        peers: dag.peers.clone(),
    }
    .execute(&mut ctx, &mut obs)
    .await
    .expect("the query routes against the served remotes");
    let executed = dag.execute(&mut ctx, &mut obs).await.expect("the DAG runs");
    let compiled = ctx
        .compiled
        .clone()
        .expect("the engine stage compiled the response");
    let mut result =
        QueryResult::from_batches(&executed.batches, &executed.result_context).unwrap();
    let mut redaction = MockRedactionService::new();
    redaction.allow("merge_request", &[2000]);
    let redacted_count = result.apply_authorizations(&redaction.check(&result.resource_checks()));
    ctx.phases.insert(RedactionOutput {
        query_result: result,
        redacted_count,
    });
    let hydrated = HydrationStage
        .execute(&mut ctx, &mut obs)
        .await
        .expect("a joined response hydrates nothing");
    let mut query_result = hydrated.query_result;
    let pagination = Some(query_engine::shared::paginate(&mut query_result, &compiled));
    PipelineOutput {
        row_count: query_result.authorized_count(),
        redacted_count: hydrated.redacted_count,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: vec![compiled.base.sql.clone()],
        compiled,
        query_result,
        result_context: hydrated.result_context,
        execution_log: ctx
            .phases
            .remove::<query_engine::shared::QueryExecutionLog>()
            .unwrap_or_default()
            .0,
        pagination,
    }
}

#[tokio::test]
async fn the_server_formats_a_joined_response_like_a_single_stage_one() {
    let output = served_output().await;
    assert_eq!(output.row_count, 1);
    assert_eq!(output.redacted_count, 1);

    let (header, rows) = GqlFormatter.table(&output);
    assert_eq!(header, ["mr", "m"]);
    assert_eq!(
        rows,
        [[
            "(:MergeRequest {id: 2000, iid: 1, title: \"Add feature A\"})",
            "(:SlackMessage {id: \"m1\", author: \"ann\", text: \"deploy to staging at 5pm\"})",
        ]]
    );

    let graph = GraphFormatter.format(&output);
    let nodes = graph["nodes"].as_array().expect("graph nodes");
    let mut found: Vec<(String, String)> = nodes
        .iter()
        .map(|node| (node["type"].to_string(), node["id"].to_string()))
        .collect();
    found.sort();
    assert_eq!(
        found,
        [
            ("\"MergeRequest\"".to_string(), "\"2000\"".to_string()),
            ("\"SlackMessage\"".to_string(), "\"m1\"".to_string()),
        ]
    );

    let toon = ToonFormatter.format(&output).to_string();
    assert!(toon.contains("deploy to staging at 5pm"), "{toon}");
    assert!(!toon.contains("Fix bug B"), "{toon}");
}
