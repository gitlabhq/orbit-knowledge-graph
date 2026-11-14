//! Queries one remote connection serves alone: the scope lookup stage its read's `via` chain
//! needs, then the stage that runs the code or live sub-pipeline.

use compiler::dag::Rendered;
use compiler::orbit_authorization::Key;
use query_data_model::implementations::live::{ExtractionRequest, RequestFilter};
use query_data_model::implementations::orbit_code::{RequestField, StructuralSearchRequest};
use serde_json::json;

use super::{caller, projects, row, run, run_with, workspaces};
use crate::dag_fakes::AllowedKeys;

pub(super) const CODE: &str = "MATCH (f:File) WHERE f.project_id = 42 AND f.content MATCHES ast('compile($$$ARGS)', 'rust') RETURN f.path, match.line, match.ARGS LIMIT 50";
pub(super) const SLACK: &str = "MATCH (m:SlackMessage) WHERE m.channel = 'orbit-dev' AND m.text CONTAINS 'deploy' RETURN m.author, m.text LIMIT 20";

/// The operation of each plan line.
pub(super) fn ops(plan: &[String]) -> Vec<&str> {
    plan.iter()
        .map(|line| line.split([' ', '(', '{']).next().unwrap_or_default())
        .collect()
}

fn code_request(rendered: &[Rendered]) -> &StructuralSearchRequest {
    rendered
        .iter()
        .find_map(|request| match request {
            Rendered::Code(request) => Some(request),
            _ => None,
        })
        .expect("a code request")
}

fn live_request(rendered: &[Rendered]) -> &ExtractionRequest {
    rendered
        .iter()
        .find_map(|request| match request {
            Rendered::Live(request) => Some(request),
            _ => None,
        })
        .expect("a live call")
}

#[tokio::test]
async fn the_code_query_plans_into_a_structural_search_and_returns_matches() {
    let ran = run(CODE, &caller("1/9970/"), projects(&[42])).await;
    let planned = ran.plan(1, false);
    assert_eq!(
        ops(&planned),
        [
            "Repositories",
            "Languages",
            "Prefilter",
            "Structural",
            "Top",
            "Select"
        ]
    );
    let emitted = ran.plan(1, true);
    assert_eq!(ops(&emitted)[..2], ["Repositories", "Restrict"]);
    assert!(emitted[1].starts_with("Restrict([42])"), "{emitted:?}");
    let Rendered::Sql { sql, .. } = &ran.rendered(0)[0] else {
        panic!("the scope lookup stage renders SQL");
    };
    assert_eq!(
        sql.split(" SETTINGS").next(),
        Some(
            "SELECT scope.id AS scope_id FROM (SELECT * FROM gl_project AS scope WHERE (startsWith(scope.traversal_path, {p0:String}) AND (scope.id = {p1:Int64})) ORDER BY scope.traversal_path ASC, scope.id ASC, scope._version DESC LIMIT 1 BY scope.traversal_path, scope.id) AS scope WHERE (scope._deleted = {p2:Bool}) LIMIT 1002"
        )
    );
    assert_eq!(
        *code_request(&ran.rendered(1)),
        StructuralSearchRequest {
            repositories: vec![42],
            heads: vec![],
            languages: vec!["rust".into()],
            trigrams: vec!["compile".into()],
            pattern: "compile($$$ARGS)".into(),
            limit: Some(1001),
            fields: vec![
                RequestField::Column("path".into()),
                RequestField::Repository("project_id".into()),
                RequestField::Line,
                RequestField::Capture("ARGS".into()),
            ],
        }
    );
    assert_eq!(
        ran.rows(),
        [
            row(
                json!({"f_path": "crates/query-engine/compiler/src/lib.rs", "match.line": 120, "match.ARGS": "raw, fe, &data_model, &ctx"})
            ),
            row(
                json!({"f_path": "crates/orbit-server/src/query.rs", "match.line": 48, "match.ARGS": "&text, Frontend::Gql, &catalog, &security"})
            ),
        ]
    );
}

#[tokio::test]
async fn the_slack_query_plans_into_a_live_call_and_returns_visible_messages() {
    let orbit = workspaces(&[("T01", 100)]);
    let ran = run(SLACK, &caller("1/9970/"), orbit.clone()).await;
    assert_eq!(
        ops(&ran.plan(1, false)),
        ["Call", "Filter", "Limit", "Select"]
    );
    assert_eq!(
        ops(&ran.plan(1, true)),
        ["Call", "Workspaces", "Filter", "Limit", "Select"]
    );
    let Rendered::Sql { sql, .. } = &ran.rendered(0)[0] else {
        panic!("the workspace lookup stage renders SQL");
    };
    assert!(
        sql.contains("FROM gl_slack_workspace AS scope FINAL WHERE startsWith(scope.traversal_path, {p0:String})")
            && sql.contains("FROM gl_group AS scope1 WHERE (startsWith(scope1.traversal_path, {p0:String})")
            && sql.contains("ON (scope1.id = scope.group_id)"),
        "the lookup reads the chain from the workspace to its group under both guards: {sql}"
    );
    assert_eq!(
        *live_request(&ran.rendered(1)),
        ExtractionRequest {
            operation: "search.messages".into(),
            workspaces: query_data_model::plan::live::Workspaces::Only(vec!["T01".into()]),
            filters: vec![
                RequestFilter {
                    field: "channel".into(),
                    op: "eq",
                    value: "orbit-dev".into(),
                },
                RequestFilter {
                    field: "text".into(),
                    op: "contains",
                    value: "deploy".into(),
                },
            ],
            window: None,
            limit: Some(1001),
            fields: vec!["author".into(), "text".into(), "id".into()],
        }
    );
    assert_eq!(
        ran.rows(),
        [row(
            json!({"m_author": "ann", "m_text": "deploy to staging at 5pm", "_orbit_m_id": "m1", "_orbit_m_type": "SlackMessage"})
        )]
    );
    let checks = AllowedKeys::checks("project", []);
    let bob = run_with(SLACK, &caller("1/9970/"), orbit, "bob", &checks).await;
    assert_eq!(bob.rows().len(), 2);
}

/// The project lookup reads the projects the code read names under the caller's scope, and the
/// code request reads only the ones it returns.
#[tokio::test]
async fn the_repository_restriction_keeps_the_projects_the_lookup_returns() {
    let query = CODE.replace("f.project_id = 42", "f.project_id IN [42, 77]");
    let narrow = run(&query, &caller("1/9970/"), projects(&[42])).await;
    let Rendered::Sql { params, .. } = &narrow.rendered(0)[0] else {
        panic!("the scope lookup stage renders SQL");
    };
    let values: Vec<&serde_json::Value> = params.iter().map(|(_, value)| value).collect();
    assert_eq!(values[..2], [&json!("1/9970/"), &json!([42, 77])]);
    assert!(narrow.plan(1, false)[0].contains("projects: [42, 77]"));
    assert_eq!(code_request(&narrow.rendered(1)).repositories, [42]);
    let paths: Vec<_> = narrow
        .rows()
        .into_iter()
        .map(|row| row["f_path"].clone())
        .collect();
    assert!(!paths.contains(&json!("src/vault.rs")), "{paths:?}");

    let wide = run(&query, &caller("1/"), projects(&[42, 77])).await;
    let paths: Vec<_> = wide
        .rows()
        .into_iter()
        .map(|row| row["f_path"].clone())
        .collect();
    assert!(paths.contains(&json!("src/vault.rs")), "{paths:?}");
}

#[tokio::test]
async fn the_workspace_precondition_keeps_unreadable_namespaces_out_of_the_call() {
    let both = workspaces(&[("T01", 100), ("T02", 101)]);
    let everywhere = run(SLACK, &caller("1/"), both).await;
    assert_eq!(
        live_request(&everywhere.rendered(1)).workspaces,
        query_data_model::plan::live::Workspaces::Only(vec!["T01".into(), "T02".into()])
    );
    assert_eq!(everywhere.rows().len(), 2);

    let narrow = run(SLACK, &caller("1/9970/"), workspaces(&[("T01", 100)])).await;
    let texts: Vec<_> = narrow
        .rows()
        .into_iter()
        .map(|row| row["m_text"].clone())
        .collect();
    assert!(!texts.contains(&json!("deploy uses the production keys")));
}

/// The lookup returns no key under the caller's scope, so the remote stage reads nothing: no
/// request goes out and the result is empty, as for a relational read.
#[tokio::test]
async fn a_caller_who_can_read_nothing_gets_no_request() {
    for query in [CODE, SLACK] {
        let ran = run(query, &caller("1/424242/"), Vec::new()).await;
        assert!(ran.kinds().contains(&"finished 1".to_string()));
        assert!(ran.rendered(1).is_empty());
        assert!(ran.rows().is_empty());
    }
}

#[tokio::test]
async fn construction_rejects_a_filter_without_push_down_and_an_unknown_capture() {
    let ctx = caller("1/9970/");
    let pushed = run(
        "MATCH (m:SlackMessage) WHERE m.workspace = 'T01' RETURN m.text LIMIT 5",
        &ctx,
        Vec::new(),
    )
    .await;
    assert_eq!(
        pushed.error(),
        "allowlist rejected: no store serves equality on SlackMessage.workspace"
    );
    let capture = run(&CODE.replace("match.ARGS", "match.BODY"), &ctx, Vec::new()).await;
    assert_eq!(
        capture.error(),
        "reference error: unknown column match.BODY; available: match.line, match.ARGS"
    );
    let relational = compiler::compile(
        CODE,
        &std::sync::Arc::new(
            query_data_model::Catalog::<query_data_model::ClickHouse>::embedded("server").unwrap(),
        ),
        &ctx,
        query_data_model::testkit::limits(),
    );
    assert_eq!(
        relational.err().unwrap().to_string(),
        "allowlist rejected: no store serves a structural search on File.content"
    );
}

#[tokio::test]
async fn code_rows_go_through_the_read_code_check() {
    let none = AllowedKeys::checks("project", []);
    let denied = run_with(CODE, &caller("1/9970/"), projects(&[42]), "alice", &none).await;
    assert!(denied.rows().is_empty());
    let only = AllowedKeys::checks("project", [Key::Int(42)]);
    let allowed = run_with(CODE, &caller("1/9970/"), projects(&[42]), "alice", &only).await;
    assert_eq!(allowed.rows().len(), 2);
}
