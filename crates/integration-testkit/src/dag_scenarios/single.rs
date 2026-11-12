//! Queries one remote connection serves alone: a one-stage DAG whose stage runs the code or live
//! sub-pipeline, after the scope lookup its read's `via` chain needs.

use std::collections::BTreeSet;

use compiler::dag::Rendered;
use compiler::orbit_authorization::Key;
use query_data_model::implementations::live::{ExtractionRequest, RequestFilter};
use query_data_model::implementations::orbit_code::{RequestField, StructuralSearchRequest};
use serde_json::json;

use super::{caller, row, run, run_with};
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
    let ran = run(CODE, &caller("1/9970/"), Vec::new()).await;
    let planned = ran.plan(0, false);
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
    let emitted = ran.plan(0, true);
    assert_eq!(ops(&emitted)[..2], ["Repositories", "Restrict"]);
    assert!(emitted[1].starts_with("Restrict([42])"), "{emitted:?}");
    let rendered = ran.rendered(0);
    let Rendered::Sql { sql, .. } = &rendered[0] else {
        panic!("the scope lookup renders first: {rendered:?}");
    };
    assert_eq!(
        sql,
        "SELECT k0.id, k0.traversal_path FROM gl_project AS k0 FINAL WHERE (startsWith(k0.traversal_path, {p0:String}) AND k0.id IN {p1:Array(Int64)})"
    );
    assert_eq!(
        *code_request(&rendered),
        StructuralSearchRequest {
            repositories: vec![42],
            languages: vec!["rust".into()],
            trigrams: vec!["compile".into()],
            pattern: "compile($$$ARGS)".into(),
            limit: Some(50),
            fields: vec![
                RequestField::Path,
                RequestField::Line,
                RequestField::Capture("ARGS".into()),
                RequestField::Repository("project_id".into()),
            ],
        }
    );
    assert_eq!(
        ran.rows(),
        [
            row(
                json!({"path": "crates/query-engine/compiler/src/lib.rs", "line": 120, "ARGS": "raw, fe, &data_model, &ctx", "project_id": 42})
            ),
            row(
                json!({"path": "crates/orbit-server/src/query.rs", "line": 48, "ARGS": "&text, Frontend::Gql, &catalog, &security", "project_id": 42})
            ),
        ]
    );
}

#[tokio::test]
async fn the_slack_query_plans_into_a_live_call_and_returns_visible_messages() {
    let ran = run(SLACK, &caller("1/9970/"), Vec::new()).await;
    assert_eq!(
        ops(&ran.plan(0, false)),
        ["Call", "Filter", "Limit", "Select"]
    );
    assert_eq!(
        ops(&ran.plan(0, true)),
        ["Call", "Workspaces", "Filter", "Limit", "Select"]
    );
    let rendered = ran.rendered(0);
    let Rendered::Sql { sql, .. } = &rendered[0] else {
        panic!("the workspace lookup renders first: {rendered:?}");
    };
    assert_eq!(
        sql,
        "SELECT k0.id, k1.traversal_path FROM gl_slack_workspace AS k0 FINAL INNER JOIN gl_group AS k1 FINAL ON (k0.group_id = k1.id) WHERE startsWith(k1.traversal_path, {p0:String})"
    );
    assert_eq!(
        *live_request(&rendered),
        ExtractionRequest {
            operation: "search.messages".into(),
            workspaces: vec!["T01".into()],
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
            limit: Some(20),
            fields: vec!["author".into(), "text".into()],
        }
    );
    assert_eq!(
        ran.rows(),
        [row(
            json!({"author": "ann", "text": "deploy to staging at 5pm"})
        )]
    );
    let checks = AllowedKeys(BTreeSet::new());
    let bob = run_with(SLACK, &caller("1/9970/"), Vec::new(), "bob", &checks).await;
    assert_eq!(bob.rows().len(), 2);
}

#[tokio::test]
async fn the_repository_restriction_drops_projects_the_caller_cannot_read() {
    let query = CODE.replace("f.project_id = 42", "f.project_id IN [42, 77]");
    let narrow = run(&query, &caller("1/9970/"), Vec::new()).await;
    assert!(narrow.plan(0, false)[0].contains("projects: [42, 77]"));
    assert_eq!(code_request(&narrow.rendered(0)).repositories, [42]);
    let paths: Vec<_> = narrow
        .rows()
        .into_iter()
        .map(|row| row["path"].clone())
        .collect();
    assert!(!paths.contains(&json!("src/vault.rs")), "{paths:?}");

    let wide = run(&query, &caller("1/"), Vec::new()).await;
    let paths: Vec<_> = wide
        .rows()
        .into_iter()
        .map(|row| row["path"].clone())
        .collect();
    assert!(paths.contains(&json!("src/vault.rs")), "{paths:?}");
}

#[tokio::test]
async fn the_workspace_precondition_keeps_unreadable_namespaces_out_of_the_call() {
    let everywhere = run(SLACK, &caller("1/"), Vec::new()).await;
    assert_eq!(
        live_request(&everywhere.rendered(0)).workspaces,
        ["T01", "T02"]
    );
    assert_eq!(everywhere.rows().len(), 2);

    let narrow = run(SLACK, &caller("1/9970/"), Vec::new()).await;
    let texts: Vec<_> = narrow
        .rows()
        .into_iter()
        .map(|row| row["text"].clone())
        .collect();
    assert!(!texts.contains(&json!("deploy uses the production keys")));
}

#[tokio::test]
async fn a_caller_who_can_read_nothing_gets_no_request() {
    for (query, message) in [
        (
            CODE,
            "authorization error: the caller can read none of the requested repositories",
        ),
        (
            SLACK,
            "authorization error: the caller can read none of the workspaces",
        ),
    ] {
        let ran = run(query, &caller("1/424242/"), Vec::new()).await;
        assert!(ran.kinds().contains(&"failed 0".to_string()));
        assert!(
            ran.rendered(0)
                .iter()
                .all(|request| matches!(request, Rendered::Sql { .. }))
        );
        assert_eq!(ran.error(), message);
    }
}

#[tokio::test]
async fn construction_rejects_a_filter_without_push_down_and_an_unknown_capture() {
    let ctx = caller("1/9970/");
    let pushed = run(
        "MATCH (m:SlackMessage) WHERE m.id = 'm1' RETURN m.text LIMIT 5",
        &ctx,
        Vec::new(),
    )
    .await;
    assert_eq!(
        pushed.error(),
        "schema violation: search.messages cannot push down equality on SlackMessage.id; it filters on channel, author, sent_at, merge_request_id, text (contains)"
    );
    let capture = run(&CODE.replace("match.ARGS", "match.BODY"), &ctx, Vec::new()).await;
    assert_eq!(
        capture.error(),
        "reference error: unknown column match.BODY; available: match.line, match.ARGS"
    );
    let relational = compiler::compile(
        CODE,
        &std::sync::Arc::new(
            query_data_model::Catalog::<query_data_model::ClickHouse>::embedded().unwrap(),
        ),
        &ctx,
        query_data_model::testkit::limits(),
    );
    assert_eq!(
        relational.err().unwrap().to_string(),
        "schema violation: structural matches run on an orbit_code connection, not in SQL"
    );
}

#[tokio::test]
async fn code_rows_go_through_the_read_code_check() {
    let none = AllowedKeys(BTreeSet::new());
    let denied = run_with(CODE, &caller("1/9970/"), Vec::new(), "alice", &none).await;
    assert!(denied.rows().is_empty());
    let only = AllowedKeys(BTreeSet::from([Key::Int(42)]));
    let allowed = run_with(CODE, &caller("1/9970/"), Vec::new(), "alice", &only).await;
    assert_eq!(allowed.rows().len(), 2);
}
