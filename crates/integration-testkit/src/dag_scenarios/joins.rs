//! Queries across connections: a relational stage hands its keys to a remote stage, and the
//! engine stage joins both sides and applies the result columns, order and limit.

use compiler::dag::{Backend, Input, KeyInput, Rendered};
use compiler::orbit_authorization::Key;
use query_engine::shared::dag::Event;
use serde_json::json;

use super::single::ops;
use super::{caller, row, run, run_with};
use crate::dag_fakes::AllowedKeys;

const MENTIONS: &str = "MATCH (mr:MergeRequest {state: 'opened'})<-[:DISCUSSES]-(m:SlackMessage) RETURN mr.iid, mr.title, m.author, m.text ORDER BY mr.iid LIMIT 10";

const BOUNDED: &str = "MATCH (p:Project)<-[:IN_PROJECT]-(f:File) WHERE p.id IN [42, 77] AND f.content MATCHES ast('compile($$$ARGS)', 'rust') RETURN p.full_path, f.path, match.line ORDER BY f.path LIMIT 20";

fn open_merge_requests() -> Vec<query_data_model::catalog::Row> {
    vec![
        row(
            json!({"mr_iid": 1, "mr_title": "Add feature A", "mr_id": 2000, "_orbit_mr_id": 2000, "_orbit_mr_type": "MergeRequest"}),
        ),
        row(
            json!({"mr_iid": 2, "mr_title": "Fix bug B", "mr_id": 2001, "_orbit_mr_id": 2001, "_orbit_mr_type": "MergeRequest"}),
        ),
    ]
}

#[tokio::test]
async fn merge_requests_join_the_slack_messages_that_mention_them() {
    let ran = run(MENTIONS, &caller("1/9970/"), open_merge_requests()).await;
    let split = ran
        .events
        .iter()
        .find_map(|event| match event {
            Event::Split { stages } => Some(stages.clone()),
            _ => None,
        })
        .unwrap();
    let backends: Vec<Backend> = split.iter().map(|stage| stage.backend).collect();
    assert_eq!(
        backends,
        [Backend::ClickHouse, Backend::Live, Backend::Engine]
    );
    assert_eq!(
        split[1].inputs,
        [Input::Keys(KeyInput {
            from: 0,
            columns: vec!["mr_id".into()],
            binding: "m".into(),
            properties: vec!["merge_request_id".into()],
        })]
    );
    assert_eq!(
        split[2].inputs,
        [Input::Rows { from: 0 }, Input::Rows { from: 1 }]
    );
    let Rendered::Sql { sql, .. } = &ran.rendered(0)[0] else {
        panic!("the merge request stage renders SQL");
    };
    assert!(
        sql.starts_with("SELECT mr.iid AS mr_iid, mr.title AS mr_title, mr.id AS mr_id, "),
        "a stage that feeds the engine hands its text on whole: {sql}"
    );
    assert!(sql.contains("startsWith(mr.traversal_path"), "{sql}");
    let call = ran
        .rendered(1)
        .into_iter()
        .find_map(|request| match request {
            Rendered::Live(call) => Some(call),
            _ => None,
        })
        .unwrap();
    assert_eq!(call.workspaces, ["T01"]);
    assert_eq!(call.filters[0].op, "in");
    assert_eq!(call.filters[0].value, json!([2000, 2001]));
    assert_eq!(
        ops(&ran.plan(2, true)),
        [
            "Rows", "Rows", "Join", "Order", "Select", "Tag", "Excerpt", "Limit"
        ]
    );
    let keys = ran.events.iter().find_map(|event| match event {
        Event::Keys { count, sample, .. } => Some((*count, sample.clone())),
        _ => None,
    });
    assert_eq!(keys, Some((2, vec![Key::Int(2000), Key::Int(2001)])));
    assert_eq!(
        ran.rows(),
        [
            row(
                json!({"mr_iid": 1, "mr_title": "Add feature A", "m_author": "ann", "m_text": "deploy to staging at 5pm", "_orbit_mr_id": 2000, "_orbit_mr_type": "MergeRequest", "_orbit_m_id": "m1", "_orbit_m_type": "SlackMessage"})
            ),
            row(
                json!({"mr_iid": 2, "mr_title": "Fix bug B", "m_author": "ann", "m_text": "deploy memes", "_orbit_mr_id": 2001, "_orbit_mr_type": "MergeRequest", "_orbit_m_id": "m4", "_orbit_m_type": "SlackMessage"})
            ),
        ]
    );
}

#[tokio::test]
async fn a_code_search_is_bounded_by_the_callers_readable_projects() {
    let projects = vec![
        row(
            json!({"p_full_path": "gitlab-org/orbit", "p_id": 42, "_orbit_p_id": 42, "_orbit_p_type": "Project"}),
        ),
        row(
            json!({"p_full_path": "secret/vault", "p_id": 77, "_orbit_p_id": 77, "_orbit_p_type": "Project"}),
        ),
    ];
    let ran = run(BOUNDED, &caller("1/9970/"), projects).await;
    let request = ran
        .rendered(1)
        .into_iter()
        .find_map(|request| match request {
            Rendered::Code(request) => Some(request),
            _ => None,
        })
        .unwrap();
    assert_eq!(request.repositories, [42]);
    assert!(ran.plan(1, false)[0].contains("projects: [42, 77]"));
    assert_eq!(
        ran.rows(),
        [
            row(
                json!({"p_full_path": "gitlab-org/orbit", "f_path": "crates/orbit-server/src/query.rs", "match.line": 48, "_orbit_p_id": 42, "_orbit_p_type": "Project"})
            ),
            row(
                json!({"p_full_path": "gitlab-org/orbit", "f_path": "crates/query-engine/compiler/src/lib.rs", "match.line": 120, "_orbit_p_id": 42, "_orbit_p_type": "Project"})
            ),
        ]
    );
}

#[tokio::test]
async fn the_observer_sees_each_stage_in_order() {
    let ran = run(MENTIONS, &caller("1/9970/"), open_merge_requests()).await;
    assert_eq!(
        ran.kinds(),
        [
            "logical",
            "split",
            "started 0",
            "planned 0",
            "emitted 0",
            "rendered 0",
            "rows 0",
            "finished 0",
            "started 1",
            "keys 0->1",
            "rendered 1",
            "planned 1",
            "emitted 1",
            "rendered 1",
            "rows 1",
            "finished 1",
            "started 2",
            "planned 2",
            "emitted 2",
            "rendered 2",
            "rows 2",
            "finished 2",
        ]
    );
    let serialized = serde_json::to_value(&ran.events).unwrap();
    assert_eq!(serialized[0]["event"], "logical");
    assert_eq!(serialized[1]["stages"][1]["backend"], "live");
}

#[tokio::test]
async fn the_split_carries_each_stages_logical_sub_plan() {
    let ran = run(MENTIONS, &caller("1/9970/"), open_merge_requests()).await;
    let split = ran
        .events
        .iter()
        .find_map(|event| match event {
            Event::Split { stages } => Some(stages.clone()),
            _ => None,
        })
        .unwrap();
    let logical: Vec<Vec<&str>> = split.iter().map(|stage| ops(&stage.logical)).collect();
    assert_eq!(
        logical,
        [
            vec!["Entities", "Filter", "Select", "Limit"],
            vec!["Entities", "Select", "Limit"],
            vec![],
        ]
    );
    assert!(split[0].logical[0].ends_with("-> [mr]"));
    assert!(split[1].logical[0].ends_with("-> [m]"));
}

/// The merge request stage's `read_merge_request` check runs before its keys reach the Slack
/// stage, so the live service never sees the key of a merge request the caller cannot read, and
/// a caller who can read none sends no call.
#[tokio::test]
async fn a_producers_row_checks_run_before_its_keys_cross_the_boundary() {
    let keys = |ran: &super::Run| {
        ran.events
            .iter()
            .find_map(|event| match event {
                Event::Keys { sample, .. } => Some(sample.clone()),
                _ => None,
            })
            .unwrap_or_default()
    };
    let one = AllowedKeys::checks("merge_request", [Key::Int(2000)]);
    let ran = run_with(
        MENTIONS,
        &caller("1/9970/"),
        open_merge_requests(),
        "alice",
        &one,
    )
    .await;
    assert_eq!(keys(&ran), [Key::Int(2000)]);
    let rows = ran.rows();
    assert!(rows.iter().all(|row| row["mr_iid"] == 1), "{rows:?}");

    let none = AllowedKeys::checks("merge_request", []);
    let ran = run_with(
        MENTIONS,
        &caller("1/9970/"),
        open_merge_requests(),
        "alice",
        &none,
    )
    .await;
    assert!(ran.rendered(1).is_empty());
    assert!(ran.rows().is_empty());
}

#[tokio::test]
async fn an_empty_key_set_skips_the_stage_that_reads_it() {
    let ran = run(MENTIONS, &caller("1/9970/"), Vec::new()).await;
    assert!(ran.rendered(1).is_empty());
    assert!(ran.rows().is_empty());
}

/// A stage that returns more rows than the key cap fails, even when many rows share a key, so
/// no later page loses rows past the cap.
#[tokio::test]
async fn a_stage_over_the_row_cap_fails_however_few_keys_it_holds() {
    let merge_requests = |count: i64| -> Vec<query_data_model::catalog::Row> {
        (0..count)
            .map(|n| {
                let id = 5000 + n % 500;
                row(
                    json!({"mr_iid": n, "mr_title": "t", "mr_id": id, "_orbit_mr_id": id, "_orbit_mr_type": "MergeRequest"}),
                )
            })
            .collect()
    };
    let error = run(MENTIONS, &caller("1/9970/"), merge_requests(1001))
        .await
        .error();
    assert_eq!(
        error,
        "limit exceeded: stage 0 returned more than 1000 rows; a stage hands at most 1000 rows to the next"
    );
    let ran = run(MENTIONS, &caller("1/9970/"), merge_requests(1000)).await;
    assert!(ran.outcome.is_ok());
}

#[tokio::test]
async fn joined_text_is_excerpted_like_a_single_stage_response() {
    let title = "a".repeat(200_000);
    let merge_requests = vec![row(
        json!({"mr_iid": 1, "mr_title": title, "mr_id": 2000, "_orbit_mr_id": 2000, "_orbit_mr_type": "MergeRequest"}),
    )];
    let ran = run(MENTIONS, &caller("1/9970/"), merge_requests).await;
    let rows = ran.rows();
    let excerpt = rows[0]["mr_title"].as_str().unwrap();
    let chars = 8 * 1024 * 1024 / 4 / 11;
    assert_eq!(excerpt, format!("{} [truncated]", "a".repeat(chars)));
    assert_eq!(rows[0]["m_text"], "deploy to staging at 5pm");
}

const RAISED: &str = "MATCH (i:Incident)-[:RAISED_IN]->(m:SlackMessage) WHERE i.title CONTAINS 'outage' RETURN i.title, m.author, m.text LIMIT 10";

/// A ClickHouse store keys incidents to the Slack messages they were raised in, on one column:
/// the edge crosses connections, so the planner splits it into a stage boundary with a key set.
#[tokio::test]
async fn a_single_column_key_crosses_from_a_relational_store() {
    let incidents = vec![row(
        json!({"i_title": "staging outage", "i_message_id": "m1", "_orbit_i_id": 7, "_orbit_i_type": "Incident"}),
    )];
    let ran = run(RAISED, &caller("1/9970/"), incidents).await;
    if let Err(error) = &ran.outcome {
        panic!("the incident query failed: {error}");
    }
    let split = ran
        .events
        .iter()
        .find_map(|event| match event {
            Event::Split { stages } => Some(stages.clone()),
            _ => None,
        })
        .unwrap();
    let backends: Vec<Backend> = split.iter().map(|stage| stage.backend).collect();
    assert_eq!(
        backends,
        [Backend::ClickHouse, Backend::Live, Backend::Engine]
    );
    assert_eq!(
        split[1].inputs,
        [Input::Keys(KeyInput {
            from: 0,
            columns: vec!["i_message_id".into()],
            binding: "m".into(),
            properties: vec!["id".into()],
        })]
    );
    let call = ran
        .rendered(1)
        .into_iter()
        .find_map(|request| match request {
            Rendered::Live(call) => Some(call),
            _ => None,
        })
        .unwrap();
    assert_eq!(call.filters[0].field, "id");
    assert_eq!(call.filters[0].value, json!(["m1"]));
    let rows = ran.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["i_title"], "staging outage");
    assert_eq!(rows[0]["m_text"], "deploy to staging at 5pm");
}
