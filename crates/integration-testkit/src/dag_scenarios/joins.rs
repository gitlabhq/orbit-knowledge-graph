//! Queries across connections: a relational stage hands its keys to a remote stage, and the
//! Combine stage joins both sides and applies the result columns, order and limit.

use compiler::dag::{Backend, Input, KeyInput, Rendered};
use compiler::orbit_authorization::Key;
use query_engine::shared::dag::Event;
use serde_json::json;

use super::single::ops;
use super::{caller, counted, projects, row, run, run_with, workspaces};
use crate::dag_fakes::AllowedKeys;

const MENTIONS: &str = "MATCH (mr:MergeRequest {state: 'opened'})<-[:DISCUSSES]-(m:SlackMessage) RETURN mr.iid, mr.title, m.author, m.text ORDER BY mr.iid LIMIT 10";

const BOUNDED: &str = "MATCH (p:Project)<-[:IN_PROJECT]-(f:File) WHERE p.id IN [42, 77] AND f.content MATCHES ast('compile($$$ARGS)', 'rust') RETURN p.full_path, f.path, match.line ORDER BY f.path LIMIT 20";

/// Two open merge requests, and the workspace the caller's lookup returns.
fn open_merge_requests() -> Vec<query_data_model::catalog::Row> {
    let mut rows = vec![
        row(json!({"mr_iid": 1, "mr_title": "Add feature A", "mr_id": 2000})),
        row(json!({"mr_iid": 2, "mr_title": "Fix bug B", "mr_id": 2001})),
    ];
    rows.extend(workspaces(&[("T01", 100)]));
    rows
}

/// The Slack stage of a query that crosses from a ClickHouse stage into Slack messages: its
/// key set from the first stage and its scope keys from the workspace lookup.
fn slack_inputs(
    positions: Vec<usize>,
    on: &str,
    data_type: query_data_model::DataType,
) -> [Input; 2] {
    [
        Input::Keys(KeyInput {
            from: 0,
            positions,
            binding: "m".into(),
            properties: super::properties("SlackMessage", &[on]),
            types: vec![data_type],
        }),
        Input::Scope(KeyInput {
            from: 1,
            positions: vec![0],
            binding: "m".into(),
            properties: super::properties("SlackMessage", &["workspace"]),
            types: vec![query_data_model::DataType::String],
        }),
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
        [
            Backend::ClickHouse,
            Backend::ClickHouse,
            Backend::Live,
            Backend::Combine
        ]
    );
    assert!(split[1].inputs.is_empty());
    assert_eq!(
        split[2].inputs,
        slack_inputs(vec![2], "merge_request_id", query_data_model::DataType::Int)
    );
    assert_eq!(
        split[3].inputs,
        [Input::Rows { from: 0 }, Input::Rows { from: 2 }]
    );
    let Rendered::Sql { sql, .. } = &ran.rendered(0)[0] else {
        panic!("the merge request stage renders SQL");
    };
    assert!(
        sql.starts_with("SELECT mr.iid AS mr_iid, mr.title AS mr_title, mr.id AS mr_id FROM "),
        "a stage that feeds the Combine stage returns exactly its contract, text whole: {sql}"
    );
    assert!(sql.contains("startsWith(mr.traversal_path"), "{sql}");
    let call = ran
        .rendered(2)
        .into_iter()
        .find_map(|request| match request {
            Rendered::Live(call) => Some(call),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        call.workspaces,
        query_data_model::plan::live::Workspaces::Only(vec!["T01".into()])
    );
    assert_eq!(call.filters[0].op, "in");
    assert_eq!(call.filters[0].value, json!([2000, 2001]));
    assert_eq!(
        ops(&ran.plan(3, true)),
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
                json!({"mr_iid": 1, "mr_title": "Add feature A", "m_author": "ann", "m_text": "deploy to staging at 5pm", "_orbit_mr_id": 2000, "_orbit_mr_type": "MergeRequest", "_orbit_m_id": "m1", "_orbit_m_type": "SlackMessage", "_orbit_e0_kind": "DISCUSSES"})
            ),
            row(
                json!({"mr_iid": 2, "mr_title": "Fix bug B", "m_author": "ann", "m_text": "deploy memes", "_orbit_mr_id": 2001, "_orbit_mr_type": "MergeRequest", "_orbit_m_id": "m4", "_orbit_m_type": "SlackMessage", "_orbit_e0_kind": "DISCUSSES"})
            ),
        ]
    );
}

fn split(ran: &super::Run) -> Vec<query_engine::shared::dag::StageInfo> {
    ran.events
        .iter()
        .find_map(|event| match event {
            Event::Split { stages } => Some(stages.clone()),
            _ => None,
        })
        .unwrap()
}

fn code_request(
    ran: &super::Run,
    stage: usize,
) -> query_data_model::implementations::orbit_code::StructuralSearchRequest {
    ran.rendered(stage)
        .into_iter()
        .find_map(|request| match request {
            Rendered::Code(request) => Some(request),
            _ => None,
        })
        .unwrap()
}

/// The rows `BOUNDED` returns for a caller who may read project 42's code.
fn bounded_rows() -> [query_data_model::catalog::Row; 2] {
    [
        row(
            json!({"p_full_path": "gitlab-org/orbit", "f_path": "crates/orbit-server/src/query.rs", "match.line": 48, "_orbit_p_id": 42, "_orbit_p_type": "Project"}),
        ),
        row(
            json!({"p_full_path": "gitlab-org/orbit", "f_path": "crates/query-engine/compiler/src/lib.rs", "match.line": 120, "_orbit_p_id": 42, "_orbit_p_type": "Project"}),
        ),
    ]
}

/// The project stage reads the projects the files are joined to, under Project's guard and its
/// `read_code` check, so its keys are the files' scope keys: the graph runs one request, not a
/// second lookup of the same projects.
#[tokio::test]
async fn a_pattern_that_reads_the_scope_head_reads_it_once() {
    let graph = vec![row(json!({"p_full_path": "gitlab-org/orbit", "p_id": 42}))];
    let all = AllowedKeys::checks("project", [Key::Int(42), Key::Int(77)]);
    let (ran, requests) = counted(BOUNDED, &caller("1/9970/"), graph, &all).await;
    assert_eq!(
        requests,
        [("clickhouse", 1), ("live", 0), ("orbit_code", 1)].into()
    );
    let split = split(&ran);
    let backends: Vec<Backend> = split.iter().map(|stage| stage.backend).collect();
    assert_eq!(
        backends,
        [Backend::ClickHouse, Backend::OrbitCode, Backend::Combine]
    );
    let key = KeyInput {
        from: 0,
        positions: vec![1],
        binding: "f".into(),
        properties: super::properties("File", &["project_id"]),
        types: vec![query_data_model::DataType::Int],
    };
    assert_eq!(
        split[1].inputs,
        [Input::Keys(key.clone()), Input::Scope(key)]
    );
    let Rendered::Sql { sql, params, .. } = &ran.rendered(0)[0] else {
        panic!("the project stage renders SQL");
    };
    assert!(
        sql.starts_with("SELECT p.full_path AS p_full_path, p.id AS p_id FROM ")
            && sql
                .contains("FROM gl_project AS p WHERE (startsWith(p.traversal_path, {p0:String})"),
        "{sql}"
    );
    assert_eq!(params[0].1, json!("1/9970/"));
    let request = code_request(&ran, 1);
    assert_eq!(request.repositories, [42]);
    assert!(ran.plan(1, false)[0].contains("projects: [42]"));
    assert_eq!(ran.rows(), bounded_rows());
}

/// Reading the head in the pattern runs the same `read_code` check the lookup ran: a project
/// the caller's scope reaches but whose code it may not read never reaches the code stage.
#[tokio::test]
async fn a_project_the_pattern_reads_but_read_code_denies_never_reaches_the_code_stage() {
    let graph = vec![
        row(json!({"p_full_path": "gitlab-org/orbit", "p_id": 42})),
        row(json!({"p_full_path": "secret/vault", "p_id": 77})),
    ];
    let only = AllowedKeys::checks("project", [Key::Int(42)]);
    let (ran, requests) = counted(BOUNDED, &caller("1/"), graph, &only).await;
    assert_eq!(
        requests,
        [("clickhouse", 1), ("live", 0), ("orbit_code", 1)].into()
    );
    let handed: Vec<Vec<Key>> = ran
        .events
        .iter()
        .filter_map(|event| match event {
            Event::Keys {
                from: 0,
                to: 1,
                sample,
                ..
            } => Some(sample.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(handed, [vec![Key::Int(42)], vec![Key::Int(42)]]);
    assert_eq!(code_request(&ran, 1).repositories, [42]);
    assert_eq!(ran.rows(), bounded_rows());
}

const UNRELATED: &str = "MATCH (p:Project)<-[:IN_PROJECT]-(mr:MergeRequest {state: 'opened'})-[:SOURCE_HEAD]->(h:Head)-[:HEAD_FILE]->(f:File) WHERE f.content MATCHES ast('todo!($$$MSG)', 'rust') RETURN p.full_path, mr.iid, f.path ORDER BY mr.iid LIMIT 20";

/// The pattern reads a project, but through the merge request, not through the heads' or the
/// files' own steps, so its rows are not their scope keys and the lookup stage still runs.
#[tokio::test]
async fn a_head_read_only_by_an_unrelated_variable_keeps_its_lookup() {
    let mut graph = vec![row(json!({
        "p_full_path": "gitlab-org/orbit", "p_id": 42,
        "mr_iid": 1, "mr_id": 2000, "mr_project_id": 42, "mr_source_branch": "feature-a",
        "_orbit_e0_kind": "IN_PROJECT", "_orbit_e0_src": 2000, "_orbit_e0_src_kind": "MergeRequest",
        "_orbit_e0_dst": 42, "_orbit_e0_dst_kind": "Project",
    }))];
    graph.extend(projects(&[42]));
    let all = AllowedKeys::checks("project", [Key::Int(42), Key::Int(77)]);
    let (ran, requests) = counted(UNRELATED, &caller("1/9970/"), graph, &all).await;
    assert_eq!(
        requests,
        [("clickhouse", 2), ("live", 0), ("orbit_code", 1)].into()
    );
    let split = split(&ran);
    let backends: Vec<Backend> = split.iter().map(|stage| stage.backend).collect();
    assert_eq!(
        backends,
        [
            Backend::ClickHouse,
            Backend::ClickHouse,
            Backend::OrbitCode,
            Backend::Combine
        ]
    );
    assert!(split[1].logical[0].ends_with("-> [scope]"));
    let scopes: Vec<usize> = split[2]
        .inputs
        .iter()
        .filter_map(|input| match input {
            Input::Scope(scope) => Some(scope.from),
            _ => None,
        })
        .collect();
    assert_eq!(scopes, [1, 1]);
    assert_eq!(code_request(&ran, 2).repositories, [42]);
    let paths: Vec<_> = ran
        .rows()
        .into_iter()
        .map(|row| row["f_path"].clone())
        .collect();
    assert_eq!(paths, [json!("src/auth.rs")]);
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
            "planned 1",
            "emitted 1",
            "rendered 1",
            "rows 1",
            "finished 1",
            "started 2",
            "keys 0->2",
            "keys 1->2",
            "planned 2",
            "emitted 2",
            "rendered 2",
            "rows 2",
            "finished 2",
            "started 3",
            "planned 3",
            "emitted 3",
            "rendered 3",
            "rows 3",
            "finished 3",
        ]
    );
    let serialized = serde_json::to_value(&ran.events).unwrap();
    assert_eq!(serialized[0]["event"], "logical");
    assert_eq!(serialized[1]["stages"][2]["backend"], "live");
    assert_eq!(serialized[1]["stages"][2]["inputs"][1]["kind"], "scope");
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
            vec!["Entities", "Follow", "Select", "Limit"],
            vec!["Entities", "Select", "Limit"],
            vec![],
        ]
    );
    assert!(split[0].logical[0].ends_with("-> [mr]"));
    assert!(split[1].logical[0].ends_with("-> [scope]"));
    assert!(split[2].logical[0].ends_with("-> [m]"));
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
    assert!(ran.rendered(2).is_empty());
    assert!(ran.rows().is_empty());
}

#[tokio::test]
async fn an_empty_key_set_skips_the_stage_that_reads_it() {
    let ran = run(MENTIONS, &caller("1/9970/"), workspaces(&[("T01", 100)])).await;
    assert!(ran.rendered(2).is_empty());
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
                row(json!({"mr_iid": n, "mr_title": "t", "mr_id": id}))
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
    let mut merge_requests = vec![row(json!({"mr_iid": 1, "mr_title": title, "mr_id": 2000}))];
    merge_requests.extend(workspaces(&[("T01", 100)]));
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
    let mut incidents = vec![row(
        json!({"i_title": "staging outage", "i_message_id": "m1", "i_id": 7}),
    )];
    incidents.extend(workspaces(&[("T01", 100)]));
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
        [
            Backend::ClickHouse,
            Backend::ClickHouse,
            Backend::Live,
            Backend::Combine
        ]
    );
    assert_eq!(
        split[2].inputs,
        slack_inputs(vec![1], "id", query_data_model::DataType::String)
    );
    let call = ran
        .rendered(2)
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

const AUTHORED: &str = "MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {state: 'opened'})<-[:DISCUSSES]-(m:SlackMessage) RETURN u.username, mr.iid, m.text";

/// A stage that reads two nodes returns both inline, identity included, with the edge between
/// them: the author is read in the stage, not left to a hydration that never runs.
#[tokio::test]
async fn a_relational_stage_returns_every_node_it_reads_inline() {
    let authored = |username: &str, user: i64, iid: i64, id: i64| {
        row(json!({
            "u_username": username, "u_id": user, "mr_iid": iid, "mr_id": id,
            "_orbit_e0_kind": "AUTHORED", "_orbit_e0_src": user, "_orbit_e0_src_kind": "User",
            "_orbit_e0_dst": id, "_orbit_e0_dst_kind": "MergeRequest",
        }))
    };
    let mut rows = vec![authored("ann", 1, 1, 2000), authored("bob", 2, 2, 2001)];
    rows.extend(workspaces(&[("T01", 100)]));
    let ran = run(AUTHORED, &caller("1/9970/"), rows).await;
    let Rendered::Sql { sql, .. } = &ran.rendered(0)[0] else {
        panic!("the merge request stage renders SQL");
    };
    assert!(
        sql.contains(
            "SELECT u.username AS u_username, u.id AS u_id, mr.iid AS mr_iid, mr.id AS mr_id, "
        ),
        "{sql}"
    );
    assert!(
        sql.contains("INNER JOIN (SELECT * FROM (SELECT * FROM gl_user AS u"),
        "{sql}"
    );
    let mut redaction = crate::mock_redaction::MockRedactionService::new();
    redaction.allow("user", &[1, 2]);
    redaction.allow("merge_request", &[2000, 2001]);
    let outcome = ran.outcome.as_ref().expect("the run succeeded");
    assert_eq!(
        crate::dag_fakes::response_edges(outcome, &redaction),
        [
            "SlackMessage:m1 -DISCUSSES-> MergeRequest:2000",
            "SlackMessage:m4 -DISCUSSES-> MergeRequest:2001",
            "User:1 -AUTHORED-> MergeRequest:2000",
            "User:2 -AUTHORED-> MergeRequest:2001",
        ]
    );
    let joined = |username: &str, user: i64, iid: i64, id: i64, message: &str, text: &str| {
        row(json!({
            "u_username": username, "mr_iid": iid, "m_text": text,
            "_orbit_u_id": user, "_orbit_u_type": "User",
            "_orbit_mr_id": id, "_orbit_mr_type": "MergeRequest",
            "_orbit_m_id": message, "_orbit_m_type": "SlackMessage",
            "_orbit_e0_kind": "AUTHORED", "_orbit_e0_src": user, "_orbit_e0_src_kind": "User",
            "_orbit_e0_dst": id, "_orbit_e0_dst_kind": "MergeRequest",
            "_orbit_e1_kind": "DISCUSSES",
        }))
    };
    assert_eq!(
        ran.rows(),
        [
            joined("ann", 1, 1, 2000, "m1", "deploy to staging at 5pm"),
            joined("bob", 2, 2, 2001, "m4", "deploy memes"),
        ]
    );
}
