//! The demo end to end on the demo services (`scripts/services.sh`): query 4 through the
//! API, its event stream and rows per caller, the page's guard, and the seed reload.

mod support;

use std::net::SocketAddr;

use orbit_demo::lockdown::CSP;
use serde_json::{Value, json};
use support::{checked, exchange, get, post};

const DISCUSSED: &str = "MATCH (mr:MergeRequest {state: 'opened'})<-[:DISCUSSES]-(m:SlackMessage) RETURN mr.iid, m.author, m.sent_at ORDER BY mr.iid LIMIT 20";

const DEEP: &str = "MATCH (m:SlackMessage)-[:MENTIONS_ISSUE]->(i:JiraIssue)-[:DEVELOPED_IN]->(mr:MergeRequest)-[:SOURCE_HEAD]->(h:Head)-[:HEAD_FILE]->(f:File) WHERE i.status = 'In Review' AND f.content MATCHES ast('todo!($$$MSG)', 'rust') RETURN i.key, m.author, m.text, mr.iid, f.path, match.MSG LIMIT 20";

const QUERY_4: &str = "MATCH (mr:MergeRequest {state: 'opened'})-[:SOURCE_HEAD]->(h:Head)-[:HEAD_FILE]->(f:File) WHERE f.content MATCHES ast('todo!($$$MSG)', 'rust') RETURN mr.iid, h.branch, f.path, match.MSG ORDER BY mr.iid LIMIT 20";

async fn events(page: SocketAddr, caller: &str, query: &str) -> Vec<Value> {
    let host = format!("localhost:{}", page.port());
    let body = json!({"query": query, "caller": caller}).to_string();
    let started = exchange(
        page,
        &post("/api/runs", &host, "application/json", "", &body),
    )
    .await;
    assert_eq!(started.status, 200, "{}", started.body);
    let id = serde_json::from_str::<Value>(&started.body).unwrap()["run"].clone();
    let stream = exchange(page, &get(&format!("/api/runs/{id}/events"), &host, "")).await;
    assert_eq!(stream.header("content-type"), Some("text/event-stream"));
    stream
        .body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str(data).unwrap())
        .collect()
}

fn kinds(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .map(|event| match (&event["event"], &event["stage"]) {
            (Value::String(kind), Value::Number(stage)) => format!("{kind} {stage}"),
            (Value::String(kind), _) if kind == "keys" => {
                format!("keys {}->{}", event["from"], event["to"])
            }
            (Value::String(kind), _) => kind.clone(),
            _ => panic!("an event without a kind: {event}"),
        })
        .collect()
}

fn paths(events: &[Value]) -> Vec<String> {
    column(events, "f_path")
}

fn column(events: &[Value], name: &str) -> Vec<String> {
    let done = events
        .iter()
        .find(|event| event["event"] == "done")
        .expect("the run finished");
    let mut paths: Vec<String> = done["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row[name].as_str().unwrap().to_string())
        .collect();
    paths.sort();
    paths
}

#[tokio::test]
async fn the_demo_runs_query_4_and_guards_its_page() {
    let demo = orbit_demo::start(checked())
        .await
        .expect("run scripts/services.sh first");
    let page = demo.addresses.page;

    let ran = events(page, "maintainer", QUERY_4).await;
    let stage = |id: usize, inputs: &[&'static str], check: bool| {
        let mut kinds = vec![format!("started {id}")];
        kinds.extend(inputs.iter().map(|input| input.to_string()));
        kinds.extend([
            format!("planned {id}"),
            format!("emitted {id}"),
            format!("rendered {id}"),
        ]);
        if check {
            kinds.push("check".into());
        }
        kinds.extend([
            format!("rows {id}"),
            format!("samples {id}"),
            format!("finished {id}"),
        ]);
        kinds
    };
    let mut expected: Vec<String> = ["caller", "plans", "logical", "split"]
        .map(String::from)
        .to_vec();
    expected.extend(stage(0, &[], true));
    expected.extend(stage(1, &["keys 0->1"], true));
    expected.extend(stage(2, &["keys 0->2", "keys 1->2", "keys 1->2"], true));
    expected.extend(stage(3, &[], false));
    expected.push("done".into());
    assert_eq!(kinds(&ran), expected);

    let split = ran.iter().find(|event| event["event"] == "split").unwrap();
    let backends: Vec<&str> = split["stages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|stage| stage["backend"].as_str().unwrap())
        .collect();
    assert_eq!(
        backends,
        ["clickhouse", "clickhouse", "orbit_code", "combine"]
    );
    let heads = ran
        .iter()
        .find(|event| event["event"] == "keys" && event["from"] == 0 && event["to"] == 2)
        .unwrap();
    assert_eq!(
        heads["sample"],
        json!([
            [1000, "combine-cursor-paging"],
            [1000, "dag-slack-lake"],
            [1000, "lake-timestamp-utc"],
            [1001, "pin-clickhouse-digest"],
            [1001, "rustfs-dev-values"],
            [1002, "live-jira-descriptions"],
            [1002, "live-slack-text"]
        ])
    );
    let code = ran
        .iter()
        .filter(|event| event["event"] == "rendered" && event["stage"] == 2)
        .flat_map(|event| event["requests"].as_array().unwrap().clone())
        .find(|request| request["kind"] == "code")
        .unwrap();
    assert_eq!(code["repositories"], json!([1000, 1001, 1002]));
    assert_eq!(code["pattern"], "todo!($$$MSG)");
    let samples = ran
        .iter()
        .find(|event| event["event"] == "samples" && event["stage"] == 2)
        .unwrap();
    let requests: Vec<&str> = samples["samples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|sample| sample["request"].as_str().unwrap())
        .collect();
    assert_eq!(requests, ["structural search"]);
    assert_eq!(
        paths(&ran),
        [
            "bridge/src/lib.rs",
            "bridge/src/lib.rs",
            "crates/query-data-model/src/timestamp.rs",
            "crates/query-engine/shared/src/combine/cursor.rs",
            "crates/query-engine/shared/src/dag/slack.rs"
        ]
    );
    let des_poc = ["bridge/src/lib.rs", "bridge/src/lib.rs"];
    assert_eq!(paths(&events(page, "reporter", QUERY_4).await), des_poc);
    let zanzibar = events(page, "zanzibar", QUERY_4).await;
    assert_eq!(paths(&zanzibar), des_poc);
    let denied = zanzibar
        .iter()
        .find(|event| event["event"] == "check" && event["resource"] == "project")
        .unwrap();
    assert_eq!(
        (&denied["provider"], &denied["permission"]),
        (&json!("rails"), &json!("read_code"))
    );
    assert_eq!(
        (&denied["asked"], &denied["allowed"]),
        (&json!([1000, 1002]), &json!([1002]))
    );

    let plans = ran.iter().find(|event| event["event"] == "plans").unwrap();
    let logical: Vec<&str> = plans["logical"]
        .as_array()
        .unwrap()
        .iter()
        .map(|line| line.as_str().unwrap().trim())
        .collect();
    for line in [
        "Limit 20",
        "Filter mr.state = 'opened'",
        "Follow mr -[SOURCE_HEAD]-> h",
        "Follow h -[HEAD_FILE]-> f",
        "Entities mr: MergeRequest",
    ] {
        assert!(logical.contains(&line), "{line} in {logical:#?}");
    }
    let code_stage = plans["stages"]["2"].as_array().unwrap();
    assert!(
        code_stage
            .iter()
            .any(|line| line.as_str().unwrap().trim() == "Entities h: Head"),
        "{code_stage:?}"
    );

    let discussed = events(page, "maintainer", DISCUSSED).await;
    assert_eq!(column(&discussed, "m_author").len(), 11, "{discussed:?}");
    let sent = column(&events(page, "reporter", DISCUSSED).await, "m_sent_at");
    assert_eq!(
        sent,
        [
            "2026-11-05T10:00:00Z",
            "2026-11-06T13:15:00Z",
            "2026-11-10T14:20:00Z"
        ]
    );

    let deep = events(page, "maintainer", DEEP).await;
    let backends: Vec<&str> =
        deep.iter().find(|event| event["event"] == "split").unwrap()["stages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|stage| stage["backend"].as_str().unwrap())
            .collect();
    assert_eq!(
        backends,
        [
            "duckdb",
            "clickhouse",
            "duckdb",
            "live",
            "clickhouse",
            "orbit_code",
            "combine"
        ]
    );
    assert_eq!(
        column(&deep, "i_key"),
        [
            "DES-11", "DES-11", "DES-9", "DES-9", "GKG-212", "GKG-212", "GKG-219"
        ]
    );
    let texts = column(&deep, "m_text");
    assert!(
        texts
            .iter()
            .any(|text| text.starts_with("!412 reads Slack messages from the lake")),
        "{texts:?}"
    );
    let des_issues = ["DES-11", "DES-11", "DES-9", "DES-9"];
    assert_eq!(
        column(&events(page, "reporter", DEEP).await, "i_key"),
        des_issues
    );
    assert_eq!(
        column(&events(page, "zanzibar", DEEP).await, "i_key"),
        des_issues
    );

    let host = format!("127.0.0.1:{}", page.port());
    let index = exchange(page, &get("/", &host, "")).await;
    assert_eq!(index.status, 200);
    assert_eq!(index.header("content-security-policy"), Some(CSP));
    assert!(index.body.contains("Execution plan") && index.body.contains("Combine (in process)"));
    let rebound = exchange(
        page,
        &get(
            "/api/meta",
            &format!("attacker.example:{}", page.port()),
            "",
        ),
    )
    .await;
    assert_eq!(rebound.status, 403);

    let refused = exchange(page, &post("/api/reload", &host, "text/plain", "", "{}")).await;
    assert_eq!(refused.status, 403);
    let reloaded = exchange(
        page,
        &post("/api/reload", &host, "application/json", "", "{}"),
    )
    .await;
    assert_eq!(reloaded.status, 200, "{}", reloaded.body);
    assert!(
        serde_json::from_str::<Value>(&reloaded.body).unwrap()["rows"]
            .as_u64()
            .unwrap()
            > 50
    );
}
