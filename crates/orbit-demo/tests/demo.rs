//! The demo end to end on the demo ClickHouse (`scripts/clickhouse.sh`): query 4 through the
//! API, its event stream and rows per caller, the page's guard, and the seed reload.

mod support;

use std::net::SocketAddr;

use orbit_demo::lockdown::CSP;
use serde_json::{Value, json};
use support::{checked, exchange, get, post};

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
    let done = events
        .iter()
        .find(|event| event["event"] == "done")
        .expect("the run finished");
    let mut paths: Vec<String> = done["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["f_path"].as_str().unwrap().to_string())
        .collect();
    paths.sort();
    paths
}

#[tokio::test]
async fn the_demo_runs_query_4_and_guards_its_page() {
    let demo = orbit_demo::start(checked())
        .await
        .expect("run scripts/clickhouse.sh first");
    let page = demo.addresses.page;

    let ran = events(page, "maintainer", QUERY_4).await;
    assert_eq!(
        kinds(&ran),
        [
            "caller",
            "stages",
            "logical",
            "split",
            "started 0",
            "planned 0",
            "emitted 0",
            "rendered 0",
            "rows 0",
            "samples 0",
            "finished 0",
            "started 1",
            "keys 0->1",
            "rendered 1",
            "planned 1",
            "emitted 1",
            "rendered 1",
            "rows 1",
            "samples 1",
            "finished 1",
            "started 2",
            "planned 2",
            "emitted 2",
            "rendered 2",
            "rows 2",
            "samples 2",
            "finished 2",
            "done",
        ]
    );
    let split = ran.iter().find(|event| event["event"] == "split").unwrap();
    let backends: Vec<&str> = split["stages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|stage| stage["backend"].as_str().unwrap())
        .collect();
    assert_eq!(backends, ["clickhouse", "orbit_code", "engine"]);
    let keys = ran.iter().find(|event| event["event"] == "keys").unwrap();
    assert_eq!(
        keys["sample"],
        json!([
            [1000, "feature-auth"],
            [1000, "fix-rate"],
            [1001, "dark-mode"]
        ])
    );
    let code = ran
        .iter()
        .filter(|event| event["event"] == "rendered" && event["stage"] == 1)
        .flat_map(|event| event["requests"].as_array().unwrap().clone())
        .find(|request| request["kind"] == "code")
        .unwrap();
    assert_eq!(code["repositories"], json!([1000, 1001]));
    assert_eq!(code["pattern"], "todo!($$$MSG)");
    let samples = ran
        .iter()
        .find(|event| event["event"] == "samples" && event["stage"] == 1)
        .unwrap();
    let requests: Vec<&str> = samples["samples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|sample| sample["request"].as_str().unwrap())
        .collect();
    assert_eq!(requests, ["scope lookup", "structural search"]);
    assert_eq!(
        paths(&ran),
        ["src/auth.rs", "src/limiter.rs", "web/theme.rs"]
    );
    assert_eq!(
        paths(&events(page, "guest", QUERY_4).await),
        ["web/theme.rs"]
    );

    let host = format!("127.0.0.1:{}", page.port());
    let index = exchange(page, &get("/", &host, "")).await;
    assert_eq!(index.status, 200);
    assert_eq!(index.header("content-security-policy"), Some(CSP));
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
