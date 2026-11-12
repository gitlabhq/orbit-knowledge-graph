use tokio::io::{AsyncReadExt, AsyncWriteExt};

use std::collections::BTreeMap;

use super::*;

const CHANNELS: &str = r#"{"ok": true, "channels": [
  {"id": "C1", "name": "team", "topic": {"value": "team chat"}},
  {"id": "C2", "name": "deploys", "topic": {"value": "deploy alerts"}}]}"#;
const USERS: &str =
    r#"{"ok": true, "members": [{"id": "U1", "name": "ada"}, {"id": "U2", "name": "bot"}]}"#;
const SEARCH: &str = r#"{"ok": true, "results": {"messages": [
  {"channel_id": "C1", "channel_name": "team", "message_ts": "1712345678.000100", "author_user_id": "U1", "author_name": "Ada", "content": "web rollout stuck on the db migration"},
  {"channel_id": "C7", "channel_name": "elsewhere", "message_ts": "1712000001.000100", "author_user_id": "U9", "author_name": "Carol", "content": "db migration in a channel nobody synced https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/42"}]}}"#;
const FOLDERS: &str = r#"[{"uid": "f-web", "title": "Web"}, {"uid": "f-db", "title": "Database"}]"#;
const DASHBOARDS: &str = r#"[
  {"uid": "d-web", "title": "Web overview", "tags": ["web", "slo"], "folderUid": "f-web", "folderTitle": "Web"},
  {"uid": "d-db", "title": "Postgres replication", "tags": ["database"], "folderUid": "f-db", "folderTitle": "Database"}]"#;
const ALERTS: &str = r#"[
  {"uid": "a-web-errors", "title": "Web error rate high", "ruleGroup": "web", "folderUID": "f-web",
   "updated": "2024-04-05T19:40:00Z", "annotations": {"summary": "web-prod 5xx above 1%", "__dashboardUid__": "d-web"}},
  {"uid": "a-web-latency", "title": "Web latency high", "ruleGroup": "web", "folderUID": "f-web",
   "updated": "2024-04-05T18:00:00Z", "annotations": {"__dashboardUid__": "d-web"}},
  {"uid": "a-db-lag", "title": "Replication lag", "ruleGroup": "db", "folderUID": "f-db",
   "updated": "2024-04-04T10:00:00Z", "annotations": {"description": "db migration is lagging replicas"}}]"#;

fn reply(request: &str) -> String {
    let target = request.split_whitespace().nth(1).unwrap_or_default();
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let query = query
        .replace('+', " ")
        .replace("%3A", ":")
        .replace("%3C", "<")
        .replace("%3E", ">")
        .replace("%23", "#")
        .replace("%40", "@");
    let has = |text: &str| query.contains(text);
    let grid = request.contains("Bearer xoxe-grid");
    let json = |body: &str| {
        format!(
            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
    };
    if grid && !has("team_id=T1") {
        return json(r#"{"ok": false, "error": "missing_argument"}"#);
    }
    let body = match path {
        "/slack/api/users.conversations" => CHANNELS.to_string(),
        "/slack/api/users.list" => USERS.to_string(),
        "/slack/api/conversations.info" if has("channel=C1") => r#"{"ok": true, "channel": {"id": "C1", "name": "team", "topic": {"value": "team chat"}}}"#.to_string(),
        "/slack/api/conversations.info" => r#"{"ok": false, "error": "channel_not_found"}"#.to_string(),
        "/slack/api/conversations.replies" if has("channel=C7") && has("ts=1712000001.000100") => {
            r#"{"ok": true, "messages": [{"ts": "1712000001.000100", "user": "U9", "text": "db migration in a channel nobody synced", "reply_count": 2}]}"#.to_string()
        }
        "/slack/api/conversations.replies" => r#"{"ok": false, "error": "thread_not_found"}"#.to_string(),
        "/slack/api/assistant.search.context" => {
            let all: serde_json::Value = serde_json::from_str(SEARCH).unwrap();
            let kept: Vec<_> = all["results"]["messages"].as_array().unwrap().iter().filter(|hit| {
                (!has("in:") || has(&format!("in:<#{}>", hit["channel_id"].as_str().unwrap())) || has(&format!("in:#{}", hit["channel_name"].as_str().unwrap())))
                    && (!has("from:") || has(&format!("from:@{}", hit["author_name"].as_str().unwrap().to_lowercase())))
            }).cloned().collect();
            serde_json::json!({"ok": true, "results": {"messages": kept}}).to_string()
        }
        "/sso/api/folders" => return "HTTP/1.1 302 Found\r\nlocation: https://accounts.google.com/o/oauth2/auth?client_id=x\r\nconnection: close\r\ncontent-length: 0\r\n\r\n".into(),
        "/grafana/api/folders" => FOLDERS.to_string(),
        "/grafana/api/folders/f-web" => r#"{"uid": "f-web", "title": "Web"}"#.to_string(),
        "/grafana/api/search" => {
            let all: Vec<serde_json::Value> = serde_json::from_str(DASHBOARDS).unwrap();
            let text = query.split('&').find_map(|pair| pair.strip_prefix("query=")).unwrap_or_default().to_lowercase();
            let kept: Vec<_> = all.into_iter().filter(|dashboard| {
                dashboard["title"].as_str().unwrap().to_lowercase().contains(&text)
                    && (!has("folderUIDs=") || has(&format!("folderUIDs={}", dashboard["folderUid"].as_str().unwrap())))
            }).collect();
            serde_json::to_string(&kept).unwrap()
        }
        "/grafana/api/v1/provisioning/alert-rules" => ALERTS.to_string(),
        _ => return "HTTP/1.1 404 Not Found\r\nconnection: close\r\ncontent-length: 2\r\n\r\n{}".into(),
    };
    json(&body)
}

async fn serve() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buffer = vec![0; 8192];
                let read = stream.read(&mut buffer).await.unwrap_or(0);
                let response = reply(&String::from_utf8_lossy(&buffer[..read]));
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    format!("http://{address}")
}

fn open(base: &str, vars: &str) -> Option<ThirdParty> {
    let vars: BTreeMap<&str, String> = vars
        .split_whitespace()
        .filter_map(|pair| pair.split_once('='))
        .map(|(name, value)| (name, value.replace("{base}", base)))
        .collect();
    ThirdParty::new(Config::from_env(|name| vars.get(name).cloned()))
}

async fn connected() -> ThirdParty {
    let vars = "SLACK_TOKEN=xoxb-good SLACK_URL={base}/slack GRAFANA_TOKEN=glsa_good GRAFANA_URL={base}/grafana";
    open(&serve().await, vars).unwrap()
}

async fn grep(tp: &ThirdParty, case: &str) -> Result<String, String> {
    let mut parts = case.split(" | ");
    let (scope, rest) = tp.scope(parts.next().unwrap());
    let mut scope = scope.unwrap_or_default();
    let within = parts.next().unwrap_or_default().split_whitespace();
    scope.within = within.map(|at| tp.parse_target(at).unwrap()).collect();
    let grepped = tp.grep(&rest, &scope).await;
    let hits = grepped.found.into_iter().flat_map(|found| found.hits);
    let hits: Vec<String> = hits.map(|hit| hit.reference).collect();
    match grepped.failed.is_empty() {
        true => Ok(hits.join(" ")),
        false => Err(format!("{:?}", grepped.failed)),
    }
}

const GREPS: &[&str] = &[
    "slack:channel:deploy |  | slack:channel:C2",
    "slack:channel:nothing-matches |  | ",
    "grafana:alert:web |  | grafana:alert:a-web-errors grafana:alert:a-web-latency",
    "slack:message:migration |  | slack:message:C1:1712345678.000100 slack:message:C7:1712000001.000100",
    "slack:message:migration | slack:channel:#team | slack:message:C1:1712345678.000100",
    "slack:message:migration | slack:channel:C1 | slack:message:C1:1712345678.000100",
    "slack:message:migration | slack:user:carol | slack:message:C7:1712000001.000100",
    "grafana:dashboard:o | grafana:folder:Web | grafana:dashboard:d-web",
    "grafana:alert:. | grafana:dashboard:d-web | grafana:alert:a-web-errors grafana:alert:a-web-latency",
];

const GREP_ERRORS: &[&str] = &[
    "web | grafana:folder:NoSuchFolder | has nothing at",
    "grafana:dashboard:web | grafana:dashboard:d-db | can be narrowed",
    "slack:message:x | slack:channel:C404 | has nothing at",
    "slack:message:migration | grafana:folder:f-web | can be narrowed",
];

#[tokio::test]
async fn grep_scopes_by_source_and_type_and_narrows_by_reference() {
    let tp = connected().await;
    let picks = tp.scope("slack:channel:x").0.unwrap().picks;
    assert_eq!(picks, [("slack".into(), "SlackChannel".into())]);
    for code in [
        "slack:deploy",
        "grafana:10.2",
        "image: grafana:10.2",
        "slack:users",
        "Slack:Channel:x",
        "slack:channel",
    ] {
        assert_eq!(tp.scope(code), (None, code.to_string()), "{code}");
    }
    for case in GREPS {
        let expected = case.rsplit(" | ").next().unwrap();
        assert_eq!(grep(&tp, case).await.unwrap(), expected, "{case}");
    }
    for case in GREP_ERRORS {
        let error = grep(&tp, case).await.unwrap_err();
        assert!(
            error.contains(case.rsplit(" | ").next().unwrap()),
            "{case}: {error}"
        );
    }
    let (scope, rest) = tp.scope("slack:message:migration");
    let text = render(&tp.grep(&rest, &scope.unwrap()).await.found);
    assert!(text.starts_with("» slack\nslack:message:C1:1712345678.000100  team · Ada · 2024-04-05 19:34\n    web rollout stuck on the db migration\n"), "{text}");
}

#[tokio::test]
async fn context_fetches_one_entity_live_with_links() {
    let tp = connected().await;
    let context = async |at: &str| tp.context(&tp.parse_target(at).unwrap()).await;
    for asked in [
        "slack:channel:C1",
        "slack:channel:#team",
        "slack:channel:TEAM",
    ] {
        let text = context(asked).await.unwrap();
        assert!(
            text.starts_with("slack:channel:C1\nname: team\n"),
            "{asked}: {text}"
        );
        assert!(
            text.contains(
                "<-[IN_CHANNEL]- message (latest 1)\n  slack:message:C1:1712345678.000100"
            ),
            "{text}"
        );
    }
    let text = context("slack:message:C7:1712000001.000100").await.unwrap();
    assert!(
        text.contains("text: db migration in a channel nobody synced\n"),
        "{text}"
    );
    assert!(text.contains("-[IN_CHANNEL]-> slack:channel:C7"), "{text}");
    let text = context("grafana:folder:f-web").await.unwrap();
    assert!(
        text.contains("dashboard (latest 1)") && text.contains("alert (latest 2)"),
        "{text}"
    );
    assert!(context("slack:message:C7:1").await.is_err());
    let error = context("slack:channel:nope").await.unwrap_err().to_string();
    assert!(
        error.contains("has nothing at slack:channel:nope"),
        "{error}"
    );
}

const QUERIES: &[&str] = &[
    "MATCH (a:GrafanaAlert)-[:IN_FOLDER]->(f:GrafanaFolder) WHERE f.title = 'Web' RETURN a.title ORDER BY a.title | Web error rate high, Web latency high",
    "MATCH (m:SlackMessage)-[:IN_CHANNEL]->(c:SlackChannel) WHERE m.text CONTAINS 'migration' RETURN c.name | team",
    "MATCH (m:SlackMessage)-[:IN_CHANNEL]->(c:SlackChannel {name: 'team'}) RETURN m.text | web rollout stuck on the db migration",
    "MATCH (c:SlackChannel) RETURN c.name ORDER BY c.name LIMIT 1 | deploys",
    "MATCH (m:SlackMessage) WHERE m.text CONTAINS 'migration' AND m.author_name = 'Carol' RETURN m.text SKIP 0 LIMIT 1 | db migration in a channel nobody synced https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/42",
];

const QUERY_ERRORS: &[&str] = &[
    "MATCH (m:SlackMessage) RETURN m.text | needs a text filter",
    "MATCH (m:SlackMessage) WHERE m.text <> 'done' RETURN m.text | uses ne",
    "MATCH (m:SlackMessage)-[:IN_CHANNEL]->(c:SlackChannel) WHERE c.name STARTS WITH 'tea' RETURN m.text | uses starts_with",
    "MATCH (a:SlackMessage)-[:IN_CHANNEL]->(c:SlackChannel)<-[:IN_CHANNEL]-(b:SlackMessage) WHERE a.text CONTAINS 'web' AND b.text CONTAINS 'migration' RETURN a.text | only once",
];

#[tokio::test]
async fn query_reads_what_it_names_in_full_or_says_why_not() {
    let tp = connected().await;
    for case in QUERIES {
        let (query, expected) = case.split_once(" | ").unwrap();
        let batches = tp.query(query, None).await.unwrap();
        assert_eq!(batches[0].schema().fields().len(), 1, "{query}");
        assert_eq!(strings(&batches).join(", "), expected, "{query}");
    }
    for case in QUERY_ERRORS {
        let (query, expected) = case.split_once(" | ").unwrap();
        let error = tp.query(query, None).await.unwrap_err().to_string();
        assert!(error.contains(expected), "{query}: {error}");
    }
    let labels = tp.labels_in("MATCH (c:SlackChannel), (p:Project) RETURN c");
    assert_eq!(
        labels,
        (
            vec!["SlackChannel".to_string()],
            vec!["Project".to_string()]
        )
    );
}

fn strings(batches: &[RecordBatch]) -> Vec<String> {
    use arrow::array::{Array, StringArray};
    let columns = batches.iter().map(|batch| batch.column(0));
    let arrays =
        columns.filter_map(|column| column.as_any().downcast_ref::<StringArray>().cloned());
    arrays
        .flat_map(|array| (0..array.len()).map(move |i| array.value(i).to_string()))
        .collect()
}

#[tokio::test]
async fn sources_come_from_env_and_failures_say_why() {
    let base = serve().await;
    assert!(open(&base, "SLACK_URL=x").is_none());
    let pick = Scope {
        picks: vec![("slack".into(), "SlackChannel".into())],
        within: Vec::new(),
    };
    let grid = |team: &str| {
        let vars = format!(
            "SLACK_TOKEN=xoxe-grid SLACK_URL={{base}}/slack SLACK_TEAM={team} GRAFANA_TOKEN=g GRAFANA_URL={{base}}/sso"
        );
        open(&base, &vars).unwrap()
    };
    let failed = format!("{:?}", grid("").grep(".", &pick).await.failed);
    assert!(failed.contains("missing_argument"), "{failed}");
    assert!(grid("T1").grep(".", &pick).await.failed.is_empty());
    let failed = format!("{:?}", grid("T1").grep(".", &Scope::default()).await.failed);
    assert!(
        failed.contains("/api/folders redirected") && !failed.contains("client_id"),
        "{failed}"
    );
    let no_url = open(&base, "GRAFANA_TOKEN=g").unwrap();
    let failed = format!("{:?}", no_url.grep(".", &Scope::default()).await.failed);
    assert!(failed.contains("grafana needs GRAFANA_URL"), "{failed}");
}

struct FakeGitLab;

#[async_trait::async_trait]
impl GitLab for FakeGitLab {
    async fn fetch(
        &self,
        label: &str,
        _: &std::collections::HashMap<String, Vec<InputFilter>>,
        mentioned: &[String],
    ) -> Result<Vec<(String, schema::Row)>> {
        assert_eq!(
            (label, mentioned),
            (
                "MergeRequest",
                &["gitlab-org/orbit/knowledge-graph#42".to_string()][..]
            )
        );
        let values = BTreeMap::from([
            ("iid".into(), 42.into()),
            ("title".into(), "Fix the migration".into()),
        ]);
        Ok(vec![(
            mentioned[0].clone(),
            schema::Row { id: 9001, values },
        )])
    }
}

#[tokio::test]
async fn mixed_queries_join_slack_messages_to_the_merge_requests_they_link() {
    let tp = connected().await;
    let query = "MATCH (m:SlackMessage)-[:MENTIONS]->(mr:MergeRequest) WHERE m.text CONTAINS 'migration' RETURN mr.title";
    let batches = tp.query(query, Some(&FakeGitLab)).await.unwrap();
    assert_eq!(strings(&batches), ["Fix the migration"]);
    let error = tp.query(query, None).await.unwrap_err().to_string();
    assert!(error.contains("needs a GitLab sign-in"), "{error}");
    assert_eq!(
        mentions(
            "see https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/42 and https://gitlab.com/g/p/-/issues/7"
        ),
        [
            (
                "MergeRequest",
                "gitlab-org/orbit/knowledge-graph".into(),
                42
            ),
            ("WorkItem", "g/p".into(), 7)
        ]
    );
}
