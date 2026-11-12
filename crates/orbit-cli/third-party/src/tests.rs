use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

const CHANNELS: &str = r#"{"ok": true, "channels": [
  {"id": "C1", "name": "team", "topic": {"value": "team chat"}},
  {"id": "C2", "name": "deploys", "topic": {"value": "deploy alerts"}}]}"#;
const USERS: &str =
    r#"{"ok": true, "members": [{"id": "U1", "name": "ada"}, {"id": "U2", "name": "bot"}]}"#;
const SEARCH: &str = r#"{"ok": true, "results": {"messages": [
  {"channel_id": "C1", "channel_name": "team", "message_ts": "1712345678.000100", "author_user_id": "U1", "author_name": "Ada", "content": "web rollout stuck on the db migration"},
  {"channel_id": "C7", "channel_name": "elsewhere", "message_ts": "1712000001.000100", "author_user_id": "U9", "author_name": "Carol", "content": "db migration in a channel nobody synced"}]}}"#;
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
    let bearer = |token: &str| request.contains(&format!("Bearer {token}"));
    let json = |body: &str| {
        format!(
            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
    };
    if bearer("xoxe-grid")
        && !path.contains("auth.")
        && !has("team_id=T1")
        && !path.ends_with("history")
    {
        return json(r#"{"ok": false, "error": "missing_argument"}"#);
    }
    let body = match path {
        "/slack/api/auth.test" if bearer("xoxb-good") => r#"{"ok": true, "team": "Orbit"}"#.to_string(),
        "/slack/api/auth.test" if bearer("xoxe-grid") => r#"{"ok": true, "team": "Org", "is_enterprise_install": true}"#.to_string(),
        "/slack/api/auth.teams.list" => r#"{"ok": true, "teams": [{"id": "T1", "name": "One"}, {"id": "T2", "name": "Two"}]}"#.to_string(),
        "/slack/api/auth.test" => r#"{"ok": false, "error": "invalid_auth"}"#.to_string(),
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
        "/grafana/api/user" if bearer("glsa_good") => r#"{"login": "ada"}"#.to_string(),
        "/grafana/api/user" => return "HTTP/1.1 401 Unauthorized\r\nconnection: close\r\ncontent-length: 2\r\n\r\n{}".into(),
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

fn open(root: &std::path::Path, sources: &str) -> ThirdParty {
    std::fs::write(root.join(CONFIG_FILE), format!("sources:\n{sources}")).unwrap();
    ThirdParty::open(root).unwrap().unwrap()
}

fn both(base: &str) -> String {
    format!(
        "  - {{name: slack, definition: slack, url: {base}/slack, auth: {{strategy: bearer, token_env: ORBIT_TEST_TOKEN_GOOD}}}}\n  - {{name: grafana, definition: grafana, url: {base}/grafana, auth: {{strategy: bearer, token_env: ORBIT_TEST_TOKEN_GOOD}}}}\n"
    )
}

async fn connected() -> (tempfile::TempDir, ThirdParty) {
    let root = tempfile::tempdir().unwrap();
    let third_party = open(root.path(), &both(&serve().await));
    (root, third_party)
}

async fn grep(third_party: &ThirdParty, query: &str, within: &[&str]) -> Vec<String> {
    let (scope, rest) = third_party.scope(query);
    let mut scope = scope.unwrap_or_default();
    scope.within = within
        .iter()
        .map(|reference| third_party.parse_target(reference).unwrap())
        .collect();
    let grepped = third_party.grep(&[rest], &scope).await;
    assert!(grepped.failed.is_empty(), "{:?}", grepped.failed);
    grepped
        .found
        .into_iter()
        .flat_map(|entity| entity.hits)
        .map(|hit| hit.reference)
        .collect()
}

#[tokio::test]
async fn grep_scopes_by_source_and_type_and_reads_each_source_live() {
    let (root, third_party) = connected().await;
    let (scope, rest) = third_party.scope("slack:channel:nothing-matches");
    assert_eq!(
        scope.unwrap().picks,
        [("slack".into(), "SlackChannel".into())]
    );
    assert_eq!(rest, "nothing-matches");
    for code in [
        "slack:deploy",
        "slack: deploy",
        "grafana:10.2",
        "image: grafana:10.2",
        "mr:deploy",
        "slack:users",
        "Slack:Channel:deploy",
        "grafana:alerts",
    ] {
        assert_eq!(third_party.scope(code), (None, code.to_string()), "{code}");
    }
    assert_eq!(
        grep(&third_party, "slack:channel:deploy", &[]).await,
        ["slack:channel:C2"]
    );
    assert_eq!(
        grep(&third_party, "grafana:alert:web", &[]).await,
        ["grafana:alert:a-web-errors", "grafana:alert:a-web-latency"]
    );
    assert!(
        grep(&third_party, "slack:channel:nothing-matches", &[])
            .await
            .is_empty()
    );
    let written: Vec<_> = std::fs::read_dir(root.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(written, [std::ffi::OsString::from(CONFIG_FILE)]);
}

#[tokio::test]
async fn grep_narrows_a_search_by_reference() {
    let (_root, third_party) = connected().await;
    let found = [
        "slack:message:C1:1712345678.000100",
        "slack:message:C7:1712000001.000100",
    ];
    assert_eq!(
        grep(&third_party, "slack:message:migration", &[]).await,
        found
    );
    assert_eq!(
        grep(
            &third_party,
            "slack:message:migration",
            &["slack:channel:#team"]
        )
        .await,
        found[..1]
    );
    assert_eq!(
        grep(
            &third_party,
            "slack:message:migration",
            &["slack:user:carol"]
        )
        .await,
        found[1..]
    );
    assert_eq!(
        grep(&third_party, "grafana:dashboard:postgres", &[]).await,
        ["grafana:dashboard:d-db"]
    );
    assert_eq!(
        grep(&third_party, "grafana:dashboard:o", &["grafana:folder:Web"]).await,
        ["grafana:dashboard:d-web"]
    );
    assert_eq!(
        grep(
            &third_party,
            "grafana:alert:.",
            &["grafana:dashboard:d-web"]
        )
        .await,
        ["grafana:alert:a-web-errors", "grafana:alert:a-web-latency"]
    );
    assert_eq!(
        grep(
            &third_party,
            "slack:message:migration",
            &["slack:channel:C1"]
        )
        .await,
        found[..1]
    );
    for (query, within, expected) in [
        ("web", "grafana:folder:NoSuchFolder", "has nothing at"),
        (
            "grafana:dashboard:web",
            "grafana:dashboard:d-db",
            "can be narrowed",
        ),
        ("slack:message:x", "slack:channel:C404", "has nothing at"),
        (
            "slack:message:migration",
            "grafana:folder:f-web",
            "one grep per source",
        ),
    ] {
        let (scope, rest) = third_party.scope(query);
        let mut scope = scope.unwrap_or_default();
        scope.within = vec![third_party.parse_target(within).unwrap()];
        let grepped = third_party.grep(&[rest], &scope).await;
        let failed = format!("{:?}", grepped.failed);
        assert!(
            grepped.found.is_empty() && failed.contains(expected),
            "{within}: {failed}"
        );
    }
}

#[tokio::test]
async fn render_shows_where_who_when_and_the_total() {
    let (_root, third_party) = connected().await;
    let (scope, rest) = third_party.scope("slack:message:migration");
    let grepped = third_party.grep(&[rest], &scope.unwrap()).await;
    let text = render(&grepped.found);
    assert!(
        text.starts_with("» slack\nslack:message:C1:1712345678.000100  team · Ada · 2024-04-05 19:34\n    web rollout stuck on the db migration\n"),
        "{text}"
    );
    let mut found = Found {
        source: "grafana".into(),
        entity: "GrafanaFolder".into(),
        hits: vec![Hit {
            source: "grafana".into(),
            entity: "GrafanaFolder".into(),
            reference: "grafana:folder:web".into(),
            label: "web".into(),
            shown: Vec::new(),
            matched: Vec::new(),
        }],
        total: 3,
    };
    assert_eq!(
        render(std::slice::from_ref(&found)),
        "» grafana\ngrafana:folder:web\n  1 of 3 folders shown; add words to narrow it\n"
    );
    found.total = GREP_LIMIT + 1;
    assert!(render(std::slice::from_ref(&found)).contains("1 of more than 20 folders shown"));
    found.total = 1;
    found.hits[0].label = "Web".into();
    assert_eq!(render(&[found]), "» grafana\ngrafana:folder:web  Web\n");
}

#[tokio::test]
async fn context_fetches_one_entity_live_by_key_or_name_with_links() {
    let (_root, third_party) = connected().await;
    let context = |reference: &str| {
        let target = third_party.parse_target(reference).unwrap();
        let third_party = &third_party;
        async move { third_party.context(&target).await }
    };
    for asked in [
        "slack:channel:C1",
        "slack:channel:#team",
        "slack:channel:TEAM",
    ] {
        let text = context(asked).await.unwrap();
        assert!(
            text.starts_with("slack:channel:C1  team\n"),
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
        text.starts_with(
            "slack:message:C7:1712000001.000100  db migration in a channel nobody synced\n"
        ),
        "{text}"
    );
    assert!(text.contains("-[IN_CHANNEL]-> slack:channel:C7"), "{text}");
    let text = context("grafana:folder:f-web").await.unwrap();
    assert!(
        text.contains("<-[IN_FOLDER]- dashboard (latest 1)"),
        "{text}"
    );
    assert!(text.contains("<-[IN_FOLDER]- alert (latest 2)"), "{text}");
    assert!(context("slack:message:C7:1").await.is_err());
    assert!(
        context("slack:channel:nope")
            .await
            .unwrap_err()
            .to_string()
            .contains("has nothing at slack:channel:nope")
    );
}

#[tokio::test]
async fn query_fetches_only_what_it_names_and_needs_a_filter_for_searched_entities() {
    let (_root, third_party) = connected().await;
    let batches = third_party
        .query("MATCH (a:GrafanaAlert)-[:IN_FOLDER]->(f:GrafanaFolder) WHERE f.title = 'Web' RETURN a.title")
        .await
        .unwrap();
    let mut titles = strings(&batches, "a_title");
    titles.sort();
    assert_eq!(titles, ["Web error rate high", "Web latency high"]);
    assert_eq!(batches[0].schema().fields().len(), 1);

    let batches = third_party
        .query("MATCH (m:SlackMessage)-[:IN_CHANNEL]->(c:SlackChannel) WHERE m.text CONTAINS 'migration' RETURN c.name")
        .await
        .unwrap();
    assert_eq!(strings(&batches, "c_name"), ["team"]);
    let batches = third_party
        .query(
            "MATCH (m:SlackMessage)-[:IN_CHANNEL]->(c:SlackChannel {name: 'team'}) RETURN m.text",
        )
        .await
        .unwrap();
    assert_eq!(
        strings(&batches, "m_text"),
        ["web rollout stuck on the db migration"]
    );

    let batches = third_party
        .query("MATCH (c:SlackChannel) RETURN c.name ORDER BY c.name LIMIT 1")
        .await
        .unwrap();
    assert_eq!(strings(&batches, "c_name"), ["deploys"]);
    let batches = third_party
        .query("MATCH (m:SlackMessage) WHERE m.text CONTAINS 'migration' AND m.author_name = 'Carol' RETURN m.text SKIP 0 LIMIT 1")
        .await
        .unwrap();
    assert_eq!(
        strings(&batches, "m_text"),
        ["db migration in a channel nobody synced"]
    );
    for (query, expected) in [
        (
            "MATCH (m:SlackMessage) RETURN m.text",
            "needs a text filter",
        ),
        (
            "MATCH (m:SlackMessage) WHERE m.text <> 'done' RETURN m.text",
            "uses ne",
        ),
        (
            "MATCH (m:SlackMessage)-[:IN_CHANNEL]->(c:SlackChannel) WHERE c.name STARTS WITH 'tea' RETURN m.text",
            "uses starts_with",
        ),
        (
            "MATCH (a:SlackMessage)-[:IN_CHANNEL]->(c:SlackChannel)<-[:IN_CHANNEL]-(b:SlackMessage) WHERE a.text CONTAINS 'web' AND b.text CONTAINS 'migration' RETURN a.text",
            "only once",
        ),
    ] {
        let error = third_party.query(query).await.unwrap_err().to_string();
        assert!(error.contains(expected), "{query}: {error}");
    }
    let (ours, theirs) = third_party.labels_in("MATCH (c:SlackChannel), (p:Project) RETURN c");
    assert_eq!(
        (ours, theirs),
        (
            vec!["SlackChannel".to_string()],
            vec!["Project".to_string()]
        )
    );
}

fn strings(batches: &[RecordBatch], name: &str) -> Vec<String> {
    use arrow::array::{Array, StringArray};
    batches
        .iter()
        .filter_map(|batch| batch.column_by_name(name))
        .filter_map(|column| column.as_any().downcast_ref::<StringArray>().cloned())
        .flat_map(|array| {
            (0..array.len())
                .filter(|i| !array.is_null(*i))
                .map(|i| array.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn verify_checks_tokens_and_lists_grid_workspaces() {
    let base = serve().await;
    let slack = setup::Connectable::named("slack").unwrap();
    let url = format!("{base}/slack");
    assert_eq!(
        slack.verify(&url, "xoxb-good").await.unwrap().shows,
        "Orbit"
    );
    assert_eq!(
        slack.verify(&url, "xoxe-grid").await.unwrap().teams.len(),
        2
    );
    assert!(
        slack
            .verify(&url, "xoxb-bad")
            .await
            .unwrap_err()
            .to_string()
            .contains("invalid_auth")
    );
    let grafana = setup::Connectable::named("grafana").unwrap();
    let url = format!("{base}/grafana");
    assert_eq!(
        grafana.verify(&url, "glsa_good").await.unwrap().shows,
        "ada"
    );
    assert!(
        grafana
            .verify(&url, "glsa_bad")
            .await
            .unwrap_err()
            .to_string()
            .contains("401")
    );

    let root = tempfile::tempdir().unwrap();
    let grid = |team: &str| {
        format!(
            "  - {{name: slack, definition: slack, url: {base}/slack, auth: {{strategy: bearer, token_env: ORBIT_TEST_TOKEN_GRID}}{team}}}\n"
        )
    };
    let failed = open(root.path(), &grid(""))
        .grep(
            &[".".into()],
            &Scope {
                picks: vec![("slack".into(), "SlackChannel".into())],
                within: Vec::new(),
            },
        )
        .await
        .failed;
    assert!(failed[0].1.contains("missing_argument"), "{failed:?}");
    let grepped = open(root.path(), &grid(", team: T1"))
        .grep(
            &[".".into()],
            &Scope {
                picks: vec![("slack".into(), "SlackChannel".into())],
                within: Vec::new(),
            },
        )
        .await;
    assert!(grepped.failed.is_empty(), "{:?}", grepped.failed);
}

#[tokio::test]
async fn failures_say_why_without_a_url_dump() {
    let root = tempfile::tempdir().unwrap();
    let base = serve().await;
    let third_party = open(
        root.path(),
        &format!(
            "  - {{name: grafana, definition: grafana, url: {base}/sso, auth: {{strategy: bearer, token_env: ORBIT_TEST_TOKEN_GOOD}}}}\n  - {{name: slack, definition: slack, url: {base}/slack, auth: {{strategy: bearer, token_env: ORBIT_TEST_TOKEN_NEVER_SET}}}}\n"
        ),
    );
    let failed = third_party
        .grep(&[".".into()], &Scope::default())
        .await
        .failed;
    let errors: Vec<&str> = failed.iter().map(|(_, error)| error.as_str()).collect();
    assert!(
        errors.iter().any(|error| error.ends_with(
            "/api/folders redirected to accounts.google.com, so it wants a sign-in; check the token"
        )),
        "{errors:?}"
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("slack needs a token in ORBIT_TEST_TOKEN_NEVER_SET")),
        "{errors:?}"
    );
    assert!(
        !errors.iter().any(|error| error.contains("client_id")),
        "{errors:?}"
    );
}
