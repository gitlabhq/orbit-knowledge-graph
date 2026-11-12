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
        "  - {{name: slack, definition: slack, url: {base}/slack, auth: {{strategy: bearer, token_env: ORBIT_TEST_TOKEN_GOOD}}}}\n  - {{name: grafana, definition: grafana, url: {base}/grafana}}\n"
    )
}

async fn synced() -> (tempfile::TempDir, ThirdParty) {
    let root = tempfile::tempdir().unwrap();
    let third_party = open(root.path(), &both(&serve().await));
    let failed = third_party.ensure_fresh().await.unwrap().failed;
    assert!(failed.is_empty(), "{failed:?}");
    (root, third_party)
}

fn counts(freshness: &Freshness) -> Vec<(&str, usize)> {
    freshness
        .synced
        .iter()
        .map(|(_, entity, count)| (entity.as_str(), *count))
        .collect()
}

async fn grep(third_party: &ThirdParty, query: &str, within: &[&str]) -> Vec<String> {
    let (scope, rest) = third_party.scope(query);
    let mut scope = scope.unwrap_or_default();
    for reference in within {
        let target = third_party.parse_target(reference).unwrap();
        match third_party.resolve_to_key(&target) {
            Ok(key) => {
                scope
                    .within
                    .push((target.source.clone(), third_party.resolve(&target).unwrap()));
                scope
                    .within_keys
                    .push((target.source.clone(), target.entity.clone(), key));
            }
            Err(_) => scope.within_names.push((
                target.source.clone(),
                target.entity.clone(),
                third_party.live_name(&target).unwrap(),
            )),
        }
    }
    let rest = if rest.trim().is_empty() {
        ".".to_string()
    } else {
        rest
    };
    assert!(
        third_party
            .search_live(std::slice::from_ref(&rest), &mut scope)
            .await
            .is_empty()
    );
    let found = third_party.grep(&[rest], Some(&scope), 20).unwrap();
    found
        .into_iter()
        .flat_map(|entity| entity.hits)
        .map(|hit| hit.reference)
        .collect()
}

#[tokio::test]
async fn sync_fetches_every_entity_once_and_again_when_stale() {
    let base = serve().await;
    let root = tempfile::tempdir().unwrap();
    let broken = "  - {name: broken, definition: grafana, url: http://127.0.0.1:1}\n";
    let third_party = open(root.path(), &format!("{}{broken}", both(&base)));
    assert_eq!(
        third_party.staleness().unwrap().never,
        ["slack", "grafana", "broken"]
    );
    let freshness = third_party.ensure_fresh().await.unwrap();
    assert_eq!(freshness.failed.len(), 1, "{:?}", freshness.failed);
    assert_eq!(
        counts(&freshness),
        [
            ("SlackChannel", 2),
            ("SlackUser", 2),
            ("GrafanaFolder", 2),
            ("GrafanaDashboard", 2),
            ("GrafanaAlert", 3),
        ]
    );
    assert_eq!(freshness.failed.len(), 1);
    assert!(third_party.ensure_fresh().await.unwrap().synced.is_empty());

    let third_party = open(
        root.path(),
        &format!(
            "{}  - {{name: grafana, definition: grafana, url: {base}/grafana, refresh: 0s}}\n",
            &both(&base)[..both(&base).find("  - {name: grafana").unwrap()]
        ),
    );
    assert_eq!(third_party.staleness().unwrap().old, ["grafana"]);
    store::forget(&ThirdParty::store_path_in(root.path()), "grafana", true).unwrap();
    assert!(
        grep(&third_party, "grafana:alert:web", &[])
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn grep_scopes_by_source_and_type_and_reads_the_graph() {
    let (_root, third_party) = synced().await;
    let (scope, rest) = third_party.scope("slack:channel:nothing-matches");
    assert_eq!(
        scope.unwrap().picks,
        [("slack".into(), Some("SlackChannel".into()))]
    );
    assert_eq!(rest, "nothing-matches");
    assert_eq!(third_party.scope("slack:deploy:prod").1, "deploy:prod");
    assert_eq!(third_party.scope("mr:deploy").0, None);

    assert_eq!(
        grep(&third_party, "slack:channels:deploy", &[]).await,
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
}

#[tokio::test]
async fn grep_searches_live_and_narrows_by_reference() {
    let (_root, third_party) = synced().await;
    let both = [
        "slack:message:C1:1712345678.000100",
        "slack:message:C7:1712000001.000100",
    ];
    assert_eq!(
        grep(&third_party, "slack:message:migration", &[]).await,
        both
    );
    assert_eq!(
        grep(
            &third_party,
            "slack:message:migration",
            &["slack:channel:#team"]
        )
        .await,
        both[..1]
    );
    assert_eq!(
        grep(
            &third_party,
            "slack:message:migration",
            &["slack:user:carol"]
        )
        .await,
        both[1..]
    );
    assert!(third_party.store_path().exists());
    let tables = duckdb_client::DuckDbClient::open(duckdb_client::OpenMode::ReadOnly(
        third_party.store_path(),
    ))
    .unwrap()
    .query_arrow("SELECT table_name FROM information_schema.tables")
    .unwrap();
    let tables = duckdb_client::string_column(&tables, "table_name");
    assert!(
        !tables.iter().any(|table| table.contains("message")),
        "{tables:?}"
    );

    assert_eq!(
        grep(&third_party, "grafana:dashboard:postgres", &[]).await,
        ["grafana:dashboard:d-db"]
    );
    assert_eq!(
        grep(&third_party, "grafana:dashboard:o", &["grafana:folder:Web"]).await,
        ["grafana:dashboard:d-web"]
    );
}

#[tokio::test]
async fn render_shows_live_hits_with_context_and_stored_ones_with_the_total() {
    let (_root, third_party) = synced().await;
    let (scope, rest) = third_party.scope("slack:message:migration");
    let mut scope = scope.unwrap();
    third_party
        .search_live(std::slice::from_ref(&rest), &mut scope)
        .await;
    let text = render(&third_party.grep(&[rest], Some(&scope), 20).unwrap());
    assert!(
        text.starts_with("» slack\nslack:message:C1:1712345678.000100  #team · ada · 2024-04-05 19:34\n    web rollout stuck on the db migration\n"),
        "{text}"
    );
    assert!(
        text.contains("slack:message:C7:1712000001.000100  #elsewhere · Carol"),
        "{text}"
    );
    let (scope, rest) = third_party.scope("grafana:alert:.");
    let text = render(&third_party.grep(&[rest], scope.as_ref(), 2).unwrap());
    assert!(
        text.ends_with("  2 of 3 alerts shown; add words to narrow it\n"),
        "{text}"
    );
}

#[tokio::test]
async fn context_reads_stored_entities_and_fetches_live_ones() {
    let (_root, third_party) = synced().await;
    let context = |reference: &str| {
        let target = third_party.parse_target(reference).unwrap();
        let third_party = &third_party;
        async move { third_party.context_live(&target).await }
    };
    let text = context("grafana:dashboard:d-web").await.unwrap();
    assert!(
        text.starts_with("grafana:dashboard:d-web  Web overview\n"),
        "{text}"
    );
    assert!(
        text.contains("-[IN_FOLDER]-> folder (1)\n  grafana:folder:f-web  Web"),
        "{text}"
    );
    assert!(text.contains("<-[ON_DASHBOARD]- alert (2)"), "{text}");
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
    assert!(context("slack:message:C7:1").await.is_err());
    assert_eq!(
        context("slack:channel:nope").await.unwrap_err().to_string(),
        "nothing synced is slack:channel:nope"
    );
    assert!(third_party.parse_target("mr:42").is_none());
}

#[tokio::test]
async fn query_runs_cypher_and_returns_only_what_it_asks_for() {
    let (_root, third_party) = synced().await;
    let batches = third_party
        .query("MATCH (a:GrafanaAlert)-[:IN_FOLDER]->(f:GrafanaFolder) WHERE f.title = 'Web' RETURN a.title")
        .unwrap();
    let mut titles: Vec<String> = optional_strings(&batches, "a_title")
        .into_iter()
        .flatten()
        .collect();
    titles.sort();
    assert_eq!(titles, ["Web error rate high", "Web latency high"]);
    assert_eq!(batches[0].schema().fields().len(), 1);
    let (ours, theirs) = third_party.labels_in("MATCH (c:SlackChannel), (p:Project) RETURN c");
    assert_eq!(
        (ours, theirs),
        (
            vec!["SlackChannel".to_string()],
            vec!["Project".to_string()]
        )
    );
    assert!(
        third_party
            .query("MATCH (m:SlackMessage) RETURN m.text")
            .is_err()
    );
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
        .ensure_fresh()
        .await
        .unwrap()
        .failed;
    assert!(failed[0].1.contains("missing_argument"), "{failed:?}");
    assert!(
        open(root.path(), &grid(", team: T1"))
            .ensure_fresh()
            .await
            .unwrap()
            .failed
            .is_empty()
    );
}

#[tokio::test]
async fn a_sign_in_redirect_fails_with_a_short_message() {
    let root = tempfile::tempdir().unwrap();
    let base = serve().await;
    let third_party = open(
        root.path(),
        &format!("  - {{name: grafana, definition: grafana, url: {base}/sso}}\n"),
    );
    let failed = third_party.ensure_fresh().await.unwrap().failed;
    assert!(
        failed[0].1.ends_with(
            "/api/folders redirected to accounts.google.com, so it wants a sign-in; check the token"
        ),
        "{failed:?}"
    );
}
