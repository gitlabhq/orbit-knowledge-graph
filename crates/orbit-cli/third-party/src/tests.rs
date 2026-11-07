use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

const CHANNELS: &str = r#"{"ok": true, "channels": [
  {"id": "C1", "name": "team", "topic": {"value": "team chat"}},
  {"id": "C2", "name": "deploys", "topic": {"value": "deploy alerts"}}]}"#;
const USERS: &str =
    r#"{"ok": true, "members": [{"id": "U1", "name": "ada"}, {"id": "U2", "name": "bot"}]}"#;
const TEAM_PAGE_1: &str = r#"{"ok": true, "messages": [
  {"ts": "1712345678.000100", "user": "U1", "text": "web rollout stuck on the db migration"},
  {"ts": "1712345679.000100", "user": "U1", "text": "fixed it"}],
  "response_metadata": {"next_cursor": "p2"}}"#;
const TEAM_PAGE_2: &str =
    r#"{"ok": true, "messages": [{"ts": "1712300000.000100", "user": "U1", "text": "planning"}]}"#;
const DEPLOYS: &str = r#"{"ok": true, "messages": [{"ts": "1712346000.000100", "user": "U2", "text": "web-prod is Degraded"}]}"#;
const PROJECTS: &str = r#"{"items": [{"metadata": {"name": "team"}}]}"#;
const APPLICATIONS: &str = r#"{"items": [
  {"metadata": {"name": "web-prod"}, "spec": {"project": "team"},
   "status": {"health": {"status": "Degraded"}, "operationState": {"message": "db migration job failed"},
     "resources": [
       {"kind": "Deployment", "namespace": "web", "name": "web", "health": {"status": "Degraded"}},
       {"kind": "Job", "namespace": "web", "name": "migrate", "health": {"status": "Degraded"}}]}},
  {"metadata": {"name": "worker-prod"}, "spec": {"project": "team"},
   "status": {"health": {"status": "Healthy"},
     "resources": [{"kind": "Deployment", "namespace": "web", "name": "worker", "health": {"status": "Healthy"}}]}}]}"#;

fn needs_team(request: &str) -> bool {
    let target = request.split_whitespace().nth(1).unwrap_or_default();
    let grid = request
        .lines()
        .any(|line| line.eq_ignore_ascii_case("authorization: Bearer xoxe-grid"));
    grid && !target.contains("auth.")
        && !target.contains("team_id=T1")
        && !target.contains("conversations.history")
}

fn fixture(request: &str) -> Option<&'static str> {
    if needs_team(request) {
        return Some(r#"{"ok": false, "error": "missing_argument", "arg": "team_id"}"#);
    }
    let target = request.split_whitespace().nth(1)?;
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let has = |pair: &str| query.split('&').any(|found| found == pair);
    let bearer = |token: &str| {
        request
            .lines()
            .any(|line| line.eq_ignore_ascii_case(&format!("authorization: Bearer {token}")))
    };
    Some(match path {
        "/slack/api/auth.test" if bearer("xoxb-good") => r#"{"ok": true, "team": "Orbit"}"#,
        "/slack/api/auth.test" if bearer("xoxe-grid") => {
            r#"{"ok": true, "team": "Org", "is_enterprise_install": true}"#
        }
        "/slack/api/auth.teams.list" if bearer("xoxe-grid") => {
            r#"{"ok": true, "teams": [{"id": "T1", "name": "One"}, {"id": "T2", "name": "Two"}]}"#
        }
        "/slack/api/auth.test" => r#"{"ok": false, "error": "invalid_auth"}"#,
        "/argocd/api/v1/session/userinfo" if bearer("argo-good") => {
            r#"{"loggedIn": true, "username": "ada"}"#
        }
        "/argocd/api/v1/session/userinfo" => r#"{"loggedIn": false}"#,
        "/sso/api/v1/projects" => return Some("REDIRECT"),
        "/slack/api/conversations.list" | "/slack/api/users.conversations" => CHANNELS,
        "/slack/api/users.list" => USERS,
        "/slack/api/conversations.history" if has("channel=C1") && has("cursor=p2") => TEAM_PAGE_2,
        "/slack/api/conversations.history" if has("channel=C1") => TEAM_PAGE_1,
        "/slack/api/conversations.history" if has("channel=C2") => DEPLOYS,
        "/argocd/api/v1/projects" => PROJECTS,
        "/argocd/api/v1/applications" => APPLICATIONS,
        _ => return None,
    })
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
                let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                let response = match fixture(&request) {
                    Some("REDIRECT") => "HTTP/1.1 302 Found\r\nlocation: https://accounts.google.com/o/oauth2/auth?client_id=x\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string(),
                    found => {
                        let (status, body) = match found {
                            Some(body) => ("200 OK", body),
                            None => ("404 Not Found", r#"{"error":"not_found"}"#),
                        };
                        format!(
                            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        )
                    }
                };
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    format!("http://{address}")
}

async fn synced() -> (tempfile::TempDir, ThirdParty) {
    let base = serve().await;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join(CONFIG_FILE),
        format!(
            "sources:\n  - {{name: slack, definition: slack, url: {base}/slack}}\n  - {{name: argocd, definition: argocd, url: {base}/argocd}}\n"
        ),
    )
    .unwrap();
    let third_party = ThirdParty::open(root.path()).unwrap().unwrap();
    let freshness = third_party.ensure_fresh().await.unwrap();
    assert!(freshness.failed.is_empty(), "{:?}", freshness.failed);
    (root, third_party)
}

fn strings(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut values = optional_strings(batches, name)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    values.sort();
    values
}

#[tokio::test]
async fn sync_follows_cursors_parents_and_nested_items_then_waits_for_refresh() {
    let base = serve().await;
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join(CONFIG_FILE);
    let write = |slack_refresh: &str| {
        std::fs::write(
            &config,
            format!("sources:\n  - {{name: slack, definition: slack, url: {base}/slack, refresh: {slack_refresh}}}\n  - {{name: argocd, definition: argocd, url: {base}/argocd}}\n  - {{name: broken, definition: argocd, url: {base}/missing}}\n"),
        )
        .unwrap();
    };
    write("15m");
    let third_party = ThirdParty::open(root.path()).unwrap().unwrap();
    let freshness = third_party.ensure_fresh().await.unwrap();
    let count = |entity: &str| {
        freshness
            .synced
            .iter()
            .find(|(_, name, _)| name == entity)
            .map(|(_, _, n)| *n)
    };
    assert_eq!(count("SlackChannel"), Some(2));
    assert_eq!(count("SlackMessage"), Some(3));
    assert_eq!(count("ArgoApplication"), Some(2));
    assert_eq!(count("ArgoResource"), Some(3));
    assert_eq!(freshness.failed.len(), 1);
    assert_eq!(freshness.failed[0].0, "broken");

    let again = third_party.ensure_fresh().await.unwrap();
    assert!(again.synced.is_empty(), "{:?}", again.synced);
    assert_eq!(again.failed.len(), 1);

    write("0s");
    let third_party = ThirdParty::open(root.path()).unwrap().unwrap();
    let again = third_party.ensure_fresh().await.unwrap();
    let sources: std::collections::BTreeSet<&str> = again
        .synced
        .iter()
        .map(|(source, _, _)| source.as_str())
        .collect();
    assert_eq!(sources, ["slack"].into());
}

fn all_hits(found: Vec<Found>) -> Vec<Hit> {
    found.into_iter().flat_map(|entity| entity.hits).collect()
}

#[tokio::test]
async fn grep_matches_words_across_search_fields() {
    let (_root, third_party) = synced().await;
    let hits = all_hits(
        third_party
            .grep(&["db migration".into()], None, 20)
            .unwrap(),
    );
    let labels: Vec<String> = hits
        .iter()
        .map(|hit| format!("{}:{}", hit.entity, hit.label))
        .collect();
    assert!(
        labels.contains(&"SlackMessage:web rollout stuck on the db migration".to_string()),
        "{labels:?}"
    );
    assert!(
        labels.contains(&"ArgoApplication:web-prod".to_string()),
        "{labels:?}"
    );

    let (scope, rest) = third_party.scope("argocd:degraded");
    assert_eq!(rest, "degraded");
    let hits = all_hits(third_party.grep(&[rest], scope.as_ref(), 20).unwrap());
    assert!(hits.iter().all(|hit| hit.source == "argocd"));
    assert!(
        hits.iter()
            .any(|hit| hit.entity == "ArgoResource" && hit.label == "migrate")
    );

    let (scope, rest) = third_party.scope("slack:messages:web_prod");
    let hits = all_hits(third_party.grep(&[rest], scope.as_ref(), 20).unwrap());
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].entity, "SlackMessage");

    assert_eq!(third_party.scope("db migration").0, None);
}

#[tokio::test]
async fn context_reads_one_entity_with_its_links() {
    let (_root, third_party) = synced().await;
    let target = third_party.parse_target("argocd:app:web-prod").unwrap();
    let text = third_party.context(&target).unwrap();
    assert!(
        text.starts_with("argocd:app:web-prod  web-prod\n"),
        "{text}"
    );
    assert!(text.contains("health_status: Degraded"), "{text}");
    assert!(
        text.contains("-[IN_PROJECT]-> project (1)\n  argocd:project:team  team"),
        "{text}"
    );
    assert!(text.contains("<-[MANAGED_BY]- resource (2)"), "{text}");
    assert!(
        text.contains("argocd:resource:web-prod:Job:web:migrate  migrate"),
        "{text}"
    );

    let target = third_party.parse_target("slack:user:U1").unwrap();
    let text = third_party.context(&target).unwrap();
    assert!(text.contains("<-[AUTHORED_BY]- message (2)"), "{text}");

    let message = third_party
        .parse_target("slack:message:C1:1712345678.000100")
        .unwrap();
    assert_eq!(message.key, "C1:1712345678.000100");
    let text = third_party.context(&message).unwrap();
    assert!(
        text.contains("-[IN_CHANNEL]-> channel (1)\n  slack:channel:C1  team"),
        "{text}"
    );

    let error = third_party
        .context(&third_party.parse_target("slack:user:U9").unwrap())
        .unwrap_err();
    assert_eq!(error.to_string(), "nothing synced is slack:user:U9");
    assert!(third_party.parse_target("slack:widget:U1").is_none());
    assert!(third_party.parse_target("slack:user:").is_none());
    assert!(third_party.parse_target("mr:42").is_none());
}

#[tokio::test]
async fn the_second_part_of_a_grep_scope_is_a_type_only_when_it_names_one() {
    let (_root, third_party) = synced().await;
    let pick = |query: &str| {
        let (scope, rest) = third_party.scope(query);
        (scope.map(|scope| scope.picks), rest)
    };
    let slack =
        |entity: Option<&str>| Some(vec![("slack".to_string(), entity.map(str::to_string))]);
    assert_eq!(pick("slack:deploy"), (slack(None), "deploy".into()));
    assert_eq!(
        pick("slack:channel"),
        (slack(Some("SlackChannel")), "".into())
    );
    assert_eq!(
        pick("slack:channels:team"),
        (slack(Some("SlackChannel")), "team".into())
    );
    assert_eq!(
        pick("slack:channel:aweofiajwefoiawjef"),
        (slack(Some("SlackChannel")), "aweofiajwefoiawjef".into())
    );
    assert_eq!(
        pick("slack:deploy:prod"),
        (slack(None), "deploy:prod".into())
    );
    assert_eq!(
        pick("argocd:app:web slack:user:ada"),
        (
            Some(vec![
                ("argocd".to_string(), Some("ArgoApplication".to_string())),
                ("slack".to_string(), Some("SlackUser".to_string())),
            ]),
            "web ada".into()
        )
    );
    assert_eq!(pick("mr:deploy").0, None);

    let (scope, rest) = third_party.scope("slack:channel:aweofiajwefoiawjef");
    let found = third_party.grep(&[rest], scope.as_ref(), 20).unwrap();
    assert!(
        found.iter().all(|entity| entity.hits.is_empty()),
        "{found:?}"
    );
    let (scope, rest) = third_party.scope("slack:channel:deploy");
    let hits = all_hits(third_party.grep(&[rest], scope.as_ref(), 20).unwrap());
    assert_eq!(
        hits.iter()
            .map(|hit| hit.reference.as_str())
            .collect::<Vec<_>>(),
        ["slack:channel:C2"]
    );
}

#[tokio::test]
async fn query_runs_cypher_over_third_party_entities() {
    let (_root, third_party) = synced().await;
    let batches = third_party
        .query("MATCH (r:ArgoResource)-[:MANAGED_BY]->(a:ArgoApplication) WHERE a.health_status = 'Degraded' RETURN r.name, r.kind")
        .unwrap();
    assert_eq!(strings(&batches, "r_name"), ["migrate", "web"]);
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["r_name", "r_kind"]);

    let batches = third_party
        .query("MATCH (m:SlackMessage)-[:AUTHORED_BY]->(u:SlackUser {name: 'ada'}) RETURN count(m) AS n")
        .unwrap();
    assert_eq!(duckdb_client::i64_column(&batches, "n"), [2]);

    let (ours, theirs) = third_party
        .labels_in("MATCH (m:SlackMessage)-[:IN_CHANNEL]->(c:SlackChannel), (p:Project) RETURN m");
    assert_eq!(ours, ["SlackMessage", "SlackChannel"]);
    assert_eq!(theirs, ["Project"]);
}

#[tokio::test]
async fn setup_verifies_tokens_against_each_source() {
    let base = serve().await;
    let slack = setup::Connectable::named("slack").unwrap();
    let slack_url = format!("{base}/slack");
    let verified = slack.verify(&slack_url, "xoxb-good").await.unwrap();
    assert_eq!(verified.shows, "Orbit");
    assert!(verified.teams.is_empty());
    let grid = slack.verify(&slack_url, "xoxe-grid").await.unwrap();
    assert_eq!(grid.shows, "Org");
    assert_eq!(
        grid.teams,
        [("T1".into(), "One".into()), ("T2".into(), "Two".into())]
    );
    let error = slack.verify(&slack_url, "xoxb-bad").await.unwrap_err();
    assert!(error.to_string().contains("invalid_auth"), "{error}");

    let argocd = setup::Connectable::named("argocd").unwrap();
    let argo_url = format!("{base}/argocd");
    assert_eq!(
        argocd.verify(&argo_url, "argo-good").await.unwrap().shows,
        "ada"
    );
    let error = argocd.verify(&argo_url, "argo-bad").await.unwrap_err();
    assert!(error.to_string().contains("did not accept"), "{error}");
}

#[tokio::test]
async fn forget_marks_stale_and_drops_only_that_source() {
    let (root, third_party) = synced().await;
    let store = ThirdParty::store_path_in(root.path());
    store::forget(&store, "argocd", false).unwrap();
    let again = third_party.ensure_fresh().await.unwrap();
    let sources: std::collections::BTreeSet<&str> = again
        .synced
        .iter()
        .map(|(source, _, _)| source.as_str())
        .collect();
    assert_eq!(sources, ["argocd"].into());

    store::forget(&store, "argocd", true).unwrap();
    let target = third_party.parse_target("argocd:app:web-prod").unwrap();
    assert!(third_party.context(&target).is_err());
    let target = third_party.parse_target("slack:user:U1").unwrap();
    let text = third_party.context(&target).unwrap();
    assert!(text.contains("<-[AUTHORED_BY]- message (2)"), "{text}");
}

#[tokio::test]
async fn the_token_env_is_sent_as_the_header_and_an_unset_one_sends_none() {
    let base = serve().await;
    let root = tempfile::tempdir().unwrap();
    let write = |env: &str| {
        std::fs::write(
            root.path().join(CONFIG_FILE),
            format!(
                "sources:\n  - {{name: slack, definition: slack, url: {base}/slack, refresh: 0s, team: T1, auth: {{header: Authorization, scheme: Bearer, token_env: {env}}}}}\n"
            ),
        )
        .unwrap();
    };
    write("ORBIT_TEST_TOKEN_NEVER_SET");
    let third_party = ThirdParty::open(root.path()).unwrap().unwrap();
    assert!(third_party.ensure_fresh().await.unwrap().failed.is_empty());
    assert_eq!(
        credentials::resolve("slack-none", Some("ORBIT_TEST_TOKEN_NEVER_SET")),
        None
    );
    assert_eq!(
        credentials::resolve("slack-none", Some("ORBIT_TEST_TOKEN_GOOD")),
        Some(("xoxb-good".into(), credentials::TokenFrom::Env))
    );

    write("ORBIT_TEST_TOKEN_GOOD");
    let third_party = ThirdParty::open(root.path()).unwrap().unwrap();
    let freshness = third_party.ensure_fresh().await.unwrap();
    assert!(freshness.failed.is_empty(), "{:?}", freshness.failed);
}

#[tokio::test]
async fn max_parents_caps_the_channels_fetched() {
    let base = serve().await;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join(CONFIG_FILE),
        format!(
            "sources:\n  - name: slack\n    url: {base}/slack\n    entities:\n      Chan:\n        type: chan\n        fetch: {{path: /api/conversations.list, items: /channels}}\n        key: [/id]\n        label: name\n        properties: {{name: {{from: /name}}}}\n      Msg:\n        type: msg\n        fetch: {{path: /api/conversations.history, params: {{channel: \"{{key}}\"}}, for_each: Chan, items: /messages, max_parents: 1}}\n        key: [\"^key\", /ts]\n        label: text\n        properties: {{text: {{from: /text}}}}\n"
        ),
    )
    .unwrap();
    let third_party = ThirdParty::open(root.path()).unwrap().unwrap();
    let freshness = third_party.ensure_fresh().await.unwrap();
    assert!(freshness.failed.is_empty(), "{:?}", freshness.failed);
    let count = |entity: &str| {
        freshness
            .synced
            .iter()
            .find(|(_, name, _)| name == entity)
            .map(|(_, _, n)| *n)
    };
    assert_eq!(count("Chan"), Some(2));
    assert_eq!(count("Msg"), Some(2));
}

#[tokio::test]
async fn cursors_follow_further_pages_up_to_max_pages() {
    let base = serve().await;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join(CONFIG_FILE),
        format!(
            "sources:\n  - name: slack\n    url: {base}/slack\n    entities:\n      Msg:\n        type: msg\n        fetch: {{path: /api/conversations.history, params: {{channel: C1}}, items: /messages, cursor: {{param: cursor, next: /response_metadata/next_cursor}}, max_pages: 2}}\n        key: [/ts]\n        label: text\n        properties: {{text: {{from: /text}}}}\n"
        ),
    )
    .unwrap();
    let third_party = ThirdParty::open(root.path()).unwrap().unwrap();
    let freshness = third_party.ensure_fresh().await.unwrap();
    assert!(freshness.failed.is_empty(), "{:?}", freshness.failed);
    assert_eq!(
        freshness.synced,
        [("slack".to_string(), "Msg".to_string(), 3)]
    );
}

#[tokio::test]
async fn team_fills_team_id_and_is_left_out_when_unset() {
    let base = serve().await;
    let root = tempfile::tempdir().unwrap();
    let write = |team: &str| {
        std::fs::write(
            root.path().join(CONFIG_FILE),
            format!(
                "sources:\n  - {{name: slack, definition: slack, url: {base}/slack, refresh: 0s, auth: {{header: Authorization, scheme: Bearer, token_env: ORBIT_TEST_TOKEN_GRID}}{team}}}\n"
            ),
        )
        .unwrap();
    };
    write("");
    let third_party = ThirdParty::open(root.path()).unwrap().unwrap();
    let freshness = third_party.ensure_fresh().await.unwrap();
    assert_eq!(freshness.failed.len(), 1);
    assert!(
        freshness.failed[0].1.contains("missing_argument"),
        "{:?}",
        freshness.failed
    );

    write(", team: T1");
    let third_party = ThirdParty::open(root.path()).unwrap().unwrap();
    let freshness = third_party.ensure_fresh().await.unwrap();
    assert!(freshness.failed.is_empty(), "{:?}", freshness.failed);
    let count = |entity: &str| {
        freshness
            .synced
            .iter()
            .find(|(_, name, _)| name == entity)
            .map(|(_, _, n)| *n)
    };
    assert_eq!(count("SlackChannel"), Some(2));
    assert_eq!(count("SlackMessage"), Some(3));
}

#[tokio::test]
async fn grep_shows_newest_first_in_full_with_where_who_when_and_the_total() {
    let (_root, third_party) = synced().await;
    let (scope, rest) = third_party.scope("slack:message:.");
    let found = third_party.grep(&[rest], scope.as_ref(), 2).unwrap();
    let messages = found
        .iter()
        .find(|entity| entity.entity == "SlackMessage")
        .unwrap();
    assert_eq!(messages.total, 3);
    let labels: Vec<&str> = messages.hits.iter().map(|hit| hit.label.as_str()).collect();
    assert_eq!(labels, ["web-prod is Degraded", "fixed it"]);
    assert_eq!(
        messages.hits[0].shown,
        ["#deploys", "bot", "2024-04-05 19:40"]
    );
    let text = render(&found);
    assert!(
        text.contains(
            "slack:message:C2:1712346000.000100  #deploys · bot · 2024-04-05 19:40\n    web-prod is Degraded\n"
        ),
        "{text}"
    );
    assert!(
        text.contains("  2 of 3 messages shown; add words to narrow it\n"),
        "{text}"
    );

    let long = "x".repeat(300);
    assert_eq!(one_line(&format!("a\n  {long}")), format!("a {long}"));
}

#[tokio::test]
async fn context_finds_an_entity_by_its_name_when_no_key_matches() {
    let (_root, third_party) = synced().await;
    for asked in [
        "slack:channel:deploys",
        "slack:channel:#deploys",
        "slack:channel:DEPLOYS",
        "slack:channel:C2",
    ] {
        let text = third_party
            .context(&third_party.parse_target(asked).unwrap())
            .unwrap();
        assert!(
            text.starts_with("slack:channel:C2  deploys\n"),
            "{asked}: {text}"
        );
    }
    let text = third_party
        .context(&third_party.parse_target("slack:user:@ada").unwrap())
        .unwrap();
    assert!(text.starts_with("slack:user:U1  ada\n"), "{text}");
    let error = third_party
        .context(&third_party.parse_target("slack:channel:nope").unwrap())
        .unwrap_err();
    assert_eq!(error.to_string(), "nothing synced is slack:channel:nope");
}

#[tokio::test]
async fn grep_within_a_reference_keeps_what_links_to_it() {
    let (_root, third_party) = synced().await;
    let within = |references: &[&str]| -> Scope {
        let mut scope = Scope::default();
        for reference in references {
            let target = third_party.parse_target(reference).unwrap();
            scope
                .within
                .push((target.source.clone(), third_party.resolve(&target).unwrap()));
        }
        scope
    };
    let references = |scope: &Scope, query: &str| -> Vec<String> {
        all_hits(third_party.grep(&[query.into()], Some(scope), 20).unwrap())
            .into_iter()
            .map(|hit| hit.reference)
            .collect()
    };

    let team = within(&["slack:channel:team"]);
    assert_eq!(
        references(&team, "fixed"),
        ["slack:message:C1:1712345679.000100"]
    );
    assert!(references(&team, "Degraded").is_empty());
    let mut everything = references(&team, ".");
    everything.sort();
    assert_eq!(
        everything,
        [
            "slack:channel:C1",
            "slack:message:C1:1712345678.000100",
            "slack:message:C1:1712345679.000100",
        ]
    );

    let by_name = within(&["slack:channel:#deploys"]);
    assert_eq!(
        references(&by_name, "Degraded"),
        ["slack:message:C2:1712346000.000100"]
    );
    let bot = within(&["slack:user:bot"]);
    assert_eq!(
        references(&bot, "Degraded"),
        ["slack:message:C2:1712346000.000100"]
    );
    let both = within(&["slack:channel:team", "slack:channel:deploys"]);
    assert_eq!(references(&both, "Degraded").len(), 1);
    assert_eq!(references(&both, "fixed").len(), 1);

    let app = within(&["argocd:app:web-prod"]);
    let mut resources = references(&app, "Degraded");
    resources.sort();
    assert_eq!(
        resources,
        [
            "argocd:app:web-prod",
            "argocd:resource:web-prod:Deployment:web:web",
            "argocd:resource:web-prod:Job:web:migrate",
        ]
    );

    let (scope, rest) = third_party.scope("slack:message:fixed");
    let mut scope = scope.unwrap();
    scope.within = team.within.clone();
    assert_eq!(
        references(&scope, &rest),
        ["slack:message:C1:1712345679.000100"]
    );

    let error = third_party
        .resolve(&third_party.parse_target("slack:channel:nope").unwrap())
        .unwrap_err();
    assert_eq!(error.to_string(), "nothing synced is slack:channel:nope");
}

#[tokio::test]
async fn a_sign_in_redirect_fails_with_a_short_message() {
    let base = serve().await;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join(CONFIG_FILE),
        format!("sources:\n  - {{name: argocd, definition: argocd, url: {base}/sso}}\n"),
    )
    .unwrap();
    let third_party = ThirdParty::open(root.path()).unwrap().unwrap();
    let freshness = third_party.ensure_fresh().await.unwrap();
    assert_eq!(freshness.failed.len(), 1);
    let error = &freshness.failed[0].1;
    assert!(
        error.ends_with(
            "/api/v1/projects redirected to accounts.google.com, so it wants a sign-in; check the token or cookie"
        ),
        "{error}"
    );
    assert!(!error.contains("client_id"), "{error}");
}

#[test]
fn render_drops_a_label_that_only_repeats_the_key() {
    let hit = |reference: &str, label: &str, shown: &[&str]| Hit {
        source: "argocd".into(),
        entity: "ArgoApplication".into(),
        id: 1,
        reference: reference.into(),
        label: label.into(),
        shown: shown.iter().map(|value| value.to_string()).collect(),
        matched: Vec::new(),
    };
    let text = render(&[Found {
        source: "argocd".into(),
        entity: "ArgoApplication".into(),
        hits: vec![
            hit(
                "argocd:app:gkg--b67b8",
                "gkg--b67b8",
                &["Healthy", "Synced"],
            ),
            hit("argocd:app:web", "Web frontend", &["Healthy"]),
            hit("argocd:project:gkg", "gkg", &[]),
        ],
        total: 3,
    }]);
    assert_eq!(
        text,
        "» argocd\nargocd:app:gkg--b67b8  Healthy · Synced\nargocd:app:web  Healthy\n    Web frontend\nargocd:project:gkg\n"
    );
}
