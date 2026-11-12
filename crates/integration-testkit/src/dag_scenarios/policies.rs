//! Remote reads under every policy a code or live store renders: `none` with a check, a `none`
//! entity next to a `via` entity in one stage, and the `global` scope kind.

use std::collections::BTreeMap;
use std::sync::Arc;

use compiler::dag::Rendered;
use compiler::orbit_authorization::{Checks, Grants, Key};
use query_data_model::plan::live::Workspaces;
use query_data_model::{Model, Packages, Tier};
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use serde_json::json;

use super::single::CODE;
use super::{Run, json_rows, row};
use crate::dag_fakes::{AllowedKeys, SeededGraph, dag_packages, run_dag};

const QUERY_4: &str = "MATCH (mr:MergeRequest {state: 'opened'})-[:SOURCE_HEAD]->(h:Head)-[:HEAD_FILE]->(f:File) WHERE f.content MATCHES ast('todo!($$$MSG)', 'rust') RETURN mr.iid, h.branch, f.path, match.MSG ORDER BY mr.iid LIMIT 20";

fn grants(model: &Model, path: &str) -> Grants {
    let session = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new(path, 20)]).unwrap();
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(&config, model, session).unwrap()
}

async fn run_on(
    model: &Arc<Model>,
    query: &str,
    graph: Vec<serde_json::Value>,
    checks: &Checks,
) -> Run {
    let graph = Arc::new(SeededGraph(graph.into_iter().map(row).collect()));
    let grants = grants(model, "1/9970/");
    let (events, outcome) = run_dag(model, graph, query, &grants, "alice", checks).await;
    Run { events, outcome }
}

/// The DAG packages with text replaced in some fragments, keeping each package's tier.
fn replaced(edits: &[(&str, &str, &str)]) -> Packages {
    let packages = dag_packages();
    let tiers = packages.tiers().clone();
    let mut by_tier: BTreeMap<Tier, BTreeMap<String, String>> = BTreeMap::new();
    for (name, text) in packages.sources() {
        let mut text = text.clone();
        for (path, find, with) in edits.iter().filter(|(path, ..)| *path == name) {
            assert!(text.contains(find), "{path} holds {find}");
            text = text.replace(find, with);
        }
        let tier = tiers[name.split('/').next().unwrap_or_default()];
        by_tier.entry(tier).or_default().insert(name.clone(), text);
    }
    by_tier
        .into_iter()
        .map(|(tier, sources)| Packages::new(tier, sources))
        .reduce(Packages::with)
        .unwrap()
}

/// A Slack listing of every message under `policy`, the entity's scope and `enforce` lines; the
/// Slack sign-in decides which messages come back.
fn listing(policy: &str) -> Packages {
    let files = [
        (
            "audit.listing/package.yaml",
            "kind: package\nname: audit.listing\nrequires: [gitlab, integration.slack]\n".to_string(),
        ),
        (
            "audit.listing/graph.yaml",
            "kind: graph\npackage: audit.listing\nentities:\n  Listed:\n    identity: id\n    label: text\n    default_columns: [id, text]\n    properties:\n      id: {type: string}\n      channel: {type: string}\n      text: {type: string}\n".to_string(),
        ),
        (
            "audit.listing/authorization.yaml",
            format!("kind: authorization\npackage: audit.listing\nentities:\n  Listed:\n    {policy}\n    permission: read\n    checks: [{{provider: slack, kind: delegated}}]\n    properties: {{open: [id, channel, text]}}\n"),
        ),
        (
            "audit.listing/storage/live.yaml",
            "kind: storage\npackage: audit.listing\nstore: slack\nentities:\n  Listed:\n    complete: true\n    columns:\n      id: {immutable: true}\n      channel: {}\n      text: {}\n    backend:\n      operation: search.messages\n      filters: [id, channel]\n".to_string(),
        ),
    ];
    let sources = files
        .into_iter()
        .map(|(path, text)| (path.to_string(), text))
        .collect();
    dag_packages().with(Packages::new(Tier::Platform, sources))
}

const LISTED: &str = "MATCH (m:Listed) WHERE m.channel = 'random' RETURN m.text LIMIT 10";

fn texts(run: Run) -> Vec<serde_json::Value> {
    let outcome = run
        .outcome
        .unwrap_or_else(|error| panic!("the run failed: {error}"));
    json_rows(&outcome.executed.batches)
        .into_iter()
        .map(|row| row["m_text"].clone())
        .collect()
}

fn calls(run: &Run) -> Vec<Workspaces> {
    run.rendered(0)
        .into_iter()
        .filter_map(|request| match request {
            Rendered::Live(call) => Some(call.workspaces),
            _ => None,
        })
        .collect()
}

/// A live entity whose only policy is its check reads every workspace the caller's sign-in
/// reaches, with no scope lookup, and its check decides the rows.
#[tokio::test]
async fn a_live_read_under_none_reads_every_workspace_its_check_allows() {
    let model = listing("scope: namespace\n    enforce: {live: none}")
        .load()
        .unwrap();
    let ran = run_on(&model, LISTED, Vec::new(), &Checks::default()).await;
    assert_eq!(calls(&ran), [Workspaces::Any]);
    assert!(
        ran.rendered(0)
            .iter()
            .all(|request| !matches!(request, Rendered::Sql { .. }))
    );
    assert_eq!(texts(ran), [json!("deploy memes")]);
}

/// A global live entity needs only the permission: it reads every workspace, with no lookup.
#[tokio::test]
async fn a_global_live_entity_reads_with_its_permission_alone() {
    let model = listing("scope: global").load().unwrap();
    let ran = run_on(&model, LISTED, Vec::new(), &Checks::default()).await;
    assert_eq!(calls(&ran), [Workspaces::Any]);
    assert_eq!(texts(ran), [json!("deploy memes")]);
}

/// A code store whose entity enforces `none` still names its repositories through the property
/// its store declares, and the `read_code` check decides the rows.
#[tokio::test]
async fn a_code_read_under_none_searches_the_named_repositories_through_its_check() {
    let model = replaced(&[
        (
            "gitlab.source_code/authorization.yaml",
            "orbit_code: {scope_key: {via: IN_PROJECT}}",
            "orbit_code: none",
        ),
        (
            "gitlab.source_code/storage/orbit_code.yaml",
            "    backend:\n",
            "    backend:\n      repository: project_id\n",
        ),
    ])
    .load()
    .unwrap();
    let allowed = AllowedKeys::checks("project", [Key::Int(42)]);
    let ran = run_on(&model, CODE, Vec::new(), &allowed).await;
    let rows = json_rows(&ran.outcome.as_ref().unwrap().executed.batches);
    assert_eq!(rows.len(), 2, "{rows:?}");

    let denied = AllowedKeys::checks("project", []);
    let ran = run_on(&model, CODE, Vec::new(), &denied).await;
    assert!(json_rows(&ran.outcome.unwrap().executed.batches).is_empty());
}

/// `Head` enforces `none` and the `File`s at each head enforce `via`: the stage looks up the
/// repositories of `File`'s chain, so it keeps the readable one instead of dropping them all.
#[tokio::test]
async fn a_none_entity_before_a_via_entity_keeps_the_readable_repositories() {
    let model = replaced(&[
        (
            "orbit_next.heads/authorization.yaml",
            "enforce: {orbit_code: {scope_key: {via: HEAD_IN}}}",
            "enforce: {orbit_code: none}\n    checks: [{provider: rails, permission: read_code, resource: project, object: Project, id: project_id}]",
        ),
        (
            "orbit_next.heads/storage/orbit_code.yaml",
            "      branch: {immutable: true}\nrelationships:",
            "      branch: {immutable: true}\n    backend: {repository: project_id}\nrelationships:",
        ),
    ])
    .load()
    .unwrap();
    let merge_requests = vec![
        json!({"mr_iid": 1, "mr_id": 2000, "mr_project_id": 42, "mr_source_branch": "feature-a", "_orbit_mr_id": 2000, "_orbit_mr_type": "MergeRequest"}),
        json!({"mr_iid": 2, "mr_id": 2001, "mr_project_id": 77, "mr_source_branch": "fix-b", "_orbit_mr_id": 2001, "_orbit_mr_type": "MergeRequest"}),
    ];
    let checks = AllowedKeys::checks("project", [Key::Int(42), Key::Int(77)]);
    let ran = run_on(&model, QUERY_4, merge_requests, &checks).await;
    let code = ran
        .rendered(1)
        .into_iter()
        .find_map(|request| match request {
            Rendered::Code(code) => Some(code),
            _ => None,
        });
    assert_eq!(code.map(|code| code.repositories), Some(vec![42]));
    let rows = json_rows(&ran.outcome.unwrap().executed.batches);
    let paths: Vec<_> = rows.iter().map(|row| row["f_path"].clone()).collect();
    assert_eq!(paths, [json!("src/auth.rs")]);
}
