use std::io::IsTerminal;
use std::process::Command;

use serde::Deserialize;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};
use super::{ResponseFormat, pretty_json, write_stdout};
use crate::tui;

const SHOWN_ITEMS_PER_DOMAIN: usize = 2;

#[derive(Deserialize)]
struct GraphStatus {
    projects: Option<Projects>,
    #[serde(default)]
    domains: Vec<Domain>,
    indexing: Option<Indexing>,
    sdlc_indexing: Option<Indexing>,
    code_indexing: Option<Indexing>,
}

#[derive(Deserialize)]
struct Projects {
    indexed: usize,
    total_known: usize,
    #[serde(default)]
    gaps: usize,
}

#[derive(Deserialize)]
struct Domain {
    name: String,
    items: Vec<Item>,
}

#[derive(Deserialize)]
struct Item {
    name: String,
    count: usize,
}

#[derive(Deserialize)]
struct Indexing {
    state: String,
}

#[derive(Deserialize)]
struct FormattedText {
    formatted_text: String,
}

pub(crate) async fn run_graph_status(
    full_path: Option<String>,
    namespace_id: Option<i64>,
    project_id: Option<i64>,
    format: Option<ResponseFormat>,
) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let full_path = match (full_path, namespace_id, project_id) {
        (None, None, None) => Some(full_path_of_current_repository(&client)?),
        (full_path, _, _) => full_path,
    };
    let scope = scope_label(full_path.as_deref(), namespace_id, project_id);
    let params = graph_status_params(full_path, namespace_id, project_id, format);

    if format.is_none() && std::io::stdout().is_terminal() {
        return show_graph_status(&client, &params, &scope).await;
    }

    let body = client.get_graph_status(&params).await?;
    match format {
        Some(ResponseFormat::Llm) => write_stdout(&unwrap_formatted_text(body)),
        _ => write_stdout(&pretty_json(&body)),
    }
}

async fn show_graph_status(
    client: &OrbitClient,
    params: &[(&'static str, String)],
    scope: &str,
) -> Result<(), RemoteError> {
    tui::intro(format!("Orbit graph status · {scope}"))?;
    let spinner = tui::spinner("Reading graph status");
    let body = client.get_graph_status(params).await.inspect_err(|_| {
        spinner.fail("Could not read graph status");
    })?;
    spinner.clear();
    let Ok(status) = serde_json::from_slice::<GraphStatus>(&body) else {
        return write_stdout(&pretty_json(&body));
    };

    tui::card("Indexing", format_indexing(&status))?;
    tui::card("In the graph", format_domains(&status.domains))?;
    let state = status.indexing.as_ref().map_or("unknown", |i| &i.state);
    match state {
        "error" => tui::outro_cancel(closing_line(state))?,
        _ => tui::outro(closing_line(state))?,
    }
    Ok(())
}

fn format_indexing(status: &GraphStatus) -> String {
    let mut rows = Vec::new();
    match (&status.sdlc_indexing, &status.code_indexing) {
        (None, None) => rows.extend(indexing_row("Indexing", &status.indexing)),
        (sdlc, code) => {
            rows.extend(indexing_row("SDLC data", sdlc));
            rows.extend(indexing_row("Code", code));
        }
    }
    if let Some(projects) = &status.projects {
        rows.push(("Projects", format_projects(projects)));
    }
    tui::align_columns(rows.into_iter())
}

fn indexing_row<'a>(label: &'a str, indexing: &Option<Indexing>) -> Option<(&'a str, String)> {
    let indexing = indexing.as_ref()?;
    Some((label, indexing.state.replace('_', " ")))
}

fn format_projects(projects: &Projects) -> String {
    if projects.total_known == 0 {
        return "none yet".to_string();
    }
    let coverage = format!(
        "{} of {} indexed",
        tui::format_with_thousands(projects.indexed),
        tui::format_with_thousands(projects.total_known)
    );
    match projects.gaps {
        0 => coverage,
        gaps => format!("{coverage} · {} failed", tui::format_with_thousands(gaps)),
    }
}

fn format_domains(domains: &[Domain]) -> String {
    let rows: Vec<(&str, String)> = domains
        .iter()
        .filter_map(|domain| Some((domain.name.as_str(), format_largest_items(&domain.items)?)))
        .collect();
    if rows.is_empty() {
        return "nothing yet".to_string();
    }
    tui::align_columns(rows.into_iter())
}

fn format_largest_items(items: &[Item]) -> Option<String> {
    let mut largest_first: Vec<&Item> = items.iter().filter(|item| item.count > 0).collect();
    if largest_first.is_empty() {
        return None;
    }
    largest_first.sort_by(|a, b| b.count.cmp(&a.count).then(a.name.cmp(&b.name)));

    let mut parts: Vec<String> = largest_first
        .iter()
        .take(SHOWN_ITEMS_PER_DOMAIN)
        .map(|item| format!("{} {}", item.name, tui::format_with_thousands(item.count)))
        .collect();
    let hidden = largest_first.len().saturating_sub(SHOWN_ITEMS_PER_DOMAIN);
    if hidden > 0 {
        parts.push(format!("{hidden} more"));
    }
    Some(parts.join(" · "))
}

fn closing_line(state: &str) -> &'static str {
    match state {
        "indexed" => "Ready.",
        "backfilling" | "indexing" => "Indexing. Counts grow as data arrives.",
        "not_indexed" => {
            "Not indexed yet. If this does not change, check that Orbit is enabled in Orbit > Configuration."
        }
        "error" => {
            "Indexing has errors. If this does not clear, contact an instance administrator."
        }
        _ => "Indexing state unknown.",
    }
}

fn unwrap_formatted_text(body: Vec<u8>) -> Vec<u8> {
    match serde_json::from_slice::<FormattedText>(&body) {
        Ok(text) => text.formatted_text.into_bytes(),
        Err(_) => body,
    }
}

fn scope_label(
    full_path: Option<&str>,
    namespace_id: Option<i64>,
    project_id: Option<i64>,
) -> String {
    match (full_path, namespace_id, project_id) {
        (Some(path), _, _) => path.to_string(),
        (_, Some(id), _) => format!("namespace {id}"),
        (_, _, Some(id)) => format!("project {id}"),
        _ => String::new(),
    }
}

fn full_path_of_current_repository(client: &OrbitClient) -> Result<String, RemoteError> {
    let no_scope = || {
        RemoteError::new(
            EXIT_GENERIC,
            "no scope to inspect\n\n\
             Pass --full-path, --namespace-id, or --project-id, or run this command\n\
             inside a clone whose `origin` remote is a GitLab project.",
        )
    };
    let remote_url = origin_remote_url().ok_or_else(no_scope)?;
    let (host, full_path) = parse_remote_url(&remote_url).ok_or_else(no_scope)?;
    let api_host = client.host()?;
    if host != api_host {
        return Err(RemoteError::new(
            EXIT_GENERIC,
            format!(
                "`origin` is on {host}, but Orbit uses {api_host}\n\n\
                 Pass --full-path, --namespace-id, or --project-id."
            ),
        ));
    }
    Ok(full_path)
}

fn origin_remote_url() -> Option<String> {
    let output = Command::new("git")
        .args(["remote", "get-url", "origin"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_string())
}

fn parse_remote_url(url: &str) -> Option<(String, String)> {
    let (host, path) = match reqwest::Url::parse(url) {
        Ok(url) => (url.host_str()?.to_string(), url.path().to_string()),
        Err(_) => {
            let (user_and_host, path) = url.split_once(':')?;
            let host = user_and_host.rsplit('@').next()?;
            (host.to_string(), path.to_string())
        }
    };
    let full_path = path.trim_matches('/').trim_end_matches(".git").to_string();
    (!full_path.is_empty()).then_some((host, full_path))
}

fn graph_status_params(
    full_path: Option<String>,
    namespace_id: Option<i64>,
    project_id: Option<i64>,
    format: Option<ResponseFormat>,
) -> Vec<(&'static str, String)> {
    let mut params = Vec::new();
    if let Some(id) = namespace_id {
        params.push(("namespace_id", id.to_string()));
    }
    if let Some(id) = project_id {
        params.push(("project_id", id.to_string()));
    }
    if let Some(path) = full_path {
        params.push(("full_path", path));
    }
    if let Some(format) = format {
        params.push(("response_format", format.as_str().to_string()));
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_from(json: &str) -> GraphStatus {
        serde_json::from_str(json).expect("graph status JSON")
    }

    #[test]
    fn remote_url_yields_host_and_full_path_for_ssh_https_and_scp_forms() {
        for url in [
            "git@gitlab.com:gitlab-org/orbit/knowledge-graph.git",
            "ssh://git@gitlab.com:2222/gitlab-org/orbit/knowledge-graph.git",
            "https://gitlab.com/gitlab-org/orbit/knowledge-graph",
        ] {
            assert_eq!(
                parse_remote_url(url),
                Some((
                    "gitlab.com".to_string(),
                    "gitlab-org/orbit/knowledge-graph".to_string()
                )),
                "{url}"
            );
        }
    }

    #[test]
    fn domains_hide_empty_entities_and_fold_the_smallest() {
        let status = status_from(
            r#"{"domains":[
                {"name":"ci","items":[
                    {"name":"Job","count":514898},{"name":"Stage","count":118598},
                    {"name":"Pipeline","count":17864},{"name":"Deployment","count":0}]},
                {"name":"plan","items":[{"name":"Milestone","count":0}]}]}"#,
        );

        assert_eq!(
            format_domains(&status.domains),
            "ci   Job 514,898 · Stage 118,598 · 1 more"
        );
    }

    #[test]
    fn indexing_shows_each_pipeline_and_failed_projects() {
        let status = status_from(
            r#"{"projects":{"indexed":40,"total_known":1200,"gaps":3},
                "indexing":{"state":"backfilling"},
                "sdlc_indexing":{"state":"indexed"},
                "code_indexing":{"state":"backfilling"}}"#,
        );

        assert_eq!(
            format_indexing(&status),
            "SDLC data   indexed\n\
             Code        backfilling\n\
             Projects    40 of 1,200 indexed · 3 failed"
        );
    }

    #[test]
    fn indexing_falls_back_to_the_overall_state() {
        let status = status_from(
            r#"{"projects":{"indexed":0,"total_known":0},"indexing":{"state":"not_indexed"}}"#,
        );

        assert_eq!(
            format_indexing(&status),
            "Indexing   not indexed\nProjects   none yet"
        );
    }

    #[test]
    fn graph_status_sends_full_path_only_when_present() {
        let params = graph_status_params(Some("gitlab-org/gitlab".to_string()), None, None, None);
        assert_eq!(params, vec![("full_path", "gitlab-org/gitlab".to_string())]);
    }

    #[test]
    fn graph_status_sends_ids_and_format() {
        let params = graph_status_params(None, Some(9970), None, Some(ResponseFormat::Llm));
        assert_eq!(
            params,
            vec![
                ("namespace_id", "9970".to_string()),
                ("response_format", "llm".to_string()),
            ]
        );
    }

    #[test]
    fn graph_status_omits_format_when_unset() {
        let params = graph_status_params(None, None, Some(278964), None);
        assert_eq!(params, vec![("project_id", "278964".to_string())]);
    }
}
