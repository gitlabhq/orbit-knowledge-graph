use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use super::client::OrbitClient;
use super::error::{EXIT_FORBIDDEN, EXIT_RATE_LIMITED, EXIT_UNAVAILABLE, RemoteError};

pub(crate) const SCOPES: &[(&str, &str)] = &[
    ("WorkItem", "work_items"),
    ("MergeRequest", "merge_requests"),
    ("Commit", "commits"),
    ("Note", "notes"),
    ("Milestone", "milestones"),
    ("User", "users"),
    ("Project", "projects"),
    ("Group", "groups"),
];
const PROJECT_SCOPES: &[&str] = &[
    "work_items",
    "merge_requests",
    "commits",
    "notes",
    "milestones",
];
const LIMIT: usize = 10;
const PER_SCOPE: usize = 3;
const SHA_CHARS: usize = 12;
const DEFAULT_BACKOFF: Duration = Duration::from_secs(60);
const BACKOFF_FILE: &str = "gitlab-grep-retry-at";

pub(crate) fn scope_for(kind: &str) -> Option<&'static str> {
    let kind = match kind.eq_ignore_ascii_case("issue") {
        true => "WorkItem",
        false => kind,
    };
    SCOPES
        .iter()
        .find(|(node, _)| node.eq_ignore_ascii_case(kind))
        .map(|(_, scope)| *scope)
}

pub(crate) fn scope_order(scope: &str) -> usize {
    SCOPES
        .iter()
        .position(|(_, name)| *name == scope)
        .unwrap_or(SCOPES.len())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    Origin(PathBuf),
    Project(String),
    Namespace(String),
    ProjectId(i64),
    GroupId(i64),
}

impl Target {
    pub(crate) fn default_scopes(&self) -> Vec<&'static str> {
        let mut scopes = PROJECT_SCOPES.to_vec();
        if matches!(self, Target::Namespace(_) | Target::GroupId(_)) {
            scopes.push("projects");
        }
        scopes
    }

    pub(crate) fn explicit(&self) -> bool {
        !matches!(self, Target::Origin(_))
    }

    fn named(&self) -> String {
        match self {
            Target::Origin(dir) => dir.display().to_string(),
            Target::Project(path) | Target::Namespace(path) => path.clone(),
            Target::ProjectId(id) => format!("Project[{id}]"),
            Target::GroupId(id) => format!("Group[{id}]"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Request {
    pub(crate) target: Target,
    pub(crate) keywords: Vec<String>,
    pub(crate) scopes: Vec<&'static str>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Found(String),
    Empty,
    Skipped(String),
    Failed(String),
}

#[derive(Deserialize)]
struct Response {
    #[serde(default)]
    target: Option<ResponseTarget>,
    #[serde(default)]
    scopes: Vec<ScopeResults>,
    #[serde(default)]
    unavailable_scopes: Vec<Unavailable>,
    #[serde(default)]
    truncated: bool,
}

#[derive(Deserialize)]
struct ResponseTarget {
    full_path: String,
}

#[derive(Deserialize)]
struct ScopeResults {
    name: String,
    #[serde(default)]
    results: Vec<Entry>,
}

#[derive(Deserialize)]
struct Entry {
    entity_type: String,
    token: Option<String>,
    #[serde(default)]
    reference: serde_json::Value,
    #[serde(default)]
    summary: String,
}

#[derive(Deserialize)]
struct Unavailable {
    name: String,
    reason: String,
}

pub(crate) async fn search(request: Request, query: String) -> Outcome {
    let origin = match &request.target {
        Target::Origin(dir) => match super::origin::project(dir) {
            Some(origin) => Some(origin),
            None => return Outcome::Skipped("no GitLab `origin` remote".into()),
        },
        _ => None,
    };
    let dir = match &request.target {
        Target::Origin(dir) => dir.clone(),
        _ => PathBuf::from("."),
    };
    let client = match OrbitClient::from_env_in(&dir) {
        Ok(client) => client,
        Err(error) => return Outcome::Skipped(first_line(&error.message)),
    };
    let api_host = match client.get_host() {
        Ok(host) => host,
        Err(error) => return Outcome::Failed(first_line(&error.message)),
    };
    let scope = match (&request.target, origin) {
        (Target::Origin(_), Some((host, path))) if host == api_host => ("full_path", path),
        (Target::Origin(_), Some((host, _))) => {
            return Outcome::Skipped(format!("`origin` is on {host}, but Orbit uses {api_host}"));
        }
        (Target::Origin(_), None) => return Outcome::Skipped("no GitLab `origin` remote".into()),
        (Target::Project(path), _) => ("full_path", path.clone()),
        (Target::Namespace(path), _) => match super::token::url_host(path) {
            Some(host) if host != api_host => {
                return Outcome::Skipped(format!("{path} is on {host}, but Orbit uses {api_host}"));
            }
            _ => ("full_path", super::token::namespace(path, &api_host)),
        },
        (Target::ProjectId(id), _) => ("project_id", id.to_string()),
        (Target::GroupId(id), _) => ("namespace_id", id.to_string()),
    };
    let backoff = backoff_path();
    if let Some(wait) = backoff.as_deref().and_then(remaining_backoff) {
        return Outcome::Skipped(format!(
            "rate limited; retrying in {}s",
            wait.as_secs().max(1)
        ));
    }
    let tasks: Vec<_> = request
        .keywords
        .iter()
        .map(|keywords| {
            let client = client.clone();
            let params = vec![
                ("keywords", keywords.clone()),
                scope.clone(),
                ("limit", LIMIT.to_string()),
                ("per_scope", PER_SCOPE.to_string()),
                ("scopes", request.scopes.join(",")),
            ];
            tokio::spawn(async move { client.get_grep(&params).await })
        })
        .collect();
    let mut responses = Vec::new();
    for task in tasks {
        let body = match task.await {
            Ok(Ok(body)) => body,
            Ok(Err(error)) => {
                if error.exit_code == EXIT_RATE_LIMITED
                    && let Some(path) = &backoff
                {
                    record_backoff(path, error.retry_after.unwrap_or(DEFAULT_BACKOFF));
                }
                return outcome_for(&request.target, &error);
            }
            Err(error) => return Outcome::Failed(error.to_string()),
        };
        match serde_json::from_slice::<Response>(&body) {
            Ok(response) => responses.push(response),
            Err(error) => return Outcome::Failed(format!("unreadable response: {error}")),
        }
    }
    let shown = responses
        .iter()
        .find_map(|response| response.target.as_ref())
        .map(|target| target.full_path.clone())
        .unwrap_or(scope.1);
    render(&format!("{api_host}/{shown}"), &query, &responses)
}

fn outcome_for(target: &Target, error: &RemoteError) -> Outcome {
    let missing = first_line(&error.message).ends_with(" not found");
    match error.exit_code {
        EXIT_UNAVAILABLE if missing && target.explicit() => Outcome::Skipped(format!(
            "no local path or GitLab project or group named {}",
            target.named()
        )),
        EXIT_UNAVAILABLE | EXIT_FORBIDDEN => Outcome::Skipped(first_line(&error.message)),
        _ => Outcome::Failed(first_line(&error.message)),
    }
}

fn first_line(message: &str) -> String {
    message.lines().next().unwrap_or_default().to_string()
}

fn backoff_path() -> Option<PathBuf> {
    crate::workspace::Workspace::default_root()
        .ok()
        .map(|root| root.join(BACKOFF_FILE))
}

fn now() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

fn remaining_backoff(path: &Path) -> Option<Duration> {
    let until: u64 = std::fs::read_to_string(path).ok()?.trim().parse().ok()?;
    Duration::from_secs(until).checked_sub(now())
}

fn record_backoff(path: &Path, wait: Duration) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(path, (now() + wait).as_secs().to_string());
}

fn render(project: &str, query: &str, responses: &[Response]) -> Outcome {
    let mut seen = std::collections::HashSet::new();
    let mut rows: Vec<(usize, String)> = Vec::new();
    for scope in responses.iter().flat_map(|response| &response.scopes) {
        for entry in &scope.results {
            let label = label(entry);
            if seen.insert(label.clone()) {
                let row = match entry.summary.is_empty() {
                    true => label,
                    false => format!("{label}  {}", entry.summary),
                };
                rows.push((scope_order(&scope.name), row));
            }
        }
    }
    if rows.is_empty() {
        return Outcome::Empty;
    }
    rows.sort_by_key(|(order, _)| *order);
    let truncated = responses.iter().any(|response| response.truncated);
    let mut failed: Vec<String> = Vec::new();
    for unavailable in responses.iter().flat_map(|r| &r.unavailable_scopes) {
        let entry = format!("{} ({})", unavailable.name, unavailable.reason);
        if unavailable.reason != "not_available_for_search_type" && !failed.contains(&entry) {
            failed.push(entry);
        }
    }
    let mut out = format!(
        "gitlab {query:?} @ {project}: {} results{}\n",
        rows.len(),
        if truncated { ", truncated" } else { "" }
    );
    for (_, row) in &rows {
        let _ = writeln!(out, "  {row}");
    }
    if !failed.is_empty() {
        let _ = writeln!(out, "  unavailable: {}", failed.join(", "));
    }
    let token = responses
        .iter()
        .flat_map(|response| &response.scopes)
        .flat_map(|scope| &scope.results)
        .find_map(|entry| entry.token.as_deref());
    if let Some(token) = token {
        let _ = writeln!(
            out,
            "next: {} context {token}",
            crate::commands::setup::spec::launcher()
        );
    }
    Outcome::Found(out)
}

fn label(entry: &Entry) -> String {
    if let Some(token) = &entry.token {
        return token.clone();
    }
    match entry
        .reference
        .get("sha")
        .and_then(serde_json::Value::as_str)
    {
        Some(sha) => format!(
            "{} {}",
            entry.entity_type,
            sha.chars().take(SHA_CHARS).collect::<String>()
        ),
        None => format!("{} {}", entry.entity_type, entry.reference),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(json: &str) -> Response {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn results_merge_across_keywords_in_scope_order() {
        let first = response(
            r#"{"scopes":[{"name":"merge_requests","results":[
                {"entity_type":"MergeRequest","token":"MergeRequest[42]","reference":{"id":42},"summary":"Add scoped search"}]}],
               "unavailable_scopes":[{"name":"notes","reason":"timeout"},{"name":"users","reason":"not_available_for_search_type"}],
               "truncated":false}"#,
        );
        let second = response(
            r#"{"scopes":[
                {"name":"work_items","results":[{"entity_type":"WorkItem","token":"WorkItem[7]","reference":{"id":7},"summary":"Grep issue"}]},
                {"name":"merge_requests","results":[{"entity_type":"MergeRequest","token":"MergeRequest[42]","reference":{"id":42},"summary":"Add scoped search"}]},
                {"name":"commits","results":[{"entity_type":"Commit","token":null,"reference":{"project_id":1,"sha":"0123456789abcdef0123"},"summary":"Fix grep"}]}],
               "unavailable_scopes":[],"truncated":true}"#,
        );
        let Outcome::Found(text) =
            render("gitlab.com/group/project", "grep|search", &[first, second])
        else {
            panic!("expected results");
        };
        assert_eq!(
            text,
            format!(
                "gitlab \"grep|search\" @ gitlab.com/group/project: 3 results, truncated\n  \
                 WorkItem[7]  Grep issue\n  \
                 MergeRequest[42]  Add scoped search\n  \
                 Commit 0123456789ab  Fix grep\n  \
                 unavailable: notes (timeout)\n\
                 next: {} context MergeRequest[42]\n",
                crate::commands::setup::spec::launcher()
            )
        );
    }

    #[test]
    fn no_results_is_empty() {
        let empty = response(r#"{"scopes":[{"name":"work_items","results":[]}]}"#);
        assert_eq!(render("h/p", "x", &[empty]), Outcome::Empty);
    }

    #[test]
    fn hosted_kinds_map_to_scopes() {
        assert_eq!(scope_for("mergerequest"), Some("merge_requests"));
        assert_eq!(scope_for("Issue"), Some("work_items"));
        assert_eq!(scope_for("Pipeline"), None);
        assert_eq!(scope_for("File"), None);
    }

    #[test]
    fn groups_also_search_projects() {
        assert!(
            !Target::Project("g/p".into())
                .default_scopes()
                .contains(&"projects")
        );
        assert!(
            Target::Namespace("g".into())
                .default_scopes()
                .contains(&"projects")
        );
        assert!(
            !Target::Origin(".".into())
                .default_scopes()
                .contains(&"users")
        );
    }

    #[test]
    fn backoff_expires() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(BACKOFF_FILE);
        assert_eq!(remaining_backoff(&path), None);
        record_backoff(&path, Duration::from_secs(30));
        assert!(remaining_backoff(&path).is_some());
        record_backoff(&path, Duration::ZERO);
        assert_eq!(remaining_backoff(&path), None);
    }
}
