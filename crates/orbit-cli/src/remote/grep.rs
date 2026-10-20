use std::fmt::Write as _;
use std::path::PathBuf;

use serde::Deserialize;

use super::client::OrbitClient;
use super::error::{EXIT_FORBIDDEN, EXIT_UNAVAILABLE, RemoteError};

pub(crate) const SCOPES: &[(&str, &str)] = &[
    ("WorkItem", "work_items"),
    ("MergeRequest", "merge_requests"),
    ("Commit", "commits"),
    ("Note", "notes"),
    ("Milestone", "milestones"),
    ("User", "users"),
    ("Project", "projects"),
    ("Group", "groups"),
    ("Branch", "branches"),
];
const BRANCHES: &str = "branches";

pub(crate) fn scope_for(kind: &str) -> Option<&'static str> {
    let lower = kind.to_ascii_lowercase();
    let singular = lower.strip_suffix('s').unwrap_or(&lower);
    let kind = match singular {
        "mr" | "pr" | "merge_request" => "MergeRequest",
        "issue" | "epic" | "work_item" => "WorkItem",
        "comment" => "Note",
        other => other,
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
    scopes: Vec<ScopeStatus>,
    #[serde(default)]
    results: Vec<Entry>,
    #[serde(default)]
    truncated: bool,
}

#[derive(Deserialize)]
struct ResponseTarget {
    full_path: String,
    #[serde(default, rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct ScopeStatus {
    name: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    reason: Option<Reason>,
}

#[derive(Deserialize)]
struct Reason {
    code: String,
}

#[derive(Deserialize)]
struct Entry {
    #[serde(default)]
    scope: String,
    #[serde(rename = "type")]
    entity_type: String,
    token: Option<String>,
    #[serde(default)]
    reference: serde_json::Value,
    #[serde(default)]
    text: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    matches: Vec<Match>,
}

#[derive(Deserialize)]
struct Match {
    start_line: u64,
    #[serde(default)]
    text: String,
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
    let home_project = match (&request.target, &origin) {
        (Target::Origin(_), Some((_, path))) => Some(path.clone()),
        _ => None,
    };
    let scope = match (&request.target, origin) {
        (Target::Origin(_), Some((host, path))) if host == api_host => {
            ("full_path", top_level_group(&path).to_string())
        }
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
    let api_scopes: Vec<&str> = request
        .scopes
        .iter()
        .copied()
        .filter(|scope| *scope != BRANCHES)
        .collect();
    let only_branches = !request.scopes.is_empty() && api_scopes.is_empty();
    let keywords = match only_branches {
        true => &[][..],
        false => &request.keywords[..],
    };
    let fetch = |keywords: Vec<String>| {
        let tasks: Vec<_> = keywords
            .into_iter()
            .map(|keywords| {
                let client = client.clone();
                let mut params = vec![("search", keywords), scope.clone()];
                if !api_scopes.is_empty() {
                    params.push(("scopes", api_scopes.join(",")));
                }
                tokio::spawn(async move { client.get_grep(&params).await })
            })
            .collect::<Vec<_>>();
        async {
            let mut responses = Vec::new();
            for task in tasks {
                let body = match task.await {
                    Ok(Ok(body)) => body,
                    Ok(Err(error)) => return Err(outcome_for(&request.target, &error)),
                    Err(error) => return Err(Outcome::Failed(error.to_string())),
                };
                match serde_json::from_slice::<Response>(&body) {
                    Ok(response) => responses.push(response),
                    Err(error) => {
                        return Err(Outcome::Failed(format!("unreadable response: {error}")));
                    }
                }
            }
            Ok(responses)
        }
    };
    let mut responses = match fetch(keywords.to_vec()).await {
        Ok(responses) => responses,
        Err(outcome) => return outcome,
    };
    let words: Vec<String> = keywords
        .iter()
        .flat_map(|keyword| keyword.split_whitespace().map(str::to_string))
        .collect();
    let mut any_word = false;
    if words.len() > keywords.len() && responses.iter().all(|response| response.results.is_empty())
    {
        if let Ok(wider) = fetch(words).await
            && wider.iter().any(|response| !response.results.is_empty())
        {
            (responses, any_word) = (wider, true);
        }
    }
    let shown = responses
        .iter()
        .find_map(|response| response.target.as_ref())
        .map(|target| target.full_path.clone())
        .unwrap_or_else(|| scope.1.clone());
    let shown = format!("{api_host}/{shown}");
    let mut found = Vec::new();
    if !responses.is_empty()
        && let Outcome::Found(text) = render(
            &shown,
            &query,
            &request.keywords,
            &responses,
            home_project.as_deref(),
        )
    {
        if any_word {
            found.push(format!(
                "» no GitLab result has every word of {query:?}; results with any of them:\n{text}"
            ));
        } else {
            found.push(text);
        }
    }
    if request.scopes.contains(&BRANCHES) {
        let project = match &home_project {
            Some(path) => path.clone(),
            None if matches!(scope.0, "full_path" | "project_id") => scope.1.clone(),
            None => return Outcome::Skipped("branches belong to a project; name one".into()),
        };
        let shown = format!("{api_host}/{project}");
        let wanted = request.keywords.join(" ");
        match super::sdlc::search_text(&client, &shown, &project, &wanted).await {
            Ok(Some(text)) => found.push(text),
            Ok(None) => {}
            Err(error) if found.is_empty() => return outcome_for(&request.target, &error),
            Err(_) => {}
        }
    }
    match found.is_empty() {
        true => Outcome::Empty,
        false => Outcome::Found(found.join("\n")),
    }
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

/// The group whose projects an `origin` search covers: the project's top-level group, so sibling
/// projects' merge requests, issues, and code are found too.
fn top_level_group(project: &str) -> &str {
    project.split('/').next().unwrap_or(project)
}

/// Renders results; `origin` is the checkout's project, whose code the local search already covers
/// and whose references are written short.
fn render(
    project: &str,
    query: &str,
    keywords: &[String],
    responses: &[Response],
    origin: Option<&str>,
) -> Outcome {
    let terms: Vec<String> = keywords
        .iter()
        .flat_map(|keyword| keyword.split_whitespace())
        .map(str::to_lowercase)
        .collect();
    let home = origin.or_else(|| {
        responses
            .iter()
            .filter_map(|response| response.target.as_ref())
            .find(|target| target.kind.eq_ignore_ascii_case("project"))
            .map(|target| target.full_path.as_str())
    });
    let mut seen = std::collections::HashSet::new();
    let mut rows: Vec<(usize, String, Option<String>)> = Vec::new();
    let mut files: Vec<(String, std::collections::BTreeMap<u64, String>)> = Vec::new();
    for entry in responses.iter().flat_map(|response| &response.results) {
        if entry.scope == "blobs" {
            let in_origin = entry
                .reference
                .get("project_path")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|path| Some(path) == origin);
            if in_origin {
                continue;
            }
            let heading = blob_heading(entry);
            let index = match files.iter().position(|(name, _)| *name == heading) {
                Some(index) => index,
                None => {
                    files.push((heading, Default::default()));
                    files.len() - 1
                }
            };
            for found in &entry.matches {
                for (offset, line) in found.text.lines().enumerate() {
                    files[index]
                        .1
                        .entry(found.start_line + offset as u64)
                        .or_insert_with(|| line.to_string());
                }
            }
            continue;
        }
        let (label, readable) = label(entry, home);
        if seen.insert(label.clone()) {
            let mut row = label;
            if let Some(state) = &entry.state {
                let _ = write!(row, " {state}");
            }
            if !entry.text.is_empty() {
                let _ = write!(row, "  {}", entry.text);
            }
            rows.push((scope_order(&entry.scope), row, readable));
        }
    }
    if rows.is_empty() && files.is_empty() {
        return Outcome::Empty;
    }
    rows.sort_by_key(|(order, _, _)| *order);
    let next = rows.iter().find_map(|(_, _, readable)| readable.clone());
    let statuses = responses.iter().flat_map(|response| &response.scopes);
    let truncated = responses.iter().any(|response| response.truncated);
    let mut failed: Vec<String> = Vec::new();
    for scope in statuses {
        let code = scope
            .reason
            .as_ref()
            .map_or("", |reason| reason.code.as_str());
        let entry = format!("{} ({code})", scope.name);
        let quiet = matches!(scope.status.as_str(), "ok" | "skipped" | "")
            || code == "not_available_for_search_type";
        if !quiet && !failed.contains(&entry) {
            failed.push(entry);
        }
    }
    let mut out = format!(
        "GitLab {query:?} in {project}{}:\n",
        if truncated { ", truncated" } else { "" }
    );
    for (_, row, _) in &rows {
        let _ = writeln!(out, "{row}");
    }
    for (heading, lines) in &files {
        let _ = writeln!(out, "{heading}");
        let mut last = None;
        for (number, line) in lines {
            if last.is_some_and(|last| number - last > 1) {
                let _ = writeln!(out, "--");
            }
            let lower = line.to_lowercase();
            let mark = match terms.iter().any(|term| lower.contains(term.as_str())) {
                true => ':',
                false => '-',
            };
            let _ = writeln!(out, "{number}{mark}{line}");
            last = Some(*number);
        }
    }
    if !failed.is_empty() {
        let _ = writeln!(out, "unavailable: {}", failed.join(", "));
    }
    if let Some(token) = next {
        let _ = writeln!(
            out,
            "next: {} context {token}",
            crate::commands::setup::spec::launcher()
        );
    }
    Outcome::Found(out)
}

fn blob_heading(entry: &Entry) -> String {
    let reference = &entry.reference;
    let path = reference
        .get("path")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(&entry.text);
    match reference
        .get("project_path")
        .and_then(serde_json::Value::as_str)
    {
        Some(project) => format!("{project}/{path}"),
        None => match reference.get("project_id") {
            Some(id) => format!("Project[{id}] {path}"),
            None => path.to_string(),
        },
    }
}

fn reference(kind: &str, sigil: char, path: Option<&str>, iid: i64, home: Option<&str>) -> String {
    match path.filter(|path| Some(*path) != home) {
        Some(path) => format!("{kind}:{path}{sigil}{iid}"),
        None => format!("{kind}:{iid}"),
    }
}

fn parent(reference: &str, home: Option<&str>) -> Option<String> {
    let (path, sigil, kind) = match reference.rsplit_once('!') {
        Some((path, _)) => (path, '!', "mr"),
        None => (reference.rsplit_once('#')?.0, '#', "issue"),
    };
    let iid = reference[path.len() + 1..].parse().ok()?;
    Some(self::reference(kind, sigil, Some(path), iid, home))
}

fn label(entry: &Entry, home: Option<&str>) -> (String, Option<String>) {
    let field = |name: &str| entry.reference.get(name);
    let iid = field("iid").and_then(serde_json::Value::as_i64);
    let path = field("project_path").and_then(serde_json::Value::as_str);
    let readable = match (entry.scope.as_str(), iid) {
        ("merge_requests", Some(iid)) => Some(reference("mr", '!', path, iid, home)),
        ("work_items", Some(iid)) => Some(reference("issue", '#', path, iid, home)),
        _ => None,
    };
    if let Some(readable) = readable {
        return (readable.clone(), Some(readable));
    }
    if entry.scope == "notes" {
        let on = field("noteable_reference")
            .and_then(serde_json::Value::as_str)
            .and_then(|noteable| parent(noteable, home))
            .or_else(|| {
                field("noteable")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            });
        return match on {
            Some(on) => (format!("comment on {on}"), Some(on)),
            None => ("comment".to_string(), None),
        };
    }
    if let Some(sha) = field("sha").and_then(serde_json::Value::as_str) {
        let commit = format!("commit:{sha}");
        let readable = (entry.scope == "commits").then(|| commit.clone());
        return (commit, readable);
    }
    match &entry.token {
        Some(token) => (token.clone(), Some(token.clone())),
        None => (format!("{} {}", entry.entity_type, entry.reference), None),
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
            r#"{"target":{"type":"Project","full_path":"group/project"},
               "scopes":[{"name":"merge_requests","status":"ok"},{"name":"notes","status":"error","reason":{"code":"timeout"}},
                {"name":"users","status":"unavailable","reason":{"code":"not_available_for_search_type"}}],
               "results":[{"scope":"merge_requests","type":"MergeRequest","token":"MergeRequest[42]","reference":{"id":42,"iid":4,"project_path":"group/project"},"state":"merged","text":"Add scoped search"}],
               "truncated":false}"#,
        );
        let second = response(
            r#"{"scopes":[],"results":[
                {"scope":"commits","type":"Commit","token":null,"reference":{"project_id":1,"sha":"0123456789abcdef0123"},"text":"Fix grep"},
                {"scope":"merge_requests","type":"MergeRequest","token":"MergeRequest[42]","reference":{"id":42,"iid":4,"project_path":"group/project"},"state":"merged","text":"Add scoped search"},
                {"scope":"notes","type":"Note","token":"Note[9]","reference":{"id":9,"noteable":"MergeRequest[42]","noteable_reference":"group/project!4"},"text":"grep should search notes"},
                {"scope":"work_items","type":"WorkItem","token":"WorkItem[7]","reference":{"id":7,"iid":2,"project_path":"group/other"},"state":"opened","text":"Grep issue"},
                {"scope":"blobs","type":"File","token":null,"reference":{"project_id":3,"project_path":"g/lib","path":"a.go"},"text":"a.go",
                 "matches":[{"start_line":10,"end_line":11,"text":"// helper\nfunc Grep() {"}]},
                {"scope":"blobs","type":"File","token":null,"reference":{"project_id":3,"project_path":"g/lib","path":"a.go"},"text":"a.go",
                 "matches":[{"start_line":11,"end_line":11,"text":"func Grep() {"},{"start_line":20,"end_line":20,"text":"x := grep()"}]}],
               "truncated":true}"#,
        );
        let keywords = ["grep".to_string(), "search".to_string()];
        let Outcome::Found(text) = render(
            "gitlab.com/group/project",
            "grep|search",
            &keywords,
            &[first, second],
            None,
        ) else {
            panic!("expected results");
        };
        assert_eq!(
            text,
            format!(
                "GitLab \"grep|search\" in gitlab.com/group/project, truncated:\n\
                 issue:group/other#2 opened  Grep issue\n\
                 mr:4 merged  Add scoped search\n\
                 commit:0123456789abcdef0123  Fix grep\n\
                 comment on mr:4  grep should search notes\n\
                 g/lib/a.go\n\
                 10-// helper\n\
                 11:func Grep() {{\n\
                 --\n\
                 20:x := grep()\n\
                 unavailable: notes (timeout)\n\
                 next: {} context issue:group/other#2\n",
                crate::commands::setup::spec::launcher()
            )
        );
    }
}
