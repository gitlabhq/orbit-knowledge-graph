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
const DEFAULT_SCOPES: &[&str] = &["work_items", "merge_requests", "notes"];
const MAX_SEARCHES: usize = 5;

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

fn api_scopes(requested: &[&'static str]) -> Vec<&'static str> {
    match requested.is_empty() {
        true => DEFAULT_SCOPES.to_vec(),
        false => requested
            .iter()
            .copied()
            .filter(|scope| *scope != BRANCHES)
            .collect(),
    }
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
    let api_scopes = api_scopes(&request.scopes);
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
    let keywords: Vec<String> = keywords
        .iter()
        .filter(|k| k.trim().len() >= 2)
        .take(MAX_SEARCHES)
        .cloned()
        .collect();
    if keywords.is_empty() && !only_branches {
        return Outcome::Empty;
    }
    let mut responses = match fetch(keywords.to_vec()).await {
        Ok(responses) => responses,
        Err(outcome) => return outcome,
    };
    let mut words: Vec<String> = Vec::new();
    for word in keywords
        .iter()
        .flat_map(|keyword| keyword.split_whitespace())
    {
        if word.len() >= 2 && !words.iter().any(|seen| seen == word) {
            words.push(word.to_string());
        }
    }
    let budget = MAX_SEARCHES.saturating_sub(keywords.len());
    let mut any_word = false;
    if words.len() > keywords.len()
        && budget > 0
        && responses.iter().all(|response| response.results.is_empty())
        && let Ok(wider) = fetch(words.into_iter().take(budget).collect()).await
        && wider.iter().any(|response| !response.results.is_empty())
    {
        (responses, any_word) = (wider, true);
    }
    let shown = responses
        .iter()
        .find_map(|response| response.target.as_ref())
        .map(|target| target.full_path.clone())
        .unwrap_or_else(|| scope.1.clone());
    let shown = format!("{api_host}/{shown}");
    let mut found = Vec::new();
    let (rendered, winner) = match responses.is_empty() {
        true => (Outcome::Empty, None),
        false => render(
            &shown,
            &query,
            &request.keywords,
            &responses,
            home_project.as_deref(),
            any_word,
            !request.scopes.is_empty(),
        ),
    };
    if let Outcome::Found(mut text) = rendered {
        if let Some(winner) = winner {
            text.push_str(&inline(&client, &winner, home_project.as_deref()).await);
        }
        if any_word {
            found.push(format!(
                "» no GitLab result has every word of {query:?}; results with any of them:\n{text}"
            ));
        } else {
            found.push(text);
        }
    }
    if request.scopes.contains(&BRANCHES) {
        let kind = responses
            .iter()
            .find_map(|response| response.target.as_ref())
            .map(|target| target.kind.to_ascii_lowercase())
            .unwrap_or_default();
        let project = match &home_project {
            Some(path) => path.clone(),
            None if matches!(scope.0, "full_path" | "project_id")
                && (kind.is_empty() || kind == "project") =>
            {
                scope.1.clone()
            }
            None if found.is_empty() => {
                return Outcome::Skipped(format!(
                    "branches belong to a project; {} is not one",
                    request.target.named()
                ));
            }
            None => return Outcome::Found(found.join("\n")),
        };
        let shown = format!("{api_host}/{project}");
        let wanted = request.keywords.join(" ");
        match super::sdlc::search_text(&client, &shown, &project, &wanted).await {
            Ok(Some(text)) => found.push(text),
            Ok(None) => {}
            Err(error)
                if found.is_empty()
                    && home_project.is_none()
                    && first_line(&error.message).ends_with(" not found") =>
            {
                return Outcome::Skipped(format!(
                    "branches belong to a project; no project named {}",
                    request.target.named()
                ));
            }
            Err(error) if found.is_empty() => return outcome_for(&request.target, &error),
            Err(_) => {}
        }
    }
    match found.is_empty() {
        true if !request.scopes.is_empty() => {
            Outcome::Skipped(nothing_found(&request, &api_scopes, &scope.1))
        }
        true => Outcome::Empty,
        false => Outcome::Found(found.join("\n")),
    }
}

fn nothing_found(request: &Request, api_scopes: &[&str], project: &str) -> String {
    let launcher = crate::commands::setup::spec::launcher();
    let words = request.keywords.join(" ");
    let searched = api_scopes
        .iter()
        .map(|scope| scope.replace('_', " "))
        .collect::<Vec<_>>()
        .join(", ");
    let mut message = format!("no {searched} match {words:?} in {project}");
    if api_scopes.len() == 1 {
        let (qualifier, sibling, label) = match api_scopes[0] {
            "merge_requests" => ("mr", "issue", "issues"),
            "work_items" => ("issue", "mr", "merge requests"),
            _ => return message,
        };
        if words.chars().all(|c| c.is_ascii_digit()) {
            message.push_str(&format!(
                "; grep searches words, `{launcher} context {qualifier}:{words}` reads that one directly"
            ));
        } else {
            message.push_str(&format!("; {label}: `{launcher} grep '{sibling}:{words}'`"));
        }
    }
    message
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

pub(crate) fn first_line(message: &str) -> String {
    message.lines().next().unwrap_or_default().to_string()
}

fn top_level_group(project: &str) -> &str {
    project.split('/').next().unwrap_or(project)
}

#[derive(Debug, PartialEq, Eq)]
struct Winner {
    token: String,
    readable: String,
}

fn words(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn title_score(title: &str, terms: &[String]) -> usize {
    let words = words(title);
    terms
        .iter()
        .filter(|term| {
            words.iter().any(|word| match term.len() >= 3 {
                true => word.starts_with(term.as_str()),
                false => word == *term,
            })
        })
        .count()
}

const INLINED: &str = "inlined-gitlab";
const INLINE_MEMORY: std::time::Duration = std::time::Duration::from_secs(30 * 60);

fn inlined_path() -> Option<PathBuf> {
    crate::workspace::Workspace::default_root()
        .ok()
        .map(|root| root.join("var").join(INLINED))
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

fn inlined_recently(record: &str, token: &str, now: u64) -> bool {
    record.lines().any(|line| {
        line.split_once('\t').is_some_and(|(at, shown)| {
            shown == token
                && at
                    .parse::<u64>()
                    .is_ok_and(|at| now.saturating_sub(at) < INLINE_MEMORY.as_secs())
        })
    })
}

fn remember_inlined(path: &std::path::Path, token: &str, now: u64) {
    use std::io::Write as _;
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(file, "{now}\t{token}");
    }
}

async fn inline(client: &OrbitClient, winner: &Winner, home: Option<&str>) -> String {
    let launcher = crate::commands::setup::spec::launcher();
    let next = format!("next: {launcher} context {}\n", winner.readable);
    let record = inlined_path();
    let now = unix_now();
    if let Some(path) = &record
        && std::fs::read_to_string(path)
            .is_ok_and(|text| inlined_recently(&text, &winner.token, now))
    {
        return format!(
            "next: {launcher} context {} (an earlier search showed it)\n",
            winner.readable
        );
    }
    let Ok(body) = super::context::fetch(client.clone(), std::slice::from_ref(&winner.token)).await
    else {
        return next;
    };
    let text = super::context::tidy(&String::from_utf8_lossy(&body), false);
    let text = match home {
        Some(home) => super::context::shorten(&text, home),
        None => text,
    };
    if !text.contains(" found=true") {
        return next;
    }
    if let Some(path) = &record {
        remember_inlined(path, &winner.token, now);
    }
    let (text, trimmed) = super::context::compact(&text);
    let mut out = format!("\n{launcher} context {}:\n{text}", winner.readable);
    if trimmed {
        let _ = writeln!(
            out,
            "  (trimmed; `{launcher} context {}` shows all)",
            winner.readable
        );
    }
    out
}

fn render(
    project: &str,
    query: &str,
    keywords: &[String],
    responses: &[Response],
    origin: Option<&str>,
    any_word: bool,
    scoped: bool,
) -> (Outcome, Option<Winner>) {
    let terms: Vec<String> = keywords
        .iter()
        .flat_map(|keyword| keyword.split_whitespace())
        .map(str::to_lowercase)
        .collect();
    let mut title_terms: Vec<String> = keywords.iter().flat_map(|keyword| words(keyword)).collect();
    title_terms.dedup();
    let home = origin.or_else(|| {
        responses
            .iter()
            .filter_map(|response| response.target.as_ref())
            .find(|target| target.kind.eq_ignore_ascii_case("project"))
            .map(|target| target.full_path.as_str())
    });
    let mut seen = std::collections::HashSet::new();
    let mut rows: Vec<Row> = Vec::new();
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
            rows.push(Row {
                order: scope_order(&entry.scope),
                score: title_score(&entry.text, &title_terms),
                full: keywords.iter().any(|keyword| {
                    let words = words(keyword);
                    words.len() >= 2 && title_score(&entry.text, &words) == words.len()
                }),
                text: row,
                token: entity_token(entry),
                readable,
            });
        }
    }
    if any_word {
        let best = rows.iter().map(|row| row.score).max().unwrap_or_default();
        rows.retain(|row| row.score * 2 >= best);
    }
    if rows.is_empty() && files.is_empty() {
        return (Outcome::Empty, None);
    }
    rows.sort_by_key(|row| (row.order, std::cmp::Reverse(row.score)));
    let winner = winner(&rows, scoped);
    let next = rows.iter().find_map(|row| row.readable.clone());
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
    for row in &rows {
        let _ = writeln!(out, "{}", row.text);
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
    if let (Some(token), None) = (next, &winner) {
        let _ = writeln!(
            out,
            "next: {} context {token}",
            crate::commands::setup::spec::launcher()
        );
    }
    (Outcome::Found(out), winner)
}

struct Row {
    order: usize,
    score: usize,
    full: bool,
    text: String,
    token: Option<String>,
    readable: Option<String>,
}

fn winner(rows: &[Row], scoped: bool) -> Option<Winner> {
    let candidates = rows.iter().filter(|row| row.token.is_some());
    let best = candidates.clone().max_by_key(|row| row.score)?;
    let count = candidates.clone().count();
    let tied = candidates.filter(|row| row.score == best.score).count() > 1;
    let only = count == 1 && (scoped || best.score >= 2);
    ((best.full && !tied) || only).then(|| Winner {
        token: best.token.clone().expect("candidates have tokens"),
        readable: best
            .readable
            .clone()
            .expect("merge requests and issues are readable"),
    })
}

fn entity_token(entry: &Entry) -> Option<String> {
    let sigil = match entry.scope.as_str() {
        "merge_requests" => '!',
        "work_items" => '#',
        _ => return None,
    };
    let path = entry.reference.get("project_path")?.as_str()?;
    let iid = entry.reference.get("iid")?.as_i64()?;
    Some(format!("{path}{sigil}{iid}"))
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
    fn results_merge_in_scope_order_and_report_failed_scopes() {
        let item = |scope: &str, kind: &str, id: u32| {
            format!(
                r#"{{"scope":"{scope}","type":"{kind}","token":"{kind}[{id}]","reference":{{"id":{id},"iid":{id},"project_path":"g/p"}},"state":"opened","text":"t{id}"}}"#
            )
        };
        let failed = r#""scopes":[{"name":"notes","status":"error","reason":{"code":"timeout"}}]"#;
        let first = response(&format!(
            r#"{{{failed},"results":[{}]}}"#,
            item("merge_requests", "MergeRequest", 4)
        ));
        let second = response(&format!(
            r#"{{"results":[{}]}}"#,
            item("work_items", "WorkItem", 2)
        ));
        let (Outcome::Found(text), _) =
            render("g", "a|b", &[], &[first, second], Some("g/p"), false, false)
        else {
            panic!("expected results");
        };
        let lines: Vec<&str> = text.lines().skip(1).take(3).collect();
        assert_eq!(
            lines,
            [
                "issue:2 opened  t2",
                "mr:4 opened  t4",
                "unavailable: notes (timeout)"
            ]
        );
    }

    #[test]
    fn an_item_inlined_within_the_memory_is_not_inlined_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(INLINED);
        remember_inlined(&path, "MergeRequest[5]", 10_000);
        let record = std::fs::read_to_string(&path).unwrap();
        assert!(inlined_recently(&record, "MergeRequest[5]", 10_060));
        assert!(!inlined_recently(
            &record,
            "MergeRequest[5]",
            10_000 + INLINE_MEMORY.as_secs()
        ));
    }
}
