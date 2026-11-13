use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cynic::QueryBuilder;
use orbit_cli_graphql::queries::grep_schema::GrepSchema;
use orbit_cli_graphql::queries::namespace_kind::{NamespaceKind, NamespaceKindVariables};
use serde::Deserialize;

use super::client::OrbitClient;
use super::error::{EXIT_FORBIDDEN, EXIT_GENERIC, EXIT_UNAVAILABLE, RemoteError};
use super::retry::Policy;
use super::server_schema::{Anchor, Entities, Entity};

/// Node kinds that GitLab grep searches. The API maps a kind to its scope, so requests send kinds as-is.
const SEARCHED_KINDS: &[&str] = &[
    "WorkItem",
    "MergeRequest",
    "Commit",
    "Note",
    "Milestone",
    "User",
    "Project",
    "Group",
    "Branch",
];
const BRANCH: &str = "Branch";
const MAX_SEARCHES: usize = 5;
/// GitLab rejects a search with no term this long as an abusive tiny search.
const MIN_SEARCH_CHARS: usize = 2;

pub(crate) fn provider(name: &str) -> Option<&'static str> {
    let provider = super::server_schema::PROVIDER;
    provider.eq_ignore_ascii_case(name).then_some(provider)
}

pub(crate) fn graph_kind(kind: &str) -> Option<&str> {
    let (name, entity) = kind.split_once(':')?;
    provider(name).map(|_| entity)
}

pub(crate) fn searched_kinds() -> &'static [&'static str] {
    SEARCHED_KINDS
}

/// The node kind GitLab grep searches for `kind`, a node name in any case or snake case.
pub(crate) fn searched_kind(kind: &str) -> Option<&'static str> {
    let flat = kind.replace('_', "");
    SEARCHED_KINDS
        .iter()
        .find(|node| node.eq_ignore_ascii_case(&flat))
        .copied()
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

#[derive(Debug, Clone, Default)]
pub(crate) struct Budget(Arc<AtomicUsize>);

impl Budget {
    const QUERIES: usize = 4;

    fn take(&self, wanted: usize) -> usize {
        let mut granted = 0;
        let _ = self
            .0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |sent| {
                granted = wanted.min(Self::QUERIES.saturating_sub(sent));
                Some(sent + granted)
            });
        granted
    }
}

impl Budget {
    fn refund(&self) {
        let _ = self
            .0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |sent| {
                sent.checked_sub(1)
            });
    }
}

impl PartialEq for Budget {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for Budget {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Request {
    pub(crate) target: Target,
    pub(crate) keywords: Vec<String>,
    pub(crate) kinds: Vec<&'static str>,
    pub(crate) graph: Option<String>,
    pub(crate) budget: Budget,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Found(String),
    Empty,
    NoMatches(String),
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

struct Resolved {
    client: OrbitClient,
    api_host: String,
    home_project: Option<String>,
    path: Option<String>,
}

impl Resolved {
    fn new(target: &Target) -> Result<Self, Outcome> {
        let origin = match target {
            Target::Origin(dir) => match super::origin::project(dir) {
                Some(origin) => Some(origin),
                None => return Err(Outcome::Skipped("no GitLab `origin` remote".into())),
            },
            _ => None,
        };
        let dir = match target {
            Target::Origin(dir) => dir.clone(),
            _ => PathBuf::from("."),
        };
        let client = OrbitClient::from_env_in(&dir)
            .map_err(|error| Outcome::Skipped(first_line(&error.message)))?;
        let api_host = client
            .get_host()
            .map_err(|error| Outcome::Failed(first_line(&error.message)))?;
        let (home_project, path) = match (target, origin) {
            (Target::Origin(_), Some((host, _))) if host != api_host => {
                return Err(Outcome::Skipped(format!(
                    "`origin` is on {host}, but Orbit uses {api_host}"
                )));
            }
            (Target::Origin(_), origin) => (origin.map(|(_, path)| path), None),
            (Target::Project(path), _) => (None, Some(path.clone())),
            (Target::Namespace(path), _) => match super::token::url_host(path) {
                Some(host) if host != api_host => {
                    return Err(Outcome::Skipped(format!(
                        "{path} is on {host}, but Orbit uses {api_host}"
                    )));
                }
                _ => (None, Some(super::token::namespace(path, &api_host))),
            },
            _ => (None, None),
        };
        Ok(Self {
            client,
            api_host,
            home_project,
            path,
        })
    }
}

pub(crate) async fn search(request: Request, query: String) -> Outcome {
    let resolved = match Resolved::new(&request.target) {
        Ok(resolved) => resolved,
        Err(outcome) => return outcome,
    };
    if let Some(kind) = request.graph.clone() {
        return graph_search(&request, resolved, &kind, &query).await;
    }
    let Resolved {
        client,
        api_host,
        home_project,
        path,
    } = resolved;
    let scope = match (&request.target, &home_project, path) {
        (Target::Origin(_), Some(home), _) => ("full_path", top_level_group(home).to_string()),
        (_, _, Some(path)) => ("full_path", path),
        (Target::ProjectId(id), _, _) => ("project_id", id.to_string()),
        (Target::GroupId(id), _, _) => ("namespace_id", id.to_string()),
        _ => return Outcome::Skipped("no GitLab `origin` remote".into()),
    };
    let api_scopes: Vec<String> = request.kinds.iter().map(|kind| kind.to_string()).collect();
    // Branches belong to a project, so for the `origin` they are searched in its project, not its group.
    let branch_project = home_project
        .as_ref()
        .filter(|_| request.kinds.contains(&BRANCH))
        .map(|project| ("full_path", project.clone()));
    let searches: Vec<((&str, String), Vec<String>)> = match &branch_project {
        Some(project) => {
            let others: Vec<String> = api_scopes
                .iter()
                .filter(|kind| *kind != BRANCH)
                .cloned()
                .collect();
            let mut searches = vec![(project.clone(), vec![BRANCH.to_string()])];
            if !others.is_empty() {
                searches.push((scope.clone(), others));
            }
            searches
        }
        None => vec![(scope.clone(), api_scopes.clone())],
    };
    let fetch = |keywords: Vec<String>| {
        let tasks: Vec<_> = keywords
            .into_iter()
            .flat_map(|keywords| {
                searches.iter().map(move |(target, scopes)| {
                    let mut params = vec![("search", keywords.clone()), target.clone()];
                    if !scopes.is_empty() {
                        params.push(("scopes", scopes.join(",")));
                    }
                    params
                })
            })
            .map(|params| {
                let client = client.clone();
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
    let keywords: Vec<String> = request
        .keywords
        .iter()
        .filter(|k| k.trim().chars().count() >= MIN_SEARCH_CHARS)
        .take(MAX_SEARCHES)
        .cloned()
        .collect();
    if keywords.is_empty() {
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
        if word.chars().count() >= MIN_SEARCH_CHARS && !words.iter().any(|seen| seen == word) {
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
            !request.kinds.is_empty(),
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
    match found.is_empty() {
        true if !request.kinds.is_empty() => {
            Outcome::NoMatches(nothing_found(&request, &api_scopes, &scope.1))
        }
        true => Outcome::Empty,
        false => Outcome::Found(found.join("\n")),
    }
}

fn nothing_found(request: &Request, api_scopes: &[String], project: &str) -> String {
    let launcher = crate::commands::setup::spec::launcher();
    let words = request.keywords.join(" ");
    let searched = api_scopes
        .iter()
        .map(|scope| plural(scope))
        .collect::<Vec<_>>()
        .join(", ");
    let mut message = format!("no {searched} match {words:?} in {project}");
    let qualifier = match api_scopes {
        [scope] if scope == "MergeRequest" => "mr",
        [scope] if scope == "WorkItem" => "issue",
        _ => return message,
    };
    if words.chars().all(|c| c.is_ascii_digit()) {
        message.push_str(&format!(
            "; grep searches words, `{launcher} context {qualifier}:{words}` reads that one directly"
        ));
    }
    message
}

/// `MergeRequest` reads as `merge requests`, and a default scope such as `work_items` as `work items`.
fn plural(scope: &str) -> String {
    let mut words = String::new();
    for (index, c) in scope.chars().enumerate() {
        if c.is_ascii_uppercase() && index > 0 {
            words.push(' ');
        }
        words.push(c.to_ascii_lowercase());
    }
    let words = words.replace('_', " ");
    match words.ends_with('s') {
        true => words,
        false if words.ends_with("ch") => format!("{words}es"),
        false => format!("{words}s"),
    }
}

/// The API answers 404 for a target it cannot find, cannot read, or whose top-level group has Orbit off.
fn outcome_for(target: &Target, error: &RemoteError) -> Outcome {
    let missing = first_line(&error.message).ends_with(" not found");
    match error.exit_code {
        EXIT_UNAVAILABLE if missing && target.explicit() => Outcome::NoMatches(missing_target(target)),
        EXIT_UNAVAILABLE if missing => Outcome::Skipped(
            "Orbit isn't enabled for the GitLab group of this repository's `origin`, or you can't read it"
                .into(),
        ),
        EXIT_UNAVAILABLE | EXIT_FORBIDDEN => Outcome::Skipped(first_line(&error.message)),
        _ => Outcome::Failed(first_line(&error.message)),
    }
}

fn missing_target(target: &Target) -> String {
    let name = target.named();
    let under = std::env::current_dir()
        .map(|dir| format!(" under {}", dir.display()))
        .unwrap_or_default();
    format!(
        "no such path {name}{under}, and no GitLab project or group named {name} that you can read with Orbit enabled"
    )
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
    let client = client.clone().without_retries();
    let Ok(body) = super::context::fetch(client, std::slice::from_ref(&winner.token)).await else {
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
    let mut scope_order: Vec<&str> = Vec::new();
    for name in responses
        .iter()
        .flat_map(|response| response.scopes.iter().map(|scope| scope.name.as_str()))
        .chain(
            responses
                .iter()
                .flat_map(|response| response.results.iter().map(|entry| entry.scope.as_str())),
        )
    {
        if !scope_order.contains(&name) {
            scope_order.push(name);
        }
    }
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
                order: scope_order
                    .iter()
                    .position(|name| *name == entry.scope)
                    .unwrap_or(scope_order.len()),
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
    if entry.scope == "branches"
        && let Some(name) = field("name").and_then(serde_json::Value::as_str)
    {
        let branch = format!("branch:{name}");
        return (branch.clone(), Some(branch));
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

enum GraphScope {
    Project(String),
    ProjectId(i64),
    ProjectColumn(i64),
    Group(String),
    GroupId(i64),
}

impl GraphScope {
    fn pattern(&self, entity: &str) -> String {
        let group = "(p:Project)<-[:CONTAINS*1..3]-(g:Group";
        let anchor = match self {
            GraphScope::Project(path) => format!("(p:Project {{full_path: {}}})", gql(path)),
            GraphScope::ProjectId(id) => format!("(p:Project {{id: {id}}})"),
            GraphScope::Group(path) => format!("{group} {{full_path: {}}})", gql(path)),
            GraphScope::GroupId(id) => format!("{group} {{id: {id}}})"),
            GraphScope::ProjectColumn(id) => return format!("(n:{entity} {{project_id: {id}}})"),
        };
        format!("(n:{entity})-[:IN_PROJECT]->{anchor}")
    }

    fn shown(&self) -> String {
        let depth = " (projects up to 3 levels down)";
        match self {
            GraphScope::Project(path) => path.clone(),
            GraphScope::ProjectId(id) | GraphScope::ProjectColumn(id) => format!("Project[{id}]"),
            GraphScope::Group(path) => format!("{path}{depth}"),
            GraphScope::GroupId(id) => format!("Group[{id}]{depth}"),
        }
    }
}

#[derive(Deserialize)]
struct GraphResponse {
    result: GraphResult,
}

#[derive(Deserialize)]
struct GraphResult {
    #[serde(default)]
    nodes: Vec<GraphNode>,
    #[serde(default)]
    pagination: GraphPagination,
}

#[derive(Default, Deserialize)]
struct GraphPagination {
    #[serde(default)]
    has_more: bool,
}

#[derive(Deserialize)]
struct GraphNode {
    #[serde(rename = "type")]
    kind: String,
    id: String,
    #[serde(flatten)]
    properties: serde_json::Map<String, serde_json::Value>,
}

#[derive(Default)]
struct GraphHits<'a> {
    rows: Vec<String>,
    searched: Vec<(&'a str, &'a str)>,
    skipped: Vec<(&'a str, &'a str)>,
    failed: Vec<String>,
    rejected: Vec<String>,
    error: Option<RemoteError>,
    truncated: bool,
}

impl GraphHits<'_> {
    const ROWS: usize = 10;
    const VALUE_CHARS: usize = 100;
}

enum GraphFailure {
    Rejected(String),
    Failed(RemoteError),
}

#[derive(Deserialize)]
struct GraphError {
    code: String,
    message: String,
}

async fn graph_search(request: &Request, resolved: Resolved, kind: &str, query: &str) -> Outcome {
    let launcher = crate::commands::setup::spec::launcher();
    let entities = match graph_entities(&resolved.client).await {
        Ok(entities) => entities,
        Err(outcome) => return outcome,
    };
    let wanted = graph_kind(kind).unwrap_or(kind);
    let Some((name, entity)) = find_entity(&entities, wanted) else {
        let known: Vec<&str> = entities.keys().map(String::as_str).collect();
        return Outcome::NoMatches(format!(
            "this GitLab lists no entity named {wanted}; entities: {}. `{launcher} grep -F` searches the text literally",
            known.join(", ")
        ));
    };
    let fields = match entity.searchable.as_deref() {
        None => {
            return Outcome::NoMatches(format!(
                "this GitLab doesn't say which {name} fields grep can search (it needs GitLab 19.6 or later); use `{launcher} query`"
            ));
        }
        Some([]) => {
            return Outcome::NoMatches(format!(
                "{name} has no text field to search; use `{launcher} query`"
            ));
        }
        Some(fields) => fields,
    };
    let scope = match graph_scope(request, &resolved, name, entity).await {
        Ok(scope) => scope,
        Err(outcome) => return outcome,
    };
    let terms: Vec<&str> = request
        .keywords
        .iter()
        .map(|keyword| keyword.trim())
        .collect();
    let planned: Vec<(&str, &str)> = fields
        .iter()
        .flat_map(|field| terms.iter().map(move |term| (field.as_str(), *term)))
        .collect();
    let hits = match graph_hits(&resolved, name, entity, &scope, planned, &request.budget).await {
        Ok(hits) => hits,
        Err(outcome) => return outcome,
    };
    if hits.searched.is_empty() && hits.rejected.is_empty() && hits.failed.is_empty() {
        return Outcome::NoMatches(format!(
            "{name} not searched: this grep already sent {} graph queries",
            Budget::QUERIES
        ));
    }
    let shown = format!("{}/{}", resolved.api_host, scope.shown());
    let mut notes = Vec::new();
    if !hits.rejected.is_empty() {
        notes.push(format!("not searched: {}", hits.rejected.join(", ")));
    }
    if !hits.failed.is_empty() {
        notes.push(format!("unavailable: {}", hits.failed.join(", ")));
    }
    if !hits.skipped.is_empty() {
        notes.push(format!(
            "not searched (a grep sends at most {} graph queries): {}; `{launcher} query` searches them",
            Budget::QUERIES,
            listed(&hits.skipped)
        ));
    }
    match (hits.rows.is_empty(), hits.error) {
        (true, Some(error)) if hits.searched.is_empty() && hits.rejected.is_empty() => {
            outcome_for(&request.target, &error)
        }
        (true, _) if hits.searched.is_empty() => Outcome::NoMatches(notes.join("\n")),
        (true, _) => {
            let searched = listed(&hits.searched);
            notes.insert(0, format!("no {name} matched {searched} in {shown}"));
            Outcome::NoMatches(notes.join("\n"))
        }
        (false, _) => {
            let truncated = if hits.truncated { ", truncated" } else { "" };
            let mut out = format!("GitLab {name} {query:?} in {shown}{truncated}:\n");
            for line in hits.rows.iter().chain(&notes) {
                let _ = writeln!(out, "{line}");
            }
            Outcome::Found(out)
        }
    }
}

async fn graph_hits<'a>(
    resolved: &Resolved,
    name: &str,
    entity: &Entity,
    scope: &GraphScope,
    mut pending: Vec<(&'a str, &'a str)>,
    budget: &Budget,
) -> Result<GraphHits<'a>, Outcome> {
    let mut hits = GraphHits::default();
    let mut seen = std::collections::HashSet::new();
    loop {
        let granted = budget.take(pending.len());
        if granted == 0 {
            break;
        }
        let round: Vec<_> = pending.drain(..granted).collect();
        let tasks: Vec<_> = round
            .iter()
            .map(|(field, term)| {
                let text = graph_query(name, scope, &entity.columns, field, term);
                let client = resolved.client.clone();
                tokio::spawn(async move { graph_nodes(&client, &text).await })
            })
            .collect();
        for (&(field, term), task) in round.iter().zip(tasks) {
            match task
                .await
                .map_err(|error| Outcome::Failed(error.to_string()))?
            {
                Ok(result) => {
                    hits.searched.push((field, term));
                    hits.truncated |= result.pagination.has_more;
                    for node in result.nodes.iter().filter(|node| node.kind == name) {
                        if seen.insert(node.id.clone()) {
                            hits.rows
                                .push(graph_row(name, node, &entity.columns, field));
                        }
                    }
                }
                Err(GraphFailure::Rejected(reason)) => {
                    budget.refund();
                    let reason = reason.rsplit(": ").next().unwrap_or(&reason);
                    let note = format!("{term:?} ({reason})");
                    if !hits.rejected.contains(&note) {
                        hits.rejected.push(note);
                    }
                    pending.retain(|(_, other)| *other != term);
                }
                Err(GraphFailure::Failed(error)) => {
                    hits.failed
                        .push(format!("{field} {term:?} ({})", first_line(&error.message)));
                    hits.error.get_or_insert(error);
                }
            }
        }
    }
    hits.skipped = pending;
    Ok(hits)
}

fn listed(searches: &[(&str, &str)]) -> String {
    let listed: Vec<String> = searches
        .iter()
        .map(|(field, term)| format!("{field} {term:?}"))
        .collect();
    listed.join(", ")
}

async fn graph_entities(client: &OrbitClient) -> Result<Entities, Outcome> {
    if let Some(entities) = super::server_schema::cached_entities(client) {
        return Ok(entities);
    }
    let launcher = crate::commands::setup::spec::launcher();
    let unlisted = || {
        Outcome::Skipped(format!(
            "this GitLab doesn't list Orbit entities over GraphQL; use `{launcher} query`"
        ))
    };
    let schema = client
        .graphql_with_policy(&GrepSchema::build(()), Policy::BEST_EFFORT)
        .await
        .map_err(|error| Outcome::Failed(first_line(&error.message)))?
        .and_then(|response| response.data)
        .and_then(|data| data.orbit)
        .and_then(|orbit| orbit.schema)
        .ok_or_else(unlisted)?;
    let in_project: Vec<String> = schema
        .edges
        .nodes
        .into_iter()
        .flatten()
        .flatten()
        .filter(|edge| edge.name == "IN_PROJECT")
        .flat_map(|edge| edge.endpoints.nodes.into_iter().flatten().flatten())
        .filter(|endpoint| endpoint.target.name == "Project")
        .map(|endpoint| endpoint.source.name)
        .collect();
    let entities: Entities = schema
        .nodes
        .nodes
        .into_iter()
        .flatten()
        .flatten()
        .filter(|node| super::server_schema::is_name(&node.name))
        .map(|node| {
            let properties: Vec<_> = node
                .properties
                .nodes
                .into_iter()
                .flatten()
                .flatten()
                .filter(|property| is_property(&property.name))
                .collect();
            let known = properties
                .iter()
                .any(|property| property.text_searchable.is_some());
            let mut searchable: Vec<(bool, String)> = properties
                .iter()
                .filter(|property| property.text_searchable == Some(true))
                .map(|property| (property.default_column != Some(true), property.name.clone()))
                .collect();
            searchable.sort_by_key(|(secondary, _)| *secondary);
            let anchor = match (
                in_project.contains(&node.name),
                properties
                    .iter()
                    .any(|property| property.name == "project_id"),
            ) {
                (true, _) => Anchor::Edge,
                (false, true) => Anchor::Column,
                (false, false) => Anchor::None,
            };
            let entity = Entity {
                searchable: known.then(|| searchable.into_iter().map(|(_, name)| name).collect()),
                columns: properties
                    .iter()
                    .filter(|property| property.default_column == Some(true))
                    .map(|property| property.name.clone())
                    .collect(),
                anchor,
            };
            (node.name, entity)
        })
        .collect();
    if entities.is_empty() {
        return Err(unlisted());
    }
    super::server_schema::remember_entities(client, &entities);
    Ok(entities)
}

fn is_property(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn find_entity<'a>(entities: &'a Entities, kind: &str) -> Option<(&'a str, &'a Entity)> {
    let flat = kind.replace('_', "").to_ascii_lowercase();
    [
        Some(flat.as_str()),
        flat.strip_suffix("es"),
        flat.strip_suffix('s'),
    ]
    .into_iter()
    .flatten()
    .find_map(|name| {
        entities
            .iter()
            .find(|(entity, _)| entity.eq_ignore_ascii_case(name))
    })
    .map(|(name, entity)| (name.as_str(), entity))
}

async fn graph_scope(
    request: &Request,
    resolved: &Resolved,
    name: &str,
    entity: &Entity,
) -> Result<GraphScope, Outcome> {
    let launcher = crate::commands::setup::spec::launcher();
    let path = match (&request.target, &resolved.home_project) {
        (Target::Origin(_), home) => home.clone(),
        _ => resolved.path.clone(),
    };
    let known_project = matches!(request.target, Target::Origin(_) | Target::Project(_));
    let lookup = |path: String| async move {
        namespace_kind(&resolved.client, &path)
            .await?
            .map(|named| (path, named))
            .ok_or_else(|| Outcome::NoMatches(missing_target(&request.target)))
    };
    match (entity.anchor, &request.target, path) {
        (Anchor::None, _, _) => Err(Outcome::NoMatches(format!(
            "{name} isn't in a project or group, so grep can't bound the search; use `{launcher} query`"
        ))),
        (Anchor::Edge, Target::ProjectId(id), _) => Ok(GraphScope::ProjectId(*id)),
        (Anchor::Edge, Target::GroupId(id), _) => Ok(GraphScope::GroupId(*id)),
        (Anchor::Edge, _, Some(path)) if known_project => Ok(GraphScope::Project(path)),
        (Anchor::Edge, _, Some(path)) => match lookup(path).await? {
            (path, Named::Project(_)) => Ok(GraphScope::Project(path)),
            (path, Named::Group) => Ok(GraphScope::Group(path)),
        },
        (Anchor::Column, Target::ProjectId(id), _) => Ok(GraphScope::ProjectColumn(*id)),
        (Anchor::Column, _, Some(path)) => match lookup(path).await? {
            (_, Named::Project(id)) => Ok(GraphScope::ProjectColumn(id)),
            (_, Named::Group) => Err(Outcome::NoMatches(format!(
                "{name} is searched one project at a time; name a project, not a group"
            ))),
        },
        (Anchor::Column, Target::GroupId(_), _) => Err(Outcome::NoMatches(format!(
            "{name} is searched one project at a time; name a project, not a group"
        ))),
        _ => Err(Outcome::Skipped("no GitLab `origin` remote".into())),
    }
}

#[derive(Clone, Copy)]
enum Named {
    Project(i64),
    Group,
}

async fn namespace_kind(client: &OrbitClient, path: &str) -> Result<Option<Named>, Outcome> {
    let full_path = cynic::Id::new(path);
    let data = client
        .graphql_with_policy(
            &NamespaceKind::build(NamespaceKindVariables {
                full_path: &full_path,
            }),
            Policy::BEST_EFFORT,
        )
        .await
        .map_err(|error| Outcome::Failed(first_line(&error.message)))?
        .and_then(|response| response.data);
    let id = |gid: &cynic::Id| gid.inner().rsplit('/').next()?.parse().ok();
    Ok(data.and_then(|data| match (data.project, data.group) {
        (Some(project), _) => id(&project.id).map(Named::Project),
        (None, Some(_)) => Some(Named::Group),
        _ => None,
    }))
}

fn graph_query(
    entity: &str,
    scope: &GraphScope,
    columns: &[String],
    field: &str,
    term: &str,
) -> String {
    let matched = scope.pattern(entity);
    let mut returned: Vec<String> = columns.iter().map(|column| format!(".{column}")).collect();
    if !columns.iter().any(|column| column == field) {
        returned.push(format!(".{field}"));
    }
    format!(
        "MATCH {matched} WHERE n.{field} CONTAINS {} RETURN n{{{}}} LIMIT {}",
        gql(term),
        returned.join(", "),
        GraphHits::ROWS
    )
}

async fn graph_nodes(client: &OrbitClient, query: &str) -> Result<GraphResult, GraphFailure> {
    let request = serde_json::json!({"query": query, "response_format": "raw"}).to_string();
    let (status, body) = client
        .grep_query(request.into_bytes())
        .await
        .map_err(GraphFailure::Failed)?;
    graph_result(status, &body)
}

fn graph_result(status: u16, body: &[u8]) -> Result<GraphResult, GraphFailure> {
    if status == 400 {
        return Err(match serde_json::from_slice::<GraphError>(body) {
            Ok(error) if error.code == "compile_error" => GraphFailure::Rejected(error.message),
            _ => GraphFailure::Failed(super::error::map_http_error(
                status,
                &String::from_utf8_lossy(body),
            )),
        });
    }
    serde_json::from_slice::<GraphResponse>(body)
        .map(|response| response.result)
        .map_err(|error| {
            GraphFailure::Failed(RemoteError::new(
                EXIT_GENERIC,
                format!("unreadable graph response: {error}"),
            ))
        })
}

fn graph_row(entity: &str, node: &GraphNode, columns: &[String], field: &str) -> String {
    let mut row = format!("{entity}[{}]", node.id);
    let extra = (!columns.iter().any(|column| column == field)).then_some(field);
    for column in columns
        .iter()
        .map(String::as_str)
        .filter(|c| *c != "id")
        .chain(extra)
    {
        let text = match node.properties.get(column) {
            None | Some(serde_json::Value::Null) => continue,
            Some(serde_json::Value::String(text)) => text.clone(),
            Some(value) => value.to_string(),
        };
        let text = text
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .replace(char::is_control, "");
        let text = match text.chars().count() > GraphHits::VALUE_CHARS {
            true => {
                let cut: String = text.chars().take(GraphHits::VALUE_CHARS).collect();
                format!("{cut}…")
            }
            false => text,
        };
        let _ = write!(row, " {column}={text}");
    }
    row
}

fn gql(value: &str) -> String {
    super::code::gql_string(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_search_that_finds_nothing_says_so_without_sending_the_agent_elsewhere() {
        let request = |words: &str| Request {
            target: Target::Project("g/p".into()),
            keywords: vec![words.into()],
            kinds: vec!["MergeRequest"],
            graph: None,
            budget: Budget::default(),
        };
        let scopes = ["MergeRequest".to_string()];
        assert_eq!(
            nothing_found(&request("survey"), &scopes, "g"),
            "no merge requests match \"survey\" in g"
        );
        assert!(nothing_found(&request("42"), &scopes, "g").contains("context mr:42"));
    }

    #[test]
    fn a_missing_explicit_target_names_the_missing_path_first() {
        let error = RemoteError::new(EXIT_UNAVAILABLE, "Project not found");
        let Outcome::NoMatches(message) =
            outcome_for(&Target::Namespace("packages/runner/src".into()), &error)
        else {
            panic!("expected a no-matches outcome");
        };
        assert!(
            message.starts_with("no such path packages/runner/src under "),
            "{message}"
        );
    }

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
        let failed = r#""scopes":[{"name":"work_items","status":"ok"},{"name":"merge_requests","status":"ok"},{"name":"notes","status":"error","reason":{"code":"timeout"}}]"#;
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
    fn a_graph_search_is_scoped_capped_and_returns_the_default_columns() {
        let columns = ["id".to_string(), "ref".to_string()];
        assert_eq!(
            graph_query(
                "Pipeline",
                &GraphScope::Group("g".into()),
                &columns,
                "sha",
                "it's"
            ),
            "MATCH (n:Pipeline)-[:IN_PROJECT]->(p:Project)<-[:CONTAINS*1..3]-(g:Group {full_path: 'g'}) \
             WHERE n.sha CONTAINS 'it\\'s' RETURN n{.id, .ref, .sha} LIMIT 10"
        );
        assert_eq!(
            GraphScope::ProjectColumn(7).pattern("Definition"),
            "(n:Definition {project_id: 7})"
        );
        let budget = Budget::default();
        assert_eq!([budget.take(3), budget.take(3), budget.take(1)], [3, 1, 0]);
        budget.refund();
        assert_eq!(budget.take(2), 1);
        let rejected =
            br#"{"code":"compile_error","message":"schema violation: at least 3 characters"}"#;
        assert!(matches!(
            graph_result(400, rejected),
            Err(GraphFailure::Rejected(_))
        ));
        assert!(matches!(
            graph_result(400, b"{}"),
            Err(GraphFailure::Failed(_))
        ));
        let node: GraphNode =
            serde_json::from_str(r#"{"type":"Job","id":"1","name":"a\u001b[31m\nb"}"#).unwrap();
        assert_eq!(
            graph_row("Job", &node, &columns, "name"),
            "Job[1] name=a[31m b"
        );
        assert!(!is_property("name} RETURN n LIMIT 1 //"));
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
