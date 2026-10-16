use std::fmt::Write as _;
use std::path::Path;
use std::sync::LazyLock;

use serde_json::Value;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};

const MAX_CANDIDATES: usize = 10;
const MAX_SEARCH_WORDS: usize = 3;
const MAX_FAILED_JOBS: usize = 5;
const TRACED_JOBS: usize = 3;
const TRACE_LINES: usize = 30;
const MAX_CHANGED_FILES: usize = 100;
const SHORT_SHA: usize = 12;

static TERMINAL_NOISE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"\x1b\[[0-9;]*[A-Za-z]|section_(?:start|end):\d+:[^\r\n]*")
        .expect("terminal noise pattern is valid")
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    Branch(String),
    Pipeline(i64),
    Job(i64),
    Commit(String),
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Target::Branch(name) => write!(f, "branch:{name}"),
            Target::Pipeline(id) => write!(f, "pipeline:{id}"),
            Target::Job(id) => write!(f, "job:{id}"),
            Target::Commit(sha) => write!(f, "commit:{sha}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Found {
    MergeRequest { note: String, reference: String },
    Text(String),
}

fn error(message: impl Into<String>) -> RemoteError {
    RemoteError::new(EXIT_GENERIC, message.into())
}

pub(crate) fn current_branch(dir: &Path) -> Result<String, RemoteError> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["symbolic-ref", "--short", "-q", "HEAD"])
        .output()
        .map_err(|e| error(format!("could not run git: {e}")))?;
    let name = String::from_utf8_lossy(&output.stdout).trim().to_string();
    match output.status.success() && !name.is_empty() {
        true => Ok(name),
        false => Err(error("no current branch; pass a branch:<name> target")),
    }
}

async fn get_json(
    client: &OrbitClient,
    path: &str,
    params: &[(&str, String)],
) -> Result<Value, RemoteError> {
    let body = client.get_api(path, params).await?;
    serde_json::from_slice(&body).map_err(|e| error(format!("unreadable response: {e}")))
}

fn text(value: &Value) -> &str {
    value.as_str().unwrap_or_default()
}

fn short(sha: &str) -> String {
    sha.chars().take(SHORT_SHA).collect()
}

fn names(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|branch| branch["name"].as_str().map(str::to_string))
        .collect()
}

fn words(text: &str) -> Vec<String> {
    let mut words: Vec<String> = text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|word| word.len() >= 2)
        .map(str::to_lowercase)
        .collect();
    words.sort();
    words.dedup();
    words
}

pub(crate) fn rank(wanted: &str, candidates: &[String]) -> Vec<String> {
    let wanted_words = words(wanted);
    let mut ranked: Vec<(usize, usize, &String)> = candidates
        .iter()
        .map(|name| {
            let lower = name.to_lowercase();
            let exact = usize::from(lower == wanted.to_lowercase());
            let matched = wanted_words
                .iter()
                .filter(|word| lower.contains(word.as_str()))
                .count();
            (exact, matched, name)
        })
        .filter(|(exact, matched, _)| exact + matched > 0)
        .collect();
    ranked.sort_by(|a, b| (b.0, b.1).cmp(&(a.0, a.1)).then(a.2.cmp(b.2)));
    ranked
        .into_iter()
        .map(|(_, _, name)| name.clone())
        .collect()
}

fn encode(project: &str) -> String {
    project.replace('/', "%2F")
}

async fn branch_exists(
    client: &OrbitClient,
    project: &str,
    name: &str,
) -> Result<bool, RemoteError> {
    let path = format!("projects/{project}/repository/branches");
    let exact = get_json(client, &path, &[("search", format!("^{name}$"))]).await?;
    Ok(names(&exact).iter().any(|found| found == name))
}

struct MergeRequest {
    iid: i64,
    state: String,
}

async fn merge_request(
    client: &OrbitClient,
    project: &str,
    branch: &str,
) -> Result<Option<MergeRequest>, RemoteError> {
    let merge_requests = get_json(
        client,
        &format!("projects/{project}/merge_requests"),
        &[
            ("source_branch", branch.to_string()),
            ("state", "all".into()),
            ("order_by", "updated_at".into()),
            ("per_page", "20".into()),
        ],
    )
    .await?;
    let list = merge_requests.as_array().cloned().unwrap_or_default();
    let chosen = list
        .iter()
        .find(|mr| mr["state"] == "opened")
        .or(list.first());
    Ok(chosen.and_then(|mr| {
        Some(MergeRequest {
            iid: mr["iid"].as_i64()?,
            state: text(&mr["state"]).to_string(),
        })
    }))
}

async fn latest_pipeline(
    client: &OrbitClient,
    project: &str,
    branch: &str,
) -> Result<Option<Value>, RemoteError> {
    let pipelines = get_json(
        client,
        &format!("projects/{project}/pipelines"),
        &[("ref", branch.to_string()), ("per_page", "1".into())],
    )
    .await?;
    Ok(pipelines.as_array().and_then(|list| list.first()).cloned())
}

pub(crate) async fn read(
    client: &OrbitClient,
    project: &str,
    target: &Target,
) -> Result<Found, RemoteError> {
    let encoded = encode(project);
    match target {
        Target::Branch(branch) => read_branch(client, project, &encoded, branch).await,
        Target::Pipeline(id) => {
            let pipeline =
                get_json(client, &format!("projects/{encoded}/pipelines/{id}"), &[]).await?;
            Ok(Found::Text(
                pipeline_text(client, &encoded, &pipeline).await?,
            ))
        }
        Target::Job(id) => Ok(Found::Text(job_text(client, &encoded, *id).await?)),
        Target::Commit(sha) => Ok(Found::Text(commit_text(client, &encoded, sha).await?)),
    }
}

async fn read_branch(
    client: &OrbitClient,
    project: &str,
    encoded: &str,
    branch: &str,
) -> Result<Found, RemoteError> {
    if let Some(mr) = merge_request(client, encoded, branch).await? {
        return Ok(Found::MergeRequest {
            note: format!("branch:{branch} mr:{} {}\n", mr.iid, mr.state),
            reference: format!("{project}!{}", mr.iid),
        });
    }
    if let Some(pipeline) = latest_pipeline(client, encoded, branch).await? {
        let mut out = format!("branch:{branch} no merge request\n");
        out.push_str(&pipeline_text(client, encoded, &pipeline).await?);
        return Ok(Found::Text(out));
    }
    let text = match branch_exists(client, encoded, branch).await? {
        true => format!("branch:{branch} no merge request or pipeline\n"),
        false => format!(
            "branch:{branch} not found; find one with `{} grep 'is:branch words'`\n",
            crate::commands::setup::spec::launcher()
        ),
    };
    Ok(Found::Text(text))
}

pub(crate) async fn search_text(
    client: &OrbitClient,
    shown: &str,
    project: &str,
    wanted: &str,
) -> Result<Option<String>, RemoteError> {
    let encoded = encode(project);
    let path = format!("projects/{encoded}/repository/branches");
    let mut by_length = words(wanted);
    by_length.sort_by_key(|word| std::cmp::Reverse(word.len()));
    let mut candidates: Vec<String> = Vec::new();
    for word in by_length.into_iter().take(MAX_SEARCH_WORDS) {
        let found = get_json(
            client,
            &path,
            &[("search", word), ("per_page", "100".into())],
        )
        .await?;
        for name in names(&found) {
            if !candidates.contains(&name) {
                candidates.push(name);
            }
        }
    }
    let ranked = rank(wanted, &candidates);
    if ranked.is_empty() {
        return Ok(None);
    }
    let tasks: Vec<_> = ranked
        .iter()
        .take(MAX_CANDIDATES)
        .map(|name| {
            let (client, project, name) = (client.clone(), encoded.clone(), name.clone());
            tokio::spawn(async move {
                let mr = merge_request(&client, &project, &name).await;
                let pipeline = latest_pipeline(&client, &project, &name).await;
                (name, mr, pipeline)
            })
        })
        .collect();
    let mut out = format!("GitLab branches matching {wanted:?} in {shown}:\n");
    for task in tasks {
        let Ok((name, mr, pipeline)) = task.await else {
            continue;
        };
        let mut row = format!("branch:{name}");
        match mr {
            Ok(Some(mr)) => {
                let _ = write!(row, "  mr:{} {}", mr.iid, mr.state);
            }
            Ok(None) => row.push_str("  no merge request"),
            Err(_) => {}
        }
        if let Ok(Some(pipeline)) = pipeline {
            let _ = write!(
                row,
                "  pipeline:{} {}",
                pipeline["id"],
                text(&pipeline["status"])
            );
        }
        let _ = writeln!(out, "{row}");
    }
    if ranked.len() > MAX_CANDIDATES {
        let _ = writeln!(out, "+{} more", ranked.len() - MAX_CANDIDATES);
    }
    let _ = writeln!(
        out,
        "next: {} context branch:{}",
        crate::commands::setup::spec::launcher(),
        ranked[0]
    );
    Ok(Some(out))
}

async fn pipeline_text(
    client: &OrbitClient,
    project: &str,
    pipeline: &Value,
) -> Result<String, RemoteError> {
    let id = pipeline["id"].as_i64().unwrap_or_default();
    let mut out = format!(
        "pipeline:{id} {} ref {} commit:{} {}\n",
        text(&pipeline["status"]),
        text(&pipeline["ref"]),
        short(text(&pipeline["sha"])),
        text(&pipeline["web_url"])
    );
    let jobs = get_json(
        client,
        &format!("projects/{project}/pipelines/{id}/jobs"),
        &[("scope[]", "failed".into()), ("per_page", "100".into())],
    )
    .await?;
    let jobs: Vec<Value> = jobs
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .take(MAX_FAILED_JOBS)
        .collect();
    if jobs.is_empty() {
        return Ok(out);
    }
    let _ = writeln!(out, "failed_jobs[{}]:", jobs.len());
    for (index, job) in jobs.iter().enumerate() {
        let _ = writeln!(out, "  {}", job_line(job));
        if index >= TRACED_JOBS {
            continue;
        }
        if let Some(job_id) = job["id"].as_i64() {
            for line in trace(client, project, job_id).await {
                let _ = writeln!(out, "    | {line}");
            }
        }
    }
    Ok(out)
}

fn job_line(job: &Value) -> String {
    let flags: Vec<&str> = [
        job["stage"].as_str(),
        job["failure_reason"].as_str(),
        job["allow_failure"]
            .as_bool()
            .filter(|allowed| *allowed)
            .map(|_| "allowed to fail"),
    ]
    .into_iter()
    .flatten()
    .collect();
    format!(
        "job:{} {} ({}) {}",
        job["id"],
        text(&job["name"]),
        flags.join(", "),
        text(&job["web_url"])
    )
}

async fn trace(client: &OrbitClient, project: &str, job: i64) -> Vec<String> {
    match client
        .get_api(&format!("projects/{project}/jobs/{job}/trace"), &[])
        .await
    {
        Ok(trace) => trace_tail(&String::from_utf8_lossy(&trace)),
        Err(_) => Vec::new(),
    }
}

async fn job_text(client: &OrbitClient, project: &str, id: i64) -> Result<String, RemoteError> {
    let job = get_json(client, &format!("projects/{project}/jobs/{id}"), &[]).await?;
    let mut out = format!(
        "{} {} pipeline:{} ref {} commit:{}\n",
        job_line(&job),
        text(&job["status"]),
        job["pipeline"]["id"],
        text(&job["ref"]),
        short(text(&job["commit"]["id"]))
    );
    for line in trace(client, project, id).await {
        let _ = writeln!(out, "  | {line}");
    }
    Ok(out)
}

async fn commit_text(
    client: &OrbitClient,
    project: &str,
    sha: &str,
) -> Result<String, RemoteError> {
    let base = format!("projects/{project}/repository/commits/{sha}");
    let commit = get_json(client, &base, &[]).await?;
    let mut out = format!(
        "commit:{} {}\n  author {} {}\n",
        short(text(&commit["id"])),
        text(&commit["title"]),
        text(&commit["author_name"]),
        text(&commit["authored_date"])
            .chars()
            .take(10)
            .collect::<String>()
    );
    let message = text(&commit["message"]);
    let body: Vec<&str> = message
        .lines()
        .skip(1)
        .skip_while(|line| line.trim().is_empty())
        .collect();
    if !body.is_empty() {
        out.push_str("  message:\n");
        for line in body {
            let _ = writeln!(out, "    {line}");
        }
    }
    if let Ok(diff) = get_json(
        client,
        &format!("{base}/diff"),
        &[("per_page", MAX_CHANGED_FILES.to_string())],
    )
    .await
    {
        let files = diff.as_array().cloned().unwrap_or_default();
        if !files.is_empty() {
            let _ = writeln!(out, "  changed_files[{}]:", files.len());
            for file in &files {
                let status = match (
                    file["new_file"].as_bool(),
                    file["deleted_file"].as_bool(),
                    file["renamed_file"].as_bool(),
                ) {
                    (Some(true), _, _) => "A",
                    (_, Some(true), _) => "D",
                    (_, _, Some(true)) => "R",
                    _ => "M",
                };
                let _ = writeln!(out, "    {status} {}", text(&file["new_path"]));
            }
        }
    }
    if let Ok(merge_requests) = get_json(client, &format!("{base}/merge_requests"), &[]).await {
        let list: Vec<String> = merge_requests
            .as_array()
            .into_iter()
            .flatten()
            .map(|mr| {
                format!(
                    "mr:{} {} {}",
                    mr["iid"],
                    text(&mr["state"]),
                    text(&mr["title"])
                )
            })
            .collect();
        if !list.is_empty() {
            let _ = writeln!(out, "  merge_requests: {}", list.join("; "));
        }
    }
    Ok(out)
}

pub(crate) fn trace_tail(trace: &str) -> Vec<String> {
    let lines: Vec<String> = trace
        .lines()
        .map(|line| {
            let line = line
                .rsplit('\r')
                .find(|part| !part.is_empty())
                .unwrap_or("");
            TERMINAL_NOISE.replace_all(line, "").trim_end().to_string()
        })
        .filter(|line| !line.is_empty())
        .collect();
    lines[lines.len().saturating_sub(TRACE_LINES)..].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn branches_rank_by_exact_name_then_matched_words() {
        let candidates = names(&["fix-approvals", "aa/show-approval-rules", "main", "rules"]);
        assert_eq!(
            rank("approval rules", &candidates),
            vec!["aa/show-approval-rules", "fix-approvals", "rules"]
        );
        assert_eq!(rank("rules", &candidates)[0], "rules");
        assert!(rank("deploy", &candidates).is_empty());
    }

    #[test]
    fn traces_keep_the_last_lines_without_terminal_codes() {
        let trace = "section_start:1:step\r\x1b[0K\x1b[32;1mRunning\x1b[0;m\nprogress 10%\rprogress 100%\n\n--- FAIL: TestX\n";
        assert_eq!(
            trace_tail(trace),
            vec!["Running", "progress 100%", "--- FAIL: TestX"]
        );
    }
}
