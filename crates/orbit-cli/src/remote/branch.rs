use std::fmt::Write as _;
use std::path::Path;
use std::sync::LazyLock;

use serde_json::Value;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};

const MAX_CANDIDATES: usize = 20;
const MAX_SEARCH_WORDS: usize = 3;
const MAX_FAILED_JOBS: usize = 5;
const TRACED_JOBS: usize = 3;
const TRACE_LINES: usize = 30;

static TERMINAL_NOISE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"\x1b\[[0-9;]*[A-Za-z]|section_(?:start|end):\d+:[^\r\n]*")
        .expect("terminal noise pattern is valid")
});

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Found {
    MergeRequest { branch: String, token: String },
    Pipeline { branch: String, text: String },
}

fn error(message: impl Into<String>) -> RemoteError {
    RemoteError::new(EXIT_GENERIC, message.into())
}

pub(crate) fn current(dir: &Path) -> Result<String, RemoteError> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["symbolic-ref", "--short", "-q", "HEAD"])
        .output()
        .map_err(|e| error(format!("could not run git: {e}")))?;
    let name = String::from_utf8_lossy(&output.stdout).trim().to_string();
    match output.status.success() && !name.is_empty() {
        true => Ok(name),
        false => Err(error("no current branch; pass --branch NAME")),
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
    words.dedup();
    words
}

pub(crate) fn pick(wanted: &str, candidates: &[String]) -> Result<String, String> {
    let wanted_words = words(wanted);
    let score = |name: &String| {
        let lower = name.to_lowercase();
        wanted_words
            .iter()
            .filter(|word| lower.contains(word.as_str()))
            .count()
    };
    let best = candidates.iter().map(score).max().unwrap_or(0);
    let top: Vec<&String> = candidates
        .iter()
        .filter(|name| best > 0 && score(name) == best)
        .collect();
    match top.as_slice() {
        [only] => Ok((*only).clone()),
        [] => Err(format!("no branch matches {wanted:?}")),
        many => Err(format!(
            "branches matching {wanted:?}: {}; pass one with --branch",
            many.iter()
                .take(MAX_CANDIDATES)
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

async fn branch_named(
    client: &OrbitClient,
    project: &str,
    wanted: &str,
) -> Result<String, RemoteError> {
    let path = format!("projects/{project}/repository/branches");
    let exact = get_json(client, &path, &[("search", format!("^{wanted}$"))]).await?;
    if names(&exact).iter().any(|name| name == wanted) {
        return Ok(wanted.to_string());
    }
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
    pick(wanted, &candidates).map_err(error)
}

pub(crate) async fn resolve(
    client: &OrbitClient,
    project: &str,
    wanted: &str,
) -> Result<Found, RemoteError> {
    let project = project.replace('/', "%2F");
    let branch = branch_named(client, &project, wanted).await?;
    let merge_requests = get_json(
        client,
        &format!("projects/{project}/merge_requests"),
        &[
            ("source_branch", branch.clone()),
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
    if let Some(id) = chosen.and_then(|mr| mr["id"].as_i64()) {
        return Ok(Found::MergeRequest {
            branch,
            token: format!("MergeRequest[{id}]"),
        });
    }
    let text = pipeline(client, &project, &branch).await?;
    Ok(Found::Pipeline { branch, text })
}

async fn pipeline(
    client: &OrbitClient,
    project: &str,
    branch: &str,
) -> Result<String, RemoteError> {
    let pipelines = get_json(
        client,
        &format!("projects/{project}/pipelines"),
        &[("ref", branch.to_string()), ("per_page", "1".into())],
    )
    .await?;
    let Some(pipeline) = pipelines.as_array().and_then(|list| list.first()) else {
        return Ok(format!("branch {branch}: no merge request or pipeline\n"));
    };
    let id = pipeline["id"].as_i64().unwrap_or_default();
    let mut out = format!("branch {branch}: no merge request\n");
    let _ = writeln!(
        out,
        "pipeline {id} {} {} {}",
        pipeline["status"].as_str().unwrap_or("unknown"),
        pipeline["sha"]
            .as_str()
            .unwrap_or_default()
            .chars()
            .take(12)
            .collect::<String>(),
        pipeline["web_url"].as_str().unwrap_or_default()
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
        let _ = writeln!(
            out,
            "  {} ({}) {}",
            job["name"].as_str().unwrap_or_default(),
            flags.join(", "),
            job["web_url"].as_str().unwrap_or_default()
        );
        if index >= TRACED_JOBS {
            continue;
        }
        let Some(job_id) = job["id"].as_i64() else {
            continue;
        };
        if let Ok(trace) = client
            .get_api(&format!("projects/{project}/jobs/{job_id}/trace"), &[])
            .await
        {
            for line in trace_tail(&String::from_utf8_lossy(&trace)) {
                let _ = writeln!(out, "    | {line}");
            }
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
    fn a_description_picks_the_branch_with_the_most_words() {
        let candidates = names(&["aa/show-approval-rules", "fix-approvals", "main"]);
        assert_eq!(
            pick("the approval rules branch", &candidates),
            Ok("aa/show-approval-rules".to_string())
        );
    }

    #[test]
    fn ties_list_the_candidates() {
        let candidates = names(&["approval-a", "approval-b"]);
        let listed = pick("approval", &candidates).unwrap_err();
        assert!(listed.contains("approval-a, approval-b"), "{listed}");
        assert!(pick("deploy", &candidates).is_err());
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
