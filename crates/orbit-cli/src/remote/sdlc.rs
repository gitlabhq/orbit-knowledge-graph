use std::fmt::Write as _;
use std::path::Path;

use serde_json::Value;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};

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
        &[("ref", branch.to_string()), ("per_page", "20".into())],
    )
    .await?;
    let list = pipelines.as_array().cloned().unwrap_or_default();
    Ok(list
        .iter()
        .find(|pipeline| text(&pipeline["status"]) != "skipped")
        .or(list.first())
        .cloned())
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
        Target::Job(id) => {
            let body = super::context::fetch(client.clone(), &[format!("Job[{id}]")]).await?;
            Ok(Found::Text(super::context::tidy(
                &String::from_utf8_lossy(&body),
                false,
            )))
        }
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
        let note = format!("branch:{branch} mr:{} {}\n", mr.iid, mr.state);
        return Ok(Found::MergeRequest {
            note,
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
            "branch:{branch} not found; find one with `{} grep 'branch:words'`\n",
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
    for word in by_length {
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
        text(&pipeline["sha"]),
        text(&pipeline["web_url"])
    );
    let jobs = get_json(
        client,
        &format!("projects/{project}/pipelines/{id}/jobs"),
        &[("scope[]", "failed".into()), ("per_page", "100".into())],
    )
    .await?;
    let mut jobs: Vec<Value> = jobs.as_array().cloned().unwrap_or_default();
    jobs.sort_by_key(|job| job["allow_failure"].as_bool().unwrap_or(false));
    if jobs.is_empty() {
        return Ok(out);
    }
    let _ = writeln!(out, "failed_jobs[{}]:", jobs.len());
    for job in &jobs {
        let _ = writeln!(out, "  {}", job_line(job));
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

async fn commit_text(
    client: &OrbitClient,
    project: &str,
    sha: &str,
) -> Result<String, RemoteError> {
    let base = format!("projects/{project}/repository/commits/{sha}");
    let commit = get_json(client, &base, &[]).await?;
    let mut out = format!(
        "commit:{} {}\n  author {} {}\n",
        text(&commit["id"]),
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
        &[("per_page", "100".into())],
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
