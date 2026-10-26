use std::path::Path;

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

/// The context endpoint token for a target in `project`.
pub(crate) fn token(project: &str, target: &Target) -> String {
    match target {
        Target::Branch(name) => format!("{project}@{name}"),
        Target::Commit(sha) => format!("{project}@{sha}"),
        Target::Pipeline(id) => format!("Pipeline[{id}]"),
        Target::Job(id) => format!("Job[{id}]"),
    }
}

/// Reads a target from the context endpoint. A branch with a merge request returns the merge
/// request, so the caller reads it in full, as for any merge request.
pub(crate) async fn read(
    client: &OrbitClient,
    project: &str,
    target: &Target,
) -> Result<Found, RemoteError> {
    let token = token(project, target);
    if let Target::Branch(branch) = target {
        let entity = super::context::fetch_entity(client, &token).await?;
        if entity["found"] != true {
            return Ok(Found::Text(format!(
                "branch:{branch} not found; find one with `{} grep 'branch:words'`\n",
                crate::commands::setup::spec::launcher()
            )));
        }
        if let Some(found) = merge_request(branch, &entity["summary"]["merge_request"]) {
            return Ok(found);
        }
    }
    let body = match target {
        Target::Job(_) => super::context::fetch(client.clone(), &[token]).await?,
        _ => super::context::fetch_summary(client.clone(), &[token]).await?,
    };
    Ok(Found::Text(super::context::tidy(
        &String::from_utf8_lossy(&body),
        false,
    )))
}

fn merge_request(branch: &str, link: &serde_json::Value) -> Option<Found> {
    let path = link["project_path"].as_str()?;
    let iid = link["iid"].as_i64()?;
    let title = link["title"].as_str().unwrap_or_default();
    Some(Found::MergeRequest {
        note: format!("branch:{branch} mr:{iid} {title}\n"),
        reference: format!("{path}!{iid}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn targets_become_context_tokens() {
        assert_eq!(
            token("g/p", &Target::Branch("feature/x".into())),
            "g/p@feature/x"
        );
        assert_eq!(
            token("g/p", &Target::Commit("1a2b3c4".into())),
            "g/p@1a2b3c4"
        );
        assert_eq!(token("g/p", &Target::Pipeline(7)), "Pipeline[7]");
        assert_eq!(token("g/p", &Target::Job(9)), "Job[9]");
    }

    #[test]
    fn a_branch_merge_request_link_becomes_its_reference() {
        let link = json!({"iid": 42, "project_path": "g/p", "title": "Fix it", "token": "MergeRequest[5]"});

        assert_eq!(
            merge_request("feature", &link),
            Some(Found::MergeRequest {
                note: "branch:feature mr:42 Fix it\n".into(),
                reference: "g/p!42".into(),
            })
        );
        assert_eq!(merge_request("feature", &serde_json::Value::Null), None);
    }
}
