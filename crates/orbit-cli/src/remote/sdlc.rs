use std::path::Path;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Found {
    MergeRequest { note: String, reference: String },
    Text(String),
}

pub(crate) fn current_branch(dir: &Path) -> Result<String, RemoteError> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["symbolic-ref", "--short", "-q", "HEAD"])
        .output()
        .map_err(|e| RemoteError::new(EXIT_GENERIC, format!("could not run git: {e}")))?;
    let name = String::from_utf8_lossy(&output.stdout).trim().to_string();
    match output.status.success() && !name.is_empty() {
        true => Ok(name),
        false => Err(RemoteError::new(
            EXIT_GENERIC,
            "no current branch; pass a branch:<name> target",
        )),
    }
}

/// Reads a branch from the context endpoint. A branch with a merge request returns the merge
/// request, so the caller reads it in full, as for any merge request.
pub(crate) async fn read_branch(
    client: &OrbitClient,
    project: &str,
    branch: &str,
) -> Result<Found, RemoteError> {
    let token = format!("{project}@{branch}");
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
    let body = super::context::fetch(client.clone(), &[token]).await?;
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
