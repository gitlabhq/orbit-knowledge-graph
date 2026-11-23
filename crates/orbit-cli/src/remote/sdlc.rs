use std::path::Path;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Found {
    MergeRequest { note: String, reference: String },
    Text(String),
}

pub(crate) fn current_branch(dir: &Path) -> Result<String, RemoteError> {
    let no_branch = |reason: String| {
        RemoteError::new(
            EXIT_GENERIC,
            format!("no current branch ({reason}); pass a branch:<name> target"),
        )
    };
    crate::workspace::repository_at(dir)
        .current_branch_name()
        .map_err(|e| no_branch(e.to_string()))?
        .ok_or_else(|| no_branch("detached HEAD".to_string()))
}

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
    use crate::test_support::git;
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

    #[test]
    fn current_branch_names_the_checked_out_branch() {
        let repo = tempfile::TempDir::new().unwrap();
        git(repo.path(), &["init", "-b", "feature/x"]);

        assert_eq!(current_branch(repo.path()).unwrap(), "feature/x");
    }

    #[test]
    fn current_branch_fails_on_a_detached_head_and_outside_a_repository() {
        let repo = tempfile::TempDir::new().unwrap();
        let outside = current_branch(repo.path()).unwrap_err().to_string();
        assert!(outside.contains("no current branch"), "{outside}");
        assert!(outside.contains("pass a branch:<name> target"), "{outside}");

        git(repo.path(), &["init", "-b", "main"]);
        git(
            repo.path(),
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@example.com",
                "commit",
                "--allow-empty",
                "-m",
                "init",
            ],
        );
        git(repo.path(), &["checkout", "--detach"]);

        let error = current_branch(repo.path()).unwrap_err();
        assert!(error.to_string().contains("branch:<name>"), "{error}");
    }
}
