use std::fmt;
use std::process::Command;

use crate::remote::error::{EXIT_GENERIC, RemoteError};

pub(super) enum Scope {
    FullPath(String),
    NamespaceId(i64),
    ProjectId(i64),
}

impl Scope {
    pub(super) fn from_flags(
        full_path: Option<String>,
        namespace_id: Option<i64>,
        project_id: Option<i64>,
    ) -> Option<Scope> {
        full_path
            .map(Scope::FullPath)
            .or(namespace_id.map(Scope::NamespaceId))
            .or(project_id.map(Scope::ProjectId))
    }

    pub(super) fn read_origin_project(api_host: &str) -> Result<Scope, RemoteError> {
        let url = read_origin_url().ok_or_else(build_missing_scope_error)?;
        let (host, full_path) = split_host_and_path(&url).ok_or_else(build_missing_scope_error)?;
        if host != api_host {
            return Err(RemoteError::new(
                EXIT_GENERIC,
                format!(
                    "`origin` is on {host}, but Orbit uses {api_host}\n\n\
                     Pass --full-path, --namespace-id, or --project-id."
                ),
            ));
        }
        Ok(Scope::FullPath(full_path))
    }

    pub(super) fn to_query_param(&self) -> (&'static str, String) {
        match self {
            Scope::FullPath(path) => ("full_path", path.clone()),
            Scope::NamespaceId(id) => ("namespace_id", id.to_string()),
            Scope::ProjectId(id) => ("project_id", id.to_string()),
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scope::FullPath(path) => f.write_str(path),
            Scope::NamespaceId(id) => write!(f, "namespace {id}"),
            Scope::ProjectId(id) => write!(f, "project {id}"),
        }
    }
}

fn read_origin_url() -> Option<String> {
    let output = Command::new("git")
        .args(["remote", "get-url", "origin"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_string())
}

fn split_host_and_path(url: &str) -> Option<(String, String)> {
    let (host, path) = split_url_with_host(url).or_else(|| split_scp_remote(url))?;
    let full_path = path.trim_matches('/').trim_end_matches(".git").to_string();
    (!full_path.is_empty()).then_some((host, full_path))
}

fn split_url_with_host(url: &str) -> Option<(String, String)> {
    let url = reqwest::Url::parse(url).ok()?;
    Some((url.host_str()?.to_string(), url.path().to_string()))
}

fn split_scp_remote(url: &str) -> Option<(String, String)> {
    let (user_and_host, path) = url.split_once(':')?;
    let host = user_and_host.rsplit('@').next()?;
    Some((host.to_string(), path.to_string()))
}

fn build_missing_scope_error() -> RemoteError {
    RemoteError::new(
        EXIT_GENERIC,
        "no scope to inspect\n\n\
         Pass --full-path, --namespace-id, or --project-id, or run this command\n\
         inside a clone whose `origin` remote is a GitLab project.",
    )
}
