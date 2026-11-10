use std::fmt;
use std::path::Path;

use serde::Deserialize;

use crate::remote::client::OrbitClient;
use crate::remote::error::{EXIT_GENERIC, RemoteError};

pub(crate) enum Scope {
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
        let (host, full_path) =
            crate::remote::origin::project(Path::new(".")).ok_or_else(build_missing_scope_error)?;
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

    pub(super) async fn resolve_full_path(
        &self,
        client: &OrbitClient,
    ) -> Result<String, RemoteError> {
        let (path, params) = match self {
            Scope::FullPath(path) => return Ok(path.clone()),
            Scope::NamespaceId(id) => (
                format!("/api/v4/groups/{id}"),
                vec![("with_projects", "false".to_string())],
            ),
            Scope::ProjectId(id) => (format!("/api/v4/projects/{id}"), Vec::new()),
        };
        let body = client.get_bytes(&path, &params).await?;
        serde_json::from_slice::<Namespace>(&body)
            .map(|namespace| namespace.full_path)
            .map_err(|error| {
                RemoteError::new(
                    EXIT_GENERIC,
                    format!("unexpected response from {path}: {error}"),
                )
            })
    }
}

#[derive(Deserialize)]
struct Namespace {
    #[serde(alias = "path_with_namespace")]
    full_path: String,
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

fn build_missing_scope_error() -> RemoteError {
    RemoteError::new(
        EXIT_GENERIC,
        "no scope to inspect\n\n\
         Pass --full-path, --namespace-id, or --project-id, or run this command\n\
         inside a clone whose `origin` remote is a GitLab project.",
    )
}
