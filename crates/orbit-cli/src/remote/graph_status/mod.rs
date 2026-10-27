mod scope;
mod summary;

use std::io::IsTerminal;

use cynic::{GraphQlError, GraphQlErrorPathSegment, QueryBuilder};
use orbit_cli_graphql::queries::graph_status::{GraphStatus, GraphStatusVariables, OrbitStatus};
use toon_format::{EncodeOptions, encode};

pub(crate) use self::scope::Scope;
use self::summary::Summary;
use super::api::ErrorExtensions;
use super::client::OrbitClient;
use super::error::{
    EXIT_FORBIDDEN, EXIT_GENERIC, RemoteError, build_graphql_error, build_not_found_error,
};
use super::legacy;
use super::status::select_status_output;
use super::{ResponseFormat, pretty_value, write_stdout};
use crate::tui;

struct ScopeStatus {
    orbit: OrbitStatus,
    field_errors: Vec<GraphQlError<ErrorExtensions>>,
}

pub(crate) async fn run_graph_status(
    full_path: Option<String>,
    namespace_id: Option<i64>,
    project_id: Option<i64>,
    format: Option<ResponseFormat>,
) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let scope = match Scope::from_flags(full_path, namespace_id, project_id) {
        Some(scope) => scope,
        None => Scope::read_origin_project(&client.get_host()?)?,
    };
    let summarize = format.is_none() && std::io::stdout().is_terminal();
    if summarize {
        tui::intro(format!("Orbit graph status · {scope}"))?;
    }

    let Some(status) = read_status_with_progress(&client, &scope, summarize).await? else {
        return legacy::run_graph_status(&client, &scope, format, summarize).await;
    };
    match summarize {
        true => show_summary(&status.orbit)?,
        false => write_stdout(&format_status(&status.orbit, format)?)?,
    }
    status.check_complete()
}

impl OrbitClient {
    pub(crate) async fn check_graph_status(&self, full_path: String) -> Result<(), RemoteError> {
        let scope = Scope::FullPath(full_path);
        match read_scope_status(self, &scope).await? {
            Some(_) => Ok(()),
            None => legacy::check_graph_status(self, &scope).await,
        }
    }
}

async fn read_status_with_progress(
    client: &OrbitClient,
    scope: &Scope,
    show_progress: bool,
) -> Result<Option<ScopeStatus>, RemoteError> {
    let read = read_scope_status(client, scope);
    if !show_progress {
        return read.await;
    }
    let spinner = tui::spinner("Reading graph status");
    let status = read
        .await
        .inspect_err(|_| spinner.fail("Could not read graph status"))?;
    spinner.clear();
    Ok(status)
}

fn show_summary(orbit: &OrbitStatus) -> Result<(), RemoteError> {
    let summary = Summary::new(orbit);
    tui::card("Indexing", summary.format_indexing())?;
    tui::card("In the graph", summary.format_contents())?;
    tui::outro(summary.describe_next_step())?;
    Ok(())
}

fn format_status(
    orbit: &OrbitStatus,
    format: Option<ResponseFormat>,
) -> Result<Vec<u8>, RemoteError> {
    match format {
        Some(ResponseFormat::Llm) => encode(orbit, &EncodeOptions::default())
            .map(String::into_bytes)
            .map_err(|error| {
                RemoteError::new(
                    EXIT_GENERIC,
                    format!("could not format graph status: {error}"),
                )
            }),
        _ => Ok(pretty_value(orbit)),
    }
}

impl ScopeStatus {
    fn check_complete(self) -> Result<(), RemoteError> {
        if self.field_errors.is_empty() {
            return Ok(());
        }
        Err(build_graphql_error(
            "graph status is incomplete",
            &self.field_errors,
        ))
    }
}

async fn read_scope_status(
    client: &OrbitClient,
    scope: &Scope,
) -> Result<Option<ScopeStatus>, RemoteError> {
    let full_path = scope.resolve_full_path(client).await?;
    let Some(response) = client
        .graphql(&GraphStatus::build(GraphStatusVariables {
            full_path: (&full_path).into(),
        }))
        .await?
    else {
        return Ok(None);
    };
    let errors = response.errors.unwrap_or_default();
    let scope_orbit = response.data.and_then(|data| {
        data.group
            .map(|group| group.orbit)
            .or(data.project.map(|project| project.orbit))
    });

    match scope_orbit {
        Some(Some(orbit)) if errors.iter().all(is_orbit_field_error) => {
            if errors.is_empty() && orbit.indexing.is_none() {
                return Err(explain_missing_status(client, scope).await);
            }
            Ok(Some(ScopeStatus {
                orbit,
                field_errors: errors,
            }))
        }
        _ if !errors.is_empty() => Err(build_graphql_error("graph status query failed", &errors)),
        Some(_) => Ok(None),
        None => Err(build_not_found_error("Group or project")),
    }
}

fn is_orbit_field_error(error: &GraphQlError<ErrorExtensions>) -> bool {
    error.path.as_deref().is_some_and(|path| {
        path.len() > 2
            && matches!(&path[1], GraphQlErrorPathSegment::Field(field) if field == "orbit")
    })
}

async fn explain_missing_status(client: &OrbitClient, scope: &Scope) -> RemoteError {
    let unavailable = match client.get_status().await {
        Ok(body) => select_status_output(&body).err(),
        Err(error) => Some(error),
    };
    unavailable.unwrap_or_else(|| {
        RemoteError::new(
            EXIT_FORBIDDEN,
            format!("Orbit is not turned on for {scope}, or your role is below Reporter."),
        )
    })
}
