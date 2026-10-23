mod scope;
mod summary;

use std::io::IsTerminal;

use serde::Deserialize;
use serde_json::{Value, json};
use toon_format::{EncodeOptions, encode};

use self::scope::Scope;
use self::summary::GraphStatus;
use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, EXIT_UNAVAILABLE, RemoteError};
use super::{ResponseFormat, pretty_value, write_stdout};
use crate::tui;

const GRAPH_STATUS_QUERY: &str = "query($fullPath: ID!) { \
     group(fullPath: $fullPath) { orbit { ...status } } \
     project(fullPath: $fullPath) { orbit { ...status } } } \
     fragment status on OrbitNamespace { \
     indexing { state areas { name state } projects { indexed totalKnown gaps } } \
     itemCounts { domains { name entities { name count } } } }";
const ORBIT_STATUS_POINTERS: [&str; 2] = ["/group/orbit", "/project/orbit"];

#[derive(Deserialize)]
struct GraphqlResponse {
    #[serde(default)]
    data: Value,
    #[serde(default)]
    errors: Vec<GraphqlError>,
}

#[derive(Deserialize)]
struct GraphqlError {
    message: String,
    path: Option<Vec<Value>>,
}

struct OrbitStatus {
    status: Value,
    field_errors: Vec<String>,
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

    let status = match (format, std::io::stdout().is_terminal()) {
        (None, true) => show_summary(&client, &scope).await?,
        (Some(ResponseFormat::Llm), _) => {
            let status = read_orbit_status(&client, &scope).await?;
            write_stdout(encode_toon(&status.status)?.as_bytes())?;
            status
        }
        _ => {
            let status = read_orbit_status(&client, &scope).await?;
            write_stdout(&pretty_value(&status.status))?;
            status
        }
    };
    status.check_complete()
}

async fn show_summary(client: &OrbitClient, scope: &Scope) -> Result<OrbitStatus, RemoteError> {
    tui::intro(format!("Orbit graph status · {scope}"))?;
    let spinner = tui::spinner("Reading graph status");
    let status = read_orbit_status(client, scope)
        .await
        .inspect_err(|_| spinner.fail("Could not read graph status"))?;
    spinner.clear();

    let summary = GraphStatus::deserialize(&status.status).map_err(map_parse_error)?;
    tui::card("Indexing", summary.format_indexing())?;
    tui::card("In the graph", summary.format_contents())?;
    tui::outro(summary.describe_next_step())?;
    Ok(status)
}

impl OrbitStatus {
    fn check_complete(self) -> Result<(), RemoteError> {
        if self.field_errors.is_empty() {
            return Ok(());
        }
        Err(RemoteError::new(
            EXIT_GENERIC,
            format!(
                "graph status is incomplete: {}",
                self.field_errors.join("; ")
            ),
        ))
    }
}

async fn read_orbit_status(
    client: &OrbitClient,
    scope: &Scope,
) -> Result<OrbitStatus, RemoteError> {
    let full_path = scope.resolve_full_path(client).await?;
    let request = json!({ "query": GRAPH_STATUS_QUERY, "variables": { "fullPath": full_path } });
    let body = client
        .query_graphql(request.to_string().into_bytes())
        .await?;
    let mut response: GraphqlResponse = serde_json::from_slice(&body).map_err(map_parse_error)?;
    let status = ORBIT_STATUS_POINTERS.iter().find_map(|pointer| {
        response
            .data
            .pointer_mut(pointer)
            .filter(|status| !status.is_null())
            .map(Value::take)
    });
    let field_errors = response.errors.iter().all(is_orbit_field_error);
    let messages: Vec<String> = response
        .errors
        .into_iter()
        .map(|error| error.message)
        .collect();

    match status {
        Some(status) if messages.is_empty() || field_errors => {
            if messages.is_empty() && status["indexing"].is_null() {
                return Err(explain_missing_status(client, scope).await);
            }
            Ok(OrbitStatus {
                status,
                field_errors: messages,
            })
        }
        _ if !messages.is_empty() => Err(RemoteError::new(
            EXIT_GENERIC,
            format!("graph status query failed: {}", messages.join("; ")),
        )),
        _ => Err(RemoteError::new(
            EXIT_UNAVAILABLE,
            format!(
                "{scope} not found\n\n\
                 The Orbit API found no group or project with this path or ID, or you\n\
                 are not a member of it."
            ),
        )),
    }
}

fn is_orbit_field_error(error: &GraphqlError) -> bool {
    error
        .path
        .as_deref()
        .is_some_and(|path| path.len() > 2 && path[1] == "orbit")
}

async fn explain_missing_status(client: &OrbitClient, scope: &Scope) -> RemoteError {
    match client.get_status().await {
        Err(error) => error,
        Ok(_) => RemoteError::new(
            EXIT_UNAVAILABLE,
            format!("Orbit is not turned on for {scope}, or you cannot read it."),
        ),
    }
}

fn encode_toon(status: &Value) -> Result<String, RemoteError> {
    encode(status, &EncodeOptions::default()).map_err(|error| {
        RemoteError::new(
            EXIT_GENERIC,
            format!("could not format graph status: {error}"),
        )
    })
}

fn map_parse_error(error: serde_json::Error) -> RemoteError {
    RemoteError::new(
        EXIT_GENERIC,
        format!("unexpected graph status response: {error}"),
    )
}
