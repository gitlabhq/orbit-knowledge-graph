mod scope;
mod summary;

use std::io::IsTerminal;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use toon_format::{EncodeOptions, encode};

use self::scope::{Scope, build_graphql_request};
use self::summary::GraphStatus;
use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, EXIT_UNAVAILABLE, RemoteError};
use super::{ResponseFormat, pretty_value, write_stdout};
use crate::tui;

const ORBIT_STATUS_POINTERS: [&str; 2] = ["/group/orbit", "/project/orbit"];

#[derive(Deserialize)]
struct GraphqlResponse {
    data: Option<Value>,
    #[serde(default)]
    errors: Vec<GraphqlError>,
}

#[derive(Deserialize)]
struct GraphqlError {
    message: String,
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

    match (format, std::io::stdout().is_terminal()) {
        (None, true) => show_summary(&client, &scope).await,
        (Some(ResponseFormat::Llm), _) => {
            let status = read_orbit_status(&client, &scope).await?;
            write_stdout(encode_toon(&status)?.as_bytes())
        }
        _ => write_stdout(&pretty_value(&read_orbit_status(&client, &scope).await?)),
    }
}

async fn show_summary(client: &OrbitClient, scope: &Scope) -> Result<(), RemoteError> {
    tui::intro(format!("Orbit graph status · {scope}"))?;
    let spinner = tui::spinner("Reading graph status");
    let status = read_orbit_status(client, scope)
        .await
        .inspect_err(|_| spinner.fail("Could not read graph status"))?;
    spinner.clear();

    let status: GraphStatus = serde_json::from_value(status).map_err(map_parse_error)?;
    tui::card("Indexing", status.format_indexing())?;
    tui::card("In the graph", status.format_contents())?;
    tui::outro(status.describe_next_step())?;
    Ok(())
}

async fn read_orbit_status(client: &OrbitClient, scope: &Scope) -> Result<Value, RemoteError> {
    let full_path = scope.resolve_full_path(client).await?;
    let request = build_graphql_request(&full_path).to_string().into_bytes();
    let body = client.query_graphql(request).await?;
    let response: GraphqlResponse = parse_response(&body)?;
    if !response.errors.is_empty() {
        let messages: Vec<String> = response.errors.into_iter().map(|e| e.message).collect();
        return Err(RemoteError::new(
            EXIT_GENERIC,
            format!("graph status query failed: {}", messages.join("; ")),
        ));
    }

    let mut data = response.data.unwrap_or_default();
    ORBIT_STATUS_POINTERS
        .iter()
        .find_map(|pointer| {
            data.pointer_mut(pointer)
                .filter(|status| !status.is_null())
                .map(Value::take)
        })
        .ok_or_else(|| {
            RemoteError::new(
                EXIT_UNAVAILABLE,
                format!(
                    "no Orbit status for {scope}\n\n\
                     The group or project does not exist, you are not a member of it, or\n\
                     Orbit is not available for it."
                ),
            )
        })
}

fn encode_toon(status: &Value) -> Result<String, RemoteError> {
    encode(status, &EncodeOptions::default()).map_err(|error| {
        RemoteError::new(
            EXIT_GENERIC,
            format!("could not format graph status: {error}"),
        )
    })
}

fn parse_response<T: DeserializeOwned>(body: &[u8]) -> Result<T, RemoteError> {
    serde_json::from_slice(body).map_err(map_parse_error)
}

fn map_parse_error(error: serde_json::Error) -> RemoteError {
    RemoteError::new(
        EXIT_GENERIC,
        format!("unexpected graph status response: {error}"),
    )
}
