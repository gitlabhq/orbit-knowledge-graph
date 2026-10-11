mod scope;
mod summary;

use std::io::IsTerminal;

use serde::Deserialize;
use serde::de::DeserializeOwned;

use self::scope::Scope;
use self::summary::GraphStatus;
use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};
use super::{ResponseFormat, pretty_json, write_stdout};
use crate::tui;

#[derive(Deserialize)]
struct FormattedText {
    formatted_text: String,
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
    let mut params = vec![scope.to_query_param()];
    if let Some(format) = format {
        params.push(("response_format", format.as_str().to_string()));
    }

    match (format, std::io::stdout().is_terminal()) {
        (None, true) => show_summary(&client, &scope, &params).await,
        (Some(ResponseFormat::Llm), _) => {
            let body = client.get_graph_status(&params).await?;
            let text: FormattedText = parse_response(&body)?;
            write_stdout(text.formatted_text.as_bytes())
        }
        _ => write_stdout(&pretty_json(&client.get_graph_status(&params).await?)),
    }
}

async fn show_summary(
    client: &OrbitClient,
    scope: &Scope,
    params: &[(&'static str, String)],
) -> Result<(), RemoteError> {
    tui::intro(format!("Orbit graph status · {scope}"))?;
    let spinner = tui::spinner("Reading graph status");
    let body = client
        .get_graph_status(params)
        .await
        .inspect_err(|_| spinner.fail("Could not read graph status"))?;
    spinner.clear();

    let status: GraphStatus = parse_response(&body)?;
    tui::card("Indexing", status.format_indexing())?;
    tui::card("In the graph", status.format_contents())?;
    tui::outro(status.describe_next_step())?;
    Ok(())
}

fn parse_response<T: DeserializeOwned>(body: &[u8]) -> Result<T, RemoteError> {
    serde_json::from_slice(body).map_err(|error| {
        RemoteError::new(
            EXIT_GENERIC,
            format!("unexpected graph status response: {error}"),
        )
    })
}
