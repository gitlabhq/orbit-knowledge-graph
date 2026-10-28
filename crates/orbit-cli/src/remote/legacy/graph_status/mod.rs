mod summary;

use serde::Deserialize;
use serde::de::DeserializeOwned;

use self::summary::GraphStatus;
use crate::remote::client::OrbitClient;
use crate::remote::error::{EXIT_GENERIC, RemoteError};
use crate::remote::graph_status::Scope;
use crate::remote::{ResponseFormat, pretty_json, write_stdout};
use crate::tui;

#[derive(Deserialize)]
struct FormattedText {
    formatted_text: String,
}

pub(crate) async fn run_graph_status(
    client: &OrbitClient,
    scope: &Scope,
    format: Option<ResponseFormat>,
    summarize: bool,
) -> Result<(), RemoteError> {
    let mut params = vec![scope.to_query_param()];
    if let Some(format) = format {
        params.push(("response_format", format.as_str().to_string()));
    }

    match (format, summarize) {
        (_, true) => show_summary(client, &params).await,
        (Some(ResponseFormat::Llm), _) => {
            let body = client.get_graph_status(&params).await?;
            let text: FormattedText = parse_response(&body)?;
            write_stdout(text.formatted_text.as_bytes())
        }
        _ => write_stdout(&pretty_json(&client.get_graph_status(&params).await?)),
    }
}

pub(crate) async fn check_graph_status(
    client: &OrbitClient,
    scope: &Scope,
) -> Result<(), RemoteError> {
    client
        .get_graph_status(&[scope.to_query_param()])
        .await
        .map(drop)
}

async fn show_summary(
    client: &OrbitClient,
    params: &[(&'static str, String)],
) -> Result<(), RemoteError> {
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
