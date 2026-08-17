use std::io::Write;

use self::error::{EXIT_GENERIC, RemoteError};

pub(crate) mod client;
pub(crate) mod dsl;
pub(crate) mod error;
pub(crate) mod graph_status;
pub(crate) mod query;
pub(crate) mod schema;
pub(crate) mod status;
pub(crate) mod tools;

pub(crate) use dsl::run_dsl;
pub(crate) use graph_status::run_graph_status;
pub(crate) use query::run_query;
pub(crate) use schema::run_schema;
pub(crate) use status::run_status;
pub(crate) use tools::run_tools;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ResponseFormat {
    Llm,
    Raw,
}

impl ResponseFormat {
    fn as_str(self) -> &'static str {
        match self {
            ResponseFormat::Llm => "llm",
            ResponseFormat::Raw => "raw",
        }
    }
}

pub(crate) fn join_url(base_url: &str, path: &str) -> String {
    format!("{}{path}", base_url.trim_end_matches('/'))
}

pub(crate) fn pretty_json(body: &[u8]) -> Vec<u8> {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(value) => pretty_value(&value),
        Err(_) => body.to_vec(),
    }
}

pub(crate) fn pretty_value(value: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec_pretty(value).unwrap_or_default()
}

pub(crate) fn write_stdout(bytes: &[u8]) -> Result<(), RemoteError> {
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(bytes)
        .and_then(|()| stdout.write_all(b"\n"))
        .and_then(|()| stdout.flush())
        .map_err(|e| RemoteError::new(EXIT_GENERIC, format!("failed to write to stdout: {e}")))
}

pub(crate) fn write_stdout_raw(bytes: &[u8]) -> Result<(), RemoteError> {
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(bytes)
        .and_then(|()| stdout.flush())
        .map_err(|e| RemoteError::new(EXIT_GENERIC, format!("failed to write to stdout: {e}")))
}

#[cfg(test)]
mod tests {
    use super::client::STATUS_PATH;
    use super::*;

    #[test]
    fn url_join_trims_trailing_slash() {
        assert_eq!(
            join_url("https://example.test/", STATUS_PATH),
            "https://example.test/api/v4/orbit/status"
        );
        assert_eq!(
            join_url("https://example.test", STATUS_PATH),
            "https://example.test/api/v4/orbit/status"
        );
    }
}
