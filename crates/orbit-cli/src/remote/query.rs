use std::io::Read;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};
use super::{ResponseFormat, write_stdout_raw};

const DEFAULT_QUERY_FORMAT: &str = "llm";
const BOM: &[u8] = &[0xEF, 0xBB, 0xBF];

pub(crate) enum QueryInput {
    Text(String),
    File(String),
}

pub(crate) async fn run_query(
    input: QueryInput,
    format_override: Option<ResponseFormat>,
) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let request_body = match input {
        QueryInput::Text(query) => build_text_request(&query, format_override)?,
        QueryInput::File(path) => build_query_request(&read_query_body(&path)?, format_override)?,
    };
    let response = match client.query_raw(request_body).await {
        Ok(response) => response,
        Err(error) => return Err(with_schema(&client, error).await),
    };
    write_stdout_raw(&response)
}

const SCHEMA_ERRORS: [&str; 3] = [
    "is unknown or unavailable in this scope",
    "unknown entity",
    "unknown relationship type",
];

async fn with_schema(client: &OrbitClient, mut error: RemoteError) -> RemoteError {
    if !SCHEMA_ERRORS
        .iter()
        .any(|pattern| error.message.contains(pattern))
    {
        return error;
    }
    let Ok(request) = build_text_request("CALL db.schema()", Some(ResponseFormat::Llm)) else {
        return error;
    };
    if let Some(schema) = client.schema_overview(request).await {
        error.message.push_str("\n\n");
        error.message.push_str(schema.trim_end());
    }
    error
}

fn read_query_body(path: &str) -> anyhow::Result<Vec<u8>> {
    if path == "-" {
        let mut buf = Vec::new();
        std::io::stdin()
            .lock()
            .read_to_end(&mut buf)
            .context("failed to read query body from stdin")?;
        return Ok(buf);
    }
    std::fs::read(path).with_context(|| format!("failed to read query body from {path}"))
}

fn build_text_request(query: &str, format: Option<ResponseFormat>) -> Result<Vec<u8>, RemoteError> {
    if query.trim().is_empty() {
        return Err(RemoteError::new(EXIT_GENERIC, "query body is empty"));
    }
    serialize_request(&serde_json::json!({
        "query": query,
        "response_format": format.map_or(DEFAULT_QUERY_FORMAT, ResponseFormat::as_str),
    }))
}

fn build_query_request(
    body: &[u8],
    format_override: Option<ResponseFormat>,
) -> Result<Vec<u8>, RemoteError> {
    let body = body.strip_prefix(BOM).unwrap_or(body);
    if body.is_empty() {
        return Err(RemoteError::new(EXIT_GENERIC, "query body is empty"));
    }

    #[derive(Deserialize)]
    struct Envelope {
        query: Option<serde_json::Value>,
        response_format: Option<String>,
    }

    let envelope: Envelope = serde_json::from_slice(body).map_err(|e| {
        RemoteError::new(EXIT_GENERIC, format!("query body is not valid JSON: {e}"))
    })?;
    let query = match envelope.query {
        Some(serde_json::Value::String(query)) => query,
        Some(_) => {
            return Err(RemoteError::new(
                EXIT_GENERIC,
                "the top-level `query` field must be a GQL string; run `glab orbit skills get orbit-gql-migration` to convert a JSON query",
            ));
        }
        None => {
            return Err(RemoteError::new(
                EXIT_GENERIC,
                "query body must contain a top-level `query` field",
            ));
        }
    };
    let format = match format_override {
        Some(format) => format.as_str().to_string(),
        None => envelope
            .response_format
            .unwrap_or_else(|| DEFAULT_QUERY_FORMAT.to_string()),
    };
    if query.trim().is_empty() {
        return Err(RemoteError::new(EXIT_GENERIC, "query body is empty"));
    }
    serialize_request(&serde_json::json!({
        "query": query,
        "response_format": format,
    }))
}

fn serialize_request(request: &impl Serialize) -> Result<Vec<u8>, RemoteError> {
    serde_json::to_vec(request).map_err(|e| {
        RemoteError::new(
            EXIT_GENERIC,
            format!("failed to serialize query request: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUERY: &str = "MATCH (p:Project {id: 1}) RETURN p";

    fn body(rest: &str) -> Vec<u8> {
        format!(r#"{{"query":"{QUERY}"{rest}}}"#).into_bytes()
    }

    #[test]
    fn query_flag_overrides_body_and_default() {
        let out = build_query_request(
            &body(r#","response_format":"raw""#),
            Some(ResponseFormat::Llm),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["response_format"], "llm");
        assert_eq!(value["query"], QUERY);
    }

    #[test]
    fn query_body_format_used_when_no_flag() {
        let out = build_query_request(&body(r#","response_format":"raw""#), None).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["response_format"], "raw");
    }

    #[test]
    fn query_defaults_to_llm_when_unspecified() {
        let out = build_query_request(&body(""), None).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["response_format"], "llm");
    }

    #[test]
    fn query_object_is_rejected() {
        let err =
            build_query_request(br#"{"query":{"query_type":"traversal"}}"#, None).unwrap_err();
        assert!(
            err.message.contains("must be a GQL string"),
            "{}",
            err.message
        );
    }

    #[test]
    fn query_strips_leading_utf8_bom() {
        let mut bytes = vec![0xEF, 0xBB, 0xBF];
        bytes.extend_from_slice(&body(""));
        let out = build_query_request(&bytes, None).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["response_format"], "llm");
    }

    #[test]
    fn query_missing_top_level_query_is_rejected() {
        let err = build_query_request(br#"{"response_format":"raw"}"#, None).unwrap_err();
        assert!(err.message.contains("top-level `query`"));
    }

    #[test]
    fn query_invalid_json_is_rejected() {
        let err = build_query_request(b"not json", None).unwrap_err();
        assert!(err.message.contains("not valid JSON"));
    }
}
