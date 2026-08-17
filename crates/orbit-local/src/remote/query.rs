use std::io::Read;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::ResponseFormat;
use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};

const QUERY_PATH: &str = "/api/v4/orbit/query";
const DEFAULT_QUERY_FORMAT: &str = "llm";

pub(crate) async fn run_query(
    source: Option<String>,
    format_override: Option<ResponseFormat>,
) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let raw_body = read_query_body(source.as_deref())?;
    let request_body = build_query_request(&raw_body, format_override)?;
    client.post_stream(QUERY_PATH, request_body, false).await
}

fn read_query_body(source: Option<&str>) -> anyhow::Result<Vec<u8>> {
    match source {
        None | Some("-") => {
            let mut buf = Vec::new();
            std::io::stdin()
                .lock()
                .read_to_end(&mut buf)
                .context("failed to read query body from stdin")?;
            Ok(buf)
        }
        Some(path) => {
            std::fs::read(path).with_context(|| format!("failed to read query body from {path}"))
        }
    }
}

fn build_query_request(
    body: &[u8],
    format_override: Option<ResponseFormat>,
) -> Result<Vec<u8>, RemoteError> {
    const BOM: &[u8] = &[0xEF, 0xBB, 0xBF];
    let body = body.strip_prefix(BOM).unwrap_or(body);
    if body.is_empty() {
        return Err(RemoteError::new(EXIT_GENERIC, "query body is empty"));
    }

    #[derive(Deserialize)]
    struct Envelope {
        query: Option<Box<RawValue>>,
        response_format: Option<String>,
    }

    let envelope: Envelope = serde_json::from_slice(body).map_err(|e| {
        RemoteError::new(EXIT_GENERIC, format!("query body is not valid JSON: {e}"))
    })?;
    let query = envelope.query.ok_or_else(|| {
        RemoteError::new(
            EXIT_GENERIC,
            "query body must contain a top-level `query` object",
        )
    })?;

    let response_format = match format_override {
        Some(format) => format.as_str().to_string(),
        None => envelope
            .response_format
            .unwrap_or_else(|| DEFAULT_QUERY_FORMAT.to_string()),
    };

    #[derive(Serialize)]
    struct Request<'a> {
        query: &'a RawValue,
        response_format: String,
    }

    serde_json::to_vec(&Request {
        query: &query,
        response_format,
    })
    .map_err(|e| {
        RemoteError::new(
            EXIT_GENERIC,
            format!("failed to serialize query request: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_flag_overrides_body_and_default() {
        let out = build_query_request(
            br#"{"query":{"a":1},"response_format":"raw"}"#,
            Some(ResponseFormat::Llm),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["response_format"], "llm");
        assert_eq!(value["query"]["a"], 1);
    }

    #[test]
    fn query_body_format_used_when_no_flag() {
        let out = build_query_request(br#"{"query":{},"response_format":"raw"}"#, None).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["response_format"], "raw");
    }

    #[test]
    fn query_defaults_to_llm_when_unspecified() {
        let out = build_query_request(br#"{"query":{}}"#, None).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["response_format"], "llm");
    }

    #[test]
    fn query_preserves_nested_query_verbatim() {
        let out = build_query_request(
            br#"{"query":{"node":{"entity":"Project","email":"a@b.com"}}}"#,
            None,
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["query"]["node"]["email"], "a@b.com");
    }

    #[test]
    fn query_strips_leading_utf8_bom() {
        let mut body = vec![0xEF, 0xBB, 0xBF];
        body.extend_from_slice(br#"{"query":{}}"#);
        let out = build_query_request(&body, None).unwrap();
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
