use std::io::Read;
use std::path::Path;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};
use super::{ResponseFormat, write_stdout_raw};

const DEFAULT_QUERY_FORMAT: &str = "llm";
const BOM: &[u8] = &[0xEF, 0xBB, 0xBF];

pub(crate) async fn run_query(
    source: Option<String>,
    format_override: Option<ResponseFormat>,
) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let request_body = match source.as_deref() {
        Some(query) if query != "-" && !Path::new(query).is_file() => {
            build_text_request(query, format_override)?
        }
        _ => build_query_request(&read_query_body(source.as_deref())?, format_override)?,
    };
    let response = client.query_raw(request_body).await?;
    write_stdout_raw(&response)
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
        query: Option<Box<RawValue>>,
        response_format: Option<String>,
    }

    let envelope: Envelope = serde_json::from_slice(body).map_err(|e| {
        RemoteError::new(EXIT_GENERIC, format!("query body is not valid JSON: {e}"))
    })?;
    let query = envelope.query.ok_or_else(|| {
        RemoteError::new(
            EXIT_GENERIC,
            "query body must contain a top-level `query` field",
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

    serialize_request(&Request {
        query: &query,
        response_format,
    })
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

    #[test]
    fn query_envelope_preserves_gql_without_a_selector() {
        let text = " MATCH (u:User {name: 'Zoë'}) RETURN u\r\n";
        let body = serde_json::to_vec(&serde_json::json!({ "query": text })).unwrap();
        let output = build_query_request(&body, None).unwrap();
        let request: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(
            request,
            serde_json::json!({ "query": text, "response_format": "llm" })
        );
    }

    #[test]
    fn query_text_is_sent_unchanged_without_a_selector() {
        let text = " MATCH (u:User {name: 'Zoë'}) RETURN u\r\n";
        let output = build_text_request(text, Some(ResponseFormat::Raw)).unwrap();
        let request: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(
            request,
            serde_json::json!({ "query": text, "response_format": "raw" })
        );
        assert!(build_text_request(" \n", None).is_err());
    }

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
