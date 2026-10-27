use std::io::Read;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

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
    let sends_json = sends_json_query(&request_body);
    let response = match client.query_raw(request_body).await {
        Ok(response) => response,
        Err(error) => {
            let error = with_language_hint(error, sends_json);
            return Err(with_schema(&client, error).await);
        }
    };
    write_stdout_raw(&response)
}

const JSON_INSTANCE_HINT: &str = "This GitLab instance reads Orbit queries as JSON. GQL needs GitLab 19.6 or\n\
     later. On GitLab 19.4 or 19.5, ask an administrator to turn on the\n\
     `orbit_gql_queries` feature flag for you.";

const GQL_INSTANCE_HINT: &str = "This GitLab instance reads Orbit queries as GQL text, such as\n\
     `MATCH (p:Project) RETURN p.full_path LIMIT 5`. JSON query objects are not accepted.";

fn sends_json_query(request: &[u8]) -> bool {
    #[derive(Deserialize)]
    struct Sent {
        query: serde_json::Value,
    }

    match serde_json::from_slice::<Sent>(request).map(|sent| sent.query) {
        Ok(serde_json::Value::Object(_)) => true,
        Ok(serde_json::Value::String(text)) => text.trim_start().starts_with('{'),
        _ => false,
    }
}

fn with_language_hint(mut error: RemoteError, sends_json: bool) -> RemoteError {
    let message = &error.message;
    let hint = match sends_json {
        false if message.contains("parse error:") => JSON_INSTANCE_HINT,
        true if message.contains("Orbit query syntax at line ")
            || message.contains("query is invalid") =>
        {
            GQL_INSTANCE_HINT
        }
        _ => return error,
    };
    error.message.truncate(error.message.trim_end().len());
    error.message.push_str("\n\n");
    error.message.push_str(hint);
    error
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

    #[test]
    fn json_queries_are_detected_in_text_and_files() {
        let text = |query: &str| build_text_request(query, None).unwrap();
        assert!(!sends_json_query(&text("MATCH (p:Project) RETURN p")));
        assert!(sends_json_query(&text(r#" {"query_type":"traversal"}"#)));
        let file = build_query_request(br#"{"query":{"query_type":"traversal"}}"#, None).unwrap();
        assert!(sends_json_query(&file));
        let file = build_query_request(br#"{"query":"MATCH (p) RETURN p"}"#, None).unwrap();
        assert!(!sends_json_query(&file));
    }

    fn http_400(body: &str) -> RemoteError {
        RemoteError::new(EXIT_GENERIC, format!("Orbit API error (HTTP 400): {body}"))
    }

    const JSON_PARSE_ERROR: &str =
        r#"{"code":"compile_error","message":"parse error: expected value at line 1 column 1"}"#;
    const GQL_PARSE_ERROR: &str = "{\"code\":\"compile_error\",\"message\":\"schema violation: Orbit query syntax at line 1, column 1: expected Statement\\nExpected one MATCH ... RETURN statement\"}\n";
    const REJECTED_OBJECT: &str = r#"{"error":"query is invalid"}"#;

    #[test]
    fn language_hint_matches_what_was_sent_and_how_it_failed() {
        for (body, sends_json, hint) in [
            (JSON_PARSE_ERROR, false, JSON_INSTANCE_HINT),
            (GQL_PARSE_ERROR, true, GQL_INSTANCE_HINT),
            (REJECTED_OBJECT, true, GQL_INSTANCE_HINT),
        ] {
            let error = with_language_hint(http_400(body), sends_json);
            assert!(error.message.starts_with("Orbit API error (HTTP 400)"));
            assert!(
                error.message.ends_with(&format!("}}\n\n{hint}")),
                "{}",
                error.message
            );
        }
    }

    #[test]
    fn other_query_errors_get_no_language_hint() {
        for (body, sends_json) in [
            (GQL_PARSE_ERROR, false),
            (JSON_PARSE_ERROR, true),
            (r#"{"message":"parse error: missing field `nodes`"}"#, true),
            (
                r#"{"message":"schema violation: /nodes: unknown entity"}"#,
                true,
            ),
        ] {
            let error = with_language_hint(http_400(body), sends_json);
            assert!(
                !error.message.contains("This GitLab instance"),
                "{}",
                error.message
            );
        }
    }
}
