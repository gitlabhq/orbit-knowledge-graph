//! Authenticated calls to the remote Orbit graph. The credential is resolved
//! from the process environment: `glab orbit` injects the `ORBIT_*` triplet
//! before exec, and the `GITLAB_*` fallback lets the binary run standalone.
//!
//! The six subcommands mirror `glab orbit remote`. Their HTTP surface is the
//! `/api/v4/orbit/*` REST family and the exit-code taxonomy (2..5) matches
//! glab's `orbiterr` package so scripting agents can branch on the same codes.

use std::io::{Read, Write};
use std::time::Duration;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

const DEFAULT_GITLAB_BASE_URL: &str = "https://gitlab.com";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(120);

const STATUS_PATH: &str = "/api/v4/orbit/status";
const SCHEMA_PATH: &str = "/api/v4/orbit/schema";
const DSL_PATH: &str = "/api/v4/orbit/schema/dsl";
const TOOLS_PATH: &str = "/api/v4/orbit/tools";
const QUERY_PATH: &str = "/api/v4/orbit/query";
const GRAPH_STATUS_PATH: &str = "/api/v4/orbit/graph_status";

const DEFAULT_QUERY_FORMAT: &str = "llm";

const EXIT_GENERIC: i32 = 1;
const EXIT_UNAVAILABLE: i32 = 2;
const EXIT_UNAUTHENTICATED: i32 = 3;
const EXIT_FORBIDDEN: i32 = 4;
const EXIT_RATE_LIMITED: i32 = 5;

/// Server response shape requested via `--response-format`. `llm` is compact
/// GOON/TOON text for agents; `raw` is structured JSON suitable for `jq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ResponseFormat {
    Llm,
    Raw,
}

/// A failed remote call carrying the process exit code it should terminate
/// with. Codes 2..5 map to stable HTTP meanings (see `map_http_error`); every
/// other failure is the generic code 1.
#[derive(Debug)]
pub(crate) struct RemoteError {
    pub exit_code: i32,
    pub message: String,
}

pub(crate) async fn run_query(
    source: Option<String>,
    format_override: Option<ResponseFormat>,
) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let raw_body = read_query_body(source.as_deref())?;
    let request_body = build_query_request(&raw_body, format_override)?;

    let response = client
        .send(
            client
                .http
                .post(client.url(QUERY_PATH))
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(request_body),
        )
        .await?;

    stream_to_stdout(response, false).await
}

pub(crate) async fn run_status() -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let response = client
        .send(client.http.get(client.url(STATUS_PATH)))
        .await?;
    let body = read_body(response).await?;
    write_stdout(&select_status_output(&body)?)
}

pub(crate) async fn run_schema(nodes: Vec<String>) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let mut request = client.http.get(client.url(SCHEMA_PATH));
    if let Some(expand) = expand_param(&nodes) {
        request = request.query(&[expand]);
    }
    let response = client.send(request).await?;
    let body = read_body(response).await?;
    write_stdout(&pretty_json(&body))
}

pub(crate) async fn run_dsl() -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let response = client.send(client.http.get(client.url(DSL_PATH))).await?;
    stream_to_stdout(response, true).await
}

pub(crate) async fn run_tools() -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let response = client.send(client.http.get(client.url(TOOLS_PATH))).await?;
    let body = read_body(response).await?;
    write_stdout(&pretty_json(&body))
}

pub(crate) async fn run_graph_status(
    full_path: Option<String>,
    namespace_id: Option<i64>,
    project_id: Option<i64>,
    format: Option<ResponseFormat>,
) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let params = graph_status_params(full_path, namespace_id, project_id, format);
    let response = client
        .send(
            client
                .http
                .get(client.url(GRAPH_STATUS_PATH))
                .query(&params),
        )
        .await?;
    let body = read_body(response).await?;
    write_stdout(&pretty_json(&body))
}

impl ResponseFormat {
    fn as_str(self) -> &'static str {
        match self {
            ResponseFormat::Llm => "llm",
            ResponseFormat::Raw => "raw",
        }
    }
}

impl RemoteError {
    fn new(exit_code: i32, message: impl Into<String>) -> Self {
        Self {
            exit_code,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for RemoteError {
    fn from(err: anyhow::Error) -> Self {
        RemoteError::new(EXIT_GENERIC, format!("{err:#}"))
    }
}

struct ResolvedEndpoint {
    base_url: String,
    header_name: String,
    header_value: String,
}

struct OrbitClient {
    endpoint: ResolvedEndpoint,
    http: reqwest::Client,
}

impl OrbitClient {
    fn from_env() -> Result<Self, RemoteError> {
        let endpoint = resolve_endpoint(|key| std::env::var(key).ok())?;

        // reqwest is compiled with `rustls-no-provider`, so a CryptoProvider
        // must be installed before building a client. install_default is
        // idempotent.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .map_err(|e| {
                RemoteError::new(EXIT_GENERIC, format!("failed to build HTTP client: {e}"))
            })?;

        Ok(Self { endpoint, http })
    }

    fn url(&self, path: &str) -> String {
        join_url(&self.endpoint.base_url, path)
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, RemoteError> {
        let response = request
            .header(
                self.endpoint.header_name.as_str(),
                self.endpoint.header_value.as_str(),
            )
            .send()
            .await
            .map_err(|e| RemoteError::new(EXIT_GENERIC, format!("Orbit request failed: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(map_http_error(status.as_u16(), &body));
        }
        Ok(response)
    }
}

/// Resolution order: the explicit `ORBIT_*` triplet wins; otherwise fall back
/// to `GITLAB_TOKEN` as a `Bearer` credential against `GITLAB_URL`.
fn resolve_endpoint(get_env: impl Fn(&str) -> Option<String>) -> anyhow::Result<ResolvedEndpoint> {
    let non_empty = |key: &str| get_env(key).filter(|value| !value.is_empty());

    if let (Some(base_url), Some(header_name), Some(header_value)) = (
        non_empty("ORBIT_API_BASE_URL"),
        non_empty("ORBIT_AUTH_HEADER_NAME"),
        non_empty("ORBIT_AUTH_HEADER_VALUE"),
    ) {
        return Ok(ResolvedEndpoint {
            base_url,
            header_name,
            header_value,
        });
    }

    if let Some(token) = non_empty("GITLAB_TOKEN") {
        let base_url =
            non_empty("GITLAB_URL").unwrap_or_else(|| DEFAULT_GITLAB_BASE_URL.to_string());
        return Ok(ResolvedEndpoint {
            base_url,
            header_name: "Authorization".to_string(),
            header_value: format!("Bearer {token}"),
        });
    }

    bail!(
        "no Orbit credential found: set ORBIT_API_BASE_URL, ORBIT_AUTH_HEADER_NAME and \
         ORBIT_AUTH_HEADER_VALUE, or GITLAB_TOKEN (with optional GITLAB_URL). Running through \
         `glab orbit` injects these automatically."
    );
}

fn map_http_error(status: u16, body: &str) -> RemoteError {
    match status {
        404 => RemoteError::new(
            EXIT_UNAVAILABLE,
            "Knowledge Graph endpoint not available (HTTP 404). The `knowledge_graph` feature \
             flag is most likely disabled for your user on this instance.",
        ),
        401 => RemoteError::new(
            EXIT_UNAUTHENTICATED,
            "not authenticated (HTTP 401). Check your token with `glab auth status` and re-run \
             `glab auth login` if it has expired.",
        ),
        403 => RemoteError::new(
            EXIT_FORBIDDEN,
            format!(
                "Knowledge Graph access denied (HTTP 403){}. If the message mentions \"No \
                 Knowledge Graph enabled namespaces\", an Owner of a top-level group you belong \
                 to must enable Orbit.",
                suffix(body)
            ),
        ),
        429 => RemoteError::new(
            EXIT_RATE_LIMITED,
            "rate limited (HTTP 429). Inspect the `Retry-After` response header and back off.",
        ),
        503 => RemoteError::new(
            EXIT_GENERIC,
            "knowledge graph service unavailable (HTTP 503). The underlying GKG service is \
             currently unreachable; retry shortly.",
        ),
        _ => RemoteError::new(
            EXIT_GENERIC,
            format!("Orbit API error (HTTP {status}){}", suffix(body)),
        ),
    }
}

/// Prepends `": "` to a non-empty body so it can be appended to a status
/// message without a trailing separator when the body is empty.
fn suffix(body: &str) -> String {
    if body.is_empty() {
        String::new()
    } else {
        format!(": {body}")
    }
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

/// Builds the `POST /orbit/query` body. The user's `query` is forwarded
/// verbatim; `response_format` priority is flag > body field > `llm` default,
/// matching glab's `buildRequest`.
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

fn join_url(base_url: &str, path: &str) -> String {
    format!("{}{path}", base_url.trim_end_matches('/'))
}

fn expand_param(nodes: &[String]) -> Option<(&'static str, String)> {
    if nodes.is_empty() {
        None
    } else {
        Some(("expand", nodes.join(",")))
    }
}

fn graph_status_params(
    full_path: Option<String>,
    namespace_id: Option<i64>,
    project_id: Option<i64>,
    format: Option<ResponseFormat>,
) -> Vec<(&'static str, String)> {
    let mut params = Vec::new();
    if let Some(id) = namespace_id {
        params.push(("namespace_id", id.to_string()));
    }
    if let Some(id) = project_id {
        params.push(("project_id", id.to_string()));
    }
    if let Some(path) = full_path {
        params.push(("full_path", path));
    }
    if let Some(format) = format {
        params.push(("response_format", format.as_str().to_string()));
    }
    params
}

/// Applies the nested-shape rules of `GET /orbit/status`: newer instances wrap
/// health under `user`/`system`. An unavailable user (or a present user with
/// no system health) is an error; otherwise the inner `system` object is
/// printed. The pre-nesting flat shape is printed as-is.
fn select_status_output(body: &[u8]) -> Result<Vec<u8>, RemoteError> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return Ok(body.to_vec());
    };

    let Some(user) = value.get("user") else {
        return Ok(pretty_json(body));
    };

    let available = user
        .get("available")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if !available {
        return Err(RemoteError::new(
            EXIT_UNAVAILABLE,
            "Orbit is not available for your user. The `knowledge_graph` feature flag is likely \
             disabled, or the instance lacks the `:orbit` license.",
        ));
    }

    match value.get("system") {
        Some(system) => Ok(pretty_value(system)),
        None => Err(RemoteError::new(
            EXIT_UNAVAILABLE,
            "Orbit status: user has access but system health is absent. This is unexpected and \
             may indicate an API contract change.",
        )),
    }
}

fn pretty_json(body: &[u8]) -> Vec<u8> {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(value) => pretty_value(&value),
        Err(_) => body.to_vec(),
    }
}

fn pretty_value(value: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec_pretty(value).unwrap_or_default()
}

async fn read_body(response: reqwest::Response) -> Result<Vec<u8>, RemoteError> {
    response
        .bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| RemoteError::new(EXIT_GENERIC, format!("failed to read response body: {e}")))
}

async fn stream_to_stdout(
    mut response: reqwest::Response,
    trailing_newline: bool,
) -> Result<(), RemoteError> {
    let mut stdout = std::io::stdout().lock();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| RemoteError::new(EXIT_GENERIC, format!("failed to read response body: {e}")))?
    {
        stdout.write_all(&chunk).map_err(|e| {
            RemoteError::new(EXIT_GENERIC, format!("failed to write to stdout: {e}"))
        })?;
    }
    if trailing_newline {
        stdout.write_all(b"\n").map_err(|e| {
            RemoteError::new(EXIT_GENERIC, format!("failed to write to stdout: {e}"))
        })?;
    }
    stdout
        .flush()
        .map_err(|e| RemoteError::new(EXIT_GENERIC, format!("failed to flush stdout: {e}")))
}

fn write_stdout(bytes: &[u8]) -> Result<(), RemoteError> {
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(bytes)
        .and_then(|()| stdout.write_all(b"\n"))
        .and_then(|()| stdout.flush())
        .map_err(|e| RemoteError::new(EXIT_GENERIC, format!("failed to write to stdout: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    #[test]
    fn orbit_triplet_takes_precedence() {
        let endpoint = resolve_endpoint(env_from(&[
            ("ORBIT_API_BASE_URL", "https://example.test"),
            ("ORBIT_AUTH_HEADER_NAME", "Private-Token"),
            ("ORBIT_AUTH_HEADER_VALUE", "glpat-xyz"),
            ("GITLAB_TOKEN", "ignored"),
        ]))
        .expect("triplet resolves");
        assert_eq!(endpoint.base_url, "https://example.test");
        assert_eq!(endpoint.header_name, "Private-Token");
        assert_eq!(endpoint.header_value, "glpat-xyz");
    }

    #[test]
    fn gitlab_token_falls_back_to_bearer() {
        let endpoint =
            resolve_endpoint(env_from(&[("GITLAB_TOKEN", "glpat-abc")])).expect("token resolves");
        assert_eq!(endpoint.base_url, "https://gitlab.com");
        assert_eq!(endpoint.header_name, "Authorization");
        assert_eq!(endpoint.header_value, "Bearer glpat-abc");
    }

    #[test]
    fn gitlab_url_overrides_default_base() {
        let endpoint = resolve_endpoint(env_from(&[
            ("GITLAB_TOKEN", "glpat-abc"),
            ("GITLAB_URL", "https://gitlab.example.com"),
        ]))
        .expect("token resolves");
        assert_eq!(endpoint.base_url, "https://gitlab.example.com");
    }

    // A partial ORBIT_* triplet must not resolve; it falls through so the
    // fallback (or the clear error) applies rather than sending a blank header.
    #[test]
    fn partial_orbit_triplet_falls_through_to_error() {
        let result = resolve_endpoint(env_from(&[("ORBIT_API_BASE_URL", "https://example.test")]));
        assert!(result.is_err());
    }

    #[test]
    fn no_credential_is_a_clear_error() {
        let result = resolve_endpoint(env_from(&[]));
        assert!(result.is_err());
    }

    #[test]
    fn http_status_maps_to_orbit_exit_codes() {
        assert_eq!(map_http_error(404, "").exit_code, EXIT_UNAVAILABLE);
        assert_eq!(map_http_error(401, "").exit_code, EXIT_UNAUTHENTICATED);
        assert_eq!(map_http_error(403, "").exit_code, EXIT_FORBIDDEN);
        assert_eq!(map_http_error(429, "").exit_code, EXIT_RATE_LIMITED);
        assert_eq!(map_http_error(503, "").exit_code, EXIT_GENERIC);
        assert_eq!(map_http_error(500, "").exit_code, EXIT_GENERIC);
    }

    #[test]
    fn forbidden_error_appends_server_body() {
        let err = map_http_error(403, "No Knowledge Graph enabled namespaces");
        assert!(
            err.message
                .contains("No Knowledge Graph enabled namespaces")
        );
    }

    #[test]
    fn generic_error_includes_status_and_body() {
        let err = map_http_error(500, "boom");
        assert!(err.message.contains("HTTP 500"));
        assert!(err.message.contains("boom"));
    }

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

    #[test]
    fn expand_param_is_absent_without_nodes() {
        assert!(expand_param(&[]).is_none());
    }

    #[test]
    fn expand_param_comma_joins_nodes() {
        let param = expand_param(&["User".to_string(), "Project".to_string()]).unwrap();
        assert_eq!(param, ("expand", "User,Project".to_string()));
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

    #[test]
    fn graph_status_sends_full_path_only_when_present() {
        let params = graph_status_params(Some("gitlab-org/gitlab".to_string()), None, None, None);
        assert_eq!(params, vec![("full_path", "gitlab-org/gitlab".to_string())]);
    }

    #[test]
    fn graph_status_sends_ids_and_format() {
        let params = graph_status_params(None, Some(9970), None, Some(ResponseFormat::Llm));
        assert_eq!(
            params,
            vec![
                ("namespace_id", "9970".to_string()),
                ("response_format", "llm".to_string()),
            ]
        );
    }

    #[test]
    fn graph_status_omits_format_when_unset() {
        let params = graph_status_params(None, None, Some(278964), None);
        assert_eq!(params, vec![("project_id", "278964".to_string())]);
    }

    #[test]
    fn status_flat_shape_is_printed_verbatim() {
        let out = select_status_output(br#"{"status":"healthy","version":"1.0"}"#).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["status"], "healthy");
    }

    #[test]
    fn status_nested_available_prints_system_only() {
        let out =
            select_status_output(br#"{"user":{"available":true},"system":{"status":"healthy"}}"#)
                .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["status"], "healthy");
        assert!(value.get("user").is_none());
    }

    #[test]
    fn status_unavailable_user_is_exit_unavailable() {
        let err = select_status_output(br#"{"user":{"available":false}}"#).unwrap_err();
        assert_eq!(err.exit_code, EXIT_UNAVAILABLE);
    }

    #[test]
    fn status_available_without_system_is_exit_unavailable() {
        let err = select_status_output(br#"{"user":{"available":true}}"#).unwrap_err();
        assert_eq!(err.exit_code, EXIT_UNAVAILABLE);
    }
}
