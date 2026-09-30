use std::sync::OnceLock;
use std::time::Duration;

use anyhow::bail;
use serde::Deserialize;

use super::error::{EXIT_GENERIC, RemoteError, map_http_error};

const DEFAULT_GITLAB_BASE_URL: &str = "https://gitlab.com";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(120);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

const STATUS_PATH: &str = "/api/v4/orbit/status";
const SCHEMA_PATH: &str = "/api/v4/orbit/schema";
const DSL_PATH: &str = "/api/v4/orbit/schema/dsl";
const TOOLS_PATH: &str = "/api/v4/orbit/tools";
const QUERY_PATH: &str = "/api/v4/orbit/query";
const GRAPH_STATUS_PATH: &str = "/api/v4/orbit/graph_status";
const SKILLS_PATH: &str = "/api/v4/orbit/skills";

pub(crate) struct SkillHttpResponse {
    pub(crate) status: u16,
    pub(crate) etag: Option<String>,
    pub(crate) body: Vec<u8>,
}

pub(crate) struct OrbitClient {
    endpoint: ResolvedEndpoint,
    http: reqwest::Client,
}

#[derive(Clone)]
struct ResolvedEndpoint {
    base_url: String,
    header_name: String,
    header_value: String,
}

impl OrbitClient {
    pub(crate) fn from_env() -> Result<Self, RemoteError> {
        let endpoint =
            resolve_endpoint(|key| std::env::var(key).ok(), resolve_via_credential_helper)?;
        Self::new(endpoint)
    }

    /// Missing or partial glab tuples must not invoke the credential helper.
    pub(crate) fn from_skill_env() -> Result<Option<Self>, RemoteError> {
        let Some(endpoint) = resolve_skill_endpoint(|key| std::env::var(key).ok()) else {
            return Ok(None);
        };
        Self::new(endpoint).map(Some)
    }

    fn new(endpoint: ResolvedEndpoint) -> Result<Self, RemoteError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let http = reqwest::Client::builder()
            .user_agent(build_user_agent(|key| std::env::var(key).ok()))
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| {
                RemoteError::new(EXIT_GENERIC, format!("failed to build HTTP client: {e}"))
            })?;
        Ok(Self { endpoint, http })
    }

    pub(crate) fn origin(&self) -> Result<String, RemoteError> {
        let url = reqwest::Url::parse(&self.endpoint.base_url).map_err(|error| {
            RemoteError::new(EXIT_GENERIC, format!("invalid Orbit API base URL: {error}"))
        })?;
        let origin = url.origin().ascii_serialization();
        if origin == "null" {
            return Err(RemoteError::new(
                EXIT_GENERIC,
                "Orbit API base URL must have an HTTP(S) origin",
            ));
        }
        Ok(origin)
    }

    pub(crate) async fn list_skills(&self) -> Result<SkillHttpResponse, RemoteError> {
        self.skill_response(self.http.get(self.url(SKILLS_PATH)))
            .await
    }

    pub(crate) async fn get_skill(
        &self,
        name: &str,
        etag: Option<&str>,
    ) -> Result<SkillHttpResponse, RemoteError> {
        let mut request = self.http.get(self.url(&format!("{SKILLS_PATH}/{name}")));
        if let Some(etag) = etag {
            request = request.header(reqwest::header::IF_NONE_MATCH, etag);
        }
        self.skill_response(request).await
    }

    async fn skill_response(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<SkillHttpResponse, RemoteError> {
        let response = self.send_authenticated(request).await?;
        let status = response.status().as_u16();
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = read_body(response).await?;
        Ok(SkillHttpResponse { status, etag, body })
    }

    pub(crate) async fn get_status(&self) -> Result<Vec<u8>, RemoteError> {
        self.get_bytes(STATUS_PATH, &[]).await
    }

    pub(crate) async fn get_schema(
        &self,
        params: &[(&str, String)],
    ) -> Result<Vec<u8>, RemoteError> {
        self.get_bytes(SCHEMA_PATH, params).await
    }

    pub(crate) async fn get_dsl(&self) -> Result<Vec<u8>, RemoteError> {
        self.get_bytes(DSL_PATH, &[]).await
    }

    pub(crate) async fn get_tools(&self) -> Result<Vec<u8>, RemoteError> {
        self.get_bytes(TOOLS_PATH, &[]).await
    }

    pub(crate) async fn get_graph_status(
        &self,
        params: &[(&str, String)],
    ) -> Result<Vec<u8>, RemoteError> {
        self.get_bytes(GRAPH_STATUS_PATH, params).await
    }

    pub(crate) async fn query_raw(&self, body: Vec<u8>) -> Result<Vec<u8>, RemoteError> {
        let response = self
            .send(
                self.http
                    .post(self.url(QUERY_PATH))
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body),
            )
            .await?;
        read_body(response).await
    }

    async fn get_bytes(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Vec<u8>, RemoteError> {
        let response = self
            .send(self.http.get(self.url(path)).query(params))
            .await?;
        read_body(response).await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.endpoint.base_url.trim_end_matches('/'))
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, RemoteError> {
        let response = self.send_authenticated(request).await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(map_http_error(status.as_u16(), &body));
        }
        Ok(response)
    }

    async fn send_authenticated(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, RemoteError> {
        let request_id = crate::telemetry::invocation_id();
        let session_id = agent_session_id();
        let mut builder = request
            .header("X-Request-ID", &request_id)
            .header("X-Orbit-Request-Id", &request_id)
            .header(
                self.endpoint.header_name.as_str(),
                self.endpoint.header_value.as_str(),
            );
        if let Some(session_id) = &session_id {
            builder = builder.header("X-Orbit-Session-Id", session_id);
        }
        builder
            .send()
            .await
            .map_err(|e| RemoteError::new(EXIT_GENERIC, format!("Orbit request failed: {e}")))
    }
}

#[derive(Deserialize)]
struct CredentialHelperResponse {
    #[serde(rename = "type")]
    response_type: String,
    instance_url: Option<String>,
    token: Option<CredentialHelperToken>,
}

#[derive(Deserialize)]
struct CredentialHelperToken {
    token: String,
}

fn resolve_skill_endpoint(get_env: impl Fn(&str) -> Option<String>) -> Option<ResolvedEndpoint> {
    let non_empty = |key: &str| get_env(key).filter(|value| !value.is_empty());
    match (
        non_empty("ORBIT_API_BASE_URL"),
        non_empty("ORBIT_AUTH_HEADER_NAME"),
        non_empty("ORBIT_AUTH_HEADER_VALUE"),
    ) {
        (Some(base_url), Some(header_name), Some(header_value)) => Some(ResolvedEndpoint {
            base_url,
            header_name,
            header_value,
        }),
        _ => None,
    }
}

fn resolve_endpoint(
    get_env: impl Fn(&str) -> Option<String>,
    credential_helper: impl FnOnce() -> Option<ResolvedEndpoint>,
) -> anyhow::Result<ResolvedEndpoint> {
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
        if !base_url.starts_with("https://") {
            eprintln!(
                "warning: GITLAB_URL is not https ({base_url}); the token will be sent in plaintext"
            );
        }
        return Ok(ResolvedEndpoint {
            base_url,
            header_name: "Authorization".to_string(),
            header_value: format!("Bearer {token}"),
        });
    }

    if let Some(endpoint) = credential_helper() {
        return Ok(endpoint);
    }

    bail!(
        "no Orbit credential found\n\n\
         Set ORBIT_API_BASE_URL, ORBIT_AUTH_HEADER_NAME, and ORBIT_AUTH_HEADER_VALUE,\n\
         set GITLAB_TOKEN with an optional GITLAB_URL, or run `glab auth login` so the\n\
         credential-helper can provide a token. Running through `glab orbit` works too."
    );
}

fn resolve_via_credential_helper() -> Option<ResolvedEndpoint> {
    static CACHED: OnceLock<Option<ResolvedEndpoint>> = OnceLock::new();

    CACHED
        .get_or_init(|| {
            let output = std::process::Command::new("glab")
                .args(["auth", "credential-helper"])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .output()
                .ok()?;

            if !output.status.success() {
                return None;
            }

            parse_credential_helper_response(&output.stdout)
        })
        .clone()
}

fn parse_credential_helper_response(json: &[u8]) -> Option<ResolvedEndpoint> {
    let resp: CredentialHelperResponse = serde_json::from_slice(json).ok()?;
    if resp.response_type != "success" {
        return None;
    }

    let token = resp.token?;
    let base_url = resp
        .instance_url
        .unwrap_or_else(|| DEFAULT_GITLAB_BASE_URL.to_string());

    Some(ResolvedEndpoint {
        base_url,
        header_name: "Authorization".to_string(),
        header_value: format!("Bearer {}", token.token),
    })
}

fn build_user_agent(get_env: impl Fn(&str) -> Option<String>) -> String {
    let mut ua = format!(
        "orbit/{} ({}, {})",
        env!("ORBIT_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
    );
    if let Some(agent) = crate::telemetry::detect_coding_agent(get_env) {
        ua.push_str(" Coding-Agent/");
        ua.push_str(&agent);
    }
    ua
}

fn agent_session_id() -> Option<String> {
    ["CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID"]
        .iter()
        .find_map(|key| {
            std::env::var(key)
                .ok()
                .filter(|value| crate::telemetry::is_safe_identifier(value))
        })
}

async fn read_body(response: reqwest::Response) -> Result<Vec<u8>, RemoteError> {
    response
        .bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| RemoteError::new(EXIT_GENERIC, format!("failed to read response body: {e}")))
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
        let endpoint = resolve_endpoint(
            env_from(&[
                ("ORBIT_API_BASE_URL", "https://example.test"),
                ("ORBIT_AUTH_HEADER_NAME", "Private-Token"),
                ("ORBIT_AUTH_HEADER_VALUE", "glpat-xyz"),
                ("GITLAB_TOKEN", "ignored"),
            ]),
            || None,
        )
        .expect("triplet resolves");
        assert_eq!(endpoint.base_url, "https://example.test");
        assert_eq!(endpoint.header_name, "Private-Token");
        assert_eq!(endpoint.header_value, "glpat-xyz");
    }

    #[test]
    fn skill_endpoint_uses_only_a_complete_orbit_tuple() {
        let endpoint = resolve_skill_endpoint(env_from(&[
            ("ORBIT_API_BASE_URL", "https://example.test"),
            ("ORBIT_AUTH_HEADER_NAME", "Private-Token"),
            ("ORBIT_AUTH_HEADER_VALUE", "glpat-xyz"),
            ("GITLAB_TOKEN", "ignored"),
        ]))
        .unwrap();
        assert_eq!(endpoint.base_url, "https://example.test");

        assert!(
            resolve_skill_endpoint(env_from(&[("ORBIT_API_BASE_URL", "https://example.test")]))
                .is_none()
        );
        assert!(resolve_skill_endpoint(env_from(&[("GITLAB_TOKEN", "ignored")])).is_none());
    }

    #[test]
    fn gitlab_token_falls_back_to_bearer() {
        let endpoint = resolve_endpoint(env_from(&[("GITLAB_TOKEN", "glpat-abc")]), || None)
            .expect("token resolves");
        assert_eq!(endpoint.base_url, "https://gitlab.com");
        assert_eq!(endpoint.header_name, "Authorization");
        assert_eq!(endpoint.header_value, "Bearer glpat-abc");
    }

    #[test]
    fn gitlab_url_overrides_default_base() {
        let endpoint = resolve_endpoint(
            env_from(&[
                ("GITLAB_TOKEN", "glpat-abc"),
                ("GITLAB_URL", "https://gitlab.example.com"),
            ]),
            || None,
        )
        .expect("token resolves");
        assert_eq!(endpoint.base_url, "https://gitlab.example.com");
    }

    #[test]
    fn partial_orbit_triplet_falls_through_to_error() {
        let result = resolve_endpoint(
            env_from(&[("ORBIT_API_BASE_URL", "https://example.test")]),
            || None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn no_credential_is_a_clear_error() {
        let result = resolve_endpoint(env_from(&[]), || None);
        assert!(result.is_err());
    }

    #[test]
    fn credential_helper_success_with_pat() {
        let json = br#"{"type":"success","instance_url":"https://gitlab.example.com","token":{"type":"pat","token":"glpat-abc"}}"#;
        let ep = parse_credential_helper_response(json).expect("parses success");
        assert_eq!(ep.base_url, "https://gitlab.example.com");
        assert_eq!(ep.header_name, "Authorization");
        assert_eq!(ep.header_value, "Bearer glpat-abc");
    }

    #[test]
    fn credential_helper_success_with_oauth2() {
        let json = br#"{"type":"success","instance_url":"https://gitlab.com","token":{"type":"oauth2","token":"oauth-tok","expiry_timestamp":"2026-01-01T00:00:00Z"}}"#;
        let ep = parse_credential_helper_response(json).expect("parses success");
        assert_eq!(ep.base_url, "https://gitlab.com");
        assert_eq!(ep.header_value, "Bearer oauth-tok");
    }

    #[test]
    fn credential_helper_defaults_to_gitlab_com_when_instance_url_missing() {
        let json = br#"{"type":"success","token":{"type":"pat","token":"glpat-xyz"}}"#;
        let ep = parse_credential_helper_response(json).expect("parses success");
        assert_eq!(ep.base_url, "https://gitlab.com");
    }

    #[test]
    fn credential_helper_error_response_returns_none() {
        let json = br#"{"type":"error","message":"glab is not authenticated"}"#;
        assert!(parse_credential_helper_response(json).is_none());
    }

    #[test]
    fn credential_helper_malformed_json_returns_none() {
        assert!(parse_credential_helper_response(b"not json").is_none());
    }

    #[test]
    fn credential_helper_missing_token_returns_none() {
        let json = br#"{"type":"success","instance_url":"https://gitlab.com"}"#;
        assert!(parse_credential_helper_response(json).is_none());
    }

    #[test]
    fn user_agent_without_coding_agent() {
        let ua = build_user_agent(env_from(&[]));
        let expected = format!(
            "orbit/{} ({}, {})",
            env!("ORBIT_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH,
        );
        assert_eq!(ua, expected);
    }

    #[test]
    fn user_agent_with_coding_agent() {
        let ua = build_user_agent(env_from(&[("CLAUDECODE", "1")]));
        assert!(
            ua.ends_with(" Coding-Agent/claude-code"),
            "expected Coding-Agent suffix, got: {ua}"
        );
    }
}
