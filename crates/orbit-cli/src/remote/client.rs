use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use serde::Deserialize;

use super::error::{EXIT_GENERIC, RemoteError, map_http_error};

const DEFAULT_GITLAB_BASE_URL: &str = "https://gitlab.com";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(120);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
const SKILL_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const GREP_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

const STATUS_PATH: &str = "/api/v4/orbit/status";
const SCHEMA_PATH: &str = "/api/v4/orbit/schema";
const DSL_PATH: &str = "/api/v4/orbit/schema/dsl";
const TOOLS_PATH: &str = "/api/v4/orbit/tools";
const QUERY_PATH: &str = "/api/v4/orbit/query";
const GRAPH_STATUS_PATH: &str = "/api/v4/orbit/graph_status";
const SKILLS_PATH: &str = "/api/v4/orbit/skills";
const GREP_PATH: &str = "/api/v4/orbit/grep";
const CONTEXT_PATH: &str = "/api/v4/orbit/context";

pub(crate) struct SkillHttpResponse {
    pub(crate) status: u16,
    pub(crate) etag: Option<String>,
    pub(crate) body: Vec<u8>,
}

#[derive(Clone)]
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
        Self::from_env_in(Path::new("."))
    }

    pub(crate) fn from_env_in(dir: &Path) -> Result<Self, RemoteError> {
        let endpoint = resolve_endpoint(
            |key| std::env::var(key).ok(),
            || resolve_via_credential_helper(dir),
        )?;
        Self::new(endpoint)
    }

    /// Resolves credentials like `from_env`, but a missing credential is not an
    /// error: it returns `None` so callers serve the embedded local skill. An
    /// invalid base URL is still an error so callers can say why they fall back.
    pub(crate) fn from_skill_env() -> Result<Option<Self>, RemoteError> {
        match resolve_endpoint(
            |key| std::env::var(key).ok(),
            || resolve_via_credential_helper(Path::new(".")),
        ) {
            Ok(endpoint) => Self::new(endpoint).map(Some),
            Err(error) if error.is::<MissingCredential>() => Ok(None),
            Err(error) => Err(error.into()),
        }
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
        let origin = self.parse_base_url()?.origin().ascii_serialization();
        if origin == "null" {
            return Err(RemoteError::new(
                EXIT_GENERIC,
                "Orbit API base URL must have an HTTP(S) origin",
            ));
        }
        Ok(origin)
    }

    pub(crate) fn get_host(&self) -> Result<String, RemoteError> {
        let url = self.parse_base_url()?;
        url.host_str()
            .map(str::to_string)
            .ok_or_else(|| RemoteError::new(EXIT_GENERIC, "Orbit API base URL must have a host"))
    }

    fn parse_base_url(&self) -> Result<reqwest::Url, RemoteError> {
        reqwest::Url::parse(&self.endpoint.base_url).map_err(|error| {
            RemoteError::new(EXIT_GENERIC, format!("invalid Orbit API base URL: {error}"))
        })
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
        let response = self
            .send_authenticated(request.timeout(SKILL_REQUEST_TIMEOUT))
            .await?;
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

    pub(crate) async fn get_grep(&self, params: &[(&str, String)]) -> Result<Vec<u8>, RemoteError> {
        let response = self
            .send(
                self.http
                    .get(self.url(GREP_PATH))
                    .query(params)
                    .timeout(GREP_REQUEST_TIMEOUT),
            )
            .await?;
        read_body(response).await
    }

    pub(crate) async fn post_context(&self, body: Vec<u8>) -> Result<Vec<u8>, RemoteError> {
        let response = self
            .send(
                self.http
                    .post(self.url(CONTEXT_PATH))
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body),
            )
            .await?;
        read_body(response).await
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
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse().ok())
                .map(Duration::from_secs);
            let body = response.text().await.unwrap_or_default();
            let mut error = map_http_error(status.as_u16(), &body);
            error.retry_after = retry_after;
            return Err(error);
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
        validate_base_url("ORBIT_API_BASE_URL", &base_url)?;
        return Ok(ResolvedEndpoint {
            base_url,
            header_name,
            header_value,
        });
    }

    if let Some(token) = non_empty("GITLAB_TOKEN") {
        let base_url =
            non_empty("GITLAB_URL").unwrap_or_else(|| DEFAULT_GITLAB_BASE_URL.to_string());
        let url = validate_base_url("GITLAB_URL", &base_url)?;
        if url.scheme() == "http" {
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
        validate_base_url(
            "glab auth credential-helper instance_url",
            &endpoint.base_url,
        )?;
        return Ok(endpoint);
    }

    Err(MissingCredential.into())
}

fn validate_base_url(source: &str, base_url: &str) -> anyhow::Result<reqwest::Url> {
    reqwest::Url::parse(base_url)
        .ok()
        .filter(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some())
        .ok_or_else(|| anyhow::anyhow!("{source} must be an absolute https URL (got `{base_url}`)"))
}

#[derive(Debug)]
struct MissingCredential;

impl std::fmt::Display for MissingCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "no Orbit credential found\n\n\
         Set ORBIT_API_BASE_URL, ORBIT_AUTH_HEADER_NAME, and ORBIT_AUTH_HEADER_VALUE,\n\
         set GITLAB_TOKEN with an optional GITLAB_URL, or run `glab auth login` so the\n\
         credential-helper can provide a token. Running through `glab orbit` works too.",
        )
    }
}

impl std::error::Error for MissingCredential {}

pub(crate) fn credentials_available() -> bool {
    resolve_endpoint(
        |key| std::env::var(key).ok(),
        || resolve_via_credential_helper(Path::new(".")),
    )
    .is_ok()
}

fn resolve_via_credential_helper(dir: &Path) -> Option<ResolvedEndpoint> {
    static CACHED: LazyLock<Mutex<HashMap<PathBuf, Option<ResolvedEndpoint>>>> =
        LazyLock::new(Default::default);

    let key = dunce::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    if let Some(cached) = CACHED.lock().ok()?.get(&key) {
        return cached.clone();
    }
    let resolved = std::process::Command::new("glab")
        .args(["auth", "credential-helper"])
        .current_dir(&key)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| parse_credential_helper_response(&output.stdout));
    CACHED.lock().ok()?.insert(key, resolved.clone());
    resolved
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
            || panic!("helper must not run"),
        )
        .expect("triplet resolves");
        assert_eq!(endpoint.base_url, "https://example.test");
        assert_eq!(endpoint.header_name, "Private-Token");
        assert_eq!(endpoint.header_value, "glpat-xyz");
    }

    fn helper_endpoint() -> ResolvedEndpoint {
        ResolvedEndpoint {
            base_url: "https://helper.test".to_string(),
            header_name: "Authorization".to_string(),
            header_value: "Bearer helper".to_string(),
        }
    }

    #[test]
    fn gitlab_token_skips_the_credential_helper() {
        let endpoint = resolve_endpoint(
            env_from(&[
                ("GITLAB_TOKEN", "glpat-abc"),
                ("GITLAB_URL", "https://gl.test"),
            ]),
            || panic!("helper must not run"),
        )
        .expect("token resolves");
        assert_eq!(endpoint.base_url, "https://gl.test");
        assert_eq!(endpoint.header_value, "Bearer glpat-abc");
    }

    #[test]
    fn partial_orbit_triplet_falls_through_to_the_helper() {
        let endpoint = resolve_endpoint(
            env_from(&[("ORBIT_API_BASE_URL", "https://example.test")]),
            || Some(helper_endpoint()),
        )
        .expect("helper resolves");
        assert_eq!(endpoint.base_url, "https://helper.test");
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
    fn scheme_less_gitlab_url_is_rejected_with_a_clear_error() {
        for url in ["gitlab.com", "gitlab.com:8080", "ftp://gitlab.com"] {
            let error = resolve_endpoint(
                env_from(&[("GITLAB_TOKEN", "t"), ("GITLAB_URL", url)]),
                || None,
            )
            .err()
            .unwrap();
            assert!(!error.is::<MissingCredential>());
            assert_eq!(
                error.to_string(),
                format!("GITLAB_URL must be an absolute https URL (got `{url}`)")
            );
        }
    }

    #[test]
    fn scheme_less_helper_instance_url_is_rejected() {
        let error = resolve_endpoint(env_from(&[]), || {
            Some(ResolvedEndpoint {
                base_url: "gitlab.com".to_string(),
                ..helper_endpoint()
            })
        })
        .err()
        .unwrap();
        assert_eq!(
            error.to_string(),
            "glab auth credential-helper instance_url must be an absolute https URL (got `gitlab.com`)"
        );
    }

    #[test]
    fn http_gitlab_url_is_accepted() {
        let endpoint = resolve_endpoint(
            env_from(&[
                ("GITLAB_TOKEN", "t"),
                ("GITLAB_URL", "http://localhost:3000"),
            ]),
            || None,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(endpoint.base_url, "http://localhost:3000");
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
