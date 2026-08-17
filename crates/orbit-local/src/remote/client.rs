use std::time::Duration;

use anyhow::bail;

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

pub(crate) struct OrbitClient {
    endpoint: ResolvedEndpoint,
    http: reqwest::Client,
}

struct ResolvedEndpoint {
    base_url: String,
    header_name: String,
    header_value: String,
}

impl OrbitClient {
    pub(crate) fn from_env() -> Result<Self, RemoteError> {
        let endpoint = resolve_endpoint(|key| std::env::var(key).ok())?;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| {
                RemoteError::new(EXIT_GENERIC, format!("failed to build HTTP client: {e}"))
            })?;

        Ok(Self { endpoint, http })
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

    bail!(
        "no Orbit credential found\n\n\
         Set ORBIT_API_BASE_URL, ORBIT_AUTH_HEADER_NAME, and ORBIT_AUTH_HEADER_VALUE,\n\
         or set GITLAB_TOKEN with an optional GITLAB_URL. Running through `glab orbit`\n\
         injects these automatically."
    );
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
}
