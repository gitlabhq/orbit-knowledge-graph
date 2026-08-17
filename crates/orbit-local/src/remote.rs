//! Authenticated calls to the remote Orbit graph. The credential is resolved
//! from the process environment: `glab orbit` injects the `ORBIT_*` triplet
//! before exec, and the `GITLAB_*` fallback lets the binary run standalone.

use std::io::{Read, Write};
use std::time::Duration;

use anyhow::{Context, Result, bail};

const DEFAULT_GITLAB_BASE_URL: &str = "https://gitlab.com";
const QUERY_PATH: &str = "/api/v4/orbit/query";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(120);

/// A resolved endpoint plus the single auth header to send with each request.
pub(crate) struct ResolvedEndpoint {
    pub base_url: String,
    pub header_name: String,
    pub header_value: String,
}

pub(crate) async fn run_query(source: Option<String>) -> Result<()> {
    let endpoint = resolve_endpoint(|key| std::env::var(key).ok())?;
    let body = read_query_body(source.as_deref())?;
    post_query(&endpoint, body).await
}

/// Resolution order: the explicit `ORBIT_*` triplet wins; otherwise fall back
/// to `GITLAB_TOKEN` as a `Bearer` credential against `GITLAB_URL`.
fn resolve_endpoint(get_env: impl Fn(&str) -> Option<String>) -> Result<ResolvedEndpoint> {
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

fn read_query_body(source: Option<&str>) -> Result<Vec<u8>> {
    let data = match source {
        None | Some("-") => {
            let mut buf = Vec::new();
            std::io::stdin()
                .lock()
                .read_to_end(&mut buf)
                .context("failed to read query body from stdin")?;
            buf
        }
        Some(path) => {
            std::fs::read(path).with_context(|| format!("failed to read query body from {path}"))?
        }
    };
    if data.is_empty() {
        bail!("query body is empty");
    }
    Ok(data)
}

async fn post_query(endpoint: &ResolvedEndpoint, body: Vec<u8>) -> Result<()> {
    let url = format!("{}{QUERY_PATH}", endpoint.base_url.trim_end_matches('/'));

    // reqwest is compiled with `rustls-no-provider`, so a CryptoProvider must
    // be installed before building a client. install_default is idempotent.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .build()
        .context("failed to build HTTP client")?;

    let mut response = client
        .post(&url)
        .header(
            endpoint.header_name.as_str(),
            endpoint.header_value.as_str(),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .with_context(|| format!("request to {url} failed"))?;

    let status = response.status();
    if !status.is_success() {
        let detail = response.text().await.unwrap_or_default();
        bail!("Orbit query failed: HTTP {status}\n{detail}");
    }

    let mut stdout = std::io::stdout().lock();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("failed to read response body")?
    {
        stdout
            .write_all(&chunk)
            .context("failed to write response to stdout")?;
    }
    stdout.flush().context("failed to flush stdout")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::resolve_endpoint;
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
}
