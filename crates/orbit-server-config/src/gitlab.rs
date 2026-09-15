//! GitLab client and server-side GitLab configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for connecting to the GitLab internal API.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct GitlabClientConfiguration {
    /// Base URL for the GitLab API (e.g. "https://staging.gitlab.com:11443").
    pub base_url: String,
    /// Base64-encoded key used to sign outbound JWT tokens.
    pub signing_key: String,
    /// Optional hostname to resolve for DNS override. When set, the host in
    /// `base_url` is resolved to the IP of this hostname instead. This allows
    /// TLS to verify against the `base_url` host while routing traffic through
    /// a different endpoint (e.g. a PSC internal gateway).
    pub resolve_host: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct JwtConfig {
    pub signing_key: Option<String>,
    pub verifying_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct GitlabConfig {
    pub base_url: Option<String>,
    pub jwt: JwtConfig,
    pub resolve_host: Option<String>,
}

impl GitlabConfig {
    pub fn client_config(&self) -> Option<GitlabClientConfiguration> {
        let base_url = self.base_url.clone()?;
        let signing_key = self.jwt.signing_key.clone()?;
        Some(GitlabClientConfiguration {
            base_url,
            signing_key,
            resolve_host: self.resolve_host.clone(),
        })
    }
}
