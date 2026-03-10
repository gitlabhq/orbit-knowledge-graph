/// Configuration for connecting to the GitLab internal API.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GitlabClientConfiguration {
    /// Base URL for the GitLab API (e.g. "https://staging.gitlab.com:11443").
    pub base_url: String,
    /// Base64-encoded key used to sign outbound JWT tokens.
    pub signing_key: String,
    /// Optional hostname to resolve for DNS override. When set, the host in
    /// `base_url` is resolved to the IP of this hostname instead. This allows
    /// TLS to verify against the `base_url` host while routing traffic through
    /// a different endpoint (e.g. a PSC internal gateway).
    #[serde(default)]
    pub resolve_host: Option<String>,
}
