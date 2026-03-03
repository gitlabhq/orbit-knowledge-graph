/// Configuration for connecting to the GitLab internal API.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GitlabClientConfiguration {
    /// Base URL for the GitLab API (e.g. "http://gitlab:3000").
    pub base_url: String,
    /// Base64-encoded key used to sign outbound JWT tokens.
    pub signing_key: String,
}
