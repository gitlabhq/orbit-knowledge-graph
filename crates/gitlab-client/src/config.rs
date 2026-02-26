/// Configuration for connecting to the GitLab internal API.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GitlabClientConfiguration {
    /// Base URL for the GitLab API (e.g. "http://gitlab:3000").
    pub base_url: String,
    /// Shared secret used to sign JWT tokens for authentication.
    pub jwt_secret: String,
}
