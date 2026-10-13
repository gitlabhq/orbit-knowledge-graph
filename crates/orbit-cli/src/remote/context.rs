use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, EXIT_UNAVAILABLE, RemoteError};

pub(crate) const MAX_TOKENS: usize = 20;

pub(crate) async fn fetch(client: OrbitClient, tokens: &[String]) -> Result<Vec<u8>, RemoteError> {
    if tokens.len() > MAX_TOKENS {
        return Err(RemoteError::new(
            EXIT_GENERIC,
            format!(
                "{} GitLab references given; context reads at most {MAX_TOKENS} per call",
                tokens.len()
            ),
        ));
    }
    let body = serde_json::to_vec(&serde_json::json!({
        "tokens": tokens,
        "response_format": "llm",
    }))
    .map_err(|error| RemoteError::new(EXIT_GENERIC, error.to_string()))?;
    client
        .post_context(body)
        .await
        .map_err(|error| match error.exit_code {
            EXIT_UNAVAILABLE => RemoteError::new(
                EXIT_UNAVAILABLE,
                "Orbit context endpoint not available\n\n\
                 `POST /api/v4/orbit/context` returned 404. This GitLab instance may not\n\
                 serve entity context yet, or Orbit is not enabled for you.",
            ),
            _ => error,
        })
}
