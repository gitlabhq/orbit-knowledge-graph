use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, EXIT_UNAVAILABLE, RemoteError};

pub(crate) async fn fetch(client: OrbitClient, tokens: &[String]) -> Result<Vec<u8>, RemoteError> {
    let tasks: Vec<_> = tokens
        .iter()
        .map(|token| {
            let (client, token) = (client.clone(), token.clone());
            tokio::spawn(async move { fetch_one(client, token).await })
        })
        .collect();
    let mut out = Vec::new();
    for task in tasks {
        let mut body = task
            .await
            .map_err(|error| RemoteError::new(EXIT_GENERIC, error.to_string()))??;
        if !body.ends_with(b"\n") {
            body.push(b'\n');
        }
        out.extend(body);
    }
    Ok(out)
}

async fn fetch_one(client: OrbitClient, token: String) -> Result<Vec<u8>, RemoteError> {
    let body = serde_json::to_vec(&serde_json::json!({
        "tokens": [token],
        "response_format": "llm",
        "expand": true,
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
