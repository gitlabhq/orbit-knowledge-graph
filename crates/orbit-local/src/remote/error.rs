pub(crate) const EXIT_GENERIC: i32 = 1;
pub(crate) const EXIT_UNAVAILABLE: i32 = 2;
const EXIT_UNAUTHENTICATED: i32 = 3;
const EXIT_FORBIDDEN: i32 = 4;
const EXIT_RATE_LIMITED: i32 = 5;

#[derive(Debug)]
pub(crate) struct RemoteError {
    pub exit_code: i32,
    pub message: String,
}

impl RemoteError {
    pub(crate) fn new(exit_code: i32, message: impl Into<String>) -> Self {
        Self {
            exit_code,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for RemoteError {
    fn from(err: anyhow::Error) -> Self {
        RemoteError::new(EXIT_GENERIC, format!("{err:#}"))
    }
}

pub(crate) fn map_http_error(status: u16, body: &str) -> RemoteError {
    match status {
        404 => RemoteError::new(
            EXIT_UNAVAILABLE,
            "Knowledge Graph endpoint not available (HTTP 404). The `knowledge_graph` feature \
             flag is most likely disabled for your user on this instance.",
        ),
        401 => RemoteError::new(
            EXIT_UNAUTHENTICATED,
            "not authenticated (HTTP 401). Check your token with `glab auth status` and re-run \
             `glab auth login` if it has expired.",
        ),
        403 => RemoteError::new(
            EXIT_FORBIDDEN,
            format!(
                "Knowledge Graph access denied (HTTP 403){}. If the message mentions \"No \
                 Knowledge Graph enabled namespaces\", an Owner of a top-level group you belong \
                 to must enable Orbit.",
                body_suffix(body)
            ),
        ),
        429 => RemoteError::new(
            EXIT_RATE_LIMITED,
            "rate limited (HTTP 429). Inspect the `Retry-After` response header and back off.",
        ),
        503 => RemoteError::new(
            EXIT_GENERIC,
            "knowledge graph service unavailable (HTTP 503). The underlying GKG service is \
             currently unreachable; retry shortly.",
        ),
        _ => RemoteError::new(
            EXIT_GENERIC,
            format!("Orbit API error (HTTP {status}){}", body_suffix(body)),
        ),
    }
}

fn body_suffix(body: &str) -> String {
    if body.is_empty() {
        String::new()
    } else {
        format!(": {body}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_status_maps_to_orbit_exit_codes() {
        assert_eq!(map_http_error(404, "").exit_code, EXIT_UNAVAILABLE);
        assert_eq!(map_http_error(401, "").exit_code, EXIT_UNAUTHENTICATED);
        assert_eq!(map_http_error(403, "").exit_code, EXIT_FORBIDDEN);
        assert_eq!(map_http_error(429, "").exit_code, EXIT_RATE_LIMITED);
        assert_eq!(map_http_error(503, "").exit_code, EXIT_GENERIC);
        assert_eq!(map_http_error(500, "").exit_code, EXIT_GENERIC);
    }

    #[test]
    fn forbidden_error_appends_server_body() {
        let err = map_http_error(403, "No Knowledge Graph enabled namespaces");
        assert!(
            err.message
                .contains("No Knowledge Graph enabled namespaces")
        );
    }

    #[test]
    fn generic_error_includes_status_and_body() {
        let err = map_http_error(500, "boom");
        assert!(err.message.contains("HTTP 500"));
        assert!(err.message.contains("boom"));
    }
}
