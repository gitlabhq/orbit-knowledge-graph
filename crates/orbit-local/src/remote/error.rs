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
            "Orbit endpoint not available\n\n\
             The `/api/v4/orbit/*` endpoints returned 404. The most likely cause is\n\
             that the `knowledge_graph` feature flag is disabled for your user on this\n\
             instance. Contact an instance administrator to enable it.",
        ),
        401 => RemoteError::new(
            EXIT_UNAUTHENTICATED,
            "not authenticated\n\n\
             The Orbit API rejected the request with HTTP 401. Run `glab auth status`\n\
             to check your token, then `glab auth login` if it has expired.",
        ),
        403 => RemoteError::new(
            EXIT_FORBIDDEN,
            format!(
                "Orbit access denied\n\n\
                 The Orbit API rejected the request with HTTP 403{}.\n\
                 If your top-level groups have not enabled Orbit, an Owner of a group\n\
                 you belong to must enable it via Orbit > Configuration in the GitLab UI.",
                body_suffix(body)
            ),
        ),
        429 => RemoteError::new(
            EXIT_RATE_LIMITED,
            "rate limited\n\n\
             The Orbit API rejected the request with HTTP 429. Inspect the `Retry-After`\n\
             response header and back off, or batch via aggregation if you are running\n\
             many small queries.",
        ),
        503 => RemoteError::new(
            EXIT_GENERIC,
            "Orbit service unavailable\n\n\
             The Orbit API returned HTTP 503. The underlying GKG service is currently\n\
             unreachable; retry shortly or check the GitLab status page.",
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
        let err = map_http_error(403, "no enabled namespaces");
        assert!(err.message.contains("no enabled namespaces"));
    }

    #[test]
    fn generic_error_includes_status_and_body() {
        let err = map_http_error(500, "boom");
        assert!(err.message.contains("HTTP 500"));
        assert!(err.message.contains("boom"));
    }
}
