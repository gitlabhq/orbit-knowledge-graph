//! Ties every ClickHouse query a request issues back to the request's
//! correlation ID. Two independent tags are set on each query:
//!
//! - `query_id`: `{prefix}-{stage}`, unique per pipeline stage so each query is
//!   individually addressable in `system.query_log`. `prefix` is the request
//!   correlation ID when it is a clean token, else a freshly generated ULID.
//! - `log_comment`: the raw correlation ID, kept verbatim so the request-level
//!   join survives even when the ID was too dirty to use as a `query_id`.

/// Build the `query_id` for a pipeline stage. Falls back to a generated ULID
/// prefix when no correlation ID is present or it is not a clean token, so the
/// value is always a valid ClickHouse `query_id`.
pub(crate) fn query_id(stage: &str) -> String {
    let prefix = labkit::correlation::current()
        .and_then(|id| sanitize(&id))
        .unwrap_or_else(labkit::correlation::generate_id);
    format!("{prefix}-{stage}")
}

/// Build the `log_comment` carrying the raw correlation ID. `suffix` tags the
/// query kind (e.g. `hydration:static`) between the `gkg` prefix and the ID.
pub(crate) fn log_comment(suffix: Option<&str>) -> String {
    let head = match suffix {
        Some(s) => format!("gkg;{s}"),
        None => "gkg".to_string(),
    };
    match labkit::correlation::current() {
        Some(id) => format!("{head};correlation_id={id}"),
        None => head,
    }
}

/// A correlation ID is usable as a `query_id` only if it is a non-empty run of
/// ASCII alphanumerics and dashes (the same rule the profiler validates against).
/// A raw ID can't go into `query_id` unchecked: `query_id` rides the request URL
/// and must be a valid, unique CH id, whereas a forwarded X-Request-Id may carry
/// URL-unsafe bytes, so those fall back to a ULID and survive raw in `log_comment`.
fn sanitize(id: &str) -> Option<String> {
    if !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        Some(id.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_accepts_clean_tokens() {
        assert_eq!(
            sanitize("01JABCDEF0123456789ABCDEFG"),
            Some("01JABCDEF0123456789ABCDEFG".to_string())
        );
        assert_eq!(sanitize("req-abc-123"), Some("req-abc-123".to_string()));
    }

    #[test]
    fn sanitize_rejects_dirty_or_empty() {
        assert_eq!(sanitize(""), None);
        assert_eq!(sanitize("has space"), None);
        assert_eq!(sanitize("has/slash"), None);
        assert_eq!(sanitize("under_score"), None);
        assert_eq!(sanitize("semi;colon"), None);
    }

    #[test]
    fn query_id_without_correlation_falls_back_to_ulid_prefix() {
        let id = query_id("base");
        let (prefix, stage) = id.rsplit_once('-').expect("stage suffix present");
        assert_eq!(stage, "base");
        assert_eq!(prefix.len(), 26);
        assert!(prefix.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn log_comment_without_correlation_is_bare_tag() {
        assert_eq!(log_comment(None), "gkg");
        assert_eq!(
            log_comment(Some("hydration:static")),
            "gkg;hydration:static"
        );
    }
}
