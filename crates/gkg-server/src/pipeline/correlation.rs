//! Tags each ClickHouse query with the request correlation ID: a per-stage `query_id` (sanitized-or-ULID prefix) and the raw ID in `log_comment`.

pub(crate) fn query_id(stage: &str) -> String {
    let prefix = labkit::correlation::current()
        .and_then(|id| sanitize(&id))
        .unwrap_or_else(labkit::correlation::generate_id);
    format!("{prefix}-{stage}")
}

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
