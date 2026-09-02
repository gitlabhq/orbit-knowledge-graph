//! Tags each ClickHouse query with the request correlation ID. Sub-queries carry
//! a per-stage `query_id` and a lightweight `gkg;<kind>;correlation_id=<id>`
//! comment; the base query additionally carries an attribution payload
//! (`gkg;<base64(json)>`) so retention can recover who ran which query with which
//! compiler and schema version.

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use serde::Serialize;

const PAYLOAD_VERSION: u32 = 1;

#[derive(Serialize)]
struct AttributionPayload<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<String>,
    user_id: u64,
    query: &'a str,
    versions: Versions,
}

#[derive(Serialize)]
struct Versions {
    payload: u32,
    dsl: String,
    schema: u32,
}

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

/// Base-query `log_comment`: the `gkg` prefix plus a base64 attribution payload
/// carrying correlation ID, user, DSL query, and the compiler/schema versions.
pub(crate) fn log_comment_base(user_id: u64, query_json: &str) -> String {
    let payload = AttributionPayload {
        correlation_id: labkit::correlation::current(),
        user_id,
        query: query_json,
        versions: Versions {
            payload: PAYLOAD_VERSION,
            dsl: orbit_utils::pinned::VERSIONS.query_dsl.clone(),
            schema: *indexer::schema::version::SCHEMA_VERSION,
        },
    };
    let json = serde_json::to_vec(&payload).unwrap_or_default();
    format!("gkg;{}", STANDARD_NO_PAD.encode(json))
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
    use labkit::correlation::CorrelationCaptureLayer;
    use tracing_subscriber::layer::SubscriberExt;

    fn with_correlation<T>(id: &str, f: impl FnOnce() -> T) -> T {
        let subscriber = tracing_subscriber::registry().with(CorrelationCaptureLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("test", correlation_id = id);
            let _guard = span.enter();
            f()
        })
    }

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

    #[test]
    fn query_id_uses_clean_correlation_id_as_prefix() {
        let id = with_correlation("req-abc-123", || query_id("base"));
        assert_eq!(id, "req-abc-123-base");
    }

    #[test]
    fn query_id_falls_back_to_ulid_for_dirty_correlation_id() {
        let id = with_correlation("dirty/id", || query_id("base"));
        let prefix = id.strip_suffix("-base").expect("stage suffix present");
        assert_eq!(prefix.len(), 26);
        assert!(prefix.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn log_comment_includes_raw_correlation_id() {
        assert_eq!(
            with_correlation("dirty/id", || log_comment(None)),
            "gkg;correlation_id=dirty/id"
        );
        assert_eq!(
            with_correlation("req-abc-123", || log_comment(Some("hydration:static"))),
            "gkg;hydration:static;correlation_id=req-abc-123"
        );
    }

    fn decode_base_payload(comment: &str) -> serde_json::Value {
        let b64 = comment.strip_prefix("gkg;").expect("gkg-prefixed payload");
        let json = STANDARD_NO_PAD.decode(b64).expect("valid base64");
        serde_json::from_slice(&json).expect("valid json")
    }

    #[test]
    fn base_payload_carries_attribution_and_versions() {
        let comment = with_correlation("req-abc-123", || {
            log_comment_base(42, r#"{"query_type":"traversal"}"#)
        });
        let p = decode_base_payload(&comment);

        assert_eq!(p["correlation_id"], "req-abc-123");
        assert_eq!(p["user_id"], 42);
        assert_eq!(p["query"], r#"{"query_type":"traversal"}"#);
        assert_eq!(p["versions"]["payload"], PAYLOAD_VERSION);
        assert_eq!(
            p["versions"]["dsl"],
            orbit_utils::pinned::VERSIONS.query_dsl
        );
        assert_eq!(
            p["versions"]["schema"],
            *indexer::schema::version::SCHEMA_VERSION
        );
    }

    #[test]
    fn base_payload_omits_correlation_when_absent() {
        let p = decode_base_payload(&log_comment_base(1, "{}"));
        assert!(p.get("correlation_id").is_none());
        assert_eq!(p["user_id"], 1);
    }
}
