use serde::Deserialize;

const MAX_BLOCK_REASON_LEN: usize = 64;

/// Only short snake_case values are kept: on self-managed the body may come from a
/// customer-controlled proxy.
pub(crate) fn parse(body: &[u8]) -> Option<String> {
    #[derive(Deserialize)]
    struct BlockBody {
        block_reason: String,
    }

    let reason = serde_json::from_slice::<BlockBody>(body).ok()?.block_reason;
    let well_formed = !reason.is_empty()
        && reason.len() <= MAX_BLOCK_REASON_LEN
        && reason.bytes().all(|b| b.is_ascii_lowercase() || b == b'_');
    well_formed.then_some(reason)
}

pub(crate) fn label(reason: Option<&str>) -> &str {
    reason.unwrap_or("none")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_block_reason() {
        assert_eq!(
            parse(br#"{"block_reason":"license_revoked"}"#).as_deref(),
            Some("license_revoked")
        );
    }

    #[test]
    fn body_without_block_reason_is_none() {
        assert_eq!(parse(b""), None);
        assert_eq!(parse(b"<html>Bad Gateway</html>"), None);
        assert_eq!(parse(br#"{"error":"x"}"#), None);
    }

    #[test]
    fn malformed_block_reason_is_dropped() {
        assert_eq!(parse(br#"{"block_reason":"Bad Value\n"}"#), None);
        assert_eq!(parse(br#"{"block_reason":""}"#), None);
        let long = format!(
            r#"{{"block_reason":"{}"}}"#,
            "a".repeat(MAX_BLOCK_REASON_LEN + 1)
        );
        assert_eq!(parse(long.as_bytes()), None);
    }
}
