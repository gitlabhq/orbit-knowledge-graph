//! Helpers for the `<org_id>/<namespace_id>/` traversal path format used
//! throughout the indexer, NATS topic routing, and query profiler.

/// Convert slash-separated segments to dot-separated, stripping empties.
///
/// `"42/9970/" → "42.9970"`, `"42/9970/12345/" → "42.9970.12345"`.
pub fn to_dotted(path: &str) -> String {
    path.split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(".")
}

/// Extract the organization ID (first segment) from a traversal path.
///
/// Returns `None` when the path is empty or the first segment isn't numeric.
pub fn org_id(path: &str) -> Option<i64> {
    path.trim_start_matches('/')
        .split('/')
        .next()
        .and_then(|s| s.parse().ok())
}

/// A traversal path is valid when it matches `<org_id>/<namespace_id>/`
/// where both segments are unsigned integers.
///
/// An empty or malformed path would cause `startsWith(traversal_path, '')`
/// to match every row in the table.
pub fn is_valid(path: &str) -> bool {
    let Some(inner) = path.strip_suffix('/') else {
        return false;
    };
    let Some((org, namespace)) = inner.split_once('/') else {
        return false;
    };
    org.parse::<u64>().is_ok() && namespace.parse::<u64>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_dotted_strips_trailing_slash() {
        assert_eq!(to_dotted("42/9970/"), "42.9970");
    }

    #[test]
    fn to_dotted_handles_deeper_paths() {
        assert_eq!(to_dotted("42/9970/12345/"), "42.9970.12345");
    }

    #[test]
    fn to_dotted_no_trailing_slash() {
        assert_eq!(to_dotted("42/9970"), "42.9970");
    }

    #[test]
    fn to_dotted_empty() {
        assert_eq!(to_dotted(""), "");
    }

    #[test]
    fn org_id_extracts_first_segment() {
        assert_eq!(org_id("42/9970/"), Some(42));
    }

    #[test]
    fn org_id_with_leading_slash() {
        assert_eq!(org_id("/42/9970/"), Some(42));
    }

    #[test]
    fn org_id_non_numeric() {
        assert_eq!(org_id("abc/9970/"), None);
    }

    #[test]
    fn org_id_empty() {
        assert_eq!(org_id(""), None);
    }

    #[test]
    fn is_valid_accepts_well_formed() {
        assert!(is_valid("1/100/"));
    }

    #[test]
    fn is_valid_rejects_missing_trailing_slash() {
        assert!(!is_valid("1/100"));
    }

    #[test]
    fn is_valid_rejects_single_segment() {
        assert!(!is_valid("100/"));
    }

    #[test]
    fn is_valid_rejects_non_numeric() {
        assert!(!is_valid("abc/100/"));
    }

    #[test]
    fn is_valid_rejects_empty() {
        assert!(!is_valid(""));
    }
}
