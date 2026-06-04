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

/// Extract the top-level namespace ID (second segment) from a traversal path.
///
/// `"42/100/" → Some(100)`, `"42/100/1000/" → Some(100)`.
/// Returns `None` when the path has fewer than two segments or the second
/// segment isn't numeric.
pub fn top_level_namespace_id(path: &str) -> Option<i64> {
    let mut segments = path.split('/').filter(|s| !s.is_empty());
    segments.next(); // skip org
    segments.next().and_then(|s| s.parse().ok())
}

/// The top-level-namespace prefix of a traversal path: the first two
/// segments with a trailing slash (`<org_id>/<top_level_ns_id>/`).
///
/// `"42/100/" → Some("42/100/")`, `"42/100/1000/" → Some("42/100/")`.
/// Returns `None` when the path has fewer than two numeric segments, so a
/// malformed path can never produce `startsWith(traversal_path, "")` (which
/// would match every row). Used to bound the system-notes resolver scans to a
/// single top-level namespace partition.
pub fn root_prefix(path: &str) -> Option<String> {
    let mut segments = path.split('/').filter(|s| !s.is_empty());
    let org = segments.next()?;
    let top_level = segments.next()?;
    if org.parse::<u64>().is_err() || top_level.parse::<u64>().is_err() {
        return None;
    }
    Some(format!("{org}/{top_level}/"))
}

/// Extract the leaf namespace ID (last segment) from a traversal path.
///
/// `"1/22/" → Some(22)`, `"1/22/33/" → Some(33)`. Returns `None` when the
/// path is empty or the last segment isn't numeric.
pub fn leaf_id(path: &str) -> Option<i64> {
    path.trim_end_matches('/')
        .rsplit('/')
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
    fn top_level_namespace_id_two_segments() {
        assert_eq!(top_level_namespace_id("42/100/"), Some(100));
    }

    #[test]
    fn top_level_namespace_id_three_segments() {
        assert_eq!(top_level_namespace_id("42/100/1000/"), Some(100));
    }

    #[test]
    fn top_level_namespace_id_single_segment() {
        assert_eq!(top_level_namespace_id("42/"), None);
    }

    #[test]
    fn top_level_namespace_id_empty() {
        assert_eq!(top_level_namespace_id(""), None);
    }

    #[test]
    fn root_prefix_two_segments() {
        assert_eq!(root_prefix("42/100/"), Some("42/100/".to_string()));
    }

    #[test]
    fn root_prefix_truncates_deeper_paths() {
        assert_eq!(root_prefix("42/100/1000/"), Some("42/100/".to_string()));
        assert_eq!(
            root_prefix("42/100/1000/2000/"),
            Some("42/100/".to_string())
        );
    }

    #[test]
    fn root_prefix_single_segment_is_none() {
        assert_eq!(root_prefix("42/"), None);
    }

    #[test]
    fn root_prefix_empty_is_none() {
        assert_eq!(root_prefix(""), None);
    }

    #[test]
    fn root_prefix_non_numeric_is_none() {
        assert_eq!(root_prefix("abc/100/"), None);
        assert_eq!(root_prefix("42/abc/"), None);
    }

    #[test]
    fn leaf_id_extracts_last_segment() {
        assert_eq!(leaf_id("1/22/"), Some(22));
    }

    #[test]
    fn leaf_id_handles_deeper_paths() {
        assert_eq!(leaf_id("1/22/33/"), Some(33));
    }

    #[test]
    fn leaf_id_no_trailing_slash() {
        assert_eq!(leaf_id("1/22"), Some(22));
    }

    #[test]
    fn leaf_id_non_numeric() {
        assert_eq!(leaf_id("1/abc/"), None);
    }

    #[test]
    fn leaf_id_empty() {
        assert_eq!(leaf_id(""), None);
    }

    #[test]
    fn leaf_id_only_slash() {
        assert_eq!(leaf_id("/"), None);
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
