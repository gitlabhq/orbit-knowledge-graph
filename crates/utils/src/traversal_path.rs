//! Helpers for the `org_id/root_namespace_id/.../id/` traversal-path format
//! that flows from Siphon CDC rows through the indexer and into ClickHouse.
//!
//! Siphon publishes paths in the form `42/9970/12345/` where segment 0 is the
//! organization ID and segment 1 is the top-level (root) namespace ID. Both
//! values are also exposed as their own columns on most tables, but several
//! call sites only have the path string and need to recover one or the other
//! without taking a dependency on the security validator in `query-engine`.

/// Returns the organization ID from segment 0 of `path`, or `None` if the
/// path is empty or the segment is not parseable as `i64`.
pub fn org_id(path: &str) -> Option<i64> {
    path.split('/').next()?.parse().ok()
}

/// Returns the top-level namespace ID from segment 1 of `path`, or `None`
/// if the segment is missing or not parseable as `i64`.
///
/// The dispatcher rejects code-indexing tasks with empty `traversal_path`,
/// so for indexer call sites the `None` arm is treated as a contract
/// violation rather than expected input.
pub fn top_level_namespace_id(path: &str) -> Option<i64> {
    path.split('/').nth(1)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn org_id_extracts_segment_zero() {
        assert_eq!(org_id("42/9970/12345/"), Some(42));
        assert_eq!(org_id("42/9970/"), Some(42));
        assert_eq!(org_id("42"), Some(42));
    }

    #[test]
    fn top_level_namespace_id_extracts_segment_one() {
        assert_eq!(top_level_namespace_id("42/9970/12345/"), Some(9970));
        assert_eq!(top_level_namespace_id("42/9970/"), Some(9970));
    }

    #[test]
    fn returns_none_for_malformed_paths() {
        assert_eq!(org_id(""), None);
        assert_eq!(top_level_namespace_id(""), None);
        assert_eq!(top_level_namespace_id("42"), None);
        assert_eq!(top_level_namespace_id("42/abc/"), None);
        assert_eq!(org_id("not_a_number/9970/"), None);
    }
}
