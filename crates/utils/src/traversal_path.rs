//! Parsers for the `org_id/root_namespace_id/.../id/` traversal-path string
//! that Siphon publishes (e.g. `42/9970/12345/`).

pub fn org_id(path: &str) -> Option<i64> {
    path.split('/').next()?.parse().ok()
}

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
