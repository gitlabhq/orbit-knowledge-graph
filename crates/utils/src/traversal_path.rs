//! Single source of truth for the `<org_id>/<namespace_id>/…/` traversal path
//! format used across the indexer, query compiler, gRPC server, and NATS topic
//! routing.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use regex::Regex;

/// Matches paths like `"1/"`, `"1/2/"`, `"123/456/789/"`.
static TRAVERSAL_PATH_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(\d+/)+$").expect("valid regex"));

// ─────────────────────────────────────────────────────────────────────────────
// Parsing helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Iterate over the non-empty segments of a traversal path.
///
/// `"1/100/1000/" → ["1", "100", "1000"]`
pub fn segments(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|s| !s.is_empty())
}

/// Convert slash-separated segments to dot-separated, stripping empties.
///
/// `"42/9970/" → "42.9970"`, `"42/9970/12345/" → "42.9970.12345"`.
pub fn to_dotted(path: &str) -> String {
    segments(path).collect::<Vec<_>>().join(".")
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
    let mut segs = segments(path);
    segs.next(); // skip org
    segs.next().and_then(|s| s.parse().ok())
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

// ─────────────────────────────────────────────────────────────────────────────
// Validation
// ─────────────────────────────────────────────────────────────────────────────

/// Validate that `path` matches `^(\d+/)+$` and every segment fits in i64.
///
/// This is the strict check used by the query compiler's `SecurityContext`
/// and the indexer's namespace deletion handler. Subsumes the old
/// two-segment-only `is_valid` check.
pub fn validate(path: &str) -> Result<(), ValidationError> {
    if !TRAVERSAL_PATH_RE.is_match(path) {
        return Err(ValidationError::Format(path.to_string()));
    }
    for seg in segments(path) {
        seg.parse::<i64>()
            .map_err(|_| ValidationError::Overflow(seg.to_string()))?;
    }
    Ok(())
}

/// Returns `true` when `path` passes [`validate`] and has at least two
/// segments (org + namespace). This is the check the indexer uses for
/// dispatchable and deletable paths -- stricter than `validate` which
/// accepts single-segment org-root paths like `"1/"`.
pub fn is_valid(path: &str) -> bool {
    validate(path).is_ok() && segments(path).count() >= 2
}

/// Errors from [`validate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    /// Path does not match the expected `<int>/<int>/…/` shape.
    Format(String),
    /// A segment is numeric but exceeds i64 range.
    Overflow(String),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Format(p) => write!(
                f,
                "invalid traversal_path format: '{p}' (expected pattern like '1/2/3/')"
            ),
            Self::Overflow(seg) => {
                write!(f, "traversal_path segment '{seg}' exceeds i64 range")
            }
        }
    }
}

impl std::error::Error for ValidationError {}

// ─────────────────────────────────────────────────────────────────────────────
// Scope checks
// ─────────────────────────────────────────────────────────────────────────────

/// Returns `true` when `path` is a prefix-match of at least one entry in
/// `allowed_paths` (i.e. the path falls within an authorized scope).
///
/// Used by the gRPC server to check that a requested traversal path is
/// within the caller's JWT-granted scopes.
pub fn is_within_scope(path: &str, allowed_paths: &[&str]) -> bool {
    allowed_paths
        .iter()
        .filter(|p| p.ends_with('/'))
        .any(|allowed| path.starts_with(allowed))
}

/// Find the longest common prefix across a set of traversal paths.
///
/// `["1/100/", "1/200/"] → "1/"`, `["1/100/1000/", "1/100/2000/"] → "1/100/"`.
/// Returns an empty string when `paths` is empty or there is no shared prefix.
pub fn lowest_common_prefix(paths: &[impl AsRef<str>]) -> String {
    if paths.is_empty() {
        return String::new();
    }
    let segs: Vec<Vec<&str>> = paths
        .iter()
        .map(|p| segments(p.as_ref()).collect())
        .collect();
    let first = &segs[0];
    let common_len = (0..first.len())
        .take_while(|&i| segs.iter().all(|s| s.get(i) == first.get(i)))
        .count();
    if common_len == 0 {
        String::new()
    } else {
        format!("{}/", first[..common_len].join("/"))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Path collapsing
// ─────────────────────────────────────────────────────────────────────────────

/// Segment-level trie node for collapsing traversal paths. Not public —
/// callers use [`minimal_prefixes`] instead.
#[derive(Default)]
struct TrieNode {
    children: BTreeMap<String, TrieNode>,
    terminal: bool,
}

impl TrieNode {
    fn insert(&mut self, path: &str) {
        let segs: Vec<&str> = segments(path).collect();
        if segs.is_empty() {
            return;
        }
        let mut node = self;
        for seg in segs {
            node = node.children.entry(seg.to_string()).or_default();
        }
        node.terminal = true;
    }

    fn collect(&self, prefix: &mut String, out: &mut Vec<String>) {
        if self.terminal {
            let mut p = prefix.clone();
            if !p.is_empty() {
                p.push('/');
            }
            out.push(p);
            return;
        }
        for (seg, child) in &self.children {
            let restore_len = prefix.len();
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(seg);
            child.collect(prefix, out);
            prefix.truncate(restore_len);
        }
    }
}

/// Collapse a set of traversal paths into the minimal set of prefixes.
///
/// A parent path subsumes its children: `["1/100/", "1/100/200/"]` collapses
/// to `["1/100/"]`. Siblings are kept: `["1/100/", "1/200/"]` stays as-is.
///
/// Used by the query compiler's security pass to produce the smallest set
/// of SQL `startsWith` predicates.
pub fn minimal_prefixes(paths: &[&str]) -> Vec<String> {
    let mut root = TrieNode::default();
    for path in paths {
        root.insert(path);
    }
    let mut result = Vec::new();
    root.collect(&mut String::new(), &mut result);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── segments ─────────────────────────────────────────────────────────

    #[test]
    fn segments_basic() {
        let s: Vec<&str> = segments("1/100/1000/").collect();
        assert_eq!(s, ["1", "100", "1000"]);
    }

    #[test]
    fn segments_no_trailing_slash() {
        let s: Vec<&str> = segments("1/100").collect();
        assert_eq!(s, ["1", "100"]);
    }

    #[test]
    fn segments_empty() {
        let s: Vec<&str> = segments("").collect();
        assert!(s.is_empty());
    }

    // ── to_dotted ────────────────────────────────────────────────────────

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

    // ── org_id / top_level / leaf ────────────────────────────────────────

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

    // ── validate ─────────────────────────────────────────────────────────

    #[test]
    fn validate_accepts_two_segments() {
        assert!(validate("1/100/").is_ok());
    }

    #[test]
    fn validate_accepts_three_segments() {
        assert!(validate("1/100/1000/").is_ok());
    }

    #[test]
    fn validate_rejects_missing_trailing_slash() {
        assert!(validate("1/100").is_err());
    }

    #[test]
    fn validate_accepts_single_segment() {
        // Single-segment paths like "100/" match ^(\d+/)+$ and are valid
        // structurally. Whether they're semantically meaningful depends on
        // context (the security pass uses org-root paths like "1/").
        assert!(validate("100/").is_ok());
    }

    #[test]
    fn validate_rejects_non_numeric() {
        assert!(validate("abc/100/").is_err());
    }

    #[test]
    fn validate_rejects_empty() {
        assert!(validate("").is_err());
    }

    #[test]
    fn validate_rejects_only_slash() {
        assert!(validate("/").is_err());
    }

    #[test]
    fn validate_rejects_double_slash() {
        assert!(validate("1//100/").is_err());
    }

    #[test]
    fn validate_overflow_segment() {
        // 10^20 exceeds i64::MAX
        let big = format!("{}/1/", "9".repeat(20));
        assert!(matches!(validate(&big), Err(ValidationError::Overflow(_))));
    }

    #[test]
    fn is_valid_requires_two_segments() {
        assert!(is_valid("1/100/"));
        assert!(!is_valid("100/")); // single segment rejected
        assert!(!is_valid("bad"));
    }

    // ── scope checks ────────────────────────────────────────────────────

    #[test]
    fn is_within_scope_matches_prefix() {
        assert!(is_within_scope("1/100/1000/", &["1/100/"]));
    }

    #[test]
    fn is_within_scope_rejects_outside() {
        assert!(!is_within_scope("1/200/", &["1/100/"]));
    }

    #[test]
    fn is_within_scope_exact_match() {
        assert!(is_within_scope("1/100/", &["1/100/"]));
    }

    #[test]
    fn is_within_scope_multiple_allowed() {
        assert!(is_within_scope("1/200/", &["1/100/", "1/200/"]));
    }

    #[test]
    fn is_within_scope_empty_allowed() {
        assert!(!is_within_scope("1/100/", &[]));
    }

    // ── lowest_common_prefix ────────────────────────────────────────────

    #[test]
    fn lcp_same_org() {
        assert_eq!(lowest_common_prefix(&["1/100/", "1/200/"]), "1/");
    }

    #[test]
    fn lcp_same_namespace() {
        assert_eq!(
            lowest_common_prefix(&["1/100/1000/", "1/100/2000/"]),
            "1/100/"
        );
    }

    #[test]
    fn lcp_no_common() {
        assert_eq!(lowest_common_prefix(&["1/100/", "2/200/"]), "");
    }

    #[test]
    fn lcp_empty() {
        let empty: &[&str] = &[];
        assert_eq!(lowest_common_prefix(empty), "");
    }

    #[test]
    fn lcp_single() {
        assert_eq!(lowest_common_prefix(&["1/100/"]), "1/100/");
    }

    // ── minimal_prefixes ──────────────────────────────────────────────

    #[test]
    fn min_single_path() {
        assert_eq!(minimal_prefixes(&["1/100/"]), vec!["1/100/"]);
    }

    #[test]
    fn min_subsumes_children() {
        assert_eq!(minimal_prefixes(&["1/100/", "1/100/1000/"]), vec!["1/100/"]);
    }

    #[test]
    fn min_sibling_paths() {
        assert_eq!(
            minimal_prefixes(&["1/100/", "1/200/"]),
            vec!["1/100/", "1/200/"]
        );
    }

    #[test]
    fn min_deep_path() {
        assert_eq!(minimal_prefixes(&["1/100/1000/"]), vec!["1/100/1000/"]);
    }

    #[test]
    fn min_org_root_subsumes_all() {
        assert_eq!(minimal_prefixes(&["1/", "1/100/", "1/200/"]), vec!["1/"]);
    }

    #[test]
    fn min_empty_path_ignored() {
        assert!(minimal_prefixes(&[""]).is_empty());
    }
}
