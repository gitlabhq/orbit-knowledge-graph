//! Helpers for the `<org_id>/<namespace_id>/` traversal path format used
//! throughout the indexer, NATS topic routing, the query profiler, and the
//! compiler and server authorization scope checks.

use std::collections::{BTreeMap, HashSet};
use std::sync::LazyLock;

use regex::Regex;

static ANY_DEPTH_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(\d+/)+$").expect("valid regex"));

pub fn segments(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|s| !s.is_empty())
}

pub fn segment_count(path: &str) -> usize {
    segments(path).count()
}

pub fn parent(path: &str) -> String {
    match path.trim_end_matches('/').rfind('/') {
        Some(i) => path[..=i].to_string(),
        None => path.to_string(),
    }
}

pub fn overlaps(a: &str, b: &str) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

pub fn is_within_scope(path: &str, allowed: &[&str]) -> bool {
    allowed.iter().any(|prefix| path.starts_with(prefix))
}

pub fn is_valid_any_depth(path: &str) -> bool {
    ANY_DEPTH_REGEX.is_match(path)
}

pub fn validate(path: &str) -> Result<(), String> {
    if !is_valid_any_depth(path) {
        return Err(format!(
            "invalid traversal_path format: '{path}' (expected pattern like '1/2/3/')"
        ));
    }
    for segment in path.trim_end_matches('/').split('/') {
        if segment.parse::<i64>().is_err() {
            return Err(format!(
                "traversal_path segment '{segment}' exceeds i64 range"
            ));
        }
    }
    Ok(())
}

pub fn prune_to_leaves(paths: &[String]) -> Vec<String> {
    if paths.len() <= 1 {
        return paths.to_vec();
    }
    let mut sorted: Vec<&str> = paths.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted.dedup();

    let mut leaves = Vec::with_capacity(sorted.len());
    for (i, path) in sorted.iter().enumerate() {
        let is_prefix_of_next = sorted.get(i + 1).is_some_and(|next| next.starts_with(path));
        if !is_prefix_of_next {
            leaves.push((*path).to_string());
        }
    }
    leaves
}

#[derive(Default)]
pub struct PathTrie {
    children: BTreeMap<String, PathTrie>,
    terminal: bool,
}

impl PathTrie {
    pub fn from_paths(paths: &[&str]) -> Self {
        let mut root = Self::default();
        for path in paths {
            root.insert(path);
        }
        root
    }

    fn insert(&mut self, path: &str) {
        let path_segments: Vec<&str> = segments(path).collect();
        debug_assert!(
            !path_segments.is_empty(),
            "PathTrie::insert called with empty path"
        );
        if path_segments.is_empty() {
            return;
        }
        let mut node = self;
        for seg in path_segments {
            node = node.children.entry(seg.to_string()).or_default();
        }
        node.terminal = true;
    }

    pub fn to_minimal_prefixes(&self) -> Vec<String> {
        let mut result = Vec::new();
        self.collect(&mut String::new(), &mut result);
        result
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

pub fn lowest_common_prefix(paths: &[String]) -> String {
    if paths.is_empty() {
        return String::new();
    }
    let segments: Vec<Vec<&str>> = paths
        .iter()
        .map(|p| p.trim_end_matches('/').split('/').collect())
        .collect();
    let first = &segments[0];
    let common_len = (0..first.len())
        .take_while(|&i| segments.iter().all(|s| s.get(i) == first.get(i)))
        .count();
    if common_len == 0 {
        String::new()
    } else {
        format!("{}/", first[..common_len].join("/"))
    }
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
    segments(path).nth(1).and_then(|s| s.parse().ok())
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
    let mut segments = segments(path);
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

/// Regex (RE2) matching the top-level `<org_id>/<namespace_id>/` prefix of a
/// traversal path. Anchor with `$` to match a path that is exactly top-level.
pub const TOP_LEVEL_PREFIX_REGEX: &str = "^[0-9]+/[0-9]+/";

/// A path is top-level when it is exactly `<org_id>/<namespace_id>/` (two
/// segments). Subgroups (three or more segments) are never indexed.
pub fn is_top_level(path: &str) -> bool {
    segment_count(path) == 2
}

/// Result of [`split_top_level`].
pub struct TopLevelSplit {
    /// Distinct IDs of top-level namespaces.
    pub ids: Vec<i64>,
    /// Traversal paths of the top-level namespaces.
    pub paths: Vec<String>,
    /// `(id, path)` rows dropped for not being top-level.
    pub skipped: Vec<(i64, String)>,
}

/// Partitions enabled `(id, path)` rows into top-level namespaces and the rows
/// dropped for not being top-level.
pub fn split_top_level(ids: Vec<i64>, paths: Vec<String>) -> TopLevelSplit {
    let mut kept_ids = HashSet::new();
    let mut kept_paths = Vec::new();
    let mut skipped = Vec::new();
    for (id, path) in ids.into_iter().zip(paths) {
        if is_top_level(&path) {
            kept_ids.insert(id);
            kept_paths.push(path);
        } else {
            skipped.push((id, path));
        }
    }
    let mut ids = kept_ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    TopLevelSplit {
        ids,
        paths: kept_paths,
        skipped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_trie_subsumes_children() {
        let t = PathTrie::from_paths(&["1/100/", "1/100/200/", "1/100/201/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/"]);
    }

    #[test]
    fn path_trie_keeps_siblings() {
        let t = PathTrie::from_paths(&["1/100/", "1/200/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "1/200/"]);
    }

    #[test]
    fn path_trie_siblings_under_shared_parent() {
        let t = PathTrie::from_paths(&["1/100/200/", "1/100/201/", "1/100/202/", "1/200/300/"]);
        let result = t.to_minimal_prefixes();
        assert_eq!(result.len(), 4);
        assert!(result.contains(&"1/200/300/".to_string()));
    }

    #[test]
    fn path_trie_single_path() {
        let t = PathTrie::from_paths(&["1/100/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/"]);
    }

    #[test]
    fn path_trie_deduplicates() {
        let t = PathTrie::from_paths(&["1/100/", "1/100/", "1/200/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "1/200/"]);
    }

    #[test]
    fn path_trie_deep_subsumption() {
        let t = PathTrie::from_paths(&["1/", "1/100/", "1/100/200/", "1/100/200/300/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/"]);
    }

    #[test]
    fn path_trie_mixed_orgs() {
        let t = PathTrie::from_paths(&["1/100/", "2/100/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "2/100/"]);
    }

    #[test]
    fn path_trie_realistic_38_paths() {
        let mut paths: Vec<String> = (100..130).map(|i| format!("1/10/{i}/")).collect();
        paths.extend((200..208).map(|i| format!("1/{i}/")));
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let t = PathTrie::from_paths(&refs);
        let result = t.to_minimal_prefixes();
        assert_eq!(result.len(), 38);
    }

    #[test]
    fn path_trie_parent_collapses_many_children() {
        let mut paths = vec!["1/10/"];
        let children: Vec<String> = (100..130).map(|i| format!("1/10/{i}/")).collect();
        let refs: Vec<&str> = children.iter().map(|s| s.as_str()).collect();
        paths.extend(refs);
        let t = PathTrie::from_paths(&paths);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/10/"]);
    }

    #[test]
    #[should_panic(expected = "empty path")]
    fn path_trie_empty_path_panics_in_debug() {
        PathTrie::from_paths(&[""]);
    }

    #[test]
    fn leaf_pruning_keeps_sibling_paths() {
        let leaves =
            prune_to_leaves(&["1/9970/".into(), "1/9970/100/".into(), "1/9970/200/".into()]);
        assert_eq!(leaves, vec!["1/9970/100/", "1/9970/200/"]);
    }

    #[test]
    fn leaf_pruning_noop_when_no_ancestors() {
        let leaves = prune_to_leaves(&["1/9970/100/".into(), "1/9970/200/".into()]);
        assert_eq!(leaves, vec!["1/9970/100/", "1/9970/200/"]);
    }

    #[test]
    fn is_within_scope_matches_descendants_and_exact() {
        assert!(is_within_scope("1/22/33/", &["1/22/"]));
        assert!(is_within_scope("1/22/", &["1/22/"]));
        assert!(is_within_scope("1/22/", &["9/", "1/"]));
        assert!(!is_within_scope("1/22/", &["1/23/"]));
        assert!(!is_within_scope("1/22/", &[]));
    }

    #[test]
    fn is_within_scope_respects_segment_boundaries() {
        assert!(!is_within_scope("1/100/", &["1/10/"]));
    }

    #[test]
    fn lowest_common_prefix_finds_shared_path() {
        assert_eq!(
            lowest_common_prefix(&["1/2/4/".into(), "1/2/5/".into()]),
            "1/2/"
        );
        assert_eq!(lowest_common_prefix(&["1/2/".into(), "1/3/".into()]), "1/");
        assert_eq!(lowest_common_prefix(&["1/".into(), "2/".into()]), "");
        assert_eq!(lowest_common_prefix(&["42/".into()]), "42/");
        assert_eq!(lowest_common_prefix(&[]), "");
    }

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

    #[test]
    fn is_valid_rejects_subgroup() {
        assert!(!is_valid("1/100/1000/"));
    }

    #[test]
    fn is_top_level_accepts_org_and_namespace() {
        assert!(is_top_level("1/100/"));
    }

    #[test]
    fn is_top_level_rejects_subgroup_and_malformed() {
        assert!(!is_top_level("1/100/200/"));
        assert!(!is_top_level("0/"));
        assert!(!is_top_level("1/"));
        assert!(!is_top_level(""));
    }

    #[test]
    fn split_top_level_keeps_top_level_and_skips_the_rest() {
        let ids = vec![1, 2, 3, 4];
        let paths = vec![
            "1/100/".to_string(),
            "1/100/200/".to_string(),
            "0/".to_string(),
            "1/300/".to_string(),
        ];
        let split = split_top_level(ids, paths);
        assert_eq!(split.ids, vec![1, 4]);
        assert_eq!(
            split.paths,
            vec!["1/100/".to_string(), "1/300/".to_string()]
        );
        assert_eq!(
            split.skipped,
            vec![(2, "1/100/200/".to_string()), (3, "0/".to_string())]
        );
    }
}

#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct TraversalPath(String);

impl TraversalPath {
    pub fn parse(path: impl Into<String>) -> Result<Self, String> {
        let path = path.into();
        validate(&path)?;
        Ok(Self(path))
    }

    pub fn new_unchecked(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn segments(&self) -> impl Iterator<Item = &str> {
        segments(&self.0)
    }

    pub fn segment_count(&self) -> usize {
        segment_count(&self.0)
    }

    pub fn parent(&self) -> Self {
        Self(parent(&self.0))
    }

    pub fn is_descendant_of(&self, ancestor: &TraversalPath) -> bool {
        self.0.starts_with(&ancestor.0)
    }

    pub fn overlaps(&self, other: &TraversalPath) -> bool {
        overlaps(&self.0, &other.0)
    }

    pub fn is_valid_any_depth(&self) -> bool {
        is_valid_any_depth(&self.0)
    }

    pub fn is_top_level(&self) -> bool {
        is_top_level(&self.0)
    }

    pub fn org_id(&self) -> Option<i64> {
        org_id(&self.0)
    }

    pub fn top_level_namespace_id(&self) -> Option<i64> {
        top_level_namespace_id(&self.0)
    }

    pub fn root_prefix(&self) -> Option<Self> {
        root_prefix(&self.0).map(Self)
    }

    pub fn leaf_id(&self) -> Option<i64> {
        leaf_id(&self.0)
    }

    pub fn to_dotted(&self) -> String {
        to_dotted(&self.0)
    }
}

impl std::fmt::Display for TraversalPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for TraversalPath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for TraversalPath {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl From<TraversalPath> for String {
    fn from(path: TraversalPath) -> Self {
        path.0
    }
}

impl PartialEq<str> for TraversalPath {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for TraversalPath {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}
