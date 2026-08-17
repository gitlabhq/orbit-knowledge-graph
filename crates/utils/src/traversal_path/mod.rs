//! Helpers for the `<org_id>/<namespace_id>/` traversal path format used
//! throughout the indexer, NATS topic routing, and query profiler.

use std::collections::HashSet;
use std::sync::LazyLock;

use regex::Regex;

mod trie;

pub use trie::TraversalPathTrie;

static ANY_DEPTH_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(\d+/)+$").expect("valid regex"));

pub fn prune_to_leaves(paths: &[TraversalPath]) -> Vec<TraversalPath> {
    if paths.len() <= 1 {
        return paths.to_vec();
    }
    let mut sorted: Vec<&TraversalPath> = paths.iter().collect();
    sorted.sort_unstable();
    sorted.dedup();

    let mut leaves = Vec::with_capacity(sorted.len());
    for (i, path) in sorted.iter().enumerate() {
        let is_prefix_of_next = sorted
            .get(i + 1)
            .is_some_and(|next| next.is_descendant_of(path));
        if !is_prefix_of_next {
            leaves.push((*path).clone());
        }
    }
    leaves
}

pub fn lowest_common_prefix(paths: &[TraversalPath]) -> TraversalPath {
    debug_assert!(
        paths.iter().all(|p| p.0.ends_with('/')),
        "lowest_common_prefix requires '/'-terminated traversal paths"
    );
    let Some((first, rest)) = paths.split_first() else {
        return TraversalPath(String::new());
    };
    let mut cursors: Vec<_> = rest.iter().map(|p| p.segments()).collect();
    let mut out = String::new();
    for seg in first.segments() {
        if !cursors.iter_mut().all(|c| c.next() == Some(seg)) {
            break;
        }
        out.push_str(seg);
        out.push('/');
    }
    TraversalPath(out)
}

/// Regex (RE2) matching the top-level `<org_id>/<namespace_id>/` prefix of a
/// traversal path. Anchor with `$` to match a path that is exactly top-level.
pub const TOP_LEVEL_PREFIX_REGEX: &str = "^[0-9]+/[0-9]+/";

/// Result of [`split_top_level`].
pub struct TopLevelSplit {
    /// Distinct IDs of top-level namespaces.
    pub ids: Vec<i64>,
    /// Traversal paths of the top-level namespaces.
    pub paths: Vec<TraversalPath>,
    /// `(id, path)` rows dropped for not being top-level.
    pub skipped: Vec<(i64, TraversalPath)>,
}

/// Partitions enabled `(id, path)` rows into top-level namespaces and the rows
/// dropped for not being top-level.
pub fn split_top_level(ids: Vec<i64>, paths: Vec<TraversalPath>) -> TopLevelSplit {
    let mut kept_ids = HashSet::new();
    let mut kept_paths = Vec::new();
    let mut skipped = Vec::new();
    for (id, path) in ids.into_iter().zip(paths) {
        if path.is_top_level() {
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

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct TraversalPath(String);

impl std::fmt::Debug for TraversalPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl TraversalPath {
    pub fn new_unchecked(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('/').filter(|s| !s.is_empty())
    }

    pub fn segment_count(&self) -> usize {
        self.segments().count()
    }

    pub fn parent(&self) -> Self {
        match self.0.trim_end_matches('/').rfind('/') {
            Some(i) => Self(self.0[..=i].to_string()),
            None => self.clone(),
        }
    }

    pub fn is_descendant_of(&self, ancestor: &TraversalPath) -> bool {
        self.0.starts_with(&ancestor.0)
    }

    pub fn is_within_scope(&self, allowed: &[&TraversalPath]) -> bool {
        allowed.iter().any(|prefix| {
            self.0.starts_with(&prefix.0)
                && (prefix.0.ends_with('/')
                    || self.0.len() == prefix.0.len()
                    || self.0.as_bytes().get(prefix.0.len()) == Some(&b'/'))
        })
    }

    pub fn overlaps(&self, other: &TraversalPath) -> bool {
        self.0.starts_with(&other.0) || other.0.starts_with(&self.0)
    }

    /// A traversal path is valid when it matches `<org_id>/<namespace_id>/`
    /// where both segments are unsigned integers.
    ///
    /// An empty or malformed path would cause `startsWith(traversal_path, '')`
    /// to match every row in the table.
    pub fn is_valid(&self) -> bool {
        let Some(inner) = self.0.strip_suffix('/') else {
            return false;
        };
        let Some((org, namespace)) = inner.split_once('/') else {
            return false;
        };
        org.parse::<u64>().is_ok() && namespace.parse::<u64>().is_ok()
    }

    pub fn is_valid_any_depth(&self) -> bool {
        ANY_DEPTH_REGEX.is_match(&self.0)
    }

    pub fn validate(&self) -> Result<(), String> {
        if !self.is_valid_any_depth() {
            return Err(format!(
                "invalid traversal_path format: '{}' (expected pattern like '1/2/3/')",
                self.0
            ));
        }
        for segment in self.0.trim_end_matches('/').split('/') {
            if segment.parse::<i64>().is_err() {
                return Err(format!(
                    "traversal_path segment '{segment}' exceeds i64 range"
                ));
            }
        }
        Ok(())
    }

    /// A path is top-level when it is exactly `<org_id>/<namespace_id>/` (two
    /// segments). Subgroups (three or more segments) are never indexed.
    pub fn is_top_level(&self) -> bool {
        self.segment_count() == 2
    }

    /// Extract the organization ID (first segment) from a traversal path.
    ///
    /// Returns `None` when the path is empty or the first segment isn't numeric.
    pub fn organization_id(&self) -> Option<i64> {
        self.0
            .trim_start_matches('/')
            .split('/')
            .next()
            .and_then(|s| s.parse().ok())
    }

    /// Extract the top-level namespace ID (second segment) from a traversal path.
    ///
    /// `"42/100/" → Some(100)`, `"42/100/1000/" → Some(100)`.
    /// Returns `None` when the path has fewer than two segments or the second
    /// segment isn't numeric.
    pub fn top_level_namespace_id(&self) -> Option<i64> {
        self.segments().nth(1).and_then(|s| s.parse().ok())
    }

    /// The top-level-namespace prefix of a traversal path: the first two
    /// segments with a trailing slash (`<org_id>/<top_level_ns_id>/`).
    ///
    /// `"42/100/" → Some("42/100/")`, `"42/100/1000/" → Some("42/100/")`.
    /// Returns `None` when the path has fewer than two numeric segments, so a
    /// malformed path can never produce `startsWith(traversal_path, "")` (which
    /// would match every row). Used to bound the system-notes resolver scans to a
    /// single top-level namespace partition.
    pub fn root_prefix(&self) -> Option<Self> {
        let mut segments = self.segments();
        let org = segments.next()?;
        let top_level = segments.next()?;
        if org.parse::<u64>().is_err() || top_level.parse::<u64>().is_err() {
            return None;
        }
        Some(Self(format!("{org}/{top_level}/")))
    }

    /// Extract the leaf namespace ID (last segment) from a traversal path.
    ///
    /// `"1/22/" → Some(22)`, `"1/22/33/" → Some(33)`. Returns `None` when the
    /// path is empty or the last segment isn't numeric.
    pub fn leaf_id(&self) -> Option<i64> {
        self.0
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .and_then(|s| s.parse().ok())
    }

    /// Convert slash-separated segments to dot-separated, stripping empties.
    ///
    /// `"42/9970/" → "42.9970"`, `"42/9970/12345/" → "42.9970.12345"`.
    pub fn to_dotted(&self) -> String {
        self.segments().collect::<Vec<_>>().join(".")
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

impl From<&str> for TraversalPath {
    fn from(path: &str) -> Self {
        Self(path.to_string())
    }
}

impl From<String> for TraversalPath {
    fn from(path: String) -> Self {
        Self(path)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(s: &str) -> TraversalPath {
        TraversalPath::from(s)
    }

    #[test]
    fn leaf_pruning_keeps_sibling_paths() {
        let leaves = prune_to_leaves(&[tp("1/9970/"), tp("1/9970/100/"), tp("1/9970/200/")]);
        assert_eq!(leaves, vec!["1/9970/100/", "1/9970/200/"]);
    }

    #[test]
    fn leaf_pruning_noop_when_no_ancestors() {
        let leaves = prune_to_leaves(&[tp("1/9970/100/"), tp("1/9970/200/")]);
        assert_eq!(leaves, vec!["1/9970/100/", "1/9970/200/"]);
    }

    #[test]
    fn is_within_scope_matches_descendants_and_exact() {
        assert!(tp("1/22/33/").is_within_scope(&[&tp("1/22/")]));
        assert!(tp("1/22/").is_within_scope(&[&tp("1/22/")]));
        assert!(tp("1/22/").is_within_scope(&[&tp("9/"), &tp("1/")]));
        assert!(!tp("1/22/").is_within_scope(&[&tp("1/23/")]));
        assert!(!tp("1/22/").is_within_scope(&[]));
    }

    #[test]
    fn is_within_scope_respects_segment_boundaries() {
        assert!(!tp("1/100/").is_within_scope(&[&tp("1/10/")]));
        assert!(!tp("1/100/").is_within_scope(&[&tp("1/10")]));
        assert!(tp("1/10/300/").is_within_scope(&[&tp("1/10")]));
        assert!(tp("1/10").is_within_scope(&[&tp("1/10")]));
    }

    #[test]
    fn lowest_common_prefix_finds_shared_path() {
        assert_eq!(lowest_common_prefix(&[tp("1/2/4/"), tp("1/2/5/")]), "1/2/");
        assert_eq!(lowest_common_prefix(&[tp("1/2/"), tp("1/3/")]), "1/");
        assert_eq!(lowest_common_prefix(&[tp("1/"), tp("2/")]), "");
        assert_eq!(lowest_common_prefix(&[tp("42/")]), "42/");
        assert_eq!(lowest_common_prefix(&[]), "");
    }

    #[test]
    fn to_dotted_strips_trailing_slash() {
        assert_eq!(tp("42/9970/").to_dotted(), "42.9970");
    }

    #[test]
    fn to_dotted_handles_deeper_paths() {
        assert_eq!(tp("42/9970/12345/").to_dotted(), "42.9970.12345");
    }

    #[test]
    fn to_dotted_no_trailing_slash() {
        assert_eq!(tp("42/9970").to_dotted(), "42.9970");
    }

    #[test]
    fn to_dotted_empty() {
        assert_eq!(tp("").to_dotted(), "");
    }

    #[test]
    fn organization_id_extracts_first_segment() {
        assert_eq!(tp("42/9970/").organization_id(), Some(42));
    }

    #[test]
    fn organization_id_with_leading_slash() {
        assert_eq!(tp("/42/9970/").organization_id(), Some(42));
    }

    #[test]
    fn organization_id_non_numeric() {
        assert_eq!(tp("abc/9970/").organization_id(), None);
    }

    #[test]
    fn organization_id_empty() {
        assert_eq!(tp("").organization_id(), None);
    }

    #[test]
    fn top_level_namespace_id_two_segments() {
        assert_eq!(tp("42/100/").top_level_namespace_id(), Some(100));
    }

    #[test]
    fn top_level_namespace_id_three_segments() {
        assert_eq!(tp("42/100/1000/").top_level_namespace_id(), Some(100));
    }

    #[test]
    fn top_level_namespace_id_single_segment() {
        assert_eq!(tp("42/").top_level_namespace_id(), None);
    }

    #[test]
    fn top_level_namespace_id_empty() {
        assert_eq!(tp("").top_level_namespace_id(), None);
    }

    #[test]
    fn root_prefix_two_segments() {
        assert_eq!(tp("42/100/").root_prefix(), Some(tp("42/100/")));
    }

    #[test]
    fn root_prefix_truncates_deeper_paths() {
        assert_eq!(tp("42/100/1000/").root_prefix(), Some(tp("42/100/")));
        assert_eq!(tp("42/100/1000/2000/").root_prefix(), Some(tp("42/100/")));
    }

    #[test]
    fn root_prefix_single_segment_is_none() {
        assert_eq!(tp("42/").root_prefix(), None);
    }

    #[test]
    fn root_prefix_empty_is_none() {
        assert_eq!(tp("").root_prefix(), None);
    }

    #[test]
    fn root_prefix_non_numeric_is_none() {
        assert_eq!(tp("abc/100/").root_prefix(), None);
        assert_eq!(tp("42/abc/").root_prefix(), None);
    }

    #[test]
    fn leaf_id_extracts_last_segment() {
        assert_eq!(tp("1/22/").leaf_id(), Some(22));
    }

    #[test]
    fn leaf_id_handles_deeper_paths() {
        assert_eq!(tp("1/22/33/").leaf_id(), Some(33));
    }

    #[test]
    fn leaf_id_no_trailing_slash() {
        assert_eq!(tp("1/22").leaf_id(), Some(22));
    }

    #[test]
    fn leaf_id_non_numeric() {
        assert_eq!(tp("1/abc/").leaf_id(), None);
    }

    #[test]
    fn leaf_id_empty() {
        assert_eq!(tp("").leaf_id(), None);
    }

    #[test]
    fn leaf_id_only_slash() {
        assert_eq!(tp("/").leaf_id(), None);
    }

    #[test]
    fn is_valid_accepts_well_formed() {
        assert!(tp("1/100/").is_valid());
    }

    #[test]
    fn is_valid_rejects_missing_trailing_slash() {
        assert!(!tp("1/100").is_valid());
    }

    #[test]
    fn is_valid_rejects_single_segment() {
        assert!(!tp("100/").is_valid());
    }

    #[test]
    fn is_valid_rejects_non_numeric() {
        assert!(!tp("abc/100/").is_valid());
    }

    #[test]
    fn is_valid_rejects_empty() {
        assert!(!tp("").is_valid());
    }

    #[test]
    fn is_valid_rejects_subgroup() {
        assert!(!tp("1/100/1000/").is_valid());
    }

    #[test]
    fn is_top_level_accepts_org_and_namespace() {
        assert!(tp("1/100/").is_top_level());
    }

    #[test]
    fn is_top_level_rejects_subgroup_and_malformed() {
        assert!(!tp("1/100/200/").is_top_level());
        assert!(!tp("0/").is_top_level());
        assert!(!tp("1/").is_top_level());
        assert!(!tp("").is_top_level());
    }

    #[test]
    fn split_top_level_keeps_top_level_and_skips_the_rest() {
        let ids = vec![1, 2, 3, 4];
        let paths = vec![
            TraversalPath::new_unchecked("1/100/"),
            TraversalPath::new_unchecked("1/100/200/"),
            TraversalPath::new_unchecked("0/"),
            TraversalPath::new_unchecked("1/300/"),
        ];
        let split = split_top_level(ids, paths);
        assert_eq!(split.ids, vec![1, 4]);
        assert_eq!(split.paths, vec!["1/100/", "1/300/"]);
        assert_eq!(
            split.skipped,
            vec![
                (2, TraversalPath::new_unchecked("1/100/200/")),
                (3, TraversalPath::new_unchecked("0/"))
            ]
        );
    }
}
