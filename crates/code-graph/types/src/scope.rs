use crate::range::Range;
use intervaltree::IntervalTree;
use std::ops;

/// An entry in the scope index: a byte range mapped to some value.
pub trait HasRange {
    fn range(&self) -> Range;
}

/// Spatial index over byte-offset ranges. Build once from definitions (or
/// any `HasRange` items), then query by point or span to find containing
/// scopes.
///
/// Uses an immutable interval tree — construction is O(n log n), point
/// queries are O(log n + k) where k is the number of overlapping results.
pub struct ScopeIndex<T: Clone> {
    tree: IntervalTree<usize, T>,
}

impl<T: Clone> ScopeIndex<T> {
    /// Build the index from items that have byte ranges.
    pub fn from_items<I>(items: I) -> Self
    where
        I: IntoIterator<Item = (Range, T)>,
    {
        let tree: IntervalTree<usize, T> = items
            .into_iter()
            .filter(|(range, _)| range.byte_offset.0 < range.byte_offset.1)
            .map(|(range, val)| {
                let interval = range.byte_offset.0..range.byte_offset.1;
                (interval, val)
            })
            .collect();

        Self { tree }
    }

    /// All entries whose byte range contains the given offset.
    pub fn query_point(&self, offset: usize) -> Vec<&T> {
        self.tree
            .query_point(offset)
            .map(|entry| &entry.value)
            .collect()
    }

    /// All entries whose byte range overlaps `start..end`.
    pub fn query_range(&self, range: ops::Range<usize>) -> Vec<&T> {
        self.tree.query(range).map(|entry| &entry.value).collect()
    }

    /// All entries that fully contain the given byte span.
    pub fn find_containing(&self, start: usize, end: usize) -> Vec<&T>
    where
        T: HasRange,
    {
        self.tree
            .query(start..end)
            .filter(|entry| {
                let r = entry.value.range();
                r.byte_offset.0 <= start && r.byte_offset.1 >= end
            })
            .map(|entry| &entry.value)
            .collect()
    }

    /// The innermost (smallest) entry that fully contains the given byte span.
    pub fn find_innermost(&self, start: usize, end: usize) -> Option<&T>
    where
        T: HasRange,
    {
        self.tree
            .query(start..end)
            .filter(|entry| {
                let r = entry.value.range();
                r.byte_offset.0 <= start && r.byte_offset.1 >= end
            })
            .min_by_key(|entry| {
                let r = entry.value.range();
                r.byte_offset.1 - r.byte_offset.0
            })
            .map(|entry| &entry.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::range::Position;

    #[derive(Debug, Clone, PartialEq)]
    struct TestNode {
        name: String,
        span: Range,
    }

    impl HasRange for TestNode {
        fn range(&self) -> Range {
            self.span
        }
    }

    fn node(name: &str, start: usize, end: usize) -> (Range, TestNode) {
        let range = Range::new(Position::new(0, start), Position::new(0, end), (start, end));
        (
            range,
            TestNode {
                name: name.to_string(),
                span: range,
            },
        )
    }

    #[test]
    fn query_point_finds_containing_scopes() {
        let index = ScopeIndex::from_items(vec![
            node("module", 0, 100),
            node("class", 10, 80),
            node("method", 20, 50),
        ]);

        let results: Vec<&str> = index
            .query_point(25)
            .iter()
            .map(|n| n.name.as_str())
            .collect();
        assert!(results.contains(&"module"));
        assert!(results.contains(&"class"));
        assert!(results.contains(&"method"));
    }

    #[test]
    fn query_point_outside_returns_empty() {
        let index = ScopeIndex::from_items(vec![node("class", 10, 50)]);
        assert!(index.query_point(5).is_empty());
        assert!(index.query_point(55).is_empty());
    }

    #[test]
    fn find_innermost_returns_smallest_containing() {
        let index = ScopeIndex::from_items(vec![
            node("module", 0, 100),
            node("class", 10, 80),
            node("method", 20, 50),
        ]);

        let innermost = index.find_innermost(25, 30).unwrap();
        assert_eq!(innermost.name, "method");
    }

    #[test]
    fn find_containing_excludes_non_containing() {
        let index = ScopeIndex::from_items(vec![node("module", 0, 100), node("sibling", 60, 90)]);

        let results: Vec<&str> = index
            .find_containing(25, 30)
            .iter()
            .map(|n| n.name.as_str())
            .collect();
        assert!(results.contains(&"module"));
        assert!(!results.contains(&"sibling"));
    }

    #[test]
    fn empty_ranges_are_filtered() {
        let index = ScopeIndex::from_items(vec![node("zero", 0, 0), node("real", 10, 50)]);
        let results = index.query_point(25);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "real");
    }
}
