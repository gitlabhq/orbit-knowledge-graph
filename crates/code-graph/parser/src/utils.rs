//! Common utilities and types for the parser core

pub use code_graph_types::{Position, Range};
use rust_lapper::{Interval, Lapper};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};
}

pub fn compare_positions(p1: &Position, p2: &Position) -> std::cmp::Ordering {
    p1.line
        .cmp(&p2.line)
        .then_with(|| p1.column.cmp(&p2.column))
}

/// Convert a tree-sitter node to a Range
pub fn node_to_range(node: &Node<StrDoc<SupportLang>>) -> Range {
    let start_pos = node.start_pos();
    let end_pos = node.end_pos();
    let byte_range = node.range();

    Range::new(
        Position::new(start_pos.line(), start_pos.column(node)),
        Position::new(end_pos.line(), end_pos.column(node)),
        (byte_range.start, byte_range.end),
    )
}

/// Trait for entities that occupy a byte `Range` in a document
pub trait HasRange {
    fn range(&self) -> Range;
}

pub struct IntervalTree<T: HasRange + Clone + Eq + Send + Sync> {
    lapper: Lapper<u64, T>,
}

impl<T: HasRange + Clone + Eq + Send + Sync> IntervalTree<T> {
    pub fn new(items: Vec<(Range, T)>) -> Self {
        let intervals: Vec<Interval<u64, T>> = items
            .into_iter()
            .map(|(range, symbol)| Interval {
                start: range.byte_offset.0 as u64,
                stop: range.byte_offset.1 as u64,
                val: symbol,
            })
            .collect();

        Self {
            lapper: Lapper::new(intervals),
        }
    }

    // Find the nearest symbol that contains the given range
    pub fn find_containing(&self, start: u64, end: u64) -> Option<&T> {
        self.lapper
            .find(start, end)
            .map(|interval| &interval.val)
            .find(|item| {
                let item_range = item.range();
                // The item contains the given range if:
                // 1. It's not the same range (to avoid self-containment)
                // 2. The item starts before or at the given start
                // 3. The item ends after or at the given end
                !(item_range.byte_offset.0 == start as usize
                    && item_range.byte_offset.1 == end as usize)
                    && item_range.byte_offset.0 <= start as usize
                    && item_range.byte_offset.1 >= end as usize
            })
    }

    // Find all symbols contained within a given range
    pub fn find_contained(&self, start: u64, end: u64) -> Vec<&T> {
        self.lapper
            .find(start, end)
            .map(|interval| &interval.val)
            .filter(|item| {
                let item_range = item.range();
                !(item_range.byte_offset.0 == start as usize
                    && item_range.byte_offset.1 == end as usize)
            })
            .collect()
    }

    // Find all symbols that contain the given range
    pub fn find_all_containing(&self, start: u64, end: u64) -> Vec<&T> {
        self.lapper
            .find(start, end)
            .map(|interval| &interval.val)
            .filter(|item| {
                let item_range = item.range();
                let item_start = item_range.byte_offset.0 as u64;
                let item_end = item_range.byte_offset.1 as u64;

                // The item contains the given range if:
                // 1. It's not the exact same range (to avoid self-containment)
                // 2. The item starts before or at the given start
                // 3. The item ends after or at the given end
                // 4. The item is strictly larger (not equal in both start and end)
                !(item_start == start && item_end == end) && item_start <= start && item_end >= end
            })
            .collect()
    }

    // Find the most immediate parent (smallest containing symbol)
    pub fn find_immediate_parent(&self, start: u64, end: u64) -> Option<&T> {
        self.find_all_containing(start, end)
            .into_iter()
            .min_by_key(|item| {
                let range = item.range();
                range.byte_offset.1 - range.byte_offset.0
            })
    }

    pub fn find_immediate_children(&self, start: u64, end: u64) -> Vec<&T> {
        self.lapper
            .find(start, end)
            .map(|interval| &interval.val)
            .filter(|item| {
                let item_range = item.range();
                let item_start = item_range.byte_offset.0 as u64;
                let item_end = item_range.byte_offset.1 as u64;

                // Item must be contained within the given range (not equal)
                item_start >= start && item_end <= end && !(item_start == start && item_end == end)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_creation() {
        let pos = Position::new(5, 10);
        assert_eq!(pos.line, 5);
        assert_eq!(pos.column, 10);
    }

    #[test]
    fn test_range_contains() {
        let range = Range::new(Position::new(1, 0), Position::new(3, 10), (0, 50));

        assert!(range.contains(&Position::new(2, 5)));
        assert!(range.contains(&Position::new(1, 0)));
        assert!(range.contains(&Position::new(3, 10)));
        assert!(!range.contains(&Position::new(0, 5)));
        assert!(!range.contains(&Position::new(4, 0)));
    }

    #[test]
    fn test_range_metrics() {
        let range = Range::new(Position::new(1, 0), Position::new(3, 10), (0, 50));

        assert_eq!(range.line_span(), 3);
        assert_eq!(range.byte_length(), 50);
    }

    #[test]
    fn test_compare_positions() {
        let p1 = Position::new(1, 5);
        let p2 = Position::new(2, 3);
        let p3 = Position::new(1, 10);

        assert_eq!(compare_positions(&p1, &p2), std::cmp::Ordering::Less);
        assert_eq!(compare_positions(&p2, &p1), std::cmp::Ordering::Greater);
        assert_eq!(compare_positions(&p1, &p3), std::cmp::Ordering::Less);
        assert_eq!(compare_positions(&p1, &p1), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_range_contains_edge_cases() {
        let range = Range::new(Position::new(2, 5), Position::new(2, 15), (10, 20));

        // Test same line range
        assert!(range.contains(&Position::new(2, 5))); // start boundary
        assert!(range.contains(&Position::new(2, 10))); // middle
        assert!(range.contains(&Position::new(2, 15))); // end boundary
        assert!(!range.contains(&Position::new(2, 4))); // before start
        assert!(!range.contains(&Position::new(2, 16))); // after end
        assert!(!range.contains(&Position::new(1, 10))); // line before
        assert!(!range.contains(&Position::new(3, 10))); // line after
    }

    #[test]
    fn test_const_functions() {
        // Test that const functions work at compile time
        const POS: Position = Position::new(1, 2);
        const RANGE: Range = Range::new(POS, Position::new(3, 4), (0, 10));

        assert_eq!(POS.line, 1);
        assert_eq!(POS.column, 2);
        assert_eq!(RANGE.line_span(), 3);
        assert_eq!(RANGE.byte_length(), 10);
    }

    #[test]
    fn interval_tree_test_find_immediate_children() {
        #[derive(Clone, Eq, PartialEq)]
        struct TestItem {
            range: Range,
            name: String,
        }

        impl HasRange for TestItem {
            fn range(&self) -> Range {
                self.range
            }
        }

        fn make_range(start: u64, end: u64) -> Range {
            Range {
                start: Position { line: 0, column: 0 },
                end: Position { line: 0, column: 0 },
                byte_offset: (start as usize, end as usize),
            }
        }

        let index = IntervalTree::new(vec![
            (
                make_range(0, 100),
                TestItem {
                    range: make_range(0, 100),
                    name: "parent".to_string(),
                },
            ),
            (
                make_range(10, 30),
                TestItem {
                    range: make_range(10, 30),
                    name: "child1".to_string(),
                },
            ),
            (
                make_range(40, 60),
                TestItem {
                    range: make_range(40, 60),
                    name: "child2".to_string(),
                },
            ),
            (
                make_range(70, 90),
                TestItem {
                    range: make_range(70, 90),
                    name: "child3".to_string(),
                },
            ),
        ]);

        // Find children within the parent range
        let children = index.find_immediate_children(0, 100);
        assert_eq!(children.len(), 3);
        let names: Vec<&str> = children.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"child1"));
        assert!(names.contains(&"child2"));
        assert!(names.contains(&"child3"));

        // Find children within a smaller range
        let children = index.find_immediate_children(5, 35);
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, "child1");

        // No children when range is too small
        let children = index.find_immediate_children(15, 25);
        assert_eq!(children.len(), 0);
    }
}
