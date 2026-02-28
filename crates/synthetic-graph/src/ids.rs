use std::sync::atomic::{AtomicI64, Ordering};

/// Thread-safe sequential ID allocator.
///
/// Used to generate globally unique entity and namespace IDs during graph
/// generation. Each call to [`next`](IdAllocator::next) returns a monotonically
/// increasing value.
#[derive(Debug)]
pub struct IdAllocator {
    counter: AtomicI64,
}

impl IdAllocator {
    pub fn new(start: i64) -> Self {
        Self {
            counter: AtomicI64::new(start),
        }
    }

    /// Allocate the next sequential ID.
    pub fn next(&self) -> i64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Current value (next ID that will be allocated).
    pub fn current(&self) -> i64 {
        self.counter.load(Ordering::SeqCst)
    }

    /// Reset the counter to a new starting value.
    pub fn reset(&self, value: i64) {
        self.counter.store(value, Ordering::SeqCst);
    }
}

/// Non-atomic sequential ID allocator for single-threaded contexts.
///
/// Lighter weight than [`IdAllocator`] when thread safety isn't needed.
pub struct SeqIdAllocator {
    next: i64,
}

impl SeqIdAllocator {
    pub fn new(start: i64) -> Self {
        Self { next: start }
    }

    pub fn allocate(&mut self) -> i64 {
        let id = self.next;
        self.next += 1;
        id
    }

    pub fn current(&self) -> i64 {
        self.next
    }
}

/// Compute a non-overlapping ID block base for a table in a deterministic layout.
///
/// Used by the datalake generator to pre-compute ID ranges for each table
/// without coordination. Each table gets `block_size` IDs starting at
/// `base_entity_id + table_position * block_size`.
pub fn table_block_base(
    base_entity_id: i64,
    table_position: usize,
    project_count: usize,
    max_rows_per_project: usize,
) -> i64 {
    let block_size = (project_count * max_rows_per_project.max(1) + 1) as i64;
    base_entity_id + table_position as i64 * block_size
}

/// Compute a synthetic row ID within a table's ID block.
pub fn synthetic_row_id(
    table_id_base: i64,
    rows_per_project: usize,
    project_index: usize,
    entity_index: usize,
) -> i64 {
    table_id_base + (project_index * rows_per_project + entity_index) as i64
}

/// Evenly spread children across parents in a stable, repeatable way.
///
/// Returns the parent index for a given child index. Used for deterministic
/// relationship wiring without randomness.
pub fn map_child_to_parent_index(
    child_index: usize,
    child_count: usize,
    parent_count: usize,
) -> usize {
    if child_count == 0 || parent_count == 0 {
        return 0;
    }
    let mapped = child_index.saturating_mul(parent_count) / child_count.max(1);
    mapped.min(parent_count.saturating_sub(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_allocator_sequential() {
        let alloc = IdAllocator::new(1);
        assert_eq!(alloc.next(), 1);
        assert_eq!(alloc.next(), 2);
        assert_eq!(alloc.next(), 3);
        assert_eq!(alloc.current(), 4);
    }

    #[test]
    fn test_seq_id_allocator() {
        let mut alloc = SeqIdAllocator::new(100);
        assert_eq!(alloc.allocate(), 100);
        assert_eq!(alloc.allocate(), 101);
        assert_eq!(alloc.current(), 102);
    }

    #[test]
    fn test_table_block_base() {
        let base = table_block_base(100, 0, 10, 50);
        assert_eq!(base, 100);
        let base = table_block_base(100, 1, 10, 50);
        assert_eq!(base, 100 + 501); // 10 * 50 + 1 = 501
    }

    #[test]
    fn test_synthetic_row_id() {
        let id = synthetic_row_id(1000, 10, 0, 0);
        assert_eq!(id, 1000);
        let id = synthetic_row_id(1000, 10, 2, 5);
        assert_eq!(id, 1025); // 1000 + 2*10 + 5
    }

    #[test]
    fn test_map_child_to_parent() {
        assert_eq!(map_child_to_parent_index(0, 10, 5), 0);
        assert_eq!(map_child_to_parent_index(2, 10, 5), 1);
        assert_eq!(map_child_to_parent_index(9, 10, 5), 4);
        assert_eq!(map_child_to_parent_index(0, 0, 5), 0);
        assert_eq!(map_child_to_parent_index(0, 5, 0), 0);
    }
}
