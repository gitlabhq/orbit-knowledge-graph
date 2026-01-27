//! Traversal ID generation for hierarchical authorization.
//!
//! Traversal IDs are hierarchical paths like `1/2/5/12` where each component
//! is a globally unique namespace ID. The path represents the ancestry chain
//! from root to the current namespace. This matches GitLab's namespace traversal
//! ID system for row-level authorization.
//!
//! Example tree:
//! ```text
//!   1 (org root)
//!   ├── 2
//!   │   ├── 4
//!   │   │   └── 7
//!   │   └── 5
//!   └── 3
//!       └── 6
//!           ├── 8
//!           └── 9
//! ```
//!
//! Produces traversal IDs: `1`, `1/2`, `1/3`, `1/2/4`, `1/2/5`, `1/3/6`, `1/2/4/7`, `1/3/6/8`, `1/3/6/9`

use fake::rand::Rng;

/// Generates hierarchical traversal IDs for an organization.
///
/// Each organization gets a set of unique traversal IDs that form a tree structure.
/// IDs are globally unique and monotonically increasing within the organization.
pub struct TraversalIdGenerator {
    /// Generated traversal IDs for this organization.
    ids: Vec<String>,
}

impl TraversalIdGenerator {
    /// Generate a hierarchy of traversal IDs for an organization.
    ///
    /// # Arguments
    /// * `org_id` - The organization ID (becomes root of all paths)
    /// * `count` - Target number of unique traversal IDs to generate
    /// * `max_depth` - Maximum depth of the hierarchy
    pub fn new(org_id: u32, count: usize, max_depth: usize) -> Self {
        let ids = generate_trie(org_id, count, max_depth);
        Self { ids }
    }

    /// Get all generated traversal IDs.
    pub fn ids(&self) -> &[String] {
        &self.ids
    }

    /// Pick a random traversal ID.
    pub fn random(&self, rng: &mut impl Rng) -> &str {
        let idx = rng.gen_range(0..self.ids.len());
        &self.ids[idx]
    }

    /// Number of unique traversal IDs.
    pub fn len(&self) -> usize {
        self.ids.len()
    }
}

/// Node in the namespace tree.
struct TrieNode {
    /// Full traversal path from root to this node.
    path: String,
    /// Child nodes.
    children: Vec<TrieNode>,
}

impl TrieNode {
    fn new(path: String) -> Self {
        Self {
            path,
            children: Vec::new(),
        }
    }

    /// Collect all traversal IDs from this subtree (breadth-first for natural ordering).
    fn collect_ids(&self, result: &mut Vec<String>) {
        result.push(self.path.clone());
        for child in &self.children {
            child.collect_ids(result);
        }
    }

    /// Count total nodes in subtree.
    fn count(&self) -> usize {
        1 + self.children.iter().map(|c| c.count()).sum::<usize>()
    }
}

/// Generate a trie of traversal IDs.
///
/// Builds a tree where each node has a globally unique, monotonically increasing ID.
/// The tree is built breadth-first to distribute nodes across depths.
fn generate_trie(org_id: u32, count: usize, max_depth: usize) -> Vec<String> {
    if count == 0 {
        return vec![];
    }

    let root_id = org_id as u64;
    let mut root = TrieNode::new(root_id.to_string());

    if count == 1 || max_depth == 1 {
        return vec![root.path];
    }

    // Next ID to assign (starts after org_id, but we use a separate counter)
    let mut next_id = root_id + 1;

    // Calculate branching factor to achieve target count within max_depth
    // For a balanced tree: count ≈ (branching_factor^max_depth - 1) / (branching_factor - 1)
    // Solve for branching_factor given count and max_depth
    let branching_factor = calculate_branching_factor(count, max_depth);

    // Build tree level by level (breadth-first)
    let mut current_level: Vec<*mut TrieNode> = vec![&mut root as *mut TrieNode];

    for depth in 1..max_depth {
        if root.count() >= count {
            break;
        }

        let mut next_level = Vec::new();

        for node_ptr in &current_level {
            if root.count() >= count {
                break;
            }

            // Determine how many children this node should have
            // Add some variance to make it more realistic
            let children_to_add = if depth == 1 {
                // First level gets more children to create wider top
                branching_factor.min(count - root.count())
            } else {
                // Deeper levels get fewer children
                let max_children = (branching_factor as f64 * 0.7).ceil() as usize;
                max_children.max(1).min(count - root.count())
            };

            // SAFETY: We only access nodes we've created and don't invalidate pointers
            let node = unsafe { &mut **node_ptr };

            for _ in 0..children_to_add {
                if root.count() >= count {
                    break;
                }

                let child_path = format!("{}/{}", node.path, next_id);
                let child = TrieNode::new(child_path);
                next_id += 1;
                node.children.push(child);
            }

            // Add new children to next level
            for child in &mut node.children {
                next_level.push(child as *mut TrieNode);
            }
        }

        current_level = next_level;
    }

    // Collect all traversal IDs
    let mut result = Vec::with_capacity(count);
    root.collect_ids(&mut result);
    result.truncate(count);
    result
}

/// Calculate optimal branching factor to achieve target node count within max_depth.
fn calculate_branching_factor(count: usize, max_depth: usize) -> usize {
    if max_depth <= 1 {
        return count;
    }

    // Binary search for branching factor
    // Total nodes in balanced tree = (b^d - 1) / (b - 1) where b = branching factor, d = depth
    let mut low = 2usize;
    let mut high = count;

    while low < high {
        let mid = (low + high) / 2;
        let total = tree_size(mid, max_depth);

        if total < count {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    low.max(2)
}

/// Calculate total nodes in a balanced tree with given branching factor and depth.
fn tree_size(branching_factor: usize, depth: usize) -> usize {
    if branching_factor <= 1 {
        return depth;
    }

    // Geometric series: 1 + b + b^2 + ... + b^(d-1) = (b^d - 1) / (b - 1)
    let mut total = 0usize;
    let mut level_size = 1usize;

    for _ in 0..depth {
        total = total.saturating_add(level_size);
        level_size = level_size.saturating_mul(branching_factor);
    }

    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    #[ignore] // Run with: cargo test -p simulator -- --ignored --nocapture
    fn test_print_example_traversal_ids() {
        println!("\n=== Example traversal IDs (org_id=1, count=20, max_depth=4) ===");
        let generator = TraversalIdGenerator::new(1, 20, 4);
        for (i, id) in generator.ids().iter().enumerate() {
            let depth = id.split('/').count();
            let indent = "  ".repeat(depth - 1);
            println!("{:3}. {}{}", i + 1, indent, id);
        }

        println!("\n=== Example traversal IDs (org_id=42, count=15, max_depth=5) ===");
        let generator = TraversalIdGenerator::new(42, 15, 5);
        for (i, id) in generator.ids().iter().enumerate() {
            let depth = id.split('/').count();
            let indent = "  ".repeat(depth - 1);
            println!("{:3}. {}{}", i + 1, indent, id);
        }
    }

    #[test]
    fn test_generate_trie_basic() {
        let generator = TraversalIdGenerator::new(1, 10, 4);

        assert_eq!(generator.len(), 10);

        // All IDs should start with org_id
        for id in generator.ids() {
            assert!(id.starts_with("1"), "ID {} should start with 1", id);
        }

        // First ID should be just the root
        assert_eq!(generator.ids()[0], "1");
    }

    #[test]
    fn test_all_ids_unique() {
        let generator = TraversalIdGenerator::new(42, 100, 5);

        let unique: HashSet<_> = generator.ids().iter().collect();
        assert_eq!(unique.len(), generator.len(), "All traversal IDs should be unique");
    }

    #[test]
    fn test_ids_are_valid_paths() {
        let generator = TraversalIdGenerator::new(1, 50, 4);

        for id in generator.ids() {
            // Should be slash-separated numbers
            for part in id.split('/') {
                assert!(
                    part.parse::<u64>().is_ok(),
                    "Each path component should be a number: {}",
                    id
                );
            }
        }
    }

    #[test]
    fn test_monotonically_increasing_ids() {
        let generator = TraversalIdGenerator::new(1, 20, 4);

        // Extract all individual IDs used in paths
        let mut all_ids: Vec<u64> = generator
            .ids()
            .iter()
            .flat_map(|path| path.split('/').map(|s| s.parse::<u64>().unwrap()))
            .collect();

        all_ids.sort();
        all_ids.dedup();

        // IDs should form a contiguous sequence starting from org_id
        for (i, &id) in all_ids.iter().enumerate() {
            assert_eq!(
                id,
                1 + i as u64,
                "IDs should be contiguous: expected {}, got {}",
                1 + i as u64,
                id
            );
        }
    }

    #[test]
    fn test_parent_paths_exist() {
        let generator = TraversalIdGenerator::new(1, 100, 5);
        let id_set: HashSet<_> = generator.ids().iter().map(|s| s.as_str()).collect();

        // Every path's parent should also exist
        for id in generator.ids() {
            if let Some(last_slash) = id.rfind('/') {
                let parent = &id[..last_slash];
                assert!(
                    id_set.contains(parent),
                    "Parent path {} should exist for {}",
                    parent,
                    id
                );
            }
        }
    }

    #[test]
    fn test_no_cyclic_ids() {
        let generator = TraversalIdGenerator::new(1, 100, 5);

        for id in generator.ids() {
            let parts: Vec<&str> = id.split('/').collect();
            let unique_parts: HashSet<&str> = parts.iter().copied().collect();

            assert_eq!(
                parts.len(),
                unique_parts.len(),
                "Path should not have repeated IDs: {}",
                id
            );
        }
    }

    #[test]
    fn test_depth_respected() {
        let max_depth = 3;
        let generator = TraversalIdGenerator::new(1, 50, max_depth);

        for id in generator.ids() {
            let depth = id.split('/').count();
            assert!(
                depth <= max_depth,
                "Depth {} exceeds max_depth {} for {}",
                depth,
                max_depth,
                id
            );
        }
    }
}
