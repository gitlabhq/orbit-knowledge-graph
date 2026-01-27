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
//!
//! ## Entity Context and Registry
//!
//! During relationship-based generation, entities are tracked with their traversal context:
//! - `EntityContext`: holds an entity's ID and traversal ID
//! - `EntityRegistry`: maintains all generated entities by type for parent lookups

use fake::rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

/// Context for a generated entity, tracking its ID and traversal path.
#[derive(Debug, Clone)]
pub struct EntityContext {
    /// The entity's unique ID.
    pub id: i64,
    /// The traversal ID (namespace path from org root).
    /// For Groups: extends parent's traversal ID with own ID.
    /// For other entities: inherits from parent container.
    pub traversal_id: String,
}

impl EntityContext {
    pub fn new(id: i64, traversal_id: String) -> Self {
        Self { id, traversal_id }
    }

    pub fn root_group(org_id: u32, group_id: i64) -> Self {
        Self {
            id: group_id,
            traversal_id: format!("{}/{}/", org_id, group_id),
        }
    }

    pub fn subgroup(parent: &EntityContext, subgroup_id: i64) -> Self {
        Self {
            id: subgroup_id,
            traversal_id: format!("{}{}/", parent.traversal_id, subgroup_id),
        }
    }

    pub fn child(parent: &EntityContext, entity_id: i64) -> Self {
        Self {
            id: entity_id,
            traversal_id: parent.traversal_id.clone(),
        }
    }
}

/// Registry of generated entities by type, for parent lookups during generation.
#[derive(Debug)]
pub struct EntityRegistry {
    /// Entities by node type.
    entities: HashMap<String, Vec<EntityContext>>,
    /// Namespace ID counter (for Groups only, monotonically increasing).
    namespace_counter: AtomicI64,
    /// Entity ID counter (for non-namespace entities).
    entity_counter: AtomicI64,
    /// Organization ID.
    org_id: u32,
}

impl EntityRegistry {
    pub fn new(org_id: u32) -> Self {
        Self {
            entities: HashMap::new(),
            namespace_counter: AtomicI64::new(org_id as i64 + 1),
            entity_counter: AtomicI64::new(1),
            org_id,
        }
    }

    pub fn org_id(&self) -> u32 {
        self.org_id
    }

    pub fn next_namespace_id(&self) -> i64 {
        self.namespace_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn next_entity_id(&self) -> i64 {
        self.entity_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn add(&mut self, node_type: &str, context: EntityContext) {
        self.entities
            .entry(node_type.to_string())
            .or_default()
            .push(context);
    }

    pub fn get(&self, node_type: &str) -> Option<&[EntityContext]> {
        self.entities.get(node_type).map(|v| v.as_slice())
    }

    pub fn get_ids(&self, node_type: &str) -> Vec<i64> {
        self.entities
            .get(node_type)
            .map(|v| v.iter().map(|e| e.id).collect())
            .unwrap_or_default()
    }

    pub fn all_entities(&self) -> &HashMap<String, Vec<EntityContext>> {
        &self.entities
    }

    pub fn count(&self, node_type: &str) -> usize {
        self.entities.get(node_type).map(|v| v.len()).unwrap_or(0)
    }

    pub fn total_count(&self) -> usize {
        self.entities.values().map(|v| v.len()).sum()
    }
}

pub struct TraversalIdGenerator {
    ids: Vec<String>,
}

impl TraversalIdGenerator {
    pub fn new(org_id: u32, count: usize, max_depth: usize) -> Self {
        let ids = generate_trie(org_id, count, max_depth);
        Self { ids }
    }

    pub fn ids(&self) -> &[String] {
        &self.ids
    }

    pub fn random(&self, rng: &mut impl Rng) -> &str {
        let idx = rng.gen_range(0..self.ids.len());
        &self.ids[idx]
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

fn generate_trie(org_id: u32, count: usize, max_depth: usize) -> Vec<String> {
    if count == 0 {
        return vec![];
    }

    let root_id = org_id as u64;

    if count == 1 || max_depth == 1 {
        return vec![format!("{}/", root_id)];
    }

    let mut nodes: Vec<(String, Vec<usize>)> = Vec::with_capacity(count);
    nodes.push((format!("{}/", root_id), Vec::new()));

    let mut current_level: Vec<usize> = vec![0];

    let mut next_id = root_id + 1;
    let branching_factor = calculate_branching_factor(count, max_depth);

    for depth in 1..max_depth {
        if nodes.len() >= count {
            break;
        }

        let mut next_level = Vec::new();

        for &parent_idx in &current_level {
            if nodes.len() >= count {
                break;
            }

            let children_to_add = if depth == 1 {
                branching_factor.min(count - nodes.len())
            } else {
                let max_children = (branching_factor as f64 * 0.7).ceil() as usize;
                max_children.max(1).min(count - nodes.len())
            };

            let parent_path = nodes[parent_idx].0.clone();

            for _ in 0..children_to_add {
                if nodes.len() >= count {
                    break;
                }

                let child_path = format!("{}{}/", parent_path, next_id);
                next_id += 1;
                let child_idx = nodes.len();
                nodes.push((child_path, Vec::new()));
                nodes[parent_idx].1.push(child_idx);
                next_level.push(child_idx);
            }
        }

        current_level = next_level;
    }

    // DFS pre-order
    let mut result = Vec::with_capacity(count.min(nodes.len()));
    let mut stack = vec![0usize];

    while let Some(idx) = stack.pop() {
        if result.len() >= count {
            break;
        }
        result.push(nodes[idx].0.clone());
        for &child_idx in nodes[idx].1.iter().rev() {
            stack.push(child_idx);
        }
    }

    result.truncate(count);
    result
}

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
            let depth = id.split('/').filter(|s| !s.is_empty()).count();
            let indent = "  ".repeat(depth - 1);
            println!("{:3}. {}{}", i + 1, indent, id);
        }

        println!("\n=== Example traversal IDs (org_id=42, count=15, max_depth=5) ===");
        let generator = TraversalIdGenerator::new(42, 15, 5);
        for (i, id) in generator.ids().iter().enumerate() {
            let depth = id.split('/').filter(|s| !s.is_empty()).count();
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
        assert_eq!(generator.ids()[0], "1/");
    }

    #[test]
    fn test_all_ids_unique() {
        let generator = TraversalIdGenerator::new(42, 100, 5);

        let unique: HashSet<_> = generator.ids().iter().collect();
        assert_eq!(
            unique.len(),
            generator.len(),
            "All traversal IDs should be unique"
        );
    }

    #[test]
    fn test_ids_are_valid_paths() {
        let generator = TraversalIdGenerator::new(1, 50, 4);

        for id in generator.ids() {
            // Should be slash-separated numbers (with trailing slash)
            for part in id.split('/').filter(|s| !s.is_empty()) {
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
            .flat_map(|path| {
                path.split('/')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.parse::<u64>().unwrap())
            })
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

        // Every path's parent should also exist (except root)
        for id in generator.ids() {
            // Remove trailing slash to find parent
            let id_without_trailing = id.trim_end_matches('/');
            if let Some(last_slash) = id_without_trailing.rfind('/') {
                let parent = format!("{}/", &id_without_trailing[..last_slash]);
                assert!(
                    id_set.contains(parent.as_str()),
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
            let parts: Vec<&str> = id.split('/').filter(|s| !s.is_empty()).collect();
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
            let depth = id.split('/').filter(|s| !s.is_empty()).count();
            assert!(
                depth <= max_depth,
                "Depth {} exceeds max_depth {} for {}",
                depth,
                max_depth,
                id
            );
        }
    }

    #[test]
    fn test_entity_context_root_group() {
        let ctx = EntityContext::root_group(1, 100);
        assert_eq!(ctx.id, 100);
        assert_eq!(ctx.traversal_id, "1/100/");
    }

    #[test]
    fn test_entity_context_subgroup() {
        let parent = EntityContext::root_group(1, 100);
        let child = EntityContext::subgroup(&parent, 101);
        assert_eq!(child.id, 101);
        assert_eq!(child.traversal_id, "1/100/101/");

        // Deeper nesting
        let grandchild = EntityContext::subgroup(&child, 102);
        assert_eq!(grandchild.id, 102);
        assert_eq!(grandchild.traversal_id, "1/100/101/102/");
    }

    #[test]
    fn test_entity_context_child_inherits() {
        let group = EntityContext::root_group(1, 100);
        let project = EntityContext::child(&group, 500);

        // Project inherits group's traversal ID
        assert_eq!(project.id, 500);
        assert_eq!(project.traversal_id, "1/100/");

        let mr = EntityContext::child(&project, 1000);
        // MR also inherits the same traversal ID
        assert_eq!(mr.id, 1000);
        assert_eq!(mr.traversal_id, "1/100/");
    }

    #[test]
    fn test_entity_registry_namespace_counter() {
        let registry = EntityRegistry::new(1);

        // Namespace counter starts at org_id + 1
        assert_eq!(registry.next_namespace_id(), 2);
        assert_eq!(registry.next_namespace_id(), 3);
        assert_eq!(registry.next_namespace_id(), 4);
    }

    #[test]
    fn test_entity_registry_entity_counter() {
        let registry = EntityRegistry::new(1);

        // Entity counter starts at 1
        assert_eq!(registry.next_entity_id(), 1);
        assert_eq!(registry.next_entity_id(), 2);
        assert_eq!(registry.next_entity_id(), 3);
    }

    #[test]
    fn test_entity_registry_add_and_get() {
        let mut registry = EntityRegistry::new(1);

        registry.add("Group", EntityContext::root_group(1, 100));
        registry.add("Group", EntityContext::root_group(1, 101));
        registry.add(
            "Project",
            EntityContext::child(&EntityContext::root_group(1, 100), 500),
        );

        assert_eq!(registry.count("Group"), 2);
        assert_eq!(registry.count("Project"), 1);
        assert_eq!(registry.count("MergeRequest"), 0);

        let groups = registry.get("Group").unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].traversal_id, "1/100/");
        assert_eq!(groups[1].traversal_id, "1/101/");

        let group_ids = registry.get_ids("Group");
        assert_eq!(group_ids, vec![100, 101]);
    }
}
