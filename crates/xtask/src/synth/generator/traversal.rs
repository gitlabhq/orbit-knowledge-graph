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

use rand::Rng;
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
    pub traversal_path: String,
}

impl EntityContext {
    pub fn new(id: i64, traversal_path: String) -> Self {
        Self { id, traversal_path }
    }

    pub fn root_group(org_id: u32, group_id: i64) -> Self {
        Self {
            id: group_id,
            traversal_path: format!("{}/{}/", org_id, group_id),
        }
    }

    pub fn subgroup(parent: &EntityContext, subgroup_id: i64) -> Self {
        Self {
            id: subgroup_id,
            traversal_path: format!("{}{}/", parent.traversal_path, subgroup_id),
        }
    }

    pub fn child(parent: &EntityContext, entity_id: i64) -> Self {
        Self {
            id: entity_id,
            traversal_path: parent.traversal_path.clone(),
        }
    }
}

/// Registry of generated entities by type, for parent lookups during generation.
#[derive(Debug)]
pub struct EntityRegistry {
    /// Full entities by node type (used during relationship generation).
    entities: HashMap<String, Vec<EntityContext>>,
    /// Compact ID-only storage (used after compaction for associations).
    ids_only: HashMap<String, Vec<i64>>,
    /// Whether the registry has been compacted.
    compacted: bool,
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
            ids_only: HashMap::new(),
            compacted: false,
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

    /// Compact the registry to only store IDs, freeing traversal path memory.
    /// Call this after relationship generation is complete, before associations.
    pub fn compact(&mut self) {
        self.compact_with_aliases(&HashMap::new());
    }

    /// Compact the registry, merging epsilon node entries into their real type names.
    ///
    /// For example, with aliases `{"Group@1": "Group", "Group@2": "Group"}`,
    /// entities stored under "Group@1" and "Group@2" get merged into "Group".
    pub fn compact_with_aliases(&mut self, aliases: &HashMap<String, String>) {
        if self.compacted {
            return;
        }
        // Convert full entities to IDs and merge, resolving aliases
        for (node_type, contexts) in self.entities.drain() {
            let real_type = aliases.get(&node_type).cloned().unwrap_or(node_type);
            let ids: Vec<i64> = contexts.into_iter().map(|c| c.id).collect();
            self.ids_only.entry(real_type).or_default().extend(ids);
        }
        // Also resolve aliases in existing id-only entries
        let id_entries: Vec<_> = self.ids_only.keys().cloned().collect();
        for key in id_entries {
            if let Some(real_type) = aliases.get(&key)
                && let Some(ids) = self.ids_only.remove(&key)
            {
                self.ids_only
                    .entry(real_type.clone())
                    .or_default()
                    .extend(ids);
            }
        }
        self.compacted = true;
    }

    /// Add a full entity context (for parent entities that may have children).
    pub fn add(&mut self, node_type: &str, context: EntityContext) {
        debug_assert!(!self.compacted, "Cannot add after compaction");
        self.entities
            .entry(node_type.to_string())
            .or_default()
            .push(context);
    }

    /// Add only an entity ID (for leaf entities with no children).
    /// This saves memory by not storing traversal paths for entities
    /// that won't be used as parents.
    pub fn add_id_only(&mut self, node_type: &str, id: i64) {
        debug_assert!(!self.compacted, "Cannot add after compaction");
        self.ids_only
            .entry(node_type.to_string())
            .or_default()
            .push(id);
    }

    pub fn get(&self, node_type: &str) -> Option<&[EntityContext]> {
        debug_assert!(!self.compacted, "Use get_ids_slice after compaction");
        self.entities.get(node_type).map(|v| v.as_slice())
    }

    /// Get entity IDs as a slice.
    /// Works after compaction, or for leaf types that were added with add_id_only.
    pub fn get_ids_slice(&self, node_type: &str) -> Option<&[i64]> {
        self.ids_only.get(node_type).map(|v| v.as_slice())
    }

    pub fn get_ids(&self, node_type: &str) -> Vec<i64> {
        if self.compacted {
            self.ids_only.get(node_type).cloned().unwrap_or_default()
        } else {
            self.entities
                .get(node_type)
                .map(|v| v.iter().map(|e| e.id).collect())
                .unwrap_or_default()
        }
    }

    pub fn all_entities(&self) -> &HashMap<String, Vec<EntityContext>> {
        &self.entities
    }

    pub fn count(&self, node_type: &str) -> usize {
        let full = self.entities.get(node_type).map(|v| v.len()).unwrap_or(0);
        let ids = self.ids_only.get(node_type).map(|v| v.len()).unwrap_or(0);
        full + ids
    }

    pub fn total_count(&self) -> usize {
        let full: usize = self.entities.values().map(|v| v.len()).sum();
        let ids: usize = self.ids_only.values().map(|v| v.len()).sum();
        full + ids
    }
}

pub struct TraversalPathGenerator {
    paths: Vec<String>,
}

impl TraversalPathGenerator {
    pub fn new(org_id: u32, count: usize, max_depth: usize) -> Self {
        let paths = generate_trie(org_id, count, max_depth);
        Self { paths }
    }

    pub fn paths(&self) -> &[String] {
        &self.paths
    }

    pub fn random(&self, rng: &mut impl Rng) -> &str {
        let idx = rng.gen_range(0..self.paths.len());
        &self.paths[idx]
    }

    pub fn len(&self) -> usize {
        self.paths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
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
    #[ignore] // Run with: cargo test -p xtask -- --ignored --nocapture
    fn test_print_example_traversal_paths() {
        println!("\n=== Example traversal paths (org_id=1, count=20, max_depth=4) ===");
        let generator = TraversalPathGenerator::new(1, 20, 4);
        for (i, path) in generator.paths().iter().enumerate() {
            let depth = path.split('/').filter(|s| !s.is_empty()).count();
            let indent = "  ".repeat(depth - 1);
            println!("{:3}. {}{}", i + 1, indent, path);
        }

        println!("\n=== Example traversal IDs (org_id=42, count=15, max_depth=5) ===");
        let generator = TraversalPathGenerator::new(42, 15, 5);
        for (i, path) in generator.paths().iter().enumerate() {
            let depth = path.split('/').filter(|s| !s.is_empty()).count();
            let indent = "  ".repeat(depth - 1);
            println!("{:3}. {}{}", i + 1, indent, path);
        }
    }

    #[test]
    fn test_generate_trie_basic() {
        let generator = TraversalPathGenerator::new(1, 10, 4);

        assert_eq!(generator.len(), 10);

        // All IDs should start with org_id
        for path in generator.paths() {
            assert!(path.starts_with("1"), "Path {} should start with 1", path);
        }

        // First ID should be just the root
        assert_eq!(generator.paths()[0], "1/");
    }

    #[test]
    fn test_all_ids_unique() {
        let generator = TraversalPathGenerator::new(42, 100, 5);

        let unique: HashSet<_> = generator.paths().iter().collect();
        assert_eq!(
            unique.len(),
            generator.len(),
            "All traversal paths should be unique"
        );
    }

    #[test]
    fn test_paths_are_valid_paths() {
        let generator = TraversalPathGenerator::new(1, 50, 4);

        for path in generator.paths() {
            // Should be slash-separated numbers (with trailing slash)
            for part in path.split('/').filter(|s| !s.is_empty()) {
                assert!(
                    part.parse::<u64>().is_ok(),
                    "Each path component should be a number: {}",
                    part
                );
            }
        }
    }

    #[test]
    fn test_monotonically_increasing_ids() {
        let generator = TraversalPathGenerator::new(1, 20, 4);

        // Extract all individual IDs used in paths
        let mut all_ids: Vec<u64> = generator
            .paths()
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
        let generator = TraversalPathGenerator::new(1, 100, 5);
        let path_set: HashSet<_> = generator.paths().iter().map(|s| s.as_str()).collect();

        // Every path's parent should also exist (except root)
        for path in generator.paths() {
            // Remove trailing slash to find parent
            let path_without_trailing = path.trim_end_matches('/');
            if let Some(last_slash) = path_without_trailing.rfind('/') {
                let parent = format!("{}/", &path_without_trailing[..last_slash]);
                assert!(
                    path_set.contains(parent.as_str()),
                    "Parent path {} should exist for {}",
                    parent,
                    path
                );
            }
        }
    }

    #[test]
    fn test_no_cyclic_ids() {
        let generator = TraversalPathGenerator::new(1, 100, 5);

        for path in generator.paths() {
            let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
            let unique_parts: HashSet<&str> = parts.iter().copied().collect();

            assert_eq!(
                parts.len(),
                unique_parts.len(),
                "Path should not have repeated sub-paths: {}",
                path
            );
        }
    }

    #[test]
    fn test_depth_respected() {
        let max_depth = 3;
        let generator = TraversalPathGenerator::new(1, 50, max_depth);

        for path in generator.paths() {
            let depth = path.split('/').filter(|s| !s.is_empty()).count();
            assert!(
                depth <= max_depth,
                "Depth {} exceeds max_depth {} for {}",
                depth,
                max_depth,
                path
            );
        }
    }

    #[test]
    fn test_entity_context_root_group() {
        let ctx = EntityContext::root_group(1, 100);
        assert_eq!(ctx.id, 100);
        assert_eq!(ctx.traversal_path, "1/100/");
    }

    #[test]
    fn test_entity_context_subgroup() {
        let parent = EntityContext::root_group(1, 100);
        let child = EntityContext::subgroup(&parent, 101);
        assert_eq!(child.id, 101);
        assert_eq!(child.traversal_path, "1/100/101/");

        // Deeper nesting
        let grandchild = EntityContext::subgroup(&child, 102);
        assert_eq!(grandchild.id, 102);
        assert_eq!(grandchild.traversal_path, "1/100/101/102/");
    }

    #[test]
    fn test_entity_context_child_inherits() {
        let group = EntityContext::root_group(1, 100);
        let project = EntityContext::child(&group, 500);

        // Project inherits group's traversal ID
        assert_eq!(project.id, 500);
        assert_eq!(project.traversal_path, "1/100/");

        let mr = EntityContext::child(&project, 1000);
        // MR also inherits the same traversal ID
        assert_eq!(mr.id, 1000);
        assert_eq!(mr.traversal_path, "1/100/");
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
    fn test_entity_registry_compact() {
        let mut registry = EntityRegistry::new(1);

        // Add full contexts (parent entities)
        registry.add("Group", EntityContext::root_group(1, 100));
        registry.add("Group", EntityContext::root_group(1, 101));

        // Add ID-only (leaf entities)
        registry.add_id_only("MergeRequest", 500);
        registry.add_id_only("MergeRequest", 501);

        // Before compaction: full contexts accessible via get()
        assert!(registry.get("Group").is_some());
        assert_eq!(registry.count("Group"), 2);
        assert_eq!(registry.count("MergeRequest"), 2);

        // Compact
        registry.compact();

        // After compaction: all IDs accessible via get_ids_slice()
        let group_ids = registry.get_ids_slice("Group").unwrap();
        assert_eq!(group_ids, &[100, 101]);

        let mr_ids = registry.get_ids_slice("MergeRequest").unwrap();
        assert_eq!(mr_ids, &[500, 501]);

        // get_ids() still works after compaction
        assert_eq!(registry.get_ids("Group"), vec![100, 101]);

        // total_count is preserved
        assert_eq!(registry.total_count(), 4);
    }

    #[test]
    fn test_entity_registry_compact_merges_full_and_id_only() {
        let mut registry = EntityRegistry::new(1);

        // Mix: some entities added as full, some as id-only, same type
        registry.add("Project", EntityContext::new(10, "1/100/".to_string()));
        registry.add_id_only("Project", 20);

        assert_eq!(registry.count("Project"), 2);

        registry.compact();

        let ids = registry.get_ids_slice("Project").unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&10));
        assert!(ids.contains(&20));
    }

    #[test]
    fn test_entity_registry_compact_with_aliases() {
        let mut registry = EntityRegistry::new(1);

        // Root-level Group entities
        registry.add("Group", EntityContext::root_group(1, 100));
        registry.add("Group", EntityContext::root_group(1, 101));

        // Virtual depth-level entries
        registry.add("Group@1", EntityContext::new(200, "1/100/200/".to_string()));
        registry.add("Group@1", EntityContext::new(201, "1/101/201/".to_string()));
        registry.add(
            "Group@2",
            EntityContext::new(300, "1/100/200/300/".to_string()),
        );

        // Non-aliased type
        registry.add_id_only("Project", 500);
        registry.add_id_only("Project", 501);

        let aliases: HashMap<String, String> = [
            ("Group@1".to_string(), "Group".to_string()),
            ("Group@2".to_string(), "Group".to_string()),
        ]
        .into_iter()
        .collect();

        registry.compact_with_aliases(&aliases);

        // All Group IDs (root + epsilon) should be merged
        let group_ids = registry.get_ids_slice("Group").unwrap();
        assert_eq!(group_ids.len(), 5); // 100, 101, 200, 201, 300
        assert!(group_ids.contains(&100));
        assert!(group_ids.contains(&201));
        assert!(group_ids.contains(&300));

        // Epsilon keys should not exist
        assert!(registry.get_ids_slice("Group@1").is_none());
        assert!(registry.get_ids_slice("Group@2").is_none());

        // Non-aliased type is unchanged
        let project_ids = registry.get_ids_slice("Project").unwrap();
        assert_eq!(project_ids.len(), 2);
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
        assert_eq!(groups[0].traversal_path, "1/100/");
        assert_eq!(groups[1].traversal_path, "1/101/");

        let group_ids = registry.get_ids("Group");
        assert_eq!(group_ids, vec![100, 101]);
    }
}
