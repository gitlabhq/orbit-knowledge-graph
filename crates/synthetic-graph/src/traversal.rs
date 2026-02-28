use std::collections::HashMap;

use rand::Rng;

use crate::ids::IdAllocator;

/// Context for a generated entity, tracking its ID and traversal path.
///
/// Traversal paths are hierarchical paths like `"1/2/5/"` where each
/// component is a globally unique namespace ID. The path represents
/// the ancestry chain from organization root to the current entity's
/// containing namespace.
#[derive(Debug, Clone)]
pub struct EntityContext {
    pub id: i64,
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

/// Registry of generated entities by type, used for parent lookups and
/// association wiring during graph generation.
///
/// Supports two storage modes:
/// - Full `EntityContext` for entities that may have children (need traversal paths).
/// - ID-only storage for leaf entities (saves ~44 bytes per entity).
///
/// After relationship generation completes, call [`compact`](EntityRegistry::compact)
/// to free traversal path memory before generating associations.
#[derive(Debug)]
pub struct EntityRegistry {
    entities: HashMap<String, Vec<EntityContext>>,
    ids_only: HashMap<String, Vec<i64>>,
    compacted: bool,
    namespace_counter: IdAllocator,
    entity_counter: IdAllocator,
    org_id: u32,
}

impl EntityRegistry {
    pub fn new(org_id: u32) -> Self {
        Self {
            entities: HashMap::new(),
            ids_only: HashMap::new(),
            compacted: false,
            namespace_counter: IdAllocator::new(org_id as i64 + 1),
            entity_counter: IdAllocator::new(1),
            org_id,
        }
    }

    /// Create a registry with custom starting counters (for multi-org generation).
    pub fn with_counters(org_id: u32, entity_start: i64, namespace_start: i64) -> Self {
        Self {
            entities: HashMap::new(),
            ids_only: HashMap::new(),
            compacted: false,
            namespace_counter: IdAllocator::new(namespace_start),
            entity_counter: IdAllocator::new(entity_start),
            org_id,
        }
    }

    pub fn org_id(&self) -> u32 {
        self.org_id
    }

    pub fn next_namespace_id(&self) -> i64 {
        self.namespace_counter.next()
    }

    pub fn next_entity_id(&self) -> i64 {
        self.entity_counter.next()
    }

    pub fn current_entity_id(&self) -> i64 {
        self.entity_counter.current()
    }

    pub fn current_namespace_id(&self) -> i64 {
        self.namespace_counter.current()
    }

    /// Free traversal path memory by converting full entities to IDs.
    pub fn compact(&mut self) {
        if self.compacted {
            return;
        }
        for (node_type, contexts) in self.entities.drain() {
            let ids: Vec<i64> = contexts.into_iter().map(|c| c.id).collect();
            self.ids_only.entry(node_type).or_default().extend(ids);
        }
        self.compacted = true;
    }

    /// Add a full entity context (for entities that may have children).
    pub fn add(&mut self, node_type: &str, context: EntityContext) {
        debug_assert!(!self.compacted, "Cannot add after compaction");
        self.entities
            .entry(node_type.to_string())
            .or_default()
            .push(context);
    }

    /// Add only an entity ID (for leaf entities with no children).
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

    pub fn is_compacted(&self) -> bool {
        self.compacted
    }
}

/// Generates a balanced trie of hierarchical traversal paths.
///
/// Used to pre-generate a set of namespace paths for a given organization
/// with controlled depth and branching.
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

fn tree_size(branching_factor: usize, depth: usize) -> usize {
    if branching_factor <= 1 {
        return depth;
    }
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

    // ---- EntityContext tests ----

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

        let grandchild = EntityContext::subgroup(&child, 102);
        assert_eq!(grandchild.id, 102);
        assert_eq!(grandchild.traversal_path, "1/100/101/102/");
    }

    #[test]
    fn test_entity_context_child_inherits() {
        let group = EntityContext::root_group(1, 100);
        let project = EntityContext::child(&group, 500);

        assert_eq!(project.id, 500);
        assert_eq!(project.traversal_path, "1/100/");

        // MR also inherits the same traversal path
        let mr = EntityContext::child(&project, 1000);
        assert_eq!(mr.id, 1000);
        assert_eq!(mr.traversal_path, "1/100/");
    }

    // ---- EntityRegistry tests ----

    #[test]
    fn test_registry_add_and_get() {
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

    #[test]
    fn test_registry_compact() {
        let mut registry = EntityRegistry::new(1);
        registry.add("Group", EntityContext::root_group(1, 100));
        registry.add_id_only("MR", 200);
        registry.compact();
        assert!(registry.is_compacted());
        assert_eq!(registry.get_ids("Group"), vec![100]);
        assert_eq!(registry.get_ids("MR"), vec![200]);
    }

    #[test]
    fn test_registry_namespace_counter() {
        let registry = EntityRegistry::new(1);
        // Namespace counter starts at org_id + 1
        assert_eq!(registry.next_namespace_id(), 2);
        assert_eq!(registry.next_namespace_id(), 3);
        assert_eq!(registry.next_namespace_id(), 4);
    }

    #[test]
    fn test_registry_entity_counter() {
        let registry = EntityRegistry::new(1);
        // Entity counter starts at 1
        assert_eq!(registry.next_entity_id(), 1);
        assert_eq!(registry.next_entity_id(), 2);
        assert_eq!(registry.next_entity_id(), 3);
    }

    // ---- TraversalPathGenerator / trie property tests ----

    #[test]
    fn test_traversal_path_generator() {
        let tpg = TraversalPathGenerator::new(1, 10, 4);
        assert_eq!(tpg.len(), 10);
        assert_eq!(tpg.paths()[0], "1/");

        let unique: HashSet<_> = tpg.paths().iter().collect();
        assert_eq!(unique.len(), 10);
    }

    #[test]
    fn test_generate_trie_basic() {
        let generator = TraversalPathGenerator::new(1, 10, 4);

        assert_eq!(generator.len(), 10);

        for path in generator.paths() {
            assert!(
                path.starts_with("1"),
                "Path {} should start with org_id",
                path
            );
        }

        assert_eq!(generator.paths()[0], "1/");
    }

    #[test]
    fn test_all_paths_unique() {
        let generator = TraversalPathGenerator::new(42, 100, 5);

        let unique: HashSet<_> = generator.paths().iter().collect();
        assert_eq!(
            unique.len(),
            generator.len(),
            "All traversal paths should be unique"
        );
    }

    #[test]
    fn test_paths_are_valid() {
        let generator = TraversalPathGenerator::new(1, 50, 4);

        for path in generator.paths() {
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

        for path in generator.paths() {
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
    fn test_no_cyclic_path_components() {
        let generator = TraversalPathGenerator::new(1, 100, 5);

        for path in generator.paths() {
            let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
            let unique_parts: HashSet<&str> = parts.iter().copied().collect();

            assert_eq!(
                parts.len(),
                unique_parts.len(),
                "Path should not have repeated components: {}",
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
    #[ignore] // Run with: cargo test -p synthetic-graph -- --ignored --nocapture
    fn test_print_example_traversal_paths() {
        println!("\n=== Example traversal paths (org_id=1, count=20, max_depth=4) ===");
        let generator = TraversalPathGenerator::new(1, 20, 4);
        for (i, path) in generator.paths().iter().enumerate() {
            let depth = path.split('/').filter(|s| !s.is_empty()).count();
            let indent = "  ".repeat(depth - 1);
            println!("{:3}. {}{}", i + 1, indent, path);
        }

        println!("\n=== Example traversal paths (org_id=42, count=15, max_depth=5) ===");
        let generator = TraversalPathGenerator::new(42, 15, 5);
        for (i, path) in generator.paths().iter().enumerate() {
            let depth = path.split('/').filter(|s| !s.is_empty()).count();
            let indent = "  ".repeat(depth - 1);
            println!("{:3}. {}{}", i + 1, indent, path);
        }
    }
}
