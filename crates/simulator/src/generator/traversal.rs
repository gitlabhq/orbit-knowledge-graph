//! Traversal ID generation for hierarchical authorization.
//!
//! Traversal IDs are hierarchical paths like `1/2/3/4` where the first component
//! is the organization_id. This matches GitLab's namespace traversal ID system
//! for row-level authorization.

use fake::rand::Rng;

/// Generates hierarchical traversal IDs for an organization.
///
/// Each organization gets a set of unique traversal IDs that form a tree structure.
/// The organization_id is always the root of the hierarchy.
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
    /// * `max_depth` - Maximum depth of the hierarchy (default 5)
    pub fn new(org_id: u32, count: usize, max_depth: usize) -> Self {
        let ids = generate_hierarchy(org_id, count, max_depth);
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

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

/// Generate a hierarchical set of traversal IDs.
///
/// Creates approximately `count` unique paths with maximum depth `max_depth`.
/// The org_id is the root of all paths.
fn generate_hierarchy(org_id: u32, count: usize, max_depth: usize) -> Vec<String> {
    let mut traversal_ids = Vec::with_capacity(count);

    // Calculate dimensions for each level to generate approximately count IDs
    for depth in 1..=max_depth {
        if traversal_ids.len() >= count {
            break;
        }

        // Calculate how many IDs to generate at this depth
        let remaining_depths = max_depth - depth + 1;
        let remaining_slots = count.saturating_sub(traversal_ids.len());
        let slots_per_depth = remaining_slots / remaining_depths;
        let target_count = if depth == max_depth {
            remaining_slots
        } else {
            slots_per_depth
        };

        // Number of levels after org_id
        let levels_needed = depth - 1;
        if levels_needed == 0 {
            // Depth 1: just the org_id itself
            traversal_ids.push(format!("{}", org_id));
        } else {
            // Calculate children per level: (levels_needed)th root of target_count
            let children_per_level = if levels_needed == 1 {
                target_count.min(remaining_slots)
            } else {
                (target_count as f64).powf(1.0 / levels_needed as f64).ceil() as usize
            };

            // Generate sequential IDs using recursive approach
            generate_recursive(
                org_id,
                &mut vec![org_id as u64],
                depth,
                children_per_level,
                &mut traversal_ids,
                count,
            );
        }
    }

    traversal_ids.truncate(count);
    traversal_ids
}

/// Recursively generate sequential IDs.
fn generate_recursive(
    _org_id: u32,
    path: &mut Vec<u64>,
    target_depth: usize,
    children_per_level: usize,
    traversal_ids: &mut Vec<String>,
    global_max: usize,
) {
    if traversal_ids.len() >= global_max {
        return;
    }

    let current_depth = path.len();

    if current_depth == target_depth {
        // Reached target depth - add this traversal ID
        let traversal_id_str = path
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join("/");
        traversal_ids.push(traversal_id_str);
        return;
    }

    // Generate children at this level sequentially
    for child_id in 1..=children_per_level as u64 {
        if traversal_ids.len() >= global_max {
            break;
        }

        path.push(child_id);
        generate_recursive(
            _org_id,
            path,
            target_depth,
            children_per_level,
            traversal_ids,
            global_max,
        );
        path.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_hierarchy() {
        let traversal_gen = TraversalIdGenerator::new(1, 100, 4);

        assert_eq!(traversal_gen.len(), 100);

        // All IDs should start with org_id
        for id in traversal_gen.ids() {
            assert!(id.starts_with("1"));
        }

        // Check structure
        let first = &traversal_gen.ids()[0];
        assert_eq!(first, "1"); // Root is just org_id
    }

    #[test]
    fn test_traversal_id_format() {
        let traversal_gen = TraversalIdGenerator::new(42, 50, 3);

        // All should start with org_id 42
        for id in traversal_gen.ids() {
            assert!(id.starts_with("42"));
            // Should be slash-separated
            assert!(id.chars().all(|c| c.is_ascii_digit() || c == '/'));
        }
    }
}
