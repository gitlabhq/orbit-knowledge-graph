use std::collections::HashMap;

use chrono::Utc;

use crate::state::{
    EnabledNamespaceRef, HierarchyMetadata, HierarchyPathEntry, HierarchyState, IdRange,
};

use crate::domain::foundation::Foundation;
use crate::domain::layout::{ProjectEntityLayout, table_base_id};
use crate::seeding::catalog;

pub fn build_state_for_continuous(
    foundation: &Foundation,
    layout: ProjectEntityLayout,
) -> HierarchyState {
    let mut entity_ranges = HashMap::new();
    if let Some(first) = foundation.users.first() {
        entity_ranges.insert(
            "User".to_string(),
            IdRange {
                first_id: first.id,
                count: foundation.users.len(),
            },
        );
    }
    if let Some(first) = foundation.groups.first() {
        entity_ranges.insert(
            "Group".to_string(),
            IdRange {
                first_id: first.id,
                count: foundation.groups.len(),
            },
        );
    }
    if let Some(first) = foundation.projects.first() {
        entity_ranges.insert(
            "Project".to_string(),
            IdRange {
                first_id: first.id,
                count: foundation.projects.len(),
            },
        );
    }

    let project_count = foundation.projects.len();
    for (entity_name, table_name) in catalog::state_range_definitions() {
        let per_project = catalog::project_rows_per_table(&layout, table_name);
        if project_count > 0 && per_project > 0 {
            entity_ranges.insert(
                entity_name.to_string(),
                IdRange {
                    first_id: table_base_id(table_name, foundation, layout),
                    count: per_project * project_count,
                },
            );
        }
    }

    let max_rows_per_project = layout.max_rows_per_project().max(1);
    let block_size = (project_count * max_rows_per_project + 1) as i64;
    // Move the next ID past all table blocks reserved during initial seeding.
    let next_entity_id = foundation.next_entity_id
        + (catalog::project_table_names_in_order().len() as i64 * block_size);

    let enabled_namespaces = foundation
        .root_group_namespace_ids
        .iter()
        .map(|namespace_id| EnabledNamespaceRef {
            root_namespace_id: *namespace_id,
            organization_id: 1,
        })
        .collect();

    let mut path_entries = Vec::with_capacity(foundation.groups.len() + foundation.projects.len());
    for group in &foundation.groups {
        path_entries.push(HierarchyPathEntry {
            entity_type: "Group".to_string(),
            id: group.id,
            traversal_path: group.traversal_path.clone(),
            namespace_id: Some(group.namespace_id),
        });
    }
    for project in &foundation.projects {
        path_entries.push(HierarchyPathEntry {
            entity_type: "Project".to_string(),
            id: project.id,
            traversal_path: project.traversal_path.clone(),
            namespace_id: Some(project.namespace_id),
        });
    }

    HierarchyState {
        metadata: HierarchyMetadata {
            next_entity_id,
            next_namespace_id: foundation.next_namespace_id,
            last_watermark: Utc::now(),
            enabled_namespaces,
            entity_ranges,
        },
        path_entries,
    }
}
