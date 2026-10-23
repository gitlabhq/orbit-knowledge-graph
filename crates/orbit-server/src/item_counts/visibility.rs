use std::collections::HashMap;

use ontology::Ontology;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{DEFAULT_PATH_ACCESS_LEVEL, SecurityContext};

pub struct Visibility {
    pub scope_groups: Vec<Vec<usize>>,
    pub entities: Vec<VisibleEntity>,
}

pub struct VisibleEntity {
    pub name: String,
    pub table: String,
    pub scope_group: usize,
}

impl Visibility {
    pub fn get_scopes(&self, entity: &VisibleEntity) -> &[usize] {
        &self.scope_groups[entity.scope_group]
    }
}

pub fn get_visibility(
    ontology: &Ontology,
    security_context: &SecurityContext,
    scopes: &[&TraversalPath],
) -> Visibility {
    let mut scope_groups: Vec<Vec<usize>> = Vec::new();
    let mut group_by_role: HashMap<u32, usize> = HashMap::new();
    let entities = ontology
        .nodes()
        .filter(|node| node.has_traversal_path)
        .filter_map(|node| {
            let required_role = node
                .redaction
                .as_ref()
                .map(|redaction| redaction.required_role.as_access_level())
                .unwrap_or(DEFAULT_PATH_ACCESS_LEVEL);
            let scope_group = *group_by_role.entry(required_role).or_insert_with(|| {
                let paths_with_role = security_context.paths_at_least(required_role);
                scope_groups.push(
                    (0..scopes.len())
                        .filter(|&index| scopes[index].is_within_scope(&paths_with_role))
                        .collect(),
                );
                scope_groups.len() - 1
            });
            (!scope_groups[scope_group].is_empty()).then(|| VisibleEntity {
                name: node.name.clone(),
                table: node.destination_table.clone(),
                scope_group,
            })
        })
        .collect();
    Visibility {
        scope_groups,
        entities,
    }
}
