use std::collections::HashMap;

use orbit_utils::traversal_path::TraversalPath;
use query_data_model::{Catalog, ClickHouse};
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
    pub fn entities_in_scope(&self, index: usize) -> Vec<&VisibleEntity> {
        self.entities
            .iter()
            .filter(|entity| self.scope_groups[entity.scope_group].contains(&index))
            .collect()
    }
}

pub fn get_visibility(
    data_model: &Catalog<ClickHouse>,
    security_context: &SecurityContext,
    scopes: &[&TraversalPath],
) -> Visibility {
    let mut scope_groups: Vec<Vec<usize>> = Vec::new();
    let mut group_by_role: HashMap<u32, usize> = HashMap::new();
    let entities = data_model
        .graph()
        .entities()
        .filter(|entity| data_model.has_path_column(entity.id))
        .filter_map(|entity| {
            let table = data_model.table_of(entity.id)?;
            let required_role = data_model
                .role_floor(entity.id)
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
                name: entity.name.clone(),
                table: table.to_string(),
                scope_group,
            })
        })
        .collect();
    Visibility {
        scope_groups,
        entities,
    }
}
