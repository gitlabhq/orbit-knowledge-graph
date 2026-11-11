use std::collections::HashMap;

use orbit_authorization::{Grants, ScopeKey, ScopeSet};
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::catalog::{PermissionId, Rule, ScopeId};
use query_data_model::{Catalog, ClickHouse};

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

/// The entities a caller can count in each requested scope: those whose rows hold a scope key,
/// in the scopes where the caller holds each entity's permission.
pub fn get_visibility(
    data_model: &Catalog<ClickHouse>,
    grants: &Grants,
    scopes: &[&TraversalPath],
) -> Visibility {
    let access = data_model.model().access();
    let mut scope_groups: Vec<Vec<usize>> = Vec::new();
    let mut groups: HashMap<(ScopeId, PermissionId), usize> = HashMap::new();
    let entities = data_model
        .graph()
        .entities()
        .filter_map(|entity| {
            let guard = data_model.guard(entity.id)?;
            let Rule::Column(_) = guard.rule else {
                return None;
            };
            let table = data_model.table_of(entity.id)?;
            let key = (guard.scope, guard.permission);
            let scope_group = *groups.entry(key).or_insert_with(|| {
                let scope = &access.scope(guard.scope).name;
                let permission = &access.permission(guard.permission).name;
                let set = grants.scope_set(scope, permission).ok();
                scope_groups.push(
                    (0..scopes.len())
                        .filter(|&index| set.is_some_and(|set| holds(set, scopes[index])))
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

fn holds(set: &ScopeSet, scope: &TraversalPath) -> bool {
    match set {
        ScopeSet::All => true,
        ScopeSet::Subtrees(roots) => {
            let roots: Vec<TraversalPath> = roots.iter().map(|root| root.as_str().into()).collect();
            scope.is_within_scope(&roots.iter().collect::<Vec<_>>())
        }
        ScopeSet::Keys(keys) => keys.contains(&ScopeKey::Text(scope.as_str().to_string())),
    }
}

#[cfg(test)]
mod tests {
    use orbit_authorization::Caller;

    use super::*;

    #[test]
    fn entities_need_a_scope_key_and_a_scope_with_their_permission() {
        let data_model = Catalog::<ClickHouse>::embedded().unwrap();
        let grants = Grants::new(Caller::new("user:1", false, ()))
            .with(
                "namespace",
                "read",
                ScopeSet::Subtrees(vec!["1/20/".into(), "1/30/".into()]),
            )
            .with(
                "namespace",
                "read_security",
                ScopeSet::Subtrees(vec!["1/30/".into()]),
            )
            .with("global", "read", ScopeSet::All);
        let scopes = ["1/20/", "1/30/", "1/99/"].map(TraversalPath::new_unchecked);
        let scopes: Vec<&TraversalPath> = scopes.iter().collect();
        let visible = get_visibility(&data_model, &grants, &scopes);

        let scopes_of = |name: &str| {
            visible
                .entities
                .iter()
                .find(|entity| entity.name == name)
                .map(|entity| {
                    (
                        entity.table.as_str(),
                        &visible.scope_groups[entity.scope_group],
                    )
                })
        };
        assert_eq!(scopes_of("Project"), Some(("gl_project", &vec![0, 1])));
        assert_eq!(scopes_of("Finding"), Some(("gl_finding", &vec![1])));
        assert_eq!(scopes_of("User"), None);
        assert!(visible.entities_in_scope(2).is_empty());
    }
}
