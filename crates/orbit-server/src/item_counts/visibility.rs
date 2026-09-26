use ontology::Ontology;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{DEFAULT_PATH_ACCESS_LEVEL, SecurityContext};

pub struct VisibleEntity {
    pub name: String,
    pub table: String,
    pub scopes: Vec<String>,
}

pub fn get_visible_entities(
    ontology: &Ontology,
    security_context: &SecurityContext,
    scopes: &[TraversalPath],
) -> Vec<VisibleEntity> {
    ontology
        .nodes()
        .filter(|node| node.has_traversal_path)
        .filter_map(|node| {
            let required_role = node
                .redaction
                .as_ref()
                .map(|redaction| redaction.required_role.as_access_level())
                .unwrap_or(DEFAULT_PATH_ACCESS_LEVEL);
            let paths_with_role = security_context.paths_at_least(required_role);
            let scopes: Vec<String> = scopes
                .iter()
                .filter(|scope| scope.is_within_scope(&paths_with_role))
                .map(|scope| scope.as_str().to_string())
                .collect();
            if scopes.is_empty() {
                return None;
            }

            Some(VisibleEntity {
                name: node.name.clone(),
                table: node.destination_table.clone(),
                scopes,
            })
        })
        .collect()
}
