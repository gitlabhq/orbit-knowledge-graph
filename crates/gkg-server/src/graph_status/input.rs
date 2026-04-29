use ontology::Ontology;
use query_engine::compiler::{DEFAULT_PATH_ACCESS_LEVEL, SecurityContext};
use tonic::Status;

const PROJECT_NODE: &str = "Project";
const CHECKPOINT_TABLE_SUFFIX: &str = "code_indexing_checkpoint";

pub struct GraphStatusInput {
    pub traversal_path: String,
    pub nodes: Vec<NodeTable>,
    pub project_tables: ProjectTables,
}

pub struct NodeTable {
    pub name: String,
    pub table: String,
}

pub struct ProjectTables {
    pub project: String,
    pub code_checkpoint: String,
}

impl GraphStatusInput {
    pub fn from_ontology(
        ontology: &Ontology,
        traversal_path: String,
        security_context: &SecurityContext,
    ) -> Result<Self, Status> {
        let nodes = ontology
            .nodes()
            .filter(|node| node.has_traversal_path)
            .filter(|node| {
                let min_role = node
                    .redaction
                    .as_ref()
                    .map(|r| r.required_role.as_access_level())
                    .unwrap_or(DEFAULT_PATH_ACCESS_LEVEL);
                !security_context.paths_at_least(min_role).is_empty()
            })
            .map(|node| NodeTable {
                name: node.name.clone(),
                table: node.destination_table.clone(),
            })
            .collect();

        let project = ontology
            .get_node(PROJECT_NODE)
            .ok_or_else(|| {
                Status::internal(format!("ontology missing required node: {PROJECT_NODE}"))
            })?
            .destination_table
            .clone();

        let code_checkpoint = ontology
            .auxiliary_tables()
            .iter()
            .find(|t| t.name.ends_with(CHECKPOINT_TABLE_SUFFIX))
            .ok_or_else(|| {
                Status::internal(format!(
                    "ontology missing auxiliary table ending with: {CHECKPOINT_TABLE_SUFFIX}"
                ))
            })?
            .name
            .clone();

        Ok(Self {
            traversal_path,
            nodes,
            project_tables: ProjectTables {
                project,
                code_checkpoint,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use query_engine::compiler::TraversalPath;
    use std::sync::Arc;

    const VULNERABILITY: &str = "Vulnerability";

    fn embedded_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    fn has_node(input: &GraphStatusInput, name: &str) -> bool {
        input.nodes.iter().any(|n| n.name == name)
    }

    #[test]
    fn reporter_excludes_security_entities() {
        let ontology = embedded_ontology();
        let ctx =
            SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/100/", 20)]).unwrap();

        let input = GraphStatusInput::from_ontology(&ontology, "1/100/".to_string(), &ctx).unwrap();

        assert!(!has_node(&input, VULNERABILITY));
        assert!(has_node(&input, "Project"));
        assert!(has_node(&input, "MergeRequest"));
    }

    #[test]
    fn security_manager_includes_security_entities() {
        let ontology = embedded_ontology();
        let ctx =
            SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/100/", 25)]).unwrap();

        let input = GraphStatusInput::from_ontology(&ontology, "1/100/".to_string(), &ctx).unwrap();

        assert!(has_node(&input, VULNERABILITY));
        assert!(has_node(&input, "Project"));
    }

    #[test]
    fn admin_includes_all_entities() {
        let ontology = embedded_ontology();
        let ctx = SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/", 50)])
            .unwrap()
            .with_role(true, Some(50));

        let input = GraphStatusInput::from_ontology(&ontology, "1/".to_string(), &ctx).unwrap();

        assert!(has_node(&input, VULNERABILITY));
        assert!(has_node(&input, "Project"));
        assert!(has_node(&input, "MergeRequest"));
    }
}
