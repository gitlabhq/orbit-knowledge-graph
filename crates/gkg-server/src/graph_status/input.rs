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

fn retarget(table: &str, from_prefix: &str, to_prefix: &str) -> String {
    format!(
        "{to_prefix}{}",
        table.strip_prefix(from_prefix).unwrap_or(table)
    )
}

impl GraphStatusInput {
    pub fn from_ontology(
        ontology: &Ontology,
        traversal_path: String,
        security_context: &SecurityContext,
        from_prefix: &str,
        target_prefix: &str,
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
                table: retarget(&node.destination_table, from_prefix, target_prefix),
            })
            .collect();

        let project = retarget(
            &ontology
                .get_node(PROJECT_NODE)
                .ok_or_else(|| {
                    Status::internal(format!("ontology missing required node: {PROJECT_NODE}"))
                })?
                .destination_table,
            from_prefix,
            target_prefix,
        );

        let code_checkpoint = retarget(
            &ontology
                .auxiliary_tables()
                .iter()
                .find(|t| t.name.ends_with(CHECKPOINT_TABLE_SUFFIX))
                .ok_or_else(|| {
                    Status::internal(format!(
                        "ontology missing auxiliary table ending with: {CHECKPOINT_TABLE_SUFFIX}"
                    ))
                })?
                .name,
            from_prefix,
            target_prefix,
        );

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

        let input =
            GraphStatusInput::from_ontology(&ontology, "1/100/".to_string(), &ctx, "", "").unwrap();

        assert!(!has_node(&input, VULNERABILITY));
        assert!(has_node(&input, "Project"));
        assert!(has_node(&input, "MergeRequest"));
    }

    #[test]
    fn security_manager_includes_security_entities() {
        let ontology = embedded_ontology();
        let ctx =
            SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/100/", 25)]).unwrap();

        let input =
            GraphStatusInput::from_ontology(&ontology, "1/100/".to_string(), &ctx, "", "").unwrap();

        assert!(has_node(&input, VULNERABILITY));
        assert!(has_node(&input, "Project"));
    }

    #[test]
    fn admin_includes_all_entities() {
        let ontology = embedded_ontology();
        let ctx = SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/", 50)])
            .unwrap()
            .with_role(true, Some(50));

        let input =
            GraphStatusInput::from_ontology(&ontology, "1/".to_string(), &ctx, "", "").unwrap();

        assert!(has_node(&input, VULNERABILITY));
        assert!(has_node(&input, "Project"));
        assert!(has_node(&input, "MergeRequest"));
    }

    #[test]
    fn retarget_replaces_prefix() {
        assert_eq!(retarget("v3_gl_project", "v3_", "v5_"), "v5_gl_project");
    }

    #[test]
    fn retarget_same_prefix_is_noop() {
        assert_eq!(retarget("v3_gl_project", "v3_", "v3_"), "v3_gl_project");
    }

    #[test]
    fn retarget_empty_from_prefix_prepends() {
        assert_eq!(retarget("gl_project", "", "v5_"), "v5_gl_project");
    }

    fn project_table(input: &GraphStatusInput) -> &str {
        input
            .nodes
            .iter()
            .find(|n| n.name == "Project")
            .map(|n| n.table.as_str())
            .unwrap()
    }

    #[test]
    fn from_ontology_retargets_versioned_tables_to_active_prefix() {
        let ontology = Arc::new(
            Ontology::load_embedded()
                .expect("ontology must load")
                .with_schema_version_prefix("v50_"),
        );
        let ctx = SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/", 50)])
            .unwrap()
            .with_role(true, Some(50));

        let pinned =
            GraphStatusInput::from_ontology(&ontology, "1/".to_string(), &ctx, "v50_", "v50_")
                .unwrap();
        let retargeted =
            GraphStatusInput::from_ontology(&ontology, "1/".to_string(), &ctx, "v50_", "v999_")
                .unwrap();

        assert_eq!(project_table(&pinned), "v50_gl_project");
        assert_eq!(project_table(&retargeted), "v999_gl_project");
        assert!(retargeted.project_tables.project.starts_with("v999_"));
        assert!(
            retargeted
                .project_tables
                .code_checkpoint
                .starts_with("v999_")
        );
    }
}
