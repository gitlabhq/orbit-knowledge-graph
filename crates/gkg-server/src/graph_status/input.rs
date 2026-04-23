use ontology::Ontology;
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
    pub fn from_ontology(ontology: &Ontology, traversal_path: String) -> Result<Self, Status> {
        let nodes = ontology
            .nodes()
            .filter(|node| node.has_traversal_path)
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
