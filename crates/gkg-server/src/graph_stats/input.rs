use ontology::Ontology;

pub struct GraphStatsInput {
    pub traversal_path: String,
    pub nodes: Vec<NodeStatsTarget>,
}

pub struct NodeStatsTarget {
    pub name: String,
    pub table: String,
}

impl GraphStatsInput {
    pub fn from_ontology(ontology: &Ontology, traversal_path: String) -> Self {
        let nodes = ontology
            .nodes()
            .filter(|node| node.has_traversal_path)
            .map(|node| NodeStatsTarget {
                name: node.name.clone(),
                table: node.destination_table.clone(),
            })
            .collect();

        Self {
            traversal_path,
            nodes,
        }
    }
}
