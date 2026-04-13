//! Graph configuration for lance-graph.

use lance_graph::GraphConfig;

/// Build the graph config registering all node labels and relationship types.
pub fn make_graph_config() -> anyhow::Result<GraphConfig> {
    let config = GraphConfig::builder()
        .with_node_label("Directory", "id")
        .with_node_label("File", "id")
        .with_node_label("Definition", "id")
        .with_node_label("ImportedSymbol", "id")
        .with_relationship("DirectoryToDirectory", "source_id", "target_id")
        .with_relationship("DirectoryToFile", "source_id", "target_id")
        .with_relationship("FileToDefinition", "source_id", "target_id")
        .with_relationship("FileToImportedSymbol", "source_id", "target_id")
        .with_relationship("DefinitionToDefinition", "source_id", "target_id")
        .build()?;

    Ok(config)
}
