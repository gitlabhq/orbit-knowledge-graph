//! Graph configuration for lance-graph.

use lance_graph::GraphConfig;

/// Build the graph config registering all node labels and relationship types.
pub fn make_graph_config() -> anyhow::Result<GraphConfig> {
    let mut config = GraphConfig::new();

    // Node labels — keyed on "id" column
    config.add_node_label("Directory", "id")?;
    config.add_node_label("File", "id")?;
    config.add_node_label("Definition", "id")?;
    config.add_node_label("ImportedSymbol", "id")?;

    // Relationship types — one per source→target kind pair
    config.add_relationship_type("DirectoryToDirectory", "source_id", "target_id")?;
    config.add_relationship_type("DirectoryToFile", "source_id", "target_id")?;
    config.add_relationship_type("FileToDefinition", "source_id", "target_id")?;
    config.add_relationship_type("FileToImportedSymbol", "source_id", "target_id")?;
    config.add_relationship_type("DefinitionToDefinition", "source_id", "target_id")?;

    Ok(config)
}
