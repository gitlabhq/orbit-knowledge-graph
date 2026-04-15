use lance_graph::GraphConfig;

pub(crate) fn make_graph_config() -> anyhow::Result<GraphConfig> {
    Ok(GraphConfig::builder()
        .with_node_label("Directory", "id")
        .with_node_label("File", "id")
        .with_node_label("Definition", "id")
        .with_node_label("ImportedSymbol", "id")
        .with_relationship("DirectoryToDirectory", "source_id", "target_id")
        .with_relationship("DirectoryToFile", "source_id", "target_id")
        .with_relationship("FileToDefinition", "source_id", "target_id")
        .with_relationship("FileToImportedSymbol", "source_id", "target_id")
        .with_relationship("DefinitionToDefinition", "source_id", "target_id")
        .build()?)
}
