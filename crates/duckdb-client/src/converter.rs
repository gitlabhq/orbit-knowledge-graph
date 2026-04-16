use arrow::record_batch::RecordBatch;
use code_graph::linker::analysis::types::{
    DefinitionNode, DirectoryNode, FileNode, GraphData, ImportedSymbolNode, ResolvedEdge,
    RowContext,
};
use gkg_utils::arrow::{AsRecordBatch, ColumnSpec, ColumnType};
use ontology::{DataType as OntDataType, Ontology};

use crate::error::Result;

/// All converted graph data as `(table_name, batch)` pairs, ready for insert.
pub struct LocalGraphData {
    pub tables: Vec<(String, RecordBatch)>,
}

/// Map ontology fields to BatchBuilder column specs.
fn entity_specs(ontology: &Ontology, entity: &str) -> Vec<ColumnSpec> {
    ontology
        .local_entity_fields(entity)
        .unwrap_or_else(|| panic!("entity '{entity}' not in local_entities"))
        .iter()
        .map(|f| ColumnSpec {
            name: f.name.clone(),
            col_type: match f.data_type {
                OntDataType::Int => ColumnType::Int,
                _ => ColumnType::Str,
            },
            nullable: f.nullable,
        })
        .collect()
}

fn edge_specs(ontology: &Ontology) -> Vec<ColumnSpec> {
    ontology
        .local_edge_columns()
        .iter()
        .map(|c| ColumnSpec {
            name: c.name.clone(),
            col_type: match c.data_type {
                OntDataType::Int => ColumnType::Int,
                _ => ColumnType::Str,
            },
            nullable: false,
        })
        .collect()
}

pub fn convert_graph_data(
    graph_data: &GraphData,
    project_id: i64,
    branch: &str,
    commit_sha: &str,
    ontology: &Ontology,
) -> Result<LocalGraphData> {
    let ctx = RowContext {
        project_id,
        branch,
        commit_sha,
    };
    let mut tables = Vec::new();

    for entity_name in ontology.local_entity_names() {
        let dest_table = ontology
            .get_node(entity_name)
            .expect("local entity must exist in nodes")
            .destination_table
            .clone();

        let specs = entity_specs(ontology, entity_name);
        let batch = match entity_name {
            "Directory" => {
                DirectoryNode::to_record_batch(&graph_data.directory_nodes, &specs, &ctx)?
            }
            "File" => FileNode::to_record_batch(&graph_data.file_nodes, &specs, &ctx)?,
            "Definition" => {
                DefinitionNode::to_record_batch(&graph_data.definition_nodes, &specs, &ctx)?
            }
            "ImportedSymbol" => ImportedSymbolNode::to_record_batch(
                &graph_data.imported_symbol_nodes,
                &specs,
                &ctx,
            )?,
            other => panic!("no converter registered for local entity '{other}'"),
        };

        tables.push((dest_table, batch));
    }

    let edge_table = ontology
        .local_edge_table_name()
        .expect("local_db.edge_table.name must be configured")
        .to_string();
    let resolved = graph_data.resolve_edges();
    let edge_batch = ResolvedEdge::to_record_batch(&resolved, &edge_specs(ontology), &ctx)?;
    tables.push((edge_table, edge_batch));

    Ok(LocalGraphData { tables })
}

/// Convert a v2 `CodeGraph` into `LocalGraphData` ready for DuckDB insert.
pub fn convert_v2_graph(
    graph: &code_graph::linker::v2::CodeGraph,
    project_id: i64,
    branch: &str,
    commit_sha: &str,
    ontology: &Ontology,
) -> Result<LocalGraphData> {
    use code_graph::linker::v2::graph::{
        DefinitionRow, DirectoryRow, EdgeRow, FileRow, ImportRow, RowContext as V2RowContext,
    };

    let ctx = V2RowContext {
        project_id,
        branch,
        commit_sha,
    };
    let ids = graph.assign_ids(project_id, branch);
    let mut tables = Vec::new();

    for entity_name in ontology.local_entity_names() {
        let dest_table = ontology
            .get_node(entity_name)
            .expect("local entity must exist in nodes")
            .destination_table
            .clone();

        let specs = entity_specs(ontology, entity_name);
        let batch = match entity_name {
            "Directory" => {
                let rows: Vec<_> = graph
                    .directories()
                    .map(|(idx, dir)| DirectoryRow { dir, id: ids[&idx] })
                    .collect();
                DirectoryRow::to_record_batch(&rows, &specs, &ctx)?
            }
            "File" => {
                let rows: Vec<_> = graph
                    .files()
                    .map(|(idx, file)| FileRow {
                        file,
                        id: ids[&idx],
                    })
                    .collect();
                FileRow::to_record_batch(&rows, &specs, &ctx)?
            }
            "Definition" => {
                let rows: Vec<_> = graph
                    .definitions()
                    .map(|(idx, file_path, def)| DefinitionRow {
                        file_path,
                        def,
                        pool: &graph.strings,
                        id: ids[&idx],
                    })
                    .collect();
                DefinitionRow::to_record_batch(&rows, &specs, &ctx)?
            }
            "ImportedSymbol" => {
                let rows: Vec<_> = graph
                    .imports_iter()
                    .map(|(idx, file_path, import)| ImportRow {
                        file_path,
                        import,
                        pool: &graph.strings,
                        id: ids[&idx],
                    })
                    .collect();
                ImportRow::to_record_batch(&rows, &specs, &ctx)?
            }
            other => panic!("no v2 converter for local entity '{other}'"),
        };

        tables.push((dest_table, batch));
    }

    // Edges
    let edge_table = ontology
        .local_edge_table_name()
        .expect("local_db.edge_table.name must be configured")
        .to_string();

    let edge_rows: Vec<_> = graph
        .graph
        .edge_indices()
        .map(|ei| {
            let (src, tgt) = graph.graph.edge_endpoints(ei).unwrap();
            let edge = &graph.graph[ei];
            EdgeRow {
                source_id: ids.get(&src).copied().unwrap_or(0),
                target_id: ids.get(&tgt).copied().unwrap_or(0),
                edge_kind: edge.relationship.edge_kind.as_ref(),
                source_node_kind: edge.relationship.source_node.as_ref(),
                target_node_kind: edge.relationship.target_node.as_ref(),
            }
        })
        .collect();
    let edge_batch = EdgeRow::to_record_batch(&edge_rows, &edge_specs(ontology), &())?;
    tables.push((edge_table, edge_batch));

    Ok(LocalGraphData { tables })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ontology() -> Ontology {
        Ontology::load_from_dir(std::path::Path::new(env!("ONTOLOGY_DIR")))
            .expect("failed to load ontology")
    }

    fn find_table<'a>(data: &'a LocalGraphData, name: &str) -> Option<&'a RecordBatch> {
        data.tables.iter().find(|(t, _)| t == name).map(|(_, b)| b)
    }

    #[test]
    fn empty_graph_produces_zero_row_batches() {
        let graph = GraphData {
            directory_nodes: vec![],
            file_nodes: vec![],
            definition_nodes: vec![],
            imported_symbol_nodes: vec![],
            relationships: vec![],
        };

        let result = convert_graph_data(&graph, 1, "main", "abc123def", &test_ontology()).unwrap();
        for (table, batch) in &result.tables {
            assert_eq!(batch.num_rows(), 0, "{table} should have 0 rows");
        }
    }

    #[test]
    fn tables_match_ontology_entities_plus_edges() {
        let ont = test_ontology();
        let graph = GraphData {
            directory_nodes: vec![],
            file_nodes: vec![],
            definition_nodes: vec![],
            imported_symbol_nodes: vec![],
            relationships: vec![],
        };

        let result = convert_graph_data(&graph, 1, "main", "abc123def", &ont).unwrap();
        let table_names: Vec<&str> = result.tables.iter().map(|(t, _)| t.as_str()).collect();

        for entity_name in ont.local_entity_names() {
            let dest = &ont.get_node(entity_name).unwrap().destination_table;
            assert!(
                table_names.contains(&dest.as_str()),
                "missing table for {entity_name}: {dest}"
            );
        }
        assert!(table_names.contains(&ont.local_edge_table_name().unwrap()));
    }

    #[test]
    fn directory_schema_matches_ontology() {
        let ont = test_ontology();
        let graph = GraphData {
            directory_nodes: vec![DirectoryNode {
                id: Some(42),
                path: "src".into(),
                absolute_path: "/repo/src".into(),
                repository_name: "repo".into(),
                name: "src".into(),
            }],
            file_nodes: vec![],
            definition_nodes: vec![],
            imported_symbol_nodes: vec![],
            relationships: vec![],
        };

        let result = convert_graph_data(&graph, 100, "main", "abc123def", &ont).unwrap();
        let batch = find_table(&result, "gl_directory").expect("gl_directory table");
        assert_eq!(batch.num_rows(), 1);

        let schema = batch.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(col_names.contains(&"id"));
        assert!(col_names.contains(&"project_id"));
        assert!(col_names.contains(&"branch"));
        assert!(col_names.contains(&"path"));
        assert!(col_names.contains(&"name"));
        assert!(!col_names.contains(&"traversal_path"));
        assert!(col_names.contains(&"commit_sha"));
    }

    #[test]
    fn file_schema_matches_ontology() {
        let ont = test_ontology();
        let graph = GraphData {
            directory_nodes: vec![],
            file_nodes: vec![FileNode {
                id: Some(10),
                path: "src/lib.rs".into(),
                absolute_path: "/repo/src/lib.rs".into(),
                language: "Rust".into(),
                repository_name: "repo".into(),
                extension: "rs".into(),
                name: "lib.rs".into(),
            }],
            definition_nodes: vec![],
            imported_symbol_nodes: vec![],
            relationships: vec![],
        };

        let result = convert_graph_data(&graph, 100, "main", "abc123def", &ont).unwrap();
        let batch = find_table(&result, "gl_file").expect("gl_file table");
        assert_eq!(batch.num_rows(), 1);

        let schema = batch.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(col_names.contains(&"path"));
        assert!(col_names.contains(&"extension"));
        assert!(col_names.contains(&"language"));
    }

    #[test]
    fn nodes_without_ids_are_skipped() {
        let graph = GraphData {
            directory_nodes: vec![DirectoryNode {
                id: None,
                path: "src".into(),
                absolute_path: "/repo/src".into(),
                repository_name: "repo".into(),
                name: "src".into(),
            }],
            file_nodes: vec![],
            definition_nodes: vec![],
            imported_symbol_nodes: vec![],
            relationships: vec![],
        };

        let result = convert_graph_data(&graph, 1, "main", "abc123def", &test_ontology()).unwrap();
        let batch = find_table(&result, "gl_directory").expect("gl_directory table");
        assert_eq!(batch.num_rows(), 0);
    }
}
