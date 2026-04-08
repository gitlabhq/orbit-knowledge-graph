use arrow::record_batch::RecordBatch;
use code_graph::linker::analysis::types::GraphData;
use gkg_utils::arrow::{BatchBuilder, ColumnSpec, ColumnType};
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
    ontology: &Ontology,
) -> Result<LocalGraphData> {
    let mut tables = Vec::new();

    // Convert each local entity using its ontology-derived schema.
    // The entity name dispatches to the right GraphData field and fill closure.
    for entity_name in ontology.local_entity_names() {
        let dest_table = ontology
            .get_node(entity_name)
            .expect("local entity must exist in nodes")
            .destination_table
            .clone();

        let batch = match entity_name {
            "Directory" => convert_entity(
                &entity_specs(ontology, entity_name),
                &graph_data.directory_nodes,
                |n, b| {
                    let Some(id) = n.id else { return Ok(()) };
                    b.col("id")?.push_int(id)?;
                    b.col("project_id")?.push_int(project_id)?;
                    b.col("branch")?.push_str(branch)?;
                    b.col("path")?.push_str(&n.path)?;
                    b.col("name")?.push_str(&n.name)?;
                    Ok(())
                },
            )?,
            "File" => convert_entity(
                &entity_specs(ontology, entity_name),
                &graph_data.file_nodes,
                |n, b| {
                    let Some(id) = n.id else { return Ok(()) };
                    b.col("id")?.push_int(id)?;
                    b.col("project_id")?.push_int(project_id)?;
                    b.col("branch")?.push_str(branch)?;
                    b.col("path")?.push_str(&n.path)?;
                    b.col("name")?.push_str(&n.name)?;
                    b.col("extension")?.push_str(&n.extension)?;
                    b.col("language")?.push_str(&n.language)?;
                    Ok(())
                },
            )?,
            "Definition" => convert_entity(
                &entity_specs(ontology, entity_name),
                &graph_data.definition_nodes,
                |n, b| {
                    let Some(id) = n.id else { return Ok(()) };
                    b.col("id")?.push_int(id)?;
                    b.col("project_id")?.push_int(project_id)?;
                    b.col("branch")?.push_str(branch)?;
                    b.col("file_path")?.push_str(n.file_path.as_ref())?;
                    b.col("fqn")?.push_str(n.fqn.to_string())?;
                    b.col("name")?.push_str(n.fqn.name())?;
                    b.col("definition_type")?
                        .push_str(n.definition_type.as_str())?;
                    b.col("start_line")?.push_int(n.range.start.line as i64)?;
                    b.col("end_line")?.push_int(n.range.end.line as i64)?;
                    b.col("start_byte")?
                        .push_int(n.range.byte_offset.0 as i64)?;
                    b.col("end_byte")?.push_int(n.range.byte_offset.1 as i64)?;
                    Ok(())
                },
            )?,
            "ImportedSymbol" => convert_entity(
                &entity_specs(ontology, entity_name),
                &graph_data.imported_symbol_nodes,
                |n, b| {
                    let Some(id) = n.id else { return Ok(()) };
                    b.col("id")?.push_int(id)?;
                    b.col("project_id")?.push_int(project_id)?;
                    b.col("branch")?.push_str(branch)?;
                    b.col("file_path")?.push_str(&n.location.file_path)?;
                    b.col("import_type")?.push_str(n.import_type.as_str())?;
                    b.col("import_path")?.push_str(&n.import_path)?;
                    b.col("identifier_name")?
                        .push_opt_str(n.identifier.as_ref().map(|i| &i.name))?;
                    b.col("identifier_alias")?
                        .push_opt_str(n.identifier.as_ref().and_then(|i| i.alias.as_ref()))?;
                    b.col("start_line")?
                        .push_int(n.location.start_line as i64)?;
                    b.col("end_line")?.push_int(n.location.end_line as i64)?;
                    b.col("start_byte")?.push_int(n.location.start_byte)?;
                    b.col("end_byte")?.push_int(n.location.end_byte)?;
                    Ok(())
                },
            )?,
            other => panic!("no converter registered for local entity '{other}'"),
        };

        tables.push((dest_table, batch));
    }

    // Edge table.
    let edge_table = ontology
        .local_edge_table_name()
        .expect("local_db.edge_table.name must be configured")
        .to_string();
    tables.push((edge_table, convert_edges(graph_data, ontology)?));

    Ok(LocalGraphData { tables })
}

fn convert_entity<N>(
    specs: &[ColumnSpec],
    nodes: &[N],
    fill: impl Fn(&N, &mut BatchBuilder) -> std::result::Result<(), arrow::error::ArrowError>,
) -> Result<RecordBatch> {
    Ok(BatchBuilder::new(specs, nodes.len())?.build(nodes, fill)?)
}

fn convert_edges(graph_data: &GraphData, ontology: &Ontology) -> Result<RecordBatch> {
    let specs = edge_specs(ontology);

    let resolved: Vec<_> = graph_data
        .relationships
        .iter()
        .filter_map(|rel| {
            let (src_kind, tgt_kind) = rel.kind.source_target_kinds();
            let src_id = lookup_node_id(graph_data, src_kind, rel.source_id)?;
            let tgt_id = lookup_node_id(graph_data, tgt_kind, rel.target_id)?;
            Some((
                src_id,
                src_kind,
                rel.relationship_type.edge_kind(),
                tgt_id,
                tgt_kind,
            ))
        })
        .collect();

    Ok(BatchBuilder::new(&specs, resolved.len())?.build(
        &resolved,
        |&(src_id, src_kind, ref rel_kind, tgt_id, tgt_kind), b| {
            b.col("source_id")?.push_int(src_id)?;
            b.col("source_kind")?.push_str(src_kind)?;
            b.col("relationship_kind")?.push_str(rel_kind)?;
            b.col("target_id")?.push_int(tgt_id)?;
            b.col("target_kind")?.push_str(tgt_kind)?;
            b.col("_version")?.push_int(0)?;
            Ok(())
        },
    )?)
}

fn lookup_node_id(graph_data: &GraphData, kind: &str, index: Option<u32>) -> Option<i64> {
    let index = index? as usize;
    match kind {
        "Directory" => graph_data.directory_nodes.get(index).and_then(|n| n.id),
        "File" => graph_data.file_nodes.get(index).and_then(|n| n.id),
        "Definition" => graph_data.definition_nodes.get(index).and_then(|n| n.id),
        "ImportedSymbol" => graph_data
            .imported_symbol_nodes
            .get(index)
            .and_then(|n| n.id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph::linker::analysis::types::{DirectoryNode, FileNode};

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

        let result = convert_graph_data(&graph, 1, "main", &test_ontology()).unwrap();
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

        let result = convert_graph_data(&graph, 1, "main", &ont).unwrap();
        let table_names: Vec<&str> = result.tables.iter().map(|(t, _)| t.as_str()).collect();

        // Every local entity's destination table should be present.
        for entity_name in ont.local_entity_names() {
            let dest = &ont.get_node(entity_name).unwrap().destination_table;
            assert!(
                table_names.contains(&dest.as_str()),
                "missing table for {entity_name}: {dest}"
            );
        }
        // Edge table should be present.
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

        let result = convert_graph_data(&graph, 100, "main", &ont).unwrap();
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
        assert!(!col_names.contains(&"commit_sha"));
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

        let result = convert_graph_data(&graph, 100, "main", &ont).unwrap();
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

        let result = convert_graph_data(&graph, 1, "main", &test_ontology()).unwrap();
        let batch = find_table(&result, "gl_directory").expect("gl_directory table");
        assert_eq!(batch.num_rows(), 0);
    }
}
