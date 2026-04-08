use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use code_graph::linker::analysis::types::GraphData;
use gkg_utils::arrow::{BatchBuilder, ColumnSpec, ColumnType};
use ontology::{DataType as OntDataType, Ontology};

use crate::error::Result;

pub struct LocalGraphData {
    pub directories: RecordBatch,
    pub files: RecordBatch,
    pub definitions: RecordBatch,
    pub imported_symbols: RecordBatch,
    pub edges: RecordBatch,
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

pub fn convert_graph_data(
    graph_data: &GraphData,
    project_id: i64,
    branch: &str,
    ontology: &Ontology,
) -> Result<LocalGraphData> {
    let dir_nodes = &graph_data.directory_nodes;
    let file_nodes = &graph_data.file_nodes;
    let def_nodes = &graph_data.definition_nodes;
    let imp_nodes = &graph_data.imported_symbol_nodes;

    Ok(LocalGraphData {
        directories: BatchBuilder::new(&entity_specs(ontology, "Directory"), dir_nodes.len())?
            .build(dir_nodes, |n, b| {
                let Some(id) = n.id else { return Ok(()) };
                b.col("id")?.push_int(id)?;
                b.col("project_id")?.push_int(project_id)?;
                b.col("branch")?.push_str(branch)?;
                b.col("path")?.push_str(&n.path)?;
                b.col("name")?.push_str(&n.name)?;
                Ok(())
            })?,

        files: BatchBuilder::new(&entity_specs(ontology, "File"), file_nodes.len())?.build(
            file_nodes,
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

        definitions: BatchBuilder::new(&entity_specs(ontology, "Definition"), def_nodes.len())?
            .build(def_nodes, |n, b| {
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
            })?,

        imported_symbols: BatchBuilder::new(
            &entity_specs(ontology, "ImportedSymbol"),
            imp_nodes.len(),
        )?
        .build(imp_nodes, |n, b| {
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
        })?,

        edges: convert_edges(graph_data)?,
    })
}

fn convert_edges(graph_data: &GraphData) -> Result<RecordBatch> {
    let rels = &graph_data.relationships;
    let cap = rels.len();
    let mut source_id = Int64Builder::with_capacity(cap);
    let mut source_kind = StringBuilder::with_capacity(cap, cap * 16);
    let mut rel_kind = StringBuilder::with_capacity(cap, cap * 16);
    let mut target_id = Int64Builder::with_capacity(cap);
    let mut target_kind = StringBuilder::with_capacity(cap, cap * 16);
    let mut ver = Int64Builder::with_capacity(cap);

    for rel in rels {
        let (src_kind_str, tgt_kind_str) = rel.kind.source_target_kinds();
        let src_id = lookup_node_id(graph_data, src_kind_str, rel.source_id);
        let tgt_id = lookup_node_id(graph_data, tgt_kind_str, rel.target_id);

        let (Some(s), Some(t)) = (src_id, tgt_id) else {
            continue;
        };

        source_id.append_value(s);
        source_kind.append_value(src_kind_str);
        rel_kind.append_value(rel.relationship_type.edge_kind());
        target_id.append_value(t);
        target_kind.append_value(tgt_kind_str);
        ver.append_value(0);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("source_id", DataType::Int64, false),
            Field::new("source_kind", DataType::Utf8, false),
            Field::new("relationship_kind", DataType::Utf8, false),
            Field::new("target_id", DataType::Int64, false),
            Field::new("target_kind", DataType::Utf8, false),
            Field::new("_version", DataType::Int64, false),
        ])),
        vec![
            Arc::new(source_id.finish()) as ArrayRef,
            Arc::new(source_kind.finish()),
            Arc::new(rel_kind.finish()),
            Arc::new(target_id.finish()),
            Arc::new(target_kind.finish()),
            Arc::new(ver.finish()),
        ],
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
        assert_eq!(result.directories.num_rows(), 0);
        assert_eq!(result.files.num_rows(), 0);
        assert_eq!(result.definitions.num_rows(), 0);
        assert_eq!(result.imported_symbols.num_rows(), 0);
        assert_eq!(result.edges.num_rows(), 0);
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
        assert_eq!(result.directories.num_rows(), 0);
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
        assert_eq!(result.directories.num_rows(), 1);

        let schema = result.directories.schema();
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
        assert_eq!(result.files.num_rows(), 1);

        let schema = result.files.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(col_names.contains(&"path"));
        assert!(col_names.contains(&"extension"));
        assert!(col_names.contains(&"language"));
    }
}
