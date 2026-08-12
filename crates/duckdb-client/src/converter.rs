use arrow::record_batch::RecordBatch;
use gkg_utils::arrow::{AsRecordBatch, ColumnSpec, ColumnType};
use ontology::{DataType as OntDataType, Ontology};

use crate::error::Result;

pub struct LocalGraphData {
    pub tables: Vec<(String, RecordBatch)>,
}

/// DuckDB's Appender does not support dictionary-encoded Arrow arrays,
/// so all string columns use plain Utf8.
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

pub fn convert_v2_graph(
    graph: &code_graph::v2::linker::concurrent_graph::ConcurrentGraph,
    project_id: i64,
    branch: &str,
    commit_sha: &str,
    ontology: &Ontology,
) -> Result<LocalGraphData> {
    use code_graph::v2::linker::graph::{
        DefinitionRow, DirectoryRow, EdgeRow, FileRow, ImportRow, RowContext as V2RowContext,
    };

    let ctx = V2RowContext {
        project_id,
        branch,
        commit_sha,
    };
    let ids = graph.assign_ids(project_id, branch);
    let write_repository_structure = !graph.parsed_only;
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
                let rows: Vec<_> = if write_repository_structure {
                    graph
                        .iter_directories()
                        .map(|(nid, path, name)| DirectoryRow {
                            path,
                            name,
                            id: ids[nid.0 as usize],
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                DirectoryRow::to_record_batch(&rows, &specs, &ctx)?
            }
            "File" => {
                let file_data: Vec<_> = if write_repository_structure {
                    graph
                        .iter_files()
                        .map(|(nid, path, name, ext, lang, size, reason)| {
                            (nid, path, name, ext, lang, size, reason.to_string())
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let rows: Vec<_> = file_data
                    .iter()
                    .map(|(nid, path, name, ext, lang, size, reason_s)| FileRow {
                        path,
                        name,
                        extension: ext,
                        language: lang.map_or("unknown", |l| l.names()[0]),
                        size: *size,
                        reason: reason_s,
                        id: ids[nid.0 as usize],
                    })
                    .collect();
                FileRow::to_record_batch(&rows, &specs, &ctx)?
            }
            "Definition" => {
                let rows: Vec<_> = graph
                    .iter_definitions()
                    .map(|(nid, file_path, def)| DefinitionRow {
                        file_path,
                        def,
                        pool: &graph.strings,
                        id: ids[nid.0 as usize],
                    })
                    .collect();
                DefinitionRow::to_record_batch(&rows, &specs, &ctx)?
            }
            "ImportedSymbol" => {
                let rows: Vec<_> = graph
                    .iter_imports()
                    .map(|(nid, file_path, import)| ImportRow {
                        file_path,
                        import,
                        pool: &graph.strings,
                        id: ids[nid.0 as usize],
                    })
                    .collect();
                ImportRow::to_record_batch(&rows, &specs, &ctx)?
            }
            other => panic!("no v2 converter for local entity '{other}'"),
        };

        tables.push((dest_table, batch));
    }

    let edge_table = ontology
        .local_edge_table_name()
        .expect("local_db.edge_table.name must be configured")
        .to_string();

    let mut edge_rows: Vec<_> = graph
        .iter_edges()
        .filter(|e| write_repository_structure || e.relationship.edge_kind.as_ref() != "CONTAINS")
        .map(|e| EdgeRow {
            source_id: ids[e.source.0 as usize],
            target_id: ids[e.target.0 as usize],
            edge_kind: e.relationship.edge_kind.as_ref(),
            source_node_kind: e.relationship.source_node.as_ref(),
            target_node_kind: e.relationship.target_node.as_ref(),
        })
        .collect();

    edge_rows.sort_by(|a, b| {
        a.edge_kind
            .cmp(b.edge_kind)
            .then_with(|| a.source_node_kind.cmp(b.source_node_kind))
            .then_with(|| a.target_node_kind.cmp(b.target_node_kind))
    });

    let edge_batch = EdgeRow::to_record_batch(&edge_rows, &edge_specs(ontology), &())?;
    tables.push((edge_table, edge_batch));

    Ok(LocalGraphData { tables })
}

pub struct DuckDbConverter {
    pub project_id: i64,
    pub branch: String,
    pub commit_sha: String,
    pub ontology: std::sync::Arc<Ontology>,
}

impl code_graph::v2::GraphConverter for DuckDbConverter {
    fn convert(
        &self,
        graph: code_graph::v2::linker::concurrent_graph::ConcurrentGraph,
    ) -> std::result::Result<Vec<(String, RecordBatch)>, code_graph::v2::SinkError> {
        convert_v2_graph(
            &graph,
            self.project_id,
            &self.branch,
            &self.commit_sha,
            &self.ontology,
        )
        .map(|data| data.tables)
        .map_err(|e| code_graph::v2::SinkError(format!("DuckDB graph conversion: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ontology() -> Ontology {
        Ontology::load_from_dir(std::path::Path::new(env!("ONTOLOGY_DIR")))
            .expect("failed to load ontology")
    }

    #[test]
    fn entity_specs_returns_columns_for_all_local_entities() {
        let ont = test_ontology();
        for entity_name in ont.local_entity_names() {
            let specs = entity_specs(&ont, entity_name);
            assert!(!specs.is_empty(), "no specs for {entity_name}");
        }
    }

    #[test]
    fn edge_specs_returns_columns() {
        let ont = test_ontology();
        let specs = edge_specs(&ont);
        assert!(!specs.is_empty());
    }
}
