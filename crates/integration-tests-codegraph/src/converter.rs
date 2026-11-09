use arrow::record_batch::RecordBatch;
use code_graph_incremental::export::{EntityTable, ExportTables};
use orbit_arrow::{AsRecordBatch, ColumnSpec, ColumnType};
use query_data_model::DataType;

use anyhow::Result;

pub struct LocalGraphData {
    pub tables: Vec<(String, RecordBatch)>,
}

/// DuckDB's Appender does not support dictionary-encoded Arrow arrays,
/// so all string columns use plain Utf8.
fn column_spec(name: &str, data_type: DataType, nullable: bool) -> ColumnSpec {
    ColumnSpec {
        name: name.to_string(),
        col_type: match data_type {
            DataType::Int => ColumnType::Int,
            _ => ColumnType::Str,
        },
        nullable,
    }
}

fn entity_specs(entity: &EntityTable) -> Vec<ColumnSpec> {
    entity
        .columns
        .iter()
        .map(|(name, data_type, nullable)| column_spec(name, *data_type, *nullable))
        .collect()
}

fn edge_specs(tables: &ExportTables) -> Vec<ColumnSpec> {
    tables
        .edge_columns
        .iter()
        .map(|(name, data_type)| column_spec(name, *data_type, false))
        .collect()
}

pub fn convert_v2_graph(
    graph: &code_graph::v2::linker::CodeGraph,
    project_id: i64,
    branch: &str,
    commit_sha: &str,
    tables: &ExportTables,
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
    let write_repository_structure = graph.output.writes_repository_structure();
    let mut batches = Vec::new();

    for entity in &tables.entities {
        let specs = entity_specs(entity);
        let batch = match entity.entity.as_str() {
            "Directory" => {
                let rows: Vec<_> = if write_repository_structure {
                    graph
                        .directories()
                        .map(|(idx, dir)| DirectoryRow {
                            dir,
                            id: ids[idx.index()],
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                DirectoryRow::to_record_batch(&rows, &specs, &ctx)?
            }
            "File" => {
                let rows: Vec<_> = if write_repository_structure {
                    graph
                        .files()
                        .map(|(idx, file)| FileRow {
                            file,
                            id: ids[idx.index()],
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                FileRow::to_record_batch(&rows, &specs, &ctx)?
            }
            "Definition" => {
                let rows: Vec<_> = graph
                    .definitions()
                    .map(|(idx, file_path, def)| DefinitionRow {
                        file_path,
                        def,
                        pool: &graph.strings,
                        id: ids[idx.index()],
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
                        id: ids[idx.index()],
                    })
                    .collect();
                ImportRow::to_record_batch(&rows, &specs, &ctx)?
            }
            other => panic!("no v2 converter for local entity '{other}'"),
        };

        batches.push((entity.table.clone(), batch));
    }

    let edge_indices: Vec<_> = graph
        .graph
        .edge_indices()
        .filter(|&ei| {
            write_repository_structure
                || graph.graph[ei].relationship.edge_kind.as_ref() != "CONTAINS"
        })
        .collect();

    let edge_batch = orbit_arrow::BatchBuilder::new(&edge_specs(tables), edge_indices.len())?
        .build(&edge_indices, |&ei, b| {
            let (src, tgt) = graph.graph.edge_endpoints(ei).unwrap();
            let edge = &graph.graph[ei];
            EdgeRow {
                source_id: ids[src.index()],
                target_id: ids[tgt.index()],
                edge_kind: edge.relationship.edge_kind.as_ref(),
                source_node_kind: edge.relationship.source_node.as_ref(),
                target_node_kind: edge.relationship.target_node.as_ref(),
            }
            .write_row(b, &())
        })?;
    batches.push((tables.edge_table.clone(), edge_batch));

    Ok(LocalGraphData { tables: batches })
}

pub struct DuckDbConverter {
    pub project_id: i64,
    pub branch: String,
    pub commit_sha: String,
    pub tables: &'static ExportTables,
}

impl code_graph::v2::GraphConverter for DuckDbConverter {
    fn convert(
        &self,
        graph: code_graph::v2::linker::CodeGraph,
    ) -> std::result::Result<Vec<(String, RecordBatch)>, code_graph::v2::SinkError> {
        convert_v2_graph(
            &graph,
            self.project_id,
            &self.branch,
            &self.commit_sha,
            self.tables,
        )
        .map(|data| data.tables)
        .map_err(|e| code_graph::v2::SinkError(format!("DuckDB graph conversion: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_specs_returns_columns_for_all_local_entities() {
        for entity in &ExportTables::embedded().entities {
            assert!(
                !entity_specs(entity).is_empty(),
                "no specs for {}",
                entity.entity
            );
        }
    }

    #[test]
    fn edge_specs_returns_columns() {
        assert!(!edge_specs(ExportTables::embedded()).is_empty());
    }
}
