//! Convert a `CodeGraph` into Arrow `RecordBatch`es for lance-graph.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use code_graph_linker::v2::graph::*;
use gkg_utils::arrow::{BatchBuilder, ColumnSpec, ColumnType};
use petgraph::graph::NodeIndex;
use rustc_hash::FxHashMap;

pub type LanceDatasets = HashMap<String, RecordBatch>;

fn str_col(name: &str) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        col_type: ColumnType::Str,
        nullable: false,
    }
}

fn int_col(name: &str) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        col_type: ColumnType::Int,
        nullable: false,
    }
}

fn opt_str_col(name: &str) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        col_type: ColumnType::Str,
        nullable: true,
    }
}

/// Convert a CodeGraph into named RecordBatches for lance-graph queries.
pub fn to_lance_datasets(graph: &CodeGraph, ctx: &RowContext<'_>) -> anyhow::Result<LanceDatasets> {
    let ids = graph.assign_ids(ctx.project_id, ctx.branch);
    let mut datasets = HashMap::new();

    // Node tables
    datasets.insert("Directory".into(), build_directory_batch(graph, &ids, ctx)?);
    datasets.insert("File".into(), build_file_batch(graph, &ids, ctx)?);
    datasets.insert(
        "Definition".into(),
        build_definition_batch(graph, &ids, ctx)?,
    );
    datasets.insert(
        "ImportedSymbol".into(),
        build_import_batch(graph, &ids, ctx)?,
    );

    // Edge tables — one per edge kind
    let edge_rows = build_edge_rows(graph, &ids);
    for (kind, rows) in group_edges_by_kind(edge_rows) {
        datasets.insert(kind, build_edge_batch(&rows)?);
    }

    Ok(datasets)
}

fn build_directory_batch(
    graph: &CodeGraph,
    ids: &FxHashMap<NodeIndex, i64>,
    ctx: &RowContext<'_>,
) -> anyhow::Result<RecordBatch> {
    let specs = vec![
        int_col("id"),
        int_col("project_id"),
        str_col("branch"),
        str_col("commit_sha"),
        str_col("path"),
        str_col("name"),
    ];
    let rows: Vec<DirectoryRow<'_>> = graph
        .directories()
        .map(|(idx, d)| DirectoryRow {
            dir: d,
            id: ids[&idx],
        })
        .collect();
    Ok(BatchBuilder::new(&specs, rows.len())?.build(&rows, |row, b| row.write_row(b, ctx))?)
}

fn build_file_batch(
    graph: &CodeGraph,
    ids: &FxHashMap<NodeIndex, i64>,
    ctx: &RowContext<'_>,
) -> anyhow::Result<RecordBatch> {
    let specs = vec![
        int_col("id"),
        int_col("project_id"),
        str_col("branch"),
        str_col("commit_sha"),
        str_col("path"),
        str_col("name"),
        str_col("extension"),
        str_col("language"),
    ];
    let rows: Vec<FileRow<'_>> = graph
        .files()
        .map(|(idx, f)| FileRow {
            file: f,
            id: ids[&idx],
        })
        .collect();
    Ok(BatchBuilder::new(&specs, rows.len())?.build(&rows, |row, b| row.write_row(b, ctx))?)
}

fn build_definition_batch(
    graph: &CodeGraph,
    ids: &FxHashMap<NodeIndex, i64>,
    ctx: &RowContext<'_>,
) -> anyhow::Result<RecordBatch> {
    let specs = vec![
        int_col("id"),
        int_col("project_id"),
        str_col("branch"),
        str_col("commit_sha"),
        str_col("file_path"),
        str_col("fqn"),
        str_col("name"),
        str_col("definition_type"),
        int_col("start_line"),
        int_col("end_line"),
        int_col("start_byte"),
        int_col("end_byte"),
    ];
    let rows: Vec<DefinitionRow<'_>> = graph
        .definitions()
        .map(|(idx, fp, d)| DefinitionRow {
            file_path: fp,
            def: d,
            id: ids[&idx],
        })
        .collect();
    Ok(BatchBuilder::new(&specs, rows.len())?.build(&rows, |row, b| row.write_row(b, ctx))?)
}

fn build_import_batch(
    graph: &CodeGraph,
    ids: &FxHashMap<NodeIndex, i64>,
    ctx: &RowContext<'_>,
) -> anyhow::Result<RecordBatch> {
    let specs = vec![
        int_col("id"),
        int_col("project_id"),
        str_col("branch"),
        str_col("commit_sha"),
        str_col("file_path"),
        str_col("import_type"),
        str_col("path"),
        opt_str_col("name"),
        opt_str_col("alias"),
    ];
    let rows: Vec<ImportRow<'_>> = graph
        .imports()
        .map(|(idx, fp, i)| ImportRow {
            file_path: fp,
            import: i,
            id: ids[&idx],
        })
        .collect();
    Ok(BatchBuilder::new(&specs, rows.len())?.build(&rows, |row, b| row.write_row(b, ctx))?)
}

fn build_edge_rows(graph: &CodeGraph, ids: &FxHashMap<NodeIndex, i64>) -> Vec<EdgeRow> {
    graph
        .graph
        .edge_indices()
        .filter_map(|edge_idx| {
            let (src, tgt) = graph.graph.edge_endpoints(edge_idx)?;
            let weight = &graph.graph[edge_idx];
            let source_id = ids.get(&src)?;
            let target_id = ids.get(&tgt)?;
            Some(EdgeRow {
                source_id: *source_id,
                target_id: *target_id,
                edge_kind: format!("{:?}", weight.relationship.edge_kind),
                source_node_kind: format!("{:?}", weight.relationship.source_node),
                target_node_kind: format!("{:?}", weight.relationship.target_node),
            })
        })
        .collect()
}

fn group_edges_by_kind(rows: Vec<EdgeRow>) -> HashMap<String, Vec<EdgeRow>> {
    let mut groups: HashMap<String, Vec<EdgeRow>> = HashMap::new();
    for row in rows {
        let key = format!("{}To{}", row.source_node_kind, row.target_node_kind);
        groups.entry(key).or_default().push(row);
    }
    groups
}

fn build_edge_batch(rows: &[EdgeRow]) -> anyhow::Result<RecordBatch> {
    let specs = vec![
        int_col("source_id"),
        int_col("target_id"),
        str_col("edge_kind"),
        str_col("source_node_kind"),
        str_col("target_node_kind"),
    ];
    Ok(BatchBuilder::new(&specs, rows.len())?.build(rows, |row, b| row.write_row(b, &()))?)
}
