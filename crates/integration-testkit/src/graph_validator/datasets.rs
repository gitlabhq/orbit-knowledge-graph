//! Convert a `CodeGraph` into Arrow `RecordBatch`es for lance-graph.
//!
//! Uses arrow 56 to match lance-graph's expected types.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_56::array::{Int64Array, Int64Builder, StringArray, StringBuilder};
use arrow_56::datatypes::{DataType, Field, Schema};
use arrow_56::record_batch::RecordBatch;
use code_graph_linker::v2::graph::*;
use rustc_hash::FxHashMap;

pub type LanceDatasets = HashMap<String, RecordBatch>;

pub fn to_lance_datasets(graph: &CodeGraph, ctx: &RowContext<'_>) -> anyhow::Result<LanceDatasets> {
    let ids = graph.assign_ids(ctx.project_id, ctx.branch);
    let mut datasets = HashMap::new();

    datasets.insert("Directory".into(), build_directory_batch(graph, &ids)?);
    datasets.insert("File".into(), build_file_batch(graph, &ids)?);
    datasets.insert("Definition".into(), build_definition_batch(graph, &ids)?);
    datasets.insert("ImportedSymbol".into(), build_import_batch(graph, &ids)?);

    let edge_rows = build_edge_rows(graph, &ids);
    for (kind, rows) in group_edges_by_kind(edge_rows) {
        datasets.insert(kind, build_edge_batch(&rows)?);
    }

    Ok(datasets)
}

struct EdgeRow {
    source_id: i64,
    target_id: i64,
    edge_kind: String,
    source_node_kind: String,
    target_node_kind: String,
}

fn build_directory_batch(
    graph: &CodeGraph,
    ids: &FxHashMap<petgraph::graph::NodeIndex, i64>,
) -> anyhow::Result<RecordBatch> {
    let dirs: Vec<_> = graph.directories().collect();
    let mut id_b = Int64Builder::with_capacity(dirs.len());
    let mut path_b = StringBuilder::with_capacity(dirs.len(), dirs.len() * 32);
    let mut name_b = StringBuilder::with_capacity(dirs.len(), dirs.len() * 16);

    for (idx, d) in &dirs {
        id_b.append_value(ids[idx]);
        path_b.append_value(&d.path);
        name_b.append_value(&d.name);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()),
            Arc::new(path_b.finish()),
            Arc::new(name_b.finish()),
        ],
    )?)
}

fn build_file_batch(
    graph: &CodeGraph,
    ids: &FxHashMap<petgraph::graph::NodeIndex, i64>,
) -> anyhow::Result<RecordBatch> {
    let files: Vec<_> = graph.files().collect();
    let mut id_b = Int64Builder::with_capacity(files.len());
    let mut path_b = StringBuilder::with_capacity(files.len(), files.len() * 32);
    let mut name_b = StringBuilder::with_capacity(files.len(), files.len() * 16);
    let mut ext_b = StringBuilder::with_capacity(files.len(), files.len() * 4);
    let mut lang_b = StringBuilder::with_capacity(files.len(), files.len() * 8);

    for (idx, f) in &files {
        id_b.append_value(ids[idx]);
        path_b.append_value(&f.path);
        name_b.append_value(&f.name);
        ext_b.append_value(&f.extension);
        lang_b.append_value(f.language.names()[0]);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("extension", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, false),
    ]));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()),
            Arc::new(path_b.finish()),
            Arc::new(name_b.finish()),
            Arc::new(ext_b.finish()),
            Arc::new(lang_b.finish()),
        ],
    )?)
}

fn build_definition_batch(
    graph: &CodeGraph,
    ids: &FxHashMap<petgraph::graph::NodeIndex, i64>,
) -> anyhow::Result<RecordBatch> {
    let defs: Vec<_> = graph.definitions().collect();
    let cap = defs.len();
    let mut id_b = Int64Builder::with_capacity(cap);
    let mut fp_b = StringBuilder::with_capacity(cap, cap * 32);
    let mut fqn_b = StringBuilder::with_capacity(cap, cap * 48);
    let mut name_b = StringBuilder::with_capacity(cap, cap * 16);
    let mut dt_b = StringBuilder::with_capacity(cap, cap * 16);
    let mut sl_b = Int64Builder::with_capacity(cap);
    let mut el_b = Int64Builder::with_capacity(cap);
    let mut sb_b = Int64Builder::with_capacity(cap);
    let mut eb_b = Int64Builder::with_capacity(cap);

    for (idx, fp, d) in &defs {
        id_b.append_value(ids[idx]);
        fp_b.append_value(fp.as_ref());
        fqn_b.append_value(d.fqn.to_string());
        name_b.append_value(&d.name);
        dt_b.append_value(d.definition_type);
        sl_b.append_value(d.range.start.line as i64);
        el_b.append_value(d.range.end.line as i64);
        sb_b.append_value(d.range.byte_offset.0 as i64);
        eb_b.append_value(d.range.byte_offset.1 as i64);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("fqn", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("definition_type", DataType::Utf8, false),
        Field::new("start_line", DataType::Int64, false),
        Field::new("end_line", DataType::Int64, false),
        Field::new("start_byte", DataType::Int64, false),
        Field::new("end_byte", DataType::Int64, false),
    ]));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()),
            Arc::new(fp_b.finish()),
            Arc::new(fqn_b.finish()),
            Arc::new(name_b.finish()),
            Arc::new(dt_b.finish()),
            Arc::new(sl_b.finish()),
            Arc::new(el_b.finish()),
            Arc::new(sb_b.finish()),
            Arc::new(eb_b.finish()),
        ],
    )?)
}

fn build_import_batch(
    graph: &CodeGraph,
    ids: &FxHashMap<petgraph::graph::NodeIndex, i64>,
) -> anyhow::Result<RecordBatch> {
    let imports: Vec<_> = graph.imports().collect();
    let cap = imports.len();
    let mut id_b = Int64Builder::with_capacity(cap);
    let mut fp_b = StringBuilder::with_capacity(cap, cap * 32);
    let mut it_b = StringBuilder::with_capacity(cap, cap * 16);
    let mut path_b = StringBuilder::with_capacity(cap, cap * 32);
    let mut name_b = StringBuilder::with_capacity(cap, cap * 16);
    let mut alias_b = StringBuilder::with_capacity(cap, cap * 16);

    for (idx, fp, imp) in &imports {
        id_b.append_value(ids[idx]);
        fp_b.append_value(fp.as_ref());
        it_b.append_value(imp.import_type);
        path_b.append_value(&imp.path);
        match &imp.name {
            Some(n) => name_b.append_value(n),
            None => name_b.append_null(),
        }
        match &imp.alias {
            Some(a) => alias_b.append_value(a),
            None => alias_b.append_null(),
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("import_type", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("alias", DataType::Utf8, true),
    ]));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()),
            Arc::new(fp_b.finish()),
            Arc::new(it_b.finish()),
            Arc::new(path_b.finish()),
            Arc::new(name_b.finish()),
            Arc::new(alias_b.finish()),
        ],
    )?)
}

fn build_edge_rows(
    graph: &CodeGraph,
    ids: &FxHashMap<petgraph::graph::NodeIndex, i64>,
) -> Vec<EdgeRow> {
    graph
        .graph
        .edge_indices()
        .filter_map(|edge_idx| {
            let (src, tgt) = graph.graph.edge_endpoints(edge_idx)?;
            let weight = &graph.graph[edge_idx];
            Some(EdgeRow {
                source_id: *ids.get(&src)?,
                target_id: *ids.get(&tgt)?,
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
    let cap = rows.len();
    let mut src_b = Int64Builder::with_capacity(cap);
    let mut tgt_b = Int64Builder::with_capacity(cap);
    let mut kind_b = StringBuilder::with_capacity(cap, cap * 16);

    for row in rows {
        src_b.append_value(row.source_id);
        tgt_b.append_value(row.target_id);
        kind_b.append_value(&row.edge_kind);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("source_id", DataType::Int64, false),
        Field::new("target_id", DataType::Int64, false),
        Field::new("edge_kind", DataType::Utf8, false),
    ]));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(src_b.finish()),
            Arc::new(tgt_b.finish()),
            Arc::new(kind_b.finish()),
        ],
    )?)
}
