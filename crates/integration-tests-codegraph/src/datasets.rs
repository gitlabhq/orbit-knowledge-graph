use std::collections::HashMap;
use std::sync::Arc;

use arrow_56::array::{Array, ArrayBuilder, BooleanBuilder, Int64Builder, StringBuilder};
use arrow_56::datatypes::{DataType, Field, Schema};
use arrow_56::record_batch::RecordBatch;
use code_graph::v2::linker::graph::*;

pub(crate) type LanceDatasets = HashMap<String, RecordBatch>;
type NodeIds = Vec<i64>;

pub(crate) fn to_lance_datasets(
    graph: &CodeGraph,
    ctx: &RowContext<'_>,
) -> anyhow::Result<LanceDatasets> {
    let ids = graph.assign_ids(ctx.project_id, ctx.branch);
    let mut datasets = HashMap::new();

    if graph.output.includes_structure() {
        datasets.insert("Directory".into(), build_directory_batch(graph, &ids)?);
        datasets.insert("File".into(), build_file_batch(graph, &ids)?);
    }
    datasets.insert("Definition".into(), build_definition_batch(graph, &ids)?);
    datasets.insert("ImportedSymbol".into(), build_import_batch(graph, &ids)?);

    let edge_rows = build_edge_rows(graph, &ids);
    for (kind, rows) in group_edges_by_kind(edge_rows) {
        datasets.insert(kind, build_edge_batch(&rows)?);
    }

    Ok(datasets)
}

fn make_batch(
    fields: &[(&str, DataType, bool)],
    columns: Vec<Box<dyn ArrayBuilder>>,
) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|(n, dt, null)| Field::new(*n, dt.clone(), *null))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|mut b| b.finish()).collect();
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn build_directory_batch(graph: &CodeGraph, ids: &NodeIds) -> anyhow::Result<RecordBatch> {
    let dirs: Vec<_> = graph.directories().collect();
    let n = dirs.len();
    let mut id_b = Int64Builder::with_capacity(n);
    let mut path_b = StringBuilder::with_capacity(n, n * 32);
    let mut name_b = StringBuilder::with_capacity(n, n * 16);

    for (idx, d) in &dirs {
        id_b.append_value(ids[idx.index()]);
        path_b.append_value(&d.path);
        name_b.append_value(&d.name);
    }

    make_batch(
        &[
            ("id", DataType::Int64, false),
            ("path", DataType::Utf8, false),
            ("name", DataType::Utf8, false),
        ],
        vec![Box::new(id_b), Box::new(path_b), Box::new(name_b)],
    )
}

fn build_file_batch(graph: &CodeGraph, ids: &NodeIds) -> anyhow::Result<RecordBatch> {
    let files: Vec<_> = graph.files().collect();
    let n = files.len();
    let mut id_b = Int64Builder::with_capacity(n);
    let mut path_b = StringBuilder::with_capacity(n, n * 32);
    let mut name_b = StringBuilder::with_capacity(n, n * 16);
    let mut ext_b = StringBuilder::with_capacity(n, n * 4);
    let mut lang_b = StringBuilder::with_capacity(n, n * 8);

    for (idx, f) in &files {
        id_b.append_value(ids[idx.index()]);
        path_b.append_value(&f.path);
        name_b.append_value(&f.name);
        ext_b.append_value(&f.extension);
        lang_b.append_value(f.language_name());
    }

    make_batch(
        &[
            ("id", DataType::Int64, false),
            ("path", DataType::Utf8, false),
            ("name", DataType::Utf8, false),
            ("extension", DataType::Utf8, false),
            ("language", DataType::Utf8, false),
        ],
        vec![
            Box::new(id_b),
            Box::new(path_b),
            Box::new(name_b),
            Box::new(ext_b),
            Box::new(lang_b),
        ],
    )
}

fn build_definition_batch(graph: &CodeGraph, ids: &NodeIds) -> anyhow::Result<RecordBatch> {
    let defs: Vec<_> = graph.definitions().collect();
    let n = defs.len();
    let mut id_b = Int64Builder::with_capacity(n);
    let mut fp_b = StringBuilder::with_capacity(n, n * 32);
    let mut fqn_b = StringBuilder::with_capacity(n, n * 48);
    let mut name_b = StringBuilder::with_capacity(n, n * 16);
    let mut dt_b = StringBuilder::with_capacity(n, n * 16);
    let mut sl_b = Int64Builder::with_capacity(n);
    let mut el_b = Int64Builder::with_capacity(n);
    let mut sb_b = Int64Builder::with_capacity(n);
    let mut eb_b = Int64Builder::with_capacity(n);

    for (idx, fp, d) in &defs {
        id_b.append_value(ids[idx.index()]);
        fp_b.append_value(fp.as_ref());
        fqn_b.append_value(graph.str(d.fqn));
        name_b.append_value(graph.str(d.name));
        dt_b.append_value(d.definition_type);
        sl_b.append_value(d.range.start.line as i64);
        el_b.append_value(d.range.end.line as i64);
        sb_b.append_value(d.range.byte_offset.0 as i64);
        eb_b.append_value(d.range.byte_offset.1 as i64);
    }

    make_batch(
        &[
            ("id", DataType::Int64, false),
            ("file_path", DataType::Utf8, false),
            ("fqn", DataType::Utf8, false),
            ("name", DataType::Utf8, false),
            ("definition_type", DataType::Utf8, false),
            ("start_line", DataType::Int64, false),
            ("end_line", DataType::Int64, false),
            ("start_byte", DataType::Int64, false),
            ("end_byte", DataType::Int64, false),
        ],
        vec![
            Box::new(id_b),
            Box::new(fp_b),
            Box::new(fqn_b),
            Box::new(name_b),
            Box::new(dt_b),
            Box::new(sl_b),
            Box::new(el_b),
            Box::new(sb_b),
            Box::new(eb_b),
        ],
    )
}

fn build_import_batch(graph: &CodeGraph, ids: &NodeIds) -> anyhow::Result<RecordBatch> {
    let imports: Vec<_> = graph.imports_iter().collect();
    let n = imports.len();
    let mut id_b = Int64Builder::with_capacity(n);
    let mut fp_b = StringBuilder::with_capacity(n, n * 32);
    let mut it_b = StringBuilder::with_capacity(n, n * 16);
    let mut path_b = StringBuilder::with_capacity(n, n * 32);
    let mut name_b = StringBuilder::with_capacity(n, n * 16);
    let mut alias_b = StringBuilder::with_capacity(n, n * 16);
    let mut type_only_b = BooleanBuilder::with_capacity(n);
    let mut has_target_b = BooleanBuilder::with_capacity(n);

    for (idx, fp, imp) in &imports {
        id_b.append_value(ids[idx.index()]);
        fp_b.append_value(fp.as_ref());
        it_b.append_value(imp.import_type);
        path_b.append_value(graph.str(imp.path));
        match imp.name {
            Some(id) => name_b.append_value(graph.str(id)),
            None => name_b.append_null(),
        }
        match imp.alias {
            Some(id) => alias_b.append_value(graph.str(id)),
            None => alias_b.append_null(),
        }
        type_only_b.append_value(imp.is_type_only);
        has_target_b.append_value(
            graph
                .graph
                .neighbors_directed(*idx, petgraph::Direction::Outgoing)
                .any(|neighbor| graph.graph[neighbor].def_id().is_some()),
        );
    }

    make_batch(
        &[
            ("id", DataType::Int64, false),
            ("file_path", DataType::Utf8, false),
            ("import_type", DataType::Utf8, false),
            ("path", DataType::Utf8, false),
            ("name", DataType::Utf8, true),
            ("alias", DataType::Utf8, true),
            ("is_type_only", DataType::Boolean, false),
            ("has_target", DataType::Boolean, false),
        ],
        vec![
            Box::new(id_b),
            Box::new(fp_b),
            Box::new(it_b),
            Box::new(path_b),
            Box::new(name_b),
            Box::new(alias_b),
            Box::new(type_only_b),
            Box::new(has_target_b),
        ],
    )
}

struct EdgeRow {
    source_id: i64,
    target_id: i64,
    edge_kind: String,
    source_node_kind: String,
    target_node_kind: String,
}

fn build_edge_rows(graph: &CodeGraph, ids: &NodeIds) -> Vec<EdgeRow> {
    graph
        .graph
        .edge_indices()
        .filter(|&edge_idx| {
            graph.output.includes_structure()
                || graph.graph[edge_idx].relationship.edge_kind.as_ref() != "CONTAINS"
        })
        .filter_map(|edge_idx| {
            let (src, tgt) = graph.graph.edge_endpoints(edge_idx)?;
            let weight = &graph.graph[edge_idx];
            Some(EdgeRow {
                source_id: ids[src.index()],
                target_id: ids[tgt.index()],
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
    let n = rows.len();
    let mut src_b = Int64Builder::with_capacity(n);
    let mut tgt_b = Int64Builder::with_capacity(n);
    let mut kind_b = StringBuilder::with_capacity(n, n * 16);

    for row in rows {
        src_b.append_value(row.source_id);
        tgt_b.append_value(row.target_id);
        kind_b.append_value(&row.edge_kind);
    }

    make_batch(
        &[
            ("source_id", DataType::Int64, false),
            ("target_id", DataType::Int64, false),
            ("edge_kind", DataType::Utf8, false),
        ],
        vec![Box::new(src_b), Box::new(tgt_b), Box::new(kind_b)],
    )
}
