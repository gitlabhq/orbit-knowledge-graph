use std::collections::HashSet;

use ontology::Ontology;

use super::input::{
    DenormalizedColumnProjection, EdgeFilter, EdgeId, EdgeKind, FkEdgeTransform, NodeColumn,
    StandaloneEdgePlan,
};
use super::{SOURCE_DATA_TABLE, Transformation};

const VERSION_ALIAS: &str = "_version";
const DELETED_ALIAS: &str = "_deleted";

pub(super) struct ProjectionSqlRenderer;

impl ProjectionSqlRenderer {
    pub(super) fn node(
        node_name: &str,
        columns: &[NodeColumn],
        destination_table: String,
        ontology: &Ontology,
    ) -> Transformation {
        Transformation {
            sql: node_select(columns),
            destination_table,
            dict_encode_columns: node_dict_columns(node_name, ontology),
        }
    }

    pub(super) fn fk_edge(fk_edge: &FkEdgeTransform, ontology: &Ontology) -> Transformation {
        let meta = edge_table_metadata(&fk_edge.relationship_kind, ontology);
        let sql = edge_transform_sql(
            &edge_id_expr(&fk_edge.source_id),
            &edge_kind_expr(&fk_edge.source_kind),
            &fk_edge.relationship_kind,
            &edge_id_expr(&fk_edge.target_id),
            &edge_kind_expr(&fk_edge.target_kind),
            fk_edge.namespaced,
            &fk_edge.denormalized_columns,
            &fk_edge.filters,
            &meta.sort_key,
        );
        Transformation {
            sql,
            destination_table: fk_edge.destination_table.clone(),
            dict_encode_columns: meta.dict_columns,
        }
    }

    pub(super) fn standalone_edge(
        edge: &StandaloneEdgePlan,
        destination_table: String,
        ontology: &Ontology,
    ) -> Transformation {
        let meta = edge_table_metadata(&edge.relationship_kind, ontology);
        let sql = edge_transform_sql(
            &edge_id_expr(&edge.source_id),
            &edge_kind_expr(&edge.source_kind),
            &edge.relationship_kind,
            &edge_id_expr(&edge.target_id),
            &edge_kind_expr(&edge.target_kind),
            edge.namespaced,
            &edge.denormalized_columns,
            &edge.filters,
            &meta.sort_key,
        );
        Transformation {
            sql,
            destination_table,
            dict_encode_columns: meta.dict_columns,
        }
    }
}

fn node_dict_columns(node_name: &str, ontology: &Ontology) -> HashSet<String> {
    ontology
        .get_node(node_name)
        .map(|node| {
            node.storage
                .columns
                .iter()
                .filter(|col| col.ch_type.starts_with("LowCardinality"))
                .map(|col| col.name.clone())
                .collect()
        })
        .unwrap_or_default()
}

fn edge_table_metadata(relationship_kind: &str, ontology: &Ontology) -> EdgeTableMetadata {
    let table = ontology.edge_table_for_relationship(relationship_kind);

    let sort_key = ontology
        .sort_key_for_table(table)
        .map(|keys| keys.to_vec())
        .unwrap_or_default();

    let dict_columns = ontology
        .edge_table_config(table)
        .map(|config| {
            config
                .storage
                .columns
                .iter()
                .filter(|col| col.ch_type.starts_with("LowCardinality"))
                .map(|col| col.name.clone())
                .collect()
        })
        .unwrap_or_default();

    EdgeTableMetadata {
        sort_key,
        dict_columns,
    }
}

struct EdgeTableMetadata {
    sort_key: Vec<String>,
    dict_columns: HashSet<String>,
}

fn node_select(columns: &[NodeColumn]) -> String {
    let mut select_list: Vec<String> = columns.iter().map(node_column_expr).collect();
    select_list.push(VERSION_ALIAS.to_string());
    select_list.push(DELETED_ALIAS.to_string());
    format!("SELECT {} FROM {SOURCE_DATA_TABLE}", select_list.join(", "))
}

fn node_column_expr(column: &NodeColumn) -> String {
    match column {
        NodeColumn::Identity(name) => name.clone(),
        NodeColumn::Rename { source, target } => format!("{source} AS {target}"),
        NodeColumn::IntEnum {
            source,
            target,
            values,
            nullable,
        } => {
            let cases: Vec<String> = values
                .iter()
                .map(|(key, value)| format!("WHEN {source} = {key} THEN '{value}'"))
                .collect();
            let null_case = if *nullable {
                format!("WHEN {source} IS NULL THEN NULL ")
            } else {
                format!("WHEN {source} IS NULL THEN '' ")
            };
            format!(
                "CASE {null_case}{} ELSE 'unknown' END AS {target}",
                cases.join(" ")
            )
        }
    }
}

fn edge_id_expr(id: &EdgeId) -> String {
    match id {
        EdgeId::Column(column) => column.clone(),
        EdgeId::Exploded { column, delimiter } => {
            format!("CAST(NULLIF(unnest(string_to_array({column}, '{delimiter}')), '') AS BIGINT)")
        }
        EdgeId::ArrayElement { column, field } => format!("unnest({column})['{field}']"),
        EdgeId::ArrayUnnest { column } => format!("unnest({column})"),
    }
}

fn edge_kind_expr(kind: &EdgeKind) -> String {
    match kind {
        EdgeKind::Literal(value) => format!("'{value}'"),
        EdgeKind::Column { column, mapping } if mapping.is_empty() => column.clone(),
        EdgeKind::Column { column, mapping } => {
            let cases: Vec<String> = mapping
                .iter()
                .map(|(from, to)| format!("WHEN {column} = '{from}' THEN '{to}'"))
                .collect();
            format!("CASE {} ELSE {column} END", cases.join(" "))
        }
    }
}

fn filter_expr(filter: &EdgeFilter) -> String {
    match filter {
        EdgeFilter::IsNotNull(column) => format!("({column} IS NOT NULL)"),
        EdgeFilter::NotEmpty(column) => format!("({column} != '')"),
        EdgeFilter::ArrayNotEmpty(column) => format!("(cardinality({column}) > 0)"),
        EdgeFilter::TypeIn { column, types } => {
            let types_list = types
                .iter()
                .map(|t| format!("'{t}'"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{column} IN ({types_list})")
        }
    }
}

fn filter_clause(filters: &[EdgeFilter]) -> Option<String> {
    if filters.is_empty() {
        return None;
    }
    Some(
        filters
            .iter()
            .map(filter_expr)
            .collect::<Vec<_>>()
            .join(" AND "),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "edge transform SQL takes each edge-transform input as a distinct typed parameter"
)]
fn edge_transform_sql(
    source_id: &str,
    source_kind: &str,
    relationship_kind: &str,
    target_id: &str,
    target_kind: &str,
    namespaced: bool,
    denormalized: &[DenormalizedColumnProjection],
    filters: &[EdgeFilter],
    sort_key: &[String],
) -> String {
    let select_list = edge_select_list(
        source_id,
        source_kind,
        relationship_kind,
        target_id,
        target_kind,
        namespaced,
        denormalized,
    );
    let mut sql = format!("SELECT {} FROM {SOURCE_DATA_TABLE}", select_list.join(", "));
    if let Some(where_sql) = filter_clause(filters) {
        sql.push_str(" WHERE ");
        sql.push_str(&where_sql);
    }
    if !sort_key.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&sort_key.join(", "));
    }
    sql
}

fn edge_select_list(
    source_id: &str,
    source_kind: &str,
    relationship_kind: &str,
    target_id: &str,
    target_kind: &str,
    namespaced: bool,
    denormalized: &[DenormalizedColumnProjection],
) -> Vec<String> {
    let traversal_path = if namespaced {
        "traversal_path".to_string()
    } else {
        "'0/' AS traversal_path".to_string()
    };

    let mut cols = vec![
        traversal_path,
        format!("{source_id} AS source_id"),
        format!("{source_kind} AS source_kind"),
        format!("'{relationship_kind}' AS relationship_kind"),
        format!("{target_id} AS target_id"),
        format!("{target_kind} AS target_kind"),
        VERSION_ALIAS.to_string(),
        DELETED_ALIAS.to_string(),
    ];

    let mut tag_groups: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for d in denormalized {
        let tag_expr = match &d.enum_mapping {
            Some(mapping) => {
                let cases: Vec<String> = mapping
                    .iter()
                    .map(|(key, value)| {
                        format!(
                            "WHEN {} = {} THEN '{}'",
                            d.source_column,
                            key,
                            value.replace('\'', "\\'")
                        )
                    })
                    .collect();
                format!(
                    "CASE WHEN {col} IS NULL THEN '{key}:null' ELSE concat('{key}:', CASE {cases} ELSE CAST({col} AS VARCHAR) END) END",
                    key = d.tag_key,
                    cases = cases.join(" "),
                    col = d.source_column
                )
            }
            None => format!(
                "CASE WHEN {col} IS NULL THEN '{key}:null' ELSE concat('{key}:', CAST({col} AS VARCHAR)) END",
                key = d.tag_key,
                col = d.source_column
            ),
        };
        tag_groups
            .entry(d.edge_column.clone())
            .or_default()
            .push(tag_expr);
    }

    for col_name in &["source_tags", "target_tags"] {
        let expr = match tag_groups.remove(*col_name) {
            Some(tag_exprs) => format!("make_array({})", tag_exprs.join(", ")),
            None => "make_array()".to_string(),
        };
        cols.push(format!("{expr} AS {col_name}"));
    }

    cols
}
