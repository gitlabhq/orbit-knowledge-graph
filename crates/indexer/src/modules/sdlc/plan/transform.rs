//! Turns a node's fields (with FK edges) or an edge's mapping into a `TransformSpec`.

use std::collections::{BTreeMap, HashSet};

use ontology::{
    DataType, DenormDirection, EdgeMapping, EnumType, EtlScope, Field, NodeEntity, NodeRef,
    NodeRefKind, Ontology, StorageColumn,
    constants::{DEFAULT_PRIMARY_KEY, DELETED_COLUMN, VERSION_COLUMN},
};

use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

use super::SOURCE_DATA_TABLE;

pub(in crate::modules::sdlc) enum TransformDeclaration {
    Node(NodeTransformDeclaration),
    Edge {
        relationship_kind: String,
        mapping: Box<EdgeMapping>,
        scope: EtlScope,
    },
    Rust(String),
}

pub(in crate::modules::sdlc) struct NodeTransformDeclaration {
    node_name: String,
    fields: Vec<Field>,
    storage_columns: Vec<StorageColumn>,
    destination_table: String,
    global: bool,
    edges: Vec<EdgeMapping>,
}

/// How an extracted block becomes graph rows; a `Rust` transform owns its outputs and runs no SQL.
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) enum TransformSpec {
    DataFusion(Vec<Transformation>),
    Rust(String),
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct Transformation {
    pub sql: String,
    pub destination_table: String,
    pub dict_encode_columns: HashSet<String>,
}

/// A node property denormalized onto an edge row as a tag; opaque to `build.rs`.
pub(super) struct DenormalizedColumnProjection {
    source_column: String,
    edge_column: String,
    /// Tag key prefix; values become `"status:failed"` tokens.
    tag_key: String,
    enum_mapping: Option<BTreeMap<i64, String>>,
}

enum NodeColumn {
    Identity(String),
    Rename {
        source: String,
        target: String,
    },
    IntEnum {
        source: String,
        target: String,
        values: BTreeMap<i64, String>,
        nullable: bool,
    },
}

enum EdgeId {
    Column(String),
    ArrayElement { column: String, field: String },
}

enum EdgeKind {
    Literal(String),
    Column {
        column: String,
        mapping: BTreeMap<String, String>,
    },
}

enum EdgeFilter {
    IsNotNull(String),
    ArrayNotEmpty(String),
    TypeIn { column: String, types: Vec<String> },
}

impl TransformDeclaration {
    pub(in crate::modules::sdlc) fn from_node_entity_and_edge_mappings(
        node: &NodeEntity,
        edges: &[EdgeMapping],
    ) -> TransformDeclaration {
        TransformDeclaration::Node(NodeTransformDeclaration {
            node_name: node.name.clone(),
            fields: node.fields.clone(),
            storage_columns: node.storage.columns.clone(),
            destination_table: node.destination_table.clone(),
            global: node.global,
            edges: edges.to_vec(),
        })
    }
}

pub(super) fn build_transform_spec(
    transform_declaration: TransformDeclaration,
    ontology: &Ontology,
) -> TransformSpec {
    match transform_declaration {
        TransformDeclaration::Node(node_transform) => {
            build_node_transform(&node_transform, ontology)
        }
        TransformDeclaration::Edge {
            relationship_kind,
            mapping,
            scope,
        } => edge_transform(&relationship_kind, &mapping, scope, ontology),
        TransformDeclaration::Rust(name) => TransformSpec::Rust(name),
    }
}

fn build_node_transform(
    transform_declaration: &NodeTransformDeclaration,
    ontology: &Ontology,
) -> TransformSpec {
    let namespaced = !transform_declaration.global;
    let node_destination =
        prefixed_table_name(&transform_declaration.destination_table, *SCHEMA_VERSION);
    let mut transforms = vec![Transformation {
        sql: node_transform_sql(&node_columns(&transform_declaration.fields)),
        destination_table: node_destination,
        dict_encode_columns: low_cardinality_columns(&transform_declaration.storage_columns),
    }];

    for mapping in &transform_declaration.edges {
        transforms.push(fk_edge_transform(
            mapping,
            &transform_declaration.node_name,
            namespaced,
            ontology,
        ));
    }

    TransformSpec::DataFusion(transforms)
}

fn edge_transform(
    relationship_kind: &str,
    mapping: &EdgeMapping,
    scope: EtlScope,
    ontology: &Ontology,
) -> TransformSpec {
    let denormalized = standalone_edge_denormalized_columns(relationship_kind, mapping, ontology);
    let filters = vec![
        EdgeFilter::IsNotNull(mapping.source.field.clone()),
        EdgeFilter::IsNotNull(mapping.target.field.clone()),
    ];
    TransformSpec::DataFusion(vec![edge_transformation(
        relationship_kind,
        mapping,
        scope == EtlScope::Namespaced,
        filters,
        denormalized,
        ontology,
    )])
}

fn fk_edge_transform(
    mapping: &EdgeMapping,
    node_name: &str,
    namespaced: bool,
    ontology: &Ontology,
) -> Transformation {
    let mut filters = Vec::new();
    for node_ref in [&mapping.source, &mapping.target] {
        if node_ref.field == DEFAULT_PRIMARY_KEY {
            continue;
        }
        filters.push(if mapping.array_field.is_some() {
            EdgeFilter::ArrayNotEmpty(node_ref.field.clone())
        } else {
            EdgeFilter::IsNotNull(node_ref.field.clone())
        });
    }
    let denormalized = fk_denormalized_columns(mapping, node_name, ontology);
    edge_transformation(
        &mapping.label,
        mapping,
        namespaced,
        filters,
        denormalized,
        ontology,
    )
}

fn edge_transformation(
    relationship_kind: &str,
    mapping: &EdgeMapping,
    namespaced: bool,
    mut filters: Vec<EdgeFilter>,
    denormalized: Vec<DenormalizedColumnProjection>,
    ontology: &Ontology,
) -> Transformation {
    let (source_kind, source_filter) = resolve_node_ref_kind(&mapping.source, || {
        ontology.get_edge_source_types(relationship_kind)
    });
    let (target_kind, target_filter) = resolve_node_ref_kind(&mapping.target, || {
        ontology.get_edge_all_target_types(relationship_kind)
    });
    filters.extend(source_filter);
    filters.extend(target_filter);

    let meta = edge_table_metadata(relationship_kind, ontology);
    let sql = build_edge_transform_sql(
        &edge_id_sql(&resolve_edge_id(
            &mapping.source,
            mapping.array_field.as_deref(),
        )),
        &edge_kind_sql(&source_kind),
        relationship_kind,
        &edge_id_sql(&resolve_edge_id(
            &mapping.target,
            mapping.array_field.as_deref(),
        )),
        &edge_kind_sql(&target_kind),
        namespaced,
        &denormalized,
        &filters,
        &meta.sort_key,
    );
    Transformation {
        sql,
        destination_table: prefixed_table_name(
            ontology.edge_table_for_relationship(relationship_kind),
            *SCHEMA_VERSION,
        ),
        dict_encode_columns: meta.dict_columns,
    }
}

fn standalone_edge_denormalized_columns(
    relationship_kind: &str,
    mapping: &EdgeMapping,
    ontology: &Ontology,
) -> Vec<DenormalizedColumnProjection> {
    let mut projections = Vec::new();
    for (node_ref, direction) in [
        (&mapping.source, DenormDirection::Source),
        (&mapping.target, DenormDirection::Target),
    ] {
        let NodeRefKind::Literal(node_kind) = &node_ref.kind else {
            continue;
        };
        for (property_name, input_field) in &node_ref.property_inputs {
            if let Some(property) = ontology.denormalized_properties().iter().find(|property| {
                property.relationship_kind == relationship_kind
                    && property.direction == direction
                    && property.node_kind == *node_kind
                    && property.property_name == *property_name
            }) {
                projections.push(DenormalizedColumnProjection {
                    source_column: input_field.clone(),
                    edge_column: property.edge_column.clone(),
                    tag_key: property.tag_key.clone(),
                    enum_mapping: property.enum_values.clone(),
                });
            }
        }
    }
    projections
}

fn fk_denormalized_columns(
    mapping: &EdgeMapping,
    node_name: &str,
    ontology: &Ontology,
) -> Vec<DenormalizedColumnProjection> {
    ontology
        .denormalized_properties()
        .iter()
        .filter(|dp| {
            dp.relationship_kind == mapping.label
                && dp.node_kind == node_name
                && matches!(
                    (&dp.direction, node_ref_direction(mapping, node_name)),
                    (DenormDirection::Source, Some(DenormDirection::Source))
                        | (DenormDirection::Target, Some(DenormDirection::Target))
                )
        })
        .map(|dp| {
            let field = ontology
                .get_node(&dp.node_kind)
                .and_then(|n| n.fields.iter().find(|f| f.name == dp.property_name));
            let source_column = field
                .and_then(|f| f.column_name())
                .unwrap_or(&dp.property_name)
                .to_string();
            DenormalizedColumnProjection {
                source_column,
                edge_column: dp.edge_column.clone(),
                tag_key: dp.tag_key.clone(),
                enum_mapping: dp.enum_values.clone(),
            }
        })
        .collect()
}

fn node_ref_direction(mapping: &EdgeMapping, node_name: &str) -> Option<DenormDirection> {
    match (&mapping.source.kind, &mapping.target.kind) {
        (NodeRefKind::Literal(kind), _) if kind == node_name => Some(DenormDirection::Source),
        (_, NodeRefKind::Literal(kind)) if kind == node_name => Some(DenormDirection::Target),
        _ => None,
    }
}

fn resolve_edge_id(node_ref: &NodeRef, array_field: Option<&str>) -> EdgeId {
    if let Some(field) = array_field
        && node_ref.field != DEFAULT_PRIMARY_KEY
    {
        return EdgeId::ArrayElement {
            column: node_ref.field.clone(),
            field: field.to_string(),
        };
    }
    EdgeId::Column(node_ref.field.clone())
}

fn resolve_node_ref_kind(
    node_ref: &NodeRef,
    resolve_allowed_types: impl FnOnce() -> Vec<String>,
) -> (EdgeKind, Option<EdgeFilter>) {
    match &node_ref.kind {
        NodeRefKind::Literal(node_type) => (EdgeKind::Literal(node_type.clone()), None),
        NodeRefKind::Derived { column, mapping } => {
            let mut filter_types = resolve_allowed_types();
            for raw in mapping.keys() {
                if !filter_types.iter().any(|t| t == raw) {
                    filter_types.push(raw.clone());
                }
            }
            let filter = if filter_types.is_empty() {
                None
            } else {
                Some(EdgeFilter::TypeIn {
                    column: column.clone(),
                    types: filter_types,
                })
            };
            let kind = EdgeKind::Column {
                column: column.clone(),
                mapping: mapping.clone(),
            };
            (kind, filter)
        }
    }
}

fn node_columns(fields: &[ontology::Field]) -> Vec<NodeColumn> {
    fields
        .iter()
        .filter_map(|field| {
            let col = field.column_name()?;
            if field.data_type == DataType::Enum
                && field.enum_type == EnumType::Int
                && let Some(values) = &field.enum_values
            {
                return Some(NodeColumn::IntEnum {
                    source: col.to_string(),
                    target: field.name.clone(),
                    values: values.clone(),
                    nullable: field.nullable,
                });
            }
            Some(if col == field.name {
                NodeColumn::Identity(field.name.clone())
            } else {
                NodeColumn::Rename {
                    source: col.to_string(),
                    target: field.name.clone(),
                }
            })
        })
        .collect()
}

fn node_transform_sql(columns: &[NodeColumn]) -> String {
    let mut select_list: Vec<String> = columns.iter().map(node_column_sql).collect();
    select_list.push(VERSION_COLUMN.to_string());
    select_list.push(DELETED_COLUMN.to_string());
    format!("SELECT {} FROM {SOURCE_DATA_TABLE}", select_list.join(", "))
}

fn node_column_sql(column: &NodeColumn) -> String {
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

struct EdgeTableMetadata {
    sort_key: Vec<String>,
    dict_columns: HashSet<String>,
}

fn edge_table_metadata(relationship_kind: &str, ontology: &Ontology) -> EdgeTableMetadata {
    let table = ontology.edge_table_for_relationship(relationship_kind);
    let sort_key = ontology
        .sort_key_for_table(table)
        .map(|keys| keys.to_vec())
        .unwrap_or_default();
    let dict_columns = ontology
        .edge_table_config(table)
        .map(|config| low_cardinality_columns(&config.storage.columns))
        .unwrap_or_default();
    EdgeTableMetadata {
        sort_key,
        dict_columns,
    }
}

fn low_cardinality_columns(columns: &[ontology::StorageColumn]) -> HashSet<String> {
    columns
        .iter()
        .filter(|col| col.ch_type.starts_with("LowCardinality"))
        .map(|col| col.name.clone())
        .collect()
}

fn edge_id_sql(id: &EdgeId) -> String {
    match id {
        EdgeId::Column(column) => column.clone(),
        EdgeId::ArrayElement { column, field } => format!("unnest({column})['{field}']"),
    }
}

fn edge_kind_sql(kind: &EdgeKind) -> String {
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

fn edge_filter_sql(filter: &EdgeFilter) -> String {
    match filter {
        EdgeFilter::IsNotNull(column) => format!("({column} IS NOT NULL)"),
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

fn edge_filters_sql(filters: &[EdgeFilter]) -> Option<String> {
    if filters.is_empty() {
        return None;
    }
    Some(
        filters
            .iter()
            .map(edge_filter_sql)
            .collect::<Vec<_>>()
            .join(" AND "),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "SQL builder takes each edge-transform input as a distinct typed parameter"
)]
fn build_edge_transform_sql(
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
    if let Some(where_sql) = edge_filters_sql(filters) {
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
        VERSION_COLUMN.to_string(),
        DELETED_COLUMN.to_string(),
    ];

    let mut tag_groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
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

    // Both tag columns always emitted (empty when unused) so the Arrow schema matches the edge table.
    for col_name in &["source_tags", "target_tags"] {
        let expr = match tag_groups.remove(*col_name) {
            Some(tag_exprs) => format!("make_array({})", tag_exprs.join(", ")),
            None => "make_array()".to_string(),
        };
        cols.push(format!("{expr} AS {col_name}"));
    }

    cols
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ontology() -> Ontology {
        Ontology::load_embedded().expect("should load ontology")
    }

    #[test]
    fn node_transform_handles_rename_and_enum() {
        let mut values = BTreeMap::new();
        values.insert(0, "active".to_string());
        values.insert(1, "blocked".to_string());

        let columns = vec![
            NodeColumn::Identity("id".to_string()),
            NodeColumn::Rename {
                source: "admin".to_string(),
                target: "is_admin".to_string(),
            },
            NodeColumn::IntEnum {
                source: "state".to_string(),
                target: "state".to_string(),
                values,
                nullable: false,
            },
        ];

        let sql = node_transform_sql(&columns);
        assert!(sql.contains("admin AS is_admin"), "sql: {sql}");
        assert!(sql.contains("WHEN state = 0 THEN 'active'"), "sql: {sql}");
        assert!(sql.contains("ELSE 'unknown' END AS state"), "sql: {sql}");
    }

    #[test]
    fn node_transform_preserves_nullable_int_enum_nulls() {
        let mut values = BTreeMap::new();
        values.insert(1, "script_failure".to_string());

        let columns = vec![NodeColumn::IntEnum {
            source: "failure_reason".to_string(),
            target: "failure_reason".to_string(),
            values,
            nullable: true,
        }];

        let sql = node_transform_sql(&columns);
        assert!(sql.contains("WHEN failure_reason IS NULL THEN NULL"));
        assert!(sql.contains("WHEN failure_reason = 1 THEN 'script_failure'"));
        assert!(sql.contains("ELSE 'unknown' END AS failure_reason"));
    }

    #[test]
    fn standalone_edge_property_inputs_select_denormalized_tag_fields() {
        let ontology = test_ontology();
        let pipeline = ontology
            .get_edge_etl("APPROVED")
            .and_then(|pipelines| pipelines.first())
            .expect("APPROVED pipeline");
        let mapping = pipeline
            .transform
            .edges()
            .first()
            .expect("APPROVED edge mapping");

        let projections = standalone_edge_denormalized_columns("APPROVED", mapping, &ontology);

        assert!(projections.iter().any(|projection| {
            projection.source_column == "user_state"
                && projection.edge_column == "source_tags"
                && projection.tag_key == "state"
        }));
        assert!(projections.iter().any(|projection| {
            projection.source_column == "merge_request_state_id"
                && projection.edge_column == "target_tags"
                && projection.tag_key == "state"
        }));
    }

    #[test]
    fn fk_edge_transform_outgoing_literal() {
        let mapping = literal_mapping("id", "Group", "owner_id", "User", "owns");
        let sql = fk_edge_transform(&mapping, "Group", true, &test_ontology()).sql;

        assert!(sql.contains("id AS source_id"));
        assert!(sql.contains("'Group' AS source_kind"));
        assert!(sql.contains("owner_id AS target_id"));
        assert!(sql.contains("'User' AS target_kind"));
        assert!(sql.contains("(owner_id IS NOT NULL)"));
    }

    #[test]
    fn fk_edge_transform_type_mapping_collapses_raw_values() {
        let mut mapping = BTreeMap::new();
        mapping.insert("Issue".to_string(), "WorkItem".to_string());
        mapping.insert("Epic".to_string(), "WorkItem".to_string());

        let edge = EdgeMapping {
            source: NodeRef {
                field: "noteable_id".to_string(),
                property_inputs: indexmap::IndexMap::new(),
                enrich: false,
                kind: NodeRefKind::Derived {
                    column: "noteable_type".to_string(),
                    mapping,
                },
            },
            target: NodeRef {
                field: "id".to_string(),
                kind: NodeRefKind::Literal("Note".to_string()),
                property_inputs: indexmap::IndexMap::new(),
                enrich: false,
            },
            label: "HAS_NOTE".to_string(),
            array_field: None,
            mutable: false,
        };

        let sql = fk_edge_transform(&edge, "Note", true, &test_ontology()).sql;
        assert!(
            sql.contains("WHEN noteable_type = 'Issue' THEN 'WorkItem'"),
            "sql: {sql}"
        );
        assert!(sql.contains("ELSE noteable_type END"), "sql: {sql}");
    }

    #[test]
    fn fk_edge_transform_array_element() {
        let edge = EdgeMapping {
            source: NodeRef {
                field: "assignees".to_string(),
                kind: NodeRefKind::Literal("User".to_string()),
                property_inputs: indexmap::IndexMap::new(),
                enrich: false,
            },
            target: NodeRef {
                field: "id".to_string(),
                kind: NodeRefKind::Literal("MergeRequest".to_string()),
                property_inputs: indexmap::IndexMap::new(),
                enrich: false,
            },
            label: "assigned".to_string(),
            array_field: Some("user_id".to_string()),
            mutable: false,
        };
        let sql = fk_edge_transform(&edge, "MergeRequest", true, &test_ontology()).sql;
        assert!(sql.contains("unnest(assignees)['user_id']"), "sql: {sql}");
        assert!(sql.contains("(cardinality(assignees) > 0)"), "sql: {sql}");
    }

    fn literal_mapping(
        from_field: &str,
        from_kind: &str,
        to_field: &str,
        to_kind: &str,
        label: &str,
    ) -> EdgeMapping {
        EdgeMapping {
            source: NodeRef {
                field: from_field.to_string(),
                kind: NodeRefKind::Literal(from_kind.to_string()),
                property_inputs: indexmap::IndexMap::new(),
                enrich: false,
            },
            target: NodeRef {
                field: to_field.to_string(),
                kind: NodeRefKind::Literal(to_kind.to_string()),
                property_inputs: indexmap::IndexMap::new(),
                enrich: false,
            },
            label: label.to_string(),
            array_field: None,
            mutable: false,
        }
    }
}
