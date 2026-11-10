//! Turns an entity's columns (with FK edges) or a relationship's mapping into a `TransformSpec`.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use query_data_model::catalog::sources::{
    EdgeMapping, Endpoint, EndpointKind, EtlScope, SourceColumn,
};
use query_data_model::implementations::clickhouse::schema::Table;
use query_data_model::{
    Catalog, ClickHouse, DEFAULT_PRIMARY_KEY, DELETED_COLUMN, DataType, DenormalizedDirection,
    DenormalizedKey, EntityId, EnumType, PropertyId, VERSION_COLUMN,
};

use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

use super::SOURCE_DATA_TABLE;
use super::build::PlanError;

pub(in crate::modules::sdlc) enum TransformDeclaration {
    Node(NodeTransformDeclaration),
    Edge {
        mapping: Box<EdgeMapping>,
        scope: EtlScope,
    },
    Rust(String),
}

pub(in crate::modules::sdlc) struct NodeTransformDeclaration {
    entity: EntityId,
    columns: Vec<SourceColumn>,
    global: bool,
    edges: Vec<EdgeMapping>,
}

/// How an extracted block becomes graph rows; a `Rust` transform owns its outputs and runs no SQL.
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) enum TransformSpec {
    DataFusion(Vec<Transformation>),
    Rust(String),
}

impl TransformSpec {
    pub(in crate::modules::sdlc) fn label(&self) -> &str {
        match self {
            Self::DataFusion(_) => "data_fusion",
            Self::Rust(name) => name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Transformation {
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

#[derive(Clone)]
enum EdgeFilter {
    IsNotNull(String),
    ArrayNotEmpty(String),
    TypeIn {
        column: String,
        types: Vec<String>,
    },
    VariantIn {
        source_kind: String,
        target_kind: String,
        variants: Vec<(String, String)>,
    },
}

impl TransformDeclaration {
    pub(in crate::modules::sdlc) fn node(
        entity: EntityId,
        columns: &[SourceColumn],
        scope: EtlScope,
        edges: &[EdgeMapping],
    ) -> TransformDeclaration {
        TransformDeclaration::Node(NodeTransformDeclaration {
            entity,
            columns: columns.to_vec(),
            global: scope == EtlScope::Global,
            edges: edges.to_vec(),
        })
    }
}

pub(super) fn compile_transform_spec(
    transform_declaration: TransformDeclaration,
    catalog: &Catalog<ClickHouse>,
) -> Result<TransformSpec, PlanError> {
    match transform_declaration {
        TransformDeclaration::Node(node_transform) => {
            build_node_transform(&node_transform, catalog)
        }
        TransformDeclaration::Edge { mapping, scope } => edge_transform(&mapping, scope, catalog),
        TransformDeclaration::Rust(name) => Ok(TransformSpec::Rust(name)),
    }
}

fn build_node_transform(
    transform_declaration: &NodeTransformDeclaration,
    catalog: &Catalog<ClickHouse>,
) -> Result<TransformSpec, PlanError> {
    let entity = transform_declaration.entity;
    let table = catalog.table_of(entity).ok_or_else(|| {
        PlanError::Invalid(format!(
            "{} has no graph table",
            catalog.graph().entity(entity).name
        ))
    })?;
    let namespaced = !transform_declaration.global;
    let mut transforms = vec![Transformation {
        sql: node_transform_sql(&node_columns(&transform_declaration.columns, catalog)),
        destination_table: prefixed_table_name(table, *SCHEMA_VERSION),
        dict_encode_columns: low_cardinality_columns(schema_table(catalog, table)),
    }];

    for mapping in &transform_declaration.edges {
        transforms.extend(fk_edge_transform(
            mapping,
            transform_declaration,
            namespaced,
            catalog,
        )?);
    }

    Ok(TransformSpec::DataFusion(transforms))
}

fn edge_transform(
    mapping: &EdgeMapping,
    scope: EtlScope,
    catalog: &Catalog<ClickHouse>,
) -> Result<TransformSpec, PlanError> {
    let denormalized = standalone_edge_denormalized_columns(mapping, catalog);
    let filters = vec![
        EdgeFilter::IsNotNull(mapping.source.field.clone()),
        EdgeFilter::IsNotNull(mapping.target.field.clone()),
    ];
    Ok(TransformSpec::DataFusion(edge_transformations(
        mapping,
        scope == EtlScope::Namespaced,
        filters,
        denormalized,
        catalog,
    )?))
}

fn fk_edge_transform(
    mapping: &EdgeMapping,
    node: &NodeTransformDeclaration,
    namespaced: bool,
    catalog: &Catalog<ClickHouse>,
) -> Result<Vec<Transformation>, PlanError> {
    let mut filters = Vec::new();
    for endpoint in [&mapping.source, &mapping.target] {
        if endpoint.field == DEFAULT_PRIMARY_KEY {
            continue;
        }
        filters.push(if mapping.array_field.is_some() {
            EdgeFilter::ArrayNotEmpty(endpoint.field.clone())
        } else {
            EdgeFilter::IsNotNull(endpoint.field.clone())
        });
    }
    let denormalized = fk_denormalized_columns(mapping, node, catalog);
    edge_transformations(mapping, namespaced, filters, denormalized, catalog)
}

fn edge_transformations(
    mapping: &EdgeMapping,
    namespaced: bool,
    mut filters: Vec<EdgeFilter>,
    denormalized: Vec<DenormalizedColumnProjection>,
    catalog: &Catalog<ClickHouse>,
) -> Result<Vec<Transformation>, PlanError> {
    let graph = catalog.graph();
    let relationship = graph.relationship(mapping.relationship);
    let relationship_kind = relationship.name.as_str();
    let variant_entities = |source: bool| -> Vec<String> {
        relationship
            .variants
            .iter()
            .map(|variant| {
                let variant = graph.variant(*variant);
                let entity = if source {
                    variant.source
                } else {
                    variant.target
                };
                graph.entity(entity).name.clone()
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    };
    let (source_kind, source_filter) =
        resolve_node_ref_kind(&mapping.source, catalog, || variant_entities(true));
    let (target_kind, target_filter) =
        resolve_node_ref_kind(&mapping.target, catalog, || variant_entities(false));
    filters.extend(source_filter);
    filters.extend(target_filter);

    let source_kind_sql = edge_kind_sql(&source_kind);
    let target_kind_sql = edge_kind_sql(&target_kind);

    let mut variants_by_table: BTreeMap<&str, Vec<(String, String)>> = BTreeMap::new();
    for variant in &mapping.variants {
        let table = catalog.edge_table(*variant).ok_or_else(|| {
            PlanError::Invalid(format!("a '{relationship_kind}' variant has no edge table"))
        })?;
        let variant = graph.variant(*variant);
        variants_by_table.entry(table).or_default().push((
            graph.entity(variant.source).name.clone(),
            graph.entity(variant.target).name.clone(),
        ));
    }
    if variants_by_table.is_empty() {
        return Err(PlanError::Invalid(format!(
            "pipeline mapping materializes no declared '{relationship_kind}' variant"
        )));
    }
    let split_route = variants_by_table.len() > 1;

    variants_by_table
        .into_iter()
        .map(|(table, variants)| {
            let mut filters = filters.clone();
            if split_route {
                filters.push(EdgeFilter::VariantIn {
                    source_kind: source_kind_sql.clone(),
                    target_kind: target_kind_sql.clone(),
                    variants,
                });
            }
            let schema = schema_table(catalog, table);
            Ok(Transformation {
                sql: build_edge_transform_sql(
                    &edge_id_sql(&resolve_edge_id(
                        &mapping.source,
                        mapping.array_field.as_deref(),
                    )),
                    &source_kind_sql,
                    relationship_kind,
                    &edge_id_sql(&resolve_edge_id(
                        &mapping.target,
                        mapping.array_field.as_deref(),
                    )),
                    &target_kind_sql,
                    namespaced,
                    &denormalized,
                    &filters,
                    schema.map_or(&[], |table| table.sort_key.as_slice()),
                ),
                destination_table: prefixed_table_name(table, *SCHEMA_VERSION),
                dict_encode_columns: low_cardinality_columns(schema),
            })
        })
        .collect()
}

fn denormalized_projection(
    catalog: &Catalog<ClickHouse>,
    mapping: &EdgeMapping,
    key: DenormalizedKey,
    source_column: String,
) -> Option<DenormalizedColumnProjection> {
    let (_, _, property) =
        catalog
            .denormalized_properties()
            .find(|(relationship, declared, _)| {
                *relationship == mapping.relationship && *declared == key
            })?;
    Some(DenormalizedColumnProjection {
        source_column,
        edge_column: property.edge_column.clone(),
        tag_key: property.tag_key.clone(),
        enum_mapping: int_enum_values(catalog, key.property),
    })
}

fn standalone_edge_denormalized_columns(
    mapping: &EdgeMapping,
    catalog: &Catalog<ClickHouse>,
) -> Vec<DenormalizedColumnProjection> {
    let mut projections = Vec::new();
    for (endpoint, direction) in [
        (&mapping.source, DenormalizedDirection::Source),
        (&mapping.target, DenormalizedDirection::Target),
    ] {
        if endpoint.kind.entity().is_none() {
            continue;
        }
        for (property, input_field) in &endpoint.properties {
            let key = DenormalizedKey {
                property: *property,
                direction,
            };
            projections.extend(denormalized_projection(
                catalog,
                mapping,
                key,
                input_field.clone(),
            ));
        }
    }
    projections
}

fn fk_denormalized_columns(
    mapping: &EdgeMapping,
    node: &NodeTransformDeclaration,
    catalog: &Catalog<ClickHouse>,
) -> Vec<DenormalizedColumnProjection> {
    let graph = catalog.graph();
    let Some(direction) = node_ref_direction(mapping, node.entity) else {
        return Vec::new();
    };
    catalog
        .denormalized_properties()
        .filter(|(relationship, key, _)| {
            *relationship == mapping.relationship
                && key.direction == direction
                && graph.property(key.property).entity == node.entity
        })
        .map(|(_, key, property)| {
            let source_column = node
                .columns
                .iter()
                .find(|column| column.property == key.property)
                .map_or_else(
                    || graph.property(key.property).name.clone(),
                    |column| column.column.clone(),
                );
            DenormalizedColumnProjection {
                source_column,
                edge_column: property.edge_column.clone(),
                tag_key: property.tag_key.clone(),
                enum_mapping: int_enum_values(catalog, key.property),
            }
        })
        .collect()
}

fn int_enum_values(
    catalog: &Catalog<ClickHouse>,
    property: PropertyId,
) -> Option<BTreeMap<i64, String>> {
    let property = catalog.graph().property(property);
    match property.enum_type {
        EnumType::Int => property.enum_values.clone(),
        EnumType::String => None,
    }
}

fn node_ref_direction(mapping: &EdgeMapping, entity: EntityId) -> Option<DenormalizedDirection> {
    if mapping.source.kind.entity() == Some(entity) {
        Some(DenormalizedDirection::Source)
    } else if mapping.target.kind.entity() == Some(entity) {
        Some(DenormalizedDirection::Target)
    } else {
        None
    }
}

fn resolve_edge_id(endpoint: &Endpoint, array_field: Option<&str>) -> EdgeId {
    if let Some(field) = array_field
        && endpoint.field != DEFAULT_PRIMARY_KEY
    {
        return EdgeId::ArrayElement {
            column: endpoint.field.clone(),
            field: field.to_string(),
        };
    }
    EdgeId::Column(endpoint.field.clone())
}

fn resolve_node_ref_kind(
    endpoint: &Endpoint,
    catalog: &Catalog<ClickHouse>,
    resolve_allowed_types: impl FnOnce() -> Vec<String>,
) -> (EdgeKind, Option<EdgeFilter>) {
    let graph = catalog.graph();
    match &endpoint.kind {
        EndpointKind::Entity(entity) => {
            (EdgeKind::Literal(graph.entity(*entity).name.clone()), None)
        }
        EndpointKind::Column { column, mapping } => {
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
                mapping: mapping
                    .iter()
                    .map(|(raw, entity)| (raw.clone(), graph.entity(*entity).name.clone()))
                    .collect(),
            };
            (kind, filter)
        }
    }
}

fn node_columns(columns: &[SourceColumn], catalog: &Catalog<ClickHouse>) -> Vec<NodeColumn> {
    columns
        .iter()
        .map(|column| {
            let property = catalog.graph().property(column.property);
            let col = column.column.as_str();
            if property.data_type == DataType::Enum
                && property.enum_type == EnumType::Int
                && let Some(values) = &property.enum_values
            {
                return NodeColumn::IntEnum {
                    source: col.to_string(),
                    target: property.name.clone(),
                    values: values.clone(),
                    nullable: property.nullable,
                };
            }
            if col == property.name {
                NodeColumn::Identity(property.name.clone())
            } else {
                NodeColumn::Rename {
                    source: col.to_string(),
                    target: property.name.clone(),
                }
            }
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

fn schema_table<'a>(catalog: &'a Catalog<ClickHouse>, table: &str) -> Option<&'a Table> {
    catalog.schema().ok()?.table(table)
}

fn low_cardinality_columns(table: Option<&Table>) -> HashSet<String> {
    table
        .into_iter()
        .flat_map(|table| &table.columns)
        .filter(|column| column.storage_type.starts_with("LowCardinality"))
        .map(|column| column.name.clone())
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
        EdgeFilter::VariantIn {
            source_kind,
            target_kind,
            variants,
        } => format!(
            "({})",
            variants
                .iter()
                .map(|(source, target)| {
                    format!("({source_kind} = '{source}' AND {target_kind} = '{target}')")
                })
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
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

    fn catalog() -> Catalog<ClickHouse> {
        Catalog::<ClickHouse>::embedded().expect("embedded catalog")
    }

    fn node(catalog: &Catalog<ClickHouse>, name: &str) -> NodeTransformDeclaration {
        let entity = catalog.graph().entity_id(name).expect("entity");
        let pipeline = catalog
            .model()
            .sources()
            .entity_pipelines(entity)
            .next()
            .expect("entity pipeline");
        NodeTransformDeclaration {
            entity,
            columns: pipeline.columns.clone(),
            global: pipeline.scope == EtlScope::Global,
            edges: Vec::new(),
        }
    }

    fn approved(catalog: &Catalog<ClickHouse>) -> EdgeMapping {
        let pipeline = catalog
            .model()
            .sources()
            .pipeline_named("APPROVED_siphon_approvals_MergeRequest")
            .expect("APPROVED pipeline");
        pipeline.transform.edges()[0].clone()
    }

    fn mapping(
        catalog: &Catalog<ClickHouse>,
        label: &str,
        source: (&str, EndpointKind),
        target: (&str, EndpointKind),
        array_field: Option<&str>,
    ) -> EdgeMapping {
        let graph = catalog.graph();
        let relationship = graph.relationship_id(label).expect("relationship");
        let variants = graph
            .relationship(relationship)
            .variants
            .iter()
            .copied()
            .filter(|variant| {
                let variant = graph.variant(*variant);
                source.1.matches(variant.source) && target.1.matches(variant.target)
            })
            .collect();
        let endpoint = |(field, kind): (&str, EndpointKind)| Endpoint {
            field: field.to_string(),
            kind,
            properties: indexmap::IndexMap::new(),
        };
        EdgeMapping {
            relationship,
            source: endpoint(source),
            target: endpoint(target),
            array_field: array_field.map(str::to_string),
            mutable: false,
            variants,
        }
    }

    fn entity(catalog: &Catalog<ClickHouse>, name: &str) -> EndpointKind {
        EndpointKind::Entity(catalog.graph().entity_id(name).expect("entity"))
    }

    #[test]
    fn job_dictionary_encodes_every_low_cardinality_column() {
        let catalog = catalog();
        let columns = low_cardinality_columns(schema_table(&catalog, "gl_job"));
        let mut columns: Vec<&str> = columns.iter().map(String::as_str).collect();
        columns.sort_unstable();
        assert_eq!(
            columns,
            [
                "scheduling_type",
                "status",
                "timeout_source",
                "type",
                "when"
            ]
        );
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
        let catalog = catalog();
        let projections = standalone_edge_denormalized_columns(&approved(&catalog), &catalog);

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

    // DataFusion 54 resolves `<string column> = <int literal>` by coercing the column,
    // so an ordinal comparison against a string enum fails the cast at runtime.
    #[test]
    fn string_enum_denormalized_tag_never_compares_against_int_ordinals() {
        let catalog = catalog();
        let projections = standalone_edge_denormalized_columns(&approved(&catalog), &catalog);
        let user_state = projections
            .iter()
            .find(|projection| projection.source_column == "user_state")
            .expect("User.state denormalized onto APPROVED");

        assert!(
            user_state.enum_mapping.is_none(),
            "User.state is enum_type: string, mapping: {:?}",
            user_state.enum_mapping
        );

        let sql = edge_select_list(
            "id",
            "'User'",
            "APPROVED",
            "merge_request_id",
            "'MergeRequest'",
            true,
            &projections,
        )
        .join(", ");
        assert!(!sql.contains("user_state = 0"), "sql: {sql}");
        assert!(
            sql.contains("concat('state:', CAST(user_state AS VARCHAR))"),
            "sql: {sql}"
        );
    }

    #[test]
    fn fk_edge_transform_outgoing_literal() {
        let catalog = catalog();
        let edge = mapping(
            &catalog,
            "OWNER",
            ("owner_id", entity(&catalog, "User")),
            ("id", entity(&catalog, "Group")),
            None,
        );
        let sql = fk_edge_transform(&edge, &node(&catalog, "Group"), true, &catalog)
            .unwrap()
            .pop()
            .unwrap()
            .sql;

        assert!(sql.contains("owner_id AS source_id"));
        assert!(sql.contains("'User' AS source_kind"));
        assert!(sql.contains("id AS target_id"));
        assert!(sql.contains("'Group' AS target_kind"));
        assert!(sql.contains("(owner_id IS NOT NULL)"));
    }

    #[test]
    fn fk_edge_transform_type_mapping_collapses_raw_values() {
        let catalog = catalog();
        let work_item = catalog.graph().entity_id("WorkItem").unwrap();
        let kind = EndpointKind::Column {
            column: "noteable_type".to_string(),
            mapping: BTreeMap::from([
                ("Issue".to_string(), work_item),
                ("Epic".to_string(), work_item),
            ]),
        };
        let edge = mapping(
            &catalog,
            "HAS_NOTE",
            ("noteable_id", kind),
            ("id", entity(&catalog, "Note")),
            None,
        );

        let sql = fk_edge_transform(&edge, &node(&catalog, "Note"), true, &catalog)
            .unwrap()
            .pop()
            .unwrap()
            .sql;
        assert!(
            sql.contains("WHEN noteable_type = 'Issue' THEN 'WorkItem'"),
            "sql: {sql}"
        );
        assert!(sql.contains("ELSE noteable_type END"), "sql: {sql}");
    }

    #[test]
    fn fk_edge_transform_array_element() {
        let catalog = catalog();
        let edge = mapping(
            &catalog,
            "ASSIGNED",
            ("assignees", entity(&catalog, "User")),
            ("id", entity(&catalog, "MergeRequest")),
            Some("user_id"),
        );
        let sql = fk_edge_transform(&edge, &node(&catalog, "MergeRequest"), true, &catalog)
            .unwrap()
            .pop()
            .unwrap()
            .sql;
        assert!(sql.contains("unnest(assignees)['user_id']"), "sql: {sql}");
        assert!(sql.contains("(cardinality(assignees) > 0)"), "sql: {sql}");
    }
}
