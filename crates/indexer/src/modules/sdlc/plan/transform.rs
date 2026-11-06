//! Turns a node's fields (with FK edges) or an edge's mapping into a `TransformSpec`.

use std::collections::{BTreeMap, HashSet};

use indexmap::IndexMap;

use ontology::{
    DataType, DenormDirection, EdgeMapping, EnumType, EtlScope, Field, NodeEntity, NodeRef,
    NodeRefKind, Ontology, StorageColumn,
    constants::{DEFAULT_PRIMARY_KEY, DELETED_COLUMN, VERSION_COLUMN},
};

use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_utils::strings::quote_ident;

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

struct EdgeSelect {
    table: String,
    columns: Vec<String>,
    tags: [String; 2],
    filter: Option<String>,
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

pub(super) fn compile_transform_spec(
    transform_declaration: TransformDeclaration,
    ontology: &Ontology,
) -> Result<TransformSpec, ontology::OntologyError> {
    match transform_declaration {
        TransformDeclaration::Node(node_transform) => {
            build_node_transform(&node_transform, ontology)
        }
        TransformDeclaration::Edge {
            relationship_kind,
            mapping,
            scope,
        } => edge_transform(&relationship_kind, &mapping, scope, ontology),
        TransformDeclaration::Rust(name) => Ok(TransformSpec::Rust(name)),
    }
}

fn build_node_transform(
    transform_declaration: &NodeTransformDeclaration,
    ontology: &Ontology,
) -> Result<TransformSpec, ontology::OntologyError> {
    let namespaced = !transform_declaration.global;
    let node_destination =
        prefixed_table_name(&transform_declaration.destination_table, *SCHEMA_VERSION);
    let mut transforms = vec![Transformation {
        sql: node_transform_sql(&node_columns(&transform_declaration.fields)),
        destination_table: node_destination,
        dict_encode_columns: low_cardinality_columns(&transform_declaration.storage_columns),
    }];

    let mut selects = Vec::new();
    for mapping in &transform_declaration.edges {
        selects.extend(fk_edge_transform(
            mapping,
            &transform_declaration.node_name,
            namespaced,
            ontology,
        )?);
    }
    transforms.extend(edge_queries(selects, ontology));

    Ok(TransformSpec::DataFusion(transforms))
}

fn edge_transform(
    relationship_kind: &str,
    mapping: &EdgeMapping,
    scope: EtlScope,
    ontology: &Ontology,
) -> Result<TransformSpec, ontology::OntologyError> {
    let denormalized = standalone_edge_denormalized_columns(relationship_kind, mapping, ontology);
    let filters = vec![
        EdgeFilter::IsNotNull(mapping.source.field.clone()),
        EdgeFilter::IsNotNull(mapping.target.field.clone()),
    ];
    let selects = edge_transformations(
        relationship_kind,
        mapping,
        scope == EtlScope::Namespaced,
        filters,
        denormalized,
        ontology,
    )?;
    Ok(TransformSpec::DataFusion(edge_queries(selects, ontology)))
}

fn fk_edge_transform(
    mapping: &EdgeMapping,
    node_name: &str,
    namespaced: bool,
    ontology: &Ontology,
) -> Result<Vec<EdgeSelect>, ontology::OntologyError> {
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
    edge_transformations(
        &mapping.label,
        mapping,
        namespaced,
        filters,
        denormalized,
        ontology,
    )
}

fn edge_transformations(
    relationship_kind: &str,
    mapping: &EdgeMapping,
    namespaced: bool,
    mut filters: Vec<EdgeFilter>,
    denormalized: Vec<DenormalizedColumnProjection>,
    ontology: &Ontology,
) -> Result<Vec<EdgeSelect>, ontology::OntologyError> {
    let (source_kind, source_filter) = resolve_node_ref_kind(&mapping.source, || {
        ontology.get_edge_source_types(relationship_kind)
    });
    let (target_kind, target_filter) = resolve_node_ref_kind(&mapping.target, || {
        ontology.get_edge_all_target_types(relationship_kind)
    });
    filters.extend(source_filter);
    filters.extend(target_filter);

    let source_kind_sql = edge_kind_sql(&source_kind);
    let target_kind_sql = edge_kind_sql(&target_kind);
    let routes = ontology.edge_routes_for_mapping(mapping);
    if routes.is_empty() {
        return Err(ontology::OntologyError::Validation(format!(
            "pipeline mapping materializes no declared '{relationship_kind}' variant"
        )));
    }

    let mut variants_by_table: BTreeMap<&str, Vec<(String, String)>> = BTreeMap::new();
    for route in &routes {
        variants_by_table
            .entry(&route.destination_table)
            .or_default()
            .push((route.source_kind.clone(), route.target_kind.clone()));
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
            Ok(EdgeSelect {
                table: table.to_string(),
                columns: edge_columns(
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
                ),
                tags: edge_tags(&denormalized),
                filter: edge_filters_sql(&filters),
            })
        })
        .collect()
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
    select_list.push(quote_ident(VERSION_COLUMN));
    select_list.push(quote_ident(DELETED_COLUMN));
    format!("SELECT {} FROM {SOURCE_DATA_TABLE}", select_list.join(", "))
}

fn node_column_sql(column: &NodeColumn) -> String {
    match column {
        NodeColumn::Identity(name) => quote_ident(name),
        NodeColumn::Rename { source, target } => {
            format!("{} AS {}", quote_ident(source), quote_ident(target))
        }
        NodeColumn::IntEnum {
            source,
            target,
            values,
            nullable,
        } => {
            let (source, target) = (quote_ident(source), quote_ident(target));
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

fn edge_table_metadata(table: &str, ontology: &Ontology) -> EdgeTableMetadata {
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
        EdgeId::Column(column) => quote_ident(column),
        EdgeId::ArrayElement { column, field } => {
            format!("unnest({})['{field}']", quote_ident(column))
        }
    }
}

fn edge_kind_sql(kind: &EdgeKind) -> String {
    match kind {
        EdgeKind::Literal(value) => format!("'{value}'"),
        EdgeKind::Column { column, mapping } if mapping.is_empty() => quote_ident(column),
        EdgeKind::Column { column, mapping } => {
            let column = quote_ident(column);
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
        EdgeFilter::IsNotNull(column) => format!("({} IS NOT NULL)", quote_ident(column)),
        EdgeFilter::ArrayNotEmpty(column) => format!("(cardinality({}) > 0)", quote_ident(column)),
        EdgeFilter::TypeIn { column, types } => {
            let types_list = types
                .iter()
                .map(|t| format!("'{t}'"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{} IN ({types_list})", quote_ident(column))
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

const NO_TAGS: &str = "CAST([] AS VARCHAR[])";
const TAGGED_TABLE: &str = "_tagged";

fn edge_queries(selects: Vec<EdgeSelect>, ontology: &Ontology) -> Vec<Transformation> {
    let mut by_table: Vec<(String, Vec<EdgeSelect>)> = Vec::new();
    for select in selects {
        match by_table
            .iter_mut()
            .find(|(table, _)| *table == select.table)
        {
            Some((_, group)) => group.push(select),
            None => by_table.push((select.table.clone(), vec![select])),
        }
    }
    by_table
        .into_iter()
        .map(|(table, selects)| {
            let meta = edge_table_metadata(&table, ontology);
            Transformation {
                sql: edge_union_sql(&selects, &meta.sort_key),
                destination_table: prefixed_table_name(&table, *SCHEMA_VERSION),
                dict_encode_columns: meta.dict_columns,
            }
        })
        .collect()
}

fn edge_union_sql(selects: &[EdgeSelect], sort_key: &[String]) -> String {
    let mut uses: IndexMap<&str, usize> = IndexMap::new();
    for tags in selects.iter().flat_map(|select| &select.tags) {
        if tags != NO_TAGS {
            *uses.entry(tags.as_str()).or_default() += 1;
        }
    }
    let shared: Vec<&str> = uses
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(tags, _)| tags)
        .collect();
    let shared_column = |index: usize| quote_ident(&format!("_tags_{index}"));
    let tag_sql = |tags: &String| {
        shared
            .iter()
            .position(|candidate| candidate == tags)
            .map_or_else(|| tags.clone(), shared_column)
    };
    let from = match shared.is_empty() {
        true => SOURCE_DATA_TABLE.to_string(),
        false => quote_ident(TAGGED_TABLE),
    };

    let mut sql = selects
        .iter()
        .map(|select| {
            let [source_tags, target_tags] = &select.tags;
            let filter = select
                .filter
                .as_ref()
                .map(|filter| format!(" WHERE {filter}"));
            format!(
                "SELECT {}, {} AS source_tags, {} AS target_tags FROM {from}{}",
                select.columns.join(", "),
                tag_sql(source_tags),
                tag_sql(target_tags),
                filter.unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    if !shared.is_empty() {
        let columns: Vec<String> = shared
            .iter()
            .enumerate()
            .map(|(index, tags)| format!("{tags} AS {}", shared_column(index)))
            .collect();
        let cte = format!("SELECT *, {} FROM {SOURCE_DATA_TABLE}", columns.join(", "));
        sql = format!(
            "WITH {} AS MATERIALIZED ({cte}) {sql}",
            quote_ident(TAGGED_TABLE)
        );
    }
    if !sort_key.is_empty() {
        let keys: Vec<String> = sort_key
            .iter()
            .map(String::as_str)
            .map(quote_ident)
            .collect();
        sql = format!("{sql} ORDER BY {}", keys.join(", "));
    }
    sql
}

fn edge_columns(
    source_id: &str,
    source_kind: &str,
    relationship_kind: &str,
    target_id: &str,
    target_kind: &str,
    namespaced: bool,
) -> Vec<String> {
    let traversal_path = if namespaced {
        quote_ident("traversal_path")
    } else {
        "'0/' AS traversal_path".to_string()
    };
    vec![
        traversal_path,
        format!("CAST({source_id} AS BIGINT) AS source_id"),
        format!("{source_kind} AS source_kind"),
        format!("'{relationship_kind}' AS relationship_kind"),
        format!("CAST({target_id} AS BIGINT) AS target_id"),
        format!("{target_kind} AS target_kind"),
        quote_ident(VERSION_COLUMN),
        quote_ident(DELETED_COLUMN),
    ]
}

/// Both tag lists are always emitted, empty when unused, so the Arrow schema matches the edge table.
fn edge_tags(denormalized: &[DenormalizedColumnProjection]) -> [String; 2] {
    ["source_tags", "target_tags"].map(|edge_column| {
        let tags: Vec<String> = denormalized
            .iter()
            .filter(|d| d.edge_column == edge_column)
            .map(tag_sql)
            .collect();
        if tags.is_empty() {
            NO_TAGS.to_string()
        } else {
            format!("CAST([{}] AS VARCHAR[])", tags.join(", "))
        }
    })
}

fn tag_sql(d: &DenormalizedColumnProjection) -> String {
    let column = quote_ident(&d.source_column);
    let value = format!("CAST({column} AS VARCHAR)");
    let value = match &d.enum_mapping {
        Some(mapping) if !mapping.is_empty() => {
            let entries: Vec<String> = mapping
                .iter()
                .map(|(key, name)| format!("{key}: '{}'", name.replace('\'', "''")))
                .collect();
            format!("MAP {{{}}}[{column}], {value}", entries.join(", "))
        }
        _ => value,
    };
    format!("'{}:' || coalesce({value}, 'null')", d.tag_key)
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
        assert!(sql.contains(r#""admin" AS "is_admin""#), "sql: {sql}");
        assert!(
            sql.contains(r#"WHEN "state" = 0 THEN 'active'"#),
            "sql: {sql}"
        );
        assert!(
            sql.contains(r#"ELSE 'unknown' END AS "state""#),
            "sql: {sql}"
        );
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
        assert!(sql.contains(r#"WHEN "failure_reason" IS NULL THEN NULL"#));
        assert!(sql.contains(r#"WHEN "failure_reason" = 1 THEN 'script_failure'"#));
        assert!(sql.contains(r#"ELSE 'unknown' END AS "failure_reason""#));
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

    // DuckDB resolves `<string column> = <int literal>` by casting the column,
    // so an ordinal comparison against a string enum fails the cast at runtime.
    #[test]
    fn string_enum_denormalized_tag_never_compares_against_int_ordinals() {
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
        let user_state = projections
            .iter()
            .find(|projection| projection.source_column == "user_state")
            .expect("User.state denormalized onto APPROVED");

        assert!(
            user_state.enum_mapping.is_none(),
            "User.state is enum_type: string, mapping: {:?}",
            user_state.enum_mapping
        );

        let sql = edge_tags(&projections).join(", ");
        assert!(!sql.contains(r#""user_state" = 0"#), "sql: {sql}");
        assert!(
            sql.contains(r#"'state:' || coalesce(CAST("user_state" AS VARCHAR), 'null')"#),
            "sql: {sql}"
        );
    }

    #[test]
    fn fk_edge_transform_outgoing_literal() {
        let mapping = literal_mapping("owner_id", "User", "id", "Group", "OWNER");
        let sql = fk_edge_transform(&mapping, "Group", true, &test_ontology())
            .unwrap()
            .pop()
            .map(|select| edge_union_sql(&[select], &[]))
            .unwrap();

        assert!(sql.contains(r#"CAST("owner_id" AS BIGINT) AS source_id"#));
        assert!(sql.contains("'User' AS source_kind"));
        assert!(sql.contains(r#"CAST("id" AS BIGINT) AS target_id"#));
        assert!(sql.contains("'Group' AS target_kind"));
        assert!(sql.contains(r#"("owner_id" IS NOT NULL)"#));
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

        let sql = fk_edge_transform(&edge, "Note", true, &test_ontology())
            .unwrap()
            .pop()
            .map(|select| edge_union_sql(&[select], &[]))
            .unwrap();
        assert!(
            sql.contains(r#"WHEN "noteable_type" = 'Issue' THEN 'WorkItem'"#),
            "sql: {sql}"
        );
        assert!(sql.contains(r#"ELSE "noteable_type" END"#), "sql: {sql}");
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
            label: "ASSIGNED".to_string(),
            array_field: Some("user_id".to_string()),
            mutable: false,
        };
        let sql = fk_edge_transform(&edge, "MergeRequest", true, &test_ontology())
            .unwrap()
            .pop()
            .map(|select| edge_union_sql(&[select], &[]))
            .unwrap();
        assert!(
            sql.contains(r#"unnest("assignees")['user_id']"#),
            "sql: {sql}"
        );
        assert!(
            sql.contains(r#"(cardinality("assignees") > 0)"#),
            "sql: {sql}"
        );
    }

    fn select(table: &str, source_id: &str, tags: &str) -> EdgeSelect {
        EdgeSelect {
            table: table.to_string(),
            columns: vec![format!("{source_id} AS source_id")],
            tags: [tags.to_string(), NO_TAGS.to_string()],
            filter: Some(format!("({source_id} IS NOT NULL)")),
        }
    }

    #[test]
    fn edge_queries_combine_each_table_and_share_repeated_tags() {
        let tags = "CAST(['k:' || coalesce(CAST(\"k\" AS VARCHAR), 'null')] AS VARCHAR[])";
        let queries = edge_queries(
            vec![
                select("gl_edge", "\"a\"", tags),
                select("gl_ci_edge", "\"b\"", NO_TAGS),
                select("gl_edge", "\"c\"", tags),
            ],
            &test_ontology(),
        );

        let tables: Vec<_> = queries
            .iter()
            .map(|q| q.destination_table.as_str())
            .collect();
        assert_eq!(
            tables,
            [
                prefixed_table_name("gl_edge", *SCHEMA_VERSION),
                prefixed_table_name("gl_ci_edge", *SCHEMA_VERSION),
            ]
        );
        let combined = &queries[0].sql;
        assert_eq!(combined.matches(" UNION ALL ").count(), 1, "{combined}");
        assert!(
            combined.starts_with(&format!(
                r#"WITH "_tagged" AS MATERIALIZED (SELECT *, {tags} AS "_tags_0" FROM source_data)"#
            )),
            "{combined}"
        );
        assert_eq!(
            combined.matches(r#""_tags_0" AS source_tags"#).count(),
            2,
            "{combined}"
        );
        assert_eq!(combined.matches(" ORDER BY ").count(), 1, "{combined}");
        assert!(!queries[1].sql.contains("_tagged"), "{}", queries[1].sql);
    }

    #[test]
    fn enum_tag_maps_ordinals_and_falls_back_to_the_raw_value() {
        let projection = DenormalizedColumnProjection {
            source_column: "state_id".to_string(),
            edge_column: "target_tags".to_string(),
            tag_key: "state".to_string(),
            enum_mapping: Some(BTreeMap::from([
                (1, "opened".to_string()),
                (2, "it's".to_string()),
            ])),
        };
        assert_eq!(
            tag_sql(&projection),
            r#"'state:' || coalesce(MAP {1: 'opened', 2: 'it''s'}["state_id"], CAST("state_id" AS VARCHAR), 'null')"#
        );
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
