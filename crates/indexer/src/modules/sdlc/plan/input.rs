use ontology::{
    constants::TRAVERSAL_PATH_COLUMN, DataType, EdgeDirection, EdgeEndpointType,
    EdgeSourceEtlConfig, EdgeTarget, EnumType, EtlConfig, EtlScope, NodeEntity, Ontology,
};
use std::collections::BTreeMap;

pub(in crate::modules::sdlc) struct PlanInput {
    pub edge_table: String,
    pub node_plans: Vec<NodePlan>,
    pub standalone_edge_plans: Vec<StandaloneEdgePlan>,
}

pub(in crate::modules::sdlc) struct NodePlan {
    pub name: String,
    pub scope: EtlScope,
    pub columns: Vec<NodeColumn>,
    pub edges: Vec<FkEdgeTransform>,
    pub extract: ExtractPlan,
}

pub(in crate::modules::sdlc) enum NodeColumn {
    Identity(String),
    Rename {
        source: String,
        target: String,
    },
    IntEnum {
        source: String,
        target: String,
        values: BTreeMap<i64, String>,
    },
}

/// An FK edge is derived from the same source data as its parent node.
/// It produces edge rows by reading FK columns from the already-extracted batch.
pub(in crate::modules::sdlc) struct FkEdgeTransform {
    pub relationship_kind: String,
    pub source_id: EdgeId,
    pub source_kind: EdgeKind,
    pub target_id: EdgeId,
    pub target_kind: EdgeKind,
    pub filters: Vec<EdgeFilter>,
    pub namespaced: bool,
}

/// A standalone edge has its own dedicated source table and extraction.
pub(in crate::modules::sdlc) struct StandaloneEdgePlan {
    pub relationship_kind: String,
    pub scope: EtlScope,
    pub source_id: EdgeId,
    pub source_kind: EdgeKind,
    pub target_id: EdgeId,
    pub target_kind: EdgeKind,
    pub filters: Vec<EdgeFilter>,
    pub namespaced: bool,
    pub extract: ExtractPlan,
}

pub(in crate::modules::sdlc) enum EdgeId {
    Column(String),
    Exploded { column: String, delimiter: String },
}

pub(in crate::modules::sdlc) enum EdgeKind {
    Literal(String),
    Column(String),
    TypeMapping {
        column: String,
        mapping: BTreeMap<String, String>,
    },
}

pub(in crate::modules::sdlc) enum EdgeFilter {
    IsNotNull(String),
    NotEmpty(String),
    TypeIn { column: String, types: Vec<String> },
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct ExtractPlan {
    pub destination_table: String,
    pub columns: Vec<ExtractColumn>,
    pub source: ExtractSource,
    pub watermark: String,
    pub deleted: String,
    pub order_by: Vec<String>,
    pub namespaced: bool,
    pub traversal_path_filter: Option<String>,
    pub additional_where: Option<String>,
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) enum ExtractColumn {
    Bare(String),
    ToString(String),
}

impl ExtractColumn {
    fn name(&self) -> &str {
        match self {
            ExtractColumn::Bare(name) | ExtractColumn::ToString(name) => name,
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) enum ExtractSource {
    Table(String),
    Raw(String),
}

pub(in crate::modules::sdlc) fn from_ontology(ontology: &Ontology) -> PlanInput {
    let edge_table = ontology.edge_table().to_string();
    let mut node_plans = Vec::new();
    let mut standalone_edge_plans = Vec::new();

    for node in ontology.nodes() {
        let Some(etl) = &node.etl else { continue };
        node_plans.push(resolve_node(node, etl, ontology));
    }

    for (relationship_kind, config) in ontology.edge_etl_configs() {
        standalone_edge_plans.push(resolve_standalone_edge(
            relationship_kind,
            config,
            ontology,
            &edge_table,
        ));
    }

    PlanInput {
        edge_table: edge_table.to_string(),
        node_plans,
        standalone_edge_plans,
    }
}

fn resolve_node(node: &NodeEntity, etl: &EtlConfig, ontology: &Ontology) -> NodePlan {
    let scope = etl.scope();
    let namespaced = scope == EtlScope::Namespaced;

    let edges = resolve_fk_edges(etl, &node.name, namespaced, ontology);

    let mut node_columns: Vec<ExtractColumn> = node
        .fields
        .iter()
        .map(|field| match &field.data_type {
            DataType::Uuid => ExtractColumn::ToString(field.source.to_string()),
            _ => ExtractColumn::Bare(field.source.to_string()),
        })
        .collect();

    // FK edge transforms read from the same extracted batch, so their columns
    // (FK id, type discriminator, traversal_path) must be in the extract too.
    let extra_fk_columns = collect_fk_extract_columns(etl, namespaced);
    append_missing(&mut node_columns, &extra_fk_columns);

    NodePlan {
        name: node.name.clone(),
        scope,
        columns: resolve_node_columns(&node.fields),
        edges,
        extract: build_extract_plan(etl, node_columns, &node.destination_table),
    }
}

/// Collects all extra column names that FK edge transforms need beyond the
/// node's own fields. This ensures the extract query includes them.
fn collect_fk_extract_columns(etl: &EtlConfig, namespaced: bool) -> Vec<String> {
    let mut columns = vec!["id".to_string()];

    for (fk_column, mapping) in etl.edges() {
        columns.push(fk_column.clone());
        if let EdgeTarget::Column(type_column) = &mapping.target {
            columns.push(type_column.clone());
        }
    }

    if namespaced && !etl.edges().is_empty() {
        columns.push(TRAVERSAL_PATH_COLUMN.to_string());
    }

    columns
}

fn resolve_node_columns(fields: &[ontology::Field]) -> Vec<NodeColumn> {
    fields
        .iter()
        .map(|field| {
            if field.data_type == DataType::Enum
                && field.enum_type == EnumType::Int
                && field.enum_values.is_some()
            {
                return NodeColumn::IntEnum {
                    source: field.source.clone(),
                    target: field.name.clone(),
                    values: field.enum_values.clone().unwrap(),
                };
            }
            if field.source == field.name {
                NodeColumn::Identity(field.name.clone())
            } else {
                NodeColumn::Rename {
                    source: field.source.clone(),
                    target: field.name.clone(),
                }
            }
        })
        .collect()
}

/// Each FK column on a node (e.g. `author_id`, `project_id`) becomes an FK edge
/// transform. These share extraction with their parent node — no separate query.
fn resolve_fk_edges(
    etl: &EtlConfig,
    node_name: &str,
    namespaced: bool,
    ontology: &Ontology,
) -> Vec<FkEdgeTransform> {
    etl.edges()
        .iter()
        .map(|(fk_column, mapping)| {
            let fk_ref = EdgeId::Column(fk_column.clone());
            let node_id = EdgeId::Column("id".to_string());

            let (fk_kind, type_filter) = match &mapping.target {
                EdgeTarget::Literal(target_type) => (EdgeKind::Literal(target_type.clone()), None),
                EdgeTarget::Column(type_column) => {
                    let allowed = ontology.get_edge_target_types(
                        &mapping.relationship_kind,
                        node_name,
                        mapping.direction,
                    );
                    let filter = if allowed.is_empty() {
                        None
                    } else {
                        Some(EdgeFilter::TypeIn {
                            column: type_column.clone(),
                            types: allowed,
                        })
                    };
                    (EdgeKind::Column(type_column.clone()), filter)
                }
            };

            let node_literal = EdgeKind::Literal(node_name.to_string());
            let (mut source_id, source_kind, mut target_id, target_kind) = match mapping.direction {
                EdgeDirection::Outgoing => (node_id, node_literal, fk_ref, fk_kind),
                EdgeDirection::Incoming => (fk_ref, fk_kind, node_id, node_literal),
            };

            let mut filters = Vec::new();
            if let Some(ref delimiter) = mapping.delimiter {
                let exploded_id = EdgeId::Exploded {
                    column: fk_column.clone(),
                    delimiter: delimiter.clone(),
                };
                match mapping.direction {
                    EdgeDirection::Outgoing => target_id = exploded_id,
                    EdgeDirection::Incoming => source_id = exploded_id,
                }
                filters.push(EdgeFilter::IsNotNull(fk_column.clone()));
                filters.push(EdgeFilter::NotEmpty(fk_column.clone()));
            } else {
                filters.push(EdgeFilter::IsNotNull(fk_column.clone()));
                if let Some(tf) = type_filter {
                    filters.push(tf);
                }
            }

            FkEdgeTransform {
                relationship_kind: mapping.relationship_kind.clone(),
                source_id,
                source_kind,
                target_id,
                target_kind,
                filters,
                namespaced,
            }
        })
        .collect()
}

/// Standalone edges have their own dedicated source table (unlike FK edges
/// which piggyback on a node's source). Each endpoint is resolved independently.
fn resolve_standalone_edge(
    relationship_kind: &str,
    config: &EdgeSourceEtlConfig,
    ontology: &Ontology,
    edge_table: &str,
) -> StandaloneEdgePlan {
    let scope = config.scope;
    let namespaced = scope == EtlScope::Namespaced;

    let (source_id, source_kind, source_filter) = resolve_endpoint(&config.from, || {
        ontology.get_edge_source_types(relationship_kind)
    });
    let (target_id, target_kind, _) = resolve_endpoint(&config.to, || {
        ontology.get_edge_all_target_types(relationship_kind)
    });

    let mut filters = vec![
        EdgeFilter::IsNotNull(config.from.id_column.clone()),
        EdgeFilter::IsNotNull(config.to.id_column.clone()),
    ];
    if let Some(f) = source_filter {
        filters.push(f);
    }

    let mut extract_columns = vec![
        ExtractColumn::Bare(config.from.id_column.clone()),
        ExtractColumn::Bare(config.to.id_column.clone()),
    ];
    if let EdgeEndpointType::Column { column, .. } = &config.from.node_type {
        append_missing(&mut extract_columns, std::slice::from_ref(column));
    }
    if let EdgeEndpointType::Column { column, .. } = &config.to.node_type {
        append_missing(&mut extract_columns, std::slice::from_ref(column));
    }
    append_missing(&mut extract_columns, &config.order_by);

    if namespaced {
        append_missing(&mut extract_columns, &[TRAVERSAL_PATH_COLUMN.to_string()]);
    }

    StandaloneEdgePlan {
        relationship_kind: relationship_kind.to_string(),
        scope,
        source_id,
        source_kind,
        target_id,
        target_kind,
        filters,
        namespaced,
        extract: ExtractPlan {
            destination_table: edge_table.to_string(),
            columns: extract_columns,
            source: ExtractSource::Table(config.source.clone()),
            watermark: config.watermark.clone(),
            deleted: config.deleted.clone(),
            order_by: config.order_by.clone(),
            namespaced,
            traversal_path_filter: None,
            additional_where: None,
        },
    }
}

fn resolve_endpoint(
    endpoint: &ontology::EdgeEndpoint,
    resolve_allowed_types: impl FnOnce() -> Vec<String>,
) -> (EdgeId, EdgeKind, Option<EdgeFilter>) {
    let id = EdgeId::Column(endpoint.id_column.clone());

    match &endpoint.node_type {
        EdgeEndpointType::Literal(node_type) => (id, EdgeKind::Literal(node_type.clone()), None),
        EdgeEndpointType::Column {
            column,
            type_mapping,
        } => {
            let allowed = resolve_allowed_types();
            let filter = if allowed.is_empty() {
                None
            } else {
                Some(EdgeFilter::TypeIn {
                    column: column.clone(),
                    types: allowed,
                })
            };
            let kind = if type_mapping.is_empty() {
                EdgeKind::Column(column.clone())
            } else {
                EdgeKind::TypeMapping {
                    column: column.clone(),
                    mapping: type_mapping.clone(),
                }
            };
            (id, kind, filter)
        }
    }
}

fn build_extract_plan(
    etl: &EtlConfig,
    table_columns: Vec<ExtractColumn>,
    destination_table: &str,
) -> ExtractPlan {
    let namespaced = etl.scope() == EtlScope::Namespaced;

    match etl {
        EtlConfig::Table {
            source,
            watermark,
            deleted,
            order_by,
            ..
        } => {
            // The default order_by from schema.yaml includes traversal_path,
            // but global source tables (e.g. siphon_users) don't have that column.
            let order_by = if namespaced {
                order_by.clone()
            } else {
                order_by
                    .iter()
                    .filter(|col| col.as_str() != TRAVERSAL_PATH_COLUMN)
                    .cloned()
                    .collect()
            };

            let mut columns = table_columns;
            append_missing(&mut columns, &order_by);

            ExtractPlan {
                destination_table: destination_table.to_string(),
                columns,
                source: ExtractSource::Table(source.clone()),
                watermark: watermark.clone(),
                deleted: deleted.clone(),
                order_by,
                namespaced,
                traversal_path_filter: None,
                additional_where: None,
            }
        }
        EtlConfig::Query {
            select,
            from,
            where_clause,
            watermark,
            deleted,
            order_by,
            traversal_path_filter,
            ..
        } => {
            let mut columns: Vec<ExtractColumn> = select
                .split(", ")
                .map(|s| ExtractColumn::Bare(s.trim().to_string()))
                .collect();
            append_missing(&mut columns, order_by);

            ExtractPlan {
                destination_table: destination_table.to_string(),
                columns,
                source: ExtractSource::Raw(from.clone()),
                watermark: watermark.clone(),
                deleted: deleted.clone(),
                order_by: order_by.clone(),
                namespaced,
                traversal_path_filter: traversal_path_filter.clone(),
                additional_where: where_clause.clone(),
            }
        }
    }
}

fn append_missing(columns: &mut Vec<ExtractColumn>, names: &[String]) {
    for name in names {
        let already_present = columns.iter().any(|c| {
            let col_name = c.name();
            col_name == name
                || col_name.ends_with(&format!(" AS {name}"))
                || col_name.ends_with(&format!(".{name}"))
        });
        if !already_present {
            columns.push(ExtractColumn::Bare(name.clone()));
        }
    }
}
