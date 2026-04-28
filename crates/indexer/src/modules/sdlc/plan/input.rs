use ontology::{
    DataType, DenormDirection, EdgeDirection, EdgeEndpointType, EdgeSourceEtlConfig, EdgeTarget,
    EnumType, EtlConfig, EtlScope, NodeEntity, Ontology, constants::TRAVERSAL_PATH_COLUMN,
};
use std::collections::{BTreeMap, BTreeSet};

use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

/// A node property to project onto the edge row during transform.
pub(in crate::modules::sdlc) struct DenormalizedColumnProjection {
    /// Column in the source MemTable (e.g. "status").
    pub source_column: String,
    /// Column on the edge table (e.g. "source_status").
    pub edge_column: String,
    /// For int-based enums: integer → string mapping.
    pub enum_mapping: Option<BTreeMap<i64, String>>,
}

pub(in crate::modules::sdlc) struct PlanInput {
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
    /// Resolved edge table for this relationship kind (prefixed).
    pub destination_table: String,
    /// Node properties projected onto the edge row (denormalized).
    pub denormalized_columns: Vec<DenormalizedColumnProjection>,
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
    /// Node properties projected onto the edge row (denormalized).
    /// Standalone edges populate this via LEFT JOIN at extract time.
    pub denormalized_columns: Vec<DenormalizedColumnProjection>,
}

pub(in crate::modules::sdlc) enum EdgeId {
    Column(String),
    Exploded { column: String, delimiter: String },
    ArrayElement { column: String, field: String },
    ArrayUnnest { column: String },
}

pub(in crate::modules::sdlc) enum EdgeKind {
    Literal(String),
    Column {
        column: String,
        mapping: BTreeMap<String, String>,
    },
}

pub(in crate::modules::sdlc) enum EdgeFilter {
    IsNotNull(String),
    NotEmpty(String),
    ArrayNotEmpty(String),
    TypeIn { column: String, types: Vec<String> },
}

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

pub(in crate::modules::sdlc) enum ExtractColumn {
    Bare(String),
    ToString(String),
    /// Postgres `date` is wider than ClickHouse `Date32` (1900-01-01..2299-12-31).
    /// A single out-of-range row would poison the whole Arrow batch, so we clamp
    /// at the SQL projection layer and let NULL propagate.
    DateClamp(String),
}

impl ExtractColumn {
    fn name(&self) -> &str {
        match self {
            ExtractColumn::Bare(name)
            | ExtractColumn::ToString(name)
            | ExtractColumn::DateClamp(name) => name,
        }
    }
}

pub(in crate::modules::sdlc) enum ExtractSource {
    Table(String),
    Raw(String),
}

pub(in crate::modules::sdlc) fn from_ontology(ontology: &Ontology) -> PlanInput {
    let mut node_plans = Vec::new();
    let mut standalone_edge_plans = Vec::new();

    for node in ontology.nodes() {
        let Some(etl) = &node.etl else { continue };
        node_plans.push(resolve_node(node, etl, ontology));
    }

    for (relationship_kind, config) in ontology.edge_etl_configs() {
        standalone_edge_plans.push(resolve_standalone_edge(relationship_kind, config, ontology));
    }

    PlanInput {
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
        .filter_map(|field| {
            let col = field.column_name()?;
            Some(match &field.data_type {
                DataType::Uuid => ExtractColumn::ToString(col.to_string()),
                DataType::Date => ExtractColumn::DateClamp(col.to_string()),
                _ => ExtractColumn::Bare(col.to_string()),
            })
        })
        .collect();

    // FK edge transforms read from the same extracted batch, so their columns
    // (FK id, type discriminator, traversal_path) must be in the extract too.
    let extra_fk_columns = collect_fk_extract_columns(etl, namespaced);
    append_missing(&mut node_columns, &extra_fk_columns);

    let node_destination = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);
    NodePlan {
        name: node.name.clone(),
        scope,
        columns: resolve_node_columns(&node.fields),
        edges,
        extract: build_extract_plan(etl, node_columns, &node_destination),
    }
}

/// Collects all extra column names that FK edge transforms need beyond the
/// node's own fields. This ensures the extract query includes them.
fn collect_fk_extract_columns(etl: &EtlConfig, namespaced: bool) -> BTreeSet<String> {
    let mut columns = BTreeSet::from(["id".to_string()]);

    for (fk_column, mapping) in etl.edge_mappings() {
        columns.insert(fk_column.clone());
        if let EdgeTarget::Column { column, .. } = &mapping.target {
            columns.insert(column.clone());
        }
    }

    if namespaced && etl.has_edges() {
        columns.insert(TRAVERSAL_PATH_COLUMN.to_string());
    }

    columns
}

fn resolve_node_columns(fields: &[ontology::Field]) -> Vec<NodeColumn> {
    fields
        .iter()
        .filter_map(|field| {
            let col = field.column_name()?;
            if field.data_type == DataType::Enum
                && field.enum_type == EnumType::Int
                && field.enum_values.is_some()
            {
                return Some(NodeColumn::IntEnum {
                    source: col.to_string(),
                    target: field.name.clone(),
                    values: field.enum_values.clone().unwrap(),
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

/// Each FK column on a node (e.g. `author_id`, `project_id`) becomes an FK edge
/// transform. These share extraction with their parent node — no separate query.
fn resolve_fk_edges(
    etl: &EtlConfig,
    node_name: &str,
    namespaced: bool,
    ontology: &Ontology,
) -> Vec<FkEdgeTransform> {
    etl.edge_mappings()
        .map(|(fk_column, mapping)| {
            let fk_ref = EdgeId::Column(fk_column.clone());
            let node_id = EdgeId::Column("id".to_string());

            let (fk_kind, type_filter) = match &mapping.target {
                EdgeTarget::Literal(target_type) => (EdgeKind::Literal(target_type.clone()), None),
                EdgeTarget::Column {
                    column: type_column,
                    type_mapping,
                } => {
                    let allowed = ontology.get_edge_target_types(
                        &mapping.relationship_kind,
                        node_name,
                        mapping.direction,
                    );
                    let mut filter_types = allowed;
                    for raw in type_mapping.keys() {
                        if !filter_types.iter().any(|t| t == raw) {
                            filter_types.push(raw.clone());
                        }
                    }
                    let filter = if filter_types.is_empty() {
                        None
                    } else {
                        Some(EdgeFilter::TypeIn {
                            column: type_column.clone(),
                            types: filter_types,
                        })
                    };
                    let kind = EdgeKind::Column {
                        column: type_column.clone(),
                        mapping: type_mapping.clone(),
                    };
                    (kind, filter)
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
            } else if let Some(ref field) = mapping.array_field {
                let array_id = EdgeId::ArrayElement {
                    column: fk_column.clone(),
                    field: field.clone(),
                };
                match mapping.direction {
                    EdgeDirection::Outgoing => target_id = array_id,
                    EdgeDirection::Incoming => source_id = array_id,
                }
                filters.push(EdgeFilter::ArrayNotEmpty(fk_column.clone()));
            } else if mapping.array {
                let array_id = EdgeId::ArrayUnnest {
                    column: fk_column.clone(),
                };
                match mapping.direction {
                    EdgeDirection::Outgoing => target_id = array_id,
                    EdgeDirection::Incoming => source_id = array_id,
                }
                filters.push(EdgeFilter::ArrayNotEmpty(fk_column.clone()));
            } else {
                filters.push(EdgeFilter::IsNotNull(fk_column.clone()));
                if let Some(tf) = type_filter {
                    filters.push(tf);
                }
            }

            let edge_dest = prefixed_table_name(
                ontology.edge_table_for_relationship(&mapping.relationship_kind),
                *SCHEMA_VERSION,
            );

            let denormalized_columns = ontology
                .denormalized_properties()
                .iter()
                .filter(|dp| {
                    dp.relationship_kind == mapping.relationship_kind
                        && dp.node_kind == node_name
                        && matches!(
                            (&dp.direction, &mapping.direction),
                            (DenormDirection::Source, EdgeDirection::Outgoing)
                                | (DenormDirection::Target, EdgeDirection::Incoming)
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
                        enum_mapping: dp.enum_values.clone(),
                    }
                })
                .collect();

            FkEdgeTransform {
                relationship_kind: mapping.relationship_kind.clone(),
                source_id,
                source_kind,
                target_id,
                target_kind,
                filters,
                namespaced,
                destination_table: edge_dest,
                denormalized_columns,
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
) -> StandaloneEdgePlan {
    let edge_table = prefixed_table_name(
        ontology.edge_table_for_relationship(relationship_kind),
        *SCHEMA_VERSION,
    );
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
        append_missing(&mut extract_columns, std::iter::once(column));
    }
    if let EdgeEndpointType::Column { column, .. } = &config.to.node_type {
        append_missing(&mut extract_columns, std::iter::once(column));
    }
    append_missing(&mut extract_columns, &config.order_by);

    if namespaced {
        append_missing(
            &mut extract_columns,
            std::iter::once(&TRAVERSAL_PATH_COLUMN.to_string()),
        );
    }

    // Build denormalized column projections and LEFT JOINs for node properties.
    let endpoints: [(&str, &str, DenormDirection); 2] = [
        (&config.from.id_column, "source", DenormDirection::Source),
        (&config.to.id_column, "target", DenormDirection::Target),
    ];
    let mut denormalized_columns = Vec::new();
    let mut joins: Vec<String> = Vec::new();
    let mut join_idx = 0usize;

    for (fk_col, _dir_label, direction) in &endpoints {
        let props: Vec<_> = ontology
            .denormalized_properties()
            .iter()
            .filter(|dp| dp.relationship_kind == relationship_kind && dp.direction == *direction)
            .collect();
        if props.is_empty() {
            continue;
        }
        // Resolve the node's datalake source table.
        let node_kind = &props[0].node_kind;
        let Some(node) = ontology.get_node(node_kind) else {
            continue;
        };
        let Some(etl) = &node.etl else { continue };
        let node_table = match etl {
            EtlConfig::Table { source, .. } => source.as_str(),
            EtlConfig::Query { from, .. } => from.as_str(),
        };

        let alias = format!("_d{join_idx}");
        join_idx += 1;
        joins.push(format!(
            "LEFT JOIN {node_table} AS {alias} ON _base.{fk_col} = {alias}.id"
        ));

        for dp in props {
            let field = node.fields.iter().find(|f| f.name == dp.property_name);
            let src_col = field
                .and_then(|f| f.column_name())
                .unwrap_or(&dp.property_name);
            let qualified = format!("{alias}.{src_col}");
            extract_columns.push(ExtractColumn::Bare(qualified));
            denormalized_columns.push(DenormalizedColumnProjection {
                source_column: format!("{alias}.{src_col}"),
                edge_column: dp.edge_column.clone(),
                enum_mapping: dp.enum_values.clone(),
            });
        }
    }

    let source = if joins.is_empty() {
        ExtractSource::Table(config.source.clone())
    } else {
        ExtractSource::Raw(format!("{} AS _base {}", config.source, joins.join(" ")))
    };

    StandaloneEdgePlan {
        relationship_kind: relationship_kind.to_string(),
        scope,
        source_id,
        source_kind,
        target_id,
        target_kind,
        filters,
        namespaced,
        denormalized_columns,
        extract: ExtractPlan {
            destination_table: edge_table,
            columns: extract_columns,
            source,
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
            // The TypeIn filter runs in the source table before the CASE in
            // `lower_edge_kind` rewrites raw Rails values to ontology names.
            // Include mapping source values so polymorphic rows survive.
            let mut filter_types = resolve_allowed_types();
            for raw in type_mapping.keys() {
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
                mapping: type_mapping.clone(),
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

fn append_missing<'a, I>(columns: &mut Vec<ExtractColumn>, names: I)
where
    I: IntoIterator<Item = &'a String>,
{
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

#[cfg(test)]
mod tests {
    use super::*;

    fn find_column<'a>(plan: &'a NodePlan, name: &str) -> Option<&'a ExtractColumn> {
        plan.extract.columns.iter().find(|c| c.name() == name)
    }

    #[test]
    fn from_ontology_emits_date_clamp_for_milestone_and_workitem() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = from_ontology(&ontology);

        let work_item = plans
            .node_plans
            .iter()
            .find(|p| p.name == "WorkItem")
            .expect("WorkItem plan should exist");
        let due = find_column(work_item, "due_date").expect("WorkItem due_date column");
        assert!(
            matches!(due, ExtractColumn::DateClamp(_)),
            "WorkItem.due_date must be DateClamp, got different variant"
        );
        let start = find_column(work_item, "start_date").expect("WorkItem start_date column");
        assert!(
            matches!(start, ExtractColumn::DateClamp(_)),
            "WorkItem.start_date must be DateClamp"
        );

        let milestone = plans
            .node_plans
            .iter()
            .find(|p| p.name == "Milestone")
            .expect("Milestone plan should exist");
        let due = find_column(milestone, "due_date").expect("Milestone due_date column");
        assert!(
            matches!(due, ExtractColumn::DateClamp(_)),
            "Milestone.due_date must be DateClamp"
        );
        let start = find_column(milestone, "start_date").expect("Milestone start_date column");
        assert!(
            matches!(start, ExtractColumn::DateClamp(_)),
            "Milestone.start_date must be DateClamp"
        );
    }

    #[test]
    fn multi_emit_fk_edges_share_parent_extract() {
        use ontology::{EdgeDirection, EdgeMapping, EdgeTarget, EtlConfig, EtlScope};
        use std::collections::BTreeMap;

        let mut edges_map: BTreeMap<String, Vec<EdgeMapping>> = BTreeMap::new();
        edges_map.insert(
            "commit_id".to_string(),
            vec![
                EdgeMapping {
                    target: EdgeTarget::Literal("Pipeline".to_string()),
                    relationship_kind: "IN_PIPELINE".to_string(),
                    direction: EdgeDirection::Outgoing,
                    delimiter: None,
                    array_field: None,
                    array: false,
                },
                EdgeMapping {
                    target: EdgeTarget::Literal("Pipeline".to_string()),
                    relationship_kind: "HAS_JOB".to_string(),
                    direction: EdgeDirection::Incoming,
                    delimiter: None,
                    array_field: None,
                    array: false,
                },
            ],
        );

        let etl = EtlConfig::Table {
            scope: EtlScope::Namespaced,
            source: "siphon_p_ci_builds".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
            edges: edges_map,
        };

        let ontology = Ontology::load_embedded().expect("ontology");
        let fk_edges = resolve_fk_edges(&etl, "Job", true, &ontology);

        assert_eq!(
            fk_edges.len(),
            2,
            "two emissions on the same column should produce two FkEdgeTransforms",
        );

        let kinds: Vec<&str> = fk_edges
            .iter()
            .map(|e| e.relationship_kind.as_str())
            .collect();
        assert!(kinds.contains(&"IN_PIPELINE"));
        assert!(kinds.contains(&"HAS_JOB"));

        let in_pipeline = fk_edges
            .iter()
            .find(|e| e.relationship_kind == "IN_PIPELINE")
            .unwrap();
        let has_job = fk_edges
            .iter()
            .find(|e| e.relationship_kind == "HAS_JOB")
            .unwrap();

        assert!(
            matches!(&in_pipeline.source_id, EdgeId::Column(c) if c == "id"),
            "outgoing source_id should be Job.id"
        );
        assert!(
            matches!(&in_pipeline.target_id, EdgeId::Column(c) if c == "commit_id"),
            "outgoing target_id should be commit_id"
        );
        assert!(
            matches!(&has_job.source_id, EdgeId::Column(c) if c == "commit_id"),
            "incoming source_id should be commit_id"
        );
        assert!(
            matches!(&has_job.target_id, EdgeId::Column(c) if c == "id"),
            "incoming target_id should be Job.id"
        );
    }

    #[test]
    fn embedded_ontology_single_emit_fks_still_produce_one_transform_each() {
        let ontology = Ontology::load_embedded().expect("ontology");
        let plans = from_ontology(&ontology);

        for node_plan in &plans.node_plans {
            let node_def = ontology
                .get_node(&node_plan.name)
                .expect("node defined in ontology");
            let Some(etl) = &node_def.etl else { continue };
            let expected_edge_count: usize = etl.edge_mappings().count();
            assert_eq!(
                node_plan.edges.len(),
                expected_edge_count,
                "node {}: expected {} FK edge transforms, got {}",
                node_plan.name,
                expected_edge_count,
                node_plan.edges.len(),
            );
        }
    }

    #[test]
    fn standalone_edge_type_mapping_keys_survive_filter() {
        let ontology = Ontology::load_embedded().expect("ontology");
        let plans = from_ontology(&ontology);

        let has_label = plans
            .standalone_edge_plans
            .iter()
            .find(|p| p.relationship_kind == "HAS_LABEL")
            .expect("HAS_LABEL standalone plan");

        let type_filter = has_label
            .filters
            .iter()
            .find_map(|f| match f {
                EdgeFilter::TypeIn { column, types } if column == "target_type" => Some(types),
                _ => None,
            })
            .expect("HAS_LABEL should have a target_type TypeIn filter");

        assert!(
            type_filter.iter().any(|t| t == "Issue"),
            "filter must include the raw Rails value that maps to WorkItem; got {type_filter:?}"
        );
        assert!(
            type_filter.iter().any(|t| t == "MergeRequest"),
            "filter must keep the ontology-native MergeRequest value; got {type_filter:?}"
        );
    }

    #[test]
    fn multi_emit_fk_does_not_duplicate_extract_columns() {
        use ontology::{EdgeDirection, EdgeMapping, EdgeTarget, EtlConfig, EtlScope};
        use std::collections::BTreeMap;

        let mut edges_map: BTreeMap<String, Vec<EdgeMapping>> = BTreeMap::new();
        edges_map.insert(
            "commit_id".to_string(),
            vec![
                EdgeMapping {
                    target: EdgeTarget::Literal("Pipeline".to_string()),
                    relationship_kind: "IN_PIPELINE".to_string(),
                    direction: EdgeDirection::Outgoing,
                    delimiter: None,
                    array_field: None,
                    array: false,
                },
                EdgeMapping {
                    target: EdgeTarget::Literal("Pipeline".to_string()),
                    relationship_kind: "HAS_JOB".to_string(),
                    direction: EdgeDirection::Incoming,
                    delimiter: None,
                    array_field: None,
                    array: false,
                },
            ],
        );

        let etl = EtlConfig::Table {
            scope: EtlScope::Namespaced,
            source: "siphon_p_ci_builds".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            edges: edges_map,
        };

        let columns = collect_fk_extract_columns(&etl, true);
        let commit_id_count = columns.iter().filter(|c| c.as_str() == "commit_id").count();
        assert_eq!(
            commit_id_count, 1,
            "commit_id should appear exactly once even with two emissions: {columns:?}"
        );
    }
}
