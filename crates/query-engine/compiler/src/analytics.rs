use crate::passes::codegen::RelationalModel;
use std::collections::BTreeSet;
use std::time::Duration;

use query_data_model::Direction;
use query_data_model::plan::logical::{EdgeField, Expr, LogicalOperation, Measure};
use serde::Serialize;

use crate::logical::columns::{returned, selections};
use crate::logical::facts::{Fact, anchors, facts};
use crate::logical::{DynamicColumnMode, Kind, Query};
use crate::passes::hydrate::HydrationPlan;
use crate::types::QueryType;

/// `query_executed` accumulates across multiple ClickHouse round-trips; all
/// other fields are written directly by observers.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecMetrics {
    #[serde(skip)]
    pub stats: Option<QueryStats>,
    #[serde(skip)]
    pub hydration: Option<HydrationPlan>,
    #[serde(skip)]
    pub has_virtual_columns: bool,
    pub compile_ms: Option<u64>,
    pub execute_ms: Option<u64>,
    pub authorization_ms: Option<u64>,
    pub hydration_ms: Option<u64>,
    pub ch_read_rows: u64,
    pub ch_read_bytes: u64,
    pub ch_memory_usage: u64,
}

impl ExecMetrics {
    pub fn ms(d: Duration) -> u64 {
        d.as_millis().min(u64::MAX as u128) as u64
    }

    pub fn query_executed(&mut self, read_rows: u64, read_bytes: u64, memory: i64) {
        self.ch_read_rows += read_rows;
        self.ch_read_bytes += read_bytes;
        if memory > 0 {
            self.ch_memory_usage = self.ch_memory_usage.max(memory as u64);
        }
    }
}

/// What a compiled query asks for, as analytics and the test assertions count it.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryStats {
    pub query_type: QueryType,
    pub node_count: usize,
    pub relationship_count: usize,
    pub is_search: bool,
    pub has_order_by: bool,
    pub has_output_sort: bool,
    pub limit: u32,
    pub skip: Option<u32>,
    pub group_by_count: usize,
    pub dynamic_columns: DynamicColumnMode,
    pub path_max_depth: Option<u32>,
    pub pinned: bool,
    pub entity_types: BTreeSet<String>,
    pub relationship_types: BTreeSet<String>,
    /// Filtered properties of nodes; `filter_fields` also counts relationship fields.
    pub node_filter_fields: BTreeSet<String>,
    pub filter_fields: BTreeSet<String>,
    pub filter_ops: BTreeSet<String>,
    pub filter_count: usize,
    pub max_hops: u32,
    pub variable_hops: bool,
    pub requested_columns: BTreeSet<String>,
    pub agg_functions: BTreeSet<String>,
    /// A topology fingerprint like `User-[AUTHORED]->MergeRequest`; none without relationships.
    pub traversal_shape: Option<String>,
}

impl QueryStats {
    pub fn new(query: Query<'_>, model: &impl RelationalModel) -> Self {
        let graph = model.catalog().graph();
        let kind = query.kind();
        let steps = query.steps();
        let node_count = query
            .operations()
            .filter(|(_, operation)| {
                matches!(
                    operation,
                    LogicalOperation::Entities(_) | LogicalOperation::Follow { .. }
                )
            })
            .count();
        let facts = facts(query);
        let anchors = anchors(query, &facts);
        let selected = selections(query);
        let field = |fact: &Fact<'_>| match fact.left {
            Expr::Property { property, .. } => Some(query.property_name(property).to_string()),
            Expr::Attribute { attribute, .. } => Some(graph.attribute(attribute).name.clone()),
            Expr::Edge { field, .. } => match field {
                EdgeField::Kind => None,
                EdgeField::Source => Some("source_id".into()),
                EdgeField::SourceKind => Some("source_kind".into()),
                EdgeField::Target => Some("target_id".into()),
                EdgeField::TargetKind => Some("target_kind".into()),
            },
            _ => None,
        };
        let node_filters = anchors.iter().flat_map(|anchor| anchor.filters.iter());
        let edge_filters = facts
            .iter()
            .filter(|fact| fact.attached && steps.iter().any(|step| step.edge == fact.binding()));
        let filters: Vec<(String, &Fact<'_>)> = node_filters
            .chain(edge_filters)
            .filter_map(|fact| Some((field(fact)?, fact)))
            .collect();
        let names = |relationships: &[query_data_model::RelationshipId]| -> Vec<String> {
            match relationships {
                [] => vec!["*".to_string()],
                named => named
                    .iter()
                    .map(|id| graph.relationship(*id).name.clone())
                    .collect(),
            }
        };
        let sort_keys = query.sort_keys();
        let (groups, measures) = query.aggregate().unwrap_or_default();
        let entity_of = |output| query.entity_name(output).unwrap_or("?");
        let traversal_shape = (!steps.is_empty()).then(|| {
            steps
                .iter()
                .map(|step| {
                    let (from, to) = match step.direction {
                        Direction::Incoming => (step.to, step.from),
                        _ => (step.from, step.to),
                    };
                    let types = names(step.relationships).join("|");
                    format!("{}-[{types}]->{}", entity_of(from), entity_of(to))
                })
                .collect::<Vec<_>>()
                .join(", ")
        });
        Self {
            query_type: kind.into(),
            node_count,
            relationship_count: steps.len(),
            is_search: kind == Kind::Traversal && node_count == 1 && steps.is_empty(),
            has_order_by: sort_keys
                .iter()
                .any(|(key, _)| matches!(key, Expr::Property { .. })),
            has_output_sort: sort_keys
                .iter()
                .any(|(key, _)| matches!(key, Expr::Output(_))),
            limit: query.row_limit().unwrap_or_default(),
            skip: query.skip(),
            group_by_count: groups.len(),
            dynamic_columns: query.dynamic_columns(),
            path_max_depth: query
                .operations()
                .find_map(|(_, operation)| match operation {
                    LogicalOperation::Shortest { max_depth, .. } => Some(*max_depth),
                    _ => None,
                }),
            pinned: anchors.iter().any(|anchor| !anchor.pinned.is_empty()),
            entity_types: query
                .bindings()
                .iter()
                .map(|binding| graph.entity(binding.entity).name.clone())
                .collect(),
            relationship_types: steps
                .iter()
                .flat_map(|step| names(step.relationships))
                .collect(),
            node_filter_fields: anchors
                .iter()
                .flat_map(|anchor| anchor.filters.iter())
                .filter_map(field)
                .collect(),
            filter_fields: filters.iter().map(|(name, _)| name.clone()).collect(),
            filter_ops: filters
                .iter()
                .filter_map(|(_, fact)| Some(fact.op()?.as_ref().to_string()))
                .collect(),
            filter_count: filters.len(),
            max_hops: steps.iter().map(|step| step.range.max).max().unwrap_or(0),
            variable_hops: steps.iter().any(|step| step.range.min != step.range.max),
            requested_columns: query
                .bindings()
                .iter()
                .flat_map(|binding| {
                    let selection = selected.get(&binding.output).copied();
                    returned(model, binding.entity, selection)
                })
                .collect(),
            agg_functions: measures
                .iter()
                .map(|measure| match measure {
                    Measure::Count(_) | Measure::CountDistinct(_) => "count",
                    Measure::Sum(_) => "sum",
                    Measure::Avg(_) => "avg",
                    Measure::Min(_) => "min",
                    Measure::Max(_) => "max",
                })
                .map(String::from)
                .collect(),
            traversal_shape,
        }
    }
}
