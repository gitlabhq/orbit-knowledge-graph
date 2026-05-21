//! Non-PII query dimensions for analytics and billing.
//!
//! [`QueryInfo`] captures the structural properties of a compiled query
//! -- types, counts, operators, flags -- without any customer data (no IDs, no
//! filter values, no traversal paths).

use std::collections::BTreeSet;

use serde::Serialize;

use crate::input::{AggFunction, DynamicColumnMode, FilterOp, Input};
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::hydrate::HydrationPlan;

/// Structural dimensions of a compiled query, free of customer-specific data.
///
/// Every field is a bounded enum label, a count, or a boolean. Extracted from
/// [`CompiledQueryContext`] after compilation and forwarded to analytics and
/// billing observers via the pipeline observer chain.
#[derive(Debug, Clone, Serialize)]
pub struct QueryInfo {
    pub query_type: &'static str,
    pub node_count: u32,
    pub relationship_count: u32,
    pub entity_types: Vec<String>,
    pub relationship_types: Vec<String>,
    pub filter_count: u32,
    pub filter_fields: Vec<String>,
    pub filter_ops: Vec<String>,
    pub is_search: bool,
    pub has_cursor: bool,
    pub has_order_by: bool,
    pub limit: u32,
    pub max_hops: u32,
    pub agg_functions: Vec<String>,
    pub group_by_count: u32,
    pub hydration_plan: &'static str,
    pub dynamic_columns: &'static str,
    pub path_max_depth: Option<u32>,
    pub has_variable_hops: bool,
    pub has_virtual_columns: bool,
}

impl From<&CompiledQueryContext> for QueryInfo {
    fn from(ctx: &CompiledQueryContext) -> Self {
        Self::extract(&ctx.input, &ctx.hydration)
    }
}

impl QueryInfo {
    fn extract(input: &Input, hydration: &HydrationPlan) -> Self {
        let mut entity_types = BTreeSet::new();
        let mut rel_types = BTreeSet::new();
        let mut filter_fields = BTreeSet::new();
        let mut filter_ops = BTreeSet::new();
        let mut filter_count: u32 = 0;
        let mut has_virtual_columns = false;
        let mut max_hops: u32 = 0;
        let mut has_variable_hops = false;

        for node in &input.nodes {
            if let Some(entity) = &node.entity {
                entity_types.insert(entity.clone());
            }
            if !node.virtual_columns.is_empty() {
                has_virtual_columns = true;
            }
            collect_filters(
                &node.filters,
                &mut filter_fields,
                &mut filter_ops,
                &mut filter_count,
            );
        }

        for rel in &input.relationships {
            for t in &rel.types {
                rel_types.insert(t.clone());
            }
            max_hops = max_hops.max(rel.max_hops);
            if rel.min_hops != rel.max_hops {
                has_variable_hops = true;
            }
            collect_filters(
                &rel.filters,
                &mut filter_fields,
                &mut filter_ops,
                &mut filter_count,
            );
        }

        let mut agg_fns = BTreeSet::new();
        for m in &input.aggregation.metrics {
            agg_fns.insert(agg_fn_label(&m.function));
        }

        Self {
            query_type: input.query_type.into(),
            node_count: input.nodes.len() as u32,
            relationship_count: input.relationships.len() as u32,
            entity_types: entity_types.into_iter().collect(),
            relationship_types: rel_types.into_iter().collect(),
            filter_count,
            filter_fields: filter_fields.into_iter().collect(),
            filter_ops: filter_ops.into_iter().collect(),
            is_search: input.is_search(),
            has_cursor: input.cursor.is_some(),
            has_order_by: input.order_by.is_some(),
            limit: input.limit,
            max_hops,
            agg_functions: agg_fns.into_iter().collect(),
            group_by_count: input.aggregation.group_by.len() as u32,
            hydration_plan: hydration_label(hydration),
            dynamic_columns: dynamic_col_label(input.options.dynamic_columns),
            path_max_depth: input.path.as_ref().map(|p| p.max_depth),
            has_variable_hops,
            has_virtual_columns,
        }
    }
}

fn collect_filters(
    filters: &std::collections::HashMap<String, Vec<crate::input::InputFilter>>,
    fields: &mut BTreeSet<String>,
    ops: &mut BTreeSet<String>,
    count: &mut u32,
) {
    for (field, entries) in filters {
        fields.insert(field.clone());
        for f in entries {
            *count += 1;
            if let Some(op) = &f.op {
                ops.insert(op_label(op));
            }
        }
    }
}

fn op_label(op: &FilterOp) -> String {
    match op {
        FilterOp::Eq => "eq",
        FilterOp::Gt => "gt",
        FilterOp::Lt => "lt",
        FilterOp::Gte => "gte",
        FilterOp::Lte => "lte",
        FilterOp::In => "in",
        FilterOp::Contains => "contains",
        FilterOp::StartsWith => "starts_with",
        FilterOp::EndsWith => "ends_with",
        FilterOp::IsNull => "is_null",
        FilterOp::IsNotNull => "is_not_null",
        FilterOp::TokenMatch => "token_match",
        FilterOp::AllTokens => "all_tokens",
        FilterOp::AnyTokens => "any_tokens",
    }
    .into()
}

fn agg_fn_label(f: &AggFunction) -> String {
    match f {
        AggFunction::Count => "count",
        AggFunction::Sum => "sum",
        AggFunction::Avg => "avg",
        AggFunction::Min => "min",
        AggFunction::Max => "max",
        AggFunction::Collect => "collect",
    }
    .into()
}

fn hydration_label(h: &HydrationPlan) -> &'static str {
    match h {
        HydrationPlan::None => "none",
        HydrationPlan::Static(_) => "static",
        HydrationPlan::Dynamic(_) => "dynamic",
    }
}

fn dynamic_col_label(mode: DynamicColumnMode) -> &'static str {
    match mode {
        DynamicColumnMode::All => "all",
        DynamicColumnMode::Default => "default",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{
        InputAggregation, InputAggregationMetric, InputFilter, InputGroupByKey, InputNode,
        InputPath, InputRelationship, PathType, QueryType,
    };

    fn search_input() -> Input {
        Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                entity: Some("User".into()),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn search() {
        let d = QueryInfo::extract(&search_input(), &HydrationPlan::None);

        assert_eq!(d.query_type, "traversal");
        assert_eq!(d.node_count, 1);
        assert_eq!(d.relationship_count, 0);
        assert_eq!(d.entity_types, ["User"]);
        assert!(d.is_search);
        assert!(!d.has_cursor);
        assert_eq!(d.filter_count, 0);
        assert_eq!(d.hydration_plan, "none");
    }

    #[test]
    fn traversal_with_filters() {
        let input = Input {
            nodes: vec![
                InputNode {
                    entity: Some("User".into()),
                    ..Default::default()
                },
                InputNode {
                    entity: Some("MergeRequest".into()),
                    filters: [(
                        "state".into(),
                        vec![InputFilter {
                            op: Some(FilterOp::Eq),
                            ..Default::default()
                        }],
                    )]
                    .into(),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["AUTHORED".into()],
                from: "u".into(),
                to: "mr".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let d = QueryInfo::extract(&input, &HydrationPlan::None);

        assert_eq!(d.node_count, 2);
        assert_eq!(d.entity_types, ["MergeRequest", "User"]);
        assert_eq!(d.relationship_types, ["AUTHORED"]);
        assert_eq!(d.filter_count, 1);
        assert_eq!(d.filter_fields, ["state"]);
        assert_eq!(d.filter_ops, ["eq"]);
        assert!(!d.is_search);
    }

    #[test]
    fn aggregation() {
        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![InputNode {
                entity: Some("Project".into()),
                ..Default::default()
            }],
            aggregation: InputAggregation {
                metrics: vec![InputAggregationMetric {
                    function: AggFunction::Count,
                    target: Some("p".into()),
                    property: None,
                    alias: None,
                }],
                group_by: vec![InputGroupByKey::Node {
                    node: "p".into(),
                    alias: None,
                }],
                sort: None,
            },
            ..Default::default()
        };
        let d = QueryInfo::extract(&input, &HydrationPlan::None);

        assert_eq!(d.query_type, "aggregation");
        assert_eq!(d.agg_functions, ["count"]);
        assert_eq!(d.group_by_count, 1);
    }

    #[test]
    fn path_finding() {
        let input = Input {
            query_type: QueryType::PathFinding,
            nodes: vec![
                InputNode {
                    entity: Some("User".into()),
                    ..Default::default()
                },
                InputNode {
                    entity: Some("Project".into()),
                    ..Default::default()
                },
            ],
            path: Some(InputPath {
                path_type: PathType::Shortest,
                from: "s".into(),
                to: "e".into(),
                max_depth: 3,
                rel_types: vec!["MEMBER_OF".into()],
                forward_first_hop_rel_types: vec![],
                backward_first_hop_rel_types: vec![],
            }),
            ..Default::default()
        };
        let d = QueryInfo::extract(&input, &HydrationPlan::Dynamic(vec![]));

        assert_eq!(d.path_max_depth, Some(3));
        assert_eq!(d.hydration_plan, "dynamic");
    }

    #[test]
    fn variable_hops() {
        let input = Input {
            nodes: vec![InputNode {
                entity: Some("Group".into()),
                ..Default::default()
            }],
            relationships: vec![InputRelationship {
                types: vec!["CONTAINS".into()],
                from: "g".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 3,
                ..Default::default()
            }],
            ..Default::default()
        };
        let d = QueryInfo::extract(&input, &HydrationPlan::None);

        assert!(d.has_variable_hops);
        assert_eq!(d.max_hops, 3);
    }

    #[test]
    fn from_compiled_context() {
        use crate::passes::codegen::{CompiledQueryContext, ParameterizedQuery, SqlDialect};
        use crate::passes::enforce::ResultContext;
        use gkg_server_config::QueryConfig;

        let ctx = CompiledQueryContext {
            query_type: QueryType::Traversal,
            base: ParameterizedQuery {
                sql: String::new(),
                params: Default::default(),
                result_context: ResultContext::new(QueryType::Traversal),
                query_config: QueryConfig::default(),
                dialect: SqlDialect::ClickHouse,
            },
            hydration: HydrationPlan::None,
            input: search_input(),
        };
        let d = QueryInfo::from(&ctx);
        assert!(d.is_search);
    }

    #[test]
    fn serializes_cleanly() {
        let d = QueryInfo::extract(&search_input(), &HydrationPlan::None);
        let json = serde_json::to_value(&d).unwrap();
        assert_eq!(json["query_type"], "traversal");
        assert_eq!(json["is_search"], true);
        assert_eq!(json["node_count"], 1);
    }
}
