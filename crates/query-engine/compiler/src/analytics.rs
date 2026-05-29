//! Non-PII query dimensions for analytics and billing.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::Serialize;

use crate::input::Input;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::hydrate::HydrationPlan;

/// Structural dimensions of a compiled query, free of customer-specific data.
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

        let mut collect_filters =
            |filters: &std::collections::HashMap<String, Vec<crate::input::InputFilter>>| {
                for (field, entries) in filters {
                    filter_fields.insert(field.clone());
                    for f in entries {
                        filter_count += 1;
                        if let Some(op) = &f.op {
                            filter_ops.insert(op.as_ref().to_owned());
                        }
                    }
                }
            };

        for node in &input.nodes {
            if let Some(entity) = &node.entity {
                entity_types.insert(entity.clone());
            }
            has_virtual_columns |= !node.virtual_columns.is_empty();
            collect_filters(&node.filters);
        }

        for rel in &input.relationships {
            rel_types.extend(rel.types.iter().cloned());
            max_hops = max_hops.max(rel.max_hops);
            has_variable_hops |= rel.min_hops != rel.max_hops;
            collect_filters(&rel.filters);
        }

        let mut agg_fns = BTreeSet::new();
        for m in &input.aggregation.metrics {
            agg_fns.insert(m.function.to_string());
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
            dynamic_columns: input.options.dynamic_columns.into(),
            path_max_depth: input.path.as_ref().map(|p| p.max_depth),
            has_variable_hops,
            has_virtual_columns,
        }
    }
}

/// Accumulated pipeline execution metrics. Embedded by observer impls.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecMetrics {
    #[serde(skip)]
    pub query_info: Option<QueryInfo>,
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

    pub fn set_query_info(&mut self, info: QueryInfo) { self.query_info = Some(info); }
    pub fn compiled(&mut self, elapsed: Duration) { self.compile_ms = Some(Self::ms(elapsed)); }
    pub fn executed(&mut self, elapsed: Duration) { self.execute_ms = Some(Self::ms(elapsed)); }
    pub fn authorized(&mut self, elapsed: Duration) { self.authorization_ms = Some(Self::ms(elapsed)); }
    pub fn hydrated(&mut self, elapsed: Duration) { self.hydration_ms = Some(Self::ms(elapsed)); }

    pub fn query_executed(&mut self, read_rows: u64, read_bytes: u64, memory: i64) {
        self.ch_read_rows += read_rows;
        self.ch_read_bytes += read_bytes;
        if memory > 0 {
            self.ch_memory_usage = self.ch_memory_usage.max(memory as u64);
        }
    }
}

fn hydration_label(h: &HydrationPlan) -> &'static str {
    match h {
        HydrationPlan::None => "none",
        HydrationPlan::Static(_) => "static",
        HydrationPlan::Dynamic(_) => "dynamic",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::*;

    fn node(entity: &str) -> InputNode {
        InputNode { entity: Some(entity.into()), ..Default::default() }
    }

    fn rel(types: &[&str], min: u32, max: u32) -> InputRelationship {
        InputRelationship {
            types: types.iter().map(|s| s.to_string()).collect(),
            from: "a".into(), to: "b".into(),
            min_hops: min, max_hops: max,
            direction: Direction::Outgoing,
            filters: Default::default(), fk_column: None,
        }
    }

    #[test]
    fn search() {
        let input = Input {
            nodes: vec![node("User")],
            ..Default::default()
        };
        let d = QueryInfo::extract(&input, &HydrationPlan::None);

        assert_eq!(d.query_type, "traversal");
        assert_eq!(d.node_count, 1);
        assert_eq!(d.entity_types, ["User"]);
        assert!(d.is_search);
        assert_eq!(d.filter_count, 0);
        assert_eq!(d.hydration_plan, "none");
    }

    #[test]
    fn traversal_with_filters() {
        let input = Input {
            nodes: vec![
                node("User"),
                InputNode {
                    entity: Some("MergeRequest".into()),
                    filters: [("state".into(), vec![InputFilter {
                        op: Some(FilterOp::Eq), ..Default::default()
                    }])].into(),
                    ..Default::default()
                },
            ],
            relationships: vec![rel(&["AUTHORED"], 1, 1)],
            ..Default::default()
        };
        let d = QueryInfo::extract(&input, &HydrationPlan::None);

        assert_eq!(d.entity_types, ["MergeRequest", "User"]);
        assert_eq!(d.relationship_types, ["AUTHORED"]);
        assert_eq!(d.filter_count, 1);
        assert_eq!(d.filter_ops, ["eq"]);
        assert!(!d.is_search);
    }

    #[test]
    fn aggregation() {
        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![node("Project")],
            aggregation: InputAggregation {
                metrics: vec![InputAggregationMetric {
                    function: AggFunction::Count, target: Some("p".into()),
                    property: None, alias: None,
                }],
                group_by: vec![InputGroupByKey::Node { node: "p".into(), alias: None }],
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
            nodes: vec![node("User"), node("Project")],
            path: Some(InputPath {
                path_type: PathType::Shortest, from: "s".into(), to: "e".into(),
                max_depth: 3, rel_types: vec!["MEMBER_OF".into()],
                forward_first_hop_rel_types: vec![], backward_first_hop_rel_types: vec![],
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
            nodes: vec![node("Group")],
            relationships: vec![rel(&["CONTAINS"], 1, 3)],
            ..Default::default()
        };
        let d = QueryInfo::extract(&input, &HydrationPlan::None);

        assert!(d.has_variable_hops);
        assert_eq!(d.max_hops, 3);
    }
}
