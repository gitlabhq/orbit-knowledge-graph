use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::passes::plan::physical::PhysicalPlan;

pub(super) fn emit_hydration<T>(
    nodes: &[PhysicalPlan],
    limit: u32,
    lowerer: &super::physical::PhysicalLowerer<'_, T>,
) -> Result<Node> {
    let mut arms = nodes.iter().map(|node| lowerer.query(node));
    let mut first = arms
        .next()
        .ok_or_else(|| QueryError::Lowering("hydration requires at least one node".into()))?;
    for arm in arms {
        first.union_all.push(arm);
    }
    first.limit = Some(limit);
    Ok(Node::Query(Box::new(first)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{ColumnSelection, Input, InputNode, QueryType};
    use crate::passes::codegen::codegen;
    use crate::passes::enforce::ResultContext;
    use crate::passes::plan::{HydrationCompileOptions, plan_clickhouse};
    use orbit_server_config::QueryConfig;
    use orbit_utils::traversal_path::TraversalPath;
    use std::sync::Arc;

    fn render(node: &Node) -> String {
        codegen(node, ResultContext::new(), QueryConfig::default())
            .unwrap()
            .sql
    }

    fn render_with_params(
        node: &Node,
    ) -> (
        String,
        std::collections::HashMap<String, crate::passes::codegen::ParamValue>,
    ) {
        let q = codegen(node, ResultContext::new(), QueryConfig::default()).unwrap();
        (q.sql, q.params)
    }

    fn plan(columns: Vec<&str>, node_ids: Vec<i64>, traversal_paths: Vec<&str>) -> InputNode {
        InputNode {
            id: "hydrate".into(),
            entity: Some("MergeRequest".into()),
            node_ids,
            columns: Some(ColumnSelection::List(
                columns.into_iter().map(String::from).collect(),
            )),
            traversal_paths: traversal_paths
                .into_iter()
                .map(TraversalPath::new_unchecked)
                .collect(),
            ..Default::default()
        }
    }

    fn emit_dynamic(plans: &[InputNode], limit: u32) -> Node {
        emit(plans, limit, true)
    }

    fn emit_static(plans: &[InputNode], limit: u32) -> Node {
        emit(plans, limit, false)
    }

    fn emit(nodes: &[InputNode], limit: u32, dynamic: bool) -> Node {
        let model = query_data_model::ClickHouseDataModel::derive(Arc::new(
            ontology::Ontology::load_embedded().unwrap(),
        ))
        .unwrap();
        let input = Input {
            query_type: QueryType::Hydration,
            nodes: nodes.to_vec(),
            limit,
            ..Default::default()
        };
        let plan = plan_clickhouse(
            &input,
            &model,
            HydrationCompileOptions {
                dynamic,
                path_segment_budget: None,
            },
            &Default::default(),
        )
        .unwrap();
        super::super::emit(&plan, &input, &model).unwrap().ast
    }

    #[test]
    fn large_dynamic_tp_sets_emit_array_exists() {
        let paths: Vec<TraversalPath> = (0..=256)
            .map(|id| TraversalPath::new_unchecked(format!("1/9970/{id}/")))
            .collect();
        let plan = InputNode {
            traversal_paths: paths.clone(),
            ..plan(vec!["title"], vec![1], vec![])
        };

        let node = emit_dynamic(&[plan], 10);
        let (sql, params) = render_with_params(&node);

        assert!(
            sql.contains("arrayExists"),
            "large dynamic TP sets should use arrayExists: {sql}"
        );
        assert_eq!(
            sql.matches("startsWith").count(),
            1,
            "arrayExists should keep one startsWith in the lambda: {sql}"
        );
        let array_params: Vec<_> = params
            .values()
            .filter_map(|p| match &p.value {
                serde_json::Value::Array(items) => Some(items),
                _ => None,
            })
            .collect();
        assert_eq!(
            array_params.len(),
            1,
            "expected one traversal-path array param"
        );
        assert_eq!(array_params[0].len(), paths.len());
    }

    #[test]
    fn large_static_tp_sets_emit_or() {
        let paths: Vec<TraversalPath> = (0..=256)
            .map(|id| TraversalPath::new_unchecked(format!("1/9970/{id}/")))
            .collect();
        let plan = InputNode {
            traversal_paths: paths,
            ..plan(vec!["title"], vec![1], vec![])
        };

        let node = emit_static(&[plan], 10);
        let sql = render(&node);

        assert!(
            !sql.contains("arrayExists"),
            "static TP sets should keep OR startsWith: {sql}"
        );
        assert_eq!(sql.matches("startsWith").count(), 257);
    }

    #[test]
    fn dynamic_tp_filter_precedes_id_filter() {
        let node = emit_dynamic(&[plan(vec!["title"], vec![1], vec!["1/9970/"])], 10);
        let sql = render(&node);
        let tp_pos = sql.find("startsWith").unwrap();
        let in_pos = sql.find(" IN ").or_else(|| sql.find(" = ")).unwrap();
        assert!(
            tp_pos < in_pos,
            "TP filter should precede ID filter for primary key pruning: {sql}"
        );
    }

    #[test]
    fn static_tp_filter_precedes_id_filter() {
        let node = emit_static(&[plan(vec!["title"], vec![1], vec!["1/9970/"])], 10);
        let sql = render(&node);
        let tp_pos = sql.find("startsWith").unwrap();
        let in_pos = sql.find(" IN ").or_else(|| sql.find(" = ")).unwrap();
        assert!(
            tp_pos < in_pos,
            "TP filter should precede ID filter for primary key pruning: {sql}"
        );
    }
}
