use std::collections::BTreeMap;

use query_data_model::QueryDataModel;

use super::bind::BoundQuery;
use super::generic::{AggregateFunction, Assignment, Expr, Measure, Node, Op};
use super::graph;
use crate::constants::redaction_id_column;
use crate::error::{QueryError, Result};
use crate::input::{AggFunction, Input, InputGroupByKey};

pub fn bind(
    input: &Input,
    model: &impl QueryDataModel,
    mut name: impl FnMut() -> String,
) -> Result<BoundQuery> {
    let mut fields = BTreeMap::new();
    let mut require = |node: &str, property: &str| {
        fields
            .entry((node.to_string(), property.to_string()))
            .or_insert_with(&mut name);
    };

    for node in &input.nodes {
        require(&node.id, &node.id_property);
    }

    for group in &input.aggregation.group_by {
        if group.truncate().is_some() {
            return Err(QueryError::Validation(
                "group truncation has no typed binding yet".into(),
            ));
        }

        if let Some(property) = group.property() {
            require(group.node(), property);
        }
    }

    for metric in &input.aggregation.metrics {
        if let Some(property) = metric.expr.property() {
            require(metric.expr.node(), property);
        }
    }

    let required = fields
        .iter()
        .map(|((node, property), name)| (node.clone(), property.clone(), name.clone()))
        .collect::<Vec<_>>();
    let edges = input
        .relationships
        .iter()
        .map(|_| std::array::from_fn(|_| name()))
        .collect::<Vec<_>>();
    let bound = graph::traversal(input, model, &required, &edges)?;
    let mut values = bound.values;
    let fields: BTreeMap<_, _> = fields.into_keys().zip(bound.required).collect();

    let resolve = |node: &str, property: Option<&str>| {
        let property = property
            .or_else(|| {
                input
                    .nodes
                    .iter()
                    .find(|binding| binding.id == node)
                    .map(|binding| binding.id_property.as_str())
            })
            .ok_or_else(|| QueryError::ReferenceError(format!("unknown aggregate node {node}")))?;

        fields
            .get(&(node.to_string(), property.to_string()))
            .copied()
            .ok_or_else(|| {
                QueryError::ReferenceError(format!("unbound aggregate property {node}.{property}"))
            })
    };
    let mut groups = Vec::new();
    let mut outputs = Vec::new();
    let mut group_values = Vec::new();

    for group in &input.aggregation.group_by {
        let source = resolve(group.node(), group.property())?;
        let output = values.allocate(values.data_type(source)?.clone());
        groups.push(Assignment {
            output,
            expression: Expr::Value(source),
        });
        group_values.push(output);
        outputs.push(match group {
            InputGroupByKey::Node { node, .. } => redaction_id_column(node),
            InputGroupByKey::Property { .. } => group.output_name(),
        });
    }

    let mut measures = Vec::new();
    for metric in &input.aggregation.metrics {
        let function = match metric.expr.function() {
            AggFunction::Count => AggregateFunction::Count,
            AggFunction::Sum => AggregateFunction::Sum,
            AggFunction::Avg => AggregateFunction::Average,
            AggFunction::Min => AggregateFunction::Minimum,
            AggFunction::Max => AggregateFunction::Maximum,
            AggFunction::Collect => {
                return Err(QueryError::Validation("collect is unsupported".into()));
            }
        };
        let source = resolve(metric.expr.node(), metric.expr.property())?;
        let output = values.allocate(function.return_type(Some(values.data_type(source)?), false)?);
        measures.push(Measure {
            output,
            function,
            argument: Some(Expr::Value(source)),
            distinct: false,
            filter: None,
        });
        outputs.push(metric.output_name());
    }

    let root = Node {
        op: Op::Aggregate { groups, measures },
        inputs: vec![bound.root],
    };
    root.output(&values)?;

    Ok(BoundQuery {
        root,
        values,
        outputs,
        required: group_values,
    })
}
