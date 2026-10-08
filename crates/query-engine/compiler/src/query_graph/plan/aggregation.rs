use super::super::api::*;
use super::traversal;
use crate::input::{AggFunction, Input, InputGroupByKey, group_by_output_names, node_group_ids};
use query_data_model::QueryDataModel;

pub(super) fn build<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    input: &Input,
) -> Result<Rows<'a>> {
    let rows = traversal::matches(q, input)?;
    let mut groups: Vec<Named> = Vec::new();
    for alias in node_group_ids(&input.aggregation.group_by) {
        let node = traversal::node(input, alias)?;
        for value in traversal::node_outputs(q.catalog(), &rows, node)? {
            if !groups.iter().any(|existing| existing.name == value.name) {
                groups.push(value);
            }
        }
    }
    for (group, label) in input
        .aggregation
        .group_by
        .iter()
        .zip(group_by_output_names(&input.aggregation.group_by))
    {
        let (alias, property, truncate) = match group {
            InputGroupByKey::Node { node, .. } => (node, "id", None),
            InputGroupByKey::Property {
                node,
                property,
                truncate,
                ..
            } => (node, property.as_str(), *truncate),
        };
        let value = traversal::property(q.catalog(), input, &rows, alias, property)?.expr();
        let value = if let Some(unit) = truncate {
            Expr::call(Function::TimeBucket(unit), [value])
        } else {
            value
        };
        groups.push(value.named(label));
    }
    let mut measures = Vec::new();
    for metric in &input.aggregation.metrics {
        let function = match metric.expr.function() {
            AggFunction::Count => Aggregate::Count,
            AggFunction::Sum => Aggregate::Sum,
            AggFunction::Avg => Aggregate::Average,
            AggFunction::Min => Aggregate::Min,
            AggFunction::Max => Aggregate::Max,
            AggFunction::Collect => Aggregate::Collect,
        };
        let argument = metric
            .expr
            .property()
            .map(|property| {
                traversal::property(q.catalog(), input, &rows, metric.expr.node(), property)
                    .map(|column| column.expr())
            })
            .transpose()?;
        measures.push(Expr::aggregate(function, argument).named(metric.output_name()));
    }
    let rows = q.aggregate(rows, groups, measures)?;
    q.limit(rows, input.limit)
}
