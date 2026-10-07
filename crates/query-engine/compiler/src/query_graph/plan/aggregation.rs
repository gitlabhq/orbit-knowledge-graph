use super::*;
use crate::input::{
    AggExpr, ColumnSelection, Input, InputGroupByKey, group_by_output_names, node_group_ids,
};

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub(super) fn aggregation(&mut self, input: &Input) -> Result<BlockId> {
        let (access, mut operation) = self.plan_access(input)?;
        let root = access.root;
        for predicate in &input.join_predicates {
            let left =
                self.aggregate_column(root, input, &predicate.lhs_node, &predicate.lhs_prop)?;
            let right =
                self.aggregate_column(root, input, &predicate.rhs_node, &predicate.rhs_prop)?;
            operation = self.filter_relation(
                operation,
                Expression::Predicate {
                    operator: predicate.op,
                    value: Box::new(Expression::Column(left)),
                    argument: Some(Box::new(Expression::Column(right))),
                    fold_case: false,
                },
            )?;
        }
        let mut outputs = self.aggregate_groups(root, input)?;
        let mut groups = Vec::new();
        for (_, value) in &outputs {
            if !groups.contains(value) {
                groups.push(value.clone());
            }
        }
        for metric in &input.aggregation.metrics {
            outputs.push((
                metric.output_name(),
                self.aggregate_measure(
                    root,
                    input,
                    &metric.expr,
                    access.aggregate_condition.as_ref(),
                )?,
            ));
        }
        let operation = self.aggregate_relation(operation, groups)?;
        let projection =
            self.project_values(self.limit_relation(operation, input.limit)?, outputs)?;
        self.finish_query(projection)
    }

    fn aggregate_groups(
        &self,
        root: BlockId,
        input: &Input,
    ) -> Result<Vec<(String, Expression<'a>)>> {
        let mut outputs = Vec::new();
        for alias in node_group_ids(&input.aggregation.group_by) {
            let (index, node) = input
                .nodes
                .iter()
                .enumerate()
                .find(|(_, node)| node.id == alias)
                .ok_or(GraphError::MissingOutput)?;
            let relation = self.input_node(root, index)?;
            let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
            if let Some(ColumnSelection::List(properties)) = &node.columns {
                for property in properties {
                    let Some(column) = self.catalog.property_column_named(entity, property) else {
                        continue;
                    };
                    let label = format!("{alias}_{property}");
                    if !outputs.iter().any(|(existing, _)| *existing == label) {
                        outputs.push((label, Expression::Column(self.column(relation, column)?)));
                    }
                }
            }
        }
        for (group, label) in input
            .aggregation
            .group_by
            .iter()
            .zip(group_by_output_names(&input.aggregation.group_by))
        {
            let (node, property, truncate) = match group {
                InputGroupByKey::Property {
                    node,
                    property,
                    truncate,
                    ..
                } => (node, property.as_str(), *truncate),
                InputGroupByKey::Node { node, .. } => (node, "id", None),
            };
            let value = Expression::Column(self.aggregate_column(root, input, node, property)?);
            outputs.push((
                label,
                match truncate {
                    Some(unit) => Expression::Bucket {
                        unit,
                        value: Box::new(value),
                    },
                    None => value,
                },
            ));
        }
        Ok(outputs)
    }

    fn aggregate_measure(
        &self,
        root: BlockId,
        input: &Input,
        measure: &AggExpr,
        condition: Option<&Expression<'a>>,
    ) -> Result<Expression<'a>> {
        if let AggExpr::Count(target) = measure
            && target.property.is_none()
        {
            return Ok(condition.cloned().map_or(Expression::Count, |condition| {
                Expression::CountIf(Box::new(condition))
            }));
        }
        let column = self.aggregate_column(
            root,
            input,
            measure.node(),
            measure.property().ok_or(GraphError::MissingOutput)?,
        )?;
        let value = Box::new(Expression::Column(column));
        Ok(if matches!(measure, AggExpr::Sum(_)) {
            Expression::Sum {
                value,
                condition: condition.cloned().map(Box::new),
            }
        } else {
            Expression::Aggregate {
                function: measure.function(),
                value,
            }
        })
    }

    fn aggregate_column(
        &self,
        root: BlockId,
        input: &Input,
        alias: &str,
        property: &str,
    ) -> Result<ColumnRef<'a>> {
        let (index, node) = input
            .nodes
            .iter()
            .enumerate()
            .find(|(_, node)| node.id == alias)
            .ok_or(GraphError::MissingOutput)?;
        if property == "id" {
            return self.input_identity(root, input, index);
        }
        let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
        let column = self
            .catalog
            .property_column_named(entity, property)
            .ok_or(GraphError::MissingOutput)?;
        self.column(self.input_node(root, index)?, column)
    }
}
