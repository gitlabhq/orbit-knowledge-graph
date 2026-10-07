use super::*;

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn aggregation(&mut self, input: &crate::input::Input) -> Result<BlockId> {
        use crate::input::{AggExpr, InputGroupByKey, group_by_output_names};
        let (root, condition) = self.plan_access(input)?;
        let operation = std::mem::replace(self.operation_mut(root)?, PhysicalOperation::One);
        let mut groups = Vec::new();
        for alias in crate::input::node_group_ids(&input.aggregation.group_by) {
            let (index, node) = input
                .nodes
                .iter()
                .enumerate()
                .find(|(_, node)| node.id == alias)
                .ok_or(GraphError::MissingOutput)?;
            let relation = self.input_node(root, index)?;
            let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
            if let Some(crate::input::ColumnSelection::List(properties)) = &node.columns {
                for property in properties {
                    let Some(column) = self.catalog.property_column_named(entity, property) else {
                        continue;
                    };
                    let value = Expression::Column(self.column(relation, column)?);
                    if !groups.contains(&value) {
                        groups.push(value.clone());
                    }
                    let label = format!("{alias}_{property}");
                    if !self.outputs(root)?.any(|output| {
                        self.output_label(output)
                            .is_ok_and(|existing| existing == label)
                    }) {
                        self.project(root, label, value)?;
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
            let column = self.aggregate_column(root, input, node, property)?;
            let value = match truncate {
                Some(unit) => Expression::Bucket {
                    unit,
                    value: Box::new(Expression::Column(column)),
                },
                None => Expression::Column(column),
            };
            if !groups.contains(&value) {
                groups.push(value.clone());
            }
            self.project(root, label, value)?;
        }
        for metric in &input.aggregation.metrics {
            let value = match &metric.expr {
                AggExpr::Count(target) if target.property.is_none() => {
                    condition.clone().map_or(Expression::Count, |condition| {
                        Expression::CountIf(Box::new(condition))
                    })
                }
                AggExpr::Sum(property) => Expression::Sum {
                    value: Box::new(Expression::Column(self.aggregate_column(
                        root,
                        input,
                        &property.node,
                        &property.property,
                    )?)),
                    condition: condition.clone().map(Box::new),
                },
                expression => Expression::Aggregate {
                    function: expression.function(),
                    value: Box::new(Expression::Column(self.aggregate_column(
                        root,
                        input,
                        expression.node(),
                        expression.property().ok_or(GraphError::MissingOutput)?,
                    )?)),
                },
            };
            self.project(root, metric.output_name(), value)?;
        }
        *self.operation_mut(root)? = operation.group_by(groups).limit(input.limit);
        Ok(root)
    }

    fn aggregate_column(
        &self,
        root: BlockId,
        input: &crate::input::Input,
        alias: &str,
        property: &str,
    ) -> Result<ColumnRef<'catalog>> {
        let (index, node) = input
            .nodes
            .iter()
            .enumerate()
            .find(|(_, node)| node.id == alias)
            .ok_or(GraphError::MissingOutput)?;
        if property == "id" {
            return self.input_identity(root, input, index);
        }
        let column = self
            .catalog
            .property_column_named(
                node.entity.as_deref().ok_or(GraphError::MissingOutput)?,
                property,
            )
            .ok_or(GraphError::MissingOutput)?;
        self.column(self.input_node(root, index)?, column)
    }
}
