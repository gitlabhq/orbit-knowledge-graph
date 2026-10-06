use super::*;

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn aggregation(&mut self, input: &crate::input::Input) -> Result<BlockId> {
        use crate::input::{AggExpr, Direction, InputGroupByKey, group_by_output_names};
        let shared_access = input
            .nodes
            .iter()
            .any(|node| node.existence == crate::input::NodeExistence::CurrentRow)
            || input.relationships.len() > 1
            || input
                .relationships
                .iter()
                .any(|relationship| relationship.hops.max > 1)
            || input
                .aggregation
                .group_by
                .iter()
                .any(|group| matches!(group, InputGroupByKey::Node { .. }))
            || !input.join_predicates.is_empty()
            || input
                .nodes
                .iter()
                .any(|node| !node.filters.is_empty() || node.id_range.is_some())
                && !input.relationships.is_empty()
            || input
                .relationships
                .iter()
                .any(|relationship| relationship.types.len() != 1)
            || input.relationships.iter().any(|relationship| {
                let entity = |alias: &str| {
                    input
                        .nodes
                        .iter()
                        .find(|node| node.id == alias)
                        .and_then(|node| node.entity.as_deref())
                };
                let (Some(from), Some(to)) = (entity(&relationship.from), entity(&relationship.to))
                else {
                    return false;
                };
                let (source, target) = if relationship.direction == Direction::Incoming {
                    (to, from)
                } else {
                    (from, to)
                };
                self.catalog
                    .foreign_key(&relationship.types, source, target)
                    .is_some()
            });
        let root = if shared_access {
            self.access(input)?
        } else {
            self.select(PhysicalOperation::One)
        };
        let mut columns = std::collections::HashMap::new();
        let mut condition = None;
        let mut operation = if shared_access {
            std::mem::replace(self.operation_mut(root)?, PhysicalOperation::One)
        } else if let [relationship] = input.relationships.as_slice() {
            if relationship.direction == Direction::Both
                || relationship.hops.min != 1
                || relationship.hops.max != 1
                || !relationship.filters.is_empty()
            {
                return Err(GraphError::UnsupportedInput(
                    "aggregation relationship".into(),
                ));
            }
            let table = self
                .catalog
                .relationship_table_for_query(&relationship.types);
            let edge = self.scan(root, table, "e0")?;
            self.bind_scan(edge, ScanInput::Relationship(0))?;
            let [kind] = relationship.types.as_slice() else {
                return Err(GraphError::UnsupportedInput(
                    "aggregation relationship kinds".into(),
                ));
            };
            let (source, target) = if relationship.direction == Direction::Incoming {
                (&relationship.to, &relationship.from)
            } else {
                (&relationship.from, &relationship.to)
            };
            let entity = |alias: &str| {
                input
                    .nodes
                    .iter()
                    .find(|node| node.id == alias)
                    .and_then(|node| node.entity.as_deref())
                    .ok_or(GraphError::MissingOutput)
            };
            let mut predicates = Vec::new();
            for (column, value) in [
                ("relationship_kind", kind.as_str()),
                ("source_kind", entity(source)?),
                ("target_kind", entity(target)?),
            ] {
                predicates.push(Expression::equal(
                    Expression::Column(self.stored_column(edge, column)?),
                    Expression::Text(value.into()),
                ));
            }
            predicates.push(Expression::equal(
                Expression::Column(self.stored_column(edge, "_deleted")?),
                Expression::Boolean(false),
            ));
            for (alias, column) in [(source, "source_id"), (target, "target_id")] {
                let node = input
                    .nodes
                    .iter()
                    .find(|node| node.id == *alias)
                    .ok_or(GraphError::MissingOutput)?;
                let column = self.stored_column(edge, column)?;
                columns.insert((node.id.clone(), "id".into()), column);
                if !node.filters.is_empty() || node.id_range.is_some() || node.id_property != "id" {
                    return Err(GraphError::UnsupportedInput(
                        "aggregation endpoint filters".into(),
                    ));
                }
                if !node.node_ids.is_empty() {
                    predicates.push(Expression::membership(column, &node.node_ids));
                }
            }
            condition = predicates
                .into_iter()
                .reduce(|left, right| Expression::And(Box::new(left), Box::new(right)));
            PhysicalOperation::source(edge)
                .filter(condition.clone().expect("edge predicates"))
                .latest(self.stored_column(edge, "_version")?, None)
        } else {
            if input.nodes.len() != 1 {
                return Err(GraphError::UnsupportedInput(
                    "disconnected aggregation".into(),
                ));
            }
            PhysicalOperation::One
        };
        for (index, node) in input.nodes.iter().enumerate() {
            let mut properties = input
                .aggregation
                .group_by
                .iter()
                .filter_map(|group| match group {
                    InputGroupByKey::Node { node: alias, .. } if alias == &node.id => Some("id"),
                    InputGroupByKey::Property {
                        node: alias,
                        property,
                        ..
                    } if alias == &node.id => Some(property.as_str()),
                    _ => None,
                })
                .chain(
                    input
                        .aggregation
                        .metrics
                        .iter()
                        .filter(|metric| metric.expr.node() == node.id)
                        .filter_map(|metric| metric.expr.property()),
                )
                .collect::<Vec<_>>();
            properties.sort_unstable();
            properties.dedup();
            if properties.is_empty() && !input.relationships.is_empty() {
                continue;
            }
            let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
            let table = self
                .catalog
                .entity_table(entity)
                .ok_or(GraphError::MissingOutput)?;
            if shared_access {
                let relation = self.input_node(root, index)?;
                for property in properties {
                    let stored = self
                        .catalog
                        .property_column_named(entity, property)
                        .ok_or(GraphError::MissingOutput)?;
                    columns.insert(
                        (node.id.clone(), property.into()),
                        self.stored_column(relation, stored)?,
                    );
                }
                columns.insert(
                    (node.id.clone(), "id".into()),
                    self.stored_column(relation, "id")?,
                );
                continue;
            }
            let node_block = if input.relationships.is_empty() {
                root
            } else {
                self.select(PhysicalOperation::One)
            };
            let relation = self.scan(node_block, table, &node.id)?;
            self.bind_scan(relation, ScanInput::Node(index))?;
            let scan = self.node_source(relation, node)?;
            let mut node_columns = Vec::new();
            if !properties.contains(&"id") {
                properties.push("id");
            }
            for property in properties {
                let stored = self
                    .catalog
                    .property_column_named(entity, property)
                    .ok_or(GraphError::MissingOutput)?;
                node_columns.push((property, self.stored_column(relation, stored)?));
            }
            let edge = columns.get(&(node.id.clone(), "id".into())).copied();
            let scan = if node_block != root {
                *self.operation_mut(node_block)? = scan;
                let derived = self.derive(root, node_block, &node.id)?;
                for (property, column) in &mut node_columns {
                    let output =
                        self.project(node_block, *property, Expression::Column(*column))?;
                    *column = self.output_column(derived, output)?;
                }
                PhysicalOperation::source(derived)
            } else {
                scan
            };
            for (property, column) in node_columns {
                columns.insert((node.id.clone(), property.into()), column);
            }
            let identity = columns[&(node.id.clone(), "id".into())];
            if let Some(edge) = edge {
                operation = operation.join(
                    scan,
                    Expression::equal(Expression::Column(identity), Expression::Column(edge)),
                );
            } else {
                operation = scan;
            }
        }
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
            let column = *columns
                .get(&(node.clone(), property.into()))
                .ok_or(GraphError::MissingOutput)?;
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
                    value: Box::new(Expression::Column(
                        *columns
                            .get(&(property.node.clone(), property.property.clone()))
                            .ok_or(GraphError::MissingOutput)?,
                    )),
                    condition: condition.clone().map(Box::new),
                },
                expression => Expression::Aggregate {
                    function: expression.function(),
                    value: Box::new(Expression::Column(
                        *columns
                            .get(&(
                                expression.node().into(),
                                expression
                                    .property()
                                    .ok_or(GraphError::MissingOutput)?
                                    .into(),
                            ))
                            .ok_or(GraphError::MissingOutput)?,
                    )),
                },
            };
            self.project(root, metric.output_name(), value)?;
        }
        *self.operation_mut(root)? = operation.group_by(groups).limit(input.limit);
        Ok(root)
    }
}
