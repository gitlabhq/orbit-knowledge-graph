use super::*;
use crate::input::{Direction, Input};
use std::collections::HashMap;

use super::access::{AccessPlan, entity};
use crate::input::FilterOp;

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn join_edges<'input>(
        &mut self,
        input: &'input Input,
        plan: &mut AccessPlan<'input, 'catalog>,
    ) -> Result<PhysicalOperation<'catalog>> {
        let root = plan.root;
        let relations = &mut plan.relations;
        let node_operations = &mut plan.node_operations;
        let node_predicates = &mut plan.node_predicates;
        let elided = &mut plan.elided;
        let entity = |alias: &str| entity(input, alias);
        let mut operation = PhysicalOperation::One;
        let mut edges = Vec::new();
        let mut filter_keys = HashMap::new();
        let mut key_scans = Vec::new();
        for (index, relationship) in input.relationships.iter().enumerate() {
            if relationship.hops.min != 1
                || relationship.hops.max != 1
                || relationship.direction == Direction::Both
            {
                return Err(GraphError::UnsupportedInput(
                    "variable or bidirectional traversal".into(),
                ));
            }
            let table = self
                .catalog
                .relationship_table_for_query(&relationship.types);
            let edge = self.scan(root, table, format!("e{index}"))?;
            self.bind_scan(edge, ScanInput::Relationship(index))?;
            let (start, end) = relationship.direction.edge_columns();
            let read = if input.relationships.len() == 1 {
                PhysicalOperation::source(edge)
            } else {
                PhysicalOperation::current(edge)
            };
            let mut scan = read.filter(Expression::equal(
                Expression::Column(self.stored_column(edge, "_deleted")?),
                Expression::Boolean(false),
            ));
            for (column, kind) in [
                (
                    "source_kind",
                    if relationship.direction == Direction::Incoming {
                        entity(&relationship.to)?
                    } else {
                        entity(&relationship.from)?
                    },
                ),
                (
                    "target_kind",
                    if relationship.direction == Direction::Incoming {
                        entity(&relationship.from)?
                    } else {
                        entity(&relationship.to)?
                    },
                ),
            ] {
                scan = scan.filter(Expression::equal(
                    Expression::Column(self.stored_column(edge, column)?),
                    Expression::Text(kind.into()),
                ));
            }
            if let [kind] = relationship.types.as_slice() {
                scan = scan.filter(Expression::equal(
                    Expression::Column(self.stored_column(edge, "relationship_kind")?),
                    Expression::Text(kind.clone()),
                ));
            } else {
                return Err(GraphError::UnsupportedInput(
                    "multiple relationship kinds".into(),
                ));
            }
            for (column, filters) in &relationship.filters {
                for filter in filters {
                    let value = filter
                        .value
                        .as_ref()
                        .and_then(serde_json::Value::as_i64)
                        .ok_or_else(|| GraphError::UnsupportedInput("edge predicate".into()))?;
                    if filter.op.unwrap_or(FilterOp::Eq) != FilterOp::Eq {
                        return Err(GraphError::UnsupportedInput("edge operator".into()));
                    }
                    scan = scan.filter(Expression::equal(
                        Expression::Column(self.stored_column(edge, column)?),
                        Expression::Integer(value),
                    ));
                }
            }
            let endpoints = [
                (relationship.from.as_str(), start),
                (relationship.to.as_str(), end),
            ];
            let mut pushed = Vec::new();
            for (alias, _) in endpoints {
                for predicate in &node_predicates[alias] {
                    let mut names = Vec::new();
                    predicate.columns(&mut |column| {
                        if let Port::Stored(stored) = column.port {
                            names.push(stored.name());
                        }
                        Ok(())
                    })?;
                    if names.len() != 1
                        || ontology::EDGE_RESERVED_COLUMNS.contains(&names[0])
                        || matches!(names[0], "id" | "_deleted" | "_version")
                        || !input
                            .nodes
                            .iter()
                            .find(|node| node.id == alias)
                            .is_some_and(|node| node.filters.contains_key(names[0]))
                        || self
                            .catalog
                            .stored_table(table)
                            .and_then(|table| table.column(names[0]))
                            .is_none()
                    {
                        continue;
                    }
                    let name = names[0];
                    let predicate = predicate.rebind(&|_| self.stored_column(edge, name))?;
                    if !pushed.contains(&predicate) {
                        pushed.push(predicate.clone());
                        scan = scan.filter(predicate);
                    }
                }
            }
            for (alias, column) in endpoints {
                let node = input
                    .nodes
                    .iter()
                    .find(|node| node.id == alias)
                    .ok_or(GraphError::MissingOutput)?;
                if node.id_property == "id" {
                    for predicate in
                        Expression::identity_predicates(self.stored_column(edge, column)?, node)
                    {
                        scan = scan.filter(predicate);
                    }
                }
                let ordered = input
                    .order_by
                    .as_ref()
                    .is_some_and(|order| order.node == alias);
                let needs_values = ordered
                    || input
                        .aggregation
                        .group_by
                        .iter()
                        .any(|group| group.node() == alias)
                    || input.aggregation.metrics.iter().any(|metric| {
                        metric.expr.node() == alias && metric.expr.property().is_some()
                    })
                    || input.join_predicates.iter().any(|predicate| {
                        predicate.lhs_node == alias || predicate.rhs_node == alias
                    });
                let filter_only =
                    !needs_values && !node.filters.is_empty() && input.relationships.len() >= 2;
                let selective = !node.node_ids.is_empty()
                    || node.id_range.is_some()
                    || node.filters.keys().any(|property| {
                        self.catalog
                            .property(entity(alias).unwrap(), property)
                            .is_some_and(|property| {
                                self.catalog.property_selectivity(property.id)
                                    == ontology::FieldSelectivity::High
                            })
                    });
                if needs_values && selective || filter_only {
                    let first = !filter_keys.contains_key(alias);
                    let candidate = if let Some(candidate) = filter_keys.get(alias) {
                        *candidate
                    } else if elided.contains(alias) {
                        let relation = relations[alias];
                        let body = relation.block;
                        let output = self.project(
                            body,
                            "id",
                            Expression::Column(self.stored_column(relation, "id")?),
                        )?;
                        let definition =
                            self.define(root, body, format!("_filter_{alias}"), false)?;
                        let candidate = (definition, output);
                        filter_keys.insert(alias, candidate);
                        candidate
                    } else {
                        let candidate = self.candidate(
                            root,
                            relations[alias],
                            "id",
                            &node_predicates[alias],
                            &[],
                            &format!("_filter_{alias}"),
                        )?;
                        let body = candidate.1.block;
                        let relation = self.input_node(
                            body,
                            input
                                .nodes
                                .iter()
                                .position(|node| node.id == alias)
                                .unwrap(),
                        )?;
                        if filter_only {
                            *self
                                .operation_mut(body)?
                                .source_mut(relation)
                                .ok_or(GraphError::MissingOutput)? =
                                PhysicalOperation::current(relation);
                        } else {
                            let version = self.stored_column(relation, "_version")?;
                            let operation = self.operation_mut(body)?;
                            *operation = std::mem::replace(operation, PhysicalOperation::One)
                                .latest(version, None);
                        }
                        filter_keys.insert(alias, candidate);
                        candidate
                    };
                    if first || !filter_only {
                        scan =
                            self.narrow(root, scan, self.stored_column(edge, column)?, candidate)?;
                    }
                }
            }
            let mut predicates = Vec::new();
            let mut memberships = Vec::new();
            let mut filtered = &scan;
            loop {
                match filtered {
                    PhysicalOperation::Filter { input, predicate } => {
                        predicates.push(predicate.clone());
                        filtered = input;
                    }
                    PhysicalOperation::Join {
                        left,
                        right,
                        kind: JoinKind::Membership,
                        condition,
                    } => {
                        let (
                            PhysicalOperation::Source { relation, .. },
                            Expression::Equal(value, key),
                        ) = (right.as_ref(), condition)
                        else {
                            return Err(GraphError::JoinShape);
                        };
                        let (
                            Source::Definition(definition),
                            Expression::Column(value),
                            Expression::Column(key),
                        ) = (
                            self.relation(*relation)?.source,
                            value.as_ref(),
                            key.as_ref(),
                        )
                        else {
                            return Err(GraphError::JoinShape);
                        };
                        let (Port::Stored(stored), Port::Output(output)) = (value.port, key.port)
                        else {
                            return Err(GraphError::JoinShape);
                        };
                        memberships.push((stored.name(), (definition, output), *relation));
                        filtered = left;
                    }
                    _ => break,
                }
            }
            predicates.reverse();
            memberships.reverse();
            key_scans.push(KeyScan {
                relation: edge,
                predicates,
                memberships: memberships
                    .iter()
                    .map(|(name, key, _)| (*name, *key))
                    .collect(),
            });
            if input.relationships.len() > 1 {
                let deletion = Expression::equal(
                    Expression::Column(self.stored_column(edge, "_deleted")?),
                    Expression::Boolean(false),
                );
                let mut current = PhysicalOperation::current(edge);
                let mut outside = Vec::new();
                for predicate in &key_scans[index].predicates {
                    if *predicate == deletion {
                        continue;
                    }
                    let mut endpoint_only = true;
                    predicate.columns(&mut |column| { endpoint_only &= matches!(column.port, Port::Stored(stored) if stored.name() == start || stored.name() == end); Ok(()) })?;
                    if endpoint_only {
                        current = current.filter(predicate.clone());
                    } else {
                        outside.push(predicate.clone());
                    }
                }
                let narrow_inside = self
                    .catalog
                    .table_sort_key(table)
                    .is_some_and(|keys| keys.iter().take(4).any(|key| key == start || key == end));
                if narrow_inside {
                    for (column, (_, output), relation) in &memberships {
                        current = current.membership(
                            self.stored_column(edge, column)?,
                            self.output_column(*relation, *output)?,
                        );
                    }
                }
                let previous =
                    edges
                        .last()
                        .and_then(|(_, ends): &(RelationId, [(&str, &str); 2])| {
                            ends.iter().find_map(|(alias, previous_column)| {
                                endpoints
                                    .iter()
                                    .find(|(current, _)| current == alias)
                                    .map(|(_, current_column)| (*previous_column, *current_column))
                            })
                        });
                let cascade = if let Some((previous_column, current_column)) = previous {
                    self.cascade_keys(root, input, &key_scans[..index], previous_column)?
                        .map(|key| {
                            (
                                self.stored_column(edge, current_column)
                                    .expect("edge endpoint"),
                                key,
                            )
                        })
                } else {
                    None
                };
                if narrow_inside && let Some((value, key)) = cascade {
                    current = current.membership(value, key);
                }
                scan = current.filter(deletion).materialize(edge);
                if !narrow_inside {
                    for (column, (_, output), relation) in &memberships {
                        scan = scan.membership(
                            self.stored_column(edge, column)?,
                            self.output_column(*relation, *output)?,
                        );
                    }
                }
                if !narrow_inside && let Some((value, key)) = cascade {
                    scan = scan.membership(value, key);
                }
                for predicate in outside {
                    scan = scan.filter(predicate);
                }
            }
            if index == 0 {
                operation = scan;
            } else {
                let (previous, previous_column, current_column) = edges
                    .iter()
                    .rev()
                    .find_map(|(previous, ends): &(RelationId, [(&str, &str); 2])| {
                        ends.iter().find_map(|(node, column)| {
                            endpoints
                                .iter()
                                .find(|(next, _)| next == node)
                                .map(|(_, next_column)| (*previous, *column, *next_column))
                        })
                    })
                    .ok_or(GraphError::JoinShape)?;
                operation = operation.join(
                    scan,
                    Expression::equal(
                        Expression::Column(self.stored_column(previous, previous_column)?),
                        Expression::Column(self.stored_column(edge, current_column)?),
                    ),
                );
            }
            edges.push((edge, endpoints));
        }
        for node in &input.nodes {
            if elided.contains(node.id.as_str()) {
                continue;
            }
            let (index, edge, column) = edges
                .iter()
                .enumerate()
                .find_map(|(index, (edge, endpoints))| {
                    endpoints
                        .iter()
                        .find(|(name, _)| *name == node.id)
                        .map(|(_, column)| (index, *edge, *column))
                })
                .ok_or(GraphError::JoinShape)?;
            let relation = relations[node.id.as_str()];
            let identity = self.stored_column(relation, &node.id_property)?;
            let mut source = node_operations
                .remove(node.id.as_str())
                .expect("declared node");
            let needs_values = input
                .order_by
                .as_ref()
                .is_some_and(|order| order.node == node.id)
                || input
                    .aggregation
                    .group_by
                    .iter()
                    .any(|group| group.node() == node.id)
                || input.aggregation.metrics.iter().any(|metric| {
                    metric.expr.node() == node.id && metric.expr.property().is_some()
                });
            let convergent = input
                .relationships
                .iter()
                .filter(|relationship| relationship.to == node.id)
                .count()
                > 1;
            if needs_values
                && !convergent
                && !filter_keys.is_empty()
                && !filter_keys.contains_key(node.id.as_str())
            {
                let keys = self.edge_keys(
                    input,
                    &key_scans[..=index],
                    column,
                    "id",
                    format!("e{index}n"),
                )?;
                let label = format!("_narrow_{}", node.id);
                let definition = self.define(root, keys.0, label, false)?;
                let mut narrowed = self.narrow(
                    root,
                    PhysicalOperation::source(relation),
                    identity,
                    (definition, keys.1),
                )?;
                let Source::Stored(table) = self.relation(relation)?.source else {
                    return Err(GraphError::MissingOutput);
                };
                for predicate in &node_predicates[node.id.as_str()] {
                    let mut immutable = true;
                    predicate.columns(&mut |column| { immutable &= matches!(column.port, Port::Stored(stored) if self.catalog.in_sort_key(table.name(), stored.name())); Ok(()) })?;
                    if immutable {
                        narrowed = narrowed.filter(predicate.clone());
                    }
                }
                narrowed = narrowed.latest(self.stored_column(relation, "_version")?, None);
                for predicate in &node_predicates[node.id.as_str()] {
                    narrowed = narrowed.filter(predicate.clone());
                }
                source = narrowed.materialize(relation);
            }
            operation = operation.join(
                source,
                Expression::equal(
                    Expression::Column(identity),
                    Expression::Column(self.stored_column(edge, column)?),
                ),
            );
        }
        Ok(operation)
    }
}
