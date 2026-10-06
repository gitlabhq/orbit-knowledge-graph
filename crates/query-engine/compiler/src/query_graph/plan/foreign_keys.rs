use super::*;
use crate::input::{Direction, Input, QueryType};
use std::collections::HashMap;

use super::access::{AccessPlan, entity};

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn narrow_star<'input>(
        &mut self,
        input: &'input Input,
        plan: &mut AccessPlan<'input, 'catalog>,
    ) -> Result<()> {
        let root = plan.root;
        let relations = &mut plan.relations;
        let node_operations = &mut plan.node_operations;
        let node_predicates = &mut plan.node_predicates;
        let elided = &mut plan.elided;
        let keys = &mut plan.keys;
        let star_center = plan.star_center;
        let entity = |alias: &str| entity(input, alias);
        let center = star_center.expect("FK star holder");
        let center_node = input
            .nodes
            .iter()
            .find(|node| node.id == center)
            .expect("center input");
        let center_selective = !center_node.node_ids.is_empty()
            || center_node.filters.keys().any(|property| {
                self.catalog
                    .property(entity(center).unwrap(), property)
                    .is_some_and(|property| {
                        self.catalog.property_selectivity(property.id)
                            == ontology::FieldSelectivity::High
                    })
            });
        let mut candidates = HashMap::new();
        let mut center_memberships = Vec::new();
        let mut targets = input
            .relationships
            .iter()
            .zip(keys.iter())
            .map(|(relationship, key)| {
                let target = if relationship.from == center {
                    relationship.to.as_str()
                } else {
                    relationship.from.as_str()
                };
                (target, key.as_ref().expect("star key"))
            })
            .collect::<Vec<_>>();
        targets.sort_by_key(|(target, _)| *target);
        for (target, key) in &targets {
            let node = input
                .nodes
                .iter()
                .find(|node| node.id == *target)
                .expect("target input");
            let holder_column = self
                .catalog
                .property_column(key.property)
                .ok_or(GraphError::MissingOutput)?;
            let target_column = self
                .catalog
                .property_column(key.referenced_key)
                .ok_or(GraphError::MissingOutput)?;
            if target_column == "id" && !node.node_ids.is_empty() {
                node_predicates
                    .get_mut(center)
                    .unwrap()
                    .push(Expression::membership(
                        self.stored_column(relations[center], holder_column)?,
                        &node.node_ids,
                    ));
            }
            if elided.contains(target) {
                continue;
            }
            if !node.filters.is_empty() || !node.node_ids.is_empty() {
                let candidate = self.candidate(
                    root,
                    relations[target],
                    target_column,
                    &node_predicates[target],
                    &[],
                    &format!("_candidate_{target}"),
                )?;
                candidates.insert(*target, candidate);
                center_memberships.push((holder_column, candidate));
            }
        }
        let mut center_operation = PhysicalOperation::current(relations[center]);
        for predicate in &node_predicates[center] {
            center_operation = center_operation.filter(predicate.clone());
        }
        if !center_memberships.is_empty() {
            let candidate = self.candidate(
                root,
                relations[center],
                "id",
                &node_predicates[center],
                &center_memberships,
                &format!("_candidate_{center}"),
            )?;
            center_operation = self.narrow(
                root,
                center_operation,
                self.stored_column(relations[center], "id")?,
                candidate,
            )?;
        }
        node_operations.insert(center, center_operation.materialize(relations[center]));
        for (target, key) in targets {
            let node = input
                .nodes
                .iter()
                .find(|node| node.id == target)
                .expect("target input");
            if elided.contains(target) {
                let holder_column = self
                    .catalog
                    .property_column(key.property)
                    .ok_or(GraphError::MissingOutput)?;
                let holder = self.stored_column(relations[center], holder_column)?;
                if !node.filters.is_empty() || node.id_range.is_some() {
                    let body = self.select(PhysicalOperation::One);
                    let relation = self.scan(
                        body,
                        self.catalog
                            .entity_table(entity(target)?)
                            .ok_or(GraphError::MissingOutput)?,
                        target,
                    )?;
                    self.bind_scan(
                        relation,
                        ScanInput::Node(
                            input
                                .nodes
                                .iter()
                                .position(|node| node.id == target)
                                .unwrap(),
                        ),
                    )?;
                    let source = self.node_source(relation, node)?;
                    *self.operation_mut(body)? = source;
                    let output = self.project(
                        body,
                        "id",
                        Expression::Column(self.stored_column(relation, "id")?),
                    )?;
                    let definition = self.define(root, body, format!("_filter_{target}"), false)?;
                    let source = node_operations.remove(center).expect("center operation");
                    let source = self.narrow(root, source, holder, (definition, output))?;
                    node_operations.insert(center, source);
                }
                continue;
            }
            let target_column = self
                .catalog
                .property_column(key.referenced_key)
                .ok_or(GraphError::MissingOutput)?;
            let candidate = if let Some(candidate) = candidates.get(target) {
                Some(*candidate)
            } else if input.query_type == QueryType::Traversal
                && center_selective
                && node.filters.is_empty()
                && node.node_ids.is_empty()
            {
                let holder_column = self
                    .catalog
                    .property_column(key.property)
                    .ok_or(GraphError::MissingOutput)?;
                Some(self.candidate(
                    root,
                    relations[center],
                    holder_column,
                    &node_predicates[center],
                    &center_memberships,
                    &format!("_narrow_{target}"),
                )?)
            } else {
                None
            };
            if let Some(candidate) = candidate {
                let relation = relations[target];
                let mut source = self.narrow(
                    root,
                    PhysicalOperation::source(relation),
                    self.stored_column(relation, target_column)?,
                    candidate,
                )?;
                let table = self
                    .catalog
                    .entity_table(entity(target)?)
                    .ok_or(GraphError::MissingOutput)?;
                let sort_key = self
                    .catalog
                    .table_sort_key(table)
                    .ok_or(GraphError::LatestShape)?;
                for predicate in &node_predicates[target] {
                    let mut immutable = true;
                    predicate.columns(&mut |column| { immutable &= matches!(column.port, Port::Stored(stored) if sort_key.iter().any(|key| key == stored.name())); Ok(()) })?;
                    if immutable {
                        source = source.filter(predicate.clone());
                    }
                }
                source = source.latest(self.stored_column(relation, "_version")?, None);
                for predicate in &node_predicates[target] {
                    source = source.filter(predicate.clone());
                }
                node_operations.insert(target, source.materialize(relation));
            }
        }
        Ok(())
    }

    pub(super) fn join_foreign_keys<'input>(
        &mut self,
        input: &'input Input,
        plan: &mut AccessPlan<'input, 'catalog>,
    ) -> Result<PhysicalOperation<'catalog>> {
        let relations = &mut plan.relations;
        let node_operations = &mut plan.node_operations;
        let elided = &mut plan.elided;
        let keys = &mut plan.keys;
        let star_center = plan.star_center;
        let star = plan.star;
        let first = if star {
            star_center.expect("FK star holder")
        } else {
            input.relationships[0].from.as_str()
        };
        let mut operation = node_operations.remove(first).expect("declared node");
        let mut reached = HashSet::from([first]);
        for (relationship, key) in input.relationships.iter().zip(keys.iter()) {
            let key = key.expect("eligible FK");
            let holder_is_from = matches!(
                (relationship.direction, key.holder),
                (Direction::Outgoing, query_data_model::Endpoint::Source)
                    | (Direction::Incoming, query_data_model::Endpoint::Target)
            );
            let (holder, target) = if holder_is_from {
                (&relationship.from, &relationship.to)
            } else {
                (&relationship.to, &relationship.from)
            };
            let next = if reached.contains(relationship.from.as_str()) {
                &relationship.to
            } else {
                &relationship.from
            };
            if elided.contains(target.as_str()) {
                reached.insert(target.as_str());
                continue;
            }
            let holder_column = self
                .catalog
                .property_column(key.property)
                .ok_or(GraphError::MissingOutput)?;
            let target_column = self
                .catalog
                .property_column(key.referenced_key)
                .ok_or(GraphError::MissingOutput)?;
            let holder =
                Expression::Column(self.stored_column(relations[holder.as_str()], holder_column)?);
            let target =
                Expression::Column(self.stored_column(relations[target.as_str()], target_column)?);
            let condition = if star {
                Expression::equal(target, holder)
            } else {
                Expression::equal(holder, target)
            };
            operation = if let Some(next) = node_operations.remove(next.as_str()) {
                operation.join(next, condition)
            } else {
                operation.filter(condition)
            };
            reached.insert(next.as_str());
        }
        Ok(operation)
    }
}
