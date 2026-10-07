use super::access::{AccessPlan, entity};
use super::keys::KeyRead;
use super::*;
use crate::input::{Direction, Input, QueryType};
use std::collections::HashMap;

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub(super) fn narrow_star<'input>(
        &mut self,
        input: &'input Input,
        plan: &mut AccessPlan<'input, 'a>,
    ) -> Result<()> {
        let center = plan.star_center.ok_or(GraphError::JoinShape)?;
        let center_node = input
            .nodes
            .iter()
            .find(|node| node.id == center)
            .ok_or(GraphError::MissingOutput)?;
        let center_selective = !center_node.node_ids.is_empty()
            || center_node.filters.keys().any(|property| {
                self.catalog
                    .property(center_node.entity.as_deref().unwrap_or_default(), property)
                    .is_some_and(|property| {
                        self.catalog.property_selectivity(property.id)
                            == ontology::FieldSelectivity::High
                    })
            });
        let center_relation = plan.relations[center];
        let mut targets = input
            .relationships
            .iter()
            .zip(&plan.keys)
            .map(|(relationship, key)| {
                let alias = if relationship.from == center {
                    relationship.to.as_str()
                } else {
                    relationship.from.as_str()
                };
                Ok((alias, key.ok_or(GraphError::JoinShape)?))
            })
            .collect::<Result<Vec<_>>>()?;
        targets.sort_by_key(|(alias, _)| *alias);
        let mut candidates = HashMap::new();
        let mut memberships = Vec::new();
        for (target, key) in &targets {
            let node = input
                .nodes
                .iter()
                .find(|node| node.id == *target)
                .ok_or(GraphError::MissingOutput)?;
            let holder_column = self
                .catalog
                .property_column(key.property)
                .ok_or(GraphError::MissingOutput)?;
            let target_column = self
                .catalog
                .property_column(key.referenced_key)
                .ok_or(GraphError::MissingOutput)?;
            if target_column == "id" && !node.node_ids.is_empty() {
                let predicate = Expression::membership(
                    self.stored_column(center_relation, holder_column)?,
                    &node.node_ids,
                );
                plan.node_predicates
                    .get_mut(center)
                    .ok_or(GraphError::MissingOutput)?
                    .push(predicate);
            }
            if !plan.elided.contains(target)
                && (!node.filters.is_empty() || !node.node_ids.is_empty())
            {
                let candidate = self.candidate(
                    plan.root,
                    (plan.relations[target], KeyRead::Raw),
                    target_column,
                    &plan.node_predicates[target],
                    &[],
                    &format!("_candidate_{target}"),
                )?;
                candidates.insert(*target, candidate);
                memberships.push((holder_column, candidate));
            }
        }
        let mut operation = self.read_relation(center_relation, ReadMode::Current)?;
        for predicate in &plan.node_predicates[center] {
            operation = self.filter_relation(operation, predicate.clone())?;
        }
        if !memberships.is_empty() {
            let candidate = self.candidate(
                plan.root,
                (center_relation, KeyRead::Raw),
                "id",
                &plan.node_predicates[center],
                &memberships,
                &format!("_candidate_{center}"),
            )?;
            operation = self.narrow(
                plan.root,
                operation,
                self.stored_column(center_relation, "id")?,
                candidate,
            )?;
        }
        plan.node_operations.insert(
            center,
            self.materialize_relation(operation, center_relation)?,
        );
        for (target, key) in targets {
            let (index, node) = input
                .nodes
                .iter()
                .enumerate()
                .find(|(_, node)| node.id == target)
                .ok_or(GraphError::MissingOutput)?;
            let holder_column = self
                .catalog
                .property_column(key.property)
                .ok_or(GraphError::MissingOutput)?;
            if plan.elided.contains(target) {
                if !node.filters.is_empty() || node.id_range.is_some() {
                    let body = self.query_in(plan.root)?;
                    let table = self
                        .catalog
                        .entity_table(entity(input, target)?)
                        .ok_or(GraphError::MissingOutput)?;
                    let scan = self.scan(body, table, target)?;
                    self.bind_scan(scan, ScanInput::Node(index))?;
                    let projection = self.project_values(
                        self.node_source(scan, node)?,
                        [(
                            "id".into(),
                            Expression::Column(self.stored_column(scan, "id")?),
                        )],
                    )?;
                    let output = projection
                        .outputs()
                        .next()
                        .ok_or(GraphError::EmptyProjection)?
                        .0;
                    self.finish_query(projection)?;
                    let definition = self.define(plan.root, body, format!("_filter_{target}"))?;
                    let operation = plan
                        .node_operations
                        .remove(center)
                        .ok_or(GraphError::MissingOutput)?;
                    let operation = self.narrow(
                        plan.root,
                        operation,
                        self.stored_column(center_relation, holder_column)?,
                        (definition, output),
                    )?;
                    plan.node_operations.insert(center, operation);
                }
                continue;
            }
            let candidate = if let Some(candidate) = candidates.get(target) {
                Some(*candidate)
            } else if input.query_type == QueryType::Traversal
                && center_selective
                && node.filters.is_empty()
                && node.node_ids.is_empty()
            {
                Some(self.candidate(
                    plan.root,
                    (center_relation, KeyRead::Raw),
                    holder_column,
                    &plan.node_predicates[center],
                    &memberships,
                    &format!("_narrow_{target}"),
                )?)
            } else {
                None
            };
            if let Some(candidate) = candidate {
                let relation = plan.relations[target];
                let column = self
                    .catalog
                    .property_column(key.referenced_key)
                    .ok_or(GraphError::MissingOutput)?;
                let operation = self.narrowed_node(
                    plan.root,
                    relation,
                    self.stored_column(relation, column)?,
                    candidate,
                    &plan.node_predicates[target],
                )?;
                plan.node_operations.insert(target, operation);
            }
        }
        Ok(())
    }

    pub(super) fn join_foreign_keys<'input>(
        &mut self,
        input: &'input Input,
        plan: &mut AccessPlan<'input, 'a>,
    ) -> Result<PhysicalOperation<'a>> {
        let first = if plan.star {
            plan.star_center.ok_or(GraphError::JoinShape)?
        } else {
            input
                .relationships
                .first()
                .ok_or(GraphError::JoinShape)?
                .from
                .as_str()
        };
        let mut operation = plan
            .node_operations
            .remove(first)
            .ok_or(GraphError::MissingOutput)?;
        let mut reached = HashSet::from([first]);
        for (relationship, key) in input.relationships.iter().zip(&plan.keys) {
            let key = key.ok_or(GraphError::JoinShape)?;
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
            if plan.elided.contains(target.as_str()) {
                reached.insert(target.as_str());
                continue;
            }
            let next = if reached.contains(relationship.from.as_str()) {
                &relationship.to
            } else {
                &relationship.from
            };
            let holder_column = self
                .catalog
                .property_column(key.property)
                .ok_or(GraphError::MissingOutput)?;
            let target_column = self
                .catalog
                .property_column(key.referenced_key)
                .ok_or(GraphError::MissingOutput)?;
            let holder = Expression::Column(
                self.stored_column(plan.relations[holder.as_str()], holder_column)?,
            );
            let target = Expression::Column(
                self.stored_column(plan.relations[target.as_str()], target_column)?,
            );
            let condition = if plan.star {
                Expression::equal(target, holder)
            } else {
                Expression::equal(holder, target)
            };
            operation = if let Some(source) = plan.node_operations.remove(next.as_str()) {
                self.join_relations(operation, source, JoinKind::Inner, condition)?
            } else {
                self.filter_relation(operation, condition)?
            };
            reached.insert(next.as_str());
        }
        Ok(operation)
    }
}
