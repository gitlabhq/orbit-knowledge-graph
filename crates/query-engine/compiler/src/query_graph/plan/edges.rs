use super::access::{AccessPlan, entity, needs_values};
use super::keys::KeyRead;
use super::*;
use crate::input::{
    Direction, Input, InputNode, InputRelationship, QueryType, RelationshipSelection,
};
use std::collections::HashMap;

struct EdgeAccess<'input, 'catalog> {
    relation: RelationId,
    table: &'catalog str,
    endpoints: [(&'input str, &'static str); 2],
    predicates: Vec<Expression<'catalog>>,
    memberships: Vec<(&'static str, (DefinitionId, OutputId))>,
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub(super) fn join_edges<'input>(
        &mut self,
        input: &'input Input,
        plan: &mut AccessPlan<'input, 'a>,
    ) -> Result<PhysicalOperation<'a>> {
        let variable = input
            .relationships
            .iter()
            .any(|relationship| relationship.hops.max > 1);
        let mut order = (0..input.relationships.len()).collect::<Vec<_>>();
        let selectivity = |alias: &str| {
            input
                .nodes
                .iter()
                .find(|node| node.id == alias)
                .map(crate::passes::plan::edge_chain::Selectivity::from_node)
        };
        if let (Some(first), Some(last)) = (input.relationships.first(), input.relationships.last())
            && selectivity(&last.to) < selectivity(&first.from)
        {
            order.reverse();
        }
        let mut tagged = HashSet::new();
        let mut candidates = HashMap::new();
        let mut edges: Vec<EdgeAccess<'input, 'a>> = Vec::new();
        let mut keys = Vec::new();
        let mut operation = None;
        for (position, index) in order.into_iter().enumerate() {
            let relationship = &input.relationships[index];
            let mut access =
                self.edge_access(input, plan, relationship, index, position, &mut tagged)?;
            for (alias, column) in access.endpoints {
                let node = input
                    .nodes
                    .iter()
                    .find(|node| node.id == alias)
                    .ok_or(GraphError::MissingOutput)?;
                if node.id_property == "id" {
                    access.predicates.extend(Expression::identity_predicates(
                        self.column(access.relation, column)?,
                        node,
                    ));
                }
                let needed = needs_values(input, node);
                let filter_only = !needed
                    && !node.filters.is_empty()
                    && input.relationships.len() >= 2
                    && plan.relations.contains_key(alias);
                if variable || !(needed && self.selective_node(node) || filter_only) {
                    continue;
                }
                let first = !candidates.contains_key(alias);
                let candidate = if let Some(candidate) = candidates.get(alias) {
                    *candidate
                } else {
                    let candidate = if plan.elided.contains(alias) {
                        let body = plan.relations[alias].block;
                        let output = self
                            .outputs(body)?
                            .next()
                            .ok_or(GraphError::EmptyProjection)?;
                        (
                            self.define(plan.root, body, format!("_filter_{alias}"))?,
                            output,
                        )
                    } else {
                        self.candidate(
                            plan.root,
                            (
                                plan.relations[alias],
                                if filter_only {
                                    KeyRead::Current
                                } else {
                                    KeyRead::Latest
                                },
                            ),
                            "id",
                            &plan.node_predicates[alias],
                            &[],
                            &format!("_filter_{alias}"),
                        )?
                    };
                    candidates.insert(alias, candidate);
                    candidate
                };
                if first || !filter_only {
                    access.memberships.push((column, candidate));
                }
            }
            keys.push(KeyScan {
                relation: access.relation,
                predicates: access.predicates.clone(),
                memberships: access.memberships.clone(),
            });
            let cascade = if input.relationships.len() > 1 && !variable {
                if let Some(previous) = edges.last().and_then(|edge| shared_endpoint(edge, &access))
                {
                    self.cascade_keys(plan.root, input, &keys[..position], previous.0)?
                        .map(|key| {
                            self.stored_column(access.relation, previous.1)
                                .map(|value| (value, key))
                        })
                        .transpose()?
                } else {
                    None
                }
            } else {
                None
            };
            if input.query_type == QueryType::Aggregation
                && input.relationships.len() == 1
                && !variable
            {
                plan.aggregate_condition = access
                    .predicates
                    .iter()
                    .cloned()
                    .reduce(|left, right| Expression::And(Box::new(left), Box::new(right)));
            }
            let source = self.edge_operation(plan.root, input, &access, variable, cascade)?;
            operation = Some(if let Some(left) = operation {
                let (previous, left_column, right_column) = edges
                    .iter()
                    .rev()
                    .find_map(|previous| {
                        shared_endpoint(previous, &access)
                            .map(|(left, right)| (previous.relation, left, right))
                    })
                    .ok_or(GraphError::JoinShape)?;
                self.join_relations(
                    left,
                    source,
                    JoinKind::Inner,
                    Expression::equal(
                        Expression::Column(self.column(previous, left_column)?),
                        Expression::Column(self.column(access.relation, right_column)?),
                    ),
                )?
            } else {
                source
            });
            edges.push(access);
        }
        let mut operation = operation.ok_or(GraphError::JoinShape)?;
        for node in &input.nodes {
            if plan.elided.contains(node.id.as_str()) {
                continue;
            }
            let (position, edge, column) = edges
                .iter()
                .enumerate()
                .find_map(|(position, edge)| {
                    edge.endpoints
                        .iter()
                        .find(|(alias, _)| *alias == node.id)
                        .map(|(_, column)| (position, edge.relation, *column))
                })
                .ok_or(GraphError::JoinShape)?;
            let relation = plan.relations[node.id.as_str()];
            let identity = self.stored_column(relation, &node.id_property)?;
            let mut source = plan
                .node_operations
                .remove(node.id.as_str())
                .ok_or(GraphError::MissingOutput)?;
            let needed = input
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
            if !variable
                && needed
                && !convergent
                && !candidates.is_empty()
                && !candidates.contains_key(node.id.as_str())
            {
                let (body, output) = self.edge_keys(
                    plan.root,
                    input,
                    &keys[..=position],
                    column,
                    "id",
                    format!("e{position}n"),
                )?;
                let definition = self.define(plan.root, body, format!("_narrow_{}", node.id))?;
                source = self.narrowed_node(
                    plan.root,
                    relation,
                    identity,
                    (definition, output),
                    &plan.node_predicates[node.id.as_str()],
                )?;
            }
            operation = self.join_relations(
                operation,
                source,
                JoinKind::Inner,
                Expression::equal(
                    Expression::Column(identity),
                    Expression::Column(self.column(edge, column)?),
                ),
            )?;
        }
        Ok(operation)
    }

    fn selective_node(&self, node: &InputNode) -> bool {
        !node.node_ids.is_empty()
            || node.id_range.is_some()
            || node.filters.keys().any(|property| {
                self.catalog
                    .property(node.entity.as_deref().unwrap_or_default(), property)
                    .is_some_and(|property| {
                        self.catalog.property_selectivity(property.id)
                            == ontology::FieldSelectivity::High
                    })
            })
    }

    fn edge_access<'input>(
        &mut self,
        input: &'input Input,
        plan: &AccessPlan<'input, 'a>,
        relationship: &'input InputRelationship,
        index: usize,
        position: usize,
        tagged: &mut HashSet<(&'input str, &'input str)>,
    ) -> Result<EdgeAccess<'input, 'a>> {
        if relationship.direction == Direction::Both {
            return Err(GraphError::UnsupportedInput(
                "bidirectional traversal".into(),
            ));
        }
        let table = self
            .catalog
            .relationship_table_for_query(relationship.types.as_slice());
        let relation = if relationship.hops.max > 1 {
            self.hop_relation(plan.root, relationship, index)?
        } else {
            let relation = self.scan(plan.root, table, format!("e{position}"))?;
            self.bind_scan(relation, ScanInput::Relationship(index))?;
            relation
        };
        let (start, end) = relationship.direction.edge_columns();
        let endpoints = [
            (relationship.from.as_str(), start),
            (relationship.to.as_str(), end),
        ];
        let mut predicates = Vec::new();
        if let RelationshipSelection::Kinds(kinds) = &relationship.types {
            let value = Expression::Column(self.column(relation, "relationship_kind")?);
            predicates.push(if let [kind] = kinds.as_slice() {
                Expression::equal(value, Expression::Text(kind.clone()))
            } else {
                Expression::In(
                    Box::new(value),
                    Box::new(Expression::Strings(kinds.clone())),
                )
            });
        }
        let (source, target) = if relationship.direction == Direction::Incoming {
            (&relationship.to, &relationship.from)
        } else {
            (&relationship.from, &relationship.to)
        };
        for (column, alias) in [("source_kind", source), ("target_kind", target)] {
            predicates.push(Expression::equal(
                Expression::Column(self.column(relation, column)?),
                Expression::Text(entity(input, alias)?.into()),
            ));
        }
        predicates.push(Expression::equal(
            Expression::Column(self.column(relation, ontology::DELETED_COLUMN)?),
            Expression::Boolean(false),
        ));
        for (column, filters) in &relationship.filters {
            let stored = self
                .catalog
                .stored_table(table)
                .and_then(|table| table.column(column))
                .ok_or(GraphError::MissingOutput)?;
            for filter in filters {
                predicates.push(self.filter_predicate(
                    self.column(relation, column)?,
                    stored,
                    filter,
                )?);
            }
        }
        for (alias, _) in endpoints {
            let node = input
                .nodes
                .iter()
                .find(|node| node.id == alias)
                .ok_or(GraphError::MissingOutput)?;
            let mut properties = node.filters.iter().collect::<Vec<_>>();
            properties.sort_by_key(|(name, _)| *name);
            for (property, filters) in properties {
                if let Some((column, groups)) =
                    super::predicates::edge_tag(self.catalog, node, property, filters, relationship)
                    && tagged.insert((alias, property.as_str()))
                {
                    for values in groups {
                        predicates.push(if values.is_empty() {
                            Expression::Boolean(false)
                        } else {
                            Expression::HasAny(
                                Box::new(Expression::Column(self.column(relation, column)?)),
                                Box::new(Expression::Array(
                                    values.into_iter().map(Expression::Text).collect(),
                                )),
                            )
                        });
                    }
                }
            }
        }
        let mut pushed = Vec::new();
        for (alias, _) in endpoints {
            let node = input
                .nodes
                .iter()
                .find(|node| node.id == alias)
                .ok_or(GraphError::MissingOutput)?;
            for predicate in &plan.node_predicates[alias] {
                let mut names = Vec::new();
                predicate.columns(&mut |column| {
                    if let Port::Stored(stored) = column.port {
                        names.push(stored.name());
                    }
                    Ok(())
                })?;
                let [name] = names.as_slice() else { continue };
                if ontology::EDGE_RESERVED_COLUMNS.contains(name)
                    || matches!(*name, "id" | "_deleted" | "_version")
                    || !node.filters.contains_key(*name)
                    || self
                        .catalog
                        .stored_table(table)
                        .and_then(|table| table.column(name))
                        .is_none()
                {
                    continue;
                }
                let predicate = predicate.rebind(&|_| self.column(relation, name))?;
                if !pushed.contains(&predicate) {
                    pushed.push(predicate);
                }
            }
        }
        predicates.extend(pushed);
        Ok(EdgeAccess {
            relation,
            table,
            endpoints,
            predicates,
            memberships: vec![],
        })
    }

    fn edge_operation(
        &mut self,
        root: BlockId,
        input: &Input,
        access: &EdgeAccess<'_, 'a>,
        variable: bool,
        cascade: Option<(ColumnRef<'a>, ColumnRef<'a>)>,
    ) -> Result<PhysicalOperation<'a>> {
        let relation = access.relation;
        if variable || input.relationships.len() == 1 && input.query_type != QueryType::Aggregation
        {
            let mode = if input.relationships.len() == 1
                || matches!(self.relation(relation)?.source, Source::Derived(_))
            {
                ReadMode::Raw
            } else {
                ReadMode::Current
            };
            let mut operation = self.read_relation(relation, mode)?;
            for predicate in &access.predicates {
                operation = self.filter_relation(operation, predicate.clone())?;
            }
            for (column, key) in &access.memberships {
                operation = self.narrow(root, operation, self.column(relation, column)?, *key)?;
            }
            return Ok(operation);
        }
        let deleted = self.stored_column(relation, ontology::DELETED_COLUMN)?;
        let deletion = Expression::equal(Expression::Column(deleted), Expression::Boolean(false));
        let latest = input.relationships.len() == 1;
        let mut operation = self.read_relation(
            relation,
            if latest {
                ReadMode::Raw
            } else {
                ReadMode::Current
            },
        )?;
        let mut outside = Vec::new();
        for predicate in &access.predicates {
            if *predicate == deletion {
                continue;
            }
            let inside = if latest {
                self.sort_key_predicate(predicate)?
            } else {
                let mut endpoint_only = true;
                predicate.columns(&mut |column| {
                    endpoint_only &= matches!(column.port, Port::Stored(stored) if access.endpoints.iter().any(|(_, name)| *name == stored.name()));
                    Ok(())
                })?;
                endpoint_only
            };
            if inside {
                operation = self.filter_relation(operation, predicate.clone())?;
            } else {
                outside.push(predicate);
            }
        }
        let narrow_inside = latest
            || self
                .catalog
                .table_sort_key(access.table)
                .is_some_and(|keys| {
                    keys.iter()
                        .take(4)
                        .any(|key| access.endpoints.iter().any(|(_, name)| key == name))
                });
        if narrow_inside {
            operation = self.edge_memberships(root, operation, access, cascade)?;
        }
        operation = if latest {
            self.latest_relation(
                operation,
                self.stored_column(relation, ontology::VERSION_COLUMN)?,
                Some(deleted),
            )?
        } else {
            self.materialize_relation(self.filter_relation(operation, deletion)?, relation)?
        };
        if !narrow_inside {
            operation = self.edge_memberships(root, operation, access, cascade)?;
        }
        for predicate in outside {
            operation = self.filter_relation(operation, predicate.clone())?;
        }
        Ok(operation)
    }

    fn edge_memberships(
        &mut self,
        root: BlockId,
        mut operation: PhysicalOperation<'a>,
        access: &EdgeAccess<'_, 'a>,
        cascade: Option<(ColumnRef<'a>, ColumnRef<'a>)>,
    ) -> Result<PhysicalOperation<'a>> {
        for (column, candidate) in &access.memberships {
            operation = self.narrow(
                root,
                operation,
                self.stored_column(access.relation, column)?,
                *candidate,
            )?;
        }
        if let Some((value, key)) = cascade {
            operation = self.membership_relation(operation, value, key)?;
        }
        Ok(operation)
    }
}

fn shared_endpoint(
    left: &EdgeAccess<'_, '_>,
    right: &EdgeAccess<'_, '_>,
) -> Option<(&'static str, &'static str)> {
    left.endpoints.iter().find_map(|(alias, column)| {
        right
            .endpoints
            .iter()
            .find(|(candidate, _)| candidate == alias)
            .map(|(_, next)| (*column, *next))
    })
}
