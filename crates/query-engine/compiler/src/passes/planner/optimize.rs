use super::*;
use ontology::constants::DEFAULT_PRIMARY_KEY;

pub fn optimize<M: QueryDataModel>(
    mut bound: BoundCatalog<M>,
    mut logical: LogicalPlan,
    scope_proofs: &std::collections::HashMap<String, crate::scope::ScopeProof>,
) -> (BoundCatalog<M>, LogicalPlan) {
    prune_fk_aggregation_leaves(&mut bound, &mut logical);
    elide_scope_container(&mut bound, &mut logical, scope_proofs);
    defer_traversal_outputs(&bound, &mut logical);
    loop {
        let previous = logical.root.clone();
        logical.root = rewrite(logical.root, &mut bound, BTreeSet::new());
        if logical.root == previous {
            break;
        }
    }
    if matches!(bound.input.query_type, crate::input::QueryType::Traversal)
        && bound
            .input
            .relationships
            .iter()
            .all(|relationship| relationship_foreign_key(&bound, relationship).is_none())
    {
        logical.root = add_sip(logical.root, &mut bound);
    }
    (bound, logical)
}

fn elide_scope_container<M: QueryDataModel>(
    bound: &mut BoundCatalog<M>,
    logical: &mut LogicalPlan,
    scope_proofs: &std::collections::HashMap<String, crate::scope::ScopeProof>,
) {
    if bound.input.query_type != crate::input::QueryType::Aggregation
        || bound
            .input
            .relationships
            .iter()
            .filter(|relationship| relationship_foreign_key(bound, relationship).is_none())
            .count()
            != 1
    {
        return;
    }
    let Some((relationship_index, anchor)) =
        bound
            .input
            .relationships
            .iter()
            .enumerate()
            .find_map(|(index, relationship)| {
                if relationship_foreign_key(bound, relationship).is_some()
                    || !relationship.filters.is_empty()
                {
                    return None;
                }
                let from = bound
                    .input
                    .nodes
                    .iter()
                    .find(|node| node.id == relationship.from)?;
                let to = bound
                    .input
                    .nodes
                    .iter()
                    .find(|node| node.id == relationship.to)?;
                let source = from.entity.as_deref()?;
                let target = to.entity.as_deref()?;
                let scope_preserving = relationship.types.iter().all(|kind| {
                    bound
                        .model
                        .variant_scope(kind, source, target)
                        .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                });
                let anchor = [from, to].into_iter().find(|node| {
                    scope_proofs.contains_key(&node.id)
                        && crate::scope::is_scope_only(node)
                        && bound
                            .input
                            .relationships
                            .iter()
                            .filter(|candidate| {
                                candidate.from == node.id || candidate.to == node.id
                            })
                            .count()
                            == 1
                        && !bound
                            .input
                            .aggregation
                            .group_by
                            .iter()
                            .any(|group| group.node() == node.id)
                        && !bound
                            .input
                            .aggregation
                            .metrics
                            .iter()
                            .any(|metric| metric.expr.node() == node.id)
                })?;
                scope_preserving.then_some((index, anchor.id.clone()))
            })
    else {
        return;
    };
    let Some(requirement) = scope_proofs.get(&anchor).cloned() else {
        return;
    };
    let removed: BTreeSet<_> = bound
        .relations()
        .filter_map(|(relation, metadata)| match metadata.origin {
            RelationOrigin::Node { input, .. } if bound.input.nodes[input.0].id == anchor => {
                Some(relation)
            }
            RelationOrigin::Edge {
                input: Some(input), ..
            } if input == InputRelationshipId(relationship_index) => Some(relation),
            _ => None,
        })
        .collect();
    logical.root = remove_relations(logical.root.clone(), bound, &removed);
    logical.scope_requirements.push(requirement);
    bound.remove_relations(&removed);
}

fn defer_traversal_outputs<M: QueryDataModel>(bound: &BoundCatalog<M>, logical: &mut LogicalPlan) {
    if bound.input.query_type != crate::input::QueryType::Traversal
        || bound.input.relationships.is_empty()
        || bound
            .input
            .relationships
            .iter()
            .any(|relationship| relationship.hops.max > 1)
        || bound
            .input
            .relationships
            .iter()
            .all(|relationship| relationship_foreign_key(bound, relationship).is_some())
    {
        return;
    }
    let deferred: BTreeSet<_> = bound
        .input
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| node.filters.is_empty())
        .map(|(index, _)| InputNodeId(index))
        .collect();
    logical.root = remove_deferred_outputs(logical.root.clone(), bound, &deferred);
}

fn remove_deferred_outputs(
    mut plan: Plan<Logical>,
    bound: &BoundCatalog<impl QueryDataModel>,
    deferred: &BTreeSet<InputNodeId>,
) -> Plan<Logical> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| remove_deferred_outputs(input, bound, deferred))
        .collect();
    if let Operator::Project(columns) = &mut plan.operator {
        columns.retain(|column| {
            let Expr::Column(id) = column.expression else {
                return true;
            };
            !matches!(bound.relation(bound.column(id).relation).origin, RelationOrigin::Node { input, .. } if deferred.contains(&input))
        });
    }
    plan
}

fn prune_fk_aggregation_leaves<M: QueryDataModel>(
    bound: &mut BoundCatalog<M>,
    logical: &mut LogicalPlan,
) {
    if bound.input.query_type != crate::input::QueryType::Aggregation {
        return;
    }
    let mut removed = BTreeSet::new();
    let protected: BTreeSet<_> = bound
        .input
        .nodes
        .iter()
        .filter(|node| !node.filters.is_empty())
        .flat_map(|node| {
            bound
                .input
                .relationships
                .iter()
                .filter(move |relationship| relationship.from == node.id)
                .filter(|relationship| {
                    relationship_foreign_key(bound, relationship).is_some()
                        && relationship_carries_node_filter(bound, relationship, node)
                })
                .map(|relationship| relationship.from.as_str())
        })
        .collect();
    for (node_index, node) in bound.input.nodes.iter().enumerate() {
        if bound.input.relationships.len() > 1
            && bound.input.nodes.iter().any(|candidate| {
                !candidate.filters.is_empty()
                    && protected.contains(candidate.id.as_str())
                    && bound
                        .input
                        .relationships
                        .iter()
                        .filter(|relationship| relationship.from == candidate.id)
                        .count()
                        > 1
            })
        {
            continue;
        }
        if !node.filters.is_empty()
            || !node.node_ids.is_empty()
            || node.id_range.is_some()
            || bound
                .input
                .aggregation
                .group_by
                .iter()
                .any(|group| group.node() == node.id)
            || bound
                .model
                .entity_minimum_access_level(node.entity.as_deref().unwrap_or_default())
                .is_some_and(|level| level > crate::types::DEFAULT_PATH_ACCESS_LEVEL)
        {
            continue;
        }
        let relationships: Vec<_> = bound
            .input
            .relationships
            .iter()
            .enumerate()
            .filter(|(_, relationship)| relationship.from == node.id || relationship.to == node.id)
            .collect();
        if protected.contains(node.id.as_str())
            && relationships.iter().any(|(_, relationship)| {
                let other = if relationship.from == node.id {
                    relationship.to.as_str()
                } else {
                    relationship.from.as_str()
                };
                protected.contains(other)
            })
        {
            continue;
        }
        let [(relationship_index, relationship)] = relationships.as_slice() else {
            continue;
        };
        let Some(foreign_key) = relationship_foreign_key(bound, relationship) else {
            continue;
        };
        let other_name = if relationship.from == node.id {
            &relationship.to
        } else {
            &relationship.from
        };
        if node.filters.keys().any(|property| {
            let Some(entity) = node.entity.as_deref() else {
                return false;
            };
            let direction = if relationship.from == node.id {
                ontology::DenormDirection::Source
            } else {
                ontology::DenormDirection::Target
            };
            denormalized_property(bound, entity, property, direction).is_some_and(|definition| {
                relationship.types.iter().any(|kind| {
                    bound
                        .model
                        .graph()
                        .relationship_id(kind)
                        .is_some_and(|relationship| definition.carries(relationship))
                })
            })
        }) {
            continue;
        }
        let Some(other) = bound
            .input
            .nodes
            .iter()
            .find(|candidate| candidate.id == *other_name)
        else {
            continue;
        };
        let node_holds_key = node.entity.as_deref().is_some_and(|entity| {
            bound.model.graph().entity_id(entity) == Some(foreign_key.holder)
        });
        let other_holds_key = other.entity.as_deref().is_some_and(|entity| {
            bound.model.graph().entity_id(entity) == Some(foreign_key.holder)
        });
        if node_holds_key || !other_holds_key {
            continue;
        }
        let metrics_supported = bound
            .input
            .aggregation
            .metrics
            .iter()
            .filter(|metric| metric.expr.node() == node.id)
            .all(|metric| {
                metric.expr.function() == crate::input::AggFunction::Count
                    && metric
                        .expr
                        .property()
                        .is_none_or(|property| property == DEFAULT_PRIMARY_KEY)
            });
        if !metrics_supported {
            continue;
        }
        removed.extend(bound.relations().filter_map(
            |(relation, metadata)| match metadata.origin {
                RelationOrigin::Node { input, .. } if input == InputNodeId(node_index) => {
                    Some(relation)
                }
                RelationOrigin::Edge {
                    input: Some(input), ..
                } if input == InputRelationshipId(*relationship_index) => Some(relation),
                _ => None,
            },
        ));
    }
    if removed.is_empty() {
        return;
    }
    logical.root = remove_relations(logical.root.clone(), bound, &removed).map_expressions(
        &mut |expression| match expression {
            Expr::Aggregate {
                function: crate::input::AggFunction::Count,
                value: Some(value),
            } if matches!(value.as_ref(), Expr::Column(column) if removed.contains(&bound.column(*column).relation)) => {
                Expr::Aggregate {
                    function: crate::input::AggFunction::Count,
                    value: None,
                }
            }
            expression => expression,
        },
    );
    bound.remove_relations(&removed);
}

fn relationship_carries_node_filter<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    relationship: &crate::input::InputRelationship,
    node: &crate::input::InputNode,
) -> bool {
    let Some(entity) = node.entity.as_deref() else {
        return false;
    };
    let direction = if relationship.from == node.id {
        ontology::DenormDirection::Source
    } else {
        ontology::DenormDirection::Target
    };
    node.filters.keys().any(|property| {
        denormalized_property(bound, entity, property, direction.clone()).is_some_and(
            |definition| {
                relationship.types.iter().any(|kind| {
                    bound
                        .model
                        .graph()
                        .relationship_id(kind)
                        .is_some_and(|relationship| definition.carries(relationship))
                })
            },
        )
    })
}

fn remove_relations(
    mut plan: Plan<Logical>,
    bound: &BoundCatalog<impl QueryDataModel>,
    removed: &BTreeSet<RelationId>,
) -> Plan<Logical> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .filter(|input| {
            input
                .relation()
                .is_none_or(|relation| !removed.contains(&relation))
        })
        .map(|input| remove_relations(input, bound, removed))
        .collect();
    if let Operator::Join(conditions) = &mut plan.operator {
        conditions.retain(|condition| {
            condition.columns().iter().all(|column| {
                bound
                    .columns
                    .get(column)
                    .is_none_or(|column| !removed.contains(&column.relation))
            })
        });
        if plan.inputs.len() == 1 {
            return plan.inputs.pop().unwrap();
        }
    }
    plan
}

fn rewrite(
    mut plan: Plan<Logical>,
    bound: &mut BoundCatalog<impl QueryDataModel>,
    mut required: BTreeSet<ColumnId>,
) -> Plan<Logical> {
    if !matches!(plan.operator, Operator::Join(_)) {
        expressions(&plan.operator)
            .into_iter()
            .for_each(|expression| required.extend(expression.columns()));
    }
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| rewrite(input, bound, required.clone()))
        .collect();
    if !matches!(plan.operator, Operator::Join(_)) {
        return plan;
    }
    let mut join = JoinEditor::new(plan).unwrap();
    let required_relations: BTreeSet<_> = required
        .iter()
        .map(|column| bound.column(*column).relation)
        .collect();
    let mut remove = Vec::new();
    let mut condition_remove = Vec::new();
    let mut filters = Vec::new();
    let mut semi_joins = Vec::new();
    for (index, input) in join.inputs().iter().enumerate() {
        let Some(relation) = input.relation() else {
            continue;
        };
        let Some(node_index) = node_index(bound, relation) else {
            continue;
        };
        if required_relations.contains(&relation) {
            continue;
        }
        let Some(primary_key) = bound.column_id(relation, DEFAULT_PRIMARY_KEY) else {
            continue;
        };
        let Some((condition_index, consumer)) =
            join.condition_entries()
                .find_map(|(condition_index, condition)| {
                    let (left, right) = equality(condition)?;
                    if left == primary_key {
                        Some((condition_index, right))
                    } else if right == primary_key {
                        Some((condition_index, left))
                    } else {
                        None
                    }
                })
        else {
            continue;
        };
        let node = &bound.input.nodes[node_index];
        let elevated = bound
            .model
            .entity_minimum_access_level(node.entity.as_deref().unwrap_or_default())
            .is_some_and(|level| level > crate::types::DEFAULT_PATH_ACCESS_LEVEL);
        let scoped = node
            .entity
            .as_deref()
            .is_some_and(|entity| bound.model.entity_has_traversal_path(entity));
        if bound.input.query_type == crate::input::QueryType::Aggregation
            && ((!elevated && scoped)
                || !node.filters.is_empty()
                || node.id_range.is_some()
                || node.node_ids.is_empty())
        {
            continue;
        }
        if bound.input.query_type == crate::input::QueryType::Aggregation
            && bound
                .input
                .aggregation
                .group_by
                .iter()
                .any(|group| group.node() == node.id)
        {
            continue;
        }
        condition_remove.push(condition_index);
        remove.push(index);
        if node.filters.is_empty() && node.id_range.is_none() && !node.node_ids.is_empty() {
            filters.push(match node.node_ids.as_slice() {
                [id] => compare(
                    CompareOp::Eq,
                    Expr::Column(consumer),
                    Expr::Literal(Value::Int(*id)),
                ),
                ids => Expr::In {
                    value: Box::new(Expr::Column(consumer)),
                    values: ids.iter().copied().map(Value::Int).collect(),
                    data_type: Some(ontology::DataType::Int),
                },
            });
        } else if !node.filters.is_empty() || node.id_range.is_some() {
            semi_joins.push((
                compare(
                    CompareOp::Eq,
                    Expr::Column(consumer),
                    Expr::Column(primary_key),
                ),
                input.clone(),
            ));
        }
    }
    condition_remove.sort_unstable_by(|left, right| right.cmp(left));
    for index in condition_remove {
        join.remove_condition(index);
    }
    join.remove_inputs(remove);
    let mut plan = join.finish();
    if !filters.is_empty() {
        plan = Plan::unary(Operator::Filter(and(filters)), plan);
    }
    for (condition, producer) in semi_joins {
        plan = plan.semi_join(producer, condition);
    }
    plan
}

pub(super) fn add_sip<F: Flavor>(
    mut plan: Plan<F>,
    bound: &mut BoundCatalog<impl QueryDataModel>,
) -> Plan<F> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| add_sip(input, bound))
        .collect();
    let Operator::Join(conditions) = &plan.operator else {
        return plan;
    };
    let relations: BTreeMap<_, _> = plan
        .inputs
        .iter()
        .enumerate()
        .filter_map(|(index, input)| input.relation().map(|relation| (relation, index)))
        .collect();
    let mut selective: BTreeSet<_> = relations
        .keys()
        .copied()
        .filter(|relation| relation_selective(bound, *relation))
        .collect();
    loop {
        let mut changed = false;
        for condition in conditions {
            let Some((left, right)) = equality(condition) else {
                continue;
            };
            let left_relation = bound.column(left).relation;
            let right_relation = bound.column(right).relation;
            let (producer_column, consumer_column) = match (
                selective.contains(&left_relation),
                selective.contains(&right_relation),
            ) {
                (true, false) => (left, right),
                (false, true) => (right, left),
                _ => continue,
            };
            let producer_relation = bound.column(producer_column).relation;
            let consumer_relation = bound.column(consumer_column).relation;
            let (Some(&producer_index), Some(&consumer_index)) = (
                relations.get(&producer_relation),
                relations.get(&consumer_relation),
            ) else {
                continue;
            };
            if matches!(plan.inputs[consumer_index].operator, Operator::SemiJoin(_)) {
                selective.insert(consumer_relation);
                continue;
            }
            if bound.input.query_type == crate::input::QueryType::Aggregation
                && projection_requires_relation(bound, &plan, consumer_relation)
                && bound
                    .input
                    .aggregation
                    .group_by
                    .iter()
                    .any(|group| {
                        bound
                            .node_input(consumer_relation)
                            .is_some_and(|input| bound.input.nodes[input.0].id == group.node())
                    })
            {
                selective.insert(consumer_relation);
                continue;
            }
            let consumer = plan.inputs[consumer_index].clone();
            plan.inputs[consumer_index] = consumer.semi_join(
                plan.inputs[producer_index].clone(),
                compare(
                    CompareOp::Eq,
                    Expr::Column(consumer_column),
                    Expr::Column(producer_column),
                ),
            );
            selective.insert(consumer_relation);
            changed = true;
        }
        if !changed {
            return plan;
        }
    }
}

fn projection_requires_relation<F: Flavor>(
    bound: &BoundCatalog<impl QueryDataModel>,
    plan: &Plan<F>,
    relation: RelationId,
) -> bool {
    let required: BTreeSet<_> = expressions(&plan.operator)
        .into_iter()
        .flat_map(Expr::columns)
        .map(|column| bound.column(column).relation)
        .collect();
    required.contains(&relation)
}

fn relation_selective(bound: &BoundCatalog<impl QueryDataModel>, relation: RelationId) -> bool {
    match bound.relation(relation).origin {
        RelationOrigin::Node { input, .. } => {
            let node = &bound.input.nodes[input.0];
            !node.node_ids.is_empty() || node.id_range.is_some() || !node.filters.is_empty()
        }
        RelationOrigin::Edge {
            input: Some(input), ..
        } => {
            let edge = &bound.input.relationships[input.0];
            !edge.filters.is_empty()
                || [&edge.from, &edge.to].into_iter().any(|name| {
                    bound
                        .input
                        .nodes
                        .iter()
                        .find(|node| node.id == **name)
                        .is_some_and(|node| {
                            !node.node_ids.is_empty()
                                || node.id_range.is_some()
                                || !node.filters.is_empty()
                        })
                })
        }
        RelationOrigin::Edge { input: None, .. } => false,
    }
}

fn expressions<F: Flavor>(operator: &Operator<F>) -> Vec<&Expr> {
    match operator {
        Operator::Filter(expression) | Operator::SemiJoin(expression) => vec![expression],
        Operator::Project(columns) => columns.iter().map(|column| &column.expression).collect(),
        Operator::Join(conditions) => conditions.iter().collect(),
        Operator::Aggregate { groups, metrics } => groups
            .iter()
            .chain(metrics)
            .map(|column| &column.expression)
            .collect(),
        Operator::Sort(keys) => keys.iter().map(|key| &key.expression).collect(),
        Operator::CurrentRows { keys, .. } => keys.iter().collect(),
        _ => vec![],
    }
}

fn equality(expression: &Expr) -> Option<(ColumnId, ColumnId)> {
    let Expr::Compare {
        op: CompareOp::Eq,
        left,
        right,
    } = expression
    else {
        return None;
    };
    let (Expr::Column(left), Expr::Column(right)) = (left.as_ref(), right.as_ref()) else {
        return None;
    };
    Some((*left, *right))
}

fn node_index(bound: &BoundCatalog<impl QueryDataModel>, relation: RelationId) -> Option<usize> {
    bound.node_input(relation).map(|input| input.0)
}

fn relationship_foreign_key<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    relationship: &crate::input::InputRelationship,
) -> Option<query_data_model::ForeignKey> {
    let (source, target) = match relationship.direction {
        crate::input::Direction::Outgoing => (&relationship.from, &relationship.to),
        crate::input::Direction::Incoming => (&relationship.to, &relationship.from),
        crate::input::Direction::Both => return None,
    };
    let source = bound
        .input
        .nodes
        .iter()
        .find(|node| node.id == *source)?
        .entity
        .as_deref()?;
    let target = bound
        .input
        .nodes
        .iter()
        .find(|node| node.id == *target)?
        .entity
        .as_deref()?;
    bound.model.foreign_key(&relationship.types, source, target)
}

fn denormalized_property<'a, M: QueryDataModel>(
    bound: &'a BoundCatalog<M>,
    entity: &str,
    property: &str,
    direction: ontology::DenormDirection,
) -> Option<&'a query_data_model::DenormalizedProperty> {
    let entity = bound.model.graph().entity_id(entity)?;
    let property = bound.model.graph().property_id(entity, property)?;
    let direction = match direction {
        ontology::DenormDirection::Source => query_data_model::DenormalizedDirection::Source,
        ontology::DenormDirection::Target => query_data_model::DenormalizedDirection::Target,
    };
    bound
        .model
        .denormalized()
        .property(query_data_model::DenormalizedKey {
            property,
            direction,
        })
}

fn compare(op: CompareOp, left: Expr, right: Expr) -> Expr {
    Expr::Compare {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn and(mut expressions: Vec<Expr>) -> Expr {
    if expressions.len() == 1 {
        expressions.pop().unwrap()
    } else {
        Expr::And(expressions)
    }
}
