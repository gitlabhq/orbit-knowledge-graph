use super::*;
use ontology::constants::DEFAULT_PRIMARY_KEY;

pub fn optimize(mut bound: BoundCatalog, mut logical: LogicalPlan) -> (BoundCatalog, LogicalPlan) {
    elide_scope_implied_relationship(&bound, &mut logical);
    prune_fk_aggregation_leaves(&mut bound, &mut logical);
    defer_traversal_outputs(&bound, &mut logical);
    if bound
        .input
        .relationships
        .iter()
        .any(|edge| edge.fk_column.is_some())
    {
        return (bound, logical);
    }
    loop {
        let previous = logical.root.clone();
        logical.root = rewrite(logical.root, &mut bound, BTreeSet::new());
        if logical.root == previous {
            break;
        }
    }
    if matches!(bound.input.query_type, crate::input::QueryType::Traversal) {
        logical.root = add_sip(logical.root, &mut bound);
    }
    (bound, logical)
}

fn defer_traversal_outputs(bound: &BoundCatalog, logical: &mut LogicalPlan) {
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
            .all(|relationship| relationship.fk_column.is_some())
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
    bound: &BoundCatalog,
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
            !matches!(bound.relation(bound.column(id).relation).origin, RelationOrigin::Node { input } if deferred.contains(&input))
        });
    }
    plan
}

fn prune_fk_aggregation_leaves(bound: &mut BoundCatalog, logical: &mut LogicalPlan) {
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
                    relationship.fk_column.is_some()
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
                .input
                .entity_auth
                .get(node.entity.as_deref().unwrap_or_default())
                .is_some_and(|auth| {
                    auth.required_access_level > crate::types::DEFAULT_PATH_ACCESS_LEVEL
                })
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
        let Some(foreign_key) = relationship.fk_column.as_deref() else {
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
            bound
                .ontology
                .denormalized_properties()
                .iter()
                .any(|definition| {
                    definition.node_kind == entity
                        && definition.property_name == *property
                        && definition.direction == direction
                        && relationship.types.contains(&definition.relationship_kind)
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
        let node_holds_key = bound
            .ontology
            .get_node(node.entity.as_deref().unwrap_or_default())
            .is_some_and(|entity| {
                entity
                    .storage
                    .columns
                    .iter()
                    .any(|column| column.name == foreign_key)
            });
        let other_holds_key = bound
            .ontology
            .get_node(other.entity.as_deref().unwrap_or_default())
            .is_some_and(|entity| {
                entity
                    .storage
                    .columns
                    .iter()
                    .any(|column| column.name == foreign_key)
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
                RelationOrigin::Node { input } if input == InputNodeId(node_index) => {
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

fn relationship_carries_node_filter(
    bound: &BoundCatalog,
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
        bound
            .ontology
            .denormalized_properties()
            .iter()
            .any(|definition| {
                definition.node_kind == entity
                    && definition.property_name == *property
                    && definition.direction == direction
                    && relationship.types.contains(&definition.relationship_kind)
            })
    })
}

fn elide_scope_implied_relationship(bound: &BoundCatalog, logical: &mut LogicalPlan) {
    if bound.input.query_type != crate::input::QueryType::Aggregation {
        return;
    }
    let mut connections = BTreeMap::<&str, usize>::new();
    for relationship in &bound.input.relationships {
        *connections.entry(&relationship.from).or_default() += 1;
        *connections.entry(&relationship.to).or_default() += 1;
    }
    let mut eligible = bound
        .input
        .relationships
        .iter()
        .enumerate()
        .filter(|(_, relationship)| relationship.fk_column.is_none());
    let Some((relationship_index, relationship)) = eligible.next() else {
        return;
    };
    if eligible.next().is_some()
        || !relationship.scope_preserving
        || !relationship.filters.is_empty()
    {
        return;
    }
    let Some(proof) = relationship.scope_proof.clone() else {
        return;
    };
    let Some(anchor_index) = bound.input.nodes.iter().position(|node| {
        [&relationship.from, &relationship.to].contains(&&node.id)
            && node.has_traversal_path
            && connections.get(node.id.as_str()) == Some(&1)
            && crate::scope::is_scope_only(node)
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
            && !bound
                .input
                .order_by
                .as_ref()
                .is_some_and(|order| order.node == node.id)
    }) else {
        return;
    };
    let removed: BTreeSet<_> = bound
        .relations
        .iter()
        .filter_map(|(relation, metadata)| match metadata.origin {
            RelationOrigin::Node { input } if input == InputNodeId(anchor_index) => Some(*relation),
            RelationOrigin::Edge {
                input: Some(input), ..
            } if input == InputRelationshipId(relationship_index) => Some(*relation),
            _ => None,
        })
        .collect();
    logical.root = remove_relations(logical.root.clone(), bound, &removed);
    logical.scope_requirements.push(proof);
}

fn remove_relations(
    mut plan: Plan<Logical>,
    bound: &BoundCatalog,
    removed: &BTreeSet<RelationId>,
) -> Plan<Logical> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .filter(|input| relation_id(input).is_none_or(|relation| !removed.contains(&relation)))
        .map(|input| remove_relations(input, bound, removed))
        .collect();
    if let Operator::Join(conditions) = &mut plan.operator {
        conditions.retain(|condition| {
            let mut columns = BTreeSet::new();
            collect_columns(condition, &mut columns);
            columns.iter().all(|column| {
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
    bound: &mut BoundCatalog,
    mut required: BTreeSet<ColumnId>,
) -> Plan<Logical> {
    if !matches!(plan.operator, Operator::Join(_)) {
        expressions(&plan.operator)
            .into_iter()
            .for_each(|expression| collect_columns(expression, &mut required));
    }
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| rewrite(input, bound, required.clone()))
        .collect();
    let Operator::Join(mut conditions) = plan.operator else {
        return plan;
    };
    let required_relations: BTreeSet<_> = required
        .iter()
        .map(|column| bound.column(*column).relation)
        .collect();
    let mut remove = Vec::new();
    let mut filters = Vec::new();
    let mut semi_joins = Vec::new();
    for (index, input) in plan.inputs.iter().enumerate() {
        let Some(relation) = relation_id(input) else {
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
            conditions
                .iter()
                .enumerate()
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
            .input
            .entity_auth
            .get(node.entity.as_deref().unwrap_or_default())
            .is_some_and(|auth| {
                auth.required_access_level > crate::types::DEFAULT_PATH_ACCESS_LEVEL
            });
        if bound.input.query_type == crate::input::QueryType::Aggregation
            && (!elevated
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
        conditions.remove(condition_index);
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
    remove.sort_unstable_by(|left, right| right.cmp(left));
    for index in remove {
        plan.inputs.remove(index);
    }
    let mut plan = if plan.inputs.len() == 1 {
        plan.inputs.pop().unwrap()
    } else {
        Plan {
            operator: Operator::Join(conditions),
            inputs: plan.inputs,
        }
    };
    if !filters.is_empty() {
        plan = Plan::unary(Operator::Filter(and(filters)), plan);
    }
    for (condition, producer) in semi_joins {
        plan = Plan {
            operator: Operator::SemiJoin(condition),
            inputs: vec![plan, producer],
        };
    }
    plan
}

fn add_sip(mut plan: Plan<Logical>, bound: &mut BoundCatalog) -> Plan<Logical> {
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
        .filter_map(|(index, input)| relation_id(input).map(|relation| (relation, index)))
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
            let consumer = plan.inputs[consumer_index].clone();
            plan.inputs[consumer_index] = Plan {
                operator: Operator::SemiJoin(compare(
                    CompareOp::Eq,
                    Expr::Column(consumer_column),
                    Expr::Column(producer_column),
                )),
                inputs: vec![consumer, plan.inputs[producer_index].clone()],
            };
            selective.insert(consumer_relation);
            changed = true;
        }
        if !changed {
            return plan;
        }
    }
}

fn relation_selective(bound: &BoundCatalog, relation: RelationId) -> bool {
    match bound.relation(relation).origin {
        RelationOrigin::Node { input } => {
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

fn collect_columns(expression: &Expr, columns: &mut BTreeSet<ColumnId>) {
    match expression {
        Expr::Column(column) => {
            columns.insert(*column);
        }
        Expr::Compare { left, right, .. } => {
            collect_columns(left, columns);
            collect_columns(right, columns);
        }
        Expr::Filter { left, right, .. } => {
            collect_columns(left, columns);
            right
                .iter()
                .for_each(|right| collect_columns(right, columns));
        }
        Expr::And(expressions)
        | Expr::Or(expressions)
        | Expr::Array(expressions)
        | Expr::Tuple(expressions) => expressions
            .iter()
            .for_each(|expression| collect_columns(expression, columns)),
        Expr::In { value, .. }
        | Expr::DateTrunc { value, .. }
        | Expr::Stringify(value)
        | Expr::ListContains { list: value, .. }
        | Expr::TokenMatch { value, .. } => collect_columns(value, columns),
        Expr::Aggregate { value, .. } => value
            .iter()
            .for_each(|value| collect_columns(value, columns)),
        Expr::JsonObject(entries) => entries
            .iter()
            .for_each(|(_, value)| collect_columns(value, columns)),
        Expr::Output(_) | Expr::Literal(_) => {}
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

fn relation_id(plan: &Plan<Logical>) -> Option<RelationId> {
    match &plan.operator {
        Operator::Scan(scan) => Some(scan.relation),
        Operator::Bind(relation) => Some(*relation),
        Operator::Filter(_)
        | Operator::Project(_)
        | Operator::Sort(_)
        | Operator::Limit(_)
        | Operator::CurrentRows { .. } => plan.inputs.first().and_then(relation_id),
        _ => None,
    }
}

fn node_index(bound: &BoundCatalog, relation: RelationId) -> Option<usize> {
    bound.node_input(relation).map(|input| input.0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{Direction, InputNode, InputRelationship, QueryType};

    #[test]
    fn prunes_unread_pinned_node_to_edge_filter() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "a".into(),
                    entity: Some("A".into()),
                    node_ids: vec![1],
                    ..Default::default()
                },
                InputNode {
                    id: "b".into(),
                    entity: Some("B".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["REL".into()],
                from: "a".into(),
                to: "b".into(),
                hops: Default::default(),
                direction: Direction::Outgoing,
                filters: Default::default(),
                fk_column: None,
                scope_proof: None,
                scope_preserving: false,
            }],
            ..Default::default()
        };
        let ontology = Arc::new(Ontology::new().with_nodes(["A", "B"]).with_edges(["REL"]));
        let (bound, logical) = bind(input, ontology).unwrap();
        let (_, optimized) = optimize(bound, logical);
        let mut node_scans = 0;
        optimized.root.visit(&mut |plan| {
            if matches!(plan.operator, Operator::Scan(_)) {
                node_scans += 1;
            }
        });
        assert_eq!(node_scans, 1);
    }
}
