use super::*;
use ontology::constants::DEFAULT_PRIMARY_KEY;

pub fn optimize(mut bound: BoundCatalog, mut logical: LogicalPlan) -> (BoundCatalog, LogicalPlan) {
    if bound
        .input
        .relationships
        .iter()
        .any(|edge| edge.fk_column.is_some())
    {
        return (bound, logical);
    }
    if bound.input.query_type != crate::input::QueryType::Aggregation {
        loop {
            let previous = logical.root.clone();
            logical.root = rewrite(logical.root, &mut bound, BTreeSet::new());
            if logical.root == previous {
                break;
            }
        }
    }
    if matches!(bound.input.query_type, crate::input::QueryType::Traversal) {
        logical.root = add_sip(logical.root, &mut bound);
    }
    (bound, logical)
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
        .filter_map(|column| bound.columns.get(column).map(|column| column.relation))
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
        let Some(primary_key) = bound.column_ids.get(&ColumnKey {
            relation,
            name: DEFAULT_PRIMARY_KEY.into(),
        }) else {
            continue;
        };
        let Some((condition_index, consumer)) =
            conditions
                .iter()
                .enumerate()
                .find_map(|(condition_index, condition)| {
                    let (left, right) = equality(condition)?;
                    if left == *primary_key {
                        Some((condition_index, right))
                    } else if right == *primary_key {
                        Some((condition_index, left))
                    } else {
                        None
                    }
                })
        else {
            continue;
        };
        let node = &bound.input.nodes[node_index];
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
                    Expr::Column(*primary_key),
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
            let left_relation = bound.columns[&left].relation;
            let right_relation = bound.columns[&right].relation;
            let (producer_column, consumer_column) = match (
                selective.contains(&left_relation),
                selective.contains(&right_relation),
            ) {
                (true, false) => (left, right),
                (false, true) => (right, left),
                _ => continue,
            };
            let producer_relation = bound.columns[&producer_column].relation;
            let consumer_relation = bound.columns[&consumer_column].relation;
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
    match bound.relations[&relation].origin {
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
    let RelationOrigin::Node { input } = bound.relations.get(&relation)?.origin else {
        return None;
    };
    Some(input.0)
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
