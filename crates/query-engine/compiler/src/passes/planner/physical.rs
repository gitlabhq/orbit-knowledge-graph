use super::*;
use crate::error::Result;
use ontology::constants::DEFAULT_PRIMARY_KEY;

pub fn plan_clickhouse(
    bound: &BoundCatalog,
    logical: LogicalPlan,
) -> Result<PlanningResult<ClickHouse>> {
    let accesses = clickhouse_accesses(bound);
    let plan = map_clickhouse(bound, &logical.root, &accesses, false);
    let ordinary = candidate(bound, plan, true);
    let mut candidates = vec![ordinary.clone()];
    if let Some(candidate) = foreign_key_candidate(bound, ordinary.clone()) {
        candidates.push(candidate);
    }
    candidates.push(text_index_candidate(bound, ordinary.clone()));
    candidates.push(edge_property_candidate(bound, ordinary));
    if let Some(candidate) = denormalized_join_candidate(bound, &logical) {
        candidates.push(candidate);
    }
    let candidate = candidates
        .into_iter()
        .min_by_key(|candidate| candidate.cost)
        .unwrap();
    Ok(PlanningResult {
        logical,
        selected: SelectedPlan { candidate },
    })
}

fn denormalized_join_candidate(
    bound: &BoundCatalog,
    logical: &LogicalPlan,
) -> Option<Candidate<ClickHouse>> {
    if !matches!(
        bound.input.query_type,
        crate::input::QueryType::Traversal | crate::input::QueryType::Aggregation
    ) || bound.input.relationships.is_empty()
    {
        return None;
    }
    let denormalized = bound.ontology.denormalized_joins().iter().find(|join| {
        join.hops.len() == bound.input.relationships.len()
            && join
                .hops
                .iter()
                .zip(&bound.input.relationships)
                .all(|(hop, relationship)| {
                    relationship.hops.max == 1
                        && relationship.types.as_slice() == [hop.relationship_kind.as_str()]
                        && bound
                            .input
                            .nodes
                            .iter()
                            .find(|node| node.id == relationship.from)
                            .and_then(|node| node.entity.as_deref())
                            == Some(hop.source_kind.as_str())
                        && bound
                            .input
                            .nodes
                            .iter()
                            .find(|node| node.id == relationship.to)
                            .and_then(|node| node.entity.as_deref())
                            == Some(hop.target_kind.as_str())
                })
    })?;
    let mut table_for_relation = BTreeMap::new();
    for hop in &denormalized.hops {
        let relationship = bound.input.relationships.iter().find(|relationship| {
            relationship.types.as_slice() == [hop.relationship_kind.as_str()]
        })?;
        table_for_relation.insert(node_relation(bound, &relationship.from)?, hop.source_table);
        table_for_relation.insert(node_relation(bound, &relationship.to)?, hop.target_table);
        if let Some(edge_table) = hop.edge_table {
            let edge_relation = bound.relations.iter().find_map(|(relation, metadata)| {
                let RelationOrigin::Edge {
                    input: Some(input), ..
                } = metadata.origin
                else {
                    return None;
                };
                (bound.input.relationships[input.0].types.as_slice()
                    == [hop.relationship_kind.as_str()])
                .then_some(*relation)
            })?;
            table_for_relation.insert(edge_relation, edge_table);
        }
    }
    let scan_relation = *table_for_relation.keys().next()?;
    let columns: BTreeMap<_, _> = bound
        .columns
        .iter()
        .filter_map(|(column, metadata)| {
            table_for_relation.get(&metadata.relation).map(|table| {
                (
                    *column,
                    PhysicalColumn(denormalized.column_for(*table, &metadata.name)),
                )
            })
        })
        .collect();
    let access = DenormalizedAccess {
        scan_relation,
        layout: TableLayout {
            table: TableName(denormalized.table.clone()),
            columns: columns.values().cloned().collect(),
            sort_key: denormalized
                .sort_key()
                .into_iter()
                .map(PhysicalColumn)
                .collect(),
            global: false,
        },
        relations: table_for_relation.keys().copied().collect(),
        columns: columns.clone(),
        residual_filters: vec![],
    };
    let mapped = map_clickhouse(bound, &logical.root, &clickhouse_accesses(bound), false);
    let mut candidate = candidate(bound, replace_with_denormalized(mapped, &access), false);
    candidate.columns.columns = columns
        .keys()
        .copied()
        .map(|column| (column, Expr::Column(column)))
        .collect();
    candidate.cost = plan_cost(&candidate.plan);
    Some(candidate)
}

fn replace_with_denormalized(
    mut plan: Plan<ClickHouse>,
    access: &DenormalizedAccess,
) -> Plan<ClickHouse> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| replace_with_denormalized(input, access))
        .collect();
    if let Operator::Join(_) = plan.operator {
        let mut kept = Vec::new();
        let mut inserted = false;
        for input in plan.inputs {
            if relation_id_ch(&input).is_some_and(|relation| access.relations.contains(&relation)) {
                if !inserted {
                    kept.push(Plan::leaf(Operator::Scan(PhysicalScan {
                        relation: access.scan_relation,
                        access: ClickHouseAccess::DenormalizedJoin(access.clone()),
                        columns: access.columns.keys().copied().collect(),
                    })));
                    inserted = true;
                }
            } else {
                kept.push(input);
            }
        }
        if kept.len() == 1 {
            return kept.pop().unwrap();
        }
        plan.inputs = kept;
    }
    plan
}

fn text_index_candidate(
    bound: &BoundCatalog,
    mut candidate: Candidate<ClickHouse>,
) -> Candidate<ClickHouse> {
    let indexed: BTreeSet<_> = bound
        .columns
        .iter()
        .filter_map(|(column, metadata)| {
            let relation = &bound.relations[&metadata.relation];
            let entity = relation.entity?;
            bound
                .ontology
                .text_index_tokenizer(&bound.entities[&entity].name, &metadata.name)
                .map(|_| *column)
        })
        .collect();
    candidate.plan = candidate
        .plan
        .map_expressions(&mut |expression| match expression {
            Expr::Filter {
                op: FilterOp::Contains,
                left,
                right: Some(right),
                ..
            } if matches!(left.as_ref(), Expr::Column(column) if indexed.contains(column)) => {
                if let Expr::Literal(token) = *right {
                    Expr::TokenMatch { value: left, token }
                } else {
                    Expr::Filter {
                        op: FilterOp::Contains,
                        left,
                        right: Some(right),
                        data_type: Some(ontology::DataType::String),
                    }
                }
            }
            expression => expression,
        });
    candidate.cost = plan_cost(&candidate.plan);
    candidate.cost.residual_filters = candidate.cost.residual_filters.saturating_sub(1);
    candidate
}

fn edge_property_candidate(
    bound: &BoundCatalog,
    mut candidate: Candidate<ClickHouse>,
) -> Candidate<ClickHouse> {
    let mut predicates: BTreeMap<RelationId, Vec<Expr>> = BTreeMap::new();
    for (edge_relation, metadata) in &bound.relations {
        let RelationOrigin::Edge {
            input: Some(edge_input),
            ..
        } = metadata.origin
        else {
            continue;
        };
        let edge = &bound.input.relationships[edge_input.0];
        for (node_name, direction) in [
            (&edge.from, ontology::DenormDirection::Source),
            (&edge.to, ontology::DenormDirection::Target),
        ] {
            let Some(node) = bound.input.nodes.iter().find(|node| node.id == *node_name) else {
                continue;
            };
            let Some(entity) = node.entity.as_deref() else {
                continue;
            };
            for (property, filters) in &node.filters {
                for filter in filters {
                    let Some(definition) =
                        bound
                            .ontology
                            .denormalized_properties()
                            .iter()
                            .find(|definition| {
                                definition.node_kind == entity
                                    && definition.property_name == *property
                                    && definition.direction == direction
                                    && edge.types.contains(&definition.relationship_kind)
                            })
                    else {
                        continue;
                    };
                    let Some(value) = filter.value.as_ref() else {
                        continue;
                    };
                    let token = value
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| value.to_string());
                    let list = bound.column_ids.get(&ColumnKey {
                        relation: *edge_relation,
                        name: definition.edge_column.clone(),
                    });
                    let Some(list) = list else {
                        continue;
                    };
                    predicates
                        .entry(*edge_relation)
                        .or_default()
                        .push(Expr::ListContains {
                            list: Box::new(Expr::Column(*list)),
                            value: Value::String(format!("{}:{token}", definition.tag_key)),
                        });
                }
            }
        }
    }
    if predicates.is_empty() {
        return candidate;
    }
    candidate.plan = inject_edge_predicates(candidate.plan, &predicates);
    candidate.cost = plan_cost(&candidate.plan);
    candidate.cost.residual_filters = candidate.cost.residual_filters.saturating_sub(1);
    candidate
}

fn inject_edge_predicates(
    mut plan: Plan<ClickHouse>,
    predicates: &BTreeMap<RelationId, Vec<Expr>>,
) -> Plan<ClickHouse> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| inject_edge_predicates(input, predicates))
        .collect();
    let Operator::Scan(scan) = &plan.operator else {
        return plan;
    };
    let Some(predicates) = predicates.get(&scan.relation) else {
        return plan;
    };
    Plan::unary(
        Operator::Filter(if predicates.len() == 1 {
            predicates[0].clone()
        } else {
            Expr::And(predicates.clone())
        }),
        plan,
    )
}

fn foreign_key_candidate(
    bound: &BoundCatalog,
    mut candidate: Candidate<ClickHouse>,
) -> Option<Candidate<ClickHouse>> {
    let mut substitutions = BTreeMap::new();
    let mut relationships = BTreeSet::new();
    let mut join_conditions = Vec::new();
    for (relation, metadata) in &bound.relations {
        let RelationOrigin::Edge {
            input: Some(input),
            depth,
            ..
        } = metadata.origin
        else {
            continue;
        };
        if depth.is_some_and(|depth| depth > 1) {
            return None;
        }
        let edge = &bound.input.relationships[input.0];
        if !edge.filters.is_empty() || edge.direction == crate::input::Direction::Both {
            return None;
        }
        let foreign_key = edge.fk_column.as_deref()?;
        let from = node_relation(bound, &edge.from)?;
        let to = node_relation(bound, &edge.to)?;
        let from_entity = bound
            .input
            .nodes
            .iter()
            .find(|node| node.id == edge.from)
            .and_then(|node| node.entity.as_deref())
            .unwrap_or_default();
        let from_holds_key = bound.ontology.get_node(from_entity).is_some_and(|node| {
            node.storage
                .columns
                .iter()
                .any(|column| column.name == foreign_key)
        });
        let (holder, referenced) = if from_holds_key {
            (from, to)
        } else {
            (to, from)
        };
        let holder_column = bound.column_ids.get(&ColumnKey {
            relation: holder,
            name: foreign_key.into(),
        })?;
        let referenced_column = bound.column_ids.get(&ColumnKey {
            relation: referenced,
            name: DEFAULT_PRIMARY_KEY.into(),
        })?;
        join_conditions.push(Expr::Compare {
            op: CompareOp::Eq,
            left: Box::new(Expr::Column(*holder_column)),
            right: Box::new(Expr::Column(*referenced_column)),
        });
        let from_id = bound.column_ids.get(&ColumnKey {
            relation: from,
            name: DEFAULT_PRIMARY_KEY.into(),
        })?;
        let to_id = bound.column_ids.get(&ColumnKey {
            relation: to,
            name: DEFAULT_PRIMARY_KEY.into(),
        })?;
        for (name, expression) in [
            (
                ontology::constants::SOURCE_ID_COLUMN,
                Expr::Column(*from_id),
            ),
            (ontology::constants::TARGET_ID_COLUMN, Expr::Column(*to_id)),
            (
                ontology::constants::SOURCE_KIND_COLUMN,
                Expr::Literal(Value::String(from_entity.into())),
            ),
            (
                ontology::constants::TARGET_KIND_COLUMN,
                Expr::Literal(Value::String(
                    bound
                        .input
                        .nodes
                        .iter()
                        .find(|node| node.id == edge.to)
                        .and_then(|node| node.entity.clone())
                        .unwrap_or_default(),
                )),
            ),
            (
                ontology::constants::RELATIONSHIP_KIND_COLUMN,
                Expr::Literal(Value::String(
                    edge.types.first().cloned().unwrap_or_default(),
                )),
            ),
        ] {
            if let Some(column) = bound.column_ids.get(&ColumnKey {
                relation: *relation,
                name: name.into(),
            }) {
                substitutions.insert(*column, expression);
            }
        }
        relationships.insert(*relation);
    }
    if relationships.is_empty() {
        return None;
    }
    candidate.plan = rewrite_fk_plan(
        bound,
        candidate.plan,
        &relationships,
        &join_conditions,
        &substitutions,
    );
    candidate.columns.columns.extend(substitutions.clone());
    candidate.outputs.nodes = candidate
        .outputs
        .nodes
        .into_iter()
        .map(|(node, mut output)| {
            if let Some(Expr::Column(column)) = substitutions.get(&output.primary_key) {
                output.primary_key = *column;
                output.relation = bound.columns[column].relation;
            }
            (node, output)
        })
        .collect();
    candidate.cost = plan_cost(&candidate.plan);
    Some(candidate)
}

fn rewrite_fk_plan(
    bound: &BoundCatalog,
    mut plan: Plan<ClickHouse>,
    relationships: &BTreeSet<RelationId>,
    join_conditions: &[Expr],
    substitutions: &BTreeMap<ColumnId, Expr>,
) -> Plan<ClickHouse> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| rewrite_fk_plan(bound, input, relationships, join_conditions, substitutions))
        .collect();
    if let Operator::Join(conditions) = &mut plan.operator {
        plan.inputs.retain(|input| {
            relation_id_ch(input).is_none_or(|relation| !relationships.contains(&relation))
        });
        for relation in join_conditions {
            if !conditions.contains(relation) {
                conditions.push(relation.clone());
            }
        }
        conditions.retain(|condition| !tautology(condition));
    }
    let mut plan = plan.map_expressions(&mut |expression| match expression {
        Expr::Column(column) => substitutions
            .get(&column)
            .cloned()
            .unwrap_or(Expr::Column(column)),
        expression => expression,
    });
    if let Operator::Join(conditions) = &mut plan.operator {
        conditions.retain(|condition| !tautology(condition));
    }
    plan
}

fn tautology(expression: &Expr) -> bool {
    matches!(expression, Expr::Compare { op: CompareOp::Eq, left, right } if left == right)
}

fn node_relation(bound: &BoundCatalog, name: &str) -> Option<RelationId> {
    bound.relations.iter().find_map(|(relation, metadata)| {
        let RelationOrigin::Node { input } = metadata.origin else {
            return None;
        };
        (bound.input.nodes[input.0].id == name).then_some(*relation)
    })
}

fn relation_id_ch(plan: &Plan<ClickHouse>) -> Option<RelationId> {
    match &plan.operator {
        Operator::Scan(scan) => Some(scan.relation),
        Operator::CurrentRows { .. } | Operator::Filter(_) | Operator::Project(_) => {
            plan.inputs.first().and_then(relation_id_ch)
        }
        _ => None,
    }
}

fn plan_cost<B: Flavor>(plan: &Plan<B>) -> Cost {
    let mut cost = Cost::default();
    plan.visit(&mut |plan| match &plan.operator {
        Operator::Scan(scan) => {
            cost.scans += 1;
            cost.columns_read += scan.column_count() as u32;
        }
        Operator::CurrentRows { strategy, .. } => {
            if format!("{strategy:?}") == "Final" {
                cost.final_reads += 1;
            }
        }
        Operator::Join(_) => cost.joins += 1,
        Operator::SemiJoin(_) => cost.semi_joins += 1,
        Operator::Union => cost.union_arms += plan.inputs.len().saturating_sub(1) as u32,
        Operator::Filter(_) => cost.residual_filters += 1,
        _ => {}
    });
    cost
}

pub fn plan_duckdb(bound: &BoundCatalog, logical: LogicalPlan) -> Result<PlanningResult<DuckDb>> {
    let accesses = duckdb_accesses(bound);
    let plan = map_duckdb(&logical.root, &accesses);
    let candidate = candidate(bound, plan, false);
    Ok(PlanningResult {
        logical,
        selected: SelectedPlan { candidate },
    })
}

fn clickhouse_accesses(
    bound: &BoundCatalog,
) -> BTreeMap<RelationId, PhysicalScan<ClickHouseAccess>> {
    bound
        .relations
        .iter()
        .map(|(relation, metadata)| {
            let access = match metadata.origin {
                RelationOrigin::Node { .. } => ClickHouseAccess::Table(TableAccess {
                    layout: node_layout(bound, *relation),
                }),
                RelationOrigin::Edge { .. } => ClickHouseAccess::EdgeTables(EdgeTableAccess {
                    layouts: edge_layouts(bound, metadata),
                }),
            };
            (
                *relation,
                PhysicalScan {
                    relation: *relation,
                    access,
                    columns: relation_columns(bound, *relation),
                },
            )
        })
        .collect()
}

fn duckdb_accesses(bound: &BoundCatalog) -> BTreeMap<RelationId, PhysicalScan<DuckDbAccess>> {
    bound
        .relations
        .iter()
        .map(|(relation, metadata)| {
            let layout = match metadata.origin {
                RelationOrigin::Node { .. } => node_layout(bound, *relation),
                RelationOrigin::Edge { .. } => edge_layouts(bound, metadata)
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| table_layout(bound, ontology::constants::EDGE_TABLE)),
            };
            (
                *relation,
                PhysicalScan {
                    relation: *relation,
                    access: DuckDbAccess::Table(TableAccess { layout }),
                    columns: relation_columns(bound, *relation),
                },
            )
        })
        .collect()
}

fn node_layout(bound: &BoundCatalog, relation: RelationId) -> TableLayout {
    let entity = bound.relations[&relation].entity.unwrap();
    let entity = &bound.entities[&entity].name;
    let table = bound.ontology.table_name(entity).unwrap_or_default();
    table_layout(bound, table)
}

fn edge_layouts(bound: &BoundCatalog, relation: &BoundRelation) -> Vec<TableLayout> {
    let mut tables: Vec<_> = if relation.relationships.is_empty()
        || relation
            .relationships
            .iter()
            .any(|kind| bound.relationships[kind].name == "*")
    {
        bound.ontology.edge_tables()
    } else {
        relation
            .relationships
            .iter()
            .map(|kind| {
                bound
                    .ontology
                    .edge_table_for_relationship(&bound.relationships[kind].name)
            })
            .collect()
    };
    tables.sort_unstable();
    tables.dedup();
    tables
        .into_iter()
        .map(|table| table_layout(bound, table))
        .collect()
}

fn table_layout(bound: &BoundCatalog, table: &str) -> TableLayout {
    TableLayout {
        table: TableName(table.into()),
        columns: bound
            .input
            .compiler
            .table_columns
            .get(table)
            .into_iter()
            .flatten()
            .cloned()
            .map(PhysicalColumn)
            .collect(),
        sort_key: bound
            .ontology
            .sort_key_for_table(table)
            .unwrap_or_default()
            .iter()
            .cloned()
            .map(PhysicalColumn)
            .collect(),
        global: bound.ontology.is_global_table(table),
    }
}

fn relation_columns(bound: &BoundCatalog, relation: RelationId) -> BTreeSet<ColumnId> {
    bound
        .columns
        .iter()
        .filter_map(|(id, column)| (column.relation == relation).then_some(*id))
        .collect()
}

fn map_clickhouse(
    bound: &BoundCatalog,
    logical: &Plan<Logical>,
    accesses: &BTreeMap<RelationId, PhysicalScan<ClickHouseAccess>>,
    suppress_current_rows: bool,
) -> Plan<ClickHouse> {
    let explicit_current_rows = matches!(logical.operator, Operator::CurrentRows { .. });
    let inputs = logical
        .inputs
        .iter()
        .map(|input| {
            map_clickhouse(
                bound,
                input,
                accesses,
                suppress_current_rows || explicit_current_rows,
            )
        })
        .collect();
    let operator = match &logical.operator {
        Operator::Scan(scan) => Operator::Scan(accesses[&scan.relation].clone()),
        Operator::Filter(expression) => Operator::Filter(expression.clone()),
        Operator::Project(columns) => Operator::Project(columns.clone()),
        Operator::Join(conditions) => Operator::Join(conditions.clone()),
        Operator::SemiJoin(condition) => Operator::SemiJoin(condition.clone()),
        Operator::Aggregate { groups, metrics } => Operator::Aggregate {
            groups: groups.clone(),
            metrics: metrics.clone(),
        },
        Operator::Union => Operator::Union,
        Operator::Bind(relation) => Operator::Bind(*relation),
        Operator::Sort(keys) => Operator::Sort(keys.clone()),
        Operator::Limit(limit) => Operator::Limit(*limit),
        Operator::CurrentRows { keys, .. } => Operator::CurrentRows {
            keys: keys.clone(),
            strategy: ClickHouseCurrentRows::LimitBy,
        },
        _ => unreachable!("ordinary traversal operator"),
    };
    let plan = Plan { operator, inputs };
    if let Operator::Scan(scan) = &plan.operator
        && !suppress_current_rows
        && matches!(
            bound.relations[&scan.relation].origin,
            RelationOrigin::Node { .. }
        )
    {
        let keys = sort_keys(
            bound,
            scan.relation,
            match &scan.access {
                ClickHouseAccess::Table(access) => &access.layout,
                _ => unreachable!(),
            },
        );
        Plan::unary(
            Operator::CurrentRows {
                keys,
                strategy: ClickHouseCurrentRows::Final,
            },
            plan,
        )
    } else {
        plan
    }
}

fn map_duckdb(
    logical: &Plan<Logical>,
    accesses: &BTreeMap<RelationId, PhysicalScan<DuckDbAccess>>,
) -> Plan<DuckDb> {
    let inputs = logical
        .inputs
        .iter()
        .map(|input| map_duckdb(input, accesses))
        .collect();
    let operator = match &logical.operator {
        Operator::Scan(scan) => Operator::Scan(accesses[&scan.relation].clone()),
        Operator::Filter(expression) => Operator::Filter(expression.clone()),
        Operator::Project(columns) => Operator::Project(columns.clone()),
        Operator::Join(conditions) => Operator::Join(conditions.clone()),
        Operator::SemiJoin(condition) => Operator::SemiJoin(condition.clone()),
        Operator::Aggregate { groups, metrics } => Operator::Aggregate {
            groups: groups.clone(),
            metrics: metrics.clone(),
        },
        Operator::Union => Operator::Union,
        Operator::Bind(relation) => Operator::Bind(*relation),
        Operator::Sort(keys) => Operator::Sort(keys.clone()),
        Operator::Limit(limit) => Operator::Limit(*limit),
        Operator::CurrentRows { keys, .. } => Operator::CurrentRows {
            keys: keys.clone(),
            strategy: DuckDbCurrentRows,
        },
        _ => unreachable!("ordinary traversal operator"),
    };
    Plan { operator, inputs }
}

fn sort_keys(bound: &BoundCatalog, relation: RelationId, layout: &TableLayout) -> Vec<Expr> {
    layout
        .sort_key
        .iter()
        .filter_map(|name| {
            bound
                .column_ids
                .get(&ColumnKey {
                    relation,
                    name: name.0.clone(),
                })
                .copied()
                .map(Expr::Column)
        })
        .collect()
}

fn candidate<B: Flavor>(bound: &BoundCatalog, plan: Plan<B>, final_rows: bool) -> Candidate<B>
where
    B::Scan: ScanRelation,
{
    let visible = visible_relations(&plan);
    let outputs = bound
        .relations
        .iter()
        .filter_map(|(relation, metadata)| {
            let RelationOrigin::Node { input } = metadata.origin else {
                return None;
            };
            if !visible.contains(relation) {
                return None;
            }
            let primary_key = bound.column_ids.get(&ColumnKey {
                relation: *relation,
                name: DEFAULT_PRIMARY_KEY.into(),
            })?;
            Some((
                input,
                OutputBinding {
                    relation: *relation,
                    primary_key: *primary_key,
                },
            ))
        })
        .collect();
    let properties = physical_properties(&plan);
    let cost = plan_cost(&plan);
    let mut candidate = Candidate {
        plan,
        columns: ColumnBindings {
            columns: bound
                .columns
                .iter()
                .filter_map(|(id, column)| {
                    visible
                        .contains(&column.relation)
                        .then_some((*id, Expr::Column(*id)))
                })
                .collect(),
        },
        outputs: OutputBindings { nodes: outputs },
        properties,
        cost,
    };
    if !final_rows {
        candidate.cost.final_reads = 0;
    }
    candidate
}

fn physical_properties<B: Flavor>(plan: &Plan<B>) -> PhysicalProperties {
    let mut properties = PhysicalProperties::default();
    plan.visit(&mut |plan| match &plan.operator {
        Operator::Sort(keys) => properties.ordered_by = keys.clone(),
        Operator::CurrentRows { keys, .. } => {
            for key in keys {
                if let Expr::Column(column) = key {
                    properties
                        .current_relations
                        .insert(column_relation(plan, *column));
                }
            }
        }
        _ => {}
    });
    properties
}

fn column_relation<B: Flavor>(plan: &Plan<B>, _column: ColumnId) -> RelationId {
    visible_relations(plan)
        .into_iter()
        .next()
        .unwrap_or(RelationId(0))
}

fn visible_relations<B: Flavor>(plan: &Plan<B>) -> BTreeSet<RelationId> {
    let mut relations = BTreeSet::new();
    plan.visit(&mut |plan| {
        if let Operator::Scan(scan) = &plan.operator {
            relations.insert(scan.relation());
        }
    });
    relations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{Direction, InputNode, InputRelationship, QueryType};

    #[test]
    fn plans_one_hop_for_both_backends() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "u".into(),
                    entity: Some("User".into()),
                    node_ids: vec![1],
                    ..Default::default()
                },
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["AUTHORED".into()],
                from: "u".into(),
                to: "mr".into(),
                hops: Default::default(),
                direction: Direction::Outgoing,
                filters: Default::default(),
                fk_column: None,
                scope_prefix: None,
                scope_preserving: false,
            }],
            ..Default::default()
        };
        let ontology = Ontology::new()
            .with_nodes(["User", "MergeRequest"])
            .with_edges(["AUTHORED"]);
        let (bound, logical) = bind(input, std::sync::Arc::new(ontology)).unwrap();
        assert_eq!(
            plan_clickhouse(&bound, logical.clone())
                .unwrap()
                .selected
                .candidate
                .cost
                .scans,
            3
        );
        assert_eq!(
            plan_duckdb(&bound, logical)
                .unwrap()
                .selected
                .candidate
                .cost
                .scans,
            3
        );
    }

    #[test]
    fn selects_exact_denormalized_join() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "u".into(),
                    entity: Some("User".into()),
                    columns: Some(crate::input::ColumnSelection::List(vec!["username".into()])),
                    ..Default::default()
                },
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    columns: Some(crate::input::ColumnSelection::List(vec!["title".into()])),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["AUTHORED".into()],
                from: "u".into(),
                to: "mr".into(),
                hops: Default::default(),
                direction: Direction::Outgoing,
                filters: Default::default(),
                fk_column: None,
                scope_prefix: None,
                scope_preserving: false,
            }],
            ..Default::default()
        };
        let ontology = Ontology::load_embedded()
            .unwrap()
            .with_denormalized_join("authored", &[("AUTHORED", "User", "MergeRequest", false)]);
        let (bound, logical) = bind(input, Arc::new(ontology)).unwrap();
        let planned = plan_clickhouse(&bound, logical).unwrap();
        assert!(matches!(
            planned.selected.candidate.plan.inputs[0].operator,
            Operator::Project(_)
        ));
        let mut denormalized = false;
        planned.selected.candidate.plan.visit(&mut |plan| {
            if matches!(
                plan.operator,
                Operator::Scan(PhysicalScan {
                    access: ClickHouseAccess::DenormalizedJoin(_),
                    ..
                })
            ) {
                denormalized = true;
            }
        });
        assert!(denormalized);
        let lowered = lower_clickhouse(&bound, planned.selected);
        assert!(lowered.is_ok(), "{lowered:?}");
    }
}
