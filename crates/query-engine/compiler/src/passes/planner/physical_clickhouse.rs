use super::*;
use crate::error::Result;
use ontology::constants::DEFAULT_PRIMARY_KEY;

pub fn plan_clickhouse(
    bound: &BoundCatalog,
    logical: LogicalPlan,
) -> Result<PlanningResult<ClickHouse>> {
    let catalog = clickhouse_catalog(bound);
    let mut plan = map_clickhouse(bound, &logical.root, &catalog, false);
    if bound.input.query_type == crate::input::QueryType::Aggregation
        && bound.input.relationships.len() > 1
    {
        plan = deduplicate_edges(plan, bound);
    }
    let ordinary = clickhouse_candidate(bound, plan);
    if bound.input.query_type == crate::input::QueryType::Aggregation
        && catalog.facts.edge_properties.len() == 1
        && bound.input.relationships.len() > 1
    {
        let edge_property = edge_property_candidate(&catalog, ordinary);
        return Ok(PlanningResult {
            logical,
            selected: SelectedPlan {
                candidate: edge_property,
            },
        });
    }
    let mut candidates = CandidateSet::default();
    candidates.insert(ordinary.clone());
    if let Some(candidate) = foreign_key_candidate(&catalog, ordinary.clone()) {
        candidates.insert(candidate);
    }
    candidates.insert(text_index_candidate(&catalog, ordinary.clone()));
    let edge_property = edge_property_candidate(&catalog, ordinary);
    let edge_count = bound
        .relations
        .values()
        .filter(|metadata| matches!(metadata.origin, RelationOrigin::Edge { input: Some(_), .. }))
        .count();
    if !catalog.facts.edge_properties.is_empty() && catalog.facts.foreign_keys.len() != edge_count {
        return Ok(PlanningResult {
            logical,
            selected: SelectedPlan {
                candidate: edge_property,
            },
        });
    }
    candidates.insert(edge_property);
    for access in &catalog.facts.denormalized_joins {
        candidates.insert(denormalized_join_candidate(&catalog, &logical, access));
    }
    Ok(PlanningResult {
        logical,
        selected: candidates.select().unwrap(),
    })
}

fn deduplicate_edges(mut plan: Plan<ClickHouse>, bound: &BoundCatalog) -> Plan<ClickHouse> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| deduplicate_edges(input, bound))
        .collect();
    let Operator::Scan(scan) = &plan.operator else {
        return plan;
    };
    if matches!(
        bound.relations[&scan.relation].origin,
        RelationOrigin::Edge { .. }
    ) {
        Plan::unary(
            Operator::CurrentRows {
                keys: vec![],
                strategy: ClickHouseCurrentRows::Final,
            },
            plan,
        )
    } else {
        plan
    }
}

fn denormalized_join_candidate(
    catalog: &BackendCatalog<'_, ClickHouse>,
    logical: &LogicalPlan,
    access: &DenormalizedAccess,
) -> Candidate<ClickHouse> {
    let mapped = map_clickhouse(catalog.bound, &logical.root, catalog, false);
    let mut candidate =
        clickhouse_candidate(catalog.bound, replace_with_denormalized(mapped, access));
    candidate.columns.columns = access
        .columns
        .keys()
        .copied()
        .map(|column| (column, Expr::Column(column)))
        .collect();
    candidate.cost = plan_cost(&candidate.plan);
    candidate
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
    catalog: &BackendCatalog<'_, ClickHouse>,
    mut candidate: Candidate<ClickHouse>,
) -> Candidate<ClickHouse> {
    let indexed: BTreeSet<_> = catalog
        .facts
        .text_indexes
        .iter()
        .map(|index| index.column)
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
    catalog: &BackendCatalog<'_, ClickHouse>,
    mut candidate: Candidate<ClickHouse>,
) -> Candidate<ClickHouse> {
    let mut predicates: BTreeMap<RelationId, Vec<Expr>> = BTreeMap::new();
    for access in &catalog.facts.edge_properties {
        predicates
            .entry(access.edge)
            .or_default()
            .push(Expr::ListContains {
                list: Box::new(Expr::Column(access.column)),
                values: access.tokens.clone(),
            });
    }
    if predicates.len() > 1 {
        let selected: BTreeSet<_> = catalog
            .facts
            .edge_properties
            .iter()
            .map(|access| access.edge)
            .collect();
        predicates.retain(|relation, _| selected.contains(relation));
    }
    if predicates.is_empty() {
        return candidate;
    }
    let physical_columns: BTreeMap<_, _> = catalog
        .facts
        .edge_properties
        .iter()
        .map(|access| (access.column, access.edge_column.clone()))
        .collect();
    candidate.plan = inject_edge_predicates(candidate.plan, &predicates, &physical_columns);
    candidate.cost = plan_cost(&candidate.plan);
    candidate.cost.residual_filters = candidate.cost.residual_filters.saturating_sub(2);
    candidate
}

fn inject_edge_predicates(
    mut plan: Plan<ClickHouse>,
    predicates: &BTreeMap<RelationId, Vec<Expr>>,
    physical_columns: &BTreeMap<ColumnId, PhysicalColumn>,
) -> Plan<ClickHouse> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| inject_edge_predicates(input, predicates, physical_columns))
        .collect();
    let Operator::Scan(scan) = &mut plan.operator else {
        return plan;
    };
    let Some(predicates) = predicates.get(&scan.relation) else {
        return plan;
    };
    scan.columns.extend(predicates.iter().flat_map(|predicate| {
        let mut columns = BTreeSet::new();
        collect_expression_columns(predicate, &mut columns);
        columns
    }));
    if let ClickHouseAccess::EdgeTables(access) = &mut scan.access {
        access
            .columns
            .extend(scan.columns.iter().filter_map(|column| {
                physical_columns
                    .get(column)
                    .map(|name| (*column, name.clone()))
            }));
    }
    Plan::unary(
        Operator::Filter(if predicates.len() == 1 {
            predicates[0].clone()
        } else {
            Expr::And(predicates.clone())
        }),
        plan,
    )
}

fn collect_expression_columns(expression: &Expr, columns: &mut BTreeSet<ColumnId>) {
    match expression {
        Expr::Column(column) => {
            columns.insert(*column);
        }
        Expr::ListContains { list, .. } => collect_expression_columns(list, columns),
        Expr::And(expressions) => expressions
            .iter()
            .for_each(|expression| collect_expression_columns(expression, columns)),
        _ => {}
    }
}

fn foreign_key_candidate(
    catalog: &BackendCatalog<'_, ClickHouse>,
    mut candidate: Candidate<ClickHouse>,
) -> Option<Candidate<ClickHouse>> {
    let bound = catalog.bound;
    let expected = bound
        .relations
        .values()
        .filter(|metadata| matches!(metadata.origin, RelationOrigin::Edge { input: Some(_), .. }))
        .count();
    if catalog.facts.foreign_keys.len() != expected {
        return None;
    }
    let mut substitutions = BTreeMap::new();
    let mut relationships = BTreeSet::new();
    let mut join_conditions = Vec::new();
    for access in &catalog.facts.foreign_keys {
        let holder_column = bound.column_ids.get(&ColumnKey {
            relation: access.holder,
            name: access.column.0.clone(),
        })?;
        let referenced_column = bound.column_ids.get(&ColumnKey {
            relation: access.referenced,
            name: DEFAULT_PRIMARY_KEY.into(),
        })?;
        join_conditions.push(Expr::Compare {
            op: CompareOp::Eq,
            left: Box::new(Expr::Column(*holder_column)),
            right: Box::new(Expr::Column(*referenced_column)),
        });
        substitutions.extend(access.substitutions.clone());
        relationships.insert(access.relationship);
    }
    if relationships.is_empty() {
        return None;
    }
    candidate.plan = rewrite_fk_plan(
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
    mut plan: Plan<ClickHouse>,
    relationships: &BTreeSet<RelationId>,
    join_conditions: &[Expr],
    substitutions: &BTreeMap<ColumnId, Expr>,
) -> Plan<ClickHouse> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| rewrite_fk_plan(input, relationships, join_conditions, substitutions))
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

fn plan_cost(plan: &Plan<ClickHouse>) -> Cost {
    let mut cost = Cost::default();
    plan.visit(&mut |plan| match &plan.operator {
        Operator::Scan(scan) => {
            cost.scans += 1;
            cost.columns_read += scan.column_count() as u32;
        }
        Operator::CurrentRows { strategy, .. } => {
            if *strategy == ClickHouseCurrentRows::Final {
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

fn clickhouse_catalog(bound: &BoundCatalog) -> BackendCatalog<'_, ClickHouse> {
    let relations = physical_relations(bound);
    let mut next_column = bound
        .columns
        .keys()
        .map(|column| column.0)
        .max()
        .unwrap_or_default()
        + 1;
    let access_paths = relations
        .iter()
        .map(|(relation, physical)| {
            let (access, physical_columns) = match physical {
                PhysicalRelation::Node { layout, .. } => (
                    ClickHouseAccess::Table(TableAccess {
                        layout: layout.clone(),
                    }),
                    BTreeMap::new(),
                ),
                PhysicalRelation::Edge { layouts, .. } => {
                    let columns: BTreeMap<_, _> = layouts
                        .iter()
                        .flat_map(|layout| &layout.columns)
                        .filter(|physical| {
                            !bound.column_ids.contains_key(&ColumnKey {
                                relation: *relation,
                                name: physical.0.clone(),
                            })
                        })
                        .map(|physical| {
                            let column = ColumnId(next_column);
                            next_column += 1;
                            (column, physical.clone())
                        })
                        .collect();
                    (
                        ClickHouseAccess::EdgeTables(EdgeTableAccess {
                            layouts: layouts.clone(),
                            columns: columns.clone(),
                        }),
                        columns,
                    )
                }
            };
            let mut columns = candidate::relation_columns(bound, *relation);
            columns.extend(physical_columns.keys());
            (
                *relation,
                vec![PhysicalScan {
                    relation: *relation,
                    access,
                    columns,
                }],
            )
        })
        .collect();
    let facts = clickhouse_facts(bound, &access_paths);
    BackendCatalog {
        bound,
        relations,
        access_paths,
        current_rows: bound
            .relations
            .keys()
            .map(|relation| {
                (
                    *relation,
                    vec![ClickHouseCurrentRows::Final, ClickHouseCurrentRows::LimitBy],
                )
            })
            .collect(),
        facts,
        marker: PhantomData,
    }
}

fn physical_relations(bound: &BoundCatalog) -> BTreeMap<RelationId, PhysicalRelation> {
    bound
        .relations
        .iter()
        .map(|(relation, metadata)| {
            let physical = match metadata.origin {
                RelationOrigin::Node { .. } => PhysicalRelation::Node {
                    relation: *relation,
                    layout: node_layout(bound, *relation),
                },
                RelationOrigin::Edge { .. } => PhysicalRelation::Edge {
                    relation: *relation,
                    layouts: edge_layouts(bound, metadata),
                },
            };
            (*relation, physical)
        })
        .collect()
}

fn clickhouse_facts(
    bound: &BoundCatalog,
    access_paths: &BTreeMap<RelationId, Vec<PhysicalScan<ClickHouseAccess>>>,
) -> ClickHouseFacts {
    ClickHouseFacts {
        foreign_keys: foreign_key_facts(bound),
        denormalized_joins: denormalized_join_facts(bound),
        edge_properties: edge_property_facts(bound, access_paths),
        text_indexes: text_index_facts(bound),
    }
}

fn foreign_key_facts(bound: &BoundCatalog) -> Vec<ForeignKeyAccess> {
    bound
        .relations
        .iter()
        .filter_map(|(relationship, metadata)| {
            let RelationOrigin::Edge {
                input: Some(input),
                depth,
                ..
            } = metadata.origin
            else {
                return None;
            };
            let edge = &bound.input.relationships[input.0];
            if depth.is_some_and(|depth| depth > 1)
                || !edge.filters.is_empty()
                || edge.direction == crate::input::Direction::Both
            {
                return None;
            }
            let column = edge.fk_column.as_deref()?;
            let (source_name, target_name) = physical_endpoints(edge)?;
            let source = node_relation(bound, source_name)?;
            let target = node_relation(bound, target_name)?;
            let source_entity = bound
                .input
                .nodes
                .iter()
                .find(|node| node.id == source_name)
                .and_then(|node| node.entity.as_deref())?;
            let target_entity = bound
                .input
                .nodes
                .iter()
                .find(|node| node.id == target_name)
                .and_then(|node| node.entity.as_deref())?;
            let source_holds_key = bound.ontology.get_node(source_entity).is_some_and(|node| {
                node.storage
                    .columns
                    .iter()
                    .any(|candidate| candidate.name == column)
            });
            let (holder, referenced) = if source_holds_key {
                (source, target)
            } else {
                (target, source)
            };
            let source_id = bound.column_ids.get(&ColumnKey {
                relation: source,
                name: DEFAULT_PRIMARY_KEY.into(),
            })?;
            let target_id = bound.column_ids.get(&ColumnKey {
                relation: target,
                name: DEFAULT_PRIMARY_KEY.into(),
            })?;
            let substitutions = [
                (
                    ontology::constants::SOURCE_ID_COLUMN,
                    Expr::Column(*source_id),
                ),
                (
                    ontology::constants::TARGET_ID_COLUMN,
                    Expr::Column(*target_id),
                ),
                (
                    ontology::constants::SOURCE_KIND_COLUMN,
                    Expr::Literal(Value::String(source_entity.into())),
                ),
                (
                    ontology::constants::TARGET_KIND_COLUMN,
                    Expr::Literal(Value::String(target_entity.into())),
                ),
                (
                    ontology::constants::RELATIONSHIP_KIND_COLUMN,
                    Expr::Literal(Value::String(edge.types.first()?.clone())),
                ),
            ]
            .into_iter()
            .filter_map(|(name, expression)| {
                bound
                    .column_ids
                    .get(&ColumnKey {
                        relation: *relationship,
                        name: name.into(),
                    })
                    .map(|column| (*column, expression))
            })
            .collect();
            Some(ForeignKeyAccess {
                relationship: *relationship,
                holder,
                referenced,
                column: PhysicalColumn(column.into()),
                substitutions,
            })
        })
        .collect()
}

fn text_index_facts(bound: &BoundCatalog) -> Vec<TextIndexAccess> {
    bound
        .columns
        .iter()
        .filter_map(|(column, metadata)| {
            let entity = bound.relations[&metadata.relation].entity?;
            bound
                .ontology
                .text_index_tokenizer(&bound.entities[&entity].name, &metadata.name)
                .map(|tokenizer| TextIndexAccess {
                    column: *column,
                    tokenizer: Tokenizer(tokenizer.into()),
                })
        })
        .collect()
}

fn edge_property_facts(
    bound: &BoundCatalog,
    access_paths: &BTreeMap<RelationId, Vec<PhysicalScan<ClickHouseAccess>>>,
) -> Vec<EdgePropertyAccess> {
    let mut facts = Vec::new();
    for (edge_relation, metadata) in &bound.relations {
        let RelationOrigin::Edge {
            input: Some(edge_input),
            ..
        } = metadata.origin
        else {
            continue;
        };
        let edge = &bound.input.relationships[edge_input.0];
        let Some((source, target)) = physical_endpoints(edge) else {
            continue;
        };
        for (node_name, direction) in [
            (source, ontology::DenormDirection::Source),
            (target, ontology::DenormDirection::Target),
        ] {
            let Some(node) = bound.input.nodes.iter().find(|node| node.id == node_name) else {
                continue;
            };
            let Some(entity) = node.entity.as_deref() else {
                continue;
            };
            let Some(node_relation) = node_relation(bound, node_name) else {
                continue;
            };
            for (property, filters) in &node.filters {
                if !filters
                    .iter()
                    .all(|filter| matches!(filter.op, None | Some(FilterOp::Eq | FilterOp::In)))
                {
                    continue;
                }
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
                let Some(source) = bound.column_ids.get(&ColumnKey {
                    relation: node_relation,
                    name: property.clone(),
                }) else {
                    continue;
                };
                let Some(edge_columns) = access_paths[edge_relation][0].access.edge_columns()
                else {
                    continue;
                };
                let Some(column) = edge_columns.iter().find_map(|(column, physical)| {
                    (physical.0 == definition.edge_column).then_some(*column)
                }) else {
                    continue;
                };
                for filter in filters {
                    let values: Vec<_> = match filter.value.as_ref() {
                        Some(serde_json::Value::Array(values)) => values.iter().collect(),
                        Some(value) => vec![value],
                        None => continue,
                    };
                    facts.push(EdgePropertyAccess {
                        edge: *edge_relation,
                        source: *source,
                        column,
                        edge_column: PhysicalColumn(definition.edge_column.clone()),
                        tokens: values
                            .into_iter()
                            .map(|value| {
                                let value = value
                                    .as_str()
                                    .map(str::to_string)
                                    .unwrap_or_else(|| value.to_string());
                                Value::String(format!("{}:{value}", definition.tag_key))
                            })
                            .collect(),
                    });
                }
            }
        }
    }
    facts
}

fn denormalized_join_facts(bound: &BoundCatalog) -> Vec<DenormalizedAccess> {
    if !matches!(
        bound.input.query_type,
        crate::input::QueryType::Traversal | crate::input::QueryType::Aggregation
    ) || bound.input.relationships.is_empty()
    {
        return vec![];
    }
    bound
        .ontology
        .denormalized_joins()
        .iter()
        .filter_map(|denormalized| {
            if denormalized.hops.len() != bound.input.relationships.len()
                || !denormalized
                    .hops
                    .iter()
                    .zip(&bound.input.relationships)
                    .all(|(hop, relationship)| {
                        relationship.hops.max == 1
                            && relationship.types.as_slice() == [hop.relationship_kind.as_str()]
                            && physical_endpoints(relationship).is_some_and(|(source, target)| {
                                bound
                                    .input
                                    .nodes
                                    .iter()
                                    .find(|node| node.id == source)
                                    .and_then(|node| node.entity.as_deref())
                                    == Some(hop.source_kind.as_str())
                                    && bound
                                        .input
                                        .nodes
                                        .iter()
                                        .find(|node| node.id == target)
                                        .and_then(|node| node.entity.as_deref())
                                        == Some(hop.target_kind.as_str())
                            })
                    })
            {
                return None;
            }
            let mut table_for_relation = BTreeMap::new();
            for (index, (hop, relationship)) in denormalized
                .hops
                .iter()
                .zip(&bound.input.relationships)
                .enumerate()
            {
                let (source, target) = physical_endpoints(relationship)?;
                table_for_relation.insert(node_relation(bound, source)?, hop.source_table);
                table_for_relation.insert(node_relation(bound, target)?, hop.target_table);
                if let Some(edge_table) = hop.edge_table {
                    let edge_relation =
                        bound.relations.iter().find_map(|(relation, metadata)| {
                            let RelationOrigin::Edge {
                                input: Some(input), ..
                            } = metadata.origin
                            else {
                                return None;
                            };
                            (input == InputRelationshipId(index)).then_some(*relation)
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
            Some(DenormalizedAccess {
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
                columns,
                residual_filters: vec![],
            })
        })
        .collect()
}

fn physical_endpoints(relationship: &crate::input::InputRelationship) -> Option<(&str, &str)> {
    match relationship.direction {
        crate::input::Direction::Outgoing => Some((&relationship.from, &relationship.to)),
        crate::input::Direction::Incoming => Some((&relationship.to, &relationship.from)),
        crate::input::Direction::Both => None,
    }
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

fn map_clickhouse(
    bound: &BoundCatalog,
    logical: &Plan<Logical>,
    catalog: &BackendCatalog<'_, ClickHouse>,
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
                catalog,
                suppress_current_rows || explicit_current_rows,
            )
        })
        .collect();
    let operator = match &logical.operator {
        Operator::Scan(scan) => Operator::Scan(catalog.access_paths[&scan.relation][0].clone()),
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
        Operator::CurrentRows { keys, .. } => {
            let relation = logical
                .inputs
                .first()
                .and_then(relation_id_logical)
                .unwrap();
            let strategy = catalog.current_rows[&relation]
                .iter()
                .find(|strategy| **strategy == ClickHouseCurrentRows::LimitBy)
                .copied()
                .unwrap();
            Operator::CurrentRows {
                keys: keys.clone(),
                strategy,
            }
        }
        _ => unreachable!("ordinary traversal operator"),
    };
    let plan = Plan { operator, inputs };
    if let Operator::Scan(scan) = &plan.operator
        && !suppress_current_rows
        && matches!(
            bound.relations[&scan.relation].origin,
            RelationOrigin::Node { .. }
        )
        && catalog.current_rows[&scan.relation].contains(&ClickHouseCurrentRows::Final)
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

fn relation_id_logical(plan: &Plan<Logical>) -> Option<RelationId> {
    match &plan.operator {
        Operator::Scan(scan) => Some(scan.relation),
        Operator::Bind(relation) => Some(*relation),
        Operator::Filter(_)
        | Operator::Project(_)
        | Operator::Sort(_)
        | Operator::Limit(_)
        | Operator::CurrentRows { .. } => plan.inputs.first().and_then(relation_id_logical),
        _ => None,
    }
}

fn clickhouse_candidate(bound: &BoundCatalog, plan: Plan<ClickHouse>) -> Candidate<ClickHouse> {
    let cost = plan_cost(&plan);
    candidate::build(bound, plan, cost)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{Direction, InputNode, InputRelationship, QueryType};

    #[test]
    fn plans_one_hop_for_clickhouse() {
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
                scope_proof: None,
                scope_preserving: false,
            }],
            ..Default::default()
        };
        let ontology = Ontology::new()
            .with_nodes(["User", "MergeRequest"])
            .with_edges(["AUTHORED"]);
        let (bound, logical) = bind(input, std::sync::Arc::new(ontology)).unwrap();
        assert_eq!(
            plan_clickhouse(&bound, logical)
                .unwrap()
                .selected
                .candidate
                .cost
                .scans,
            3
        );
    }

    #[test]
    fn builds_clickhouse_catalog_from_stable_ids() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "u".into(),
                    entity: Some("User".into()),
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
                scope_proof: None,
                scope_preserving: false,
            }],
            ..Default::default()
        };
        let ontology = Ontology::new()
            .with_nodes(["User", "MergeRequest"])
            .with_edges(["AUTHORED"]);
        let (bound, _) = bind(input, Arc::new(ontology)).unwrap();

        let clickhouse = clickhouse_catalog(&bound);

        assert_eq!(clickhouse.relations.len(), bound.relations.len());
        assert_eq!(clickhouse.access_paths.len(), bound.relations.len());
        assert_eq!(clickhouse.current_rows.len(), bound.relations.len());
        assert!(
            clickhouse
                .access_paths
                .iter()
                .all(|(relation, paths)| { paths.len() == 1 && paths[0].relation == *relation })
        );
    }

    #[test]
    fn incoming_fk_facts_follow_physical_edge_direction() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "note".into(),
                    entity: Some("Note".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "author".into(),
                    entity: Some("User".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["AUTHORED".into()],
                from: "note".into(),
                to: "author".into(),
                hops: Default::default(),
                direction: Direction::Incoming,
                filters: Default::default(),
                fk_column: Some("author_id".into()),
                scope_proof: None,
                scope_preserving: false,
            }],
            ..Default::default()
        };
        let ontology = Ontology::load_embedded().unwrap();
        let (bound, _) = bind(input, Arc::new(ontology)).unwrap();
        let catalog = clickhouse_catalog(&bound);
        let access = &catalog.facts.foreign_keys[0];
        let edge = bound
            .relations
            .iter()
            .find_map(|(relation, metadata)| {
                matches!(metadata.origin, RelationOrigin::Edge { .. }).then_some(*relation)
            })
            .unwrap();
        let source = node_relation(&bound, "author").unwrap();
        let target = node_relation(&bound, "note").unwrap();
        let source_id = bound.column_ids[&ColumnKey {
            relation: source,
            name: DEFAULT_PRIMARY_KEY.into(),
        }];
        let target_id = bound.column_ids[&ColumnKey {
            relation: target,
            name: DEFAULT_PRIMARY_KEY.into(),
        }];
        let edge_source = bound.column_ids[&ColumnKey {
            relation: edge,
            name: ontology::constants::SOURCE_ID_COLUMN.into(),
        }];
        let edge_target = bound.column_ids[&ColumnKey {
            relation: edge,
            name: ontology::constants::TARGET_ID_COLUMN.into(),
        }];

        assert_eq!(access.substitutions[&edge_source], Expr::Column(source_id));
        assert_eq!(access.substitutions[&edge_target], Expr::Column(target_id));
    }

    #[test]
    fn candidate_set_keeps_the_cheapest_plan_per_property_key() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let (bound, logical) = bind(input, Arc::new(Ontology::new().with_nodes(["User"]))).unwrap();
        let catalog = clickhouse_catalog(&bound);
        let plan = map_clickhouse(&bound, &logical.root, &catalog, false);
        let cheaper = clickhouse_candidate(&bound, plan.clone());
        let mut expensive = clickhouse_candidate(&bound, plan);
        expensive.cost.scans += 1;
        let mut candidates = CandidateSet::default();

        candidates.insert(expensive);
        candidates.insert(cheaper.clone());

        assert_eq!(candidates.candidates.len(), 1);
        assert_eq!(candidates.select().unwrap().candidate.cost, cheaper.cost);
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
                scope_proof: None,
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
