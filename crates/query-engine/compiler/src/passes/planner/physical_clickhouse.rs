use super::*;
use crate::error::Result;
use ontology::constants::DEFAULT_PRIMARY_KEY;

struct ClickHouseCatalog<'a> {
    bound: &'a BoundCatalog<query_data_model::ClickHouseDataModel>,
    access_paths: BTreeMap<RelationId, PhysicalScan<ClickHouseAccess>>,
    foreign_keys: Vec<ForeignKeyAccess>,
    edge_properties: Vec<EdgePropertyAccess>,
}

pub fn plan_clickhouse(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    logical: LogicalPlan,
) -> Result<PlanningResult<ClickHouse>> {
    let catalog = clickhouse_catalog(bound);
    let mut plan = map_clickhouse(bound, &logical.root, &catalog, false);
    if bound.input.query_type == crate::input::QueryType::Aggregation {
        plan = deduplicate_edges(plan, bound, bound.input.relationships.len() > 1);
    }
    let ordinary = clickhouse_candidate(bound, plan);
    if bound.input.query_type == crate::input::QueryType::PathFinding {
        let candidate = pathfinding_candidate(bound, ordinary).ok_or_else(|| {
            crate::error::QueryError::PipelineInvariant(
                "path finding has no physical frontier".into(),
            )
        })?;
        return Ok(PlanningResult {
            logical,
            selected: SelectedPlan { candidate },
        });
    }
    if bound.input.query_type == crate::input::QueryType::Aggregation
        && catalog.edge_properties.len() == 1
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
    if bound.input.query_type == crate::input::QueryType::Neighbors {
        let candidate = fused_neighbors_candidate(bound, ordinary.clone()).unwrap_or(ordinary);
        return Ok(PlanningResult {
            logical,
            selected: SelectedPlan { candidate },
        });
    }
    let mut candidates = CandidateSet::default();
    candidates.insert(ordinary.clone());
    let foreign_key = foreign_key_candidate(&catalog, ordinary.clone());
    if let Some(candidate) = foreign_key.clone() {
        candidates.insert(candidate);
    }
    let edge_count = bound
        .relations
        .values()
        .filter(|metadata| matches!(metadata.origin, RelationOrigin::Edge { input: Some(_), .. }))
        .count();
    let edge_property = edge_property_candidate(&catalog, ordinary);
    if catalog.foreign_keys.len() == edge_count
        && let Some(candidate) = foreign_key
    {
        return Ok(PlanningResult {
            logical,
            selected: SelectedPlan { candidate },
        });
    }
    if !catalog.edge_properties.is_empty() && catalog.foreign_keys.len() != edge_count {
        return Ok(PlanningResult {
            logical,
            selected: SelectedPlan {
                candidate: edge_property,
            },
        });
    }
    candidates.insert(edge_property);
    Ok(PlanningResult {
        logical,
        selected: candidates.select().unwrap(),
    })
}

fn pathfinding_candidate(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    mut candidate: Candidate<ClickHouse>,
) -> Option<Candidate<ClickHouse>> {
    let path = bound.input.path.as_ref()?;
    let start = bound.node_relation(&path.from)?;
    let end = bound.node_relation(&path.to)?;
    let start_plan = relation_plan(&candidate.plan, start)?;
    let end_plan = relation_plan(&candidate.plan, end)?;
    let edge_scan = first_edge_scan(&candidate.plan)?;
    let scoped = [start, end].into_iter().all(|relation| {
        let RelationOrigin::Node { entity, .. } = bound.relation(relation).origin else {
            return false;
        };
        bound
            .model
            .entity_has_traversal_path(bound.entity_name(entity))
    });
    let backward_depth = path.max_depth / 2;
    candidate.plan = Plan {
        operator: Operator::Extension(ClickHouseExtension::PathFinding {
            max_depth: path.max_depth,
            forward_depth: path.max_depth - backward_depth,
            backward_depth,
            scoped,
        }),
        inputs: vec![start_plan, end_plan, Plan::leaf(Operator::Scan(edge_scan))],
    };
    candidate.cost = plan_cost(&candidate.plan);
    Some(candidate)
}

fn relation_plan(plan: &Plan<ClickHouse>, relation: RelationId) -> Option<Plan<ClickHouse>> {
    if plan.relation() == Some(relation) {
        return Some(plan.clone());
    }
    plan.inputs
        .iter()
        .find_map(|input| relation_plan(input, relation))
}

fn first_edge_scan(plan: &Plan<ClickHouse>) -> Option<PhysicalScan<ClickHouseAccess>> {
    if let Operator::Scan(scan) = &plan.operator
        && matches!(scan.access, ClickHouseAccess::EdgeTables(_))
    {
        return Some(scan.clone());
    }
    plan.inputs.iter().find_map(first_edge_scan)
}

fn fused_neighbors_candidate(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    mut candidate: Candidate<ClickHouse>,
) -> Option<Candidate<ClickHouse>> {
    let neighbors = bound.input.neighbors.as_ref()?;
    if neighbors.direction != crate::input::Direction::Both {
        return None;
    }
    let center = bound.input.nodes.first()?;
    if !center.filters.is_empty()
        || center.id_range.is_some()
        || bound
            .model
            .redaction_id_column_named(center.entity.as_deref()?)
            .is_some_and(|column| column != DEFAULT_PRIMARY_KEY)
    {
        return None;
    }
    let Operator::Limit(limit) = candidate.plan.operator else {
        return None;
    };
    let [union] = candidate.plan.inputs.as_slice() else {
        return None;
    };
    let Operator::Union = union.operator else {
        return None;
    };
    let [outgoing, incoming] = union.inputs.as_slice() else {
        return None;
    };
    let outgoing_scan = single_edge_scan(outgoing)?;
    let incoming_scan = single_edge_scan(incoming)?;
    let (ClickHouseAccess::EdgeTables(outgoing_access), ClickHouseAccess::EdgeTables(incoming_access)) =
        (&outgoing_scan.access, &incoming_scan.access)
    else {
        return None;
    };
    if outgoing_access.layouts != incoming_access.layouts {
        return None;
    }
    let center = bound.node_relation(&center.id)?;
    candidate.plan = Plan::unary(
        Operator::Limit(limit),
        Plan {
            operator: Operator::Extension(ClickHouseExtension::FusedNeighbors { center }),
            inputs: vec![Plan::leaf(Operator::Scan(outgoing_scan))],
        },
    );
    candidate.cost = plan_cost(&candidate.plan);
    Some(candidate)
}

fn single_edge_scan(plan: &Plan<ClickHouse>) -> Option<PhysicalScan<ClickHouseAccess>> {
    let mut scan = None;
    plan.visit(&mut |plan| {
        if let Operator::Scan(candidate) = &plan.operator
            && matches!(candidate.access, ClickHouseAccess::EdgeTables(_))
        {
            if scan.is_some() {
                scan = None;
            } else {
                scan = Some(candidate.clone());
            }
        }
    });
    scan
}

fn deduplicate_edges(
    mut plan: Plan<ClickHouse>,
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    multiple_edges: bool,
) -> Plan<ClickHouse> {
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| deduplicate_edges(input, bound, multiple_edges))
        .collect();
    let Operator::Scan(scan) = &plan.operator else {
        return plan;
    };
    if matches!(
        bound.relation(scan.relation).origin,
        RelationOrigin::Edge { .. }
    ) {
        Plan::unary(
            Operator::CurrentRows {
                keys: vec![],
                strategy: if multiple_edges {
                    ClickHouseCurrentRows::Final
                } else {
                    ClickHouseCurrentRows::LimitBy
                },
            },
            plan,
        )
    } else {
        plan
    }
}

fn edge_property_candidate(
    catalog: &ClickHouseCatalog<'_>,
    mut candidate: Candidate<ClickHouse>,
) -> Candidate<ClickHouse> {
    let mut predicates: BTreeMap<RelationId, Vec<Expr>> = BTreeMap::new();
    for access in &catalog.edge_properties {
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
    scan.columns
        .extend(predicates.iter().flat_map(Expr::columns));
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

fn foreign_key_candidate(
    catalog: &ClickHouseCatalog<'_>,
    mut candidate: Candidate<ClickHouse>,
) -> Option<Candidate<ClickHouse>> {
    let bound = catalog.bound;
    let mut required = BTreeSet::new();
    candidate.plan.visit(&mut |plan| {
        if let Operator::Scan(scan) = &plan.operator
            && matches!(
                bound.relation(scan.relation).origin,
                RelationOrigin::Edge { input: Some(_), .. }
            )
        {
            required.insert(scan.relation);
        }
    });
    let available: BTreeSet<_> = catalog
        .foreign_keys
        .iter()
        .map(|access| access.relationship)
        .collect();
    if required.is_empty() || !required.is_subset(&available) {
        return None;
    }
    let mut substitutions = BTreeMap::new();
    let mut relationships = BTreeSet::new();
    let mut join_conditions = Vec::new();
    let visible = candidate.plan.visible_relations();
    for access in catalog
        .foreign_keys
        .iter()
        .filter(|access| required.contains(&access.relationship))
    {
        let holder_column = bound.column_id(access.holder, &access.column.0)?;
        let referenced_column = bound.column_id(access.referenced, DEFAULT_PRIMARY_KEY)?;
        if !visible.contains(&access.holder) {
            return None;
        }
        if visible.contains(&access.referenced) {
            join_conditions.push(Expr::from(holder_column).eq(referenced_column));
        }
        substitutions.extend(access.substitutions.iter().map(|(column, expression)| {
            (
                *column,
                if !visible.contains(&access.referenced)
                    && expression == &Expr::Column(referenced_column)
                {
                    Expr::Column(holder_column)
                } else {
                    expression.clone()
                },
            )
        }));
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
    let is_join = matches!(plan.operator, Operator::Join(_));
    let mut plan = if is_join {
        let mut join = JoinEditor::new(plan).unwrap();
        join.retain_inputs(|input| {
            input
                .relation()
                .is_none_or(|relation| !relationships.contains(&relation))
        });
        join.add_conditions(join_conditions.iter().cloned());
        join.retain_conditions(|condition| !tautology(condition));
        join.finish()
    } else {
        plan
    };
    plan = plan.map_expressions(&mut |expression| match expression {
        Expr::Column(column) => substitutions
            .get(&column)
            .cloned()
            .unwrap_or(Expr::Column(column)),
        expression => expression,
    });
    if matches!(plan.operator, Operator::Join(_)) {
        let mut join = JoinEditor::new(plan).unwrap();
        join.retain_conditions(|condition| !tautology(condition));
        plan = join.finish();
    }
    plan
}

fn tautology(expression: &Expr) -> bool {
    matches!(expression, Expr::Compare { op: CompareOp::Eq, left, right } if left == right)
}

fn node_relation(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    name: &str,
) -> Option<RelationId> {
    bound.node_relation(name)
}

fn plan_cost(plan: &Plan<ClickHouse>) -> Cost {
    let mut cost = Cost::default();
    plan.visit(&mut |plan| match &plan.operator {
        Operator::Scan(scan) => {
            cost.scans += 1;
            cost.edge_scans += matches!(scan.access, ClickHouseAccess::EdgeTables(_)) as u32;
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

fn clickhouse_catalog(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
) -> ClickHouseCatalog<'_> {
    let mut next_column = bound
        .columns
        .keys()
        .map(|column| column.0)
        .max()
        .unwrap_or_default()
        + 1;
    let access_paths = bound
        .relations
        .iter()
        .map(|(relation, metadata)| {
            let (access, physical_columns) = match metadata.origin {
                RelationOrigin::Node { .. } => (
                    ClickHouseAccess::Table(TableAccess {
                        layout: node_layout(bound, *relation),
                    }),
                    BTreeMap::new(),
                ),
                RelationOrigin::Edge { .. } => {
                    let layouts = edge_layouts(bound, metadata);
                    let columns: BTreeMap<_, _> = layouts
                        .iter()
                        .flat_map(|layout| &layout.columns)
                        .filter(|physical| bound.column_id(*relation, &physical.0).is_none())
                        .map(|physical| {
                            let column = ColumnId(next_column);
                            next_column += 1;
                            (column, physical.clone())
                        })
                        .collect();
                    (
                        ClickHouseAccess::EdgeTables(EdgeTableAccess {
                            layouts,
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
                PhysicalScan {
                    relation: *relation,
                    access,
                    columns,
                },
            )
        })
        .collect();
    let edge_properties = edge_property_facts(bound, &access_paths);
    ClickHouseCatalog {
        bound,
        access_paths,
        foreign_keys: foreign_key_facts(bound),
        edge_properties,
    }
}

fn foreign_key_facts(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
) -> Vec<ForeignKeyAccess> {
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
            let source_entity_id = bound.model.graph().entity_id(source_entity)?;
            let target_entity_id = bound.model.graph().entity_id(target_entity)?;
            let relationship_ids: Vec<_> = edge
                .types
                .iter()
                .filter_map(|kind| bound.model.graph().relationship_id(kind))
                .collect();
            let foreign_key = bound.model.query_backend().foreign_key(
                bound.model.graph(),
                &relationship_ids,
                source_entity_id,
                target_entity_id,
            )?;
            let column = bound.model.property_column(foreign_key.property)?;
            let source_holds_key = foreign_key.holder == source_entity_id;
            let (holder, referenced) = if source_holds_key {
                (source, target)
            } else {
                (target, source)
            };
            let source_id = bound.column_id(source, DEFAULT_PRIMARY_KEY)?;
            let target_id = bound.column_id(target, DEFAULT_PRIMARY_KEY)?;
            let substitutions = [
                (
                    ontology::constants::SOURCE_ID_COLUMN,
                    Expr::Column(source_id),
                ),
                (
                    ontology::constants::TARGET_ID_COLUMN,
                    Expr::Column(target_id),
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
                    .column_id(*relationship, name)
                    .map(|column| (column, expression))
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

fn edge_property_facts(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    access_paths: &BTreeMap<RelationId, PhysicalScan<ClickHouseAccess>>,
) -> Vec<EdgePropertyAccess> {
    let mut facts = Vec::new();
    for (edge_relation, metadata) in bound.relations() {
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
            (source, query_data_model::DenormalizedDirection::Source),
            (target, query_data_model::DenormalizedDirection::Target),
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
                let Some(entity_id) = bound.model.graph().entity_id(entity) else {
                    continue;
                };
                let Some(property_id) = bound.model.graph().property_id(entity_id, property) else {
                    continue;
                };
                let Some(definition) =
                    bound
                        .model
                        .denormalized()
                        .property(query_data_model::DenormalizedKey {
                            property: property_id,
                            direction,
                        })
                else {
                    continue;
                };
                if !definition.relationships.iter().any(|relationship| {
                    edge.types.iter().any(|kind| {
                        bound.model.graph().relationship_id(kind) == Some(*relationship)
                    })
                }) {
                    continue;
                }
                let Some(source) = bound.column_id(node_relation, property) else {
                    continue;
                };
                let Some(edge_columns) = access_paths[&edge_relation].access.edge_columns() else {
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
                        edge: edge_relation,
                        source,
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

fn physical_endpoints(relationship: &crate::input::InputRelationship) -> Option<(&str, &str)> {
    match relationship.direction {
        crate::input::Direction::Outgoing => Some((&relationship.from, &relationship.to)),
        crate::input::Direction::Incoming => Some((&relationship.to, &relationship.from)),
        crate::input::Direction::Both => None,
    }
}

fn node_layout(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    relation: RelationId,
) -> TableLayout {
    let RelationOrigin::Node { entity, .. } = bound.relation(relation).origin else {
        unreachable!()
    };
    let table = bound
        .model
        .query_backend()
        .entity_table(entity)
        .unwrap_or_default();
    table_layout(bound, table)
}

fn edge_layouts(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    relation: &BoundRelation,
) -> Vec<TableLayout> {
    let RelationOrigin::Edge { relationships, .. } = &relation.origin else {
        unreachable!()
    };
    let tables = if relationships.is_empty()
        || relationships
            .iter()
            .any(|kind| bound.relationship_name(*kind) == "*")
    {
        QueryBackendCatalog::edge_tables(bound.model.query_backend(), &[])
    } else {
        QueryBackendCatalog::edge_tables(bound.model.query_backend(), relationships)
    };
    tables
        .into_iter()
        .map(|table| table_layout(bound, &table))
        .collect()
}

fn table_layout(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    table: &str,
) -> TableLayout {
    let layout = bound.model.backend().table(table).unwrap();
    TableLayout {
        table: TableName(table.into()),
        columns: layout.columns.iter().cloned().map(PhysicalColumn).collect(),
        sort_key: layout
            .sort_key
            .iter()
            .cloned()
            .map(PhysicalColumn)
            .collect(),
        global: layout
            .entity
            .is_some_and(|entity| bound.model.query_backend().entity_is_global(entity)),
    }
}

fn map_clickhouse(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    logical: &Plan<Logical>,
    catalog: &ClickHouseCatalog<'_>,
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
        Operator::Scan(scan) => Operator::Scan(catalog.access_paths[&scan.relation].clone()),
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
            bound.relation(scan.relation).origin,
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

fn sort_keys(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    relation: RelationId,
    layout: &TableLayout,
) -> Vec<Expr> {
    layout
        .sort_key
        .iter()
        .filter_map(|name| bound.column_id(relation, &name.0).map(Expr::Column))
        .collect()
}

fn clickhouse_candidate(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    plan: Plan<ClickHouse>,
) -> Candidate<ClickHouse> {
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
            }],
            ..Default::default()
        };
        let ontology = ontology::Ontology::new()
            .with_nodes(["User", "MergeRequest"])
            .with_edges(["AUTHORED"]);
        let model =
            Arc::new(query_data_model::ClickHouseDataModel::derive(Arc::new(ontology)).unwrap());
        let (bound, logical) = bind(input, model).unwrap();
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
            }],
            ..Default::default()
        };
        let ontology = ontology::Ontology::new()
            .with_nodes(["User", "MergeRequest"])
            .with_edges(["AUTHORED"]);
        let model =
            Arc::new(query_data_model::ClickHouseDataModel::derive(Arc::new(ontology)).unwrap());
        let (bound, _) = bind(input, model).unwrap();

        let clickhouse = clickhouse_catalog(&bound);

        assert_eq!(clickhouse.access_paths.len(), bound.relations.len());
        assert!(
            clickhouse
                .access_paths
                .iter()
                .all(|(relation, path)| path.relation == *relation)
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
            }],
            ..Default::default()
        };
        let ontology = ontology::Ontology::load_embedded().unwrap();
        let model =
            Arc::new(query_data_model::ClickHouseDataModel::derive(Arc::new(ontology)).unwrap());
        let (bound, _) = bind(input, model).unwrap();
        let catalog = clickhouse_catalog(&bound);
        let access = &catalog.foreign_keys[0];
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
        let ontology = ontology::Ontology::new().with_nodes(["User"]);
        let model =
            Arc::new(query_data_model::ClickHouseDataModel::derive(Arc::new(ontology)).unwrap());
        let (bound, logical) = bind(input, model).unwrap();
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
}
