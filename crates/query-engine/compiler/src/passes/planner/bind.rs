use super::*;
use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, Direction, InputGroupByKey, InputNode, QueryType};
use ontology::constants::{
    DEFAULT_PRIMARY_KEY, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN, TARGET_KIND_COLUMN,
};
use std::collections::HashMap;

pub fn bind(input: Input, ontology: Arc<Ontology>) -> Result<(BoundCatalog, LogicalPlan)> {
    if !matches!(
        input.query_type,
        QueryType::Traversal
            | QueryType::Aggregation
            | QueryType::Neighbors
            | QueryType::PathFinding
            | QueryType::Hydration
    ) {
        return Err(QueryError::PipelineInvariant(
            "query shape is not populated".into(),
        ));
    }
    let query_type = input.query_type;
    let mut builder = Builder::new(input, ontology);
    let plan = match query_type {
        QueryType::Traversal => builder.traversal()?,
        QueryType::Aggregation => builder.aggregation()?,
        QueryType::Neighbors => builder.neighbors()?,
        QueryType::PathFinding => builder.pathfinding()?,
        QueryType::Hydration => builder.hydration()?,
    };
    Ok((
        builder.catalog,
        LogicalPlan {
            root: plan,
            scope_requirements: vec![],
        },
    ))
}

struct Builder {
    catalog: BoundCatalog,
    next_relation: u32,
    next_column: u32,
    next_entity: u32,
    next_relationship: u32,
    next_output: u32,
}

impl Builder {
    fn new(input: Input, ontology: Arc<Ontology>) -> Self {
        Self {
            catalog: BoundCatalog {
                input,
                ontology,
                relations: BTreeMap::new(),
                column_ids: BTreeMap::new(),
                columns: BTreeMap::new(),
                entity_ids: BTreeMap::new(),
                entities: BTreeMap::new(),
                relationship_ids: BTreeMap::new(),
                relationships: BTreeMap::new(),
                outputs: BTreeMap::new(),
            },
            next_relation: 0,
            next_column: 0,
            next_entity: 0,
            next_relationship: 0,
            next_output: 0,
        }
    }

    fn traversal(&mut self) -> Result<Plan<Logical>> {
        let (graph, nodes, edges) = self.graph()?;
        let mut projections: Vec<_> = edges
            .iter()
            .flat_map(|edge| self.edge_outputs(edge))
            .collect();
        if edges.is_empty()
            || nodes
                .iter()
                .any(|node| !requested(&self.catalog.input.nodes[node.input.0]).is_empty())
        {
            for node in &nodes {
                projections.extend(self.node_outputs(node));
            }
        }
        let mut plan = graph.project(projections);
        if let Some(order) = self.catalog.input.order_by.clone() {
            let node = nodes
                .iter()
                .find(|node| self.catalog.input.nodes[node.input.0].id == order.node)
                .ok_or_else(|| QueryError::PipelineInvariant("order node is missing".into()))?;
            plan = plan.sort(vec![SortKey {
                expression: self.column(node.relation, &order.property, None).into(),
                descending: order.direction == crate::input::OrderDirection::Desc,
            }]);
        } else if self.catalog.input.cursor.is_some() && edges.is_empty() {
            plan = plan.sort(vec![SortKey {
                expression: nodes[0].id.into(),
                descending: false,
            }]);
        }
        Ok(plan.limit(self.catalog.input.fetch_limit()))
    }

    fn aggregation(&mut self) -> Result<Plan<Logical>> {
        let groups = self.catalog.input.aggregation.group_by.clone();
        let metrics = self.catalog.input.aggregation.metrics.clone();
        let sort = self.catalog.input.aggregation.sort.clone();
        let (graph, nodes, _) = self.graph()?;
        let nodes_by_name: HashMap<_, _> = nodes
            .iter()
            .map(|node| (self.catalog.input.nodes[node.input.0].id.clone(), node))
            .collect();
        let mut group_exprs = Vec::new();
        for group in groups {
            match group {
                InputGroupByKey::Property {
                    node,
                    property,
                    truncate,
                    alias,
                } => {
                    let column = self.column(nodes_by_name[&node].relation, &property, None);
                    let expression = truncate.map_or(column.into(), |unit| Expr::DateTrunc {
                        unit,
                        value: Box::new(column.into()),
                    });
                    let output = alias.unwrap_or_else(|| match truncate {
                        Some(unit) => format!("{node}_{property}_{}", unit.name()),
                        None => format!("{node}_{property}"),
                    });
                    group_exprs.push(self.named(expression, output));
                }
                InputGroupByKey::Node { node, .. } => {
                    group_exprs.extend(self.node_outputs(nodes_by_name[&node]));
                }
            }
        }
        let mut metric_exprs = Vec::new();
        for metric in metrics {
            let function = metric.expr.function();
            let value = metric.expr.property().map(|property| {
                Box::new(
                    self.column(nodes_by_name[metric.expr.node()].relation, property, None)
                        .into(),
                )
            });
            let expression = Expr::Aggregate { function, value };
            metric_exprs.push(self.named(expression, metric.output_name()));
        }
        let mut plan = graph.aggregate(group_exprs, metric_exprs);
        if let Some(sort) = sort {
            let output = self
                .catalog
                .outputs
                .iter()
                .find_map(|(id, output)| (output.name == sort.column).then_some(*id));
            if let Some(output) = output {
                plan = plan.sort(vec![SortKey {
                    expression: output.into(),
                    descending: sort.direction == crate::input::OrderDirection::Desc,
                }]);
            }
        }
        Ok(plan.limit(self.catalog.input.fetch_limit()))
    }

    fn neighbors(&mut self) -> Result<Plan<Logical>> {
        let config =
            self.catalog.input.neighbors.clone().ok_or_else(|| {
                QueryError::PipelineInvariant("neighbors config is missing".into())
            })?;
        let plan = match config.direction {
            Direction::Outgoing => self.neighbor_arm(true, config.rel_types)?,
            Direction::Incoming => self.neighbor_arm(false, config.rel_types)?,
            Direction::Both => Plan::union(vec![
                self.neighbor_arm(true, config.rel_types.clone())?,
                self.neighbor_arm(false, config.rel_types)?,
            ]),
        };
        Ok(plan.limit(self.catalog.input.fetch_limit()))
    }

    fn neighbor_arm(&mut self, outgoing: bool, kinds: Vec<String>) -> Result<Plan<Logical>> {
        let node = self.node(0)?;
        let relationships = kinds.iter().map(|name| self.relationship(name)).collect();
        let edge = self.edge_scan(
            RelationOrigin::Edge {
                input: None,
                depth: None,
                hop: None,
            },
            None,
            relationships,
        );
        let mut predicates = match kinds.as_slice() {
            [] => vec![],
            [name] if name != "*" => vec![compare(
                CompareOp::Eq,
                Expr::Column(edge.relationship_kind),
                literal(name.clone()),
            )],
            names if names.iter().all(|name| name != "*") => vec![Expr::In {
                value: Box::new(Expr::Column(edge.relationship_kind)),
                values: names.iter().cloned().map(Value::String).collect(),
                data_type: Some(ontology::DataType::String),
            }],
            _ => vec![],
        };
        let (center_id, neighbor_id, center_kind, neighbor_kind) = if outgoing {
            (
                edge.source_id,
                edge.target_id,
                edge.source_kind,
                edge.target_kind,
            )
        } else {
            (
                edge.target_id,
                edge.source_id,
                edge.target_kind,
                edge.source_kind,
            )
        };
        if let Some(entity) = self.catalog.input.nodes[0].entity.clone() {
            predicates.push(compare(
                CompareOp::Eq,
                Expr::Column(center_kind),
                literal(entity),
            ));
        }
        let plan = Plan::join(
            [edge.plan.filter(predicates), node.plan],
            vec![Expr::from(center_id).eq(node.id)],
        );
        let columns = vec![
            NamedExpr {
                expression: Expr::Column(neighbor_id),
                output: self.output(crate::constants::neighbor_id_column()),
            },
            NamedExpr {
                expression: Expr::Column(neighbor_kind),
                output: self.output(crate::constants::neighbor_type_column()),
            },
            NamedExpr {
                expression: Expr::Column(edge.relationship_kind),
                output: self.output(crate::constants::relationship_type_column()),
            },
            NamedExpr {
                expression: literal(i64::from(outgoing)),
                output: self.output(crate::constants::neighbor_is_outgoing_column()),
            },
            NamedExpr {
                expression: Expr::Column(node.id),
                output: self.output(crate::constants::primary_key_column(
                    &self.catalog.input.nodes[0].id,
                )),
            },
            NamedExpr {
                expression: Expr::Column(self.column(
                    node.relation,
                    &self.catalog.input.nodes[0].redaction_id_column.clone(),
                    None,
                )),
                output: self.output(crate::constants::redaction_id_column(
                    &self.catalog.input.nodes[0].id,
                )),
            },
            NamedExpr {
                expression: literal(
                    self.catalog.input.nodes[0]
                        .entity
                        .clone()
                        .unwrap_or_default(),
                ),
                output: self.output(crate::constants::redaction_type_column(
                    &self.catalog.input.nodes[0].id,
                )),
            },
        ];
        Ok(plan.project(columns))
    }

    fn pathfinding(&mut self) -> Result<Plan<Logical>> {
        let path = self
            .catalog
            .input
            .path
            .clone()
            .ok_or_else(|| QueryError::PipelineInvariant("path config is missing".into()))?;
        let start = self
            .catalog
            .input
            .nodes
            .iter()
            .position(|node| node.id == path.from)
            .unwrap();
        let end = self
            .catalog
            .input
            .nodes
            .iter()
            .position(|node| node.id == path.to)
            .unwrap();
        let path_output = self.output(crate::constants::path_column());
        let kinds_output = self.output(crate::constants::edge_kinds_column());
        let depth_output = self.output("depth");
        let mut arms = Vec::new();
        let relationships: Vec<_> = path
            .rel_types
            .iter()
            .map(|name| self.relationship(name))
            .collect();
        let scoped_by_path = self.catalog.input.nodes[start].has_traversal_path
            && self.catalog.input.nodes[end].has_traversal_path;
        for depth in 1..=path.max_depth {
            let start_node = self.node(start)?;
            let end_node = self.node(end)?;
            let mut chain = self.edge_chain(depth, &relationships, scoped_by_path);
            for (hop, edge) in chain.hops.iter_mut().enumerate() {
                let hop = hop as u32 + 1;
                let mut predicates = match path.rel_types.as_slice() {
                    [] => vec![],
                    [name] if name != "*" => vec![compare(
                        CompareOp::Eq,
                        Expr::Column(edge.relationship_kind),
                        literal(name.clone()),
                    )],
                    names if names.iter().all(|name| name != "*") => vec![Expr::In {
                        value: Box::new(Expr::Column(edge.relationship_kind)),
                        values: names.iter().cloned().map(Value::String).collect(),
                        data_type: Some(ontology::DataType::String),
                    }],
                    _ => vec![],
                };
                if path.rel_types.as_slice() == ["*"] {
                    let start_entity = self.catalog.input.nodes[start]
                        .entity
                        .as_deref()
                        .unwrap_or_default();
                    let end_entity = self.catalog.input.nodes[end]
                        .entity
                        .as_deref()
                        .unwrap_or_default();
                    let endpoint_types: Vec<_> = if hop == 1 {
                        self.catalog
                            .ontology
                            .edges()
                            .filter(|edge| edge.source_kind == start_entity)
                            .map(|edge| Value::String(edge.relationship_kind.clone()))
                            .collect()
                    } else if hop == depth {
                        self.catalog
                            .ontology
                            .edges()
                            .filter(|edge| edge.target_kind == end_entity)
                            .map(|edge| Value::String(edge.relationship_kind.clone()))
                            .collect()
                    } else {
                        vec![]
                    };
                    if !endpoint_types.is_empty() {
                        predicates.push(Expr::In {
                            value: Box::new(Expr::Column(edge.relationship_kind)),
                            values: endpoint_types,
                            data_type: Some(ontology::DataType::String),
                        });
                    }
                }
                if hop == 1 {
                    predicates.extend(ids(
                        edge.source_id,
                        &self.catalog.input.nodes[start].node_ids,
                    ));
                }
                if hop == depth {
                    predicates.extend(ids(edge.target_id, &self.catalog.input.nodes[end].node_ids));
                }
                edge.plan = edge.plan.clone().filter(predicates);
            }
            chain
                .conditions
                .insert(0, Expr::from(start_node.id).eq(chain.hops[0].source_id));
            chain
                .conditions
                .push(Expr::from(chain.hops.last().unwrap().target_id).eq(end_node.id));
            let inputs = std::iter::once(start_node.plan)
                .chain(chain.hops.iter().map(|edge| edge.plan.clone()))
                .chain(std::iter::once(end_node.plan));
            let plan = Plan::join(inputs, chain.conditions);
            let start_kind = self.catalog.input.nodes[start]
                .entity
                .clone()
                .unwrap_or_default();
            let path_values = std::iter::once(Expr::Tuple(vec![
                Expr::Column(start_node.id),
                literal(start_kind),
            ]))
            .chain(chain.hops.iter().map(|edge| {
                Expr::Tuple(vec![
                    Expr::Column(edge.target_id),
                    Expr::Column(edge.target_kind),
                ])
            }))
            .collect();
            let edge_kinds = chain
                .hops
                .iter()
                .map(|edge| Expr::Column(edge.relationship_kind))
                .collect();
            arms.push(plan.project(vec![
                NamedExpr {
                    expression: Expr::Array(path_values),
                    output: path_output,
                },
                NamedExpr {
                    expression: Expr::Array(edge_kinds),
                    output: kinds_output,
                },
                NamedExpr {
                    expression: literal(i64::from(depth)),
                    output: depth_output,
                },
            ]));
        }
        Ok(Plan::union(arms)
            .sort(vec![SortKey {
                expression: depth_output.into(),
                descending: false,
            }])
            .limit(self.catalog.input.fetch_limit()))
    }

    fn hydration(&mut self) -> Result<Plan<Logical>> {
        let mut arms = Vec::new();
        for index in 0..self.catalog.input.nodes.len() {
            let input = self.catalog.input.nodes[index].clone();
            let node = self.node(index)?;
            let id = self.column(
                node.relation,
                &input.id_property,
                Some(ontology::DataType::Int),
            );
            let current = Plan::unary(
                Operator::CurrentRows {
                    keys: vec![Expr::Column(id)],
                    strategy: (),
                },
                node.plan,
            );
            let current = if input.traversal_paths.is_empty() {
                current
            } else {
                let traversal_path = self.column(
                    node.relation,
                    ontology::constants::TRAVERSAL_PATH_COLUMN,
                    Some(ontology::DataType::String),
                );
                let leaves = orbit_utils::traversal_path::prune_to_leaves(&input.traversal_paths);
                let leaves = self
                    .catalog
                    .input
                    .path_segment_budget
                    .map_or(leaves.clone(), |budget| {
                        crate::passes::shared::generalize_to_budget(leaves, budget)
                    });
                let predicates = leaves
                    .into_iter()
                    .map(|path| Expr::Filter {
                        op: FilterOp::StartsWith,
                        left: Box::new(Expr::Column(traversal_path)),
                        right: Some(Box::new(literal(path.as_str().to_string()))),
                        data_type: Some(ontology::DataType::String),
                    })
                    .collect();
                current.filter(vec![Expr::Or(predicates)])
            };
            let properties = requested(&input)
                .into_iter()
                .map(|name| {
                    let column = self.column(node.relation, &name, None);
                    (name, Expr::Stringify(Box::new(Expr::Column(column))))
                })
                .collect();
            let id_output = format!("{}_{}", input.id, input.id_property);
            let entity_output = format!("{}_entity_type", input.id);
            let properties_output = format!("{}_props", input.id);
            arms.push(current.project(vec![
                self.named(id, id_output),
                self.named(input.entity.unwrap_or_default(), entity_output),
                self.named(
                    Expr::Stringify(Box::new(Expr::JsonObject(properties))),
                    properties_output,
                ),
            ]));
        }
        Ok(Plan::union_or_single(arms).limit(self.catalog.input.fetch_limit()))
    }

    fn graph(&mut self) -> Result<(Plan<Logical>, Vec<Node>, Vec<Edge>)> {
        if self.catalog.input.nodes.is_empty() {
            return Err(QueryError::PipelineInvariant(
                "traversal has no nodes".into(),
            ));
        }
        let mut nodes = Vec::new();
        for index in 0..self.catalog.input.nodes.len() {
            nodes.push(self.node(index)?);
        }
        let node_relations: HashMap<_, _> = nodes
            .iter()
            .map(|node| {
                (
                    self.catalog.input.nodes[node.input.0].id.clone(),
                    node.relation,
                )
            })
            .collect();
        let mut edges = Vec::new();
        for index in 0..self.catalog.input.relationships.len() {
            edges.push(self.edge(index, &node_relations)?);
        }
        let mut bindings = HashMap::<RelationId, Expr>::new();
        let mut conditions = Vec::new();
        for edge in &edges {
            for (node, column) in [(edge.from, edge.from_id()), (edge.to, edge.to_id())] {
                let edge_column = Expr::from(column);
                if let Some(bound) = bindings.insert(node, edge_column.clone()) {
                    conditions.push(compare(CompareOp::Eq, bound, edge_column));
                }
            }
        }
        for node in &nodes {
            if let Some(bound) = bindings.get(&node.relation).cloned() {
                let id = self.column(
                    node.relation,
                    DEFAULT_PRIMARY_KEY,
                    Some(ontology::DataType::Int),
                );
                conditions.push(compare(CompareOp::Eq, bound, Expr::Column(id)));
            }
        }
        for predicate in self.catalog.input.join_predicates.clone() {
            let left_relation = nodes
                .iter()
                .find(|node| self.catalog.input.nodes[node.input.0].id == predicate.lhs_node)
                .unwrap()
                .relation;
            let right_relation = nodes
                .iter()
                .find(|node| self.catalog.input.nodes[node.input.0].id == predicate.rhs_node)
                .unwrap()
                .relation;
            let left = self.column(left_relation, &predicate.lhs_prop, None);
            let right = self.column(right_relation, &predicate.rhs_prop, None);
            let op = match predicate.op {
                FilterOp::Eq => CompareOp::Eq,
                FilterOp::Ne => CompareOp::Ne,
                FilterOp::Lt => CompareOp::Lt,
                FilterOp::Lte => CompareOp::Le,
                FilterOp::Gt => CompareOp::Gt,
                FilterOp::Gte => CompareOp::Ge,
                _ => unreachable!(),
            };
            conditions.push(compare(op, Expr::Column(left), Expr::Column(right)));
        }
        let inputs = edges
            .iter()
            .map(|edge| edge.plan.clone())
            .chain(nodes.iter().map(|node| node.plan.clone()));
        Ok((Plan::join(inputs, conditions), nodes, edges))
    }

    fn node(&mut self, index: usize) -> Result<Node> {
        let input = self.catalog.input.nodes[index].clone();
        let entity_name = input
            .entity
            .as_deref()
            .ok_or_else(|| QueryError::PipelineInvariant("node entity is missing".into()))?;
        let entity = self.entity(entity_name);
        let relation = self.relation(
            RelationOrigin::Node {
                input: InputNodeId(index),
            },
            Some(entity),
            vec![],
        );
        let id = self.column(relation, DEFAULT_PRIMARY_KEY, Some(ontology::DataType::Int));
        let deleted = self.column(
            relation,
            ontology::constants::DELETED_COLUMN,
            Some(ontology::DataType::Bool),
        );
        let plan = Plan::leaf(Operator::Scan(LogicalScan { relation })).filter({
            let mut predicates = self.node_predicates(relation, &input, id);
            predicates.push(Expr::from(deleted).eq(false));
            predicates
        });
        Ok(Node {
            relation,
            input: InputNodeId(index),
            id,
            plan,
        })
    }

    fn edge(&mut self, index: usize, nodes: &HashMap<String, RelationId>) -> Result<Edge> {
        let input = self.catalog.input.relationships[index].clone();
        let relationships = input
            .types
            .iter()
            .map(|name| self.relationship(name))
            .collect();
        if let Some(foreign_key) = input.fk_column.as_deref() {
            let from = nodes[&input.from];
            let to = nodes[&input.to];
            let from_entity = self
                .catalog
                .input
                .nodes
                .iter()
                .find(|node| node.id == input.from)
                .and_then(|node| node.entity.as_deref())
                .unwrap_or_default();
            let from_holds_key = self
                .catalog
                .ontology
                .get_node(from_entity)
                .is_some_and(|node| {
                    node.storage
                        .columns
                        .iter()
                        .any(|column| column.name == foreign_key)
                });
            self.column(
                if from_holds_key { from } else { to },
                foreign_key,
                Some(ontology::DataType::Int),
            );
        }
        let edge_scan = self.edge_scan(
            RelationOrigin::Edge {
                input: Some(InputRelationshipId(index)),
                depth: None,
                hop: None,
            },
            None,
            relationships,
        );
        let relation = edge_scan.relation;
        let plan = if input.hops.max == 1 {
            edge_scan
                .plan
                .filter(self.edge_predicates(relation, &input))
        } else {
            let mut arms = Vec::new();
            let relationships: Vec<_> = input
                .types
                .iter()
                .map(|name| self.relationship(name))
                .collect();
            for depth in input.hops.min.max(1)..=input.hops.max {
                let mut chain = self.edge_chain(depth, &relationships, false);
                for (hop, edge) in chain.hops.iter_mut().enumerate() {
                    let hop = hop as u32 + 1;
                    let mut predicates = self.kind_predicates(edge.relation, &input.types);
                    if hop == 1 {
                        let source = edge.from_id(input.direction);
                        let kind = edge.from_kind(input.direction);
                        let from = self
                            .catalog
                            .input
                            .nodes
                            .iter()
                            .find(|node| node.id == input.from)
                            .cloned()
                            .unwrap();
                        predicates.extend(ids(source, &from.node_ids));
                        if let Some(entity) = from.entity.as_deref() {
                            predicates.push(compare(
                                CompareOp::Eq,
                                Expr::Column(kind),
                                literal(entity.to_string()),
                            ));
                        }
                    }
                    if hop == depth {
                        let target = edge.to_id(input.direction);
                        let kind = edge.to_kind(input.direction);
                        let to = self
                            .catalog
                            .input
                            .nodes
                            .iter()
                            .find(|node| node.id == input.to)
                            .cloned()
                            .unwrap();
                        predicates.extend(ids(target, &to.node_ids));
                        if let Some(entity) = to.entity.as_deref() {
                            predicates.push(compare(
                                CompareOp::Eq,
                                Expr::Column(kind),
                                literal(entity.to_string()),
                            ));
                        }
                    }
                    edge.plan = edge.plan.clone().filter(predicates);
                }
                let first = &chain.hops[0];
                let last = chain.hops.last().unwrap();
                let columns = [
                    (RELATIONSHIP_KIND_COLUMN, first.relationship_kind),
                    (SOURCE_ID_COLUMN, first.source_id),
                    (SOURCE_KIND_COLUMN, first.source_kind),
                    (TARGET_ID_COLUMN, last.target_id),
                    (TARGET_KIND_COLUMN, last.target_kind),
                    (
                        ontology::constants::SOURCE_TAGS_COLUMN,
                        self.column(
                            first.relation,
                            ontology::constants::SOURCE_TAGS_COLUMN,
                            Some(ontology::DataType::String),
                        ),
                    ),
                    (
                        ontology::constants::TARGET_TAGS_COLUMN,
                        self.column(
                            last.relation,
                            ontology::constants::TARGET_TAGS_COLUMN,
                            Some(ontology::DataType::String),
                        ),
                    ),
                ]
                .into_iter()
                .map(|(name, column)| NamedExpr {
                    expression: Expr::Column(column),
                    output: self.output(name),
                })
                .collect();
                let mut columns: Vec<NamedExpr> = columns;
                columns.push(NamedExpr {
                    expression: literal(i64::from(depth)),
                    output: self.output("depth"),
                });
                columns.push(NamedExpr {
                    expression: Expr::Array(
                        chain
                            .hops
                            .iter()
                            .map(|hop| {
                                Expr::Tuple(vec![
                                    Expr::Column(hop.target_id),
                                    Expr::Column(hop.target_kind),
                                ])
                            })
                            .collect(),
                    ),
                    output: self.output(crate::constants::PATH_NODES_COLUMN),
                });
                arms.push(Plan::unary(
                    Operator::Project(columns),
                    Plan::join(chain.hops.into_iter().map(|hop| hop.plan), chain.conditions),
                ));
            }
            Plan::unary(
                Operator::Bind(relation),
                Plan {
                    operator: Operator::Union,
                    inputs: arms,
                },
            )
            .filter({
                let (source, target) = input.direction.edge_columns();
                let source = self.column(relation, source, Some(ontology::DataType::Int));
                let target = self.column(relation, target, Some(ontology::DataType::Int));
                let from = self
                    .catalog
                    .input
                    .nodes
                    .iter()
                    .find(|node| node.id == input.from)
                    .cloned()
                    .unwrap();
                let to = self
                    .catalog
                    .input
                    .nodes
                    .iter()
                    .find(|node| node.id == input.to)
                    .cloned()
                    .unwrap();
                let mut predicates: Vec<_> = ids(source, &from.node_ids)
                    .into_iter()
                    .chain(ids(target, &to.node_ids))
                    .collect();
                for (node, column_name, direction) in [
                    (
                        &from,
                        ontology::constants::SOURCE_TAGS_COLUMN,
                        ontology::DenormDirection::Source,
                    ),
                    (
                        &to,
                        ontology::constants::TARGET_TAGS_COLUMN,
                        ontology::DenormDirection::Target,
                    ),
                ] {
                    let Some(entity) = node.entity.as_deref() else {
                        continue;
                    };
                    for (property, filters) in &node.filters {
                        let Some(definition) =
                            self.catalog.ontology.denormalized_properties().iter().find(
                                |definition| {
                                    definition.node_kind == entity
                                        && definition.property_name == *property
                                        && definition.direction == direction
                                        && input.types.contains(&definition.relationship_kind)
                                },
                            )
                        else {
                            continue;
                        };
                        let tag_key = definition.tag_key.clone();
                        let list =
                            self.column(relation, column_name, Some(ontology::DataType::String));
                        for filter in filters {
                            let values: Vec<_> = match filter.value.as_ref() {
                                Some(serde_json::Value::Array(values)) => values.iter().collect(),
                                Some(value) => vec![value],
                                None => continue,
                            };
                            predicates.push(Expr::ListContains {
                                list: Box::new(Expr::Column(list)),
                                values: values
                                    .into_iter()
                                    .map(|value| {
                                        let value = value
                                            .as_str()
                                            .map(str::to_string)
                                            .unwrap_or_else(|| value.to_string());
                                        Value::String(format!("{}:{value}", tag_key))
                                    })
                                    .collect(),
                            });
                        }
                    }
                }
                predicates
            })
        };
        Ok(Edge {
            relation,
            input: InputRelationshipId(index),
            from: nodes[&input.from],
            to: nodes[&input.to],
            direction: input.direction,
            relationship_kind: self.column(
                relation,
                RELATIONSHIP_KIND_COLUMN,
                Some(ontology::DataType::String),
            ),
            source_id: self.column(relation, SOURCE_ID_COLUMN, Some(ontology::DataType::Int)),
            source_kind: self.column(
                relation,
                SOURCE_KIND_COLUMN,
                Some(ontology::DataType::String),
            ),
            target_id: self.column(relation, TARGET_ID_COLUMN, Some(ontology::DataType::Int)),
            target_kind: self.column(
                relation,
                TARGET_KIND_COLUMN,
                Some(ontology::DataType::String),
            ),
            plan,
        })
    }

    fn kind_predicates(&mut self, relation: RelationId, kinds: &[String]) -> Vec<Expr> {
        let kind = self.column(
            relation,
            RELATIONSHIP_KIND_COLUMN,
            Some(ontology::DataType::String),
        );
        match kinds {
            [] => vec![],
            [name] if name != "*" => vec![compare(
                CompareOp::Eq,
                Expr::Column(kind),
                literal(name.clone()),
            )],
            names if names.iter().all(|name| name != "*") => vec![Expr::In {
                value: Box::new(Expr::Column(kind)),
                values: names.iter().cloned().map(Value::String).collect(),
                data_type: Some(ontology::DataType::String),
            }],
            _ => vec![],
        }
    }

    fn node_predicates(
        &mut self,
        relation: RelationId,
        node: &InputNode,
        id: ColumnId,
    ) -> Vec<Expr> {
        let mut predicates = self.filters(relation, &node.filters);
        predicates.extend(ids(id, &node.node_ids));
        if let Some(range) = &node.id_range {
            predicates.push(Expr::from(id).ge(range.start));
            predicates.push(Expr::from(id).le(range.end));
        }
        predicates
    }

    fn edge_predicates(
        &mut self,
        relation: RelationId,
        edge: &crate::input::InputRelationship,
    ) -> Vec<Expr> {
        let mut predicates = Vec::new();
        let kind = self.column(
            relation,
            RELATIONSHIP_KIND_COLUMN,
            Some(ontology::DataType::String),
        );
        match edge.types.as_slice() {
            [] => {}
            [name] if name != "*" => predicates.push(compare(
                CompareOp::Eq,
                Expr::Column(kind),
                literal(name.clone()),
            )),
            names if names.iter().all(|name| name != "*") => predicates.push(Expr::In {
                value: Box::new(Expr::Column(kind)),
                values: names.iter().cloned().map(Value::String).collect(),
                data_type: Some(ontology::DataType::String),
            }),
            _ => {}
        }
        let (source, target) = edge.direction.edge_columns();
        for (node_name, id_name) in [(&edge.from, source), (&edge.to, target)] {
            let node = self
                .catalog
                .input
                .nodes
                .iter()
                .find(|node| node.id == *node_name)
                .cloned()
                .unwrap();
            let id = self.column(relation, id_name, Some(ontology::DataType::Int));
            predicates.extend(ids(id, &node.node_ids));
            if let Some(entity) = node.entity {
                let kind_name = if id_name == SOURCE_ID_COLUMN {
                    SOURCE_KIND_COLUMN
                } else {
                    TARGET_KIND_COLUMN
                };
                let kind = self.column(relation, kind_name, Some(ontology::DataType::String));
                predicates.push(compare(CompareOp::Eq, Expr::Column(kind), literal(entity)));
            }
        }
        predicates.extend(self.filters(relation, &edge.filters));
        predicates
    }

    fn filters(
        &mut self,
        relation: RelationId,
        filters: &HashMap<String, Vec<crate::input::InputFilter>>,
    ) -> Vec<Expr> {
        let mut entries: Vec<_> = filters.iter().collect();
        entries.sort_unstable_by_key(|(name, _)| *name);
        entries
            .into_iter()
            .flat_map(|(name, filters)| {
                filters
                    .iter()
                    .map(|filter| {
                        let column = self.column(relation, name, filter.data_type);
                        if filter.op == Some(FilterOp::In) {
                            return Expr::In {
                                value: Box::new(Expr::Column(column)),
                                values: filter
                                    .value
                                    .as_ref()
                                    .and_then(serde_json::Value::as_array)
                                    .into_iter()
                                    .flatten()
                                    .map(value_of)
                                    .collect(),
                                data_type: filter.data_type,
                            };
                        }
                        Expr::Filter {
                            op: filter.op.unwrap_or(FilterOp::Eq),
                            left: Box::new(Expr::Column(column)),
                            right: filter.rhs_column.as_ref().map_or_else(
                                || {
                                    filter
                                        .value
                                        .as_ref()
                                        .map(|value| Box::new(Expr::Literal(value_of(value))))
                                },
                                |(node, property)| {
                                    let relation = self
                                        .catalog
                                        .relations
                                        .iter()
                                        .find_map(|(relation, metadata)| {
                                            let RelationOrigin::Node { input } = metadata.origin
                                            else {
                                                return None;
                                            };
                                            (self.catalog.input.nodes[input.0].id == *node)
                                                .then_some(*relation)
                                        })
                                        .unwrap();
                                    Some(Box::new(Expr::Column(self.column(
                                        relation,
                                        property,
                                        filter.data_type,
                                    ))))
                                },
                            ),
                            data_type: filter.data_type,
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn node_outputs(&mut self, node: &Node) -> Vec<NamedExpr> {
        let input = self.catalog.input.nodes[node.input.0].clone();
        let mut columns = requested(&input);
        for virtual_column in &input.virtual_columns {
            let Some(entity) = input.entity.as_deref() else {
                continue;
            };
            let Some(field) = self.catalog.ontology.get_node(entity).and_then(|entity| {
                entity
                    .fields
                    .iter()
                    .find(|field| field.name == virtual_column.column_name)
            }) else {
                continue;
            };
            if let ontology::FieldSource::Virtual(source) = &field.source {
                for dependency in &source.depends_on {
                    if !columns.contains(dependency) {
                        columns.push(dependency.clone());
                    }
                }
            }
        }
        columns
            .into_iter()
            .map(|name| {
                let column = self.column(node.relation, &name, None);
                NamedExpr {
                    expression: Expr::Column(column),
                    output: self.output(format!("{}_{}", input.id, name)),
                }
            })
            .collect()
    }

    fn edge_outputs(&mut self, edge: &Edge) -> Vec<NamedExpr> {
        let multi_hop = self.catalog.input.relationships[edge.input.0].hops.max > 1;
        let prefix = if multi_hop {
            format!("hop_e{}", edge.input.0)
        } else {
            format!("e{}", edge.input.0)
        };
        let mut outputs: Vec<_> = [
            (edge.relationship_kind, "type"),
            (edge.source_id, "src"),
            (edge.source_kind, "src_type"),
            (edge.target_id, "dst"),
            (edge.target_kind, "dst_type"),
        ]
        .into_iter()
        .map(|(column, suffix)| self.named(column, format!("{prefix}_{suffix}")))
        .collect();
        let is_multi_hop = matches!(self.catalog.relations[&edge.relation].origin, RelationOrigin::Edge { input: Some(input), .. } if self.catalog.input.relationships[input.0].hops.max > 1);
        if is_multi_hop {
            for (name, suffix, data_type) in [
                (
                    crate::constants::PATH_NODES_COLUMN,
                    "path_nodes",
                    ontology::DataType::String,
                ),
                ("depth", "depth", ontology::DataType::Int),
            ] {
                let column = self.column(edge.relation, name, Some(data_type));
                outputs.push(NamedExpr {
                    expression: Expr::Column(column),
                    output: self.output(format!("{prefix}_{suffix}")),
                });
            }
        }
        outputs
    }

    fn relation(
        &mut self,
        origin: RelationOrigin,
        entity: Option<EntityId>,
        relationships: Vec<RelationshipId>,
    ) -> RelationId {
        let id = RelationId(self.next_relation);
        self.next_relation += 1;
        self.catalog.relations.insert(
            id,
            BoundRelation {
                origin,
                entity,
                relationships,
            },
        );
        id
    }

    fn edge_scan(
        &mut self,
        origin: RelationOrigin,
        entity: Option<EntityId>,
        relationships: Vec<RelationshipId>,
    ) -> EdgeScan {
        let relation = self.relation(origin, entity, relationships);
        EdgeScan {
            relation,
            relationship_kind: self.column(
                relation,
                RELATIONSHIP_KIND_COLUMN,
                Some(ontology::DataType::String),
            ),
            source_id: self.column(relation, SOURCE_ID_COLUMN, Some(ontology::DataType::Int)),
            source_kind: self.column(
                relation,
                SOURCE_KIND_COLUMN,
                Some(ontology::DataType::String),
            ),
            target_id: self.column(relation, TARGET_ID_COLUMN, Some(ontology::DataType::Int)),
            target_kind: self.column(
                relation,
                TARGET_KIND_COLUMN,
                Some(ontology::DataType::String),
            ),
            traversal_path: None,
            plan: Plan::leaf(Operator::Scan(LogicalScan { relation })),
        }
    }

    fn edge_chain(
        &mut self,
        depth: u32,
        relationships: &[RelationshipId],
        scoped_by_path: bool,
    ) -> EdgeChain {
        let mut hops: Vec<_> = (1..=depth)
            .map(|hop| {
                self.edge_scan(
                    RelationOrigin::Edge {
                        input: None,
                        depth: Some(depth),
                        hop: Some(hop),
                    },
                    None,
                    relationships.to_vec(),
                )
            })
            .collect();
        if scoped_by_path {
            for hop in &mut hops {
                hop.traversal_path = Some(self.column(
                    hop.relation,
                    ontology::constants::TRAVERSAL_PATH_COLUMN,
                    Some(ontology::DataType::String),
                ));
            }
        }
        let mut conditions = Vec::new();
        for pair in hops.windows(2) {
            conditions.push(Expr::from(pair[0].target_id).eq(pair[1].source_id));
            if scoped_by_path {
                conditions.push(
                    Expr::from(pair[0].traversal_path.unwrap()).eq(pair[1].traversal_path.unwrap()),
                );
            }
        }
        EdgeChain { hops, conditions }
    }

    fn column(
        &mut self,
        relation: RelationId,
        name: &str,
        data_type: Option<ontology::DataType>,
    ) -> ColumnId {
        let key = ColumnKey {
            relation,
            name: name.into(),
        };
        if let Some(id) = self.catalog.column_ids.get(&key) {
            return *id;
        }
        let id = ColumnId(self.next_column);
        self.next_column += 1;
        self.catalog.column_ids.insert(key, id);
        self.catalog.columns.insert(
            id,
            BoundColumn {
                relation,
                name: name.into(),
                data_type,
            },
        );
        id
    }

    fn entity(&mut self, name: &str) -> EntityId {
        if let Some(id) = self.catalog.entity_ids.get(name) {
            return *id;
        }
        let id = EntityId(self.next_entity);
        self.next_entity += 1;
        self.catalog.entity_ids.insert(name.into(), id);
        self.catalog
            .entities
            .insert(id, BoundEntity { name: name.into() });
        id
    }

    fn relationship(&mut self, name: &str) -> RelationshipId {
        if let Some(id) = self.catalog.relationship_ids.get(name) {
            return *id;
        }
        let id = RelationshipId(self.next_relationship);
        self.next_relationship += 1;
        self.catalog.relationship_ids.insert(name.into(), id);
        self.catalog
            .relationships
            .insert(id, BoundRelationship { name: name.into() });
        id
    }

    fn output(&mut self, name: impl Into<String>) -> OutputId {
        let id = OutputId(self.next_output);
        self.next_output += 1;
        self.catalog
            .outputs
            .insert(id, BoundOutput { name: name.into() });
        id
    }

    fn named(&mut self, expression: impl Into<Expr>, name: impl Into<String>) -> NamedExpr {
        NamedExpr {
            expression: expression.into(),
            output: self.output(name),
        }
    }
}

#[derive(Clone)]
struct Node {
    relation: RelationId,
    input: InputNodeId,
    id: ColumnId,
    plan: Plan<Logical>,
}

#[derive(Clone)]
struct Edge {
    relation: RelationId,
    input: InputRelationshipId,
    from: RelationId,
    to: RelationId,
    direction: Direction,
    relationship_kind: ColumnId,
    source_id: ColumnId,
    source_kind: ColumnId,
    target_id: ColumnId,
    target_kind: ColumnId,
    plan: Plan<Logical>,
}

#[derive(Clone)]
struct EdgeScan {
    relation: RelationId,
    relationship_kind: ColumnId,
    source_id: ColumnId,
    source_kind: ColumnId,
    target_id: ColumnId,
    target_kind: ColumnId,
    traversal_path: Option<ColumnId>,
    plan: Plan<Logical>,
}

impl EdgeScan {
    fn from_id(&self, direction: Direction) -> ColumnId {
        match direction {
            Direction::Outgoing | Direction::Both => self.source_id,
            Direction::Incoming => self.target_id,
        }
    }

    fn to_id(&self, direction: Direction) -> ColumnId {
        match direction {
            Direction::Outgoing | Direction::Both => self.target_id,
            Direction::Incoming => self.source_id,
        }
    }

    fn from_kind(&self, direction: Direction) -> ColumnId {
        match direction {
            Direction::Outgoing | Direction::Both => self.source_kind,
            Direction::Incoming => self.target_kind,
        }
    }

    fn to_kind(&self, direction: Direction) -> ColumnId {
        match direction {
            Direction::Outgoing | Direction::Both => self.target_kind,
            Direction::Incoming => self.source_kind,
        }
    }
}

struct EdgeChain {
    hops: Vec<EdgeScan>,
    conditions: Vec<Expr>,
}

impl Edge {
    fn from_id(&self) -> ColumnId {
        match self.direction {
            Direction::Outgoing | Direction::Both => self.source_id,
            Direction::Incoming => self.target_id,
        }
    }

    fn to_id(&self) -> ColumnId {
        match self.direction {
            Direction::Outgoing | Direction::Both => self.target_id,
            Direction::Incoming => self.source_id,
        }
    }
}

fn ids(column: ColumnId, values: &[i64]) -> Vec<Expr> {
    match values {
        [] => vec![],
        [value] => vec![Expr::from(column).eq(*value)],
        values => vec![Expr::In {
            value: Box::new(Expr::Column(column)),
            values: values.iter().copied().map(Value::Int).collect(),
            data_type: Some(ontology::DataType::Int),
        }],
    }
}

fn requested(node: &InputNode) -> Vec<String> {
    match &node.columns {
        Some(ColumnSelection::List(columns)) => columns.clone(),
        _ => vec![],
    }
}

fn compare(op: CompareOp, left: Expr, right: Expr) -> Expr {
    Expr::Compare {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn literal(value: impl Into<Value>) -> Expr {
    Expr::Literal(value.into())
}

fn value_of(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Bool(value) => Value::Bool(*value),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map_or_else(|| Value::Float(value.to_string()), Value::Int),
        serde_json::Value::String(value) => Value::String(value.clone()),
        value => Value::String(value.to_string()),
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_single_node_to_ids() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                node_ids: vec![1],
                columns: Some(ColumnSelection::List(vec!["username".into()])),
                ..Default::default()
            }],
            limit: 10,
            ..Default::default()
        };
        let ontology = Ontology::new().with_nodes(["User"]);
        let (catalog, plan) = bind(input, std::sync::Arc::new(ontology)).unwrap();
        assert_eq!(catalog.relations.len(), 1);
        assert_eq!(catalog.columns.len(), 3);
        assert!(matches!(plan.root.operator, Operator::Limit(11)));
    }

    #[test]
    fn binds_one_hop_traversal() {
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
            relationships: vec![crate::input::InputRelationship {
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
        let (catalog, _) = bind(input, std::sync::Arc::new(ontology)).unwrap();
        assert_eq!(catalog.relations.len(), 3);
        assert_eq!(catalog.relationships.len(), 1);
    }
}
