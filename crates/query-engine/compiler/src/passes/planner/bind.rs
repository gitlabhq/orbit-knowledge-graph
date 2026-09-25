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
        let mut projections = Vec::new();
        for edge in &edges {
            projections.extend(self.edge_outputs(edge));
        }
        if edges.is_empty()
            || nodes
                .iter()
                .any(|node| !requested(&self.catalog.input.nodes[node.input.0]).is_empty())
        {
            for node in &nodes {
                projections.extend(self.node_outputs(node));
            }
        }
        let mut plan = Plan::unary(Operator::Project(projections), graph);
        if let Some(order) = self.catalog.input.order_by.clone() {
            let relation = nodes
                .iter()
                .find(|node| self.catalog.input.nodes[node.input.0].id == order.node)
                .map(|node| node.relation)
                .ok_or_else(|| QueryError::PipelineInvariant("order node is missing".into()))?;
            let column = self.column(relation, &order.property, None);
            plan = Plan::unary(
                Operator::Sort(vec![SortKey {
                    expression: Expr::Column(column),
                    descending: order.direction == crate::input::OrderDirection::Desc,
                }]),
                plan,
            );
        } else if self.catalog.input.cursor.is_some() && edges.is_empty() {
            let id = self.column(
                nodes[0].relation,
                DEFAULT_PRIMARY_KEY,
                Some(ontology::DataType::Int),
            );
            plan = Plan::unary(
                Operator::Sort(vec![SortKey {
                    expression: Expr::Column(id),
                    descending: false,
                }]),
                plan,
            );
        }
        Ok(Plan::unary(
            Operator::Limit(self.catalog.input.fetch_limit()),
            plan,
        ))
    }

    fn aggregation(&mut self) -> Result<Plan<Logical>> {
        let groups = self.catalog.input.aggregation.group_by.clone();
        let metrics = self.catalog.input.aggregation.metrics.clone();
        let sort = self.catalog.input.aggregation.sort.clone();
        let (graph, nodes, _) = self.graph()?;
        let relations: HashMap<_, _> = nodes
            .iter()
            .map(|node| {
                (
                    self.catalog.input.nodes[node.input.0].id.clone(),
                    node.relation,
                )
            })
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
                    let column = self.column(relations[&node], &property, None);
                    let expression =
                        truncate.map_or(Expr::Column(column), |unit| Expr::DateTrunc {
                            unit,
                            value: Box::new(Expr::Column(column)),
                        });
                    group_exprs.push(NamedExpr {
                        expression,
                        output: self.output(alias.unwrap_or_else(|| match truncate {
                            Some(unit) => format!("{node}_{property}_{}", unit.name()),
                            None => format!("{node}_{property}"),
                        })),
                    });
                }
                InputGroupByKey::Node { node, .. } => {
                    let selected = nodes
                        .iter()
                        .find(|candidate| self.catalog.input.nodes[candidate.input.0].id == node)
                        .unwrap();
                    group_exprs.extend(self.node_outputs(selected));
                }
            }
        }
        let mut metric_exprs = Vec::new();
        for metric in metrics {
            let function = metric.expr.function();
            let value = metric.expr.property().map(|property| {
                Box::new(Expr::Column(self.column(
                    relations[metric.expr.node()],
                    property,
                    None,
                )))
            });
            metric_exprs.push(NamedExpr {
                expression: Expr::Aggregate { function, value },
                output: self.output(metric.output_name()),
            });
        }
        let mut plan = Plan::unary(
            Operator::Aggregate {
                groups: group_exprs,
                metrics: metric_exprs,
            },
            graph,
        );
        if let Some(sort) = sort {
            let output = self
                .catalog
                .outputs
                .iter()
                .find_map(|(id, output)| (output.name == sort.column).then_some(*id));
            if let Some(output) = output {
                plan = Plan::unary(
                    Operator::Sort(vec![SortKey {
                        expression: Expr::Output(output),
                        descending: sort.direction == crate::input::OrderDirection::Desc,
                    }]),
                    plan,
                );
            }
        }
        Ok(Plan::unary(
            Operator::Limit(self.catalog.input.fetch_limit()),
            plan,
        ))
    }

    fn neighbors(&mut self) -> Result<Plan<Logical>> {
        let config =
            self.catalog.input.neighbors.clone().ok_or_else(|| {
                QueryError::PipelineInvariant("neighbors config is missing".into())
            })?;
        let plan = match config.direction {
            Direction::Outgoing => self.neighbor_arm(true, config.rel_types)?,
            Direction::Incoming => self.neighbor_arm(false, config.rel_types)?,
            Direction::Both => Plan {
                operator: Operator::Union,
                inputs: vec![
                    self.neighbor_arm(true, config.rel_types.clone())?,
                    self.neighbor_arm(false, config.rel_types)?,
                ],
            },
        };
        Ok(Plan::unary(
            Operator::Limit(self.catalog.input.fetch_limit()),
            plan,
        ))
    }

    fn neighbor_arm(&mut self, outgoing: bool, kinds: Vec<String>) -> Result<Plan<Logical>> {
        let node = self.node(0)?;
        let relationships = kinds.iter().map(|name| self.relationship(name)).collect();
        let edge_relation = self.relation(
            RelationOrigin::Edge {
                input: None,
                depth: None,
                hop: None,
            },
            None,
            relationships,
        );
        let kind = self.column(
            edge_relation,
            RELATIONSHIP_KIND_COLUMN,
            Some(ontology::DataType::String),
        );
        let mut predicates = match kinds.as_slice() {
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
        };
        let (center_id, neighbor_id, center_kind, neighbor_kind) = if outgoing {
            (
                SOURCE_ID_COLUMN,
                TARGET_ID_COLUMN,
                SOURCE_KIND_COLUMN,
                TARGET_KIND_COLUMN,
            )
        } else {
            (
                TARGET_ID_COLUMN,
                SOURCE_ID_COLUMN,
                TARGET_KIND_COLUMN,
                SOURCE_KIND_COLUMN,
            )
        };
        let edge_center = self.column(edge_relation, center_id, Some(ontology::DataType::Int));
        let node_id = self.column(
            node.relation,
            DEFAULT_PRIMARY_KEY,
            Some(ontology::DataType::Int),
        );
        if let Some(entity) = self.catalog.input.nodes[0].entity.clone() {
            let center_kind =
                self.column(edge_relation, center_kind, Some(ontology::DataType::String));
            predicates.push(compare(
                CompareOp::Eq,
                Expr::Column(center_kind),
                literal(entity),
            ));
        }
        let plan = join(
            [
                filter(
                    Plan::leaf(Operator::Scan(LogicalScan {
                        relation: edge_relation,
                    })),
                    predicates,
                ),
                node.plan,
            ],
            vec![compare(
                CompareOp::Eq,
                Expr::Column(edge_center),
                Expr::Column(node_id),
            )],
        );
        let neighbor_id = self.column(edge_relation, neighbor_id, Some(ontology::DataType::Int));
        let neighbor_kind = self.column(
            edge_relation,
            neighbor_kind,
            Some(ontology::DataType::String),
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
                expression: Expr::Column(kind),
                output: self.output(crate::constants::relationship_type_column()),
            },
            NamedExpr {
                expression: literal(i64::from(outgoing)),
                output: self.output(crate::constants::neighbor_is_outgoing_column()),
            },
            NamedExpr {
                expression: Expr::Column(node_id),
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
        Ok(Plan::unary(Operator::Project(columns), plan))
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
        for depth in 1..=path.max_depth {
            let start_node = self.node(start)?;
            let end_node = self.node(end)?;
            let mut edges = Vec::new();
            for hop in 1..=depth {
                let relationships = path
                    .rel_types
                    .iter()
                    .map(|name| self.relationship(name))
                    .collect();
                let relation = self.relation(
                    RelationOrigin::Edge {
                        input: None,
                        depth: Some(depth),
                        hop: Some(hop),
                    },
                    None,
                    relationships,
                );
                let kind = self.column(
                    relation,
                    RELATIONSHIP_KIND_COLUMN,
                    Some(ontology::DataType::String),
                );
                let mut predicates = match path.rel_types.as_slice() {
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
                            value: Box::new(Expr::Column(kind)),
                            values: endpoint_types,
                            data_type: Some(ontology::DataType::String),
                        });
                    }
                }
                if hop == 1 {
                    let source =
                        self.column(relation, SOURCE_ID_COLUMN, Some(ontology::DataType::Int));
                    predicates.extend(ids(source, &self.catalog.input.nodes[start].node_ids));
                }
                if hop == depth {
                    let target =
                        self.column(relation, TARGET_ID_COLUMN, Some(ontology::DataType::Int));
                    predicates.extend(ids(target, &self.catalog.input.nodes[end].node_ids));
                }
                edges.push((
                    relation,
                    filter(
                        Plan::leaf(Operator::Scan(LogicalScan { relation })),
                        predicates,
                    ),
                ));
            }
            let mut conditions = Vec::new();
            let scoped_by_path = self.catalog.input.nodes[start].has_traversal_path
                && self.catalog.input.nodes[end].has_traversal_path;
            let start_id = self.column(
                start_node.relation,
                DEFAULT_PRIMARY_KEY,
                Some(ontology::DataType::Int),
            );
            let first_source =
                self.column(edges[0].0, SOURCE_ID_COLUMN, Some(ontology::DataType::Int));
            conditions.push(compare(
                CompareOp::Eq,
                Expr::Column(start_id),
                Expr::Column(first_source),
            ));
            for pair in edges.windows(2) {
                let left = self.column(pair[0].0, TARGET_ID_COLUMN, Some(ontology::DataType::Int));
                let right = self.column(pair[1].0, SOURCE_ID_COLUMN, Some(ontology::DataType::Int));
                conditions.push(compare(
                    CompareOp::Eq,
                    Expr::Column(left),
                    Expr::Column(right),
                ));
                if scoped_by_path {
                    let left_path = self.column(
                        pair[0].0,
                        ontology::constants::TRAVERSAL_PATH_COLUMN,
                        Some(ontology::DataType::String),
                    );
                    let right_path = self.column(
                        pair[1].0,
                        ontology::constants::TRAVERSAL_PATH_COLUMN,
                        Some(ontology::DataType::String),
                    );
                    conditions.push(compare(
                        CompareOp::Eq,
                        Expr::Column(left_path),
                        Expr::Column(right_path),
                    ));
                }
            }
            let last_target = self.column(
                edges.last().unwrap().0,
                TARGET_ID_COLUMN,
                Some(ontology::DataType::Int),
            );
            let end_id = self.column(
                end_node.relation,
                DEFAULT_PRIMARY_KEY,
                Some(ontology::DataType::Int),
            );
            conditions.push(compare(
                CompareOp::Eq,
                Expr::Column(last_target),
                Expr::Column(end_id),
            ));
            let inputs = std::iter::once(start_node.plan)
                .chain(edges.iter().map(|edge| edge.1.clone()))
                .chain(std::iter::once(end_node.plan));
            let plan = join(inputs, conditions);
            let start_kind = self.catalog.input.nodes[start]
                .entity
                .clone()
                .unwrap_or_default();
            let path_values = std::iter::once(Expr::Tuple(vec![
                Expr::Column(start_id),
                literal(start_kind),
            ]))
            .chain(edges.iter().map(|edge| {
                let id = self.column(edge.0, TARGET_ID_COLUMN, Some(ontology::DataType::Int));
                let kind =
                    self.column(edge.0, TARGET_KIND_COLUMN, Some(ontology::DataType::String));
                Expr::Tuple(vec![Expr::Column(id), Expr::Column(kind)])
            }))
            .collect();
            let edge_kinds = edges
                .iter()
                .map(|edge| {
                    Expr::Column(self.column(
                        edge.0,
                        RELATIONSHIP_KIND_COLUMN,
                        Some(ontology::DataType::String),
                    ))
                })
                .collect();
            arms.push(Plan::unary(
                Operator::Project(vec![
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
                ]),
                plan,
            ));
        }
        let union = Plan {
            operator: Operator::Union,
            inputs: arms,
        };
        let sorted = Plan::unary(
            Operator::Sort(vec![SortKey {
                expression: Expr::Output(depth_output),
                descending: false,
            }]),
            union,
        );
        Ok(Plan::unary(
            Operator::Limit(self.catalog.input.fetch_limit()),
            sorted,
        ))
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
                filter(current, vec![Expr::Or(predicates)])
            };
            let properties = requested(&input)
                .into_iter()
                .map(|name| {
                    let column = self.column(node.relation, &name, None);
                    (name, Expr::Stringify(Box::new(Expr::Column(column))))
                })
                .collect();
            arms.push(Plan::unary(
                Operator::Project(vec![
                    NamedExpr {
                        expression: Expr::Column(id),
                        output: self.output(format!("{}_{}", input.id, input.id_property)),
                    },
                    NamedExpr {
                        expression: literal(input.entity.unwrap_or_default()),
                        output: self.output(format!("{}_entity_type", input.id)),
                    },
                    NamedExpr {
                        expression: Expr::Stringify(Box::new(Expr::JsonObject(properties))),
                        output: self.output(format!("{}_props", input.id)),
                    },
                ]),
                current,
            ));
        }
        Ok(Plan::unary(
            Operator::Limit(self.catalog.input.fetch_limit()),
            if arms.len() == 1 {
                arms.pop().unwrap()
            } else {
                Plan {
                    operator: Operator::Union,
                    inputs: arms,
                }
            },
        ))
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
            let (source, target) = edge.direction.edge_columns();
            for (node, name) in [(edge.from, source), (edge.to, target)] {
                let edge_column =
                    Expr::Column(self.column(edge.relation, name, Some(ontology::DataType::Int)));
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
        Ok((join(inputs, conditions), nodes, edges))
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
        let plan = filter(Plan::leaf(Operator::Scan(LogicalScan { relation })), {
            let mut predicates = self.node_predicates(relation, &input, id);
            let deleted = self.column(
                relation,
                ontology::constants::DELETED_COLUMN,
                Some(ontology::DataType::Bool),
            );
            predicates.push(compare(
                CompareOp::Eq,
                Expr::Column(deleted),
                literal(false),
            ));
            predicates
        });
        Ok(Node {
            relation,
            input: InputNodeId(index),
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
        let relation = self.relation(
            RelationOrigin::Edge {
                input: Some(InputRelationshipId(index)),
                depth: None,
                hop: None,
            },
            None,
            relationships,
        );
        let plan = if input.hops.max == 1 {
            filter(
                Plan::leaf(Operator::Scan(LogicalScan { relation })),
                self.edge_predicates(relation, &input),
            )
        } else {
            let mut arms = Vec::new();
            for depth in input.hops.min.max(1)..=input.hops.max {
                let mut hops = Vec::new();
                for hop in 1..=depth {
                    let relationships = input
                        .types
                        .iter()
                        .map(|name| self.relationship(name))
                        .collect();
                    let hop_relation = self.relation(
                        RelationOrigin::Edge {
                            input: None,
                            depth: Some(depth),
                            hop: Some(hop),
                        },
                        None,
                        relationships,
                    );
                    let scan = filter(
                        Plan::leaf(Operator::Scan(LogicalScan {
                            relation: hop_relation,
                        })),
                        {
                            let mut predicates = self.kind_predicates(hop_relation, &input.types);
                            if hop == 1 {
                                let (source_name, _) = input.direction.edge_columns();
                                let source = self.column(
                                    hop_relation,
                                    source_name,
                                    Some(ontology::DataType::Int),
                                );
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
                                    let kind = self.column(
                                        hop_relation,
                                        if source_name == SOURCE_ID_COLUMN {
                                            SOURCE_KIND_COLUMN
                                        } else {
                                            TARGET_KIND_COLUMN
                                        },
                                        Some(ontology::DataType::String),
                                    );
                                    predicates.push(compare(
                                        CompareOp::Eq,
                                        Expr::Column(kind),
                                        literal(entity.to_string()),
                                    ));
                                }
                            }
                            if hop == depth {
                                let (_, target_name) = input.direction.edge_columns();
                                let target = self.column(
                                    hop_relation,
                                    target_name,
                                    Some(ontology::DataType::Int),
                                );
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
                                    let kind = self.column(
                                        hop_relation,
                                        if target_name == SOURCE_ID_COLUMN {
                                            SOURCE_KIND_COLUMN
                                        } else {
                                            TARGET_KIND_COLUMN
                                        },
                                        Some(ontology::DataType::String),
                                    );
                                    predicates.push(compare(
                                        CompareOp::Eq,
                                        Expr::Column(kind),
                                        literal(entity.to_string()),
                                    ));
                                }
                            }
                            predicates
                        },
                    );
                    hops.push((hop_relation, scan));
                }
                let mut conditions = Vec::new();
                for pair in hops.windows(2) {
                    let left =
                        self.column(pair[0].0, TARGET_ID_COLUMN, Some(ontology::DataType::Int));
                    let right =
                        self.column(pair[1].0, SOURCE_ID_COLUMN, Some(ontology::DataType::Int));
                    conditions.push(compare(
                        CompareOp::Eq,
                        Expr::Column(left),
                        Expr::Column(right),
                    ));
                }
                let first = hops[0].0;
                let last = hops.last().unwrap().0;
                let columns = [
                    (
                        RELATIONSHIP_KIND_COLUMN,
                        self.column(
                            first,
                            RELATIONSHIP_KIND_COLUMN,
                            Some(ontology::DataType::String),
                        ),
                    ),
                    (
                        SOURCE_ID_COLUMN,
                        self.column(first, SOURCE_ID_COLUMN, Some(ontology::DataType::Int)),
                    ),
                    (
                        SOURCE_KIND_COLUMN,
                        self.column(first, SOURCE_KIND_COLUMN, Some(ontology::DataType::String)),
                    ),
                    (
                        TARGET_ID_COLUMN,
                        self.column(last, TARGET_ID_COLUMN, Some(ontology::DataType::Int)),
                    ),
                    (
                        TARGET_KIND_COLUMN,
                        self.column(last, TARGET_KIND_COLUMN, Some(ontology::DataType::String)),
                    ),
                    (
                        ontology::constants::SOURCE_TAGS_COLUMN,
                        self.column(
                            first,
                            ontology::constants::SOURCE_TAGS_COLUMN,
                            Some(ontology::DataType::String),
                        ),
                    ),
                    (
                        ontology::constants::TARGET_TAGS_COLUMN,
                        self.column(
                            last,
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
                        hops.iter()
                            .map(|hop| {
                                let id = self.column(
                                    hop.0,
                                    TARGET_ID_COLUMN,
                                    Some(ontology::DataType::Int),
                                );
                                let kind = self.column(
                                    hop.0,
                                    TARGET_KIND_COLUMN,
                                    Some(ontology::DataType::String),
                                );
                                Expr::Tuple(vec![Expr::Column(id), Expr::Column(kind)])
                            })
                            .collect(),
                    ),
                    output: self.output(crate::constants::PATH_NODES_COLUMN),
                });
                arms.push(Plan::unary(
                    Operator::Project(columns),
                    join(hops.into_iter().map(|hop| hop.1), conditions),
                ));
            }
            filter(
                Plan::unary(
                    Operator::Bind(relation),
                    Plan {
                        operator: Operator::Union,
                        inputs: arms,
                    },
                ),
                {
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
                            let list = self.column(
                                relation,
                                column_name,
                                Some(ontology::DataType::String),
                            );
                            for filter in filters {
                                let values: Vec<_> = match filter.value.as_ref() {
                                    Some(serde_json::Value::Array(values)) => {
                                        values.iter().collect()
                                    }
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
                },
            )
        };
        Ok(Edge {
            relation,
            input: InputRelationshipId(index),
            from: nodes[&input.from],
            to: nodes[&input.to],
            direction: input.direction,
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
            predicates.push(compare(
                CompareOp::Ge,
                Expr::Column(id),
                literal(range.start),
            ));
            predicates.push(compare(CompareOp::Le, Expr::Column(id), literal(range.end)));
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
            (RELATIONSHIP_KIND_COLUMN, "type", ontology::DataType::String),
            (SOURCE_ID_COLUMN, "src", ontology::DataType::Int),
            (SOURCE_KIND_COLUMN, "src_type", ontology::DataType::String),
            (TARGET_ID_COLUMN, "dst", ontology::DataType::Int),
            (TARGET_KIND_COLUMN, "dst_type", ontology::DataType::String),
        ]
        .into_iter()
        .map(|(name, suffix, data_type)| {
            let column = self.column(edge.relation, name, Some(data_type));
            NamedExpr {
                expression: Expr::Column(column),
                output: self.output(format!("{prefix}_{suffix}")),
            }
        })
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
}

#[derive(Clone)]
struct Node {
    relation: RelationId,
    input: InputNodeId,
    plan: Plan<Logical>,
}

#[derive(Clone)]
struct Edge {
    relation: RelationId,
    input: InputRelationshipId,
    from: RelationId,
    to: RelationId,
    direction: Direction,
    plan: Plan<Logical>,
}

fn filter(plan: Plan<Logical>, predicates: Vec<Expr>) -> Plan<Logical> {
    if predicates.is_empty() {
        plan
    } else {
        Plan::unary(Operator::Filter(conjunction(predicates)), plan)
    }
}

fn join(inputs: impl IntoIterator<Item = Plan<Logical>>, conditions: Vec<Expr>) -> Plan<Logical> {
    let mut inputs: Vec<_> = inputs.into_iter().collect();
    if inputs.len() == 1 {
        inputs.pop().unwrap()
    } else {
        Plan {
            operator: Operator::Join(conditions),
            inputs,
        }
    }
}

fn ids(column: ColumnId, values: &[i64]) -> Vec<Expr> {
    match values {
        [] => vec![],
        [value] => vec![compare(
            CompareOp::Eq,
            Expr::Column(column),
            literal(*value),
        )],
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

fn conjunction(mut predicates: Vec<Expr>) -> Expr {
    if predicates.len() == 1 {
        predicates.pop().unwrap()
    } else {
        Expr::And(predicates)
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
