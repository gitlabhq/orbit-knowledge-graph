use super::*;
use crate::input::{
    AggFunction, ColumnSelection, Direction, FilterOp, Input, InputGroupByKey, InputNode,
    InputRelationship, OrderDirection, QueryType,
};
use ontology::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN,
    SOURCE_KIND_COLUMN, TARGET_ID_COLUMN, TARGET_KIND_COLUMN,
};
use std::collections::{BTreeMap, HashMap};

pub fn plan(input: &Input) -> LogicalPlan {
    let mut planner = Planner {
        input,
        relations: BTreeMap::new(),
        next_relation: 0,
    };
    let root = match input.query_type {
        QueryType::Traversal => planner.traversal(),
        QueryType::Aggregation => planner.aggregation(),
        QueryType::Neighbors => planner.neighbors(),
        QueryType::PathFinding => planner.pathfinding(),
        QueryType::Hydration => planner.hydration(),
    }
    .limit(input.fetch_limit());
    LogicalPlan {
        root,
        relations: planner.relations,
    }
}

struct Planner<'a> {
    input: &'a Input,
    relations: BTreeMap<RelationId, LogicalRelation>,
    next_relation: u32,
}

impl Planner<'_> {
    fn relation(&mut self, alias: impl Into<String>, source: LogicalRelationSource) -> RelationId {
        let relation = RelationId(self.next_relation);
        self.next_relation += 1;
        self.relations.insert(
            relation,
            LogicalRelation {
                alias: alias.into(),
                source,
            },
        );
        relation
    }

    fn node(&mut self, index: usize, alias: &str) -> (RelationId, Rel) {
        let input_node = &self.input.nodes[index];
        let relation = self.relation(alias, LogicalRelationSource::Node(index));
        let scan = node(
            relation,
            input_node.entity.clone().unwrap_or_default(),
            alias,
        )
        .filter(node_predicates(relation, input_node));
        (relation, scan)
    }

    fn edge(
        &mut self,
        index: Option<usize>,
        relationships: Vec<String>,
        alias: &str,
    ) -> (RelationId, Rel) {
        let relation = self.relation(alias, LogicalRelationSource::Edge(index));
        (relation, edge(relation, relationships, alias))
    }

    fn traversal(&mut self) -> Rel {
        let order_by = self.input.order_by.clone();
        let (relation, edges, nodes) = self.graph();
        let columns = edges
            .iter()
            .flat_map(|edge| edge_output(edge.relation, &edge.prefix, edge.multi_hop))
            .chain(nodes.iter().flat_map(|node| node_projection(node.relation, node.input)))
            .collect();
        relation
            .project(columns)
            .sort(traversal_sort(&order_by, &nodes))
    }

    fn aggregation(&mut self) -> Rel {
        let group_by = self.input.aggregation.group_by.clone();
        let metrics_input = self.input.aggregation.metrics.clone();
        let sort_input = self.input.aggregation.sort.clone();
        let (relation, _, nodes) = self.graph();
        let node_relations: HashMap<_, _> = nodes
            .iter()
            .map(|node| (node.input.id.as_str(), node.relation))
            .collect();
        let groups = group_by
            .iter()
            .flat_map(|group| match group {
                InputGroupByKey::Property {
                    node,
                    property,
                    truncate,
                    ..
                } => {
                    let value = column(node_relations[node.as_str()], property);
                    vec![named(
                        truncate.map_or(value.clone(), |unit| Expr::DateTrunc {
                            unit,
                            value: Box::new(value),
                        }),
                        group.output_name(),
                    )]
                }
                InputGroupByKey::Node { node, .. } => nodes
                    .iter()
                    .find(|candidate| candidate.input.id == *node)
                    .map(|node| node_projection(node.relation, node.input))
                    .unwrap_or_default(),
            })
            .collect();
        let metrics = metrics_input
            .iter()
            .map(|metric| {
                let function = metric.expr.function();
                named(
                    Expr::Aggregate {
                        function,
                        value: (function != AggFunction::Count)
                            .then(|| metric.expr.property())
                            .flatten()
                            .map(|property| {
                                Box::new(column(
                                    node_relations[metric.expr.node()],
                                    property,
                                ))
                            }),
                    },
                    metric.output_name(),
                )
            })
            .collect();
        let sort = sort_input
            .iter()
            .map(|sort| SortKey {
                expression: Expr::Identifier(sort.column.clone()),
                descending: sort.direction == OrderDirection::Desc,
            })
            .collect();
        relation.aggregate(groups, metrics).sort(sort)
    }

    fn neighbors(&mut self) -> Rel {
        let config = self.input.neighbors.as_ref().expect("neighbors config");
        let center_index = 0;
        let arm = |planner: &mut Self, outgoing: bool| {
            let edge_alias = if outgoing { "out" } else { "in" };
            let (edge_id, edge_scan) =
                planner.edge(None, config.rel_types.clone(), edge_alias);
            let (center_id, center_scan) = planner.node(center_index, &format!("center_{edge_alias}"));
            let (edge_center, edge_neighbor, edge_kind, neighbor_kind) = if outgoing {
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
            join(
                [
                    edge_scan.filter(relationship_types(edge_id, &config.rel_types)),
                    center_scan,
                ],
                [column(edge_id, edge_center).eq(column(center_id, DEFAULT_PRIMARY_KEY))],
            )
            .filter([column(edge_id, edge_kind).eq(literal(
                planner.input.nodes[center_index]
                    .entity
                    .clone()
                    .unwrap_or_default(),
            ))])
            .project(vec![
                named(column(edge_id, edge_neighbor), crate::constants::neighbor_id_column()),
                named(column(edge_id, neighbor_kind), crate::constants::neighbor_type_column()),
                named(
                    column(edge_id, RELATIONSHIP_KIND_COLUMN),
                    crate::constants::relationship_type_column(),
                ),
                named(literal(outgoing), crate::constants::neighbor_is_outgoing_column()),
            ])
        };
        match config.direction {
            Direction::Outgoing => arm(self, true),
            Direction::Incoming => arm(self, false),
            Direction::Both => union([arm(self, true), arm(self, false)]),
        }
    }

    fn pathfinding(&mut self) -> Rel {
        let config = self.input.path.as_ref().expect("path config");
        let start_index = node_index(self.input, &config.from);
        let end_index = node_index(self.input, &config.to);
        let arms: Vec<_> = (1..=config.max_depth)
            .map(|depth| {
                let (start_id, start) = self.node(start_index, &format!("start_{depth}"));
                let (end_id, end) = self.node(end_index, &format!("end_{depth}"));
                let edges: Vec<_> = (1..=depth)
                    .map(|hop| {
                        let alias = format!("path_{depth}_{hop}");
                        self.edge(None, config.rel_types.clone(), &alias)
                    })
                    .collect();
                let conditions = std::iter::once(
                    column(start_id, DEFAULT_PRIMARY_KEY)
                        .eq(column(edges[0].0, SOURCE_ID_COLUMN)),
                )
                .chain(edges.windows(2).map(|edge| {
                    column(edge[0].0, TARGET_ID_COLUMN)
                        .eq(column(edge[1].0, SOURCE_ID_COLUMN))
                }))
                .chain(std::iter::once(
                    column(edges.last().unwrap().0, TARGET_ID_COLUMN)
                        .eq(column(end_id, DEFAULT_PRIMARY_KEY)),
                ));
                let path = std::iter::once(Expr::Tuple(vec![
                    column(start_id, DEFAULT_PRIMARY_KEY),
                    literal(
                        self.input.nodes[start_index]
                            .entity
                            .clone()
                            .unwrap_or_default(),
                    ),
                ]))
                .chain(edges.iter().map(|edge| {
                    Expr::Tuple(vec![
                        column(edge.0, TARGET_ID_COLUMN),
                        column(edge.0, TARGET_KIND_COLUMN),
                    ])
                }))
                .collect();
                join(
                    std::iter::once(start)
                        .chain(edges.iter().map(|edge| edge.1.clone().filter(
                            relationship_types(edge.0, &config.rel_types),
                        )))
                        .chain(std::iter::once(end)),
                    conditions,
                )
                .project(vec![
                    named(Expr::Array(path), crate::constants::path_column()),
                    named(
                        Expr::Array(
                            edges
                                .iter()
                                .map(|edge| column(edge.0, RELATIONSHIP_KIND_COLUMN))
                                .collect(),
                        ),
                        crate::constants::edge_kinds_column(),
                    ),
                    named(literal(i64::from(depth)), "depth"),
                ])
            })
            .collect();
        union(arms).sort(vec![SortKey {
            expression: Expr::Identifier("depth".into()),
            descending: false,
        }])
    }

    fn hydration(&mut self) -> Rel {
        let arms: Vec<_> = self
            .input
            .nodes
            .iter()
            .enumerate()
            .map(|(index, input_node)| {
                let alias = format!("{}_{}", input_node.id, index);
                let (relation, scan) = self.node(index, &alias);
                let properties = requested(input_node)
                    .into_iter()
                    .map(|property| {
                        (
                            property.clone(),
                            Expr::Stringify(Box::new(column(relation, property))),
                        )
                    })
                    .collect();
                scan.latest_by(vec![column(relation, &input_node.id_property)])
                    .filter([column(relation, DELETED_COLUMN).eq(literal(false))])
                    .project(vec![
                        named(
                            column(relation, &input_node.id_property),
                            format!("{}_{}", input_node.id, input_node.id_property),
                        ),
                        named(
                            literal(input_node.entity.clone().unwrap_or_default()),
                            format!("{}_entity_type", input_node.id),
                        ),
                        named(
                            Expr::JsonObject(properties),
                            format!("{}_props", input_node.id),
                        ),
                    ])
            })
            .collect();
        union(arms)
    }

    fn graph(&mut self) -> (Rel, Vec<EdgePlan>, Vec<NodePlan<'_>>) {
        let nodes: Vec<_> = self
            .input
            .nodes
            .iter()
            .enumerate()
            .map(|(index, input)| {
                let (relation, scan) = self.node(index, &input.id);
                NodePlan {
                    relation,
                    input,
                    scan,
                }
            })
            .collect();
        let node_ids: HashMap<_, _> = nodes
            .iter()
            .map(|node| (node.input.id.as_str(), node.relation))
            .collect();
        let edges: Vec<_> = self
            .input
            .relationships
            .iter()
            .enumerate()
            .map(|(index, input)| self.relationship(index, input, &node_ids))
            .collect();
        let relation = join(
            edges
                .iter()
                .map(|edge| edge.scan.clone())
                .chain(nodes.iter().map(|node| node.scan.clone())),
            join_conditions(&edges, &nodes, &self.input.join_predicates),
        );
        (relation, edges, nodes)
    }

    fn relationship(
        &mut self,
        index: usize,
        input: &InputRelationship,
        node_ids: &HashMap<&str, RelationId>,
    ) -> EdgePlan {
        let alias = format!("e{index}");
        let (relation, scan) = if input.hops.max == 1 {
            let (relation, scan) = self.edge(Some(index), input.types.clone(), &alias);
            (relation, scan.filter(edge_predicates(input, relation, node_ids, self.input)))
        } else {
            let relation = self.relation(&alias, LogicalRelationSource::Edge(Some(index)));
            let arms: Vec<_> = (input.hops.min.max(1)..=input.hops.max)
                .map(|depth| {
                    let hops: Vec<_> = (1..=depth)
                        .map(|hop| {
                            self.edge(
                                None,
                                input.types.clone(),
                                &format!("{alias}_h{hop}"),
                            )
                        })
                        .collect();
                    let conditions = hops.windows(2).map(|pair| {
                        column(pair[0].0, TARGET_ID_COLUMN)
                            .eq(column(pair[1].0, SOURCE_ID_COLUMN))
                    });
                    let first = hops[0].0;
                    let last = hops.last().unwrap().0;
                    join(
                        hops.iter().map(|hop| {
                            hop.1.clone().filter(relationship_types(hop.0, &input.types))
                        }),
                        conditions,
                    )
                    .project(vec![
                        named(column(first, RELATIONSHIP_KIND_COLUMN), RELATIONSHIP_KIND_COLUMN),
                        named(column(first, SOURCE_ID_COLUMN), SOURCE_ID_COLUMN),
                        named(column(first, SOURCE_KIND_COLUMN), SOURCE_KIND_COLUMN),
                        named(column(last, TARGET_ID_COLUMN), TARGET_ID_COLUMN),
                        named(column(last, TARGET_KIND_COLUMN), TARGET_KIND_COLUMN),
                        named(
                            Expr::Array(
                                hops.iter()
                                    .map(|hop| {
                                        Expr::Tuple(vec![
                                            column(hop.0, TARGET_ID_COLUMN),
                                            column(hop.0, TARGET_KIND_COLUMN),
                                        ])
                                    })
                                    .collect(),
                            ),
                            crate::constants::PATH_NODES_COLUMN,
                        ),
                    ])
                })
                .collect();
            (relation, union(arms).alias(relation, alias.clone()))
        };
        EdgePlan {
            relation,
            prefix: if input.hops.max > 1 {
                format!("hop_e{index}")
            } else {
                alias
            },
            multi_hop: input.hops.max > 1,
            from: node_ids[&input.from.as_str()],
            to: node_ids[&input.to.as_str()],
            direction: input.direction,
            scan,
        }
    }

}

fn traversal_sort(
    order_by: &Option<crate::input::InputOrderBy>,
    nodes: &[NodePlan<'_>],
) -> Vec<SortKey> {
    order_by
        .iter()
        .filter_map(|order| {
            let relation = nodes
                .iter()
                .find(|node| node.input.id == order.node)?
                .relation;
            Some(SortKey {
                expression: column(relation, &order.property),
                descending: order.direction == OrderDirection::Desc,
            })
        })
        .collect()
}

struct NodePlan<'a> {
    relation: RelationId,
    input: &'a InputNode,
    scan: Rel,
}

struct EdgePlan {
    relation: RelationId,
    prefix: String,
    multi_hop: bool,
    from: RelationId,
    to: RelationId,
    direction: Direction,
    scan: Rel,
}

fn edge_output(relation: RelationId, prefix: &str, multi_hop: bool) -> Vec<NamedExpr> {
    let mut columns: Vec<_> = [
        (RELATIONSHIP_KIND_COLUMN, "type"),
        (SOURCE_ID_COLUMN, "src"),
        (SOURCE_KIND_COLUMN, "src_type"),
        (TARGET_ID_COLUMN, "dst"),
        (TARGET_KIND_COLUMN, "dst_type"),
    ]
    .into_iter()
    .map(|(name, suffix)| named(column(relation, name), format!("{prefix}_{suffix}")))
    .collect();
    if multi_hop {
        columns.push(named(
            column(relation, crate::constants::PATH_NODES_COLUMN),
            format!("{prefix}_path_nodes"),
        ));
    }
    columns
}

fn node_projection(relation: RelationId, node: &InputNode) -> Vec<NamedExpr> {
    requested(node)
        .into_iter()
        .map(|property| {
            named(
                column(relation, &property),
                format!("{}_{}", node.id, property),
            )
        })
        .collect()
}

fn node_predicates(relation: RelationId, node: &InputNode) -> Vec<Expr> {
    let mut predicates = filters(relation, &node.filters);
    predicates.extend(ids(relation, DEFAULT_PRIMARY_KEY, &node.node_ids));
    if let Some(range) = &node.id_range {
        predicates.extend([
            column(relation, DEFAULT_PRIMARY_KEY).compare(CompareOp::Ge, literal(range.start)),
            column(relation, DEFAULT_PRIMARY_KEY).compare(CompareOp::Le, literal(range.end)),
        ]);
    }
    predicates
}

fn edge_predicates(
    relationship: &InputRelationship,
    relation: RelationId,
    node_ids: &HashMap<&str, RelationId>,
    input: &Input,
) -> Vec<Expr> {
    let (start, end) = relationship.direction.edge_columns();
    let mut predicates = relationship_types(relation, &relationship.types);
    for (node_alias, id_column) in [(&relationship.from, start), (&relationship.to, end)] {
        let node = input
            .nodes
            .iter()
            .find(|node| node.id == *node_alias)
            .unwrap();
        predicates.extend(node.entity.iter().map(|entity| {
            column(relation, kind_column(id_column)).eq(literal(entity.clone()))
        }));
        predicates.extend(ids(relation, id_column, &node.node_ids));
        let _ = node_ids[node_alias.as_str()];
    }
    predicates.extend(filters(relation, &relationship.filters));
    predicates
}

fn join_conditions(
    edges: &[EdgePlan],
    nodes: &[NodePlan<'_>],
    predicates: &[crate::input::JoinPredicate],
) -> Vec<Expr> {
    let mut bindings = HashMap::<RelationId, Expr>::new();
    let mut conditions = Vec::new();
    for edge in edges {
        let (start, end) = edge.direction.edge_columns();
        for (node, edge_column) in [(edge.from, start), (edge.to, end)] {
            let edge_column = column(edge.relation, edge_column);
            if let Some(bound) = bindings.insert(node, edge_column.clone()) {
                conditions.push(bound.eq(edge_column));
            }
        }
    }
    conditions.extend(nodes.iter().filter_map(|node| {
        bindings
            .get(&node.relation)
            .cloned()
            .map(|bound| bound.eq(column(node.relation, DEFAULT_PRIMARY_KEY)))
    }));
    let relations: HashMap<_, _> = nodes
        .iter()
        .map(|node| (node.input.id.as_str(), node.relation))
        .collect();
    conditions.extend(predicates.iter().map(|predicate| {
        column(relations[predicate.lhs_node.as_str()], &predicate.lhs_prop).compare(
            comparison(predicate.op),
            column(relations[predicate.rhs_node.as_str()], &predicate.rhs_prop),
        )
    }));
    conditions
}

fn ids(relation: RelationId, name: &str, ids: &[i64]) -> Vec<Expr> {
    match ids {
        [] => Vec::new(),
        [id] => vec![column(relation, name).eq(literal(*id))],
        ids => vec![Expr::In {
            value: Box::new(column(relation, name)),
            values: ids.iter().copied().map(Value::Int).collect(),
            data_type: Some(ontology::DataType::Int),
        }],
    }
}

fn relationship_types(relation: RelationId, relationships: &[String]) -> Vec<Expr> {
    match relationships {
        [] => Vec::new(),
        [relationship] if relationship != "*" => vec![column(relation, RELATIONSHIP_KIND_COLUMN)
            .eq(literal(relationship.clone()))],
        relationships if relationships.iter().all(|relationship| relationship != "*") => {
            vec![Expr::In {
                value: Box::new(column(relation, RELATIONSHIP_KIND_COLUMN)),
                values: relationships.iter().cloned().map(Value::String).collect(),
                data_type: Some(ontology::DataType::String),
            }]
        }
        _ => Vec::new(),
    }
}

fn comparison(operator: FilterOp) -> CompareOp {
    match operator {
        FilterOp::Eq => CompareOp::Eq,
        FilterOp::Ne => CompareOp::Ne,
        FilterOp::Lt => CompareOp::Lt,
        FilterOp::Lte => CompareOp::Le,
        FilterOp::Gt => CompareOp::Gt,
        FilterOp::Gte => CompareOp::Ge,
        _ => unreachable!("validated property comparison"),
    }
}

fn node_index(input: &Input, id: &str) -> usize {
    input
        .nodes
        .iter()
        .position(|node| node.id == id)
        .expect("validated node reference")
}

fn kind_column(id_column: &str) -> &'static str {
    if id_column == SOURCE_ID_COLUMN {
        SOURCE_KIND_COLUMN
    } else {
        TARGET_KIND_COLUMN
    }
}

fn requested(node: &InputNode) -> Vec<String> {
    match &node.columns {
        Some(ColumnSelection::List(columns)) => columns.clone(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{HopRange, InputNeighbors, InputPath, PathType};

    fn query(query_type: QueryType) -> Input {
        Input {
            query_type,
            limit: 10,
            nodes: vec![InputNode {
                id: "center".into(),
                entity: Some("Node".into()),
                node_ids: vec![1],
                columns: Some(ColumnSelection::List(vec!["name".into()])),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn neighbors_is_union_of_relational_arms() {
        let mut input = query(QueryType::Neighbors);
        input.neighbors = Some(InputNeighbors {
            direction: Direction::Both,
            rel_types: vec!["REL".into()],
        });
        let plan = plan(&input);
        assert!(plan.root.explain().contains("Union"));
        assert!(plan.relations.len() >= 4);
    }

    #[test]
    fn pathfinding_is_union_of_bounded_join_arms() {
        let mut input = query(QueryType::PathFinding);
        input.nodes.push(InputNode {
            id: "end".into(),
            entity: Some("Node".into()),
            node_ids: vec![2],
            ..Default::default()
        });
        input.path = Some(InputPath {
            path_type: PathType::Shortest,
            from: "center".into(),
            to: "end".into(),
            max_depth: 2,
            rel_types: vec!["REL".into()],
            forward_first_hop_rel_types: Vec::new(),
            backward_first_hop_rel_types: Vec::new(),
        });
        let plan = plan(&input);
        assert!(plan.root.explain().contains("Union"));
        assert!(plan.root.explain().contains("Join"));
    }

    #[test]
    fn logical_plan_ignores_compiler_metadata() {
        let input = query(QueryType::Traversal);
        let mut changed = input.clone();
        changed.compiler.default_edge_table = "backend_table".into();
        assert_eq!(plan(&input), plan(&changed));
    }

    #[test]
    fn relationship_hops_are_expanded_before_physical_planning() {
        let mut input = query(QueryType::Traversal);
        input.nodes.push(InputNode {
            id: "end".into(),
            entity: Some("Node".into()),
            ..Default::default()
        });
        input.relationships.push(InputRelationship {
            from: "center".into(),
            to: "end".into(),
            types: vec!["REL".into()],
            hops: HopRange { min: 1, max: 2 },
            direction: Direction::Outgoing,
            filters: Default::default(),
            fk_column: None,
            scope_prefix: None,
            scope_preserving: false,
        });
        assert!(plan(&input).root.explain().contains("Union"));
    }
}
