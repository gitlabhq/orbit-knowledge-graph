use super::*;
use compiler::query_graph::{
    BlockId, ColumnRef, Expression, JoinKind, LatestRows, OperationKind, Port, QueryGraph,
    ReadMode, Relational, Source,
};
use query_data_model::QueryDataModel;

pub(crate) trait GraphPhase: std::fmt::Debug {
    const PLANNED: bool;
    fn version(&self) -> ColumnRef<'_>;
}

impl GraphPhase for LatestRows<'_> {
    const PLANNED: bool = true;
    fn version(&self) -> ColumnRef<'_> {
        self.version()
    }
}

impl GraphPhase for std::convert::Infallible {
    const PLANNED: bool = false;
    fn version(&self) -> ColumnRef<'_> {
        match *self {}
    }
}

fn tables<M: QueryDataModel + ?Sized, L>(
    graph: &QueryGraph<'_, M, L>,
    root: BlockId,
    matches: impl Fn(&str) -> bool,
) -> Vec<String> {
    let mut tables = Vec::new();
    for block in graph.reachable_blocks(root).unwrap() {
        for relation in graph.relations(block).unwrap() {
            let relation = graph.relation(relation).unwrap();
            if matches(&relation.hint)
                && let Source::Stored(table) = relation.source
            {
                tables.push(table.name().to_owned());
            }
        }
    }
    tables.sort();
    tables.dedup();
    tables
}

pub(crate) fn graph_pathfinding<'a, M: QueryDataModel + ?Sized>(
    graph: &QueryGraph<'a, M, LatestRows<'a>>,
    root: BlockId,
    input: &Input,
) -> Tree {
    let path = input.path.as_ref().unwrap();
    let start = input
        .nodes
        .iter()
        .find(|node| node.id == path.from)
        .unwrap();
    let end = input.nodes.iter().find(|node| node.id == path.to).unwrap();
    let (mut forward, mut backward, mut scoped) = (0, 0, false);
    for (definition, body) in graph.definitions(root).unwrap() {
        let name = graph.definition_hint(definition).unwrap();
        if !matches!(name, "forward" | "backward") {
            continue;
        }
        for arm in graph
            .union_arms(body)
            .unwrap()
            .unwrap_or(std::slice::from_ref(&body))
        {
            for output in graph.outputs(*arm).unwrap() {
                let label = graph.output_label(output).unwrap();
                if label == "depth"
                    && let Expression::Integer(depth) = graph.projection(output).unwrap()
                {
                    if name == "forward" {
                        forward = forward.max(*depth);
                    } else {
                        backward = backward.max(*depth);
                    }
                }
                scoped |= label == "traversal_path";
            }
        }
    }
    let tables = tables(graph, root, |hint| hint.starts_with('e') || hint == "_e");
    let kinds = |node: &compiler::InputNode, source| {
        if path.rel_types.is_any() {
            graph
                .catalog()
                .graph()
                .relationship_names(
                    if source { node.entity.as_deref() } else { None },
                    if source { None } else { node.entity.as_deref() },
                )
                .join(", ")
        } else {
            String::new()
        }
    };
    let nodes = [start, end]
        .iter()
        .map(|node| {
            let mut predicates = Vec::new();
            for block in graph.blocks() {
                let Ok(mut operation) = graph.operation(block) else {
                    continue;
                };
                let mut filters = Vec::new();
                while let OperationKind::Filter { input, predicate } = operation.kind() {
                    filters.extend(conjunction(graph, predicate));
                    operation = input;
                }
                if let OperationKind::Source { relation, .. } = operation.kind()
                    && graph.relation(*relation).unwrap().hint == node.id
                {
                    predicates.extend(filters);
                }
            }
            filter(
                predicates,
                leaf(
                    Operator::NodeScan,
                    format!("{} AS {}", node.entity.as_deref().unwrap(), node.id),
                ),
            )
        })
        .collect();
    Tree::node(
        Operator::PathFinding,
        format!(
            "{}->{} depth={} forward={forward} backward={backward} scoped={scoped} tables=[{}] relationships=[{}] forward_kinds=[{}] backward_kinds=[{}]",
            path.from,
            path.to,
            path.max_depth,
            tables.join(", "),
            path.rel_types.join(", "),
            kinds(start, true),
            kinds(end, false)
        ),
        nodes,
    )
}

pub(crate) fn graph_neighbors<'a, M: QueryDataModel + ?Sized>(
    graph: &QueryGraph<'a, M, LatestRows<'a>>,
    root: BlockId,
    input: &Input,
) -> Tree {
    let center = &input.nodes[0];
    let config = input.neighbors.as_ref().unwrap();
    let tables = |root| tables(graph, root, |hint| matches!(hint, "e" | "_e"));
    let OperationKind::Limit { input: source, .. } = graph.operation(root).unwrap().kind() else {
        unreachable!()
    };
    let access = if matches!(source.kind(), OperationKind::Expand { .. }) {
        format!("fused table={}", tables(root).join(", "))
    } else {
        let OperationKind::Source { relation, .. } = source.kind() else {
            unreachable!()
        };
        let Source::Derived(body) = graph.relation(*relation).unwrap().source else {
            unreachable!()
        };
        let (outgoing, incoming) = if config.direction == compiler::input::Direction::Both {
            let arms = graph.union_arms(body).unwrap().unwrap();
            (tables(arms[0]), tables(arms[1]))
        } else {
            (tables(body), tables(body))
        };
        format!(
            "directional outgoing=[{}] incoming=[{}]",
            outgoing.join(", "),
            incoming.join(", ")
        )
    };
    let center_scan = graph.reachable_blocks(root).unwrap().iter().any(|block| {
        graph.relations(*block).unwrap().any(|relation| {
            graph.relation(relation).unwrap().input
                == Some(compiler::query_graph::ScanInput::Node(0))
        })
    });
    let lookup = graph
        .catalog()
        .traversal_path_lookup(
            center.entity.as_deref().unwrap(),
            ontology::TraversalPathKind::Id,
        )
        .map_or_else(
            || "none".into(),
            |(table, column)| format!("{table}.{column}"),
        );
    Tree::node(
        Operator::Neighbors,
        format!(
            "center={} direction={} {access} center_filter={} relationships=[{}] path_lookup={lookup}",
            center.id,
            direction_name(config.direction),
            center_scan && (!center.filters.is_empty() || center.id_range.is_some()),
            config.rel_types.join(", ")
        ),
        vec![leaf(
            Operator::NodeScan,
            format!("{} AS {}", center.entity.as_deref().unwrap(), center.id),
        )],
    )
}

pub(crate) fn graph_hydration<'a, M: QueryDataModel + ?Sized>(
    graph: &QueryGraph<'a, M, LatestRows<'a>>,
    root: BlockId,
) -> Tree {
    let OperationKind::Limit { input, .. } = graph.operation(root).unwrap().kind() else {
        unreachable!()
    };
    let OperationKind::Source { relation, .. } = input.kind() else {
        unreachable!()
    };
    let Source::Derived(body) = graph.relation(*relation).unwrap().source else {
        unreachable!()
    };
    let arms = graph
        .union_arms(body)
        .unwrap()
        .unwrap_or(std::slice::from_ref(&body));
    Tree::node(
        Operator::Hydration,
        "",
        arms.iter()
            .map(|arm| {
                let OperationKind::Filter { input, predicate } =
                    graph.operation(*arm).unwrap().kind()
                else {
                    unreachable!()
                };
                let OperationKind::Source { relation, .. } = input.kind() else {
                    unreachable!()
                };
                let Source::Derived(keys) = graph.relation(*relation).unwrap().source else {
                    unreachable!()
                };
                let fields = graph
                    .outputs(*arm)
                    .unwrap()
                    .find_map(|output| match graph.projection(output).unwrap() {
                        Expression::JsonObject(fields) => Some(fields),
                        _ => None,
                    })
                    .unwrap();
                let projection = fields
                    .iter()
                    .map(|(_, value)| match value {
                        Expression::ToString(value) => graph_expression(graph, value),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Tree::node(
                    Operator::Project,
                    projection,
                    vec![filter(
                        vec![graph_expression(graph, predicate)],
                        graph_operation(graph, graph.operation(keys).unwrap()),
                    )],
                )
            })
            .collect(),
    )
}

pub(crate) fn query_graph<'a, M: QueryDataModel + ?Sized, L: GraphPhase>(
    graph: &QueryGraph<'a, M, L>,
    root: BlockId,
) -> Tree {
    if let Some(arms) = graph.union_arms(root).unwrap() {
        return Tree::node(
            Operator::Union,
            "ALL",
            arms.iter().map(|arm| query_graph(graph, *arm)).collect(),
        );
    }
    let projection = graph
        .outputs(root)
        .unwrap()
        .map(|output| {
            format!(
                "{} AS {}",
                graph_expression(graph, graph.projection(output).unwrap()),
                graph.output_label(output).unwrap()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut operation = graph.operation(root).unwrap();
    let limit = if let OperationKind::Limit { input, count } = operation.kind() {
        operation = input;
        Some(*count)
    } else {
        None
    };
    let dedup = if !L::PLANNED
        && let OperationKind::FirstBy { input, keys } = operation.kind()
    {
        operation = input;
        Some(keys)
    } else {
        None
    };
    let order = if let OperationKind::Sort { input, keys } = operation.kind() {
        operation = input;
        Some(keys)
    } else {
        None
    };
    let mut tree = if let OperationKind::Aggregate { input, groups } = operation.kind() {
        let groups = groups
            .iter()
            .map(|group| graph_expression(graph, group))
            .collect::<Vec<_>>();
        let head = if groups.is_empty() {
            projection
        } else {
            format!("group {}, {projection}", groups.join(", "))
        };
        let source = graph_operation(graph, input);
        Tree::node(
            Operator::Aggregate,
            head,
            vec![if L::PLANNED {
                Tree::node(Operator::Project, "", vec![source])
            } else {
                source
            }],
        )
    } else {
        Tree::node(
            Operator::Project,
            projection,
            vec![graph_operation(graph, operation)],
        )
    };
    if !L::PLANNED {
        if let Some(keys) = order {
            tree = Tree::node(Operator::Sort, graph_order(graph, keys), vec![tree]);
        }
        if let Some(keys) = dedup {
            tree = Tree::node(
                Operator::Deduplicate,
                format!("LimitBy 1 BY {}", columns(graph, keys)),
                vec![tree],
            );
        }
        if let Some(count) = limit {
            tree = Tree::node(Operator::Limit, count.to_string(), vec![tree]);
        }
    }
    let definitions = graph
        .definitions(root)
        .unwrap()
        .map(|(definition, body)| {
            Tree::node(
                Operator::Cte,
                graph.definition_hint(definition).unwrap(),
                vec![query_graph(graph, body)],
            )
        })
        .collect::<Vec<_>>();
    if definitions.is_empty() {
        tree
    } else {
        Tree::node(
            Operator::With,
            "",
            definitions.into_iter().chain([tree]).collect(),
        )
    }
}

fn columns<'a, M: QueryDataModel + ?Sized, L: GraphPhase>(
    graph: &QueryGraph<'a, M, L>,
    keys: &[ColumnRef<'a>],
) -> String {
    keys.iter()
        .map(|column| graph_expression(graph, &Expression::Column(*column)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn graph_order<'a, M: QueryDataModel + ?Sized, L: GraphPhase>(
    graph: &QueryGraph<'a, M, L>,
    keys: &[(ColumnRef<'a>, bool)],
) -> String {
    keys.iter()
        .map(|(column, descending)| {
            format!(
                "{}{}",
                graph_expression(graph, &Expression::Column(*column)),
                if *descending { " DESC" } else { "" }
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn expression_list<'a, M: QueryDataModel + ?Sized, L: GraphPhase>(
    graph: &QueryGraph<'a, M, L>,
    values: &[Expression<'a>],
) -> String {
    values
        .iter()
        .map(|value| graph_expression(graph, value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn graph_expression<'a, M: QueryDataModel + ?Sized, L: GraphPhase>(
    graph: &QueryGraph<'a, M, L>,
    value: &Expression<'a>,
) -> String {
    let render = |value: &Expression<'a>| graph_expression(graph, value);
    match value {
        Expression::Column(column) => {
            let name = match column.port() {
                Port::Stored(column) => column.name(),
                Port::Output(output) => graph.output_label(output).unwrap(),
            };
            format!("{}.{name}", graph.relation(column.relation()).unwrap().hint)
        }
        Expression::Text(value) => text_literal(value),
        Expression::Integer(value) => value.to_string(),
        Expression::Boolean(value) => value.to_string(),
        Expression::Integers(values) => format!(
            "[{}]",
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expression::Strings(values) => format!(
            "[{}]",
            values
                .iter()
                .map(|value| text_literal(value))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expression::InQuery { value, key } => {
            format!("{} IN {}", render(value), render(&Expression::Column(*key)))
        }
        Expression::Add(left, right) => format!("({} + {})", render(left), render(right)),
        Expression::Prefixes {
            value,
            paths,
            array,
        } => {
            let value = render(value);
            let Expression::Strings(paths) = paths.as_ref() else {
                unreachable!()
            };
            let paths = paths
                .iter()
                .map(|path| text_literal(path))
                .collect::<Vec<_>>();
            if L::PLANNED {
                format!(
                    "{value} PREFIX {} [{}]",
                    if *array { "SET" } else { "UNION" },
                    paths.join(", ")
                )
            } else if *array {
                format!(
                    "ArrayExists(_gkg_path -> StartsWith({value}, _gkg_path), [{}])",
                    paths.join(", ")
                )
            } else {
                paths
                    .iter()
                    .map(|path| format!("StartsWith({value}, {path})"))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            }
        }
        Expression::Predicate {
            operator,
            value,
            argument,
            fold_case,
        } => {
            let value = render(value);
            let argument = argument
                .as_ref()
                .map(|argument| render(argument))
                .unwrap_or_default();
            let comparison = match operator {
                FilterOp::Eq => Some("="),
                FilterOp::Ne => Some("!="),
                FilterOp::Gt => Some(">"),
                FilterOp::Lt => Some("<"),
                FilterOp::Gte => Some(">="),
                FilterOp::Lte => Some("<="),
                FilterOp::In => Some("IN"),
                _ => None,
            };
            if let Some(comparison) = comparison {
                format!("{value} {comparison} {argument}")
            } else if matches!(operator, FilterOp::IsNull | FilterOp::IsNotNull) {
                format!(
                    "{value} IS {}NULL",
                    if *operator == FilterOp::IsNotNull {
                        "NOT "
                    } else {
                        ""
                    }
                )
            } else if L::PLANNED {
                format!("{}({value}, {argument})", operator.as_ref())
            } else {
                let fold = |value| {
                    if *fold_case {
                        format!("Lower({value})")
                    } else {
                        value
                    }
                };
                format!("{operator:?}({}, {})", fold(value), fold(argument))
            }
        }
        Expression::Array(values)
            if values
                .iter()
                .all(|value| matches!(value, Expression::Tuple(_))) =>
        {
            if L::PLANNED {
                format!(
                    "path[{}]",
                    values
                        .iter()
                        .map(|value| match value {
                            Expression::Tuple(fields) =>
                                format!("({})", expression_list(graph, fields)),
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                format!("Array({})", expression_list(graph, values))
            }
        }
        Expression::Tuple(values) => format!("Tuple({})", expression_list(graph, values)),
        Expression::Array(values) => format!("[{}]", expression_list(graph, values)),
        Expression::Bucket { unit, value } => format!("bucket({}, {})", unit.name(), render(value)),
        Expression::GreaterEqual(left, right) | Expression::LessEqual(left, right) => format!(
            "{} {} {}",
            render(left),
            if matches!(value, Expression::GreaterEqual(..)) {
                ">="
            } else {
                "<="
            },
            render(right)
        ),
        Expression::Count => "COUNT()".into(),
        Expression::HasAny(value, values) => {
            let value = render(value);
            let Expression::Array(values) = values.as_ref() else {
                unreachable!()
            };
            let values = values.iter().map(render).collect::<Vec<_>>();
            if L::PLANNED {
                format!("{value} HAS ANY [{}]", values.join(", "))
            } else if let [element] = values.as_slice() {
                format!("ArrayContains({value}, {element})")
            } else {
                format!("ArrayContainsAny({value}, Array({}))", values.join(", "))
            }
        }
        Expression::CountIf(condition) => {
            let parts = conjunction(graph, condition);
            let condition = if L::PLANNED {
                parts.join(", ")
            } else {
                parts
                    .iter()
                    .map(|part| format!("({part})"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            };
            format!("COUNT() FILTER [{condition}]")
        }
        Expression::In(left, right)
            if !L::PLANNED
                && matches!(right.as_ref(), Expression::Integers(values) if values.len() == 1) =>
        {
            let Expression::Integers(values) = right.as_ref() else {
                unreachable!()
            };
            format!("{} = {}", render(left), values[0])
        }
        Expression::In(left, right) => format!("{} IN {}", render(left), render(right)),
        Expression::Equal(left, right) => format!("{} = {}", render(left), render(right)),
        Expression::Or(left, right) => format!("({}) OR ({})", render(left), render(right)),
        Expression::And(_, _) => conjunction(graph, value)
            .iter()
            .map(|part| {
                if part.starts_with("ArrayContains") || part.contains(" IN ") {
                    part.clone()
                } else {
                    format!("({part})")
                }
            })
            .collect::<Vec<_>>()
            .join(" AND "),
        value => format!("{value:?}"),
    }
}

fn conjunction<'a, M: QueryDataModel + ?Sized, L: GraphPhase>(
    graph: &QueryGraph<'a, M, L>,
    value: &Expression<'a>,
) -> Vec<String> {
    if let Expression::And(left, right) = value {
        conjunction(graph, left)
            .into_iter()
            .chain(conjunction(graph, right))
            .collect()
    } else {
        vec![graph_expression(graph, value)]
    }
}

fn graph_operation<'a, M: QueryDataModel + ?Sized, L: GraphPhase>(
    graph: &QueryGraph<'a, M, L>,
    operation: &Relational<'a, L>,
) -> Tree {
    match operation.kind() {
        OperationKind::One => leaf(Operator::Scan, "One"),
        OperationKind::Materialize { input, relation } => {
            let source = graph_operation(graph, input);
            Tree::node(
                Operator::Bind,
                &graph.relation(*relation).unwrap().hint,
                vec![if L::PLANNED {
                    source
                } else {
                    Tree::node(Operator::Project, "*", vec![source])
                }],
            )
        }
        OperationKind::Source { relation, read } => {
            let declaration = graph.relation(*relation).unwrap();
            match declaration.source {
                Source::Stored(table) => scan(
                    table.name(),
                    &declaration.hint,
                    matches!(read, ReadMode::Current),
                ),
                Source::Definition(definition) => scan(
                    graph.definition_hint(definition).unwrap(),
                    &declaration.hint,
                    false,
                ),
                Source::Derived(body) => {
                    if let Some(arms) = graph.union_arms(body).unwrap()
                        && matches!(
                            declaration.input,
                            Some(compiler::query_graph::ScanInput::Relationship(_))
                        )
                    {
                        Tree::node(
                            Operator::Union,
                            format!("ALL AS {}", declaration.hint),
                            arms.iter().map(|arm| query_graph(graph, *arm)).collect(),
                        )
                    } else {
                        Tree::node(
                            Operator::Bind,
                            &declaration.hint,
                            vec![query_graph(graph, body)],
                        )
                    }
                }
            }
        }
        OperationKind::Filter { input, predicate } => {
            filter(conjunction(graph, predicate), graph_operation(graph, input))
        }
        OperationKind::Join {
            left,
            right,
            kind: JoinKind::Membership,
            condition,
        } => {
            let Expression::Equal(value, key) = condition else {
                unreachable!()
            };
            let OperationKind::Source { relation, .. } = right.kind() else {
                unreachable!()
            };
            if matches!(
                graph.relation(*relation).unwrap().source,
                Source::Definition(_)
            ) {
                filter(
                    vec![format!(
                        "{} IN {}",
                        graph_expression(graph, value),
                        graph_expression(graph, key)
                    )],
                    graph_operation(graph, left),
                )
            } else {
                let keys = match graph.relation(*relation).unwrap().source {
                    Source::Derived(body) => query_graph(graph, body),
                    _ => graph_operation(graph, right),
                };
                Tree::node(
                    Operator::SemiJoin,
                    format!("{} IN subquery", graph_expression(graph, value)),
                    vec![graph_operation(graph, left), keys],
                )
            }
        }
        OperationKind::Join {
            left,
            right,
            kind,
            condition,
        } => Tree::node(
            if matches!(kind, JoinKind::Semi) {
                Operator::SemiJoin
            } else {
                Operator::Join
            },
            format!("ON {}", graph_expression(graph, condition)),
            vec![graph_operation(graph, left), graph_operation(graph, right)],
        ),
        OperationKind::Aggregate { input, groups } => Tree::node(
            Operator::Aggregate,
            format!("{groups:?}"),
            vec![graph_operation(graph, input)],
        ),
        OperationKind::Latest { input, requirement } => {
            let relation = graph.relation(requirement.version().relation()).unwrap();
            let Source::Stored(table) = relation.source else {
                unreachable!()
            };
            let keys = graph
                .catalog()
                .table_sort_key(table.name())
                .unwrap()
                .iter()
                .map(|name| format!("{}.{name}", relation.hint))
                .collect::<Vec<_>>()
                .join(", ");
            Tree::node(
                Operator::Deduplicate,
                format!("LimitBy {keys}"),
                vec![graph_operation(graph, input)],
            )
        }
        OperationKind::FirstBy { input, keys } => Tree::node(
            Operator::Deduplicate,
            format!("LimitBy 1 BY {}", columns(graph, keys)),
            vec![graph_operation(graph, input)],
        ),
        OperationKind::Sort { input, keys } => Tree::node(
            Operator::Sort,
            graph_order(graph, keys),
            vec![graph_operation(graph, input)],
        ),
        OperationKind::Limit { input, count } => Tree::node(
            Operator::Limit,
            count.to_string(),
            vec![graph_operation(graph, input)],
        ),
        OperationKind::Expand { input, column } => Tree::node(
            Operator::Project,
            format!(
                "expand {}",
                graph_expression(graph, &Expression::Column(*column))
            ),
            vec![graph_operation(graph, input)],
        ),
    }
}
