use super::{Operator, Tree, direction_name, filter, leaf, scan};
use compiler::input::Input;
use compiler::query_graph::api::{
    Aggregate, Column, Expr, ExprKind, Function, Join, Named, OperationKind, Operator as Binary,
    QueryGraph, QueryId, Read, Rows,
};
use query_data_model::QueryDataModel;
use query_engine::compiler;

pub(crate) fn query_graph<M: QueryDataModel + ?Sized>(
    graph: &QueryGraph<'_, M>,
    root: QueryId,
) -> Tree {
    View { graph, root }.query(root)
}

struct View<'graph, 'catalog, M: QueryDataModel + ?Sized> {
    graph: &'graph QueryGraph<'catalog, M>,
    root: QueryId,
}

impl<M: QueryDataModel + ?Sized> View<'_, '_, M> {
    fn definition(&self, query: QueryId) -> Option<&str> {
        for owner in self.graph.reachable(self.root).unwrap() {
            for (name, body) in self.graph.definitions(owner).unwrap() {
                if body == query {
                    return Some(name);
                }
            }
        }
        None
    }

    fn column(&self, column: &Column) -> String {
        for query in self.graph.reachable(self.root).unwrap() {
            let mut name = None;
            self.graph
                .rows(query)
                .unwrap()
                .walk(&mut |rows| {
                    if rows.columns().contains(column) {
                        match rows.kind() {
                            OperationKind::Scan { table, label, .. } => {
                                name = Some(format!(
                                    "{}.{}",
                                    label.as_deref().unwrap_or(table.name()),
                                    column.name()
                                ));
                            }
                            OperationKind::Read { query, .. } => {
                                name = Some(self.definition(*query).map_or_else(
                                    || column.name().to_owned(),
                                    |name| format!("{name}.{}", column.name()),
                                ));
                            }
                            _ => {}
                        }
                    }
                    Ok::<_, std::convert::Infallible>(())
                })
                .unwrap();
            if let Some(name) = name {
                return name;
            }
        }
        column.name().into()
    }

    fn expression(&self, value: &Expr) -> String {
        match value.kind() {
            ExprKind::Column(column) => self.column(column),
            ExprKind::Literal(value) => literal(&value.value),
            ExprKind::Scalar { column, .. } => format!("scalar({})", column.name()),
            ExprKind::Binary {
                operator,
                left,
                right,
            } => {
                let operator = match operator {
                    Binary::Equal => "=",
                    Binary::NotEqual => "!=",
                    Binary::Greater => ">",
                    Binary::GreaterEqual => ">=",
                    Binary::Less => "<",
                    Binary::LessEqual => "<=",
                    Binary::And => "AND",
                    Binary::Or => "OR",
                    Binary::Add => "+",
                    Binary::In => "IN",
                };
                format!(
                    "{} {operator} {}",
                    self.expression(left),
                    self.expression(right)
                )
            }
            ExprKind::Aggregate {
                function,
                arguments,
                filter,
            } => {
                let name = match function {
                    Aggregate::Count => "count",
                    Aggregate::Sum => "sum",
                    Aggregate::Average => "avg",
                    Aggregate::Min => "min",
                    Aggregate::Max => "max",
                    Aggregate::Collect => "collect",
                    Aggregate::ArgMax => "argMaxOrNull",
                };
                let mut arguments = arguments
                    .iter()
                    .map(|value| self.expression(value))
                    .collect::<Vec<_>>();
                if let Some(filter) = filter {
                    arguments.push(self.expression(filter));
                }
                format!(
                    "{name}{}({})",
                    if filter.is_some() { "If" } else { "" },
                    arguments.join(", ")
                )
            }
            ExprKind::Call {
                function,
                arguments,
            } => {
                let arguments = arguments
                    .iter()
                    .map(|value| self.expression(value))
                    .collect::<Vec<_>>();
                match function {
                    Function::Array => format!("[{}]", arguments.join(", ")),
                    Function::IsNull => format!("{} IS NULL", arguments[0]),
                    Function::IsNotNull => format!("{} IS NOT NULL", arguments[0]),
                    Function::TimeBucket(unit) => {
                        format!("date_trunc({}, {})", unit.name(), arguments[0])
                    }
                    Function::TupleField(index) => format!("field({}, {index})", arguments[0]),
                    _ => format!("{function:?}({})", arguments.join(", ")),
                }
            }
        }
    }

    fn outputs(&self, values: &[Named]) -> String {
        values
            .iter()
            .map(|value| format!("{} AS {}", self.expression(&value.value), value.name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn query(&self, query: QueryId) -> Tree {
        let tree = self.rows(self.graph.rows(query).unwrap());
        let definitions = self
            .graph
            .definitions(query)
            .unwrap()
            .map(|(name, body)| Tree::node(Operator::Cte, name, vec![self.query(body)]))
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

    fn rows(&self, rows: &Rows<'_>) -> Tree {
        match rows.kind() {
            OperationKind::Unit => leaf(Operator::Scan, "One"),
            OperationKind::Scan {
                table, label, read, ..
            } => scan(
                table.name(),
                label.as_deref().unwrap_or(table.name()),
                *read == Read::Current,
            ),
            OperationKind::Read { query, .. } => {
                if let Some(name) = self.definition(*query) {
                    scan(name, name, false)
                } else {
                    Tree::node(Operator::Bind, "", vec![self.query(*query)])
                }
            }
            OperationKind::Filter { input, predicate } => {
                filter(self.conjunction(predicate), self.rows(input))
            }
            OperationKind::Select { input, values } => Tree::node(
                Operator::Project,
                self.outputs(values),
                vec![self.rows(input)],
            ),
            OperationKind::Aggregate {
                input,
                groups,
                measures,
            } => {
                let mut values = groups
                    .iter()
                    .map(|group| {
                        format!("group {} AS {}", self.expression(&group.value), group.name)
                    })
                    .collect::<Vec<_>>();
                values.extend(
                    measures.iter().map(|value| {
                        format!("{} AS {}", self.expression(&value.value), value.name)
                    }),
                );
                Tree::node(
                    Operator::Aggregate,
                    values.join(", "),
                    vec![self.rows(input)],
                )
            }
            OperationKind::Join {
                left,
                right,
                kind: Join::Membership,
                condition,
            } => {
                let ExprKind::Binary {
                    left: value,
                    right: key,
                    ..
                } = condition.kind()
                else {
                    unreachable!()
                };
                if let OperationKind::Read { query, .. } = right.kind()
                    && self.definition(*query).is_some()
                {
                    filter(
                        vec![format!(
                            "{} IN {}",
                            self.expression(value),
                            self.expression(key)
                        )],
                        self.rows(left),
                    )
                } else {
                    Tree::node(
                        Operator::SemiJoin,
                        format!("{} IN subquery", self.expression(value)),
                        vec![self.rows(left), self.rows(right)],
                    )
                }
            }
            OperationKind::Join {
                left,
                right,
                kind,
                condition,
            } => Tree::node(
                if *kind == Join::Semi {
                    Operator::SemiJoin
                } else {
                    Operator::Join
                },
                if *kind == Join::Cross {
                    "CROSS".into()
                } else {
                    format!("ON {}", self.expression(condition))
                },
                vec![self.rows(left), self.rows(right)],
            ),
            OperationKind::Latest { input, keys, .. } | OperationKind::FirstBy { input, keys } => {
                Tree::node(
                    Operator::Deduplicate,
                    format!(
                        "LimitBy {}{}",
                        if matches!(rows.kind(), OperationKind::FirstBy { .. }) {
                            "1 BY "
                        } else {
                            ""
                        },
                        keys.iter()
                            .map(|key| self.column(key))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    vec![self.rows(input)],
                )
            }
            OperationKind::Limit { input, count } => {
                Tree::node(Operator::Limit, count.to_string(), vec![self.rows(input)])
            }
            OperationKind::Sort { input, keys } => Tree::node(
                Operator::Sort,
                keys.iter()
                    .map(|key| {
                        format!(
                            "{}{}",
                            self.expression(&key.value),
                            if key.descending { " DESC" } else { "" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                vec![self.rows(input)],
            ),
            OperationKind::Expand { input, value } => Tree::node(
                Operator::Project,
                format!("expand {} AS {}", self.expression(&value.value), value.name),
                vec![self.rows(input)],
            ),
            OperationKind::Union { arms } => Tree::node(
                Operator::Union,
                "ALL",
                arms.iter().map(|arm| self.query(*arm)).collect(),
            ),
        }
    }

    fn conjunction(&self, value: &Expr) -> Vec<String> {
        if let ExprKind::Binary {
            operator: Binary::And,
            left,
            right,
        } = value.kind()
        {
            self.conjunction(left)
                .into_iter()
                .chain(self.conjunction(right))
                .collect()
        } else {
            vec![self.expression(value)]
        }
    }
}

fn literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => format!("'{}'", value.replace('\'', "''")),
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values.iter().map(literal).collect::<Vec<_>>().join(", ")
        ),
        _ => value.to_string(),
    }
}

fn tables<M: QueryDataModel + ?Sized>(graph: &QueryGraph<'_, M>, root: QueryId) -> Vec<String> {
    let mut tables = Vec::new();
    for query in graph.reachable(root).unwrap() {
        graph
            .rows(query)
            .unwrap()
            .walk(&mut |rows| {
                if let OperationKind::Scan { table, .. } = rows.kind()
                    && table.column("relationship_kind").is_some()
                {
                    tables.push(table.name().to_owned());
                }
                Ok::<_, std::convert::Infallible>(())
            })
            .unwrap();
    }
    tables.sort();
    tables.dedup();
    tables
}

pub(crate) fn graph_hydration<M: QueryDataModel + ?Sized>(
    graph: &QueryGraph<'_, M>,
    root: QueryId,
) -> Tree {
    let view = View { graph, root };
    let mut arms = Vec::new();
    for query in graph.reachable(root).unwrap() {
        graph
            .rows(query)
            .unwrap()
            .walk(&mut |rows| {
                if let OperationKind::Select { input, values } = rows.kind() {
                    for value in values {
                        if let ExprKind::Call {
                            function: Function::JsonObject(_),
                            arguments,
                        } = value.value.kind()
                        {
                            let fields = arguments
                                .iter()
                                .map(|value| {
                                    if let ExprKind::Call {
                                        function: Function::ToString,
                                        arguments,
                                    } = value.kind()
                                    {
                                        view.expression(&arguments[0])
                                    } else {
                                        view.expression(value)
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join(", ");
                            arms.push(Tree::node(
                                Operator::Project,
                                fields,
                                vec![view.rows(input)],
                            ));
                        }
                    }
                }
                Ok::<_, std::convert::Infallible>(())
            })
            .unwrap();
    }
    Tree::node(Operator::Hydration, "", arms)
}

pub(crate) fn graph_neighbors<M: QueryDataModel + ?Sized>(
    graph: &QueryGraph<'_, M>,
    root: QueryId,
    input: &Input,
) -> Tree {
    let center = &input.nodes[0];
    let config = input.neighbors.as_ref().unwrap();
    let mut fused = false;
    let mut center_scan = false;
    let mut outgoing = Vec::new();
    let mut incoming = Vec::new();
    for query in graph.reachable(root).unwrap() {
        graph
            .rows(query)
            .unwrap()
            .walk(&mut |rows| {
                fused |= matches!(rows.kind(), OperationKind::Expand { .. });
                if let OperationKind::Scan { label, .. } = rows.kind() {
                    center_scan |= label.as_deref() == Some(&center.id);
                }
                if let OperationKind::Select { values, .. } = rows.kind() {
                    for value in values {
                        if value.name == compiler::constants::neighbor_is_outgoing_column()
                            && let ExprKind::Literal(value) = value.value.kind()
                        {
                            if value.value == 1 {
                                outgoing.extend(tables(graph, query));
                            } else {
                                incoming.extend(tables(graph, query));
                            }
                        }
                    }
                }
                Ok::<_, std::convert::Infallible>(())
            })
            .unwrap();
    }
    outgoing.sort();
    outgoing.dedup();
    incoming.sort();
    incoming.dedup();
    let access = if fused {
        format!("fused table={}", tables(graph, root).join(", "))
    } else {
        format!(
            "directional outgoing=[{}] incoming=[{}]",
            outgoing.join(", "),
            incoming.join(", ")
        )
    };
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
            config.rel_types.join(", "),
        ),
        vec![leaf(
            Operator::NodeScan,
            format!("{} AS {}", center.entity.as_deref().unwrap(), center.id),
        )],
    )
}

pub(crate) fn graph_pathfinding<M: QueryDataModel + ?Sized>(
    graph: &QueryGraph<'_, M>,
    root: QueryId,
    input: &Input,
) -> Tree {
    let path = input.path.as_ref().unwrap();
    let mut depths = [0, 0];
    let mut scoped = false;
    for (name, query) in graph.definitions(root).unwrap() {
        let index = match name {
            "forward" => 0,
            "backward" => 1,
            _ => continue,
        };
        for query in graph.reachable(query).unwrap() {
            graph
                .rows(query)
                .unwrap()
                .walk(&mut |rows| {
                    if let OperationKind::Select { values, .. } = rows.kind() {
                        for value in values {
                            if value.name == "depth"
                                && let ExprKind::Literal(value) = value.value.kind()
                            {
                                depths[index] =
                                    depths[index].max(value.value.as_i64().unwrap_or(0));
                            }
                            scoped |= value.name == ontology::TRAVERSAL_PATH_COLUMN;
                        }
                    }
                    Ok::<_, std::convert::Infallible>(())
                })
                .unwrap();
        }
    }
    let endpoints = [&path.from, &path.to]
        .map(|alias| input.nodes.iter().find(|node| node.id == *alias).unwrap());
    let kinds = |index: usize| {
        if path.rel_types.is_any() {
            graph
                .catalog()
                .graph()
                .relationship_names(
                    if index == 0 {
                        endpoints[index].entity.as_deref()
                    } else {
                        None
                    },
                    if index == 1 {
                        endpoints[index].entity.as_deref()
                    } else {
                        None
                    },
                )
                .join(", ")
        } else {
            String::new()
        }
    };
    let view = View { graph, root };
    let children = endpoints
        .iter()
        .map(|node| {
            let mut predicates = Vec::new();
            for query in graph.reachable(root).unwrap() {
                graph
                    .rows(query)
                    .unwrap()
                    .walk(&mut |rows| {
                        if let OperationKind::Filter { predicate, input } = rows.kind()
                            && input.column_from(&node.id, "id").is_ok()
                        {
                            predicates.extend(view.conjunction(predicate));
                        }
                        Ok::<_, std::convert::Infallible>(())
                    })
                    .unwrap();
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
            "{}->{} depth={} forward={} backward={} scoped={scoped} tables=[{}] relationships=[{}] forward_kinds=[{}] backward_kinds=[{}]",
            path.from,
            path.to,
            path.max_depth,
            depths[0],
            depths[1],
            tables(graph, root).join(", "),
            path.rel_types.join(", "),
            kinds(0),
            kinds(1),
        ),
        children,
    )
}
