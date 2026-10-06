use compiler::ast::{Expr, Node, Op, Query, SelectExpr, TableRef};
use compiler::input::{ColumnSelection, FilterOp, Input, InputFilter, OrderDirection};
use compiler::passes::plan::QueryPlan;
use compiler::passes::plan::physical::{ExecutionPlan, PhysicalPlan, PhysicalSource};
use compiler::passes::plan::requirements::{Column, OutputValue, Predicate, Projection};
use query_engine::compiler;
use std::collections::HashMap;

use super::operator::Operator;
use super::pattern::Expression as Tree;

fn leaf(label: Operator, text: impl Into<String>) -> Tree {
    Tree::node(label, text, vec![])
}

fn filter(predicates: Vec<String>, input: Tree) -> Tree {
    if predicates.is_empty() {
        return input;
    }
    if input.label == Operator::Filter {
        let mut input = input;
        input.items.extend(predicates);
        input
    } else {
        Tree::node(Operator::Filter, predicates.join(", "), vec![input])
    }
}

pub fn logical(input: &Input) -> Tree {
    let mut children: Vec<_> = input
        .nodes
        .iter()
        .map(|node| {
            let mut predicates = input_filters(&node.id, &node.filters);
            if !node.node_ids.is_empty() {
                predicates.push(format!("{}.id IN {:?}", node.id, node.node_ids));
            }
            if let Some(range) = &node.id_range {
                predicates.extend([
                    format!("{}.id >= {}", node.id, range.start),
                    format!("{}.id <= {}", node.id, range.end),
                ]);
            }
            let scan = filter(
                predicates,
                leaf(
                    Operator::NodeScan,
                    format!(
                        "{} AS {}",
                        node.entity.as_deref().unwrap_or("Unresolved"),
                        node.id
                    ),
                ),
            );
            match &node.columns {
                Some(ColumnSelection::List(columns)) => Tree::node(
                    Operator::Project,
                    columns
                        .iter()
                        .map(|column| format!("{}.{column}", node.id))
                        .collect::<Vec<_>>()
                        .join(", "),
                    vec![scan],
                ),
                Some(ColumnSelection::All) => {
                    Tree::node(Operator::Project, format!("{}.*", node.id), vec![scan])
                }
                None => scan,
            }
        })
        .collect();
    children.extend(input.relationships.iter().enumerate().map(|(index, edge)| {
        let (source, target, arrow) = match edge.direction {
            compiler::input::Direction::Incoming => (&edge.to, &edge.from, "->"),
            compiler::input::Direction::Outgoing => (&edge.from, &edge.to, "->"),
            compiler::input::Direction::Both => (&edge.from, &edge.to, "--"),
        };
        let alias = format!("e{index}");
        let depth = if edge.hops.min == 1 && edge.hops.max == 1 {
            String::new()
        } else {
            format!(" HOPS {}..{}", edge.hops.min, edge.hops.max)
        };
        filter(
            input_filters(&alias, &edge.filters),
            leaf(
                Operator::EdgeScan,
                format!(
                    "{} {source}{arrow}{target} AS {alias}{depth}",
                    edge.types.join("|")
                ),
            ),
        )
    }));
    let mut tree = Tree::node(Operator::Input, input.query_type.to_string(), children);
    if !input.aggregation.metrics.is_empty() || !input.aggregation.group_by.is_empty() {
        let groups = input.aggregation.group_by.iter().map(|group| {
            let value = group.property().map_or_else(
                || group.node().into(),
                |property| format!("{}.{property}", group.node()),
            );
            let value = group.truncate().map_or_else(
                || value.clone(),
                |unit| format!("date_trunc({}, {value})", unit.name()),
            );
            format!("group {value} AS {}", group.output_name())
        });
        let metrics = input.aggregation.metrics.iter().map(|metric| {
            let argument = metric.expr.property().map_or_else(
                || metric.expr.node().into(),
                |property| format!("{}.{property}", metric.expr.node()),
            );
            format!(
                "{}({argument}) AS {}",
                metric.expr.function().to_string().to_uppercase(),
                metric.output_name()
            )
        });
        tree = Tree::node(
            Operator::Aggregate,
            groups.chain(metrics).collect::<Vec<_>>().join(", "),
            vec![tree],
        );
    }
    if let Some(order) = &input.order_by {
        tree = Tree::node(
            Operator::Sort,
            format!(
                "{}.{}{}",
                order.node,
                order.property,
                if order.direction == OrderDirection::Desc {
                    " DESC"
                } else {
                    ""
                }
            ),
            vec![tree],
        );
    }
    if let Some(order) = &input.aggregation.sort {
        tree = Tree::node(
            Operator::Sort,
            format!(
                "{}{}",
                order.column,
                if order.direction == OrderDirection::Desc {
                    " DESC"
                } else {
                    ""
                }
            ),
            vec![tree],
        );
    }
    Tree::node(Operator::Limit, input.limit.to_string(), vec![tree])
}

fn input_filters(alias: &str, filters: &HashMap<String, Vec<InputFilter>>) -> Vec<String> {
    let mut ordered: Vec<_> = filters.iter().collect();
    ordered.sort_by_key(|(property, _)| *property);
    ordered
        .into_iter()
        .flat_map(|(property, filters)| {
            filters
                .iter()
                .map(move |filter| property_filter(alias, property, filter))
        })
        .collect()
}

fn property_filter(alias: &str, property: &str, filter: &InputFilter) -> String {
    let value = filter.rhs_column.as_ref().map_or_else(
        || literal(filter.value.as_ref().unwrap_or(&serde_json::Value::Null)),
        |(node, column)| format!("{node}.{column}"),
    );
    let op = filter.op.unwrap_or(FilterOp::Eq);
    let operator = match op {
        FilterOp::Eq => "=",
        FilterOp::Ne => "!=",
        FilterOp::Gt => ">",
        FilterOp::Gte => ">=",
        FilterOp::Lt => "<",
        FilterOp::Lte => "<=",
        FilterOp::In => "IN",
        FilterOp::IsNull => return format!("{alias}.{property} IS NULL"),
        FilterOp::IsNotNull => return format!("{alias}.{property} IS NOT NULL"),
        FilterOp::Contains
        | FilterOp::StartsWith
        | FilterOp::EndsWith
        | FilterOp::TokenMatch
        | FilterOp::AllTokens
        | FilterOp::AnyTokens => return format!("{}({alias}.{property}, {value})", op.as_ref()),
    };
    format!("{alias}.{property} {operator} {value}")
}

pub fn physical(
    plan: &QueryPlan,
    ast: &Node,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    bindings: &query_data_model::bindings::QueryBindings,
    names: &compiler::config::BindingNames,
) -> (Tree, Tree) {
    use query_data_model::QueryBackendCatalog;
    let sources = PlannedSources {
        names,
        bindings,
        storage: model.query_backend().storage(),
    };
    let planned_column = |value: &Column| planned_column(names, value);
    let planned_predicate = |value| planned_predicate(names, value);
    let planned = match plan {
        QueryPlan::Traversal(plan) => sources.execution_tree(&plan.operation.execution),
        QueryPlan::Aggregation(plan) => {
            let result = &plan.operation.result;
            let group = |value: &compiler::passes::plan::aggregation::Group| {
                let column = planned_column(&value.column);
                value.truncate.map_or_else(
                    || column.clone(),
                    |unit| format!("bucket({}, {column})", unit.name()),
                )
            };
            let condition = if result.condition.is_empty() {
                String::new()
            } else {
                format!(
                    " FILTER [{}]",
                    result
                        .condition
                        .iter()
                        .flat_map(planned_predicate)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            let items =
                result
                    .groups
                    .iter()
                    .map(|value| format!("group {}", group(value)))
                    .chain(result.group_outputs.iter().map(|(value, name)| {
                        format!("{} AS {}", group(value), names.exports[name])
                    }))
                    .chain(result.measures.iter().map(|measure| {
                        let argument = measure
                            .argument
                            .as_ref()
                            .map(planned_column)
                            .unwrap_or_default();
                        format!(
                            "{}({argument}){condition} AS {}",
                            measure.function.to_string().to_uppercase(),
                            names.exports[&measure.name]
                        )
                    }))
                    .collect::<Vec<_>>()
                    .join(", ");
            let tree = Tree::node(
                Operator::Aggregate,
                items,
                vec![sources.execution_source(&plan.operation.execution)],
            );
            let tree = sort(
                tree,
                result.order.iter().map(|(export, direction)| {
                    (
                        names.exports[export].clone(),
                        *direction == OrderDirection::Desc,
                    )
                }),
            );
            sources.definitions(&plan.operation.execution, tree)
        }
        QueryPlan::Neighbors(plan) => {
            let operation = &plan.operation;
            let access = operation.fused_table.as_ref().map_or_else(
                || {
                    format!(
                        "directional outgoing=[{}] incoming=[{}]",
                        operation.edge.outgoing_tables.join(", "),
                        operation.edge.incoming_tables.join(", ")
                    )
                },
                |table| format!("fused table={table}"),
            );
            Tree::node(
                Operator::Neighbors,
                format!(
                    "center={} direction={} {access} center_filter={} relationships=[{}] path_lookup={}",
                    operation.center,
                    match operation.direction {
                        compiler::input::Direction::Both => "both",
                        compiler::input::Direction::Incoming => "incoming",
                        compiler::input::Direction::Outgoing => "outgoing",
                    },
                    operation.has_non_denorm,
                    operation
                        .edge
                        .rel_type_filter
                        .as_deref()
                        .unwrap_or_default()
                        .join(", "),
                    operation.center_tp_lookup.as_ref().map_or_else(
                        || "none".into(),
                        |(table, column)| format!("{table}.{column}")
                    )
                ),
                vec![planned_node(&plan.nodes[&operation.center])],
            )
        }
        QueryPlan::PathFinding(plan) => {
            let operation = &plan.operation;
            Tree::node(
                Operator::PathFinding,
                format!(
                    "{}->{} depth={} forward={} backward={} scoped={} tables=[{}] relationships=[{}] forward_kinds=[{}] backward_kinds=[{}]",
                    operation.start,
                    operation.end,
                    operation.max_depth,
                    operation.forward_depth,
                    operation.backward_depth,
                    operation.scoped_by_tp,
                    operation.edge.tables.join(", "),
                    operation
                        .edge
                        .rel_type_filter
                        .as_deref()
                        .unwrap_or_default()
                        .join(", "),
                    operation
                        .forward_first_hop_filter
                        .as_deref()
                        .unwrap_or_default()
                        .join(", "),
                    operation
                        .backward_first_hop_filter
                        .as_deref()
                        .unwrap_or_default()
                        .join(", ")
                ),
                vec![
                    planned_node(&plan.nodes[&operation.start]),
                    planned_node(&plan.nodes[&operation.end]),
                ],
            )
        }
        QueryPlan::Hydration(plan) => Tree::node(
            Operator::Hydration,
            "",
            plan.operation
                .nodes
                .iter()
                .map(|node| sources.physical_tree(node))
                .collect(),
        ),
    };
    let emitted = match ast {
        Node::Query(value) => sources.query(value),
        Node::Insert(_) => leaf(Operator::Insert, ""),
    };
    (planned, emitted)
}

fn planned_node(node: &compiler::passes::plan::NodePlan) -> Tree {
    let mut predicates: Vec<_> = node
        .filters
        .iter()
        .map(|(name, bound)| property_filter(&node.alias, name, &bound.filter))
        .collect();
    if !node.node_ids.is_empty() {
        predicates.push(if let [id] = node.node_ids.as_slice() {
            format!("{}.id = {id}", node.alias)
        } else {
            format!(
                "{}.id IN [{}]",
                node.alias,
                node.node_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        });
    }
    if let Some(range) = &node.id_range {
        predicates.extend([
            format!("{}.id >= {}", node.alias, range.start),
            format!("{}.id <= {}", node.alias, range.end),
        ]);
    }
    predicates.push(format!("{}._deleted = false", node.alias));
    filter(
        predicates,
        leaf(
            Operator::NodeScan,
            format!(
                "{} AS {}",
                node.entity.as_deref().unwrap_or("Unresolved"),
                node.alias
            ),
        ),
    )
}

struct PlannedSources<'a, T> {
    names: &'a compiler::config::BindingNames,
    bindings: &'a query_data_model::bindings::QueryBindings,
    storage: &'a query_data_model::storage::StorageCatalog<T>,
}

impl<T> PlannedSources<'_, T> {
    fn execution_tree(&self, execution: &ExecutionPlan) -> Tree {
        self.definitions(execution, self.execution_source(execution))
    }

    fn execution_source(&self, execution: &ExecutionPlan) -> Tree {
        Tree::node(
            Operator::Project,
            planned_projections(self.names, &execution.outputs),
            vec![self.physical_source(&execution.source)],
        )
    }

    fn definitions(&self, execution: &ExecutionPlan, source: Tree) -> Tree {
        if execution.definitions.is_empty() {
            source
        } else {
            Tree::node(
                Operator::With,
                "",
                execution
                    .definitions
                    .iter()
                    .map(|(name, keys)| {
                        Tree::node(
                            Operator::Cte,
                            &self.names.definitions[name],
                            vec![self.physical_tree(keys)],
                        )
                    })
                    .chain([source])
                    .collect(),
            )
        }
    }

    fn physical_tree(&self, plan: &PhysicalPlan) -> Tree {
        Tree::node(
            Operator::Project,
            planned_projections(self.names, &plan.outputs),
            vec![self.physical_source(&plan.source)],
        )
    }

    fn physical_source(&self, plan: &PhysicalSource) -> Tree {
        let planned_predicate = |value| planned_predicate(self.names, value);
        let planned_column = |value| planned_column(self.names, value);
        match plan {
            PhysicalSource::Union { relation, arms, .. } => Tree::node(
                Operator::Union,
                format!("ALL AS {}", self.names.relations[relation]),
                arms.iter().map(|arm| self.physical_tree(arm)).collect(),
            ),
            PhysicalSource::Scan {
                relation, final_, ..
            } => {
                let query_data_model::bindings::RelationSource::Scan(table) =
                    self.bindings.source(*relation).unwrap()
                else {
                    panic!("expected scan");
                };
                scan(
                    self.storage.table(*table).name(),
                    &self.names.relations[relation],
                    *final_,
                )
            }
            PhysicalSource::Filter { predicates, input } => filter(
                predicates.iter().flat_map(planned_predicate).collect(),
                self.physical_source(input),
            ),
            PhysicalSource::KeyFilter { value, keys, input } => Tree::node(
                Operator::SemiJoin,
                format!("{} IN subquery", planned_column(value)),
                vec![self.physical_source(input), self.physical_tree(keys)],
            ),
            PhysicalSource::Scope {
                relation, input, ..
            } => {
                let tree = if input.outputs.is_empty() {
                    self.physical_source(&input.source)
                } else {
                    self.physical_tree(input)
                };
                Tree::node(Operator::Bind, &self.names.relations[relation], vec![tree])
            }
            PhysicalSource::Latest {
                sort_key,
                input,
                aggregate_condition,
                ..
            } => Tree::node(
                Operator::Deduplicate,
                format!(
                    "LimitBy {}",
                    sort_key
                        .iter()
                        .map(|column| { planned_column(column) })
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                vec![filter(
                    aggregate_condition
                        .iter()
                        .flat_map(planned_predicate)
                        .collect(),
                    self.physical_source(input),
                )],
            ),
            PhysicalSource::Join {
                endpoints,
                predicates,
                left,
                right,
            } => Tree::node(
                Operator::Join,
                planned_join(self.names, endpoints, predicates),
                vec![self.physical_source(left), self.physical_source(right)],
            ),
        }
    }
}

fn planned_column(names: &compiler::config::BindingNames, value: &Column) -> String {
    let (relation, name) = names.column(*value);
    format!("{relation}.{name}")
}

fn planned_join(
    names: &compiler::config::BindingNames,
    (left, right): &(Column, Column),
    predicates: &[Predicate],
) -> String {
    let planned_column = |value| planned_column(names, value);
    let planned_predicate = |value| planned_predicate(names, value);
    let equality = format!("{} = {}", planned_column(left), planned_column(right));
    let condition = if predicates.is_empty() {
        equality
    } else {
        std::iter::once(equality)
            .chain(predicates.iter().flat_map(planned_predicate))
            .map(|value| format!("({value})"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    format!("ON {condition}")
}

fn planned_predicate(names: &compiler::config::BindingNames, value: &Predicate) -> Vec<String> {
    let planned_column = |value| planned_column(names, value);
    let membership = |column: String, values: Vec<String>| {
        if let [value] = values.as_slice() {
            format!("{column} = {value}")
        } else {
            format!("{column} IN [{}]", values.join(", "))
        }
    };
    match value {
        Predicate::PathPrefixes { column, paths } => {
            use compiler::passes::plan::requirements::PrefixPaths;
            let (mode, paths) = match paths {
                PrefixPaths::Union(paths) => ("UNION", paths),
                PrefixPaths::Set(paths) => ("SET", paths),
            };
            vec![format!(
                "{} PREFIX {mode} [{}]",
                planned_column(column),
                paths
                    .iter()
                    .map(|path| text_literal(path.as_str()))
                    .collect::<Vec<_>>()
                    .join(", ")
            )]
        }
        Predicate::Property { column, filter, .. } => {
            let (relation, name) = names.column(*column);
            vec![property_filter(relation, name, filter)]
        }
        Predicate::Ids { column, values } => vec![membership(
            planned_column(column),
            values.iter().map(ToString::to_string).collect(),
        )],
        Predicate::IdRange { column, start, end } => vec![
            format!("{} >= {start}", planned_column(column)),
            format!("{} <= {end}", planned_column(column)),
        ],
        Predicate::Live { column } => vec![format!("{} = false", planned_column(column))],
        Predicate::EntityKind { column, entity } => vec![format!(
            "{} = {}",
            planned_column(column),
            text_literal(entity)
        )],
        Predicate::RelationshipKinds { column, kinds } => vec![membership(
            planned_column(column),
            kinds.iter().map(|kind| text_literal(kind)).collect(),
        )],
        Predicate::Tags { column, values } => vec![format!(
            "{} HAS ANY [{}]",
            planned_column(column),
            values
                .iter()
                .map(|value| text_literal(value))
                .collect::<Vec<_>>()
                .join(", ")
        )],
        Predicate::Membership {
            column,
            definition,
            key,
        } => vec![format!(
            "{} IN {}.{}",
            planned_column(column),
            names.definitions[definition],
            names.exports[key]
        )],
    }
}

fn planned_projections(names: &compiler::config::BindingNames, values: &[Projection]) -> String {
    let planned_column = |value| planned_column(names, value);
    values
        .iter()
        .map(|projection| {
            let value = match &projection.value {
                OutputValue::Properties(columns) => format!(
                    "properties({})",
                    columns
                        .iter()
                        .map(|(_, column)| planned_column(column))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                OutputValue::Column(value) => planned_column(value),
                OutputValue::Text(value) => text_literal(value),
                OutputValue::Depth(value) => value.to_string(),
                OutputValue::Path(steps) => format!(
                    "path[{}]",
                    steps
                        .iter()
                        .map(|(id, kind)| format!(
                            "({}, {})",
                            planned_column(id),
                            planned_column(kind)
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            };
            format!("{value} AS {}", names.exports[&projection.name])
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn scan(table: &str, alias: &str, final_: bool) -> Tree {
    let scan = leaf(Operator::Scan, format!("Table({table}) AS {alias}"));
    if final_ {
        Tree::node(Operator::Deduplicate, "Final", vec![scan])
    } else {
        scan
    }
}

fn join_head(names: &compiler::config::BindingNames, kind: &str, condition: &Expr) -> String {
    format!(
        "{}ON {}",
        if kind == "INNER" {
            String::new()
        } else {
            format!("{kind} ")
        },
        expression(names, condition)
    )
}

fn text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => text_literal(value),
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values.iter().map(literal).collect::<Vec<_>>().join(", ")
        ),
        value => value.to_string(),
    }
}

fn expression(names: &compiler::config::BindingNames, value: &Expr) -> String {
    let expression = |value: &Expr| expression(names, value);
    match value {
        Expr::EmptyTupleArray(fields) => format!("empty_tuple_array({fields:?})"),
        Expr::TokenSearch { mode, value, query } => format!(
            "tokens_{}({}, {})",
            mode.to_string().to_lowercase(),
            expression(value),
            expression(query)
        ),
        Expr::TimeBucket { unit, value } => {
            format!("bucket({}, {})", unit.name(), expression(value))
        }
        Expr::Aggregate {
            function,
            argument,
            distinct,
            condition,
        } => {
            let argument = argument
                .as_ref()
                .map(|value| expression(value))
                .unwrap_or_default();
            let mut value = format!(
                "{}({}{argument})",
                function.to_string().to_uppercase(),
                if *distinct { "DISTINCT " } else { "" }
            );
            if let Some(condition) = condition {
                value.push_str(&format!(" FILTER [{}]", expression(condition)));
            }
            value
        }
        Expr::Column(column) => planned_column(names, column),
        Expr::Output(export) => names.exports[export].clone(),
        Expr::Identifier(name) => name.clone(),
        Expr::Literal(value) | Expr::Param { value, .. } => literal(value),
        Expr::FuncCall { name, args } => format!(
            "{name}({})",
            args.iter().map(expression).collect::<Vec<_>>().join(", ")
        ),
        Expr::BinaryOp { op, left, right } => {
            let operand = |value: &Expr| {
                let text = expression(value);
                if matches!(value, Expr::BinaryOp { op: child, .. } if child != op || !matches!(op, Op::And | Op::Or))
                {
                    format!("({text})")
                } else {
                    text
                }
            };
            format!("{} {op} {}", operand(left), operand(right))
        }
        Expr::UnaryOp {
            op: op @ (Op::IsNull | Op::IsNotNull),
            expr,
        } => format!("{} {op}", expression(expr)),
        Expr::UnaryOp { op, expr } => format!("{op}({})", expression(expr)),
        Expr::Lambda { param, body } => format!("{param} -> {}", expression(body)),
        Expr::InSubquery {
            expr,
            cte_name,
            column,
        } => format!(
            "{} IN {}.{}",
            expression(expr),
            names.definitions[cte_name],
            names.exports[column]
        ),
        Expr::InSelect { expr, .. } => format!("{} IN subquery", expression(expr)),
        Expr::Scalar(_) => "scalar(subquery)".into(),
        Expr::Star => "*".into(),
    }
}

impl<T> PlannedSources<'_, T> {
    fn query_filter(&self, predicate: &Expr, input: Tree) -> Tree {
        let expression = |value| expression(self.names, value);
        match predicate {
            Expr::BinaryOp {
                op: Op::And,
                left,
                right,
            } => self.query_filter(right, self.query_filter(left, input)),
            Expr::InSelect { expr, query: keys } => Tree::node(
                Operator::SemiJoin,
                format!("{} IN subquery", expression(expr)),
                vec![input, self.query(keys)],
            ),
            _ => filter(vec![expression(predicate)], input),
        }
    }

    fn projections(&self, values: &[SelectExpr]) -> String {
        let expression = |value| expression(self.names, value);
        values
            .iter()
            .map(|value| {
                value.alias.as_ref().map_or_else(
                    || expression(&value.expr),
                    |alias| {
                        format!(
                            "{} AS {}",
                            expression(&value.expr),
                            self.names.exports[alias]
                        )
                    },
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn relation(&self, value: &TableRef) -> Tree {
        match value {
            TableRef::Cte { relation } => scan(
                self.names.source(self.bindings, *relation).unwrap(),
                &self.names.relations[relation],
                false,
            ),
            TableRef::Scan {
                relation, final_, ..
            } => scan(
                self.names.source(self.bindings, *relation).unwrap(),
                &self.names.relations[relation],
                *final_,
            ),
            TableRef::Join {
                join_type,
                left,
                right,
                on,
            } => Tree::node(
                Operator::Join,
                join_head(self.names, &join_type.to_string(), on),
                vec![self.relation(left), self.relation(right)],
            ),
            TableRef::Subquery {
                query: inner,
                relation,
            } => Tree::node(
                Operator::Bind,
                &self.names.relations[relation],
                vec![self.query(inner)],
            ),
            TableRef::Union { queries, relation } => Tree::node(
                Operator::Union,
                format!("ALL AS {}", self.names.relations[relation]),
                queries.iter().map(|query| self.query(query)).collect(),
            ),
        }
    }

    fn query(&self, value: &Query) -> Tree {
        let expression = |value: &Expr| expression(self.names, value);
        let mut tree = self.relation(&value.from);
        if let Some(predicate) = &value.where_clause {
            tree = self.query_filter(predicate, tree);
        }
        let projection = self.projections(&value.select);
        let aggregate = !value.group_by.is_empty()
            || value
                .select
                .iter()
                .any(|value| matches!(value.expr, Expr::Aggregate { .. }));
        if !aggregate {
            tree = Tree::node(Operator::Project, projection, vec![tree]);
        } else {
            let items = value
                .group_by
                .iter()
                .map(|key| format!("group {}", expression(key)))
                .chain([projection])
                .collect::<Vec<_>>()
                .join(", ");
            tree = Tree::node(Operator::Aggregate, items, vec![tree]);
        }
        if let Some(predicate) = &value.having {
            tree = self.query_filter(predicate, tree);
        }
        if value.distinct {
            tree = Tree::node(Operator::Distinct, "", vec![tree]);
        }
        tree = sort(
            tree,
            value
                .order_by
                .iter()
                .map(|order| (expression(&order.expr), order.desc)),
        );
        if let Some((limit, keys)) = &value.limit_by {
            tree = Tree::node(
                Operator::Deduplicate,
                format!(
                    "LimitBy {limit} BY {}",
                    keys.iter().map(expression).collect::<Vec<_>>().join(", ")
                ),
                vec![tree],
            );
        }
        if !value.union_all.is_empty() {
            tree = Tree::node(
                Operator::Union,
                "ALL",
                std::iter::once(tree)
                    .chain(value.union_all.iter().map(|query| self.query(query)))
                    .collect(),
            );
        }
        if let Some(limit) = value.limit {
            tree = Tree::node(Operator::Limit, limit.to_string(), vec![tree]);
        }
        if !value.ctes.is_empty() {
            tree = Tree::node(
                Operator::With,
                "",
                value
                    .ctes
                    .iter()
                    .map(|cte| {
                        Tree::node(
                            Operator::Cte,
                            &self.names.definitions[&cte.name],
                            vec![self.query(&cte.query)],
                        )
                    })
                    .chain([tree])
                    .collect(),
            );
        }
        tree
    }
}

fn sort(input: Tree, keys: impl Iterator<Item = (String, bool)>) -> Tree {
    let keys: Vec<_> = keys
        .map(|(value, descending)| format!("{value}{}", if descending { " DESC" } else { "" }))
        .collect();
    if keys.is_empty() {
        input
    } else {
        Tree::node(Operator::Sort, keys.join(", "), vec![input])
    }
}

#[test]
fn exact_assertions_preserve_nested_filter_order() {
    let storage = query_data_model::storage::StorageCatalog::new([
        query_data_model::storage::TableLayout::new(
            "gl_project",
            ["id", "star_count"]
                .map(|name| query_data_model::storage::StoredColumn::new(name, ()))
                .to_vec(),
            &[],
        )
        .unwrap(),
    ])
    .unwrap();
    let mut bindings = query_data_model::bindings::QueryBindings::new();
    let relation = bindings
        .scan(
            &storage,
            bindings.root(),
            storage.resolve_table("gl_project").unwrap(),
        )
        .unwrap();
    let mut names = compiler::config::BindingNames::default();
    names.relations.insert(relation, "p".into());
    let mut column = |name: &str| {
        let column = bindings
            .stored_column(
                bindings.root(),
                relation,
                storage.resolve_column("gl_project", name).unwrap(),
            )
            .unwrap();
        names.exports.insert(column.export(), name.into());
        column
    };
    let predicates = [
        Predicate::Ids {
            column: column("id"),
            values: vec![1],
        },
        Predicate::Ids {
            column: column("star_count"),
            values: vec![2],
        },
    ];
    let source = predicates.iter().fold(
        PhysicalSource::Scan {
            relation,
            final_: false,
            relationship: None,
        },
        |input, predicate| PhysicalSource::Filter {
            predicates: vec![predicate.clone()],
            input: Box::new(input),
        },
    );
    let assertions: super::Assertions = orbit_utils::yaml::from_str(
        "exact: (Filter p.id = 1, p.star_count = 2 (Scan Table(gl_project) AS p))",
    )
    .unwrap();
    assertions
        .check(
            &PlannedSources {
                names: &names,
                bindings: &bindings,
                storage: &storage,
            }
            .physical_source(&source),
            "planned",
        )
        .unwrap();
    let emitted = query_filter(
        &Expr::conjoin(vec![
            Expr::eq(Expr::col("p", "id"), Expr::int(1)),
            Expr::eq(Expr::col("p", "star_count"), Expr::int(2)),
        ])
        .unwrap(),
        scan("gl_project", "p", false),
    );
    assertions.check(&emitted, "emitted").unwrap();
}
