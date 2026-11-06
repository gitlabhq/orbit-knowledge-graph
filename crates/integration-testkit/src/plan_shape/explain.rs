use compiler::input::{ColumnSelection, FilterOp, Input, InputFilter, OrderDirection};
use compiler::passes::plan::QueryPlan;
use compiler::passes::plan::graph::{
    ExecutionPlan, RelationGraph, RelationId, RelationNode, RelationRef,
};
use compiler::passes::plan::predicates::{Column, OutputValue, Predicate, Projection};
use query_engine::compiler;
use std::collections::HashMap;

use super::operator::Operator;
use super::pattern::Expression as Tree;

pub(super) fn leaf(label: Operator, text: impl Into<String>) -> Tree {
    Tree::node(label, text, vec![])
}

pub(super) fn filter(predicates: Vec<String>, input: Tree) -> Tree {
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
    tree = filter(
        input
            .predicates
            .iter()
            .map(|expression| {
                boolean_text(expression, &|leaf: &compiler::input::Condition| {
                    let alias = match &leaf.target {
                        compiler::input::PredicateTarget::Node(alias) => alias.clone(),
                        compiler::input::PredicateTarget::Relationship(index) => {
                            format!("e{index}")
                        }
                    };
                    property_filter(&alias, &leaf.property, &leaf.filter)
                })
            })
            .collect(),
        tree,
    );
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
    if let Some(skip) = input.skip {
        tree = Tree::node(Operator::Offset, skip.to_string(), vec![tree]);
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

pub fn physical(plan: &QueryPlan) -> Tree {
    match plan {
        QueryPlan::Traversal(plan) => execution_tree(&plan.operation.execution),
        QueryPlan::Aggregation(plan) => {
            let result = &plan.operation.result;
            let group = |value: &compiler::passes::plan::query_types::aggregation::Group| {
                let column = planned_value(&value.value);
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
            let items = result
                .groups
                .iter()
                .map(|value| format!("group {}", group(value)))
                .chain(
                    result
                        .group_outputs
                        .iter()
                        .map(|(value, name)| format!("{} AS {name}", group(value))),
                )
                .chain(result.measures.iter().map(|measure| {
                    let argument = measure
                        .argument
                        .as_ref()
                        .map(planned_value)
                        .unwrap_or_default();
                    format!(
                        "{}({argument}){condition} AS {}",
                        measure.function.to_string().to_uppercase(),
                        measure.name
                    )
                }))
                .collect::<Vec<_>>()
                .join(", ");
            let tree = Tree::node(
                Operator::Aggregate,
                items,
                vec![execution_tree(&plan.operation.execution)],
            );
            sort(
                tree,
                result.order.iter().map(|order| {
                    (
                        order.column.clone(),
                        order.direction == OrderDirection::Desc,
                    )
                }),
            )
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
                    "center={} direction={} {access} center_filter={} relationships=[{}] incoming_scoped=[{}] incoming_cross_scope=[{}]",
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
                    operation.incoming_scope_preserving.join(", "),
                    operation.incoming_cross_scope.join(", ")
                ),
                vec![planned_node(&plan.nodes[&operation.center])],
            )
        }
        QueryPlan::PathFinding(plan) => {
            let operation = &plan.operation;
            Tree::node(
                Operator::PathFinding,
                format!(
                    "{}->{} depth={} forward={} backward={} scoped={} tables=[{}] relationships=[{}]",
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
                .map(|node| {
                    use compiler::passes::plan::query_types::hydration::HydrationPathFilter;
                    let mut predicates = Vec::new();
                    if !node.node_ids.is_empty() {
                        predicates.push(format!(
                            "{}.{} IN {:?}",
                            node.alias, node.id_property, node.node_ids
                        ));
                    }
                    if let Some(paths) = &node.path_filter {
                        let (mode, paths) = match paths {
                            HydrationPathFilter::PrefixUnion(paths) => ("PREFIX UNION", paths),
                            HydrationPathFilter::PrefixSet(paths) => ("PREFIX SET", paths),
                        };
                        predicates.insert(
                            0,
                            format!(
                                "{}.traversal_path {mode} [{}]",
                                node.alias,
                                paths
                                    .iter()
                                    .map(|path| text_literal(path.as_str()))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ),
                        );
                    }
                    let read = Tree::node(
                        Operator::Deduplicate,
                        format!(
                            "LimitBy {}",
                            node.sort_key
                                .iter()
                                .map(|key| format!("{}.{key}", node.alias))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                        vec![filter(predicates, scan(&node.table, &node.alias, false))],
                    );
                    Tree::node(
                        Operator::Project,
                        node.columns
                            .iter()
                            .map(|column| format!("{}.{column}", node.alias))
                            .collect::<Vec<_>>()
                            .join(", "),
                        vec![filter(
                            vec![format!("{}._deleted = false", node.alias)],
                            read,
                        )],
                    )
                })
                .collect(),
        ),
    }
}

fn planned_node(node: &compiler::passes::plan::NodePlan) -> Tree {
    filter(
        node.conditions()
            .iter()
            .flat_map(planned_predicate)
            .collect(),
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

fn execution_tree(execution: &ExecutionPlan) -> Tree {
    let usage = execution.graph.usage(execution.root);
    let materialized: std::collections::HashSet<_> = usage.materialized().collect();
    let source = Tree::node(
        Operator::Project,
        planned_projections(&execution.outputs),
        vec![relation_node(
            execution.root,
            &execution.graph,
            &materialized,
            None,
        )],
    );
    let relations: Vec<_> = usage.materialized().collect();
    if relations.is_empty() {
        source
    } else {
        Tree::node(
            Operator::With,
            "",
            relations
                .into_iter()
                .map(|id| {
                    Tree::node(
                        Operator::Cte,
                        format!("_orbit_relation_{}", id.0),
                        vec![relation_node(id, &execution.graph, &materialized, Some(id))],
                    )
                })
                .chain([source])
                .collect(),
        )
    }
}

fn relation_ref(
    reference: &RelationRef,
    graph: &RelationGraph,
    materialized: &std::collections::HashSet<RelationId>,
    current: Option<RelationId>,
) -> Tree {
    if materialized.contains(&reference.id) && current != Some(reference.id) {
        return leaf(
            Operator::Scan,
            format!(
                "Relation({}) AS {}",
                reference.id.0,
                reference.alias.as_deref().unwrap_or("_orbit_relation")
            ),
        );
    }
    let tree = relation_node(reference.id, graph, materialized, current);
    match (reference.alias.as_ref(), graph.get(reference.id)) {
        (Some(alias), RelationNode::Read { alias: source, .. }) if alias == source => tree,
        (Some(alias), RelationNode::Owned { alias: source, .. }) if alias == source => tree,
        (Some(alias), _) => Tree::node(Operator::Bind, alias, vec![tree]),
        (None, _) => tree,
    }
}

fn relation_node(
    id: RelationId,
    graph: &RelationGraph,
    materialized: &std::collections::HashSet<RelationId>,
    current: Option<RelationId>,
) -> Tree {
    match graph.get(id) {
        RelationNode::UnionAll { alias, inputs, .. } => Tree::node(
            Operator::Union,
            format!("ALL AS {alias}"),
            inputs
                .iter()
                .map(|input| relation_ref(input, graph, materialized, current))
                .collect(),
        ),
        RelationNode::Read {
            table,
            alias,
            final_,
        } => scan(table, alias, *final_),
        RelationNode::Owned { input, alias, .. } => match graph.get(input.id) {
            RelationNode::Read { alias: source, .. } if source == alias => {
                relation_node(input.id, graph, materialized, current)
            }
            _ => Tree::node(
                Operator::Bind,
                alias,
                vec![relation_ref(input, graph, materialized, current)],
            ),
        },
        RelationNode::Filter { predicates, input } => filter(
            predicates.iter().flat_map(planned_predicate).collect(),
            relation_ref(input, graph, materialized, current),
        ),
        RelationNode::Project { outputs, input } => Tree::node(
            Operator::Project,
            planned_projections(outputs),
            vec![relation_ref(input, graph, materialized, current)],
        ),
        RelationNode::Bind { alias, input } => Tree::node(
            Operator::Bind,
            alias,
            vec![relation_ref(input, graph, materialized, current)],
        ),
        RelationNode::Latest {
            alias,
            sort_key,
            input,
            condition,
        } => Tree::node(
            Operator::Deduplicate,
            format!(
                "LimitBy {}",
                sort_key
                    .iter()
                    .map(|column| format!("{alias}.{column}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            vec![filter(
                condition.iter().flat_map(planned_predicate).collect(),
                relation_ref(input, graph, materialized, current),
            )],
        ),
        RelationNode::Join {
            endpoints,
            predicates,
            left,
            right,
        } => Tree::node(
            Operator::Join,
            planned_join(endpoints, predicates),
            vec![
                relation_ref(left, graph, materialized, current),
                relation_ref(right, graph, materialized, current),
            ],
        ),
    }
}

fn planned_column(value: &Column) -> String {
    format!("{}.{}", value.source, value.name)
}

fn planned_join((left, right): &(Column, Column), predicates: &[Predicate]) -> String {
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

fn planned_predicate(value: &Predicate) -> Vec<String> {
    let membership = |column: String, values: Vec<String>| {
        if let [value] = values.as_slice() {
            format!("{column} = {value}")
        } else {
            format!("{column} IN [{}]", values.join(", "))
        }
    };
    match value {
        Predicate::Boolean(expression) => vec![boolean_text(expression, &|leaf| {
            planned_predicate(leaf).join(" AND ")
        })],
        Predicate::Property { column, filter, .. } => {
            vec![property_filter(&column.source, &column.name, filter)]
        }
        Predicate::Ids { column, values } => vec![membership(
            planned_column(column),
            values.iter().map(ToString::to_string).collect(),
        )],
        Predicate::IdRange { column, start, end } => vec![
            format!("{} >= {start}", planned_column(column)),
            format!("{} <= {end}", planned_column(column)),
        ],
        Predicate::Live { alias } => vec![format!("{alias}._deleted = false")],
        Predicate::EntityKind { column, entity } => vec![format!(
            "{} = {}",
            planned_column(column),
            text_literal(entity)
        )],
        Predicate::ColumnEq { left, right } => vec![format!(
            "{} = {}",
            planned_column(left),
            planned_column(right)
        )],
        Predicate::RelationshipKinds { alias, kinds } => vec![membership(
            format!("{alias}.relationship_kind"),
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
            columns,
            relation,
            keys,
        } => vec![format!(
            "{} IN _orbit_relation_{}.{}",
            membership_columns(columns),
            relation.0,
            membership_keys(keys)
        )],
    }
}

fn membership_columns(columns: &[Column]) -> String {
    let columns: Vec<_> = columns.iter().map(planned_column).collect();
    match columns.as_slice() {
        [column] => column.clone(),
        _ => format!("({})", columns.join(", ")),
    }
}

pub(super) fn membership_keys(keys: &[String]) -> String {
    match keys {
        [key] => key.clone(),
        _ => format!("({})", keys.join(", ")),
    }
}

fn boolean_text<T>(
    expression: &compiler::input::BooleanExpression<T>,
    leaf: &impl Fn(&T) -> String,
) -> String {
    expression.fold(
        leaf,
        &|children| {
            children
                .iter()
                .map(|child| format!("({child})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        },
        &|child| format!("NOT ({child})"),
    )
}

fn planned_value(value: &OutputValue) -> String {
    match value {
        OutputValue::Column(value) | OutputValue::EpochAsNull(value, _) => planned_column(value),
        OutputValue::Text(value) => text_literal(value),
        OutputValue::Depth(value) => value.to_string(),
        OutputValue::Path(steps) => format!(
            "path[{}]",
            steps
                .iter()
                .map(|(id, kind)| format!("({}, {})", planned_column(id), planned_column(kind)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        OutputValue::PathAppend { path, id, kind } => format!(
            "append({}, ({}, {}))",
            planned_column(path),
            planned_column(id),
            planned_column(kind)
        ),
        OutputValue::PathPrepend { path, id, kind } => format!(
            "prepend(({}, {}), {})",
            planned_column(id),
            planned_column(kind),
            planned_column(path)
        ),
    }
}

fn planned_projections(values: &[Projection]) -> String {
    values
        .iter()
        .map(|projection| {
            format!(
                "{} AS {}",
                planned_value(&projection.value),
                projection.name
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn scan(table: &str, alias: &str, final_: bool) -> Tree {
    let scan = leaf(Operator::Scan, format!("Table({table}) AS {alias}"));
    if final_ {
        Tree::node(Operator::Deduplicate, "Final", vec![scan])
    } else {
        scan
    }
}

fn text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

pub(super) fn literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => text_literal(value),
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values.iter().map(literal).collect::<Vec<_>>().join(", ")
        ),
        value => value.to_string(),
    }
}

pub(super) fn sort(input: Tree, keys: impl Iterator<Item = (String, bool)>) -> Tree {
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
    let predicates = [
        Predicate::Ids {
            column: Column::new("p", "id"),
            values: vec![1],
        },
        Predicate::Ids {
            column: Column::new("p", "star_count"),
            values: vec![2],
        },
    ];
    let mut graph = RelationGraph::new(true);
    let read = graph.read("gl_project", "p", false);
    let source = predicates.iter().fold(read, |relation, predicate| {
        graph.filter(relation, vec![predicate.clone()])
    });
    let assertions: super::Assertions = orbit_utils::yaml::from_str(
        "exact: (Filter p.id = 1, p.star_count = 2 (Scan Table(gl_project) AS p))",
    )
    .unwrap();
    assertions
        .check(
            &relation_node(source, &graph, &Default::default(), None),
            "planned",
        )
        .unwrap();
}
