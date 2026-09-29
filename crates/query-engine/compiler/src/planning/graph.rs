use std::collections::HashMap;

use query_data_model::{EdgeField, QueryDataModel};

use super::bind::{BoundQuery, Plan, Source, bind_filter, bind_node, call};
use super::generic::{Expr, JoinKind, Node, Op, ValueType, Values};
use super::physical::Scalar;
use crate::error::{QueryError, Result};
use crate::input::{Direction, HopRange, Input, QueryType};

pub fn traversal(
    input: &Input,
    model: &impl QueryDataModel,
    required: &[(String, String, String)],
    edge_outputs: &[[String; 5]],
) -> Result<BoundQuery> {
    if !matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) || input.nodes.is_empty()
        || !input.join_predicates.is_empty()
    {
        return Err(QueryError::Validation(
            "graph binding requires a traversal without column comparisons".into(),
        ));
    }

    if edge_outputs.len() != input.relationships.len() {
        return Err(QueryError::PipelineInvariant(
            "edge output contract has the wrong length".into(),
        ));
    }

    let mut values = Values::default();
    let mut nodes = HashMap::new();
    let mut outputs = Vec::new();
    let mut required_values = HashMap::new();
    let mut components: Vec<(Vec<String>, Plan)> = Vec::new();

    for node in &input.nodes {
        if nodes.contains_key(&node.id) {
            return Err(QueryError::ReferenceError("duplicate node binding".into()));
        }

        let fields: Vec<_> = required
            .iter()
            .filter(|(alias, _, _)| alias == &node.id)
            .map(|(_, property, name)| (property.clone(), name.clone()))
            .collect();
        let identity = fields
            .iter()
            .position(|(property, _)| property == &node.id_property)
            .ok_or_else(|| {
                QueryError::PipelineInvariant("node identity must be required".into())
            })?;
        let mut source = node.clone();
        if input.query_type == QueryType::Aggregation {
            source.columns = Some(crate::input::ColumnSelection::List(vec![]));
        }

        let single = Input {
            nodes: vec![source],
            ..Default::default()
        };
        let bound = bind_node(&single, model, &fields, None, values)?;
        values = bound.values;

        for ((_, name), value) in fields.iter().zip(&bound.required) {
            required_values.insert(name.clone(), *value);
        }

        nodes.insert(
            node.id.clone(),
            (
                bound.required[identity],
                node.entity.as_deref().unwrap_or_default(),
            ),
        );
        outputs.extend(bound.outputs);
        components.push((vec![node.id.clone()], bound.root));
    }

    let mut edge_plans = Vec::new();
    for (relationship, names) in input.relationships.iter().zip(edge_outputs) {
        if relationship.hops != HopRange::default() || relationship.direction == Direction::Both {
            return Err(QueryError::Validation(
                "edge binding currently supports directed single hops".into(),
            ));
        }

        let from = nodes
            .get(&relationship.from)
            .ok_or_else(|| QueryError::ReferenceError("missing source node".into()))?;
        let to = nodes
            .get(&relationship.to)
            .ok_or_else(|| QueryError::ReferenceError("missing target node".into()))?;
        let (source, target) = if relationship.direction == Direction::Incoming {
            (to, from)
        } else {
            (from, to)
        };
        let fields = [
            EdgeField::SourceId,
            EdgeField::TargetId,
            EdgeField::SourceKind,
            EdgeField::TargetKind,
            EdgeField::RelationshipKind,
        ];
        let fields: Vec<_> = fields
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                let data_type = if index < 2 {
                    ValueType::Int64
                } else {
                    ValueType::String
                };
                (values.allocate(data_type), field)
            })
            .collect();
        let ids: Vec<_> = fields.iter().map(|(value, _)| *value).collect();
        let kinds = relationship
            .types
            .iter()
            .map(|name| {
                model.graph().relationship_id(name).ok_or_else(|| {
                    QueryError::ReferenceError(format!("unknown relationship {name}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut predicate = call(
            Scalar::And,
            vec![
                call(
                    Scalar::Equal,
                    vec![Expr::Value(ids[2]), Expr::String(source.1.into())],
                ),
                call(
                    Scalar::Equal,
                    vec![Expr::Value(ids[3]), Expr::String(target.1.into())],
                ),
            ],
        );

        if let Some(kinds) = relationship
            .types
            .iter()
            .map(|kind| {
                call(
                    Scalar::Equal,
                    vec![Expr::Value(ids[4]), Expr::String(kind.clone())],
                )
            })
            .reduce(|left, right| call(Scalar::Or, vec![left, right]))
        {
            predicate = call(Scalar::And, vec![predicate, kinds]);
        }

        let mut filters: Vec<_> = relationship.filters.iter().collect();
        filters.sort_unstable_by_key(|(name, _)| *name);

        for (name, filters) in filters {
            let field = match name.as_str() {
                "source_id" => EdgeField::SourceId,
                "target_id" => EdgeField::TargetId,
                "source_kind" => EdgeField::SourceKind,
                "target_kind" => EdgeField::TargetKind,
                "relationship_kind" => EdgeField::RelationshipKind,
                _ => {
                    return Err(QueryError::Validation(format!(
                        "edge property {name} has no semantic binding yet"
                    )));
                }
            };
            let value = fields
                .iter()
                .find(|(_, candidate)| *candidate == field)
                .unwrap()
                .0;

            for filter in filters {
                predicate = call(
                    Scalar::And,
                    vec![predicate, bind_filter(value, filter, &values)?],
                );
            }
        }

        let edge = Node {
            op: Op::Filter(predicate),
            inputs: vec![Node {
                op: Op::Read(Source::Edge {
                    relationships: kinds,
                    fields,
                }),
                inputs: vec![],
            }],
        };
        edge_plans.push((relationship, edge, source.0, target.0, ids));
        outputs.extend(names.iter().cloned());
    }

    let mut output_values = components
        .iter()
        .map(|(_, node)| node.output(&values))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    for (relationship, edge, source, target, ids) in edge_plans {
        let from_index = components
            .iter()
            .position(|(aliases, _)| aliases.contains(&relationship.from))
            .unwrap();
        let (mut aliases, left) = components.remove(from_index);
        let (from, to) = if relationship.direction == Direction::Incoming {
            ((target, ids[1]), (source, ids[0]))
        } else {
            ((source, ids[0]), (target, ids[1]))
        };
        let equality =
            |(node, edge)| call(Scalar::Equal, vec![Expr::Value(node), Expr::Value(edge)]);
        let already_joined = aliases.contains(&relationship.to);
        let condition = if already_joined {
            call(Scalar::And, vec![equality(from), equality(to)])
        } else {
            equality(from)
        };
        let mut root = join(left, edge, condition);

        if !already_joined {
            let to_index = components
                .iter()
                .position(|(aliases, _)| aliases.contains(&relationship.to))
                .unwrap();
            let (right_aliases, right) = components.remove(to_index);
            aliases.extend(right_aliases);
            root = join(root, right, equality(to));
        }

        output_values.extend(ids);
        components.push((aliases, root));
    }

    if components.len() != 1 {
        return Err(QueryError::Validation("disconnected traversal".into()));
    }

    let (_, root) = components.pop().unwrap();
    let root = Node {
        op: Op::Project(
            output_values
                .into_iter()
                .map(|value| super::generic::Assignment {
                    output: value,
                    expression: Expr::Value(value),
                })
                .collect(),
        ),
        inputs: vec![root],
    };
    let required = required
        .iter()
        .map(|(_, _, name)| {
            required_values
                .get(name)
                .copied()
                .ok_or_else(|| QueryError::ReferenceError("required binding is missing".into()))
        })
        .collect::<Result<_>>()?;

    root.output(&values)?;
    Ok(BoundQuery {
        root,
        values,
        outputs,
        required,
    })
}

fn join(left: Plan, right: Plan, condition: Expr<Scalar>) -> Plan {
    Node {
        op: Op::Join {
            kind: JoinKind::Inner,
            condition,
        },
        inputs: vec![left, right],
    }
}
