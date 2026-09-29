use std::convert::Infallible;

use query_data_model::{QueryBackendCatalog, QueryDataModel};

use super::bind::{Source, bind_node, dynamic_edges};
use super::generic::{
    Assignment, Expr as PlanExpr, Node, Op, Operation, Schema, ValueId, ValueType, Values,
};
use super::physical::Scalar;
use crate::ast::{self, Cte, Expr, Identifier, JoinType, OrderExpr, Query, SelectExpr, TableRef};
use crate::constants::{edge_kinds_column, path_authorizations_column, path_column};
use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, Input};
use crate::lowering::{Context, EmitOperation, SqlFragment, lower_with_context, resolve, scalar};
use crate::passes::enforce::ResultBindings;

#[derive(Clone, PartialEq)]
pub struct PathFinding {
    endpoints: [(ValueId, String); 2],
    edge: [ValueId; 5],
    outputs: [ValueId; 3],
    max_depth: u32,
    authorization: Option<([ValueId; 3], ValueId)>,
    edge_owner: Option<ValueId>,
}

fn output_types() -> [ValueType; 3] {
    [
        ValueType::Int64,
        ValueType::List(Box::new(ValueType::Record(vec![
            ValueType::Int64,
            ValueType::String,
        ]))),
        ValueType::List(Box::new(ValueType::String)),
    ]
}

impl Operation for PathFinding {
    fn output(&self, inputs: &[Schema], values: &Values) -> Result<Schema> {
        let [start, end, edges, ..] = inputs else {
            return Err(QueryError::PipelineInvariant(
                "path finding requires two endpoints and edges".into(),
            ));
        };
        if !(1..=3).contains(&self.max_depth)
            || !start.contains(&self.endpoints[0].0)
            || !end.contains(&self.endpoints[1].0)
            || self.edge.iter().any(|value| !edges.contains(value))
        {
            return Err(QueryError::PipelineInvariant(
                "invalid bounded path inputs".into(),
            ));
        }
        for (value, kind) in self
            .edge
            .iter()
            .zip([
                ValueType::Int64,
                ValueType::Int64,
                ValueType::String,
                ValueType::String,
                ValueType::String,
            ])
            .chain(self.outputs.iter().zip(output_types()))
        {
            if values.data_type(*value)? != &kind {
                return Err(QueryError::PipelineInvariant(
                    "invalid path field type".into(),
                ));
            }
        }
        let mut outputs = self.outputs.to_vec();
        if let Some((fields, output)) = self.authorization {
            if inputs.len() != 4
                || fields.iter().any(|field| !inputs[3].contains(field))
                || values.data_type(output)? != &output_types()[1]
            {
                return Err(QueryError::PipelineInvariant(
                    "invalid path authorization input".into(),
                ));
            }
            outputs.push(output);
        } else if inputs.len() != 3 {
            return Err(QueryError::PipelineInvariant(
                "unexpected path input".into(),
            ));
        }
        Ok(outputs)
    }

    fn map_values(&mut self, map: &mut impl FnMut(&mut ValueId)) {
        for (value, _) in &mut self.endpoints {
            map(value);
        }
        self.edge.iter_mut().for_each(&mut *map);
        if let Some(owner) = &mut self.edge_owner {
            map(owner);
        }
        self.outputs.iter_mut().for_each(&mut *map);
        if let Some((fields, output)) = &mut self.authorization {
            fields.iter_mut().for_each(&mut *map);
            map(output);
        }
    }
}

impl EmitOperation for PathFinding {
    fn emit(&self, mut inputs: Vec<SqlFragment>, context: &mut Context) -> Result<SqlFragment> {
        let authorization = self
            .authorization
            .map(|_| inputs.pop().expect("verified authorization input"));
        let [start, end, edges]: [SqlFragment; 3] = inputs
            .try_into()
            .map_err(|_| QueryError::Lowering("path finding requires three fragments".into()))?;
        let names: [Identifier; 5] = std::array::from_fn(|_| context.alias());
        let authorization_name = context.alias();
        let mut arms = Vec::new();
        for depth in 1..=self.max_depth {
            let (mut from, bindings, _) = context.relation(start.clone());
            let start_id = resolve(&bindings, self.endpoints[0].0)?;
            let mut previous = (start_id.clone(), Expr::string(&self.endpoints[0].1));
            let mut identities = vec![previous.clone()];
            let mut owners = Vec::new();
            let mut nodes = vec![Expr::func(
                "tuple",
                vec![previous.0.clone(), previous.1.clone()],
            )];
            let mut kinds = Vec::new();
            let mut unique = Vec::new();
            for _ in 0..depth {
                let (relation, bindings, _) = context.relation(edges.clone());
                if let Some(owner) = self.edge_owner {
                    owners.push(resolve(&bindings, owner)?);
                }
                let [source, target, source_kind, target_kind, kind] =
                    self.edge.map(|value| resolve(&bindings, value));
                let (source, target, source_kind, target_kind, kind) =
                    (source?, target?, source_kind?, target_kind?, kind?);
                let on = Expr::and(
                    Expr::eq(previous.0, source),
                    Expr::eq(previous.1, source_kind),
                );
                from = TableRef::join(JoinType::Inner, from, relation, on);
                let next = Expr::func("tuple", vec![target.clone(), target_kind.clone()]);
                for visited in &nodes {
                    unique.push(Expr::binary(ast::Op::Ne, next.clone(), visited.clone()));
                }
                nodes.push(next);
                kinds.push(kind);
                previous = (target, target_kind);
                identities.push(previous.clone());
            }
            let (target, bindings, _) = context.relation(end.clone());
            let end_id = resolve(&bindings, self.endpoints[1].0)?;
            from = TableRef::join(
                JoinType::Inner,
                from,
                target,
                Expr::and(
                    Expr::eq(previous.0, end_id.clone()),
                    Expr::eq(previous.1, Expr::string(&self.endpoints[1].1)),
                ),
            );
            let mut select = vec![
                SelectExpr::new(Expr::lit(i64::from(depth)), &names[0]),
                SelectExpr::new(Expr::func("array", nodes), &names[1]),
                SelectExpr::new(Expr::func("array", kinds), &names[2]),
                SelectExpr::new(start_id, &names[3]),
                SelectExpr::new(end_id, &names[4]),
            ];
            if let (Some(fragment), Some((fields, _))) = (&authorization, self.authorization) {
                let mut checks = Vec::new();
                for (index, (id, kind)) in identities.into_iter().enumerate() {
                    let (relation, bindings, _) = context.relation(fragment.clone());
                    let mut on = Expr::and(
                        Expr::eq(id, resolve(&bindings, fields[0])?),
                        Expr::eq(kind.clone(), resolve(&bindings, fields[1])?),
                    );
                    for owner in owners
                        .get(index.wrapping_sub(1))
                        .into_iter()
                        .chain(owners.get(index))
                    {
                        on = Expr::and(on, Expr::eq(resolve(&bindings, fields[2])?, owner.clone()));
                    }
                    from = TableRef::join(JoinType::Inner, from, relation, on);
                    checks.push(Expr::func(
                        "tuple",
                        vec![resolve(&bindings, fields[2])?, kind],
                    ));
                }
                select.push(SelectExpr::new(
                    Expr::func("array", checks),
                    &authorization_name,
                ));
            }
            arms.push(Query {
                select,
                from,
                where_clause: Expr::conjoin(unique),
                ..Default::default()
            });
        }
        let candidates = context.alias();
        let union = context.alias();
        let mut candidate_query = Query {
            select: names
                .iter()
                .map(|name| SelectExpr::new(Expr::col(&union, name), name))
                .collect(),
            from: TableRef::union_all(arms, &union),
            ..Default::default()
        };
        if authorization.is_some() {
            candidate_query.select.push(SelectExpr::new(
                Expr::col(&union, &authorization_name),
                &authorization_name,
            ));
        }
        let shortest = context.alias();
        let source = context.alias();
        let minimum = Query {
            select: vec![
                SelectExpr::new(
                    Expr::func("min", vec![Expr::col(&source, &names[0])]),
                    &names[0],
                ),
                SelectExpr::new(Expr::col(&source, &names[3]), &names[3]),
                SelectExpr::new(Expr::col(&source, &names[4]), &names[4]),
            ],
            from: TableRef::Reference {
                name: candidates.clone(),
                alias: source.clone(),
            },
            group_by: vec![Expr::col(&source, &names[3]), Expr::col(&source, &names[4])],
            ..Default::default()
        };
        let paths = context.alias();
        let lengths = context.alias();
        let on = Expr::conjoin(
            [0, 3, 4]
                .map(|index| {
                    Expr::eq(
                        Expr::col(&paths, &names[index]),
                        Expr::col(&lengths, &names[index]),
                    )
                })
                .to_vec(),
        )
        .expect("path join keys");
        Ok(SqlFragment {
            query: Query {
                ctes: vec![
                    Cte::new(&candidates, candidate_query),
                    Cte::new(&shortest, minimum),
                ],
                distinct: true,
                from: TableRef::join(
                    JoinType::Inner,
                    TableRef::Reference {
                        name: candidates,
                        alias: paths.clone(),
                    },
                    TableRef::Reference {
                        name: shortest,
                        alias: lengths,
                    },
                    on,
                ),
                ..Default::default()
            },
            exports: self
                .outputs
                .iter()
                .zip(&names)
                .map(|(value, name)| (*value, Expr::col(&paths, name)))
                .chain(
                    self.authorization
                        .map(|(_, value)| (value, Expr::col(&paths, &authorization_name))),
                )
                .collect(),
        })
    }
}

pub fn plan<S: EmitOperation>(
    input: &Input,
    model: &impl QueryDataModel,
    mut select: impl FnMut(Source, &mut Values) -> Result<Node<S, Scalar, Infallible>>,
) -> Result<(ast::Node, ResultBindings)> {
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| QueryError::Validation("missing path specification".into()))?;
    let mut values = Values::default();
    let mut inputs = Vec::new();
    let mut endpoints = Vec::new();
    for alias in [&path.from, &path.to] {
        let mut node = input
            .nodes
            .iter()
            .find(|node| &node.id == alias)
            .cloned()
            .ok_or_else(|| QueryError::ReferenceError("path endpoint is missing".into()))?;
        node.columns = Some(ColumnSelection::List(vec![]));
        let required = [(node.id_property.clone(), Identifier::generated())];
        let entity = node
            .entity
            .clone()
            .ok_or_else(|| QueryError::ReferenceError("path endpoint requires a type".into()))?;
        let bound = bind_node(
            &Input {
                nodes: vec![node],
                ..Default::default()
            },
            model,
            &required,
            None,
            values,
        )?;
        values = bound.values;
        endpoints.push((bound.required[0], entity));
        inputs.push(
            bound
                .root
                .expand_sources(&mut |source| select(source, &mut values))?
                .map_extensions(&mut |never| match never {}),
        );
    }
    let (mut edge_source, edge) = dynamic_edges(&path.rel_types, model, &mut values)?;
    let outputs = output_types().map(|kind| values.allocate(kind));
    let mut authorization_inputs = Vec::new();
    let mut authorization_arms = Vec::new();
    let mut entities = model
        .graph()
        .variants()
        .filter(|variant| {
            path.rel_types.iter().any(|kind| {
                kind == "*" || kind == &model.graph().relationship(variant.relationship).name
            })
        })
        .flat_map(|variant| [variant.source, variant.target])
        .collect::<Vec<_>>();
    entities.sort_by_key(|entity| model.graph().entity(*entity).name.as_str());
    entities.dedup();
    let owner_property = entities
        .first()
        .and_then(|entity| model.redaction_id_column(*entity))
        .filter(|property| {
            *property != "id"
                && entities
                    .iter()
                    .all(|entity| model.redaction_id_column(*entity) == Some(*property))
        });
    let edge_owner = if let (
        Some(property),
        Source::Edge {
            relationships,
            properties,
            ..
        },
    ) = (owner_property, &mut edge_source)
    {
        if model
            .query_backend()
            .edge_tables(relationships)
            .iter()
            .all(|table| model.table_column_type(table, property) == Some(ontology::DataType::Int))
        {
            let value = values.allocate(ValueType::Int64);
            properties.push((value, property.into()));
            Some(value)
        } else {
            None
        }
    } else {
        None
    };
    inputs.push(select(edge_source, &mut values)?.map_extensions(&mut |never| match never {}));
    let needs_authorization = entities.iter().any(|entity| {
        model
            .redaction_id_column(*entity)
            .is_some_and(|column| column != "id")
    });
    if needs_authorization {
        for entity in entities {
            let name = &model.graph().entity(entity).name;
            if model.entity_table(name).is_none() {
                continue;
            }
            let property = model.redaction_id_column(entity).unwrap_or("id");
            let fields = ["id", property].map(|name| {
                model.property_for_entity_id(entity, name).ok_or_else(|| {
                    QueryError::ReferenceError("missing path identity property".into())
                })
            });
            let properties = fields
                .into_iter()
                .map(|field| {
                    let field = field?;
                    Ok((values.allocate(ValueType::from(field.data_type)), field.id))
                })
                .collect::<Result<Vec<_>>>()?;
            let identity = properties[0].0;
            let authorization_id = properties[1].0;
            let source = select(
                Source::Entity {
                    binding: format!("path_identity_{name}"),
                    entity,
                    properties,
                },
                &mut values,
            )?;
            let kind = values.allocate(ValueType::String);
            authorization_arms.push(vec![identity, kind, authorization_id]);
            authorization_inputs.push(Node {
                op: Op::Project(vec![
                    Assignment {
                        output: identity,
                        expression: PlanExpr::Value(identity),
                    },
                    Assignment {
                        output: kind,
                        expression: PlanExpr::String(name.clone()),
                    },
                    Assignment {
                        output: authorization_id,
                        expression: PlanExpr::Value(authorization_id),
                    },
                ]),
                inputs: vec![source.map_extensions(&mut |never| match never {})],
            });
        }
    }
    let authorization = if needs_authorization {
        let fields = [ValueType::Int64, ValueType::String, ValueType::Int64]
            .map(|kind| values.allocate(kind));
        inputs.push(Node {
            op: Op::Union {
                outputs: fields.to_vec(),
                arms: authorization_arms,
            },
            inputs: authorization_inputs,
        });
        Some((fields, values.allocate(output_types()[1].clone())))
    } else {
        None
    };
    let root = Node {
        op: Op::Extension(PathFinding {
            endpoints: endpoints.try_into().expect("two endpoints"),
            edge,
            outputs,
            max_depth: path.max_depth,
            authorization,
            edge_owner,
        }),
        inputs,
    };
    let mut context = Context::default();
    let fragment = lower_with_context(&root, &values, &mut context, &scalar::emit)?;
    let order = fragment
        .exports
        .iter()
        .map(|(_, expression)| expression.clone())
        .collect::<Vec<_>>();
    let mut names = vec![
        "depth".into(),
        path_column().into(),
        edge_kinds_column().into(),
    ];
    if authorization.is_some() {
        names.push(path_authorizations_column().into());
    }
    let mut query = fragment.into_query(&names)?;
    query.order_by = vec![OrderExpr::asc(order[0].clone())];
    let stable_order = order[1..3]
        .iter()
        .map(|expression| OrderExpr::asc(Expr::func("toString", vec![expression.clone()])))
        .collect();
    Ok((
        ast::Node::Query(Box::new(query)),
        ResultBindings {
            source_bindings: context.source_bindings,
            stable_order,
        },
    ))
}
