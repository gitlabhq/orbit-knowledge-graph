use std::convert::Infallible;

use query_data_model::QueryDataModel;

use super::bind::{Source, bind_node, dynamic_edges};
use super::generic::{Node, Op, Operation, Schema, ValueId, ValueType, Values};
use super::physical::Scalar;
use crate::ast::{self, Expr, Identifier, JoinType, OrderExpr, Query, TableRef};
use crate::constants::{
    neighbor_id_column, neighbor_is_outgoing_column, neighbor_type_column, relationship_type_column,
};
use crate::error::{QueryError, Result};
use crate::input::{Direction, Input, OrderDirection};
use crate::lowering::{Context, EmitOperation, SqlFragment, lower_with_context, resolve, scalar};
use crate::passes::enforce::{ResultBindings, ReturnRequirements};

#[derive(Clone, PartialEq)]
pub struct Neighbors {
    center: Schema,
    identity: ValueId,
    entity: String,
    edge: [ValueId; 5],
    outputs: [ValueId; 4],
    direction: Direction,
}

impl Operation for Neighbors {
    fn output(&self, inputs: &[Schema], values: &Values) -> Result<Schema> {
        let [center, edge] = inputs else {
            return Err(QueryError::PipelineInvariant(
                "neighbors requires center and edge inputs".into(),
            ));
        };
        if center != &self.center
            || !center.contains(&self.identity)
            || self.edge.iter().any(|value| !edge.contains(value))
        {
            return Err(QueryError::PipelineInvariant(
                "neighbors inputs do not satisfy its value contract".into(),
            ));
        }
        for (value, expected) in self
            .edge
            .iter()
            .zip([
                ValueType::Int64,
                ValueType::Int64,
                ValueType::String,
                ValueType::String,
                ValueType::String,
            ])
            .chain(self.outputs.iter().zip([
                ValueType::Int64,
                ValueType::String,
                ValueType::String,
                ValueType::Int64,
            ]))
        {
            if values.data_type(*value)? != &expected {
                return Err(QueryError::PipelineInvariant(
                    "invalid neighbors field type".into(),
                ));
            }
        }
        Ok(self.center.iter().chain(&self.outputs).copied().collect())
    }

    fn map_values(&mut self, map: &mut impl FnMut(&mut ValueId)) {
        self.center.iter_mut().for_each(&mut *map);
        map(&mut self.identity);
        self.edge.iter_mut().for_each(&mut *map);
        self.outputs.iter_mut().for_each(map);
    }
}

impl EmitOperation for Neighbors {
    fn emit(&self, mut inputs: Vec<SqlFragment>, context: &mut Context) -> Result<SqlFragment> {
        let edges = inputs.pop().expect("verified neighbors arity");
        let center = inputs.pop().expect("verified neighbors arity");
        let (center_from, center_bindings, _) = context.relation(center);
        let (edge_from, edge_bindings, _) = context.relation(edges);
        let directions: &[bool] = match self.direction {
            Direction::Outgoing => &[true],
            Direction::Incoming => &[false],
            Direction::Both => &[true, false],
        };
        let output_values: Vec<_> = self.center.iter().chain(&self.outputs).copied().collect();
        let names: Vec<_> = output_values.iter().map(|_| context.alias()).collect();
        let mut queries = Vec::new();
        for outgoing in directions {
            let (center_id, neighbor_id, center_kind, neighbor_kind) = if *outgoing {
                (self.edge[0], self.edge[1], self.edge[2], self.edge[3])
            } else {
                (self.edge[1], self.edge[0], self.edge[3], self.edge[2])
            };
            let on = Expr::and(
                Expr::eq(
                    resolve(&center_bindings, self.identity)?,
                    resolve(&edge_bindings, center_id)?,
                ),
                Expr::eq(
                    resolve(&edge_bindings, center_kind)?,
                    Expr::string(&self.entity),
                ),
            );
            let mut exports = self
                .center
                .iter()
                .map(|value| Ok((*value, resolve(&center_bindings, *value)?)))
                .collect::<Result<Vec<_>>>()?;
            exports.extend([
                (self.outputs[0], resolve(&edge_bindings, neighbor_id)?),
                (self.outputs[1], resolve(&edge_bindings, neighbor_kind)?),
                (self.outputs[2], resolve(&edge_bindings, self.edge[4])?),
                (self.outputs[3], Expr::lit(i64::from(*outgoing))),
            ]);
            queries.push(
                SqlFragment {
                    query: Query {
                        from: TableRef::join(
                            JoinType::Inner,
                            center_from.clone(),
                            edge_from.clone(),
                            on,
                        ),
                        ..Default::default()
                    },
                    exports,
                }
                .into_query(&names)?,
            );
        }
        let alias = context.alias();
        Ok(SqlFragment {
            query: Query {
                from: TableRef::union_all(queries, &alias),
                ..Default::default()
            },
            exports: output_values
                .into_iter()
                .zip(names)
                .map(|(value, name)| (value, Expr::col(&alias, name)))
                .collect(),
        })
    }
}

pub fn plan<S: EmitOperation>(
    input: &Input,
    requirements: &ReturnRequirements,
    model: &impl QueryDataModel,
    mut select: impl FnMut(Source, &mut Values) -> Result<Node<S, Scalar, Infallible>>,
) -> Result<(ast::Node, ResultBindings)> {
    let [center] = input.nodes.as_slice() else {
        return Err(QueryError::Validation(
            "neighbors requires one center".into(),
        ));
    };
    let neighbors = input
        .neighbors
        .as_ref()
        .ok_or_else(|| QueryError::Validation("neighbors specification is missing".into()))?;
    let required: Vec<_> = requirements
        .required
        .iter()
        .map(|(_, property, name)| (property.clone(), name.clone()))
        .collect();
    let single = Input {
        nodes: vec![center.clone()],
        ..Default::default()
    };
    let bound = bind_node(&single, model, &required, None, Values::default())?;
    let mut values = bound.values;
    let physical = bound
        .root
        .expand_sources(&mut |source| select(source, &mut values))?;
    let center_schema = physical.output(&values)?;
    let (source, edge) = dynamic_edges(&neighbors.rel_types, model, &mut values)?;
    let edges = select(source, &mut values)?;
    let outputs = [
        ValueType::Int64,
        ValueType::String,
        ValueType::String,
        ValueType::Int64,
    ]
    .map(|kind| values.allocate(kind));
    let root = Node {
        op: Op::Extension(Neighbors {
            center: center_schema,
            identity: bound.required[0],
            entity: center
                .entity
                .clone()
                .ok_or_else(|| QueryError::ReferenceError("center entity is missing".into()))?,
            edge,
            outputs,
            direction: neighbors.direction,
        }),
        inputs: vec![
            physical.map_extensions(&mut |never| match never {}),
            edges.map_extensions(&mut |never| match never {}),
        ],
    };
    let mut context = Context::default();
    let fragment = lower_with_context(&root, &values, &mut context, &scalar::emit)?;
    let bindings = fragment.exports.iter().cloned().collect();
    let mut stable_order = std::iter::once(bound.required[0])
        .chain(outputs)
        .map(|value| resolve(&bindings, value).map(OrderExpr::asc))
        .collect::<Result<Vec<_>>>()?;
    let mut names = bound.outputs;
    names.extend(
        [
            neighbor_id_column(),
            neighbor_type_column(),
            relationship_type_column(),
            neighbor_is_outgoing_column(),
        ]
        .map(Identifier::from),
    );
    let mut query = fragment.into_query(&names)?;
    if let Some(order) = &input.order_by {
        query.order_by.push(OrderExpr {
            expr: resolve(&bindings, bound.required[1])?,
            desc: order.direction == OrderDirection::Desc,
        });
        stable_order.retain(|key| key.expr != query.order_by[0].expr);
    }
    Ok((
        ast::Node::Query(Box::new(query)),
        ResultBindings {
            source_bindings: context.source_bindings,
            stable_order,
        },
    ))
}
