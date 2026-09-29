use std::collections::HashMap;
use std::convert::Infallible;

pub mod scalar;

use crate::ast::{self, Expr, Query, SelectExpr, TableRef};
use crate::error::{QueryError, Result};
use crate::planning::generic::{self, Function, JoinKind, Node, Op, Operation, ValueId, Values};
use crate::planning::physical::{CurrentRows, Read};

impl EmitOperation for Read {
    fn emit(&self, _: Vec<SqlFragment>, context: &mut Context) -> Result<SqlFragment> {
        let alias = context.alias();
        let from = match self.current_rows {
            CurrentRows::Snapshot => TableRef::scan(&self.table, &alias),
            CurrentRows::Final => TableRef::scan_final(&self.table, &alias),
        };

        Ok(SqlFragment {
            query: Query {
                from,
                ..Default::default()
            },
            exports: self
                .columns
                .iter()
                .map(|(value, column)| (*value, Expr::col(&alias, column)))
                .collect(),
        })
    }
}

impl EmitOperation for Infallible {
    fn emit(&self, _: Vec<SqlFragment>, _: &mut Context) -> Result<SqlFragment> {
        match *self {}
    }
}

pub type Bindings = HashMap<ValueId, Expr>;

pub struct SqlFragment {
    pub query: Query,
    pub exports: Vec<(ValueId, Expr)>,
}

#[derive(Default)]
pub struct Context {
    next_alias: usize,
}

impl Context {
    pub fn alias(&mut self) -> String {
        let alias = format!("_q{}", self.next_alias);
        self.next_alias += 1;

        alias
    }

    pub fn relation(
        &mut self,
        mut fragment: SqlFragment,
    ) -> (TableRef, Bindings, Vec<ast::OrderExpr>) {
        let alias = self.alias();
        let mut bindings = Bindings::new();

        fragment.query.select = fragment
            .exports
            .into_iter()
            .map(|(value, expression)| {
                let column = self.alias();
                bindings.insert(value, Expr::col(&alias, &column));
                SelectExpr::new(expression, column)
            })
            .collect();
        let order = fragment
            .query
            .order_by
            .iter()
            .map(|key| {
                let column = self.alias();
                fragment
                    .query
                    .select
                    .push(SelectExpr::new(key.expr.clone(), &column));
                ast::OrderExpr {
                    expr: Expr::col(&alias, column),
                    desc: key.desc,
                }
            })
            .collect();

        (TableRef::subquery(fragment.query, alias), bindings, order)
    }
}

pub trait EmitOperation: Operation {
    fn emit(&self, inputs: Vec<SqlFragment>, context: &mut Context) -> Result<SqlFragment>;
}

pub fn lower<S: EmitOperation, F: Function, E: EmitOperation>(
    plan: &Node<S, F, E>,
    values: &Values,
    emit_expression: &impl Fn(&generic::Expr<F>, &Bindings) -> Result<Expr>,
) -> Result<SqlFragment> {
    lower_with_context(plan, values, &mut Context::default(), emit_expression)
}

pub fn lower_with_context<S: EmitOperation, F: Function, E: EmitOperation>(
    plan: &Node<S, F, E>,
    values: &Values,
    context: &mut Context,
    emit_expression: &impl Fn(&generic::Expr<F>, &Bindings) -> Result<Expr>,
) -> Result<SqlFragment> {
    plan.output(values)?;

    emit_node(plan, context, emit_expression)
}

fn emit_node<S: EmitOperation, F: Function, E: EmitOperation>(
    node: &Node<S, F, E>,
    context: &mut Context,
    emit_expression: &impl Fn(&generic::Expr<F>, &Bindings) -> Result<Expr>,
) -> Result<SqlFragment> {
    let mut inputs = node
        .inputs
        .iter()
        .map(|input| emit_node(input, context, emit_expression))
        .collect::<Result<Vec<_>>>()?;

    match &node.op {
        Op::Read(source) => source.emit(inputs, context),
        Op::Extension(extension) => extension.emit(inputs, context),
        Op::Aggregate { groups, measures } => {
            let input = inputs.pop().expect("verified aggregate arity");
            let (from, bindings, _) = context.relation(input);
            let mut query = Query {
                from,
                ..Default::default()
            };
            let mut exports = Vec::new();

            for group in groups {
                let expression = emit_expression(&group.expression, &bindings)?;
                query.group_by.push(expression.clone());
                exports.push((group.output, expression));
            }

            for measure in measures {
                let argument = measure
                    .argument
                    .as_ref()
                    .map(|arg| emit_expression(arg, &bindings).map(Box::new))
                    .transpose()?;
                let filter = measure
                    .filter
                    .as_ref()
                    .map(|filter| emit_expression(filter, &bindings).map(Box::new))
                    .transpose()?;
                exports.push((
                    measure.output,
                    Expr::Aggregate {
                        name: measure.function.to_string(),
                        argument,
                        distinct: measure.distinct,
                        filter,
                    },
                ));
            }

            Ok(SqlFragment { query, exports })
        }
        Op::Union { outputs, arms } => {
            let columns: Vec<_> = outputs.iter().map(|_| context.alias()).collect();
            let queries = inputs
                .into_iter()
                .zip(arms)
                .map(|(input, arm)| {
                    let (from, bindings, _) = context.relation(input);
                    let select = arm
                        .iter()
                        .zip(&columns)
                        .map(|(value, column)| {
                            Ok(SelectExpr::new(resolve(&bindings, *value)?, column))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Query {
                        from,
                        select,
                        ..Default::default()
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let alias = context.alias();
            let query = Query {
                from: TableRef::union_all(queries, &alias),
                ..Default::default()
            };
            Ok(SqlFragment {
                query,
                exports: outputs
                    .iter()
                    .zip(columns)
                    .map(|(value, column)| (*value, Expr::col(&alias, column)))
                    .collect(),
            })
        }
        Op::Join { kind, condition } => {
            let right = inputs.pop().expect("verified join arity");
            let left = inputs.pop().expect("verified join arity");
            let left_values: Vec<_> = left.exports.iter().map(|(value, _)| *value).collect();
            let right_values: Vec<_> = right.exports.iter().map(|(value, _)| *value).collect();

            let (left, mut bindings, _) = context.relation(left);
            let (right, right_bindings, _) = context.relation(right);
            bindings.extend(right_bindings);
            let condition = emit_expression(condition, &bindings)?;

            let (query, output) = match kind {
                JoinKind::Inner => (
                    Query {
                        from: TableRef::join(ast::JoinType::Inner, left, right, condition),
                        ..Default::default()
                    },
                    left_values
                        .into_iter()
                        .chain(right_values)
                        .collect::<Vec<_>>(),
                ),
                JoinKind::Semi | JoinKind::Anti => {
                    let Expr::BinaryOp {
                        op: ast::Op::Eq,
                        left: key,
                        right: lookup,
                    } = condition
                    else {
                        return Err(QueryError::Lowering(
                            "membership lowering requires an equality key".into(),
                        ));
                    };

                    if !left_values
                        .iter()
                        .any(|value| bindings.get(value) == Some(key.as_ref()))
                        || !right_values
                            .iter()
                            .any(|value| bindings.get(value) == Some(lookup.as_ref()))
                    {
                        return Err(QueryError::Lowering(
                            "membership keys must belong to their respective inputs".into(),
                        ));
                    }

                    let membership = Expr::InSelect {
                        expr: key.clone(),
                        query: Box::new(Query {
                            select: vec![SelectExpr::new(*lookup.clone(), context.alias())],
                            from: right,
                            where_clause: Some(Expr::unary(ast::Op::IsNotNull, *lookup)),
                            ..Default::default()
                        }),
                    };
                    if *kind == JoinKind::Anti {
                        return Err(QueryError::Lowering(
                            "anti join requires null-aware existence lowering".into(),
                        ));
                    }

                    (
                        Query {
                            from: left,
                            where_clause: Some(Expr::and(
                                Expr::unary(ast::Op::IsNotNull, *key),
                                membership,
                            )),
                            ..Default::default()
                        },
                        left_values,
                    )
                }
            };

            let exports = output
                .into_iter()
                .map(|value| Ok((value, resolve(&bindings, value)?)))
                .collect::<Result<_>>()?;

            Ok(SqlFragment { query, exports })
        }
        op => {
            let input = inputs.pop().expect("verified unary arity");
            let output: Vec<_> = input.exports.iter().map(|(value, _)| *value).collect();
            let (from, bindings, order_by) = context.relation(input);
            let mut query = Query {
                from,
                order_by,
                ..Default::default()
            };

            let exports = if let Op::Project(assignments) = op {
                assignments
                    .iter()
                    .map(|assignment| {
                        Ok((
                            assignment.output,
                            emit_expression(&assignment.expression, &bindings)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                match op {
                    Op::Filter(predicate) => {
                        query.where_clause = Some(emit_expression(predicate, &bindings)?)
                    }
                    Op::Limit(limit) => query.limit = Some(*limit),
                    Op::Sort(keys) => {
                        query.order_by.clear();
                        for key in keys {
                            let expression = resolve(&bindings, key.value)?;
                            query.order_by.push(ast::OrderExpr {
                                expr: Expr::unary(ast::Op::IsNull, expression.clone()),
                                desc: key.nulls_first,
                            });
                            query.order_by.push(ast::OrderExpr {
                                expr: expression,
                                desc: key.descending,
                            });
                        }
                    }
                    _ => unreachable!("handled non-unary operation"),
                }

                output
                    .into_iter()
                    .map(|value| Ok((value, resolve(&bindings, value)?)))
                    .collect::<Result<Vec<_>>>()?
            };

            Ok(SqlFragment { query, exports })
        }
    }
}

pub fn resolve(bindings: &Bindings, value: ValueId) -> Result<Expr> {
    bindings.get(&value).cloned().ok_or_else(|| {
        QueryError::Lowering(format!("value {value:?} is not visible in this SQL scope"))
    })
}

impl SqlFragment {
    pub fn into_query(mut self, names: &[String]) -> Result<Query> {
        if names.len() != self.exports.len() {
            return Err(QueryError::Lowering(
                "result name count does not match plan output".into(),
            ));
        }

        self.query.select = self
            .exports
            .into_iter()
            .zip(names)
            .map(|((_, expression), name)| SelectExpr::new(expression, name))
            .collect();

        Ok(self.query)
    }
}
