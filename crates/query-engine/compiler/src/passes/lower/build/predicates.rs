use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Expr, Function, Relation, RelationalBuilder, RelationalError, Subquery, UnaryOp,
};

use super::expr::{self, Filter};
use super::{Built, call, column, eq, int, string};
use crate::input::BooleanExpression;
use crate::passes::plan::graph::RelationId;
use crate::passes::plan::predicates::{Column, OutputValue, Predicate, Projection};

#[derive(Default)]
pub(crate) struct Shared(Vec<(RelationId, Subquery)>);

impl Shared {
    pub(crate) fn insert(&mut self, relation: RelationId, body: Subquery) {
        self.0.push((relation, body));
    }

    pub(crate) fn get(&self, relation: RelationId) -> Built<&Subquery> {
        self.0
            .iter()
            .find(|(id, _)| *id == relation)
            .map(|(_, body)| body)
            .ok_or(RelationalError::Incomplete("a shared relation"))
    }
}

pub(crate) struct Lowering<'s> {
    pub scope: &'s [&'s Relation],
    pub shared: &'s Shared,
}

impl Lowering<'_> {
    pub(crate) fn predicate<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        value: &Predicate,
    ) -> Built<Expr> {
        self.lower(q, value, false)
    }

    fn boolean<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        expression: &BooleanExpression<Box<Predicate>>,
        negated: bool,
    ) -> Built<Expr> {
        Ok(match expression {
            BooleanExpression::Leaf(leaf) => self.lower(q, leaf, negated)?,
            BooleanExpression::And(children) => {
                let mut parts = Vec::new();
                for child in children {
                    parts.push(self.boolean(q, child, negated)?);
                }
                expr::conjoined(parts)
            }
            BooleanExpression::Not(child) => Expr::Unary {
                op: UnaryOp::Not,
                value: Box::new(self.boolean(q, child, true)?),
            },
        })
    }

    fn column(&self, value: &Column) -> Built<Expr> {
        column(self.scope, value)
    }

    fn lower<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        value: &Predicate,
        negated: bool,
    ) -> Built<Expr> {
        Ok(match value {
            Predicate::Boolean(expression) => self.boolean(q, expression, negated)?,
            Predicate::Property {
                column,
                filter,
                data_type,
                in_sort_key,
                text_indexed,
                null_as_epoch,
                ..
            } => expr::filter::<B>(
                self.scope,
                Filter {
                    column,
                    filter,
                    data_type: data_type.as_ref(),
                    in_sort_key: *in_sort_key,
                    text_indexed: *text_indexed,
                    null_as_epoch: *null_as_epoch,
                    negated,
                },
            )?,
            Predicate::Ids { column, values } => expr::ids::<B>(self.column(column)?, values),
            Predicate::IdRange { column, start, end } => {
                expr::id_range(self.column(column)?, *start, *end)
            }
            Predicate::Live { alias } => expr::live::<B>(self.scope, alias)?,
            Predicate::EntityKind { column, entity } => eq(self.column(column)?, string(entity)),
            Predicate::ColumnEq { left, right } => eq(self.column(left)?, self.column(right)?),
            Predicate::RelationshipKinds { alias, kinds } => {
                expr::relationship_kinds::<B>(self.scope, alias, kinds)?
                    .unwrap_or_else(|| super::literal(true.into()))
            }
            Predicate::Tags { column, values } => expr::tags(self.column(column)?, values),
            Predicate::Membership {
                columns,
                relation,
                keys,
            } => {
                let mut values = Vec::new();
                for value in columns {
                    values.push(self.column(value)?);
                }
                let value = match values.len() {
                    1 => values.remove(0),
                    _ => call(Function::Tuple, values),
                };
                let body = self.shared.get(*relation)?.clone();
                let name = crate::passes::lower::graph::relation_name(*relation);
                let keys = q.subquery(&name, |q| {
                    let rows = q.read(&body, None)?;
                    let keys = keys
                        .iter()
                        .map(|key| rows.column(key))
                        .collect::<Built<Vec<_>>>()?;
                    q.select(rows, keys)
                })?;
                keys.contains(value)
            }
        })
    }

    pub(crate) fn output<B: RelationalBackend>(&self, value: &OutputValue) -> Built<Expr> {
        let step = |id: &Column, kind: &Column| -> Built<Expr> {
            let pair = call(Function::Tuple, vec![self.column(id)?, self.column(kind)?]);
            Ok(call(Function::Array, vec![pair]))
        };
        Ok(match value {
            OutputValue::Column(value) => self.column(value)?,
            OutputValue::EpochAsNull(value, data_type) => {
                expr::null_if_epoch::<B>(self.column(value)?, *data_type)
            }
            OutputValue::Text(value) => string(value),
            OutputValue::Depth(value) => int(i64::from(*value)),
            OutputValue::Path(steps) => {
                let mut pairs = Vec::new();
                for (id, kind) in steps {
                    let pair = vec![self.column(id)?, self.column(kind)?];
                    pairs.push(call(Function::Tuple, pair));
                }
                call(Function::Array, pairs)
            }
            OutputValue::PathAppend { path, id, kind } => call(
                Function::ArrayConcat,
                vec![self.column(path)?, step(id, kind)?],
            ),
            OutputValue::PathPrepend { path, id, kind } => call(
                Function::ArrayConcat,
                vec![step(id, kind)?, self.column(path)?],
            ),
        })
    }

    pub(crate) fn projections<B: RelationalBackend>(
        &self,
        values: &[Projection],
    ) -> Built<Vec<(String, Expr)>> {
        values
            .iter()
            .filter(|value| {
                !super::stored::<B>()
                    || !matches!(&value.value, OutputValue::Column(column) if super::versioning(&column.name))
            })
            .map(|value| Ok((value.name.clone(), self.output::<B>(&value.value)?)))
            .collect()
    }
}
