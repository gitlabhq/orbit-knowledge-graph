use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Expr, Function, Relation, RelationalBuilder, UnaryOp,
};

use super::expr::{self, Filter};
use super::{Built, call, column, eq, int, string};
use crate::passes::plan::boolean::Boolean;
use crate::passes::plan::predicates::{Column, OutputValue, Predicate, Projection};

pub(crate) struct Lowering<'s> {
    pub scope: &'s [&'s Relation],
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
        expression: &Boolean<Box<Predicate>>,
        negated: bool,
    ) -> Built<Expr> {
        Ok(match expression {
            Boolean::Leaf(leaf) => self.lower(q, leaf, negated)?,
            Boolean::And(children) => {
                let mut parts = Vec::new();
                for child in children {
                    parts.push(self.boolean(q, child, negated)?);
                }
                expr::conjoined(parts)
            }
            Boolean::Not(child) => Expr::Unary {
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
                test,
                data_type,
                in_sort_key,
                text_indexed,
                null_as_epoch,
                ..
            } => expr::filter::<B>(
                self.scope,
                Filter {
                    column,
                    test,
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
                let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
                let name = format!("{}keys", crate::constants::internal_column_prefix());
                super::membership(q, value, &relation.rows, &name, &keys)?
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
