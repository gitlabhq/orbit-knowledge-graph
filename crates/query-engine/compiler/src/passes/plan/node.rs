//! A binding as the planner reads it: its source table, pinned ids, id range, and the filters
//! attached to it, each bound to the storage facts that decide how it lowers.

use ontology::DataType;
use ontology::constants::DEFAULT_PRIMARY_KEY;
use orbit_utils::query_types::SqlType;
use query_data_model::PropertyId;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, Function, TextMatch, UnaryOp,
};
use query_data_model::plan::logical::{
    Comparison, Expr as Logical, Literal, Matching, Operand, Predicate,
};
use serde_json::Value as Json;

use super::filters::NullAsEpoch;
use crate::logical::Query;
use crate::logical::facts::{Anchor, Fact};
use crate::passes::codegen::RelationalModel;
use crate::passes::lower::build::expr::{epoch, in_list, null_if_epoch, parameter_type};
use crate::passes::lower::build::sources::Scope;
use crate::passes::lower::build::{Built, and, binary, call, col, eq, param};

pub(crate) struct Node<'p> {
    pub alias: &'p str,
    pub entity: &'p str,
    pub table: Option<&'p str>,
    pub redaction: &'p str,
    pub pinned: Vec<i64>,
    pub range: Option<(i64, i64)>,
    pub filters: Vec<Filter<'p>>,
    pub has_traversal_path: bool,
}

pub(crate) struct Filter<'p> {
    fact: Fact<'p>,
    pub id: PropertyId,
    property: &'p str,
    rhs: Option<&'p str>,
    data_type: DataType,
    in_sort_key: bool,
    text_indexed: bool,
    null_as_epoch: NullAsEpoch,
}

impl<'p> Node<'p> {
    pub fn new<M: RelationalModel>(query: Query<'p>, anchor: Anchor<'p>, model: &'p M) -> Self {
        let binding = anchor.binding;
        let graph = query.model.graph();
        let source = model
            .catalog()
            .source(binding.entity)
            .map(|(_, source)| source);
        let table = source.map(|source| source.table.as_str());
        let epoch_type = |property| {
            model
                .null_as_epoch(property)
                .then_some(graph.property(property).data_type)
        };
        let mut filters: Vec<Filter<'p>> = anchor
            .filters
            .into_iter()
            .filter_map(|fact| {
                let Logical::Property { property, .. } = fact.left else {
                    return None;
                };
                let declared = graph.property(property);
                let rhs = match fact.right() {
                    Some(Logical::Property { property, .. }) => Some(property),
                    _ => None,
                };
                Some(Filter {
                    fact,
                    id: property,
                    property: &declared.name,
                    rhs: rhs.map(|rhs| graph.property(rhs).name.as_str()),
                    data_type: declared.data_type,
                    in_sort_key: table
                        .is_some_and(|table| model.in_sort_key(table, &declared.name)),
                    text_indexed: model.has_text_index(property),
                    null_as_epoch: NullAsEpoch {
                        column: epoch_type(property),
                        rhs: rhs.and_then(epoch_type),
                    },
                })
            })
            .collect();
        filters.sort_by_key(|filter| filter.property);
        if filters
            .iter()
            .any(|filter| filter.in_sort_key && filter.matches_values())
        {
            for filter in &mut filters {
                filter.text_indexed = false;
            }
        }
        Self {
            alias: binding.name,
            entity: &graph.entity(binding.entity).name,
            table,
            redaction: model
                .redaction_id_column(binding.entity)
                .unwrap_or(DEFAULT_PRIMARY_KEY),
            pinned: anchor.pinned,
            range: anchor.range,
            filters,
            has_traversal_path: source.is_some_and(|source| source.path_column.is_some()),
        }
    }

    pub fn is_id_pinned_only(&self) -> bool {
        !self.pinned.is_empty() && self.filters.is_empty() && self.range.is_none()
    }

    pub fn uses_default_pk(&self) -> bool {
        self.redaction == DEFAULT_PRIMARY_KEY
    }
}

impl Filter<'_> {
    pub fn tags(&self, key: &str) -> Option<Vec<String>> {
        let tag = |literal: &Literal| match json(literal) {
            Json::String(text) => format!("{key}:{text}"),
            value => format!("{key}:{value}"),
        };
        match self.fact.leaf {
            Predicate::Compare {
                right: Operand::Literal(literal),
                ..
            } if self.fact.comparison == Some(Comparison::Eq) => Some(vec![tag(literal)]),
            Predicate::In { values, .. } if !values.is_empty() => {
                Some(values.iter().map(tag).collect())
            }
            _ => None,
        }
    }

    fn matches_values(&self) -> bool {
        match self.fact.leaf {
            Predicate::In { .. } => true,
            Predicate::Compare { .. } => self.fact.comparison == Some(Comparison::Eq),
            _ => false,
        }
    }

    pub fn lower<B: RelationalBackend>(&self, scope: &Scope<'_>, alias: &str) -> Built<Expr> {
        let raw = col(scope, alias, self.property)?;
        let epochs = self.null_as_epoch;
        let data_type = parameter_type(Some(&self.data_type));
        let comparison = self.fact.comparison.map(binary_op);
        let expression = match self.fact.leaf {
            Predicate::Compare {
                right: Operand::Expr(_),
                ..
            } => {
                let rhs = col(scope, alias, self.rhs.unwrap_or_default())?;
                let rhs = read::<B>(rhs, epochs.rhs);
                let op = comparison.expect("property comparisons carry an operator");
                return Ok(binary(op, read::<B>(raw, epochs.column), rhs));
            }
            Predicate::Null { null, .. } => {
                let null = *null != self.fact.complemented;
                return Ok(match (null, epochs.column) {
                    (true, Some(data_type)) => eq(raw, epoch::<B>(data_type)),
                    (false, Some(data_type)) => binary(BinaryOp::Ne, raw, epoch::<B>(data_type)),
                    (true, None) => unary(UnaryOp::IsNull, raw),
                    (false, None) => unary(UnaryOp::IsNotNull, raw),
                });
            }
            Predicate::Compare {
                right: Operand::Literal(literal),
                ..
            } => {
                let op = comparison.expect("comparisons carry an operator");
                binary(op, raw.clone(), param::<B>(data_type, json(literal)))
            }
            Predicate::In { values, .. } => {
                let values = values.iter().map(json).collect();
                in_list::<B>(raw.clone(), data_type, values).expect("validated non-empty IN list")
            }
            Predicate::Text {
                matching, needle, ..
            } => {
                let fold = |expr: Expr| match self.in_sort_key {
                    true => expr,
                    false => call(Function::Lower, vec![expr]),
                };
                let needle = fold(param::<B>(SqlType::String, needle.clone().into()));
                let value = fold(raw.clone());
                let mode = match matching {
                    Matching::Contains => TextMatch::Contains,
                    Matching::Token => TextMatch::Token,
                    Matching::AllTokens => TextMatch::AllTokens,
                    Matching::AnyTokens => TextMatch::AnyTokens,
                    Matching::StartsWith => {
                        return Ok(call(Function::StartsWith, vec![value, needle]));
                    }
                    Matching::EndsWith => return Ok(call(Function::EndsWith, vec![value, needle])),
                };
                Expr::TextSearch {
                    mode,
                    value: Box::new(value),
                    query: Box::new(needle),
                }
            }
            Predicate::And(_) | Predicate::Not(_) => {
                unreachable!("facts are leaves")
            }
        };
        let expression = match epochs.column {
            Some(data_type) => and(
                expression,
                binary(BinaryOp::Ne, raw.clone(), epoch::<B>(data_type)),
            ),
            None => expression,
        };
        if !(self.text_indexed && self.matches_values()) {
            return Ok(expression);
        }
        let lower = |expr| call(Function::Lower, vec![expr]);
        let mut candidates: Vec<_> = self
            .fact
            .literals()
            .iter()
            .map(|literal| lower(param::<B>(data_type, json(literal))))
            .collect();
        let matched = match candidates.len() {
            1 => eq(lower(raw), candidates.remove(0)),
            _ => binary(BinaryOp::In, lower(raw), call(Function::Tuple, candidates)),
        };
        Ok(and(expression, matched))
    }
}

fn read<B: RelationalBackend>(column: Expr, null_as_epoch: Option<DataType>) -> Expr {
    match null_as_epoch {
        Some(data_type) => null_if_epoch::<B>(column, data_type),
        None => column,
    }
}

fn unary(op: UnaryOp, value: Expr) -> Expr {
    Expr::Unary {
        op,
        value: Box::new(value),
    }
}

fn binary_op(comparison: Comparison) -> BinaryOp {
    match comparison {
        Comparison::Eq => BinaryOp::Eq,
        Comparison::Ne => BinaryOp::Ne,
        Comparison::Lt => BinaryOp::Lt,
        Comparison::Le => BinaryOp::Le,
        Comparison::Gt => BinaryOp::Gt,
        Comparison::Ge => BinaryOp::Ge,
    }
}

fn json(literal: &Literal) -> Json {
    match literal {
        Literal::String(text) | Literal::Temporal(text) => Json::String(text.clone()),
        Literal::Int(number) => Json::from(*number),
        Literal::Float(number) => Json::from(*number),
        Literal::Bool(flag) => Json::Bool(*flag),
    }
}
