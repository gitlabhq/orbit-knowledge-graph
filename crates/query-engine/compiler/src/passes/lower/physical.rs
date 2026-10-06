use crate::ast::{Cte, Expr, OrderExpr, Query, SelectExpr, TableRef};
use crate::passes::plan::physical::{ExecutionPlan, PhysicalPlan, PhysicalSource};

use super::requirements::{column, predicate, projections};
use super::{EmitOutput, NodeBinding};
use query_data_model::bindings::{QueryBindings, RelationSource};
use query_data_model::storage::StorageCatalog;

pub(super) struct PhysicalLowerer<'a, T> {
    pub bindings: &'a QueryBindings,
    pub storage: &'a StorageCatalog<T>,
}

impl<T> PhysicalLowerer<'_, T> {
    fn latest_row_dedup(
        &self,
        alias: &str,
        keys: &[query_data_model::bindings::ColumnRef],
    ) -> (Vec<OrderExpr>, Option<(u32, Vec<Expr>)>) {
        let keys: Vec<_> = keys
            .iter()
            .map(|key| {
                let query_data_model::bindings::ExportOrigin::Stored(column) = self
                    .bindings
                    .origin(key.export())
                    .expect("planned key export")
                else {
                    unreachable!("latest-row key must be stored")
                };
                Expr::col(alias, self.storage.column(column).name())
            })
            .collect();
        let mut order: Vec<_> = keys.iter().cloned().map(OrderExpr::asc).collect();
        order.push(OrderExpr::desc(Expr::col(alias, ontology::VERSION_COLUMN)));
        (order, Some((1, keys)))
    }
    pub(super) fn execute(&self, plan: &ExecutionPlan) -> EmitOutput {
        let source = self.emit_source(&plan.source);
        EmitOutput {
            from: source.from,
            where_parts: source.predicates,
            select: projections(&plan.outputs),
            ctes: plan
                .definitions
                .iter()
                .map(|(name, keys)| Cte::new(name, self.query(keys)))
                .collect(),
            nodes: plan
                .bindings
                .iter()
                .map(|binding| {
                    (
                        binding.node.clone(),
                        NodeBinding::source(
                            &binding.alias,
                            &binding.column,
                            binding.joined.then(|| binding.node.clone()),
                        ),
                    )
                })
                .collect(),
        }
    }

    pub(super) fn query(&self, plan: &PhysicalPlan) -> Query {
        let (source, order_by, limit_by, condition) = match &plan.source {
            PhysicalSource::Latest {
                alias,
                sort_key,
                input,
                aggregate_condition,
            } => {
                let (order_by, limit_by) = self.latest_row_dedup(alias, sort_key);
                (
                    input.as_ref(),
                    order_by,
                    limit_by,
                    aggregate_condition.as_slice(),
                )
            }
            source => (source, vec![], None, [].as_slice()),
        };
        let mut output = self.emit_source(source);
        output.predicates.extend(condition.iter().map(predicate));
        Query {
            select: if plan.outputs.is_empty() {
                vec![SelectExpr::star()]
            } else {
                projections(&plan.outputs)
            },
            from: output.from,
            where_clause: Expr::conjoin(output.predicates),
            order_by,
            limit_by,
            ..Default::default()
        }
    }

    fn emit_source(&self, plan: &PhysicalSource) -> SourceOutput {
        match plan {
            PhysicalSource::Union {
                alias,
                arms,
                relationship,
            } => {
                let queries = arms.iter().map(|arm| self.query(arm)).collect();
                SourceOutput {
                    from: TableRef::union_all(queries, alias).with_relationship(*relationship),
                    predicates: vec![],
                }
            }
            PhysicalSource::Scan {
                relation,
                alias,
                final_,
                relationship,
            } => SourceOutput {
                from: TableRef::Scan {
                    table: {
                        let RelationSource::Scan(table) = self
                            .bindings
                            .source(*relation)
                            .expect("planned scan binding")
                        else {
                            unreachable!("scan source must reference a stored table")
                        };
                        self.storage.table(*table).name().to_owned()
                    },
                    alias: alias.clone(),
                    final_: *final_,
                    relationship: *relationship,
                },
                predicates: vec![],
            },
            PhysicalSource::Filter { predicates, input } => {
                let mut output = self.emit_source(input);
                output.predicates.extend(predicates.iter().map(predicate));
                output
            }
            PhysicalSource::KeyFilter { value, keys, input } => {
                let mut output = self.emit_source(input);
                output.predicates.push(Expr::InSelect {
                    expr: Box::new(column(value)),
                    query: Box::new(self.query(keys)),
                });
                output
            }
            PhysicalSource::Scope { alias, input } => SourceOutput {
                from: TableRef::subquery(self.query(input), alias),
                predicates: vec![],
            },
            PhysicalSource::Latest {
                alias,
                input,
                sort_key,
                aggregate_condition,
            } => {
                let mut output = self.emit_source(input);
                output
                    .predicates
                    .extend(aggregate_condition.iter().map(predicate));
                let (order_by, limit_by) = self.latest_row_dedup(alias, sort_key);
                output.from = TableRef::subquery(
                    Query {
                        select: vec![SelectExpr::star()],
                        from: output.from,
                        where_clause: Expr::conjoin(std::mem::take(&mut output.predicates)),
                        order_by,
                        limit_by,
                        ..Default::default()
                    },
                    alias,
                );
                output
            }
            PhysicalSource::Join {
                endpoints,
                predicates,
                left,
                right,
            } => {
                let mut left = self.emit_source(left);
                let right = self.emit_source(right);
                let condition = predicates.iter().map(predicate).fold(
                    Expr::eq(column(&endpoints.0), column(&endpoints.1)),
                    Expr::and,
                );
                left.from = TableRef::join(
                    crate::ast::JoinType::Inner,
                    left.from,
                    right.from,
                    condition,
                );
                left.predicates.extend(right.predicates);
                left
            }
        }
    }
}

struct SourceOutput {
    from: TableRef,
    predicates: Vec<Expr>,
}
