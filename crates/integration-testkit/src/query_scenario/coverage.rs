use std::collections::BTreeSet;

use query_engine::compiler::ast::{Expr, Function, Node, Query, SelectExpr, SqlType, TableRef};
use query_engine::compiler::input::{AggFunction, TruncateUnit};
use query_engine::compiler::passes::codegen::duckdb;
use strum::IntoEnumIterator;

#[derive(Default)]
pub(super) struct Coverage(BTreeSet<String>);

impl Coverage {
    pub(super) fn record(&mut self, ast: &Node) {
        if let Node::Query(query) = ast {
            self.query(query);
        }
    }

    pub(super) fn unreached(&self) -> Vec<String> {
        renderable()
            .into_iter()
            .filter(|construct| !self.0.contains(construct))
            .collect()
    }

    fn insert(&mut self, construct: String) {
        self.0.insert(construct);
    }

    fn query(&mut self, query: &Query) {
        for cte in &query.ctes {
            self.query(&cte.query);
        }
        if query.distinct {
            self.insert("DISTINCT".into());
        }
        if !query.union_all.is_empty() {
            self.insert("UNION ALL".into());
        }
        for select in &query.select {
            self.expr(&select.expr);
        }
        self.table(&query.from);
        for expr in query
            .where_clause
            .iter()
            .chain(&query.group_by)
            .chain(&query.having)
            .chain(query.order_by.iter().map(|order| &order.expr))
            .chain(query.limit_by.iter().flat_map(|(_, keys)| keys))
        {
            self.expr(expr);
        }
        for arm in &query.union_all {
            self.query(arm);
        }
    }

    fn table(&mut self, table: &TableRef) {
        match table {
            TableRef::Scan { .. } | TableRef::Cte { .. } => {}
            TableRef::Join {
                left, right, on, ..
            } => {
                self.table(left);
                self.table(right);
                self.expr(on);
            }
            TableRef::Union { queries, .. } => {
                for query in queries {
                    self.query(query);
                }
            }
            TableRef::Subquery { query, .. } => self.query(query),
        }
    }

    fn expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Column { .. } | Expr::Identifier(_) | Expr::Literal(_) | Expr::Star => {}
            Expr::Param { data_type, .. } => {
                if matches!(data_type, SqlType::Timestamp { .. }) {
                    self.insert("timestamp parameter".into());
                }
            }
            Expr::FuncCall { name, args } => {
                self.insert(format!("function {name}"));
                for arg in args {
                    self.expr(arg);
                }
            }
            Expr::EmptyTupleArray(_) => self.insert("empty tuple array".into()),
            Expr::Aggregate {
                function,
                argument,
                distinct,
                condition,
            } => {
                self.insert(format!("aggregate {function}"));
                if *distinct {
                    self.insert("aggregate DISTINCT".into());
                }
                if condition.is_some() {
                    self.insert("aggregate FILTER".into());
                }
                for value in argument.iter().chain(condition) {
                    self.expr(value);
                }
            }
            Expr::TimeBucket { unit, value } => {
                self.insert(format!("time bucket {}", unit.name()));
                self.expr(value);
            }
            Expr::TextSearch { value, query, .. } => {
                self.insert("text search".into());
                self.expr(value);
                self.expr(query);
            }
            Expr::Lambda { body, .. } => {
                self.insert("lambda".into());
                self.expr(body);
            }
            Expr::BinaryOp { left, right, .. } => {
                self.expr(left);
                self.expr(right);
            }
            Expr::UnaryOp { expr, .. } | Expr::InSubquery { expr, .. } => self.expr(expr),
            Expr::InSelect { expr, query } => {
                self.insert("IN subquery".into());
                self.expr(expr);
                self.query(query);
            }
            Expr::Scalar(query) => {
                self.insert("scalar subquery".into());
                self.query(query);
            }
        }
    }
}

fn renderable() -> Vec<String> {
    let renders = |expr: Expr| {
        let ast = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(expr, "value")],
            from: TableRef::scan("source", "s"),
            ..Default::default()
        }));
        duckdb::codegen(&ast, Default::default()).is_ok()
    };
    let functions = Function::iter().filter(|function| {
        let arity = (1..=3)
            .find(|count| function.accepts_arity(*count))
            .unwrap();
        renders(Expr::func(*function, vec![Expr::int(1); arity]))
    });
    let aggregates = AggFunction::iter().filter(|function| {
        renders(Expr::Aggregate {
            function: *function,
            argument: Some(Box::new(Expr::int(1))),
            distinct: false,
            condition: None,
        })
    });
    functions
        .map(|function| format!("function {function}"))
        .chain(aggregates.map(|function| format!("aggregate {function}")))
        .chain(TruncateUnit::iter().map(|unit| format!("time bucket {}", unit.name())))
        .chain(
            [
                "UNION ALL",
                "aggregate DISTINCT",
                "aggregate FILTER",
                "timestamp parameter",
                "empty tuple array",
                "lambda",
            ]
            .map(String::from),
        )
        .collect()
}
