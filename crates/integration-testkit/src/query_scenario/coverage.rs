use std::collections::BTreeSet;

use query_engine::compiler::ast::{
    Expr, Function, Node, Query, SelectExpr, SqlType, TableRef, visit,
};
use query_engine::compiler::input::{AggFunction, TruncateUnit};
use query_engine::compiler::passes::codegen::duckdb;
use strum::{IntoEnumIterator, VariantNames};

#[derive(Default)]
pub struct Coverage(BTreeSet<String>);

impl Coverage {
    pub(crate) fn record(&mut self, ast: &Node) {
        if let Node::Query(query) = ast {
            self.query(query);
        }
    }

    pub fn problems(&self, compiler_fixtures: &std::path::Path) -> Vec<String> {
        let mut fixtures = Coverage::default();
        crate::compiler::record_fixtures(
            crate::scenario::Backend::DuckDb,
            compiler_fixtures,
            &mut fixtures,
        );
        let renderable = renderable();
        let excluded = |construct: &str| EXCLUDED.iter().any(|(name, _)| *name == construct);
        let unreached = renderable
            .iter()
            .filter(|construct| !excluded(construct) && !self.0.contains(*construct))
            .map(|construct| format!("no scenario reaches {construct}"));
        let stale = EXCLUDED.iter().filter_map(|(construct, _)| {
            if !renderable.iter().any(|name| name == construct) {
                Some(format!(
                    "{construct} is excluded but DuckDB does not render it"
                ))
            } else if self.0.contains(*construct) {
                Some(format!("{construct} is excluded but a scenario reaches it"))
            } else if fixtures.0.contains(*construct) {
                Some(format!(
                    "{construct} is excluded but a DuckDB compiler fixture reaches it"
                ))
            } else {
                None
            }
        });
        unreached.chain(stale).collect()
    }

    fn query(&mut self, query: &Query) {
        let mut found = Vec::new();
        visit::visit_queries(query, &mut |query| {
            if query.distinct {
                found.push("DISTINCT".to_string());
            }
            if !query.union_all.is_empty() {
                found.push("UNION ALL".to_string());
            }
            for expression in visit::query_expressions(query) {
                visit::visit_expressions(expression, &mut |expression| {
                    found.extend(constructs(expression));
                    Ok(())
                })?;
            }
            Ok(())
        })
        .expect("the coverage walk does not fail");
        self.0.extend(found);
    }
}

fn constructs(expression: &Expr) -> Vec<String> {
    match expression {
        Expr::Param {
            data_type: SqlType::Timestamp { .. },
            ..
        } => vec!["timestamp parameter".into()],
        Expr::FuncCall { name, .. } => vec![format!("function {name}")],
        Expr::EmptyTupleArray(_) => vec!["empty tuple array".into()],
        Expr::Aggregate {
            function,
            distinct,
            condition,
            ..
        } => std::iter::once(format!("aggregate {function}"))
            .chain(distinct.then(|| "aggregate DISTINCT".to_string()))
            .chain(condition.as_ref().map(|_| "aggregate FILTER".to_string()))
            .collect(),
        Expr::TimeBucket { unit, .. } => vec![format!("time bucket {}", unit.name())],
        Expr::Lambda { .. } => vec!["lambda".into()],
        Expr::InSelect { .. } => vec!["IN subquery".into()],
        Expr::Scalar(_) => vec!["scalar subquery".into()],
        _ => vec![],
    }
}

const EXCLUDED: &[(&str, &str)] = &[
    ("function ToJson", "hydration-only"),
    ("function Object", "hydration-only"),
    ("function ArrayExists", "hydration-only"),
    ("function ByteLength", "response-policy-only"),
    ("function Substring", "response-policy-only"),
    ("function Concat", "response-policy-only"),
    ("function Coalesce", "tp_bounds-only"),
    ("function ArgMaxOrNull", "tp_bounds-only"),
    (
        "function If",
        "tp_bounds and response policy; neighbor paths need a variant scope, which DuckDB has none of",
    ),
    ("function ArrayMap", "security-check-only"),
    ("function ArrayContains", "empty denormalized catalog"),
    ("function ArrayContainsAny", "empty denormalized catalog"),
    ("function NullIf", "epoch-null off"),
    ("aggregate collect", "validator-rejected"),
    ("function CountSubstrings", "no lowering emits"),
    ("function ArrayResize", "no lowering emits"),
    ("function ArrayContainsAll", "no lowering emits"),
    ("function ArgMax", "no lowering emits"),
    ("DISTINCT", "no lowering emits"),
    ("scalar subquery", "tp_bounds and security check only"),
];

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
        .chain(
            TruncateUnit::VARIANTS
                .iter()
                .map(|unit| format!("time bucket {unit}")),
        )
        .chain(
            [
                "UNION ALL",
                "DISTINCT",
                "IN subquery",
                "scalar subquery",
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
