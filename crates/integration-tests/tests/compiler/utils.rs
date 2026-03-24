//! SQL assertion utilities backed by sqlparser.
//!
//! Parses compiled SQL with the ClickHouse dialect and provides
//! structured accessors so tests validate the AST, not string fragments.

use sqlparser::ast::{
    Cte, Expr, GroupByExpr, LimitClause, OrderByKind, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, With,
};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;

/// Parsed SQL wrapper for structured assertions.
pub struct ParsedSql {
    pub statements: Vec<Statement>,
    pub raw: String,
}

impl ParsedSql {
    /// Parse SQL using ClickHouse dialect. Panics on parse failure.
    pub fn parse(sql: &str) -> Self {
        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .unwrap_or_else(|e| panic!("failed to parse SQL:\n{sql}\n\nerror: {e}"));
        Self {
            statements,
            raw: sql.to_string(),
        }
    }

    /// Get the first (and usually only) statement.
    pub fn statement(&self) -> &Statement {
        assert!(
            !self.statements.is_empty(),
            "expected at least one statement"
        );
        &self.statements[0]
    }

    /// Get the top-level Query (unwrapping Statement::Query).
    pub fn query(&self) -> &Query {
        match self.statement() {
            Statement::Query(q) => q,
            other => panic!("expected Query, got: {other:?}"),
        }
    }

    /// Get the outermost SELECT body (descends through SetExpr::Select).
    pub fn select(&self) -> &Select {
        extract_select(self.query())
    }

    /// True if any CTE name matches.
    pub fn has_cte(&self, name: &str) -> bool {
        self.ctes().iter().any(|c| c.alias.name.value == name)
    }

    /// All CTEs from the WITH clause.
    pub fn ctes(&self) -> Vec<&Cte> {
        match &self.query().with {
            Some(With { cte_tables, .. }) => cte_tables.iter().collect(),
            None => vec![],
        }
    }

    /// Fallback for things sqlparser doesn't model well.
    pub fn raw_contains(&self, s: &str) -> bool {
        self.raw.contains(s)
    }

    /// Assert the SQL parses and return self for chaining.
    pub fn assert_valid(self) -> Self {
        self
    }
}

fn extract_select(query: &Query) -> &Select {
    match query.body.as_ref() {
        SetExpr::Select(s) => s,
        SetExpr::SetOperation { .. } => {
            panic!("top-level body is a set operation (UNION), not a plain SELECT")
        }
        SetExpr::Query(q) => extract_select(q),
        other => panic!("unexpected SetExpr variant: {other:?}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Select-level helpers
// ─────────────────────────────────────────────────────────────────────────────

impl ParsedSql {
    /// All projected column aliases/expressions as strings.
    pub fn select_columns(&self) -> Vec<String> {
        self.select()
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(e) => e.to_string(),
                SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                SelectItem::QualifiedWildcard(name, _) => format!("{name}.*"),
                SelectItem::Wildcard(_) => "*".to_string(),
            })
            .collect()
    }

    /// True if any SELECT column alias or expression contains the substring.
    pub fn has_select_column(&self, needle: &str) -> bool {
        self.select()
            .projection
            .iter()
            .any(|item| item.to_string().contains(needle))
    }

    /// True if SELECT has no columns matching the substring.
    pub fn lacks_select_column(&self, needle: &str) -> bool {
        !self.has_select_column(needle)
    }

    /// True if there's a LIMIT clause.
    pub fn has_limit(&self) -> bool {
        self.query().limit_clause.is_some()
    }

    /// Get the LIMIT value as u64.
    pub fn limit_value(&self) -> Option<u64> {
        match &self.query().limit_clause {
            Some(LimitClause::LimitOffset { limit, .. }) => limit.as_ref().and_then(|e| match e {
                Expr::Value(v) => v.to_string().parse().ok(),
                _ => None,
            }),
            Some(LimitClause::OffsetCommaLimit { limit, .. }) => match limit {
                Expr::Value(v) => v.to_string().parse().ok(),
                _ => None,
            },
            None => None,
        }
    }

    /// Get the OFFSET value as u64.
    pub fn offset_value(&self) -> Option<u64> {
        match &self.query().limit_clause {
            Some(LimitClause::LimitOffset { offset, .. }) => {
                offset.as_ref().and_then(|o| match &o.value {
                    Expr::Value(v) => v.to_string().parse().ok(),
                    _ => None,
                })
            }
            Some(LimitClause::OffsetCommaLimit { offset, .. }) => match offset {
                Expr::Value(v) => v.to_string().parse().ok(),
                _ => None,
            },
            None => None,
        }
    }

    /// True if there's an ORDER BY clause.
    pub fn has_order_by(&self) -> bool {
        match &self.query().order_by {
            Some(ob) => match &ob.kind {
                OrderByKind::Expressions(exprs) => !exprs.is_empty(),
                OrderByKind::All(_) => true,
            },
            None => false,
        }
    }

    /// True if there's a GROUP BY clause.
    pub fn has_group_by(&self) -> bool {
        matches!(&self.select().group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty())
    }

    /// True if there's a WHERE clause.
    pub fn has_where(&self) -> bool {
        self.select().selection.is_some()
    }

    /// The WHERE clause as a string.
    pub fn where_str(&self) -> Option<String> {
        self.select().selection.as_ref().map(|e| e.to_string())
    }

    /// True if any FROM table reference contains the name.
    pub fn has_table(&self, name: &str) -> bool {
        self.select().from.iter().any(|t| table_contains(t, name))
    }

    /// True if there's any JOIN in the FROM clause.
    pub fn has_join(&self) -> bool {
        self.select().from.iter().any(|t| !t.joins.is_empty())
    }

    /// True if there's no JOIN in the FROM clause.
    pub fn lacks_join(&self) -> bool {
        !self.has_join()
    }

    /// True if the query body is a UNION ALL.
    pub fn is_union_all(&self) -> bool {
        matches!(
            self.query().body.as_ref(),
            SetExpr::SetOperation {
                op: sqlparser::ast::SetOperator::Union,
                ..
            }
        )
    }

    /// Collect all table names referenced anywhere in FROM/JOIN.
    pub fn all_tables(&self) -> Vec<String> {
        let mut tables = Vec::new();
        for from in &self.select().from {
            collect_table_names(&from.relation, &mut tables);
            for join in &from.joins {
                collect_table_names(&join.relation, &mut tables);
            }
        }
        tables
    }
}

fn table_contains(twj: &TableWithJoins, name: &str) -> bool {
    table_factor_contains(&twj.relation, name)
        || twj
            .joins
            .iter()
            .any(|j| table_factor_contains(&j.relation, name))
}

fn table_factor_contains(tf: &TableFactor, name: &str) -> bool {
    match tf {
        TableFactor::Table { name: n, .. } => n.to_string().contains(name),
        TableFactor::Derived { alias, .. } => {
            alias.as_ref().is_some_and(|a| a.name.value.contains(name))
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_contains(table_with_joins, name),
        _ => false,
    }
}

fn collect_table_names(tf: &TableFactor, out: &mut Vec<String>) {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            out.push(name.to_string());
            if let Some(a) = alias {
                out.push(a.name.value.clone());
            }
        }
        TableFactor::Derived { alias, .. } => {
            if let Some(a) = alias {
                out.push(a.name.value.clone());
            }
        }
        _ => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Param helpers
// ─────────────────────────────────────────────────────────────────────────────

use compiler::passes::codegen::ParamValue;
use std::collections::HashMap;

/// True if any param value matches the given JSON value.
pub fn has_param_value(params: &HashMap<String, ParamValue>, val: &serde_json::Value) -> bool {
    params.values().any(|p| &p.value == val)
}
