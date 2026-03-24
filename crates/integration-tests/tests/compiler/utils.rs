//! SQL assertion utilities backed by sqlparser's Visitor.
//!
//! Parses compiled SQL with the ClickHouse dialect, then walks the
//! entire AST (CTEs, subqueries, unions) to collect function names,
//! column references, table names, aliases, and operators. Tests
//! assert against these collected sets instead of raw string matching.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, Function, GroupByExpr, LimitClause, ObjectName, OrderByKind, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, Visit, Visitor,
};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;

use compiler::passes::codegen::{ParamValue, ParameterizedQuery};

// ─────────────────────────────────────────────────────────────────────────────
// Collector — walks the full AST via Visitor
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Collector {
    functions: HashSet<String>,
    columns: HashSet<String>,
    tables: HashSet<String>,
    aliases: HashSet<String>,
    cte_names: HashSet<String>,
    operators: HashSet<String>,
    has_union_all: bool,
}

impl Visitor for Collector {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Function(Function { name, .. }) => {
                self.functions.insert(name.to_string().to_uppercase());
            }
            Expr::Identifier(ident) => {
                self.columns.insert(ident.value.clone());
            }
            Expr::CompoundIdentifier(parts) => {
                let full = parts
                    .iter()
                    .map(|p| p.value.as_str())
                    .collect::<Vec<_>>()
                    .join(".");
                self.columns.insert(full);
                if let Some(last) = parts.last() {
                    self.columns.insert(last.value.clone());
                }
            }
            Expr::BinaryOp { op, .. } => {
                self.operators.insert(op.to_string());
            }
            Expr::Like { .. } => {
                self.operators.insert("LIKE".to_string());
            }
            Expr::InList { .. } => {
                self.operators.insert("IN".to_string());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
        self.tables.insert(relation.to_string());
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<()> {
        match table_factor {
            TableFactor::Table { alias: Some(a), .. }
            | TableFactor::Derived { alias: Some(a), .. } => {
                self.aliases.insert(a.name.value.clone());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.cte_names.insert(cte.alias.name.value.clone());
            }
        }
        if matches!(
            query.body.as_ref(),
            SetExpr::SetOperation {
                op: sqlparser::ast::SetOperator::Union,
                set_quantifier: sqlparser::ast::SetQuantifier::All,
                ..
            }
        ) {
            self.has_union_all = true;
        }
        ControlFlow::Continue(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ParsedSql — the public API
// ─────────────────────────────────────────────────────────────────────────────

pub struct ParsedSql {
    pub statements: Vec<Statement>,
    pub raw: String,
    collected: Collector,
}

impl ParsedSql {
    /// Parse a `ParameterizedQuery` by rendering (inlining params) first.
    /// This is the preferred entry point — parameterized placeholders like
    /// `{p0:String}` aren't valid SQL and can't be parsed directly.
    pub fn from_query(query: &ParameterizedQuery) -> Self {
        Self::parse(&query.render())
    }

    /// Parse raw SQL using ClickHouse dialect. Panics on parse failure.
    /// Prefer `from_query` for compiler output.
    pub fn parse(sql: &str) -> Self {
        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .unwrap_or_else(|e| panic!("failed to parse SQL:\n{sql}\n\nerror: {e}"));

        let mut collected = Collector::default();
        for stmt in &statements {
            let _ = stmt.visit(&mut collected);
        }

        // Also collect SELECT aliases (ExprWithAlias) which the visitor
        // doesn't traverse as relations/identifiers.
        for stmt in &statements {
            if let Statement::Query(q) = stmt {
                collect_select_aliases(q, &mut collected.aliases);
            }
        }

        Self {
            statements,
            raw: sql.to_string(),
            collected,
        }
    }

    // ── Structural queries ───────────────────────────────────────────────

    pub fn query(&self) -> &Query {
        assert!(!self.statements.is_empty(), "parsed SQL has no statements");
        match &self.statements[0] {
            Statement::Query(q) => q,
            other => panic!("expected Query, got: {other:?}"),
        }
    }

    pub fn select(&self) -> &Select {
        extract_select(self.query())
    }

    // ── Visitor-based lookups ────────────────────────────────────────────

    /// True if any function call in the AST matches (case-insensitive).
    pub fn has_function(&self, name: &str) -> bool {
        self.collected.functions.contains(&name.to_uppercase())
    }

    /// True if any column reference contains the substring.
    pub fn has_column_ref(&self, needle: &str) -> bool {
        self.collected.columns.iter().any(|c| c.contains(needle))
    }

    /// True if no column reference contains the substring.
    pub fn lacks_column_ref(&self, needle: &str) -> bool {
        !self.has_column_ref(needle)
    }

    /// True if any table name matches.
    pub fn has_table(&self, name: &str) -> bool {
        self.collected.tables.iter().any(|t| t.contains(name))
    }

    /// True if any alias matches.
    pub fn has_alias(&self, name: &str) -> bool {
        self.collected.aliases.contains(name)
    }

    /// True if a CTE with this name exists.
    pub fn has_cte(&self, name: &str) -> bool {
        self.collected.cte_names.contains(name)
    }

    /// True if any binary/unary operator matches.
    pub fn has_operator(&self, op: &str) -> bool {
        self.collected.operators.contains(op)
    }

    /// True if a UNION ALL appears anywhere in the query tree.
    pub fn has_union_all(&self) -> bool {
        self.collected.has_union_all
    }

    // ── SELECT-level helpers ─────────────────────────────────────────────

    /// True if any SELECT item (alias or expression) contains the substring.
    pub fn has_select_column(&self, needle: &str) -> bool {
        self.select()
            .projection
            .iter()
            .any(|item| item.to_string().contains(needle))
    }

    pub fn lacks_select_column(&self, needle: &str) -> bool {
        !self.has_select_column(needle)
    }

    pub fn has_where(&self) -> bool {
        self.select().selection.is_some()
    }

    pub fn where_str(&self) -> Option<String> {
        self.select().selection.as_ref().map(|e| e.to_string())
    }

    pub fn has_group_by(&self) -> bool {
        matches!(&self.select().group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty())
    }

    pub fn has_order_by(&self) -> bool {
        match &self.query().order_by {
            Some(ob) => match &ob.kind {
                OrderByKind::Expressions(exprs) => !exprs.is_empty(),
                OrderByKind::All(_) => true,
            },
            None => false,
        }
    }

    pub fn has_join(&self) -> bool {
        self.select().from.iter().any(|t| !t.joins.is_empty())
    }

    pub fn lacks_join(&self) -> bool {
        !self.has_join()
    }

    pub fn has_limit(&self) -> bool {
        self.query().limit_clause.is_some()
    }

    pub fn limit_value(&self) -> Option<u64> {
        match &self.query().limit_clause {
            Some(LimitClause::LimitOffset { limit, .. }) => limit.as_ref().and_then(expr_to_u64),
            Some(LimitClause::OffsetCommaLimit { limit, .. }) => expr_to_u64(limit),
            None => None,
        }
    }

    pub fn offset_value(&self) -> Option<u64> {
        match &self.query().limit_clause {
            Some(LimitClause::LimitOffset { offset, .. }) => {
                offset.as_ref().and_then(|o| expr_to_u64(&o.value))
            }
            Some(LimitClause::OffsetCommaLimit { offset, .. }) => expr_to_u64(offset),
            None => None,
        }
    }

    /// Fallback for anything the visitor doesn't cover.
    pub fn raw_contains(&self, s: &str) -> bool {
        self.raw.contains(s)
    }
}

fn expr_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(v) => v.to_string().parse().ok(),
        _ => None,
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

fn collect_select_aliases(query: &Query, aliases: &mut HashSet<String>) {
    if let SetExpr::Select(s) = query.body.as_ref() {
        for item in &s.projection {
            if let SelectItem::ExprWithAlias { alias, .. } = item {
                aliases.insert(alias.value.clone());
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Param helpers
// ─────────────────────────────────────────────────────────────────────────────

/// True if any param value matches the given JSON value.
pub fn has_param_value(params: &HashMap<String, ParamValue>, val: &serde_json::Value) -> bool {
    params.values().any(|p| &p.value == val)
}
