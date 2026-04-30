//! SQL-oriented Abstract Syntax Tree
//!
//! Intermediate representation between JSON input and SQL output.
//! Each node maps directly to ClickHouse SQL constructs.

use std::collections::HashSet;
use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value;

pub use gkg_utils::clickhouse::{ChScalar, ChType};

// ─────────────────────────────────────────────────────────────────────────────
// Expressions
// ─────────────────────────────────────────────────────────────────────────────

/// Expression that produces a value in SQL.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference → `table.column`
    Column { table: String, column: String },
    /// Bare SQL identifier, used for lambda parameters.
    Identifier(String),
    /// Constant value → parameterized as `{pN:Type}`, type inferred from Value.
    Literal(Value),
    /// Constant value with explicit ClickHouse type → `{pN:Type}`.
    Param { data_type: ChType, value: Value },
    /// Function call → `NAME(arg1, arg2, ...)`
    /// Used for aggregates (COUNT, SUM) and ClickHouse functions (arrayConcat, has).
    FuncCall { name: String, args: Vec<Expr> },
    /// Lambda expression → `param -> body`.
    Lambda { param: String, body: Box<Expr> },
    /// Binary operation → `(left OP right)`
    /// Examples: `x = y`, `a AND b`, `col IN (1, 2, 3)`
    BinaryOp {
        op: Op,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary operation → `(OP expr)` or `(expr OP)` for postfix ops
    /// Prefix: `NOT active` → `(NOT t.active)`
    /// Postfix: `IS NULL` → `(t.deleted_at IS NULL)`
    UnaryOp { op: Op, expr: Box<Expr> },
    /// Subquery IN check → `expr IN (SELECT column FROM cte_name)`
    /// Used for SIP pre-filtering: materialize IDs in a CTE, then filter
    /// multiple tables against the same set.
    InSubquery {
        expr: Box<Expr>,
        cte_name: String,
        column: String,
    },
    /// Wildcard → `*`
    Star,
}

/// SQL operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum Op {
    // Comparison
    #[strum(serialize = "=")]
    Eq,
    #[strum(serialize = "!=")]
    Ne,
    #[strum(serialize = "<")]
    Lt,
    #[strum(serialize = "<=")]
    Le,
    #[strum(serialize = ">")]
    Gt,
    #[strum(serialize = ">=")]
    Ge,
    #[strum(serialize = "IN")]
    In,
    #[strum(serialize = "LIKE")]
    Like,
    // Logical
    #[strum(serialize = "AND")]
    And,
    #[strum(serialize = "OR")]
    Or,
    #[strum(serialize = "NOT")]
    Not,
    // Null checks
    #[strum(serialize = "IS NULL")]
    IsNull,
    #[strum(serialize = "IS NOT NULL")]
    IsNotNull,
    // Arithmetic
    #[strum(serialize = "+")]
    Add,
}

// ─────────────────────────────────────────────────────────────────────────────
// Table references
// ─────────────────────────────────────────────────────────────────────────────

/// Source of rows in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    /// Read from a physical table → `table AS alias [FINAL]`
    Scan {
        table: String,
        alias: String,
        final_: bool,
    },
    /// Combine two sources → `left JOIN_TYPE JOIN right ON condition`
    Join {
        join_type: JoinType,
        left: Box<TableRef>,
        right: Box<TableRef>,
        on: Expr,
    },
    /// Union of queries as a derived table → `(SELECT ... UNION ALL SELECT ...) AS alias`
    /// Used for multi-hop traversals with unrolled joins.
    Union { queries: Vec<Query>, alias: String },
    /// Derived table from a subquery → `(SELECT ...) AS alias`
    /// Used internally for deduplication of aggregation queries.
    Subquery { query: Box<Query>, alias: String },
}

/// SQL JOIN types.
/// - Inner: only matching rows from both sides
/// - Left: all rows from left, matching from right (NULLs if no match)
/// - Right: all rows from right, matching from left
/// - Full: all rows from both sides
/// - Cross: cartesian product, no ON condition
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

// ─────────────────────────────────────────────────────────────────────────────
// Query structures
// ─────────────────────────────────────────────────────────────────────────────

/// Expression in SELECT clause → `expr AS alias` or just `expr`
#[derive(Debug, Clone, PartialEq)]
pub struct SelectExpr {
    pub expr: Expr,
    pub alias: Option<String>,
}

impl SelectExpr {
    pub fn new(expr: Expr, alias: impl Into<String>) -> Self {
        Self {
            expr,
            alias: Some(alias.into()),
        }
    }

    pub fn star() -> Self {
        Self {
            expr: Expr::Star,
            alias: None,
        }
    }
}

/// Ordering specification → `expr ASC` or `expr DESC`
#[derive(Debug, Clone, PartialEq)]
pub struct OrderExpr {
    pub expr: Expr,
    pub desc: bool,
}

/// Named Common Table Expression (CTE) for WITH clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: String,
    pub query: Box<Query>,
    pub recursive: bool,
    /// When true, emit `name AS MATERIALIZED (...)` so ClickHouse evaluates
    /// the CTE body once and caches the result. Without this, ClickHouse
    /// inlines non-recursive CTEs at every reference site, re-executing the
    /// scan for each `IN (SELECT ... FROM cte)`.
    pub materialized: bool,
}

impl Cte {
    pub fn new(name: impl Into<String>, query: Query) -> Self {
        Self {
            name: name.into(),
            query: Box::new(query),
            recursive: false,
            materialized: false,
        }
    }
}

/// Complete SQL query:
/// ```sql
/// WITH cte1 AS (...), cte2 AS (...)
/// SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY ... LIMIT ...
/// UNION ALL SELECT ...
/// SETTINGS key = value
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub ctes: Vec<Cte>,
    /// When true, emit `SELECT DISTINCT` instead of `SELECT`.
    pub distinct: bool,
    pub select: Vec<SelectExpr>,
    pub from: TableRef,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderExpr>,
    /// `LIMIT n BY col1, col2` — ClickHouse per-group limit (applied after ORDER BY).
    pub limit_by: Option<(u32, Vec<Expr>)>,
    pub limit: Option<u32>,
    /// Additional queries to UNION ALL with this one (for recursive CTEs).
    pub union_all: Vec<Query>,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            ctes: vec![],
            distinct: false,
            select: vec![],
            from: TableRef::Scan {
                table: String::new(),
                alias: String::new(),
                final_: false,
            },
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit_by: None,
            limit: None,
            union_all: vec![],
        }
    }
}

/// SQL identifier pattern: ASCII letter or underscore, then alphanumerics/underscores.
static SAFE_IDENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").expect("valid regex"));

/// `INSERT INTO table (cols) VALUES (row1), (row2), ...`
///
/// Table and column names are interpolated as raw identifiers (not parameterized),
/// so they are validated at construction time via [`Insert::new`]. Fields are
/// private to enforce this — use the constructor.
#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    table: String,
    columns: Vec<String>,
    values: Vec<Vec<Expr>>,
}

impl Insert {
    pub fn new(table: impl Into<String>, columns: Vec<String>, values: Vec<Vec<Expr>>) -> Self {
        let table = table.into();
        debug_assert!(
            SAFE_IDENT.is_match(&table),
            "INSERT table name is not a safe identifier: {table:?}"
        );
        for col in &columns {
            debug_assert!(
                SAFE_IDENT.is_match(col),
                "INSERT column name is not a safe identifier: {col:?}"
            );
        }
        Self {
            table,
            columns,
            values,
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn values(&self) -> &[Vec<Expr>] {
        &self.values
    }
}

/// Top-level AST node.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Query(Box<Query>),
    Insert(Box<Insert>),
}

// ─────────────────────────────────────────────────────────────────────────────
// Builder helpers
// ─────────────────────────────────────────────────────────────────────────────

impl Expr {
    pub fn col(table: impl Into<String>, column: impl Into<String>) -> Self {
        Expr::Column {
            table: table.into(),
            column: column.into(),
        }
    }

    pub fn ident(name: impl Into<String>) -> Self {
        Expr::Identifier(name.into())
    }

    pub fn lit(value: impl Into<Value>) -> Self {
        Expr::Literal(value.into())
    }

    pub fn param(data_type: ChType, value: impl Into<Value>) -> Self {
        Expr::Param {
            data_type,
            value: value.into(),
        }
    }

    pub fn string(value: impl Into<String>) -> Self {
        Expr::Param {
            data_type: ChType::String,
            value: Value::String(value.into()),
        }
    }

    pub fn int(value: i64) -> Self {
        Expr::Param {
            data_type: ChType::Int64,
            value: Value::Number(value.into()),
        }
    }

    pub fn uint32(value: u32) -> Self {
        Expr::Param {
            data_type: ChType::UInt32,
            value: Value::Number(value.into()),
        }
    }

    pub fn func(name: impl Into<String>, args: Vec<Expr>) -> Self {
        Expr::FuncCall {
            name: name.into(),
            args,
        }
    }

    pub fn lambda(param: impl Into<String>, body: Expr) -> Self {
        Expr::Lambda {
            param: param.into(),
            body: Box::new(body),
        }
    }

    pub fn eq(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            op: Op::Eq,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn binary(op: Op, left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn unary(op: Op, expr: Expr) -> Self {
        Expr::UnaryOp {
            op,
            expr: Box::new(expr),
        }
    }

    /// Combine expressions with AND, ignoring None values.
    pub fn and_all(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
        exprs
            .into_iter()
            .flatten()
            .reduce(|a, b| Expr::binary(Op::And, a, b))
    }

    /// Combine expressions with OR, ignoring None values.
    pub fn or_all(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
        exprs
            .into_iter()
            .flatten()
            .reduce(|a, b| Expr::binary(Op::Or, a, b))
    }

    /// Match a column against a set of values.
    /// 0 values → None, 1 value → Eq, N values → IN.
    pub fn col_in(
        table: impl Into<String>,
        column: impl Into<String>,
        data_type: ChType,
        values: Vec<Value>,
    ) -> Option<Self> {
        match values.len() {
            0 => None,
            1 => Some(Expr::eq(
                Expr::col(table, column),
                Expr::Param {
                    data_type,
                    value: values.into_iter().next().unwrap(),
                },
            )),
            _ => Some(Expr::binary(
                Op::In,
                Expr::col(table, column),
                Expr::Param {
                    data_type: data_type.to_array(),
                    value: Value::Array(values),
                },
            )),
        }
    }

    /// Combine two expressions with AND.
    pub fn and(left: Expr, right: Expr) -> Expr {
        Expr::binary(Op::And, left, right)
    }

    /// Flatten an AND tree into a list of conjuncts.
    /// `(A AND B) AND C` becomes `[A, B, C]`.
    pub fn flatten_and(self) -> Vec<Expr> {
        match self {
            Expr::BinaryOp {
                op: Op::And,
                left,
                right,
            } => {
                let mut out = left.flatten_and();
                out.extend(right.flatten_and());
                out
            }
            other => vec![other],
        }
    }

    /// Rebuild an AND chain from conjuncts. Returns None if empty.
    pub fn conjoin(exprs: Vec<Expr>) -> Option<Expr> {
        exprs.into_iter().reduce(Expr::and)
    }

    /// Combine two expressions with OR.
    pub fn or(left: Expr, right: Expr) -> Expr {
        Expr::binary(Op::Or, left, right)
    }

    /// Collect all unique table aliases referenced by column expressions.
    pub fn column_aliases(&self) -> HashSet<String> {
        let mut aliases = HashSet::new();
        self.collect_aliases(&mut aliases);
        aliases
    }

    fn collect_aliases(&self, out: &mut HashSet<String>) {
        match self {
            Expr::Column { table, .. } => {
                out.insert(table.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                left.collect_aliases(out);
                right.collect_aliases(out);
            }
            Expr::FuncCall { args, .. } => {
                for a in args {
                    a.collect_aliases(out);
                }
            }
            Expr::Lambda { body, .. } => body.collect_aliases(out),
            Expr::UnaryOp { expr, .. } => expr.collect_aliases(out),
            Expr::InSubquery { expr, .. } => expr.collect_aliases(out),
            Expr::Identifier(_) | Expr::Literal(_) | Expr::Param { .. } | Expr::Star => {}
        }
    }

    /// Returns true if this expression tree contains an `InSubquery` node.
    pub fn contains_in_subquery(&self) -> bool {
        match self {
            Expr::InSubquery { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                left.contains_in_subquery() || right.contains_in_subquery()
            }
            Expr::FuncCall { args, .. } => args.iter().any(|a| a.contains_in_subquery()),
            Expr::Lambda { body, .. } => body.contains_in_subquery(),
            Expr::UnaryOp { expr, .. } => expr.contains_in_subquery(),
            _ => false,
        }
    }

    /// Collect all column names (not aliases) referenced by this expression.
    pub fn referenced_columns(&self) -> HashSet<String> {
        let mut cols = HashSet::new();
        self.collect_columns(&mut cols);
        cols
    }

    fn collect_columns(&self, out: &mut HashSet<String>) {
        match self {
            Expr::Column { column, .. } => {
                out.insert(column.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                left.collect_columns(out);
                right.collect_columns(out);
            }
            Expr::FuncCall { args, .. } => {
                for a in args {
                    a.collect_columns(out);
                }
            }
            Expr::Lambda { body, .. } => body.collect_columns(out),
            Expr::UnaryOp { expr, .. } => expr.collect_columns(out),
            Expr::InSubquery { expr, .. } => expr.collect_columns(out),
            Expr::Identifier(_) | Expr::Literal(_) | Expr::Param { .. } | Expr::Star => {}
        }
    }

    /// Check if this expression only references columns from `alias`
    /// (or is a constant/literal).
    pub fn references_only(&self, alias: &str) -> bool {
        match self {
            Expr::Column { table, .. } => table == alias,
            Expr::Identifier(_) | Expr::Literal(_) | Expr::Param { .. } | Expr::Star => true,
            Expr::FuncCall { args, .. } => args.iter().all(|a| a.references_only(alias)),
            Expr::Lambda { body, .. } => body.references_only(alias),
            Expr::BinaryOp { left, right, .. } => {
                left.references_only(alias) && right.references_only(alias)
            }
            Expr::UnaryOp { expr, .. } => expr.references_only(alias),
            Expr::InSubquery { expr, .. } => expr.references_only(alias),
        }
    }
}

impl TableRef {
    pub fn scan(table: impl Into<String>, alias: impl Into<String>) -> Self {
        TableRef::Scan {
            table: table.into(),
            alias: alias.into(),
            final_: false,
        }
    }

    pub fn scan_final(table: impl Into<String>, alias: impl Into<String>) -> Self {
        TableRef::Scan {
            table: table.into(),
            alias: alias.into(),
            final_: true,
        }
    }

    pub fn join(join_type: JoinType, left: TableRef, right: TableRef, on: Expr) -> Self {
        TableRef::Join {
            join_type,
            left: Box::new(left),
            right: Box::new(right),
            on,
        }
    }

    pub fn union_all(queries: Vec<Query>, alias: impl Into<String>) -> Self {
        TableRef::Union {
            queries,
            alias: alias.into(),
        }
    }

    pub fn subquery(query: Query, alias: impl Into<String>) -> Self {
        TableRef::Subquery {
            query: Box::new(query),
            alias: alias.into(),
        }
    }
}
