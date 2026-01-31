//! SQL-oriented Abstract Syntax Tree
//!
//! Intermediate representation between JSON input and SQL output.
//! Each node maps directly to ClickHouse SQL constructs.

use serde_json::Value;

// ─────────────────────────────────────────────────────────────────────────────
// Expressions
// ─────────────────────────────────────────────────────────────────────────────

/// Expression that produces a value in SQL.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference → `table.column`
    Column { table: String, column: String },
    /// Constant value → parameterized as `{p0:Type}`
    Literal(Value),
    /// Function call → `NAME(arg1, arg2, ...)`
    /// Used for aggregates (COUNT, SUM) and ClickHouse functions (arrayConcat, has).
    FuncCall { name: String, args: Vec<Expr> },
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
}

/// SQL operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    Like,
    ILike,
    // Logical
    And,
    Or,
    Not,
    // Null checks
    IsNull,
    IsNotNull,
    // Arithmetic
    Add,
}

impl Op {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "!=",
            Op::Lt => "<",
            Op::Le => "<=",
            Op::Gt => ">",
            Op::Ge => ">=",
            Op::In => "IN",
            Op::Like => "LIKE",
            Op::ILike => "ILIKE",
            Op::And => "AND",
            Op::Or => "OR",
            Op::Not => "NOT",
            Op::IsNull => "IS NULL",
            Op::IsNotNull => "IS NOT NULL",
            Op::Add => "+",
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Table references
// ─────────────────────────────────────────────────────────────────────────────

/// Source of rows in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    /// Read from a physical table → `table AS alias`
    /// If type_filter is set, adds filtering on relationship_kind column.
    /// Single type: `alias.relationship_kind = {type_alias:String}`
    /// Multiple types: `alias.relationship_kind IN ({type_alias:Array(String)})`
    Scan {
        table: String,
        alias: String,
        type_filter: Option<Vec<String>>,
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
}

/// SQL JOIN types.
/// - Inner: only matching rows from both sides
/// - Left: all rows from left, matching from right (NULLs if no match)
/// - Right: all rows from right, matching from left
/// - Full: all rows from both sides
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    pub fn as_sql(&self) -> &'static str {
        match self {
            JoinType::Inner => "INNER",
            JoinType::Left => "LEFT",
            JoinType::Right => "RIGHT",
            JoinType::Full => "FULL",
        }
    }
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
}

impl Cte {
    pub fn new(name: impl Into<String>, query: Query) -> Self {
        Self {
            name: name.into(),
            query: Box::new(query),
            recursive: false,
        }
    }

    pub fn recursive(name: impl Into<String>, query: Query) -> Self {
        Self {
            name: name.into(),
            query: Box::new(query),
            recursive: true,
        }
    }
}

/// Complete SQL query:
/// ```sql
/// WITH cte1 AS (...), cte2 AS (...)
/// SELECT ... FROM ... WHERE ... GROUP BY ... ORDER BY ... LIMIT ...
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub ctes: Vec<Cte>,
    pub select: Vec<SelectExpr>,
    pub from: TableRef,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderExpr>,
    pub limit: Option<u32>,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            ctes: vec![],
            select: vec![],
            from: TableRef::Scan {
                table: String::new(),
                alias: String::new(),
                type_filter: None,
            },
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
        }
    }
}

/// Recursive CTE for path finding:
/// ```sql
/// WITH RECURSIVE name AS (
///   base_query
///   UNION ALL
///   recursive_query
/// )
/// final_query
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RecursiveCte {
    pub name: String,
    pub base: Query,
    pub recursive: Query,
    pub max_depth: u32,
    pub final_query: Query,
}

/// Top-level AST node - either a simple query or a recursive CTE.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Query(Box<Query>),
    RecursiveCte(Box<RecursiveCte>),
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

    pub fn lit(value: impl Into<Value>) -> Self {
        Expr::Literal(value.into())
    }

    pub fn func(name: impl Into<String>, args: Vec<Expr>) -> Self {
        Expr::FuncCall {
            name: name.into(),
            args,
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

    /// Combine two expressions with OR.
    pub fn or(left: Expr, right: Expr) -> Expr {
        Expr::binary(Op::Or, left, right)
    }
}

impl TableRef {
    pub fn scan(table: impl Into<String>, alias: impl Into<String>) -> Self {
        TableRef::Scan {
            table: table.into(),
            alias: alias.into(),
            type_filter: None,
        }
    }

    pub fn scan_with_filter(
        table: impl Into<String>,
        alias: impl Into<String>,
        type_filter: Vec<String>,
    ) -> Self {
        TableRef::Scan {
            table: table.into(),
            alias: alias.into(),
            type_filter: Some(type_filter),
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

    pub fn union(queries: Vec<Query>, alias: impl Into<String>) -> Self {
        TableRef::Union {
            queries,
            alias: alias.into(),
        }
    }
}
