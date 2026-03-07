//! SQL-oriented Abstract Syntax Tree
//!
//! Intermediate representation between JSON input and SQL output.
//! Each node maps directly to ClickHouse SQL constructs.

use serde_json::Value;

// ─────────────────────────────────────────────────────────────────────────────
// ClickHouse parameter types
// ─────────────────────────────────────────────────────────────────────────────

/// ClickHouse types used in parameterized query placeholders (`{pN:Type}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
pub enum ChType {
    String,
    Int64,
    Float64,
    Bool,
}

impl ChType {
    /// Infer ClickHouse type from a JSON value.
    pub fn from_value(v: &Value) -> Self {
        match v {
            Value::Number(n) if n.is_i64() => ChType::Int64,
            Value::Number(_) => ChType::Float64,
            Value::Bool(_) => ChType::Bool,
            _ => ChType::String,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Expressions
// ─────────────────────────────────────────────────────────────────────────────

/// Expression that produces a value in SQL.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference → `table.column`
    Column { table: String, column: String },
    /// Constant value → parameterized as `{pN:Type}`, type inferred from Value.
    Literal(Value),
    /// Constant value with explicit ClickHouse type → `{pN:Type}`.
    Param { data_type: ChType, value: Value },
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
    #[strum(serialize = "ILIKE")]
    ILike,
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
    /// Read from a physical table → `table AS alias`
    Scan { table: String, alias: String },
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
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
/// SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY ... LIMIT ...
/// UNION ALL SELECT ...
/// SETTINGS key = value
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub ctes: Vec<Cte>,
    pub select: Vec<SelectExpr>,
    pub from: TableRef,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderExpr>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    /// Additional queries to UNION ALL with this one (for recursive CTEs).
    pub union_all: Vec<Query>,
    /// ClickHouse SET statements prepended to the query (for recursive CTEs).
    pub set_statements: Vec<(String, String)>,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            ctes: vec![],
            select: vec![],
            from: TableRef::Scan {
                table: String::new(),
                alias: String::new(),
            },
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
            union_all: vec![],
            set_statements: vec![],
        }
    }
}

/// Top-level AST node - either a simple query or a recursive CTE.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Query(Box<Query>),
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

    pub fn param(data_type: ChType, value: impl Into<Value>) -> Self {
        Expr::Param {
            data_type,
            value: value.into(),
        }
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
