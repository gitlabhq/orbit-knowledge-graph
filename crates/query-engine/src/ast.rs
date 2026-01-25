//! SQL-oriented Abstract Syntax Tree
//!
//! Intermediate representation between the LLM's JSON input and final SQL output.
//! Nodes map directly to SQL constructs.

use serde_json::Value;

/// Expression that produces a value
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference: table.column
    Column { table: String, column: String },
    /// Constant value
    Literal(Value),
    /// Function call: COUNT(x), SUM(x), etc.
    FuncCall { name: String, args: Vec<Expr> },
    /// Binary operation: x = y, x AND y, etc.
    BinaryOp { op: Op, left: Box<Expr>, right: Box<Expr> },
    /// Unary operation: NOT x, x IS NULL, etc.
    UnaryOp { op: Op, expr: Box<Expr> },
}

/// SQL operators
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

/// Table reference (scan or join result)
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    /// Reads from a physical table
    Scan {
        table: String,
        alias: String,
        type_filter: Option<String>,
    },
    /// Combines two table references
    Join {
        join_type: JoinType,
        left: Box<TableRef>,
        right: Box<TableRef>,
        on: Expr,
    },
}

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

/// Expression with optional alias for SELECT clause
#[derive(Debug, Clone, PartialEq)]
pub struct SelectExpr {
    pub expr: Expr,
    pub alias: Option<String>,
}

/// Ordering specification
#[derive(Debug, Clone, PartialEq)]
pub struct OrderExpr {
    pub expr: Expr,
    pub desc: bool,
}

/// Complete SQL query
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub select: Vec<SelectExpr>,
    pub from: TableRef,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderExpr>,
    pub limit: Option<u32>,
}

/// Recursive CTE for path finding
#[derive(Debug, Clone, PartialEq)]
pub struct RecursiveCte {
    pub name: String,
    pub base: Query,
    pub recursive: Query,
    pub max_depth: u32,
    pub final_query: Query,
}

/// AST node (either a Query or RecursiveCTE)
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Query(Box<Query>),
    RecursiveCte(Box<RecursiveCte>),
}

// Builder helpers
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

    /// Combine expressions with AND, ignoring None values
    pub fn and_all(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
        exprs
            .into_iter()
            .flatten()
            .reduce(|acc, e| Expr::binary(Op::And, acc, e))
    }

    /// Combine expressions with OR, ignoring None values
    pub fn or_all(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
        exprs
            .into_iter()
            .flatten()
            .reduce(|acc, e| Expr::binary(Op::Or, acc, e))
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
        type_filter: impl Into<String>,
    ) -> Self {
        TableRef::Scan {
            table: table.into(),
            alias: alias.into(),
            type_filter: Some(type_filter.into()),
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
}
