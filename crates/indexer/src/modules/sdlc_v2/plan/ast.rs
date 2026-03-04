/// Minimal SQL AST for ETL query building.
///
/// A subset of `query_engine::ast` with one addition: `Expr::Raw` for
/// verbatim SQL fragments (ClickHouse params, CASE expressions, complex
/// DataFusion expressions). This keeps the AST simple while allowing
/// escape hatches for expressions too complex to model structurally.

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference: `table.column` or just `column` when table is empty.
    Column { table: String, column: String },
    /// Verbatim SQL fragment — ClickHouse params, CASE expressions, etc.
    Raw(String),
    /// Binary operation: `(left OP right)`.
    BinaryOp {
        op: Op,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary postfix: `(expr IS NOT NULL)`.
    IsNotNull(Box<Expr>),
    /// Function call: `name(arg1, arg2, ...)`.
    FuncCall { name: String, args: Vec<Expr> },
    /// Type cast: `CAST(expr AS data_type)`.
    Cast {
        expr: Box<Expr>,
        data_type: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Le,
    Gt,
    And,
    Or,
}

impl Op {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "!=",
            Op::Le => "<=",
            Op::Gt => ">",
            Op::And => "AND",
            Op::Or => "OR",
        }
    }
}

/// Expression in SELECT clause: `expr AS alias` or just `expr`.
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

    pub fn bare(expr: Expr) -> Self {
        Self { expr, alias: None }
    }
}

/// Ordering specification — always ascending (cursor pagination is ASC-only).
#[derive(Debug, Clone, PartialEq)]
pub struct OrderExpr {
    pub expr: Expr,
}

/// Source of rows in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    /// Simple table scan: `table` or `table AS alias`.
    Scan {
        table: String,
        alias: Option<String>,
    },
    /// Verbatim FROM clause — complex JOINs from ontology YAML.
    Raw(String),
}

/// Complete SQL query.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub select: Vec<SelectExpr>,
    pub from: TableRef,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderExpr>,
    pub limit: Option<u64>,
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

    pub fn raw(sql: impl Into<String>) -> Self {
        Expr::Raw(sql.into())
    }

    pub fn func(name: impl Into<String>, args: Vec<Expr>) -> Self {
        Expr::FuncCall {
            name: name.into(),
            args,
        }
    }

    pub fn cast(expr: Expr, data_type: impl Into<String>) -> Self {
        Expr::Cast {
            expr: Box::new(expr),
            data_type: data_type.into(),
        }
    }

    pub fn is_not_null(expr: Expr) -> Self {
        Expr::IsNotNull(Box::new(expr))
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

    pub fn and_all(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
        exprs
            .into_iter()
            .flatten()
            .reduce(|a, b| Expr::binary(Op::And, a, b))
    }

    pub fn or_all(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
        exprs
            .into_iter()
            .flatten()
            .reduce(|a, b| Expr::binary(Op::Or, a, b))
    }
}

impl TableRef {
    pub fn scan(table: impl Into<String>, alias: Option<String>) -> Self {
        TableRef::Scan {
            table: table.into(),
            alias,
        }
    }
}
